// Package auth — map.go: per-user permission Whitelist + Filter callback.
// See INVARIANTS.md Security/PII §5 (PK always preserved). Filter rule set:
// (1) non-whitelisted table → silent drop, (2) PK column always copied,
// (3) UPDATE with no non-PK whitelisted column → silent drop.
package auth

import (
	"encoding/json"
	"errors"

	"github.com/jackc/pglogrepl"

	"github.com/walera/walera/internal/wal"
)

// Whitelist is the immutable per-user permission snapshot consumed by Filter.
// Constructed via ParseWhitelist; published via atomic.Pointer[Whitelist] swap.
type Whitelist struct {
	// UserID is the authenticated user identifier (opaque to Walera).
	UserID string

	// Tables is the per-table set of allowed readable columns. Absence of the
	// table key denotes a NON-allowed table. Empty set means PK-only.
	Tables map[string]map[string]struct{}

	// TTLSeconds is the snapshot's lifetime; refresh interval for auth.Subscriber.
	TTLSeconds int

	// RefreshLSN is the WAL LSN at which this snapshot becomes effective;
	// stamped by auth.Subscriber.swapMap.
	RefreshLSN pglogrepl.LSN
}

// Allowed reports whether column is in the readable-column set for table.
func (m *Whitelist) Allowed(table, column string) bool {
	if m == nil {
		return false
	}
	cols, ok := m.Tables[table]
	if !ok {
		return false
	}
	_, ok = cols[column]
	return ok
}

// Filter is the per-change authorization callback. See package doc for rules.
func (m *Whitelist) Filter(c wal.Change) (wal.Change, bool) {
	if m == nil {
		return c, true
	}
	cols, ok := m.Tables[c.Table]
	if !ok {
		return c, true
	}

	out := c
	out.Data = nil
	out.Changed = nil

	switch c.Op {
	case wal.OpInsert:
		out.Data = make(map[string]any, len(c.Data))
		for k, v := range c.Data {
			if k == c.PKCol {
				out.Data[k] = v
				continue
			}
			if _, allow := cols[k]; allow {
				out.Data[k] = v
			}
		}
		return out, false

	case wal.OpUpdate:
		out.Changed = make(map[string]any, len(c.Changed))
		keptNonPK := false
		for k, v := range c.Changed {
			if k == c.PKCol {
				out.Changed[k] = v
				continue
			}
			if _, allow := cols[k]; allow {
				out.Changed[k] = v
				keptNonPK = true
			}
		}
		if !keptNonPK {
			// Rule 3: no non-PK whitelisted column survived → silent drop.
			return c, true
		}
		return out, false

	case wal.OpDelete:
		return out, false

	default:
		return c, true
	}
}

// wireMap is the JSON wire shape returned by /auth/permissions. Unknown
// fields are silently ignored by encoding/json.
type wireMap struct {
	UserID     string              `json:"user_id"`
	Tables     map[string][]string `json:"tables"`
	TTLSeconds int                 `json:"ttl_seconds"`
}

// ParseWhitelist decodes the auth-backend response body and validates the
// user_id-non-empty invariant. RefreshLSN is left at zero; auth.Subscriber
// stamps it at the atomic-swap moment.
func ParseWhitelist(body []byte) (*Whitelist, error) {
	var w wireMap
	if err := json.Unmarshal(body, &w); err != nil {
		return nil, err
	}
	if w.UserID == "" {
		return nil, errors.New("auth: ParseWhitelist: user_id empty")
	}

	m := &Whitelist{
		UserID:     w.UserID,
		TTLSeconds: w.TTLSeconds,
		Tables:     make(map[string]map[string]struct{}, len(w.Tables)),
	}
	for tbl, cols := range w.Tables {
		set := make(map[string]struct{}, len(cols))
		for _, c := range cols {
			set[c] = struct{}{}
		}
		m.Tables[tbl] = set
	}
	return m, nil
}
