package auth

import (
	"encoding/json"
	"errors"

	"github.com/jackc/pglogrepl"

	"github.com/walera/walera/internal/wal"
)

type Whitelist struct {
	UserID string

	Tables map[string]map[string]struct{}

	TTLSeconds int

	RefreshLSN pglogrepl.LSN
}

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

func (m *Whitelist) Filter(c wal.Change) (wal.Change, bool) {
	if m == nil {
		return c, true
	}
	cols, ok := m.Tables[c.Table]
	if !ok {
		// Absent-table gate (D-07 / TXN-03): drop all ops for tables not in the whitelist,
		// including PK-only OpDelete events. Clear Data and Changed so no row content leaks
		// even if a caller mistakenly ignores drop=true.
		sanitized := c
		sanitized.Data = nil
		sanitized.Changed = nil
		return sanitized, true
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

			return c, true
		}
		return out, false

	case wal.OpDelete:
		return out, false

	default:
		return c, true
	}
}

type wireMap struct {
	UserID     string              `json:"user_id"`
	Tables     map[string][]string `json:"tables"`
	TTLSeconds int                 `json:"ttl_seconds"`
}

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
