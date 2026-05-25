// Package sse — SSE wire-format (spec §3.5) producer. Each Encode call
// returns a single contiguous []byte for one frame; a sync.Pool of
// *bytes.Buffer keeps allocations bounded under high tx/s.
package sse

import (
	"bytes"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/wal"
)

// heartbeatBytes is the canonical SSE comment-line heartbeat (":\n\n").
// Never mutated; callers share a reference. Returned by
// (*Encoder).EncodeHeartbeat for API symmetry.
var heartbeatBytes = []byte(":\n\n")

// shutdownBytes is the canonical spec §3.5 shutdown frame
// ("event: shutdown\ndata: {\"reason\":\"service_restart\"}\n\n").
// Emitted by the writer's Done-arm when sub.Reason() == "shutdown" —
// distinct event name from EncodeError(reason="shutdown") so clients
// branch on event type without parsing data. Never mutated.
var shutdownBytes = []byte("event: shutdown\ndata: {\"reason\":\"service_restart\"}\n\n")

// txEvent is the JSON shape of one transaction event in the SSE data
// payload. Lifted from cmd/cdc-sse/main.go verbatim so the encoding is
// identical.
type txEvent struct {
	TxID      uint32        `json:"tx_id"`
	CommitLSN string        `json:"commit_lsn"` // pglogrepl.LSN.String() — "0/16B23A8"
	CommitTS  string        `json:"commit_ts"`  // time.RFC3339Nano UTC
	Changes   []changeEvent `json:"changes"`
}

// changeEvent is the JSON shape of one DML change within a tx event.
// Bare table name (no schema prefix) per spec §3.5.
type changeEvent struct {
	Op      string         `json:"op"`
	Table   string         `json:"table"`
	PK      string         `json:"pk"`
	Data    map[string]any `json:"data,omitempty"`    // INSERT: full new row
	Changed map[string]any `json:"changed,omitempty"` // UPDATE: only changed columns
}

// txToEvent converts a wal.Tx to the JSON-serialisable txEvent shape,
// emitting only the changes whose index appears in matched. When matched
// is nil every change in tx.Changes is emitted (test/tool convenience;
// the router always supplies non-nil matched).
func txToEvent(tx wal.Tx, matched []int) txEvent {
	var changes []changeEvent
	if matched == nil {
		changes = make([]changeEvent, 0, len(tx.Changes))
		for _, ch := range tx.Changes {
			changes = append(changes, changeEvent{
				Op:      string(ch.Op),
				Table:   ch.Table,
				PK:      ch.PK,
				Data:    ch.Data,
				Changed: ch.Changed,
			})
		}
	} else {
		changes = make([]changeEvent, 0, len(matched))
		for _, idx := range matched {
			if idx < 0 || idx >= len(tx.Changes) {
				continue // defensive — router builds matched from len(Changes)
			}
			ch := tx.Changes[idx]
			changes = append(changes, changeEvent{
				Op:      string(ch.Op),
				Table:   ch.Table,
				PK:      ch.PK,
				Data:    ch.Data,
				Changed: ch.Changed,
			})
		}
	}
	return txEvent{
		TxID:      tx.ID,
		CommitLSN: tx.CommitLSN.String(),
		CommitTS:  tx.CommitTS.UTC().Format(time.RFC3339Nano),
		Changes:   changes,
	}
}

// Encoder produces SSE wire-format byte slices from router.Event values
// and from reason strings (error frame). Safe for concurrent use across
// many writer goroutines — the underlying buffer is taken from / returned
// to a sync.Pool on each call. MaxPayloadBytes caps the serialized frame
// size; on overflow Encode returns (nil, true) and the writer drops the
// subscriber with reason "tx_too_large". Zero disables the cap.
type Encoder struct {
	bufPool         sync.Pool
	maxPayloadBytes int
}

// NewEncoder constructs an Encoder with a primed buffer pool and the
// supplied payload cap. Pass 0 to disable the cap.
func NewEncoder(maxPayloadBytes int) *Encoder {
	return &Encoder{
		bufPool: sync.Pool{
			New: func() any { return new(bytes.Buffer) },
		},
		maxPayloadBytes: maxPayloadBytes,
	}
}

// Encode produces the SSE frame bytes for one router.Event:
// "event: tx\nid: <commit_lsn>/<tx_id>\ndata: <json>\n\n". Returns
// (frameBytes, false) on success; (nil, true) when the serialized frame
// exceeds maxPayloadBytes. The returned slice is a fresh copy — the
// pooled buffer is returned before Encode returns.
func (e *Encoder) Encode(ev router.Event) ([]byte, bool) {
	buf := e.bufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		e.bufPool.Put(buf)
	}()

	buf.WriteString("event: tx\n")
	buf.WriteString("id: ")
	buf.WriteString(ev.Tx.CommitLSN.String())
	buf.WriteByte('/')
	buf.WriteString(strconv.FormatUint(uint64(ev.Tx.ID), 10))
	buf.WriteString("\ndata: ")

	payload, err := json.Marshal(txToEvent(ev.Tx, ev.MatchedIndices))
	if err != nil {
		// json.Marshal on these types effectively cannot fail short of an
		// unsupported value inside Data/Changed. Emit a minimal placeholder
		// so the writer loop stays simple.
		payload = []byte(`{"tx_id":0,"commit_lsn":"","commit_ts":"","changes":[]}`)
	}
	buf.Write(payload)
	buf.WriteString("\n\n")

	// Overflow check AFTER full serialization so the cap applies to the
	// final wire frame (envelope + id: header). Zero cap disables.
	if e.maxPayloadBytes > 0 && buf.Len() > e.maxPayloadBytes {
		return nil, true
	}

	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, false
}

// EncodeError produces the SSE error frame
// "event: error\ndata: {\"reason\":\"<reason>\"}\n\n"; reason is
// JSON-escaped via json.Marshal so embedded quotes / backslashes /
// control chars are safe.
func (e *Encoder) EncodeError(reason string) []byte {
	buf := e.bufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		e.bufPool.Put(buf)
	}()

	reasonJSON, err := json.Marshal(reason)
	if err != nil {
		// json.Marshal of a string cannot fail; defensive fallback.
		reasonJSON = []byte(`"unknown"`)
	}

	buf.WriteString("event: error\ndata: {\"reason\":")
	buf.Write(reasonJSON)
	buf.WriteString("}\n\n")

	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out
}

// EncodeHeartbeat returns the SSE heartbeat comment ":\n\n". The returned
// slice is a shared, immutable package-level constant — do NOT mutate.
func (e *Encoder) EncodeHeartbeat() []byte {
	return heartbeatBytes
}

// EncodeShutdown returns the spec §3.5 shutdown frame. The returned slice
// is a shared, immutable package-level constant — do NOT mutate.
func (e *Encoder) EncodeShutdown() []byte {
	return shutdownBytes
}
