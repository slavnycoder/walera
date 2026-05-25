package sse

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/wal"
)

var fixedCommitTS = time.Date(2026, 5, 15, 12, 30, 45, 123456789, time.UTC)

func buildTx(commitLSN pglogrepl.LSN, txID uint32, changes []wal.Change) wal.Tx {
	return wal.Tx{
		ID:        txID,
		CommitLSN: commitLSN,
		CommitTS:  fixedCommitTS,
		Changes:   changes,
	}
}

func TestEncoder_TxInsert(t *testing.T) {
	t.Parallel()

	enc := NewEncoder(0)

	tx := buildTx(pglogrepl.LSN(0x16B23A8), 7777, []wal.Change{
		{
			Schema: "public",
			Table:  "users",
			Op:     wal.OpInsert,
			PK:     "42",
			PKCol:  "id",
			Data:   map[string]any{"id": "42", "name": "alice"},
		},
	})
	ev := router.Event{Tx: tx, MatchedIndices: []int{0}}

	out, overflow := enc.Encode(ev)
	if overflow {
		t.Fatalf("unexpected overflow=true with cap=0 (disabled)")
	}
	s := string(out)

	if !strings.HasPrefix(s, "event: tx\n") {
		t.Fatalf("frame must start with %q; got %q", "event: tx\n", s[:min(len(s), 32)])
	}
	if !strings.Contains(s, "id: 7777\n") {
		t.Fatalf("frame must contain %q; got: %s", "id: 7777\\n", s)
	}
	if strings.Contains(s, "commit_lsn") {
		t.Fatalf("frame must NOT contain %q (LSN removed from wire); got: %s", "commit_lsn", s)
	}
	if !strings.Contains(s, "data: {") {
		t.Fatalf("frame must contain %q; got: %s", "data: {", s)
	}
	if !strings.HasSuffix(s, "}\n\n") {
		t.Fatalf("frame must end with %q; got tail %q", "}\\n\\n", s[len(s)-min(len(s), 8):])
	}

	const dataPrefix = "data: "
	dataStart := strings.Index(s, dataPrefix)
	if dataStart < 0 {
		t.Fatalf("no data: prefix in frame: %s", s)
	}
	dataStart += len(dataPrefix)
	dataEnd := strings.Index(s[dataStart:], "\n\n")
	if dataEnd < 0 {
		t.Fatalf("no \\n\\n terminator in frame: %s", s)
	}
	payload := s[dataStart : dataStart+dataEnd]

	var got struct {
		TxID     uint32 `json:"tx_id"`
		CommitTS string `json:"commit_ts"`
		Changes  []struct {
			Op    string         `json:"op"`
			Table string         `json:"table"`
			PK    string         `json:"pk"`
			Data  map[string]any `json:"data"`
		} `json:"changes"`
	}
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatalf("data payload is not JSON: %v\npayload=%s", err, payload)
	}
	if got.TxID != 7777 {
		t.Errorf("tx_id = %d; want 7777", got.TxID)
	}
	if _, err := time.Parse(time.RFC3339Nano, got.CommitTS); err != nil {
		t.Errorf("commit_ts %q is not RFC3339Nano parseable: %v", got.CommitTS, err)
	}
	if len(got.Changes) != 1 {
		t.Fatalf("len(changes) = %d; want 1", len(got.Changes))
	}
	if got.Changes[0].Op != "insert" {
		t.Errorf("changes[0].op = %q; want %q", got.Changes[0].Op, "insert")
	}
	if got.Changes[0].Table != "users" {
		t.Errorf("changes[0].table = %q; want %q (bare table name)", got.Changes[0].Table, "users")
	}
	if got.Changes[0].PK != "42" {
		t.Errorf("changes[0].pk = %q; want %q", got.Changes[0].PK, "42")
	}
	if name, _ := got.Changes[0].Data["name"].(string); name != "alice" {
		t.Errorf("changes[0].data.name = %v; want %q", got.Changes[0].Data["name"], "alice")
	}
}

func TestEncoder_TxUpdate_MatchedSubset(t *testing.T) {
	t.Parallel()

	enc := NewEncoder(0)

	tx := buildTx(pglogrepl.LSN(0x100), 1, []wal.Change{
		{Schema: "public", Table: "orders", Op: wal.OpInsert, PK: "1", Data: map[string]any{"id": "1"}},
		{Schema: "public", Table: "orders", Op: wal.OpUpdate, PK: "2", Changed: map[string]any{"name": "bob"}},
		{Schema: "public", Table: "orders", Op: wal.OpInsert, PK: "3", Data: map[string]any{"id": "3"}},
	})
	ev := router.Event{Tx: tx, MatchedIndices: []int{1}}

	out, overflow := enc.Encode(ev)
	if overflow {
		t.Fatalf("unexpected overflow=true with cap=0")
	}

	const dataPrefix = "data: "
	dataStart := strings.Index(string(out), dataPrefix) + len(dataPrefix)
	dataEnd := strings.Index(string(out[dataStart:]), "\n\n")
	payload := out[dataStart : dataStart+dataEnd]

	var got struct {
		Changes []struct {
			Op   string         `json:"op"`
			PK   string         `json:"pk"`
			Data map[string]any `json:"data"`
		} `json:"changes"`
	}
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("data payload is not JSON: %v", err)
	}
	if len(got.Changes) != 1 {
		t.Fatalf("len(changes) = %d; want 1 (only matched index)", len(got.Changes))
	}
	if got.Changes[0].PK != "2" {
		t.Errorf("changes[0].pk = %q; want %q (matched index 1)", got.Changes[0].PK, "2")
	}
	if got.Changes[0].Op != "update" {
		t.Errorf("changes[0].op = %q; want %q", got.Changes[0].Op, "update")
	}
	if name, _ := got.Changes[0].Data["name"].(string); name != "bob" {
		t.Errorf("changes[0].data.name = %v; want %q", got.Changes[0].Data["name"], "bob")
	}
	if strings.Contains(string(out), `"changed"`) {
		t.Errorf("frame must NOT contain %q (unified into data); got: %s", `"changed"`, out)
	}
}

func TestEncoder_EncodeError(t *testing.T) {
	t.Parallel()

	enc := NewEncoder(0)
	got := string(enc.EncodeError("slow_consumer"))
	want := "event: error\ndata: {\"reason\":\"slow_consumer\"}\n\n"
	if got != want {
		t.Errorf("EncodeError = %q\nwant         %q", got, want)
	}
}

func TestEncoder_EncodeError_QuotesEscaped(t *testing.T) {
	t.Parallel()

	enc := NewEncoder(0)
	got := enc.EncodeError(`a"b`)

	if !bytes.Contains(got, []byte(`{"reason":"a\"b"}`)) {
		t.Errorf("escaped reason missing in frame: %q", got)
	}
}

func TestEncoder_EncodeHeartbeat(t *testing.T) {
	t.Parallel()

	enc := NewEncoder(0)
	got := enc.EncodeHeartbeat()
	want := []byte(":\n\n")
	if !bytes.Equal(got, want) {
		t.Errorf("EncodeHeartbeat = %q; want %q", got, want)
	}
	if len(got) != 3 {
		t.Errorf("len(EncodeHeartbeat()) = %d; want 3", len(got))
	}
}

func TestEncoder_EncodeShutdown(t *testing.T) {
	t.Parallel()

	enc := NewEncoder(0)
	got := enc.EncodeShutdown()
	want := []byte("event: shutdown\ndata: {\"reason\":\"service_restart\"}\n\n")
	if !bytes.Equal(got, want) {
		t.Errorf("EncodeShutdown = %q\nwant            %q", got, want)
	}

	got2 := enc.EncodeShutdown()
	if !bytes.Equal(got, got2) {
		t.Errorf("EncodeShutdown not stable across calls")
	}
}

func TestEncoder_PoolReuse(t *testing.T) {
	t.Parallel()

	enc := NewEncoder(0)
	tx := buildTx(pglogrepl.LSN(0x1), 1, []wal.Change{
		{Schema: "public", Table: "t", Op: wal.OpInsert, PK: "x", Data: map[string]any{"id": "x"}},
	})

	var wg sync.WaitGroup
	const G = 16
	wg.Add(G)
	for g := 0; g < G; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				out, overflow := enc.Encode(router.Event{Tx: tx, MatchedIndices: []int{0}})
				if overflow {
					t.Errorf("frame %d: unexpected overflow=true", i)
					return
				}
				if !bytes.HasPrefix(out, []byte("event: tx\n")) {
					t.Errorf("frame %d corrupted: %q", i, out[:min(len(out), 32)])
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestEncoder_RespectsPayloadCap(t *testing.T) {
	t.Parallel()

	enc := NewEncoder(100)
	tx := buildTx(pglogrepl.LSN(0x42), 99, []wal.Change{
		{
			Schema: "public",
			Table:  "users",
			Op:     wal.OpInsert,
			PK:     "1",
			PKCol:  "id",
			Data: map[string]any{
				"id":   "1",
				"name": strings.Repeat("x", 200),
			},
		},
	})

	out, overflow := enc.Encode(router.Event{Tx: tx, MatchedIndices: []int{0}})
	if !overflow {
		t.Fatalf("expected overflow=true; got overflow=false, len(out)=%d", len(out))
	}
	if out != nil {
		t.Errorf("expected nil bytes on overflow; got %d bytes", len(out))
	}
}

func TestEncoder_BelowCapEncodesNormally(t *testing.T) {
	t.Parallel()

	enc := NewEncoder(10 * 1024 * 1024)
	tx := buildTx(pglogrepl.LSN(0x7), 8, []wal.Change{
		{Schema: "public", Table: "users", Op: wal.OpInsert, PK: "1", PKCol: "id", Data: map[string]any{"id": "1"}},
	})

	out, overflow := enc.Encode(router.Event{Tx: tx, MatchedIndices: []int{0}})
	if overflow {
		t.Fatalf("unexpected overflow=true for small event under 10 MiB cap")
	}
	if !bytes.HasPrefix(out, []byte("event: tx\n")) {
		t.Errorf("frame must start with %q; got %q", "event: tx\n", out[:min(len(out), 32)])
	}
}

func TestEncoder_HeartbeatNotCapped(t *testing.T) {
	t.Parallel()

	enc := NewEncoder(1)
	got := enc.EncodeHeartbeat()
	if len(got) != 3 {
		t.Errorf("len(heartbeat) = %d; want 3", len(got))
	}
	if !bytes.Equal(got, []byte(":\n\n")) {
		t.Errorf("heartbeat = %q; want %q", got, ":\n\n")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
