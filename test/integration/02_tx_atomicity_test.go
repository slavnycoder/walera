//go:build integration

// Package integration — scenario 02: per-transaction atomicity.
//
// A multi-row INSERT inside a single COMMIT must arrive at the subscriber
// as ONE SSEEvent with N changes — never as N separate Events. This is the
// router's "one Event per subscriber per tx" invariant.
//
// To prove the assertion isn't accidentally measuring buffer accumulation,
// the test follows the multi-row tx with a SEPARATE single-row tx and
// confirms the subscriber sees a distinct second Event with one change.
package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func Test02TxAtomicity(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)

	// Wildcard subscription so every users-row INSERT routes regardless of PK.
	h.Auth.SetMap(
		"test-token",
		"test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email", "name"}},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	events, errCh, closeFn := h.Client.Connect(ctx, "users/all", "test-token")
	defer closeFn()

	// First tx — three rows inside a single BEGIN/COMMIT.
	if err := h.PG.ExecBatch(ctx,
		[]string{
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
		},
		[][]any{
			{1, "u1@x", "Alice"},
			{2, "u2@x", "Bob"},
			{3, "u3@x", "Cara"},
		},
	); err != nil {
		t.Fatalf("multi-row tx: %v", err)
	}

	ev1 := readTxEvent(ctx, t, h, events, errCh)
	if got, want := len(ev1.Changes), 3; got != want {
		t.Fatalf("multi-row tx: expected %d changes in one Event, got %d (raw=%v)", want, got, ev1.Changes)
	}
	pks := []string{ev1.Changes[0].PK, ev1.Changes[1].PK, ev1.Changes[2].PK}
	if pks[0] != "1" || pks[1] != "2" || pks[2] != "3" {
		t.Errorf("multi-row tx pk order = %v, want [1 2 3]", pks)
	}
	for i, c := range ev1.Changes {
		if c.Op != "insert" {
			t.Errorf("change[%d].op = %q, want %q", i, c.Op, "insert")
		}
		if c.Table != "users" {
			t.Errorf("change[%d].table = %q, want %q", i, c.Table, "users")
		}
	}

	// Second tx — single row. Distinct Event proves the previous Event's
	// 3-change accumulation was NOT buffer aggregation across tx boundaries.
	if err := h.PG.Exec(ctx,
		"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
		4, "u4@x", "Dora",
	); err != nil {
		t.Fatalf("single-row tx: %v", err)
	}
	ev2 := readTxEvent(ctx, t, h, events, errCh)
	if got, want := len(ev2.Changes), 1; got != want {
		t.Fatalf("single-row tx: expected %d change, got %d (raw=%v)", want, got, ev2.Changes)
	}
	if ev2.Changes[0].PK != "4" {
		t.Errorf("single-row tx pk = %q, want %q", ev2.Changes[0].PK, "4")
	}
}

// txEventPayload is the decoded shape of an SSE "tx" event's data field.
// Lives in scenario_02 so it's free to evolve as later scenarios need more
// fields; helpers in other scenario files use scenario-local shapes.
type txEventPayload struct {
	TxID     uint32 `json:"tx_id"`
	CommitTS string `json:"commit_ts"`
	Changes  []struct {
		Op      string         `json:"op"`
		Table   string         `json:"table"`
		PK      string         `json:"pk"`
		Data    map[string]any `json:"data,omitempty"`
		Changed map[string]any `json:"changed,omitempty"`
	} `json:"changes"`
}

func readTxEvent(ctx context.Context, t *testing.T, h *Harness, events <-chan SSEEvent, errCh <-chan error) txEventPayload {
	t.Helper()
	for {
		select {
		case ev := <-events:
			if ev.Type == "" {
				// Heartbeat (parsed as zero-type) — skip.
				continue
			}
			if ev.Type != "tx" {
				t.Fatalf("expected tx event, got %q (data=%s)", ev.Type, string(ev.Data))
			}
			var p txEventPayload
			if err := json.Unmarshal(ev.Data, &p); err != nil {
				t.Fatalf("decode payload: %v (raw=%s)", err, string(ev.Data))
			}
			return p
		case err := <-errCh:
			t.Fatalf("client error: %v", err)
		case <-ctx.Done():
			t.Fatalf("timeout waiting for tx event; stderr:\n%s", h.Binary.Stderr())
		}
	}
}
