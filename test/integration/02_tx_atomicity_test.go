//go:build integration

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

	// One transaction, multiple changes for the SAME anchor row (users:1):
	// INSERT then two UPDATEs. Under the multi_root guard (spec §1.6) this is
	// the only legal shape for >1 change per tx on the anchor table — multiple
	// distinct PKs in one tx would be dropped per-subscriber as a writer-side
	// discipline violation (see README "Writer-side discipline").
	if err := h.PG.ExecBatch(ctx,
		[]string{
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			"UPDATE users SET email = $2 WHERE id = $1",
			"UPDATE users SET name = $2 WHERE id = $1",
		},
		[][]any{
			{1, "u1@x", "Alice"},
			{1, "u1@new"},
			{1, "Alice2"},
		},
	); err != nil {
		t.Fatalf("multi-change tx: %v", err)
	}

	ev1 := readTxEvent(ctx, t, h, events, errCh)
	if got, want := len(ev1.Changes), 3; got != want {
		t.Fatalf("multi-change tx: expected %d changes in one Event, got %d (raw=%v)", want, got, ev1.Changes)
	}
	wantOps := []string{"insert", "update", "update"}
	for i, c := range ev1.Changes {
		if c.PK != "1" {
			t.Errorf("change[%d].pk = %q, want %q (single-anchor tx)", i, c.PK, "1")
		}
		if c.Op != wantOps[i] {
			t.Errorf("change[%d].op = %q, want %q", i, c.Op, wantOps[i])
		}
		if c.Table != "users" {
			t.Errorf("change[%d].table = %q, want %q", i, c.Table, "users")
		}
	}

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

type txEventPayload struct {
	TxID     uint32 `json:"tx_id"`
	CommitTS string `json:"commit_ts"`
	Changes  []struct {
		Op    string         `json:"op"`
		Table string         `json:"table"`
		PK    string         `json:"pk"`
		Data  map[string]any `json:"data,omitempty"`
	} `json:"changes"`
}

func readTxEvent(ctx context.Context, t *testing.T, h *Harness, events <-chan SSEEvent, errCh <-chan error) txEventPayload {
	t.Helper()
	for {
		select {
		case ev := <-events:
			if ev.Type == "" {

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
