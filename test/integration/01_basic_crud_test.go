//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func Test01BasicCRUD(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)

	h.Auth.SetMap(
		"test-token",
		"test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email"}},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	events, errCh, closeFn := h.Client.Connect(ctx, "users/42", "test-token")
	defer closeFn()

	if err := h.PG.Exec(ctx,
		"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
		42, "a@b.c", "Alice",
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var ev SSEEvent
	select {
	case ev = <-events:
	case err := <-errCh:
		t.Fatalf("client error: %v", err)
	case <-ctx.Done():
		t.Fatalf("timeout waiting for tx event; stderr:\n%s", h.Binary.Stderr())
	}

	if ev.Type != "tx" {
		t.Fatalf("expected event type %q, got %q (data=%s)", "tx", ev.Type, string(ev.Data))
	}

	var payload struct {
		TxID     uint32 `json:"tx_id"`
		CommitTS string `json:"commit_ts"`
		Changes  []struct {
			Op    string         `json:"op"`
			Table string         `json:"table"`
			PK    string         `json:"pk"`
			Data  map[string]any `json:"data,omitempty"`
		} `json:"changes"`
	}
	if err := json.Unmarshal(ev.Data, &payload); err != nil {
		t.Fatalf("decode payload: %v (raw=%s)", err, string(ev.Data))
	}

	if got, want := len(payload.Changes), 1; got != want {
		t.Fatalf("expected %d change, got %d (raw=%s)", want, got, string(ev.Data))
	}
	c0 := payload.Changes[0]
	if c0.Op != "insert" {
		t.Errorf("op = %q, want %q", c0.Op, "insert")
	}
	if c0.Table != "users" {
		t.Errorf("table = %q, want %q", c0.Table, "users")
	}
	if c0.PK != "42" {
		t.Errorf("pk = %q, want %q", c0.PK, "42")
	}

	if got := c0.Data["id"]; got != "42" {
		t.Errorf("data.id = %v (%T), want %q", got, got, "42")
	}
	if got := c0.Data["email"]; got != "a@b.c" {
		t.Errorf("data.email = %v, want %q", got, "a@b.c")
	}
	if _, present := c0.Data["name"]; present {
		t.Errorf("data.name unexpectedly present (whitelist excludes it): %v", c0.Data["name"])
	}

	if !strings.Contains(h.Binary.Stderr(), "maxprocs") {
		t.Errorf("binary stderr missing automaxprocs marker (Pitfall G9); stderr=\n%s", h.Binary.Stderr())
	}
}
