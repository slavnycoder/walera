//go:build integration

// Package integration — scenario 01: basic CRUD (spec §9 #1).
//
// Subscribe to "users:42" with a whitelist of {id, email}; INSERT a row with
// (id=42, email=a@b.c, name=Alice); assert one SSEEvent of type "tx" arrives
// carrying one Change with op=insert, pk=42, data.id=42, data.email=a@b.c,
// and data.name absent (whitelist excludes it).
//
// Also smoke-tests Pitfall G9: the spawned binary's stderr must mention
// "automaxprocs" — proves the side-effect import survived the build.
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

	// Install the whitelist for our token / user. The mock binds:
	//   token "test-token" → user "test-user" → tables=[users], fields=id,email.
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

	// At this point the SSE handler has Register()'d the subscriber and
	// written 200; the start_lsn is wal.CurrentLSN() at registration time,
	// which is 0 (or close to 0) on a freshly-booted reader. Subsequent txs
	// will pass the start_lsn cutoff.
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
	// data.id is OIDInt8 → string in the wal mapper.
	if got := c0.Data["id"]; got != "42" {
		t.Errorf("data.id = %v (%T), want %q", got, got, "42")
	}
	if got := c0.Data["email"]; got != "a@b.c" {
		t.Errorf("data.email = %v, want %q", got, "a@b.c")
	}
	if _, present := c0.Data["name"]; present {
		t.Errorf("data.name unexpectedly present (whitelist excludes it): %v", c0.Data["name"])
	}

	// Pitfall G9 smoke test — automaxprocs side-effect import must have run.
	// zerolog's debug logs from automaxprocs land on stderr because the
	// binary writes structured JSON to stderr (D-26). The literal string
	// "automaxprocs" appears in its setup log line.
	if !strings.Contains(h.Binary.Stderr(), "maxprocs") {
		t.Errorf("binary stderr missing automaxprocs marker (Pitfall G9); stderr=\n%s", h.Binary.Stderr())
	}
}
