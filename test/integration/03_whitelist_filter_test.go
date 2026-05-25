//go:build integration

// Package integration — scenario 03: whitelist field filtering.
//
// Three sub-tests:
//
//  1. PK-only whitelist: subscribe with fields={"users": ["id"]} (no email,
//     no name); INSERT a row with all three fields; assert only the PK
//     survives in data. PK MUST always be present in the payload, even
//     when the whitelist excludes it.
//
//  2. Hidden-update silent drop: with PK-only whitelist, UPDATE the row's
//     `name` (non-whitelisted) column only; assert NO event is delivered
//     within a 2s budget. (The router's Filter dispatch returns drop=true
//     when filtered changes count drops to zero.)
//
//  3. Whitelisted-update: with fields={"users": ["id", "email"]}, UPDATE
//     the row's `email` column; assert the event arrives carrying the
//     changed `email` field (and the PK).
package integration

import (
	"context"
	"testing"
	"time"
)

func Test03WhitelistFilter(t *testing.T) {
	t.Parallel()

	t.Run("PKOnly_InsertHidesNonWhitelisted", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		// PK-only whitelist — neither email nor name should appear in data.
		h.Auth.SetMap(
			"test-token",
			"test-user",
			[]string{"users"},
			map[string][]string{"users": {"id"}},
		)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		events, errCh, closeFn := h.Client.Connect(ctx, "users/77", "test-token")
		defer closeFn()

		if err := h.PG.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			77, "hidden@x", "Hidden",
		); err != nil {
			t.Fatalf("insert: %v", err)
		}
		p := readTxEvent(ctx, t, h, events, errCh)
		if len(p.Changes) != 1 {
			t.Fatalf("expected 1 change, got %d", len(p.Changes))
		}
		c := p.Changes[0]
		if c.PK != "77" {
			t.Errorf("pk = %q, want %q", c.PK, "77")
		}
		// PK column MUST appear in data even when whitelist excludes it.
		if got := c.Data["id"]; got != "77" {
			t.Errorf("data.id = %v (%T), want %q (PK must always survive)", got, got, "77")
		}
		if _, ok := c.Data["email"]; ok {
			t.Errorf("data.email leaked through whitelist: %v", c.Data["email"])
		}
		if _, ok := c.Data["name"]; ok {
			t.Errorf("data.name leaked through whitelist: %v", c.Data["name"])
		}
	})

	t.Run("HiddenUpdateOnly_SilentDrop", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		// PK-only whitelist — UPDATE of `name` only has zero whitelisted columns.
		h.Auth.SetMap(
			"test-token",
			"test-user",
			[]string{"users"},
			map[string][]string{"users": {"id"}},
		)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		events, errCh, closeFn := h.Client.Connect(ctx, "users/88", "test-token")
		defer closeFn()

		// Seed the row (INSERT delivers — confirms wiring) then UPDATE only
		// the non-whitelisted column and assert silent drop.
		if err := h.PG.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			88, "init@x", "Init",
		); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
		_ = readTxEvent(ctx, t, h, events, errCh)

		if err := h.PG.Exec(ctx,
			"UPDATE users SET name = $1 WHERE id = $2",
			"NewName", 88,
		); err != nil {
			t.Fatalf("update: %v", err)
		}
		// Expect NO tx event within 2s — the router silently drops UPDATEs
		// whose changed columns are all non-whitelisted.
		// Heartbeats may arrive (router heartbeat is 1s in test config) and
		// must not be confused with a tx event.
		dropCtx, dropCancel := context.WithTimeout(ctx, 2*time.Second)
		defer dropCancel()
		for {
			select {
			case ev := <-events:
				if ev.Type == "tx" {
					t.Fatalf("expected silent drop, got tx event: %s", string(ev.Data))
				}
				// Heartbeats / errors are surfaced via errCh below; ignore.
			case err := <-errCh:
				t.Fatalf("client error during silent-drop window: %v", err)
			case <-dropCtx.Done():
				return // budget elapsed without a tx event → pass.
			}
		}
	})

	t.Run("WhitelistedUpdate_DeliversChangedField", func(t *testing.T) {
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
		events, errCh, closeFn := h.Client.Connect(ctx, "users/99", "test-token")
		defer closeFn()

		if err := h.PG.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			99, "old@x", "Eve",
		); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
		ins := readTxEvent(ctx, t, h, events, errCh)
		if ins.Changes[0].Op != "insert" {
			t.Errorf("seed: op = %q, want insert", ins.Changes[0].Op)
		}

		if err := h.PG.Exec(ctx,
			"UPDATE users SET email = $1 WHERE id = $2",
			"new@x", 99,
		); err != nil {
			t.Fatalf("update: %v", err)
		}
		upd := readTxEvent(ctx, t, h, events, errCh)
		if len(upd.Changes) != 1 {
			t.Fatalf("expected 1 change, got %d", len(upd.Changes))
		}
		c := upd.Changes[0]
		if c.Op != "update" {
			t.Errorf("op = %q, want %q", c.Op, "update")
		}
		if c.PK != "99" {
			t.Errorf("pk = %q, want %q", c.PK, "99")
		}
		// Whitelist excludes `name`; even if PG WAL emits it (it shouldn't
		// for a column that did not change), the auth filter strips it.
		if got := c.Changed["email"]; got != "new@x" {
			t.Errorf("changed.email = %v, want %q", got, "new@x")
		}
		if _, ok := c.Changed["name"]; ok {
			t.Errorf("changed.name leaked through whitelist: %v", c.Changed["name"])
		}
	})
}

// (Helper readTxEvent is defined in 02_tx_atomicity_test.go.)
