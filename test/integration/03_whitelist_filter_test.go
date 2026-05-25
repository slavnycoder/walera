//go:build integration

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

		dropCtx, dropCancel := context.WithTimeout(ctx, 2*time.Second)
		defer dropCancel()
		for {
			select {
			case ev := <-events:
				if ev.Type == "tx" {
					t.Fatalf("expected silent drop, got tx event: %s", string(ev.Data))
				}

			case err := <-errCh:
				t.Fatalf("client error during silent-drop window: %v", err)
			case <-dropCtx.Done():
				return
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

		if got := c.Data["email"]; got != "new@x" {
			t.Errorf("data.email = %v, want %q", got, "new@x")
		}
		if _, ok := c.Data["name"]; ok {
			t.Errorf("data.name leaked through whitelist: %v", c.Data["name"])
		}
	})
}
