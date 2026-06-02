//go:build integration

package integration

import (
	"bytes"
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

	// Two subscribers on the SAME hot row with DIFFERENT whitelists. The narrow
	// one must never see a field only the wide one is authorized for. Proves
	// filtering is per-subscriber, not per-tx.
	t.Run("CrossSubscriber_FieldIsolation", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		h.Auth.SetMap("tok-narrow", "user-narrow", []string{"users"},
			map[string][]string{"users": {"id", "name"}})
		h.Auth.SetMap("tok-wide", "user-wide", []string{"users"},
			map[string][]string{"users": {"id", "email", "name"}})

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		evN, errN, closeN := h.Client.Connect(ctx, "users/42", "tok-narrow")
		defer closeN()
		evW, errW, closeW := h.Client.Connect(ctx, "users/42", "tok-wide")
		defer closeW()

		if err := h.PG.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			42, "old@x", "Eve",
		); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
		_ = readTxEvent(ctx, t, h, evN, errN)
		_ = readTxEvent(ctx, t, h, evW, errW)

		// Update both columns so the new tuple carries name regardless of any
		// changed-column diffing.
		if err := h.PG.Exec(ctx,
			"UPDATE users SET email = $1, name = $2 WHERE id = $3",
			"secret@x", "Eve2", 42,
		); err != nil {
			t.Fatalf("update: %v", err)
		}

		pn, ok := readTxWithin(t, evN, errN, 5*time.Second)
		if !ok {
			t.Fatalf("narrow subscriber received no update; stderr:\n%s", h.Binary.Stderr())
		}
		if _, leaked := pn.Changes[0].Data["email"]; leaked {
			t.Errorf("narrow subscriber saw non-whitelisted email: %v", pn.Changes[0].Data["email"])
		}
		if _, ok := pn.Changes[0].Data["name"]; !ok {
			t.Errorf("narrow subscriber missing whitelisted name: %+v", pn.Changes[0].Data)
		}

		pw, ok := readTxWithin(t, evW, errW, 5*time.Second)
		if !ok {
			t.Fatalf("wide subscriber received no update; stderr:\n%s", h.Binary.Stderr())
		}
		if got := pw.Changes[0].Data["email"]; got != "secret@x" {
			t.Errorf("wide subscriber email = %v, want secret@x", got)
		}
	})

	// A case-variant whitelist entry ("Email") must NOT expose the actual column
	// ("email"). Pins the exact, case-sensitive match in Whitelist.Filter — no
	// normalization bypass.
	t.Run("ColumnNameNormalization_NoBypass", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		h.Auth.SetMap("test-token", "test-user", []string{"users"},
			map[string][]string{"users": {"id", "Email"}})

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		events, errCh, closeFn := h.Client.Connect(ctx, "users/55", "test-token")
		defer closeFn()

		if err := h.PG.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			55, "leak@x", "N",
		); err != nil {
			t.Fatalf("insert: %v", err)
		}
		c := readTxEvent(ctx, t, h, events, errCh).Changes[0]
		if _, ok := c.Data["email"]; ok {
			t.Errorf("case-variant whitelist 'Email' exposed column 'email' (bypass): %v", c.Data["email"])
		}
		if got := c.Data["id"]; got != "55" {
			t.Errorf("PK must always survive: data.id = %v", got)
		}
	})

	// A hidden field's value (and the bearer token) must never reach the SSE
	// wire or the process logs. Enforces the "never log row data/tokens" rule.
	t.Run("NoPIIInFramesOrLogs", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		h.Auth.SetMap("tok-canary", "user-canary", []string{"users"},
			map[string][]string{"users": {"id"}})

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		events, errCh, closeFn := h.Client.Connect(ctx, "users/66", "tok-canary")
		defer closeFn()

		const canary = "PII_CANARY_d41d8cd98f"
		if err := h.PG.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			66, canary+"@x", canary,
		); err != nil {
			t.Fatalf("insert: %v", err)
		}

		var raw []byte
		deadline := time.After(10 * time.Second)
		for raw == nil {
			select {
			case ev, ok := <-events:
				if !ok {
					t.Fatalf("events closed before tx; stderr:\n%s", h.Binary.Stderr())
				}
				if ev.Type == "tx" {
					raw = ev.Data
				}
			case err := <-errCh:
				t.Fatalf("client error: %v", err)
			case <-deadline:
				t.Fatalf("no tx within budget; stderr:\n%s", h.Binary.Stderr())
			}
		}
		if bytes.Contains(raw, []byte(canary)) {
			t.Errorf("hidden-field PII canary leaked into SSE frame: %s", raw)
		}

		// Allow any debug logging to flush, then scan stderr for the canary and
		// the bearer token.
		time.Sleep(200 * time.Millisecond)
		assertAbsentInLogs(t, h, canary, "tok-canary")
	})
}
