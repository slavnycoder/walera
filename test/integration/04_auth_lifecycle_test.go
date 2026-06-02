//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func Test04AuthLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("Reject_401_BeforeSSEHeaders", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)

		h.Auth.SetMap(
			"test-token",
			"test-user",
			[]string{"users"},
			map[string][]string{"users": {"id"}},
		)
		h.Auth.Revoke("test-user")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		events, errCh, closeFn := h.Client.Connect(ctx, "users/42", "test-token")
		defer closeFn()

		assert401 := func(err error) {
			t.Helper()
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) {
				t.Fatalf("expected *HTTPError, got %T: %v", err, err)
			}
			if httpErr.Status != 401 {
				t.Fatalf("status = %d, want 401 (body=%s)", httpErr.Status, httpErr.Body)
			}
			if !bytes.Contains(httpErr.Body, []byte("revoked")) {
				t.Fatalf("body missing 'revoked': %s", httpErr.Body)
			}
		}

		select {
		case err := <-errCh:
			assert401(err)
		case ev, ok := <-events:
			if ok {
				t.Fatalf("unexpected event on rejected connect: %+v", ev)
			}

			select {
			case err := <-errCh:
				assert401(err)
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("events closed without errCh delivering *HTTPError; stderr:\n%s", h.Binary.Stderr())
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for 401 rejection; stderr:\n%s", h.Binary.Stderr())
		}
	})

	t.Run("Connect_OK_ThenRevoke_MidStream", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		h.Auth.SetMap(
			"test-token",
			"test-user",
			[]string{"users"},
			map[string][]string{"users": {"id", "email"}},
		)

		if err := h.Auth.SetTTL("test-token", 1); err != nil {
			t.Fatalf("SetTTL: %v", err)
		}

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
		_ = readTxEvent(ctx, t, h, events, errCh)

		h.Auth.Revoke("test-user")

		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case ev, ok := <-events:
				if !ok {

					t.Fatalf("events channel closed before observing auth_revoked")
				}
				if ev.Type == "error" && strings.Contains(string(ev.Data), "auth_revoked") {
					return
				}

			case err := <-errCh:

				t.Fatalf("client error before observing auth_revoked: %v", err)
			case <-time.After(250 * time.Millisecond):

			case <-ctx.Done():
				t.Fatalf("ctx done while waiting for auth_revoked; stderr:\n%s", h.Binary.Stderr())
			}
		}
		t.Fatalf("did not observe auth_revoked within budget; stderr:\n%s", h.Binary.Stderr())
	})

	t.Run("Reject_Forbidden_NotAllowed", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)

		h.Auth.SetMap(
			"test-token",
			"test-user",
			[]string{"orders"},
			map[string][]string{"orders": {"id"}},
		)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		events, errCh, closeFn := h.Client.Connect(ctx, "users/42", "test-token")
		defer closeFn()

		assert403 := func(err error) {
			t.Helper()
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) {
				t.Fatalf("expected *HTTPError, got %T: %v", err, err)
			}
			if httpErr.Status != 403 {
				t.Fatalf("status = %d, want 403 (body=%s)", httpErr.Status, httpErr.Body)
			}
			if !bytes.Contains(httpErr.Body, []byte("not_allowed")) {
				t.Fatalf("body missing 'not_allowed': %s", httpErr.Body)
			}
		}

		select {
		case err := <-errCh:
			assert403(err)
		case ev, ok := <-events:
			if ok {
				t.Fatalf("unexpected event on forbidden connect: %+v", ev)
			}
			select {
			case err := <-errCh:
				assert403(err)
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("events closed without errCh delivering *HTTPError; stderr:\n%s", h.Binary.Stderr())
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for 403; stderr:\n%s", h.Binary.Stderr())
		}
	})

	// A refresh that NARROWS fields (drops a column but keeps the channel table)
	// must take effect mid-stream: the dropped field stops being delivered, and
	// the subscription stays alive (narrowing is not revocation).
	t.Run("Narrow_Fields_MidStream", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		h.Auth.SetMap("test-token", "test-user", []string{"users"},
			map[string][]string{"users": {"id", "email", "name"}})

		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		events, errCh, closeFn := h.Client.Connect(ctx, "users/77", "test-token")
		defer closeFn()

		if err := h.PG.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			77, "a@b.c", "Al",
		); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
		ins := readTxEvent(ctx, t, h, events, errCh)
		if _, ok := ins.Changes[0].Data["email"]; !ok {
			t.Fatalf("precondition: wide whitelist should expose email, got %+v", ins.Changes[0].Data)
		}

		// Narrow the whitelist (drop email) and wait for the refresh loop
		// (TTL=1s) to pick it up before mutating data again.
		base := h.Auth.PermissionsRequestCount()
		h.Auth.SetMap("test-token", "test-user", []string{"users"},
			map[string][]string{"users": {"id", "name"}})
		waitForRefresh(t, h, base, 1, 10*time.Second)
		time.Sleep(300 * time.Millisecond) // let walera swap the map post-refresh

		if err := h.PG.Exec(ctx,
			"UPDATE users SET email = $1, name = $2 WHERE id = $3",
			"after@x", "Al2", 77,
		); err != nil {
			t.Fatalf("update: %v", err)
		}

		p, ok := readTxWithin(t, events, errCh, 5*time.Second)
		if !ok {
			t.Fatalf("no update delivered after narrowing; stderr:\n%s", h.Binary.Stderr())
		}
		if _, leaked := p.Changes[0].Data["email"]; leaked {
			t.Errorf("email delivered after removal from whitelist: %v", p.Changes[0].Data["email"])
		}
		if _, ok := p.Changes[0].Data["name"]; !ok {
			t.Errorf("name should still be delivered after narrowing: %+v", p.Changes[0].Data)
		}

		// Subscription must remain alive — narrowing is not revocation.
		select {
		case ev, ok := <-events:
			if ok && ev.Type == "error" && strings.Contains(string(ev.Data), "auth_revoked") {
				t.Errorf("field narrowing wrongly triggered auth_revoked")
			}
		case <-time.After(500 * time.Millisecond):
		}
	})
}
