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
}
