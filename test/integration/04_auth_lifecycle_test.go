//go:build integration

// Package integration — scenario 04: auth lifecycle.
//
// Three sub-tests:
//
//  1. Reject_401_BeforeSSEHeaders: unknown token → the SSE handler forwards
//     the auth backend's 401 verbatim BEFORE any SSE headers are written.
//     We assert by observing the errCh (Connect's status-check fails the
//     non-200 path) and that no events are received.
//
//  2. Connect_OK_ThenRevoke_MidStream: token is valid → INSERT delivers →
//     Revoke the user → within 2s the background auth-refresh ticker
//     observes 401 → the subscriber receives `event: error, data:
//     {"reason":"auth_revoked"}` and the connection closes.
//
//  3. Reject_Forbidden_NotAllowed: the auth backend returns a 200 map that
//     does not contain the requested table in Tables; the SSE handler
//     converts that to 403 {"reason":"not_allowed"} before SSE headers.
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
		// Do NOT call SetMap — the mock returns 404 for unknown tokens (which
		// the SSE handler forwards verbatim). For a deterministic 401 we
		// install a map then Revoke the user.
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

		// Connect must surface *HTTPError on errCh BEFORE events is closed-
		// and-empty (TEST-07). Go's select randomizes among ready cases and
		// <-events on a closed channel is immediately ready, so we may pick
		// the events branch first; in that case the typed error is already
		// queued on errCh (cap 1; send-before-close in sse_client.go) and we
		// re-poll with a short budget.
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
			// events closed; the error MUST have arrived on errCh — re-poll.
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
		// TTL 1s — the refresh ticker fires shortly after Revoke; the test
		// budget of 5s is well above the worst-case refresh+jitter window.
		if err := h.Auth.SetTTL("test-token", 1); err != nil {
			t.Fatalf("SetTTL: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		events, errCh, closeFn := h.Client.Connect(ctx, "users/42", "test-token")
		defer closeFn()

		// Confirm wiring with a happy-path INSERT first.
		if err := h.PG.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			42, "a@b.c", "Alice",
		); err != nil {
			t.Fatalf("insert: %v", err)
		}
		_ = readTxEvent(ctx, t, h, events, errCh)

		// Flip the mock to 401-on-next-refresh.
		h.Auth.Revoke("test-user")

		// Within ~5s the refresh ticker should fire, see 401, and the
		// subscriber should be Drop("auth_revoked")'d. The writer's
		// terminal frame is `event: error, data: {"reason":"auth_revoked"}`.
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case ev, ok := <-events:
				if !ok {
					// Channel closed = connection ended; acceptable if we
					// already observed the error event below.
					t.Fatalf("events channel closed before observing auth_revoked")
				}
				if ev.Type == "error" && strings.Contains(string(ev.Data), "auth_revoked") {
					return // success
				}
				// Heartbeats / unrelated events: keep waiting.
			case err := <-errCh:
				// Stream ended after the error frame — re-poll buffer once
				// to surface the terminal event. Here we treat a clean EOF
				// AFTER the deadline as failure, but a close that arrives
				// before we observed the error frame is also failure.
				t.Fatalf("client error before observing auth_revoked: %v", err)
			case <-time.After(250 * time.Millisecond):
				// Tick — retry in the outer for loop.
			case <-ctx.Done():
				t.Fatalf("ctx done while waiting for auth_revoked; stderr:\n%s", h.Binary.Stderr())
			}
		}
		t.Fatalf("did not observe auth_revoked within budget; stderr:\n%s", h.Binary.Stderr())
	})

	t.Run("Reject_Forbidden_NotAllowed", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		// Map does not include the requested `users` table, so the handler
		// returns 403 with body {"reason":"not_allowed"}.
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

		// As with Reject_401_BeforeSSEHeaders: errCh delivers *HTTPError
		// BEFORE events is closed, but select may race-pick the closed-events
		// branch first; re-poll errCh in that case.
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
