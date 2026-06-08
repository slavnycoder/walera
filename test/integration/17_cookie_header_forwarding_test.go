//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// Test17CookieHeaderForwarding exercises the cookie+header forwarding feature
// end-to-end through the real SSE handshake and the real auth client running
// inside the spawned binary. Three independent properties are asserted:
//
//  1. Allowlisted cookie + header reach the auth backend on the OpenSession call.
//  2. A non-allowlisted cookie and a non-allowlisted header are stripped before
//     the OpenSession call (they never reach the backend).
//  3. A bearer-less open authenticated purely by an allowlisted session cookie
//     succeeds and streams a subsequent row change.
func Test17CookieHeaderForwarding(t *testing.T) {
	t.Parallel()

	const (
		sessionCookie   = "walera_session" // allowlisted cookie
		tenantHeader    = "X-Tenant-Id"    // allowlisted header
		trackingCookie  = "tracking_id"    // NOT allowlisted
		debugHeader     = "X-Debug-Trace"  // NOT allowlisted
		tenantHeaderVal = "tenant-42"
		sessionVal      = "sess-abc123"
		trackingVal     = "track-should-not-pass"
		debugVal        = "debug-should-not-pass"
	)

	// Allowlisted_Reaches_NonAllowlisted_Stripped: with a bearer token present,
	// the allowlisted cookie+header are threaded into the OpenSession call while
	// the non-allowlisted cookie/header are dropped by ForwardedFromRequest.
	t.Run("Allowlisted_Reaches_NonAllowlisted_Stripped", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t,
			WithForwardedCookies(sessionCookie),
			WithForwardedHeaders(tenantHeader),
		)
		h.Auth.SetMap(
			"test-token",
			"test-user",
			[]string{"users"},
			map[string][]string{"users": {"id", "email"}},
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		events, errCh, closeFn := h.Client.ConnectWith(ctx, "users/42", "test-token",
			WithCookie(sessionCookie, sessionVal),
			WithCookie(trackingCookie, trackingVal),
			WithHeader(tenantHeader, tenantHeaderVal),
			WithHeader(debugHeader, debugVal),
		)
		defer closeFn()

		// Drive one change so we know the handshake (and thus the OpenSession
		// call) completed and the stream is live.
		if err := h.PG.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			42, "a@b.c", "Alice",
		); err != nil {
			t.Fatalf("insert: %v", err)
		}
		p := readTxEvent(ctx, t, h, events, errCh)
		if len(p.Changes) != 1 || p.Changes[0].PK != "42" {
			t.Fatalf("expected one change for pk 42, got %+v", p.Changes)
		}

		// Allowlisted cookie forwarded.
		if got := cookieValue(h.Auth.LastOpenCookies(), sessionCookie); got != sessionVal {
			t.Errorf("allowlisted cookie %q not forwarded to backend: got %q, want %q",
				sessionCookie, got, sessionVal)
		}
		// Non-allowlisted cookie stripped.
		if got := cookieValue(h.Auth.LastOpenCookies(), trackingCookie); got != "" {
			t.Errorf("non-allowlisted cookie %q leaked to backend: got %q", trackingCookie, got)
		}

		hdrs := h.Auth.LastOpenHeaders()
		// Allowlisted header forwarded.
		if got := hdrs.Get(tenantHeader); got != tenantHeaderVal {
			t.Errorf("allowlisted header %q not forwarded to backend: got %q, want %q",
				tenantHeader, got, tenantHeaderVal)
		}
		// Non-allowlisted header stripped.
		if got := hdrs.Get(debugHeader); got != "" {
			t.Errorf("non-allowlisted header %q leaked to backend: got %q", debugHeader, got)
		}

		// Forwarded credential values must never appear in process logs.
		assertAbsentInLogs(t, h, sessionVal, tenantHeaderVal, trackingVal, debugVal)
	})

	// CookieOnly_Open_Succeeds_And_Streams: no bearer at all — the open is
	// authenticated purely by the allowlisted session cookie, and the stream
	// delivers a subsequent change.
	t.Run("CookieOnly_Open_Succeeds_And_Streams", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t,
			WithForwardedCookies(sessionCookie),
		)
		// Register the session cookie -> user (no bearer token registered).
		h.Auth.SetCookieMap(
			sessionCookie, sessionVal,
			"cookie-user",
			[]string{"users"},
			map[string][]string{"users": {"id", "email"}},
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		events, errCh, closeFn := h.Client.ConnectWith(ctx, "users/77", "",
			WithoutBearer(),
			WithCookie(sessionCookie, sessionVal),
		)
		defer closeFn()

		if err := h.PG.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			77, "c@d.e", "Carol",
		); err != nil {
			t.Fatalf("insert: %v", err)
		}

		p := readTxEvent(ctx, t, h, events, errCh)
		if len(p.Changes) != 1 {
			t.Fatalf("cookie-only stream: expected 1 change, got %d (%+v)", len(p.Changes), p.Changes)
		}
		c0 := p.Changes[0]
		if c0.Op != "insert" || c0.Table != "users" || c0.PK != "77" {
			t.Errorf("cookie-only stream: unexpected change op=%q table=%q pk=%q", c0.Op, c0.Table, c0.PK)
		}
		if got := c0.Data["email"]; got != "c@d.e" {
			t.Errorf("cookie-only stream: data.email = %v, want %q", got, "c@d.e")
		}

		// The backend saw the cookie but no Authorization bearer.
		if got := cookieValue(h.Auth.LastOpenCookies(), sessionCookie); got != sessionVal {
			t.Errorf("session cookie %q not forwarded on bearer-less open: got %q", sessionCookie, got)
		}
		if got := h.Auth.LastOpenHeaders().Get("Authorization"); got != "" {
			t.Errorf("bearer-less open unexpectedly carried Authorization: %q", got)
		}

		assertAbsentInLogs(t, h, sessionVal)
	})
}

// cookieValue returns the value of the named cookie in the slice, or "".
func cookieValue(cookies []*http.Cookie, name string) string {
	for _, ck := range cookies {
		if ck.Name == name {
			return ck.Value
		}
	}
	return ""
}
