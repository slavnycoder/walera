//go:build integration

// Package integration — scenario SEC-02: oversized X-Request-ID rejection
// (SEC-02 / F-P1-02 regression coverage; TEST-09.2).
//
// Locks the validRequestID defence in internal/sse/handler.go:117-133,
// 484-492. A 256-byte X-Request-ID exceeds the 128-byte cap; the handler
// MUST return 400 application/json {"error":"invalid_request_id"} BEFORE
// any /auth/permissions call. The mock auth backend's request counter
// MUST be unchanged across the request — the threat the defence closes
// is log + backend amplification, NOT client-side rejection alone.
//
// Inline http.NewRequestWithContext is required because sse_client.Connect
// hard-codes X-Request-ID via randomHex(16) at sse_client.go:117 and the
// existing Client API does not expose a header override. Mirrors the
// unit-test pattern at internal/sse/handler_test.go:1309-1312.
package integration

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func Test_SEC02_OversizedRequestID_Rejected(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)

	// Configure a valid permission map even though we expect ZERO calls
	// to the backend. The point: a regression that DID forward the bad
	// ID would 500 (no map) instead of silently bypassing the defence
	// — but the security-relevant invariant is PermissionsRequestCount()
	// unchanged, NOT the 5xx. Having the map installed keeps the failure
	// mode crisp.
	h.Auth.SetMap(
		"test-token",
		"test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email"}},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Capture the pre-test counter IMMEDIATELY before http.DefaultClient.Do
	// per RESEARCH §"Risks & Mitigations" item "SEC-02 races a background
	// refresh /auth/permissions call". Allow a 50ms settle (below) before
	// re-reading.
	//
	// WR-03 (2026-05-18): use PermissionsRequestCount() instead of
	// RequestCount() so health-channel probes (channel=_health) emitted by
	// the auth breaker's background loop do NOT race the invariant. The
	// SEC-02 contract is "the rejected handshake did NOT forward a USER-
	// permission lookup to the backend" — health pings are not relevant
	// to that invariant. The original (total) RequestCount-based
	// assertion would flake the moment a future warm-up tick on a
	// different path bumped the counter; this fix pins the assertion to
	// the channel-specific counter the test actually cares about.
	hitsBefore := h.Auth.PermissionsRequestCount()

	// Inline request build: sse_client.Connect would override X-Request-ID
	// to randomHex(16); we MUST set our 256-byte fixture directly.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		h.Binary.BaseURL()+"/sse/v1/users/42", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Accept", "text/event-stream")
	// 256 × 'A' = 2× the 128-byte cap at handler.go:129.
	req.Header.Set("X-Request-ID", strings.Repeat("A", 256))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v; stderr:\n%s", err, h.Binary.Stderr())
	}
	defer resp.Body.Close() //nolint:errcheck

	// Assertion 1 — status code: 400 Bad Request.
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d; want 400 (stderr:\n%s)", resp.StatusCode, h.Binary.Stderr())
	}

	// Assertion 2 — Content-Type prefix application/json. Use HasPrefix
	// (not equality) so a future stdlib change adding `; charset=utf-8`
	// would not break the test.
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q; want prefix \"application/json\" (stderr:\n%s)", ct, h.Binary.Stderr())
	}

	// Assertion 3 — body substring "invalid_request_id". Substring is
	// more durable than exact match — the exact JSON shape is locked by
	// unit tests at handler_test.go:1322-1323.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Contains(body, []byte("invalid_request_id")) {
		t.Errorf("body = %q; want substring \"invalid_request_id\" (stderr:\n%s)", string(body), h.Binary.Stderr())
	}

	// Settle window per RESEARCH §"Common Pitfalls" Pitfall 3 — gives
	// any in-flight background refresh tick a chance to land BEFORE we
	// capture hitsAfter. Without this the counter could lag the
	// request-response cycle by a few ms and yield a flake-shaped
	// false negative.
	time.Sleep(50 * time.Millisecond)

	// Assertion 4 — auth-backend invariant. THIS is the security-
	// relevant assertion: the threat closed by SEC-02 is log + backend
	// amplification, not client-side rejection alone. A future gate-
	// ordering refactor that moved auth-backend forwarding BEFORE
	// validRequestID would let the other three assertions pass while
	// silently regressing the defence.
	//
	// Channel-specific counter (WR-03): PermissionsRequestCount excludes
	// channel=_health probes so this assertion is robust against the
	// breaker's background health loop. Strict equality (== before) is
	// the strongest possible invariant: the rejected handshake forwards
	// ZERO user-permission lookups to the backend, regardless of any
	// concurrent health pings.
	hitsAfter := h.Auth.PermissionsRequestCount()
	if hitsAfter != hitsBefore {
		t.Errorf("mock auth backend received %d user-permission call(s) for rejected handshake; want 0 (stderr:\n%s)",
			hitsAfter-hitsBefore, h.Binary.Stderr())
	}
}
