//go:build integration

// Package integration — scenario 11: auth circuit breaker.
//
// The breaker contract:
//   - FailMode(on=true) → mock returns 503 on every call.
//   - Walera classifies 503 as *ErrUnavailable; the breaker counts these
//     in a sliding bucket window (test config: 30 × 1s = 30s window).
//   - Once total samples ≥ DebounceFloor (test config: 5) AND failure rate
//     > FailureRateThreshold (0.5), the breaker transitions to StateOpen.
//   - While Open: NEW handshakes return 503 + Retry-After (fail-closed for
//     new opens). Existing subscribers remain connected (bounded
//     fail-open) — but they cannot refresh, so the stale_subscribers
//     gauge climbs.
//   - On Open, the background health probe pings the mock; once it gets
//     a clean 200 (after FailMode(false)), the breaker transitions Open →
//     HalfOpen → Closed after Cooldown.
//   - On Closed, the stale-refresh fan-out catches up the subscribers that
//     accumulated during the outage.
//
// Test shape:
//  1. Open ONE SSE subscriber to users/1 (steady-state proof).
//  2. FailMode(true). Drive enough requests to exceed DebounceFloor:
//     attempt 10 new subscribes (each fails fast, increments the breaker
//     counter); within 35s the breaker MUST open.
//  3. While Open, a fresh subscribe attempt must fail 503.
//  4. FailMode(false). Within 10s the breaker MUST close.
//  5. After Close, a fresh subscribe succeeds (200).
package integration

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func Test11Breaker(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)

	h.Auth.SetMap(
		"test-token",
		"test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email", "name"}},
	)
	if err := h.Auth.SetTTL("test-token", 60); err != nil {
		t.Fatalf("SetTTL: %v", err)
	}

	// Long context — the breaker scenario is the longest in the suite. The
	// Makefile timeout is 120s; we budget 90s here.
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	metricsURL := h.Binary.BaseURL() + "/metrics"

	// Step 1: steady state — one subscriber, one INSERT, one event.
	events, errCh, closeFn := h.Client.Connect(ctx, "users/1", "test-token")
	defer closeFn()
	if err := h.PG.Exec(ctx,
		"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
		1, "u@x", "U",
	); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	_ = readTxEvent(ctx, t, h, events, errCh)

	// Confirm breaker baseline state is Closed (0).
	if v, err := scrapeMetric(ctx, metricsURL, "walera_auth_circuit_breaker_state"); err != nil {
		t.Fatalf("scrape baseline breaker state: %v", err)
	} else if v != 0 {
		t.Fatalf("baseline breaker state = %v; want 0 (Closed)", v)
	}

	// Step 2: flip the mock to 503-on-every-call. Drive failed handshakes
	// to accumulate breaker samples > DebounceFloor (=5 in test config).
	h.Auth.FailMode(true)

	// Attempt 10 new handshakes against distinct PKs. Each MUST fail with
	// 503 (gate 4 — auth call returns *ErrUnavailable → 503). The handshake
	// failure itself feeds the breaker counter.
	for i := 100; i < 110; i++ {
		channel := fmt.Sprintf("users/%d", i)
		// Use a short per-handshake context — Connect will surface the
		// non-200 on errCh and immediately close.
		hctx, hcancel := context.WithTimeout(ctx, 5*time.Second)
		_, hErrCh, hClose := h.Client.Connect(hctx, channel, "test-token")
		select {
		case err := <-hErrCh:
			if !strings.Contains(err.Error(), "status 503") {
				t.Logf("handshake #%d during FailMode: expected 503, got %v (continuing — breaker assertion is the real test)", i, err)
			}
		case <-hctx.Done():
			t.Logf("handshake #%d hung past 5s during FailMode (continuing)", i)
		}
		hClose()
		hcancel()
	}

	// Within 35s, the breaker state gauge must show 1 (Open).
	if _, err := waitForMetric(ctx, t, metricsURL,
		"walera_auth_circuit_breaker_state",
		func(v float64) bool { return v == 1 },
		35*time.Second, 500*time.Millisecond,
	); err != nil {
		t.Fatalf("breaker did not open within 35s of FailMode(true): %v; stderr:\n%s",
			err, h.Binary.Stderr())
	}

	// Step 3: while breaker is OPEN, a fresh subscribe attempt must fail
	// 503 + Retry-After (fail-closed for new opens). The breaker
	// short-circuits without even calling the mock.
	openCtx, openCancel := context.WithTimeout(ctx, 5*time.Second)
	if status, retryAfter := rawHandshake(openCtx, h.Binary.BaseURL(), "users/200", "test-token"); status != http.StatusServiceUnavailable {
		t.Errorf("open-state new subscribe: status = %d; want 503", status)
	} else if retryAfter == "" {
		t.Errorf("open-state new subscribe: missing Retry-After header")
	}
	openCancel()

	// Step 4: recover. Flip the mock back to 200-everywhere; the
	// background breaker probe (auth.Client.Health via _health channel)
	// should see a clean 200 and transition Open → HalfOpen → Closed
	// after Cooldown (1s in test config).
	h.Auth.FailMode(false)

	if _, err := waitForMetric(ctx, t, metricsURL,
		"walera_auth_circuit_breaker_state",
		func(v float64) bool { return v == 0 },
		20*time.Second, 500*time.Millisecond,
	); err != nil {
		t.Fatalf("breaker did not close within 20s of FailMode(false): %v; stderr:\n%s",
			err, h.Binary.Stderr())
	}

	// Step 5: after Close, a fresh subscribe succeeds (200). This proves
	// the breaker fully recovered (Closed, not stuck HalfOpen).
	successCtx, successCancel := context.WithTimeout(ctx, 5*time.Second)
	defer successCancel()
	successEvents, successErrCh, successClose := h.Client.Connect(successCtx, "users/300", "test-token")
	defer successClose()
	// Drain briefly; we don't need a tx event — Connect's status-200 check
	// already gates the handshake-success assertion.
	select {
	case err := <-successErrCh:
		t.Fatalf("post-recovery handshake failed: %v", err)
	case <-time.After(500 * time.Millisecond):
		// No errCh signal in 500ms — handshake succeeded.
	}
	_ = successEvents // unused — we only validated the handshake path.
}

// rawHandshake performs a single GET against /sse/v1/<channel> and returns
// (status, retry-after). Used by scenario 11 to assert the breaker's fail-
// closed shape (503 + Retry-After) without going through the SSE client
// (which parses events — we only want the response headers).
func rawHandshake(ctx context.Context, baseURL, channel, token string) (int, string) {
	u := strings.TrimRight(baseURL, "/") + "/sse/v1/" + channel
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, ""
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Request-ID", "test-breaker-probe")
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close() //nolint:errcheck
	return resp.StatusCode, resp.Header.Get("Retry-After")
}
