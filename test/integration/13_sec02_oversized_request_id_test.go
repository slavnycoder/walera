//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Test13SecHandshakeAdmission groups the security-relevant handshake rejection
// paths: malformed request id (400), per-user concurrency (429), global
// concurrency (503), and resource release after a slow/timed-out auth call.
// Each subtest asserts not just the rejection but that the admission slot is
// released afterwards — a leaked slot would be a permanent denial-of-service.
func Test13SecHandshakeAdmission(t *testing.T) {
	t.Parallel()

	t.Run("OversizedRequestID_400", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		h.Auth.SetMap("test-token", "test-user", []string{"users"},
			map[string][]string{"users": {"id", "email"}})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		hitsBefore := h.Auth.PermissionsRequestCount()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			h.Binary.BaseURL()+"/sse/v1/users/42", nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("X-Request-ID", strings.Repeat("A", 256))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do request: %v; stderr:\n%s", err, h.Binary.Stderr())
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %d; want 400 (stderr:\n%s)", resp.StatusCode, h.Binary.Stderr())
		}
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("Content-Type = %q; want prefix \"application/json\"", ct)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if !bytes.Contains(body, []byte("invalid_request_id")) {
			t.Errorf("body = %q; want substring \"invalid_request_id\"", string(body))
		}

		time.Sleep(50 * time.Millisecond)
		if hitsAfter := h.Auth.PermissionsRequestCount(); hitsAfter != hitsBefore {
			t.Errorf("auth backend received %d permission call(s) for a 400-rejected handshake; want 0",
				hitsAfter-hitsBefore)
		}
	})

	// A single token over its per-user concurrency limit gets 429; closing one
	// stream frees the slot so a new one is admitted (no permanent lockout).
	t.Run("PerUser_429_AndRelease", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t, WithPerUserConcurrent(3))
		h.Auth.SetMap("test-token", "test-user", []string{"users"},
			map[string][]string{"users": {"id"}})
		if err := h.Auth.SetTTL("test-token", 60); err != nil {
			t.Fatalf("SetTTL: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		closes := make([]func(), 0, 3)
		for i := 1; i <= 3; i++ {
			_, ec, cf := h.Client.Connect(ctx, fmt.Sprintf("users/%d", i), "test-token")
			select {
			case err := <-ec:
				t.Fatalf("held stream %d failed: %v; stderr:\n%s", i, err, h.Binary.Stderr())
			case <-time.After(300 * time.Millisecond):
			}
			closes = append(closes, cf)
		}
		defer func() {
			for _, c := range closes {
				c()
			}
		}()

		// 4th concurrent stream for the same user → 429.
		_, ec4, cf4 := h.Client.Connect(ctx, "users/4", "test-token")
		defer cf4()
		select {
		case err := <-ec4:
			var he *HTTPError
			if !errors.As(err, &he) || he.Status != http.StatusTooManyRequests {
				t.Fatalf("4th stream: want 429 HTTPError, got %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("4th stream neither streamed nor 429'd; stderr:\n%s", h.Binary.Stderr())
		}

		if v, err := scrapeMetric(ctx, h.Binary.BaseURL()+"/metrics",
			`walera_limit_rejected_total{kind="per_user_concurrent"}`); err != nil {
			t.Fatalf("scrape per_user metric: %v", err)
		} else if v < 1 {
			t.Errorf("per_user_concurrent rejections = %v; want >= 1", v)
		}

		// Release one slot → a new stream is admitted again.
		closes[0]()
		closes[0] = func() {}
		waitForHandshakeOK(ctx, t, h, "users/5", "test-token", 5*time.Second)
	})

	// Holding the global semaphore at capacity returns 503 + Retry-After;
	// releasing a slot re-admits.
	t.Run("Global_503_Retry", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t, WithGlobalConcurrent(2))
		h.Auth.SetMap("test-token", "test-user", []string{"users"},
			map[string][]string{"users": {"id"}})
		if err := h.Auth.SetTTL("test-token", 60); err != nil {
			t.Fatalf("SetTTL: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		closes := make([]func(), 0, 2)
		for i := 1; i <= 2; i++ {
			_, ec, cf := h.Client.Connect(ctx, fmt.Sprintf("users/%d", i), "test-token")
			select {
			case err := <-ec:
				t.Fatalf("held stream %d failed: %v; stderr:\n%s", i, err, h.Binary.Stderr())
			case <-time.After(300 * time.Millisecond):
			}
			closes = append(closes, cf)
		}
		defer func() {
			for _, c := range closes {
				c()
			}
		}()

		status, retryAfter := rawHandshake(ctx, h.Binary.BaseURL(), "users/3", "test-token")
		if status != http.StatusServiceUnavailable {
			t.Errorf("3rd handshake over global cap: status = %d; want 503", status)
		}
		if retryAfter == "" {
			t.Errorf("503 over global cap missing Retry-After header")
		}

		if v, err := scrapeMetric(ctx, h.Binary.BaseURL()+"/metrics",
			`walera_limit_rejected_total{kind="global_concurrent"}`); err != nil {
			t.Fatalf("scrape global metric: %v", err)
		} else if v < 1 {
			t.Errorf("global_concurrent rejections = %v; want >= 1", v)
		}

		closes[0]()
		closes[0] = func() {}
		waitForHandshakeOK(ctx, t, h, "users/3", "test-token", 5*time.Second)
	})

	// A slow auth backend (slower than request_timeout) must make the handshake
	// time out to 503 — not hang — and release the global slot it took. With a
	// global cap of 1, a leaked slot would wedge every later handshake at 503.
	t.Run("SlowAuthBackend_ReleasesSlot", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t, WithGlobalConcurrent(1))
		h.Auth.SetMap("test-token", "test-user", []string{"users"},
			map[string][]string{"users": {"id"}})
		if err := h.Auth.SetTTL("test-token", 60); err != nil {
			t.Fatalf("SetTTL: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		h.Auth.SetOpenDelay(5 * time.Second) // > request_timeout (2s)
		start := time.Now()
		status, _ := rawHandshake(ctx, h.Binary.BaseURL(), "users/1", "test-token")
		if status != http.StatusServiceUnavailable {
			t.Errorf("slow-auth handshake: status = %d; want 503", status)
		}
		if elapsed := time.Since(start); elapsed > 4*time.Second {
			t.Errorf("slow-auth handshake hung %v; expected ~request_timeout (2s)", elapsed)
		}

		// Clearing the delay, a fresh handshake must succeed — proving the slot
		// held by the timed-out attempt was released, not leaked.
		h.Auth.SetOpenDelay(0)
		waitForHandshakeOK(ctx, t, h, "users/1", "test-token", 8*time.Second)
	})
}

// waitForHandshakeOK polls the SSE handshake until it returns 200 or the
// deadline elapses, tolerating the brief latency between a peer close and the
// server releasing the admission slot.
func waitForHandshakeOK(ctx context.Context, t *testing.T, h *Harness, channel, token string, deadline time.Duration) {
	t.Helper()
	end := time.After(deadline)
	for {
		if status, _ := rawHandshake(ctx, h.Binary.BaseURL(), channel, token); status == http.StatusOK {
			return
		}
		select {
		case <-end:
			t.Fatalf("handshake never returned 200 within %v; stderr:\n%s", deadline, h.Binary.Stderr())
		case <-time.After(150 * time.Millisecond):
		}
	}
}
