//go:build integration

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

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	metricsURL := h.Binary.BaseURL() + "/metrics"

	events, errCh, closeFn := h.Client.Connect(ctx, "users/1", "test-token")
	defer closeFn()
	if err := h.PG.Exec(ctx,
		"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
		1, "u@x", "U",
	); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	_ = readTxEvent(ctx, t, h, events, errCh)

	if v, err := scrapeMetric(ctx, metricsURL, "walera_auth_circuit_breaker_state"); err != nil {
		t.Fatalf("scrape baseline breaker state: %v", err)
	} else if v != 0 {
		t.Fatalf("baseline breaker state = %v; want 0 (Closed)", v)
	}

	h.Auth.FailMode(true)

	for i := 100; i < 110; i++ {
		channel := fmt.Sprintf("users/%d", i)

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

	if _, err := waitForMetric(ctx, t, metricsURL,
		"walera_auth_circuit_breaker_state",
		func(v float64) bool { return v == 1 },
		35*time.Second, 500*time.Millisecond,
	); err != nil {
		t.Fatalf("breaker did not open within 35s of FailMode(true): %v; stderr:\n%s",
			err, h.Binary.Stderr())
	}

	openCtx, openCancel := context.WithTimeout(ctx, 5*time.Second)
	if status, retryAfter := rawHandshake(openCtx, h.Binary.BaseURL(), "users/200", "test-token"); status != http.StatusServiceUnavailable {
		t.Errorf("open-state new subscribe: status = %d; want 503", status)
	} else if retryAfter == "" {
		t.Errorf("open-state new subscribe: missing Retry-After header")
	}
	openCancel()

	h.Auth.FailMode(false)

	if _, err := waitForMetric(ctx, t, metricsURL,
		"walera_auth_circuit_breaker_state",
		func(v float64) bool { return v == 0 },
		20*time.Second, 500*time.Millisecond,
	); err != nil {
		t.Fatalf("breaker did not close within 20s of FailMode(false): %v; stderr:\n%s",
			err, h.Binary.Stderr())
	}

	successCtx, successCancel := context.WithTimeout(ctx, 5*time.Second)
	defer successCancel()
	successEvents, successErrCh, successClose := h.Client.Connect(successCtx, "users/300", "test-token")
	defer successClose()

	select {
	case err := <-successErrCh:
		t.Fatalf("post-recovery handshake failed: %v", err)
	case <-time.After(500 * time.Millisecond):

	}
	_ = successEvents
}

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
