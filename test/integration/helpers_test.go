//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// assertAbsentInLogs fails the test if any of the given canary strings (secrets,
// PII, bearer tokens) appears in the binary's captured stderr. Enforces the
// "never log row data, tokens, or secrets" constraint.
func assertAbsentInLogs(t *testing.T, h *Harness, needles ...string) {
	t.Helper()
	logs := h.Binary.Stderr()
	for _, s := range needles {
		if s == "" {
			continue
		}
		if strings.Contains(logs, s) {
			t.Errorf("secret/PII %q leaked into process logs", s)
		}
	}
}

// readTxWithin waits up to d for a tx event without failing the test when none
// arrives (ok=false). Non-tx frames (heartbeats, initial_data) are skipped. A
// client error is still fatal.
func readTxWithin(t *testing.T, events <-chan SSEEvent, errCh <-chan error, d time.Duration) (txEventPayload, bool) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return txEventPayload{}, false
			}
			if ev.Type != "tx" {
				continue
			}
			return decodeTxPayload(t, ev.Data), true
		case err := <-errCh:
			t.Fatalf("client error while awaiting tx: %v", err)
		case <-deadline:
			return txEventPayload{}, false
		}
	}
}

// expectNoTxWithin fails if any tx event arrives within d.
func expectNoTxWithin(t *testing.T, events <-chan SSEEvent, errCh <-chan error, d time.Duration) {
	t.Helper()
	if p, ok := readTxWithin(t, events, errCh, d); ok {
		t.Fatalf("expected no tx event, got changes: %+v", p.Changes)
	}
}

// waitForRefresh blocks until the mock auth has served at least n more
// user-permission refreshes than base, so a mid-stream SetMap is guaranteed to
// have been picked up before the test proceeds.
func waitForRefresh(t *testing.T, h *Harness, base, n int64, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if h.Auth.PermissionsRequestCount()-base >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("auth refresh did not advance by %d within %v (delta=%d)",
		n, deadline, h.Auth.PermissionsRequestCount()-base)
}

func decodeTxPayload(t *testing.T, data []byte) txEventPayload {
	t.Helper()
	var p txEventPayload
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("decode tx payload: %v (raw=%s)", err, string(data))
	}
	return p
}

func scrapeMetric(ctx context.Context, metricsURL, name string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return 0, fmt.Errorf("scrape: build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("scrape: do: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("scrape: status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, fmt.Errorf("scrape: read: %w", err)
	}
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || line[0] == '#' {
			continue
		}

		if !strings.HasPrefix(line, name) {
			continue
		}

		rest := line[len(name):]
		if len(rest) == 0 || (rest[0] != ' ' && rest[0] != '{') {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		val, err := strconv.ParseFloat(fields[len(fields)-1], 64)

		if err != nil && len(fields) >= 3 {
			val, err = strconv.ParseFloat(fields[len(fields)-2], 64)
		}
		if err != nil {
			return 0, fmt.Errorf("scrape: parse value for %q: %w (line=%q)", name, err, line)
		}
		return val, nil
	}
	return 0, fmt.Errorf("scrape: metric %q not found", name)
}

func waitForMetric(ctx context.Context, t *testing.T, metricsURL, name string, predicate func(float64) bool, deadline time.Duration, step time.Duration) (float64, error) {
	t.Helper()
	end := time.Now().Add(deadline)
	var last float64
	for time.Now().Before(end) {
		v, err := scrapeMetric(ctx, metricsURL, name)
		if err == nil {
			last = v
			if predicate(v) {
				return v, nil
			}
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(step):
		}
	}
	return last, fmt.Errorf("waitForMetric: %q never satisfied predicate within %v (last=%v)", name, deadline, last)
}
