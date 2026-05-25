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
