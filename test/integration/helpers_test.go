//go:build integration

// Package integration — helpers_test.go gathers small test-only helpers used
// across multiple scenario files. The helpers exist because:
//
//   - readTxEvent (defined in 02_tx_atomicity_test.go) FATALs on any non-tx
//     event. Scenarios that count events without failing on heartbeats /
//     shutdown / error frames need a non-fatal variant.
//   - scrapeMetric is reused by scenarios 07, 10, and 11 to read a single
//     gauge / counter sample out of the /metrics text exposition format.
//
// These helpers live in their own *_test.go file (not a non-test .go file)
// so they remain scoped to `go test -tags=integration` builds and never
// contribute symbols to the production binary.
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

// decodeTxPayload unmarshals a "tx" event's data field into txEventPayload.
// Fatals on JSON error. Use in scenarios that already filtered ev.Type=="tx".
func decodeTxPayload(t *testing.T, data []byte) txEventPayload {
	t.Helper()
	var p txEventPayload
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("decode tx payload: %v (raw=%s)", err, string(data))
	}
	return p
}

// scrapeMetric reads metricsURL (= Binary.BaseURL()+"/metrics") and returns
// the float value of the FIRST line matching `name<optional-labels> VALUE`.
// Labels are not parsed — pass the bare name (e.g. "walera_pg_reconnects_total")
// for un-labelled families. For labelled metrics, pass a substring like
// `walera_subscribers_active{type="exact"}`.
//
// Returns (0, error) if the metric is not present in the scrape body. A
// missing metric is distinct from value=0: callers asserting "metric exists
// AND value > N" should branch on the error.
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
		// Lines are `<metric>[{<labels>}] <value> [<timestamp>]`.
		if !strings.HasPrefix(line, name) {
			continue
		}
		// Ensure the match is at a word boundary: name must be followed by a
		// space (un-labelled) or `{` (labelled). Without this guard,
		// `walera_pg_reconnects_total` would also match `walera_pg_reconnects_total_xxx`
		// if such a metric ever existed.
		rest := line[len(name):]
		if len(rest) == 0 || (rest[0] != ' ' && rest[0] != '{') {
			continue
		}
		// Split off the value. Final token before any timestamp is the value.
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		val, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		// Some lines include a trailing timestamp — if the last token parses,
		// great; otherwise try the second-to-last (Prometheus text format
		// places `<value> <timestamp>`).
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

// waitForMetric polls scrapeMetric every `step` until predicate(value)
// returns true or the deadline elapses. Returns the final scraped value
// and nil on success, or the last value and an error on timeout.
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
