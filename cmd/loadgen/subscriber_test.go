// Package main — subscriber_test.go covers the Subscriber HTTP-loop using
// httptest.Server fixtures (no live Walera dependency).
//
// Quick task 260518-lh1 / T-LH1-02..03 — three contract tests:
//  1. Counts received frames via the Prometheus registry's Gatherer.
//  2. Exercises jittered-backoff reconnect by closing the connection
//     mid-stream and asserting the subscriber re-opens within budget.
//  3. Locks the security posture: the literal auth token MUST NEVER appear
//     in subscriber logs; only auth_token_len is logged.
package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"
)

// gatherCounter extracts a single counter value from the registry by metric
// name. Returns 0 when the metric has no samples yet.
func gatherCounter(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("registry.Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var sum float64
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				sum += c.GetValue()
			}
		}
		return sum
	}
	return 0
}

// writeFrame writes one SSE tx frame to w and flushes it so the client sees
// it immediately. Mirrors internal/sse/encoder.go's wire shape.
func writeFrame(w http.ResponseWriter, id, payload string) {
	_, _ = fmt.Fprintf(w, "event: tx\nid: %s\ndata: %s\n\n", id, payload)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func TestSubscriber_ReceivesAndCounts(t *testing.T) {
	// Serve the three canned frames on the FIRST connection only; reject
	// subsequent reconnect attempts with 401 so the count stays deterministic.
	// (The subscriber reconnects on EOF; without the gate, the same 3 frames
	// would be re-counted on every reconnect inside the test budget.)
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		writeFrame(w, "0/1/1", `{"tx_id":1}`)
		writeFrame(w, "0/1/2", `{"tx_id":2}`)
		writeFrame(w, "0/1/3", `{"tx_id":3}`)
	}))
	defer srv.Close()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	logBuf := &bytes.Buffer{}
	sub := &Subscriber{
		URL:     srv.URL,
		Channel: "orders/42",
		Token:   "test-token",
		Client:  &http.Client{Timeout: 5 * time.Second},
		M:       m,
		Log:     zerolog.New(logBuf).With().Timestamp().Logger(),
		Backoff: backoffCfg{Initial: 5 * time.Millisecond, Max: 100 * time.Millisecond},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	sub.Run(ctx)

	got := gatherCounter(t, reg, "loadgen_events_received_total")
	if got != 3 {
		t.Errorf("loadgen_events_received_total = %v; want 3", got)
	}
}

func TestSubscriber_Reconnects(t *testing.T) {
	var connCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&connCount, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		writeFrame(w, fmt.Sprintf("0/1/%d", n), fmt.Sprintf(`{"tx_id":%d}`, n))
		// Returning from the handler closes the connection; the subscriber
		// observes EOF and reconnects via jittered backoff.
	}))
	defer srv.Close()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	logBuf := &bytes.Buffer{}
	sub := &Subscriber{
		URL:     srv.URL,
		Channel: "orders/42",
		Token:   "test-token",
		Client:  &http.Client{Timeout: 5 * time.Second},
		M:       m,
		Log:     zerolog.New(logBuf).With().Timestamp().Logger(),
		Backoff: backoffCfg{Initial: 1 * time.Millisecond, Max: 10 * time.Millisecond},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	sub.Run(ctx)

	events := gatherCounter(t, reg, "loadgen_events_received_total")
	reconnects := gatherCounter(t, reg, "loadgen_reconnects_total")
	if events < 2 {
		t.Errorf("loadgen_events_received_total = %v; want >= 2", events)
	}
	if reconnects < 1 {
		t.Errorf("loadgen_reconnects_total = %v; want >= 1", reconnects)
	}
}

// TestSubscriber_DoesNotLogToken enforces the no-token-in-logs security
// posture. The literal token string MUST NEVER appear in any log line;
// the subscriber logs only the integer auth_token_len. This pins the
// mitigation in CI so a future log-line refactor cannot silently
// re-introduce a token leak.
func TestSubscriber_DoesNotLogToken(t *testing.T) {
	// Server immediately 401s so the subscriber loops once and exits early.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	logBuf := &bytes.Buffer{}
	const tokenLiteral = "super-secret-XYZ"
	sub := &Subscriber{
		URL:     srv.URL,
		Channel: "orders/42",
		Token:   tokenLiteral,
		Client:  &http.Client{Timeout: 5 * time.Second},
		M:       m,
		Log:     zerolog.New(logBuf).With().Timestamp().Logger(),
		Backoff: backoffCfg{Initial: 1 * time.Millisecond, Max: 5 * time.Millisecond},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	sub.Run(ctx)

	logged := logBuf.String()
	if strings.Contains(logged, tokenLiteral) {
		t.Fatalf("subscriber log contains literal auth token; want presence/length only.\nlog:\n%s", logged)
	}
	if !strings.Contains(logged, "auth_token_len") {
		t.Errorf("subscriber log missing 'auth_token_len' field; want it logged in lieu of the literal token.\nlog:\n%s", logged)
	}
}

// Compile-time sanity that the prometheus client_model dto import is wired
// (it is referenced indirectly via Gather() result types). Without this,
// `goimports` could drop the import on a future edit.
var _ = (*dto.MetricFamily)(nil)
