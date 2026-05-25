// Package main — subscriber.go implements one SSE subscriber loop:
// HTTP GET against /sse/v1/<channel> with `Authorization: Bearer <token>`,
// stream-decode SSE frames via bufio.Scanner, hand each accumulated frame
// to ParseFrame, increment Prometheus counters, and reconnect on EOF /
// error with full-jitter exponential backoff (cap 30s).
//
// Quick task 260518-lh1 / T-LH1-02..03. Security posture: NEVER log the
// literal auth token — only token presence/length (auth_token_len). Verified
// by TestSubscriber_DoesNotLogToken in subscriber_test.go.
package main

import (
	"bufio"
	"context"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// metrics is the loadgen Prometheus metric set. Inlined per the
// "<80 lines" project convention rather than a separate file.
type metrics struct {
	subscribersActive     prometheus.Gauge
	eventsReceivedTotal   prometheus.Counter
	eventLagSeconds       prometheus.Histogram
	connectionErrorsTotal prometheus.Counter
	reconnectsTotal       prometheus.Counter
}

// newMetrics constructs and registers the loadgen metric set against reg.
// Buckets for event_lag_seconds use prometheus.DefBuckets — adequate for
// per-frame observed latency from 5ms to 10s.
func newMetrics(reg *prometheus.Registry) *metrics {
	m := &metrics{
		subscribersActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "loadgen_subscribers_active",
			Help: "Currently-open SSE subscribers.",
		}),
		eventsReceivedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "loadgen_events_received_total",
			Help: "Total SSE tx events received across all subscribers.",
		}),
		eventLagSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "loadgen_event_lag_seconds",
			Help:    "Observed latency between commit_ts and frame receive.",
			Buckets: prometheus.DefBuckets,
		}),
		connectionErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "loadgen_connection_errors_total",
			Help: "Total connection errors (dial / non-2xx / scanner).",
		}),
		reconnectsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "loadgen_reconnects_total",
			Help: "Total reconnect attempts after an error or EOF.",
		}),
	}
	reg.MustRegister(
		m.subscribersActive,
		m.eventsReceivedTotal,
		m.eventLagSeconds,
		m.connectionErrorsTotal,
		m.reconnectsTotal,
	)
	return m
}

// backoffCfg tunes the full-jitter exponential reconnect curve.
// Default (newSubscriber): Initial=100ms, Max=30s.
type backoffCfg struct {
	Initial time.Duration
	Max     time.Duration
}

// Subscriber owns one long-lived SSE connection. The struct is exported in
// the package-main sense (capitalised) for the test-package's sake.
type Subscriber struct {
	URL     string         // base URL (no path); the loop appends /sse/v1/<channel>
	Channel string         // entity/PK (e.g. "orders/42") or "table/all"
	Token   string         // Bearer credential; NEVER logged (T-LH1-02)
	Client  *http.Client   // configured by caller (timeout)
	M       *metrics       // shared across all subscribers in a run
	Log     zerolog.Logger // contextual logger
	Backoff backoffCfg     // jittered-backoff curve
}

// Run executes the subscribe loop until ctx is cancelled. Each iteration:
//
//  1. Open GET /sse/v1/<channel> with the bearer header.
//  2. On non-2xx or transport error: increment connection_errors_total +
//     reconnects_total, sleep jittered backoff, retry.
//  3. On 2xx: increment subscribers_active gauge, decode frames via
//     bufio.Scanner (1 MiB max line), accumulate lines until a blank
//     terminator, hand to ParseFrame, increment events_received_total on
//     ok. On EOF/scanner error: decrement gauge, increment reconnects,
//     backoff, retry.
func (s *Subscriber) Run(ctx context.Context) {
	// Log open intent ONCE per subscriber with the token LENGTH only.
	// Subsequent per-attempt logs reference the same token via its
	// length, never its value (no-token-in-logs security invariant).
	s.Log.Info().
		Int("auth_token_len", len(s.Token)).
		Str("channel", s.Channel).
		Msg("subscriber opening")

	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}
		opened := s.runOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if opened {
			// Successful open + clean close — reset the backoff counter.
			attempt = 0
		} else {
			attempt++
		}
		s.M.reconnectsTotal.Inc()
		sleep := jitteredBackoff(attempt, s.Backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

// runOnce performs a single connect+stream attempt. Returns true if the
// HTTP request reached a 2xx and the stream loop actually consumed frames
// (so the caller can decide whether to reset the backoff counter).
func (s *Subscriber) runOnce(ctx context.Context) (opened bool) {
	url := s.URL + "/sse/v1/" + s.Channel
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		s.M.connectionErrorsTotal.Inc()
		s.Log.Warn().Err(err).Msg("subscriber: build request failed")
		return false
	}
	req.Header.Set("Authorization", "Bearer "+s.Token)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := s.Client.Do(req)
	if err != nil {
		s.M.connectionErrorsTotal.Inc()
		s.Log.Warn().Err(err).Msg("subscriber: do request failed")
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		s.M.connectionErrorsTotal.Inc()
		s.Log.Warn().Int("status", resp.StatusCode).Msg("subscriber: non-2xx response")
		return false
	}

	s.M.subscribersActive.Inc()
	defer s.M.subscribersActive.Dec()

	scanner := bufio.NewScanner(resp.Body)
	// SSE frame data can be large (server cap is 10 MiB); allow up to 1 MiB
	// per line which is plenty for any plausible SSE field.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var frame []string
	for scanner.Scan() {
		if ctx.Err() != nil {
			return true
		}
		line := scanner.Text()
		if line == "" {
			// Blank line terminates one SSE frame.
			if ev, _, ok := ParseFrame(frame); ok {
				s.M.eventsReceivedTotal.Inc()
				_ = ev // future: observe per-event-type lag
			}
			frame = frame[:0]
			continue
		}
		frame = append(frame, line)
	}
	if err := scanner.Err(); err != nil {
		s.M.connectionErrorsTotal.Inc()
		s.Log.Warn().Err(err).Msg("subscriber: scanner error")
	}
	return true
}

// jitteredBackoff returns a full-jitter exponential backoff value:
//
//	cap = min(max, initial * 2^attempt)
//	sleep = rand[0, cap)
//
// On a zero or negative attempt the result is in [0, initial). Capped at
// cfg.Max so a long outage cannot push delays past 30s (T-LH1-03 mitigates
// reconnect-storm DoS amplification).
func jitteredBackoff(attempt int, cfg backoffCfg) time.Duration {
	if cfg.Initial <= 0 {
		cfg.Initial = 100 * time.Millisecond
	}
	if cfg.Max <= 0 {
		cfg.Max = 30 * time.Second
	}
	if attempt < 0 {
		attempt = 0
	}
	// Cap the multiplier to avoid int64 overflow on huge attempt counts.
	if attempt > 30 {
		attempt = 30
	}
	d := cfg.Initial << attempt
	if d <= 0 || d > cfg.Max {
		d = cfg.Max
	}
	// Full jitter in [0, d).
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d)))
}
