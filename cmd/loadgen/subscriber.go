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

type metrics struct {
	subscribersActive     prometheus.Gauge
	eventsReceivedTotal   prometheus.Counter
	eventLagSeconds       prometheus.Histogram
	connectionErrorsTotal prometheus.Counter
	reconnectsTotal       prometheus.Counter
}

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

type backoffCfg struct {
	Initial time.Duration
	Max     time.Duration
}

type Subscriber struct {
	URL     string
	Channel string
	Token   string
	Client  *http.Client
	M       *metrics
	Log     zerolog.Logger
	Backoff backoffCfg
}

func (s *Subscriber) Run(ctx context.Context) {

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

	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var frame []string
	for scanner.Scan() {
		if ctx.Err() != nil {
			return true
		}
		line := scanner.Text()
		if line == "" {

			if ev, _, ok := ParseFrame(frame); ok {
				s.M.eventsReceivedTotal.Inc()
				_ = ev
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

	if attempt > 30 {
		attempt = 30
	}
	d := cfg.Initial << attempt
	if d <= 0 || d > cfg.Max {
		d = cfg.Max
	}

	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d)))
}
