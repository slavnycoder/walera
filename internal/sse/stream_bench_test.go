// Package sse — stream_bench_test.go is the  entry point that
// exercises the (*Handler).runWriter hot path the  CI regression
// gate samples on every PR touching internal/sse/...:
//
//   - BenchmarkRunWriter covers  — the runWriter decomposition
//     into headersAndPreamble + mainLoop + finalizeError. Each iteration
//     drives ONE walk through headersAndPreamble + a single mainLoop
//     tick that returns via the request context's already-closed
//     Done() arm. The select inside runWriter takes the
//     r.Context().Done() branch, calls sub.Drop("client_closed"), and
//     then waits for the pool worker's doneCh (bounded by
//     h.cfg.WriteTimeout) before unwinding the deferred chain.
//
//   - Table-driven sub-benchmarks /exact and /wildcard. The shape
//     differs in router.Kind (which selects bufCap: ExactBuffer vs
//     WildcardBuffer) and drives the only branch inside runWriter that
//     depends on the subscription kind. The two shapes capture both
//     router.SubscriberConfig allocation profiles so a regression in
//     either path ( split + helper-extraction PRs) trips the
//     gate.
//
// Fixture choices (per RESEARCH.md §Q2 caveat 3):
//   - fakeResponseWriter from pool_test.go is the http.ResponseWriter
//     stand-in. It satisfies http.Flusher and (via package-level
//     adapter) http.ResponseController surface. The hijack path
//     declines via ErrNotSupported so runWriter takes the
//     respWriter+rc fallback — the bench deliberately AVOIDS the real
//     loopback-TCP hijack path because kernel send-buffering adds
//     OS-scheduler noise that benchstat's Mann-Whitney comparison
//     cannot disentangle from genuine code-level regressions.
//   - HeartbeatInterval is pinned to time.Hour in the pool so the
//     per-worker hbTicker cannot fire mid-iteration.
//   - WriteTimeout is small (50ms) so the fallback select in
//     runWriter's r.Context().Done() arm returns inside a single
//     iteration even when the pool worker is slow to observe Drop.
package sse

import (
	"context"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/limits"
	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
)

// benchRunWriterAuthBackend is a stub auth.Prober for the Breaker; the
// bench never reaches a real /auth/permissions call because we drive
// runWriter directly with a pre-built handshakeResult, bypassing the
// gate sequence in runHandshake.
type benchRunWriterAuthBackend struct{}

func (benchRunWriterAuthBackend) CheckAuth(_ context.Context) error { return nil }

// buildRunWriterHandler constructs a minimal *Handler with the
// collaborators runWriter actually dereferences: pool (real, drives
// Attach), broadcaster (fakeBroadcaster from handler_test.go), auth
// client + breaker + registry (real — auth.NewSubscriber's constructor
// panics on nil), metrics (real Prometheus registry — a fresh one per
// call, no global side effect), limits (real but generous so the
// deferred Release calls inside runHandshakeAndWriter are no-ops on
// this path). All collaborators are constructed once per sub-bench
// (outside b.Loop()) so per-iteration accounting only includes the
// runWriter call body itself.
func buildRunWriterHandler(b *testing.B) (*Handler, *WriterPool) {
	b.Helper()
	logger := zerolog.Nop()
	m := metrics.New()

	authCfg := auth.Config{
		BackendURL:        "http://127.0.0.1:1", // never dialled — gate is bypassed
		DefaultTTLSeconds: 60,
		RequestTimeout:    2 * time.Second,
		Breaker: auth.BreakerConfig{
			WindowBuckets:        30,
			BucketSeconds:        1,
			FailureRateThreshold: 0.5,
			DebounceFloor:        20,
			Cooldown:             30 * time.Second,
			StaleRefreshJitter:   5 * time.Second,
		},
	}
	breaker := auth.NewBreaker(authCfg.Breaker, auth.BreakerDeps{
		Prober:  benchRunWriterAuthBackend{},
		Logger:  logger,
		Metrics: m,
	})
	authClient := auth.New(authCfg, auth.Deps{
		Logger:  logger,
		Breaker: breaker,
		Metrics: m,
	})
	authReg := auth.NewSubscribers(auth.SubscribersDeps{
		Logger:  logger,
		Metrics: m,
	})

	lcfg := limits.Config{
		GlobalConcurrent:     1024,
		PerUserConcurrentMax: 1024,
		PerUserRatePerSecond: 1e6,
		PerUserBurst:         1024,
		PreAuthRatePerSecond: 1e6,
		PreAuthBurst:         1024,
		SweepInterval:        60 * time.Second,
		SweepIdleThreshold:   5 * time.Minute,
	}
	lim := limits.New(lcfg, limits.Deps{Logger: logger, Metrics: m})

	bc := &fakeBroadcaster{}

	cfg := Config{
		Addr:              ":0",
		HeartbeatInterval: time.Hour,
		MaxPayloadBytes:   10 * 1024 * 1024,
		WriteTimeout:      50 * time.Millisecond,
		Router: router.Config{
			ExactBuffer:       16,
			WildcardBuffer:    32,
			MaxChangesPerTx:   10000,
			HeartbeatInterval: time.Hour,
		},
		Auth: authCfg,
	}

	pool := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       8,
		MaxWaitMs:          2,
		WriteTimeout:       cfg.WriteTimeout,
		HeartbeatInterval:  time.Hour,
		DrainThresholdSubs: 1,
	}, PoolDeps{
		Encoder: NewEncoder(cfg.MaxPayloadBytes),
		Metrics: newFakeMetrics(),
		Logger:  logger,
	})

	h := NewHandler(cfg, Deps{
		Broadcaster: bc,
		Auth: AuthDeps{
			Client:      authClient,
			Subscribers: authReg,
			Breaker:     breaker,
		},
		Limits:  lim,
		Pool:    pool,
		Logger:  logger,
		Metrics: m,
	})
	return h, pool
}

// buildRunWriterWhitelist returns a Whitelist that authorises the
// `users` table; matches the validMapBackend shape in handler_test.go
// so the bench drives the same per-subscriber authorisation surface
// that the production handshake would have produced.
func buildRunWriterWhitelist() *auth.Whitelist {
	return &auth.Whitelist{
		UserID: "bench-user",
		Tables: map[string]map[string]struct{}{
			"users": {
				"id":   {},
				"name": {},
			},
		},
		TTLSeconds: 60,
	}
}

// BenchmarkRunWriter covers  — the (*Handler).runWriter
// decomposition. Each iteration constructs a per-iteration cancelled
// request context (so the r.Context().Done() arm fires immediately
// inside runWriter's terminal select), invokes runWriter with the
// pre-built handshakeResult, and lets the deferred chain unwind. The
// terminal sub.Drop("client_closed") + bounded wait on doneCh complete
// inside h.cfg.WriteTimeout (50ms) on the worst path.
//
// Sub-bench shapes:
//   - exact:    router.KindExact subscription, BufferCap = ExactBuffer.
//     Drives the bufCap branch that selects the per-exact buffer size
//     and the exact router.SubscriberConfig literal.
//   - wildcard: router.KindWildcard subscription, BufferCap =
//     WildcardBuffer. Drives the alternate bufCap branch + the wildcard
//     SubscriberConfig literal (with empty PK).
func BenchmarkRunWriter(b *testing.B) {
	shapes := []struct {
		name string
		kind router.Kind
		pk   string
	}{
		{"exact", router.KindExact, "42"},
		{"wildcard", router.KindWildcard, ""},
	}

	for _, shape := range shapes {
		b.Run(shape.name, func(b *testing.B) {
			h, pool := buildRunWriterHandler(b)
			b.Cleanup(func() { _ = pool.Shutdown(context.Background()) })

			// Pre-built handshake result — bypasses runHandshake so the
			// bench measures runWriter's body only. globalAcquired /
			// perUserAcquired are left false so the deferred Release
			// path in runHandshakeAndWriter (which we do NOT call) is
			// a non-issue.
			hs := handshakeResult{
				authMap:   buildRunWriterWhitelist(),
				token:     "bench-token",
				requestID: "bench-req-id",
				clientIP:  "127.0.0.1",
				userID:    "bench-user",
			}

			channelStr := "users"
			if shape.kind == router.KindExact {
				channelStr = "users:" + shape.pk
			} else {
				channelStr = "users:all"
			}

			startLSN := pglogrepl.LSN(0)
			counter := 0

			// b.ReportAllocs() is required when using b.Loop() to
			// enable alloc reporting in the absence of -benchmem.
			b.ReportAllocs()
			for b.Loop() {
				// Cancelled context → runWriter's terminal select
				// takes the r.Context().Done() arm immediately,
				// drops the sub with "client_closed", waits for the
				// pool worker's doneCh (bounded by WriteTimeout),
				// then unwinds the deferred chain.
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				req := httptest.NewRequestWithContext(ctx, "GET", "/sse/v1/users/"+shape.pk, nil)
				req.Header.Set("Authorization", "Bearer bench-token")

				rw := &fakeResponseWriter{}

				// Unique subscriber ID per iteration via the auto-gen
				// crypto/rand path inside router.NewSubscriber —
				// runWriter constructs the router.Subscriber itself,
				// so we only need to vary inputs that flow into the
				// SubscriberConfig literal (Kind + PK) per shape.
				h.runWriter(rw, req, "users", shape.pk, channelStr, shape.kind, startLSN, hs)
				counter++
			}
			// Keep handler + pool live across the timed region so the
			// compiler cannot DCE the per-iteration runWriter call.
			_ = h
			_ = pool
			_ = counter
			_ = strconv.Itoa(counter)
		})
	}
}
