// Package sse — handler_panic_test.go covers the NewHandler construction
// gate: each required Deps field must panic with the exact format
// "sse.NewHandler: Deps.<Field> is required".
package sse

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/limits"
	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
)

// newHandlerPanicValidDeps returns a fully-populated Deps so each
// per-field test only nils one field. Uses lightweight collaborators —
// nothing crosses a network or spawns a goroutine.
func newHandlerPanicValidDeps(t *testing.T) (Config, Deps) {
	t.Helper()
	logger := zerolog.Nop()
	m := metrics.New()
	authCfg := auth.Config{
		BackendURL:        "http://127.0.0.1:1",
		ServiceToken:      "svc",
		DefaultTTLSeconds: 60,
		RequestTimeout:    time.Second,
		HealthChannel:     "_health",
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
		// sseTestProber adapter declared in handler_test.go — both files
		// are in `package sse` and compile together as a single test binary.
		Prober:  sseTestProber(func(_ context.Context) error { return nil }),
		Logger:  logger,
		Metrics: m,
	})
	authClient := auth.New(authCfg, auth.Deps{
		Logger:  logger,
		Breaker: breaker,
		Metrics: m,
	})
	authReg := auth.NewSubscribers(auth.SubscribersDeps{Logger: logger, Metrics: m})
	lim := limits.New(limits.Config{
		GlobalConcurrent:     1024,
		PerUserConcurrentMax: 10,
		PerUserRatePerSecond: 100,
		PerUserBurst:         100,
		PreAuthRatePerSecond: 100,
		PreAuthBurst:         100,
		SweepInterval:        time.Minute,
		SweepIdleThreshold:   5 * time.Minute,
	}, limits.Deps{Logger: logger, Metrics: m})
	enc := NewEncoder(10 * 1024 * 1024)
	pool := NewPool(PoolConfig{
		PoolFactor:   1,
		SubQueueSize: 4,
	}, PoolDeps{Encoder: enc, Metrics: newFakeMetrics(), Logger: logger})
	t.Cleanup(func() { _ = pool.Shutdown(context.Background()) })

	cfg := Config{
		Addr:              ":0",
		HeartbeatInterval: 15 * time.Second,
		MaxPayloadBytes:   10 * 1024 * 1024,
		WriteTimeout:      5 * time.Second,
		Router: router.Config{
			ExactBuffer:    16,
			WildcardBuffer: 32,
		},
		Auth: authCfg,
	}
	deps := Deps{
		Broadcaster: &fakeBroadcaster{},
		Auth: AuthDeps{
			Client:      authClient,
			Subscribers: authReg,
			Breaker:     breaker,
		},
		Limits:  lim,
		Pool:    pool,
		Logger:  logger,
		Metrics: m,
	}
	return cfg, deps
}

func TestNewHandler_PanicsOnNilDeps(t *testing.T) {
	// NOTE: not t.Parallel() — each sub-test builds its own pool via
	// newHandlerPanicValidDeps with t.Cleanup; running the table in
	// parallel would proliferate goroutine accounting in -race runs.
	cases := []struct {
		name    string
		mutate  func(d *Deps)
		wantMsg string
	}{
		{
			name:    "Broadcaster",
			mutate:  func(d *Deps) { d.Broadcaster = nil },
			wantMsg: "sse.NewHandler: Deps.Broadcaster is required",
		},
		{
			name:    "AuthClient",
			mutate:  func(d *Deps) { d.Auth.Client = nil },
			wantMsg: "sse.NewHandler: Deps.Auth.Client is required",
		},
		{
			name:    "AuthSubscribers",
			mutate:  func(d *Deps) { d.Auth.Subscribers = nil },
			wantMsg: "sse.NewHandler: Deps.Auth.Subscribers is required",
		},
		{
			name:    "Breaker",
			mutate:  func(d *Deps) { d.Auth.Breaker = nil },
			wantMsg: "sse.NewHandler: Deps.Auth.Breaker is required",
		},
		{
			name:    "Limits",
			mutate:  func(d *Deps) { d.Limits = nil },
			wantMsg: "sse.NewHandler: Deps.Limits is required",
		},
		{
			name:    "Pool",
			mutate:  func(d *Deps) { d.Pool = nil },
			wantMsg: "sse.NewHandler: Deps.Pool is required",
		},
		{
			name:    "Metrics",
			mutate:  func(d *Deps) { d.Metrics = nil },
			wantMsg: "sse.NewHandler: Deps.Metrics is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, deps := newHandlerPanicValidDeps(t)
			tc.mutate(&deps)
			assertSSEPanic(t, tc.wantMsg, func() {
				_ = NewHandler(cfg, deps)
			})
		})
	}
}
