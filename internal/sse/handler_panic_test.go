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

func newHandlerPanicValidDeps(t *testing.T) (Config, Deps) {
	t.Helper()
	logger := zerolog.Nop()
	m := metrics.New()
	authCfg := auth.Config{
		BackendURL:        "http://127.0.0.1:1",
		DefaultTTLSeconds: 60,
		RequestTimeout:    time.Second,
		HealthChannel:     "_health",
		Breaker: auth.BreakerConfig{
			WindowBuckets:        30,
			BucketSeconds:        1,
			FailureRateThreshold: 0.5,
			DebounceFloor:        20,
			Cooldown:             30 * time.Second,
		},
	}
	breaker := auth.NewBreaker(authCfg.Breaker, auth.BreakerDeps{

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
	}, limits.Deps{Logger: logger, Metrics: m})
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
		},
		Limits:  lim,
		Logger:  logger,
		Metrics: m,
	}
	return cfg, deps
}

func TestNewHandler_PanicsOnNilDeps(t *testing.T) {

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
			name:    "Limits",
			mutate:  func(d *Deps) { d.Limits = nil },
			wantMsg: "sse.NewHandler: Deps.Limits is required",
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

func assertSSEPanic(t *testing.T, want any, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic with value %v; got no panic", want)
		}
		if r != want {
			t.Fatalf("panic value: got %v (%T); want %v (%T)", r, r, want, want)
		}
	}()
	fn()
}
