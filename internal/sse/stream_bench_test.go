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

type benchRunWriterAuthBackend struct{}

func (benchRunWriterAuthBackend) CheckAuth(_ context.Context) error { return nil }

func buildRunWriterHandler(b *testing.B) (*Handler, *WriterPool) {
	b.Helper()
	logger := zerolog.Nop()
	m := metrics.New()

	authCfg := auth.Config{
		BackendURL:        "http://127.0.0.1:1",
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

			b.ReportAllocs()
			for b.Loop() {

				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				req := httptest.NewRequestWithContext(ctx, "GET", "/sse/v1/users/"+shape.pk, nil)
				req.Header.Set("Authorization", "Bearer bench-token")

				rw := &fakeResponseWriter{}

				h.runWriter(rw, req, "users", shape.pk, channelStr, shape.kind, startLSN, hs)
				counter++
			}

			_ = h
			_ = pool
			_ = counter
			_ = strconv.Itoa(counter)
		})
	}
}
