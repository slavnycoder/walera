package app

import (
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/health"
	"github.com/walera/walera/internal/limits"
	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/wal"
	"github.com/walera/walera/internal/walconn"
)

func BenchmarkInitializeApp(b *testing.B) {
	cfg := newSingletonBenchConfig(b)
	logger := zerolog.Nop()

	var adminConn walconn.AdminConn

	b.ReportAllocs()
	for b.Loop() {
		a, cleanup, err := InitializeApp(*cfg, logger, adminConn)
		if err != nil {
			b.Fatal(err)
		}

		cleanup()

		_ = a.SSEPool.Shutdown(b.Context())
		_ = a
	}
}

func newSingletonBenchConfig(b *testing.B) *AppConfig {
	b.Helper()
	return &AppConfig{
		Log: LogConfig{Level: "info"},
		HTTP: HTTPConfig{
			Addr:                ":0",
			MaxPayloadBytes:     10 * 1024 * 1024,
			WriteTimeout:        5 * time.Second,
			MaxHeaderBytes:      16 * 1024,
			H2CEnabled:          false,
			PProfAddr:           "",
			PoolFactor:          1,
			SubQueueSize:        32,
			MaxWaitMs:           2,
			DrainThresholdSubs:  0,
			MaxBatchBytesPerSub: 65536,
			BatchingDisabled:    false,
		},
		WAL: wal.Config{
			PostgresDSN:             "postgres://a:b@127.0.0.1:1/db",
			ReplicationDSN:          "postgres://r:b@127.0.0.1:1/db?replication=database",
			PublicationName:         "walera_pub",
			SlotNamePrefix:          "walera",
			SlotHeadroomMin:         1,
			LagSampleInterval:       5 * time.Second,
			NaiveTimestampAssumeUTC: true,
			Bootstrap:               wal.BootstrapConfig{Mode: "off"},
		},
		Router: router.Config{
			HeartbeatInterval: 30 * time.Second,
			ExactBuffer:       64,
			WildcardBuffer:    512,
		},
		Auth: auth.Config{
			BackendURL:        "https://auth.example/test",
			DefaultTTLSeconds: 60,
			RequestTimeout:    5 * time.Second,
			Breaker: auth.BreakerConfig{
				WindowBuckets:        30,
				BucketSeconds:        1,
				FailureRateThreshold: 0.5,
				DebounceFloor:        10,
				Cooldown:             10 * time.Second,
				StaleRefreshJitter:   1 * time.Second,
			},
		},
		Limits: limits.Config{
			GlobalConcurrent: 50000,
		},
		Health:  health.Config{},
		Metrics: metrics.Config{SampleInterval: 5 * time.Second},
		Shutdown: ShutdownConfig{
			Deadline:      ShutdownDeadline(10 * time.Second),
			DrainDeadline: DrainDeadline(8 * time.Second),
		},
	}
}
