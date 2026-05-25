// initialize_bench_test.go — BenchmarkInitializeApp pins the per-call cost
// of the hand-written wiring (covers the wirer decomposition
// of the v2.4 readability sweep; v2.4 benchmark baseline). Any future
// split of InitializeApp into wireCore / wireAuth / wireDataPlane /
// wireHTTP helpers must keep total allocs/op + ns/op within 3% of this
// baseline.
//
// Fixture strategy (locked by the plan):
//
//	newSingletonBenchConfig is an inline parallel of the existing
//	newSingletonTestConfig fixture in app_singleton_test.go but takes
//	*testing.B instead of *testing.T. This keeps the benchmark wave's
//	files-modified contract honest: only the two new bench files are
//	touched; no caller of the existing newSingletonTestConfig is
//	perturbed. The two builders will be merged behind a *testing.TB
//	signature in a follow-up cleanup chore tracked in v2.4 deferred items.
//
// Loop safety:
//   - InitializeApp threads adminConn into *App but no constructor
//     dereferences it at construction time; a typed-nil walconn.AdminConn
//     is safe and matches the pattern used by TestApp_MetricsRegistrySingleton.
//   - The bench MUST NOT invoke the App Run entry point — that would spawn
//     long-lived goroutines and trip goleak.VerifyTestMain at the end of
//     the package's test binary.
//   - cleanup() is invoked every iteration to mirror production teardown.
//   - cfg is built programmatically (struct literal only, no env-driven
//     config loader, no os.Setenv) so the bench is parallel-safe and
//     config-load I/O does not pollute the measurement.
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

// BenchmarkInitializeApp measures one full InitializeApp wiring pass per
// iteration. No sub-benchmarks — the constructor has a single shape
// (cfg, logger, adminConn) and is exercised end-to-end.
func BenchmarkInitializeApp(b *testing.B) {
	cfg := newSingletonBenchConfig(b)
	logger := zerolog.Nop()
	// Typed-nil AdminConn: no constructor in InitializeApp dereferences
	// adminConn at construction time. The bench never invokes the App
	// Run or Shutdown entry points, so the conn is never touched.
	var adminConn walconn.AdminConn

	b.ReportAllocs()
	for b.Loop() {
		a, cleanup, err := InitializeApp(*cfg, logger, adminConn)
		if err != nil {
			b.Fatal(err)
		}
		// cleanup() is currently a no-op placeholder (the production
		// adminConn close lives in App.Shutdown step 4). Call it anyway
		// so the bench mirrors the real call-site contract.
		cleanup()
		// Shut the SSE pool's per-shard worker goroutines down between
		// iterations to keep goleak.VerifyTestMain green at the end of
		// the test binary. NewPool spawns one worker per shard at
		// construction; without this, every iteration leaks one set of
		// workers and the package's goleak gate would fire.
		//
		// Shutdown with a context that already has a generous deadline
		// keeps the bench's wall-clock variance bounded if a future
		// drain pathology slips in.
		_ = a.SSEPool.Shutdown(b.Context())
		_ = a
	}
}

// newSingletonBenchConfig mirrors newSingletonTestConfig in
// app_singleton_test.go but takes *testing.B so the bench can use the
// same parameter shape as a standard testing helper. The body is a
// verbatim copy because widening the existing fixture's signature to
// testing.TB would touch app_singleton_test.go and break the benchmark
// wave's files-modified contract; that widening is the deferred follow-up.
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
			ServiceToken:      "svc-tok-test",
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
