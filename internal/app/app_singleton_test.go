// app_singleton_test.go pins the singleton-identity invariant the
// wiring in initialize.go must preserve: every consumer that
// publishes into the Prometheus registry MUST see the same
// *metrics.Registry pointer; the *sse.PoolMetricsAdapter the WriterPool
// consumes MUST wrap that same *metrics.Registry.
//
// Constructed via InitializeApp (the hand-written wiring in
// internal/app/initialize.go) so the test validates the production
// graph, not a parallel hand-rolled fixture.
package app

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/health"
	"github.com/walera/walera/internal/limits"
	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/sse"
	"github.com/walera/walera/internal/wal"
	"github.com/walera/walera/internal/walconn"
)

func TestApp_MetricsRegistrySingleton(t *testing.T) {
	t.Parallel()

	cfg := newSingletonTestConfig(t)
	logger := zerolog.Nop()

	// Typed-nil AdminConn: InitializeApp stores it in *App without
	// dereferencing; the test never calls a.Shutdown (which would
	// attempt Close — the existing nil-guard inside Shutdown Step 4
	// short-circuits even if a future test does call it) and never
	// calls a.Run (which would attempt PG connectivity).
	//
	// Construction-time invariant: no constructor in initialize.go may
	// dereference adminConn at construction time. The wiring graph
	// threads adminConn into Runnables (which run at a.Run() time,
	// never invoked here) and into Shutdown step 4 (never invoked
	// here). A future constructor that touches adminConn inside its
	// body — e.g., a startup-time PG schema probe added to a new
	// health constructor — will NPE this test with no obvious signal
	// that the test is the broken party.
	// If you are seeing such an NPE: update this fixture to open a
	// real conn via testcontainers (and consider whether the new
	// constructor really belongs at construction time or at Run time).
	var adminConn walconn.AdminConn

	a, cleanup, err := InitializeApp(*cfg, logger, adminConn)
	if err != nil {
		t.Fatalf("InitializeApp: %v", err)
	}
	defer cleanup()

	// NewSSEPool spawns one worker goroutine per shard at construction;
	// the goleak TestMain at end-of-binary would surface those workers
	// as leaks unless shut down. Drain the pool explicitly via its
	// Shutdown method (the same call App.Shutdown step-1 issues). The
	// pool is empty (no Attach calls in this test) so Shutdown returns
	// near-instantly.
	t.Cleanup(func() {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer scancel()
		// Log (not Errorf) so a shutdown error surfaces in the test
		// output without flipping a green run red. A non-nil err here
		// signals either a 5-second sctx expiry (a real shutdown-
		// pathology regression worth investigating) or an internal
		// pool contract violation; either deserves visibility. The
		// goleak TestMain still backstops by failing on leaked
		// workers, so a silent shutdown failure cannot escape.
		if err := a.SSEPool.Shutdown(sctx); err != nil {
			t.Logf("pool shutdown error: %v", err)
		}
	})

	want := a.Metrics
	if want == nil {
		t.Fatal("a.Metrics is nil — InitializeApp did not construct the metrics registry")
	}

	// Pointer-identity assertions for every consumer with a Metrics()
	// accessor. If a future constructor is added that takes a registry
	// but does NOT thread the shared pointer, InitializeApp would
	// construct a second metrics.Registry and one of these checks fires.
	if got := a.AuthClient.Metrics(); got != want {
		t.Errorf("AuthClient.Metrics() = %p; want %p", got, want)
	}
	if got := a.Breaker.Metrics(); got != want {
		t.Errorf("Breaker.Metrics() = %p; want %p", got, want)
	}
	if got := a.SubRegistry.Metrics(); got != want {
		t.Errorf("SubRegistry.Metrics() = %p; want %p", got, want)
	}
	if got := a.WalReader.Metrics(); got != want {
		t.Errorf("WalReader.Metrics() = %p; want %p", got, want)
	}
	if got := a.RouterIndex.Metrics(); got != want {
		t.Errorf("RouterIndex.Metrics() = %p; want %p", got, want)
	}
	if got := a.HealthServer.Metrics(); got != want {
		t.Errorf("HealthServer.Metrics() = %p; want %p", got, want)
	}
	if got := a.Limits.Metrics(); got != want {
		t.Errorf("Limits.Metrics() = %p; want %p", got, want)
	}

	// PoolMetricsAdapter wraps the same *metrics.Registry pointer.
	// SSEPool.Metrics() returns the metricsIface interface value;
	// assert it concretely wraps a *PoolMetricsAdapter and that
	// adapter's Registry matches.
	poolMetrics := a.SSEPool.Metrics()
	adapter, ok := poolMetrics.(*sse.PoolMetricsAdapter)
	if !ok {
		t.Fatalf("SSEPool.Metrics() type = %T; want *sse.PoolMetricsAdapter", poolMetrics)
	}
	if got := adapter.Registry(); got != want {
		t.Errorf("PoolMetricsAdapter wraps registry %p; want %p", got, want)
	}
}

// newSingletonTestConfig builds the minimum *AppConfig the initialize.go
// constructors need. Every field is populated literally so the test
// stays parallel-safe (t.Setenv is incompatible with t.Parallel); the
// fixture mirrors the validated defaults LoadAppConfig produces plus
// the three mandatory WAL knobs.
//
// No constructor dials at NewX time — wal.Reader.New does NOT open a
// connection; the http.Server is not bound; the auth.Client only
// constructs an *http.Client. The test never calls a.Run or
// a.Shutdown so the bound handles stay un-listened-to.
func newSingletonTestConfig(t *testing.T) *AppConfig {
	t.Helper()
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
