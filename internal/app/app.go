package app

// See internal/app/doc.go for the package narrative.

import (
	"context"
	"net/http"

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

// App is the owned-singleton handle returned by InitializeApp to
// cmd/cdc-sse/main.go. Each field carries a distinct lifetime and
// shutdown obligation, documented inline below.
type App struct {
	// Config is the aggregate loaded configuration pointer.
	Config *AppConfig

	// Logger is the process-wide structured logger.
	Logger zerolog.Logger

	// Metrics is the shared Prometheus registry every internal/<pkg>
	// publishes into. Pointer identity matters
	// (TestApp_MetricsRegistrySingleton pins this).
	Metrics *metrics.Registry

	// AdminConn is the non-replication PostgreSQL connection held open
	// for the lifetime of the WAL reader. Closed exactly once during
	// Shutdown step 4 (sole close site).
	AdminConn walconn.AdminConn

	// WalReader owns the pgoutput logical-replication connection and
	// decode loop. The wal-reader Runnable in App.Run owns the goroutine.
	WalReader *wal.Reader

	// TxCh is the read-only WAL-transaction channel the wal-reader
	// produces and the router-ingest Runnable consumes.
	TxCh <-chan wal.Tx

	// AuthClient is the authorization client. The auth cycle break is
	// completed inside wireAuth; see INVARIANTS.md §2.
	AuthClient *auth.Client

	// Breaker is the auth-circuit-breaker FSM. The auth-breaker-fsm
	// Runnable in App.Run owns the goroutine.
	Breaker *auth.Breaker

	// SubRegistry is the per-user auth-subscribers channel set. The
	// auth-stale-watcher Runnable in App.Run owns the goroutine.
	SubRegistry *auth.Subscribers

	// Limits is the per-user concurrency-limits keeper. The
	// limits-sweeper Runnable in App.Run owns the goroutine.
	Limits *limits.Limits

	// Encoder is the shared SSE encoder both the router and the
	// WriterPool use so wire bytes cannot drift between paths.
	Encoder *sse.Encoder

	// RouterIndex is the routing index that fans transactions out to
	// subscriber writers. Drained by Shutdown's concurrent first wave.
	RouterIndex *router.Broadcaster

	// SSEPool is the process-wide WriterPool. Drained by Shutdown's
	// concurrent first wave.
	SSEPool *sse.WriterPool

	// SSEHandler owns the /sse/v1/{table}/{pk} route.
	SSEHandler *sse.Handler

	// HealthServer owns /healthz, /readyz, /metrics. Readiness probe
	// goroutine started via StartReadinessProbe(ctx) from App.Run
	// BEFORE the Runnables iteration.
	HealthServer *health.Server

	// HTTPServer is the main HTTP listener for the SSE handler and
	// health routes. Shut down by Shutdown's concurrent first wave.
	HTTPServer *http.Server

	// PProfServer is the optional opt-in pprof debug listener. Nil
	// when cfg.HTTP.PProfAddr is empty.
	PProfServer *http.Server

	// Runnables is the slice of long-running goroutines spawned by
	// (*App).Run via the safego spawn primitive. Read-only after Run
	// begins iterating.
	Runnables []Runnable

	// cancel is the unexported context.CancelFunc App.Run captures from
	// its `stop` parameter BEFORE iterating Runnables so the 4
	// cancel-on-error OnError closures can cascade-cancel siblings.
	cancel context.CancelFunc
}
