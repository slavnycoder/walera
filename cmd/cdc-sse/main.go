// Command cdc-sse connects to PostgreSQL via logical replication, decodes WAL events
// into typed transactions, routes them to SSE subscribers connected via HTTP, and
// streams them in text/event-stream format to each subscribed client. Structured
// logs go to stderr. The process exits cleanly on SIGINT/SIGTERM with a 10s
// graceful shutdown deadline (hard cap; the inner broadcast-drain deadline is 8s).
//
// Usage:
//
//	./cdc-sse --config ./config.yaml [--dev-log] [--healthcheck]
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	// Container-aware GOMAXPROCS — explicit call below so the
	// side-effect-import variant does not spam stderr on every
	// `-healthcheck` invocation (~30/min at compose `interval: 2s`).
	"go.uber.org/automaxprocs/maxprocs"

	"github.com/walera/walera/internal/app"
	"github.com/walera/walera/internal/wal"
	"github.com/walera/walera/internal/walconn"
)

// main wires the cdc-sse process through the internal/app composition root:
//
//  1. parse CLI flags (--config, --dev-log, --healthcheck)
//  2. distroless --healthcheck early-exit BEFORE any logger/config init
//  3. build the zerolog.Logger (dev-console vs JSON)
//  4. maxprocs.Set with the logger as sink
//  5. app.LoadAppConfig + zerolog.ParseLevel for the level tune
//  6. signal.NotifyContext for SIGINT/SIGTERM
//  7. walconn.NewAdminConn → app.PrepareDatabase(ctx, cfg, logger, adminConn)
//     → app.InitializeApp(*cfg, logger, adminConn). InitializeApp is a
//     hand-written composition root (internal/app/initialize.go);
//     PrepareDatabase owns the DB-side bootstrap so InitializeApp
//     stays pure construction (no I/O).
//  8. defer cleanup(); a.Run(ctx, stop); a.Shutdown(ctx, stop, a.TxCh)
func main() {
	configPath := flag.String("config", "./config.yaml", "path to config YAML file")
	devLog := flag.Bool("dev-log", false, "enable human-readable console log output (stderr)")
	healthcheckFlag := flag.Bool("healthcheck", false, "perform a single GET to /healthz on the local server and exit 0 if 200, 1 otherwise (for distroless container HEALTHCHECK)")
	flag.Parse()

	// Distroless production images carry no shell, wget, or curl; this flag
	// gives the compose / k8s HEALTHCHECK a binary-only path. It runs BEFORE
	// any logger/config/DB initialisation because the container HEALTHCHECK
	// may execute in a partially-configured environment.
	if *healthcheckFlag {
		runHealthcheckProbe()
	}

	var logger zerolog.Logger
	if *devLog {
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).
			With().Timestamp().Caller().Logger()
	} else {
		logger = zerolog.New(os.Stderr).With().Timestamp().Caller().Logger()
	}

	// GOMAXPROCS from cgroup quota. Sized AFTER the -healthcheck short-circuit
	// so probe invocations stay silent; routed through zerolog so the line is
	// structured JSON matching the rest of startup output.
	undo, err := maxprocs.Set(maxprocs.Logger(func(format string, args ...interface{}) {
		logger.Info().Msgf("maxprocs: "+format, args...)
	}))
	if err != nil {
		logger.Warn().Err(err).Msg("automaxprocs.Set failed")
	}
	defer undo()

	cfg, err := app.LoadAppConfig(*configPath)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to load configuration")
	}
	if level, lvlErr := zerolog.ParseLevel(cfg.Log.Level); lvlErr != nil {
		logger.Warn().Str("level", cfg.Log.Level).Msg("invalid log level; defaulting to info")
	} else {
		logger = logger.Level(level)
	}
	wal.NaiveTimestampAssumeUTC = cfg.WAL.NaiveTimestampAssumeUTC

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Open the admin connection BEFORE PrepareDatabase / InitializeApp.
	// cfg.WAL.PostgresDSN is walconn.AdminDSN (a named string type) — the
	// constructor's typed signature accepts it directly, so no cast is
	// needed at this boundary.
	adminConn, err := walconn.NewAdminConn(ctx, cfg.WAL.PostgresDSN)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to PostgreSQL (admin connection)")
	}
	// Run the PostgreSQL bootstrap (verify GUC prereqs → create-or-verify
	// publication → warn on low slot headroom) BEFORE constructing the
	// in-memory graph. On error, close adminConn best-effort before the
	// process exits so the partially-initialized connection does not leak.
	if err := app.PrepareDatabase(ctx, cfg, logger, adminConn); err != nil {
		closeAdminConnOnErr(logger, adminConn, "PrepareDatabase failure")
		logger.Fatal().Err(err).Msg("failed to prepare PostgreSQL")
	}

	a, cleanup, err := app.InitializeApp(*cfg, logger, adminConn)
	if err != nil {
		// InitializeApp never owned the conn (main opened it and threaded
		// it in); the responsibility to clean up on InitializeApp failure
		// therefore stays with main on the error path. Once *App exists,
		// Shutdown step 4 owns the close.
		closeAdminConnOnErr(logger, adminConn, "InitializeApp failure")
		logger.Fatal().Err(err).Msg("failed to initialize application")
	}
	defer cleanup()

	if err := a.Run(ctx, stop); err != nil {
		logger.Error().Err(err).Msg("application Run returned error")
	}
	logger.Info().Msg("shutdown signal received; starting graceful shutdown sequence")
	if err := a.Shutdown(ctx, stop, a.TxCh); err != nil {
		logger.Error().Err(err).Msg("application Shutdown returned error")
	}
}

// closeAdminConnOnErr is the best-effort error-cleanup helper used on every
// failure branch between NewAdminConn and the *App handle returned by
// InitializeApp. Once *App exists, Shutdown step 4 owns the close — main
// MUST NOT defer a top-level adminConn.Close after success or it will
// double-close.
//
// Explicit-parameter discipline: the helper takes adminConn as an explicit
// parameter rather than capturing it via closure. The closure form was safe
// today (adminConn is `:=`-initialized once and never reassigned), but a
// future edit that adds a startup-time admin-conn reconnect on retry would
// silently make the closure close the NEW conn instead of the original — a
// value-vs-variable capture footgun. Passing the conn explicitly pins the
// value at the call site and is immune to future reassignment.
//
// The `where` parameter names the failure branch so the log line points at
// the call site without requiring a stack walk; mirrors the existing
// "during startup error cleanup" framing.
//
// Uses context.Background() rather than the caller's ctx because ctx may
// already be cancelled by the time the close runs (e.g. SIGINT during
// startup). Named pointer types (walconn.AdminConn) do not inherit the
// underlying *pgx.Conn method set; cast at the boundary to reach .Close.
func closeAdminConnOnErr(logger zerolog.Logger, c walconn.AdminConn, where string) {
	if cerr := (*pgx.Conn)(c).Close(context.Background()); cerr != nil {
		logger.Warn().Err(cerr).Str("where", where).
			Msg("failed to close admin conn during startup error cleanup")
	}
}
