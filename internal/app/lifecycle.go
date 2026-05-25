// Package app — lifecycle.go owns (*App).Run and (*App).Shutdown.
// See INVARIANTS.md §3 for the 5-step shutdown sequence contract.
package app

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/walera/walera/internal/safego"
	"github.com/walera/walera/internal/wal"
)

// Run starts every long-running goroutine in a.Runnables via the safego
// spawn primitive. Before iterating Runnables, Run invokes
// (*health.Server).StartReadinessProbe(ctx). Run captures `stop` into
// a.cancel BEFORE iterating so the 4 cancel-on-error Runnables
// (wal-reader, router-ingest, http-server, pprof-server) can
// cascade-cancel siblings from their OnError closures. Run blocks on
// <-ctx.Done() and returns nil; callers MUST invoke (*App).Shutdown
// after Run returns.
func (a *App) Run(ctx context.Context, stop context.CancelFunc) error {
	a.cancel = stop

	// Nil-tolerance: production always populates HealthServer via
	// InitializeApp; the lifecycle regression tests construct a minimal
	// *App without a *health.Server.
	if a.HealthServer != nil {
		a.HealthServer.StartReadinessProbe(ctx)
	}

	for _, r := range a.Runnables {
		safego.Go(r.Name, func() {
			err := r.Run(ctx)
			// Suppress OnError on clean shutdown: long-running Runnables
			// return ctx.Err() when the signal-context is cancelled, and
			// that path is not a real failure. Gate the callback on
			// ctx.Err() == nil so OnError invokes only on the "real"
			// error path.
			if err != nil && ctx.Err() == nil && r.OnError != nil {
				r.OnError(err)
			}
		})
	}

	<-ctx.Done()
	return nil
}

// Shutdown runs the 5-step shutdown sequence; see INVARIANTS.md §3.
func (a *App) Shutdown(ctx context.Context, stop context.CancelFunc, txCh <-chan wal.Tx) error {
	shutdownStart := time.Now()
	forceTimer := time.AfterFunc(a.Config.Shutdown.Deadline.Duration(), func() {
		a.Logger.Error().
			Dur("deadline", a.Config.Shutdown.Deadline.Duration()).
			Msg("shutdown: hard cap reached; forced exit")
		os.Exit(1)
	})
	defer forceTimer.Stop()

	// Step 1: concurrent shutdown wave (pool + http + broadcast + optional pprof).
	a.shutdownStep1Wave(ctx, a.Config.Shutdown.DrainDeadline.Duration())

	// Step 3: cancel reader/limits/health/samplers via the
	// signal.NotifyContext cancel function; drain txCh to wait for the
	// reader's deferred close.
	stop()
	for range txCh {
	}

	// Step 4: admin-conn close (sole close site) + 50ms flush sleep.
	// Nil-tolerance for the lifecycle regression tests' minimal *App.
	if a.AdminConn != nil {
		if err := (*pgx.Conn)(a.AdminConn).Close(context.Background()); err != nil {
			a.Logger.Warn().
				Err(err).
				Str("step", "admin_conn_close").
				Msg("admin conn close error")
		}
	}
	time.Sleep(50 * time.Millisecond)

	// Step 5: implicit os.Exit(0).
	a.Logger.Info().
		Int64("elapsed_ms", time.Since(shutdownStart).Milliseconds()).
		Msg("shutdown complete")
	return nil
}

// shutdownStep1Wave runs the production Step-1 concurrent shutdown wave
// with all start-callbacks set to nil.
func (a *App) shutdownStep1Wave(ctx context.Context, drainDeadline time.Duration) {
	a.shutdownStep1WaveWithCallbacks(ctx, drainDeadline, nil, nil, nil, nil)
}

// shutdownStep1WaveWithCallbacks executes the Step-1 concurrent shutdown
// wave (pool + http + broadcast + optional pprof) under a shared
// sync.WaitGroup. Each on*Start callback, when non-nil, is invoked from
// inside the goroutine BEFORE the corresponding Shutdown call returns —
// giving tests a way to capture exact start timestamps for the
// parallelism assertion. Production passes nil for every callback.
func (a *App) shutdownStep1WaveWithCallbacks(
	ctx context.Context,
	drainDeadline time.Duration,
	onPoolStart, onHTTPStart, onBroadcastStart, onPProfStart func(),
) {
	// ctx is intentionally NOT propagated into the per-arm sctx/bctx
	// timeouts. Production reaches this helper AFTER `<-ctx.Done()` has
	// already fired in main; plumbing the cancelled ctx through
	// context.WithTimeout would immediately expire every sub-context and
	// break the 9 s soft budget each arm depends on. The parameter is
	// retained for tests and a future caller that reuses this helper
	// with a live ctx.
	_ = ctx

	var wg sync.WaitGroup
	wg.Add(3)

	// shutdown-pool: 9-second sctx mirrors the http arm; keeps a 1s
	// cushion under the 10s forceTimer hard cap.
	safego.Go("shutdown-pool", func() {
		defer wg.Done()
		if onPoolStart != nil {
			onPoolStart()
		}
		stepStart := time.Now()
		sctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
		defer cancel()
		if err := a.SSEPool.Shutdown(sctx); err != nil {
			a.Logger.Warn().Err(err).
				Str("step", "pool").
				Int64("elapsed_ms", time.Since(stepStart).Milliseconds()).
				Msg("pool.Shutdown did not fully drain in time")
			return
		}
		a.Logger.Info().
			Str("step", "pool").
			Int64("elapsed_ms", time.Since(stepStart).Milliseconds()).
			Msg("sse pool shutdown complete")
	})

	// shutdown-pprof: only joined when the opt-in pprof listener is
	// running (conditional WaitGroup delta keeps zero-overhead when
	// disabled).
	if a.PProfServer != nil {
		wg.Add(1)
		safego.Go("shutdown-pprof", func() {
			defer wg.Done()
			if onPProfStart != nil {
				onPProfStart()
			}
			stepStart := time.Now()
			sctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
			defer cancel()
			if err := a.PProfServer.Shutdown(sctx); err != nil {
				a.Logger.Error().Err(err).
					Str("step", "pprof").
					Int64("elapsed_ms", time.Since(stepStart).Milliseconds()).
					Msg("pprofSrv.Shutdown error")
				return
			}
			a.Logger.Info().
				Str("step", "pprof").
				Int64("elapsed_ms", time.Since(stepStart).Milliseconds()).
				Msg("pprof server shutdown complete")
		})
	}

	safego.Go("shutdown-http", func() {
		defer wg.Done()
		if onHTTPStart != nil {
			onHTTPStart()
		}
		stepStart := time.Now()
		sctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
		defer cancel()
		if err := a.HTTPServer.Shutdown(sctx); err != nil {
			a.Logger.Error().Err(err).
				Str("step", "http").
				Int64("elapsed_ms", time.Since(stepStart).Milliseconds()).
				Msg("srv.Shutdown error")
			return
		}
		a.Logger.Info().
			Str("step", "http").
			Int64("elapsed_ms", time.Since(stepStart).Milliseconds()).
			Msg("http.Server shutdown complete")
	})

	safego.Go("shutdown-broadcast", func() {
		defer wg.Done()
		if onBroadcastStart != nil {
			onBroadcastStart()
		}
		stepStart := time.Now()
		bctx, cancel := context.WithTimeout(context.Background(), drainDeadline)
		defer cancel()
		if err := a.RouterIndex.Shutdown(bctx, drainDeadline); err != nil {
			a.Logger.Warn().Err(err).
				Str("step", "broadcast").
				Int64("elapsed_ms", time.Since(stepStart).Milliseconds()).
				Msg("routerIndex.Shutdown did not fully drain in time")
			return
		}
		a.Logger.Info().
			Str("step", "broadcast").
			Int64("elapsed_ms", time.Since(stepStart).Milliseconds()).
			Msg("router drain complete")
	})

	wg.Wait()
}
