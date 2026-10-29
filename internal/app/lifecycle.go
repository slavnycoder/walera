package app

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/walera/walera/internal/wal"
)

func (a *App) Run(ctx context.Context, stop context.CancelFunc) error {
	a.cancel = stop

	if a.HealthServer != nil {
		a.HealthServer.StartReadinessProbe(ctx)
	}

	for _, r := range a.Runnables {
		go func() {
			err := r.Run(ctx)

			if err != nil && ctx.Err() == nil && r.OnError != nil {
				r.OnError(err)
			}
		}()
	}

	<-ctx.Done()
	return nil
}

func (a *App) Shutdown(ctx context.Context, stop context.CancelFunc, txCh <-chan wal.Tx) error {
	shutdownStart := time.Now()
	forceTimer := time.AfterFunc(a.Config.Shutdown.Deadline.Duration(), func() {
		a.Logger.Error().
			Dur("deadline", a.Config.Shutdown.Deadline.Duration()).
			Msg("shutdown: hard cap reached; forced exit")
		os.Exit(1)
	})
	defer forceTimer.Stop()

	a.shutdownStep1Wave(ctx)

	stop()
	for range txCh {
	}

	if a.AdminConn != nil {
		if err := (*pgx.Conn)(a.AdminConn).Close(context.Background()); err != nil {
			a.Logger.Warn().
				Err(err).
				Str("step", "admin_conn_close").
				Msg("admin conn close error")
		}
	}
	time.Sleep(50 * time.Millisecond)

	a.Logger.Info().
		Int64("elapsed_ms", time.Since(shutdownStart).Milliseconds()).
		Msg("shutdown complete")
	return nil
}

func (a *App) shutdownStep1Wave(ctx context.Context) {
	a.shutdownStep1WaveWithCallbacks(ctx, nil, nil, nil)
}

func (a *App) shutdownStep1WaveWithCallbacks(
	ctx context.Context,
	onHTTPStart, onBroadcastStart, onPProfStart func(),
) {

	_ = ctx

	var wg sync.WaitGroup
	wg.Add(2)

	if a.PProfServer != nil {
		wg.Add(1)
		go func() { //nolint:gosec // G118: shutdown runs after the parent ctx is cancelled and needs its own deadline
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
		}()
	}

	go func() { //nolint:gosec // G118: shutdown runs after the parent ctx is cancelled and needs its own deadline
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
	}()

	go func() {
		defer wg.Done()
		if onBroadcastStart != nil {
			onBroadcastStart()
		}
		stepStart := time.Now()
		a.RouterIndex.Shutdown()
		a.Logger.Info().
			Str("step", "broadcast").
			Int64("elapsed_ms", time.Since(stepStart).Milliseconds()).
			Msg("router drain complete")
	}()

	wg.Wait()
}
