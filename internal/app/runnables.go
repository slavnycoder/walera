// Package app — runnables.go holds the private helpers used by
// initialize.go: the Runnable-slice builder and the two HTTP-server
// factory functions plus the slot-name derivation. Constructor-pure
// (no goroutine spawn); Runnable names are load-bearing (operator alert
// surface). See INVARIANTS.md §1 for the safego.Go call-site contract.
package app

import (
	"context"
	"net"
	"net/http"
	pprof "net/http/pprof"
	"os"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/wal"
)

// buildRunnables assembles the 8-or-9 long-running goroutines as
// []Runnable. App.Run gates each OnError on err != nil && ctx.Err() == nil;
// the closures here do log + a.cancel() cascade. pprof-server is
// conditionally appended when a.PProfServer != nil.
func buildRunnables(a *App, slotName wal.SlotName) []Runnable {
	runs := []Runnable{
		{
			Name: "wal-reader",
			Run:  func(ctx context.Context) error { return a.WalReader.Run(ctx) },
			OnError: func(err error) {
				a.Logger.Error().Err(err).Msg("WAL reader exited with error; shutting down")
				a.cancel()
			},
		},
		{
			Name: "auth-breaker-fsm",
			Run:  func(ctx context.Context) error { a.Breaker.Run(ctx); return nil },
		},
		{
			Name: "limits-sweeper",
			Run:  func(ctx context.Context) error { a.Limits.RunSweeper(ctx); return nil },
		},
		{
			Name: "auth-stale-watcher",
			Run: func(ctx context.Context) error {
				a.SubRegistry.WatchBreaker(
					ctx,
					a.Breaker,
					a.Config.Auth.Breaker.StaleRefreshJitter,
					a.Config.Auth.DefaultTTLSeconds,
				)
				return nil
			},
		},
		{
			Name: "router-ingest",
			Run:  func(ctx context.Context) error { return a.RouterIndex.Ingest(ctx, a.TxCh) },
			OnError: func(err error) {
				a.Logger.Error().Err(err).Msg("router ingest exited with error")
				a.cancel()
			},
		},
		{
			Name: "http-server",
			Run: func(ctx context.Context) error {
				a.Logger.Info().Str("addr", a.Config.HTTP.Addr).Msg("HTTP server listening")
				if err := a.HTTPServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					return err
				}
				return nil
			},
			OnError: func(err error) {
				a.Logger.Error().Err(err).Msg("HTTP server exited with error")
				a.cancel()
			},
		},
	}

	if a.PProfServer != nil {
		runs = append(runs, Runnable{
			Name: "pprof-server",
			Run: func(ctx context.Context) error {
				a.Logger.Info().Str("addr", a.Config.HTTP.PProfAddr).Msg("pprof server listening")
				if err := a.PProfServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					return err
				}
				return nil
			},
			OnError: func(err error) {
				a.Logger.Error().Err(err).Msg("pprof server exited with error")
				a.cancel()
			},
		})
	}

	runs = append(runs,
		Runnable{
			Name: "wal-lag-sampler",
			Run: func(ctx context.Context) error {
				wal.SampleLagLoop(ctx, a.AdminConn, slotName, a.Metrics,
					a.Config.WAL.LagSampleInterval, a.Logger)
				return nil
			},
		},
		// metrics-sampler ticks the two routing-index GaugeVec series.
		Runnable{
			Name: "metrics-sampler",
			Run: func(ctx context.Context) error {
				ticker := time.NewTicker(a.Config.Metrics.SampleInterval)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return nil
					case <-ticker.C:
						a.Metrics.RoutingIndexSize("exact").Set(float64(a.RouterIndex.ExactLen()))
						a.Metrics.RoutingIndexSize("wildcard").Set(float64(a.RouterIndex.WildcardLen()))
					}
				}
			},
		},
	)
	return runs
}

// newMainHTTPServer builds the main HTTP listener (SSE + health) with
// per-conn TCP keepalive and optional h2c (cfg.HTTP.H2CEnabled). Process-
// wide singleton; the http-server Runnable owns its goroutine.
func newMainHTTPServer(cfg AppConfig, mux *http.ServeMux, logger zerolog.Logger) *http.Server {
	srv := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
		// MaxHeaderBytes: stdlib rejects oversized headers with 431.
		MaxHeaderBytes: cfg.HTTP.MaxHeaderBytes,
		ConnState: func(c net.Conn, state http.ConnState) {
			if state != http.StateNew {
				return
			}
			tcpConn, ok := c.(*net.TCPConn)
			if !ok {
				return
			}
			_ = tcpConn.SetKeepAlive(true)
			_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
		},
	}
	if cfg.HTTP.H2CEnabled {
		p := new(http.Protocols)
		p.SetHTTP1(true)
		p.SetUnencryptedHTTP2(true)
		srv.Protocols = p
		logger.Info().Msg("h2c enabled on SSE listener (http.h2c_enabled=true)")
	}
	return srv
}

// newPProfHTTPServer builds the opt-in pprof listener on a dedicated mux
// (never the stdlib default). Returns nil when cfg.HTTP.PProfAddr is empty.
// The `pprof` named import keeps handler refs explicit (no blank side-effect).
func newPProfHTTPServer(cfg AppConfig, logger zerolog.Logger) *http.Server {
	if cfg.HTTP.PProfAddr == "" {
		logger.Info().Msg("pprof disabled (http.pprof_addr empty)")
		return nil
	}
	pprofMux := http.NewServeMux()
	pprofMux.HandleFunc("/debug/pprof/", pprof.Index)
	pprofMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	pprofMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	pprofMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	pprofMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return &http.Server{
		Addr:              cfg.HTTP.PProfAddr,
		Handler:           pprofMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// deriveSlotName derives the WAL slot name from hostname + pid via
// wal.Config.NewSlotName. The same formula is independently re-derived
// inside wal.bootstrapSlot; any change MUST be applied at both sites.
// os.Hostname error → "unknown" fallback (v2.2 behaviour).
func deriveSlotName(cfg AppConfig) wal.SlotName {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return cfg.WAL.NewSlotName(hostname, os.Getpid())
}
