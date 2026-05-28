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
	}

	if a.Config.Auth.DefaultTTLSeconds > 0 {
		runs = append(runs, Runnable{
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
		})
	} else {
		a.Logger.Info().
			Str("docs", "https://github.com/walera/walera/blob/master/docs/auth.md").
			Msg("auth periodic-refresh disabled (auth.default_ttl_seconds is 0 or unset)")
	}

	runs = append(runs,
		Runnable{
			Name: "router-ingest",
			Run:  func(ctx context.Context) error { return a.RouterIndex.Ingest(ctx, a.TxCh) },
			OnError: func(err error) {
				a.Logger.Error().Err(err).Msg("router ingest exited with error")
				a.cancel()
			},
		},
		Runnable{
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
	)

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

func newMainHTTPServer(cfg AppConfig, mux *http.ServeMux, logger zerolog.Logger) *http.Server {
	srv := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,

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

func deriveSlotName(cfg AppConfig) wal.SlotName {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return cfg.WAL.NewSlotName(hostname, os.Getpid())
}
