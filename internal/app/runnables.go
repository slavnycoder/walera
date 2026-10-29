package app

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/rs/zerolog"
)

func buildRunnables(a *App) []Runnable {
	runs := []Runnable{
		{
			Name: "wal-reader",
			Run:  func(ctx context.Context) error { return a.WalReader.Run(ctx) },
			OnError: func(err error) {
				a.Logger.Error().Err(err).Msg("WAL reader exited with error; shutting down")
				a.cancel()
			},
		},
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
