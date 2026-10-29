package app

import (
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

type coreWiring struct {
	Registry  *metrics.Registry
	WalReader *wal.Reader
	TxCh      <-chan wal.Tx
}

type authWiring struct {
	AuthClient *auth.Client
	Limits     *limits.Limits
}

type dataPlaneWiring struct {
	Encoder     *sse.Encoder
	Broadcaster *router.Broadcaster
	Handler     *sse.Handler
}

type httpWiring struct {
	HealthServer *health.Server
	Mux          *http.ServeMux
	MainServer   *http.Server
}

func InitializeApp(cfg AppConfig, logger zerolog.Logger, adminConn walconn.AdminConn) (*App, func(), error) {
	cw := wireCore(cfg, logger)
	aw := wireAuth(cfg, logger, cw.Registry)
	dp := wireDataPlane(cfg, logger, cw.Registry, aw)
	hw := wireHTTP(cfg, logger, cw, aw, dp.Handler)

	a := &App{
		Config:       &cfg,
		Logger:       logger,
		Metrics:      cw.Registry,
		AdminConn:    adminConn,
		WalReader:    cw.WalReader,
		TxCh:         cw.TxCh,
		AuthClient:   aw.AuthClient,
		Limits:       aw.Limits,
		Encoder:      dp.Encoder,
		RouterIndex:  dp.Broadcaster,
		SSEHandler:   dp.Handler,
		HealthServer: hw.HealthServer,
		HTTPServer:   hw.MainServer,
	}
	a.Runnables = buildRunnables(a)
	return a, func() {}, nil
}

func wireCore(cfg AppConfig, logger zerolog.Logger) coreWiring {
	registry := metrics.New()

	walReader, txCh := wal.New(cfg.WAL, wal.Deps{Logger: logger})

	return coreWiring{
		Registry:  registry,
		WalReader: walReader,
		TxCh:      txCh,
	}
}

func wireAuth(cfg AppConfig, logger zerolog.Logger, registry *metrics.Registry) authWiring {
	authClient := auth.New(cfg.Auth, auth.Deps{
		Logger:  logger,
		Metrics: registry,
	})

	lim := limits.New(cfg.Limits, limits.Deps{Logger: logger})

	return authWiring{
		AuthClient: authClient,
		Limits:     lim,
	}
}

func wireDataPlane(cfg AppConfig, logger zerolog.Logger, registry *metrics.Registry, aw authWiring) dataPlaneWiring {
	encoder := sse.NewEncoder(cfg.HTTP.MaxPayloadBytes)

	broadcaster := router.New(cfg.Router, router.Deps{
		Logger:  logger,
		Metrics: registry,
		Encoder: encoder,
	})

	handler := sse.NewHandler(
		sse.Config{
			Addr:              cfg.HTTP.Addr,
			CORSOrigins:       cfg.HTTP.CORSOrigins,
			HeartbeatInterval: cfg.Router.HeartbeatInterval,
			MaxPayloadBytes:   cfg.HTTP.MaxPayloadBytes,
			WriteTimeout:      cfg.HTTP.WriteTimeout,
			Router:            cfg.Router,
			Auth:              cfg.Auth,
		},
		sse.Deps{
			Broadcaster: broadcaster,
			Auth: sse.AuthDeps{
				Client: aw.AuthClient,
			},
			Limits:  aw.Limits,
			Logger:  logger,
			Metrics: registry,
		},
	)

	return dataPlaneWiring{
		Encoder:     encoder,
		Broadcaster: broadcaster,
		Handler:     handler,
	}
}

func wireHTTP(cfg AppConfig, logger zerolog.Logger, cw coreWiring, aw authWiring, handler *sse.Handler) httpWiring {
	healthSrv := health.New(cfg.Health, health.Deps{
		Logger:    logger,
		PgChecker: cw.WalReader,
		Metrics:   cw.Registry,
	})
	healthSrv.SetCORSOrigins(cfg.HTTP.CORSOrigins)

	mux := http.NewServeMux()
	healthSrv.Routes(mux)
	handler.Routes(mux)

	mainSrv := newMainHTTPServer(cfg, mux, logger)

	return httpWiring{
		HealthServer: healthSrv,
		Mux:          mux,
		MainServer:   mainSrv,
	}
}
