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
	AuthClient  *auth.Client
	Breaker     *auth.Breaker
	SubRegistry *auth.Subscribers
	Limits      *limits.Limits
}

type dataPlaneWiring struct {
	Encoder     *sse.Encoder
	Broadcaster *router.Broadcaster
	PoolMetrics *sse.PoolMetricsAdapter
	Pool        *sse.WriterPool
	Handler     *sse.Handler
}

type httpWiring struct {
	HealthServer *health.Server
	Mux          *http.ServeMux
	MainServer   *http.Server
	PProfServer  *http.Server
	SlotName     wal.SlotName
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
		Breaker:      aw.Breaker,
		SubRegistry:  aw.SubRegistry,
		Limits:       aw.Limits,
		Encoder:      dp.Encoder,
		RouterIndex:  dp.Broadcaster,
		SSEPool:      dp.Pool,
		SSEHandler:   dp.Handler,
		HealthServer: hw.HealthServer,
		HTTPServer:   hw.MainServer,
		PProfServer:  hw.PProfServer,
	}
	a.Runnables = buildRunnables(a, hw.SlotName)
	return a, func() {}, nil
}

func wireCore(cfg AppConfig, logger zerolog.Logger) coreWiring {
	registry := metrics.New()

	walReader, txCh := wal.New(cfg.WAL, wal.Deps{
		Logger:  logger,
		Metrics: registry,
	})

	return coreWiring{
		Registry:  registry,
		WalReader: walReader,
		TxCh:      txCh,
	}
}

func wireAuth(cfg AppConfig, logger zerolog.Logger, registry *metrics.Registry) authWiring {
	authClient := auth.New(cfg.Auth, auth.Deps{
		Breaker: nil,
		Logger:  logger,
		Metrics: registry,
	})
	breaker := auth.NewBreaker(cfg.Auth.Breaker, auth.BreakerDeps{
		Prober:  authClient,
		Logger:  logger,
		Metrics: registry,
	})

	authClient.SetBreaker(breaker)

	subscribers := auth.NewSubscribers(auth.SubscribersDeps{
		Logger:  logger,
		Metrics: registry,
	})

	lim := limits.New(cfg.Limits, limits.Deps{
		Logger:  logger,
		Metrics: registry,
	})

	return authWiring{
		AuthClient:  authClient,
		Breaker:     breaker,
		SubRegistry: subscribers,
		Limits:      lim,
	}
}

func wireDataPlane(cfg AppConfig, logger zerolog.Logger, registry *metrics.Registry, aw authWiring) dataPlaneWiring {
	encoder := sse.NewEncoder(cfg.HTTP.MaxPayloadBytes)

	broadcaster := router.New(cfg.Router, router.Deps{
		Logger:  logger,
		Metrics: registry,
		Encoder: encoder,
	})

	poolMetrics := sse.NewPoolMetricsAdapter(registry)
	writerPool := sse.NewPool(sse.PoolConfig{
		PoolFactor:          cfg.HTTP.PoolFactor,
		SubQueueSize:        cfg.HTTP.SubQueueSize,
		MaxWaitMs:           cfg.HTTP.MaxWaitMs,
		BatchingDisabled:    cfg.HTTP.BatchingDisabled,
		DrainThresholdSubs:  cfg.HTTP.DrainThresholdSubs,
		MaxBatchBytesPerSub: cfg.HTTP.MaxBatchBytesPerSub,
		WriteTimeout:        cfg.HTTP.WriteTimeout,
		HeartbeatInterval:   cfg.Router.HeartbeatInterval,
	}, sse.PoolDeps{
		Encoder: encoder,
		Metrics: poolMetrics,
		Logger:  logger,
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
				Client:      aw.AuthClient,
				Subscribers: aw.SubRegistry,
				Breaker:     aw.Breaker,
			},
			Limits:  aw.Limits,
			Pool:    writerPool,
			Logger:  logger,
			Metrics: registry,
		},
	)

	return dataPlaneWiring{
		Encoder:     encoder,
		Broadcaster: broadcaster,
		PoolMetrics: poolMetrics,
		Pool:        writerPool,
		Handler:     handler,
	}
}

func wireHTTP(cfg AppConfig, logger zerolog.Logger, cw coreWiring, aw authWiring, handler *sse.Handler) httpWiring {
	healthSrv := health.New(cfg.Health, health.Deps{
		Logger:      logger,
		PgChecker:   cw.WalReader,
		AuthChecker: aw.AuthClient,
		Metrics:     cw.Registry,
	})
	healthSrv.SetCORSOrigins(cfg.HTTP.CORSOrigins)

	mux := http.NewServeMux()
	healthSrv.Routes(mux)
	handler.Routes(mux)

	mainSrv := newMainHTTPServer(cfg, mux, logger)

	pprofSrv := newPProfHTTPServer(cfg, logger)

	slotName := deriveSlotName(cfg)

	return httpWiring{
		HealthServer: healthSrv,
		Mux:          mux,
		MainServer:   mainSrv,
		PProfServer:  pprofSrv,
		SlotName:     slotName,
	}
}
