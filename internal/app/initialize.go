// Package app — initialize.go is the single hand-written singleton wiring
// file. See internal/app/doc.go for the wirer grouping rationale and
// INVARIANTS.md §2 for the auth cycle-break contract.
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

// coreWiring carries the metrics registry + WAL reader/tx-channel produced
// by wireCore.
type coreWiring struct {
	Registry  *metrics.Registry
	WalReader *wal.Reader
	TxCh      <-chan wal.Tx
}

// authWiring carries the auth-subsystem singletons produced by wireAuth.
type authWiring struct {
	AuthClient  *auth.Client
	Breaker     *auth.Breaker
	SubRegistry *auth.Subscribers
	Limits      *limits.Limits
}

// dataPlaneWiring carries the SSE encoder + router broadcaster + writer
// pool (with its Prometheus adapter) + SSE handler produced by
// wireDataPlane.
type dataPlaneWiring struct {
	Encoder     *sse.Encoder
	Broadcaster *router.Broadcaster
	PoolMetrics *sse.PoolMetricsAdapter
	Pool        *sse.WriterPool
	Handler     *sse.Handler
}

// httpWiring carries the health server, the shared mux, the main and
// optional pprof HTTP servers, and the per-process WAL replication slot
// name produced by wireHTTP.
type httpWiring struct {
	HealthServer *health.Server
	Mux          *http.ServeMux
	MainServer   *http.Server
	PProfServer  *http.Server
	SlotName     wal.SlotName
}

// InitializeApp constructs every long-lived singleton in topological order
// and returns the assembled *App handle plus a no-op cleanup closure.
// cfg is passed BY VALUE; adminConn ownership transfers to *App on success
// (Shutdown step 4 is the sole close site).
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

// wireCore constructs the metrics registry and the WAL reader + its tx
// channel — the data-flow roots every subsequent helper depends on.
// Registry pointer identity is preserved across the rest of the graph.
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

// wireAuth constructs the auth client, its circuit breaker, the per-user
// subscribers registry and the limits keeper. Encapsulates the auth
// cycle break (see INVARIANTS.md §2).
func wireAuth(cfg AppConfig, logger zerolog.Logger, registry *metrics.Registry) authWiring {
	authClient := auth.New(cfg.Auth, auth.Deps{
		Breaker: nil, // installed two lines below; nopBreaker substituted until then
		Logger:  logger,
		Metrics: registry,
	})
	breaker := auth.NewBreaker(cfg.Auth.Breaker, auth.BreakerDeps{
		Prober:  authClient, // *Client satisfies Prober via CheckAuth
		Logger:  logger,
		Metrics: registry,
	})
	// auth cycle break; see INVARIANTS.md §2.
	authClient.SetBreaker(breaker) // init-only; guarded by sync.Once inside SetBreaker

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

// wireDataPlane constructs the SSE encoder, router broadcaster, writer
// pool (with its Prometheus adapter) and the SSE HTTP handler.
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

// wireHTTP constructs the health server, the shared HTTP mux (health
// routes mounted BEFORE SSE routes), the main HTTP server and the
// optional pprof listener, and derives the per-process WAL replication
// slot name.
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
