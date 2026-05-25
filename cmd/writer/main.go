package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	mathrand "math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"go.uber.org/automaxprocs/maxprocs"
	"golang.org/x/time/rate"

	"github.com/walera/walera/internal/writer"
)

type writerFlags struct {
	configPath   *string
	scenarioName *string
	commitRate   *float64
	rowsPerTx    *int
	pgDSN        *string
	poolMaxConns *int
	httpAddr     *string
	targetTables *string
	logLevel     *string
	arrivalDist  *string
	rampDuration *time.Duration
	healthcheck  *bool
}

func registerFlags() *writerFlags {
	f := &writerFlags{
		configPath:   flag.String("config", "", "path to writer YAML config (optional)"),
		scenarioName: flag.String("scenario", "", "scenario name; pass `list` to print all and exit 0"),
		commitRate:   flag.Float64("commit-rate", 0, "target tx/sec (overrides scenario default)"),
		rowsPerTx:    flag.Int("rows-per-tx", 0, "rows inserted per transaction (overrides scenario default)"),
		pgDSN:        flag.String("pg-dsn", "", "PostgreSQL admin DSN (falls back to WRITER_PG_DSN, then WALERA_DATABASE_URL)"),
		poolMaxConns: flag.Int("pool-max-conns", 0, "max pgx pool connections (default 8)"),
		httpAddr:     flag.String("http-addr", "", "HTTP listener for /control + /metrics + /healthz"),
		targetTables: flag.String("target-tables", "", "comma-separated list of target tables"),
		logLevel:     flag.String("log-level", "", "log level: debug|info|warn|error"),
		arrivalDist:  flag.String("arrival-distribution", "", "inter-arrival distribution: poisson|uniform"),
		rampDuration: flag.Duration("ramp-duration", 0, "ramp-up scenario ramp duration"),
		healthcheck:  flag.Bool("healthcheck", false, "probe http://127.0.0.1:<port>/healthz then exit 0 (200) or 1 (otherwise)"),
	}
	flag.Parse()
	return f
}

func main() {
	f := registerFlags()

	if *f.healthcheck {
		runHealthcheck(*f.configPath, *f.httpAddr)
		return
	}

	if *f.scenarioName == "list" {
		printScenarioList()
		return
	}

	logger := newLogger()
	setMaxprocs(logger)

	cfg := loadConfig(*f.configPath, logger)
	logger = applyLogLevel(logger, cfg.Log.Level)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := writer.NewPool(ctx, cfg.PG, cfg.Pool)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to construct pgx pool")
	}
	defer pool.Close()

	lim := rate.NewLimiter(rate.Limit(cfg.Scenario.CommitRate), 1)
	scenarioPtr := newScenarioPtr(cfg)
	reg := newRegistryWithScenario(cfg)

	go samplePoolStats(ctx, pool, reg)
	go runScenarioTicker(ctx, scenarioPtr, lim, reg)

	commitDone := launchCommitLoop(ctx, pool, lim, scenarioPtr, cfg, logger, reg)
	srv := startHTTPServer(cfg, lim, scenarioPtr, reg, logger, stop)
	logStartup(logger, cfg)

	<-ctx.Done()
	gracefulShutdown(srv, commitDone, logger)
}

func runHealthcheck(configPath, httpAddrFlag string) {
	addr := ""
	if cfg, err := writer.Load(configPath, flag.CommandLine); err == nil {
		addr = cfg.HTTP.Addr
	} else if httpAddrFlag != "" {
		addr = httpAddrFlag
	} else if envAddr := os.Getenv("WRITER_HTTP_ADDR"); envAddr != "" {
		addr = envAddr
	}
	port := "9100"
	if addr != "" {
		if i := strings.LastIndex(addr, ":"); i >= 0 && i+1 < len(addr) {
			port = addr[i+1:]
		}
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		os.Exit(1)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	os.Exit(0)
}

func printScenarioList() {
	reg := writer.Registry()
	order := []string{"smoke", "ramp-up", "steady", "spike", "soak", "stress"}
	for _, n := range order {
		if _, ok := reg[n]; ok {
			fmt.Println(n)
		}
	}
	os.Exit(0)
}

func newLogger() zerolog.Logger {
	return zerolog.New(os.Stderr).With().Timestamp().Caller().Logger()
}

func setMaxprocs(logger zerolog.Logger) {
	_, err := maxprocs.Set(maxprocs.Logger(func(format string, args ...interface{}) {
		logger.Info().Msgf("maxprocs: "+format, args...)
	}))
	if err != nil {
		logger.Warn().Err(err).Msg("automaxprocs.Set failed")
	}
}

func loadConfig(configPath string, logger zerolog.Logger) *writer.WriterConfig {
	cfg, err := writer.Load(configPath, flag.CommandLine)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to load writer configuration")
	}
	return cfg
}

func applyLogLevel(logger zerolog.Logger, level string) zerolog.Logger {
	if lvl, err := zerolog.ParseLevel(level); err == nil {
		return logger.Level(lvl)
	}
	logger.Warn().Str("level", level).Msg("invalid log.level; defaulting to info")
	return logger.Level(zerolog.InfoLevel)
}

func newScenarioPtr(cfg *writer.WriterConfig) *atomic.Pointer[writer.ScenarioStateExport] {
	scenario := buildScenarioFromConfig(cfg)
	state := writer.NewScenarioState(scenario, time.Now(), cfg.Scenario.CommitRate, cfg.Scenario.RowsPerTx, cfg.PG.TargetTables)
	var p atomic.Pointer[writer.ScenarioStateExport]
	p.Store(state)
	return &p
}

func newRegistryWithScenario(cfg *writer.WriterConfig) *writer.WriterRegistry {
	reg := writer.NewRegistry()
	reg.SetActiveScenario(cfg.Scenario.Name)
	reg.SetCommitRate(cfg.Scenario.Name, cfg.Scenario.CommitRate)
	return reg
}

func samplePoolStats(ctx context.Context, pool *pgxpool.Pool, reg *writer.WriterRegistry) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stat := pool.Stat()
			reg.SetPoolStats(int(stat.AcquiredConns()), int(stat.IdleConns()))
		}
	}
}

func runScenarioTicker(
	ctx context.Context,
	scenarioPtr *atomic.Pointer[writer.ScenarioStateExport],
	lim *rate.Limiter,
	reg *writer.WriterRegistry,
) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			st := scenarioPtr.Load()
			if st == nil {
				continue
			}
			newRate, _ := st.Scenario.Tick(time.Since(st.StartedAt))
			if newRate > 0 && rate.Limit(newRate) != lim.Limit() {
				lim.SetLimit(rate.Limit(newRate))
				reg.SetCommitRate(st.Scenario.Name(), newRate)
			}
		}
	}
}

func launchCommitLoop(
	ctx context.Context,
	pool *pgxpool.Pool,
	lim *rate.Limiter,
	scenarioPtr *atomic.Pointer[writer.ScenarioStateExport],
	cfg *writer.WriterConfig,
	logger zerolog.Logger,
	reg *writer.WriterRegistry,
) chan error {
	onCommit := func(scenario, target string, rows int) {
		reg.TxTotal(scenario, target)
		reg.RowsTotal(scenario, target, "insert", rows)
	}
	onError := func(reason string) { reg.Errors(reason) }

	rng := mathrand.New(mathrand.NewPCG(uint64(time.Now().UnixNano()), uint64(os.Getpid())))
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error().
					Interface("panic", r).
					Bytes("stack", debug.Stack()).
					Msg("commit loop panic; exiting writer")
				os.Exit(1)
			}
		}()
		done <- writer.RunCommitLoop(
			ctx, pool, lim,
			scenarioPtr,
			writer.ArrivalDistribution(cfg.Arrivals.Distribution),
			rng, cfg.PG, logger, onCommit, onError)
	}()
	return done
}

func startHTTPServer(
	cfg *writer.WriterConfig,
	lim *rate.Limiter,
	scenarioPtr *atomic.Pointer[writer.ScenarioStateExport],
	reg *writer.WriterRegistry,
	logger zerolog.Logger,
	stop context.CancelFunc,
) *http.Server {
	srv := writer.NewServer(writer.ServerConfig{Addr: cfg.HTTP.Addr}, writer.ServerDeps{
		Limiter:      lim,
		ScenarioPtr:  scenarioPtr,
		Registry:     reg,
		Logger:       logger,
		Targets:      cfg.PG.TargetTables,
		RampDuration: cfg.Scenario.RampDuration,
		CORSOrigins:  cfg.HTTP.CORSOrigins,
	})
	go func() {
		logger.Info().Str("addr", cfg.HTTP.Addr).Msg("HTTP server listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("HTTP server exited with error")
			stop()
		}
	}()
	return srv
}

func logStartup(logger zerolog.Logger, cfg *writer.WriterConfig) {
	logger.Info().
		Str("scenario", cfg.Scenario.Name).
		Float64("commit_rate", cfg.Scenario.CommitRate).
		Int("rows_per_tx", cfg.Scenario.RowsPerTx).
		Str("arrival_distribution", cfg.Arrivals.Distribution).
		Int("pool_max_conns", cfg.Pool.MaxConns).
		Str("http_addr", cfg.HTTP.Addr).
		Msg("writer started")
}

func gracefulShutdown(srv *http.Server, commitDone chan error, logger zerolog.Logger) {
	logger.Info().Msg("shutdown signal received")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn().Err(err).Msg("HTTP server shutdown returned error")
	}
	select {
	case <-commitDone:
	case <-time.After(2 * time.Second):
	}
}

func buildScenarioFromConfig(cfg *writer.WriterConfig) writer.Scenario {
	s := writer.BuildScenario(cfg.Scenario.Name, cfg.Scenario.CommitRate, cfg.Scenario.RowsPerTx, cfg.Scenario.RampDuration)
	if s == nil {
		return writer.NewSmokeScenario(cfg.Scenario.CommitRate, cfg.Scenario.RowsPerTx)
	}
	return s
}

var _ = pgxpool.Pool{}
