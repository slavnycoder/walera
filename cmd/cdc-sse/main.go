package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"go.uber.org/automaxprocs/maxprocs"

	"github.com/walera/walera/internal/app"
	"github.com/walera/walera/internal/walconn"
)

func main() {
	configPath := flag.String("config", "./config.yaml", "path to config YAML file")
	devLog := flag.Bool("dev-log", false, "enable human-readable console log output (stderr)")
	healthcheckFlag := flag.Bool("healthcheck", false, "perform a single GET to /healthz on the local server and exit 0 if 200, 1 otherwise (for distroless container HEALTHCHECK)")
	flag.Parse()

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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	adminConn, err := walconn.NewAdminConn(ctx, cfg.WAL.PostgresDSN)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to PostgreSQL (admin connection)")
	}

	if err := app.PrepareDatabase(ctx, cfg, logger, adminConn); err != nil {
		closeAdminConnOnErr(logger, adminConn, "PrepareDatabase failure")
		logger.Fatal().Err(err).Msg("failed to prepare PostgreSQL")
	}

	a, cleanup, err := app.InitializeApp(*cfg, logger, adminConn)
	if err != nil {

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

func closeAdminConnOnErr(logger zerolog.Logger, c walconn.AdminConn, where string) {
	if cerr := (*pgx.Conn)(c).Close(context.Background()); cerr != nil {
		logger.Warn().Err(cerr).Str("where", where).
			Msg("failed to close admin conn during startup error cleanup")
	}
}
