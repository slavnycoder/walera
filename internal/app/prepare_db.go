package app

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/walconn"
)

func PrepareDatabase(ctx context.Context, cfg *AppConfig, logger zerolog.Logger, adminConn walconn.AdminConn) error {
	conn := (*pgx.Conn)(adminConn)
	return prepareDatabase(ctx, cfg, logger, conn, os.Hostname, os.Getpid)
}

func prepareDatabase(ctx context.Context, cfg *AppConfig, logger zerolog.Logger, conn bootstrapDB, hostnameFn func() (string, error), pidFn func() int) error {
	if err := verifyPGPrereqs(ctx, conn, logger); err != nil {
		return err
	}
	if err := verifyReplicationRole(ctx, conn, logger); err != nil {
		return err
	}
	if err := bootstrapPublication(ctx, conn, bootstrapConfig{
		PublicationName: string(cfg.WAL.PublicationName),
		Tables:          cfg.WAL.Tables,
	}, logger); err != nil {
		return err
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	slotName := string(cfg.WAL.NewSlotName(hostname, pidFn()))
	checkSlotHeadroom(ctx, conn, cfg.WAL.SlotHeadroomMin, slotName, logger)
	return nil
}
