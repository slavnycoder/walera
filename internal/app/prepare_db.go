package app

import (
	"context"
	"os"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/walconn"
)

func PrepareDatabase(ctx context.Context, cfg *AppConfig, logger zerolog.Logger, adminConn walconn.AdminConn) error {
	if err := verifyPGPrereqs(ctx, adminConn, logger); err != nil {
		return err
	}
	if err := verifyReplicationRole(ctx, adminConn, logger); err != nil {
		return err
	}
	if err := bootstrapPublication(ctx, adminConn, bootstrapConfig{
		Mode:            cfg.WAL.Bootstrap.Mode,
		PublicationName: string(cfg.WAL.PublicationName),
		Tables:          cfg.WAL.Bootstrap.Tables,
		CreateRoles:     cfg.WAL.Bootstrap.CreateRoles,

		ReplicationDSN: string(cfg.WAL.ReplicationDSN),
		PostgresDSN:    string(cfg.WAL.PostgresDSN),
	}, logger); err != nil {
		return err
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	slotName := string(cfg.WAL.NewSlotName(hostname, os.Getpid()))
	checkSlotHeadroom(ctx, adminConn, cfg.WAL.SlotHeadroomMin, slotName, logger)
	return nil
}
