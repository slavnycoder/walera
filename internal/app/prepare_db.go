// Package app — prepare_db.go owns PrepareDatabase, the PostgreSQL
// bootstrap entry point cmd/cdc-sse/main.go calls AFTER opening the admin
// connection and BEFORE invoking InitializeApp. Splitting it out keeps
// InitializeApp pure construction (no DB I/O).
package app

import (
	"context"
	"os"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/walconn"
)

// PrepareDatabase runs the PostgreSQL bootstrap sequence: verifyPGPrereqs
// → bootstrapPublication → checkSlotHeadroom. The caller (cmd/cdc-sse)
// owns adminConn until *App takes ownership in InitializeApp; *AppConfig
// MUST NOT be mutated between PrepareDatabase and InitializeApp.
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
		// DSN casts: typed wal.Config fields → bootstrapConfig bare strings.
		ReplicationDSN: string(cfg.WAL.ReplicationDSN),
		PostgresDSN:    string(cfg.WAL.PostgresDSN),
	}, logger); err != nil {
		return err
	}
	// Deterministic per-process slot name (hostname+pid) — matches the name
	// bootstrapSlot builds in this same process, mirroring its "unknown"
	// hostname fallback.
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	slotName := string(cfg.WAL.NewSlotName(hostname, os.Getpid()))
	checkSlotHeadroom(ctx, adminConn, cfg.WAL.SlotHeadroomMin, slotName, logger)
	return nil
}
