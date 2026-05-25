package wal

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
)

func bootstrapSlot(
	ctx context.Context,
	pgConn *pgconn.PgConn,
	cfg Config,
	log zerolog.Logger,
) (slotName string, startLSN pglogrepl.LSN, err error) {

	hostname, hostErr := os.Hostname()
	if hostErr != nil {
		hostname = "unknown"
	}
	typedSlotName := cfg.NewSlotName(hostname, os.Getpid())
	slotName = string(typedSlotName)

	if _, err = pglogrepl.IdentifySystem(ctx, pgConn); err != nil {
		return "", 0, fmt.Errorf("wal: IdentifySystem: %w", err)
	}

	slotResult, err := pglogrepl.CreateReplicationSlot(ctx, pgConn, string(typedSlotName), "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: true,
			Mode:      pglogrepl.LogicalReplication,
		})
	if err != nil {
		return "", 0, fmt.Errorf("wal: CreateReplicationSlot %q: %w", slotName, err)
	}

	startLSN, err = pglogrepl.ParseLSN(slotResult.ConsistentPoint)
	if err != nil {
		return "", 0, fmt.Errorf("wal: ParseLSN %q: %w", slotResult.ConsistentPoint, err)
	}
	setLSN(startLSN)
	log.Info().Str("start_lsn", startLSN.String()).Str("slot", slotName).Msg("replication slot created")

	err = pglogrepl.StartReplication(ctx, pgConn, string(typedSlotName), startLSN,
		pglogrepl.StartReplicationOptions{
			Mode: pglogrepl.LogicalReplication,
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", cfg.PublicationName),
			},
		})
	if err != nil {
		return "", 0, fmt.Errorf("wal: StartReplication %q: %w", slotName, err)
	}
	log.Info().Str("publication", string(cfg.PublicationName)).Msg("replication started")

	return slotName, startLSN, nil
}
