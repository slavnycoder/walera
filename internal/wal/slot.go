// Package wal — slot.go owns the temporary logical replication slot
// lifecycle for the pgoutput plugin.
//
// Scope:
//   - Slot-name construction via cfg.NewSlotName(hostname, pid).
//   - System identification (pglogrepl.IdentifySystem).
//   - Temporary logical slot creation (CreateReplicationSlot with
//     Temporary=true, Mode=LogicalReplication, plugin "pgoutput").
//   - Parsing the consistent-point LSN returned by slot creation.
//   - StartReplication PluginArgs assembly: proto_version '1' (pinned per
//     reader.go invariant 2) and publication_names '<name>'.
//
// The standby ticker and the reconnect/backoff loop stay in reader.go —
// goroutine ownership is unchanged (D-06).
package wal

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
)

// bootstrapSlot performs the slot-creation handshake against pgConn and
// returns the slot name and start LSN at which StartReplication began
// streaming. lastCommittedLSN is updated BEFORE StartReplication is invoked
// so the standby ticker, which may begin sending ACKs immediately, never
// observes a zero LSN.
//
// Slot policy: matches docs/adr/0004-replication-slot-policy.md (Temporary
// slot policy). The slot is created with Temporary=true so PostgreSQL drops
// it automatically when the replication connection closes; no persistent
// slot survives a Walera restart. This contract is asserted end-to-end by
// test/integration/14_slot_lifecycle_test.go (WAL-06).
//
// Error strings and log field keys are preserved verbatim from the prior
// inline implementation in reader.runOnce.
func bootstrapSlot(
	ctx context.Context,
	pgConn *pgconn.PgConn,
	cfg Config,
	log zerolog.Logger,
) (slotName string, startLSN pglogrepl.LSN, err error) {
	// 1. Build slot name from hostname + pid.
	hostname, hostErr := os.Hostname()
	if hostErr != nil {
		hostname = "unknown"
	}
	typedSlotName := cfg.NewSlotName(hostname, os.Getpid())
	slotName = string(typedSlotName)

	// 2. Identify system.
	if _, err = pglogrepl.IdentifySystem(ctx, pgConn); err != nil {
		return "", 0, fmt.Errorf("wal: IdentifySystem: %w", err)
	}

	// 3. Create temporary logical replication slot.
	slotResult, err := pglogrepl.CreateReplicationSlot(ctx, pgConn, string(typedSlotName), "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: true,
			Mode:      pglogrepl.LogicalReplication,
		})
	if err != nil {
		return "", 0, fmt.Errorf("wal: CreateReplicationSlot %q: %w", slotName, err)
	}

	// 4. Parse the consistent point LSN returned by slot creation.
	startLSN, err = pglogrepl.ParseLSN(slotResult.ConsistentPoint)
	if err != nil {
		return "", 0, fmt.Errorf("wal: ParseLSN %q: %w", slotResult.ConsistentPoint, err)
	}
	setLSN(startLSN)
	log.Info().Str("start_lsn", startLSN.String()).Str("slot", slotName).Msg("replication slot created")

	// 5. Start replication with proto_version '1' pinned.
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
