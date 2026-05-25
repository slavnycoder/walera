package wal

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/walconn"
)

func SampleLagLoop(
	ctx context.Context,
	adminConn walconn.AdminConn,
	slotName SlotName,
	m *metrics.Registry,
	sampleInterval time.Duration,
	log zerolog.Logger,
) {

	pgxConn := (*pgx.Conn)(adminConn)

	const query = `
        SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
        FROM pg_replication_slots
        WHERE slot_name = $1
    `

	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sampleCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			var lagBytes int64

			err := pgxConn.QueryRow(sampleCtx, query, slotName).Scan(&lagBytes)
			cancel()
			if err != nil {

				log.Debug().Err(err).Str("slot", string(slotName)).Msg("wal lag sample skipped")
				continue
			}
			m.WALLSNLagBytes().Set(float64(lagBytes))
		}
	}
}
