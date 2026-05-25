// Package wal — internal/wal/lag_sampler.go: walera_wal_lsn_lag_bytes sampler.
//
// SampleLagLoop is a long-lived goroutine spawned by cmd/cdc-sse/main.go via
// safego.Go("wal-lag-sampler", ...). It polls
// `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)` against the
// admin connection every sampleInterval (default 5s) and updates the
// walera_wal_lsn_lag_bytes gauge.
//
// Slot races: during the reconnect window the temporary slot momentarily
// does not exist in pg_replication_slots. The query uses `COALESCE(..., 0)`
// on the diff PLUS row-absent on the slot itself; either way the err path
// logs Debug and skips — NEVER warn/error, because slot-absent during
// reconnect is expected.
//
// Admin conn lifecycle: the caller (main.go) owns adminConn's open/close.
// This loop calls only QueryRow; it must NOT close adminConn.
package wal

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/walconn"
)

// SampleLagLoop polls pg_wal_lsn_diff every sampleInterval and updates the
// walera_wal_lsn_lag_bytes gauge. Exits cleanly on ctx.Done.
//
// Caller invariants:
//   - adminConn is non-nil and connected when SampleLagLoop is invoked.
//   - The caller (main.go) closes adminConn after this loop has returned.
//   - sampleInterval > 0 (validated by config.validate).
//
// Per-query timeout: 2s via context.WithTimeout (matches the readyz / auth
// per-call timeout). Wraps QueryRow + Scan + cancel into one logical unit.
func SampleLagLoop(
	ctx context.Context,
	adminConn walconn.AdminConn,
	slotName SlotName,
	m *metrics.Registry,
	sampleInterval time.Duration,
	log zerolog.Logger,
) {
	// Named pointer types (`type AdminConn *pgx.Conn`) do NOT inherit the
	// method set of their underlying pointer type — Go's method promotion
	// applies only to defined non-pointer types and to embedded fields.
	// Shadow the parameter once at the function boundary with the underlying
	// *pgx.Conn so the QueryRow call resolves against pgx.Conn's method set.
	// Mirrors Plan 03-01's "explicit cast at every external-API boundary"
	// discipline applied to the pointer-type kind change.
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
			// slotName is wal.SlotName (named string) — pgx serialises via
			// the underlying string kind, but the typed parameter binding
			// goes through reflection so an explicit string(slotName) cast
			// at the QueryRow boundary is unnecessary (pgx's value encoder
			// uses reflect.Kind, not Type). Confirmed by existing
			// integration tests; the cast at the zerolog.Str site below IS
			// required because zerolog.Event.Str has a typed string parameter.
			err := pgxConn.QueryRow(sampleCtx, query, slotName).Scan(&lagBytes)
			cancel()
			if err != nil {
				// Slot may not exist yet (reconnect window) — Debug, not Warn.
				// PII-safe fields: slot name only (PK-shaped identifier).
				log.Debug().Err(err).Str("slot", string(slotName)).Msg("wal lag sample skipped")
				continue
			}
			m.WALLSNLagBytes().Set(float64(lagBytes))
		}
	}
}
