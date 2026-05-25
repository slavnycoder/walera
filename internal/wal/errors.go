// Package wal — errors.go houses the package's exported sentinel errors.
//
// Keeping sentinels in a dedicated file keeps the consumer-facing surface
// scannable: callers can reach for ErrNotConnected (etc.) without spelunking
// through reader.go's long body.
package wal

import "errors"

// ErrNotConnected is returned by Reader.CheckPG when the replication
// connection is not currently open. It is the sentinel that the
// internal/health.PgChecker interface bridges to operator-visible
// /readyz state.
var ErrNotConnected = errors.New("wal: replication connection not established")
