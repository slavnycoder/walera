// Package writer — loop.go drives the single commit goroutine: read the
// active scenarioPtr, block in waitArrival, round-robin a target, call
// commitOnce, dispatch via onCommit / onError. See INVARIANTS.md
// (PII discipline, error classification, ctx-cancel contract).
package writer

import (
	"context"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand/v2"
	"net"
	"sync/atomic"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

// commitOncePool is the BeginTx seam commitOnce needs. The test-only
// fakePool in commitonce_test.go is the second implementation. See
// INVARIANTS.md.
type commitOncePool interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// commitOnceFn is the indirection through which RunCommitLoop calls
// commitOnce. Tests overwrite this variable to inject deterministic
// success/failure without touching Postgres.
var commitOnceFn = realCommitOnce

// realCommitOnce is the production commitOnce that satisfies the indirection.
func realCommitOnce(ctx context.Context, pool commitOncePool, target string, rowsPerTx int, rng *mathrand.Rand, cfg WriterPGConfig) error {
	return commitOnceImpl(ctx, pool, target, rowsPerTx, rng, cfg)
}

// commitOnce is the public entry used by RunCommitLoop (via the indirection
// above) and by integration tests. It opens a tx, inserts rowsPerTx rows
// into target (with line_items siblings when target=="orders"), then
// commits.
func commitOnce(ctx context.Context, pool *pgxpool.Pool, target string, rowsPerTx int, rng *mathrand.Rand, cfg WriterPGConfig) error {
	return commitOnceImpl(ctx, pool, target, rowsPerTx, rng, cfg)
}

func commitOnceImpl(ctx context.Context, pool commitOncePool, target string, rowsPerTx int, rng *mathrand.Rand, cfg WriterPGConfig) error {
	timeout := cfg.TxTimeout
	if timeout <= 0 {
		timeout = 5e9 // 5s safety fallback (defensive — Load should enforce >0)
	}
	txCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tx, err := pool.BeginTx(txCtx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("writer commit: begin: %w", err)
	}
	// rollback is a no-op after a successful commit; ignore the error.
	defer func() { _ = tx.Rollback(txCtx) }()

	for i := 0; i < rowsPerTx; i++ {
		if err := insertOne(txCtx, tx, target, rng); err != nil {
			return fmt.Errorf("writer commit: insert %s: %w", target, err)
		}
	}

	if err := tx.Commit(txCtx); err != nil {
		return fmt.Errorf("writer commit: commit: %w", err)
	}
	return nil
}

// insertOne executes the INSERT for a single row into target. For "orders"
// it also inserts a line_items row referencing the freshly-inserted parent
// (exercises the root-bump trigger). devices/articles are simple inserts.
func insertOne(ctx context.Context, tx pgx.Tx, target string, rng *mathrand.Rand) error {
	switch target {
	case "orders":
		var orderID int64
		if err := tx.QueryRow(ctx,
			"INSERT INTO orders (customer_name, total_cents, status) VALUES ($1, $2, $3) RETURNING id",
			fmt.Sprintf("customer-%d", rng.Uint64()),
			int64(rng.IntN(100000)),
			"pending",
		).Scan(&orderID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx,
			"INSERT INTO line_items (orders_id, sku, qty, unit_price_cents) VALUES ($1, $2, $3, $4)",
			orderID,
			fmt.Sprintf("SKU-%d", rng.IntN(10000)),
			int32(1+rng.IntN(5)),
			int64(rng.IntN(10000)),
		)
		return err
	case "devices":
		_, err := tx.Exec(ctx,
			"INSERT INTO devices (name, firmware_version, metadata) VALUES ($1, $2, $3::jsonb)",
			fmt.Sprintf("device-%d", rng.Uint64()),
			"1.0.0",
			"{}",
		)
		return err
	case "articles":
		_, err := tx.Exec(ctx,
			"INSERT INTO articles (slug, title, body, published) VALUES ($1, $2, $3, $4)",
			fmt.Sprintf("slug-%d", rng.Uint64()),
			"generated",
			"writer body",
			false,
		)
		return err
	default:
		return fmt.Errorf("unknown target table %q", target)
	}
}

// classify maps an error to a writer_errors_total{reason} label:
// "" for nil, "pg_constraint" for SQLSTATE 23xxx, "pg_conn" for
// net/syscall/io.EOF/ECONNRESET, "pg_other" for everything else.
func classify(err error) string {
	if err == nil {
		return ""
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if len(pgErr.Code) >= 2 && pgErr.Code[:2] == "23" {
			return "pg_constraint"
		}
		return "pg_other"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) {
		return "pg_conn"
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return "pg_conn"
	}
	return "pg_other"
}

// RunCommitLoop is the writer's single dedicated commit goroutine. It
// blocks in waitArrival, atomically reads the active scenarioState on
// every iteration, calls commitOnce, and dispatches the result via the
// nil-safe onCommit / onError callbacks. Returns ctx.Err() on
// cancellation; never returns nil.
func RunCommitLoop(
	ctx context.Context,
	pool *pgxpool.Pool,
	lim *rate.Limiter,
	scenarioPtr *atomic.Pointer[scenarioState],
	dist ArrivalDistribution,
	rng *mathrand.Rand,
	cfg WriterPGConfig,
	logger zerolog.Logger,
	onCommit func(scenario, target string, rows int),
	onError func(reason string),
) error {
	// poolAdapter lets the indirection accept the real *pgxpool.Pool or
	// nil (unit-test path where commitOnceFn is stubbed).
	var poolAdapter commitOncePool
	if pool != nil {
		poolAdapter = pool
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := waitArrival(ctx, lim, dist, rng); err != nil {
			return err
		}

		st := scenarioPtr.Load()
		if st == nil {
			// No active scenario — yield back to ctx loop.
			continue
		}
		target := st.NextTarget()
		if target == "" {
			// Misconfiguration; log once-ish and continue.
			logger.Warn().Str("scenario", st.Scenario.Name()).Msg("writer commit loop: empty target list")
			continue
		}

		err := commitOnceFn(ctx, poolAdapter, target, st.RowsPerTx, rng, cfg)
		if err != nil {
			reason := classify(err)
			logger.Debug().
				Str("scenario", st.Scenario.Name()).
				Str("target", target).
				Int("rows", st.RowsPerTx).
				Str("reason", reason).
				Msg("writer commit failed")
			if onError != nil {
				onError(reason)
			}
			continue
		}
		if onCommit != nil {
			onCommit(st.Scenario.Name(), target, st.RowsPerTx)
		}
	}
}
