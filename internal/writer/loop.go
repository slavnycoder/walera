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

type commitOncePool interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

var commitOnceFn = realCommitOnce

func realCommitOnce(ctx context.Context, pool commitOncePool, target string, rowsPerTx int, rng *mathrand.Rand, cfg WriterPGConfig) error {
	return commitOnceImpl(ctx, pool, target, rowsPerTx, rng, cfg)
}

func commitOnce(ctx context.Context, pool *pgxpool.Pool, target string, rowsPerTx int, rng *mathrand.Rand, cfg WriterPGConfig) error {
	return commitOnceImpl(ctx, pool, target, rowsPerTx, rng, cfg)
}

func commitOnceImpl(ctx context.Context, pool commitOncePool, target string, rowsPerTx int, rng *mathrand.Rand, cfg WriterPGConfig) error {
	timeout := cfg.TxTimeout
	if timeout <= 0 {
		timeout = 5e9
	}
	txCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tx, err := pool.BeginTx(txCtx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("writer commit: begin: %w", err)
	}

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

// Depth-4 chain fan-out per orders insert. The triggers on line_items,
// line_item_options, and option_audits chain bumps all the way up to
// orders.updated_at — a single chain produces 1 INSERT + 3 line_item
// INSERTs + 6 option INSERTs + 6 audit INSERTs = 16 WAL row records and
// 3+6+6 = 15 cascaded UPDATE WAL records, all inside one transaction.
const (
	lineItemsPerOrder  = 3
	optionsPerLineItem = 2
	auditsPerOption    = 1
)

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
		for li := 0; li < lineItemsPerOrder; li++ {
			var lineItemID int64
			if err := tx.QueryRow(ctx,
				"INSERT INTO line_items (orders_id, sku, qty, unit_price_cents) VALUES ($1, $2, $3, $4) RETURNING id",
				orderID,
				fmt.Sprintf("SKU-%d", rng.IntN(10000)),
				int32(1+rng.IntN(5)),
				int64(rng.IntN(10000)),
			).Scan(&lineItemID); err != nil {
				return err
			}
			for op := 0; op < optionsPerLineItem; op++ {
				var optionID int64
				if err := tx.QueryRow(ctx,
					"INSERT INTO line_item_options (line_items_id, key, value) VALUES ($1, $2, $3) RETURNING id",
					lineItemID,
					fmt.Sprintf("attr-%d", rng.IntN(32)),
					fmt.Sprintf("val-%d", rng.IntN(256)),
				).Scan(&optionID); err != nil {
					return err
				}
				for au := 0; au < auditsPerOption; au++ {
					if _, err := tx.Exec(ctx,
						"INSERT INTO option_audits (line_item_options_id, actor, note) VALUES ($1, $2, $3)",
						optionID,
						"writer",
						fmt.Sprintf("rev-%d", rng.IntN(1024)),
					); err != nil {
						return err
					}
				}
			}
		}
		return nil
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

			continue
		}
		target := st.NextTarget()
		if target == "" {

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
