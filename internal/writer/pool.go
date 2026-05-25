package writer

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(ctx context.Context, cfg WriterPGConfig, poolCfg WriterPoolConfig) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("writer pool: parse dsn: %w", err)
	}
	pcfg.MaxConns = int32(poolCfg.MaxConns)
	pcfg.MinConns = int32(poolCfg.MinConns)
	pcfg.MaxConnIdleTime = 30 * time.Second
	pcfg.MaxConnLifetime = time.Hour
	pcfg.MaxConnLifetimeJitter = 5 * time.Minute

	p, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("writer pool: connect: %w", err)
	}
	return p, nil
}
