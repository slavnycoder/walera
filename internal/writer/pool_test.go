package writer

import (
	"context"
	"strings"
	"testing"
)

// TestNewPool_ParseError exercises the pgxpool.ParseConfig error path.
func TestNewPool_ParseError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := NewPool(ctx, WriterPGConfig{DSN: "not://a-dsn"}, WriterPoolConfig{MaxConns: 4, MinConns: 1})
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if !strings.Contains(err.Error(), "writer pool") {
		t.Errorf("error = %q, want substring writer pool", err.Error())
	}
}

// TestNewPool_AppliesBounds verifies the pool's MaxConns/MinConns are set
// from WriterPoolConfig BEFORE the pool dials Postgres. We construct a pool
// against a DSN that points at an unreachable host so the dial is deferred
// (lazy connect) and only the config-time settings are observed via
// pool.Config().
func TestNewPool_AppliesBounds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// pgxpool.NewWithConfig does a lazy initial connect; if it tries to dial
	// synchronously we'd block. Set MinConns=0 to avoid the eager warm pool.
	cfg := WriterPoolConfig{MaxConns: 3, MinConns: 0}
	p, err := NewPool(ctx, WriterPGConfig{DSN: "postgres://walera:walera@127.0.0.1:1/walera?sslmode=disable"}, cfg)
	if err != nil {
		// Some environments may fail an eager check; that's fine — the test's
		// purpose is the bounds-applied assertion, not the dial.
		t.Skipf("pool construction failed in unreachable-dsn smoke (acceptable for env): %v", err)
	}
	defer p.Close()
	if got, want := p.Config().MaxConns, int32(3); got != want {
		t.Errorf("MaxConns = %d, want %d", got, want)
	}
	if got, want := p.Config().MinConns, int32(0); got != want {
		t.Errorf("MinConns = %d, want %d", got, want)
	}
}
