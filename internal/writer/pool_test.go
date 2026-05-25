package writer

import (
	"context"
	"strings"
	"testing"
)

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

func TestNewPool_AppliesBounds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := WriterPoolConfig{MaxConns: 3, MinConns: 0}
	p, err := NewPool(ctx, WriterPGConfig{DSN: "postgres://walera:walera@127.0.0.1:1/walera?sslmode=disable"}, cfg)
	if err != nil {

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
