//go:build integration

// Package writer — loop_integration_test.go boots a testcontainers Postgres
// with the testbench demo schema and exercises commitOnce end-to-end. The
// build tag keeps these tests out of the default `go test ./...` run so unit
// tests stay fast; CI invokes `go test -tags=integration` separately.
package writer

import (
	"context"
	"fmt"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// schemaSQL returns the testbench/migrations/002_demo_schema.sql contents.
// The integration test resolves the path relative to repo root.
func schemaSQL(t *testing.T) string {
	t.Helper()
	// Resolve repo root from the test working dir (internal/writer).
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root := filepath.Clean(filepath.Join(cwd, "..", ".."))
	return filepath.Join(root, "testbench", "migrations", "002_demo_schema.sql")
}

// bootPG launches PostgreSQL 18 with logical replication enabled and the
// demo schema applied (001 publication + 002 demo). Returns the admin DSN.
func bootPG(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root := filepath.Clean(filepath.Join(cwd, "..", ".."))
	pub := filepath.Join(root, "testbench", "migrations", "001_publication.sql")
	demo := schemaSQL(t)

	c, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("walera_test"),
		postgres.WithUsername("walera"),
		postgres.WithPassword("walera"),
		postgres.WithInitScripts(pub, demo),
		postgres.WithSQLDriver("pgx"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_INITDB_ARGS": "",
		}),
		// Logical replication is not strictly required for these writer tests
		// but matches the testbench production posture.
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("postgres.Run: %v", err)
	}
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(c)
	})
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return dsn
}

func TestNewPool_BoundedMaxConns(t *testing.T) {
	dsn := bootPG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p, err := NewPool(ctx, WriterPGConfig{DSN: dsn}, WriterPoolConfig{MaxConns: 3, MinConns: 1})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()
	if got, want := p.Config().MaxConns, int32(3); got != want {
		t.Errorf("MaxConns = %d, want %d", got, want)
	}
	if got, want := p.Config().MinConns, int32(1); got != want {
		t.Errorf("MinConns = %d, want %d", got, want)
	}
}

func TestCommitOnce_Orders_FiresRootBumpTrigger(t *testing.T) {
	dsn := bootPG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p, err := NewPool(ctx, WriterPGConfig{DSN: dsn}, WriterPoolConfig{MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	rng := mathrand.New(mathrand.NewPCG(42, 42))
	cfg := WriterPGConfig{TxTimeout: 5 * time.Second}

	if err := commitOnce(ctx, p, "orders", 2, rng, cfg); err != nil {
		t.Fatalf("commitOnce orders: %v", err)
	}

	// The most recently inserted orders row should have 2 line_items.
	var liCount int
	if err := p.QueryRow(ctx,
		"SELECT COUNT(*) FROM line_items WHERE orders_id = (SELECT MAX(id) FROM orders)").Scan(&liCount); err != nil {
		t.Fatalf("query line_items: %v", err)
	}
	if liCount != 2 {
		t.Errorf("line_items count for newest orders = %d, want 2", liCount)
	}

	// The trigger should have bumped orders.updated_at to ~now.
	var bumped bool
	if err := p.QueryRow(ctx,
		"SELECT updated_at > NOW() - interval '5 seconds' FROM orders WHERE id = (SELECT MAX(id) FROM orders)").Scan(&bumped); err != nil {
		t.Fatalf("query orders.updated_at: %v", err)
	}
	if !bumped {
		t.Errorf("orders.updated_at not bumped by root-bump trigger")
	}
}

func TestCommitOnce_Devices_SimpleInsert(t *testing.T) {
	dsn := bootPG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p, err := NewPool(ctx, WriterPGConfig{DSN: dsn}, WriterPoolConfig{MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	var beforeDevices, beforeLI int
	_ = p.QueryRow(ctx, "SELECT COUNT(*) FROM devices").Scan(&beforeDevices)
	_ = p.QueryRow(ctx, "SELECT COUNT(*) FROM line_items").Scan(&beforeLI)

	rng := mathrand.New(mathrand.NewPCG(7, 7))
	if err := commitOnce(ctx, p, "devices", 3, rng, WriterPGConfig{TxTimeout: 5 * time.Second}); err != nil {
		t.Fatalf("commitOnce devices: %v", err)
	}

	var afterDevices, afterLI int
	_ = p.QueryRow(ctx, "SELECT COUNT(*) FROM devices").Scan(&afterDevices)
	_ = p.QueryRow(ctx, "SELECT COUNT(*) FROM line_items").Scan(&afterLI)

	if afterDevices-beforeDevices != 3 {
		t.Errorf("devices delta = %d, want 3", afterDevices-beforeDevices)
	}
	if afterLI != beforeLI {
		t.Errorf("line_items count changed unexpectedly (before=%d after=%d)", beforeLI, afterLI)
	}
}

func TestCommitOnce_TxTimeout(t *testing.T) {
	dsn := bootPG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p, err := NewPool(ctx, WriterPGConfig{DSN: dsn}, WriterPoolConfig{MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	rng := mathrand.New(mathrand.NewPCG(9, 9))
	err = commitOnce(ctx, p, "devices", 1, rng, WriterPGConfig{TxTimeout: 1 * time.Nanosecond})
	if err == nil {
		t.Fatalf("expected deadline-exceeded error, got nil")
	}
}

// Sanity: confirm the demo schema is reachable through pgx directly (helps
// debug environment misconfigurations).
func TestBootPG_Reachable(t *testing.T) {
	dsn := bootPG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer conn.Close(ctx)

	var n int
	if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM orders").Scan(&n); err != nil {
		t.Fatalf("orders count: %v", err)
	}
	if n < 5 {
		t.Errorf("orders count = %d, want >= 5 (demo seed)", n)
	}
}

// guard against accidental imports tree-shaking pgxpool out of go.sum.
var _ = pgxpool.Pool{}

// unused helper kept to silence the formatter when we later wire mock probes.
func _unused() { _ = fmt.Sprintf("") }
