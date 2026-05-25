//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type PG struct {
	Container *postgres.PostgresContainer
	DSN       string
}

func NewPG(t *testing.T) *PG {
	t.Helper()
	ctx := context.Background()

	c, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("walera_test"),
		postgres.WithUsername("walera"),
		postgres.WithPassword("walera"),
		postgres.WithConfigFile("testdata/postgresql-walera.conf"),
		postgres.WithInitScripts("testdata/001_publication.sql"),
		postgres.WithSQLDriver("pgx"),
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
		if err := testcontainers.TerminateContainer(c); err != nil {
			t.Logf("pg cleanup: %v", err)
		}
	})

	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return &PG{Container: c, DSN: dsn}
}

func (p *PG) Stop(ctx context.Context) error {
	return testcontainers.TerminateContainer(p.Container)
}

func (p *PG) Exec(ctx context.Context, sql string, args ...any) error {
	conn, err := pgx.Connect(ctx, p.DSN)
	if err != nil {
		return fmt.Errorf("pg exec: connect: %w", err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	if _, err := conn.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("pg exec: %w", err)
	}
	return nil
}

func (p *PG) ExecBatch(ctx context.Context, stmts []string, perStmtArgs [][]any) error {
	if len(stmts) != len(perStmtArgs) {
		return errors.New("pg execbatch: len(stmts) != len(perStmtArgs)")
	}
	conn, err := pgx.Connect(ctx, p.DSN)
	if err != nil {
		return fmt.Errorf("pg execbatch: connect: %w", err)
	}
	defer conn.Close(ctx) //nolint:errcheck

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg execbatch: begin: %w", err)
	}
	for i, s := range stmts {
		if _, err := tx.Exec(ctx, s, perStmtArgs[i]...); err != nil {
			_ = tx.Rollback(ctx) //nolint:errcheck
			return fmt.Errorf("pg execbatch: stmt %d: %w", i, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pg execbatch: commit: %w", err)
	}
	return nil
}

func (p *PG) CreatePublication(t *testing.T, name string, tables []string) {
	t.Helper()
	ctx := context.Background()
	var sql string
	if len(tables) == 0 {
		sql = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", name)
	} else {
		sql = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", name, joinIdents(tables))
	}
	if err := p.Exec(ctx, sql); err != nil {
		t.Fatalf("CreatePublication %q: %v", name, err)
	}
	t.Cleanup(func() { p.DropPublication(t, name) })
}

func (p *PG) DropPublication(t *testing.T, name string) {
	t.Helper()
	ctx := context.Background()
	sql := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", name)
	if err := p.Exec(ctx, sql); err != nil {
		t.Fatalf("DropPublication %q: %v", name, err)
	}
}

func (p *PG) CreateLogicalSlot(t *testing.T, name string, temporary bool) {
	t.Helper()
	ctx := context.Background()
	sql := "SELECT pg_create_logical_replication_slot($1, 'pgoutput', $2)"
	conn, err := pgx.Connect(ctx, p.DSN)
	if err != nil {
		t.Fatalf("CreateLogicalSlot %q: connect: %v", name, err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	if _, err := conn.Exec(ctx, sql, name, temporary); err != nil {
		t.Fatalf("CreateLogicalSlot %q: %v", name, err)
	}
	if !temporary {
		t.Cleanup(func() { p.DropSlot(t, name) })
	}
}

func (p *PG) DropSlot(t *testing.T, name string) {
	t.Helper()
	if !p.SlotExists(t, name) {
		return
	}
	ctx := context.Background()
	if err := p.Exec(ctx, "SELECT pg_drop_replication_slot($1)", name); err != nil {
		t.Fatalf("DropSlot %q: %v", name, err)
	}
}

func (p *PG) SlotExists(t *testing.T, name string) bool {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, p.DSN)
	if err != nil {
		t.Fatalf("SlotExists %q: connect: %v", name, err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	var exists bool
	if err := conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		name,
	).Scan(&exists); err != nil {
		t.Fatalf("SlotExists %q: %v", name, err)
	}
	return exists
}

func joinIdents(idents []string) string {
	switch len(idents) {
	case 0:
		return ""
	case 1:
		return idents[0]
	default:
		out := idents[0]
		for _, s := range idents[1:] {
			out += ", " + s
		}
		return out
	}
}

func (p *PG) ReplicationDSN() string {
	u, err := url.Parse(p.DSN)
	if err != nil {

		return p.DSN + "&replication=database"
	}
	q := u.Query()
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	return u.String()
}
