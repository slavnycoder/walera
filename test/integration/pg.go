//go:build integration

// Package integration — pg.go wraps testcontainers-go/modules/postgres to boot
// a PostgreSQL 18 container configured for logical replication (wal_level =
// logical, max_replication_slots = 10, max_wal_senders = 10). The init script
// at testdata/001_publication.sql is executed by the docker-entrypoint at
// initdb time and creates the cdc_sse_streamer publication along with the
// users / orders / audit_log table stubs the scenarios need.
//
// CRITICAL — DB NAME (Pitfall G6): we boot under the `walera_test` database
// (NEVER the default `postgres`). The system DB on a PG container is owned by
// internal processes and is brittle for replication-slot tests.
//
// Build tag: every file in test/integration/ except doc.go starts with
// `//go:build integration` so `go test ./...` reports `[no test files]` for
// this package without the tag (Pitfall G4).
package integration

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib" // enables postgres.WithSQLDriver("pgx") fast path
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// PG wraps a running postgres container plus its admin DSN. Used directly by
// scenarios (PG.Exec) and indirectly by the spawned binary (PG.DSN /
// PG.ReplicationDSN propagate into the generated walera-test.yaml config).
type PG struct {
	Container *postgres.PostgresContainer
	DSN       string
}

// NewPG boots a `postgres:18-alpine` container with wal_level=logical and
// runs the publication migration. Caller invokes via NewHarness(t) or
// directly. On test cleanup the container is terminated.
//
// The wait strategy uses `wait.ForLog("database system is ready to accept
// connections").WithOccurrence(2)` per TEST-04: PG logs that message twice
// during init — once at the end of initdb (single-user mode), once at the
// final ready-to-accept-connections moment after the entrypoint script has
// finished running 001_publication.sql.
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

// Stop terminates the container. Used by the restart-resume scenario
// (test 07). For other scenarios this is a no-op invoked only via
// t.Cleanup.
func (p *PG) Stop(ctx context.Context) error {
	return testcontainers.TerminateContainer(p.Container)
}

// Exec opens a one-shot connection to the admin DSN and runs sql with args.
// Convenience for scenarios that need to fire DML against the test DB
// (INSERT / UPDATE / DELETE / BEGIN-COMMIT blocks).
//
// A fresh connection per call is intentional: it sidesteps the connection-
// pool lifecycle complications that would otherwise leak across t.Cleanup
// boundaries when scenarios run with t.Parallel(). The boot cost (~5ms per
// connect to a local container) is well under the budget for the four
// scenarios in this plan.
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

// ExecBatch runs every statement in stmts inside a single explicit
// transaction (BEGIN / ... / COMMIT). Used by scenario 02 to construct a
// multi-row tx whose WAL representation MUST arrive at the subscriber as ONE
// Event with N changes (transactional atomicity).
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

// CreatePublication issues `CREATE PUBLICATION <name> FOR TABLE <tables...>`
// against the admin DSN. If tables is empty, falls back to
// `CREATE PUBLICATION <name> FOR ALL TABLES`. Registers a t.Cleanup that
// calls DropPublication so scenarios don't have to remember teardown.
//
// Helper that unblocks the 14_slot_lifecycle_test.go scenarios on the
// WAL-01 publication-reuse path. The helper takes fully-qualified table
// names exactly as the caller supplies them — no implicit schema prefix
// or sanitization. Callers derive unique publication names from t.Name()
// to avoid collisions under t.Parallel.
//
// SQL errors fail the test via t.Fatalf — there is no recovery path for a
// scenario that cannot install its publication.
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

// DropPublication issues `DROP PUBLICATION IF EXISTS <name>` against the
// admin DSN. Idempotent — the IF EXISTS clause keeps double-drop from
// t.Cleanup + an explicit scenario teardown safe.
func (p *PG) DropPublication(t *testing.T, name string) {
	t.Helper()
	ctx := context.Background()
	sql := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", name)
	if err := p.Exec(ctx, sql); err != nil {
		t.Fatalf("DropPublication %q: %v", name, err)
	}
}

// CreateLogicalSlot issues
//
//	SELECT pg_create_logical_replication_slot(<name>, 'pgoutput', <temporary>)
//
// against the admin DSN. When temporary=false, registers a t.Cleanup
// calling DropSlot for safe teardown. When temporary=true, NO cleanup is
// registered because temporary slots vanish automatically on session close
// — registering a cleanup would race with that drop and produce noisy
// "slot does not exist" errors.
//
// Unblocks the SlotAlreadyExists scenario in
// 14_slot_lifecycle_test.go, which pre-creates a non-temporary slot with
// the name Walera would otherwise pick to assert the observable
// already-exists behaviour.
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

// DropSlot drops the replication slot if it currently exists. Prechecks
// pg_replication_slots so the helper is safe to call from t.Cleanup after a
// temporary slot has already vanished — PG raises if the slot is missing
// at drop time, which would surface as noisy cleanup output.
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

// SlotExists returns true when pg_replication_slots contains a row whose
// slot_name matches name. Used by 14_slot_lifecycle_test.go to verify
// temporary-slot lifecycle (slot present while connected, absent after the
// replication connection closes).
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

// joinIdents concatenates identifier strings with ", " for use in the
// FOR TABLE clause of CreatePublication. The caller is responsible for
// supplying schema-qualified or unqualified identifiers exactly as PG
// expects — the helper does not validate or quote.
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

// ReplicationDSN returns the admin DSN with `replication=database` appended
// in the query string. The Walera replication code path (pgconn directly via
// pglogrepl) requires this query parameter.
//
// Uses net/url so we correctly preserve any pre-existing query keys (the
// testcontainers DSN typically only sets `sslmode`).
func (p *PG) ReplicationDSN() string {
	u, err := url.Parse(p.DSN)
	if err != nil {
		// Best-effort fallback: testcontainers always returns a parseable URL,
		// but if that invariant changes we still produce something useable.
		return p.DSN + "&replication=database"
	}
	q := u.Query()
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	return u.String()
}
