//go:build integration

package app

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/walera/walera/internal/walconn"
)

func bootPGSuper(t *testing.T) (host string, port string) {
	t.Helper()
	ctx := context.Background()

	c, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("walera_test"),
		postgres.WithUsername("walera"),
		postgres.WithPassword("walera"),
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
		_ = testcontainers.TerminateContainer(c)
	})

	h, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	p, err := c.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("mapped port: %v", err)
	}
	return h, p.Port()
}

func dsnFor(host, port, role, password string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/walera_test?sslmode=disable",
		role, password, host, port)
}

func connAs(t *testing.T, ctx context.Context, dsn string) walconn.AdminConn {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect as role: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return walconn.AdminConn(conn)
}

func TestVerifyReplicationRole_AllCases(t *testing.T) {
	host, port := bootPGSuper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	superDSN := dsnFor(host, port, "walera", "walera")
	superConn, err := pgx.Connect(ctx, superDSN)
	if err != nil {
		t.Fatalf("pgx.Connect superuser: %v", err)
	}
	defer superConn.Close(context.Background())

	for _, ddl := range []string{
		"CREATE ROLE repl_role WITH LOGIN REPLICATION PASSWORD 'pw'",
		"CREATE ROLE login_role WITH LOGIN PASSWORD 'pw'",
	} {
		if _, err := superConn.Exec(ctx, ddl); err != nil {
			t.Fatalf("provision role (%s): %v", ddl, err)
		}
	}

	t.Run("replication_role_passes", func(t *testing.T) {
		conn := connAs(t, ctx, dsnFor(host, port, "repl_role", "pw"))
		if err := verifyReplicationRole(ctx, conn, zerolog.Nop()); err != nil {
			t.Fatalf("expected nil for REPLICATION role, got: %v", err)
		}
	})

	t.Run("superuser_passes", func(t *testing.T) {
		conn := connAs(t, ctx, dsnFor(host, port, "walera", "walera"))
		if err := verifyReplicationRole(ctx, conn, zerolog.Nop()); err != nil {
			t.Fatalf("expected nil for superuser, got: %v", err)
		}
	})

	t.Run("login_only_fails_with_actionable_message", func(t *testing.T) {
		conn := connAs(t, ctx, dsnFor(host, port, "login_role", "pw"))
		err := verifyReplicationRole(ctx, conn, zerolog.Nop())
		if err == nil {
			t.Fatal("expected non-nil error for LOGIN-only role, got nil")
		}
		msg := err.Error()
		lower := strings.ToLower(msg)
		if !strings.Contains(msg, "login_role") {
			t.Errorf("error must name the role; got: %q", msg)
		}
		if !strings.Contains(msg, "ALTER ROLE") {
			t.Errorf("error must give the ALTER ROLE remedy; got: %q", msg)
		}
		if !strings.Contains(lower, "required") && !strings.Contains(lower, "prerequisite") {
			t.Errorf("error must state REPLICATION is required/prerequisite; got: %q", msg)
		}
		if !strings.Contains(lower, "replication") {
			t.Errorf("error must mention REPLICATION; got: %q", msg)
		}
		if strings.Contains(lower, "slot") {
			t.Errorf("error must NOT mention slot; got: %q", msg)
		}
		if !strings.Contains(msg, "docs/operations.md#required-runtime") {
			t.Errorf("error must reference the ops docs anchor; got: %q", msg)
		}
	})
}
