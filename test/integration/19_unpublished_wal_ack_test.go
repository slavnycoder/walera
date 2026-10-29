//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Writes to unpublished tables produce no BEGIN/COMMIT on PG 15+, only
// keepalives. Walera must still confirm them, or the slot pins WAL forever.
func Test19UnpublishedWritesAdvanceSlot(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, h.PG.DSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background()) //nolint:errcheck

	if _, err := conn.Exec(ctx, "CREATE TABLE unpublished_bulk (id bigserial PRIMARY KEY, payload text)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	var before string
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&before); err != nil {
		t.Fatalf("current lsn: %v", err)
	}
	// ~4 MB of WAL in a few transactions, none touching a published table.
	for range 4 {
		if _, err := conn.Exec(ctx,
			"INSERT INTO unpublished_bulk (payload) SELECT repeat(md5(g::text), 32) FROM generate_series(1, 1000) g",
		); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	deadline := time.Now().Add(20 * time.Second)
	var confirmed string
	for time.Now().Before(deadline) {
		var advanced bool
		err := conn.QueryRow(ctx,
			`SELECT coalesce(confirmed_flush_lsn::text, ''), coalesce(confirmed_flush_lsn > $1::pg_lsn, false)
			   FROM pg_replication_slots WHERE slot_name LIKE 'walera%' AND active
			  ORDER BY slot_name LIMIT 1`, before,
		).Scan(&confirmed, &advanced)
		if err != nil {
			t.Fatalf("pg_replication_slots: %v", err)
		}
		if advanced {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("confirmed_flush_lsn = %s did not advance past %s within 20s", confirmed, before)
}
