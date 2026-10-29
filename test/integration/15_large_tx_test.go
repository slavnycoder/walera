//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func querySlotLag(t *testing.T, p *PG, prefix string) (int64, bool) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, p.DSN)
	if err != nil {
		t.Fatalf("querySlotLag: connect: %v", err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	var lag int64
	err = conn.QueryRow(ctx, `
        SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), COALESCE(confirmed_flush_lsn, pg_current_wal_lsn()))::bigint
        FROM pg_replication_slots
        WHERE slot_name LIKE $1
        ORDER BY slot_name LIMIT 1
    `, prefix+"%").Scan(&lag)
	if err == pgx.ErrNoRows {
		return 0, false
	}
	if err != nil {
		t.Fatalf("querySlotLag: query: %v", err)
	}
	return lag, true
}

func confirmedSlotDrained(t *testing.T, p *PG, prefix string, tolerance int64, deadline time.Duration) (int64, bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	var last int64
	for time.Now().Before(end) {
		lag, ok := querySlotLag(t, p, prefix)
		if !ok {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		last = lag
		if lag <= tolerance {
			return lag, true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return last, false
}

// TestLargeTransactionBackpressure (WAL-05): one Postgres transaction with
// 10 000 row changes must not wedge the pipeline. The harness caps
// router.max_changes_per_tx at 1000, so the wildcard subscriber is dropped
// with an explicit tx_too_large error frame (never a silent truncation), the
// replication slot still drains, and the service keeps serving new
// subscribers.
func TestLargeTransactionBackpressure(t *testing.T) {
	h := NewHarness(t)
	h.Auth.SetMap(
		"test-token", "test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email", "name"}},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	events, errCh, closeFn := h.Client.Connect(ctx, "users/all", "test-token")
	defer closeFn()

	if err := h.PG.Exec(ctx, `
        INSERT INTO users (id, email, name)
        SELECT g + 10000, 'u' || g || '@x', repeat('n', 50)
        FROM generate_series(1, 10000) g
    `); err != nil {
		t.Fatalf("large insert: %v", err)
	}

	deadline := time.After(15 * time.Second)
wait:
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("events closed without a tx_too_large error frame; stderr:\n%s", h.Binary.Stderr())
			}
			switch ev.Type {
			case "tx":
				t.Fatalf("oversized tx delivered (%d bytes); want tx_too_large drop", len(ev.Data))
			case "error":
				if !strings.Contains(string(ev.Data), `"tx_too_large"`) {
					t.Fatalf("error event = %s; want tx_too_large", ev.Data)
				}
				break wait
			default:
				// heartbeats and other frames are irrelevant here
			}
		case err := <-errCh:
			t.Fatalf("client error before tx_too_large: %v", err)
		case <-deadline:
			t.Fatalf("no tx_too_large frame within 15s; stderr:\n%s", h.Binary.Stderr())
		}
	}

	if lag, drained := confirmedSlotDrained(t, h.PG, "walera_test_", 4*1024, 15*time.Second); !drained {
		t.Errorf("slot never drained to <= 4 KiB lag (last=%d bytes after 15s)", lag)
	}

	// The service keeps serving: a fresh wildcard subscriber gets a small tx.
	events2, errCh2, closeFn2 := h.Client.Connect(ctx, "users/all", "test-token")
	defer closeFn2()
	if err := h.PG.Exec(ctx,
		"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
		1, "a@b.c", "Alice",
	); err != nil {
		t.Fatalf("follow-up insert: %v", err)
	}
	ev := readTxEvent(ctx, t, h, events2, errCh2)
	if len(ev.Changes) != 1 {
		t.Fatalf("follow-up tx changes = %d; want 1 (%+v)", len(ev.Changes), ev.Changes)
	}
}
