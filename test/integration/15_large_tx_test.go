//go:build integration

package integration

import (
	"context"
	"runtime"
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

func sampleHeapAlloc() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
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

// TestLargeTransactionBackpressure (WAL-05): a single Postgres transaction
// containing 10 000 row changes flows through the WAL reader and broker
// without unbounded memory growth, and the replication slot drains afterwards.
//
// Under the writer-side one-root-per-tx discipline (README "Writer-side
// discipline"), 10 000 distinct anchor-table PKs in a single transaction is a
// deliberate violation: the broker drops the tx per-subscriber with
// `tx_dropped_total{reason="multi_root"}` and keeps the connection open.
// This test exercises exactly that pathological shape and pins the
// pipeline-level invariants that survive it:
//
//   - WAL reader consumes the whole tx (replication slot lag returns to ~0).
//   - Broker assembles + routes the tx without leaking memory
//     (heap peak ≤ 2× baseline).
//   - The wildcard subscriber registers exactly one multi_root drop, stays
//     connected (no `error` SSE event, no disconnect), and remains visible
//     in `walera_subscribers_active`.
func TestLargeTransactionBackpressure(t *testing.T) {

	t.Setenv("GOMEMLIMIT", "512MiB")

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

	metricsURL := h.Binary.BaseURL() + "/metrics"
	if _, err := waitForMetric(ctx, t, metricsURL,
		`walera_subscribers_active{type="wildcard"}`,
		func(v float64) bool { return v >= 1 },
		5*time.Second, 50*time.Millisecond,
	); err != nil {
		t.Fatalf("subscriber never registered: %v", err)
	}

	baseMultiRoot, err := scrapeMetric(ctx, metricsURL, `walera_tx_dropped_total{reason="multi_root"}`)
	if err != nil {
		t.Fatalf("scrape baseline multi_root: %v", err)
	}

	baseline := sampleHeapAlloc()

	lagBegin, _ := querySlotLag(t, h.PG, "walera_test_")

	const expectedChanges = 10000
	insertSQL := `
        INSERT INTO users (id, email, name)
        SELECT g + 10000, 'u' || g || '@x', repeat('n', 50)
        FROM generate_series(1, 10000) g
    `
	if err := h.PG.Exec(ctx, insertSQL); err != nil {
		t.Fatalf("large insert: %v", err)
	}

	// The tx is a writer-side discipline violation; no tx event must reach the
	// subscriber. Monitor the channel for a bounded window — any tx event or
	// errCh signal here is a regression. While waiting, sample heap during the
	// tx-assembly window so we observe the peak rather than the post-GC floor.
	deadline := time.Now().Add(5 * time.Second)
	midHeap := sampleHeapAlloc()
	var lagMid int64
	for time.Now().Before(deadline) {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("events channel closed during large-tx ingest (subscriber unexpectedly disconnected)")
			}
			if ev.Type == "tx" {
				t.Fatalf("multi_root discipline violation tx unexpectedly delivered: %s", string(ev.Data))
			}
			if ev.Type == "error" {
				t.Fatalf("subscriber received error event during multi_root drop (expected silent drop): %s", string(ev.Data))
			}
		case err := <-errCh:
			t.Fatalf("subscriber unexpectedly errored during large-tx ingest: %v (multi_root must NOT disconnect)", err)
		case <-time.After(50 * time.Millisecond):
		}
		if h := sampleHeapAlloc(); h > midHeap {
			midHeap = h
		}
		if lag, ok := querySlotLag(t, h.PG, "walera_test_"); ok && lag > lagMid {
			lagMid = lag
		}
	}

	// Multi-root drop must have fired exactly once for this subscriber.
	mrAfter, err := waitForMetric(ctx, t, metricsURL,
		`walera_tx_dropped_total{reason="multi_root"}`,
		func(v float64) bool { return v >= baseMultiRoot+1 },
		5*time.Second, 50*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("multi_root counter did not increment (baseline=%v): %v", baseMultiRoot, err)
	}
	if got := mrAfter - baseMultiRoot; got != 1 {
		t.Errorf("multi_root delta: got %v; want 1 (one drop per matched subscriber per tx)", got)
	}

	// Subscriber must still be registered — multi_root keeps the connection.
	if v, err := scrapeMetric(ctx, metricsURL, `walera_subscribers_active{type="wildcard"}`); err != nil {
		t.Fatalf("scrape walera_subscribers_active: %v", err)
	} else if v < 1 {
		t.Errorf("walera_subscribers_active{type=wildcard} = %v after multi_root drop; want >= 1 (subscriber must stay connected)", v)
	}

	// Slot lag must drain — the WAL reader keeps consuming regardless of the
	// per-subscriber drop.
	lagEnd, drained := confirmedSlotDrained(t, h.PG, "walera_test_", 4*1024, 15*time.Second)
	endHeap := sampleHeapAlloc()

	peak := midHeap
	if endHeap > peak {
		peak = endHeap
	}
	bound := 2 * baseline

	if peak > bound {
		t.Errorf("WAL-05 large-tx test failed:\n"+
			"  HeapAlloc: baseline=%d mid=%d end=%d peak=%d (bound=2×baseline=%d)\n"+
			"  Slot lag:  begin=%d mid=%d end=%d (expect: rises during tx, drains after)",
			baseline, midHeap, endHeap, peak, bound,
			lagBegin, lagMid, lagEnd)
	}

	if !drained {
		t.Errorf("slot never drained to ≤ 4 KiB lag (last=%d bytes after 15s); HeapAlloc baseline=%d mid=%d end=%d",
			lagEnd, baseline, midHeap, endHeap)
	}

	t.Logf("WAL-05 PASS: %d-change multi_root tx flowed through; HeapAlloc baseline=%d mid=%d end=%d peak=%d (bound=%d); slot lag begin=%d mid=%d end=%d",
		expectedChanges, baseline, midHeap, endHeap, peak, bound, lagBegin, lagMid, lagEnd)

	if metric, err := scrapeMetric(ctx, metricsURL, "walera_wal_lsn_lag_bytes"); err == nil {
		t.Logf("walera_wal_lsn_lag_bytes (in-process sampler) = %v", metric)
	}
}

