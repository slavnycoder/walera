//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
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

	baseline := sampleHeapAlloc()

	lagBegin, _ := querySlotLag(t, h.PG, "walera_test_")

	insertSQL := `
        INSERT INTO users (id, email, name)
        SELECT g + 10000, 'u' || g || '@x', repeat('n', 50)
        FROM generate_series(1, 10000) g
    `
	if err := h.PG.Exec(ctx, insertSQL); err != nil {
		t.Fatalf("large insert: %v", err)
	}

	const slowDrainSleep = 5 * time.Millisecond
	const expectedEvents = 10000

	var received int
	var lagMid int64
	midHeap := uint64(0)
	midSampled := false

drainLoop:
	for received < expectedEvents {
		select {
		case ev, ok := <-events:
			if !ok {
				break drainLoop
			}
			if ev.Type == "tx" {

				received += countChanges(ev.Data)
			}
			time.Sleep(slowDrainSleep)

			if !midSampled && received >= expectedEvents/4 {
				midHeap = sampleHeapAlloc()
				if lag, ok := querySlotLag(t, h.PG, "walera_test_"); ok {
					lagMid = lag
				}
				midSampled = true
			}
		case err := <-errCh:
			t.Fatalf("client error after %d events: %v", received, err)
		case <-ctx.Done():
			t.Fatalf("timeout after %d/%d events; stderr:\n%s", received, expectedEvents, h.Binary.Stderr())
		}
	}

	if !midSampled {

		midHeap = sampleHeapAlloc()
		if lag, ok := querySlotLag(t, h.PG, "walera_test_"); ok {
			lagMid = lag
		}
	}

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

	t.Logf("WAL-05 PASS: HeapAlloc baseline=%d mid=%d end=%d peak=%d (bound=%d); slot lag begin=%d mid=%d end=%d",
		baseline, midHeap, endHeap, peak, bound, lagBegin, lagMid, lagEnd)

	if metric, err := scrapeMetric(ctx, metricsURL, "walera_wal_lsn_lag_bytes"); err == nil {
		t.Logf("walera_wal_lsn_lag_bytes (in-process sampler) = %v", metric)
	}
}

func countChanges(data []byte) int {

	n := 0
	depth := 0
	inChanges := false
	const marker = `"changes":[`
	for i := 0; i+len(marker) <= len(data); i++ {
		if !inChanges && string(data[i:i+len(marker)]) == marker {
			inChanges = true
			i += len(marker) - 1
			continue
		}
		if inChanges {
			switch data[i] {
			case '{':
				if depth == 0 {
					n++
				}
				depth++
			case '}':
				depth--
			case ']':
				if depth == 0 {
					return n
				}
			}
		}
	}
	return n
}

var _ = http.StatusOK
var _ = fmt.Sprintf
var _ = strconv.Itoa
