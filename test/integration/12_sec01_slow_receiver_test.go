//go:build integration

package integration

import (
	"bufio"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func Test_SEC01_SlowReceiverDropped(t *testing.T) {
	t.Skip("flaky on slow CI hardware; tracked as Phase 11 follow-up. " +
		"Slow-receiver drop is also covered by internal/sse " +
		"TestPoolSlowClientIsolation and TestPoolSlowClientIsolationStress.")
	t.Parallel()

	h := NewHarness(t, WithWriteTimeout(200*time.Millisecond))

	h.Auth.SetMap(
		"test-token",
		"test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email", "name"}},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rawConn := dialRawSSE(t, h.Binary.BaseURL(), "users/42", "test-token")
	defer rawConn.Close() //nolint:errcheck

	br := bufio.NewReaderSize(rawConn, 64*1024)
	if err := readHTTPResponseHeaders(br); err != nil {
		t.Fatalf("read response headers: %v; stderr:\n%s", err, h.Binary.Stderr())
	}

	metricsURL := h.Binary.BaseURL() + "/metrics"
	if _, err := waitForMetric(ctx, t, metricsURL,
		`walera_subscribers_active{type="exact"}`,
		func(v float64) bool { return v >= 1 },
		3*time.Second, 50*time.Millisecond,
	); err != nil {
		t.Fatalf("subscriber never registered: %v; stderr:\n%s", err, h.Binary.Stderr())
	}

	baseline, _ := scrapeMetric(ctx, metricsURL, `walera_tx_dropped_total{reason="slow_consumer"}`)

	const bigNameSize = 900 * 1024
	const burst = 20
	bigName := strings.Repeat("x", bigNameSize)
	conn, err := pgx.Connect(ctx, h.PG.DSN)
	if err != nil {
		t.Fatalf("pg connect: %v; stderr:\n%s", err, h.Binary.Stderr())
	}
	defer conn.Close(ctx) //nolint:errcheck
	if _, err := conn.Exec(ctx, "SET synchronous_commit = off"); err != nil {
		t.Logf("set synchronous_commit: %v (continuing)", err)
	}
	if _, err := conn.Exec(ctx,
		"INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, name = EXCLUDED.name",
		42, "u42@x", "small",
	); err != nil {
		t.Fatalf("seed users/42: %v; stderr:\n%s", err, h.Binary.Stderr())
	}
	for i := 0; i < burst; i++ {
		if _, err := conn.Exec(ctx,
			"UPDATE users SET name = $1 WHERE id = $2",
			bigName, 42,
		); err != nil {

			t.Logf("big update #%d: %v (continuing)", i, err)
			break
		}
	}

	if _, err := waitForMetric(ctx, t, metricsURL,
		`walera_tx_dropped_total{reason="slow_consumer"}`,
		func(v float64) bool { return v > baseline },
		8*time.Second, 50*time.Millisecond,
	); err != nil {
		t.Fatalf("slow_consumer counter never bumped above baseline=%v within 8s: %v; stderr:\n%s",
			baseline, err, h.Binary.Stderr())
	}

	_ = readUntilErrorFrame(rawConn, br, "slow_consumer", 5*time.Second)
}
