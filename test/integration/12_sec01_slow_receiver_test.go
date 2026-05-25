//go:build integration

// Package integration — scenario SEC-01: writer-level slow_consumer
// disconnect (SEC-01 / F-P1-01 regression coverage; TEST-09.1).
//
// Locks the per-frame SetWriteDeadline defence in
// internal/sse/writer.go:84-95. A client that opens an SSE connection on an
// EXACT channel (users/42) and stops reading triggers a kernel send-buffer
// stall; the writer's SetWriteDeadline fires at http.write_timeout,
// classifies the error as os.ErrDeadlineExceeded → "slow_consumer",
// increments walera_tx_dropped_total{reason="slow_consumer"}, and drops the
// subscriber.
//
// Distinguished from Test06SlowConsumer (router-level wildcard buffer
// overflow at users/all + wildcard_buffer=1): SEC-01 uses an EXACT
// subscription so the router-level fan-out drop path cannot fire — the
// writer-level path is the only signal. See RESEARCH.md §Pitfall 1.
//
// Deviation from plan (documented in 15-01-SUMMARY.md):
// The plan claimed prelude + heartbeats accumulate enough in the kernel
// send buffer to trip SetWriteDeadline=200ms on a non-reading client. On
// Linux loopback this is false — TCP autotune absorbs many MiB regardless
// of receive-side backpressure (same finding that drove
// Test06SlowConsumer to use wildcard_buffer=1 + big-tx burst). To force
// the writer-level deadline to fire deterministically, the test issues a
// sequence of near-cap UPDATE events that exceed the kernel send buffer
// capacity and exhaust the writer's per-frame 200ms deadline budget.
//
// Strategy (mirrors Test06's big-tx pattern at the WRITER level instead
// of the ROUTER level):
//  1. Permission map allows the `name` field so the long-name payload
//     is delivered in the SSE frame body.
//  2. Seed users/42 then issue a burst of UPDATE users SET name = (~900 KiB
//     blob) WHERE id = 42 transactions in quick succession. Each tx
//     produces one near-cap SSE frame (~900 KiB after JSON encoding).
//     With the client not reading, the writer's TCP w.Write of the
//     FIRST few frames already fills the kernel send buffer; the next
//     w.Write blocks beyond 200ms and SetWriteDeadline fires →
//     os.ErrDeadlineExceeded → drop with reason="slow_consumer".
//
// EXACT-subscription constraint preserved: only id=42 routes here, so
// the router-level wildcard fan-out drop path cannot fire. The writer
// drains the per-subscriber channel (exact_buffer=16) fast enough that
// the channel does not saturate before the writer's per-frame deadline
// trips — the writer-level path is the only signal.
//
// Burst sizing (WR-01 fix, 2026-05-18): the original 5×900 KiB ≈ 4.5 MiB
// payload assumed Linux defaults (wmem_max ≈ 4 MiB). Cloud / dev hosts
// routinely raise net.core.wmem_max to 16 MiB or higher, in which case
// 4.5 MiB fits comfortably in the kernel send buffer, the writer's
// w.Write never blocks past 200ms, and the test silently flakes with an
// opaque waitForMetric timeout. The burst is now sized to ≈ 18 MiB
// total payload (20 × 900 KiB), which overflows a 16 MiB send buffer
// with ~2 MiB headroom. Hosts with wmem_max > 18 MiB would still flake;
// every failure path below logs h.Binary.Stderr() so a future
// CI-platform change produces a diagnosable failure that points
// operators at kernel tuning rather than at the test.
//
// Wall-clock budget: ~10s ceiling (15s ctx with 5s tail for terminal
// frame read).
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
	// Short WriteTimeout per RESEARCH §"Common Pitfalls" Pitfall 2. 200ms
	// = 4× headroom over the writer_test.go 50ms unit-test pattern.
	h := NewHarness(t, WithWriteTimeout(200*time.Millisecond))

	// Whitelist includes `name` so the big-update payload reaches the
	// writer; without `name` the encoder strips it before the frame is
	// built and the frames would never approach the send-buffer cap.
	h.Auth.SetMap(
		"test-token",
		"test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email", "name"}},
	)

	// 20s ctx accommodates the WR-01 burst bump (5→20 events ≈ 18 MiB
	// pipeline work) plus subscriber-registration wait + 8s assertion
	// + 5s terminal-frame read tail. Original 15s budget was sized for
	// the pre-fix 5-event burst.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// EXACT subscription — NEVER users/all. Per RESEARCH §"Common Pitfalls"
	// Pitfall 1: a wildcard channel + wildcard_buffer=1 (default test
	// fixture) would let the router-level drop fire, producing the same
	// metric label "slow_consumer" from the WRONG code path. The exact
	// subscription excludes the router-level path entirely.
	rawConn := dialRawSSE(t, h.Binary.BaseURL(), "users/42", "test-token")
	defer rawConn.Close() //nolint:errcheck

	br := bufio.NewReaderSize(rawConn, 64*1024)
	if err := readHTTPResponseHeaders(br); err != nil {
		t.Fatalf("read response headers: %v; stderr:\n%s", err, h.Binary.Stderr())
	}

	// Confirm subscriber registration via /metrics — no socket reads, so
	// loopback TCP autotune does not grow the receive buffer.
	metricsURL := h.Binary.BaseURL() + "/metrics"
	if _, err := waitForMetric(ctx, t, metricsURL,
		`walera_subscribers_active{type="exact"}`,
		func(v float64) bool { return v >= 1 },
		3*time.Second, 50*time.Millisecond,
	); err != nil {
		t.Fatalf("subscriber never registered: %v; stderr:\n%s", err, h.Binary.Stderr())
	}

	// Capture the pre-test counter value.
	baseline, _ := scrapeMetric(ctx, metricsURL, `walera_tx_dropped_total{reason="slow_consumer"}`)

	// Issue a sequence of near-cap UPDATEs on users/42. Each tx produces
	// one ~900 KiB SSE frame. We send 20 in quick succession; the kernel
	// send buffer (wmem_default ~212992 B; wmem_max ~4 MiB on stock
	// Linux, raised to 16 MiB+ on tuned cloud / dev hosts) fills with
	// the client not reading, the writer's w.Write blocks, and the
	// 200ms deadline trips. Total payload ≈ 18 MiB — overflows a
	// 16 MiB wmem_max with ~2 MiB headroom (see header comment for the
	// WR-01 rationale).
	const bigNameSize = 900 * 1024 // 900 KiB — well under max_payload_bytes=1 MiB
	const burst = 20               // 20 × 900 KiB ≈ 18 MiB — exceeds raised Linux wmem_max (16 MiB)
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
			// A late UPDATE after the subscriber is killed can still
			// proceed (kill is subscriber-side); treat errors as
			// best-effort and continue — the assertion runs against
			// the metric regardless.
			t.Logf("big update #%d: %v (continuing)", i, err)
			break
		}
	}

	// Assertion budget: 8s — accommodates WAL-pipeline latency (a few
	// hundred ms on a freshly-booted PG container) PLUS the time to
	// stream ~18 MiB of WAL through the pgoutput decoder and into the
	// writer's TCP buffer, PLUS the per-frame 200ms write deadline that
	// trips once the send buffer is full. The WR-01 burst bump from 5
	// to 20 events grew the pipeline work proportionally; 8s preserves
	// the original ~2× safety factor over the formal SC of
	// write_timeout + 1s. On a host where the kernel send buffer is
	// large enough to absorb the full 18 MiB the writer's deadline
	// never trips and this times out — the stderr in the failure
	// message points operators at net.core.wmem_max.
	if _, err := waitForMetric(ctx, t, metricsURL,
		`walera_tx_dropped_total{reason="slow_consumer"}`,
		func(v float64) bool { return v > baseline },
		8*time.Second, 50*time.Millisecond,
	); err != nil {
		t.Fatalf("slow_consumer counter never bumped above baseline=%v within 8s: %v; stderr:\n%s",
			baseline, err, h.Binary.Stderr())
	}

	// Best-effort terminal-frame read. Per CONTEXT.md §decisions item 2:
	// "The raw conn either closes or emits a terminal error frame —
	// both are acceptable; the writer's Done arm has a 50 ms best-
	// effort window." We discard the error — both outcomes satisfy
	// SC #1.
	_ = readUntilErrorFrame(rawConn, br, "slow_consumer", 5*time.Second)
}
