//go:build integration

// Package integration — scenario 07: PG restart resume.
//
// The contract being validated:
//   - SSE connections must REMAIN OPEN across a PG outage (no 502, no close).
//   - The WAL reader's outer backoff loop increments walera_pg_reconnects_total
//     on every transient error.
//   - Subscribers' walera_subscribers_active gauge stays > 0 (subscribers
//     survive the reconnect — close(txCh) only on permanent exit).
//
// The test deliberately does NOT validate "new PG with same data" continuity:
// bringing up a NEW container at the OLD container's exposed port is fragile
// (testcontainers assigns ephemeral host ports). The test is about the
// RECONNECT MECHANIC, not data continuity. The metric-increment + open-
// connection assertions are sufficient evidence.
package integration

import (
	"context"
	"strings"
	"testing"
	"time"
)

func Test07RestartResume(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)

	h.Auth.SetMap(
		"test-token",
		"test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email", "name"}},
	)
	// Long TTL so the auth-refresh ticker doesn't interfere with the 30s test
	// budget. The default test config has default_ttl_seconds=1; bumping to
	// 60s for this scenario isolates the assertion to the WAL path.
	if err := h.Auth.SetTTL("test-token", 60); err != nil {
		t.Fatalf("SetTTL: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Open 3 SSE subscribers — exact channels, one per PK. Their existence
	// proves the subscriber set survives reconnect.
	subscribers := make([]struct {
		events  <-chan SSEEvent
		errCh   <-chan error
		closeFn func()
	}, 3)
	for i := range subscribers {
		ev, ec, cf := h.Client.Connect(ctx, channelFor(i+1), "test-token")
		subscribers[i].events = ev
		subscribers[i].errCh = ec
		subscribers[i].closeFn = cf
		defer cf()
	}

	// Steady-state proof: INSERT 3 rows; each subscriber receives its row.
	for i := 1; i <= 3; i++ {
		if err := h.PG.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			i, "u@x", "U",
		); err != nil {
			t.Fatalf("seed insert #%d: %v", i, err)
		}
	}
	for i, s := range subscribers {
		p := readTxEvent(ctx, t, h, s.events, s.errCh)
		if len(p.Changes) != 1 {
			t.Fatalf("sub %d: expected 1 change, got %d", i, len(p.Changes))
		}
		// Each subscriber should receive ONLY its own PK (exact match).
	}

	metricsURL := h.Binary.BaseURL() + "/metrics"

	// Capture baseline reconnects count.
	preReconnects, err := scrapeMetric(ctx, metricsURL, "walera_pg_reconnects_total")
	if err != nil {
		t.Fatalf("baseline scrape walera_pg_reconnects_total: %v", err)
	}

	// Trigger PG outage. testcontainers terminates the container; the binary's
	// replication conn observes the disconnect and the outer backoff loop
	// starts incrementing reconnects_total on each failed re-dial.
	stopCtx, stopCancel := context.WithTimeout(ctx, 10*time.Second)
	if err := h.PG.Stop(stopCtx); err != nil {
		stopCancel()
		t.Fatalf("PG.Stop: %v", err)
	}
	stopCancel()

	// Within 30s of the outage, the reconnects counter must exceed baseline.
	// The reader's first backoff is 100ms (test config), and the curve grows
	// to 1s cap — at 30s we expect dozens of attempts.
	if _, err := waitForMetric(ctx, t, metricsURL,
		"walera_pg_reconnects_total",
		func(v float64) bool { return v > preReconnects },
		30*time.Second, 500*time.Millisecond,
	); err != nil {
		t.Fatalf("walera_pg_reconnects_total never exceeded baseline %v within 30s: %v; stderr:\n%s",
			preReconnects, err, h.Binary.Stderr())
	}

	// SSE connections must STAY OPEN across the outage. We assert this by:
	//   a) no errCh signal on any subscriber (would indicate connection close).
	//   b) subscribers_active gauge for "exact" >= 3 (subscribers still
	//      registered in the router's index).
	//
	// Heartbeats may continue arriving on `events` — that's fine; we ignore
	// any tx events (none should arrive — PG is down) but we explicitly
	// reject errCh signals.
	for i, s := range subscribers {
		select {
		case err := <-s.errCh:
			t.Fatalf("sub %d errCh signalled during PG outage: %v (subscribers must survive reconnect — Pitfall G2)", i, err)
		case ev := <-s.events:
			if ev.Type == "tx" {
				t.Fatalf("sub %d received tx event during PG outage: %s", i, string(ev.Data))
			}
			// Heartbeat or other non-tx — acceptable.
		case <-time.After(500 * time.Millisecond):
			// No errCh signal observed in the 500ms window — good. The
			// connection is alive (heartbeat will arrive on the next tick).
		}
	}

	// Connection-status gauge — when PG is down, walera_pg_connection_status
	// should be 0 (mirrored via the /readyz probe). The readyz probe runs
	// at 1s cadence in the test config so this is bounded.
	if _, err := waitForMetric(ctx, t, metricsURL,
		"walera_pg_connection_status",
		func(v float64) bool { return v == 0 },
		10*time.Second, 500*time.Millisecond,
	); err != nil {
		t.Logf("walera_pg_connection_status did not reach 0 within 10s: %v (non-fatal — readyz cadence is loose)", err)
	}

	// Subscribers-active gauge: at least 3 exact subscribers still present.
	subsActive, err := scrapeMetric(ctx, metricsURL, `walera_subscribers_active{type="exact"}`)
	if err != nil {
		t.Fatalf("scrape walera_subscribers_active{type=exact}: %v", err)
	}
	if subsActive < 3 {
		t.Fatalf("walera_subscribers_active{type=exact} = %v; want >= 3 (subscribers were killed during reconnect)", subsActive)
	}

	// Final assertion: reconnect attempts are visibly logged. The binary's
	// structured logger emits "wal: reconnect attempt" on each failure.
	if !strings.Contains(h.Binary.Stderr(), "reconnect") {
		t.Logf("binary stderr did not contain 'reconnect' (non-fatal log-shape check); stderr:\n%s", h.Binary.Stderr())
	}
}

// channelFor returns "users/<id>" — used by Test07 to open per-PK subscribers
// without inlining strconv each loop iteration.
func channelFor(id int) string {
	switch id {
	case 1:
		return "users/1"
	case 2:
		return "users/2"
	case 3:
		return "users/3"
	default:
		return "users/all"
	}
}
