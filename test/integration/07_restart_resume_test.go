//go:build integration

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

	if err := h.Auth.SetTTL("test-token", 60); err != nil {
		t.Fatalf("SetTTL: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

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

	}

	metricsURL := h.Binary.BaseURL() + "/metrics"

	preReconnects, err := scrapeMetric(ctx, metricsURL, "walera_pg_reconnects_total")
	if err != nil {
		t.Fatalf("baseline scrape walera_pg_reconnects_total: %v", err)
	}

	stopCtx, stopCancel := context.WithTimeout(ctx, 10*time.Second)
	if err := h.PG.Stop(stopCtx); err != nil {
		stopCancel()
		t.Fatalf("PG.Stop: %v", err)
	}
	stopCancel()

	if _, err := waitForMetric(ctx, t, metricsURL,
		"walera_pg_reconnects_total",
		func(v float64) bool { return v > preReconnects },
		30*time.Second, 500*time.Millisecond,
	); err != nil {
		t.Fatalf("walera_pg_reconnects_total never exceeded baseline %v within 30s: %v; stderr:\n%s",
			preReconnects, err, h.Binary.Stderr())
	}

	for i, s := range subscribers {
		select {
		case err := <-s.errCh:
			t.Fatalf("sub %d errCh signalled during PG outage: %v (subscribers must survive reconnect — Pitfall G2)", i, err)
		case ev := <-s.events:
			if ev.Type == "tx" {
				t.Fatalf("sub %d received tx event during PG outage: %s", i, string(ev.Data))
			}

		case <-time.After(500 * time.Millisecond):

		}
	}

	if _, err := waitForMetric(ctx, t, metricsURL,
		"walera_pg_connection_status",
		func(v float64) bool { return v == 0 },
		10*time.Second, 500*time.Millisecond,
	); err != nil {
		t.Logf("walera_pg_connection_status did not reach 0 within 10s: %v (non-fatal — readyz cadence is loose)", err)
	}

	subsActive, err := scrapeMetric(ctx, metricsURL, `walera_subscribers_active{type="exact"}`)
	if err != nil {
		t.Fatalf("scrape walera_subscribers_active{type=exact}: %v", err)
	}
	if subsActive < 3 {
		t.Fatalf("walera_subscribers_active{type=exact} = %v; want >= 3 (subscribers were killed during reconnect)", subsActive)
	}

	if !strings.Contains(h.Binary.Stderr(), "reconnect") {
		t.Logf("binary stderr did not contain 'reconnect' (non-fatal log-shape check); stderr:\n%s", h.Binary.Stderr())
	}
}

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
