//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"
)

func Test10WildcardFanout(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type wsub struct {
		idx     int
		events  <-chan SSEEvent
		errCh   <-chan error
		closeFn func()
	}
	const N = 3
	subs := make([]wsub, N)
	for i := range subs {
		ev, ec, cf := h.Client.Connect(ctx, "users/all", "test-token")
		subs[i] = wsub{idx: i, events: ev, errCh: ec, closeFn: cf}
		defer cf()
	}

	time.Sleep(250 * time.Millisecond)

	if err := h.PG.Exec(ctx,
		"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
		42, "x@y.z", "X",
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var wg sync.WaitGroup
	failures := make(chan string, N)
	for _, s := range subs {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case ev, ok := <-s.events:
					if !ok {
						failures <- "sub " + itoa(s.idx) + ": events closed before tx event"
						return
					}
					if ev.Type != "tx" {
						continue
					}
					p := decodeTxPayload(t, ev.Data)
					if len(p.Changes) != 1 || p.Changes[0].PK != "42" {
						failures <- "sub " + itoa(s.idx) + ": unexpected payload " + string(ev.Data)
						return
					}
					return
				case err := <-s.errCh:
					failures <- "sub " + itoa(s.idx) + ": errCh: " + err.Error()
					return
				case <-ctx.Done():
					failures <- "sub " + itoa(s.idx) + ": ctx done"
					return
				}
			}
			failures <- "sub " + itoa(s.idx) + ": timeout waiting for tx event"
		}()
	}
	wg.Wait()
	close(failures)
	var failed []string
	for f := range failures {
		failed = append(failed, f)
	}
	if len(failed) > 0 {
		t.Fatalf("wildcard fan-out failures: %v; stderr:\n%s", failed, h.Binary.Stderr())
	}

	metricsURL := h.Binary.BaseURL() + "/metrics"
	v, err := scrapeMetric(ctx, metricsURL, `walera_subscribers_active{type="wildcard"}`)
	if err != nil {
		t.Fatalf("scrape walera_subscribers_active{type=wildcard}: %v", err)
	}
	if v < float64(N) {
		t.Fatalf("walera_subscribers_active{type=wildcard} = %v; want >= %d", v, N)
	}
}
