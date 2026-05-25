package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
)

func newRegistrySubscriber(t *testing.T, _ string) (*Subscriber, *atomic.Int64, *httptest.Server) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
	}))
	t.Cleanup(srv.Close)
	c, _ := newSubTestClient(t, srv.URL)
	rsub := router.NewSubscriber(
		router.SubscriberConfig{
			Kind:      router.KindExact,
			Schema:    "public",
			Table:     "orders",
			PK:        "1",
			BufferCap: 4,
		},
		router.SubscriberDeps{Parent: context.Background()},
	)
	s := NewSubscriber(
		SubscriberConfig{
			InitialMap: makeMap(t, 100, "id"),
			Channel:    "public.orders:1",
			Token:      "test-token",
			DefaultTTL: 60 * time.Second,
		},
		SubscriberDeps{
			Sub:     rsub,
			Client:  c,
			Metrics: metrics.New(),
		},
	)
	return s, &hits, srv
}

func newTestRegistry(_ *testing.T) *Subscribers {
	return NewSubscribers(SubscribersDeps{
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
	})
}

func TestRegistry_AddAndLen(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)

	if got := r.Len(); got != 0 {
		t.Fatalf("empty Len: got %d; want 0", got)
	}

	s1, _, _ := newRegistrySubscriber(t, "")
	s2, _, _ := newRegistrySubscriber(t, "")

	r.Add(s1)
	if got := r.Len(); got != 1 {
		t.Fatalf("Len after first Add: got %d; want 1", got)
	}
	r.Add(s2)
	if got := r.Len(); got != 2 {
		t.Fatalf("Len after second Add: got %d; want 2", got)
	}
	r.Remove(s1.ID())
	if got := r.Len(); got != 1 {
		t.Fatalf("Len after Remove: got %d; want 1", got)
	}
}

func TestRegistry_RemoveAbsentIDNoOp(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)

	r.Remove("nonexistent")
	if got := r.Len(); got != 0 {
		t.Fatalf("Len after no-op Remove: got %d; want 0", got)
	}
}

func TestRegistry_AddDuplicateIDOverwrites(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	s1, _, _ := newRegistrySubscriber(t, "")

	s2 := &Subscriber{
		Sub:    s1.Sub,
		client: s1.client,
	}
	s2.AuthMap.Store(makeMap(t, 100, "id"))

	r.Add(s1)
	r.Add(s2)
	if got := r.Len(); got != 1 {
		t.Fatalf("Len after duplicate Add: got %d; want 1", got)
	}

	r.mu.Lock()
	stored := r.subs[s1.ID()]
	r.mu.Unlock()
	if stored != s2 {
		t.Fatalf("stored sub: got %p; want %p (s2)", stored, s2)
	}
}

func TestRegistry_FanoutStaleRefreshRespectsTTL(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)

	sStale, hitsStale, _ := newRegistrySubscriber(t, "")
	sFresh, hitsFresh, _ := newRegistrySubscriber(t, "")

	sStale.lastRefresh.Store(time.Now().Add(-10 * time.Minute).UnixNano())

	r.Add(sStale)
	r.Add(sFresh)

	r.fanoutStaleRefreshes(0, 60)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if hitsStale.Load() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := hitsStale.Load(); got < 1 {
		t.Fatalf("stale sub hits: got %d; want >= 1", got)
	}
	if got := hitsFresh.Load(); got != 0 {
		t.Fatalf("fresh sub hits: got %d; want 0", got)
	}
}

func TestRegistry_FanoutJitterScheduling(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)

	const N = 10
	hits := make([]*atomic.Int64, N)
	for i := 0; i < N; i++ {
		s, h, _ := newRegistrySubscriber(t, "")

		s.lastRefresh.Store(time.Now().Add(-time.Hour).UnixNano())
		hits[i] = h
		r.Add(s)
	}

	r.fanoutStaleRefreshes(50*time.Millisecond, 60)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		done := true
		for i := 0; i < N; i++ {
			if hits[i].Load() < 1 {
				done = false
				break
			}
		}
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	for i := 0; i < N; i++ {
		if got := hits[i].Load(); got < 1 {
			t.Fatalf("sub %d hits: got %d; want >= 1", i, got)
		}
	}
}

func TestRegistry_WatchBreakerExitsOnContextCancel(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)

	bk := NewBreaker(BreakerConfig{
		WindowBuckets:        30,
		BucketSeconds:        1,
		FailureRateThreshold: 0.5,
		DebounceFloor:        20,
		Cooldown:             30 * time.Second,
		StaleRefreshJitter:   5 * time.Second,
	}, BreakerDeps{
		Prober:  proberFunc(func(_ context.Context) error { return nil }),
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.WatchBreaker(ctx, bk, 0, 60)
	}()

	cancel()
	select {
	case <-done:

	case <-time.After(200 * time.Millisecond):
		t.Fatalf("WatchBreaker did not exit within 200ms of ctx cancel")
	}
}

func TestRegistry_WatchBreakerCycles(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)

	probe := func(_ context.Context) error { return nil }

	var nowNanos atomic.Int64
	nowNanos.Store(time.Unix(0, 0).UnixNano())
	clock := func() time.Time { return time.Unix(0, nowNanos.Load()) }
	cfg := BreakerConfig{
		WindowBuckets:        30,
		BucketSeconds:        1,
		FailureRateThreshold: 0.5,
		DebounceFloor:        20,
		Cooldown:             30 * time.Second,
		StaleRefreshJitter:   5 * time.Second,
	}
	bk := NewBreaker(cfg, BreakerDeps{
		Prober:  proberFunc(probe),
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
	})
	bk.SetClockForTest(clock, time.Millisecond)

	s, hits, _ := newRegistrySubscriber(t, "")
	s.lastRefresh.Store(time.Now().Add(-time.Hour).UnixNano())
	r.Add(s)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		r.WatchBreaker(ctx, bk, 0, 60)
	}()

	const (
		pollInterval   = 10 * time.Millisecond
		pollDeadline   = 2 * time.Second
		startupBarrier = 100 * time.Millisecond
	)

	time.Sleep(startupBarrier)

	driveOneCloseCycle := func() State {
		for i := 0; i < 30; i++ {
			bk.window.tick()
		}
		for i := 0; i < 21; i++ {
			bk.RecordResult(false)
		}
		nowNanos.Add(int64(cfg.Cooldown) + int64(time.Second))
		bk.tickFSM(ctx)
		return bk.State()
	}

	const Cycles = 3
	for cycle := 0; cycle < Cycles; cycle++ {
		wantHits := int64(cycle + 1)
		deadline := time.Now().Add(pollDeadline)
		for hits.Load() < wantHits {
			if time.Now().After(deadline) {
				t.Fatalf("cycle %d: hits=%d; want >= %d (after %v)", cycle, hits.Load(), wantHits, pollDeadline)
			}

			s.lastRefresh.Store(time.Now().Add(-time.Hour).UnixNano())
			if st := driveOneCloseCycle(); st != StateClosed {
				t.Fatalf("cycle %d: breaker did not close: state=%v", cycle, st)
			}
			time.Sleep(pollInterval)
		}
	}

	cancel()
	select {
	case <-watcherDone:
	case <-time.After(pollDeadline):
		t.Fatalf("WatchBreaker did not exit within %v of cancel", pollDeadline)
	}
}
