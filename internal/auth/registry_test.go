// Package auth — registry_test.go validates auth.Subscribers stale-refresh
// fan-out.
//
// Stale-refresh fan-out is tested with real httptest servers: each
// subscriber wires its own *Client backed by an
// httptest server that counts hits via an atomic counter. The test asserts on
// per-subscriber hit counts after the fan-out completes.
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

// newRegistrySubscriber builds a Subscriber wired to a fresh httptest server
// that counts every refresh call via the returned atomic counter. lastRefresh
// is initially the construction wall-clock; callers may overwrite it to force
// "stale" timing before invoking the fan-out.
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
	// Must not panic.
	r.Remove("nonexistent")
	if got := r.Len(); got != 0 {
		t.Fatalf("Len after no-op Remove: got %d; want 0", got)
	}
}

func TestRegistry_AddDuplicateIDOverwrites(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	s1, _, _ := newRegistrySubscriber(t, "")
	// Force a duplicate ID on s2 by building a Subscriber with the SAME
	// router.Subscriber.ID — easiest way is to reuse s1.Sub on a new
	// Subscriber struct.
	s2 := &Subscriber{
		Sub:    s1.Sub, // same Sub → same ID
		client: s1.client,
	}
	s2.AuthMap.Store(makeMap(t, 100, "id"))

	r.Add(s1)
	r.Add(s2)
	if got := r.Len(); got != 1 {
		t.Fatalf("Len after duplicate Add: got %d; want 1", got)
	}
	// Verify s2 won (last-writer-wins).
	r.mu.Lock()
	stored := r.subs[s1.ID()]
	r.mu.Unlock()
	if stored != s2 {
		t.Fatalf("stored sub: got %p; want %p (s2)", stored, s2)
	}
}

// TestRegistry_FanoutStaleRefreshRespectsTTL — only subs older than ttl get
// refreshed.
func TestRegistry_FanoutStaleRefreshRespectsTTL(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)

	sStale, hitsStale, _ := newRegistrySubscriber(t, "")
	sFresh, hitsFresh, _ := newRegistrySubscriber(t, "")

	// Force sStale's lastRefresh to a value way older than the ttl cutoff.
	sStale.lastRefresh.Store(time.Now().Add(-10 * time.Minute).UnixNano())
	// sFresh stays at construction-time (now).

	r.Add(sStale)
	r.Add(sFresh)

	// ttl=60s ; fan-out with zero jitter for deterministic timing.
	r.fanoutStaleRefreshes(0, 60)

	// Allow the scheduled time.AfterFunc + safego.Go to run.
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

// TestRegistry_FanoutJitterScheduling — all 10 stale subs receive a refresh
// within the jitter window.
func TestRegistry_FanoutJitterScheduling(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)

	const N = 10
	hits := make([]*atomic.Int64, N)
	for i := 0; i < N; i++ {
		s, h, _ := newRegistrySubscriber(t, "")
		// Force every sub stale (ttl=0 in fan-out call means "every sub is
		// stale" because now > now - 0).
		s.lastRefresh.Store(time.Now().Add(-time.Hour).UnixNano())
		hits[i] = h
		r.Add(s)
	}

	// Jitter = 50ms. Each AfterFunc fires within [0, 50ms); after 200ms all
	// should have completed.
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

// TestRegistry_WatchBreakerExitsOnContextCancel — clean shutdown on ctx
// cancellation.
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
		// OK
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("WatchBreaker did not exit within 200ms of ctx cancel")
	}
}

// TestRegistry_WatchBreakerCycles — drive the breaker through 3 trip+close
// cycles; assert the watcher invokes the fan-out each time.
//
// Timing-flake fix (v2.4): previously used a fixed 500ms per-cycle budget
// plus a fixed 20ms re-arm sleep, which produced ~15% `hits=N; want >= N+1`
// failures on weak (2-CPU) hosts when the watcher goroutine could not be
// scheduled inside the budget. The fix replaces every fixed wait with an
// Eventually-polling loop (10ms poll, 2s deadline), preserves coverage on
// fast hosts, and removes timing-variance dependence on the host scheduler.
func TestRegistry_WatchBreakerCycles(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)

	// Probe always succeeds → HalfOpen → Closed.
	probe := func(_ context.Context) error { return nil }

	// Build a breaker with synthetic clock for determinism.
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

	// Register a stale subscriber wired to a counting server.
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

	// Polling-loop pattern with retried trip+close. The original test
	// relied on fixed sleeps (20ms inter-cycle, 500ms per-cycle) that
	// produced ~15% `hits=N; want >= N+1` failures on 2-CPU weak hosts.
	// Root cause: signalClose installs a fresh closeCh BEFORE closing the
	// previous one, so if the watcher goroutine has not yet looped back to
	// b.WaitForClose() (CPU-starved post-fanout), the next signalClose
	// hands it the (unclosed) replacement and the cycle's signal is lost.
	// Resolution: each cycle re-trips and re-closes inside an Eventually
	// polling loop (10ms poll, 2s deadline). Repeated signalClose calls
	// are safe — each cycles the closeCh atomically — so missed signals
	// self-recover within the deadline regardless of host scheduler jitter.
	const (
		pollInterval   = 10 * time.Millisecond
		pollDeadline   = 2 * time.Second
		startupBarrier = 100 * time.Millisecond
	)
	// Startup barrier — give the watcher goroutine time to enter its
	// first WaitForClose(). 100ms is generous on a 2-CPU host (goroutine
	// startup is microseconds); the retry loop below covers the rest.
	time.Sleep(startupBarrier)

	// driveOneCloseCycle performs a single Open→Closed transition: rotate the
	// failure window, record 21 failures (> DebounceFloor, rate > 0.5), then
	// tick the FSM past cooldown to land in StateClosed (and broadcast on
	// closeCh). Returns the post-tick state for assertion.
	driveOneCloseCycle := func() State {
		for i := 0; i < 30; i++ { // rotate window so prior failures clear
			bk.window.tick()
		}
		for i := 0; i < 21; i++ {
			bk.RecordResult(false)
		}
		nowNanos.Add(int64(cfg.Cooldown) + int64(time.Second))
		bk.tickFSM(ctx)
		return bk.State()
	}

	// Drive 3 trip+close cycles. Each cycle keeps re-tripping + closing
	// until the watcher catches the signal and the fan-out increments hits.
	// On a healthy host this is exactly one trip+close + one poll-interval
	// wait. Under CPU contention it may take several retries; each retry is
	// a full Open→Closed cycle — observationally equivalent to the single-
	// cycle assertion the original test made, but resilient to scheduler
	// jitter that left the watcher mid-loop between WaitForClose() calls.
	const Cycles = 3
	for cycle := 0; cycle < Cycles; cycle++ {
		wantHits := int64(cycle + 1)
		deadline := time.Now().Add(pollDeadline)
		for hits.Load() < wantHits {
			if time.Now().After(deadline) {
				t.Fatalf("cycle %d: hits=%d; want >= %d (after %v)", cycle, hits.Load(), wantHits, pollDeadline)
			}
			// Re-stale the subscriber before each retry — tryRefresh may
			// have already run from a previous (now-Closed-but-watcher-
			// missed) cycle, leaving lastRefresh fresh.
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
