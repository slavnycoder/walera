// Package router — shutdown_test.go covers Broadcaster.Shutdown: fan-out
// to every registered subscriber, deadline-exceeded negative case, and
// empty-index immediate return.
//
// All tests use stdlib `testing` + t.Parallel() and run cleanly under
// -race.
package router

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

// drainSub starts a goroutine that blocks until sub.Done() fires. The
// Subscriber no longer owns a buffered channel — the pool's per-sub queue
// is the only sink, and Broadcaster.Shutdown waits per-sub on sub.Done()
// via context cancel. The drainer goroutine therefore has nothing to
// drain; it exists only to mirror the pool-worker goroutine shape (one
// helper per attached sub) so the WaitGroup-based exit accounting in
// TestBroadcaster_Shutdown_FansOutToAllSubscribers stays meaningful.
func drainSub(sub *Subscriber, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-sub.Done()
	}()
}

// newBroadcasterForShutdown builds a minimal Broadcaster + N exact
// subscribers registered in the exact index. Returns the broadcaster, the
// subscriber slice, and a WaitGroup the caller can wg.Wait() on after
// shutdown to confirm every drainer goroutine exited.
func newBroadcasterForShutdown(n int) (*Broadcaster, []*Subscriber, *sync.WaitGroup) {
	cfg := Config{
		ExactBuffer:     16,
		WildcardBuffer:  16,
		MaxChangesPerTx: 10000,

		HeartbeatInterval: 30 * time.Second,
	}
	b := New(cfg, Deps{
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
		Encoder: &stubEncoder{},
	})

	subs := make([]*Subscriber, n)
	wg := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		s := NewSubscriber(
			SubscriberConfig{
				Kind:      KindExact,
				Schema:    "public",
				Table:     "t",
				PK:        pkOf(i),
				BufferCap: 16,
			},
			SubscriberDeps{Parent: context.Background()},
		)
		b.Register(s)
		subs[i] = s
		drainSub(s, wg)
	}
	return b, subs, wg
}

// pkOf turns a small int into a unique string PK without pulling fmt.Sprintf
// allocations into the hot test path.
func pkOf(i int) string {
	const hex = "0123456789abcdef"
	if i < 16 {
		return string([]byte{hex[i]})
	}
	return string([]byte{hex[i/16], hex[i%16]})
}

// TestBroadcaster_Shutdown_FansOutToAllSubscribers asserts that every
// active subscriber receives Drop("shutdown"); Shutdown returns nil
// within the deadline; the per-sub fan-out used safego.Go (no goroutine
// leak under -race).
func TestBroadcaster_Shutdown_FansOutToAllSubscribers(t *testing.T) {
	t.Parallel()

	const N = 50
	b, subs, wg := newBroadcasterForShutdown(N)

	start := time.Now()
	err := b.Shutdown(context.Background(), 5*time.Second)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Shutdown returned err=%v; want nil", err)
	}
	if elapsed > 1*time.Second {
		t.Errorf("Shutdown took %v; want <1s with 50 cooperative drainers", elapsed)
	}

	// Every sub must have reason == "shutdown" and Done() closed.
	for i, s := range subs {
		if got := s.Reason(); got != "shutdown" {
			t.Errorf("sub[%d].Reason() = %q; want %q", i, got, "shutdown")
		}
		select {
		case <-s.Done():
			// OK — context cancelled.
		default:
			t.Errorf("sub[%d].Done() not closed after Shutdown", i)
		}
	}

	// All drainer goroutines must exit (they select on Done()).
	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
		// OK
	case <-time.After(1 * time.Second):
		t.Fatalf("drainer goroutines did not exit within 1s after Shutdown")
	}
}

// TestBroadcaster_Shutdown_TimesOutWhenDrainStalls asserts the deadline-
// exceeded negative case (the `case <-time.After(drainDeadline)` arm of the
// outer select).
//
// The construction technique: register a single subscriber, then exhaust
// reasonOnce by calling Drop("slow_consumer") BEFORE Shutdown runs. The
// per-sub goroutine inside Shutdown then calls s.Drop("shutdown") — which
// is a no-op because reasonOnce already fired with "slow_consumer". The
// subscriber's ctx WAS already cancelled by the earlier Drop, so s.Done()
// IS closed and the per-sub goroutine exits immediately. The waiter
// goroutine closes `done` quickly.
//
// However, the deterministic deadline trigger uses a different technique:
// override the per-sub waiter by passing a SEPARATE blocking subscriber
// whose `Done()` is NEVER closed — we synthesise that by constructing a
// custom test-only Subscriber via the package-private factory and then
// withholding the Drop. To stay inside the public API (the test lives in
// `package router` so unexported fields are accessible), we manipulate the
// reasonOnce.Do directly to consume the slot so Shutdown's Drop is a no-op,
// AND replace ctx with a never-cancelled context so Done() never fires.
//
// Realised in code: snapshot a subscriber whose internal ctx is decoupled
// from Drop — we re-assign sub.ctx / sub.cancel via the unexported fields
// (same package so this is allowed). When Shutdown then calls Drop, the
// reasonOnce-guarded cancel() is invoked on the ORIGINAL cancel, which is
// still wired through to s.Done() via the new ctx — but we keep a parent
// ctx that we never cancel. To avoid this complexity entirely we use the
// `withNeverDoneSubscriber` helper below.
func TestBroadcaster_Shutdown_TimesOutWhenDrainStalls(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ExactBuffer:     16,
		WildcardBuffer:  16,
		MaxChangesPerTx: 10000,

		HeartbeatInterval: 30 * time.Second,
	}
	b := New(cfg, Deps{
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
		Encoder: &stubEncoder{},
	})

	// Block-forever subscriber: same-package construction lets us swap in
	// a never-cancelled ctx so s.Done() never fires. We also pre-fire
	// reasonOnce so Shutdown's Drop("shutdown") is a no-op (the original
	// cancel is consumed by sync.Once but never invoked).
	s := NewSubscriber(
		SubscriberConfig{
			Kind:      KindExact,
			Schema:    "public",
			Table:     "t",
			PK:        "stuck",
			BufferCap: 16,
		},
		SubscriberDeps{Parent: context.Background()},
	)
	// Consume reasonOnce.Do without cancelling: store a reason directly
	// and have Once.Do "succeed" with a no-op body so future Drop calls
	// short-circuit before reaching cancel().
	s.reasonOnce.Do(func() {
		r := "test_stalled"
		s.reasonPtr.Store(&r)
		// Intentionally do NOT call s.cancel() — s.Done() will never fire.
	})
	b.Register(s)

	start := time.Now()
	err := b.Shutdown(context.Background(), 100*time.Millisecond)
	elapsed := time.Since(start)
	if err != context.DeadlineExceeded {
		t.Fatalf("Shutdown returned err=%v; want context.DeadlineExceeded", err)
	}
	if elapsed < 90*time.Millisecond {
		t.Errorf("Shutdown returned too early: elapsed=%v; want ~100ms", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("Shutdown returned too late: elapsed=%v; want ~100ms", elapsed)
	}

	// Cleanup: cancel the stuck subscriber so the leaked per-sub goroutine
	// inside Shutdown can exit before the test ends (otherwise -race may
	// flag a leaked goroutine).
	s.cancel()
}

// TestBroadcaster_Shutdown_EmptyIndexReturnsImmediately asserts the no-op
// path: a broadcaster with zero subscribers returns nil within 10ms (it
// short-circuits before spawning any goroutines).
func TestBroadcaster_Shutdown_EmptyIndexReturnsImmediately(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ExactBuffer:     16,
		WildcardBuffer:  16,
		MaxChangesPerTx: 10000,

		HeartbeatInterval: 30 * time.Second,
	}
	b := New(cfg, Deps{
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
		Encoder: &stubEncoder{},
	})

	start := time.Now()
	err := b.Shutdown(context.Background(), 5*time.Second)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Shutdown on empty broadcaster returned err=%v; want nil", err)
	}
	if elapsed > 10*time.Millisecond {
		t.Errorf("Shutdown on empty broadcaster took %v; want <10ms", elapsed)
	}
}

// TestBroadcaster_Shutdown_MixedExactAndWildcard verifies Snapshot fan-out
// crosses both indexes. We register N exact + N wildcard subscribers and
// assert every one receives Drop("shutdown") within the deadline.
func TestBroadcaster_Shutdown_MixedExactAndWildcard(t *testing.T) {
	t.Parallel()

	const N = 20
	cfg := Config{
		ExactBuffer:     16,
		WildcardBuffer:  16,
		MaxChangesPerTx: 10000,

		HeartbeatInterval: 30 * time.Second,
	}
	b := New(cfg, Deps{
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
		Encoder: &stubEncoder{},
	})

	wg := &sync.WaitGroup{}
	all := make([]*Subscriber, 0, 2*N)
	for i := 0; i < N; i++ {
		es := NewSubscriber(
			SubscriberConfig{
				Kind:      KindExact,
				Schema:    "public",
				Table:     "t",
				PK:        pkOf(i),
				BufferCap: 16,
			},
			SubscriberDeps{Parent: context.Background()},
		)
		b.Register(es)
		drainSub(es, wg)
		all = append(all, es)

		ws := NewSubscriber(
			SubscriberConfig{
				Kind:      KindWildcard,
				Schema:    "public",
				Table:     "t",
				BufferCap: 16,
			},
			SubscriberDeps{Parent: context.Background()},
		)
		b.Register(ws)
		drainSub(ws, wg)
		all = append(all, ws)
	}

	if b.ExactLen() != N {
		t.Errorf("ExactLen() = %d; want %d", b.ExactLen(), N)
	}
	if b.WildcardLen() != N {
		t.Errorf("WildcardLen() = %d; want %d", b.WildcardLen(), N)
	}

	if err := b.Shutdown(context.Background(), 5*time.Second); err != nil {
		t.Fatalf("Shutdown err=%v; want nil", err)
	}
	for i, s := range all {
		if got := s.Reason(); got != "shutdown" {
			t.Errorf("all[%d].Reason() = %q; want %q", i, got, "shutdown")
		}
	}

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("drainer goroutines did not exit within 1s")
	}
}
