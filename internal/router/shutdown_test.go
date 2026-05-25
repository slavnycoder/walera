package router

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

func drainSub(sub *Subscriber, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-sub.Done()
	}()
}

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

func pkOf(i int) string {
	const hex = "0123456789abcdef"
	if i < 16 {
		return string([]byte{hex[i]})
	}
	return string([]byte{hex[i/16], hex[i%16]})
}

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

	for i, s := range subs {
		if got := s.Reason(); got != "shutdown" {
			t.Errorf("sub[%d].Reason() = %q; want %q", i, got, "shutdown")
		}
		select {
		case <-s.Done():

		default:
			t.Errorf("sub[%d].Done() not closed after Shutdown", i)
		}
	}

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:

	case <-time.After(1 * time.Second):
		t.Fatalf("drainer goroutines did not exit within 1s after Shutdown")
	}
}

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

	s.reasonOnce.Do(func() {
		r := "test_stalled"
		s.reasonPtr.Store(&r)

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

	s.cancel()
}

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
