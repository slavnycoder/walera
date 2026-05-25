package router

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestIndex_AddLookupRemove(t *testing.T) {
	t.Parallel()
	idx := newIndex()
	sub := &Subscriber{id: "test"}
	const key = "public.users:42"

	if got := idx.Lookup(key); got != nil {
		t.Errorf("Lookup on empty index: got %v; want nil", got)
	}

	idx.Add(key, sub)
	if got := idx.Lookup(key); len(got) != 1 || got[0] != sub {
		t.Errorf("Lookup after Add: got %v; want [%p]", got, sub)
	}
	if got, want := idx.Len(), 1; got != want {
		t.Errorf("Len after Add: got %d; want %d", got, want)
	}

	idx.Remove(key, sub)
	if got := idx.Lookup(key); got != nil {
		t.Errorf("Lookup after Remove: got %v; want nil", got)
	}
	if got, want := idx.Len(), 0; got != want {
		t.Errorf("Len after Remove: got %d; want %d", got, want)
	}
}

func TestIndex_AddLookupRemove_MultipleSubscribers(t *testing.T) {
	t.Parallel()
	idx := newIndex()
	const key = "public.users:42"

	a := &Subscriber{id: "a"}
	b := &Subscriber{id: "b"}

	idx.Add(key, a)
	idx.Add(key, b)

	subs := idx.Lookup(key)
	if len(subs) != 2 || !containsSubscriber(subs, a) || !containsSubscriber(subs, b) {
		t.Fatalf("Lookup after two Adds: got %v; want both %p and %p", subs, a, b)
	}
	if got, want := idx.Len(), 2; got != want {
		t.Errorf("Len after two Adds: got %d; want %d", got, want)
	}

	subs[0] = nil
	if got := idx.Lookup(key); len(got) != 2 || !containsSubscriber(got, a) || !containsSubscriber(got, b) {
		t.Errorf("mutating Lookup result leaked into index: got %v", got)
	}

	idx.Remove(key, a)
	subs = idx.Lookup(key)
	if len(subs) != 1 || subs[0] != b {
		t.Errorf("Lookup after removing a: got %v; want [%p]", subs, b)
	}
	if got, want := idx.Len(), 1; got != want {
		t.Errorf("Len after removing a: got %d; want %d", got, want)
	}

	idx.Remove(key, a)
	if got, want := idx.Len(), 1; got != want {
		t.Errorf("Len after removing a twice: got %d; want %d", got, want)
	}

	idx.Remove(key, b)
	if got := idx.Lookup(key); got != nil {
		t.Errorf("Lookup after removing both: got %v; want nil", got)
	}
	if got, want := idx.Len(), 0; got != want {
		t.Errorf("Len after removing both: got %d; want %d", got, want)
	}
}

func containsSubscriber(subs []*Subscriber, want *Subscriber) bool {
	for _, sub := range subs {
		if sub == want {
			return true
		}
	}
	return false
}

func TestIndex_ShardDistribution(t *testing.T) {
	t.Parallel()
	idx := newIndex()
	const N = 1000
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("public.users:%d", i)
		idx.Add(key, &Subscriber{id: key})
	}
	seen := 0
	for i := range idx.shards {
		s := &idx.shards[i]
		s.mu.RLock()
		if len(s.subs) > 0 {
			seen++
		}
		s.mu.RUnlock()
	}
	if seen < numShards {
		t.Errorf("shard coverage: %d/%d shards have entries; want all populated", seen, numShards)
	}
}

func TestIndexConcurrent(t *testing.T) {
	t.Parallel()
	idx := newIndex()

	const (
		G = 256
		N = 1024
	)
	var wg sync.WaitGroup
	wg.Add(G)

	ctx := context.Background()

	for g := 0; g < G; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < N; i++ {
				key := fmt.Sprintf("public.users:%d", g*N+i)
				sub := NewSubscriber(
					SubscriberConfig{
						ID:        key,
						Kind:      KindExact,
						Schema:    "public",
						Table:     "users",
						PK:        fmt.Sprintf("%d", g*N+i),
						BufferCap: 1,
					},
					SubscriberDeps{Parent: ctx},
				)
				idx.Add(key, sub)
				if got := idx.Lookup(key); len(got) != 1 || got[0] != sub {
					t.Errorf("Lookup(%q) = %v; want [%p]", key, got, sub)
				}
				idx.Remove(key, sub)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:

	case <-time.After(30 * time.Second):
		t.Fatal("TestIndexConcurrent: timed out waiting for goroutines (30s)")
	}

	if got, want := idx.Len(), 0; got != want {
		t.Errorf("idx.Len() after all Removes: got %d; want %d", got, want)
	}
}
