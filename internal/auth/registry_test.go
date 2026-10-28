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
			UserID:     "user-1",
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
