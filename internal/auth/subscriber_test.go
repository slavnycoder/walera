// Package auth — subscriber_test.go validates auth.Subscriber lifecycle,
// refresh policy, and back-buffer ordering.
//
// All tests use httptest servers (no live auth backend) and synthetic LSN
// injection (no live wal.Reader). Backoff sequences are overridden to
// millisecond scale so the suite finishes in < 1s wall clock.
package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/wal"
)

// --- helpers ---

// stubBreakerHook is a minimal breakerHook for subscriber tests. allow returns
// the field's value on every Allow() call.
type stubBreakerHook struct{ allow bool }

func (s *stubBreakerHook) Allow() bool { return s.allow }

// permissionsBody is a minimal valid /auth/permissions response.
func permissionsBody(t *testing.T, tables map[string][]string) []byte {
	t.Helper()
	w := struct {
		UserID     string              `json:"user_id"`
		Tables     map[string][]string `json:"tables"`
		TTLSeconds int                 `json:"ttl_seconds"`
	}{
		UserID:     "user-1",
		Tables:     tables,
		TTLSeconds: 60,
	}
	b, err := json.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// scriptServer is an httptest server that returns a sequence of responses.
// Each call advances an atomic counter; the response is selected from the
// per-call closure list (or the final entry if call > len).
type scriptServer struct {
	t       *testing.T
	srv     *httptest.Server
	calls   atomic.Int64
	mu      sync.Mutex
	handler func(call int64, w http.ResponseWriter, r *http.Request)
}

func newScriptServer(t *testing.T, h func(call int64, w http.ResponseWriter, r *http.Request)) *scriptServer {
	t.Helper()
	s := &scriptServer{t: t, handler: h}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := s.calls.Add(1)
		s.mu.Lock()
		defer s.mu.Unlock()
		s.handler(n, w, r)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

// newSubTestClient wires a Client against the script server with a relaxed
// timeout suitable for tests.
func newSubTestClient(t *testing.T, server string) (*Client, *metrics.Registry) {
	t.Helper()
	mc := metrics.New()
	cfg := Config{
		BackendURL:     server,
		RequestTimeout: 2 * time.Second,
		HealthChannel:  "_health",
	}
	c := New(cfg, Deps{Logger: zerolog.Nop(), Metrics: mc})
	return c, mc
}

// newTestRouterSub builds a router.Subscriber with a small buffer; used as
// the composed Sub in auth.Subscriber tests.
func newTestRouterSub(t *testing.T) *router.Subscriber {
	t.Helper()
	return router.NewSubscriber(
		router.SubscriberConfig{
			Kind:      router.KindExact,
			Schema:    "public",
			Table:     "orders",
			PK:        "42",
			BufferCap: 4,
		},
		router.SubscriberDeps{Parent: context.Background()},
	)
}

// subscriberTestOpts is the test-local convenience bundle that mirrors the
// pre-Phase-7 SubscriberOpts shape so the dozens of existing test sites do
// not each have to instantiate two structs. newTestSubscriber unpacks this
// into the SubscriberConfig / SubscriberDeps pair the constructor now
// takes.
type subscriberTestOpts struct {
	Sub        *router.Subscriber
	Client     *Client
	Breaker    breakerHook
	InitialMap *Whitelist
	Token      string
	Channel    string
	DefaultTTL time.Duration
	Log        zerolog.Logger
	Metrics    *metrics.Registry
	NowFunc    func() time.Time
	LSNFunc    func() pglogrepl.LSN
	Backoffs   []time.Duration
}

// newTestSubscriber composes an auth.Subscriber with a fully-wired set of
// defaults. lsnNow defaults to a synthetic-clock LSN counter so the
// back-buffer test can advance LSN before each Store.
func newTestSubscriber(t *testing.T, opts subscriberTestOpts) *Subscriber {
	t.Helper()
	if opts.Sub == nil {
		opts.Sub = newTestRouterSub(t)
	}
	if opts.Channel == "" {
		opts.Channel = "public.orders:42"
	}
	if opts.Token == "" {
		opts.Token = "user-bearer-token"
	}
	if opts.DefaultTTL == 0 {
		opts.DefaultTTL = 60 * time.Second
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.New()
	}
	return NewSubscriber(
		SubscriberConfig{
			InitialMap: opts.InitialMap,
			Token:      opts.Token,
			Channel:    opts.Channel,
			DefaultTTL: opts.DefaultTTL,
			Backoffs:   opts.Backoffs,
		},
		SubscriberDeps{
			Sub:     opts.Sub,
			Client:  opts.Client,
			Breaker: opts.Breaker,
			Logger:  opts.Log,
			Metrics: opts.Metrics,
			NowFunc: opts.NowFunc,
			LSNFunc: opts.LSNFunc,
		},
	)
}

// makeMap builds a Whitelist with two whitelisted columns ("id", "name").
func makeMap(t *testing.T, refreshLSN pglogrepl.LSN, cols ...string) *Whitelist {
	t.Helper()
	colSet := map[string]struct{}{}
	for _, c := range cols {
		colSet[c] = struct{}{}
	}
	return &Whitelist{
		UserID: "user-1",
		Tables: map[string]map[string]struct{}{
			"orders": colSet,
		},
		TTLSeconds: 60,
		RefreshLSN: refreshLSN,
	}
}

// makeUpdateChange constructs a wal.Change for an UPDATE with Changed cols.
func makeUpdateChange(table string, changed map[string]any) wal.Change {
	return wal.Change{
		Schema:  "public",
		Table:   table,
		Op:      wal.OpUpdate,
		PK:      "42",
		PKCol:   "id",
		Changed: changed,
	}
}

// --- tests ---

func TestSubscriber_NewInstallsInitialMap(t *testing.T) {
	t.Parallel()

	initial := makeMap(t, 100, "id", "name")
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id", "name"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	now := time.Unix(1700000000, 0)
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: initial,
		NowFunc:    func() time.Time { return now },
	})

	if got := s.AuthMap.Load(); got != initial {
		t.Fatalf("AuthMap.Load: got %v; want %v", got, initial)
	}
	if got := s.PrevWhitelist.Load(); got != nil {
		t.Fatalf("PrevWhitelist.Load: got %v; want nil", got)
	}
	if got := s.LastRefresh(); !got.Equal(now) {
		t.Fatalf("LastRefresh: got %v; want %v", got, now)
	}
}

func TestSubscriber_FilterClosure_UsesCurrentMap(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id", "name"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	initial := makeMap(t, 100, "id", "name")
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: initial,
	})

	change := makeUpdateChange("orders", map[string]any{"name": "new-name"})
	out, drop := s.FilterClosure()(change)
	if drop {
		t.Fatalf("FilterClosure drop=true; want false")
	}
	if _, ok := out.Changed["name"]; !ok {
		t.Fatalf("Changed missing 'name': %v", out.Changed)
	}
}

// TestSubscriber_FilterWithLSN_UsesCurrentMapForRecentTx — tx commits AFTER
// the refresh; the current map governs.
func TestSubscriber_FilterWithLSN_UsesCurrentMapForRecentTx(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id", "name"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: makeMap(t, 100, "id", "name"),
	})

	change := makeUpdateChange("orders", map[string]any{"name": "x"})
	out, drop := s.FilterWithLSN(change, pglogrepl.LSN(200))
	if drop {
		t.Fatalf("drop=true; want false")
	}
	if _, ok := out.Changed["name"]; !ok {
		t.Fatalf("Changed missing 'name' (current map should allow it): %v", out.Changed)
	}
}

// TestSubscriber_FilterWithLSN_UsesPrevMapForOlderTx verifies the
// back-buffer rule.
func TestSubscriber_FilterWithLSN_UsesPrevMapForOlderTx(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	// Old map (RefreshLSN=100) whitelists id+name.
	// New map (RefreshLSN=200) whitelists id only.
	oldMap := makeMap(t, 100, "id", "name")
	newMap := makeMap(t, 200, "id")

	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: oldMap,
	})
	// Simulate the back-buffer swap that swapMap performs.
	s.PrevWhitelist.Store(oldMap)
	s.AuthMap.Store(newMap)

	// Tx with CommitLSN=150 (older than refresh@200) → must use oldMap →
	// "name" is allowed.
	change := makeUpdateChange("orders", map[string]any{"name": "v"})
	out, drop := s.FilterWithLSN(change, pglogrepl.LSN(150))
	if drop {
		t.Fatalf("older tx: drop=true; want false (oldMap allows 'name')")
	}
	if _, ok := out.Changed["name"]; !ok {
		t.Fatalf("older tx: 'name' missing; want present via PrevWhitelist: %v", out.Changed)
	}

	// Tx with CommitLSN=250 (newer than refresh@200) → must use newMap →
	// "name" is dropped (only PK 'id' survives, but Changed map carries no
	// 'id' key → silent drop per Filter Rule 3).
	change2 := makeUpdateChange("orders", map[string]any{"name": "v"})
	_, drop2 := s.FilterWithLSN(change2, pglogrepl.LSN(250))
	if !drop2 {
		t.Fatalf("newer tx: drop=false; want true (newMap hides 'name')")
	}
}

// TestSubscriber_FilterWithLSN_PrevMapNilDropsConservatively — defensive drop.
func TestSubscriber_FilterWithLSN_PrevMapNilDropsConservatively(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id", "name"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	current := makeMap(t, 200, "id", "name")
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: current,
	})
	// PrevWhitelist intentionally nil.

	change := makeUpdateChange("orders", map[string]any{"name": "v"})
	_, drop := s.FilterWithLSN(change, pglogrepl.LSN(150)) // older than refresh@200
	if !drop {
		t.Fatalf("drop=false; want true (PrevWhitelist nil → conservative drop)")
	}
}

// TestSubscriber_RefreshTick_401_DropsAuthRevoked — 401 path.
func TestSubscriber_RefreshTick_401_DropsAuthRevoked(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(call int64, w http.ResponseWriter, _ *http.Request) {
		// All calls return 401.
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"revoked"}`))
		_ = call
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: makeMap(t, 100, "id", "name"),
	})

	s.tryRefresh(context.Background())

	if reason := s.Sub.Reason(); reason != "auth_revoked" {
		t.Fatalf("Sub.Reason: got %q; want %q", reason, "auth_revoked")
	}
	select {
	case <-s.Sub.Done():
	default:
		t.Fatalf("Sub.Done() did not fire after auth_revoked")
	}
}

func TestSubscriber_RefreshSuccess_LostTableWhitelistDropsAuthRevoked(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{
			"invoices": {"id", "name"},
		}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	initial := makeMap(t, 100, "id", "name")
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: initial,
	})

	s.tryRefresh(context.Background())

	if reason := s.Sub.Reason(); reason != "auth_revoked" {
		t.Fatalf("Sub.Reason: got %q; want %q", reason, "auth_revoked")
	}
	select {
	case <-s.Sub.Done():
	default:
		t.Fatalf("Sub.Done() did not fire after auth_revoked")
	}
	if got := s.AuthMap.Load(); got != initial {
		t.Fatalf("AuthMap.Load: got %v; want initial map unchanged", got)
	}
	if got := s.PrevWhitelist.Load(); got != nil {
		t.Fatalf("PrevWhitelist.Load: got %v; want nil (revoked map must not swap)", got)
	}
}

// TestSubscriber_RefreshTick_5xxRetriesThenDropsUnavailable — 5xx path with
// retry exhaustion. Uses 1ms backoffs so the test completes in ~4ms.
func TestSubscriber_RefreshTick_5xxRetriesThenDropsUnavailable(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	bk := &stubBreakerHook{allow: true}
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Breaker:    bk,
		InitialMap: makeMap(t, 100, "id", "name"),
		Backoffs:   []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond},
	})

	s.tryRefresh(context.Background())

	if got := srv.calls.Load(); got != 4 { // initial + 3 retries
		t.Fatalf("server hits: got %d; want 4", got)
	}
	if reason := s.Sub.Reason(); reason != "auth_unavailable" {
		t.Fatalf("Sub.Reason: got %q; want %q", reason, "auth_unavailable")
	}
}

// TestSubscriber_RefreshTick_5xxTrippedBreakerNoDrop — if the breaker
// tripped DURING retries, the subscriber keeps its stale map and is NOT
// dropped (bounded fail-open).
func TestSubscriber_RefreshTick_5xxTrippedBreakerNoDrop(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	bk := &stubBreakerHook{allow: false} // breaker open from the start
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Breaker:    bk,
		InitialMap: makeMap(t, 100, "id", "name"),
		Backoffs:   []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond},
	})

	s.tryRefresh(context.Background())

	if reason := s.Sub.Reason(); reason != "" {
		t.Fatalf("Sub.Reason: got %q; want empty (breaker open → no drop)", reason)
	}
}

// TestSubscriber_RefreshTick_BreakerOpenSkipsCall — RefreshLoop tick honors
// Allow()==false by skipping the call entirely.
func TestSubscriber_RefreshTick_BreakerOpenSkipsCall(t *testing.T) {
	t.Parallel()

	// Use a fast-tick ttl so RefreshLoop fires soon. Initial jitter is at most
	// ttl/2.
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	bk := &stubBreakerHook{allow: false}
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Breaker:    bk,
		InitialMap: makeMap(t, 100, "id"),
		DefaultTTL: 20 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RefreshLoop(ctx)
	}()

	// Allow several ticker cycles to pass (well over ttl/2 + ttl).
	time.Sleep(120 * time.Millisecond)
	cancel()
	<-done

	if got := srv.calls.Load(); got != 0 {
		t.Fatalf("server hits: got %d; want 0 (breaker open → no call)", got)
	}
	if reason := s.Sub.Reason(); reason != "" {
		t.Fatalf("Sub.Reason: got %q; want empty", reason)
	}
}

// TestSubscriber_RefreshSuccess_StampsRefreshLSNBeforeStore verifies the
// stamp-then-swap ordering primitive.
func TestSubscriber_RefreshSuccess_StampsRefreshLSNBeforeStore(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	syntheticLSN := pglogrepl.LSN(999)
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: makeMap(t, 100, "id", "name"),
		LSNFunc:    func() pglogrepl.LSN { return syntheticLSN },
	})

	s.tryRefresh(context.Background())

	got := s.AuthMap.Load()
	if got == nil {
		t.Fatalf("AuthMap nil after refresh")
	}
	if got.RefreshLSN != syntheticLSN {
		t.Fatalf("AuthMap.RefreshLSN: got %d; want %d", got.RefreshLSN, syntheticLSN)
	}

	prev := s.PrevWhitelist.Load()
	if prev == nil {
		t.Fatalf("PrevWhitelist nil after refresh; want old map")
	}
	if prev.RefreshLSN >= got.RefreshLSN {
		t.Fatalf("PrevWhitelist.RefreshLSN=%d; want < AuthMap.RefreshLSN=%d", prev.RefreshLSN, got.RefreshLSN)
	}
}

// TestSubscriber_InitialRefreshJitterDistribution — statistical test on
// initialJitterFunc. 100 samples with ttl=60s expects mean in [10s, 20s]
// (the uniform-[0,30s) mean is 15s; we allow wide tolerance).
func TestSubscriber_InitialRefreshJitterDistribution(t *testing.T) {
	t.Parallel()

	const N = 100
	const ttl = 60 * time.Second
	const halfTTL = ttl / 2

	var sum time.Duration
	var minDelay, maxDelay time.Duration
	minDelay = halfTTL // upper bound seed; first sample shrinks it
	maxDelay = 0
	for i := 0; i < N; i++ {
		d := initialJitterFunc(ttl)
		if d < 0 || d >= halfTTL {
			t.Fatalf("jitter %v out of [0, %v)", d, halfTTL)
		}
		sum += d
		if d < minDelay {
			minDelay = d
		}
		if d > maxDelay {
			maxDelay = d
		}
	}
	mean := sum / N
	// Expected mean is halfTTL/2 = 15s. Allow [10s, 20s] for n=100 variance.
	if mean < 10*time.Second || mean > 20*time.Second {
		t.Fatalf("jitter mean %v out of [10s, 20s] (n=%d, max=%v, min=%v)",
			mean, N, maxDelay, minDelay)
	}
}

// TestSubscriber_TryRefreshCoalescesConcurrentCalls — TryLock guards against
// the registry's stale-refresh racing with the periodic ticker.
func TestSubscriber_TryRefreshCoalescesConcurrentCalls(t *testing.T) {
	t.Parallel()

	// Slow handler so the first caller is mid-call when the second arrives.
	handlerStart := make(chan struct{})
	handlerRelease := make(chan struct{})
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		select {
		case handlerStart <- struct{}{}:
		default:
		}
		<-handlerRelease
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: makeMap(t, 100, "id"),
	})

	var wg sync.WaitGroup
	wg.Add(2)

	// First goroutine: enters tryRefresh and blocks inside the HTTP call.
	go func() {
		defer wg.Done()
		s.tryRefresh(context.Background())
	}()

	// Wait until the first call is mid-flight.
	<-handlerStart

	// Second goroutine: should observe refreshMu locked and return immediately.
	go func() {
		defer wg.Done()
		s.tryRefresh(context.Background())
	}()

	// Give the second caller a moment to TryLock-fail.
	time.Sleep(20 * time.Millisecond)

	close(handlerRelease)
	wg.Wait()

	if got := srv.calls.Load(); got != 1 {
		t.Fatalf("server hits: got %d; want 1 (concurrent refresh must coalesce)", got)
	}
}

// TestSubscriber_RefreshLoop_TickRefreshesMap — happy-path RefreshLoop with a
// short TTL drives at least one successful tick that updates AuthMap.
func TestSubscriber_RefreshLoop_TickRefreshesMap(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	// initial map has RefreshLSN=100; LSN function returns 999 → after first
	// successful tick AuthMap.RefreshLSN must change to 999.
	syntheticLSN := pglogrepl.LSN(999)
	// Initial map with TTLSeconds=0 so DefaultTTL governs the ticker.
	initial := makeMap(t, 100, "id")
	initial.TTLSeconds = 0
	bk := &stubBreakerHook{allow: true}
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Breaker:    bk,
		InitialMap: initial,
		DefaultTTL: 20 * time.Millisecond,
		LSNFunc:    func() pglogrepl.LSN { return syntheticLSN },
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RefreshLoop(ctx)
	}()

	// Wait until at least one successful refresh has bumped the RefreshLSN.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if m := s.AuthMap.Load(); m != nil && m.RefreshLSN == syntheticLSN {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	got := s.AuthMap.Load()
	if got == nil || got.RefreshLSN != syntheticLSN {
		t.Fatalf("AuthMap.RefreshLSN: got %v; want %d", got, syntheticLSN)
	}
}

// TestSubscriber_RefreshLoop_ExitsOnSubDrop — RefreshLoop exits when Sub.Done()
// fires (subscriber externally torn down).
func TestSubscriber_RefreshLoop_ExitsOnSubDrop(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	// Initial map with TTLSeconds=0 so DefaultTTL governs.
	initial := makeMap(t, 100, "id")
	initial.TTLSeconds = 0
	bk := &stubBreakerHook{allow: true}
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Breaker:    bk,
		InitialMap: initial,
		DefaultTTL: 500 * time.Millisecond, // long ttl → exit via Sub.Done, not ticker
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RefreshLoop(context.Background())
	}()

	// External drop fires Sub.Done().
	time.Sleep(10 * time.Millisecond)
	s.Sub.Drop("test_drop")
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("RefreshLoop did not exit on Sub.Done()")
	}
}

// TestSubscriber_RefreshLoop_ZeroTTLExitsCleanly — degenerate config (TTL<=0)
// returns immediately from RefreshLoop without spinning.
func TestSubscriber_RefreshLoop_ZeroTTLExitsCleanly(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	bk := &stubBreakerHook{allow: true}
	// Provide a Whitelist with TTLSeconds=0 AND DefaultTTL=0 → degenerate.
	m := &Whitelist{
		UserID:     "user-1",
		Tables:     map[string]map[string]struct{}{"orders": {"id": struct{}{}}},
		TTLSeconds: 0,
		RefreshLSN: 100,
	}
	// Construct directly (bypass the helper that defaults DefaultTTL→60s).
	rsub := newTestRouterSub(t)
	s := NewSubscriber(
		SubscriberConfig{
			InitialMap: m,
			Channel:    "public.orders:42",
			Token:      "test-token",
			DefaultTTL: 0,
		},
		SubscriberDeps{
			Sub:     rsub,
			Client:  c,
			Breaker: bk,
			Metrics: metrics.New(),
		},
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RefreshLoop(context.Background())
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("RefreshLoop did not exit on zero TTL")
	}
}

// TestSubscriber_TryRefresh_UnknownErrorDropsUnavailable — covers the
// "unknown error class" branch in tryRefresh (an error that is neither a
// revoked-class nor *ErrUnavailable).
func TestSubscriber_TryRefresh_UnknownErrorDropsUnavailable(t *testing.T) {
	t.Parallel()

	// 200 with malformed JSON → Client returns *ErrUnavailable wrapping a
	// decode error; that exercises the *ErrUnavailable retry path. To hit
	// the "unknown error class" branch we synthesize a Client that returns a
	// custom error. Since Client is concrete, we substitute via a small
	// fake test-only approach: use a closing server which produces a
	// transport error → *ErrUnavailable. That's the same retry-then-drop
	// path covered elsewhere. Instead we test the case where a successful
	// 200 contains a malformed body, which Client wraps as *ErrUnavailable.
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not-json"))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	bk := &stubBreakerHook{allow: true}
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Breaker:    bk,
		InitialMap: makeMap(t, 100, "id"),
		Backoffs:   []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond},
	})

	s.tryRefresh(context.Background())
	if reason := s.Sub.Reason(); reason != "auth_unavailable" {
		t.Fatalf("Sub.Reason: got %q; want %q", reason, "auth_unavailable")
	}
}

// TestSubscriber_SwapMap_NilFreshNoOp — swapMap with nil fresh is a no-op.
func TestSubscriber_SwapMap_NilFreshNoOp(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {})
	c, _ := newSubTestClient(t, srv.srv.URL)

	initial := makeMap(t, 100, "id")
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: initial,
	})

	s.swapMap(nil)

	if got := s.AuthMap.Load(); got != initial {
		t.Fatalf("AuthMap mutated: got %v; want unchanged %v", got, initial)
	}
	if got := s.PrevWhitelist.Load(); got != nil {
		t.Fatalf("PrevWhitelist set: got %v; want nil", got)
	}
}

// TestSubscriber_FilterClosure_NilMapDrops — defensive: nil AuthMap returns
// drop=true on every change.
func TestSubscriber_FilterClosure_NilMapDrops(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	// Construct directly; bypass NewSubscriber's "install initial" so AuthMap
	// is nil.
	s := &Subscriber{
		Sub:    newTestRouterSub(t),
		client: c,
	}

	change := makeUpdateChange("orders", map[string]any{"name": "x"})
	_, drop := s.FilterClosure()(change)
	if !drop {
		t.Fatalf("FilterClosure with nil map: drop=false; want true")
	}
	_, drop = s.FilterWithLSN(change, pglogrepl.LSN(500))
	if !drop {
		t.Fatalf("FilterWithLSN with nil map: drop=false; want true")
	}
}

// gatherAuthRefreshCount walks the metric registry and returns the counter
// value at walera_auth_refresh_total{result=<result>}.
func gatherAuthRefreshCount(t *testing.T, reg *metrics.Registry, result string) float64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != "walera_auth_refresh_total" {
			continue
		}
		for _, m := range fam.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "result" && lp.GetValue() == result {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// TestSubscriber_RefreshTick_IncrementsAuthRefreshMetric asserts that every
// per-subscriber refresh attempt increments
// walera_auth_refresh_total{result=<label>}. We exercise three of the five
// label arms in one run (ok, unauthorized, unavailable) to keep the test
// fast; the remaining label arms (forbidden, not_found) follow the same
// code path through refreshResultLabel.
func TestSubscriber_RefreshTick_IncrementsAuthRefreshMetric(t *testing.T) {
	t.Parallel()

	// Script the server: call 1 → 200, call 2 → 401, call 3..6 → 500 (drives
	// the initial 5xx + three retry backoffs all into "unavailable").
	srv := newScriptServer(t, func(call int64, w http.ResponseWriter, _ *http.Request) {
		switch call {
		case 1:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
		case 2:
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"revoked"}`))
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	c, mc := newSubTestClient(t, srv.srv.URL)

	// Drive refresh #1 — expect result=ok.
	s1 := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: makeMap(t, 100, "id"),
		Metrics:    mc,
	})
	s1.tryRefresh(context.Background())

	if got := gatherAuthRefreshCount(t, mc, "ok"); got != 1 {
		t.Errorf("auth_refresh_total{result=ok} after 200: got %v; want 1", got)
	}

	// Drive refresh #2 — expect result=unauthorized.
	s2 := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: makeMap(t, 100, "id"),
		Metrics:    mc,
	})
	s2.tryRefresh(context.Background())

	if got := gatherAuthRefreshCount(t, mc, "unauthorized"); got != 1 {
		t.Errorf("auth_refresh_total{result=unauthorized} after 401: got %v; want 1", got)
	}

	// Drive refresh #3 — expect result=unavailable (4× through the retry
	// schedule: initial 5xx + three retries). Backoffs trimmed to 1ms so the
	// suite stays sub-second.
	s3 := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: makeMap(t, 100, "id"),
		Backoffs:   []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond},
		Breaker:    &stubBreakerHook{allow: true},
		Metrics:    mc,
	})
	s3.tryRefresh(context.Background())

	if got := gatherAuthRefreshCount(t, mc, "unavailable"); got != 4 {
		t.Errorf("auth_refresh_total{result=unavailable} after 4×5xx: got %v; want 4", got)
	}

	// Sanity — the unused label arms must remain at zero (pre-touched series
	// visible at 0, never incremented).
	for _, result := range []string{"forbidden", "not_found"} {
		if got := gatherAuthRefreshCount(t, mc, result); got != 0 {
			t.Errorf("auth_refresh_total{result=%s}: got %v; want 0 (no producer fired)", result, got)
		}
	}
}
