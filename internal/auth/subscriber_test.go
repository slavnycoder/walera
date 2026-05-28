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

type stubBreakerHook struct{ allow bool }

func (s *stubBreakerHook) Allow() bool { return s.allow }

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

func newSubTestClient(t *testing.T, server string) (*Client, *metrics.Registry) {
	t.Helper()
	mc := metrics.New()
	cfg := Config{
		BackendURL:     server,
		RequestTimeout: 2 * time.Second,
		HealthChannel:  "_health",
		Signing: SigningConfig{
			Secret: "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk",
			Kid:    "v1",
		},
	}
	c := New(cfg, Deps{Logger: zerolog.Nop(), Metrics: mc})
	return c, mc
}

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

type subscriberTestOpts struct {
	Sub        *router.Subscriber
	Client     *Client
	Breaker    breakerHook
	InitialMap *Whitelist
	UserID     string
	Channel    string
	DefaultTTL time.Duration
	Log        zerolog.Logger
	Metrics    *metrics.Registry
	NowFunc    func() time.Time
	LSNFunc    func() pglogrepl.LSN
	Backoffs   []time.Duration
}

func newTestSubscriber(t *testing.T, opts subscriberTestOpts) *Subscriber {
	t.Helper()
	if opts.Sub == nil {
		opts.Sub = newTestRouterSub(t)
	}
	if opts.Channel == "" {
		opts.Channel = "public.orders:42"
	}
	if opts.UserID == "" {
		opts.UserID = "user-1"
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
			UserID:     opts.UserID,
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

func TestSubscriber_FilterWithLSN_UsesPrevMapForOlderTx(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	oldMap := makeMap(t, 100, "id", "name")
	newMap := makeMap(t, 200, "id")

	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: oldMap,
	})

	s.PrevWhitelist.Store(oldMap)
	s.AuthMap.Store(newMap)

	change := makeUpdateChange("orders", map[string]any{"name": "v"})
	out, drop := s.FilterWithLSN(change, pglogrepl.LSN(150))
	if drop {
		t.Fatalf("older tx: drop=true; want false (oldMap allows 'name')")
	}
	if _, ok := out.Changed["name"]; !ok {
		t.Fatalf("older tx: 'name' missing; want present via PrevWhitelist: %v", out.Changed)
	}

	change2 := makeUpdateChange("orders", map[string]any{"name": "v"})
	_, drop2 := s.FilterWithLSN(change2, pglogrepl.LSN(250))
	if !drop2 {
		t.Fatalf("newer tx: drop=false; want true (newMap hides 'name')")
	}
}

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

	change := makeUpdateChange("orders", map[string]any{"name": "v"})
	_, drop := s.FilterWithLSN(change, pglogrepl.LSN(150))
	if !drop {
		t.Fatalf("drop=false; want true (PrevWhitelist nil → conservative drop)")
	}
}

func TestSubscriber_RefreshTick_401_DropsAuthRevoked(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(call int64, w http.ResponseWriter, _ *http.Request) {

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

	if got := srv.calls.Load(); got != 4 {
		t.Fatalf("server hits: got %d; want 4", got)
	}
	if reason := s.Sub.Reason(); reason != "auth_unavailable" {
		t.Fatalf("Sub.Reason: got %q; want %q", reason, "auth_unavailable")
	}
}

func TestSubscriber_RefreshTick_5xxTrippedBreakerNoDrop(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	bk := &stubBreakerHook{allow: false}
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

func TestSubscriber_RefreshTick_BreakerOpenSkipsCall(t *testing.T) {
	t.Parallel()

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

func TestSubscriber_InitialRefreshJitterDistribution(t *testing.T) {
	t.Parallel()

	const N = 100
	const ttl = 60 * time.Second
	const halfTTL = ttl / 2

	var sum time.Duration
	var minDelay, maxDelay time.Duration
	minDelay = halfTTL
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

	if mean < 10*time.Second || mean > 20*time.Second {
		t.Fatalf("jitter mean %v out of [10s, 20s] (n=%d, max=%v, min=%v)",
			mean, N, maxDelay, minDelay)
	}
}

func TestSubscriber_TryRefreshCoalescesConcurrentCalls(t *testing.T) {
	t.Parallel()

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

	go func() {
		defer wg.Done()
		s.tryRefresh(context.Background())
	}()

	<-handlerStart

	go func() {
		defer wg.Done()
		s.tryRefresh(context.Background())
	}()

	time.Sleep(20 * time.Millisecond)

	close(handlerRelease)
	wg.Wait()

	if got := srv.calls.Load(); got != 1 {
		t.Fatalf("server hits: got %d; want 1 (concurrent refresh must coalesce)", got)
	}
}

func TestSubscriber_RefreshLoop_TickRefreshesMap(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	syntheticLSN := pglogrepl.LSN(999)

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

func TestSubscriber_RefreshLoop_ExitsOnSubDrop(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	initial := makeMap(t, 100, "id")
	initial.TTLSeconds = 0
	bk := &stubBreakerHook{allow: true}
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Breaker:    bk,
		InitialMap: initial,
		DefaultTTL: 500 * time.Millisecond,
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RefreshLoop(context.Background())
	}()

	time.Sleep(10 * time.Millisecond)
	s.Sub.Drop("test_drop")
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("RefreshLoop did not exit on Sub.Done()")
	}
}

func TestSubscriber_RefreshLoop_ZeroTTLExitsCleanly(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	bk := &stubBreakerHook{allow: true}

	m := &Whitelist{
		UserID:     "user-1",
		Tables:     map[string]map[string]struct{}{"orders": {"id": struct{}{}}},
		TTLSeconds: 0,
		RefreshLSN: 100,
	}

	rsub := newTestRouterSub(t)
	s := NewSubscriber(
		SubscriberConfig{
			InitialMap: m,
			Channel:    "public.orders:42",
			UserID:     "user-1",
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

func TestSubscriber_TryRefresh_UnknownErrorDropsUnavailable(t *testing.T) {
	t.Parallel()

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

func TestSubscriber_DefaultTTLZeroIgnoresInitialMapTTL(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	initial := makeMap(t, 100, "id")
	initial.TTLSeconds = 60

	s := NewSubscriber(
		SubscriberConfig{
			InitialMap: initial,
			UserID:     "user-1",
			Channel:    "public.orders:42",
			DefaultTTL: 0,
		},
		SubscriberDeps{
			Sub:     newTestRouterSub(t),
			Client:  c,
			Metrics: metrics.New(),
		},
	)

	if s.ttl != 0 {
		t.Fatalf("ttl: got %s; want 0 (feature gated by walera config)", s.ttl)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RefreshLoop(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("RefreshLoop did not exit when DefaultTTL=0 and InitialMap.TTLSeconds>0")
	}
}

func TestSubscriber_LogRefreshError_SilentWhenTTLDisabled(t *testing.T) {
	t.Parallel()

	var buf threadSafeBuf
	logger := zerolog.New(&buf)

	s := &Subscriber{
		Sub: newTestRouterSub(t),
		log: logger,
		ttl: 0,
	}
	s.logRefreshError(&ErrUnavailable{Cause: context.DeadlineExceeded}, 0)

	if buf.Len() != 0 {
		t.Fatalf("log output with ttl=0: got %q; want empty", buf.String())
	}
}

func TestSubscriber_LogRefreshError_IncludesDocsURL(t *testing.T) {
	t.Parallel()

	var buf threadSafeBuf
	logger := zerolog.New(&buf)

	s := &Subscriber{
		Sub:    newTestRouterSub(t),
		log:    logger,
		ttl:    60 * time.Second,
		userID: "user-1",
	}
	s.logRefreshError(&ErrUnavailable{Cause: context.DeadlineExceeded}, 2)

	line := buf.String()
	if line == "" {
		t.Fatal("expected log line; got empty buffer")
	}
	if !contains(line, DocsURL) {
		t.Errorf("log missing docs URL %q: %s", DocsURL, line)
	}
	if !contains(line, `"result":"unavailable"`) {
		t.Errorf("log missing result label: %s", line)
	}
	if !contains(line, `"attempt":2`) {
		t.Errorf("log missing attempt counter: %s", line)
	}
}

type threadSafeBuf struct {
	mu  sync.Mutex
	buf []byte
}

func (b *threadSafeBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *threadSafeBuf) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buf)
}

func (b *threadSafeBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

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

func TestSubscriber_FilterClosure_NilMapDrops(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

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

func TestSubscriber_RefreshTick_IncrementsAuthRefreshMetric(t *testing.T) {
	t.Parallel()

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

	s1 := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: makeMap(t, 100, "id"),
		Metrics:    mc,
	})
	s1.tryRefresh(context.Background())

	if got := gatherAuthRefreshCount(t, mc, "ok"); got != 1 {
		t.Errorf("auth_refresh_total{result=ok} after 200: got %v; want 1", got)
	}

	s2 := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: makeMap(t, 100, "id"),
		Metrics:    mc,
	})
	s2.tryRefresh(context.Background())

	if got := gatherAuthRefreshCount(t, mc, "unauthorized"); got != 1 {
		t.Errorf("auth_refresh_total{result=unauthorized} after 401: got %v; want 1", got)
	}

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

	for _, result := range []string{"forbidden", "not_found"} {
		if got := gatherAuthRefreshCount(t, mc, result); got != 0 {
			t.Errorf("auth_refresh_total{result=%s}: got %v; want 0 (no producer fired)", result, got)
		}
	}
}
