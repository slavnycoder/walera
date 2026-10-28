package auth

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

// TestSubscriber_FilterWithLSN_NarrowingBoundary_NoLeak pins the exact LSN edge
// after a grant is narrowed through the real swapMap path. A tx that commits at
// or before the refresh boundary keeps the old (wide) grant — those are
// in-flight, point-in-time-authorized changes. A tx that commits strictly after
// the boundary must use the new (narrow) grant, so a now-forbidden field cannot
// leak through the prev-map fallback window.
func TestSubscriber_FilterWithLSN_NarrowingBoundary_NoLeak(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {})
	c, _ := newSubTestClient(t, srv.srv.URL)

	wide := makeMap(t, 100, "id", "email")
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: wide,
		LSNFunc:    func() pglogrepl.LSN { return 500 }, // boundary stamped on swap
	})

	// Narrow the grant (drop email) via the production swap path: swapMap stamps
	// RefreshLSN = LSNFunc() = 500 and moves the wide map to PrevWhitelist.
	s.swapMap(makeMap(t, 0, "id"))
	if got := s.AuthMap.Load().RefreshLSN; got != 500 {
		t.Fatalf("post-swap RefreshLSN: got %d; want 500", got)
	}

	// At or before the boundary: in-flight txs keep the old (wide) grant.
	for _, lsn := range []pglogrepl.LSN{499, 500} {
		out, drop := s.FilterWithLSN(makeUpdateChange("orders", map[string]any{"email": "secret"}), lsn)
		if drop {
			t.Fatalf("commitLSN=%d: drop=true; want delivered via prev (wide) map", lsn)
		}
		if _, ok := out.Changed["email"]; !ok {
			t.Fatalf("commitLSN=%d: email missing; in-flight tx should use old grant", lsn)
		}
	}

	// Strictly after the boundary: the new (narrow) grant applies — email must
	// not leak. This is the security guarantee.
	if _, drop := s.FilterWithLSN(makeUpdateChange("orders", map[string]any{"email": "secret"}), 501); !drop {
		t.Fatalf("commitLSN=501: drop=false; want true (email forbidden after narrowing)")
	}
}

// TestSubscriber_ConcurrentSwapDropAndDelivery_RaceClean exercises the delivery
// path concurrently with grant swaps and a revoke. Run under -race it guards the
// atomic.Pointer swaps of AuthMap/PrevWhitelist against future regressions, and
// asserts a post-revoke delivery does not leak the narrowed field.
func TestSubscriber_ConcurrentSwapDropAndDelivery_RaceClean(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {})
	c, _ := newSubTestClient(t, srv.srv.URL)

	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: makeMap(t, 1, "id", "email"),
		LSNFunc:    func() pglogrepl.LSN { return 10 },
	})

	const readers = 8
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = s.FilterWithLSN(makeUpdateChange("orders", map[string]any{"email": "x"}), pglogrepl.LSN(1000))
				}
			}
		}()
	}

	// Writer flips the grant wide<->narrow repeatedly, then revokes.
	for i := 0; i < 200; i++ {
		if i%2 == 0 {
			s.swapMap(makeMap(t, 0, "id")) // narrow
		} else {
			s.swapMap(makeMap(t, 0, "id", "email")) // wide
		}
	}
	s.Sub.Drop("auth_revoked")

	close(stop)
	wg.Wait()

	// Settle on the narrow grant (boundary 10 < read LSN 1000) and confirm no
	// leak on a post-revoke delivery.
	s.swapMap(makeMap(t, 0, "id"))
	if _, drop := s.FilterWithLSN(makeUpdateChange("orders", map[string]any{"email": "x"}), pglogrepl.LSN(1000)); !drop {
		t.Fatalf("post-revoke delivery leaked email; want drop")
	}
}

// An auth outage must not extend a live grant past TTL + retry backoffs.
func TestSubscriber_AuthOutage_DropsAfterTTL(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	initial := makeMap(t, 100, "id", "email")
	initial.TTLSeconds = 0
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: initial,
		DefaultTTL: 15 * time.Millisecond,
		Backoffs:   []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.RefreshLoop(ctx) }()
	defer func() { cancel(); <-done }()

	select {
	case <-s.Sub.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber still alive 2s into an auth outage; want auth_unavailable drop")
	}
	if got := s.Sub.Reason(); got != "auth_unavailable" {
		t.Fatalf("Sub.Reason = %q; want auth_unavailable", got)
	}
}
