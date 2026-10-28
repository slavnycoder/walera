package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

func usersMap(userID string, cols ...string) *Whitelist {
	set := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		set[c] = struct{}{}
	}
	return &Whitelist{
		UserID:     userID,
		Tables:     map[string]map[string]struct{}{"users": set},
		TTLSeconds: 60,
	}
}

func TestTryRefresh_Revoked403_DropsSub(t *testing.T) {
	t.Parallel()
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Channel:    "users:42",
		UserID:     "u1",
		InitialMap: usersMap("u1", "id"),
	})

	s.tryRefresh(context.Background())
	if r := s.Sub.Reason(); r != "auth_revoked" {
		t.Errorf("drop reason = %q; want auth_revoked", r)
	}
}

// An on-demand swap during a TTL backoff sleep must succeed, and the chain must
// not then drop the freshly refreshed stream.
func TestTryRefresh_OnDemandNotStarvedDuringBackoff(t *testing.T) {
	t.Parallel()
	attemptFailed := make(chan struct{}, 1)
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		select {
		case attemptFailed <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusInternalServerError)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	initial := usersMap("u1", "id")
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Breaker:    &stubBreakerHook{allow: true},
		Channel:    "users:42",
		UserID:     "u1",
		InitialMap: initial,
		Backoffs:   []time.Duration{500 * time.Millisecond},
	})

	reg := newTestRegistry(t)
	reg.Add(s)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.tryRefresh(context.Background())
	}()

	// The first TTL attempt has failed; tryRefresh is now in its backoff sleep
	// with refreshMu released.
	<-attemptFailed

	// On-demand refresh during the backoff window must succeed, not get Busy.
	var sum RefreshSummary
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if sum = reg.RefreshOwnByChannel("users:42", "u1", usersMap("u1", "id", "name")); sum.Busy == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if sum.Swapped != 1 {
		t.Fatalf("on-demand during TTL backoff = %+v; want Swapped=1 (refreshMu must be free during the sleep)", sum)
	}

	wg.Wait()

	if r := s.Sub.Reason(); r != "" {
		t.Fatalf("stream dropped with reason %q after a concurrent on-demand swap; want kept alive (no wrongful drop)", r)
	}
	if _, ok := s.AuthMap.Load().Tables["users"]["name"]; !ok {
		t.Errorf("AuthMap missing users.name; on-demand swap not visible")
	}
	if n := srv.calls.Load(); n != 1 {
		t.Errorf("backend calls = %d; want 1 (one failed TTL attempt; the chain must stop after the on-demand swap)", n)
	}
}

// A second chain started during the first one's backoff must no-op.
func TestTryRefresh_BackoffChainCoalesces(t *testing.T) {
	t.Parallel()
	attemptFailed := make(chan struct{}, 1)
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		select {
		case attemptFailed <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusInternalServerError)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Breaker:    &stubBreakerHook{allow: true},
		Channel:    "users:42",
		UserID:     "u1",
		InitialMap: usersMap("u1", "id"),
		Backoffs:   []time.Duration{300 * time.Millisecond},
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Chain A: attempt0 (500) -> backoff -> attempt1 (500) -> drop.
		s.tryRefresh(context.Background())
	}()

	// Chain A is now in its backoff sleep, still holding chainMu.
	<-attemptFailed

	// Chain B starts during A's backoff: chainMu is held, so it must no-op
	// immediately without contacting the backend.
	s.tryRefresh(context.Background())

	// refreshMu must be free during chain A's backoff; the call count alone
	// wouldn't catch a chain that holds it across sleeps.
	acquired := false
	deadline := time.Now().Add(200 * time.Millisecond) // < the 300ms backoff
	for time.Now().Before(deadline) {
		if s.refreshMu.TryLock() {
			s.refreshMu.Unlock()
			acquired = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !acquired {
		t.Fatalf("refreshMu never free during chain A's backoff; the chain must release the swap lock between retries (#3b)")
	}

	wg.Wait()

	if n := srv.calls.Load(); n != 2 {
		t.Fatalf("backend calls = %d; want 2 (chain A's two attempts only; the concurrent chain must coalesce)", n)
	}
	if r := s.Sub.Reason(); r != "auth_unavailable" {
		t.Errorf("drop reason = %q; want auth_unavailable", r)
	}
}

// Deterministic coverage; via tryRefresh this guard needs a precise race.
func TestDropIfUnchanged(t *testing.T) {
	t.Parallel()
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"users": {"id"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	t.Run("unchanged drops auth_unavailable", func(t *testing.T) {
		t.Parallel()
		s := newTestSubscriber(t, subscriberTestOpts{
			Client:     c,
			Channel:    "users:42",
			UserID:     "u1",
			InitialMap: usersMap("u1", "id"),
		})
		mapAtChainStart := s.AuthMap.Load()
		s.dropIfUnchanged(mapAtChainStart)
		if r := s.Sub.Reason(); r != "auth_unavailable" {
			t.Fatalf("drop reason = %q; want auth_unavailable (snapshot unchanged -> drop)", r)
		}
		select {
		case <-s.Sub.Done():
		default:
			t.Fatalf("Sub.Done() did not fire after dropIfUnchanged")
		}
	})

	t.Run("changed skips the drop", func(t *testing.T) {
		t.Parallel()
		s := newTestSubscriber(t, subscriberTestOpts{
			Client:     c,
			Channel:    "users:42",
			UserID:     "u1",
			InitialMap: usersMap("u1", "id"),
		})
		mapAtChainStart := s.AuthMap.Load()
		// Simulate a concurrent on-demand refresh swapping a fresh whitelist.
		s.swapMap(usersMap("u1", "id", "name"))
		if s.AuthMap.Load() == mapAtChainStart {
			t.Fatalf("swapMap did not change AuthMap pointer; test premise broken")
		}
		s.dropIfUnchanged(mapAtChainStart)
		if r := s.Sub.Reason(); r != "" {
			t.Fatalf("drop reason = %q; want empty (a concurrent swap must suppress the drop)", r)
		}
		select {
		case <-s.Sub.Done():
			t.Fatalf("Sub.Done() fired; want stream kept alive after a concurrent swap")
		default:
		}
	})
}

func ownSub(t *testing.T, c *Client, userID, channel string) *Subscriber {
	t.Helper()
	return newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Channel:    channel,
		UserID:     userID,
		InitialMap: usersMap(userID, "id"),
	})
}

func TestRefreshOwnByChannel_OnlyOwnChannelUser(t *testing.T) {
	t.Parallel()
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"users": {"id", "name"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	target := ownSub(t, c, "u1", "users:42")    // should be refreshed
	otherUser := ownSub(t, c, "u2", "users:42") // different user, same channel
	otherChan := ownSub(t, c, "u1", "users:99") // same user, different channel

	reg := newTestRegistry(t)
	reg.Add(target)
	reg.Add(otherUser)
	reg.Add(otherChan)

	sum := reg.RefreshOwnByChannel("users:42", "u1", usersMap("u1", "id", "name"))
	if sum.Matched != 1 || sum.Swapped != 1 {
		t.Fatalf("summary = %+v; want Matched=1 Swapped=1", sum)
	}
	if _, ok := target.AuthMap.Load().Tables["users"]["name"]; !ok {
		t.Errorf("target not refreshed: users.name missing")
	}
	if _, ok := otherUser.AuthMap.Load().Tables["users"]["name"]; ok {
		t.Errorf("other user's subscriber was refreshed; want untouched")
	}
	if _, ok := otherChan.AuthMap.Load().Tables["users"]["name"]; ok {
		t.Errorf("other channel's subscriber was refreshed; want untouched")
	}
}

func TestRefreshOwnByChannel_FanOutMultipleOwnSubs(t *testing.T) {
	t.Parallel()
	// Same user, same channel, two distinct live subscribers (e.g. two tabs).
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"users": {"id", "name"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	a := ownSub(t, c, "u1", "users:42")
	b := ownSub(t, c, "u1", "users:42")

	reg := newTestRegistry(t)
	reg.Add(a)
	reg.Add(b)

	sum := reg.RefreshOwnByChannel("users:42", "u1", usersMap("u1", "id", "name"))
	if sum.Matched != 2 || sum.Swapped != 2 {
		t.Fatalf("summary = %+v; want Matched=2 Swapped=2", sum)
	}
	for i, s := range []*Subscriber{a, b} {
		if _, ok := s.AuthMap.Load().Tables["users"]["name"]; !ok {
			t.Errorf("sub %d not refreshed: users.name missing", i)
		}
	}
}

func TestRefreshOwnByChannel_SwappedAndBusy(t *testing.T) {
	t.Parallel()
	// Two same-user/same-channel subs; one is mid-refresh (lock held) and must be
	// reported Busy while the other swaps. Exercises the registry's mixed
	// aggregation (Swapped>0 AND Busy>0) that the HTTP precedence relies on.
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"users": {"id", "name"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	free := ownSub(t, c, "u1", "users:42")
	busy := ownSub(t, c, "u1", "users:42")

	reg := newTestRegistry(t)
	reg.Add(free)
	reg.Add(busy)

	busy.refreshMu.Lock() // simulate an in-flight TTL refresh on one sub
	sum := reg.RefreshOwnByChannel("users:42", "u1", usersMap("u1", "id", "name"))
	busy.refreshMu.Unlock()

	if sum.Matched != 2 || sum.Swapped != 1 || sum.Busy != 1 {
		t.Fatalf("summary = %+v; want Matched=2 Swapped=1 Busy=1", sum)
	}
	if _, ok := free.AuthMap.Load().Tables["users"]["name"]; !ok {
		t.Errorf("free sub not refreshed")
	}
	if _, ok := busy.AuthMap.Load().Tables["users"]["name"]; ok {
		t.Errorf("busy sub was refreshed despite held lock")
	}
}

func TestRefreshOwnByChannel_NoMatch(t *testing.T) {
	t.Parallel()
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"users": {"id"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	reg := newTestRegistry(t)
	reg.Add(ownSub(t, c, "u1", "users:42"))

	sum := reg.RefreshOwnByChannel("users:777", "u1", usersMap("u1", "id", "name"))
	if sum.Matched != 0 {
		t.Fatalf("summary = %+v; want Matched=0", sum)
	}
	if n := srv.calls.Load(); n != 0 {
		t.Errorf("backend calls = %d; want 0 (no match must not contact the backend)", n)
	}
}

// -race check: FilterWithLSN reads vs on-demand and TTL swaps on one subscriber.
func TestRefreshOwnByChannel_RaceWithFilterAndTTL(t *testing.T) {
	t.Parallel()
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"users": {"id", "name"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Channel:    "users:42",
		UserID:     "u1",
		InitialMap: usersMap("u1", "id"),
	})

	reg := newTestRegistry(t)
	reg.Add(s)

	change := makeUpdateChange("users", map[string]any{"id": 1, "name": "x"})

	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.FilterWithLSN(change, pglogrepl.LSN(1<<40))
			}
		}
	}()

	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				reg.RefreshOwnByChannel("users:42", "u1", usersMap("u1", "id", "name"))
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for j := 0; j < 100; j++ {
			s.tryRefresh(context.Background())
		}
	}()
	workers.Wait()
	close(stop)
	readers.Wait()

	if r := s.Sub.Reason(); r != "" {
		t.Errorf("subscriber dropped with reason %q; want kept alive", r)
	}
	if _, ok := s.AuthMap.Load().Tables["users"]["name"]; !ok {
		t.Errorf("final whitelist missing users.name after refreshes")
	}
}

func TestRefreshOwnByChannel_WildcardNotCrossMatched(t *testing.T) {
	t.Parallel()
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"users": {"id", "name"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	reg := newTestRegistry(t)
	reg.Add(ownSub(t, c, "u1", "users:all"))

	if sum := reg.RefreshOwnByChannel("users:42", "u1", usersMap("u1", "id", "name")); sum.Matched != 0 {
		t.Errorf("exact channel matched a wildcard subscriber: %+v", sum)
	}
	if sum := reg.RefreshOwnByChannel("users:all", "u1", usersMap("u1", "id", "name")); sum.Matched != 1 || sum.Swapped != 1 {
		t.Errorf("wildcard channel summary = %+v; want Matched=1 Swapped=1", sum)
	}
}

func TestRefreshOwnByChannel_AppliesGivenWhitelist_NoBackendCall(t *testing.T) {
	t.Parallel()
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)
	sub := ownSub(t, c, "u1", "users:42")
	reg := newTestRegistry(t)
	reg.Add(sub)

	fresh := usersMap("u1", "id", "name")
	fresh.InitialData = json.RawMessage(`{"startup":"payload"}`)

	sum := reg.RefreshOwnByChannel("users:42", "u1", fresh)
	if sum.Matched != 1 || sum.Swapped != 1 {
		t.Fatalf("summary = %+v; want Matched=1 Swapped=1", sum)
	}
	if n := srv.calls.Load(); n != 0 {
		t.Errorf("backend calls = %d; want 0 (whitelist is supplied by the caller)", n)
	}
	got := sub.AuthMap.Load()
	if _, ok := got.Tables["users"]["name"]; !ok {
		t.Errorf("whitelist not swapped: users.name missing")
	}
	if got.InitialData != nil {
		t.Errorf("swapped whitelist retains InitialData (%d bytes); want nil", len(got.InitialData))
	}
}

func TestRefreshOwnByChannel_RevokedDropsSub(t *testing.T) {
	t.Parallel()
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)
	sub := ownSub(t, c, "u1", "users:42")
	reg := newTestRegistry(t)
	reg.Add(sub)

	fresh := &Whitelist{UserID: "u1", Tables: map[string]map[string]struct{}{"orders": {}}}
	sum := reg.RefreshOwnByChannel("users:42", "u1", fresh)
	if sum.Matched != 1 || sum.Revoked != 1 {
		t.Fatalf("summary = %+v; want Matched=1 Revoked=1", sum)
	}
	if r := sub.Sub.Reason(); r != "auth_revoked" {
		t.Errorf("drop reason = %q; want auth_revoked", r)
	}
}
