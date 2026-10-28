package auth

import (
	"context"
	"net/http"
	"sync"
	"testing"

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

func TestRefreshOnce_Swapped(t *testing.T) {
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

	if got := s.RefreshOnce(context.Background()); got != RefreshSwapped {
		t.Fatalf("RefreshOnce = %v; want RefreshSwapped", got)
	}
	if _, ok := s.AuthMap.Load().Tables["users"]["name"]; !ok {
		t.Errorf("AuthMap not swapped: users.name missing after refresh")
	}
	if r := s.Sub.Reason(); r != "" {
		t.Errorf("subscriber dropped with reason %q; want not dropped", r)
	}
}

func TestRefreshOnce_RevokedTableMissing_DropsSub(t *testing.T) {
	t.Parallel()
	// Fresh whitelist no longer grants the channel's table -> revoked.
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Channel:    "users:42",
		UserID:     "u1",
		InitialMap: usersMap("u1", "id"),
	})

	if got := s.RefreshOnce(context.Background()); got != RefreshRevoked {
		t.Fatalf("RefreshOnce = %v; want RefreshRevoked", got)
	}
	if r := s.Sub.Reason(); r != "auth_revoked" {
		t.Errorf("drop reason = %q; want auth_revoked", r)
	}
}

func TestRefreshOnce_Revoked403_DropsSub(t *testing.T) {
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

	if got := s.RefreshOnce(context.Background()); got != RefreshRevoked {
		t.Fatalf("RefreshOnce = %v; want RefreshRevoked", got)
	}
	if r := s.Sub.Reason(); r != "auth_revoked" {
		t.Errorf("drop reason = %q; want auth_revoked", r)
	}
}

func TestRefreshOnce_Unavailable_KeepsStream(t *testing.T) {
	t.Parallel()
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)
	initial := usersMap("u1", "id")
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Channel:    "users:42",
		UserID:     "u1",
		InitialMap: initial,
	})

	if got := s.RefreshOnce(context.Background()); got != RefreshUnavailable {
		t.Fatalf("RefreshOnce = %v; want RefreshUnavailable", got)
	}
	// Best-effort: the stream must NOT be dropped on a transient failure.
	if r := s.Sub.Reason(); r != "" {
		t.Errorf("subscriber dropped with reason %q; want kept alive", r)
	}
	if s.AuthMap.Load() != initial {
		t.Errorf("AuthMap changed on a failed refresh; want initial map intact")
	}
}

func TestRefreshOnce_Busy_NoBackendCall(t *testing.T) {
	t.Parallel()
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"users": {"id"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Channel:    "users:42",
		UserID:     "u1",
		InitialMap: usersMap("u1", "id"),
	})

	// Simulate a concurrent in-flight refresh holding the lock.
	s.refreshMu.Lock()
	got := s.RefreshOnce(context.Background())
	s.refreshMu.Unlock()

	if got != RefreshBusy {
		t.Fatalf("RefreshOnce = %v; want RefreshBusy", got)
	}
	if n := srv.calls.Load(); n != 0 {
		t.Errorf("backend calls = %d; want 0 (busy must not contact the backend)", n)
	}
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

	sum := reg.RefreshOwnByChannel(context.Background(), "users:42", "u1")
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

	sum := reg.RefreshOwnByChannel(context.Background(), "users:42", "u1")
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
	sum := reg.RefreshOwnByChannel(context.Background(), "users:42", "u1")
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

// denyBreaker reports the breaker as open (Allow() == false).
type denyBreaker struct{}

func (denyBreaker) Allow() bool { return false }

func TestRefreshOnce_BreakerOpen_StillAttempts(t *testing.T) {
	t.Parallel()
	// Unlike the TTL RefreshLoop (which skips a tick when the breaker is open),
	// the on-demand RefreshOnce performs its single attempt regardless of breaker
	// state — the breaker gate lives in RefreshLoop, not in refreshAttempt.
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"users": {"id", "name"}}))
	})
	c, _ := newSubTestClient(t, srv.srv.URL)
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Breaker:    denyBreaker{}, // breaker open
		Channel:    "users:42",
		UserID:     "u1",
		InitialMap: usersMap("u1", "id"),
	})

	if got := s.RefreshOnce(context.Background()); got != RefreshSwapped {
		t.Fatalf("RefreshOnce = %v; want RefreshSwapped (breaker must not short-circuit on-demand)", got)
	}
	if n := srv.calls.Load(); n != 1 {
		t.Errorf("backend calls = %d; want 1 (single attempt performed)", n)
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

	sum := reg.RefreshOwnByChannel(context.Background(), "users:777", "u1")
	if sum.Matched != 0 {
		t.Fatalf("summary = %+v; want Matched=0", sum)
	}
	if n := srv.calls.Load(); n != 0 {
		t.Errorf("backend calls = %d; want 0 (no match must not contact the backend)", n)
	}
}

// TestRefreshOnce_RaceWithFilterAndTTL exercises the atomic whitelist swap under
// concurrent WAL-hot-path reads (FilterWithLSN), on-demand refreshes (RefreshOnce),
// and a TTL tick (tryRefresh) on the same subscriber. The backend always grants
// the channel so the subscriber is never dropped; the test asserts -race cleanliness
// and that the final installed whitelist is the refreshed one.
func TestRefreshOnce_RaceWithFilterAndTTL(t *testing.T) {
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
				s.RefreshOnce(context.Background())
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

	if sum := reg.RefreshOwnByChannel(context.Background(), "users:42", "u1"); sum.Matched != 0 {
		t.Errorf("exact channel matched a wildcard subscriber: %+v", sum)
	}
	if sum := reg.RefreshOwnByChannel(context.Background(), "users:all", "u1"); sum.Matched != 1 || sum.Swapped != 1 {
		t.Errorf("wildcard channel summary = %+v; want Matched=1 Swapped=1", sum)
	}
}
