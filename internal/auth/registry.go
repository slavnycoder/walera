package auth

import (
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

type Subscribers struct {
	mu   sync.Mutex
	subs map[string]*Subscriber

	log zerolog.Logger
	mc  *metrics.Registry
}

type SubscribersDeps struct {
	Logger zerolog.Logger

	Metrics *metrics.Registry
}

func validateSubscribersDeps(deps SubscribersDeps) {
	if deps.Metrics == nil {
		panic("auth.NewSubscribers: Deps.Metrics is required")
	}
}

func NewSubscribers(deps SubscribersDeps) *Subscribers {
	validateSubscribersDeps(deps)
	return &Subscribers{
		subs: make(map[string]*Subscriber, 1024),
		log:  deps.Logger,
		mc:   deps.Metrics,
	}
}

func (r *Subscribers) Metrics() *metrics.Registry { return r.mc }

func (r *Subscribers) Add(s *Subscriber) {
	r.mu.Lock()
	r.subs[s.ID()] = s
	r.mu.Unlock()
}

func (r *Subscribers) Remove(id string) {
	r.mu.Lock()
	delete(r.subs, id)
	r.mu.Unlock()
}

func (r *Subscribers) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.subs)
}

type RefreshSummary struct {
	Matched int
	Swapped int
	Revoked int
	Busy    int
}

// RefreshOwnByChannel swaps fresh (from the caller's OpenSession) into userID's live
// subs on channel. Exact match: no wildcard cross-match, no touching other users.
// A sub refreshed after fetchedAt is newer than fresh and is left as is.
func (r *Subscribers) RefreshOwnByChannel(channel, userID string, fresh *Whitelist, fetchedAt time.Time) RefreshSummary {
	r.mu.Lock()
	matched := make([]*Subscriber, 0, 4)
	for _, s := range r.subs {
		if s.channel == channel && s.userID == userID {
			matched = append(matched, s)
		}
	}
	r.mu.Unlock()

	sum := RefreshSummary{Matched: len(matched)}
	for _, s := range matched {
		if !s.refreshMu.TryLock() {
			sum.Busy++
			continue
		}
		if s.lastRefresh.Load() > fetchedAt.UnixNano() {
			sum.Swapped++ // already current
		} else if s.applyFetched(fresh, nil) == attemptRevoked {
			sum.Revoked++
		} else {
			sum.Swapped++
		}
		s.refreshMu.Unlock()
	}
	return sum
}
