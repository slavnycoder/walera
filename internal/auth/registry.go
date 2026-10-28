package auth

import (
	"context"
	mathrand "math/rand/v2"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/safego"
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

// RefreshSummary aggregates the per-subscriber outcomes of an on-demand refresh.
type RefreshSummary struct {
	Matched     int
	Swapped     int
	Revoked     int
	Unavailable int
	Busy        int
}

// RefreshOwnByChannel synchronously refreshes the live subscribers owned by
// userID and subscribed to channel (the caller's-own scope used by
// POST /sse/v1/refresh). Matching subscribers are snapshotted under the registry
// lock, then each is refreshed off-lock — RefreshOnce performs a blocking backend
// call and must never run while r.mu is held (mirrors fanoutStaleRefreshes). The
// match is exact string equality on both channel and userID, so a wildcard
// subscriber is not refreshed by an exact channel and vice versa, and one user's
// POST can never refresh another user's stream. Returns the aggregate outcome.
func (r *Subscribers) RefreshOwnByChannel(ctx context.Context, channel, userID string) RefreshSummary {
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
		switch s.RefreshOnce(ctx) {
		case RefreshSwapped:
			sum.Swapped++
		case RefreshRevoked:
			sum.Revoked++
		case RefreshUnavailable:
			sum.Unavailable++
		case RefreshBusy:
			sum.Busy++
		}
	}
	return sum
}

func (r *Subscribers) WatchBreaker(ctx context.Context, b *Breaker, jitter time.Duration, ttlSeconds int) {
	for {
		ch := b.WaitForClose()
		select {
		case <-ctx.Done():
			return
		case <-ch:
			r.fanoutStaleRefreshes(jitter, ttlSeconds)
		}
	}
}

func (r *Subscribers) fanoutStaleRefreshes(jitter time.Duration, ttlSeconds int) {
	now := time.Now().UnixNano()
	staleCutoff := now - int64(ttlSeconds)*int64(time.Second)

	r.mu.Lock()
	stale := make([]*Subscriber, 0, len(r.subs))
	for _, s := range r.subs {
		if s.lastRefresh.Load() < staleCutoff {
			stale = append(stale, s)
		}
	}
	r.mu.Unlock()

	r.mc.AuthBreakerStaleSubs().Set(float64(len(stale)))

	for _, s := range stale {
		s2 := s
		var delay time.Duration
		if jitter > 0 {
			delay = time.Duration(mathrand.Int64N(int64(jitter)))
		}
		time.AfterFunc(delay, func() {
			safego.Go("auth-stale-refresh-"+s2.ID(), func() {
				s2.tryRefresh(context.Background())
			})
		})
	}
}
