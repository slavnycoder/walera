// Package auth — registry.go: in-memory index of every live *Subscriber and
// the breaker-aware stale-refresh watcher.
// See INVARIANTS.md Concurrency §4 (stale-subs sampler semantics).
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

// Subscribers is the in-memory index of every live *Subscriber. Construct via
// NewSubscribers; install via the SSE handler (Add on handshake, Remove on
// Sub.Done()).
type Subscribers struct {
	mu   sync.Mutex
	subs map[string]*Subscriber

	log zerolog.Logger
	mc  *metrics.Registry
}

// SubscribersDeps groups the collaborators required by NewSubscribers.
type SubscribersDeps struct {
	// Logger zero value is a usable Nop.
	Logger zerolog.Logger
	// Metrics receives the stale-subscribers gauge updates. Required.
	Metrics *metrics.Registry
}

func validateSubscribersDeps(deps SubscribersDeps) {
	if deps.Metrics == nil {
		panic("auth.NewSubscribers: Deps.Metrics is required")
	}
}

// NewSubscribers constructs an empty Subscribers pre-allocated for 1024
// subscribers (grows naturally beyond).
func NewSubscribers(deps SubscribersDeps) *Subscribers {
	validateSubscribersDeps(deps)
	return &Subscribers{
		subs: make(map[string]*Subscriber, 1024),
		log:  deps.Logger,
		mc:   deps.Metrics,
	}
}

// Metrics returns the registry this Subscribers index publishes counters into.
func (r *Subscribers) Metrics() *metrics.Registry { return r.mc }

// Add installs s into the registry keyed by s.ID(). A duplicate ID overwrites
// the existing entry (last-writer-wins).
func (r *Subscribers) Add(s *Subscriber) {
	r.mu.Lock()
	r.subs[s.ID()] = s
	r.mu.Unlock()
}

// Remove deletes the subscriber with the given id. No-op if absent.
func (r *Subscribers) Remove(id string) {
	r.mu.Lock()
	delete(r.subs, id)
	r.mu.Unlock()
}

// Len returns the current subscriber count.
func (r *Subscribers) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.subs)
}

// WatchBreaker is the stale-refresh watcher loop. On every Open→Closed
// transition: walk the registry, identify subscribers whose lastRefresh
// predates `now - ttlSeconds`, schedule a one-shot refresh per stale sub with
// uniform jitter in [0, jitter). Re-arms by re-fetching b.WaitForClose() each
// iteration (the channel is recreated on every close cycle).
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

// fanoutStaleRefreshes identifies stale subscribers and schedules a jittered
// one-shot tryRefresh per sub. The mutex is RELEASED before scheduling so a
// long jitter window does not stall Add/Remove (copy-before-unlock). The
// walera_auth_breaker_stale_subscribers gauge is set to len(stale).
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
