// Package auth — subscriber.go: per-connection auth state machine.
// See internal/auth/INVARIANTS.md Concurrency §2 (Whitelist.Swap atomic
// publication) and §3 (LSN stamping order). Security: s.token is never logged.
package auth

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	mathrand "math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/wal"
)

// initialJitterFunc computes the startup-jitter for one Subscriber's
// RefreshLoop. Package-level var so the jitter-distribution test can sample
// it directly. Default: uniform in [0, ttl/2). Production callers MUST NOT
// reassign this.
var initialJitterFunc = func(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0
	}
	half := int64(ttl / 2)
	if half <= 0 {
		return 0
	}
	return time.Duration(mathrand.Int64N(half))
}

// breakerHook is the minimal Allow() seam the Subscriber needs from a Breaker.
// Tests inject a stub; production always passes a real *Breaker.
type breakerHook interface {
	Allow() bool
}

// defaultBackoffs is the retry schedule for *ErrUnavailable. Three retries
// totaling ~13s before drop(auth_unavailable).
var defaultBackoffs = []time.Duration{1 * time.Second, 3 * time.Second, 9 * time.Second}

// SubscriberConfig groups the koanf-derivable values for NewSubscriber.
type SubscriberConfig struct {
	// InitialMap is the Whitelist installed at construction. Must be non-nil.
	InitialMap *Whitelist

	// Token is the user's bearer token. Stored ONLY as a struct field; never
	// logged.
	Token string

	// Channel is the subscription channel ("entity:id"); passed verbatim into
	// every Client.Permissions call.
	Channel string

	// DefaultTTL is the refresh interval used when InitialMap.TTLSeconds is zero.
	DefaultTTL time.Duration

	// Backoffs is the *ErrUnavailable retry sequence. Nil → defaultBackoffs.
	Backoffs []time.Duration
}

// SubscriberDeps groups the collaborators for NewSubscriber.
type SubscriberDeps struct {
	// Sub is the router-level subscriber owning the SSE connection lifecycle.
	// Required.
	Sub *router.Subscriber

	// Client performs the periodic /auth/permissions refresh call. Required.
	Client *Client

	// Breaker is consulted at every refresh tick. Nil means "always allow".
	Breaker breakerHook

	// Logger zero value is a usable Nop.
	Logger zerolog.Logger

	// Metrics receives walera_auth_refresh_total{result} increments. Required.
	Metrics *metrics.Registry

	// NowFunc is the wall-clock source. Nil → time.Now.
	NowFunc func() time.Time

	// LSNFunc returns the current WAL commit LSN; used to stamp
	// Whitelist.RefreshLSN on every successful refresh. Nil → wal.CurrentLSN.
	LSNFunc func() pglogrepl.LSN
}

func validateSubscriberDeps(deps SubscriberDeps) {
	if deps.Sub == nil {
		panic("auth.NewSubscriber: Deps.Sub is required")
	}
	if deps.Client == nil {
		panic("auth.NewSubscriber: Deps.Client is required")
	}
	if deps.Metrics == nil {
		panic("auth.NewSubscriber: Deps.Metrics is required")
	}
}

// Subscriber is the per-connection auth state. See package doc.
type Subscriber struct {
	// Sub is the embedded router subscriber (composition).
	Sub *router.Subscriber

	// AuthMap is the live per-user permission snapshot. atomic.Pointer for
	// lock-free hot-path loads on every tx dispatch.
	AuthMap atomic.Pointer[Whitelist]
	// PrevWhitelist is the 1-slot back-buffer; see FilterWithLSN.
	PrevWhitelist atomic.Pointer[Whitelist]

	client  *Client
	breaker breakerHook

	refreshMu sync.Mutex

	lastRefresh atomic.Int64

	log zerolog.Logger
	mc  *metrics.Registry

	token   string
	channel string
	ttl     time.Duration

	now      func() time.Time
	lsn      func() pglogrepl.LSN
	backoffs []time.Duration
}

// NewSubscriber constructs a Subscriber. InitialMap is installed into AuthMap
// immediately so FilterClosure works before the first refresh tick.
func NewSubscriber(cfg SubscriberConfig, deps SubscriberDeps) *Subscriber {
	validateSubscriberDeps(deps)
	now := deps.NowFunc
	if now == nil {
		now = time.Now
	}
	lsn := deps.LSNFunc
	if lsn == nil {
		lsn = wal.CurrentLSN
	}
	backoffs := cfg.Backoffs
	if backoffs == nil {
		backoffs = defaultBackoffs
	}
	ttl := cfg.DefaultTTL
	if cfg.InitialMap != nil && cfg.InitialMap.TTLSeconds > 0 {
		ttl = time.Duration(cfg.InitialMap.TTLSeconds) * time.Second
	}
	s := &Subscriber{
		Sub:      deps.Sub,
		client:   deps.Client,
		breaker:  deps.Breaker,
		log:      deps.Logger,
		mc:       deps.Metrics,
		token:    cfg.Token,
		channel:  cfg.Channel,
		ttl:      ttl,
		now:      now,
		lsn:      lsn,
		backoffs: backoffs,
	}
	s.AuthMap.Store(cfg.InitialMap)
	s.lastRefresh.Store(now().UnixNano())
	return s
}

// ID returns the underlying router subscriber's identifier.
func (s *Subscriber) ID() string { return s.Sub.ID() }

// LastRefresh returns the wall-clock time of the last successful refresh.
func (s *Subscriber) LastRefresh() time.Time {
	return time.Unix(0, s.lastRefresh.Load())
}

// FilterClosure returns a 1-arg filter closure matching router.Subscriber.Filter.
// Always uses AuthMap (cannot honor the back-buffer rule without an LSN arg —
// see FilterWithLSN).
func (s *Subscriber) FilterClosure() func(c wal.Change) (wal.Change, bool) {
	return func(c wal.Change) (wal.Change, bool) {
		m := s.AuthMap.Load()
		if m == nil {
			return c, true
		}
		return m.Filter(c)
	}
}

// FilterWithLSN is the back-buffer-honoring filter. Use AuthMap when
// txCommitLSN > AuthMap.RefreshLSN; otherwise consult PrevWhitelist (drop
// conservatively if nil).
func (s *Subscriber) FilterWithLSN(c wal.Change, txCommitLSN pglogrepl.LSN) (wal.Change, bool) {
	m := s.AuthMap.Load()
	if m == nil {
		return c, true
	}
	if txCommitLSN <= m.RefreshLSN {
		prev := s.PrevWhitelist.Load()
		if prev == nil {
			return c, true
		}
		m = prev
	}
	return m.Filter(c)
}

// RefreshLoop is the background refresh ticker. Initial jitter [0, ttl/2) at
// startup prevents the thundering herd. Exits on ctx.Done() OR Sub.Done().
func (s *Subscriber) RefreshLoop(ctx context.Context) {
	jitter := initialJitterFunc(s.ttl)
	if jitter > 0 {
		select {
		case <-time.After(jitter):
		case <-ctx.Done():
			return
		case <-s.Sub.Done():
			return
		}
	}

	if s.ttl <= 0 {
		return
	}
	ticker := time.NewTicker(s.ttl)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.Sub.Done():
			return
		case <-ticker.C:
			if s.breaker != nil && !s.breaker.Allow() {
				continue
			}
			s.tryRefresh(ctx)
		}
	}
}

// tryRefresh performs one refresh attempt. Coalesces concurrent calls via
// TryLock. On success: see swapMap for ordering (INVARIANTS.md §2-3).
func (s *Subscriber) tryRefresh(ctx context.Context) {
	if !s.refreshMu.TryLock() {
		return
	}
	defer s.refreshMu.Unlock()

	fresh, err := s.client.Permissions(ctx, s.token, s.channel, newRequestID())
	s.recordRefreshResult(err)
	if err == nil {
		if !s.refreshMapAllowed(fresh) {
			s.Sub.Drop("auth_revoked")
			return
		}
		s.swapMap(fresh)
		return
	}

	if isRevokedErr(err) {
		s.Sub.Drop("auth_revoked")
		return
	}

	if _, isUnavailable := err.(*ErrUnavailable); !isUnavailable {
		s.Sub.Drop("auth_unavailable")
		return
	}

	for _, backoff := range s.backoffs {
		select {
		case <-ctx.Done():
			return
		case <-s.Sub.Done():
			return
		case <-time.After(backoff):
		}
		fresh, err = s.client.Permissions(ctx, s.token, s.channel, newRequestID())
		s.recordRefreshResult(err)
		if err == nil {
			if !s.refreshMapAllowed(fresh) {
				s.Sub.Drop("auth_revoked")
				return
			}
			s.swapMap(fresh)
			return
		}
		if isRevokedErr(err) {
			s.Sub.Drop("auth_revoked")
			return
		}
		if _, isUnavailable := err.(*ErrUnavailable); !isUnavailable {
			s.Sub.Drop("auth_unavailable")
			return
		}
	}

	// All retries exhausted: drop only if the breaker is still closed
	// (bounded fail-open).
	if s.breaker == nil || s.breaker.Allow() {
		s.Sub.Drop("auth_unavailable")
	}
}

// refreshMapAllowed re-applies the SSE-handshake table gate before publishing
// a refreshed map. A successful response can still revoke this subscription.
func (s *Subscriber) refreshMapAllowed(fresh *Whitelist) bool {
	table := tableFromChannel(s.channel)
	if table == "" || fresh == nil {
		return false
	}
	_, ok := fresh.Tables[table]
	return ok
}

func tableFromChannel(channel string) string {
	table, _, _ := strings.Cut(channel, ":")
	if table == "" {
		return ""
	}
	if _, suffix, ok := strings.Cut(table, "."); ok {
		table = suffix
	}
	return table
}

// swapMap is the ordering primitive for atomic permission-map publication.
// See INVARIANTS.md Concurrency §2 (atomic publication) and §3 (LSN stamping
// order): stamp fresh.RefreshLSN BEFORE any Store; demote AuthMap →
// PrevWhitelist; promote fresh → AuthMap.
func (s *Subscriber) swapMap(fresh *Whitelist) {
	if fresh == nil {
		return
	}
	fresh.RefreshLSN = s.lsn()
	s.PrevWhitelist.Store(s.AuthMap.Load())
	s.AuthMap.Store(fresh)
	s.lastRefresh.Store(s.now().UnixNano())
}

// isRevokedErr reports whether err is one of the three 4xx classes that map to
// the auth_revoked disposition.
func isRevokedErr(err error) bool {
	switch err.(type) {
	case *ErrUnauthorized, *ErrForbidden, *ErrNotFound:
		return true
	}
	return false
}

// refreshResultLabel maps a Permissions() outcome to the
// walera_auth_refresh_total{result} label.
func refreshResultLabel(err error) string {
	if err == nil {
		return "ok"
	}
	switch err.(type) {
	case *ErrUnauthorized:
		return "unauthorized"
	case *ErrForbidden:
		return "forbidden"
	case *ErrNotFound:
		return "not_found"
	}
	return "unavailable"
}

// recordRefreshResult increments walera_auth_refresh_total{result=<label>}.
func (s *Subscriber) recordRefreshResult(err error) {
	if s.mc == nil {
		return
	}
	s.mc.AuthRefresh(refreshResultLabel(err)).Inc()
}

// newRequestID generates a 32-char hex request ID for the X-Request-ID header.
func newRequestID() string {
	var buf [16]byte
	if _, err := cryptorand.Read(buf[:]); err != nil {
		panic("auth: crypto/rand.Read failed: " + err.Error())
	}
	return hex.EncodeToString(buf[:])
}
