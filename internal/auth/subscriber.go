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

type breakerHook interface {
	Allow() bool
}

var defaultBackoffs = []time.Duration{1 * time.Second, 3 * time.Second, 9 * time.Second}

type SubscriberConfig struct {
	InitialMap *Whitelist

	UserID string

	Channel string

	DefaultTTL time.Duration

	Backoffs []time.Duration
}

type SubscriberDeps struct {
	Sub *router.Subscriber

	Client *Client

	Breaker breakerHook

	Logger zerolog.Logger

	Metrics *metrics.Registry

	NowFunc func() time.Time

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

type Subscriber struct {
	Sub *router.Subscriber

	AuthMap atomic.Pointer[Whitelist]

	PrevWhitelist atomic.Pointer[Whitelist]

	client  *Client
	breaker breakerHook

	refreshMu sync.Mutex

	lastRefresh atomic.Int64

	log zerolog.Logger
	mc  *metrics.Registry

	userID  string
	channel string
	ttl     time.Duration

	now      func() time.Time
	lsn      func() pglogrepl.LSN
	backoffs []time.Duration
}

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
	// Periodic refresh is gated by the walera config (auth.default_ttl_seconds).
	// When the operator hasn't opted in (DefaultTTL <= 0), the feature stays
	// off — the auth-backend's per-response ttl_seconds cannot enable it.
	var ttl time.Duration
	if cfg.DefaultTTL > 0 {
		ttl = cfg.DefaultTTL
		if cfg.InitialMap != nil && cfg.InitialMap.TTLSeconds > 0 {
			ttl = time.Duration(cfg.InitialMap.TTLSeconds) * time.Second
		}
	}
	userID := cfg.UserID
	if userID == "" && cfg.InitialMap != nil {
		userID = cfg.InitialMap.UserID
	}
	s := &Subscriber{
		Sub:      deps.Sub,
		client:   deps.Client,
		breaker:  deps.Breaker,
		log:      deps.Logger,
		mc:       deps.Metrics,
		userID:   userID,
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

func (s *Subscriber) ID() string { return s.Sub.ID() }

func (s *Subscriber) LastRefresh() time.Time {
	return time.Unix(0, s.lastRefresh.Load())
}

func (s *Subscriber) FilterClosure() func(c wal.Change) (wal.Change, bool) {
	return func(c wal.Change) (wal.Change, bool) {
		m := s.AuthMap.Load()
		if m == nil {
			return c, true
		}
		return m.Filter(c)
	}
}

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

// attemptResult classifies a single fetch+swap cycle.
type attemptResult int

const (
	attemptSwapped     attemptResult = iota // fresh permissions fetched and swapped
	attemptRevoked                          // channel no longer granted; subscriber dropped (auth_revoked)
	attemptUnavailable                      // *ErrUnavailable (transient backend failure)
	attemptFailed                           // other (non-revoked, non-unavailable) error
)

// fetchPermissions performs one backend permission re-fetch and records the
// outcome metric. It does NO logging and NO whitelist swap, and holds no lock —
// the returned (*Whitelist, error) is handed to applyFetched, which makes the
// swap/drop decision under refreshMu. Splitting fetch from apply lets a single
// backend round-trip fan out to every subscriber on the same (user, channel)
// without a K-fold round-trip (see Subscribers.RefreshOwnByChannel).
func (s *Subscriber) fetchPermissions(ctx context.Context, requestID string) (*Whitelist, error) {
	fresh, err := s.client.RefreshPermissions(ctx, s.userID, s.channel, requestID)
	s.recordRefreshResult(err)
	return fresh, err
}

// applyFetched turns a fetchPermissions result into the swap/drop decision: on
// success it performs the atomic whitelist swap (or drops auth_revoked when the
// channel is no longer granted); on a definitive revocation error it drops the
// subscriber; a transient *ErrUnavailable leaves the stream intact
// (attemptUnavailable) and any other error is attemptFailed. It does NOT drop on
// a transient/unexpected error — that decision is left to the caller, because
// the TTL path drops while the on-demand (best-effort) path keeps the stream
// alive. It performs the swap and so MUST be called with refreshMu held. It does
// NOT log — logging is single-sourced by the caller (TTL: refreshAttempt;
// on-demand: serveRefresh).
func (s *Subscriber) applyFetched(fresh *Whitelist, err error) attemptResult {
	if err == nil {
		if !s.refreshMapAllowed(fresh) {
			s.Sub.Drop("auth_revoked")
			return attemptRevoked
		}
		s.swapMap(fresh)
		return attemptSwapped
	}
	if isRevokedErr(err) {
		s.Sub.Drop("auth_revoked")
		return attemptRevoked
	}
	if _, isUnavailable := err.(*ErrUnavailable); !isUnavailable {
		return attemptFailed
	}
	return attemptUnavailable
}

// refreshAttempt performs one TTL permission re-fetch and, on success, the
// atomic whitelist swap; on a definitive revocation it drops the subscriber. It
// records the metric (via fetchPermissions) and logs the failure (TTL-gated)
// exactly as before. It does NOT drop the subscriber on a transient/unexpected
// error — that decision is left to the caller. Must be called with refreshMu
// held. logRefreshError lives here and only here, so the on-demand path can
// route through fetchPermissions+applyFetched without a duplicate warn.
func (s *Subscriber) refreshAttempt(ctx context.Context, attempt int, requestID string) attemptResult {
	fresh, err := s.fetchPermissions(ctx, requestID)
	if err != nil {
		s.logRefreshError(err, attempt)
	}
	return s.applyFetched(fresh, err)
}

func (s *Subscriber) tryRefresh(ctx context.Context) {
	if !s.refreshMu.TryLock() {
		return
	}
	defer s.refreshMu.Unlock()

	switch s.refreshAttempt(ctx, 0, newRequestID()) {
	case attemptSwapped, attemptRevoked:
		return
	case attemptFailed:
		s.Sub.Drop("auth_unavailable")
		return
	case attemptUnavailable:
		// fall through to the backoff retry chain
	}

	for i, backoff := range s.backoffs {
		select {
		case <-ctx.Done():
			return
		case <-s.Sub.Done():
			return
		case <-time.After(backoff):
		}
		switch s.refreshAttempt(ctx, i+1, newRequestID()) {
		case attemptSwapped, attemptRevoked:
			return
		case attemptFailed:
			s.Sub.Drop("auth_unavailable")
			return
		case attemptUnavailable:
			// keep retrying
		}
	}

	if s.breaker == nil || s.breaker.Allow() {
		s.Sub.Drop("auth_unavailable")
	}
}

// RefreshResult is the outcome of an on-demand synchronous refresh (RefreshOnce).
type RefreshResult int

const (
	// RefreshSwapped: fresh permissions were fetched and the whitelist swapped.
	RefreshSwapped RefreshResult = iota
	// RefreshRevoked: the channel is no longer granted; the subscriber was
	// dropped with reason auth_revoked (identical to the TTL path).
	RefreshRevoked
	// RefreshUnavailable: the backend re-fetch failed (transient or unexpected).
	// The live whitelist is left intact and the stream is NOT dropped.
	RefreshUnavailable
	// RefreshBusy: another refresh (a TTL tick or breaker fan-out) currently
	// holds refreshMu, so no fetch was performed by this call.
	RefreshBusy
)

// RefreshOnce performs a single synchronous permission re-fetch and atomic
// whitelist swap. It shares refreshMu — and therefore the swap/drop/guard
// primitives — with the TTL loop and the breaker fan-out, so it can never race
// or double-swap a concurrent refresh. Like the TTL loop it is gated by the
// AUTH-04 breaker: when the breaker is open it returns RefreshUnavailable
// without contacting the failing backend (so on-demand POSTs don't hammer it
// and delay half-open). Unlike the TTL tryRefresh it runs NO backoff retries: a
// transient backend failure returns RefreshUnavailable and leaves the current
// whitelist in place (best-effort, the stream is preserved). A definitive
// revocation drops the subscriber (auth_revoked), exactly as TTL does. If a
// concurrent refresh holds the lock, it returns RefreshBusy without contacting
// the backend. It does NOT log — on-demand failures are logged once in
// serveRefresh.
func (s *Subscriber) RefreshOnce(ctx context.Context, requestID string) RefreshResult {
	if s.breaker != nil && !s.breaker.Allow() {
		return RefreshUnavailable
	}
	if !s.refreshMu.TryLock() {
		return RefreshBusy
	}
	defer s.refreshMu.Unlock()

	fresh, err := s.fetchPermissions(ctx, requestID)
	switch s.applyFetched(fresh, err) {
	case attemptSwapped:
		return RefreshSwapped
	case attemptRevoked:
		return RefreshRevoked
	default: // attemptUnavailable, attemptFailed
		return RefreshUnavailable
	}
}

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

func (s *Subscriber) swapMap(fresh *Whitelist) {
	if fresh == nil {
		return
	}
	// Shallow-clone so one fetched whitelist can be fanned out to K subscribers
	// on the same (user, channel) without a data race on RefreshLSN: each sub
	// stores its own copy stamped with its own LSN. The Tables map is read-only
	// after decode, so sharing it across copies is safe.
	cp := *fresh
	cp.RefreshLSN = s.lsn()
	s.PrevWhitelist.Store(s.AuthMap.Load())
	s.AuthMap.Store(&cp)
	s.lastRefresh.Store(s.now().UnixNano())
}

func isRevokedErr(err error) bool {
	switch err.(type) {
	case *ErrUnauthorized, *ErrForbidden, *ErrNotFound:
		return true
	}
	return false
}

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

// logRefreshError emits a warn-level entry on every failed refresh attempt
// while the TTL feature is enabled (ttl > 0). The docs link gives on-call
// engineers a direct pointer to the auth contract without leaking the
// channel or permission payload.
func (s *Subscriber) logRefreshError(err error, attempt int) {
	if s.ttl <= 0 {
		return
	}
	s.log.Warn().
		Err(err).
		Str("subscriber_id", s.Sub.ID()).
		Str("user_id", s.userID).
		Str("result", refreshResultLabel(err)).
		Int("attempt", attempt).
		Str("docs", DocsURL).
		Msg("auth permission refresh failed")
}

func (s *Subscriber) recordRefreshResult(err error) {
	if s.mc == nil {
		return
	}
	s.mc.AuthRefresh(refreshResultLabel(err)).Inc()
}

func newRequestID() string {
	var buf [16]byte
	if _, err := cryptorand.Read(buf[:]); err != nil {
		panic("auth: crypto/rand.Read failed: " + err.Error())
	}
	return hex.EncodeToString(buf[:])
}
