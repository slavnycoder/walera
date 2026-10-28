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

	// refreshMu guards one fetch+swap. Shared by the TTL chain (per attempt, never
	// across a backoff sleep) and on-demand RefreshOwnByChannel.
	refreshMu sync.Mutex
	// chainMu dedups concurrent TTL/breaker-fan-out chains and is held across
	// backoff sleeps. Lock order: chainMu → refreshMu.
	chainMu sync.Mutex

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

// applyFetched turns a RefreshPermissions result into the swap/drop decision: on
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

// refreshAttempt performs one TTL permission re-fetch and applies it. It does not
// drop on a transient error; the caller decides. Must hold refreshMu.
func (s *Subscriber) refreshAttempt(ctx context.Context, attempt int, requestID string) attemptResult {
	fresh, err := s.client.RefreshPermissions(ctx, s.userID, s.channel, requestID)
	s.recordRefreshResult(err)
	if err != nil {
		s.logRefreshError(err, attempt)
	}
	return s.applyFetched(fresh, err)
}

// tryRefresh runs the TTL/breaker-fan-out refresh chain: an immediate attempt
// followed by the bounded backoff retry chain. chainMu (not refreshMu) dedups
// concurrent chains, so the swap lock is released for the whole duration of
// every backoff sleep and an on-demand RefreshOwnByChannel can swap fresh permissions
// while this chain waits between retries (previously it was starved for the full
// backoff window, up to ~13s). Each individual fetch+swap runs under refreshMu
// via lockedAttempt, and every auth_unavailable drop goes through dropIfUnchanged
// so the drop decision is serialized under refreshMu against an in-flight
// on-demand swap. Behavior in the pure-TTL case (no concurrent on-demand swap)
// is unchanged: same attempt count, same drop/breaker semantics.
func (s *Subscriber) tryRefresh(ctx context.Context) {
	if !s.chainMu.TryLock() {
		return
	}
	defer s.chainMu.Unlock()

	// Snapshot the live whitelist pointer at chain start, before the first
	// attempt. swapMap is the only writer of AuthMap after construction and
	// always stores a fresh pointer, so a changed pointer means a concurrent
	// successful refresh (an on-demand RefreshOwnByChannel that grabbed refreshMu) has
	// already brought the stream current — the chain must then stop without
	// dropping. Sampling here, not after attempt 0, closes the window where an
	// on-demand swap landing during attempt 0 would otherwise be adopted as the
	// baseline and the fresh stream still dropped at the end of the chain.
	mapAtChainStart := s.AuthMap.Load()

	switch s.lockedAttempt(ctx, 0, newRequestID()) {
	case attemptSwapped, attemptRevoked:
		return
	case attemptFailed:
		s.dropIfUnchanged(mapAtChainStart)
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
		// Another path (e.g. an on-demand revocation, which drops the sub
		// without swapping AuthMap) may have dropped it while we slept; stop
		// rather than fetch and swap onto a dead subscriber.
		select {
		case <-s.Sub.Done():
			return
		default:
		}
		if s.AuthMap.Load() != mapAtChainStart {
			return
		}
		switch s.lockedAttempt(ctx, i+1, newRequestID()) {
		case attemptSwapped, attemptRevoked:
			return
		case attemptFailed:
			s.dropIfUnchanged(mapAtChainStart)
			return
		case attemptUnavailable:
			// keep retrying
		}
	}

	if s.breaker == nil || s.breaker.Allow() {
		s.dropIfUnchanged(mapAtChainStart)
	}
}

// lockedAttempt runs exactly one refreshAttempt under refreshMu — the swap lock
// shared with on-demand RefreshOwnByChannel and the breaker fan-out. It is the only
// place the TTL chain takes refreshMu for a fetch, and it holds the lock only
// for that single fetch+swap, never across a backoff sleep, so on-demand refresh
// is not starved while the chain retries.
func (s *Subscriber) lockedAttempt(ctx context.Context, attempt int, requestID string) attemptResult {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	return s.refreshAttempt(ctx, attempt, requestID)
}

// dropIfUnchanged drops the subscriber (auth_unavailable) only if no concurrent
// refresh swapped a fresh whitelist since mapAtChainStart was sampled. The check
// and drop run under refreshMu so the decision is serialized against an in-flight
// on-demand RefreshOwnByChannel: if one is mid-fetch it holds refreshMu, so this blocks
// until it stores its result, then observes the changed pointer and skips the
// drop — never killing a stream that was just successfully refreshed. Dropping
// under refreshMu mirrors applyFetched's auth_revoked drop.
func (s *Subscriber) dropIfUnchanged(mapAtChainStart *Whitelist) {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	if s.AuthMap.Load() == mapAtChainStart {
		s.Sub.Drop("auth_unavailable")
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
	// Copy so one whitelist can be applied to K subscribers, each stamped with its
	// own LSN. Tables is read-only after decode and safe to share. InitialData is
	// a handshake-only payload; don't keep it alive per subscriber.
	cp := *fresh
	cp.InitialData = nil
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
