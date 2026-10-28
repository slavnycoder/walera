package auth

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	mathrand "math/rand/v2"
	"strings"
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

var defaultBackoffs = []time.Duration{1 * time.Second, 3 * time.Second, 9 * time.Second}

type SubscriberConfig struct {
	InitialMap *Whitelist

	UserID string

	Channel string

	DefaultTTL time.Duration

	Backoffs []time.Duration

	// Bearer and Forwarded are the client's own credentials, replayed on every
	// refresh so logout and token expiry reach a live stream. Never logged.
	Bearer    string
	Forwarded ForwardedAuth
}

type SubscriberDeps struct {
	Sub *router.Subscriber

	Client *Client

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

	client *Client
	bearer string
	fwd    ForwardedAuth

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
		bearer:   cfg.Bearer,
		fwd:      cfg.Forwarded,
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
			s.tryRefresh(ctx)
		}
	}
}

type attemptResult int

const (
	attemptSwapped     attemptResult = iota // fresh permissions fetched and swapped
	attemptRevoked                          // channel no longer granted; subscriber dropped (auth_revoked)
	attemptUnavailable                      // *ErrUnavailable (transient backend failure)
	attemptFailed                           // other (non-revoked, non-unavailable) error
)

// applyFetched swaps or drops auth_revoked; transient errors are left to the caller.
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

// tryRefresh re-opens the session with the client's credentials. Only
// *ErrUnavailable is retried; every other outcome swaps or drops.
func (s *Subscriber) tryRefresh(ctx context.Context) {
	for attempt := 0; ; attempt++ {
		fresh, err := s.client.OpenSession(ctx, s.bearer, s.fwd, s.channel, newRequestID())
		s.recordRefreshResult(err)
		if err != nil {
			s.logRefreshError(err, attempt)
		}
		switch s.applyFetched(fresh, err) {
		case attemptSwapped, attemptRevoked:
			return
		case attemptFailed:
			s.Sub.Drop("auth_unavailable")
			return
		case attemptUnavailable:
		}
		if attempt == len(s.backoffs) {
			s.Sub.Drop("auth_unavailable")
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-s.Sub.Done():
			return
		case <-time.After(s.backoffs[attempt]):
		}
	}
}

func (s *Subscriber) refreshMapAllowed(fresh *Whitelist) bool {
	table := tableFromChannel(s.channel)
	if table == "" || fresh == nil || fresh.UserID != s.userID {
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
	// Copy: one whitelist is shared by K subs, each needs its own LSN (Tables is
	// read-only). InitialData is handshake-only; don't retain it per sub.
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
