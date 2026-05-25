// Package limits — limits.go implements the admission-control gates.
//
// All four gates are non-blocking: AcquireGlobal / AcquirePerUser /
// AllowPreAuthRate / AllowPerUserRate return bool. Callers translate false
// to a 429/503 response with the appropriate Retry-After header.
package limits

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/time/rate"

	"github.com/walera/walera/internal/metrics"
)

// rateEntry pairs an x/time/rate.Limiter with the unix-nano timestamp of its
// most recent touch. The sweeper deletes entries with stale lastSeen.
type rateEntry struct {
	lim      *rate.Limiter
	lastSeen atomic.Int64
}

// Limits is the admission-control facade. Construct via New; safe for
// concurrent use from any goroutine.
type Limits struct {
	cfg Config
	log zerolog.Logger
	mc  *metrics.Registry

	// globalSem is the pre-auth global-concurrency semaphore. Cap =
	// cfg.GlobalConcurrent. Implemented as a buffered chan struct{}; the
	// non-blocking select-with-default in AcquireGlobal is the canonical Go
	// idiom for "try acquire" without spawning a goroutine.
	globalSem chan struct{}

	// perUserConcurrent counts in-flight SSE streams per authenticated user.
	// Stored as sync.Map[userID]*atomic.Int64 — sync.Map is the right choice
	// over map+mutex because the workload is dominated by reads
	// (LoadOrStore returns the existing entry on hot paths) and lock
	// contention dominates a plain mutex at high subscriber counts.
	perUserConcurrent sync.Map

	// perUserRate is sync.Map[userID]*rateEntry — post-auth per-user token bucket.
	perUserRate sync.Map

	// preAuthRate is sync.Map[clientIP]*rateEntry — pre-auth per-IP token bucket.
	preAuthRate sync.Map

	// trustedProxies is the parsed CIDR allowlist for XFF acceptance.
	// Populated once by New(); request-time IsTrustedProxy walks this slice
	// with no further allocation. Empty (default) means XFF is ignored
	// entirely.
	trustedProxies []*net.IPNet
}

// Deps bundles the collaborators limits.New requires at construction.
// Required fields panic on nil; Logger is the value-type exception
// (zerolog.Logger zero value is a usable Nop logger).
type Deps struct {
	// Logger is the structured logger; zero value is a usable Nop logger so
	// this field has no nil-check.
	Logger zerolog.Logger
	// Metrics is the typed Prometheus registry. Required — Limits.New
	// pre-touches the limit_rejected_total{kind=...} series so scrapers see
	// them at zero from t=0.
	Metrics *metrics.Registry
}

// validateDeps panics with the canonical message format when any required
// Deps field is nil. Logger is exempt (value-type with usable zero value).
func validateDeps(d Deps) {
	if d.Metrics == nil {
		panic("limits.New: Deps.Metrics is required")
	}
}

// New constructs a Limits. The four limit_rejected_total{kind=...} series
// are pre-touched at zero so scrapers see them from t=0.
func New(cfg Config, deps Deps) *Limits {
	validateDeps(deps)
	// Parse trusted-proxy CIDRs once. config.validate has already rejected
	// malformed entries at startup; this defensive pass
	// covers callers that bypass Load(cfg) (test fixtures, future direct
	// New() use) and skips malformed entries with a Warn rather than
	// panicking the process.
	parsed := make([]*net.IPNet, 0, len(cfg.TrustedProxies))
	for _, s := range cfg.TrustedProxies {
		_, ipNet, err := net.ParseCIDR(s)
		if err != nil {
			deps.Logger.Warn().Str("cidr", s).Err(err).Msg("limits.New: trusted_proxies entry failed to parse — skipping")
			continue
		}
		parsed = append(parsed, ipNet)
	}
	l := &Limits{
		cfg:            cfg,
		log:            deps.Logger,
		mc:             deps.Metrics,
		globalSem:      make(chan struct{}, cfg.GlobalConcurrent),
		trustedProxies: parsed,
	}
	for _, k := range []string{"global_concurrent", "per_user_concurrent", "pre_auth_rate", "per_user_rate"} {
		l.mc.LimitRejected(k).Add(0)
	}
	return l
}

// Metrics returns the registry this Limits keeper publishes counters into.
// Exposed so the composition-root singleton-identity test can compare the
// pointer every consumer received against the registry the composition
// root built.
func (l *Limits) Metrics() *metrics.Registry { return l.mc }

// IsTrustedProxy reports whether ip falls within any configured
// trusted-proxy CIDR. Returns false on a nil IP or when the allowlist
// is empty (the default behaviour ignores XFF entirely). Allocation-free
// on the hot path — the underlying []*net.IPNet was parsed once at
// New() time.
func (l *Limits) IsTrustedProxy(ip net.IP) bool {
	if ip == nil {
		return false
	}
	for _, n := range l.trustedProxies {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// AcquireGlobal attempts to reserve one slot in the global semaphore. Returns
// true on success; false (and increments the global rejection counter) when
// the semaphore is full. Pair every true return with exactly one ReleaseGlobal.
func (l *Limits) AcquireGlobal() bool {
	select {
	case l.globalSem <- struct{}{}:
		return true
	default:
		l.mc.LimitRejected("global_concurrent").Inc()
		return false
	}
}

// ReleaseGlobal returns one slot to the global semaphore. The default branch
// is defensive: a Release without a prior successful Acquire underflows the
// semaphore, which is a programmer-contract violation; the default ensures
// the call does not deadlock when this happens, and the Warn surfaces the
// violation that the silent underflow previously hid (mirrors ReleasePerUser's
// Warn).
func (l *Limits) ReleaseGlobal() {
	select {
	case <-l.globalSem:
	default:
		l.log.Warn().Msg("limits: ReleaseGlobal without prior Acquire")
	}
}

// AcquirePerUser attempts to reserve one slot in the per-user concurrency
// counter. Returns (true, newCount) on success or (false, currentCount)
// on overflow. The caller MUST call ReleasePerUser(userID) exactly once
// per successful Acquire.
//
// Implementation note: the increment is performed BEFORE the cap check so
// concurrent callers see a monotonically-increasing counter and the overflow
// detection is race-free. On overflow we decrement back — a brief overshoot
// is visible to a third concurrent caller, but the cap re-check below
// ensures convergence.
func (l *Limits) AcquirePerUser(userID string) (bool, int64) {
	v, _ := l.perUserConcurrent.LoadOrStore(userID, &atomic.Int64{})
	counter := v.(*atomic.Int64)
	n := counter.Add(1)
	if n > int64(l.cfg.PerUserConcurrentMax) {
		counter.Add(-1)
		l.mc.LimitRejected("per_user_concurrent").Inc()
		return false, n - 1
	}
	return true, n
}

// ReleasePerUser decrements the per-user concurrency counter. Logs a Warn
// (defensive) if the counter for this userID does not exist — release without
// prior acquire is a programmer-contract violation; we do not allocate a
// fresh counter just to underflow it.
func (l *Limits) ReleasePerUser(userID string) {
	v, ok := l.perUserConcurrent.Load(userID)
	if !ok {
		l.log.Warn().Str("user_id", userID).Msg("limits: ReleasePerUser without prior Acquire")
		return
	}
	v.(*atomic.Int64).Add(-1)
}

// AllowPreAuthRate consults the per-IP token bucket. On success the entry's
// lastSeen is refreshed so the sweeper does not evict it. On rejection it
// increments the pre-auth rejection counter.
func (l *Limits) AllowPreAuthRate(clientIP string) bool {
	v, _ := l.preAuthRate.LoadOrStore(clientIP, &rateEntry{
		lim: rate.NewLimiter(rate.Limit(l.cfg.PreAuthRatePerSecond), l.cfg.PreAuthBurst),
	})
	e := v.(*rateEntry)
	e.lastSeen.Store(time.Now().UnixNano())
	if !e.lim.Allow() {
		l.mc.LimitRejected("pre_auth_rate").Inc()
		return false
	}
	return true
}

// AllowPerUserRate consults the per-user token bucket.
func (l *Limits) AllowPerUserRate(userID string) bool {
	v, _ := l.perUserRate.LoadOrStore(userID, &rateEntry{
		lim: rate.NewLimiter(rate.Limit(l.cfg.PerUserRatePerSecond), l.cfg.PerUserBurst),
	})
	e := v.(*rateEntry)
	e.lastSeen.Store(time.Now().UnixNano())
	if !e.lim.Allow() {
		l.mc.LimitRejected("per_user_rate").Inc()
		return false
	}
	return true
}

// preAuthRetryAfter returns the duration to wait before the next pre-auth
// rate attempt succeeds (drives the Retry-After header on 429 responses).
// Returns a one-second default when the entry is absent — should not normally
// happen because callers consult this AFTER an AllowPreAuthRate(false),
// which Loads or Stores the entry.
//
// Implementation: Reserve() reserves a token at some future time; .Delay()
// returns how long until that token is granted; .Cancel() returns the token
// (the caller has already decided to reject — they should not consume it).
func (l *Limits) preAuthRetryAfter(clientIP string) time.Duration {
	v, ok := l.preAuthRate.Load(clientIP)
	if !ok {
		return time.Second
	}
	r := v.(*rateEntry).lim.Reserve()
	d := r.Delay()
	r.Cancel()
	return d
}

// perUserRateRetryAfter is the per-user analogue of preAuthRetryAfter.
func (l *Limits) perUserRateRetryAfter(userID string) time.Duration {
	v, ok := l.perUserRate.Load(userID)
	if !ok {
		return time.Second
	}
	r := v.(*rateEntry).lim.Reserve()
	d := r.Delay()
	r.Cancel()
	return d
}
