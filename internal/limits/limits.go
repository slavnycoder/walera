package limits

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
)

type Limits struct {
	cfg Config
	log zerolog.Logger

	globalSem chan struct{}

	perUserConcurrent sync.Map

	trustedProxies []*net.IPNet
}

type Deps struct {
	Logger zerolog.Logger
}

func New(cfg Config, deps Deps) *Limits {
	parsed := make([]*net.IPNet, 0, len(cfg.TrustedProxies))
	for _, s := range cfg.TrustedProxies {
		_, ipNet, err := net.ParseCIDR(s)
		if err != nil {
			deps.Logger.Warn().Str("cidr", s).Err(err).Msg("limits.New: trusted_proxies entry failed to parse — skipping")
			continue
		}
		parsed = append(parsed, ipNet)
	}
	return &Limits{
		cfg:            cfg,
		log:            deps.Logger,
		globalSem:      make(chan struct{}, cfg.GlobalConcurrent),
		trustedProxies: parsed,
	}
}

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

func (l *Limits) AcquireGlobal() bool {
	select {
	case l.globalSem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *Limits) ReleaseGlobal() {
	select {
	case <-l.globalSem:
	default:
		l.log.Warn().Msg("limits: ReleaseGlobal without prior Acquire")
	}
}

func (l *Limits) AcquirePerUser(userID string) (bool, int64) {
	v, _ := l.perUserConcurrent.LoadOrStore(userID, &atomic.Int64{})
	counter := v.(*atomic.Int64)
	n := counter.Add(1)
	if n > int64(l.cfg.PerUserConcurrentMax) {
		counter.Add(-1)
		return false, n - 1
	}
	return true, n
}

func (l *Limits) ReleasePerUser(userID string) {
	v, ok := l.perUserConcurrent.Load(userID)
	if !ok {
		l.log.Warn().Str("user_id", userID).Msg("limits: ReleasePerUser without prior Acquire")
		return
	}
	v.(*atomic.Int64).Add(-1)
}
