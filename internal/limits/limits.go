package limits

import (
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
)

type Limits struct {
	cfg Config
	log zerolog.Logger

	globalSem chan struct{}

	perUserConcurrent sync.Map
}

type Deps struct {
	Logger zerolog.Logger
}

func New(cfg Config, deps Deps) *Limits {
	return &Limits{
		cfg:       cfg,
		log:       deps.Logger,
		globalSem: make(chan struct{}, cfg.GlobalConcurrent),
	}
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
