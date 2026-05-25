package auth

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

type State uint32

const (
	StateClosed State = 0

	StateOpen State = 1

	StateHalfOpen State = 2
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

type Prober interface {
	CheckAuth(ctx context.Context) error
}

type Breaker struct {
	cfg BreakerConfig
	log zerolog.Logger
	mc  *metrics.Registry

	probe  Prober
	state  atomic.Uint32
	window *window

	closeMu sync.Mutex
	closeCh chan struct{}

	openedAt atomic.Int64

	clock        func() time.Time
	tickInterval time.Duration
}

type BreakerDeps struct {
	Prober Prober

	Logger zerolog.Logger

	Metrics *metrics.Registry
}

func validateBreakerDeps(deps BreakerDeps) {
	if deps.Prober == nil {
		panic("auth.NewBreaker: Deps.Prober is required")
	}
	if deps.Metrics == nil {
		panic("auth.NewBreaker: Deps.Metrics is required")
	}
}

func NewBreaker(cfg BreakerConfig, deps BreakerDeps) *Breaker {
	validateBreakerDeps(deps)
	tick := time.Duration(cfg.BucketSeconds) * time.Second
	if tick <= 0 {
		tick = time.Second
	}
	b := &Breaker{
		cfg:          cfg,
		log:          deps.Logger,
		mc:           deps.Metrics,
		probe:        deps.Prober,
		window:       newWindow(),
		closeCh:      make(chan struct{}),
		clock:        time.Now,
		tickInterval: tick,
	}
	b.state.Store(uint32(StateClosed))
	deps.Metrics.AuthBreakerState().Set(0)
	return b
}

func (b *Breaker) Metrics() *metrics.Registry { return b.mc }

func (b *Breaker) Allow() bool {
	return State(b.state.Load()) != StateOpen
}

func (b *Breaker) State() State {
	return State(b.state.Load())
}

func (b *Breaker) RecordResult(success bool) {
	b.window.Record(success)
	if State(b.state.Load()) != StateClosed {
		return
	}
	rate, total := b.window.FailureRate()
	if total < uint64(b.cfg.DebounceFloor) {
		return
	}
	if rate <= b.cfg.FailureRateThreshold {
		return
	}
	if b.state.CompareAndSwap(uint32(StateClosed), uint32(StateOpen)) {
		b.openedAt.Store(b.clock().UnixNano())
		b.mc.AuthBreakerState().Set(1)
		b.log.Warn().
			Str("breaker_state", "open").
			Float64("failure_rate", rate).
			Uint64("total_samples", total).
			Msg("auth: circuit breaker opened")
	}
}

func (b *Breaker) WaitForClose() <-chan struct{} {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()
	return b.closeCh
}

func (b *Breaker) signalClose() {
	b.closeMu.Lock()
	old := b.closeCh
	b.closeCh = make(chan struct{})
	close(old)
	b.closeMu.Unlock()
}

func (b *Breaker) Run(ctx context.Context) {
	ticker := time.NewTicker(b.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.window.tick()
			b.tickFSM(ctx)
		}
	}
}

func (b *Breaker) tickFSM(ctx context.Context) {
	switch State(b.state.Load()) {
	case StateOpen:
		elapsed := b.clock().UnixNano() - b.openedAt.Load()
		if elapsed < int64(b.cfg.Cooldown) {
			return
		}
		if !b.state.CompareAndSwap(uint32(StateOpen), uint32(StateHalfOpen)) {
			return
		}
		b.mc.AuthBreakerState().Set(2)
		b.log.Info().Str("breaker_state", "half_open").Msg("auth: circuit breaker half-open; running probe")
		b.runProbe(ctx)
	case StateHalfOpen:

	case StateClosed:

	}
}

func (b *Breaker) runProbe(ctx context.Context) {
	if b.probe == nil {
		panic("auth.Breaker.runProbe: nil Prober (invariant violation)")
	}
	err := b.probe.CheckAuth(ctx)
	if err == nil {
		b.transitionToClosed()
		return
	}
	if _, isUnavailable := err.(*ErrUnavailable); !isUnavailable {
		b.transitionToClosed()
		return
	}
	if b.state.CompareAndSwap(uint32(StateHalfOpen), uint32(StateOpen)) {
		b.openedAt.Store(b.clock().UnixNano())
		b.mc.AuthBreakerState().Set(1)
		b.log.Warn().
			Str("breaker_state", "open").
			Err(err).
			Msg("auth: circuit breaker reopened after failed probe")
	}
}

func (b *Breaker) transitionToClosed() {
	if !b.state.CompareAndSwap(uint32(StateHalfOpen), uint32(StateClosed)) {
		return
	}
	b.openedAt.Store(0)
	b.mc.AuthBreakerState().Set(0)
	b.signalClose()
	b.log.Info().Str("breaker_state", "closed").Msg("auth: circuit breaker closed")
}

func (b *Breaker) SetClockForTest(clock func() time.Time, tickInterval time.Duration) {
	b.clock = clock
	b.tickInterval = tickInterval
}
