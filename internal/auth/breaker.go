// Package auth — breaker.go: hand-rolled circuit-breaker FSM.
//
// States (the FSM ordering narrative below is the canonical home — do NOT
// relocate to doc.go or INVARIANTS.md; see REQUIREMENTS.md sweep constraint):
//
//   - Closed:   Allow=true. RecordResult samples the sliding window. Closed→Open
//     fires when total samples ≥ DebounceFloor AND failure rate >
//     FailureRateThreshold.
//   - Open:     Allow=false (fail-closed for new opens). After Cooldown elapses
//     the FSM goroutine flips Open→HalfOpen.
//   - HalfOpen: Allow=true. The FSM synchronously calls the Prober. nil error
//     OR any non-*ErrUnavailable error → close (backend answered).
//     *ErrUnavailable → reopen and restart Cooldown.
//
// On every Open→Closed transition signalClose installs a fresh closeCh BEFORE
// closing the old one (see signalClose). WaitForClose pairs with this so the
// stale-refresh fan-out cannot miss a wake.
package auth

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

// State is the breaker's exported enum. Lock-free reads via atomic.Uint32.
type State uint32

const (
	// StateClosed is the default state; Allow returns true.
	StateClosed State = 0
	// StateOpen rejects new opens (Allow returns false).
	StateOpen State = 1
	// StateHalfOpen runs a single probe; Allow returns true.
	StateHalfOpen State = 2
)

// String returns the lowercase state name used in zerolog fields.
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

// Prober is the dependency called by the FSM on Open→HalfOpen. Any non-network
// response (incl. 401/403/404) means the backend ANSWERED — breaker closes.
// Only *ErrUnavailable keeps it Open.
type Prober interface {
	CheckAuth(ctx context.Context) error
}

// Breaker is the hand-rolled circuit-breaker FSM. Construct via NewBreaker;
// the FSM goroutine is started by the caller via safego.Go("auth-breaker-fsm",
// func() { b.Run(ctx) }).
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

// BreakerDeps groups the collaborators required by NewBreaker.
type BreakerDeps struct {
	// Prober is invoked by the FSM on Open→HalfOpen. Required.
	Prober Prober
	// Logger receives state-transition events. Zero value is a usable Nop.
	Logger zerolog.Logger
	// Metrics receives walera_auth_circuit_breaker_state updates. Required.
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

// NewBreaker constructs a Breaker with state=Closed and a fresh sliding
// window. The walera_auth_circuit_breaker_state gauge is set to 0 immediately.
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

// Metrics returns the registry this Breaker publishes counters into.
func (b *Breaker) Metrics() *metrics.Registry { return b.mc }

// Allow reports whether the breaker permits a fresh call. Lock-free.
func (b *Breaker) Allow() bool {
	return State(b.state.Load()) != StateOpen
}

// State returns the current breaker state via a lock-free atomic load.
func (b *Breaker) State() State {
	return State(b.state.Load())
}

// RecordResult is the hot-path call from auth.Client.Permissions. Allocation-free.
// Closed→Open fires when total samples ≥ DebounceFloor AND rate >
// FailureRateThreshold; CompareAndSwap guards concurrent trip races.
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

// WaitForClose returns the channel closed on the NEXT Open→Closed transition.
// Callers re-call WaitForClose after each close to arm for the next cycle.
func (b *Breaker) WaitForClose() <-chan struct{} {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()
	return b.closeCh
}

// signalClose installs a fresh closeCh THEN closes the previous one. The
// install-before-close ordering is the correctness invariant called out in the
// package comment — concurrent WaitForClose readers never receive a closed
// channel intended for the NEXT cycle.
func (b *Breaker) signalClose() {
	b.closeMu.Lock()
	old := b.closeCh
	b.closeCh = make(chan struct{})
	close(old)
	b.closeMu.Unlock()
}

// Run is the FSM goroutine entry point. On every tick: rotate the sliding
// window THEN evaluate FSM transitions. Exits on ctx.Done().
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

// tickFSM evaluates the FSM transitions; see this file's package comment for
// the state table.
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
		// Defensive no-op — runProbe is synchronous within tickFSM.
	case StateClosed:
		// No-op; Closed→Open is the hot-path responsibility.
	}
}

// runProbe invokes the injected Prober.CheckAuth. nil OR non-*ErrUnavailable
// error → close. *ErrUnavailable → reopen + restart cooldown. A nil Prober is
// an invariant violation (validateBreakerDeps rejects at NewBreaker).
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

// transitionToClosed performs HalfOpen→Closed and broadcasts on closeCh.
func (b *Breaker) transitionToClosed() {
	if !b.state.CompareAndSwap(uint32(StateHalfOpen), uint32(StateClosed)) {
		return
	}
	b.openedAt.Store(0)
	b.mc.AuthBreakerState().Set(0)
	b.signalClose()
	b.log.Info().Str("breaker_state", "closed").Msg("auth: circuit breaker closed")
}

// SetClockForTest replaces the breaker's clock and tick interval. TEST-ONLY.
func (b *Breaker) SetClockForTest(clock func() time.Time, tickInterval time.Duration) {
	b.clock = clock
	b.tickInterval = tickInterval
}
