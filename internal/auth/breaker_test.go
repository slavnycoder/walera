// Package auth — breaker_test.go validates the hand-rolled circuit-breaker
// FSM.
//
// All tests use synthetic clocks (no time.Sleep) and drive transitions by
// calling tickFSM directly. The compile-time interface assertion at the top
// of the file proves *Breaker satisfies the BreakerHook contract from
// client.go.
package auth

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

// Compile-time interface-satisfaction proofs for *Breaker and *Client
// live in assertions.go so `go build ./...` enforces them (REVIEW.md
// IN-03). Keeping them out of this _test.go file means a future refactor
// that breaks the contract fails the build, not just the test run.

// proberFunc adapts a func(ctx) error stub to the Prober interface so
// existing closure-based test probes migrate with a single wrap. Mirrors
// stdlib's http.HandlerFunc / sort.SliceStable patterns. Lowercase —
// test-scope only; cross-package callers (e.g. internal/sse tests)
// declare their own file-local adapter, per RESEARCH.md
// §"Decision on cross-package test adapter".
type proberFunc func(ctx context.Context) error

// CheckAuth invokes the wrapped function, satisfying the Prober interface.
func (f proberFunc) CheckAuth(ctx context.Context) error { return f(ctx) }

// gatherSingleGauge returns the value of the single-instance gauge family
// named `name`, or 0 if not registered. walera_auth_circuit_breaker_state
// has no labels so this is the right shape — copy from router_test's
// gatherGauge but specialized for label-less gauges.
func gatherSingleGauge(t *testing.T, reg *metrics.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		ms := fam.GetMetric()
		if len(ms) != 1 {
			t.Fatalf("%s: got %d series; want 1", name, len(ms))
		}
		return ms[0].GetGauge().GetValue()
	}
	return 0
}

// _ matches the unused-import guard since dto might not always be required.
var _ = (*dto.Metric)(nil)

// newTestBreaker builds a Breaker driven by an injectable clock. nowNanos is
// the synthetic-clock source; tests advance time by calling nowNanos.Add(...)
// before invoking b.tickFSM. tickInterval is set to a tiny value (1ms) but
// Run is never started — tests drive the FSM directly via tickFSM.
func newTestBreaker(t *testing.T, probe func(ctx context.Context) error) (*Breaker, *atomic.Int64) {
	t.Helper()
	var nowNanos atomic.Int64
	nowNanos.Store(time.Unix(0, 0).UnixNano())
	clock := func() time.Time { return time.Unix(0, nowNanos.Load()) }
	cfg := BreakerConfig{
		WindowBuckets:        30,
		BucketSeconds:        1,
		FailureRateThreshold: 0.5,
		DebounceFloor:        20,
		Cooldown:             30 * time.Second,
		StaleRefreshJitter:   5 * time.Second,
	}
	// Prober is a required Dep. Existing tests that did not exercise the
	// HalfOpen path used a nil probe (which the FSM treated as instant-close);
	// preserve the same observable behaviour by substituting a nil-returning
	// probe here.
	if probe == nil {
		probe = func(_ context.Context) error { return nil }
	}
	b := NewBreaker(cfg, BreakerDeps{
		Prober:  proberFunc(probe),
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
	})
	b.SetClockForTest(clock, time.Millisecond)
	return b, &nowNanos
}

func TestBreaker_InitiallyClosedAllowsCalls(t *testing.T) {
	t.Parallel()
	b, _ := newTestBreaker(t, nil)
	if got := b.State(); got != StateClosed {
		t.Errorf("initial state: got %v; want StateClosed", got)
	}
	if !b.Allow() {
		t.Error("Allow on fresh breaker: got false; want true")
	}
	if v := gatherSingleGauge(t, b.mc, "walera_auth_circuit_breaker_state"); v != 0 {
		t.Errorf("gauge: got %v; want 0", v)
	}
}

func TestBreaker_ClosedToOpenOnFailureBurst(t *testing.T) {
	t.Parallel()
	b, _ := newTestBreaker(t, nil)
	for i := 0; i < 21; i++ {
		b.RecordResult(false)
	}
	if got := b.State(); got != StateOpen {
		t.Errorf("state after 21 failures: got %v; want StateOpen", got)
	}
	if b.Allow() {
		t.Error("Allow when Open: got true; want false")
	}
	if v := gatherSingleGauge(t, b.mc, "walera_auth_circuit_breaker_state"); v != 1 {
		t.Errorf("gauge: got %v; want 1", v)
	}
}

func TestBreaker_ClosedDoesNotTripBelowDebounceFloor(t *testing.T) {
	t.Parallel()
	b, _ := newTestBreaker(t, nil)
	for i := 0; i < 19; i++ {
		b.RecordResult(false)
	}
	if got := b.State(); got != StateClosed {
		t.Errorf("state after 19 failures (below floor): got %v; want StateClosed", got)
	}
	if !b.Allow() {
		t.Error("Allow when Closed: got false; want true")
	}
	if v := gatherSingleGauge(t, b.mc, "walera_auth_circuit_breaker_state"); v != 0 {
		t.Errorf("gauge: got %v; want 0", v)
	}
}

func TestBreaker_ClosedDoesNotTripBelowFailureRate(t *testing.T) {
	t.Parallel()
	b, _ := newTestBreaker(t, nil)
	// 11 successes + 9 failures = 20 total (floor); rate=0.45 (≤ 0.5).
	for i := 0; i < 11; i++ {
		b.RecordResult(true)
	}
	for i := 0; i < 9; i++ {
		b.RecordResult(false)
	}
	if got := b.State(); got != StateClosed {
		t.Errorf("state at rate=0.45 below threshold: got %v; want StateClosed", got)
	}
}

func TestBreaker_OpenStaysOpenDuringCooldown(t *testing.T) {
	t.Parallel()
	b, now := newTestBreaker(t, func(ctx context.Context) error { return nil })
	// Trip.
	for i := 0; i < 21; i++ {
		b.RecordResult(false)
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("precondition: state should be StateOpen, got %v", got)
	}
	// Advance less than Cooldown (10s of 30s).
	now.Add(int64(10 * time.Second))
	b.tickFSM(context.Background())
	if got := b.State(); got != StateOpen {
		t.Errorf("state mid-cooldown: got %v; want StateOpen", got)
	}
	if b.Allow() {
		t.Error("Allow during cooldown: got true; want false")
	}
}

func TestBreaker_OpenToHalfOpenAfterCooldownProbeSuccess(t *testing.T) {
	t.Parallel()
	successProbe := func(ctx context.Context) error { return nil }
	b, now := newTestBreaker(t, successProbe)
	// Capture closeCh before tripping.
	ch1 := b.WaitForClose()
	// Trip.
	for i := 0; i < 21; i++ {
		b.RecordResult(false)
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("precondition: state should be StateOpen, got %v", got)
	}
	// Advance past cooldown.
	now.Add(int64(31 * time.Second))
	b.tickFSM(context.Background())
	if got := b.State(); got != StateClosed {
		t.Errorf("state after probe success: got %v; want StateClosed", got)
	}
	if v := gatherSingleGauge(t, b.mc, "walera_auth_circuit_breaker_state"); v != 0 {
		t.Errorf("gauge: got %v; want 0", v)
	}
	// The previously captured channel must be closed by signalClose.
	select {
	case <-ch1:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("WaitForClose channel did not fire after Open→Closed")
	}
}

func TestBreaker_HalfOpenToOpenOnProbeFailure(t *testing.T) {
	t.Parallel()
	failProbe := func(ctx context.Context) error { return &ErrUnavailable{} }
	b, now := newTestBreaker(t, failProbe)
	// Trip.
	for i := 0; i < 21; i++ {
		b.RecordResult(false)
	}
	// Advance past cooldown, capture old openedAt.
	now.Add(int64(31 * time.Second))
	beforeProbeNanos := now.Load()
	b.tickFSM(context.Background())
	if got := b.State(); got != StateOpen {
		t.Errorf("state after probe failure: got %v; want StateOpen", got)
	}
	if v := gatherSingleGauge(t, b.mc, "walera_auth_circuit_breaker_state"); v != 1 {
		t.Errorf("gauge: got %v; want 1", v)
	}
	// Cooldown must have been reset to the new "now" — openedAt should now
	// equal beforeProbeNanos (the clock value when tickFSM ran).
	if got := b.openedAt.Load(); got != beforeProbeNanos {
		t.Errorf("openedAt after reopen: got %d; want %d (cooldown not reset)", got, beforeProbeNanos)
	}
}

func TestBreaker_ProbeNon200ResponseCountsAsReachable(t *testing.T) {
	t.Parallel()
	// Any non-network response means the backend answered. Use
	// *ErrUnauthorized to assert the breaker closes (the backend was
	// reachable).
	probe := func(ctx context.Context) error { return &ErrUnauthorized{Body: []byte("nope")} }
	b, now := newTestBreaker(t, probe)
	for i := 0; i < 21; i++ {
		b.RecordResult(false)
	}
	now.Add(int64(31 * time.Second))
	b.tickFSM(context.Background())
	if got := b.State(); got != StateClosed {
		t.Errorf("state after probe-401: got %v; want StateClosed (backend reachable)", got)
	}
	if v := gatherSingleGauge(t, b.mc, "walera_auth_circuit_breaker_state"); v != 0 {
		t.Errorf("gauge: got %v; want 0", v)
	}
}

func TestBreaker_WaitForCloseChannelCyclesAcrossTransitions(t *testing.T) {
	t.Parallel()
	successProbe := func(ctx context.Context) error { return nil }
	b, now := newTestBreaker(t, successProbe)
	for cycle := 0; cycle < 5; cycle++ {
		ch := b.WaitForClose()
		// Trip the breaker.
		for i := 0; i < 21; i++ {
			b.RecordResult(false)
		}
		if got := b.State(); got != StateOpen {
			t.Fatalf("cycle %d: precondition state should be StateOpen, got %v", cycle, got)
		}
		// Advance past cooldown and run probe.
		now.Add(int64(31 * time.Second))
		b.tickFSM(context.Background())
		if got := b.State(); got != StateClosed {
			t.Fatalf("cycle %d: state after probe success: got %v; want StateClosed", cycle, got)
		}
		// Captured channel must be closed.
		select {
		case <-ch:
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("cycle %d: WaitForClose channel did not fire", cycle)
		}
		// New channel must be different and NOT closed.
		newCh := b.WaitForClose()
		if newCh == ch {
			t.Fatalf("cycle %d: new channel reference equals old", cycle)
		}
		select {
		case <-newCh:
			t.Fatalf("cycle %d: freshly created channel is already closed", cycle)
		default:
		}
		// Reset the window so the next 21 failures aren't blocked by
		// the carry-over of prior successes/failures (real production
		// behavior — buckets age out — but tests advance the clock
		// further than 30s already, so the bucket rotation needs
		// explicit ticks via the FSM goroutine. Without Run() we must
		// manually advance the window so the next trip happens.
		for i := 0; i < windowBuckets; i++ {
			b.window.tick()
		}
	}
}

func TestBreaker_RecordResultAllocFreeOnHotPath(t *testing.T) {
	// NOTE: testing.AllocsPerRun cannot run inside t.Parallel — it depends
	// on GC quiescence which parallel tests violate. Run serially.
	b, _ := newTestBreaker(t, nil)
	allocs := testing.AllocsPerRun(1000, func() {
		b.RecordResult(true)
	})
	if allocs != 0 {
		t.Errorf("RecordResult AllocsPerRun: got %v; want 0", allocs)
	}
}

func TestBreaker_AllowAllocFreeOnHotPath(t *testing.T) {
	// NOTE: serial — see TestBreaker_RecordResultAllocFreeOnHotPath rationale.
	b, _ := newTestBreaker(t, nil)
	allocs := testing.AllocsPerRun(1000, func() {
		_ = b.Allow()
	})
	if allocs != 0 {
		t.Errorf("Allow AllocsPerRun: got %v; want 0", allocs)
	}
}

func TestBreaker_SatisfiesBreakerHookInterface(t *testing.T) {
	t.Parallel()
	// The package-level `var _ BreakerHook = (*Breaker)(nil)` at the top of
	// this file is the actual compile-time assertion. The test body is a
	// trivial runtime no-op whose mere compilation proves the contract.
	var _ BreakerHook = (*Breaker)(nil)
}
