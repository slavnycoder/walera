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

type proberFunc func(ctx context.Context) error

func (f proberFunc) CheckAuth(ctx context.Context) error { return f(ctx) }

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

var _ = (*dto.Metric)(nil)

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

	for i := 0; i < 21; i++ {
		b.RecordResult(false)
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("precondition: state should be StateOpen, got %v", got)
	}

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

	ch1 := b.WaitForClose()

	for i := 0; i < 21; i++ {
		b.RecordResult(false)
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("precondition: state should be StateOpen, got %v", got)
	}

	now.Add(int64(31 * time.Second))
	b.tickFSM(context.Background())
	if got := b.State(); got != StateClosed {
		t.Errorf("state after probe success: got %v; want StateClosed", got)
	}
	if v := gatherSingleGauge(t, b.mc, "walera_auth_circuit_breaker_state"); v != 0 {
		t.Errorf("gauge: got %v; want 0", v)
	}

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

	for i := 0; i < 21; i++ {
		b.RecordResult(false)
	}

	now.Add(int64(31 * time.Second))
	beforeProbeNanos := now.Load()
	b.tickFSM(context.Background())
	if got := b.State(); got != StateOpen {
		t.Errorf("state after probe failure: got %v; want StateOpen", got)
	}
	if v := gatherSingleGauge(t, b.mc, "walera_auth_circuit_breaker_state"); v != 1 {
		t.Errorf("gauge: got %v; want 1", v)
	}

	if got := b.openedAt.Load(); got != beforeProbeNanos {
		t.Errorf("openedAt after reopen: got %d; want %d (cooldown not reset)", got, beforeProbeNanos)
	}
}

func TestBreaker_ProbeNon200ResponseCountsAsReachable(t *testing.T) {
	t.Parallel()

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

		for i := 0; i < 21; i++ {
			b.RecordResult(false)
		}
		if got := b.State(); got != StateOpen {
			t.Fatalf("cycle %d: precondition state should be StateOpen, got %v", cycle, got)
		}

		now.Add(int64(31 * time.Second))
		b.tickFSM(context.Background())
		if got := b.State(); got != StateClosed {
			t.Fatalf("cycle %d: state after probe success: got %v; want StateClosed", cycle, got)
		}

		select {
		case <-ch:
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("cycle %d: WaitForClose channel did not fire", cycle)
		}

		newCh := b.WaitForClose()
		if newCh == ch {
			t.Fatalf("cycle %d: new channel reference equals old", cycle)
		}
		select {
		case <-newCh:
			t.Fatalf("cycle %d: freshly created channel is already closed", cycle)
		default:
		}

		for i := 0; i < windowBuckets; i++ {
			b.window.tick()
		}
	}
}

func TestBreaker_RecordResultAllocFreeOnHotPath(t *testing.T) {

	b, _ := newTestBreaker(t, nil)
	allocs := testing.AllocsPerRun(1000, func() {
		b.RecordResult(true)
	})
	if allocs != 0 {
		t.Errorf("RecordResult AllocsPerRun: got %v; want 0", allocs)
	}
}

func TestBreaker_AllowAllocFreeOnHotPath(t *testing.T) {

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

	var _ BreakerHook = (*Breaker)(nil)
}
