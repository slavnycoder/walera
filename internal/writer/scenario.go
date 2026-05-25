// Package writer — scenario.go defines the Scenario interface and the six
// registered implementations (smoke, ramp-up, steady, spike, soak, stress).
// scenarioState is the value behind atomic.Pointer[scenarioState] so the
// commit-loop hot path reads it lock-free. See INVARIANTS.md.
package writer

import (
	"math"
	"sync/atomic"
	"time"
)

// Scenario maps elapsed-since-start to (commit rate, rows per tx).
// Implementations MUST be safe for concurrent reads. The six production
// implementations are Smoke, RampUp, Steady, Spike, Soak, and Stress.
type Scenario interface {
	Name() string
	Tick(elapsed time.Duration) (commitRate float64, rowsPerTx int)
}

// scenarioState is the value behind atomic.Pointer[scenarioState]; the
// commit loop reads it via .Load() on every iteration. Fields are
// effectively immutable once published — /control swaps the pointer rather
// than mutating in place. nextIdx is the one mutable field (atomic.Add).
type scenarioState struct {
	Scenario   Scenario
	StartedAt  time.Time
	RowsPerTx  int
	CommitRate float64
	Targets    []string
	nextIdx    atomic.Uint64
}

// ScenarioStateExport is the public alias for scenarioState so cmd/writer's
// atomic.Pointer unifies with the package-internal one.
type ScenarioStateExport = scenarioState

// NewScenarioState constructs the initial scenarioState consumed by
// RunCommitLoop. The returned pointer is safe to .Store() into an
// atomic.Pointer[ScenarioStateExport].
func NewScenarioState(s Scenario, startedAt time.Time, commitRate float64, rowsPerTx int, targets []string) *ScenarioStateExport {
	return &scenarioState{
		Scenario:   s,
		StartedAt:  startedAt,
		RowsPerTx:  rowsPerTx,
		CommitRate: commitRate,
		Targets:    append([]string(nil), targets...),
	}
}

// NextTarget returns the next round-robin target table. Returns "" when
// Targets is empty (defensive — avoids panicking the commit goroutine).
func (s *scenarioState) NextTarget() string {
	if len(s.Targets) == 0 {
		return ""
	}
	// Add returns post-increment; subtract 1 so the first call yields Targets[0].
	idx := s.nextIdx.Add(1) - 1
	return s.Targets[idx%uint64(len(s.Targets))]
}

// swapScenario atomically replaces the active scenarioState and returns
// the previous value.
func swapScenario(ptr *atomic.Pointer[scenarioState], next *scenarioState) *scenarioState {
	return ptr.Swap(next)
}

// Registry returns the map of all six scenarios keyed by name with
// baseline defaults. Use for enumeration / name validation only — runtime
// scenarios MUST be constructed via BuildScenario. See INVARIANTS.md.
func Registry() map[string]Scenario {
	return map[string]Scenario{
		"smoke":   NewSmokeScenario(5.0, 1),
		"ramp-up": newRampUpScenario(100.0, 1, 5*time.Minute),
		"steady":  newSteadyScenario(100.0, 1),
		"spike":   newSpikeScenario(10.0, 100.0, 1, 5*time.Second, 30*time.Second),
		"soak":    newSoakScenario(50.0, 1),
		"stress":  newStressScenario(200.0, 1),
	}
}

// BuildScenario constructs a Scenario by name with the operator-supplied
// commitRate / rowsPerTx / rampDur values baked in. rampDur is only
// consulted by the ramp-up scenario (0 → default 5m). Returns nil for
// unknown names. See INVARIANTS.md for why Registry() is enumeration-only.
func BuildScenario(name string, commitRate float64, rowsPerTx int, rampDur time.Duration) Scenario {
	switch name {
	case "smoke":
		return NewSmokeScenario(commitRate, rowsPerTx)
	case "ramp-up":
		if rampDur <= 0 {
			rampDur = 5 * time.Minute
		}
		return newRampUpScenario(commitRate, rowsPerTx, rampDur)
	case "steady":
		return newSteadyScenario(commitRate, rowsPerTx)
	case "spike":
		// Baseline = commitRate; burst = 10× baseline; 5s every 30s.
		return newSpikeScenario(commitRate, commitRate*10, rowsPerTx, 5*time.Second, 30*time.Second)
	case "soak":
		return newSoakScenario(commitRate, rowsPerTx)
	case "stress":
		return newStressScenario(commitRate, rowsPerTx)
	default:
		return nil
	}
}

// isKnownScenario reports whether name is one of the six registered scenario
// names (smoke, ramp-up, steady, spike, soak, stress).
func isKnownScenario(name string) bool {
	_, ok := Registry()[name]
	return ok
}

// ---- smoke ----

type smokeScenario struct {
	rate float64
	rows int
}

// NewSmokeScenario is the constant-low scenario for cold-start verification.
func NewSmokeScenario(commitRate float64, rowsPerTx int) Scenario {
	return &smokeScenario{rate: commitRate, rows: rowsPerTx}
}

func (s *smokeScenario) Name() string { return "smoke" }
func (s *smokeScenario) Tick(_ time.Duration) (float64, int) {
	return s.rate, s.rows
}

// ---- ramp-up ----

type rampUpScenario struct {
	target  float64
	rows    int
	rampDur time.Duration
}

// newRampUpScenario linearly interpolates 0 → target over rampDur then
// holds; rowsPerTx is constant.
func newRampUpScenario(target float64, rowsPerTx int, rampDur time.Duration) Scenario {
	return &rampUpScenario{target: target, rows: rowsPerTx, rampDur: rampDur}
}

func (s *rampUpScenario) Name() string { return "ramp-up" }
func (s *rampUpScenario) Tick(elapsed time.Duration) (float64, int) {
	if s.rampDur <= 0 || elapsed >= s.rampDur {
		return s.target, s.rows
	}
	frac := elapsed.Seconds() / s.rampDur.Seconds()
	return s.target * frac, s.rows
}

// ---- steady ----

type steadyScenario struct {
	rate float64
	rows int
}

// newSteadyScenario is the basic "hold a constant value" scenario.
func newSteadyScenario(commitRate float64, rowsPerTx int) Scenario {
	return &steadyScenario{rate: commitRate, rows: rowsPerTx}
}

func (s *steadyScenario) Name() string                        { return "steady" }
func (s *steadyScenario) Tick(_ time.Duration) (float64, int) { return s.rate, s.rows }

// ---- spike ----

type spikeScenario struct {
	baseline      float64
	burst         float64
	rows          int
	burstDuration time.Duration
	period        time.Duration
}

// newSpikeScenario produces a steady baseline with a periodic burst: burst
// for burstDuration, baseline for the rest of each period. At elapsed=0
// the scenario is in the burst window.
func newSpikeScenario(baseline, burst float64, rowsPerTx int, burstDuration, period time.Duration) Scenario {
	return &spikeScenario{
		baseline:      baseline,
		burst:         burst,
		rows:          rowsPerTx,
		burstDuration: burstDuration,
		period:        period,
	}
}

func (s *spikeScenario) Name() string { return "spike" }
func (s *spikeScenario) Tick(elapsed time.Duration) (float64, int) {
	if s.period <= 0 {
		return s.baseline, s.rows
	}
	off := elapsed % s.period
	if off < s.burstDuration {
		return s.burst, s.rows
	}
	return s.baseline, s.rows
}

// ---- soak ----

type soakScenario struct {
	rate float64
	rows int
}

// newSoakScenario is a long-duration steady scenario; semantically
// identical to steady, registered separately for operator clarity.
func newSoakScenario(commitRate float64, rowsPerTx int) Scenario {
	return &soakScenario{rate: commitRate, rows: rowsPerTx}
}

func (s *soakScenario) Name() string                        { return "soak" }
func (s *soakScenario) Tick(_ time.Duration) (float64, int) { return s.rate, s.rows }

// ---- stress ----

type stressScenario struct {
	start float64
	rows  int
}

// newStressScenario doubles `start` tx/s every 60s, capped at MaxInt32.
func newStressScenario(start float64, rowsPerTx int) Scenario {
	return &stressScenario{start: start, rows: rowsPerTx}
}

func (s *stressScenario) Name() string { return "stress" }
func (s *stressScenario) Tick(elapsed time.Duration) (float64, int) {
	doublings := int(elapsed / time.Minute)
	if doublings < 0 {
		doublings = 0
	}
	if doublings > 30 {
		doublings = 30 // 2^30 already > 1e9 — cap to avoid overflow.
	}
	r := s.start * math.Pow(2, float64(doublings))
	if r > math.MaxInt32 {
		r = math.MaxInt32
	}
	return r, s.rows
}
