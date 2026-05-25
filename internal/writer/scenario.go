package writer

import (
	"math"
	"sync/atomic"
	"time"
)

type Scenario interface {
	Name() string
	Tick(elapsed time.Duration) (commitRate float64, rowsPerTx int)
}

type scenarioState struct {
	Scenario   Scenario
	StartedAt  time.Time
	RowsPerTx  int
	CommitRate float64
	Targets    []string
	nextIdx    atomic.Uint64
}

type ScenarioStateExport = scenarioState

func NewScenarioState(s Scenario, startedAt time.Time, commitRate float64, rowsPerTx int, targets []string) *ScenarioStateExport {
	return &scenarioState{
		Scenario:   s,
		StartedAt:  startedAt,
		RowsPerTx:  rowsPerTx,
		CommitRate: commitRate,
		Targets:    append([]string(nil), targets...),
	}
}

func (s *scenarioState) NextTarget() string {
	if len(s.Targets) == 0 {
		return ""
	}

	idx := s.nextIdx.Add(1) - 1
	return s.Targets[idx%uint64(len(s.Targets))]
}

func swapScenario(ptr *atomic.Pointer[scenarioState], next *scenarioState) *scenarioState {
	return ptr.Swap(next)
}

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

		return newSpikeScenario(commitRate, commitRate*10, rowsPerTx, 5*time.Second, 30*time.Second)
	case "soak":
		return newSoakScenario(commitRate, rowsPerTx)
	case "stress":
		return newStressScenario(commitRate, rowsPerTx)
	default:
		return nil
	}
}

func isKnownScenario(name string) bool {
	_, ok := Registry()[name]
	return ok
}

type smokeScenario struct {
	rate float64
	rows int
}

func NewSmokeScenario(commitRate float64, rowsPerTx int) Scenario {
	return &smokeScenario{rate: commitRate, rows: rowsPerTx}
}

func (s *smokeScenario) Name() string { return "smoke" }
func (s *smokeScenario) Tick(_ time.Duration) (float64, int) {
	return s.rate, s.rows
}

type rampUpScenario struct {
	target  float64
	rows    int
	rampDur time.Duration
}

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

type steadyScenario struct {
	rate float64
	rows int
}

func newSteadyScenario(commitRate float64, rowsPerTx int) Scenario {
	return &steadyScenario{rate: commitRate, rows: rowsPerTx}
}

func (s *steadyScenario) Name() string                        { return "steady" }
func (s *steadyScenario) Tick(_ time.Duration) (float64, int) { return s.rate, s.rows }

type spikeScenario struct {
	baseline      float64
	burst         float64
	rows          int
	burstDuration time.Duration
	period        time.Duration
}

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

type soakScenario struct {
	rate float64
	rows int
}

func newSoakScenario(commitRate float64, rowsPerTx int) Scenario {
	return &soakScenario{rate: commitRate, rows: rowsPerTx}
}

func (s *soakScenario) Name() string                        { return "soak" }
func (s *soakScenario) Tick(_ time.Duration) (float64, int) { return s.rate, s.rows }

type stressScenario struct {
	start float64
	rows  int
}

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
		doublings = 30
	}
	r := s.start * math.Pow(2, float64(doublings))
	if r > math.MaxInt32 {
		r = math.MaxInt32
	}
	return r, s.rows
}
