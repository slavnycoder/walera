package writer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegistry_AllSixNames(t *testing.T) {
	reg := Registry()
	wantNames := []string{"smoke", "ramp-up", "steady", "spike", "soak", "stress"}
	if len(reg) != len(wantNames) {
		t.Fatalf("Registry size = %d, want %d (got %v)", len(reg), len(wantNames), keysOf(reg))
	}
	for _, n := range wantNames {
		s, ok := reg[n]
		if !ok {
			t.Errorf("Registry missing scenario %q", n)
			continue
		}
		if s.Name() != n {
			t.Errorf("Registry[%q].Name() = %q", n, s.Name())
		}
	}
}

func keysOf(m map[string]Scenario) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestSteady_Tick(t *testing.T) {
	s := newSteadyScenario(100.0, 1)
	r0, rows0 := s.Tick(0)
	if r0 != 100.0 || rows0 != 1 {
		t.Errorf("steady@t=0 = (%v,%d), want (100,1)", r0, rows0)
	}
	r1, rows1 := s.Tick(time.Hour)
	if r1 != 100.0 || rows1 != 1 {
		t.Errorf("steady@t=1h = (%v,%d), want (100,1)", r1, rows1)
	}
}

func TestRampUp_Tick(t *testing.T) {
	s := newRampUpScenario(100.0, 1, 10*time.Second)
	// At elapsed=1s, expect rate ≈ 10.
	r1, _ := s.Tick(1 * time.Second)
	if r1 < 9.9 || r1 > 10.1 {
		t.Errorf("ramp-up@t=1s rate = %v, want ≈10", r1)
	}
	// At elapsed=10s, expect rate = 100.
	r10, _ := s.Tick(10 * time.Second)
	if r10 < 99.9 || r10 > 100.1 {
		t.Errorf("ramp-up@t=10s rate = %v, want ≈100", r10)
	}
	// At elapsed=11s, expect rate clamped to 100.
	r11, _ := s.Tick(11 * time.Second)
	if r11 != 100.0 {
		t.Errorf("ramp-up@t=11s rate = %v, want 100 (clamped)", r11)
	}
}

func TestSpike_Tick(t *testing.T) {
	s := newSpikeScenario(10.0, 100.0, 1, 5*time.Second, 30*time.Second)
	cases := []struct {
		elapsed time.Duration
		want    float64
		label   string
	}{
		{0, 100.0, "t=0 burst start"},
		{6 * time.Second, 10.0, "t=6s baseline"},
		{30 * time.Second, 100.0, "t=30s next burst"},
		{35 * time.Second, 10.0, "t=35s back to baseline"},
	}
	for _, c := range cases {
		got, _ := s.Tick(c.elapsed)
		if got != c.want {
			t.Errorf("spike %s: got %v, want %v", c.label, got, c.want)
		}
	}
}

func TestStress_Tick(t *testing.T) {
	s := newStressScenario(200.0, 1)
	cases := []struct {
		elapsed time.Duration
		want    float64
	}{
		{0, 200.0},
		{60 * time.Second, 400.0},
		{120 * time.Second, 800.0},
	}
	for _, c := range cases {
		got, _ := s.Tick(c.elapsed)
		if got != c.want {
			t.Errorf("stress@t=%v: got %v, want %v", c.elapsed, got, c.want)
		}
	}
}

func TestSmoke_Tick(t *testing.T) {
	s := NewSmokeScenario(5.0, 1)
	for _, d := range []time.Duration{0, 30 * time.Second, 60 * time.Second} {
		r, rows := s.Tick(d)
		if r != 5.0 || rows != 1 {
			t.Errorf("smoke@t=%v = (%v,%d), want (5,1)", d, r, rows)
		}
	}
}

func TestSwapScenario_Atomic(t *testing.T) {
	var ptr atomic.Pointer[scenarioState]
	a := &scenarioState{
		Scenario:   newSteadyScenario(10.0, 1),
		StartedAt:  time.Now(),
		CommitRate: 10.0,
		RowsPerTx:  1,
		Targets:    []string{"orders"},
	}
	ptr.Store(a)

	const N = 500
	var wg sync.WaitGroup
	wg.Add(2)

	// Reader goroutine — observes scenarioPtr.Load() under -race.
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			st := ptr.Load()
			if st == nil {
				t.Errorf("unexpected nil state")
				return
			}
			// Touch the scenario to ensure no torn read.
			_ = st.Scenario.Name()
			_ = st.NextTarget()
		}
	}()

	// Writer goroutine — swaps the scenario state in.
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			next := &scenarioState{
				Scenario:   newSteadyScenario(20.0, 1),
				StartedAt:  time.Now(),
				CommitRate: 20.0,
				RowsPerTx:  1,
				Targets:    []string{"devices", "articles"},
			}
			swapScenario(&ptr, next)
		}
	}()

	wg.Wait()
}

func TestNextTarget_RoundRobin(t *testing.T) {
	st := &scenarioState{
		Targets: []string{"a", "b", "c"},
	}
	want := []string{"a", "b", "c", "a", "b", "c"}
	for i, w := range want {
		got := st.NextTarget()
		if got != w {
			t.Errorf("call %d: got %q, want %q", i, got, w)
		}
	}
}

// TestNextTarget_Empty defends NextTarget against an empty Targets slice
// (rule 2: defensive coding for a hot-path read).
func TestNextTarget_Empty(t *testing.T) {
	st := &scenarioState{Targets: nil}
	if got := st.NextTarget(); got != "" {
		t.Errorf("NextTarget() on empty targets = %q, want \"\"", got)
	}
}
