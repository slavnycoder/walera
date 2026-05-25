package writer

import (
	"context"
	"flag"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// TestSoak_Tick_Constant exercises the soak scenario's Tick (held out of the
// main test file because its semantics are intentionally identical to steady;
// the separate test pins coverage and prevents accidental divergence).
func TestSoak_Tick_Constant(t *testing.T) {
	s := newSoakScenario(50.0, 2)
	for _, d := range []time.Duration{0, time.Hour, 24 * time.Hour} {
		r, rows := s.Tick(d)
		if r != 50.0 || rows != 2 {
			t.Errorf("soak@t=%v = (%v,%d), want (50,2)", d, r, rows)
		}
	}
}

// TestStress_Tick_Cap covers the doubling-cap branch (≥30 doublings flattens
// to the math.MaxInt32 ceiling).
func TestStress_Tick_Cap(t *testing.T) {
	s := newStressScenario(200.0, 1)
	r, _ := s.Tick(3600 * time.Hour) // way past 30 doublings
	// At cap the result is clamped to math.MaxInt32.
	if r <= 0 {
		t.Errorf("stress at cap returned non-positive rate %v", r)
	}
}

// TestSpike_Tick_DegeneratePeriod exercises the period<=0 guard.
func TestSpike_Tick_DegeneratePeriod(t *testing.T) {
	s := &spikeScenario{
		baseline:      10.0,
		burst:         100.0,
		rows:          1,
		burstDuration: 5 * time.Second,
		period:        0,
	}
	r, _ := s.Tick(time.Second)
	if r != 10.0 {
		t.Errorf("spike degenerate-period: got %v, want baseline=10", r)
	}
}

// TestRampUp_DegenerateDuration exercises the rampDur<=0 fast path.
func TestRampUp_DegenerateDuration(t *testing.T) {
	s := newRampUpScenario(100.0, 1, 0)
	r, _ := s.Tick(time.Second)
	if r != 100.0 {
		t.Errorf("ramp-up rampDur=0: got %v, want target=100", r)
	}
}

// TestLoad_FlagOverridesScenarioName covers applyFlagOverrides via flag.Visit.
func TestLoad_FlagOverridesScenarioName(t *testing.T) {
	defer resetEnv(t)()
	t.Setenv("WRITER_PG_DSN", "postgres://u:p@h/db")
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	scen := fs.String("scenario", "", "")
	rate := fs.Float64("commit-rate", 0, "")
	rows := fs.Int("rows-per-tx", 0, "")
	ramp := fs.Duration("ramp-duration", 0, "")
	pgDsn := fs.String("pg-dsn", "", "")
	pool := fs.Int("pool-max-conns", 0, "")
	httpAddr := fs.String("http-addr", "", "")
	tables := fs.String("target-tables", "", "")
	logLvl := fs.String("log-level", "", "")
	dist := fs.String("arrival-distribution", "", "")
	if err := fs.Parse([]string{
		"--scenario", "steady",
		"--commit-rate", "33.3",
		"--rows-per-tx", "4",
		"--ramp-duration", "30s",
		"--pg-dsn", "postgres://x/y",
		"--pool-max-conns", "16",
		"--http-addr", ":9999",
		"--target-tables", "orders, articles",
		"--log-level", "debug",
		"--arrival-distribution", "uniform",
	}); err != nil {
		t.Fatalf("flag parse: %v", err)
	}
	_ = scen
	_ = rate
	_ = rows
	_ = ramp
	_ = pgDsn
	_ = pool
	_ = httpAddr
	_ = tables
	_ = logLvl
	_ = dist

	cfg, err := Load("", fs)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Scenario.Name != "steady" {
		t.Errorf("scenario.name = %q, want steady", cfg.Scenario.Name)
	}
	if cfg.Scenario.CommitRate != 33.3 {
		t.Errorf("scenario.commit_rate = %v, want 33.3", cfg.Scenario.CommitRate)
	}
	if cfg.Scenario.RowsPerTx != 4 {
		t.Errorf("scenario.rows_per_tx = %d, want 4", cfg.Scenario.RowsPerTx)
	}
	if cfg.Scenario.RampDuration != 30*time.Second {
		t.Errorf("scenario.ramp_duration = %v, want 30s", cfg.Scenario.RampDuration)
	}
	if cfg.PG.DSN != "postgres://x/y" {
		t.Errorf("pg.dsn = %q, want postgres://x/y", cfg.PG.DSN)
	}
	if cfg.Pool.MaxConns != 16 {
		t.Errorf("pool.max_conns = %d, want 16", cfg.Pool.MaxConns)
	}
	if cfg.HTTP.Addr != ":9999" {
		t.Errorf("http.addr = %q, want :9999", cfg.HTTP.Addr)
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("log.level = %q, want debug", cfg.Log.Level)
	}
	if cfg.Arrivals.Distribution != "uniform" {
		t.Errorf("arrivals.distribution = %q, want uniform", cfg.Arrivals.Distribution)
	}
	wantTables := []string{"orders", "articles"}
	if len(cfg.PG.TargetTables) != len(wantTables) {
		t.Fatalf("pg.target_tables = %v, want %v", cfg.PG.TargetTables, wantTables)
	}
	for i, w := range wantTables {
		if cfg.PG.TargetTables[i] != w {
			t.Errorf("pg.target_tables[%d] = %q, want %q", i, cfg.PG.TargetTables[i], w)
		}
	}
}

// TestLoad_YAML exercises the YAML-file layer (writes a temp file).
func TestLoad_YAML(t *testing.T) {
	defer resetEnv(t)()
	dir := t.TempDir()
	path := filepath.Join(dir, "writer.yaml")
	body := `pg:
  dsn: "postgres://yaml/dsn"
scenario:
  name: "ramp-up"
  commit_rate: 77.0
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write yaml: %v", err)
	}
	cfg, err := Load(path, newTestFlagSet())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PG.DSN != "postgres://yaml/dsn" {
		t.Errorf("pg.dsn = %q, want postgres://yaml/dsn", cfg.PG.DSN)
	}
	if cfg.Scenario.Name != "ramp-up" {
		t.Errorf("scenario.name = %q, want ramp-up", cfg.Scenario.Name)
	}
	if cfg.Scenario.CommitRate != 77.0 {
		t.Errorf("scenario.commit_rate = %v, want 77.0", cfg.Scenario.CommitRate)
	}
}

// TestLoad_YAML_InvalidFile returns a structured error.
func TestLoad_YAML_InvalidFile(t *testing.T) {
	defer resetEnv(t)()
	dir := t.TempDir()
	path := filepath.Join(dir, "broken.yaml")
	if err := os.WriteFile(path, []byte("not: valid: yaml: ::: \x00"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := Load(path, newTestFlagSet())
	if err == nil {
		t.Fatalf("expected YAML parse error, got nil")
	}
	if !strings.Contains(err.Error(), "writer config") {
		t.Errorf("error = %q, want substring writer config", err.Error())
	}
}

// TestValidate_MultipleFailures exercises the multi-error path.
func TestValidate_MultipleFailures(t *testing.T) {
	cfg := &WriterConfig{
		PG:       WriterPGConfig{DSN: ""},
		Arrivals: WriterArrivalsConfig{Distribution: "bogus"},
		Pool:     WriterPoolConfig{MaxConns: 0, MinConns: -1},
		Scenario: WriterScenarioConfig{CommitRate: 0, RowsPerTx: 0},
	}
	err := validate(cfg)
	if err == nil {
		t.Fatalf("expected validation error")
	}
	for _, want := range []string{
		"pg.dsn is required",
		"arrivals.distribution",
		"pool.max_conns",
		"pool.min_conns",
		"scenario.commit_rate",
		"scenario.rows_per_tx",
		"pg.tx_timeout",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

// TestWaitArrival_Poisson_DegenerateLambda hits the lambda<=0 branch.
func TestWaitArrival_Poisson_DegenerateLambda(t *testing.T) {
	lim := rate.NewLimiter(rate.Limit(0), 1)
	rng := mathrand.New(mathrand.NewPCG(1, 1))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Burn the initial burst.
	if err := waitArrival(ctx, lim, DistPoisson, rng); err != nil {
		t.Fatalf("first wait: %v", err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- waitArrival(ctx, lim, DistPoisson, rng) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Errorf("expected ctx err on degenerate lambda")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("waitArrival did not return")
	}
}

// TestWaitArrival_Poisson_CancelDuringSleep covers the ctx.Done branch in the
// timer select.
func TestWaitArrival_Poisson_CancelDuringSleep(t *testing.T) {
	// Very low rate so Exp(1)/λ is huge — guarantees we sit in the timer.
	lim := rate.NewLimiter(rate.Limit(0.001), 1)
	rng := mathrand.New(mathrand.NewPCG(7, 7))
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() { errCh <- waitArrival(ctx, lim, DistPoisson, rng) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Errorf("expected ctx err")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("waitArrival did not return on cancel")
	}
}
