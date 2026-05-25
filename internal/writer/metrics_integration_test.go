//go:build integration

package writer

import (
	"context"
	mathrand "math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

func TestCommitOnce_RollbackDoesNotBumpTxTotal_Integration(t *testing.T) {
	dsn := bootPG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p, err := NewPool(ctx, WriterPGConfig{DSN: dsn}, WriterPoolConfig{MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	if _, err := p.Exec(ctx, "ALTER TABLE devices ADD CONSTRAINT devices_name_unique UNIQUE (name)"); err != nil {
		t.Fatalf("add unique constraint: %v", err)
	}

	rng := mathrand.New(mathrand.NewPCG(123, 456))
	firstName := "device-" + uint64ToStr(rng.Uint64())

	if _, err := p.Exec(ctx,
		"INSERT INTO devices (name, firmware_version, metadata) VALUES ($1, '1.0.0', '{}'::jsonb)",
		firstName,
	); err != nil {
		t.Fatalf("seed conflicting row: %v", err)
	}

	reg := NewRegistry()
	reg.SetActiveScenario("test")
	reg.SetCommitRate("test", 1)

	var txCount int64
	var errCount int64

	onCommit := func(scenario, target string, rows int) {
		atomic.AddInt64(&txCount, 1)
		reg.TxTotal(scenario, target)
		reg.RowsTotal(scenario, target, "insert", rows)
	}
	onError := func(reason string) {
		atomic.AddInt64(&errCount, 1)
		reg.Errors(reason)
	}
	_ = onCommit

	rng2 := mathrand.New(mathrand.NewPCG(123, 456))
	err = commitOnce(ctx, p, "devices", 1, rng2, WriterPGConfig{TxTimeout: 5 * time.Second})
	if err == nil {
		t.Fatalf("expected UNIQUE violation, got nil error")
	}

	reason := classify(err)
	if reason != "pg_constraint" {
		t.Fatalf("classify(unique_violation) = %q, want pg_constraint (err=%v)", reason, err)
	}
	onError(reason)

	if v, ok := metricValueByLabels(t, reg, "writer_tx_total",
		map[string]string{"scenario": "test", "target": "devices"}); ok && v != 0 {
		t.Errorf("writer_tx_total{test,devices} = %v, want 0 (or series absent)", v)
	}
	if v, ok := metricValueByLabels(t, reg, "writer_errors_total",
		map[string]string{"reason": "pg_constraint"}); !ok || v != 1 {
		t.Errorf("writer_errors_total{pg_constraint} = %v (ok=%v), want 1", v, ok)
	}

	if atomic.LoadInt64(&txCount) != 0 {
		t.Errorf("onCommit invoked %d times on a failing commit (want 0)", txCount)
	}
	if atomic.LoadInt64(&errCount) != 1 {
		t.Errorf("onError invoked %d times (want 1)", errCount)
	}
}

func TestRunCommitLoop_WithMetrics_TxTotalMatchesObservedCommits_Integration(t *testing.T) {
	dsn := bootPG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p, err := NewPool(ctx, WriterPGConfig{DSN: dsn}, WriterPoolConfig{MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	reg := NewRegistry()
	reg.SetActiveScenario("steady")
	reg.SetCommitRate("steady", 10)

	lim := rate.NewLimiter(rate.Limit(10), 1)
	var ptr atomic.Pointer[scenarioState]
	ptr.Store(NewScenarioState(newSteadyScenario(10, 1), time.Now(), 10, 1, []string{"devices"}))

	var observed int64
	onCommit := func(scenario, target string, rows int) {
		atomic.AddInt64(&observed, 1)
		reg.TxTotal(scenario, target)
		reg.RowsTotal(scenario, target, "insert", rows)
	}
	onError := func(reason string) {
		reg.Errors(reason)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	rng := mathrand.New(mathrand.NewPCG(7, 11))
	done := make(chan struct{})
	go func() {
		_ = RunCommitLoop(runCtx, p, lim, &ptr, DistUniform, rng,
			WriterPGConfig{TxTimeout: 5 * time.Second},
			zerolog.Nop(), onCommit, onError)
		close(done)
	}()

	time.Sleep(600 * time.Millisecond)
	runCancel()
	<-done

	v, ok := metricValueByLabels(t, reg, "writer_tx_total",
		map[string]string{"scenario": "steady", "target": "devices"})
	if !ok {
		t.Fatalf("writer_tx_total{steady,devices} series missing")
	}
	got := atomic.LoadInt64(&observed)
	if int64(v) != got {
		t.Errorf("writer_tx_total = %v, want %d (observed onCommit count)", v, got)
	}
	if got < 2 {
		t.Errorf("observed = %d, want >=2 at 10tx/s over 600ms", got)
	}
}

func uint64ToStr(v uint64) string {
	if v == 0 {
		return "0"
	}
	const digits = "0123456789"
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = digits[v%10]
		v /= 10
	}
	return string(buf[i:])
}

var _ = pgx.ErrNoRows
