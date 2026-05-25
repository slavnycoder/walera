package writer

import (
	"context"
	"errors"
	"io"
	mathrand "math/rand/v2"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

func TestClassify_Nil(t *testing.T) {
	if got := classify(nil); got != "" {
		t.Errorf("classify(nil) = %q, want \"\"", got)
	}
}

func TestClassify_PgConstraint(t *testing.T) {
	err := &pgconn.PgError{Code: "23505", Message: "unique_violation"}
	if got := classify(err); got != "pg_constraint" {
		t.Errorf("classify(unique violation) = %q, want pg_constraint", got)
	}
}

func TestClassify_PgOther(t *testing.T) {
	err := &pgconn.PgError{Code: "42601", Message: "syntax_error"}
	if got := classify(err); got != "pg_other" {
		t.Errorf("classify(syntax error) = %q, want pg_other", got)
	}
}

func TestClassify_DeadlineExceeded(t *testing.T) {
	if got := classify(context.DeadlineExceeded); got != "pg_other" {
		t.Errorf("classify(deadline) = %q, want pg_other", got)
	}
}

func TestClassify_NetConn(t *testing.T) {
	cases := []error{
		io.EOF,
		syscall.ECONNRESET,
		&net.OpError{Op: "read", Err: errors.New("connection reset")},
	}
	for _, err := range cases {
		if got := classify(err); got != "pg_conn" {
			t.Errorf("classify(%T) = %q, want pg_conn", err, got)
		}
	}
}

// TestRunCommitLoop_RespectsScenarioSwap drives the loop with the test
// commit function and an in-memory rate.Limiter, verifying that swapping the
// scenarioState changes the observed commit count over a fixed window.
func TestRunCommitLoop_RespectsScenarioSwap(t *testing.T) {
	// Stub the commit function with a fast no-op so the loop is bound by the
	// rate.Limiter, not by Postgres latency.
	origCommitOnce := commitOnceFn
	t.Cleanup(func() { commitOnceFn = origCommitOnce })
	commitOnceFn = func(ctx context.Context, _ commitOncePool, target string, rows int, _ *mathrand.Rand, _ WriterPGConfig) error {
		return nil
	}

	lim := rate.NewLimiter(rate.Limit(10), 1)

	var ptr atomic.Pointer[scenarioState]
	st := &scenarioState{
		Scenario:   newSteadyScenario(10, 1),
		StartedAt:  time.Now(),
		CommitRate: 10,
		RowsPerTx:  1,
		Targets:    []string{"orders"},
	}
	ptr.Store(st)

	var commitCount int64
	onCommit := func(_, _ string, _ int) { atomic.AddInt64(&commitCount, 1) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := zerolog.Nop()
	rng := mathrand.New(mathrand.NewPCG(1, 1))

	doneCh := make(chan struct{})
	go func() {
		_ = RunCommitLoop(ctx, nil, lim, &ptr, DistUniform, rng, WriterPGConfig{TxTimeout: time.Second}, logger, onCommit, nil)
		close(doneCh)
	}()

	// Window 1 (200ms at 10/s ≈ 2 commits).
	time.Sleep(200 * time.Millisecond)
	baseline := atomic.LoadInt64(&commitCount)

	// Swap to 50 tx/s.
	lim.SetLimit(rate.Limit(50))
	newSt := &scenarioState{
		Scenario:   newSteadyScenario(50, 1),
		StartedAt:  time.Now(),
		CommitRate: 50,
		RowsPerTx:  1,
		Targets:    []string{"orders"},
	}
	swapScenario(&ptr, newSt)
	atomic.StoreInt64(&commitCount, 0) // reset window counter

	// Window 2 (200ms at 50/s ≈ 10 commits; allow scheduling tolerance).
	time.Sleep(200 * time.Millisecond)
	after := atomic.LoadInt64(&commitCount)

	cancel()
	<-doneCh

	t.Logf("baseline window commits: %d; after-swap window commits: %d", baseline, after)
	// Threshold ≥5: at 50 tx/s over 200ms the ideal is 10; CI scheduling
	// jitter pushes the observed count into a band roughly 5–12. The
	// invariant we care about is that the post-swap window observes
	// noticeably more commits than the pre-swap window.
	if after < 5 {
		t.Errorf("after-swap commit count %d < 5 (expected ≥5 at 50tx/s over 200ms)", after)
	}
	if after <= baseline {
		t.Errorf("scenario swap did not increase commit count (baseline=%d after=%d)", baseline, after)
	}
}

func TestRunCommitLoop_CancelExits(t *testing.T) {
	origCommitOnce := commitOnceFn
	t.Cleanup(func() { commitOnceFn = origCommitOnce })
	commitOnceFn = func(ctx context.Context, _ commitOncePool, target string, rows int, _ *mathrand.Rand, _ WriterPGConfig) error {
		return nil
	}

	lim := rate.NewLimiter(rate.Limit(100), 1)
	var ptr atomic.Pointer[scenarioState]
	ptr.Store(&scenarioState{
		Scenario:  newSteadyScenario(100, 1),
		StartedAt: time.Now(),
		Targets:   []string{"orders"},
	})

	ctx, cancel := context.WithCancel(context.Background())

	rng := mathrand.New(mathrand.NewPCG(1, 1))
	done := make(chan error, 1)
	go func() {
		done <- RunCommitLoop(ctx, nil, lim, &ptr, DistUniform, rng, WriterPGConfig{TxTimeout: time.Second}, zerolog.Nop(), nil, nil)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// ok
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("RunCommitLoop did not exit within 500ms of ctx cancel")
	}
}

// TestRunCommitLoop_OnError exercises the onError callback path by injecting
// a failing commit function.
func TestRunCommitLoop_OnError(t *testing.T) {
	origCommitOnce := commitOnceFn
	t.Cleanup(func() { commitOnceFn = origCommitOnce })

	wantErr := &pgconn.PgError{Code: "23505", Message: "dup"}
	commitOnceFn = func(ctx context.Context, _ commitOncePool, _ string, _ int, _ *mathrand.Rand, _ WriterPGConfig) error {
		return wantErr
	}

	lim := rate.NewLimiter(rate.Limit(50), 1)
	var ptr atomic.Pointer[scenarioState]
	ptr.Store(&scenarioState{
		Scenario:  newSteadyScenario(50, 1),
		StartedAt: time.Now(),
		Targets:   []string{"orders"},
	})

	var errCount int64
	var lastReason atomic.Value
	onError := func(reason string) {
		atomic.AddInt64(&errCount, 1)
		lastReason.Store(reason)
	}

	ctx, cancel := context.WithCancel(context.Background())
	rng := mathrand.New(mathrand.NewPCG(1, 1))
	done := make(chan struct{})
	go func() {
		_ = RunCommitLoop(ctx, nil, lim, &ptr, DistUniform, rng, WriterPGConfig{TxTimeout: time.Second}, zerolog.Nop(), nil, onError)
		close(done)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	if atomic.LoadInt64(&errCount) == 0 {
		t.Fatalf("onError was never invoked")
	}
	if r, _ := lastReason.Load().(string); r != "pg_constraint" {
		t.Errorf("last reason = %q, want pg_constraint", r)
	}
}
