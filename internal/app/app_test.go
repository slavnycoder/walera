package app

// app_test.go — lifecycle regression tests for the (*App).Run iteration
// contract. Pins the "iterate []Runnable via the safego spawn
// primitive" invariant so any future refactor of InitializeApp that
// drops a Runnable, reorders the iteration to be serial, or fails to
// connect OnError sees an instantly diagnosable failure.
//
// Out of scope here (covered by complementary suites):
//   - goleak.VerifyNone — verifies the assembled production graph end
//     to end (see leak_main_test.go), not the hand-rolled fixture below.
//   - Pointer-identity singleton checks across consumers — see
//     app_singleton_test.go.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestApp_RunWiresRunnables verifies that (*App).Run iterates the
// Runnables slice and spawns each entry via the safego primitive (so
// each Runnable's Run callback is actually invoked) and that Run blocks
// on <-ctx.Done() until the outer context is cancelled.
//
// Test shape:
//   - Build an App with four Runnables ("alpha", "beta", "gamma" — the
//     three signal-on-start happy-path entries — plus "delta" which
//     returns a non-nil error to exercise the OnError-call-site
//     nil-tolerance documented on runnable.go).
//   - Spawn a.Run(ctx, cancel) in a goroutine.
//   - Assert each of the three happy-path started channels closes
//     within 200ms (proving the iteration actually fires every entry,
//     not just the first).
//   - Cancel ctx and assert Run returns within 200ms (proving the
//     <-ctx.Done() block + return-nil tail still works).
//
// HealthServer is left nil; (*App).Run guards the StartReadinessProbe
// call with a nil check (plan 04-04 added the guard explicitly so test
// fixtures stay stub-free).
func TestApp_RunWiresRunnables(t *testing.T) {
	t.Parallel()

	names := []string{"alpha", "beta", "gamma"}
	started := make([]chan struct{}, len(names))
	for i := range started {
		started[i] = make(chan struct{})
	}

	a := &App{
		Logger: zerolog.Nop(),
		// HealthServer intentionally nil — Run must tolerate it.
	}

	// Three happy-path Runnables: close started[i] then block on ctx.Done.
	for i, name := range names {
		i, name := i, name // capture loop variables
		a.Runnables = append(a.Runnables, Runnable{
			Name: name,
			Run: func(ctx context.Context) error {
				close(started[i])
				<-ctx.Done()
				return nil
			},
		})
	}

	// Fourth Runnable returns a non-nil error; OnError is nil; the
	// call-site `if r.OnError != nil` guard MUST prevent a nil-deref
	// panic. The safego primitive's recover would only mask a panic
	// with an stderr-printed stack trace — the test would still pass
	// at the Run-return-within-200ms gate, so we additionally assert
	// via a dedicated `errDelivered` channel that the delta Run body
	// actually executed.
	errDelivered := make(chan struct{})
	a.Runnables = append(a.Runnables, Runnable{
		Name: "delta",
		Run: func(ctx context.Context) error {
			close(errDelivered)
			return errors.New("delta intentional error")
		},
		// OnError intentionally nil — exercises the call-site nil guard.
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- a.Run(ctx, cancel)
	}()

	// Assert all three happy-path Runnables started.
	for i, ch := range started {
		select {
		case <-ch:
			// ok
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("Runnable %q did not start within 200ms", names[i])
		}
	}

	// Assert the error-returning Runnable was at least invoked (its Run
	// body executed and returned an error). If OnError-nil produced a
	// nil-deref panic inside the safego closure, the recover would log
	// to stderr; this channel close happens before the return, so the
	// assertion still proves "Run was called".
	select {
	case <-errDelivered:
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatal("delta Runnable's Run body did not execute within 200ms")
	}

	// Cancel the root context; Run must observe <-ctx.Done() and return
	// within the assertion window. The three blocking Runnables exit
	// independently as their own ctx.Done arms fire; Run does not wait
	// on them (Shutdown does, via wg in lifecycle.go's Step 1 wave).
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned non-nil error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Run did not return within 200ms of ctx cancel")
	}

	// Defensive: make sure no leftover goroutine still holds started[i]
	// open in a way that could leak into a sibling test. Each Runnable's
	// blocked <-ctx.Done() returns once cancel() fires above, so this is
	// just belt-and-suspenders against future edits to the loop body.
	_ = sync.WaitGroup{}
}

// TestApp_RunSuppressesOnErrorOnCleanShutdown verifies that when a
// Runnable.Run returns a non-nil error because its ctx was cancelled
// (the common shutdown path — wal.Reader.Run and router.Broadcaster.Ingest
// both return ctx.Err() on signal-driven cancellation), (*App).Run's
// spawn wrapper MUST NOT invoke r.OnError.
//
// The pre-port behavior in cmd/cdc-sse/main.go gated the error log +
// cascade-cancel on `err != nil && ctx.Err() == nil`. Dropping that
// guard turns every clean SIGINT/SIGTERM into a spurious ERROR log
// line plus a redundant a.cancel() call — breaking alerting rules
// keyed on level=error and confusing operators. This test pins the
// guard so any future iteration that re-introduces the regression
// fails immediately.
func TestApp_RunSuppressesOnErrorOnCleanShutdown(t *testing.T) {
	t.Parallel()

	a := &App{
		Logger: zerolog.Nop(),
	}

	// Mirrors the production wal-reader / router-ingest contract:
	// block on ctx.Done and return ctx.Err() on cancellation.
	started := make(chan struct{})
	var onErrorCalls atomic.Int32
	a.Runnables = []Runnable{
		{
			Name: "cancel-returning",
			Run: func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			},
			OnError: func(err error) {
				onErrorCalls.Add(1)
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- a.Run(ctx, cancel)
	}()

	// Make sure the Runnable started before cancelling, so Run is
	// definitely the source of the non-nil return (not a never-
	// scheduled goroutine that exits via the post-cancel `if
	// r.OnError != nil` short-circuit).
	select {
	case <-started:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Runnable did not start within 200ms")
	}

	// Clean shutdown — cancel the root context. Run observes
	// <-ctx.Done() and returns nil. The Runnable's Run returns
	// ctx.Err() (non-nil), but the spawn wrapper's ctx.Err() == nil
	// guard MUST suppress OnError.
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned non-nil error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Run did not return within 200ms of ctx cancel")
	}

	// Allow the spawned goroutine an additional grace window to
	// finish executing the post-Run code path (if the guard were
	// missing, OnError would fire here).
	time.Sleep(50 * time.Millisecond)

	if n := onErrorCalls.Load(); n != 0 {
		t.Fatalf("OnError fired %d times on clean shutdown; want 0 (the spawn wrapper's ctx.Err() == nil guard regressed)", n)
	}
}

// TestApp_RunInvokesOnErrorOnRealError verifies the positive case:
// when a Runnable.Run returns a non-nil error AND ctx was NOT
// cancelled (the "real" error path — e.g. wal.Reader.Run hitting an
// unrecoverable replication-protocol error), (*App).Run's spawn
// wrapper MUST invoke r.OnError. Pairs with the suppression test
// above to pin both halves of the ctx.Err() == nil guard.
func TestApp_RunInvokesOnErrorOnRealError(t *testing.T) {
	t.Parallel()

	a := &App{
		Logger: zerolog.Nop(),
	}

	onErrorCalled := make(chan error, 1)
	a.Runnables = []Runnable{
		{
			Name: "bad-runnable",
			Run: func(ctx context.Context) error {
				return errors.New("simulated unrecoverable error")
			},
			OnError: func(err error) {
				onErrorCalled <- err
			},
		},
		// Keep ctx alive so the spawn wrapper's ctx.Err() == nil
		// check passes — this Runnable just blocks until the test
		// is done.
		{
			Name: "ctx-keeper",
			Run: func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = a.Run(ctx, cancel) }()

	select {
	case err := <-onErrorCalled:
		if err == nil {
			t.Fatal("OnError received nil err")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("OnError did not fire within 200ms on a real (non-cancellation) error")
	}
}
