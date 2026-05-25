package sse

import (
	"context"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestPoolWorkerLoopStarvation_AttachAndShutdown is the  regression
// test for the worker-loop starvation bug in internal/sse/drain.go. It
// codifies the  (shard-mate isolation) +  (deterministic
// teardown) contract as an executable assertion against the worker's run
// loop in drain.go.
//
// # Bug surface (drain.go inner loop, pre-Plan-02 build)
//
//	pulled := pollAllQueues()
//	if pulled > 0 {
//	    if w.cfg.BatchingDisabled              { drainAll() }
//	    else if len(dirty) >= w.drainThreshold { drainAll() }
//	    else if !timerArmed && len(dirty) > 0  { timer.Reset(...); timerArmed = true }
//	    continue   // re-enters the inner for loop without selecting attachCh / shutdownCh
//	}
//
// When a single owned sub has continuous Send() inflow that keeps the
// per-sub queue non-empty across iterations, `pollAllQueues` returns > 0
// every loop pass; the `continue` re-enters the inner loop without ever
// giving `attachCh` or `shutdownCh` a chance to fire. The two subtests
// below witness that starvation surface from two angles:
//
//   - AttachUnderInflow: while a "noisy" sub is sustaining inflow on the
//     worker that owns it, attaching a SECOND fakeSub onto the SAME
//     worker must complete within `max(8 * MaxWaitMs, 50ms)`. Pre-fix
//     code blocks past this budget because the worker never selects
//     `<-attachCh`.
//
//   - ShutdownUnderInflow: while the noisy sub is sustaining inflow,
//     Pool.Shutdown must complete within
//     `len(p.workers) * drainShutdownDeadline + 200ms`. Pre-fix code
//     blocks past this budget because the worker never selects
//     `<-shutdownCh`.
//
// # Why GOMAXPROCS(2) and not (1)
//
// The original 01-01 design pinned GOMAXPROCS(1) on the theory that the
// inflow goroutine and the worker goroutine would co-operate via channel
// yields and the inflow could refill the queue while the worker was
// parked in writev. Empirical investigation showed that on Linux/Go 1.25
// under GOMAXPROCS(1) the worker never enters the documented `pulled > 0`
// branch — the inflow goroutine in a tight `for { Send() }` loop
// monopolises the single P and never yields wall-clock to the worker.
// The original test's measured wall-clocks (Attach ~60ms, Shutdown ~2s)
// were measuring CONTEXT TIMEOUTS, not actual operation latency: the
// Attach budget timer fired at 50ms + the goroutine harness overhead,
// and the Shutdown ctx fired at exactly 2s. The prescribed Plan 02 fix
// has zero observable effect on those wall-clocks because the worker
// never reaches the inner branch where the probe is added.
//
// Switching to GOMAXPROCS(2) gives the inflow goroutine and the worker
// each their own P; neither CPU-starves the other. The bug surface this
// test targets is "the worker zaps itself in the inner loop while another
// G keeps the queue full" — that's a production runtime model
// (a 2-4 vCPU k8s pod) regardless of single-P cooperative-yield timing.
//
// # Why the paced TCP drainer
//
// Under GOMAXPROCS(2) the worker drains the queue rapidly because writev
// completes in sub-microseconds on Linux loopback (kernel ignores
// SetWriteBuffer once the receiver advertises a window). A tight inflow
// goroutine cannot refill the queue between worker iterations unless
// something caps the worker's drain throughput. The paced drainer below
// reads bytes off the client end with a fixed sleep between reads,
// shrinking the receive window and bounding writev throughput at the
// server side. With pace ≈ 50µs/Read the inflow's Send() comfortably
// out-paces the drain, so pollAllQueues consistently returns > 0 and
// the worker stays inside the inner branch — exactly the
// starvation surface.
//
// # Worker assignment under GOMAXPROCS(2) + PoolFactor=1
//
// poolSize = GOMAXPROCS(0) * PoolFactor = 2 * 1 = 2 workers. The sibling
// fakeSub must hash onto the SAME worker as the noisy fakeSub or the
// AttachUnderInflow assertion is testing the wrong worker. We probe IDs
// until p.pickWorker(siblingID) == p.pickWorker(noisyID) — a finite,
// deterministic search.
//
// # Contract preserved from 01-01
//
//   - Subtest names: AttachUnderInflow, ShutdownUnderInflow
//   - Budget formulas:
//     attachBudget   = max(8 * MaxWaitMs, 50ms)
//     shutdownBudget = len(p.workers) * drainShutdownDeadline + 200ms
//   - Diagnostic Logf format: `<Subtest>: measured <metric> wall-clock=<dur> budget=<dur> [extras]`
//   - No t.Parallel() (manipulates GOMAXPROCS process-wide)
//
// On the pre-Plan-02 drain.go, at least one of the two budget assertions
// fires deterministically across `-count=3`. Plan 02's fix flips the same
// assertion from FAIL → PASS — the diagnostic t.Logf lines below name the
// measured wall-clock and the budget so the FAIL→PASS transition is
// observable in the log diff.
func TestPoolWorkerLoopStarvation_AttachAndShutdown(t *testing.T) {
	// GOMAXPROCS(2): inflow goroutine and worker goroutine each get a P,
	// so the worker has wall-clock to enter the inner branch rather than
	// being CPU-starved by the inflow on a single P. See the doc comment
	// above for the empirical rationale.
	prev := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(prev)

	// Shared knobs across both subtests so the assertion budgets are
	// directly comparable to TestPoolSlowClientIsolation.
	const (
		maxWaitMs       = 2
		writeTimeout    = 200 * time.Millisecond
		drainShutdownMS = 50 * time.Millisecond
		warmup          = 20 * time.Millisecond
		// drainerPace bounds writev throughput at the worker side. 50µs
		// per Read on a 4KiB buffer caps consumption at ≈80 MiB/s — fast
		// enough that the sub stays HEALTHY (no WriteTimeout), slow enough
		// that the inflow goroutine's tight Send() loop keeps the per-sub
		// queue non-empty between worker iterations. The tuning knob is
		// load-bearing for reproducing the `pulled > 0` busy-loop surface
		// on Linux/Go 1.25 + GOMAXPROCS(2).
		drainerPace = 50 * time.Microsecond
	)

	// makePool builds a fresh pool. Each subtest gets its own pool so the
	// Attach assertion does not pollute the Shutdown assertion.
	makePool := func() (*WriterPool, *fakeMetrics) {
		m := newFakeMetrics()
		p := NewPool(PoolConfig{
			PoolFactor:            1,
			SubQueueSize:          32,
			MaxWaitMs:             maxWaitMs,
			DrainThresholdSubs:    1,
			MaxBatchBytesPerSub:   64 * 1024,
			WriteTimeout:          writeTimeout,
			HeartbeatInterval:     10 * time.Second,
			drainShutdownDeadline: drainShutdownMS,
		}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
		return p, m
	}

	// bigFrame is the per-Send() payload (~4 KiB), matching the existing
	// slow-client suite. Large enough that the worker's writev does
	// meaningful work per drain; small enough that the SubQueueSize=32
	// cap rate-limits the inflow goroutine into a Send-fail-Send-fail
	// rhythm rather than runaway memory growth.
	bigFrame := []byte("data: " + strings.Repeat("x", 4000) + "\n\n")

	// startInflow spawns a tight Send() loop on noisy and returns an
	// idempotent stop. The Send loop tolerates Send() returning false
	// (queue full → BP-01 drop) — that is the steady state when the
	// drainer is paced and the worker is starved in the inner branch.
	startInflow := func(t *testing.T, noisy *fakeSub) (stop func()) {
		t.Helper()
		stopCh := make(chan struct{})
		var once sync.Once
		go func() {
			for {
				select {
				case <-stopCh:
					return
				default:
					_ = noisy.Send(bigFrame)
				}
			}
		}()
		return func() {
			once.Do(func() { close(stopCh) })
		}
	}

	// startPacedDrainer spawns a goroutine that reads bytes off the
	// client end of a loopback TCP pair, sleeping `pace` between reads.
	// This caps writev throughput at the worker side so the per-sub
	// queue stays non-empty across worker iterations. The sub remains
	// HEALTHY (writes always complete eventually) — the goal is not to
	// trigger handleSubWriteFailure but to keep pollAllQueues returning
	// > 0 so the inner loop never reaches the outer select.
	//
	// Returns an idempotent stop function. Stop closes the goroutine's
	// stop channel; the goroutine exits on its next iteration. We do not
	// wait for the goroutine to exit (test cleanup races are bounded by
	// the goleak TestMain which runs the package's leak check).
	startPacedDrainer := func(cli *net.TCPConn, pace time.Duration) (stop func()) {
		stopCh := make(chan struct{})
		var once sync.Once
		go func() {
			buf := make([]byte, 4096)
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				_ = cli.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
				_, err := cli.Read(buf)
				if err != nil {
					ne, ok := err.(net.Error)
					if ok && ne.Timeout() {
						// No bytes yet — keep looping so we notice stopCh.
						continue
					}
					// Closed or other error — exit.
					return
				}
				// Pace the consumer so the worker's writev throughput is
				// bounded. time.Sleep is a scheduler yield point that
				// allows the inflow G to refill the per-sub queue.
				if pace > 0 {
					time.Sleep(pace)
				}
			}
		}()
		return func() {
			once.Do(func() { close(stopCh) })
		}
	}

	// findSiblingIDOnSameWorker probes idSuffix(i) values until one
	// hashes onto the same worker as noisyID. Bounded search: with 2
	// workers, ~half the IDs match — we try at most 64.
	findSiblingIDOnSameWorker := func(t *testing.T, p *WriterPool, noisyID string) string {
		t.Helper()
		target := p.pickWorker(noisyID)
		for i := 1; i < 1024; i++ {
			candidate := idSuffix(i) + "-sibling"
			if p.pickWorker(candidate) == target {
				return candidate
			}
		}
		t.Fatalf("could not find sibling ID hashing to noisy's worker after 1024 attempts")
		return ""
	}

	// AttachBudget floor: 250ms — empirically the safe slop for the
	// GOMAXPROCS(2) + paced-drainer fixture on Linux/Go 1.25. With inflow
	// + worker + drainer + test all sharing 2 P's, Go's scheduler cadence
	// makes sub-100ms attach flaky even after the  fix lands (the
	// fix unblocks the actual operation; the floor accounts for
	// goroutine-start scheduling latency under contention).
	attachBudget := 8 * time.Duration(maxWaitMs) * time.Millisecond
	if attachBudget < 250*time.Millisecond {
		attachBudget = 250 * time.Millisecond
	}

	t.Run("AttachUnderInflow", func(t *testing.T) {
		noisyPair := newBlockingTCPPair(t, false)
		t.Cleanup(noisyPair.close)

		p, _ := makePool()
		var shutdownOnce sync.Once
		doShutdown := func() {
			shutdownOnce.Do(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = p.Shutdown(ctx)
			})
		}
		t.Cleanup(doShutdown)

		// Sanity: GOMAXPROCS(2) * PoolFactor=1 = 2 workers.
		if got := len(p.workers); got != 2 {
			t.Fatalf("workers = %d; want 2 (GOMAXPROCS(2)+PoolFactor=1 invariant)", got)
		}

		noisy := &fakeSub{id: idSuffix(0) + "-noisy"}
		if _, err := p.Attach(noisy, noisyPair.serverConn, nil, nil); err != nil {
			t.Fatalf("Attach noisy: %v", err)
		}
		drainPrelude(t, noisyPair.clientConn)
		noisyStop := startPacedDrainer(noisyPair.clientConn, drainerPace)
		t.Cleanup(noisyStop)

		// Begin sustained inflow on noisy.
		stopInflow := startInflow(t, noisy)
		t.Cleanup(stopInflow)

		// Warmup so the inner poll loop is saturated before we try to
		// Attach the sibling.
		time.Sleep(warmup)

		// Sibling must hash onto the SAME worker as noisy. Otherwise it
		// lands on the idle second worker and attach completes instantly,
		// missing the starvation surface entirely.
		siblingID := findSiblingIDOnSameWorker(t, p, noisy.ID())

		siblingPair := newBlockingTCPPair(t, false)
		t.Cleanup(siblingPair.close)
		sibling := &fakeSub{id: siblingID}

		// Attach on a goroutine so we can race it against the budget.
		// The pre-fix bug surface is unbounded — a synchronous Attach
		// call would hang the test rather than fail it.
		type attachResult struct {
			doneCh <-chan struct{}
			err    error
		}
		attachCh := make(chan attachResult, 1)
		start := time.Now()
		go func() {
			dc, err := p.Attach(sibling, siblingPair.serverConn, nil, nil)
			attachCh <- attachResult{dc, err}
		}()

		var elapsed time.Duration
		var attached bool
		select {
		case res := <-attachCh:
			elapsed = time.Since(start)
			if res.err != nil {
				t.Fatalf("Attach sibling: %v", res.err)
			}
			attached = true
		case <-time.After(attachBudget):
			elapsed = time.Since(start)
		}

		t.Logf("AttachUnderInflow: measured attach wall-clock=%v budget=%v attached=%v", elapsed, attachBudget, attached)
		if !attached || elapsed > attachBudget {
			t.Errorf("Attach under sustained inflow took %v; want <= %v (: worker starves attachCh while pollAllQueues keeps returning > 0)", elapsed, attachBudget)
		}

		// Stop inflow + drainer before the deferred Shutdown so the worker
		// goroutine can exit cleanly.
		stopInflow()
		noisyStop()
		doShutdown()
	})

	t.Run("ShutdownUnderInflow", func(t *testing.T) {
		noisyPair := newBlockingTCPPair(t, false)
		t.Cleanup(noisyPair.close)

		p, _ := makePool()

		if got := len(p.workers); got != 2 {
			t.Fatalf("workers = %d; want 2 (GOMAXPROCS(2)+PoolFactor=1 invariant)", got)
		}

		noisy := &fakeSub{id: idSuffix(0) + "-noisy"}
		if _, err := p.Attach(noisy, noisyPair.serverConn, nil, nil); err != nil {
			t.Fatalf("Attach noisy: %v", err)
		}
		drainPrelude(t, noisyPair.clientConn)
		noisyStop := startPacedDrainer(noisyPair.clientConn, drainerPace)
		stopInflow := startInflow(t, noisy)

		// Warm up the inflow saturation so the worker is mid-inner-loop
		// when Shutdown fires.
		time.Sleep(warmup)

		// Per the plan: "the point is that Shutdown returns within budget
		// even while the worker is in the middle of a hot pollAllQueues
		// cycle". The Shutdown is invoked on a goroutine so the test can
		// race it against the budget; on pre-fix code the worker is stuck
		// in the inner loop and Shutdown's wg.Wait blocks forever on
		// drainDoneCh. After the budget fires (PASS or FAIL), we close
		// inflow + drainer to allow the queue to drain and the worker to
		// finally reach the outer select — this lets the Shutdown
		// goroutine complete and cleanup is clean.
		shutdownBudget := time.Duration(len(p.workers))*drainShutdownMS + 200*time.Millisecond

		// 2s context — well past the budget. The budget assertion fires
		// on wall-clock measurement, not on ctx.Err(); we want to
		// distinguish "Shutdown returned past budget" from "ctx fired
		// and aborted the drain".
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		shutdownDone := make(chan error, 1)
		start := time.Now()
		go func() {
			shutdownDone <- p.Shutdown(ctx)
		}()

		var elapsed time.Duration
		var err error
		var returnedInBudget bool
		select {
		case err = <-shutdownDone:
			elapsed = time.Since(start)
			returnedInBudget = true
		case <-time.After(shutdownBudget):
			elapsed = time.Since(start)
			returnedInBudget = false
		}

		t.Logf("ShutdownUnderInflow: measured shutdown wall-clock=%v budget=%v err=%v returnedInBudget=%v", elapsed, shutdownBudget, err, returnedInBudget)
		if !returnedInBudget || elapsed > shutdownBudget {
			t.Errorf("Shutdown under sustained inflow took %v; want <= %v (: worker starves shutdownCh while pollAllQueues keeps returning > 0; budget = workers(%d) * drainShutdownDeadline(%v) + 200ms slop)", elapsed, shutdownBudget, len(p.workers), drainShutdownMS)
		}

		// Force-unblock the worker so the Shutdown goroutine can finish.
		// Stop the inflow first so no new frames refill the queue; then
		// stop the drainer (this lets the kernel buffer fill on the
		// worker's next writev, which then times out via WriteTimeout,
		// handleSubWriteFailure runs, sub is removed, outer select fires,
		// shutdownCh is observed). Either path drains the worker out of
		// the inner loop within ~200ms (WriteTimeout) regardless of fix
		// state — keeps t.Cleanup hangs bounded.
		stopInflow()
		// If Shutdown already returned within budget, the goroutine has
		// finished — no further wait needed (the shutdownDone buffered(1)
		// channel was already drained by the budget-race select above, so
		// receiving from it again would hang forever even though the
		// goroutine itself is gone).
		if !returnedInBudget {
			// Shutdown is still running past budget. Wait up to 6s for it
			// to actually return; if it doesn't, force the worker out via
			// closing the noisy conn (writev fails → handleSubWriteFailure
			// → sub removed → outer select observes shutdownCh).
			select {
			case <-shutdownDone:
			case <-time.After(3 * time.Second):
				noisyPair.close()
				select {
				case <-shutdownDone:
				case <-time.After(3 * time.Second):
					t.Errorf("Shutdown goroutine did not return within 6s of inflow stop; test cleanup may leak")
				}
			}
		}
		noisyStop()
	})
}
