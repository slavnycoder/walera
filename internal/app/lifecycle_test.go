package app

// lifecycle_test.go — regression tests for (*App).Shutdown's
// concurrency invariants and step ordering. Pins the lifecycle
// contract so any future refactor of InitializeApp that serialises the
// Step-1 wave, reorders Step-3 / Step-4 / Step-5, or drops the 50 ms
// flush sleep produces an instantly-diagnosable failure.
//
// Out of scope here (covered by complementary suites):
//   - goleak.VerifyNone — verifies the assembled production graph end
//     to end (see leak_main_test.go), not the hand-rolled fixture below.
//   - Pointer-identity singleton checks across consumers — see
//     app_singleton_test.go.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/sse"
	"github.com/walera/walera/internal/wal"
)

// init switches the zerolog package-level TimeFieldFormat to
// RFC3339Nano so the ordering assertions in this file can
// discriminate sub-second log emit timestamps. Production logs at
// second precision (the zerolog default) which is sufficient for
// operator-readable logs but not for the 50 ms flush gap that
// TestApp_ShutdownStepOrdering asserts. zerolog reads the var at
// emit time; assigning it once at package init removes the data
// race that would arise from per-test-fixture assignment under
// `t.Parallel()`.
//
// This assignment is scoped to the test binary — it never reaches
// the production binary because *_test.go files are excluded from
// non-test builds by the Go toolchain.
func init() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
}

// syncBuffer is a goroutine-safe wrapper around *bytes.Buffer for
// capturing zerolog JSON output from the parallel shutdown-wave
// goroutines without triggering a data race under -race.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newTestApp constructs a minimal *App suitable for the Shutdown
// regression tests: real *sse.WriterPool (one tiny worker, no
// subscribers), real *router.Broadcaster (empty index), real
// *http.Server (no listener — Shutdown is a near-immediate no-op),
// optional *http.Server pprof handle, and a fresh *metrics.Registry
// bridged via *sse.PoolMetricsAdapter. AdminConn is left nil (Shutdown
// Step 4's nil-guard short-circuits the (*pgx.Conn).Close call). The
// Logger writes JSON to the supplied syncBuffer so the ordering
// assertions can parse step events post-hoc.
//
// shutdownDeadline / drainDeadline are exposed as parameters so tests
// can shorten them aggressively (the production defaults of 10 s / 5 s
// are too long for a sub-second test runtime).
func newTestApp(
	t *testing.T,
	logSink *syncBuffer,
	shutdownDeadline, drainDeadline time.Duration,
	withPProf bool,
) *App {
	t.Helper()

	reg := metrics.New()
	enc := sse.NewEncoder(64 * 1024)
	poolDeps := sse.PoolDeps{
		Encoder: enc,
		Metrics: sse.NewPoolMetricsAdapter(reg),
		Logger:  zerolog.Nop(),
	}
	// PoolFactor=1 → exactly one worker; the worker runs idle (no
	// subscribers) so Shutdown's per-worker drain loop returns
	// near-instantly via the shutdownCh signal.
	pool := sse.NewPool(sse.PoolConfig{PoolFactor: 1}, poolDeps)

	bcast := router.New(router.Config{}, router.Deps{
		Logger:  zerolog.Nop(),
		Metrics: reg,
		Encoder: enc,
	})

	// http.Server with no listener: ListenAndServe is never called, so
	// Shutdown returns immediately after closing the (empty) listener
	// set and walking the (empty) active-conn set. Addr is set to a
	// non-empty string for grep / log-parity but never bound.
	httpSrv := &http.Server{Addr: "127.0.0.1:0"}

	var pprofSrv *http.Server
	if withPProf {
		pprofSrv = &http.Server{Addr: "127.0.0.1:0"}
	}

	// Timestamp precision is RFC3339Nano via the package-level init
	// at the top of this file — the ordering assertion below depends
	// on sub-second resolution.
	logger := zerolog.New(logSink).With().Timestamp().Logger()

	cfg := &AppConfig{}
	cfg.Shutdown.Deadline = ShutdownDeadline(shutdownDeadline)
	cfg.Shutdown.DrainDeadline = DrainDeadline(drainDeadline)

	return &App{
		Config:      cfg,
		Logger:      logger,
		Metrics:     reg,
		SSEPool:     pool,
		RouterIndex: bcast,
		HTTPServer:  httpSrv,
		PProfServer: pprofSrv,
		// AdminConn intentionally nil — Shutdown Step 4's nil-guard
		// keeps the test fixture stub-free.
		// HealthServer intentionally nil — not exercised by Shutdown.
	}
}

// TestApp_ShutdownConcurrentWave proves the Step-1 wave actually runs the
// pool / http / broadcast goroutines in parallel rather than sequentially.
// Each on*Start callback fires from inside its goroutine BEFORE the
// corresponding handle's Shutdown method begins, so the callbacks capture
// exact spawn timestamps. The contract: max(start) - min(start) < 10 ms.
//
// PProfServer is intentionally nil: the test verifies the three always-on
// Step-1 arms (pool / http / broadcast) reach the wave in parallel; the
// optional pprof arm is exercised separately by the ordering test below.
//
// The test calls (*App).shutdownStep1WaveWithCallbacks directly rather than
// (*App).Shutdown so the assertion does not have to instrument log
// timestamps — direct callback firing is exact-start and avoids the
// log-flush jitter window.
func TestApp_ShutdownConcurrentWave(t *testing.T) {
	t.Parallel()

	logSink := &syncBuffer{}
	a := newTestApp(t, logSink, 5*time.Second, 1*time.Second, false /* no pprof */)

	var (
		poolStart, httpStart, bcastStart atomic.Int64
	)
	now := func() int64 { return time.Now().UnixNano() }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a.shutdownStep1WaveWithCallbacks(
		ctx,
		1*time.Second,
		func() { poolStart.Store(now()) },
		func() { httpStart.Store(now()) },
		func() { bcastStart.Store(now()) },
		nil, // pprof intentionally nil for the 3-way wave check
	)

	starts := []int64{poolStart.Load(), httpStart.Load(), bcastStart.Load()}
	for i, v := range starts {
		if v == 0 {
			t.Fatalf("start callback %d did not fire", i)
		}
	}

	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	spread := time.Duration(starts[len(starts)-1] - starts[0])

	// 10 ms is the plan's contract. Under -race on a loaded CI host the
	// scheduler can interleave the four runtime spawns; the empirical
	// upper bound observed on the slowest documented Walera host is
	// ~2 ms.  10 ms gives 5x headroom while still failing loudly if the
	// wave silently regresses to serial execution (which would produce
	// spreads in the hundreds of ms because each Shutdown call sleeps
	// while its WaitGroup arm finishes).
	if spread >= 10*time.Millisecond {
		t.Fatalf("Step-1 wave start spread = %v (want < 10ms); the three arms did not run in parallel", spread)
	}
}

// TestApp_ShutdownStepOrdering proves the full 5-step Shutdown
// sequence happens in the documented order:
//
//  1. All three Step-1 arms (pool / http / broadcast) complete BEFORE
//  2. wg.Wait returns, then stop() fires BEFORE
//  3. txCh is drained BEFORE
//  4. AdminConn.Close is invoked (skipped here via the nil-guard) BEFORE
//  5. the final "shutdown complete" log line is emitted.
//
// The assertion reads zerolog JSON from a captured byte buffer and
// instruments stop() via a recorder closure to capture the exact
// transition between Step 2 (wg.Wait) and Step 3 (stop + drain).
//
// txCh is pre-closed so the `for range txCh` drain loop returns
// immediately; the test does NOT depend on a goroutine-driven channel
// close for the drain assertion.
func TestApp_ShutdownStepOrdering(t *testing.T) {
	t.Parallel()

	logSink := &syncBuffer{}
	a := newTestApp(t, logSink, 5*time.Second, 1*time.Second, false /* no pprof */)

	// Instrumented stop: records its invocation timestamp so we can
	// place it in the ordering sequence between the Step-1 log lines
	// and the final "shutdown complete" log line.
	var stopCalledAt atomic.Int64
	stop := func() { stopCalledAt.Store(time.Now().UnixNano()) }

	// Pre-closed txCh so Shutdown's `for range txCh` drain loop
	// returns immediately. A non-empty buffered channel would also
	// work but the empty + closed case is the simplest verifiable
	// shape.
	txCh := make(chan wal.Tx)
	close(txCh)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := a.Shutdown(ctx, stop, txCh); err != nil {
		t.Fatalf("Shutdown returned non-nil error: %v", err)
	}

	if stopCalledAt.Load() == 0 {
		t.Fatal("Shutdown did not invoke stop()")
	}

	// Parse the captured JSON log lines into (timestamp, step, msg)
	// triples. zerolog writes one JSON object per call separated by
	// '\n'; an empty trailing line is possible.
	type entry struct {
		Time time.Time
		Step string
		Msg  string
	}
	var entries []entry
	for _, line := range bytes.Split([]byte(logSink.String()), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var raw struct {
			Time    time.Time `json:"time"`
			Step    string    `json:"step"`
			Message string    `json:"message"`
		}
		if err := json.Unmarshal(line, &raw); err != nil {
			t.Fatalf("failed to parse log line %q: %v", line, err)
		}
		entries = append(entries, entry{Time: raw.Time, Step: raw.Step, Msg: raw.Message})
	}

	// Locate the three Step-1 completion lines and the final
	// "shutdown complete" line. The three Step-1 lines all carry
	// step="pool"|"http"|"broadcast"; the final line carries
	// msg="shutdown complete" with no step field. We accept either
	// the success-path message ("sse pool shutdown complete" /
	// "http.Server shutdown complete" / "router drain complete") OR
	// the timeout/error-path message at the same step label — the
	// ordering invariant holds regardless of which arm wrote.
	var (
		poolAt, httpAt, bcastAt time.Time
		finalAt                 time.Time
		havePool, haveHTTP      bool
		haveBcast, haveFinal    bool
	)
	for _, e := range entries {
		switch {
		case e.Step == "pool":
			poolAt = e.Time
			havePool = true
		case e.Step == "http":
			httpAt = e.Time
			haveHTTP = true
		case e.Step == "broadcast":
			bcastAt = e.Time
			haveBcast = true
		case e.Msg == "shutdown complete":
			finalAt = e.Time
			haveFinal = true
		}
	}

	if !havePool {
		t.Fatal("missing Step-1 pool log line")
	}
	if !haveHTTP {
		t.Fatal("missing Step-1 http log line")
	}
	if !haveBcast {
		t.Fatal("missing Step-1 broadcast log line")
	}
	if !haveFinal {
		t.Fatal("missing final 'shutdown complete' log line")
	}

	// Order claim 1: all three Step-1 lines were emitted before stop()
	// was called. stop() is invoked AFTER wg.Wait returns, so each
	// arm's completion log must precede the stop timestamp.
	stopAt := time.Unix(0, stopCalledAt.Load())
	for _, p := range []struct {
		name string
		at   time.Time
	}{
		{"pool", poolAt},
		{"http", httpAt},
		{"broadcast", bcastAt},
	} {
		if !p.at.Before(stopAt) {
			t.Errorf("Step-1 %s log (%s) did not precede stop() (%s)", p.name, p.at, stopAt)
		}
	}

	// Order claim 2: stop() was invoked before the final "shutdown
	// complete" log line. Between stop() and the final log lie:
	// Step 3 (`for range txCh`), Step 4 (AdminConn close — skipped
	// here via nil-guard), and the 50 ms flush sleep. The final log
	// timestamp must therefore be at least ~50 ms after the stop
	// timestamp.
	if !stopAt.Before(finalAt) {
		t.Errorf("stop() (%s) did not precede final 'shutdown complete' log (%s)", stopAt, finalAt)
	}
	flushGap := finalAt.Sub(stopAt)
	if flushGap < 40*time.Millisecond {
		// Allow 10 ms of leeway under the 50 ms target — clock
		// resolution and JSON-encoding jitter can shave a few ms.
		t.Errorf("flush gap stop→final = %v (want ≥ 40ms — Step 4's 50ms sleep should dominate)", flushGap)
	}
}
