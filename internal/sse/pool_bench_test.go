// Package sse — pool_bench_test.go is the  entry point that
// exercises the two pool-worker hot paths the  CI regression
// gate samples on every PR touching internal/sse/...:
//
//   - BenchmarkPoolWorkerRun covers  — the per-cycle work
//     inside (*poolWorker).run. The iteration body is exactly the inner
//     drainAll-loop step that  will later split into
//     markDirty/drainAll/pollAllQueues helpers: enqueue a frame into
//     each owned sub's buffer and call w.drainSub on every dirty sub.
//     The closures inside run() (markDirty, drainAll, pollAllQueues,
//     sweepHeartbeats, evictDone) are not method-accessible so we drive
//     the inner kernel directly via the *poolWorker's exported
//     drainSub — same per-sub write surface, same metric emission, same
//     allocation profile. No new exported helper is introduced.
//
//   - BenchmarkPoolWorkerDrainShutdown covers  — the
//     (*poolWorker).drainShutdown teardown surface. drainShutdown is
//     DESTRUCTIVE (closes st.done, marks st.inDisconnected) so each
//     iteration MUST allocate a fresh worker + freshly attached subs
//     inside the timed region; the setup cost is paid by both the
//     baseline and the PR run and the ratio cancels in benchstat's
//     Mann-Whitney comparison.
//
// Fixtures (fakeEncoder, newFakeMetrics, fakeSub, fakeResponseWriter)
// come from pool_test.go via same-package access — no fixture lift, no
// new helper file. Heartbeat noise is suppressed via
// HeartbeatInterval: time.Hour per  baseline header guidance.
package sse

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// benchPoolConfig returns a PoolConfig tuned for deterministic
// allocation accounting: heartbeat sweep effectively disabled
// (HeartbeatInterval set to one hour so the per-worker hbTicker cannot
// fire mid-iteration); drainShutdownDeadline tight at 50 ms so the
//
//	bench mirrors the spec §3.5 cap the production path
//
// honours; DrainThresholdSubs=1 so the dirty-list shape matches the
// "eager drain" regime  will refactor.
func benchPoolConfig() PoolConfig {
	return PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          64,
		MaxWaitMs:             2,
		DrainThresholdSubs:    1,
		MaxBatchBytesPerSub:   64 * 1024,
		WriteTimeout:          200 * time.Millisecond,
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: 50 * time.Millisecond,
	}
}

// benchFrame is the payload used by both benchmarks. 64 bytes mimics a
// typical Walera SSE event line (`data: {...}\n\n`) without inflating
// the per-iteration syscall cost beyond steady-state.
var benchFrame = []byte("data: {\"k\":\"v\",\"id\":\"abcdef0123456789\",\"ts\":1700000000}\n\n")

// buildBenchWorker constructs a *poolWorker with the benchPoolConfig +
// the same-package fakeEncoder / newFakeMetrics fixtures. Worker is
// NOT started — the bench drives drainSub directly (BenchmarkPoolWorkerRun)
// or drainShutdown directly (BenchmarkPoolWorkerDrainShutdown). Avoids
// the run() goroutine entirely so the bench measures the function under
// test without scheduler / channel-handoff noise.
func buildBenchWorker() *poolWorker {
	return newPoolWorker(0, benchPoolConfig(), fakeEncoder{}, newFakeMetrics(), zerolog.Nop())
}

// buildBenchSubState constructs a *subState wired to a fresh
// fakeResponseWriter + http.ResponseController. Mirrors the manual
// construction pattern already in pool_test.go (around line 2708) for
// the drainSubDeadline regression test — no Attach round-trip, no
// run-loop. id is unique per-call so xxhash sharding stays deterministic
// across sub-bench shapes.
func buildBenchSubState(id string) *subState {
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	return &subState{
		sub:         &fakeSub{id: id, kind: "wildcard"},
		queue:       make(chan []byte, 64),
		respWriter:  rw,
		rc:          rc,
		done:        make(chan struct{}),
		connectedAt: time.Now(),
		lastWriteAt: time.Now(),
	}
}

// benchSubsShapes is the table-driven fan-out the  baseline
// captures: 1 (per-sub kernel), 8 (typical worker partition under a
// 4-CPU pod with 100 attached subs), 64 (busy partition with sustained
// inflow). benchstat sub-bench naming convention is lower-snake.
var benchSubsShapes = []struct {
	name string
	n    int
}{
	{"subs_1", 1},
	{"subs_8", 8},
	{"subs_64", 64},
}

// BenchmarkPoolWorkerRun exercises  — the per-cycle work inside
// (*poolWorker).run's drainAll closure. Each iteration appends one
// frame to each sub's buffer (the markDirty + buffer-append step) and
// calls w.drainSub on every sub (the drainAll step that issues one
// writev / Write+Flush per dirty sub and emits EventsSentInc per
// frame). This is the inner kernel that  will later split
// into markDirty / drainAll / pollAllQueues helpers; the per-iteration
// allocation profile here is what the  gate samples to detect
// any regression introduced by that split.
func BenchmarkPoolWorkerRun(b *testing.B) {
	for _, shape := range benchSubsShapes {
		b.Run(shape.name, func(b *testing.B) {
			w := buildBenchWorker()
			states := make([]*subState, shape.n)
			for i := 0; i < shape.n; i++ {
				states[i] = buildBenchSubState("bench-run-" + strconv.Itoa(i))
			}
			// b.ReportAllocs() is required when using b.Loop() to enable
			// alloc reporting in the absence of the -benchmem CLI flag.
			b.ReportAllocs()
			for b.Loop() {
				now := time.Now()
				// markDirty + buffer-append phase: production code does
				// this inside pollAllQueues. We replay it inline so the
				// dirty-list growth + bufBytes accounting matches the
				// run-loop's per-cycle shape.
				for _, st := range states {
					st.buffer = append(st.buffer, benchFrame)
					st.bufBytes += len(benchFrame)
				}
				// drainAll phase: production code calls w.drainSub on
				// every dirty sub. drainSub resets st.buffer back to
				// empty + zeroes st.bufBytes, so the next iteration
				// starts from the same shape as the run-loop's next
				// pollAllQueues entry.
				for _, st := range states {
					w.drainSub(st, now)
				}
			}
			// Keep fixtures alive across the loop so the compiler cannot
			// DCE-elide the buffer mutations / drainSub calls above.
			_ = w
			_ = states
		})
	}
}

// BenchmarkPoolWorkerDrainShutdown exercises  — the
// (*poolWorker).drainShutdown teardown path. drainShutdown is
// destructive (closes st.done, marks st.inDisconnected, emits the
// per-sub shutdown frame via EncodeShutdown), so the per-iteration
// body MUST allocate a fresh worker + freshly attached subs. There is
// no way to amortise the setup. The setup cost is paid by both the
// baseline and the PR run; benchstat's Mann-Whitney comparison sees
// the ratio, not the absolute, so the inclusion is benign for
// regression detection.
func BenchmarkPoolWorkerDrainShutdown(b *testing.B) {
	for _, shape := range benchSubsShapes {
		b.Run(shape.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				w := buildBenchWorker()
				for i := 0; i < shape.n; i++ {
					st := buildBenchSubState("bench-shutdown-" + strconv.Itoa(i))
					// attachSub records st in w.subs without running the
					// full Attach handshake (no attachCh / no
					// goroutine). drainShutdown iterates w.subs, so
					// this is the minimal wiring drainShutdown needs.
					w.attachSub(st)
				}
				w.drainShutdown()
				// Keep w alive past drainShutdown so the
				// per-sub close(st.done) writes cannot be DCE-elided.
				_ = w
			}
		})
	}
}
