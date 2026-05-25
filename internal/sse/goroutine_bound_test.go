// Package sse — goroutine_bound_test.go gates POOL-04 + VAL-04: total
// goroutine count is O(GOMAXPROCS), not O(N subs).
// Attaches 10,000 fake subscribers to a freshly-constructed pool and
// asserts the post-attach `runtime.NumGoroutine()` growth stays within
// `poolSize + 50`, where `poolSize = GOMAXPROCS × cfg.PoolFactor`. ANY
// per-sub goroutine growth would manifest as growth ≈ 10000, far above
// the 50-goroutine slop (50 = stdlib net/http scaffolding + race-detector
// internals + test-runner ambient goroutines + margin for future test
// scaffolding additions, per VAL-04 in REQUIREMENTS.md and
// `.planning/research/batched-flush-redesign.md` §Validation Plan).
// Reads GOMAXPROCS via `runtime.GOMAXPROCS(0)` and does NOT mutate the
// process value (B4 plan-checker fix). Mutating GOMAXPROCS races with
// any other parallel test in the same go-test binary; the assertion is
// a structural property that holds at any GOMAXPROCS value.
// Expected runtime: ≤ 2s on a 4-CPU CI runner. The 10k attach loop is
// the bottleneck; each iteration blocks on the worker's unbuffered
// `attachCh` until accepted.
package sse

import (
	"context"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// boundFakeSub is a subscriber implementation purpose-built for the
// goroutine-bound test. Done() never fires, so the worker's evictDone
// step just polls and skips on every cycle (the intended test shape per
// the 16-04 plan: stub subs accumulate, no per-sub leakage).
type boundFakeSub struct {
	id   string
	done chan struct{} // never closed
	mu   sync.Mutex
	send func(frame []byte) bool
}

func (s *boundFakeSub) ID() string         { return s.id }
func (s *boundFakeSub) KindString() string { return "exact" }
func (s *boundFakeSub) WireSendFunc(send func(frame []byte) bool) {
	s.mu.Lock()
	s.send = send
	s.mu.Unlock()
}
func (s *boundFakeSub) Done() <-chan struct{} { return s.done }
func (s *boundFakeSub) Reason() string        { return "" }

// noopRespWriter implements http.ResponseWriter as a black hole — Write
// succeeds with len(p), Flush is a no-op. The prelude lands in /dev/null
// (we are not asserting wire bytes here, only goroutine count). Sharing
// one across 10k subs is safe: pool.Attach's prelude write is the only
// write before the test's measurement point, and even concurrent writes
// would be harmless (Header / Flush are stateless).
type noopRespWriter struct{}

func (noopRespWriter) Header() http.Header         { return http.Header{} }
func (noopRespWriter) WriteHeader(int)             {}
func (noopRespWriter) Write(p []byte) (int, error) { return len(p), nil }
func (noopRespWriter) Flush()                      {}

// TestPoolGoroutineBoundAt10k asserts that attaching 10,000 subscribers
// does NOT spawn 10,000 goroutines. The pool's design — one worker per
// `GOMAXPROCS × PoolFactor`, no per-sub goroutine — is verified
// structurally: growth ≤ poolSize + 50 (the VAL-04 named bound).
// Per-sub leakage would show as growth ≈ 10000.
func TestPoolGoroutineBoundAt10k(t *testing.T) {
	// Do NOT mutate runtime.GOMAXPROCS — that races with other tests
	// running in parallel in the same go-test binary (B4 plan fix).
	// Read the current value; the gate is a structural property that
	// holds at any GOMAXPROCS.
	maxProcs := runtime.GOMAXPROCS(0)
	cfg := PoolConfig{
		PoolFactor:   2,
		SubQueueSize: 4,
		MaxWaitMs:    2,
		// Long heartbeat so the per-sub time.NewTicker fires at most once
		// during the test; the pool already accounts for these (one ticker
		// per sub) but the test asserts goroutine count, not ticker count,
		// and Go's time package multiplexes tickers onto a fixed runtime
		// goroutine pool (no per-ticker goroutine).
		HeartbeatInterval:     time.Hour,
		WriteTimeout:          time.Second,
		drainShutdownDeadline: 50 * time.Millisecond,
	}
	poolSize := maxProcs * cfg.PoolFactor // workers actually started

	// Snapshot BEFORE constructing the pool so the worker goroutines
	// themselves count toward `growth`. The assertion compensates
	// (growth ≤ poolSize + 50, per VAL-04).
	preCount := runtime.NumGoroutine()

	p := NewPool(cfg, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	const nSubs = 10000
	subs := make([]*boundFakeSub, nSubs)
	rw := noopRespWriter{} // one shared writer — see type comment
	for i := range subs {
		subs[i] = &boundFakeSub{
			id:   "sub-" + strconv.Itoa(i),
			done: make(chan struct{}), // never closed
		}
		rc := http.NewResponseController(rw)
		if _, err := p.Attach(subs[i], nil, rw, rc); err != nil {
			t.Fatalf("Attach[%d]: %v", i, err)
		}
	}

	// Give workers time to settle: the prelude has been written
	// synchronously inside Attach, but the worker's inner poll loop may
	// still be churning. 100ms is well past steady state.
	time.Sleep(100 * time.Millisecond)

	postCount := runtime.NumGoroutine()
	growth := postCount - preCount

	// Slop = 50 (per VAL-04 in REQUIREMENTS.md and
	// `.planning/research/batched-flush-redesign.md` §Validation Plan).
	// The figure covers stdlib net/http accept-loops + race-detector
	// internals + test-runner ambient goroutines + a margin against
	// future test scaffolding additions. ANY per-sub goroutine leak
	// would produce growth ≈ 10000, two orders of magnitude above the
	// bound.
	if growth > poolSize+50 {
		t.Errorf("goroutine growth %d exceeds poolSize(%d)+50; per-sub goroutine leak likely (preCount=%d, postCount=%d, GOMAXPROCS=%d, PoolFactor=%d)",
			growth, poolSize, preCount, postCount, maxProcs, cfg.PoolFactor)
	}
	t.Logf("goroutine bound OK: preCount=%d, postCount=%d, growth=%d, poolSize=%d, GOMAXPROCS=%d",
		preCount, postCount, growth, poolSize, maxProcs)
}
