// Package auth — breaker_window.go: 30-bucket sliding-window ring buffer
// powering the breaker failure-rate calculation. See internal/auth/breaker.go
// for FSM semantics; this file is lock-free counters only.
package auth

import "sync/atomic"

// windowBuckets is the number of buckets in the sliding window. Fixed at 30.
const windowBuckets = 30

// bucket is one slot in the ring buffer.
type bucket struct {
	successes atomic.Uint64
	failures  atomic.Uint64
}

// window is the 30-bucket ring buffer. `current` is the index Record writes to;
// the FSM goroutine advances `current` via tick().
type window struct {
	buckets [windowBuckets]bucket
	current atomic.Uint64
}

// newWindow returns a zeroed window with current=0.
func newWindow() *window {
	return &window{}
}

// Record atomically increments the success or failure counter in the current
// bucket. Zero allocations, zero locks; safe for concurrent callers.
func (w *window) Record(success bool) {
	idx := w.current.Load() % uint64(windowBuckets)
	if success {
		w.buckets[idx].successes.Add(1)
	} else {
		w.buckets[idx].failures.Add(1)
	}
}

// FailureRate sums every bucket's counters and returns failures/total + total.
// Returns (0, 0) on an empty window.
func (w *window) FailureRate() (rate float64, total uint64) {
	var s, f uint64
	for i := range w.buckets {
		s += w.buckets[i].successes.Load()
		f += w.buckets[i].failures.Load()
	}
	total = s + f
	if total == 0 {
		return 0, 0
	}
	return float64(f) / float64(total), total
}

// tick advances `current` by one bucket. Single-writer (FSM goroutine).
// Ordering: zero the entering bucket BEFORE storing the new current so readers
// redirected by current.Load() observe zeros, not stale counts.
func (w *window) tick() {
	next := (w.current.Load() + 1) % uint64(windowBuckets)
	w.buckets[next].successes.Store(0)
	w.buckets[next].failures.Store(0)
	w.current.Store(next)
}
