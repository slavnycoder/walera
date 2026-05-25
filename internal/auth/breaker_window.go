package auth

import "sync/atomic"

const windowBuckets = 30

type bucket struct {
	successes atomic.Uint64
	failures  atomic.Uint64
}

type window struct {
	buckets [windowBuckets]bucket
	current atomic.Uint64
}

func newWindow() *window {
	return &window{}
}

func (w *window) Record(success bool) {
	idx := w.current.Load() % uint64(windowBuckets)
	if success {
		w.buckets[idx].successes.Add(1)
	} else {
		w.buckets[idx].failures.Add(1)
	}
}

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

func (w *window) tick() {
	next := (w.current.Load() + 1) % uint64(windowBuckets)
	w.buckets[next].successes.Store(0)
	w.buckets[next].failures.Store(0)
	w.current.Store(next)
}
