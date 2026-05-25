package auth

import (
	"sync"
	"testing"
)

func TestWindow_EmptyReturnsZero(t *testing.T) {
	t.Parallel()
	w := newWindow()
	rate, total := w.FailureRate()
	if rate != 0 || total != 0 {
		t.Fatalf("empty window: got (rate=%v, total=%d); want (0, 0)", rate, total)
	}
}

func TestWindow_SingleSuccessBucket(t *testing.T) {
	t.Parallel()
	w := newWindow()
	for i := 0; i < 5; i++ {
		w.Record(true)
	}
	rate, total := w.FailureRate()
	if total != 5 {
		t.Errorf("total: got %d; want 5", total)
	}
	if rate != 0.0 {
		t.Errorf("rate: got %v; want 0.0", rate)
	}
}

func TestWindow_SingleFailureBucket(t *testing.T) {
	t.Parallel()
	w := newWindow()
	for i := 0; i < 5; i++ {
		w.Record(false)
	}
	rate, total := w.FailureRate()
	if total != 5 {
		t.Errorf("total: got %d; want 5", total)
	}
	if rate != 1.0 {
		t.Errorf("rate: got %v; want 1.0", rate)
	}
}

func TestWindow_MixedBucket(t *testing.T) {
	t.Parallel()
	w := newWindow()
	for i := 0; i < 3; i++ {
		w.Record(true)
	}
	for i := 0; i < 7; i++ {
		w.Record(false)
	}
	rate, total := w.FailureRate()
	if total != 10 {
		t.Errorf("total: got %d; want 10", total)
	}
	const want = 0.7
	if rate < want-0.001 || rate > want+0.001 {
		t.Errorf("rate: got %v; want %v (±0.001)", rate, want)
	}
}

func TestWindow_AcrossBuckets(t *testing.T) {
	t.Parallel()
	w := newWindow()
	for i := 0; i < 5; i++ {
		w.Record(true)
	}
	w.tick()
	for i := 0; i < 10; i++ {
		w.Record(false)
	}
	rate, total := w.FailureRate()
	if total != 15 {
		t.Errorf("total: got %d; want 15", total)
	}

	if rate < 0.66 || rate > 0.67 {
		t.Errorf("rate: got %v; want in [0.66, 0.67]", rate)
	}
}

func TestWindow_RotationZeroesEnteringBucket(t *testing.T) {
	t.Parallel()
	w := newWindow()
	for i := 0; i < 5; i++ {
		w.Record(true)
	}

	for i := 0; i < 30; i++ {
		w.tick()
	}
	rate, total := w.FailureRate()
	if total != 0 {
		t.Errorf("total after 30 ticks: got %d; want 0", total)
	}
	if rate != 0 {
		t.Errorf("rate after 30 ticks: got %v; want 0", rate)
	}
}

func TestWindow_PartialRotationKeepsRecentBuckets(t *testing.T) {
	t.Parallel()
	w := newWindow()
	for i := 0; i < 10; i++ {
		w.Record(false)
	}
	for i := 0; i < 5; i++ {
		w.tick()
	}
	for i := 0; i < 5; i++ {
		w.Record(true)
	}
	rate, total := w.FailureRate()
	if total != 15 {
		t.Errorf("total: got %d; want 15", total)
	}

	if rate < 0.66 || rate > 0.67 {
		t.Errorf("rate: got %v; want in [0.66, 0.67]", rate)
	}
}

func TestWindow_ConcurrentRecordSafeUnderRace(t *testing.T) {
	t.Parallel()
	w := newWindow()
	const goroutines = 100
	const perGoroutine = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				w.Record(true)
			}
		}()
	}
	wg.Wait()
	rate, total := w.FailureRate()
	if total != goroutines*perGoroutine {
		t.Errorf("total: got %d; want %d", total, goroutines*perGoroutine)
	}
	if rate != 0 {
		t.Errorf("rate: got %v; want 0", rate)
	}
}
