package writer

import (
	"context"
	"math"
	mathrand "math/rand/v2"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestWaitArrival_Uniform_DeterministicSpacing(t *testing.T) {
	const lambda = 10.0
	const n = 100
	lim := rate.NewLimiter(rate.Limit(lambda), 1)
	rng := mathrand.New(mathrand.NewPCG(42, 42))
	ctx := context.Background()

	if err := waitArrival(ctx, lim, DistUniform, rng); err != nil {
		t.Fatalf("waitArrival initial: %v", err)
	}

	intervals := make([]float64, 0, n)
	last := time.Now()
	for i := 0; i < n; i++ {
		if err := waitArrival(ctx, lim, DistUniform, rng); err != nil {
			t.Fatalf("waitArrival: %v", err)
		}
		now := time.Now()
		intervals = append(intervals, now.Sub(last).Seconds())
		last = now
	}

	mean := meanFloat(intervals)
	stddev := stddevFloat(intervals, mean)

	if math.Abs(mean-0.1) > 0.05 {
		t.Errorf("uniform mean = %.4f s, want ~0.1 s ±0.05", mean)
	}
	if stddev > 0.005 {
		t.Errorf("uniform stddev = %.4f s, want < 0.005 s (deterministic spacing)", stddev)
	}
}

func TestWaitArrival_Poisson_ExponentialShape(t *testing.T) {
	const lambda = 10.0
	const n = 1000
	lim := rate.NewLimiter(rate.Limit(lambda), 1)
	rng := mathrand.New(mathrand.NewPCG(42, 42))
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := waitArrival(ctx, lim, DistPoisson, rng); err != nil {
			t.Fatalf("warmup: %v", err)
		}
	}

	intervals := make([]float64, 0, n)
	last := time.Now()
	for i := 0; i < n; i++ {
		if err := waitArrival(ctx, lim, DistPoisson, rng); err != nil {
			t.Fatalf("waitArrival: %v", err)
		}
		now := time.Now()
		intervals = append(intervals, now.Sub(last).Seconds())
		last = now
	}
	mean := meanFloat(intervals)
	stddev := stddevFloat(intervals, mean)

	if math.Abs(mean-0.1) > 0.015 {
		t.Errorf("poisson mean = %.4f s, want ~0.1 s ±0.015", mean)
	}

	cov := stddev / mean
	if cov < 0.5 || cov > 1.3 {
		t.Errorf("poisson CoV (stddev/mean) = %.3f, want in [0.5, 1.3]", cov)
	}
}

func TestWaitArrival_Cancel(t *testing.T) {
	lim := rate.NewLimiter(rate.Limit(0.01), 1)
	rng := mathrand.New(mathrand.NewPCG(1, 1))
	ctx, cancel := context.WithCancel(context.Background())

	if err := waitArrival(ctx, lim, DistUniform, rng); err != nil {
		t.Fatalf("first waitArrival: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- waitArrival(ctx, lim, DistUniform, rng)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected cancellation error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("waitArrival did not return after ctx cancel")
	}
}

func meanFloat(xs []float64) float64 {
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

func stddevFloat(xs []float64, mean float64) float64 {
	var s float64
	for _, x := range xs {
		d := x - mean
		s += d * d
	}
	return math.Sqrt(s / float64(len(xs)))
}
