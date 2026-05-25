// Package writer — arrivals.go selects the inter-arrival schedule for the
// commit loop: DistUniform uses rate.Limiter alone (deterministic 1/λ),
// DistPoisson samples Exp(1)/λ delays without consuming a limiter token.
// See INVARIANTS.md (mean inter-arrival contract, rate-source-of-truth).
package writer

import (
	"context"
	mathrand "math/rand/v2"
	"time"

	"golang.org/x/time/rate"
)

// ArrivalDistribution selects the inter-arrival shape.
type ArrivalDistribution string

const (
	// DistPoisson produces Exp(1/λ) inter-arrival times.
	DistPoisson ArrivalDistribution = "poisson"
	// DistUniform produces deterministic 1/λ spacing via rate.Limiter alone.
	DistUniform ArrivalDistribution = "uniform"
)

// waitArrival blocks until the next commit slot. DistUniform forwards to
// lim.WaitN. DistPoisson samples Exp(1)/λ from rng and sleeps. Returns
// ctx.Err() on cancellation. rng is injected for deterministic tests.
func waitArrival(ctx context.Context, lim *rate.Limiter, dist ArrivalDistribution, rng *mathrand.Rand) error {
	if dist == DistUniform {
		return lim.WaitN(ctx, 1)
	}
	lambda := float64(lim.Limit())
	if lambda <= 0 {
		return lim.WaitN(ctx, 1) // degenerate — avoid div-by-zero.
	}
	exp := rng.ExpFloat64()
	delay := time.Duration(exp / lambda * float64(time.Second))
	if delay <= 0 {
		return nil
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
