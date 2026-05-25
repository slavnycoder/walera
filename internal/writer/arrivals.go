package writer

import (
	"context"
	mathrand "math/rand/v2"
	"time"

	"golang.org/x/time/rate"
)

type ArrivalDistribution string

const (
	DistPoisson ArrivalDistribution = "poisson"

	DistUniform ArrivalDistribution = "uniform"
)

func waitArrival(ctx context.Context, lim *rate.Limiter, dist ArrivalDistribution, rng *mathrand.Rand) error {
	if dist == DistUniform {
		return lim.WaitN(ctx, 1)
	}
	lambda := float64(lim.Limit())
	if lambda <= 0 {
		return lim.WaitN(ctx, 1)
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
