package wal

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

func gatherCounterValue(t *testing.T, m *metrics.Registry, name string) float64 {
	t.Helper()
	mfs, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if c := metric.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	return 0
}

func TestReader_Run_BackoffAndReconnects(t *testing.T) {
	t.Parallel()

	transient := errors.New("simulated transient PG error")

	r, _ := New(Config{
		PostgresDSN:     "irrelevant",
		ReplicationDSN:  "irrelevant",
		PublicationName: "irrelevant",
		SlotNamePrefix:  "walera",
		Reconnect: ReconnectConfig{
			ResetAfterSuccessDuration: time.Hour,
		},
	}, Deps{Logger: zerolog.Nop(), Metrics: metrics.New()})

	r.rng = rand.New(rand.NewPCG(42, 42))

	r.computeBackoffFn = func(attempt int) time.Duration {
		return 5 * time.Millisecond
	}

	var calls atomic.Int32
	var lastObservedConnected atomic.Bool
	successCh := make(chan struct{}, 1)

	r.runOnceFn = func(ctx context.Context) error {
		n := calls.Add(1)

		r.connected.Store(true)
		defer r.connected.Store(false)
		lastObservedConnected.Store(true)

		if n <= 3 {

			return transient
		}

		successCh <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	runDone := make(chan error, 1)
	go func() {
		runDone <- r.Run(ctx)
	}()

	select {
	case <-successCh:
	case <-time.After(6 * time.Second):
		t.Fatalf("did not reach successful runOnce within 6s; calls=%d", calls.Load())
	}

	if !r.IsConnected() {
		t.Errorf("IsConnected() during successful attempt: false; want true")
	}

	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v; want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of ctx cancellation")
	}

	if got := calls.Load(); got < 4 {
		t.Errorf("runOnceFn calls: %d; want >= 4", got)
	}

	if got := gatherCounterValue(t, r.metrics, "walera_pg_reconnects_total"); got != 3 {
		t.Errorf("walera_pg_reconnects_total: %v; want 3 (one per failed attempt)", got)
	}

	if r.IsConnected() {
		t.Errorf("IsConnected() after Run exit: true; want false (deferred Store(false) in runOnce)")
	}
}

func TestReader_Run_CtxCancelDuringBackoff(t *testing.T) {
	t.Parallel()

	r, _ := New(Config{
		SlotNamePrefix: "walera",
		Reconnect: ReconnectConfig{
			ResetAfterSuccessDuration: time.Hour,
		},
	}, Deps{Logger: zerolog.Nop(), Metrics: metrics.New()})

	r.runOnceFn = func(ctx context.Context) error {
		return errors.New("force transient")
	}

	r.computeBackoffFn = func(attempt int) time.Duration { return 30 * time.Second }

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- r.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run: %v; want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of cancel during backoff")
	}
}

func TestReader_ComputeBackoff_CurveAndJitterEnvelope(t *testing.T) {
	t.Parallel()

	r, _ := New(Config{SlotNamePrefix: "walera"}, Deps{Logger: zerolog.Nop(), Metrics: metrics.New()})

	r.rng = rand.New(rand.NewPCG(7, 11))

	curveBases := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
	}

	for attempt := 0; attempt < 10; attempt++ {
		got := r.computeBackoff(attempt)
		idx := attempt
		if idx >= len(curveBases) {
			idx = len(curveBases) - 1
		}
		base := curveBases[idx]
		lower := time.Duration(float64(base) * 0.75)
		upper := time.Duration(float64(base) * 1.25)
		if got < lower || got > upper {
			t.Errorf("computeBackoff(%d) = %v; want in [%v, %v] (base=%v)", attempt, got, lower, upper, base)
		}
	}
}

func TestReader_Run_ResetAfterSuccess(t *testing.T) {
	t.Parallel()

	r, _ := New(Config{
		SlotNamePrefix: "walera",
		Reconnect: ReconnectConfig{
			ResetAfterSuccessDuration: 50 * time.Millisecond,
		},
	}, Deps{Logger: zerolog.Nop(), Metrics: metrics.New()})

	var calls atomic.Int32
	r.computeBackoffFn = func(attempt int) time.Duration { return time.Millisecond }
	r.runOnceFn = func(ctx context.Context) error {
		n := calls.Add(1)

		switch n {
		case 1:
			time.Sleep(100 * time.Millisecond)
			return errors.New("transient after long success")
		case 2:
			return errors.New("immediate transient")
		default:
			<-ctx.Done()
			return ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- r.Run(ctx)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if calls.Load() < 3 {
		t.Fatalf("only %d calls within 5s; want >= 3", calls.Load())
	}

	cancel()
	<-runDone

	if got := gatherCounterValue(t, r.metrics, "walera_pg_reconnects_total"); got < 2 {
		t.Errorf("walera_pg_reconnects_total: %v; want >= 2 (one per failed runOnce, reset does not skip inc)", got)
	}
}

var _ = (*dto.MetricFamily)(nil)
