package wal

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

func TestSampleLagLoop_RespectsCtxCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {

		SampleLagLoop(ctx, nil, "irrelevant", metrics.New(), time.Hour, zerolog.Nop())
		close(done)
	}()

	select {
	case <-done:

	case <-time.After(100 * time.Millisecond):
		t.Fatal("SampleLagLoop did not return within 100ms of pre-cancelled ctx")
	}
}

func TestSampleLagLoop_GoroutineExitsAfterCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		SampleLagLoop(ctx, nil, "test_slot", metrics.New(), time.Hour, zerolog.Nop())
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("SampleLagLoop did not exit within 200ms of cancel")
	}
}
