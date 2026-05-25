// Package wal — lag_sampler_test.go covers the walera_wal_lsn_lag_bytes
// sampler at the goroutine-lifecycle level. Wall-clock polling against a
// real PG connection is covered by the test/integration suite.
package wal

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

// TestSampleLagLoop_RespectsCtxCancel ensures SampleLagLoop returns promptly
// when its context is cancelled, even if the ticker has not yet fired. The
// admin connection is nil here on purpose — the test verifies the ctx.Done
// branch precedes any QueryRow call, so the nil deref never happens.
//
// This is the lightweight goroutine-leak gate for -race: a buggy loop that
// ranged over ticker.C unconditionally would block on the (never-firing)
// ticker channel and miss the cancel.
func TestSampleLagLoop_RespectsCtxCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel — the loop should observe Done on the very first select

	done := make(chan struct{})
	go func() {
		// Set sampleInterval high enough that ticker.C cannot fire before the
		// pre-cancelled ctx.Done branch wins the select. adminConn=nil is safe
		// because the ticker branch is never taken on this path.
		SampleLagLoop(ctx, nil, "irrelevant", metrics.New(), time.Hour, zerolog.Nop())
		close(done)
	}()

	select {
	case <-done:
		// Pass — the loop returned without ever calling QueryRow on the nil conn.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("SampleLagLoop did not return within 100ms of pre-cancelled ctx")
	}
}

// TestSampleLagLoop_GoroutineExitsAfterCancel verifies that a loop running
// with a live (un-cancelled) ctx exits within one sampleInterval of cancel.
// Again uses adminConn=nil and a Hour interval so the ticker branch never
// runs — we're asserting the select sees Done within the next iteration.
func TestSampleLagLoop_GoroutineExitsAfterCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		SampleLagLoop(ctx, nil, "test_slot", metrics.New(), time.Hour, zerolog.Nop())
		close(done)
	}()

	// Give the goroutine a moment to enter the select.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("SampleLagLoop did not exit within 200ms of cancel")
	}
}
