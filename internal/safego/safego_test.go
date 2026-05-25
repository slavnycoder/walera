package safego_test

import (
	"sync"
	"testing"
	"time"

	"github.com/walera/walera/internal/safego"
)

// TestGo_NilPointerPanic verifies that a goroutine spawned via safego.Go that
// dereferences a nil pointer does NOT kill the test process (D-31, success criterion #5).
func TestGo_NilPointerPanic(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	safego.Go("test-nil-panic", func() {
		defer wg.Done()
		// Intentionally dereference a nil pointer to trigger a panic.
		var p *int
		_ = *p
	})

	// If safego.Go did not recover the panic, the test would crash here.
	// Use a generous timeout to avoid false positives under load.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Parent goroutine survived — test passes.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for panicking goroutine to finish")
	}
}

// TestGo_StringPanic verifies that a goroutine spawned via safego.Go that panics
// with a string value does NOT kill the test process (D-31).
func TestGo_StringPanic(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	safego.Go("test-string-panic", func() {
		defer wg.Done()
		panic("intentional string panic for test")
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Parent goroutine survived — test passes.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for panicking goroutine to finish")
	}
}
