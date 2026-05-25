package safego_test

import (
	"sync"
	"testing"
	"time"

	"github.com/walera/walera/internal/safego"
)

func TestGo_NilPointerPanic(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	safego.Go("test-nil-panic", func() {
		defer wg.Done()

		var p *int
		_ = *p
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:

	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for panicking goroutine to finish")
	}
}

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

	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for panicking goroutine to finish")
	}
}
