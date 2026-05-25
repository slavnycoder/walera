package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestApp_RunWiresRunnables(t *testing.T) {
	t.Parallel()

	names := []string{"alpha", "beta", "gamma"}
	started := make([]chan struct{}, len(names))
	for i := range started {
		started[i] = make(chan struct{})
	}

	a := &App{
		Logger: zerolog.Nop(),
	}

	for i, name := range names {
		i, name := i, name
		a.Runnables = append(a.Runnables, Runnable{
			Name: name,
			Run: func(ctx context.Context) error {
				close(started[i])
				<-ctx.Done()
				return nil
			},
		})
	}

	errDelivered := make(chan struct{})
	a.Runnables = append(a.Runnables, Runnable{
		Name: "delta",
		Run: func(ctx context.Context) error {
			close(errDelivered)
			return errors.New("delta intentional error")
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- a.Run(ctx, cancel)
	}()

	for i, ch := range started {
		select {
		case <-ch:

		case <-time.After(200 * time.Millisecond):
			t.Fatalf("Runnable %q did not start within 200ms", names[i])
		}
	}

	select {
	case <-errDelivered:

	case <-time.After(200 * time.Millisecond):
		t.Fatal("delta Runnable's Run body did not execute within 200ms")
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned non-nil error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Run did not return within 200ms of ctx cancel")
	}

	_ = sync.WaitGroup{}
}

func TestApp_RunSuppressesOnErrorOnCleanShutdown(t *testing.T) {
	t.Parallel()

	a := &App{
		Logger: zerolog.Nop(),
	}

	started := make(chan struct{})
	var onErrorCalls atomic.Int32
	a.Runnables = []Runnable{
		{
			Name: "cancel-returning",
			Run: func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			},
			OnError: func(err error) {
				onErrorCalls.Add(1)
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- a.Run(ctx, cancel)
	}()

	select {
	case <-started:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Runnable did not start within 200ms")
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned non-nil error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Run did not return within 200ms of ctx cancel")
	}

	time.Sleep(50 * time.Millisecond)

	if n := onErrorCalls.Load(); n != 0 {
		t.Fatalf("OnError fired %d times on clean shutdown; want 0 (the spawn wrapper's ctx.Err() == nil guard regressed)", n)
	}
}

func TestApp_RunInvokesOnErrorOnRealError(t *testing.T) {
	t.Parallel()

	a := &App{
		Logger: zerolog.Nop(),
	}

	onErrorCalled := make(chan error, 1)
	a.Runnables = []Runnable{
		{
			Name: "bad-runnable",
			Run: func(ctx context.Context) error {
				return errors.New("simulated unrecoverable error")
			},
			OnError: func(err error) {
				onErrorCalled <- err
			},
		},

		{
			Name: "ctx-keeper",
			Run: func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = a.Run(ctx, cancel) }()

	select {
	case err := <-onErrorCalled:
		if err == nil {
			t.Fatal("OnError received nil err")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("OnError did not fire within 200ms on a real (non-cancellation) error")
	}
}
