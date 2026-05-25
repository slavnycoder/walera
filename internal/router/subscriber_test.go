package router

import (
	"context"
	"encoding/hex"
	"testing"
	"time"
)

func newTestSubscriber() *Subscriber {
	return NewSubscriber(
		SubscriberConfig{
			Kind:      KindExact,
			Schema:    "public",
			Table:     "users",
			PK:        "42",
			BufferCap: 1,
		},
		SubscriberDeps{Parent: context.Background()},
	)
}

func TestSubscriber_DropIsIdempotent(t *testing.T) {
	t.Parallel()
	sub := newTestSubscriber()

	sub.Drop("slow_consumer")
	sub.Drop("slow_consumer")

	if got, want := sub.Reason(), "slow_consumer"; got != want {
		t.Errorf("Reason after two drops: got %q; want %q", got, want)
	}

	select {
	case <-sub.Done():

	case <-time.After(100 * time.Millisecond):
		t.Fatal("subscriber Done channel was not closed within 100ms of Drop")
	}
}

func TestSubscriber_DropDifferentReasonsKeepsFirst(t *testing.T) {
	t.Parallel()
	sub := newTestSubscriber()

	sub.Drop("slow_consumer")
	sub.Drop("tx_too_large")

	if got, want := sub.Reason(), "slow_consumer"; got != want {
		t.Errorf("Reason after racing drops: got %q; want %q", got, want)
	}
}

func TestSubscriber_ReasonEmptyBeforeDrop(t *testing.T) {
	t.Parallel()
	sub := newTestSubscriber()
	if got := sub.Reason(); got != "" {
		t.Errorf("Reason on fresh subscriber: got %q; want \"\"", got)
	}
}

func TestSubscriber_IDIsHex32(t *testing.T) {
	t.Parallel()
	sub := newTestSubscriber()
	id := sub.ID()
	if got, want := len(id), 32; got != want {
		t.Errorf("len(ID()): got %d; want %d (id=%q)", got, want, id)
	}
	if _, err := hex.DecodeString(id); err != nil {
		t.Errorf("ID() is not valid hex: %v (id=%q)", err, id)
	}
}

func TestSubscriber_IDFromOptsIsUsed(t *testing.T) {
	t.Parallel()
	sub := NewSubscriber(
		SubscriberConfig{
			ID:        "explicit-id-12345",
			Kind:      KindExact,
			Schema:    "public",
			Table:     "users",
			PK:        "1",
			BufferCap: 1,
		},
		SubscriberDeps{Parent: context.Background()},
	)
	if got, want := sub.ID(), "explicit-id-12345"; got != want {
		t.Errorf("ID(): got %q; want %q", got, want)
	}
}

func TestSubscriber_AccessorsReflectOpts(t *testing.T) {
	t.Parallel()
	sub := newTestSubscriber()
	if got, want := sub.Kind(), KindExact; got != want {
		t.Errorf("Kind(): got %q; want %q", got, want)
	}
	if got, want := sub.KindString(), "exact"; got != want {
		t.Errorf("KindString(): got %q; want %q", got, want)
	}
	if got, want := sub.Schema(), "public"; got != want {
		t.Errorf("Schema(): got %q; want %q", got, want)
	}
	if got, want := sub.Table(), "users"; got != want {
		t.Errorf("Table(): got %q; want %q", got, want)
	}
	if got, want := sub.PK(), "42"; got != want {
		t.Errorf("PK(): got %q; want %q", got, want)
	}
	if sub.Done() == nil {
		t.Error("Done() returned nil")
	}
}

func TestSubscriber_SendUnwiredReturnsFalse(t *testing.T) {
	t.Parallel()
	sub := newTestSubscriber()
	if sub.send([]byte("anything")) {
		t.Error("send() on unwired subscriber returned true; want false")
	}
}

func TestSubscriber_WireSendFuncCapturesFrames(t *testing.T) {
	t.Parallel()
	sub := newTestSubscriber()
	var captured [][]byte
	sub.WireSendFunc(func(frame []byte) bool {
		fc := make([]byte, len(frame))
		copy(fc, frame)
		captured = append(captured, fc)
		return true
	})
	if !sub.send([]byte("frame-1")) {
		t.Errorf("first send returned false; want true")
	}
	if !sub.send([]byte("frame-2")) {
		t.Errorf("second send returned false; want true")
	}
	if got, want := len(captured), 2; got != want {
		t.Fatalf("captured frame count: got %d; want %d", got, want)
	}
	if string(captured[0]) != "frame-1" || string(captured[1]) != "frame-2" {
		t.Errorf("captured frames: got %q,%q; want %q,%q", captured[0], captured[1], "frame-1", "frame-2")
	}
}

func TestSubscriber_WireSendFuncBP01(t *testing.T) {
	t.Parallel()
	sub := newTestSubscriber()
	var calls int
	sub.WireSendFunc(func(_ []byte) bool {
		calls++

		return calls <= 2
	})
	if !sub.send([]byte("a")) {
		t.Errorf("call 1: got false; want true")
	}
	if !sub.send([]byte("b")) {
		t.Errorf("call 2: got false; want true")
	}
	if sub.send([]byte("c")) {
		t.Errorf("call 3: got true; want false (BP-01 simulated)")
	}
}

func TestSubscriber_WireSendFuncOverwrite(t *testing.T) {
	t.Parallel()
	sub := newTestSubscriber()
	var firstCalled, secondCalled bool
	sub.WireSendFunc(func(_ []byte) bool { firstCalled = true; return true })
	sub.WireSendFunc(func(_ []byte) bool { secondCalled = true; return true })
	_ = sub.send([]byte("x"))
	if firstCalled {
		t.Error("first wired closure was invoked after rewiring; atomic.Value swap regression")
	}
	if !secondCalled {
		t.Error("second wired closure was NOT invoked after rewiring")
	}
}
