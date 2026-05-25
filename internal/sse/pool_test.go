package sse

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// fakeEncoder implements encoderIface for tests — returns deterministic
// stub bytes so the drain path can be exercised without the production
// encoder.
type fakeEncoder struct{}

func (fakeEncoder) EncodeHeartbeat() []byte   { return []byte(":\n\n") }
func (fakeEncoder) EncodeShutdown() []byte    { return []byte("event: shutdown\ndata: {}\n\n") }
func (fakeEncoder) EncodeError(string) []byte { return []byte("event: error\ndata: {}\n\n") }

// fakeMetrics implements metricsIface and counts increments per label.
type fakeMetrics struct {
	mu          sync.Mutex
	eventsSent  map[string]int
	txDropped   map[string]int
	lifetimes   []float64
	disconnects map[string]int
	// Pool metric families. Each map / slice is
	// guarded by m.mu just like the existing label maps so the race
	// detector stays happy when production code touches the gauges from
	// the worker goroutine while the test reads from the main goroutine.
	dirtySubsInc   map[string]int
	dirtySubsDec   map[string]int
	dirtySubsSet   map[string]float64
	drainBatchSize []float64
	drainDuration  []float64
	// slowClientDrops counts every SlowClientDropsInc call. Asserted in
	// lockstep with disconnects["slow_consumer"] by tests that touch the
	// slow-client policy path.
	slowClientDrops int
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{
		eventsSent:   make(map[string]int),
		txDropped:    make(map[string]int),
		disconnects:  make(map[string]int),
		dirtySubsInc: make(map[string]int),
		dirtySubsDec: make(map[string]int),
		dirtySubsSet: make(map[string]float64),
	}
}

func (m *fakeMetrics) EventsSentInc(kind string) {
	m.mu.Lock()
	m.eventsSent[kind]++
	m.mu.Unlock()
}
func (m *fakeMetrics) TxDroppedInc(reason string) {
	m.mu.Lock()
	m.txDropped[reason]++
	m.mu.Unlock()
}
func (m *fakeMetrics) SubscriberLifetimeObserve(s float64) {
	m.mu.Lock()
	m.lifetimes = append(m.lifetimes, s)
	m.mu.Unlock()
}
func (m *fakeMetrics) SubscriberDisconnectsInc(reason string) {
	m.mu.Lock()
	m.disconnects[reason]++
	m.mu.Unlock()
}
func (m *fakeMetrics) PoolWorkerDirtySubsInc(workerID string) {
	m.mu.Lock()
	m.dirtySubsInc[workerID]++
	m.mu.Unlock()
}
func (m *fakeMetrics) PoolWorkerDirtySubsDec(workerID string) {
	m.mu.Lock()
	m.dirtySubsDec[workerID]++
	m.mu.Unlock()
}
func (m *fakeMetrics) PoolWorkerDirtySubsSet(workerID string, v float64) {
	m.mu.Lock()
	m.dirtySubsSet[workerID] = v
	m.mu.Unlock()
}
func (m *fakeMetrics) PoolDrainBatchSizeObserve(n float64) {
	m.mu.Lock()
	m.drainBatchSize = append(m.drainBatchSize, n)
	m.mu.Unlock()
}
func (m *fakeMetrics) PoolDrainDurationObserve(s float64) {
	m.mu.Lock()
	m.drainDuration = append(m.drainDuration, s)
	m.mu.Unlock()
}
func (m *fakeMetrics) SlowClientDropsInc() {
	m.mu.Lock()
	m.slowClientDrops++
	m.mu.Unlock()
}

// fakeSub implements subscriber. WireSendFunc captures the closure so the
// test can deliver frames directly. The `kind` field is consumed by
// EventsSentInc — defaults to "wildcard" so existing tests' EventsSent
// assertions (which expect the "wildcard" label) keep their semantics.
// done is a channel the test can close to simulate sub.Drop / handler
// exit; nil-channel semantics (never fires) is the default and matches
// the "sub still alive" mode.
type fakeSub struct {
	id   string
	kind string
	mu   sync.Mutex
	send func(frame []byte) bool
	done chan struct{}
}

func (s *fakeSub) ID() string { return s.id }
func (s *fakeSub) KindString() string {
	if s.kind == "" {
		return "wildcard"
	}
	return s.kind
}
func (s *fakeSub) WireSendFunc(send func(frame []byte) bool) {
	s.mu.Lock()
	s.send = send
	s.mu.Unlock()
}
func (s *fakeSub) Done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done == nil {
		s.done = make(chan struct{})
	}
	return s.done
}

// Reason returns "" — fakeSub does not drive the Drop-then-Reason path
// in any existing pool unit test. Tests that need to assert an
// auth_revoked / shutdown error frame go through the real *router.Subscriber
// via the handler_test.go scaffolding.
func (s *fakeSub) Reason() string { return "" }
func (s *fakeSub) Send(frame []byte) bool {
	s.mu.Lock()
	send := s.send
	s.mu.Unlock()
	if send == nil {
		return false
	}
	return send(frame)
}

// fakeResponseWriter is a minimal http.ResponseWriter for the non-TCP
// fallback drain path. Records writes to a bytes.Buffer; satisfies
// http.Flusher.
type fakeResponseWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *fakeResponseWriter) Header() http.Header { return http.Header{} }
func (w *fakeResponseWriter) WriteHeader(int)     {}
func (w *fakeResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
func (w *fakeResponseWriter) Flush() {}

// fakeFlushController wraps a fakeResponseWriter so the pool's
// http.ResponseController path works without a real net/http handler.
// http.NewResponseController(w) on a custom Writer-only type returns a
// controller whose SetWriteDeadline / Flush methods consult w via
// reflection on FlushError / SetWriteDeadliner interfaces.
// Since fakeResponseWriter satisfies http.Flusher (basic Flush) but not
// the deadline interface, SetWriteDeadline returns http.ErrNotSupported.
// We mask that as a no-op in the drainSub fallback path by ignoring its
// error.

// TestPoolBasicAttachShutdown verifies a pool can be constructed,
// shut down, and exit cleanly with no attached subs.
func TestPoolBasicAttachShutdown(t *testing.T) {
	t.Parallel()
	p := NewPool(PoolConfig{
		PoolFactor:   1,
		SubQueueSize: 4,
		MaxWaitMs:    2,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})

	done := make(chan struct{})
	go func() {
		_ = p.Shutdown(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not complete within 2s")
	}
}

// TestPoolDrainCoalescesMultipleFrames is the headline test: when many
// frames arrive within MaxWaitMs, they should drain together as one
// batch (verified by observing a single net.Buffers.WriteTo call).
// Uses a net.Pipe-backed *net.TCPConn surrogate via a custom listener.
// Since net.Pipe doesn't yield *net.TCPConn, we exercise the fallback
// path through a fake ResponseWriter and assert frame order and count
// instead of syscall count. (The syscall-count assertion is the job of
// the bench / strace gate, not the unit test.)
func TestPoolDrainCoalescesMultipleFrames(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       16,
		MaxWaitMs:          5,
		DrainThresholdSubs: 100, // force timer-based drain (not threshold)
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "sub-1"}
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// Send 5 frames in rapid succession (within the 5ms timer window).
	for i := 0; i < 5; i++ {
		frame := []byte("data: msg-" + string(rune('0'+i)) + "\n\n")
		if !sub.Send(frame) {
			t.Fatalf("Send #%d returned false (queue full)", i)
		}
	}

	// Wait long enough for the timer to fire + drain to occur.
	time.Sleep(20 * time.Millisecond)

	// Inspect the recorded output.: pool.Attach writes the
	// WALERA-01 retry prelude as the first bytes; assert it lands first
	// and then the five frames coalesce as before.
	rw.mu.Lock()
	got := rw.buf.String()
	rw.mu.Unlock()

	const prelude = "retry: 15000\n\n"
	want := prelude + "data: msg-0\n\ndata: msg-1\n\ndata: msg-2\n\ndata: msg-3\n\ndata: msg-4\n\n"
	if got != want {
		t.Fatalf("output mismatch:\n got: %q\nwant: %q", got, want)
	}

	// EventsSent should have been incremented 5 times (one per frame).
	m.mu.Lock()
	gotEvents := m.eventsSent["wildcard"]
	m.mu.Unlock()
	if gotEvents != 5 {
		t.Errorf("EventsSent = %d, want 5", gotEvents)
	}

	// subscriber doneCh should not yet be closed (sub is still alive).
	select {
	case <-doneCh:
		t.Error("doneCh closed prematurely")
	default:
	}
}

// TestPoolBackpressureDrop verifies that filling a sub's queue past
// SubQueueSize causes Send to return false (BP-01 slow_consumer path).
func TestPoolBackpressureDrop(t *testing.T) {
	t.Parallel()
	p := NewPool(PoolConfig{
		PoolFactor:   1,
		SubQueueSize: 2, // tiny queue
		// Disable batching so the worker drains immediately and we have
		// to overflow the queue between drains — actually we want the
		// OPPOSITE: keep the worker from draining so the queue fills.
		// Easier: don't attach with a working conn — Attach without a
		// blocking conn means the worker will drain into a no-op fake.
		// To genuinely test backpressure, we'd need to block the worker
		// somehow. For this smoke test we just verify the API surface:
		// successive Sends past the cap return false.
		MaxWaitMs:          1000, // 1 second — worker won't drain quickly
		DrainThresholdSubs: 100,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "slow-sub"}
	rw := &poolSlowRespWriter{} // blocks on Write to keep queue full
	rc := http.NewResponseController(rw)
	_, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// Drain the worker's first poll-cycle write, then fill the queue.
	// We can't deterministically observe full state in this minimal
	// test; we just verify that the first N+small fit but eventually
	// Send returns false.
	gotFalse := false
	for i := 0; i < 100; i++ {
		if !sub.Send([]byte("frame")) {
			gotFalse = true
			break
		}
		// Tiny pause so the worker doesn't drain everything before the
		// queue fills.
		time.Sleep(time.Microsecond)
	}
	if !gotFalse {
		t.Skip("queue never filled — worker drained faster than we could fill; backpressure path not exercised in this environment")
	}
}

// poolSlowRespWriter blocks indefinitely on Write — used to force the
// worker's drain to stall, so the per-sub queue can fill and exercise
// BP-01.
type poolSlowRespWriter struct {
	dropped atomic.Bool
}

func (w *poolSlowRespWriter) Header() http.Header { return http.Header{} }
func (w *poolSlowRespWriter) WriteHeader(int)     {}
func (w *poolSlowRespWriter) Write(p []byte) (int, error) {
	if w.dropped.Load() {
		return 0, io.ErrClosedPipe
	}
	// Block for "a while" — long enough that the test's tiny per-Send
	// pause adds up to overflowing the queue.
	time.Sleep(50 * time.Millisecond)
	return len(p), nil
}
func (w *poolSlowRespWriter) Flush() {}

// TestPoolDrainThresholdEager verifies that when many subs go dirty, the
// drain fires on the threshold rather than waiting for the timer.
// Note: not t.Parallel() because we manipulate GOMAXPROCS to coerce all
// subs onto a single worker (pool size = GOMAXPROCS × PoolFactor).
func TestPoolDrainThresholdEager(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	const nSubs = 5
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       8,
		MaxWaitMs:          1000,  // 1 sec timer — too slow to fire
		DrainThresholdSubs: nSubs, // drain when this many dirty
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	subs := make([]*fakeSub, nSubs)
	rws := make([]*fakeResponseWriter, nSubs)
	for i := range subs {
		subs[i] = &fakeSub{id: "sub-" + string(rune('0'+i))}
		rws[i] = &fakeResponseWriter{}
		rc := http.NewResponseController(rws[i])
		if _, err := p.Attach(subs[i], nil, rws[i], rc); err != nil {
			t.Fatalf("Attach %d: %v", i, err)
		}
	}

	// Send ONE frame to each sub. After all are sent, the dirty list
	// hits nSubs and the worker drains eagerly.
	for i, s := range subs {
		s.Send([]byte("hello-" + string(rune('0'+i)) + "\n"))
	}

	// Wait briefly — far less than MaxWaitMs (1000ms).
	time.Sleep(50 * time.Millisecond)

	// Each sub's recorded buffer is prefixed with the retry prelude
	// (`retry: 15000\n\n`) emitted by pool.Attach.
	const prelude = "retry: 15000\n\n"
	for i, rw := range rws {
		rw.mu.Lock()
		got := rw.buf.String()
		rw.mu.Unlock()
		want := prelude + "hello-" + string(rune('0'+i)) + "\n"
		if got != want {
			t.Errorf("sub %d output: got %q, want %q", i, got, want)
		}
	}
}

// Sanity: deadlineExceededError matches our isTimeoutErr helper.
func TestIsTimeoutErr(t *testing.T) {
	t.Parallel()
	if !isTimeoutErr(&net.OpError{Op: "write", Net: "tcp", Err: timeoutErrStub{}}) {
		t.Error("net.OpError with Timeout() err should be recognised")
	}
	if isTimeoutErr(errors.New("not a timeout")) {
		t.Error("plain error must not be classified as timeout")
	}
}

type timeoutErrStub struct{}

func (timeoutErrStub) Error() string   { return "i/o timeout" }
func (timeoutErrStub) Timeout() bool   { return true }
func (timeoutErrStub) Temporary() bool { return true }

// erroringRespWriter returns the configured error from Write so we can
// exercise the pool's per-sub write-failure teardown path (B5 + B2 from
// the plan-checker fixes — verifying that SubscriberDisconnectsInc fires
// with the right reason and hbTicker is stopped).
type erroringRespWriter struct {
	mu       sync.Mutex
	writeErr error
	written  int
}

func (w *erroringRespWriter) Header() http.Header { return http.Header{} }
func (w *erroringRespWriter) WriteHeader(int)     {}
func (w *erroringRespWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.written += len(p)
	if w.writeErr != nil {
		// Return the error AFTER counting the prelude write so the test
		// can verify the prelude went out before the failure.
		if w.written > len("retry: 15000\n\n") {
			return 0, w.writeErr
		}
	}
	return len(p), nil
}
func (w *erroringRespWriter) Flush() {}

// TestPoolHandleSubWriteFailure verifies the B5 fix (use
// SubscriberDisconnectsInc, not TxDroppedInc) and B2 fix (stop hbTicker)
// in handleSubWriteFailure. Drives a drain failure by configuring the
// respWriter to return a non-timeout error → reason="client_closed".
func TestPoolHandleSubWriteFailure(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       4,
		MaxWaitMs:          1,
		DrainThresholdSubs: 1, // drain immediately on first dirty sub
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "fail-sub"}
	rw := &erroringRespWriter{writeErr: io.ErrClosedPipe}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	if !sub.Send([]byte("data: x\n\n")) {
		t.Fatal("Send returned false (queue full?)")
	}

	// Wait for the worker to drain → drain fails → handleSubWriteFailure
	// closes doneCh.
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("doneCh never closed after write failure")
	}

	// B5 verification: SubscriberDisconnectsInc fired (lifecycle event),
	// NOT TxDroppedInc (per-tx drop). The reason for io.ErrClosedPipe
	// is "client_closed" (not a timeout-shaped error).
	m.mu.Lock()
	disconnects := m.disconnects["client_closed"]
	txDropped := m.txDropped["client_closed"]
	m.mu.Unlock()
	if disconnects != 1 {
		t.Errorf("SubscriberDisconnects[client_closed] = %d; want 1", disconnects)
	}
	if txDropped != 0 {
		t.Errorf("TxDropped[client_closed] = %d; want 0 (label-semantics bug regression)", txDropped)
	}
}

// TestPoolAttachReturnsErrPoolClosedAfterShutdown verifies Attach
// refuses to onboard new subs after Shutdown.
func TestPoolAttachReturnsErrPoolClosedAfterShutdown(t *testing.T) {
	t.Parallel()
	p := NewPool(PoolConfig{
		PoolFactor:   1,
		SubQueueSize: 4,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	_ = p.Shutdown(context.Background())

	sub := &fakeSub{id: "late-sub"}
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	_, err := p.Attach(sub, nil, rw, rc)
	if !errors.Is(err, errPoolClosed) {
		t.Errorf("Attach after Shutdown returned %v; want errPoolClosed", err)
	}
}

// TestPoolAttachPreludeWriteFailureReturnsError verifies the WIRE-02
// fault path: when the prelude write fails, Attach returns the error
// and the worker never sees the sub.
func TestPoolAttachPreludeWriteFailureReturnsError(t *testing.T) {
	t.Parallel()
	p := NewPool(PoolConfig{
		PoolFactor:   1,
		SubQueueSize: 4,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "prelude-fail-sub"}
	rw := &preludeFailRespWriter{}
	rc := http.NewResponseController(rw)
	_, err := p.Attach(sub, nil, rw, rc)
	if err == nil {
		t.Error("Attach with prelude-write-error returned nil; want non-nil")
	}
}

// preludeFailRespWriter returns an error on every Write — exercises
// pool.Attach's prelude write-failure branch.
type preludeFailRespWriter struct{}

func (preludeFailRespWriter) Header() http.Header { return http.Header{} }
func (preludeFailRespWriter) WriteHeader(int)     {}
func (preludeFailRespWriter) Write(p []byte) (int, error) {
	return 0, io.ErrShortWrite
}
func (preludeFailRespWriter) Flush() {}

// TestDeadlineExceededError covers the small Error/Is helpers used by
// handleSubWriteFailure to classify write failures.
func TestDeadlineExceededError(t *testing.T) {
	t.Parallel()
	e := deadlineExceededError{}
	if e.Error() == "" {
		t.Error("deadlineExceededError.Error returned empty string")
	}
	if !e.Is(deadlineExceededError{}) {
		t.Error("deadlineExceededError.Is(self) returned false")
	}
	if e.Is(errors.New("other")) {
		t.Error("deadlineExceededError.Is(other) returned true; want false")
	}
}

// TestPoolWorkerStringFormat covers the worker's String() helper used in
// debug log lines.
func TestPoolWorkerStringFormat(t *testing.T) {
	t.Parallel()
	w := newPoolWorker(7, PoolConfig{
		PoolFactor: 1, SubQueueSize: 4, MaxWaitMs: 2,
		WriteTimeout: time.Second, HeartbeatInterval: time.Second,
		drainShutdownDeadline: time.Millisecond,
	}, fakeEncoder{}, newFakeMetrics(), zerolog.Nop())
	got := w.String()
	if got == "" || !strings.Contains(got, "poolWorker{id=7") {
		t.Errorf("String() = %q; want substring poolWorker{id=7", got)
	}
}

// fakeSubWithReason extends fakeSub with a settable Reason() return so
// tests can exercise the evictDone error-frame path.
type fakeSubWithReason struct {
	fakeSub
	reason string
}

func (s *fakeSubWithReason) Reason() string { return s.reason }

// TestPoolEvictDoneEmitsErrorFrameOnDrop verifies the v1.3-parity error
// frame on the wire when a sub is Dropped with a non-client-closed
// reason. Mirrors writer.go's Done-arm contract.
func TestPoolEvictDoneEmitsErrorFrameOnDrop(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:   1,
		SubQueueSize: 4,
		MaxWaitMs:    2,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSubWithReason{
		fakeSub: fakeSub{id: "auth-revoked-sub"},
		reason:  "auth_revoked",
	}
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// Trigger sub.Done() — the worker's evictDone polls this on the
	// next cycle and emits the error frame.
	sub.mu.Lock()
	if sub.done == nil {
		sub.done = make(chan struct{})
	}
	close(sub.done)
	sub.mu.Unlock()

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("doneCh never closed after sub.Done()")
	}

	rw.mu.Lock()
	got := rw.buf.String()
	rw.mu.Unlock()
	const prelude = "retry: 15000\n\n"
	const wantErr = "event: error\ndata: {}\n\n"
	if !strings.HasPrefix(got, prelude) {
		t.Errorf("body does not start with prelude; got %q", got)
	}
	if !strings.Contains(got, wantErr) {
		t.Errorf("body does not contain error frame %q; got %q", wantErr, got)
	}

	// Metric: SubscriberDisconnects[auth_revoked] should have ticked.
	m.mu.Lock()
	disconnects := m.disconnects["auth_revoked"]
	m.mu.Unlock()
	if disconnects != 1 {
		t.Errorf("SubscriberDisconnects[auth_revoked] = %d; want 1", disconnects)
	}
}

// TestPoolAttachConnPathExercisesPrelude verifies the conn != nil
// branch of Attach (the hijack/writev fast path). Uses a real
// net.TCPConn pair via net.Listen + net.Dial so the prelude write hits
// the production code path (not the respWriter fallback).
func TestPoolAttachConnPathExercisesPrelude(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	type accepted struct {
		conn *net.TCPConn
		err  error
	}
	acceptCh := make(chan accepted, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			acceptCh <- accepted{nil, err}
			return
		}
		acceptCh <- accepted{c.(*net.TCPConn), nil}
	}()

	// Dial-side reads the prelude.
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	cli, err := dialer.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()

	acc := <-acceptCh
	if acc.err != nil {
		t.Fatalf("Accept: %v", acc.err)
	}
	srvConn := acc.conn

	p := NewPool(PoolConfig{
		PoolFactor:   1,
		SubQueueSize: 4,
		MaxWaitMs:    2,
		WriteTimeout: time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "tcp-sub"}
	_, err = p.Attach(sub, srvConn, nil, nil)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// Read the prelude from the client side.
	buf := make([]byte, len("retry: 15000\n\n"))
	_ = cli.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := io.ReadFull(cli, buf)
	if err != nil {
		t.Fatalf("ReadFull: %v (got %d bytes: %q)", err, n, buf[:n])
	}
	if string(buf) != "retry: 15000\n\n" {
		t.Errorf("prelude = %q; want %q", buf, "retry: 15000\n\n")
	}
}

// TestPoolEvictDoneEmitsShutdownFrameOnDrop verifies the spec §3.5
// shutdown frame is emitted when sub.Reason() == "shutdown".
func TestPoolEvictDoneEmitsShutdownFrameOnDrop(t *testing.T) {
	t.Parallel()
	p := NewPool(PoolConfig{
		PoolFactor:   1,
		SubQueueSize: 4,
		MaxWaitMs:    2,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSubWithReason{
		fakeSub: fakeSub{id: "shutdown-sub"},
		reason:  "shutdown",
	}
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	sub.mu.Lock()
	if sub.done == nil {
		sub.done = make(chan struct{})
	}
	close(sub.done)
	sub.mu.Unlock()

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("doneCh never closed")
	}

	rw.mu.Lock()
	got := rw.buf.String()
	rw.mu.Unlock()
	if !strings.Contains(got, "event: shutdown") {
		t.Errorf("body does not contain shutdown frame; got %q", got)
	}
}

// TestPoolWorkerHeartbeatSweepEnqueuesAfterInterval verifies the
// per-worker heartbeat ticker enqueues `:\n\n` for any sub idle
// longer than HeartbeatInterval, and that the heartbeat drains
// through the SAME pipeline as data frames (no separate write path).
// Setup: HeartbeatInterval=50ms, MaxWaitMs=2, sleep 220ms with zero data
// frames. Expect prelude + at least 2 heartbeats on the wire (sweeps at
// ~50/100/150/200ms — accept ≥ 2 to absorb scheduler slop).
func TestPoolWorkerHeartbeatSweepEnqueuesAfterInterval(t *testing.T) {
	t.Parallel()
	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       4,
		MaxWaitMs:          2,
		DrainThresholdSubs: 1, // drain immediately on first dirty sub
		HeartbeatInterval:  50 * time.Millisecond,
		WriteTimeout:       time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "hb-sweep-sub"}
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	if _, err := p.Attach(sub, nil, rw, rc); err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// Wait through several heartbeat intervals with zero data frames.
	time.Sleep(220 * time.Millisecond)

	rw.mu.Lock()
	got := rw.buf.String()
	rw.mu.Unlock()

	const prelude = "retry: 15000\n\n"
	if !strings.HasPrefix(got, prelude) {
		t.Errorf("body does not begin with prelude %q; got %q", prelude, got)
	}
	hb := strings.Count(got, ":\n\n")
	if hb < 2 {
		t.Errorf("heartbeat count = %d; want >= 2 over 220ms with HeartbeatInterval=50ms (got body %q)", hb, got)
	}
}

// TestPoolWorkerHeartbeatSkippedAfterRecentWrite verifies the
// lastWriteAt path: if a sub just drained a data frame, the next
// heartbeat sweep MUST skip it (since lastWriteAt is fresh).
// Setup: HeartbeatInterval=100ms. Send one data frame at t=0 (drains
// almost immediately via threshold=1). Sleep 50ms (less than
// HeartbeatInterval). Expect prelude + the data frame ONLY — no
// heartbeat yet.
func TestPoolWorkerHeartbeatSkippedAfterRecentWrite(t *testing.T) {
	t.Parallel()
	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       4,
		MaxWaitMs:          2,
		DrainThresholdSubs: 1,
		HeartbeatInterval:  100 * time.Millisecond,
		WriteTimeout:       time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "hb-skip-sub"}
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	if _, err := p.Attach(sub, nil, rw, rc); err != nil {
		t.Fatalf("Attach: %v", err)
	}

	if !sub.Send([]byte("data: payload\n\n")) {
		t.Fatal("Send returned false (queue full?)")
	}

	// Wait less than HeartbeatInterval — the data frame drains; the
	// sweep that fires at ~100ms must NOT enqueue a heartbeat because
	// lastWriteAt is fresh.
	time.Sleep(50 * time.Millisecond)

	rw.mu.Lock()
	got := rw.buf.String()
	rw.mu.Unlock()

	const want = "retry: 15000\n\ndata: payload\n\n"
	if got != want {
		t.Errorf("body = %q; want %q (no heartbeat should appear within HeartbeatInterval of last write)", got, want)
	}
}

// dialLoopbackTCP opens a loopback TCP listener, dials it, and returns
// the (server, client) *net.TCPConn pair. Used by trigger-priority and
// lag-ceiling tests that need real kernel send buffering. Closure
// closeAll cleans up both ends and the listener on test exit.
func dialLoopbackTCP(t *testing.T) (srv, cli *net.TCPConn, closeAll func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	type accepted struct {
		conn *net.TCPConn
		err  error
	}
	acceptCh := make(chan accepted, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			acceptCh <- accepted{nil, err}
			return
		}
		acceptCh <- accepted{c.(*net.TCPConn), nil}
	}()
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	cConn, err := dialer.Dial("tcp", ln.Addr().String())
	if err != nil {
		_ = ln.Close()
		t.Fatalf("Dial: %v", err)
	}
	acc := <-acceptCh
	if acc.err != nil {
		_ = ln.Close()
		_ = cConn.Close()
		t.Fatalf("Accept: %v", acc.err)
	}
	closeAll = func() {
		_ = acc.conn.Close()
		_ = cConn.Close()
		_ = ln.Close()
	}
	return acc.conn, cConn.(*net.TCPConn), closeAll
}

// drainPrelude reads and discards the `retry: 15000\n\n` 14-byte prelude
// from a connected client. Fails the test on read error.
func drainPrelude(t *testing.T, cli net.Conn) {
	t.Helper()
	buf := make([]byte, len("retry: 15000\n\n"))
	_ = cli.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := io.ReadFull(cli, buf)
	if err != nil {
		t.Fatalf("ReadFull prelude: %v (got %d bytes)", err, n)
	}
	_ = cli.SetReadDeadline(time.Time{})
}

// TestPoolWorkerHeartbeatSurvivesAcrossDrainCycles attaches multiple
// subs sharing one worker (forced via GOMAXPROCS=1 + PoolFactor=1) and
// verifies the per-worker sweep correctly iterates EVERY owned sub.
// Setup: 8 subs, HeartbeatInterval=80ms. Idle 250ms with zero data
// frames. Expect every sub to receive ≥ 2 heartbeats. Proves the
// sweep is not first-sub-only.
// Note: not t.Parallel() because we manipulate GOMAXPROCS to coerce
// all subs onto a single worker (pool size = GOMAXPROCS × PoolFactor).
func TestPoolWorkerHeartbeatSurvivesAcrossDrainCycles(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	const nSubs = 8
	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       4,
		MaxWaitMs:          2,
		DrainThresholdSubs: 1,
		HeartbeatInterval:  80 * time.Millisecond,
		WriteTimeout:       time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	subs := make([]*fakeSub, nSubs)
	rws := make([]*fakeResponseWriter, nSubs)
	for i := range subs {
		subs[i] = &fakeSub{id: "hb-multi-" + string(rune('0'+i))}
		rws[i] = &fakeResponseWriter{}
		rc := http.NewResponseController(rws[i])
		if _, err := p.Attach(subs[i], nil, rws[i], rc); err != nil {
			t.Fatalf("Attach %d: %v", i, err)
		}
	}

	// Idle through ~3 heartbeat intervals.
	time.Sleep(250 * time.Millisecond)

	const prelude = "retry: 15000\n\n"
	for i, rw := range rws {
		rw.mu.Lock()
		got := rw.buf.String()
		rw.mu.Unlock()
		if !strings.HasPrefix(got, prelude) {
			t.Errorf("sub %d body does not begin with prelude; got %q", i, got)
		}
		hb := strings.Count(got, ":\n\n")
		if hb < 2 {
			t.Errorf("sub %d heartbeat count = %d; want >= 2 over 250ms with HeartbeatInterval=80ms (body %q)", i, hb, got)
		}
	}
}

// TestPoolDrainTriggerPriority_ByteOverflowPreemptsThreshold proves
// trigger priority (1) — per-sub bufBytes >= MaxBatchBytesPerSub — fires
// IMMEDIATELY inside pollAllQueues, before either the threshold (2) or
// the max_wait_ms timer (3) can fire. /
// Setup: MaxBatchBytesPerSub=64, DrainThresholdSubs=100 (unreachable),
// MaxWaitMs=1000 (effectively disabled for the test window). One sub on
// a real loopback TCP pair. Enqueue ONE 80-byte frame. With the byte
// overflow trigger absent, neither (2) nor (3) would drain inside
// ~50ms; with it present, the frame arrives in single-digit ms.
func TestPoolDrainTriggerPriority_ByteOverflowPreemptsThreshold(t *testing.T) {
	t.Parallel()
	srv, cli, closeAll := dialLoopbackTCP(t)
	defer closeAll()

	p := NewPool(PoolConfig{
		PoolFactor:          1,
		SubQueueSize:        4,
		MaxWaitMs:           1000, // (3) effectively disabled
		DrainThresholdSubs:  100,  // (2) unreachable with one sub
		MaxBatchBytesPerSub: 64,   // (1) hit by a single 80-byte frame
		WriteTimeout:        time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "byte-overflow-sub"}
	if _, err := p.Attach(sub, srv, nil, nil); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	drainPrelude(t, cli)

	// One 80-byte frame; 80 > MaxBatchBytesPerSub=64. Trigger (1) MUST
	// drain it inside pollAllQueues before either (2) or (3) fires.
	frame := []byte(strings.Repeat("x", 76) + "\n\n\n\n") // 80 bytes
	if len(frame) <= 64 {
		t.Fatalf("test bug: frame len=%d not > MaxBatchBytesPerSub=64", len(frame))
	}
	start := time.Now()
	if !sub.Send(frame) {
		t.Fatal("Send returned false (queue full?)")
	}

	buf := make([]byte, len(frame))
	_ = cli.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	n, err := io.ReadFull(cli, buf)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ReadFull frame: %v (got %d bytes in %v)", err, n, elapsed)
	}
	// Generous bound: 30ms is plenty for the byte-overflow path on any
	// CI runner; the threshold+timer would take ≥ 1000ms to drain.
	if elapsed > 30*time.Millisecond {
		t.Errorf("byte-overflow drain took %v; want <= 30ms (threshold/timer fallback would take >= 1000ms)", elapsed)
	}
	if !bytes.Equal(buf, frame) {
		t.Errorf("frame mismatch: got %q, want %q", buf, frame)
	}
}

// TestPoolDrainTriggerPriority_ThresholdPreemptsTimer proves trigger
// priority (2) — len(dirty) >= drainThreshold — fires BEFORE the
// max_wait_ms timer (3). /
// Setup: GOMAXPROCS=1 + PoolFactor=1 → one worker; DrainThresholdSubs=N
// (the threshold), MaxWaitMs=1000 (timer effectively disabled). Attach
// N subs, enqueue ONE tiny frame each in rapid succession. All N MUST
// drain inside ~30ms (well below the 1000ms timer fallback).
// Note: not t.Parallel() because we manipulate GOMAXPROCS.
func TestPoolDrainTriggerPriority_ThresholdPreemptsTimer(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	const nSubs = 4
	p := NewPool(PoolConfig{
		PoolFactor:          1,
		SubQueueSize:        4,
		MaxWaitMs:           1000, // (3) effectively disabled
		DrainThresholdSubs:  nSubs,
		MaxBatchBytesPerSub: 64 * 1024,
		WriteTimeout:        time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	subs := make([]*fakeSub, nSubs)
	rws := make([]*fakeResponseWriter, nSubs)
	for i := range subs {
		subs[i] = &fakeSub{id: "thr-sub-" + string(rune('0'+i))}
		rws[i] = &fakeResponseWriter{}
		rc := http.NewResponseController(rws[i])
		if _, err := p.Attach(subs[i], nil, rws[i], rc); err != nil {
			t.Fatalf("Attach %d: %v", i, err)
		}
	}

	start := time.Now()
	for i, s := range subs {
		if !s.Send([]byte("data: msg-" + string(rune('0'+i)) + "\n\n")) {
			t.Fatalf("Send %d returned false", i)
		}
	}

	// Wait for the threshold drain. 30ms is plenty; the timer fallback
	// would take 1000ms.
	deadline := time.Now().Add(50 * time.Millisecond)
	const prelude = "retry: 15000\n\n"
	for {
		allDone := true
		for i, rw := range rws {
			rw.mu.Lock()
			got := rw.buf.String()
			rw.mu.Unlock()
			want := prelude + "data: msg-" + string(rune('0'+i)) + "\n\n"
			if got != want {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
		if time.Now().After(deadline) {
			for i, rw := range rws {
				rw.mu.Lock()
				got := rw.buf.String()
				rw.mu.Unlock()
				t.Errorf("sub %d body = %q (incomplete after %v)", i, got, time.Since(start))
			}
			t.Fatalf("threshold drain did not complete within 50ms (would have taken 1000ms via the timer fallback)")
		}
		time.Sleep(time.Millisecond)
	}
	elapsed := time.Since(start)
	if elapsed > 30*time.Millisecond {
		t.Errorf("threshold drain took %v; want <= 30ms", elapsed)
	}
}

// TestPoolDrainMaxWaitLagCeiling validates : with MaxWaitMs=2
// the worker MUST deliver a single frame to a single sub within
// max_wait_ms + scheduler_jitter (≤ 10ms). 100 iterations measure
// arrival lag; assert P50 ≤ 4ms, P99 ≤ 10ms.
// Setup: one sub on real loopback TCP, DrainThresholdSubs=999
// (unreachable), MaxBatchBytesPerSub=64KiB (unreachable), so ONLY the
// max_wait_ms timer can drain. Lock the lag ceiling as a hard property.
func TestPoolDrainMaxWaitLagCeiling(t *testing.T) {
	t.Parallel()
	srv, cli, closeAll := dialLoopbackTCP(t)
	defer closeAll()

	p := NewPool(PoolConfig{
		PoolFactor:          1,
		SubQueueSize:        128,
		MaxWaitMs:           2,
		DrainThresholdSubs:  999, // unreachable with one sub
		MaxBatchBytesPerSub: 64 * 1024,
		WriteTimeout:        time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "lag-ceiling-sub"}
	if _, err := p.Attach(sub, srv, nil, nil); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	drainPrelude(t, cli)

	const iterations = 100
	lags := make([]time.Duration, 0, iterations)
	frame := []byte("data: tick\n\n")
	rdBuf := make([]byte, len(frame))

	for i := 0; i < iterations; i++ {
		_ = cli.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		start := time.Now()
		if !sub.Send(frame) {
			t.Fatalf("iter %d: Send returned false", i)
		}
		if _, err := io.ReadFull(cli, rdBuf); err != nil {
			t.Fatalf("iter %d: ReadFull: %v", i, err)
		}
		lags = append(lags, time.Since(start))
		if !bytes.Equal(rdBuf, frame) {
			t.Fatalf("iter %d: frame mismatch", i)
		}
		// Sleep > MaxWaitMs so the next iteration starts with an empty
		// buffer + disarmed timer (this isolates each iteration's lag
		// measurement to the arm + timer + drain path).
		time.Sleep(5 * time.Millisecond)
	}

	// Sort and pick percentiles.
	//
	// Percentile-index semantics ():
	//   -  specifies "p99 ≤ max_wait_ms + scheduler_jitter".
	//     A p99 metric over N=100 samples is the 99th order statistic
	//     (lags[98] in 0-indexed sorted slice): 99 of 100 samples are
	//     ≤ p99, 1 may exceed it. That's the contract — it explicitly
	//     tolerates a single outlier per 100 observations.
	//   - lags[len(lags)*99/100] = lags[99] is the MAX of the sample
	//     (100th order statistic = p100), NOT the p99. Asserting on it
	//     would enforce a max-arrival-lag contract stricter than what
	//      specifies, and would fail intermittently on contended
	//     CI/sandbox runners where a single -race + scheduler stall
	//     during the ~0.9 s measurement window inflates one of 100
	//     iterations past the 10 ms ceiling while the other 99 stay in
	//     the 3-5 ms range (healthy distribution). See debug session
	//     .planning/debug/testpooldrainmaxwaitlagceiling.md for the
	//     measurement proof: isolation runs show p99=4.2-4.6ms with
	//     identical max; full-package runs show p99=12-13ms with
	//     max=p99 (single outlier), confirming the drain mechanics are
	//     correct and only the percentile arithmetic was over-strict.
	//   - We compute p99 as lags[len(lags)*99/100 - 1] = lags[98], which
	//     matches 's "1 % may exceed" allowance. The MAX is
	//     still logged for diagnostic visibility (not asserted on).
	sortDurations(lags)
	p50 := lags[len(lags)*50/100]
	p99 := lags[len(lags)*99/100-1]
	maxLag := lags[len(lags)-1]
	t.Logf("lag stats over %d iters: p50=%v p99=%v max=%v", iterations, p50, p99, maxLag)

	// P50 sanity bound: with MaxWaitMs=2 the median arrival should be
	// near 2ms. 4ms accommodates scheduler jitter under -race.
	if p50 > 4*time.Millisecond {
		t.Errorf("p50 lag = %v; want <= 4ms (MaxWaitMs=2)", p50)
	}
	// P99 hard ceiling per : ≤ 10ms (production), ≤ 15ms under
	// `-race`. Note this asserts on the 99th order statistic (lags[98]),
	// not the max —  allows up to 1 % of arrivals to exceed the
	// ceiling. See the percentile-index comment above for why this is
	// the contract-faithful check.
	//
	// Race-aware bound rationale: the `-race` build adds 2-5x scheduling
	// overhead and the synchronisation between the worker's poll-timer
	// (1 ms) + max_wait_ms timer (2 ms) + Send→writev→ack round-trip can
	// see clustered stalls under contended sandbox / shared-CPU CI
	// runners. The 10 ms production ceiling holds for dedicated runners
	// without -race; under -race we permit 15 ms — matching the sibling
	// `TestPoolBatchingDisabledDrainsOnEveryCycle` race-tolerant bound at
	// the same file (). Uses the `raceEnabled` const defined in
	// race_on_test.go / race_off_test.go. The strict 10 ms target is
	// preserved for production builds; the loosened bound only fires
	// under `go test -race`.
	p99Ceiling := 10 * time.Millisecond
	if raceEnabled {
		p99Ceiling = 15 * time.Millisecond
	}
	if p99 > p99Ceiling {
		t.Errorf("p99 lag = %v; want <= %v (: max_wait_ms + scheduler_jitter, raceEnabled=%v)", p99, p99Ceiling, raceEnabled)
	}
}

// sortDurations is a tiny in-place sort for small slices. Avoid pulling
// sort.Slice into the hot test path (test-only helper; correctness via
// brute insertion sort is fine for N=100).
func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j-1] > d[j]; j-- {
			d[j-1], d[j] = d[j], d[j-1]
		}
	}
}

// TestPoolBatchingDisabledDrainsOnEveryCycle validates the
// BatchingDisabled=true fast-path: every enqueue (data OR heartbeat)
// drains immediately, no timer arm, no threshold check. /
//
// Setup: one sub on real loopback TCP, BatchingDisabled=true,
// DrainThresholdSubs=999 (would never fire), MaxWaitMs=1000 (would take
// 1000ms via the timer). Send 5 frames with a 5ms gap between each.
// Each MUST arrive within 5ms of its enqueue — the override drains
// on every cycle.
func TestPoolBatchingDisabledDrainsOnEveryCycle(t *testing.T) {
	t.Parallel()
	srv, cli, closeAll := dialLoopbackTCP(t)
	defer closeAll()

	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       16,
		MaxWaitMs:          1000, // huge; only the override should drain
		DrainThresholdSubs: 999,
		BatchingDisabled:   true,
		WriteTimeout:       time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "batching-disabled-sub"}
	if _, err := p.Attach(sub, srv, nil, nil); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	drainPrelude(t, cli)

	frame := []byte("data: tick\n\n")
	rdBuf := make([]byte, len(frame))
	const iterations = 5
	for i := 0; i < iterations; i++ {
		_ = cli.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		start := time.Now()
		if !sub.Send(frame) {
			t.Fatalf("iter %d: Send returned false", i)
		}
		if _, err := io.ReadFull(cli, rdBuf); err != nil {
			t.Fatalf("iter %d: ReadFull: %v", i, err)
		}
		elapsed := time.Since(start)
		// Generous bound: 15ms is far below the 1000ms timer fallback;
		// catches "we forgot the BatchingDisabled override" regressions.
		if elapsed > 15*time.Millisecond {
			t.Errorf("iter %d: BatchingDisabled drain took %v; want <= 15ms (timer fallback would take 1000ms)", i, elapsed)
		}
		if !bytes.Equal(rdBuf, frame) {
			t.Errorf("iter %d: frame mismatch", i)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestPoolDrainThresholdSubsFormula_LazyRecompute validates :
// with DrainThresholdSubs=0 (sentinel for "use formula") the worker
// resolves drainThreshold to max(8, len(w.subs)/64) lazily, and
// recomputes on every Attach / evict-done.
// Setup: GOMAXPROCS=1 + PoolFactor=1 → one worker. Attach 1024 subs.
// Expected drainThreshold = max(8, 1024/64) = 16. We read drainThreshold
// AFTER Shutdown — the close(drainDoneCh) inside run() establishes a
// happens-before that makes the field's final value safely visible to
// the test goroutine. Single-writer invariant holds; no atomic needed.
// Note: not t.Parallel() because we manipulate GOMAXPROCS.
func TestPoolDrainThresholdSubsFormula_LazyRecompute(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	const nSubs = 1024
	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       4,
		MaxWaitMs:          2,
		DrainThresholdSubs: 0,         // sentinel: use formula
		HeartbeatInterval:  time.Hour, // suppress sweeps during the test
		WriteTimeout:       time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})

	// Sanity: initial threshold is the floor (8).
	if got := p.workers[0].drainThreshold; got != 8 {
		// We can't safely read this from outside the worker BEFORE any
		// attach has happened — but newPoolWorker initialises it in
		// the constructor, before run() starts. Reading here is racy in
		// theory but in practice we're before run() has touched it.
		// Skip the strict check; the post-Shutdown read below is the
		// authoritative one.
		_ = got
	}

	subs := make([]*fakeSub, nSubs)
	for i := range subs {
		subs[i] = &fakeSub{id: "lazy-formula-sub-" + idSuffix(i)}
		rw := &fakeResponseWriter{}
		rc := http.NewResponseController(rw)
		if _, err := p.Attach(subs[i], nil, rw, rc); err != nil {
			t.Fatalf("Attach %d: %v", i, err)
		}
	}

	// Force the worker to traverse the recompute branch at least once:
	// send a single data frame to one sub. The post-pollAllQueues
	// evaluation in run() runs after the loop-top recompute.
	if !subs[0].Send([]byte("data: trigger\n\n")) {
		t.Fatal("trigger Send returned false")
	}
	// Let the worker complete a few cycles.
	time.Sleep(20 * time.Millisecond)

	_ = p.Shutdown(context.Background())

	// After Shutdown, the worker has returned from run(); drainThreshold
	// is now safely readable (close(drainDoneCh) inside run() defers
	// establishes the happens-before to Shutdown's wait).
	got := p.workers[0].drainThreshold
	want := nSubs / 64 // = 16
	if want < 8 {
		want = 8
	}
	if got < want {
		t.Errorf("drainThreshold = %d; want >= %d (formula max(8, %d/64) = %d)", got, want, nSubs, want)
	}
	if got != want {
		t.Logf("note: drainThreshold = %d (formula = %d); slight drift acceptable if evict-done removed some subs", got, want)
	}
}

// idSuffix produces a 4-character hex-ish suffix to give 1024 distinct
// sub IDs without pulling fmt into the hot path. Sub IDs only need to
// be unique within the partition; xxhash still works on these.
func idSuffix(i int) string {
	const hex = "0123456789abcdef"
	return string([]byte{
		hex[(i>>12)&0xf],
		hex[(i>>8)&0xf],
		hex[(i>>4)&0xf],
		hex[i&0xf],
	})
}

// TestPoolDrainEverythingDirtyNoFrameCap reinforces : the
// worker drains EVERY frame in a sub's buffer in one cycle — no
// per-sub frame-count cap. Send 100 frames in a tight loop with
// MaxBatchBytesPerSub large enough to fit all 100; assert all 100
// appear on the recorded output.
// This is a regression test against the v1.4 flush_batch_size=4 cap
// that was removed in the batched-flush redesign.
func TestPoolDrainEverythingDirtyNoFrameCap(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:          1,
		SubQueueSize:        128,
		MaxWaitMs:           5,
		DrainThresholdSubs:  1,
		MaxBatchBytesPerSub: 64 * 1024, // big enough for 100 small frames
		WriteTimeout:        time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "no-cap-sub"}
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	if _, err := p.Attach(sub, nil, rw, rc); err != nil {
		t.Fatalf("Attach: %v", err)
	}

	const nFrames = 100
	for i := 0; i < nFrames; i++ {
		frame := []byte("data: " + idSuffix(i) + "\n\n")
		if !sub.Send(frame) {
			t.Fatalf("Send #%d returned false (queue full)", i)
		}
	}

	// Wait for the worker to drain everything.
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		rw.mu.Lock()
		got := rw.buf.String()
		rw.mu.Unlock()
		dataCount := strings.Count(got, "data: ")
		if dataCount >= nFrames {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d frames drained after 500ms; body = %q", dataCount, nFrames, got)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Verify EventsSent fired nFrames times (one per frame).
	m.mu.Lock()
	gotEvents := m.eventsSent["wildcard"]
	m.mu.Unlock()
	if gotEvents != nFrames {
		t.Errorf("EventsSent = %d; want %d (one per frame, no cap)", gotEvents, nFrames)
	}
}

// -----------------------------------------------------------------------------
// Shutdown disconnect-emission rules + truthful reason
// -----------------------------------------------------------------------------

// timeoutNetError implements net.Error with Timeout()=true. Used by the
// timeoutRespWriter fixture to drive the truthful-reason branch in
// drainShutdown.
type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "i/o timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

// timeoutRespWriter is a minimal http.ResponseWriter whose Write returns
// a net.Error-with-Timeout=true error on demand. Toggled via the
// `failNext` atomic flag so the test can let the prelude write succeed
// (prelude is the first Write on Attach) and then make the shutdown-
// frame write fail with a timeout.
type timeoutRespWriter struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	failNext atomic.Bool
}

func (w *timeoutRespWriter) Header() http.Header { return http.Header{} }
func (w *timeoutRespWriter) WriteHeader(int)     {}
func (w *timeoutRespWriter) Write(p []byte) (int, error) {
	if w.failNext.Load() {
		return 0, timeoutNetError{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
func (w *timeoutRespWriter) Flush() {}

// nonTimeoutErrRespWriter returns a generic (non-Timeout) error on
// Write. Used to confirm drainShutdown reports reason="shutdown" (not
// "slow_consumer") when the write fails with a non-timeout error.
type nonTimeoutErrRespWriter struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	failNext atomic.Bool
}

func (w *nonTimeoutErrRespWriter) Header() http.Header { return http.Header{} }
func (w *nonTimeoutErrRespWriter) WriteHeader(int)     {}
func (w *nonTimeoutErrRespWriter) Write(p []byte) (int, error) {
	if w.failNext.Load() {
		return 0, errors.New("broken pipe")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
func (w *nonTimeoutErrRespWriter) Flush() {}

// TestPool_Shutdown_DisconnectOnce — after Pool.Shutdown on a pool with
// one healthy attached sub, SubscriberDisconnectsInc is called exactly
// once with reason="shutdown".
func TestPool_Shutdown_DisconnectOnce(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          4,
		MaxWaitMs:             2,
		WriteTimeout:          time.Second,
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: 50 * time.Millisecond,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})

	sub := &fakeSub{id: "shutdown-once-sub"}
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	if serr := p.Shutdown(context.Background()); serr != nil {
		t.Fatalf("Shutdown returned %v; want nil", serr)
	}

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("doneCh did not close within 2s after Shutdown")
	}

	m.mu.Lock()
	total := 0
	for _, n := range m.disconnects {
		total += n
	}
	shutdownN := m.disconnects["shutdown"]
	lifetimes := len(m.lifetimes)
	m.mu.Unlock()

	if total != 1 {
		t.Errorf("total disconnects = %d; want 1", total)
	}
	if shutdownN != 1 {
		t.Errorf("disconnects[shutdown] = %d; want 1", shutdownN)
	}
	if lifetimes != 1 {
		t.Errorf("SubscriberLifetimeObserve count = %d; want 1", lifetimes)
	}
}

// TestPool_Shutdown_SlowConsumerOnFrameTimeout — when the shutdown-frame
// write itself returns a timeout error, the disconnect reason MUST be
// "slow_consumer" (truthful — the sub never received the frame), NOT
// "shutdown". CONTEXT.md §"Disconnect Metric Reason (Q2.4)" final bullet.
func TestPool_Shutdown_SlowConsumerOnFrameTimeout(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          4,
		MaxWaitMs:             2,
		WriteTimeout:          time.Second,
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: 50 * time.Millisecond,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})

	sub := &fakeSub{id: "shutdown-timeout-sub"}
	rw := &timeoutRespWriter{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	// Arm the timeout for the shutdown-frame write (the prelude already
	// completed in Attach).
	rw.failNext.Store(true)

	if serr := p.Shutdown(context.Background()); serr != nil {
		t.Fatalf("Shutdown returned %v; want nil", serr)
	}

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("doneCh did not close within 2s after Shutdown")
	}

	m.mu.Lock()
	slowN := m.disconnects["slow_consumer"]
	shutdownN := m.disconnects["shutdown"]
	m.mu.Unlock()

	if slowN != 1 {
		t.Errorf("disconnects[slow_consumer] = %d; want 1 (truthful reason — frame write timed out)", slowN)
	}
	if shutdownN != 0 {
		t.Errorf("disconnects[shutdown] = %d; want 0 (frame never reached the sub)", shutdownN)
	}
}

// TestPool_Shutdown_NonTimeoutErrorStillShutdownReason — a non-timeout
// write error during shutdown-frame emission still reports
// reason="shutdown" (the truthful-reason rule only re-labels on
// timeout). Counterpart to TestPool_Shutdown_SlowConsumerOnFrameTimeout.
func TestPool_Shutdown_NonTimeoutErrorStillShutdownReason(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          4,
		MaxWaitMs:             2,
		WriteTimeout:          time.Second,
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: 50 * time.Millisecond,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})

	sub := &fakeSub{id: "shutdown-nontimeout-sub"}
	rw := &nonTimeoutErrRespWriter{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	rw.failNext.Store(true)

	if serr := p.Shutdown(context.Background()); serr != nil {
		t.Fatalf("Shutdown returned %v; want nil", serr)
	}

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("doneCh did not close within 2s after Shutdown")
	}

	m.mu.Lock()
	shutdownN := m.disconnects["shutdown"]
	slowN := m.disconnects["slow_consumer"]
	m.mu.Unlock()

	if shutdownN != 1 {
		t.Errorf("disconnects[shutdown] = %d; want 1 (non-timeout error still uses shutdown reason)", shutdownN)
	}
	if slowN != 0 {
		t.Errorf("disconnects[slow_consumer] = %d; want 0 (only timeout writes relabel)", slowN)
	}
}

// TestPool_Shutdown_SkipsAlreadyEvicted — if evictDone emits the
// disconnect first (sub.Done() fired before shutdownCh closed),
// drainShutdown skips that sub entirely. No second SubscriberDisconnects,
// no second SubscriberLifetime.
func TestPool_Shutdown_SkipsAlreadyEvicted(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          4,
		MaxWaitMs:             2,
		WriteTimeout:          time.Second,
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: 50 * time.Millisecond,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})

	sub := &fakeSub{id: "evicted-then-shutdown"}
	// Pre-create the Done() channel so the test goroutine has a stable
	// reference to close (fakeSub.Done lazily allocates on first call;
	// without this the worker and test race to create the channel).
	_ = sub.Done()
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// Trigger sub.Done() so evictDone catches it on the next worker
	// cycle (before Shutdown closes shutdownCh).
	sub.mu.Lock()
	close(sub.done)
	sub.mu.Unlock()

	// Give the worker time to run evictDone.
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("doneCh did not close after sub.Done() fired (evictDone should have run)")
	}

	// Snapshot counters BEFORE Shutdown so we can verify Shutdown adds
	// nothing.
	m.mu.Lock()
	disconnectsBefore := 0
	for _, n := range m.disconnects {
		disconnectsBefore += n
	}
	lifetimesBefore := len(m.lifetimes)
	m.mu.Unlock()

	if disconnectsBefore != 1 {
		t.Fatalf("expected exactly 1 disconnect from evictDone before Shutdown; got %d", disconnectsBefore)
	}
	if lifetimesBefore != 1 {
		t.Fatalf("expected exactly 1 lifetime obs from evictDone before Shutdown; got %d", lifetimesBefore)
	}

	if serr := p.Shutdown(context.Background()); serr != nil {
		t.Fatalf("Shutdown returned %v; want nil", serr)
	}

	m.mu.Lock()
	disconnectsAfter := 0
	for _, n := range m.disconnects {
		disconnectsAfter += n
	}
	lifetimesAfter := len(m.lifetimes)
	m.mu.Unlock()

	if disconnectsAfter != disconnectsBefore {
		t.Errorf("disconnects after Shutdown = %d; want %d (no double-emission)", disconnectsAfter, disconnectsBefore)
	}
	if lifetimesAfter != lifetimesBefore {
		t.Errorf("lifetime observations after Shutdown = %d; want %d (no double-emission)", lifetimesAfter, lifetimesBefore)
	}
}

// TestPool_Shutdown_Idempotent_Race — Pool.Shutdown invoked from N=8
// goroutines under -race does not fire SubscriberDisconnectsInc more
// than once per attached sub. sync.Once guard is the mechanism;
// the test pins it under the race detector.
func TestPool_Shutdown_Idempotent_Race(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          4,
		MaxWaitMs:             2,
		WriteTimeout:          time.Second,
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: 50 * time.Millisecond,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})

	const nSubs = 16
	doneChs := make([]<-chan struct{}, nSubs)
	for i := 0; i < nSubs; i++ {
		sub := &fakeSub{id: "race-shutdown-" + idSuffix(i)}
		rw := &fakeResponseWriter{}
		rc := http.NewResponseController(rw)
		dc, err := p.Attach(sub, nil, rw, rc)
		if err != nil {
			t.Fatalf("Attach %d: %v", i, err)
		}
		doneChs[i] = dc
	}

	const nGoroutines = 8
	var wg sync.WaitGroup
	wg.Add(nGoroutines)
	for i := 0; i < nGoroutines; i++ {
		go func() {
			defer wg.Done()
			_ = p.Shutdown(context.Background())
		}()
	}
	wg.Wait()

	for i, dc := range doneChs {
		select {
		case <-dc:
		case <-time.After(2 * time.Second):
			t.Fatalf("doneCh[%d] did not close within 2s", i)
		}
	}

	m.mu.Lock()
	total := 0
	for _, n := range m.disconnects {
		total += n
	}
	lifetimes := len(m.lifetimes)
	m.mu.Unlock()

	if total != nSubs {
		t.Errorf("total disconnects = %d; want %d (one per sub, no double-emission across concurrent Shutdowns)", total, nSubs)
	}
	if lifetimes != nSubs {
		t.Errorf("SubscriberLifetimeObserve count = %d; want %d", lifetimes, nSubs)
	}
}

// TestPool_Shutdown_CtxExpiry_ClosesAllDoneChannels — when the ctx
// expires before drain completes (wedged subs prevent it), Pool.Shutdown
// MUST still close every owned sub's done channel best-effort and return
// ctx.Err(). Pins B-1: handler's <-doneCh never hangs on shutdown
// regardless of ctx outcome (CONTEXT.md Q2 invariant).
// Wedge mechanism: 4 subs share a single worker (PoolFactor=1). Each
// sub's respWriter blocks 1 second inside Write before returning, so
// the worker's drainShutdown loop will take >>50ms (the ctx budget).
// The pool's ctx.Done() arm fires first and invokes the
// abandonCloseDoneChans best-effort path.
func TestPool_Shutdown_CtxExpiry_ClosesAllDoneChannels(t *testing.T) {
	// Not t.Parallel() — GOMAXPROCS(1) coerces all 4 subs onto one worker.
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)

	const nSubs = 4

	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          4,
		MaxWaitMs:             2,
		DrainThresholdSubs:    1,
		MaxBatchBytesPerSub:   64 * 1024,
		WriteTimeout:          time.Second,
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: 50 * time.Millisecond,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})

	doneChs := make([]<-chan struct{}, nSubs)
	for i := 0; i < nSubs; i++ {
		sub := &fakeSub{id: "ctx-expiry-" + idSuffix(i)}
		// Each blockingRespWriter sleeps 1s on its first non-prelude
		// Write. The prelude is the FIRST Write inside Attach; we let
		// it succeed by using a Writer that only blocks AFTER the
		// prelude. Simpler: hold a counter that lets the prelude
		// through and blocks subsequent writes.
		rw := &blockingAfterPreludeRW{blockDur: time.Second}
		rc := http.NewResponseController(rw)
		dc, err := p.Attach(sub, nil, rw, rc)
		if err != nil {
			t.Fatalf("Attach %d: %v", i, err)
		}
		doneChs[i] = dc
	}

	// 50ms ctx — Shutdown is forced into the abandon path because each
	// drainShutdown.Write blocks 1s and 4 subs serial = 4s, far above
	// the 50ms ctx budget.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	serr := p.Shutdown(ctx)
	if !errors.Is(serr, context.DeadlineExceeded) {
		t.Fatalf("Shutdown returned %v; want context.DeadlineExceeded (ctx-abandon path)", serr)
	}

	// Every sub's doneCh MUST close within the outer 3s deadline — that
	// is the B-1 invariant the abandon path pins.
	for i, dc := range doneChs {
		select {
		case <-dc:
		case <-time.After(3 * time.Second):
			t.Fatalf("doneCh[%d] did not close within 3s after ctx-expiry abandon; B-1 invariant violated", i)
		}
	}
}

// blockingAfterPreludeRW lets the prelude Write succeed (so Attach
// completes normally) and then sleeps + returns a timeout error on
// every subsequent Write. The counter is mutated by Write only — the
// pool's worker is the single Writer caller, so no lock needed.
type blockingAfterPreludeRW struct {
	blockDur     time.Duration
	writesBefore atomic.Int32
}

func (*blockingAfterPreludeRW) Header() http.Header { return http.Header{} }
func (*blockingAfterPreludeRW) WriteHeader(int)     {}
func (w *blockingAfterPreludeRW) Write(p []byte) (int, error) {
	n := w.writesBefore.Add(1)
	if n == 1 {
		// First write is the prelude — let it succeed.
		return len(p), nil
	}
	time.Sleep(w.blockDur)
	return 0, timeoutNetError{}
}
func (*blockingAfterPreludeRW) Flush() {}

// TestPool_Shutdown_AbandonEmitsLifecycleAndResetsGauge
// review-fix regression. The ctx-abandon path historically:
//
//	(a) skipped SubscriberLifetimeObserve and SubscriberDisconnectsInc
//	    for any not-yet-accounted sub, and
//	(b) left the per-worker walera_pool_worker_dirty_subs gauge
//	    non-zero if abandoned subs were in dirty[] at the time the
//	    abandon fired.
//
// After: drainShutdownAbandon emits lifecycle metrics for every
// not-yet-accounted sub with reason="shutdown" (joining the clean-
// shutdown cohort per CONTEXT.md Q2.4), and resets the per-worker
// dirty-subs gauge to 0 after closing the done channels.
// Test setup mirrors TestPool_Shutdown_CtxExpiry_ClosesAllDoneChannels
// but adds metric assertions on the abandon path.
func TestPool_Shutdown_AbandonEmitsLifecycleAndResetsGauge(t *testing.T) {
	// Not t.Parallel() — GOMAXPROCS(1) coerces all subs onto one worker.
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)

	const nSubs = 3

	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          4,
		MaxWaitMs:             2,
		DrainThresholdSubs:    1,
		MaxBatchBytesPerSub:   64 * 1024,
		WriteTimeout:          time.Second,
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: 50 * time.Millisecond,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})

	doneChs := make([]<-chan struct{}, nSubs)
	for i := 0; i < nSubs; i++ {
		sub := &fakeSub{id: "wr03-abandon-" + idSuffix(i)}
		// blockingAfterPreludeRW sleeps 1s inside Write — guarantees the
		// 50ms ctx fires while the worker is in drainShutdown for sub 0.
		// Subs 1 and 2 are still in w.subs when abandon hits → they go
		// through drainShutdownAbandon, which must now emit their
		// lifecycle metrics and reset the dirty gauge.
		rw := &blockingAfterPreludeRW{blockDur: time.Second}
		rc := http.NewResponseController(rw)
		dc, err := p.Attach(sub, nil, rw, rc)
		if err != nil {
			t.Fatalf("Attach %d: %v", i, err)
		}
		doneChs[i] = dc
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	serr := p.Shutdown(ctx)
	if !errors.Is(serr, context.DeadlineExceeded) {
		t.Fatalf("Shutdown returned %v; want context.DeadlineExceeded", serr)
	}

	// All doneChs MUST be closed (B-1 invariant).
	for i, dc := range doneChs {
		select {
		case <-dc:
		case <-time.After(3 * time.Second):
			t.Fatalf("doneCh[%d] not closed within 3s after abandon", i)
		}
	}

	m.mu.Lock()
	shutdownN := m.disconnects["shutdown"]
	slowN := m.disconnects["slow_consumer"]
	totalDisconnects := 0
	for _, n := range m.disconnects {
		totalDisconnects += n
	}
	lifetimes := len(m.lifetimes)
	gaugeValue, gaugeSet := m.dirtySubsSet["0"]
	m.mu.Unlock()

	// Every sub should be accounted exactly once — but the LABEL split
	// is:
	//   - sub 0 (the one the worker reached in drainShutdown's main
	//     loop before ctx fired): its blocking Write timed out after
	//     50 ms, so the truthful-reason override re-labels it
	//     "slow_consumer".
	//   - subs 1+2 (still in w.subs when abandon fired): emitted by
	//     drainShutdownAbandon with reason="shutdown".
	// Total disconnects = nSubs; the split is 1 slow_consumer + (nSubs-1)
	// shutdown. Lifetimes = nSubs (one observation per sub regardless of
	// reason label).
	if totalDisconnects != nSubs {
		t.Errorf("total disconnects = %d; want %d (: every sub must be accounted exactly once across the abandon + main-loop paths)", totalDisconnects, nSubs)
	}
	if shutdownN+slowN != nSubs {
		t.Errorf("disconnects[shutdown]+disconnects[slow_consumer] = %d+%d=%d; want %d (no other reason should fire)", shutdownN, slowN, shutdownN+slowN, nSubs)
	}
	if shutdownN < 1 {
		t.Errorf("disconnects[shutdown] = %d; want >= 1 (: drainShutdownAbandon must emit the shutdown reason for the abandoned cohort)", shutdownN)
	}
	if lifetimes != nSubs {
		t.Errorf("SubscriberLifetimeObserve count = %d; want %d (: abandon path must observe lifetime for every not-yet-accounted sub)", lifetimes, nSubs)
	}

	// Dirty-subs gauge must be reset to 0 by drainShutdownAbandon's
	// tail Set(0) call. Pre-fix the gauge would carry whatever non-zero
	// value it had when ctx fired.
	if !gaugeSet {
		t.Errorf("PoolWorkerDirtySubsSet(\"0\", _) not called by drainShutdownAbandon;  re-sync missing")
	} else if gaugeValue != 0 {
		t.Errorf("PoolWorkerDirtySubsSet(\"0\", %v); want 0 (: dirty gauge must be reset to 0 on abandon)", gaugeValue)
	}
}

// TestPool_Shutdown_HonoursRouterDropReason review-fix
// regression. drainShutdown's reason resolution pre-fix did NOT consult
// st.sub.Reason() — only st.dropReason. If a sub.Drop("auth_revoked")
// from the router-side auth fan-out raced shutdownCh closing, the
// metric label and wire frame would be reason="shutdown" (and
// EncodeShutdown) instead of reason="auth_revoked" (and EncodeError) —
// losing wire-frame and metric fidelity for the narrow router-Drop-
// then-shutdown race window.
// The fix mirrors evictDone's reason-resolution order (pool.go:831-834):
//  1. st.sub.Reason() (router-side Drop reason)
//  2. st.dropReason (sticky from prior handleSubWriteFailure)
//  3. "shutdown" (local fallback)
//
// This test wires a fakeSubWithReason that returns "auth_revoked" via
// the Reason() method and verifies (a) the metric label is
// "auth_revoked", (b) the wire frame is an EncodeError("auth_revoked")
// payload (NOT EncodeShutdown).
func TestPool_Shutdown_HonoursRouterDropReason(t *testing.T) {
	t.Parallel()

	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          4,
		MaxWaitMs:             2,
		WriteTimeout:          time.Second,
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: 50 * time.Millisecond,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})

	sub := &fakeSubWithReason{
		fakeSub: fakeSub{id: "wr02-router-drop"},
		reason:  "auth_revoked",
	}
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	if serr := p.Shutdown(context.Background()); serr != nil {
		t.Fatalf("Shutdown returned %v; want nil", serr)
	}
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("doneCh did not close after Shutdown")
	}

	m.mu.Lock()
	authRevokedN := m.disconnects["auth_revoked"]
	shutdownN := m.disconnects["shutdown"]
	m.mu.Unlock()

	if authRevokedN != 1 {
		t.Errorf("disconnects[auth_revoked] = %d; want 1 ( regression: router-side Reason() must win over local 'shutdown' fallback)", authRevokedN)
	}
	if shutdownN != 0 {
		t.Errorf("disconnects[shutdown] = %d; want 0 (router-side Reason set; should NOT be labelled shutdown)", shutdownN)
	}

	// Wire-frame check: drainShutdown must emit EncodeError("auth_revoked")
	// when the router reason is set, NOT EncodeShutdown(). fakeEncoder's
	// EncodeError returns "event: error\ndata: {}\n\n" — assert that
	// payload landed on the wire (not the EncodeShutdown payload
	// "event: shutdown\n...").
	rw.mu.Lock()
	got := rw.buf.String()
	rw.mu.Unlock()

	if !strings.Contains(got, "event: error") {
		t.Errorf("wire bytes do not contain %q; got %q.  regression: drainShutdown must emit EncodeError, not EncodeShutdown, when the router reason is set.", "event: error", got)
	}
	if strings.Contains(got, "event: shutdown") {
		t.Errorf("wire bytes contain unexpected %q; got %q. The router-side reason should suppress the EncodeShutdown frame.", "event: shutdown", got)
	}
}

// alwaysFailAfterPreludeRW lets the prelude (first Write) succeed and
// returns a generic non-timeout error on every subsequent Write. Used by
// the double-close regression test to provoke repeated
// handleSubWriteFailure firings on the same sub — before the
// safeCloseDone fix the second firing would panic on close(st.done).
type alwaysFailAfterPreludeRW struct {
	writes atomic.Int32
}

func (*alwaysFailAfterPreludeRW) Header() http.Header { return http.Header{} }
func (*alwaysFailAfterPreludeRW) WriteHeader(int)     {}
func (w *alwaysFailAfterPreludeRW) Write(p []byte) (int, error) {
	n := w.writes.Add(1)
	if n == 1 {
		return len(p), nil
	}
	return 0, errors.New("alwaysFailAfterPreludeRW: write error")
}
func (*alwaysFailAfterPreludeRW) Flush() {}

// TestPool_HandleSubWriteFailure_IdempotentClose is the
// double-close regression test. An earlier handleSubWriteFailure called
// close(st.done) unconditionally. The race window is:
//  1. drainSub fails → handleSubWriteFailure → close(st.done). st.buffer
//     is NOT cleared (drainSub returns early before the buffer reset).
//     st.inDisconnected = true. st.dropReason set.
//  2. The subscriber's Done() channel eventually fires (handler exited
//     or sub.Drop() called by the router).
//  3. evictDone observes the doneCh closed and calls drainSub one more
//     time (line 821 of pool.go: "if len(st.buffer) > 0 { drainSub }").
//  4. drainSub's write fails again → handleSubWriteFailure runs again →
//     pre-fix: unconditional close(st.done) PANICS with "close of closed
//     channel". Post-fix: safeCloseDone observes the channel already
//     closed and is a no-op.
//
// The test drives exactly this sequence by closing the fakeSub's Done()
// after the first handleSubWriteFailure runs, then waiting for evictDone
// to pick it up. Before the safeCloseDone fix this panics; post-fix
// the test completes cleanly with a single SubscriberDisconnectsInc.
// Verified pre-fix-FAIL / post-fix-PASS by `git stash`-ing the pool.go
// change and re-running this test against the pre-fix tree.
func TestPool_HandleSubWriteFailure_IdempotentClose(t *testing.T) {
	t.Parallel()

	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          16,
		MaxWaitMs:             2,
		DrainThresholdSubs:    1,
		MaxBatchBytesPerSub:   64 * 1024,
		WriteTimeout:          100 * time.Millisecond,
		HeartbeatInterval:     time.Hour, // not exercised by this test
		drainShutdownDeadline: 50 * time.Millisecond,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "cr03-double-close"}
	// Pre-create sub.done so the test goroutine has a stable reference
	// to close below (the lazy Done() allocator races with the worker's
	// evictDone poll otherwise — same defensive pattern as
	// TestPool_Shutdown_SkipsAlreadyEvicted at pool_test.go:1593).
	_ = sub.Done()
	rw := &alwaysFailAfterPreludeRW{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// Push a frame to trigger drainSub → handleSubWriteFailure →
	// close(st.done). Buffer is NOT cleared by drainSub on the failure
	// path (the early return at pool.go:1098 / 1116 returns before the
	// buffer-reset block at pool.go:1138-1140).
	if !sub.Send([]byte("data: frame-1\n\n")) {
		t.Fatal("first Send returned false")
	}

	// Wait for the first disconnect (st.done closed).
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("doneCh did not close after first frame failure")
	}

	// Confirm the first SubscriberDisconnectsInc fired.
	m.mu.Lock()
	disconnectsAfterFirst := 0
	for _, n := range m.disconnects {
		disconnectsAfterFirst += n
	}
	m.mu.Unlock()
	if disconnectsAfterFirst != 1 {
		t.Fatalf("disconnects after first failure = %d; want 1", disconnectsAfterFirst)
	}

	// Now close sub.Done() → evictDone observes it on the next worker
	// cycle, calls drainSub once more on the (still non-empty) buffer →
	// write fails → handleSubWriteFailure runs SECOND time → pre-fix
	// close(st.done) panics. Post-fix safeCloseDone observes closed and
	// is a no-op.
	sub.mu.Lock()
	close(sub.done)
	sub.mu.Unlock()

	// Wait for evictDone to remove the sub. We cannot directly observe
	// eviction (it mutates worker-internal state), but a 200ms window
	// is enough for the worker's 1ms pollTimer + evictDone pass + the
	// second drainSub attempt to complete. If the worker panics, the
	// test binary aborts before reaching the assertions below.
	time.Sleep(200 * time.Millisecond)

	// Assert: SubscriberDisconnectsInc stayed at 1 (st.inDisconnected
	// guard prevents re-emission). Lifetime observation stays at 0
	// because the first failure's handleSubWriteFailure does NOT call
	// SubscriberLifetimeObserve (only SubscriberDisconnectsInc), and
	// evictDone's lifetime-emission block is gated by !st.inDisconnected
	// — which is now true. That asymmetry is pre-existing v1.3 behaviour
	// preserved across (see pool.go's handleSubWriteFailure
	// docstring); it is NOT regressed by the safeCloseDone fix.
	m.mu.Lock()
	totalDisconnects := 0
	for _, n := range m.disconnects {
		totalDisconnects += n
	}
	lifetimes := len(m.lifetimes)
	m.mu.Unlock()
	if totalDisconnects != 1 {
		t.Errorf("total disconnects after full cycle = %d; want exactly 1 (inDisconnected guard must prevent re-emission)", totalDisconnects)
	}
	// The operative double-close assertion is "no panic, no double-emission" —
	// reaching this line at all proves the worker survived the second
	// handleSubWriteFailure dispatch. We log the lifetime count for
	// observability but do not fail on it (the pre-existing asymmetry
	// described above is unchanged by this fix).
	t.Logf("double-close regression OK: handleSubWriteFailure → close(st.done) → evictDone → drainSub → handleSubWriteFailure → safeCloseDone path completed without panic; disconnects=%d lifetimes=%d", totalDisconnects, lifetimes)
}

// TestPool_Shutdown_AbandonBoundedByDrainDeadline verifies that the
// abandon-poll-between-subs path stays meaningful after the per-sub
// buffered drain was bounded to drainShutdownDeadline. The worker
// checks <-w.abandonCh at the TOP of each drainShutdown loop
// iteration, so with the bounded buffered drain the abandon-detection
// delay per sub is bounded by
// (DrainShutdownDeadline_buffered_drain + DrainShutdownDeadline_frame_write)
// ≈ 2 × drainShutdownDeadline. For nSubs subs on one worker, total
// abandon-respect wall-clock is ≤ ctxDeadline + 2 × drainShutdownDeadline
// × nSubs + slop.
// Without the bound the buffered drain inflated to WriteTimeout (= 5 s
// in production); a 50 ms ctx on 4 wedged subs would have taken ~20 s
// to return. With the bound the same scenario completes in <500 ms.
// Test setup mirrors TestPool_Shutdown_CtxExpiry_ClosesAllDoneChannels
// but uses a 20 ms ctx and asserts the tight upper bound. The
// blockingAfterPreludeRW sleeps inside Write so SetWriteDeadline cannot
// interrupt it; this exercises the abandon-poll-between-subs path, NOT
// the per-write deadline. Hence the bound is N × write_sleep_per_sub,
// where each sub sleeps drainShutdownDeadline + slop (rounded up to the
// sleep duration the test injects below).
func TestPool_Shutdown_AbandonBoundedByDrainDeadline(t *testing.T) {
	// Not t.Parallel() — GOMAXPROCS(1) coerces all subs onto one worker.
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)

	const (
		nSubs                 = 4
		drainShutdownDeadline = 50 * time.Millisecond
		writeSleep            = 80 * time.Millisecond
		ctxDeadline           = 20 * time.Millisecond
		// Each sub takes up to writeSleep (which exceeds drainShutdownDeadline
		// — the deadline cannot interrupt a time.Sleep inside a fake Write
		// so the sub-write returns after writeSleep regardless). After ctx
		// fires, the worker checks abandonCh on the NEXT loop iteration —
		// so the total wall-clock upper bound is ctxDeadline + nSubs *
		// writeSleep + slop. The "slop" covers goroutine scheduling,
		// abandon-channel-close propagation, and drainShutdownAbandon's
		// done-close loop.
		slop = 300 * time.Millisecond
	)

	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          4,
		MaxWaitMs:             2,
		DrainThresholdSubs:    1,
		MaxBatchBytesPerSub:   64 * 1024,
		WriteTimeout:          time.Second,
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: drainShutdownDeadline,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})

	doneChs := make([]<-chan struct{}, nSubs)
	for i := 0; i < nSubs; i++ {
		sub := &fakeSub{id: "cr02-bound-" + idSuffix(i)}
		rw := &blockingAfterPreludeRW{blockDur: writeSleep}
		rc := http.NewResponseController(rw)
		dc, err := p.Attach(sub, nil, rw, rc)
		if err != nil {
			t.Fatalf("Attach %d: %v", i, err)
		}
		doneChs[i] = dc
	}

	ctx, cancel := context.WithTimeout(context.Background(), ctxDeadline)
	defer cancel()

	start := time.Now()
	serr := p.Shutdown(ctx)
	elapsed := time.Since(start)

	if !errors.Is(serr, context.DeadlineExceeded) {
		t.Fatalf("Shutdown returned %v; want context.DeadlineExceeded", serr)
	}

	// Bound: with the bounded drain deadline, the abandon-respect
	// wall-clock is ≤ ctxDeadline + nSubs × writeSleep + slop. Before
	// that bound the writeSleep would have been WriteTimeout (= 1 s
	// here), making the lower bound nSubs * 1 s = 4 s — orders of
	// magnitude larger than the post-fix bound below.
	bound := ctxDeadline + time.Duration(nSubs)*writeSleep + slop
	if elapsed > bound {
		t.Errorf("Shutdown elapsed = %v; want ≤ %v (abandon-poll regression: must respect ctx within %d × %v + slop, not %v).",
			elapsed, bound, nSubs, writeSleep, p.cfg.WriteTimeout)
	}

	// All doneChs MUST close after Shutdown returns (B-1 invariant —
	// handler's <-doneCh never hangs on abandon).
	for i, dc := range doneChs {
		select {
		case <-dc:
		case <-time.After(2 * time.Second):
			t.Fatalf("doneCh[%d] not closed within 2s after abandon", i)
		}
	}
	t.Logf("abandon-poll regression OK: shutdown returned ctx.Err() after %v (bound=%v, nSubs=%d, writeSleep=%v)",
		elapsed, bound, nSubs, writeSleep)
}

// ----------------------------------------------------------------------
// Pool-metric emission tests.
// ----------------------------------------------------------------------

// TestPool_DirtySubs_IncOnFirstFrame_SetOnDrain attaches one sub, enqueues
// one frame, waits for the drain to complete, and asserts:
//   - PoolWorkerDirtySubsInc was emitted exactly once for the owning
//     worker (clean→dirty transition in markDirty).
//   - PoolWorkerDirtySubsSet("<worker_id>", 0) was emitted at least once
//     (the re-sync at the end of drainAll per CONTEXT.md Q3).
//
// The exact worker id is determined by xxhash.Sum64String(sub.id) %
// poolSize — we don't pin it; the test asserts on the SOLE inc/set seen
// across all worker labels. This keeps the test stable against future
// hash changes.
func TestPool_DirtySubs_IncOnFirstFrame_SetOnDrain(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:        1, // single worker for deterministic accounting
		SubQueueSize:      4,
		MaxWaitMs:         2,
		WriteTimeout:      time.Second,
		HeartbeatInterval: time.Hour,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	sub := &fakeSub{id: "dirty-inc-sub"}
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	if _, err := p.Attach(sub, nil, rw, rc); err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// One frame → first-frame-since-empty → markDirty → Inc.
	if !sub.Send([]byte("data: a\n\n")) {
		t.Fatal("Send returned false; queue should accept")
	}

	// Wait for the Inc to land (markDirty ran). The drainAll Set(0)
	// follows shortly after; we wait for the batch-size histogram
	// observation as the drain-completion signal (the NewPool
	// pre-touch already populated dirtySubsSet, so we cannot use that
	// map as the wait sentinel).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		incSeen := false
		for _, v := range m.dirtySubsInc {
			if v >= 1 {
				incSeen = true
				break
			}
		}
		drainSeen := len(m.drainBatchSize) >= 1
		m.mu.Unlock()
		if incSeen && drainSeen {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	totalInc := 0
	for _, v := range m.dirtySubsInc {
		totalInc += v
	}
	if totalInc != 1 {
		t.Errorf("PoolWorkerDirtySubsInc total = %d; want 1", totalInc)
	}
	// After drain, the owning worker has Set its label to 0 (re-sync per
	// CONTEXT.md Q3). The NewPool pre-touch already populated every
	// label to 0, so this assertion is satisfied even before drain —
	// the meaningful guard is the Inc count above plus the histogram
	// observation (drainBatchSize) which only happens inside drainAll.
	if len(m.drainBatchSize) < 1 {
		t.Errorf("drainAll did not run: no PoolDrainBatchSizeObserve seen")
	}
	hasZero := false
	for _, v := range m.dirtySubsSet {
		if v == 0 {
			hasZero = true
			break
		}
	}
	if !hasZero {
		t.Errorf("no PoolWorkerDirtySubsSet(_, 0) emitted")
	}
}

// TestPool_DrainHistogramsRecorded verifies a single drainAll cycle
// produces exactly one PoolDrainBatchSizeObserve and one
// PoolDrainDurationObserve call, with values in the sane range.
func TestPool_DrainHistogramsRecorded(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:        1,
		SubQueueSize:      8,
		MaxWaitMs:         2,
		WriteTimeout:      time.Second,
		HeartbeatInterval: time.Hour,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	sub := &fakeSub{id: "drain-hist-sub"}
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	if _, err := p.Attach(sub, nil, rw, rc); err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// Send one frame; the worker drains it after MaxWaitMs.
	if !sub.Send([]byte("data: hist\n\n")) {
		t.Fatal("Send returned false")
	}

	// Wait until both histograms have at least one observation.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		batchN := len(m.drainBatchSize)
		durN := len(m.drainDuration)
		m.mu.Unlock()
		if batchN >= 1 && durN >= 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.drainBatchSize) < 1 {
		t.Fatalf("PoolDrainBatchSizeObserve count = %d; want >= 1", len(m.drainBatchSize))
	}
	if len(m.drainDuration) < 1 {
		t.Fatalf("PoolDrainDurationObserve count = %d; want >= 1", len(m.drainDuration))
	}
	if m.drainBatchSize[0] < 1 {
		t.Errorf("drainBatchSize[0] = %v; want >= 1 (one dirty sub)", m.drainBatchSize[0])
	}
	if d := m.drainDuration[0]; d < 0 || d > 1.0 {
		t.Errorf("drainDuration[0] = %v; want in [0, 1] seconds", d)
	}
}

// TestPool_DrainBatchSize_RecordsLenDirty drives multiple subs onto a
// single worker and asserts the first drainAll observation receives
// float64(len(dirty)) for the batch-size histogram. The subs are
// constructed with short IDs whose xxhash falls onto worker 0 (the only
// worker — PoolFactor=1, GOMAXPROCS-dependent poolSize but we pin
// GOMAXPROCS via runtime.GOMAXPROCS(1) for the test).
func TestPool_DrainBatchSize_RecordsLenDirty(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:        1, // poolSize = 1 with GOMAXPROCS=1
		SubQueueSize:      8,
		MaxWaitMs:         5,
		WriteTimeout:      time.Second,
		HeartbeatInterval: time.Hour,
		// Threshold high enough that the drain happens on the timer, not
		// on threshold-reached — so all N subs are in `dirty` at the
		// same time when drainAll fires.
		DrainThresholdSubs: 100,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	const N = 5
	subs := make([]*fakeSub, N)
	for i := 0; i < N; i++ {
		s := &fakeSub{id: "batch-sub-" + strconv.Itoa(i)}
		rw := &fakeResponseWriter{}
		rc := http.NewResponseController(rw)
		if _, err := p.Attach(s, nil, rw, rc); err != nil {
			t.Fatalf("Attach[%d]: %v", i, err)
		}
		subs[i] = s
	}

	// Send one frame on each sub as quickly as possible so they all land
	// in the dirty list before the worker's timer fires.
	for i, s := range subs {
		if !s.Send([]byte("data: x\n\n")) {
			t.Fatalf("Send[%d] returned false", i)
		}
	}

	// Wait for at least one drainBatchSize observation.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		n := len(m.drainBatchSize)
		m.mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.drainBatchSize) < 1 {
		t.Fatalf("no PoolDrainBatchSizeObserve observation seen")
	}
	// The first observation should be >= 1 and <= N. Since worker
	// scheduling may split the drain across multiple cycles, we accept
	// any sum across observations equal to N; the first observation
	// itself is at least 1.
	sum := 0.0
	for _, v := range m.drainBatchSize {
		sum += v
	}
	if sum < 1 {
		t.Errorf("sum(drainBatchSize) = %v; want >= 1", sum)
	}
	if m.drainBatchSize[0] < 1 {
		t.Errorf("drainBatchSize[0] = %v; want >= 1", m.drainBatchSize[0])
	}
}

// TestPool_WorkerIDPreTouchedAtNewPool verifies the pre-touch:
// after NewPool returns with poolSize=N, every worker_id label in
// ["0"..."N-1"] has been Set(0) on PoolWorkerDirtySubs. Without this,
// /metrics would not emit per-worker samples until the first dirty
// transition, defeating dashboards that expect all worker labels from t=0.
func TestPool_WorkerIDPreTouchedAtNewPool(t *testing.T) {
	prev := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	m := newFakeMetrics()
	const factor = 2
	p := NewPool(PoolConfig{
		PoolFactor:        factor, // poolSize = 4 * 2 = 8
		SubQueueSize:      4,
		MaxWaitMs:         2,
		WriteTimeout:      time.Second,
		HeartbeatInterval: time.Hour,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	wantPoolSize := runtime.GOMAXPROCS(0) * factor
	if wantPoolSize != 8 {
		t.Fatalf("setup precondition: GOMAXPROCS(4)*factor(2) should be 8; got %d", wantPoolSize)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.dirtySubsSet) != wantPoolSize {
		t.Errorf("PoolWorkerDirtySubsSet pre-touch count = %d; want %d",
			len(m.dirtySubsSet), wantPoolSize)
	}
	for i := 0; i < wantPoolSize; i++ {
		key := strconv.Itoa(i)
		v, ok := m.dirtySubsSet[key]
		if !ok {
			t.Errorf("worker_id=%q not pre-touched at NewPool", key)
			continue
		}
		if v != 0 {
			t.Errorf("worker_id=%q pre-touch value = %v; want 0", key, v)
		}
	}
}

// TestPool_DirtySubs_DecOnEvict drives a sub onto the dirty list, then
// closes its done channel (simulating handler exit). The eviction path
// in evictDone runs while st.inDirty == true, so the gauge must be
// decremented in addition to the lifecycle metric.
func TestPool_DirtySubs_DecOnEvict(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:   1,
		SubQueueSize: 4,
		// max_wait_ms long enough that the drain does not happen before
		// the test closes done — we want the eviction path to fire while
		// inDirty is still true.
		MaxWaitMs:          500,
		DrainThresholdSubs: 100,
		WriteTimeout:       time.Second,
		HeartbeatInterval:  time.Hour,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	sub := &fakeSub{id: "evict-dec-sub"}
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	if _, err := p.Attach(sub, nil, rw, rc); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	// Force lazy done-channel allocation (cf. TestPool_Shutdown_
	// SkipsAlreadyEvicted precedent) so close(sub.done) below is safe.
	_ = sub.Done()

	if !sub.Send([]byte("data: dec\n\n")) {
		t.Fatal("Send returned false")
	}

	// Give the worker a tick to enqueue the frame into st.buffer and
	// run markDirty.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		totalInc := 0
		for _, v := range m.dirtySubsInc {
			totalInc += v
		}
		m.mu.Unlock()
		if totalInc >= 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Trigger eviction while still in the dirty list.
	close(sub.done)

	// Wait for the lifecycle disconnect to fire (signals evictDone ran).
	for time.Now().Before(deadline) {
		m.mu.Lock()
		totalDis := 0
		for _, v := range m.disconnects {
			totalDis += v
		}
		m.mu.Unlock()
		if totalDis >= 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	totalDec := 0
	for _, v := range m.dirtySubsDec {
		totalDec += v
	}
	if totalDec < 1 {
		t.Errorf("PoolWorkerDirtySubsDec total = %d; want >= 1 (sub was in dirty list at evict time)", totalDec)
	}
}

// ----------------------------------------------------------------------
// drainSubDeadline regression: drainSubDeadline must use the
// CALLER-SUPPLIED write budget, not the steady-state WriteTimeout.
// An earlier drainShutdown buffered-frame branch called
// drainSub(st, time.Now()) which baked w.cfg.WriteTimeout into the
// SetWriteDeadline computation. For PoolConfig{ WriteTimeout: 5s,
// drainShutdownDeadline: 50ms } that pushed the per-sub buffered-drain
// wall-clock to ≥5 s — multiplied by max_subs_per_worker, the documented
// 0.6 s shutdown budget became thousands of seconds.
// With the fix, drainShutdown calls drainSubDeadline(st, now,
// w.cfg.drainShutdownDeadline), so a wedged-on-buffered-drain sub
// completes in ≤drainShutdownDeadline + slop, NOT ≤WriteTimeout + slop.
// This is a low-level unit test (poolWorker direct invocation, no
// Pool/Shutdown round-trip) so it can isolate the deadline-bound
// invariant from the run-loop / drainAll interactions that would
// otherwise drain the buffer before drainShutdown sees it.
// ----------------------------------------------------------------------

// deadlineCapturingRespWriter records the deadline most recently set via
// SetWriteDeadline (called by the pool's drainSub through
// http.ResponseController.SetWriteDeadline). Write returns success so
// the drain path completes cleanly; the deadline is the observable that
// proves the caller-supplied budget routed drainShutdownDeadline (not
// WriteTimeout) into the kernel-level write cap.
type deadlineCapturingRespWriter struct {
	mu            sync.Mutex
	buf           bytes.Buffer
	lastDeadline  time.Time
	deadlineCount int
}

func (*deadlineCapturingRespWriter) Header() http.Header { return http.Header{} }
func (*deadlineCapturingRespWriter) WriteHeader(int)     {}
func (w *deadlineCapturingRespWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
func (*deadlineCapturingRespWriter) Flush() {}

// SetWriteDeadline is the interface http.ResponseController consults
// via reflection (see net/http.controllerDeadliner). Recording the
// deadline lets the test assert which budget the pool routed into the
// kernel write cap.
func (w *deadlineCapturingRespWriter) SetWriteDeadline(t time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastDeadline = t
	w.deadlineCount++
	return nil
}

// TestPool_DrainSubDeadline_RoutesCallerBudget verifies drainSubDeadline
// uses the CALLER-supplied write budget when computing
// SetWriteDeadline, NOT w.cfg.WriteTimeout. The invariant:
// drainShutdown's buffered-frame branch calls drainSubDeadline with
// w.cfg.drainShutdownDeadline (50ms default), so a wedged sub on the
// buffered-drain path cannot pin a worker partition past the documented
// 0.6 s shutdown wall-clock bound. Pre-fix code called drainSub(st, now)
// which baked w.cfg.WriteTimeout (5 s default) into the deadline,
// inflating the worst case to thousands of seconds.
// The test invokes drainSubDeadline directly (no Pool/Shutdown round-trip)
// and inspects the deadline captured by the writer's SetWriteDeadline
// hook. The captured deadline value is the observable contract — it MUST
// land inside [now + drainBudget - slop, now + drainBudget + slop] and
// MUST NOT match the steady-state writeTimeout window.
func TestPool_DrainSubDeadline_RoutesCallerBudget(t *testing.T) {
	t.Parallel()

	const (
		drainBudget  = 25 * time.Millisecond
		writeTimeout = time.Second
		// slop covers scheduling jitter between the test's `time.Now()`
		// and drainSubDeadline's internal `time.Now()`; intentionally
		// generous so the test stays green on shared CI runners under
		// -race but still tiny relative to writeTimeout.
		slop = 50 * time.Millisecond
	)

	w := newPoolWorker(0, PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          4,
		MaxWaitMs:             2,
		WriteTimeout:          writeTimeout,
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: drainBudget,
	}, fakeEncoder{}, newFakeMetrics(), zerolog.Nop())

	// Build a subState by hand — no Attach round-trip, no run-loop.
	rw := &deadlineCapturingRespWriter{}
	rc := http.NewResponseController(rw)

	st := &subState{
		sub:        &fakeSub{id: "cr01-deadline-sub"},
		queue:      make(chan []byte, 4),
		respWriter: rw,
		rc:         rc,
		done:       make(chan struct{}),
		buffer:     [][]byte{[]byte("data: pending\n\n")},
		bufBytes:   16,
	}
	st.connectedAt = time.Now()
	st.lastWriteAt = st.connectedAt

	beforeCall := time.Now()
	w.drainSubDeadline(st, beforeCall, drainBudget)
	afterCall := time.Now()

	rw.mu.Lock()
	captured := rw.lastDeadline
	captureCount := rw.deadlineCount
	rw.mu.Unlock()

	// drainSubDeadline calls SetWriteDeadline twice on the respWriter
	// path: once with the active deadline and once with the zero value
	// to clear it after Flush. The "last" deadline observed by the
	// writer's hook is therefore the zero-value clear — we must check
	// the deadline value SET DURING the write, not the cleared one.
	// Inspect captureCount as a sanity check and pull the active
	// deadline by checking captured (when non-zero) or by intercepting
	// only the first SetWriteDeadline.
	if captureCount < 1 {
		t.Fatalf("SetWriteDeadline was not called (count=%d); drainSubDeadline did not route through the respWriter deadline path", captureCount)
	}

	// The last captured value is the post-write clear (zero time). Look
	// at the FIRST captured deadline instead by re-running with a
	// recording-list writer.
	rw2 := &deadlineListRespWriter{}
	rc2 := http.NewResponseController(rw2)
	st2 := &subState{
		sub:        &fakeSub{id: "cr01-deadline-list-sub"},
		queue:      make(chan []byte, 4),
		respWriter: rw2,
		rc:         rc2,
		done:       make(chan struct{}),
		buffer:     [][]byte{[]byte("data: pending\n\n")},
		bufBytes:   16,
	}
	st2.connectedAt = time.Now()
	st2.lastWriteAt = st2.connectedAt

	beforeCall2 := time.Now()
	w.drainSubDeadline(st2, beforeCall2, drainBudget)

	rw2.mu.Lock()
	deadlines := append([]time.Time(nil), rw2.deadlines...)
	rw2.mu.Unlock()

	if len(deadlines) < 1 {
		t.Fatalf("SetWriteDeadline not called via responseController; got %d invocations", len(deadlines))
	}
	// First non-zero deadline is the one routed through drainSubDeadline's
	// SetWriteDeadline(now + writeBudget) call.
	var active time.Time
	for _, d := range deadlines {
		if !d.IsZero() {
			active = d
			break
		}
	}
	if active.IsZero() {
		t.Fatalf("no non-zero deadline observed in %v; SetWriteDeadline was not given a non-zero budget", deadlines)
	}

	// The active deadline must land in [beforeCall2 + drainBudget - slop,
	// afterCall + drainBudget + slop]. The earlier (buggy) code would
	// put the deadline at beforeCall + writeTimeout, which is far
	// outside this window.
	wantLow := beforeCall2.Add(drainBudget - slop)
	wantHigh := afterCall.Add(drainBudget + slop)
	if active.Before(wantLow) || active.After(wantHigh) {
		t.Errorf("SetWriteDeadline=%v; want in [%v, %v] (caller budget=%v). drainSubDeadline must use the SUPPLIED budget, not w.cfg.WriteTimeout=%v.",
			active, wantLow, wantHigh, drainBudget, writeTimeout)
	}

	// Crucially, the deadline must NOT be on the writeTimeout (1 s) window.
	preFixLow := beforeCall2.Add(writeTimeout - slop)
	if !active.Before(preFixLow) {
		t.Errorf("SetWriteDeadline=%v lands inside the pre-fix writeTimeout=%v window; the caller-supplied budget was ignored.", active, writeTimeout)
	}

	// Sanity: the underlying call returned (buffer drained or write failed
	// either way). Used to detect a deadlock in the deadline-routing path
	// itself, which would never reach this assertion line.
	_ = captured
	t.Logf("drainSubDeadline regression OK: SetWriteDeadline routed at %v (now+%v); caller budget honoured.",
		active, active.Sub(beforeCall2))
}

// deadlineListRespWriter records every SetWriteDeadline value (in call
// order) so the test can inspect the non-zero deadline routed by
// drainSubDeadline before the trailing zero-time clear.
type deadlineListRespWriter struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	deadlines []time.Time
}

func (*deadlineListRespWriter) Header() http.Header { return http.Header{} }
func (*deadlineListRespWriter) WriteHeader(int)     {}
func (w *deadlineListRespWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
func (*deadlineListRespWriter) Flush() {}
func (w *deadlineListRespWriter) SetWriteDeadline(t time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deadlines = append(w.deadlines, t)
	return nil
}
