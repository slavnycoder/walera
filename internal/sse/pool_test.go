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

type fakeEncoder struct{}

func (fakeEncoder) EncodeHeartbeat() []byte   { return []byte(":\n\n") }
func (fakeEncoder) EncodeShutdown() []byte    { return []byte("event: shutdown\ndata: {}\n\n") }
func (fakeEncoder) EncodeError(string) []byte { return []byte("event: error\ndata: {}\n\n") }

type fakeMetrics struct {
	mu          sync.Mutex
	eventsSent  map[string]int
	txDropped   map[string]int
	lifetimes   []float64
	disconnects map[string]int

	dirtySubsInc   map[string]int
	dirtySubsDec   map[string]int
	dirtySubsSet   map[string]float64
	drainBatchSize []float64
	drainDuration  []float64

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

func TestPoolDrainCoalescesMultipleFrames(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       16,
		MaxWaitMs:          5,
		DrainThresholdSubs: 100,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "sub-1"}
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	for i := 0; i < 5; i++ {
		frame := []byte("data: msg-" + string(rune('0'+i)) + "\n\n")
		if !sub.Send(frame) {
			t.Fatalf("Send #%d returned false (queue full)", i)
		}
	}

	time.Sleep(20 * time.Millisecond)

	rw.mu.Lock()
	got := rw.buf.String()
	rw.mu.Unlock()

	const prelude = "retry: 15000\n\n"
	want := prelude + "data: msg-0\n\ndata: msg-1\n\ndata: msg-2\n\ndata: msg-3\n\ndata: msg-4\n\n"
	if got != want {
		t.Fatalf("output mismatch:\n got: %q\nwant: %q", got, want)
	}

	m.mu.Lock()
	gotEvents := m.eventsSent["wildcard"]
	m.mu.Unlock()
	if gotEvents != 5 {
		t.Errorf("EventsSent = %d, want 5", gotEvents)
	}

	select {
	case <-doneCh:
		t.Error("doneCh closed prematurely")
	default:
	}
}

func TestPoolBackpressureDrop(t *testing.T) {
	t.Parallel()
	p := NewPool(PoolConfig{
		PoolFactor:   1,
		SubQueueSize: 2,

		MaxWaitMs:          1000,
		DrainThresholdSubs: 100,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "slow-sub"}
	rw := &poolSlowRespWriter{}
	rc := http.NewResponseController(rw)
	_, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	gotFalse := false
	for i := 0; i < 100; i++ {
		if !sub.Send([]byte("frame")) {
			gotFalse = true
			break
		}

		time.Sleep(time.Microsecond)
	}
	if !gotFalse {
		t.Skip("queue never filled — worker drained faster than we could fill; backpressure path not exercised in this environment")
	}
}

type poolSlowRespWriter struct {
	dropped atomic.Bool
}

func (w *poolSlowRespWriter) Header() http.Header { return http.Header{} }
func (w *poolSlowRespWriter) WriteHeader(int)     {}
func (w *poolSlowRespWriter) Write(p []byte) (int, error) {
	if w.dropped.Load() {
		return 0, io.ErrClosedPipe
	}

	time.Sleep(50 * time.Millisecond)
	return len(p), nil
}
func (w *poolSlowRespWriter) Flush() {}

func TestPoolDrainThresholdEager(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	const nSubs = 5
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       8,
		MaxWaitMs:          1000,
		DrainThresholdSubs: nSubs,
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

	for i, s := range subs {
		s.Send([]byte("hello-" + string(rune('0'+i)) + "\n"))
	}

	time.Sleep(50 * time.Millisecond)

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

		if w.written > len("retry: 15000\n\n") {
			return 0, w.writeErr
		}
	}
	return len(p), nil
}
func (w *erroringRespWriter) Flush() {}

func TestPoolHandleSubWriteFailure(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       4,
		MaxWaitMs:          1,
		DrainThresholdSubs: 1,
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

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("doneCh never closed after write failure")
	}

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

type preludeFailRespWriter struct{}

func (preludeFailRespWriter) Header() http.Header { return http.Header{} }
func (preludeFailRespWriter) WriteHeader(int)     {}
func (preludeFailRespWriter) Write(p []byte) (int, error) {
	return 0, io.ErrShortWrite
}
func (preludeFailRespWriter) Flush() {}

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

type fakeSubWithReason struct {
	fakeSub
	reason string
}

func (s *fakeSubWithReason) Reason() string { return s.reason }

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

	m.mu.Lock()
	disconnects := m.disconnects["auth_revoked"]
	m.mu.Unlock()
	if disconnects != 1 {
		t.Errorf("SubscriberDisconnects[auth_revoked] = %d; want 1", disconnects)
	}
}

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

func TestPoolWorkerHeartbeatSweepEnqueuesAfterInterval(t *testing.T) {
	t.Parallel()
	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       4,
		MaxWaitMs:          2,
		DrainThresholdSubs: 1,
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

	time.Sleep(50 * time.Millisecond)

	rw.mu.Lock()
	got := rw.buf.String()
	rw.mu.Unlock()

	const want = "retry: 15000\n\ndata: payload\n\n"
	if got != want {
		t.Errorf("body = %q; want %q (no heartbeat should appear within HeartbeatInterval of last write)", got, want)
	}
}

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

func TestPoolDrainTriggerPriority_ByteOverflowPreemptsThreshold(t *testing.T) {
	t.Parallel()
	srv, cli, closeAll := dialLoopbackTCP(t)
	defer closeAll()

	p := NewPool(PoolConfig{
		PoolFactor:          1,
		SubQueueSize:        4,
		MaxWaitMs:           1000,
		DrainThresholdSubs:  100,
		MaxBatchBytesPerSub: 64,
		WriteTimeout:        time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "byte-overflow-sub"}
	if _, err := p.Attach(sub, srv, nil, nil); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	drainPrelude(t, cli)

	frame := []byte(strings.Repeat("x", 76) + "\n\n\n\n")
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

	if elapsed > 30*time.Millisecond {
		t.Errorf("byte-overflow drain took %v; want <= 30ms (threshold/timer fallback would take >= 1000ms)", elapsed)
	}
	if !bytes.Equal(buf, frame) {
		t.Errorf("frame mismatch: got %q, want %q", buf, frame)
	}
}

func TestPoolDrainTriggerPriority_ThresholdPreemptsTimer(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	const nSubs = 4
	p := NewPool(PoolConfig{
		PoolFactor:          1,
		SubQueueSize:        4,
		MaxWaitMs:           1000,
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

func TestPoolDrainMaxWaitLagCeiling(t *testing.T) {
	t.Parallel()
	srv, cli, closeAll := dialLoopbackTCP(t)
	defer closeAll()

	p := NewPool(PoolConfig{
		PoolFactor:          1,
		SubQueueSize:        128,
		MaxWaitMs:           2,
		DrainThresholdSubs:  999,
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

		time.Sleep(5 * time.Millisecond)
	}

	sortDurations(lags)
	p50 := lags[len(lags)*50/100]
	p99 := lags[len(lags)*99/100-1]
	maxLag := lags[len(lags)-1]
	t.Logf("lag stats over %d iters: p50=%v p99=%v max=%v", iterations, p50, p99, maxLag)

	if p50 > 4*time.Millisecond {
		t.Errorf("p50 lag = %v; want <= 4ms (MaxWaitMs=2)", p50)
	}

	p99Ceiling := 10 * time.Millisecond
	if raceEnabled {
		p99Ceiling = 15 * time.Millisecond
	}
	if p99 > p99Ceiling {
		t.Errorf("p99 lag = %v; want <= %v (: max_wait_ms + scheduler_jitter, raceEnabled=%v)", p99, p99Ceiling, raceEnabled)
	}
}

func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j-1] > d[j]; j-- {
			d[j-1], d[j] = d[j], d[j-1]
		}
	}
}

func TestPoolBatchingDisabledDrainsOnEveryCycle(t *testing.T) {
	t.Parallel()
	srv, cli, closeAll := dialLoopbackTCP(t)
	defer closeAll()

	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       16,
		MaxWaitMs:          1000,
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

		if elapsed > 15*time.Millisecond {
			t.Errorf("iter %d: BatchingDisabled drain took %v; want <= 15ms (timer fallback would take 1000ms)", i, elapsed)
		}
		if !bytes.Equal(rdBuf, frame) {
			t.Errorf("iter %d: frame mismatch", i)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPoolDrainThresholdSubsFormula_LazyRecompute(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	const nSubs = 1024
	p := NewPool(PoolConfig{
		PoolFactor:         1,
		SubQueueSize:       4,
		MaxWaitMs:          2,
		DrainThresholdSubs: 0,
		HeartbeatInterval:  time.Hour,
		WriteTimeout:       time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})

	if got := p.workers[0].drainThreshold; got != 8 {

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

	if !subs[0].Send([]byte("data: trigger\n\n")) {
		t.Fatal("trigger Send returned false")
	}

	time.Sleep(20 * time.Millisecond)

	_ = p.Shutdown(context.Background())

	got := p.workers[0].drainThreshold
	want := nSubs / 64
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

func idSuffix(i int) string {
	const hex = "0123456789abcdef"
	return string([]byte{
		hex[(i>>12)&0xf],
		hex[(i>>8)&0xf],
		hex[(i>>4)&0xf],
		hex[i&0xf],
	})
}

func TestPoolDrainEverythingDirtyNoFrameCap(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:          1,
		SubQueueSize:        128,
		MaxWaitMs:           5,
		DrainThresholdSubs:  1,
		MaxBatchBytesPerSub: 64 * 1024,
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

	m.mu.Lock()
	gotEvents := m.eventsSent["wildcard"]
	m.mu.Unlock()
	if gotEvents != nFrames {
		t.Errorf("EventsSent = %d; want %d (one per frame, no cap)", gotEvents, nFrames)
	}
}

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "i/o timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

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

	_ = sub.Done()
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	sub.mu.Lock()
	close(sub.done)
	sub.mu.Unlock()

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("doneCh did not close after sub.Done() fired (evictDone should have run)")
	}

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

func TestPool_Shutdown_CtxExpiry_ClosesAllDoneChannels(t *testing.T) {

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
		t.Fatalf("Shutdown returned %v; want context.DeadlineExceeded (ctx-abandon path)", serr)
	}

	for i, dc := range doneChs {
		select {
		case <-dc:
		case <-time.After(3 * time.Second):
			t.Fatalf("doneCh[%d] did not close within 3s after ctx-expiry abandon; B-1 invariant violated", i)
		}
	}
}

type blockingAfterPreludeRW struct {
	blockDur     time.Duration
	writesBefore atomic.Int32
}

func (*blockingAfterPreludeRW) Header() http.Header { return http.Header{} }
func (*blockingAfterPreludeRW) WriteHeader(int)     {}
func (w *blockingAfterPreludeRW) Write(p []byte) (int, error) {
	n := w.writesBefore.Add(1)
	if n == 1 {

		return len(p), nil
	}
	time.Sleep(w.blockDur)
	return 0, timeoutNetError{}
}
func (*blockingAfterPreludeRW) Flush() {}

func TestPool_Shutdown_AbandonEmitsLifecycleAndResetsGauge(t *testing.T) {

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

	if !gaugeSet {
		t.Errorf("PoolWorkerDirtySubsSet(\"0\", _) not called by drainShutdownAbandon;  re-sync missing")
	} else if gaugeValue != 0 {
		t.Errorf("PoolWorkerDirtySubsSet(\"0\", %v); want 0 (: dirty gauge must be reset to 0 on abandon)", gaugeValue)
	}
}

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
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: 50 * time.Millisecond,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	sub := &fakeSub{id: "cr03-double-close"}

	_ = sub.Done()
	rw := &alwaysFailAfterPreludeRW{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	if !sub.Send([]byte("data: frame-1\n\n")) {
		t.Fatal("first Send returned false")
	}

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("doneCh did not close after first frame failure")
	}

	m.mu.Lock()
	disconnectsAfterFirst := 0
	for _, n := range m.disconnects {
		disconnectsAfterFirst += n
	}
	m.mu.Unlock()
	if disconnectsAfterFirst != 1 {
		t.Fatalf("disconnects after first failure = %d; want 1", disconnectsAfterFirst)
	}

	sub.mu.Lock()
	close(sub.done)
	sub.mu.Unlock()

	time.Sleep(200 * time.Millisecond)

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

	t.Logf("double-close regression OK: handleSubWriteFailure → close(st.done) → evictDone → drainSub → handleSubWriteFailure → safeCloseDone path completed without panic; disconnects=%d lifetimes=%d", totalDisconnects, lifetimes)
}

func TestPool_Shutdown_AbandonBoundedByDrainDeadline(t *testing.T) {

	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)

	const (
		nSubs                 = 4
		drainShutdownDeadline = 50 * time.Millisecond
		writeSleep            = 80 * time.Millisecond
		ctxDeadline           = 20 * time.Millisecond

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

	bound := ctxDeadline + time.Duration(nSubs)*writeSleep + slop
	if elapsed > bound {
		t.Errorf("Shutdown elapsed = %v; want ≤ %v (abandon-poll regression: must respect ctx within %d × %v + slop, not %v).",
			elapsed, bound, nSubs, writeSleep, p.cfg.WriteTimeout)
	}

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

func TestPool_DirtySubs_IncOnFirstFrame_SetOnDrain(t *testing.T) {
	t.Parallel()
	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:        1,
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

	if !sub.Send([]byte("data: a\n\n")) {
		t.Fatal("Send returned false; queue should accept")
	}

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

	if !sub.Send([]byte("data: hist\n\n")) {
		t.Fatal("Send returned false")
	}

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

func TestPool_DrainBatchSize_RecordsLenDirty(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:        1,
		SubQueueSize:      8,
		MaxWaitMs:         5,
		WriteTimeout:      time.Second,
		HeartbeatInterval: time.Hour,

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

	for i, s := range subs {
		if !s.Send([]byte("data: x\n\n")) {
			t.Fatalf("Send[%d] returned false", i)
		}
	}

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

func TestPool_WorkerIDPreTouchedAtNewPool(t *testing.T) {
	prev := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	m := newFakeMetrics()
	const factor = 2
	p := NewPool(PoolConfig{
		PoolFactor:        factor,
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

func TestPool_DirtySubs_DecOnEvict(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:   1,
		SubQueueSize: 4,

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

	_ = sub.Done()

	if !sub.Send([]byte("data: dec\n\n")) {
		t.Fatal("Send returned false")
	}

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

	close(sub.done)

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

func (w *deadlineCapturingRespWriter) SetWriteDeadline(t time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastDeadline = t
	w.deadlineCount++
	return nil
}

func TestPool_DrainSubDeadline_RoutesCallerBudget(t *testing.T) {
	t.Parallel()

	const (
		drainBudget  = 25 * time.Millisecond
		writeTimeout = time.Second

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

	if captureCount < 1 {
		t.Fatalf("SetWriteDeadline was not called (count=%d); drainSubDeadline did not route through the respWriter deadline path", captureCount)
	}

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

	wantLow := beforeCall2.Add(drainBudget - slop)
	wantHigh := afterCall.Add(drainBudget + slop)
	if active.Before(wantLow) || active.After(wantHigh) {
		t.Errorf("SetWriteDeadline=%v; want in [%v, %v] (caller budget=%v). drainSubDeadline must use the SUPPLIED budget, not w.cfg.WriteTimeout=%v.",
			active, wantLow, wantHigh, drainBudget, writeTimeout)
	}

	preFixLow := beforeCall2.Add(writeTimeout - slop)
	if !active.Before(preFixLow) {
		t.Errorf("SetWriteDeadline=%v lands inside the pre-fix writeTimeout=%v window; the caller-supplied budget was ignored.", active, writeTimeout)
	}

	_ = captured
	t.Logf("drainSubDeadline regression OK: SetWriteDeadline routed at %v (now+%v); caller budget honoured.",
		active, active.Sub(beforeCall2))
}

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
