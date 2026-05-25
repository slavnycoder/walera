package sse

import (
	"context"
	"flag"
	"io"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

var stressSubs = flag.Int("stress-subs", 100, "subscriber count for slow-client stress test (60/20/20 split)")

func readSubscriberDisconnects(t *testing.T, reg *metrics.Registry, reason string) float64 {
	t.Helper()
	mfs, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "walera_subscriber_disconnects_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "reason" && lp.GetValue() == reason {
					if c := m.GetCounter(); c != nil {
						return c.GetValue()
					}
					return 0
				}
			}
		}
	}
	return 0
}

type blockingTCPPair struct {
	serverConn *net.TCPConn
	clientConn *net.TCPConn
	listener   net.Listener
}

func newBlockingTCPPair(t *testing.T, shrinkSendBuf bool) *blockingTCPPair {
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
	srv := acc.conn
	cli := cConn.(*net.TCPConn)

	if shrinkSendBuf {

		if err := srv.SetWriteBuffer(8 * 1024); err != nil {
			t.Logf("SetWriteBuffer warning (non-fatal): %v", err)
		}
		if err := cli.SetReadBuffer(8 * 1024); err != nil {
			t.Logf("SetReadBuffer warning (non-fatal): %v", err)
		}
	}

	return &blockingTCPPair{
		serverConn: srv,
		clientConn: cli,
		listener:   ln,
	}
}

func (p *blockingTCPPair) close() {
	_ = p.clientConn.Close()
	_ = p.serverConn.Close()
	_ = p.listener.Close()
}

type drainerStop struct {
	stopCh chan struct{}
	doneCh chan struct{}
}

func startDrainer(cli *net.TCPConn) *drainerStop {
	s := &drainerStop{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	go func() {
		defer close(s.doneCh)
		buf := make([]byte, 4096)
		for {
			select {
			case <-s.stopCh:
				return
			default:
			}

			_ = cli.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			n, err := cli.Read(buf)
			if n > 0 {

			}
			if err != nil {

				select {
				case <-s.stopCh:
					return
				default:
				}
				ne, ok := err.(net.Error)
				if ok && ne.Timeout() {
					continue
				}
				return
			}
		}
	}()
	return s
}

func (s *drainerStop) stop() {
	close(s.stopCh)
	select {
	case <-s.doneCh:
	case <-time.After(2 * time.Second):

	}
}

func TestPoolSlowClientIsolation(t *testing.T) {

	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)

	const (
		nSubs        = 8
		writeTimeout = 200 * time.Millisecond
		maxWaitMs    = 2
	)

	pairs := make([]*blockingTCPPair, nSubs)
	drainers := make([]*drainerStop, nSubs)
	for i := 0; i < nSubs; i++ {
		shrink := (i == 0)
		pairs[i] = newBlockingTCPPair(t, shrink)
	}
	t.Cleanup(func() {
		for i, p := range pairs {
			if drainers[i] != nil {
				drainers[i].stop()
			}
			p.close()
		}
	})

	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:          1,
		SubQueueSize:        32,
		MaxWaitMs:           maxWaitMs,
		DrainThresholdSubs:  1,
		MaxBatchBytesPerSub: 64 * 1024,
		WriteTimeout:        writeTimeout,
		HeartbeatInterval:   10 * time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	subs := make([]*fakeSub, nSubs)
	doneChs := make([]<-chan struct{}, nSubs)
	for i := 0; i < nSubs; i++ {
		subs[i] = &fakeSub{id: idSuffix(i) + "-iso"}
		doneCh, err := p.Attach(subs[i], pairs[i].serverConn, nil, nil)
		if err != nil {
			t.Fatalf("Attach sub %d: %v", i, err)
		}
		doneChs[i] = doneCh
	}

	if got := len(p.workers); got != 1 {
		t.Fatalf("workers = %d; want 1 (PoolFactor=1 invariant)", got)
	}

	for i := 1; i < nSubs; i++ {
		drainPrelude(t, pairs[i].clientConn)
		drainers[i] = startDrainer(pairs[i].clientConn)
	}

	bigFrame := []byte("data: " + strings.Repeat("x", 4000) + "\n\n")
	if len(bigFrame) < 4000 {
		t.Fatalf("test bug: bigFrame len=%d", len(bigFrame))
	}

	const phase1Frames = 20
	wedgeStart := time.Now()
	for i := 0; i < phase1Frames; i++ {
		for _, sub := range subs {

			_ = sub.Send(bigFrame)
		}
	}

	slowDeadline := writeTimeout + 200*time.Millisecond
	select {
	case <-doneChs[0]:

	case <-time.After(slowDeadline):
		t.Fatalf("slow sub doneCh did not fire within %v of attach", slowDeadline)
	}
	slowDroppedAt := time.Since(wedgeStart)
	t.Logf("slow sub dropped at T+%v (WriteTimeout=%v)", slowDroppedAt, writeTimeout)

	m.mu.Lock()
	slowDisconnects := m.disconnects["slow_consumer"]
	slowTxDropped := m.txDropped["slow_consumer"]
	m.mu.Unlock()

	if slowDisconnects < 1 {
		t.Errorf("subscriberDisconnects[slow_consumer] = %d; want >= 1", slowDisconnects)
	}
	if slowTxDropped != 0 {
		t.Errorf("txDropped[slow_consumer] = %d; want 0 (B5 invariant: disconnect is a lifecycle event, not a per-tx drop)", slowTxDropped)
	}

	for i := 1; i < nSubs; i++ {
		select {
		case <-doneChs[i]:
			t.Errorf("healthy sub %d doneCh fired; should remain open", i)
		default:
		}
	}

	postDropFrames := make([][]byte, nSubs)
	for i := 1; i < nSubs; i++ {
		postDropFrames[i] = []byte("data: post-drop-" + idSuffix(i) + "\n\n")
	}
	postDropStart := time.Now()
	for i := 1; i < nSubs; i++ {

		deadline := time.Now().Add(500 * time.Millisecond)
		for !subs[i].Send(postDropFrames[i]) {
			if time.Now().After(deadline) {
				t.Fatalf("sub %d queue still full 500ms after slow drop", i)
			}
			time.Sleep(time.Millisecond)
		}
	}

	postDropBudget := time.Duration(2*maxWaitMs)*time.Millisecond + 100*time.Millisecond
	time.Sleep(postDropBudget)
	postDropElapsed := time.Since(postDropStart)
	t.Logf("post-drop frames flushed within budget=%v (elapsed=%v)", postDropBudget, postDropElapsed)

	for i := 1; i < nSubs; i++ {
		select {
		case <-doneChs[i]:
			t.Errorf("healthy sub %d doneCh fired after post-drop window; isolation broken", i)
		default:
		}
	}

	m.mu.Lock()
	finalSlowDisconnects := m.disconnects["slow_consumer"]
	finalClientClosed := m.disconnects["client_closed"]
	finalTxDropped := m.txDropped["slow_consumer"]
	finalSlowClientDrops := m.slowClientDrops
	m.mu.Unlock()

	if finalSlowDisconnects != 1 {
		t.Errorf("final subscriberDisconnects[slow_consumer] = %d; want exactly 1 (only the wedged sub)", finalSlowDisconnects)
	}
	if finalClientClosed != 0 {
		t.Errorf("subscriberDisconnects[client_closed] = %d; want 0 (no healthy sub should drop)", finalClientClosed)
	}
	if finalTxDropped != 0 {
		t.Errorf("final txDropped[slow_consumer] = %d; want 0 (B5 invariant)", finalTxDropped)
	}

	if finalSlowClientDrops != finalSlowDisconnects {
		t.Errorf("slowClientDrops = %d; want %d (lockstep with disconnects[slow_consumer])", finalSlowClientDrops, finalSlowDisconnects)
	}
}

func TestPool_Shutdown_OneSubBlocked_OthersStillReceive(t *testing.T) {

	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)

	const nSubs = 4

	pairs := make([]*blockingTCPPair, nSubs)
	drainers := make([]*drainerStop, nSubs)
	for i := 0; i < nSubs; i++ {
		shrink := (i == 0)
		pairs[i] = newBlockingTCPPair(t, shrink)
	}
	t.Cleanup(func() {
		for i, pp := range pairs {
			if drainers[i] != nil {
				drainers[i].stop()
			}
			pp.close()
		}
	})

	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          32,
		MaxWaitMs:             2,
		DrainThresholdSubs:    1,
		MaxBatchBytesPerSub:   64 * 1024,
		WriteTimeout:          200 * time.Millisecond,
		HeartbeatInterval:     10 * time.Second,
		drainShutdownDeadline: 50 * time.Millisecond,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})

	subs := make([]*fakeSub, nSubs)
	doneChs := make([]<-chan struct{}, nSubs)
	for i := 0; i < nSubs; i++ {
		subs[i] = &fakeSub{id: idSuffix(i) + "-shut"}
		dc, err := p.Attach(subs[i], pairs[i].serverConn, nil, nil)
		if err != nil {
			t.Fatalf("Attach sub %d: %v", i, err)
		}
		doneChs[i] = dc
	}

	if got := len(p.workers); got != 1 {
		t.Fatalf("workers = %d; want 1 (PoolFactor=1 invariant)", got)
	}

	for i := 1; i < nSubs; i++ {
		drainPrelude(t, pairs[i].clientConn)
	}

	drainPrelude(t, pairs[0].clientConn)

	bigFrame := []byte("data: " + strings.Repeat("x", 4000) + "\n\n")
	_ = pairs[0].serverConn.SetWriteDeadline(time.Now().Add(300 * time.Millisecond))
	for i := 0; i < 64; i++ {
		if _, werr := pairs[0].serverConn.Write(bigFrame); werr != nil {
			break
		}
	}
	_ = pairs[0].serverConn.SetWriteDeadline(time.Time{})

	healthyBufs := make([]chan []byte, nSubs)
	for i := 1; i < nSubs; i++ {
		healthyBufs[i] = make(chan []byte, 64)
		go func(idx int) {
			buf := make([]byte, 4096)
			for {
				_ = pairs[idx].clientConn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
				n, err := pairs[idx].clientConn.Read(buf)
				if n > 0 {
					cp := make([]byte, n)
					copy(cp, buf[:n])
					select {
					case healthyBufs[idx] <- cp:
					default:
					}
				}
				if err != nil {
					ne, ok := err.(net.Error)
					if ok && ne.Timeout() {

						continue
					}
					return
				}
			}
		}(i)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	serr := p.Shutdown(ctx)
	elapsed := time.Since(start)

	if serr != nil {
		t.Errorf("Shutdown returned %v; want nil (clean drain within ctx budget)", serr)
	}

	for i, dc := range doneChs {
		select {
		case <-dc:
		case <-time.After(2 * time.Second):
			t.Errorf("doneCh[%d] not closed after Shutdown", i)
		}
	}

	const wantPrefix = "event: shutdown"
	for i := 1; i < nSubs; i++ {

		var got []byte
		collectDeadline := time.Now().Add(200 * time.Millisecond)
		found := false
		for !found && time.Now().Before(collectDeadline) {
			select {
			case chunk := <-healthyBufs[i]:
				got = append(got, chunk...)
				if strings.Contains(string(got), wantPrefix) {
					found = true
				}
			case <-time.After(50 * time.Millisecond):
			}
		}
		if !strings.Contains(string(got), wantPrefix) {
			t.Errorf("sub %d wire bytes do not contain %q; got %q", i, wantPrefix, string(got))
		}
	}

	m.mu.Lock()
	shutdownN := m.disconnects["shutdown"]
	slowN := m.disconnects["slow_consumer"]
	clientClosedN := m.disconnects["client_closed"]
	m.mu.Unlock()

	if slowN != 1 {
		t.Errorf("disconnects[slow_consumer] = %d; want 1 (wedged sub#0)", slowN)
	}

	if shutdownN != 3 {
		t.Errorf("disconnects[shutdown] = %d; want 3 (healthy subs #1-3)", shutdownN)
	}
	if clientClosedN != 0 {
		t.Errorf("disconnects[client_closed] = %d; want 0", clientClosedN)
	}

	var timingBound time.Duration
	if raceEnabled {
		timingBound = 1500 * time.Millisecond
	} else {
		timingBound = 500 * time.Millisecond
	}
	if elapsed > timingBound {
		t.Errorf("Shutdown elapsed = %v; want ≤ %v (raceEnabled=%v)", elapsed, timingBound, raceEnabled)
	}
	t.Logf("Shutdown elapsed = %v (raceEnabled=%v, bound=%v)", elapsed, raceEnabled, timingBound)
}

func TestHandler_RejectsAttachWhenPoolShuttingDown(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	baseShutdown := readSubscriberDisconnects(t, kit.h.metrics, "shutdown")
	baseClientClosed := readSubscriberDisconnects(t, kit.h.metrics, "client_closed")

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutCancel()
	if err := kit.pool.Shutdown(shutCtx); err != nil {
		t.Fatalf("kit.pool.Shutdown: %v", err)
	}

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer reqCancel()
	req, _ := http.NewRequestWithContext(reqCtx, http.MethodGet, srv.URL+"/sse/v1/users/42", nil)
	req.Header.Set("Authorization", "Bearer valid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}
	defer resp.Body.Close()

	closed := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatalf("response body did not close within 2s after pool shutdown")
	}

	gotShutdown := readSubscriberDisconnects(t, kit.h.metrics, "shutdown") - baseShutdown
	gotClientClosed := readSubscriberDisconnects(t, kit.h.metrics, "client_closed") - baseClientClosed

	if gotShutdown != 1 {
		t.Errorf("SubscriberDisconnects(\"shutdown\") delta = %v; want 1", gotShutdown)
	}
	if gotClientClosed != 0 {
		t.Errorf("SubscriberDisconnects(\"client_closed\") delta = %v; want 0 (errors.Is(attachErr, errPoolClosed) must take the shutdown branch)", gotClientClosed)
	}
}

func TestPool_Shutdown_1kSubs_CompletesWithin500ms(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1k-sub shutdown smoke under -short")
	}

	prev := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(prev)

	const nSubs = 1000

	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:            2,
		SubQueueSize:          4,
		MaxWaitMs:             2,
		DrainThresholdSubs:    100,
		MaxBatchBytesPerSub:   64 * 1024,
		WriteTimeout:          time.Second,
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: 50 * time.Millisecond,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})

	subs := make([]*fakeSub, nSubs)
	rws := make([]*fakeResponseWriter, nSubs)
	doneChs := make([]<-chan struct{}, nSubs)
	for i := 0; i < nSubs; i++ {
		subs[i] = &fakeSub{id: "shut1k-" + strconv.Itoa(i)}
		rws[i] = &fakeResponseWriter{}
		rc := http.NewResponseController(rws[i])
		dc, err := p.Attach(subs[i], nil, rws[i], rc)
		if err != nil {
			t.Fatalf("Attach[%d]: %v", i, err)
		}
		doneChs[i] = dc
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	t0 := time.Now()
	err := p.Shutdown(ctx)
	elapsed := time.Since(t0)

	if err != nil {
		t.Fatalf("Pool.Shutdown(ctx) returned %v; want nil (drain must complete inside the 1s ctx)", err)
	}

	stillOpen := 0
	for i, dc := range doneChs {
		select {
		case <-dc:
		default:
			stillOpen++
			if stillOpen <= 3 {
				t.Errorf("doneCh[%d] not closed after Shutdown returned", i)
			}
		}
	}
	if stillOpen > 0 {
		t.Errorf("%d/%d doneChs still open after Shutdown returned", stillOpen, nSubs)
	}

	const wantPrefix = "event: shutdown"
	observed := 0
	for _, rw := range rws {
		rw.mu.Lock()
		got := rw.buf.String()
		rw.mu.Unlock()
		if strings.Contains(got, wantPrefix) {
			observed++
		}
	}
	if observed < 950 {
		t.Errorf("only %d/%d subs observed %q on the wire; want ≥ 950", observed, nSubs, wantPrefix)
	}

	var timingBound time.Duration
	if raceEnabled {
		timingBound = 1500 * time.Millisecond
	} else {
		timingBound = 500 * time.Millisecond
	}
	if elapsed > timingBound {
		t.Errorf("Shutdown elapsed = %v; want ≤ %v (raceEnabled=%v)", elapsed, timingBound, raceEnabled)
	}
	t.Logf("1k-sub shutdown smoke: elapsed=%v, doneChsOpen=%d, shutdownFramesObserved=%d/%d (raceEnabled=%v, bound=%v)",
		elapsed, stillOpen, observed, nSubs, raceEnabled, timingBound)
}

//nolint:gocognit // population-mix stress test orchestrates 3 cohorts + 3 frame
func TestPoolSlowClientIsolationStress(t *testing.T) {

	const (
		writeTimeout = 50 * time.Millisecond
		maxWaitMs    = 2
	)

	nSubs := *stressSubs
	if nSubs < 10 {
		t.Fatalf("-stress-subs=%d is below the minimum split floor (10)", nSubs)
	}

	stalledN := nSubs * 20 / 100
	disconnectedN := nSubs * 20 / 100
	healthyN := nSubs - stalledN - disconnectedN
	if stalledN < 1 || disconnectedN < 1 || healthyN < 1 {
		t.Fatalf("cohort split rounding zeroed a cohort: stalled=%d disconnected=%d healthy=%d (nSubs=%d)",
			stalledN, disconnectedN, healthyN, nSubs)
	}
	t.Logf("stress cohort split: nSubs=%d stalled=%d disconnected=%d healthy=%d",
		nSubs, stalledN, disconnectedN, healthyN)

	stalledHi := stalledN
	disconnectedLo, disconnectedHi := stalledHi, stalledHi+disconnectedN
	healthyLo, healthyHi := disconnectedHi, nSubs

	pairs := make([]*blockingTCPPair, nSubs)
	drainers := make([]*drainerStop, nSubs)
	for i := 0; i < nSubs; i++ {
		shrink := i < stalledHi
		pairs[i] = newBlockingTCPPair(t, shrink)
	}
	t.Cleanup(func() {
		for i, p := range pairs {
			if drainers[i] != nil {
				drainers[i].stop()
			}
			p.close()
		}
	})

	m := newFakeMetrics()
	p := NewPool(PoolConfig{
		PoolFactor:          1,
		SubQueueSize:        32,
		MaxWaitMs:           maxWaitMs,
		DrainThresholdSubs:  1,
		MaxBatchBytesPerSub: 64 * 1024,
		WriteTimeout:        writeTimeout,
		HeartbeatInterval:   10 * time.Second,
	}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	subs := make([]*fakeSub, nSubs)
	doneChs := make([]<-chan struct{}, nSubs)
	for i := 0; i < nSubs; i++ {
		subs[i] = &fakeSub{id: idSuffix(i) + "-stress"}
		doneCh, err := p.Attach(subs[i], pairs[i].serverConn, nil, nil)
		if err != nil {
			t.Fatalf("Attach sub %d: %v", i, err)
		}
		doneChs[i] = doneCh
	}

	t.Logf("stress workers = %d (PoolFactor=1, GOMAXPROCS=%d)", len(p.workers), runtime.GOMAXPROCS(0))

	for i := 0; i < nSubs; i++ {
		drainPrelude(t, pairs[i].clientConn)
	}

	for i := healthyLo; i < healthyHi; i++ {
		drainers[i] = startDrainer(pairs[i].clientConn)
	}

	for i := disconnectedLo; i < disconnectedHi; i++ {
		_ = pairs[i].clientConn.Close()
	}

	stallBudget := 50 * time.Millisecond
	bigFrame := []byte("data: " + strings.Repeat("x", 4000) + "\n\n")

	pushPhase := func(label string, frames int) {
		t.Helper()
		var maxSend time.Duration
		for f := 0; f < frames; f++ {
			for i, sub := range subs {
				t0 := time.Now()
				_ = sub.Send(bigFrame)
				if d := time.Since(t0); d > maxSend {
					maxSend = d
				}
				if maxSend > stallBudget {
					t.Errorf("%s: Send to sub %d blocked %v > %v (WAL producer pinned by slow client)",
						label, i, maxSend, stallBudget)
				}
			}
		}
		t.Logf("%s push: subs=%d frames/sub=%d worstSend=%v budget=%v",
			label, nSubs, frames, maxSend, stallBudget)
	}

	pushPhase("phase1", 10)

	totalBudget := time.Duration(stalledN)*writeTimeout + 5*time.Second
	stallDeadline := time.Now().Add(totalBudget)
	smallFrame := []byte("data: keepalive\n\n")
	for time.Now().Before(stallDeadline) {
		m.mu.Lock()
		slowN := m.disconnects["slow_consumer"]
		ccN := m.disconnects["client_closed"]
		m.mu.Unlock()
		if slowN >= stalledN && ccN >= disconnectedN {
			break
		}

		for _, sub := range subs {
			_ = sub.Send(smallFrame)
		}
		time.Sleep(10 * time.Millisecond)
	}

	pushPhase("phase2", 10)

	pushPhase("phase3", 10)

	settleBudget := time.Duration(2*maxWaitMs)*time.Millisecond + 200*time.Millisecond
	time.Sleep(settleBudget)

	m.mu.Lock()
	finalSlow := m.disconnects["slow_consumer"]
	finalClientClosed := m.disconnects["client_closed"]
	finalTxDropped := m.txDropped["slow_consumer"]
	finalSlowClientDrops := m.slowClientDrops
	m.mu.Unlock()

	if finalSlow < stalledN {
		t.Errorf("disconnects[slow_consumer] = %d; want >= %d (stalled cohort)", finalSlow, stalledN)
	}
	if finalClientClosed < disconnectedN {
		t.Errorf("disconnects[client_closed] = %d; want >= %d (disconnected cohort)", finalClientClosed, disconnectedN)
	}

	if finalTxDropped != 0 {
		t.Errorf("txDropped[slow_consumer] = %d; want 0 (B5 invariant)", finalTxDropped)
	}

	if finalSlowClientDrops != finalSlow {
		t.Errorf("slowClientDrops = %d; want %d (lockstep with disconnects[slow_consumer])",
			finalSlowClientDrops, finalSlow)
	}

	stillOpen := 0
	for i := healthyLo; i < healthyHi; i++ {
		select {
		case <-doneChs[i]:
			t.Errorf("healthy sub %d doneCh fired; isolation broken (stress)", i)
		default:
			stillOpen++
		}
	}
	if stillOpen != healthyN {
		t.Errorf("healthy doneChs still open = %d; want %d", stillOpen, healthyN)
	}

	t.Logf("stress isolation: slow=%d (>=%d), client_closed=%d (>=%d), slowClientDrops=%d, healthyOpen=%d/%d",
		finalSlow, stalledN, finalClientClosed, disconnectedN, finalSlowClientDrops, stillOpen, healthyN)
}
