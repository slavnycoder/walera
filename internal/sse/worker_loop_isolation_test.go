package sse

import (
	"context"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestPoolWorkerLoopStarvation_AttachAndShutdown(t *testing.T) {

	prev := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(prev)

	const (
		maxWaitMs       = 2
		writeTimeout    = 200 * time.Millisecond
		drainShutdownMS = 50 * time.Millisecond
		warmup          = 20 * time.Millisecond

		drainerPace = 50 * time.Microsecond
	)

	makePool := func() (*WriterPool, *fakeMetrics) {
		m := newFakeMetrics()
		p := NewPool(PoolConfig{
			PoolFactor:            1,
			SubQueueSize:          32,
			MaxWaitMs:             maxWaitMs,
			DrainThresholdSubs:    1,
			MaxBatchBytesPerSub:   64 * 1024,
			WriteTimeout:          writeTimeout,
			HeartbeatInterval:     10 * time.Second,
			drainShutdownDeadline: drainShutdownMS,
		}, PoolDeps{Encoder: fakeEncoder{}, Metrics: m, Logger: zerolog.Nop()})
		return p, m
	}

	bigFrame := []byte("data: " + strings.Repeat("x", 4000) + "\n\n")

	startInflow := func(t *testing.T, noisy *fakeSub) (stop func()) {
		t.Helper()
		stopCh := make(chan struct{})
		var once sync.Once
		go func() {
			for {
				select {
				case <-stopCh:
					return
				default:
					_ = noisy.Send(bigFrame)
				}
			}
		}()
		return func() {
			once.Do(func() { close(stopCh) })
		}
	}

	startPacedDrainer := func(cli *net.TCPConn, pace time.Duration) (stop func()) {
		stopCh := make(chan struct{})
		var once sync.Once
		go func() {
			buf := make([]byte, 4096)
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				_ = cli.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
				_, err := cli.Read(buf)
				if err != nil {
					ne, ok := err.(net.Error)
					if ok && ne.Timeout() {

						continue
					}

					return
				}

				if pace > 0 {
					time.Sleep(pace)
				}
			}
		}()
		return func() {
			once.Do(func() { close(stopCh) })
		}
	}

	findSiblingIDOnSameWorker := func(t *testing.T, p *WriterPool, noisyID string) string {
		t.Helper()
		target := p.pickWorker(noisyID)
		for i := 1; i < 1024; i++ {
			candidate := idSuffix(i) + "-sibling"
			if p.pickWorker(candidate) == target {
				return candidate
			}
		}
		t.Fatalf("could not find sibling ID hashing to noisy's worker after 1024 attempts")
		return ""
	}

	attachBudget := 8 * time.Duration(maxWaitMs) * time.Millisecond
	if attachBudget < 250*time.Millisecond {
		attachBudget = 250 * time.Millisecond
	}

	t.Run("AttachUnderInflow", func(t *testing.T) {
		noisyPair := newBlockingTCPPair(t, false)
		t.Cleanup(noisyPair.close)

		p, _ := makePool()
		var shutdownOnce sync.Once
		doShutdown := func() {
			shutdownOnce.Do(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = p.Shutdown(ctx)
			})
		}
		t.Cleanup(doShutdown)

		if got := len(p.workers); got != 2 {
			t.Fatalf("workers = %d; want 2 (GOMAXPROCS(2)+PoolFactor=1 invariant)", got)
		}

		noisy := &fakeSub{id: idSuffix(0) + "-noisy"}
		if _, err := p.Attach(noisy, noisyPair.serverConn, nil, nil); err != nil {
			t.Fatalf("Attach noisy: %v", err)
		}
		drainPrelude(t, noisyPair.clientConn)
		noisyStop := startPacedDrainer(noisyPair.clientConn, drainerPace)
		t.Cleanup(noisyStop)

		stopInflow := startInflow(t, noisy)
		t.Cleanup(stopInflow)

		time.Sleep(warmup)

		siblingID := findSiblingIDOnSameWorker(t, p, noisy.ID())

		siblingPair := newBlockingTCPPair(t, false)
		t.Cleanup(siblingPair.close)
		sibling := &fakeSub{id: siblingID}

		type attachResult struct {
			doneCh <-chan struct{}
			err    error
		}
		attachCh := make(chan attachResult, 1)
		start := time.Now()
		go func() {
			dc, err := p.Attach(sibling, siblingPair.serverConn, nil, nil)
			attachCh <- attachResult{dc, err}
		}()

		var elapsed time.Duration
		var attached bool
		select {
		case res := <-attachCh:
			elapsed = time.Since(start)
			if res.err != nil {
				t.Fatalf("Attach sibling: %v", res.err)
			}
			attached = true
		case <-time.After(attachBudget):
			elapsed = time.Since(start)
		}

		t.Logf("AttachUnderInflow: measured attach wall-clock=%v budget=%v attached=%v", elapsed, attachBudget, attached)
		if !attached || elapsed > attachBudget {
			t.Errorf("Attach under sustained inflow took %v; want <= %v (: worker starves attachCh while pollAllQueues keeps returning > 0)", elapsed, attachBudget)
		}

		stopInflow()
		noisyStop()
		doShutdown()
	})

	t.Run("ShutdownUnderInflow", func(t *testing.T) {
		noisyPair := newBlockingTCPPair(t, false)
		t.Cleanup(noisyPair.close)

		p, _ := makePool()

		if got := len(p.workers); got != 2 {
			t.Fatalf("workers = %d; want 2 (GOMAXPROCS(2)+PoolFactor=1 invariant)", got)
		}

		noisy := &fakeSub{id: idSuffix(0) + "-noisy"}
		if _, err := p.Attach(noisy, noisyPair.serverConn, nil, nil); err != nil {
			t.Fatalf("Attach noisy: %v", err)
		}
		drainPrelude(t, noisyPair.clientConn)
		noisyStop := startPacedDrainer(noisyPair.clientConn, drainerPace)
		stopInflow := startInflow(t, noisy)

		time.Sleep(warmup)

		shutdownBudget := time.Duration(len(p.workers))*drainShutdownMS + 200*time.Millisecond

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		shutdownDone := make(chan error, 1)
		start := time.Now()
		go func() {
			shutdownDone <- p.Shutdown(ctx)
		}()

		var elapsed time.Duration
		var err error
		var returnedInBudget bool
		select {
		case err = <-shutdownDone:
			elapsed = time.Since(start)
			returnedInBudget = true
		case <-time.After(shutdownBudget):
			elapsed = time.Since(start)
			returnedInBudget = false
		}

		t.Logf("ShutdownUnderInflow: measured shutdown wall-clock=%v budget=%v err=%v returnedInBudget=%v", elapsed, shutdownBudget, err, returnedInBudget)
		if !returnedInBudget || elapsed > shutdownBudget {
			t.Errorf("Shutdown under sustained inflow took %v; want <= %v (: worker starves shutdownCh while pollAllQueues keeps returning > 0; budget = workers(%d) * drainShutdownDeadline(%v) + 200ms slop)", elapsed, shutdownBudget, len(p.workers), drainShutdownMS)
		}

		stopInflow()

		if !returnedInBudget {

			select {
			case <-shutdownDone:
			case <-time.After(3 * time.Second):
				noisyPair.close()
				select {
				case <-shutdownDone:
				case <-time.After(3 * time.Second):
					t.Errorf("Shutdown goroutine did not return within 6s of inflow stop; test cleanup may leak")
				}
			}
		}
		noisyStop()
	})
}
