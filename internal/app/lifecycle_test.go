package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/sse"
	"github.com/walera/walera/internal/wal"
)

func init() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newTestApp(
	t *testing.T,
	logSink *syncBuffer,
	shutdownDeadline, drainDeadline time.Duration,
	withPProf bool,
) *App {
	t.Helper()

	reg := metrics.New()
	enc := sse.NewEncoder(64 * 1024)
	poolDeps := sse.PoolDeps{
		Encoder: enc,
		Metrics: sse.NewPoolMetricsAdapter(reg),
		Logger:  zerolog.Nop(),
	}

	pool := sse.NewPool(sse.PoolConfig{PoolFactor: 1}, poolDeps)

	bcast := router.New(router.Config{}, router.Deps{
		Logger:  zerolog.Nop(),
		Metrics: reg,
		Encoder: enc,
	})

	httpSrv := &http.Server{Addr: "127.0.0.1:0"}

	var pprofSrv *http.Server
	if withPProf {
		pprofSrv = &http.Server{Addr: "127.0.0.1:0"}
	}

	logger := zerolog.New(logSink).With().Timestamp().Logger()

	cfg := &AppConfig{}
	cfg.Shutdown.Deadline = ShutdownDeadline(shutdownDeadline)
	cfg.Shutdown.DrainDeadline = DrainDeadline(drainDeadline)

	return &App{
		Config:      cfg,
		Logger:      logger,
		Metrics:     reg,
		SSEPool:     pool,
		RouterIndex: bcast,
		HTTPServer:  httpSrv,
		PProfServer: pprofSrv,
	}
}

func TestApp_ShutdownConcurrentWave(t *testing.T) {
	t.Parallel()

	logSink := &syncBuffer{}
	a := newTestApp(t, logSink, 5*time.Second, 1*time.Second, false)

	var (
		poolStart, httpStart, bcastStart atomic.Int64
	)
	now := func() int64 { return time.Now().UnixNano() }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a.shutdownStep1WaveWithCallbacks(
		ctx,
		1*time.Second,
		func() { poolStart.Store(now()) },
		func() { httpStart.Store(now()) },
		func() { bcastStart.Store(now()) },
		nil,
	)

	starts := []int64{poolStart.Load(), httpStart.Load(), bcastStart.Load()}
	for i, v := range starts {
		if v == 0 {
			t.Fatalf("start callback %d did not fire", i)
		}
	}

	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	spread := time.Duration(starts[len(starts)-1] - starts[0])

	if spread >= 10*time.Millisecond {
		t.Fatalf("Step-1 wave start spread = %v (want < 10ms); the three arms did not run in parallel", spread)
	}
}

func TestApp_ShutdownStepOrdering(t *testing.T) {
	t.Parallel()

	logSink := &syncBuffer{}
	a := newTestApp(t, logSink, 5*time.Second, 1*time.Second, false)

	var stopCalledAt atomic.Int64
	stop := func() { stopCalledAt.Store(time.Now().UnixNano()) }

	txCh := make(chan wal.Tx)
	close(txCh)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := a.Shutdown(ctx, stop, txCh); err != nil {
		t.Fatalf("Shutdown returned non-nil error: %v", err)
	}

	if stopCalledAt.Load() == 0 {
		t.Fatal("Shutdown did not invoke stop()")
	}

	type entry struct {
		Time time.Time
		Step string
		Msg  string
	}
	var entries []entry
	for _, line := range bytes.Split([]byte(logSink.String()), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var raw struct {
			Time    time.Time `json:"time"`
			Step    string    `json:"step"`
			Message string    `json:"message"`
		}
		if err := json.Unmarshal(line, &raw); err != nil {
			t.Fatalf("failed to parse log line %q: %v", line, err)
		}
		entries = append(entries, entry{Time: raw.Time, Step: raw.Step, Msg: raw.Message})
	}

	var (
		poolAt, httpAt, bcastAt time.Time
		finalAt                 time.Time
		havePool, haveHTTP      bool
		haveBcast, haveFinal    bool
	)
	for _, e := range entries {
		switch {
		case e.Step == "pool":
			poolAt = e.Time
			havePool = true
		case e.Step == "http":
			httpAt = e.Time
			haveHTTP = true
		case e.Step == "broadcast":
			bcastAt = e.Time
			haveBcast = true
		case e.Msg == "shutdown complete":
			finalAt = e.Time
			haveFinal = true
		}
	}

	if !havePool {
		t.Fatal("missing Step-1 pool log line")
	}
	if !haveHTTP {
		t.Fatal("missing Step-1 http log line")
	}
	if !haveBcast {
		t.Fatal("missing Step-1 broadcast log line")
	}
	if !haveFinal {
		t.Fatal("missing final 'shutdown complete' log line")
	}

	stopAt := time.Unix(0, stopCalledAt.Load())
	for _, p := range []struct {
		name string
		at   time.Time
	}{
		{"pool", poolAt},
		{"http", httpAt},
		{"broadcast", bcastAt},
	} {
		if !p.at.Before(stopAt) {
			t.Errorf("Step-1 %s log (%s) did not precede stop() (%s)", p.name, p.at, stopAt)
		}
	}

	if !stopAt.Before(finalAt) {
		t.Errorf("stop() (%s) did not precede final 'shutdown complete' log (%s)", stopAt, finalAt)
	}
	flushGap := finalAt.Sub(stopAt)
	if flushGap < 40*time.Millisecond {

		t.Errorf("flush gap stop→final = %v (want ≥ 40ms — Step 4's 50ms sleep should dominate)", flushGap)
	}
}
