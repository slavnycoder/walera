package sse

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func benchPoolConfig() PoolConfig {
	return PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          64,
		MaxWaitMs:             2,
		DrainThresholdSubs:    1,
		MaxBatchBytesPerSub:   64 * 1024,
		WriteTimeout:          200 * time.Millisecond,
		HeartbeatInterval:     time.Hour,
		drainShutdownDeadline: 50 * time.Millisecond,
	}
}

var benchFrame = []byte("data: {\"k\":\"v\",\"id\":\"abcdef0123456789\",\"ts\":1700000000}\n\n")

func buildBenchWorker() *poolWorker {
	return newPoolWorker(0, benchPoolConfig(), fakeEncoder{}, newFakeMetrics(), zerolog.Nop())
}

func buildBenchSubState(id string) *subState {
	rw := &fakeResponseWriter{}
	rc := http.NewResponseController(rw)
	return &subState{
		sub:         &fakeSub{id: id, kind: "wildcard"},
		queue:       make(chan []byte, 64),
		respWriter:  rw,
		rc:          rc,
		done:        make(chan struct{}),
		connectedAt: time.Now(),
		lastWriteAt: time.Now(),
	}
}

var benchSubsShapes = []struct {
	name string
	n    int
}{
	{"subs_1", 1},
	{"subs_8", 8},
	{"subs_64", 64},
}

func BenchmarkPoolWorkerRun(b *testing.B) {
	for _, shape := range benchSubsShapes {
		b.Run(shape.name, func(b *testing.B) {
			w := buildBenchWorker()
			states := make([]*subState, shape.n)
			for i := 0; i < shape.n; i++ {
				states[i] = buildBenchSubState("bench-run-" + strconv.Itoa(i))
			}

			b.ReportAllocs()
			for b.Loop() {
				now := time.Now()

				for _, st := range states {
					st.buffer = append(st.buffer, benchFrame)
					st.bufBytes += len(benchFrame)
				}

				for _, st := range states {
					w.drainSub(st, now)
				}
			}

			_ = w
			_ = states
		})
	}
}

func BenchmarkPoolWorkerDrainShutdown(b *testing.B) {
	for _, shape := range benchSubsShapes {
		b.Run(shape.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				w := buildBenchWorker()
				for i := 0; i < shape.n; i++ {
					st := buildBenchSubState("bench-shutdown-" + strconv.Itoa(i))

					w.attachSub(st)
				}
				w.drainShutdown()

				_ = w
			}
		})
	}
}
