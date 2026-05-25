package sse

import (
	"context"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type boundFakeSub struct {
	id   string
	done chan struct{}
	mu   sync.Mutex
	send func(frame []byte) bool
}

func (s *boundFakeSub) ID() string         { return s.id }
func (s *boundFakeSub) KindString() string { return "exact" }
func (s *boundFakeSub) WireSendFunc(send func(frame []byte) bool) {
	s.mu.Lock()
	s.send = send
	s.mu.Unlock()
}
func (s *boundFakeSub) Done() <-chan struct{} { return s.done }
func (s *boundFakeSub) Reason() string        { return "" }

type noopRespWriter struct{}

func (noopRespWriter) Header() http.Header         { return http.Header{} }
func (noopRespWriter) WriteHeader(int)             {}
func (noopRespWriter) Write(p []byte) (int, error) { return len(p), nil }
func (noopRespWriter) Flush()                      {}

func TestPoolGoroutineBoundAt10k(t *testing.T) {

	maxProcs := runtime.GOMAXPROCS(0)
	cfg := PoolConfig{
		PoolFactor:   2,
		SubQueueSize: 4,
		MaxWaitMs:    2,

		HeartbeatInterval:     time.Hour,
		WriteTimeout:          time.Second,
		drainShutdownDeadline: 50 * time.Millisecond,
	}
	poolSize := maxProcs * cfg.PoolFactor

	preCount := runtime.NumGoroutine()

	p := NewPool(cfg, PoolDeps{Encoder: fakeEncoder{}, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	defer func() { _ = p.Shutdown(context.Background()) }()

	const nSubs = 10000
	subs := make([]*boundFakeSub, nSubs)
	rw := noopRespWriter{}
	for i := range subs {
		subs[i] = &boundFakeSub{
			id:   "sub-" + strconv.Itoa(i),
			done: make(chan struct{}),
		}
		rc := http.NewResponseController(rw)
		if _, err := p.Attach(subs[i], nil, rw, rc); err != nil {
			t.Fatalf("Attach[%d]: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	postCount := runtime.NumGoroutine()
	growth := postCount - preCount

	if growth > poolSize+50 {
		t.Errorf("goroutine growth %d exceeds poolSize(%d)+50; per-sub goroutine leak likely (preCount=%d, postCount=%d, GOMAXPROCS=%d, PoolFactor=%d)",
			growth, poolSize, preCount, postCount, maxProcs, cfg.PoolFactor)
	}
	t.Logf("goroutine bound OK: preCount=%d, postCount=%d, growth=%d, poolSize=%d, GOMAXPROCS=%d",
		preCount, postCount, growth, poolSize, maxProcs)
}
