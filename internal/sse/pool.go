package sse

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/rs/zerolog"
)

const drainShutdownDeadline = 50 * time.Millisecond

type PoolConfig struct {
	PoolFactor int

	SubQueueSize int

	MaxWaitMs int

	BatchingDisabled bool

	DrainThresholdSubs int

	MaxBatchBytesPerSub int

	WriteTimeout time.Duration

	HeartbeatInterval time.Duration

	drainShutdownDeadline time.Duration
}

func (c *PoolConfig) applyDefaults() {
	if c.PoolFactor < 1 {
		c.PoolFactor = 2
	}
	if c.SubQueueSize < 1 {
		c.SubQueueSize = 32
	}
	if c.MaxWaitMs <= 0 {
		c.MaxWaitMs = 2
	}
	if c.DrainThresholdSubs < 1 {

		c.DrainThresholdSubs = 0
	}
	if c.MaxBatchBytesPerSub < 1 {
		c.MaxBatchBytesPerSub = 64 * 1024
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 5 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 15 * time.Second
	}
	if c.drainShutdownDeadline <= 0 {
		c.drainShutdownDeadline = drainShutdownDeadline
	}
}

type subscriber interface {
	ID() string

	KindString() string

	WireSendFunc(send func(frame []byte) bool)

	Done() <-chan struct{}

	Reason() string
}

type encoderIface interface {
	EncodeHeartbeat() []byte
	EncodeShutdown() []byte
	EncodeError(reason string) []byte
}

type metricsIface interface {
	EventsSentInc(kind string)
	TxDroppedInc(reason string)
	SubscriberLifetimeObserve(seconds float64)
	SubscriberDisconnectsInc(reason string)
	PoolWorkerDirtySubsInc(workerID string)
	PoolWorkerDirtySubsDec(workerID string)
	PoolWorkerDirtySubsSet(workerID string, v float64)
	PoolDrainBatchSizeObserve(n float64)
	PoolDrainDurationObserve(seconds float64)
	SlowClientDropsInc()
}

type PoolMetrics = metricsIface

type WriterPool struct {
	cfg     PoolConfig
	workers []*poolWorker
	enc     encoderIface
	metrics metricsIface
	logger  zerolog.Logger

	closed atomic.Bool

	shutdownOnce sync.Once
}

type PoolDeps struct {
	Encoder encoderIface

	Metrics metricsIface

	Logger zerolog.Logger
}

func validatePoolDeps(deps PoolDeps) {
	if deps.Encoder == nil {
		panic("sse.NewPool: Deps.Encoder is required")
	}
	if deps.Metrics == nil {
		panic("sse.NewPool: Deps.Metrics is required")
	}
}

func NewPool(cfg PoolConfig, deps PoolDeps) *WriterPool {
	validatePoolDeps(deps)
	enc := deps.Encoder
	m := deps.Metrics
	logger := deps.Logger
	cfg.applyDefaults()

	poolSize := runtime.GOMAXPROCS(0) * cfg.PoolFactor
	if poolSize < 1 {
		poolSize = 1
	}

	p := &WriterPool{
		cfg:     cfg,
		enc:     enc,
		metrics: m,
		logger:  logger,
		workers: make([]*poolWorker, poolSize),
	}

	for i := 0; i < poolSize; i++ {
		w := newPoolWorker(i, cfg, enc, m, logger.With().Int("pool_worker", i).Logger())
		p.workers[i] = w
		go w.run()
	}

	if m != nil {
		for i := 0; i < poolSize; i++ {
			m.PoolWorkerDirtySubsSet(strconv.Itoa(i), 0)
		}
	}
	return p
}

func (p *WriterPool) Metrics() PoolMetrics { return p.metrics }

func (p *WriterPool) pickWorker(subID string) *poolWorker {
	idx := int(xxhash.Sum64String(subID) % uint64(len(p.workers)))
	return p.workers[idx]
}

func (p *WriterPool) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var retErr error
	p.shutdownOnce.Do(func() {

		p.closed.Store(true)

		for _, w := range p.workers {
			close(w.shutdownCh)
		}

		var wg sync.WaitGroup
		abandoned := false
		var abandonOnce sync.Once
		signalAbandon := func() {
			abandonOnce.Do(func() {
				abandoned = true
				for _, w := range p.workers {
					close(w.abandonCh)
				}
			})
		}
		for _, w := range p.workers {
			wg.Add(1)
			go func(w *poolWorker) {
				defer wg.Done()
				select {
				case <-w.drainDoneCh:
				case <-ctx.Done():
					signalAbandon()
					<-w.drainDoneCh
				}
			}(w)
		}
		wg.Wait()
		if abandoned {
			retErr = ctx.Err()
		}
	})
	return retErr
}

type poolWorker struct {
	id      int
	cfg     PoolConfig
	enc     encoderIface
	metrics metricsIface
	logger  zerolog.Logger

	attachCh chan *subState

	shutdownCh chan struct{}

	drainDoneCh chan struct{}

	abandonCh chan struct{}

	drainThreshold int

	hbTicker *time.Ticker

	thresholdDirty bool

	workerIDLabel string

	subs []*subState

	timer                  *time.Timer
	timerArmed             bool
	shutdownObservedInPoll bool
	dirty                  []*subState
}

func newPoolWorker(id int, cfg PoolConfig, enc encoderIface, m metricsIface, logger zerolog.Logger) *poolWorker {

	threshold := cfg.DrainThresholdSubs
	if threshold < 1 {
		threshold = 8
	}

	w := &poolWorker{
		id:             id,
		cfg:            cfg,
		enc:            enc,
		metrics:        m,
		logger:         logger,
		attachCh:       make(chan *subState),
		shutdownCh:     make(chan struct{}),
		drainDoneCh:    make(chan struct{}),
		abandonCh:      make(chan struct{}),
		drainThreshold: threshold,

		workerIDLabel: strconv.Itoa(id),

		hbTicker: time.NewTicker(cfg.HeartbeatInterval),
		subs:     make([]*subState, 0, 128),
	}

	w.timer = time.NewTimer(time.Hour)
	if !w.timer.Stop() {
		<-w.timer.C
	}
	w.dirty = make([]*subState, 0, 128)
	return w
}

func (w *poolWorker) String() string {
	return fmt.Sprintf("poolWorker{id=%d, subs=%d}", w.id, len(w.subs))
}
