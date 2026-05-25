// Package sse — WriterPool struct + xxhash-based subscriber→worker
// mapping. Drain: worker_loop.go + drain_helpers.go + shutdown.go.
// Attach: attach.go. See INVARIANTS.md §8, §9.
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

// drainShutdownDeadline is the spec §3.5 per-sub final-frame write cap
// (INVARIANTS.md §7). HARDCODED — not exposed via koanf.
const drainShutdownDeadline = 50 * time.Millisecond

// PoolConfig holds the constructor parameters for NewPool. Fields
// mirror koanf keys in config.go.
type PoolConfig struct {
	// PoolFactor multiplies runtime.GOMAXPROCS(0). Default 2.
	PoolFactor int

	// SubQueueSize is the per-sub buffered chan capacity. Default 32;
	// hitting cap → slow_consumer drop.
	SubQueueSize int

	// MaxWaitMs bounds per-frame batching lag. Zero → unset (default 2 ms);
	// to disable batching set BatchingDisabled.
	MaxWaitMs int

	// BatchingDisabled drains every cycle (TLS / ultra-low-latency).
	BatchingDisabled bool

	// DrainThresholdSubs is the dirty-sub count for eager drain.
	// Default formula: max(8, partition_size / 64).
	DrainThresholdSubs int

	// MaxBatchBytesPerSub is the per-sub buffer-growth safety cap.
	// Default 64 KiB; overflow → immediate drain.
	MaxBatchBytesPerSub int

	// WriteTimeout is the per-sub SetWriteDeadline budget. Default 5s.
	WriteTimeout time.Duration

	// HeartbeatInterval is the per-sub `:\n\n` cadence. Default 15s.
	HeartbeatInterval time.Duration

	// drainShutdownDeadline caps each per-sub final-frame write during
	// graceful shutdown (INVARIANTS.md §7). Unexported — test injection.
	drainShutdownDeadline time.Duration
}

// applyDefaults fills in zero-valued PoolConfig fields.
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
		// Sentinel 0 → NewPool uses the default formula.
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

// subscriber is the minimal interface the pool needs. Production wires
// *router.Subscriber; tests substitute fakes.
type subscriber interface {
	ID() string
	// KindString returns "exact" | "wildcard" for EventsSentInc labels
	// (avoids importing router.Kind into the pool).
	KindString() string
	// WireSendFunc installs the closure the router calls to deliver a
	// pre-encoded frame; returns false on full queue → router drops
	// with reason "slow_consumer".
	WireSendFunc(send func(frame []byte) bool)
	// Done fires when the sub should be torn down; pool polls per cycle.
	Done() <-chan struct{}
	// Reason returns the sticky Drop reason or "" if not yet dropped.
	Reason() string
}

// encoderIface is the minimal frame-encoder seam.
type encoderIface interface {
	EncodeHeartbeat() []byte
	EncodeShutdown() []byte
	EncodeError(reason string) []byte
}

// metricsIface is the per-frame metrics increment-and-observe surface.
// SlowClientDropsInc is called sibling-to every
// SubscriberDisconnectsInc("slow_consumer") in the drain path — never
// on the per-tx TxDroppedInc("slow_consumer") (router-side queue-full).
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

// PoolMetrics is the exported alias for metricsIface (composition-root
// binding handle).
type PoolMetrics = metricsIface

// WriterPool owns worker goroutines and routes attach/detach via
// xxhash sharding.
type WriterPool struct {
	cfg     PoolConfig
	workers []*poolWorker
	enc     encoderIface
	metrics metricsIface
	logger  zerolog.Logger

	// closed gates Attach (Shutdown sets it).
	closed atomic.Bool

	// shutdownOnce guards fan-out against parallel Shutdown calls.
	shutdownOnce sync.Once
}

// PoolDeps groups the collaborators required by NewPool.
// Required (nil-checked): Encoder, Metrics. Optional: Logger.
type PoolDeps struct {
	// Encoder produces heartbeat / shutdown / error frame bytes.
	Encoder encoderIface
	// Metrics is the per-pool metrics adapter.
	Metrics metricsIface
	// Logger threaded to each worker; zero value is a usable Nop.
	Logger zerolog.Logger
}

// validatePoolDeps panics on any required nil field.
func validatePoolDeps(deps PoolDeps) {
	if deps.Encoder == nil {
		panic("sse.NewPool: Deps.Encoder is required")
	}
	if deps.Metrics == nil {
		panic("sse.NewPool: Deps.Metrics is required")
	}
}

// NewPool constructs a WriterPool. Workers are spawned immediately;
// caller MUST invoke Shutdown before process exit to drain in-flight
// frames.
func NewPool(cfg PoolConfig, deps PoolDeps) *WriterPool {
	validatePoolDeps(deps)
	enc := deps.Encoder
	m := deps.Metrics
	logger := deps.Logger
	cfg.applyDefaults()

	// GOMAXPROCS must reflect container quota (cmd/cdc-sse/main.go
	// calls automaxprocs.Set before NewPool).
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

	// Pre-touch every worker_id series so /metrics exposes one sample
	// per worker from t=0 (idle workers otherwise render "no data").
	// The Set(0) at drainAll-tail is the authoritative gauge value.
	if m != nil {
		for i := 0; i < poolSize; i++ {
			m.PoolWorkerDirtySubsSet(strconv.Itoa(i), 0)
		}
	}
	return p
}

// Metrics returns the metrics adapter. Tests type-assert to
// *PoolMetricsAdapter at the seam.
func (p *WriterPool) Metrics() PoolMetrics { return p.metrics }

// pickWorker returns the worker assigned to subID. Stable for the
// pool's lifetime — satisfies the single-writer per-sub invariant.
func (p *WriterPool) pickWorker(subID string) *poolWorker {
	idx := int(xxhash.Sum64String(subID) % uint64(len(p.workers)))
	return p.workers[idx]
}

// Shutdown drains all subscribers and stops all workers, honouring
// ctx as a wall-clock budget. Sequence: (1) p.closed = true so
// in-flight Attach returns errPoolClosed; (2) close shutdownCh per
// worker (worker drains + emits §3.5 shutdown frame + closes st.done);
// (3) wait on drainDoneCh bounded by ctx.Done(). Idempotent
// (sync.Once). On ctx.Done() the abandon path STILL closes every
// owned sub's done channel best-effort BEFORE returning ctx.Err() —
// every handler blocked on <-doneCh is unblocked. Returns nil on
// clean drain; ctx.Err() on expiry.
func (p *WriterPool) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var retErr error
	p.shutdownOnce.Do(func() {
		// Set the gate BEFORE closing shutdownCh; the
		// select-on-attachCh/shutdownCh in Attach covers the racy window.
		p.closed.Store(true)

		for _, w := range p.workers {
			close(w.shutdownCh)
		}

		// On ctx-expiry signal abandonCh to ALL workers — drainShutdown
		// checks this between subs and closes remaining doneChs.
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

// poolWorker is one drain loop. Owns its subs partition; only the
// worker goroutine reads or writes it.
type poolWorker struct {
	id      int
	cfg     PoolConfig
	enc     encoderIface
	metrics metricsIface
	logger  zerolog.Logger

	// attachCh — Attach handoff. Never closed.
	attachCh chan *subState

	// shutdownCh — closed by Pool.Shutdown (single closer).
	shutdownCh chan struct{}

	// drainDoneCh — closed by the worker after drain completes.
	drainDoneCh chan struct{}

	// abandonCh — closed by Pool.Shutdown on ctx-expiry; worker checks
	// FIRST between subs (INVARIANTS.md §4).
	abandonCh chan struct{}

	// drainThreshold — dirty-sub count for eager drain.
	drainThreshold int

	// hbTicker — SINGLE per-worker heartbeat ticker (O(poolSize) total).
	hbTicker *time.Ticker

	// thresholdDirty marks partition-size change for lazy recompute.
	thresholdDirty bool

	// workerIDLabel — strconv.Itoa(id) cached for hot-path metric labels.
	workerIDLabel string

	// Per-worker state, owned exclusively by the worker goroutine.
	// NOT thread-safe.
	subs []*subState // partition; index is sub's slot within this worker

	// Run-loop-owned single-writer state (INVARIANTS.md §9). Promoted
	// from loop-locals to avoid closure-capture escape. run() runs
	// exactly once per worker so field-zero values are correct.
	timer                  *time.Timer
	timerArmed             bool
	shutdownObservedInPoll bool
	dirty                  []*subState // cap 128, pre-allocated in newPoolWorker
}

func newPoolWorker(id int, cfg PoolConfig, enc encoderIface, m metricsIface, logger zerolog.Logger) *poolWorker {
	// Sentinel 0 → formula max(8, len(w.subs)/64) recomputed lazily.
	// Seed floor 8 until first attach lifts it.
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
		// Cache the worker-id string once; metric emission is hot-path.
		workerIDLabel: strconv.Itoa(id),
		// ONE heartbeat ticker per worker (stopped in run()'s shutdown arm).
		hbTicker: time.NewTicker(cfg.HeartbeatInterval),
		subs:     make([]*subState, 0, 128),
	}
	// dirty pre-allocated at cap 128 to preserve the stack-allocated
	// capacity of the pre-decomposition local (must NOT regress
	// allocator profile).
	w.timer = time.NewTimer(time.Hour)
	if !w.timer.Stop() {
		<-w.timer.C
	}
	w.dirty = make([]*subState, 0, 128)
	return w
}

// String is the debug-log + panic-message formatter.
func (w *poolWorker) String() string {
	return fmt.Sprintf("poolWorker{id=%d, subs=%d}", w.id, len(w.subs))
}
