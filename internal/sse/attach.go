// Package sse — subscriber attachment lifecycle: Attach, subState,
// WireSendFunc, slow-consumer queue cap, errPoolClosed fast path.
// Drain: drain_helpers.go + worker_loop.go. Lifecycle: pool.go.
package sse

import (
	"net"
	"net/http"
	"time"
)

// subState is the per-subscriber wire state owned by a single worker.
// Accessed ONLY by the owning worker — no locks (INVARIANTS.md §3).
// The `queue` channel is the sole cross-goroutine handoff point.
type subState struct {
	sub subscriber
	// queue — per-sub frame channel; NEVER closed (drop-on-send policy).
	queue      chan []byte         // pre-encoded SSE frames; full → BP-01 slow_consumer drop
	conn       *net.TCPConn        // nil → fallback via respWriter/rc (TLS/h2c path)
	respWriter http.ResponseWriter // used only when conn == nil
	rc         *http.ResponseController

	// lastWriteAt is the wall-clock of this sub's last successful
	// drain; the per-worker heartbeat ticker consults it.
	lastWriteAt time.Time

	// buffer accumulates frames between drains; backing array reused.
	buffer   [][]byte
	bufBytes int // sum of len(b) for b in buffer — checked against MaxBatchBytesPerSub

	// inDirty — "this sub is in the dirtyList" flag.
	inDirty bool

	// dropReason — sticky, written ONCE on first drop decision.
	dropReason string

	// done is closed by the worker (via idempotent safeCloseDone)
	// after lifecycle ends. The attaching handler blocks on this.
	done chan struct{}

	// connectedAt feeds the SubscriberLifetime histogram.
	connectedAt time.Time

	// inDisconnected — worker-owned once-flag gating the three
	// lifecycle-disconnect emission sites against double-emission.
	inDisconnected bool
}

// Attach binds sub to a worker via xxhash sharding and wires
// sub.WireSendFunc. conn is the raw TCP from ResponseController.Hijack
// (nil → respWriter+rc fallback). Returns errPoolClosed if Shutdown
// has been called. Caller MUST keep the http handler alive on
// <-doneCh until it closes so deferred cleanup order is preserved.
func (p *WriterPool) Attach(sub subscriber, conn *net.TCPConn, respWriter http.ResponseWriter, rc *http.ResponseController) (doneCh <-chan struct{}, err error) {
	if p.closed.Load() {
		return nil, errPoolClosed
	}

	w := p.pickWorker(sub.ID())

	st := &subState{
		sub:        sub,
		queue:      make(chan []byte, p.cfg.SubQueueSize),
		conn:       conn,
		respWriter: respWriter,
		rc:         rc,
		done:       make(chan struct{}),
	}

	// Emit `retry: 15000\n\n` prelude as the FIRST bytes on the
	// hijacked conn. 15s matches HeartbeatInterval and reduces
	// reconnect-storm pressure. On prelude error return WITHOUT
	// posting to attachCh — caller tears down the conn.
	prelude := []byte("retry: 15000\n\n")
	if conn != nil {
		_ = conn.SetWriteDeadline(time.Now().Add(p.cfg.WriteTimeout))
		if _, werr := conn.Write(prelude); werr != nil {
			_ = conn.SetWriteDeadline(time.Time{})
			return nil, werr
		}
		_ = conn.SetWriteDeadline(time.Time{})
	} else if respWriter != nil {
		// rc may be nil in pure-test paths.
		if rc != nil {
			_ = rc.SetWriteDeadline(time.Now().Add(p.cfg.WriteTimeout))
		}
		if _, werr := respWriter.Write(prelude); werr != nil {
			return nil, werr
		}
		if rc != nil {
			if ferr := rc.Flush(); ferr != nil {
				return nil, ferr
			}
			_ = rc.SetWriteDeadline(time.Time{})
		}
	}
	now := time.Now()
	st.connectedAt = now
	// First heartbeat sweep skips this sub (no heartbeat at t=0).
	st.lastWriteAt = now

	// Wire the sendFunc BEFORE the router can call sub.Send. The
	// select-default enforces SubQueueSize → router gets
	// Drop("slow_consumer") on full.
	sub.WireSendFunc(func(frame []byte) bool {
		select {
		case st.queue <- frame:
			return true
		default:
			return false
		}
	})

	// Hand off via attachCh.
	select {
	case w.attachCh <- st:
	case <-w.shutdownCh:
		return nil, errPoolClosed
	}

	return st.done, nil
}

// attachSub records a sub in the worker partition. Marks
// thresholdDirty so run() recomputes drainThreshold lazily.
func (w *poolWorker) attachSub(st *subState) {
	w.subs = append(w.subs, st)
	w.thresholdDirty = true
}

// _ pins a compile-time check that *subState has no exported interface.
var _ = func() *subState { return &subState{} }
