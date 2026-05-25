// Package sse — per-worker graceful and force-exit teardown. See
// INVARIANTS.md §4 (abandonCh priority), §5 (recover + named writeErr),
// §6 (recover atomic with Write/Flush), §7 (drainShutdownDeadline cap).
package sse

import (
	"errors"
	"fmt"
	"time"
)

// drainShutdown action signals returned by collectDirty.
const (
	actionContinue = iota // proceed with this sub: emit + close
	actionSkipSub         // skip this sub (already-done): no emit, no close
	actionAbandon         // abandonCh fired: outer loop returns immediately
)

// drainShutdown is the per-worker tail invoked when shutdownCh closes.
// Emits the spec §3.5 shutdown frame to each owned sub with a tight
// per-sub deadline. Subs already exited are skipped (writing to a
// torn-down respWriter would panic inside net/http).
func (w *poolWorker) drainShutdown() {
	shutdownFrame := w.enc.EncodeShutdown()
	for i, st := range w.subs {
		if st == nil {
			continue
		}
		reason, action := w.collectDirty(i, st)
		switch action {
		case actionAbandon:
			// collectDirty already invoked drainShutdownAbandon(i).
			return
		case actionSkipSub:
			continue
		}
		writeErr := w.emitFinalFrames(st, reason, shutdownFrame)
		w.closeAndCount(st, reason, writeErr)
	}
}

// collectDirty performs the per-iteration entry actions: honour
// abandonCh FIRST (INVARIANTS.md §4), skip already-exited subs, or
// resolve the truthful disconnect reason. Reason resolution order:
// (1) st.sub.Reason() (router-side Drop wins); (2) st.dropReason
// (sticky from prior handleSubWriteFailure); (3) "shutdown" fallback.
// Truthful-reason override on timeout-class writeErr happens AFTER the
// write in closeAndCount.
func (w *poolWorker) collectDirty(i int, st *subState) (reason string, action int) {
	// abandonCh priority FIRST.
	select {
	case <-w.abandonCh:
		w.drainShutdownAbandon(i)
		return "", actionAbandon
	default:
	}
	// Skip already-exited subs (respWriter may be torn down).
	select {
	case <-st.done:
		return "", actionSkipSub
	default:
	}

	reason = st.sub.Reason()
	if reason == "" {
		reason = st.dropReason
	}
	if reason == "" || reason == "client_closed" {
		reason = "shutdown"
	}
	return reason, actionContinue
}

// emitFinalFrames selects the wire frame (EncodeShutdown vs
// EncodeError(reason)), drains any buffered frames within
// drainShutdownDeadline, and writes the final frame with a per-sub
// deadline. Named writeErr return so the deferred recover() can
// capture into it (INVARIANTS.md §5).
func (w *poolWorker) emitFinalFrames(st *subState, reason string, shutdownFrame []byte) (writeErr error) {
	var frame []byte
	if reason == "shutdown" {
		frame = shutdownFrame
	} else {
		frame = w.enc.EncodeError(reason)
	}

	// Best-effort drain of buffered frames under drainShutdownDeadline
	// (NOT steady-state WriteTimeout) so one wedged sub cannot pin the
	// partition past the spec §3.5 budget.
	if len(st.buffer) > 0 {
		w.drainSubDeadline(st, time.Now(), w.cfg.drainShutdownDeadline)
	}

	// Per-sub fresh time.Now() (mirrors drainSub's stale-time.Now() fix).
	func() {
		defer func() {
			if r := recover(); r != nil {
				// respWriter+rc fallback can panic if net/http finished
				// the response — treat as non-timeout failure so reason
				// stays "shutdown".
				writeErr = fmt.Errorf("drainShutdown panic: %v", r)
			}
		}()
		deadline := time.Now().Add(w.cfg.drainShutdownDeadline)
		if st.conn != nil {
			_ = st.conn.SetWriteDeadline(deadline)
			_, werr := st.conn.Write(frame)
			writeErr = werr
		} else if st.respWriter != nil {
			if st.rc != nil {
				_ = st.rc.SetWriteDeadline(deadline)
			}
			if _, werr := st.respWriter.Write(frame); werr != nil {
				writeErr = werr
			} else if st.rc != nil {
				if ferr := st.rc.Flush(); ferr != nil {
					writeErr = ferr
				}
			}
		}
	}()
	return writeErr
}

// closeAndCount runs the per-iteration tail: truthful-reason override
// on timeout-class writeErr, lifecycle metric emission guarded by
// inDisconnected, lockstep SlowClientDropsInc on slow_consumer
// relabel, and safeCloseDone.
func (w *poolWorker) closeAndCount(st *subState, reason string, writeErr error) {
	// Truthful-reason override: if the frame write timed out, the sub
	// never received the frame — re-label "slow_consumer". Only
	// applies when reason was the local "shutdown" fallback; a sticky
	// router-side reason still wins (the slow write is a downstream
	// symptom).
	if reason == "shutdown" && writeErr != nil &&
		(errors.Is(writeErr, deadlineExceededError{}) || isTimeoutErr(writeErr)) {
		reason = "slow_consumer"
	}
	if !st.inDisconnected {
		if w.metrics != nil {
			w.metrics.SubscriberLifetimeObserve(time.Since(st.connectedAt).Seconds())
			w.metrics.SubscriberDisconnectsInc(reason)
			if reason == "slow_consumer" {
				w.metrics.SlowClientDropsInc()
			}
		}
		st.inDisconnected = true
	}
	safeCloseDone(st)
}

// drainShutdownAbandon walks the remaining subs (starting at `from`)
// when the ctx-abandon signal fires, emits lifecycle metrics for any
// sub not yet accounted, closes each done channel best-effort, and
// re-syncs the per-worker dirty-subs gauge to 0. Enforces "HTTP
// handler's <-doneCh always unblocks" regardless of ctx outcome. Runs
// in the worker goroutine — single-writer access to w.subs.
func (w *poolWorker) drainShutdownAbandon(from int) {
	for i := from; i < len(w.subs); i++ {
		st := w.subs[i]
		if st == nil {
			continue
		}
		if !st.inDisconnected {
			if w.metrics != nil {
				w.metrics.SubscriberLifetimeObserve(time.Since(st.connectedAt).Seconds())
				w.metrics.SubscriberDisconnectsInc("shutdown")
			}
			st.inDisconnected = true
		}
		safeCloseDone(st)
	}
	// Re-sync dirty-subs gauge to 0 — abandoned subs would otherwise
	// leak non-zero gauge values during the force-exit window.
	if w.metrics != nil {
		w.metrics.PoolWorkerDirtySubsSet(w.workerIDLabel, 0)
	}
}
