// Package sse — steady-state per-subscriber write surface: drainSub,
// drainSubDeadline (also used at shutdown deadline from shutdown.go),
// safeCloseDone (idempotent gate against double-close), and
// handleSubWriteFailure (classify timeout vs client-close, emit
// disconnect metric). Single-writer contract: INVARIANTS.md §3.
package sse

import (
	"errors"
	"net"
	"time"
)

// drainSub writes all frames buffered for st as ONE writev (real
// *net.TCPConn) or a sequence of Write+Flush (TLS / h2c fallback).
// Updates EventsSent on success; triggers slow_consumer drop on a
// SetWriteDeadline-exceeded error. Uses cfg.WriteTimeout; the
// shutdown-path buffered drain uses drainSubDeadline directly with
// the tighter shutdown deadline.
func (w *poolWorker) drainSub(st *subState, now time.Time) {
	w.drainSubDeadline(st, now, w.cfg.WriteTimeout)
}

// drainSubDeadline is the deadline-parameterised core of drainSub.
// All state mutations (buffer reset, lastWriteAt, EventsSentInc,
// handleSubWriteFailure dispatch) are identical between steady-state
// and shutdown paths — only the SetWriteDeadline computation differs.
func (w *poolWorker) drainSubDeadline(st *subState, now time.Time, writeBudget time.Duration) {
	if len(st.buffer) == 0 {
		return
	}

	// SetWriteDeadline budget MUST be measured from when THIS sub's
	// write begins (not drainAll-loop start) — otherwise one slow sub
	// poisons the deadline for shard-mates. `now` is still used for
	// lastWriteAt (ms of skew is harmless).
	deadlineStart := time.Now()
	if st.conn != nil {
		// Hijacked path: ONE writev(2) for all accumulated frames.
		_ = st.conn.SetWriteDeadline(deadlineStart.Add(writeBudget))
		bufs := net.Buffers(st.buffer)
		_, err := bufs.WriteTo(st.conn)
		_ = st.conn.SetWriteDeadline(time.Time{})
		if err != nil {
			w.handleSubWriteFailure(st, err)
			return
		}
		st.lastWriteAt = now
	} else {
		// Fallback (TLS, h2c, *httptest.ResponseRecorder): per-frame
		// Write + Flush. No writev coalescing possible.
		_ = st.rc.SetWriteDeadline(deadlineStart.Add(writeBudget))
		for _, frame := range st.buffer {
			if _, err := st.respWriter.Write(frame); err != nil {
				w.handleSubWriteFailure(st, err)
				return
			}
		}
		if err := st.rc.Flush(); err != nil {
			w.handleSubWriteFailure(st, err)
			return
		}
		_ = st.rc.SetWriteDeadline(time.Time{})
		st.lastWriteAt = now
	}

	// One EventsSentInc per frame, labelled with the sub's actual kind.
	if w.metrics != nil {
		kind := st.sub.KindString()
		for range st.buffer {
			w.metrics.EventsSentInc(kind)
		}
	}

	// Reset the buffer (reuse backing array).
	for i := range st.buffer {
		st.buffer[i] = nil
	}
	st.buffer = st.buffer[:0]
	st.bufBytes = 0
}

// safeCloseDone closes st.done idempotently. Four code paths can
// independently reach the close site (evictDone,
// handleSubWriteFailure, drainShutdown, drainShutdownAbandon); a bare
// close() would race them under load. Single-writer invariant
// (only the owning worker calls this for a given st) makes the
// select-then-close pattern race-free.
func safeCloseDone(st *subState) {
	select {
	case <-st.done:
	default:
		close(st.done)
	}
}

// handleSubWriteFailure marks the sub dropped and increments the
// disconnect metric. SubscriberDisconnectsInc is a lifecycle event
// (not TxDroppedInc which is per-tx). slow_consumer drops also bump
// SlowClientDropsInc to keep the spec-named counter in lockstep.
func (w *poolWorker) handleSubWriteFailure(st *subState, err error) {
	reason := "client_closed"
	if errors.Is(err, deadlineExceededError{}) || isTimeoutErr(err) {
		reason = "slow_consumer"
	}
	w.logger.Info().
		Str("subscriber_id", st.sub.ID()).
		Err(err).
		Str("reason", reason).
		Msg("sse drain failed; subscriber dropped")
	st.dropReason = reason
	if !st.inDisconnected {
		if w.metrics != nil {
			w.metrics.SubscriberDisconnectsInc(reason)
			if reason == "slow_consumer" {
				w.metrics.SlowClientDropsInc()
			}
		}
		st.inDisconnected = true
	}
	// Route through safeCloseDone so a second drainSub on the same st
	// does not panic on double-close.
	safeCloseDone(st)
}

// deadlineExceededError is a stand-in for os.ErrDeadlineExceeded —
// matched via errors.Is so this file avoids the os import.
type deadlineExceededError struct{}

func (deadlineExceededError) Error() string { return "deadline exceeded" }
func (deadlineExceededError) Is(target error) bool {
	return target.Error() == "deadline exceeded"
}

// isTimeoutErr returns true if err implements net.Error and reports
// Timeout()==true. Catches os.ErrDeadlineExceeded without an explicit
// dependency.
func isTimeoutErr(err error) bool {
	type timeout interface{ Timeout() bool }
	t, ok := err.(timeout)
	return ok && t.Timeout()
}
