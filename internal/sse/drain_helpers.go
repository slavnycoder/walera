package sse

import (
	"errors"
	"net"
	"time"
)

func (w *poolWorker) drainSub(st *subState, now time.Time) {
	w.drainSubDeadline(st, now, w.cfg.WriteTimeout)
}

func (w *poolWorker) drainSubDeadline(st *subState, now time.Time, writeBudget time.Duration) {
	if len(st.buffer) == 0 {
		return
	}

	deadlineStart := time.Now()
	if st.conn != nil {

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

	if w.metrics != nil {
		kind := st.sub.KindString()
		for range st.buffer {
			w.metrics.EventsSentInc(kind)
		}
	}

	for i := range st.buffer {
		st.buffer[i] = nil
	}
	st.buffer = st.buffer[:0]
	st.bufBytes = 0
}

func safeCloseDone(st *subState) {
	select {
	case <-st.done:
	default:
		close(st.done)
	}
}

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

	safeCloseDone(st)
}

type deadlineExceededError struct{}

func (deadlineExceededError) Error() string { return "deadline exceeded" }
func (deadlineExceededError) Is(target error) bool {
	return target.Error() == "deadline exceeded"
}

func isTimeoutErr(err error) bool {
	type timeout interface{ Timeout() bool }
	t, ok := err.(timeout)
	return ok && t.Timeout()
}
