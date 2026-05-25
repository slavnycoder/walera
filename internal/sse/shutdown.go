package sse

import (
	"errors"
	"fmt"
	"time"
)

const (
	actionContinue = iota
	actionSkipSub
	actionAbandon
)

func (w *poolWorker) drainShutdown() {
	shutdownFrame := w.enc.EncodeShutdown()
	for i, st := range w.subs {
		if st == nil {
			continue
		}
		reason, action := w.collectDirty(i, st)
		switch action {
		case actionAbandon:

			return
		case actionSkipSub:
			continue
		}
		writeErr := w.emitFinalFrames(st, reason, shutdownFrame)
		w.closeAndCount(st, reason, writeErr)
	}
}

func (w *poolWorker) collectDirty(i int, st *subState) (reason string, action int) {

	select {
	case <-w.abandonCh:
		w.drainShutdownAbandon(i)
		return "", actionAbandon
	default:
	}

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

func (w *poolWorker) emitFinalFrames(st *subState, reason string, shutdownFrame []byte) (writeErr error) {
	var frame []byte
	if reason == "shutdown" {
		frame = shutdownFrame
	} else {
		frame = w.enc.EncodeError(reason)
	}

	if len(st.buffer) > 0 {
		w.drainSubDeadline(st, time.Now(), w.cfg.drainShutdownDeadline)
	}

	func() {
		defer func() {
			if r := recover(); r != nil {

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

func (w *poolWorker) closeAndCount(st *subState, reason string, writeErr error) {

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

	if w.metrics != nil {
		w.metrics.PoolWorkerDirtySubsSet(w.workerIDLabel, 0)
	}
}
