package sse

import (
	"time"
)

const hbEpsilon = time.Millisecond

func (w *poolWorker) run() {
	defer close(w.drainDoneCh)

	for {

		if w.cfg.DrainThresholdSubs < 1 && w.thresholdDirty {
			t := len(w.subs) / 64
			if t < 8 {
				t = 8
			}
			w.drainThreshold = t
			w.thresholdDirty = false
		}

		w.evictDone()

		pulled := w.pollAllQueues()
		if w.shutdownObservedInPoll {

			return
		}
		if pulled > 0 {
			if w.cfg.BatchingDisabled {
				w.drainAll()
			} else if len(w.dirty) >= w.drainThreshold {
				w.drainAll()
			} else if !w.timerArmed && len(w.dirty) > 0 {
				w.timer.Reset(time.Duration(w.cfg.MaxWaitMs) * time.Millisecond)
				w.timerArmed = true
			}
			continue
		}

		pollTimer := time.NewTimer(time.Millisecond)
		select {
		case st := <-w.attachCh:
			pollTimer.Stop()
			w.attachSub(st)

		case <-w.timer.C:
			pollTimer.Stop()
			w.timerArmed = false
			w.drainAll()

		case <-w.hbTicker.C:

			pollTimer.Stop()
			w.sweepHeartbeats()
			if len(w.dirty) > 0 {
				if w.cfg.BatchingDisabled {
					w.drainAll()
				} else if len(w.dirty) >= w.drainThreshold {
					w.drainAll()
				} else if !w.timerArmed {
					w.timer.Reset(time.Duration(w.cfg.MaxWaitMs) * time.Millisecond)
					w.timerArmed = true
				}
			}

		case <-pollTimer.C:

		case <-w.shutdownCh:
			pollTimer.Stop()

			w.hbTicker.Stop()
			w.drainAll()
			w.drainShutdown()
			return
		}
	}
}

func (w *poolWorker) markDirty(st *subState) {
	if st.inDirty {
		return
	}
	st.inDirty = true
	w.dirty = append(w.dirty, st)

	if w.metrics != nil {
		w.metrics.PoolWorkerDirtySubsInc(w.workerIDLabel)
	}
}

func (w *poolWorker) drainAll() {
	if len(w.dirty) == 0 {
		return
	}

	t0 := time.Now()
	if w.metrics != nil {
		w.metrics.PoolDrainBatchSizeObserve(float64(len(w.dirty)))
	}
	now := t0
	for _, st := range w.dirty {
		w.drainSub(st, now)
		st.inDirty = false
	}
	w.dirty = w.dirty[:0]
	if w.metrics != nil {
		w.metrics.PoolDrainDurationObserve(time.Since(t0).Seconds())
		w.metrics.PoolWorkerDirtySubsSet(w.workerIDLabel, 0)
	}
	if w.timerArmed {
		if !w.timer.Stop() {
			select {
			case <-w.timer.C:
			default:
			}
		}
		w.timerArmed = false
	}
}

func (w *poolWorker) sweepHeartbeats() {
	hb := w.enc.EncodeHeartbeat()
	now := time.Now()
	threshold := w.cfg.HeartbeatInterval - hbEpsilon
	for _, st := range w.subs {
		if st == nil {
			continue
		}
		if now.Sub(st.lastWriteAt) < threshold {
			continue
		}
		if len(st.buffer) == 0 {
			w.markDirty(st)
		}
		st.buffer = append(st.buffer, hb)
		st.bufBytes += len(hb)
	}
}

func (w *poolWorker) evictDone() {
	for i := 0; i < len(w.subs); {
		st := w.subs[i]
		if st == nil {
			w.subs[i] = w.subs[len(w.subs)-1]
			w.subs = w.subs[:len(w.subs)-1]
			w.thresholdDirty = true
			continue
		}
		doneCh := st.sub.Done()
		select {
		case <-doneCh:
			if len(st.buffer) > 0 {
				w.drainSub(st, time.Now())
			}
			reason := st.sub.Reason()
			if reason == "" {
				reason = st.dropReason
			}
			if reason != "" && reason != "client_closed" {
				var frame []byte
				if reason == "shutdown" {
					frame = w.enc.EncodeShutdown()
				} else {
					frame = w.enc.EncodeError(reason)
				}
				deadline := time.Now().Add(w.cfg.drainShutdownDeadline)
				func() {
					defer func() { _ = recover() }()
					if st.conn != nil {
						_ = st.conn.SetWriteDeadline(deadline)
						_, _ = st.conn.Write(frame)
					} else if st.respWriter != nil {
						if st.rc != nil {
							_ = st.rc.SetWriteDeadline(deadline)
						}
						_, _ = st.respWriter.Write(frame)
						if st.rc != nil {
							_ = st.rc.Flush()
						}
					}
				}()
			}
			if !st.inDisconnected {
				if w.metrics != nil {
					w.metrics.SubscriberLifetimeObserve(time.Since(st.connectedAt).Seconds())
					metricReason := reason
					if metricReason == "" {
						metricReason = "client_closed"
					}
					w.metrics.SubscriberDisconnectsInc(metricReason)
					if metricReason == "slow_consumer" {
						w.metrics.SlowClientDropsInc()
					}
				}
				st.inDisconnected = true
			}
			safeCloseDone(st)
			if st.inDirty {
				for j, d := range w.dirty {
					if d == st {
						w.dirty[j] = w.dirty[len(w.dirty)-1]
						w.dirty = w.dirty[:len(w.dirty)-1]
						break
					}
				}
				st.inDirty = false
				if w.metrics != nil {
					w.metrics.PoolWorkerDirtySubsDec(w.workerIDLabel)
				}
			}
			w.subs[i] = w.subs[len(w.subs)-1]
			w.subs = w.subs[:len(w.subs)-1]
			w.thresholdDirty = true

		default:
			i++
		}
	}
}

func (w *poolWorker) pollAllQueues() int {
	n := 0
	for _, st := range w.subs {
		if st == nil {
			continue
		}

		for {
			select {
			case frame, ok := <-st.queue:
				if !ok {
					continue
				}
				if len(st.buffer) == 0 {
					w.markDirty(st)
				}
				st.buffer = append(st.buffer, frame)
				st.bufBytes += len(frame)
				n++
				if st.bufBytes >= w.cfg.MaxBatchBytesPerSub {

					w.drainSub(st, time.Now())
					st.inDirty = false
					for i, d := range w.dirty {
						if d == st {
							w.dirty[i] = w.dirty[len(w.dirty)-1]
							w.dirty = w.dirty[:len(w.dirty)-1]
							break
						}
					}
					if w.metrics != nil {
						w.metrics.PoolWorkerDirtySubsDec(w.workerIDLabel)
					}
				}
			case attachSt := <-w.attachCh:

				w.attachSub(attachSt)
				goto nextSub
			case <-w.shutdownCh:

				w.hbTicker.Stop()
				w.drainAll()
				w.drainShutdown()
				w.shutdownObservedInPoll = true
				return n
			default:
				goto nextSub
			}
		}
	nextSub:
	}
	return n
}
