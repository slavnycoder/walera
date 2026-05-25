// Package sse — writer pool's per-worker run loop and its helpers:
// run (outer select), pollAllQueues (inner non-blocking poll),
// sweepHeartbeats, markDirty / drainAll, evictDone. Per-sub write
// surface: drain_helpers.go. Per-worker shutdown: shutdown.go.
package sse

import (
	"time"
)

// hbEpsilon is the jitter slack subtracted from cfg.HeartbeatInterval
// inside sweepHeartbeats so a ticker fire arriving fractionally early
// still produces the heartbeat.
const hbEpsilon = time.Millisecond

// run is the worker's main loop. Hybrid: non-blocking per-sub queue
// polls in a tight inner loop, with an outer blocking select that
// wakes when there's nothing to do. Avoids O(N) reflect.Select.
//
// Drain trigger priority (highest to lowest):
//  1. Per-sub byte overflow — st.bufBytes >= MaxBatchBytesPerSub
//     drains that sub inline inside pollAllQueues.
//  2. Dirty-threshold met — len(dirty) >= drainThreshold → drainAll().
//  3. max_wait_ms timer — armed lazily after the first dirty frame.
//
// BatchingDisabled=true overrides 1/2/3: every pull + every sweep
// with dirty > 0 drains now (TLS / ultra-low-latency mode).
//
// drainThreshold: when cfg.DrainThresholdSubs > 0 the operator value
// is used; when 0 (default after applyDefaults) the formula
// max(8, len(w.subs)/64) is recomputed lazily when thresholdDirty is
// set.
func (w *poolWorker) run() {
	defer close(w.drainDoneCh)

	for {
		// Lazy recompute when the formula path is active and partition
		// size changed. Cheap O(1) per Attach / Detach.
		if w.cfg.DrainThresholdSubs < 1 && w.thresholdDirty {
			t := len(w.subs) / 64
			if t < 8 {
				t = 8
			}
			w.drainThreshold = t
			w.thresholdDirty = false
		}

		// Evict subs whose handlers exited BEFORE polling/writing to
		// their respWriters — otherwise a write could race net/http's
		// bufio.Writer recycle in finishRequest.
		w.evictDone()

		pulled := w.pollAllQueues()
		if w.shutdownObservedInPoll {
			// pollAllQueues observed shutdownCh mid-drain and completed
			// drainShutdown bookkeeping.
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

		// Outer blocking select. 1 ms poll-timer keeps the worker
		// responsive without a dedicated wake channel.
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
			// Per-worker heartbeat sweep. Frame drains via the same
			// trigger evaluation as data frames.
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
			// Loop and re-poll queues.

		case <-w.shutdownCh:
			pollTimer.Stop()
			// Stop heartbeat ticker BEFORE drainShutdown so no further
			// sweep can race partition teardown.
			w.hbTicker.Stop()
			w.drainAll()
			w.drainShutdown()
			return
		}
	}
}

// markDirty appends st to the worker's dirty list when not already
// present and bumps the per-worker dirty-subs gauge on the clean→dirty
// transition. Single-writer (called only from the worker goroutine).
func (w *poolWorker) markDirty(st *subState) {
	if st.inDirty {
		return
	}
	st.inDirty = true
	w.dirty = append(w.dirty, st)
	// Set(0) at the end of drainAll closes any Inc/Dec drift.
	if w.metrics != nil {
		w.metrics.PoolWorkerDirtySubsInc(w.workerIDLabel)
	}
}

// drainAll drains every sub on the dirty list (one writev per dirty
// sub via drainSub), resets the per-worker dirty-subs gauge to 0 (the
// authoritative re-sync), and stops the max_wait_ms timer if armed.
func (w *poolWorker) drainAll() {
	if len(w.dirty) == 0 {
		return
	}
	// BatchSizeObserve receives len(dirty) BEFORE the drain (the work
	// the worker is about to do); DurationObserve receives Since(t0)
	// AFTER. Set(0) re-syncs the gauge.
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

// sweepHeartbeats walks the partition on every fire of the
// per-worker heartbeat ticker. For each sub whose lastWriteAt is
// older than HeartbeatInterval-hbEpsilon, enqueues
// enc.EncodeHeartbeat via the SAME markDirty + buffer-append path
// data frames take. A heartbeat is 3 bytes — cannot overflow
// MaxBatchBytesPerSub on its own. Single-writer read of lastWriteAt
// is racy-but-safe.
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

// evictDone walks the partition and removes any sub whose Done()
// fired. Symmetric counterpart to Attach: post-handler-return,
// respWriter is no longer safe to write to (net/http recycles its
// bufio.Writer in finishRequest); evicting here prevents drainShutdown
// from racing the recycle.
//
// Per evicted sub: drain any buffered frames; emit the v1.3
// error/shutdown frame when reason is non-empty and not
// "client_closed" (recover() brackets Write+Flush — INVARIANTS.md §6);
// emit SubscriberLifetimeObserve + SubscriberDisconnectsInc once
// (inDisconnected-guarded); slow_consumer also bumps SlowClientDropsInc;
// safeCloseDone; remove from subs (compact-swap) and dirty (Dec gauge).
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
			// Do not advance i — swapped-in element still needs checking.
		default:
			i++
		}
	}
}

// pollAllQueues attempts a non-blocking receive on every owned sub's
// queue. Returns frames pulled (across all subs); 0 → worker enters
// the outer blocking select. The inner select also observes attachCh
// and shutdownCh so sustained per-sub inflow cannot starve them; the
// attach case yields via goto nextSub; the shutdown case mirrors the
// outer shutdownCh arm exactly.
func (w *poolWorker) pollAllQueues() int {
	n := 0
	for _, st := range w.subs {
		if st == nil {
			continue
		}
		// Drain everything pending into the per-sub buffer. The buffer
		// is then drained as one writev later.
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
					// Per-sub safety cap — drain THIS sub now.
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
				// Apply mid-drain; outer for-loop re-polls next iter.
				w.attachSub(attachSt)
				goto nextSub
			case <-w.shutdownCh:
				// Mirror outer shutdownCh arm exactly.
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
