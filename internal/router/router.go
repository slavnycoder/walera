package router

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/safego"
	"github.com/walera/walera/internal/wal"
)

type encoderIface interface {
	Encode(Event) ([]byte, bool)
}

type Broadcaster struct {
	cfg      Config
	log      zerolog.Logger
	metrics  *metrics.Registry
	enc      encoderIface
	exact    *index
	wildcard *wildcardIndex
}

type Deps struct {
	Logger zerolog.Logger

	Metrics *metrics.Registry

	Encoder encoderIface
}

func validateDeps(d Deps) {
	if d.Metrics == nil {
		panic("router.New: Deps.Metrics is required")
	}
	if d.Encoder == nil {
		panic("router.New: Deps.Encoder is required")
	}
}

func New(cfg Config, deps Deps) *Broadcaster {
	validateDeps(deps)
	b := &Broadcaster{
		cfg:      cfg,
		log:      deps.Logger,
		metrics:  deps.Metrics,
		enc:      deps.Encoder,
		exact:    newIndex(),
		wildcard: newWildcardIndex(),
	}
	b.metrics.TxDropped("slow_consumer").Add(0)
	b.metrics.TxDropped("tx_too_large").Add(0)
	b.metrics.TxDropped("multi_root").Add(0)
	return b
}

func (b *Broadcaster) Register(sub *Subscriber) {
	switch sub.Kind() {
	case KindExact:
		key := sub.Schema() + "." + sub.Table() + ":" + sub.PK()
		b.exact.Add(key, sub)
		b.metrics.SubscribersActive(string(KindExact)).Inc()
		b.log.Info().
			Str("subscriber_id", sub.ID()).
			Str("kind", string(KindExact)).
			Str("channel", key).
			Str("start_lsn", sub.StartLSN().String()).
			Msg("subscriber registered")
	case KindWildcard:
		key := sub.Schema() + "." + sub.Table()
		b.wildcard.Add(key, sub)
		b.metrics.SubscribersActive(string(KindWildcard)).Inc()
		b.log.Info().
			Str("subscriber_id", sub.ID()).
			Str("kind", string(KindWildcard)).
			Str("channel", key).
			Str("start_lsn", sub.StartLSN().String()).
			Msg("subscriber registered")
	}
}

func (b *Broadcaster) Deregister(sub *Subscriber) {
	reason := sub.Reason()
	switch sub.Kind() {
	case KindExact:
		key := sub.Schema() + "." + sub.Table() + ":" + sub.PK()
		b.exact.Remove(key, sub)
		b.metrics.SubscribersActive(string(KindExact)).Dec()
		b.log.Info().
			Str("subscriber_id", sub.ID()).
			Str("kind", string(KindExact)).
			Str("channel", key).
			Str("reason", reason).
			Msg("subscriber deregistered")
	case KindWildcard:
		key := sub.Schema() + "." + sub.Table()
		b.wildcard.Remove(key, sub)
		b.metrics.SubscribersActive(string(KindWildcard)).Dec()
		b.log.Info().
			Str("subscriber_id", sub.ID()).
			Str("kind", string(KindWildcard)).
			Str("channel", key).
			Str("reason", reason).
			Msg("subscriber deregistered")
	}
}

func (b *Broadcaster) Ingest(ctx context.Context, txCh <-chan wal.Tx) error {
	for {
		select {
		case tx, ok := <-txCh:
			if !ok {
				return nil
			}
			b.routeTx(tx)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *Broadcaster) routeTx(tx wal.Tx) {
	lookupTimer := prometheus.NewTimer(b.metrics.RouteLookupDuration())
	defer lookupTimer.ObserveDuration()

	eligible := make(map[*Subscriber]struct{}, 8)
	b.mergeMatches(tx, eligible)

	b.metrics.RoutingFanOut().Observe(float64(len(eligible)))
	if len(eligible) == 0 {
		return
	}

	// fullIndices covers the whole tx; dispatchEvent's filter loop is the sole
	// delivery gate (per-change whitelist). Allocated once and shared read-only
	// across sequential dispatch calls — no goroutine spawned in dispatchEvent
	// (SAFE-01; INVARIANT #3 drop sites stay in dispatchEvent).
	fullIndices := make([]int, len(tx.Changes))
	for i := range fullIndices {
		fullIndices[i] = i
	}

	var totalDelivered int64
	var totalBeyondAnchor int64
	for sub := range eligible {
		delivered, beyondAnchor := b.dispatchEvent(tx, sub, fullIndices)
		totalDelivered += int64(delivered)
		totalBeyondAnchor += int64(beyondAnchor)
	}
	// Observe fan-out work and beyond-anchor counter once per tx
	// (INVARIANT #4: stays in routeTx frame). Guard on >0 so a matched tx
	// whose eligible subscribers were all dropped (slow-consumer / tx-too-large)
	// does not inflate the histogram SampleCount / counter with empty work —
	// the registry pre-touch already seeds both series at t=0.
	if totalDelivered > 0 {
		b.metrics.TxFanOutWork().Observe(float64(totalDelivered))
	}
	if totalBeyondAnchor > 0 {
		b.metrics.CoBeyondAnchorTotal().Add(float64(totalBeyondAnchor))
	}
}

func (b *Broadcaster) matchExact(key string) []*Subscriber {
	return b.exact.Lookup(key)
}

func (b *Broadcaster) matchWildcard(key string) []*Subscriber {
	return b.wildcard.Lookup(key)
}

// mergeMatches builds the per-tx eligibility set: a subscriber is eligible once any
// change in the tx matches its exact or wildcard key. The map[*Subscriber]struct{}
// inherently deduplicates multiple matches (INVARIANT #1; TXN-01/TXN-04).
func (b *Broadcaster) mergeMatches(tx wal.Tx, eligible map[*Subscriber]struct{}) {
	for _, ch := range tx.Changes {
		for _, sub := range b.matchExact(ch.Key()) {
			eligible[sub] = struct{}{}
		}
		for _, sub := range b.matchWildcard(ch.WildcardKey()) {
			eligible[sub] = struct{}{}
		}
	}
}

// dispatchEvent applies the subscriber's whitelist filter to the full tx index set,
// enforces the post-filter MaxChangesPerTx cap, encodes, and sends.
// Returns (delivered, beyondAnchor): delivered is the count of post-filter changes sent
// (0 on any drop or silent skip); beyondAnchor is the count of delivered changes whose
// routing key does NOT match the subscriber's own anchor key.
// All Drop call sites remain exclusively inside this function (INVARIANT #3).
func (b *Broadcaster) dispatchEvent(tx wal.Tx, sub *Subscriber, indices []int) (int, int) {
	if tx.CommitLSN <= sub.StartLSN() {
		return 0, 0
	}

	capLimit := b.cfg.MaxChangesPerTx

	ev := Event{Tx: tx, MatchedIndices: indices}
	if sub.Filter != nil {
		filtered := make([]wal.Change, 0, len(indices))
		for _, i := range indices {
			ch, drop := sub.Filter(tx.Changes[i], tx.CommitLSN)
			if drop {
				continue
			}
			filtered = append(filtered, ch)
		}
		if len(filtered) == 0 {
			return 0, 0 // silent drop — no metric, no Drop() (INVARIANT #3)
		}
		// Cap is post-filter (delivered count), not pre-filter (candidate count).
		// Counting pre-filter would falsely drop narrow-whitelist subscribers on large txs.
		if len(filtered) > capLimit {
			b.metrics.TxDropped("tx_too_large").Inc()
			sub.Drop("tx_too_large")
			channel := sub.Schema() + "." + sub.Table()
			if sub.Kind() == KindExact {
				channel = channel + ":" + sub.PK()
			}
			b.log.Warn().
				Str("subscriber_id", sub.ID()).
				Str("channel", channel).
				Int("filtered_count", len(filtered)).
				Int("limit", capLimit).
				Str("commit_lsn", tx.CommitLSN.String()).
				Msg("subscriber dropped: tx_too_large")
			return 0, 0
		}
		subTx := tx
		subTx.Changes = filtered
		newIdx := make([]int, len(filtered))
		for i := range filtered {
			newIdx[i] = i
		}
		ev = Event{Tx: subTx, MatchedIndices: newIdx}
	} else {
		// nil-Filter fast path: ev.Tx.Changes shares backing array with tx.Changes
		// (no clone; see TestRouteTxNilFilterUnchangedFastPath).
		// Cap still applies on the full index count.
		if len(indices) > capLimit {
			b.metrics.TxDropped("tx_too_large").Inc()
			sub.Drop("tx_too_large")
			channel := sub.Schema() + "." + sub.Table()
			if sub.Kind() == KindExact {
				channel = channel + ":" + sub.PK()
			}
			b.log.Warn().
				Str("subscriber_id", sub.ID()).
				Str("channel", channel).
				Int("change_count", len(indices)).
				Int("limit", capLimit).
				Str("commit_lsn", tx.CommitLSN.String()).
				Msg("subscriber dropped: tx_too_large")
			return 0, 0
		}
	}

	frame, overflow := b.enc.Encode(ev)
	if overflow {
		b.metrics.TxDropped("tx_too_large").Inc()
		sub.Drop("tx_too_large")
		b.log.Warn().
			Str("subscriber_id", sub.ID()).
			Str("commit_lsn", tx.CommitLSN.String()).
			Int("matched_changes", len(ev.MatchedIndices)).
			Msg("subscriber dropped: tx_too_large (payload cap)")
		return 0, 0
	}

	if !sub.send(frame) {
		b.metrics.TxDropped("slow_consumer").Inc()
		sub.Drop("slow_consumer")
		b.log.Warn().
			Str("subscriber_id", sub.ID()).
			Str("reason", "slow_consumer").
			Str("commit_lsn", tx.CommitLSN.String()).
			Msg("subscriber dropped: slow_consumer")
		return 0, 0
	}

	// Compute beyond-anchor delta from the post-filter event.
	// Branch on sub.Kind() to use the correct key form:
	//   KindExact:    anchor = "schema.table:pk"   → compare ch.Key()
	//   KindWildcard: anchor = "schema.table"       → compare ch.WildcardKey()
	var anchorKey string
	if sub.Kind() == KindExact {
		anchorKey = sub.Schema() + "." + sub.Table() + ":" + sub.PK()
	} else {
		anchorKey = sub.Schema() + "." + sub.Table()
	}
	var beyondAnchor int
	for _, idx := range ev.MatchedIndices {
		ch := ev.Tx.Changes[idx]
		var routingKey string
		if sub.Kind() == KindExact {
			routingKey = ch.Key()
		} else {
			routingKey = ch.WildcardKey()
		}
		if routingKey != anchorKey {
			beyondAnchor++
		}
	}

	return len(ev.MatchedIndices), beyondAnchor // delivered count + beyond-anchor delta
}

func (b *Broadcaster) Metrics() *metrics.Registry { return b.metrics }

func (b *Broadcaster) ExactLen() int { return b.exact.Len() }

func (b *Broadcaster) WildcardLen() int { return b.wildcard.Len() }

func (b *Broadcaster) Shutdown(ctx context.Context, drainDeadline time.Duration) error {
	subs := append(b.exact.Snapshot(), b.wildcard.Snapshot()...)

	b.log.Debug().
		Int("subscribers", len(subs)).
		Msg("broadcaster shutdown: fanning out shutdown reason")

	if len(subs) == 0 {
		b.log.Info().
			Int("subscribers_remaining", 0).
			Msg("broadcaster shutdown: no active subscribers")
		return nil
	}

	var wg sync.WaitGroup
	for _, sub := range subs {
		wg.Add(1)
		s := sub
		safego.Go("shutdown-broadcast-"+s.ID(), func() {
			defer wg.Done()
			s.Drop("shutdown")
			select {
			case <-s.Done():
			case <-ctx.Done():
				return
			}
		})
	}

	done := make(chan struct{})
	safego.Go("shutdown-broadcast-waiter", func() {
		wg.Wait()
		close(done)
	})

	select {
	case <-done:
		b.log.Info().
			Int("subscribers_remaining", 0).
			Msg("broadcaster shutdown: drain complete")
		return nil
	case <-time.After(drainDeadline):
		return context.DeadlineExceeded
	case <-ctx.Done():
		return ctx.Err()
	}
}
