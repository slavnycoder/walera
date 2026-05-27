package router

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
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

	// fullIndices is allocated once and shared read-only across the sequential
	// dispatch loop (SAFE-01: no goroutine spawned in dispatchEvent).
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
	// Guard on >0 so a matched tx whose eligible subscribers were all dropped
	// does not inflate the histogram/counter with empty work — registry
	// pre-touch already seeds both series at t=0 (INVARIANT #4).
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

// mergeMatches builds the per-tx eligibility set: a subscriber becomes eligible
// once any change in the tx matches its exact or wildcard key. The set shape
// inherently deduplicates multiple matches.
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

func subChannelKey(sub *Subscriber) string {
	key := sub.Schema() + "." + sub.Table()
	if sub.Kind() == KindExact {
		key += ":" + sub.PK()
	}
	return key
}

func matchesAnchor(sub *Subscriber, ch wal.Change) bool {
	if ch.Schema != sub.Schema() || ch.Table != sub.Table() {
		return false
	}
	switch sub.Kind() {
	case KindExact:
		return ch.PK == sub.PK()
	case KindWildcard:
		return true
	default:
		return false
	}
}

// dispatchEvent applies the subscriber's whitelist filter to the full tx index
// set, enforces the post-filter MaxChangesPerTx cap, encodes, and sends.
// Returns (delivered, beyondAnchor): delivered counts post-filter changes sent
// (0 on any drop or silent skip); beyondAnchor counts delivered changes whose
// routing key differs from the subscriber's own anchor key.
// All Drop call sites must stay inside this function (INVARIANT #3).
func (b *Broadcaster) dispatchEvent(tx wal.Tx, sub *Subscriber, indices []int) (int, int) {
	if tx.CommitLSN <= sub.StartLSN() {
		return 0, 0
	}

	capLimit := b.cfg.MaxChangesPerTx
	subSchema := sub.Schema()
	subTable := sub.Table()

	// Multi-root guard: a tx touching >1 distinct PK of the subscriber's anchor
	// table violates the one-root-per-tx writer-side discipline (spec §1.6).
	// Per-subscriber tx drop: counter + log, but the connection stays open —
	// the client resyncs from the primary API and the next well-formed tx is
	// delivered normally. INVARIANT #2: this is the sole .Inc() site for
	// tx_dropped_total{reason="multi_root"}.
	if hasMultipleAnchorRoots(tx, subSchema, subTable) {
		b.metrics.TxDropped("multi_root").Inc()
		b.log.Warn().
			Str("subscriber_id", sub.ID()).
			Str("channel", subChannelKey(sub)).
			Str("commit_lsn", tx.CommitLSN.String()).
			Msg("subscriber tx dropped: multi_root (writer-side discipline violation)")
		return 0, 0
	}

	ev := Event{Tx: tx, MatchedIndices: indices}
	if sub.Filter != nil {
		filtered := make([]wal.Change, 0, len(indices))
		anchorSurvived := false
		for _, i := range indices {
			raw := tx.Changes[i]
			if raw.Schema != subSchema {
				continue
			}
			rawAnchor := matchesAnchor(sub, raw)
			ch, drop := sub.Filter(raw, tx.CommitLSN)
			if drop {
				continue
			}
			if rawAnchor {
				anchorSurvived = true
			}
			filtered = append(filtered, ch)
		}
		if len(filtered) == 0 || !anchorSurvived {
			return 0, 0 // silent drop — no metric, no Drop() (INVARIANT #3)
		}
		// Cap is post-filter, not pre-filter: pre-filter would falsely drop
		// narrow-whitelist subscribers on large txs.
		if len(filtered) > capLimit {
			b.dropTxTooLarge(sub, tx.CommitLSN, "filtered_count", len(filtered), capLimit)
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
		// nil-Filter path: schema-scope MatchedIndices in a single pass,
		// allocating a new slice only on the first cross-schema change.
		// TestRouteTxNilFilterUnchangedFastPath pins no-clone of tx.Changes
		// when every change belongs to subSchema.
		schemaIdx := indices
		cloned := false
		anchorSurvived := false
		for pos, i := range indices {
			raw := tx.Changes[i]
			if raw.Schema != subSchema {
				if !cloned {
					schemaIdx = make([]int, pos, len(indices))
					copy(schemaIdx, indices[:pos])
					cloned = true
				}
				continue
			}
			if matchesAnchor(sub, raw) {
				anchorSurvived = true
			}
			if cloned {
				schemaIdx = append(schemaIdx, i)
			}
		}
		if len(schemaIdx) == 0 || !anchorSurvived {
			return 0, 0
		}
		if len(schemaIdx) > capLimit {
			b.dropTxTooLarge(sub, tx.CommitLSN, "change_count", len(schemaIdx), capLimit)
			return 0, 0
		}
		if cloned {
			ev = Event{Tx: tx, MatchedIndices: schemaIdx}
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

	// Beyond-anchor delta: count delivered changes whose routing key differs
	// from the subscriber's anchor. KindExact compares ch.Key() against
	// "schema.table:pk"; KindWildcard compares ch.WildcardKey() against
	// "schema.table".
	isExact := sub.Kind() == KindExact
	anchorKey := subChannelKey(sub)
	var beyondAnchor int
	for _, idx := range ev.MatchedIndices {
		ch := ev.Tx.Changes[idx]
		routingKey := ch.WildcardKey()
		if isExact {
			routingKey = ch.Key()
		}
		if routingKey != anchorKey {
			beyondAnchor++
		}
	}

	return len(ev.MatchedIndices), beyondAnchor
}

// hasMultipleAnchorRoots reports whether tx contains >1 distinct PK on the
// (schema, table) anchor pair. A single-PK anchor table (one or more changes
// for the same row) is normal; two distinct anchor PKs in the same tx signals
// a writer-side discipline violation (spec §1.6, §4.4).
func hasMultipleAnchorRoots(tx wal.Tx, schema, table string) bool {
	var first string
	for _, ch := range tx.Changes {
		if ch.Schema != schema || ch.Table != table {
			continue
		}
		if first == "" {
			first = ch.PK
			continue
		}
		if ch.PK != first {
			return true
		}
	}
	return false
}

func (b *Broadcaster) dropTxTooLarge(sub *Subscriber, commitLSN pglogrepl.LSN, countField string, count, limit int) {
	b.metrics.TxDropped("tx_too_large").Inc()
	sub.Drop("tx_too_large")
	b.log.Warn().
		Str("subscriber_id", sub.ID()).
		Str("channel", subChannelKey(sub)).
		Int(countField, count).
		Int("limit", limit).
		Str("commit_lsn", commitLSN.String()).
		Msg("subscriber dropped: tx_too_large")
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
