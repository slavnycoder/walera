package router

import (
	"context"
	"sync/atomic"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
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
	closed   atomic.Bool
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
	return &Broadcaster{
		cfg:      cfg,
		log:      deps.Logger,
		metrics:  deps.Metrics,
		enc:      deps.Encoder,
		exact:    newIndex(),
		wildcard: newWildcardIndex(),
	}
}

func (b *Broadcaster) Register(sub *Subscriber) {
	// Add before checking closed: either Shutdown's snapshot sees sub or we see closed.
	defer func() {
		if b.closed.Load() {
			sub.Drop("shutdown")
		}
	}()
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
	eligible := make(map[*Subscriber]struct{}, 8)
	b.mergeMatches(tx, eligible)

	if len(eligible) == 0 {
		return
	}

	// fullIndices is allocated once and shared read-only across the sequential
	// dispatch loop (SAFE-01: no goroutine spawned in dispatchEvent).
	fullIndices := make([]int, len(tx.Changes))
	for i := range fullIndices {
		fullIndices[i] = i
	}

	for sub := range eligible {
		b.dispatchEvent(tx, sub, fullIndices)
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
// All Drop call sites must stay inside this function (INVARIANT #3).
func (b *Broadcaster) dispatchEvent(tx wal.Tx, sub *Subscriber, indices []int) {
	if tx.CommitLSN <= sub.StartLSN() {
		return
	}
	// Dropped but not yet deregistered: skip, or a full queue floods slow_consumer.
	select {
	case <-sub.Done():
		return
	default:
	}

	// No filter means no grant: never deliver unfiltered rows.
	if sub.Filter == nil {
		return
	}

	capLimit := b.cfg.MaxChangesPerTx
	subSchema := sub.Schema()
	subTable := sub.Table()

	// Wildcard subscribers may see every row; the guard only stops an exact
	// subscriber from receiving another root's rows and children.
	if sub.Kind() == KindExact && hasMultipleAnchorRoots(tx, subSchema, subTable) {
		b.log.Warn().
			Str("subscriber_id", sub.ID()).
			Str("channel", subChannelKey(sub)).
			Str("commit_lsn", tx.CommitLSN.String()).
			Msg("subscriber tx dropped: multi_root (writer-side discipline violation)")
		return
	}

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
		return // silent drop — no Drop() (INVARIANT #3)
	}
	// Cap is post-filter, not pre-filter: pre-filter would falsely drop
	// narrow-whitelist subscribers on large txs.
	if len(filtered) > capLimit {
		b.dropTxTooLarge(sub, tx.CommitLSN, "filtered_count", len(filtered), capLimit)
		return
	}
	subTx := tx
	subTx.Changes = filtered
	newIdx := make([]int, len(filtered))
	for i := range filtered {
		newIdx[i] = i
	}
	ev := Event{Tx: subTx, MatchedIndices: newIdx}

	frame, overflow := b.enc.Encode(ev)
	if overflow {
		sub.Drop("tx_too_large")
		b.log.Warn().
			Str("subscriber_id", sub.ID()).
			Str("commit_lsn", tx.CommitLSN.String()).
			Int("matched_changes", len(ev.MatchedIndices)).
			Msg("subscriber dropped: tx_too_large (payload cap)")
		return
	}

	if !sub.Send(frame) {
		sub.Drop("slow_consumer")
		b.log.Warn().
			Str("subscriber_id", sub.ID()).
			Str("reason", "slow_consumer").
			Str("commit_lsn", tx.CommitLSN.String()).
			Msg("subscriber dropped: slow_consumer")
	}
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

// Shutdown drops every subscriber with "shutdown"; Register drops any that
// arrive later. Handlers write the shutdown frame and exit on their own.
func (b *Broadcaster) Shutdown() {
	b.closed.Store(true)
	subs := append(b.exact.Snapshot(), b.wildcard.Snapshot()...)
	for _, s := range subs {
		s.Drop("shutdown")
	}
	b.log.Info().
		Int("subscribers", len(subs)).
		Msg("broadcaster shutdown: subscribers dropped")
}
