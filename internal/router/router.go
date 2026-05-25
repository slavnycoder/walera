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

	matched := make(map[*Subscriber][]int, 8)
	b.mergeMatches(tx, matched)

	b.metrics.RoutingFanOut().Observe(float64(len(matched)))

	for sub, indices := range matched {
		b.dispatchEvent(tx, sub, indices)
	}
}

func (b *Broadcaster) matchExact(key string) []*Subscriber {
	return b.exact.Lookup(key)
}

func (b *Broadcaster) matchWildcard(key string) []*Subscriber {
	return b.wildcard.Lookup(key)
}

func (b *Broadcaster) mergeMatches(tx wal.Tx, matched map[*Subscriber][]int) {
	for i, ch := range tx.Changes {
		for _, sub := range b.matchExact(ch.Key()) {
			matched[sub] = append(matched[sub], i)
		}
		for _, sub := range b.matchWildcard(ch.WildcardKey()) {
			matched[sub] = append(matched[sub], i)
		}
	}
}

func (b *Broadcaster) dispatchEvent(tx wal.Tx, sub *Subscriber, indices []int) {
	if tx.CommitLSN <= sub.StartLSN() {
		return
	}

	capLimit := b.cfg.MaxChangesPerTx
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
		return
	}

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
			return
		}
		subTx := tx
		subTx.Changes = filtered
		newIdx := make([]int, len(filtered))
		for i := range filtered {
			newIdx[i] = i
		}
		ev = Event{Tx: subTx, MatchedIndices: newIdx}
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
		return
	}

	if !sub.send(frame) {
		b.metrics.TxDropped("slow_consumer").Inc()
		sub.Drop("slow_consumer")
		b.log.Warn().
			Str("subscriber_id", sub.ID()).
			Str("reason", "slow_consumer").
			Str("commit_lsn", tx.CommitLSN.String()).
			Msg("subscriber dropped: slow_consumer")
		return
	}
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
