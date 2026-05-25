// Package router — router.go: Broadcaster fan-out engine. See doc.go invariants 1-7 and INVARIANTS.md §Concurrency.
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

// encoderIface is the package-private seam over the Event-to-wire encoder.
// It is intentionally distinct from internal/sse's same-named control-frame
// encoderIface (EncodeHeartbeat/EncodeShutdown/EncodeError): the two
// interfaces have non-overlapping method sets and serve different roles.
// router must not import sse; unifying them would invert the dependency
// direction. See INVARIANTS.md §Concurrency "encoderIface decoupling".
type encoderIface interface {
	Encode(Event) ([]byte, bool)
}

// Broadcaster owns the routing data plane (exact + wildcard indexes) and
// the Ingest loop. Construct via New. Register/Deregister are concurrent-safe;
// Ingest is single-goroutine. See doc.go invariants 1-7.
type Broadcaster struct {
	cfg      Config
	log      zerolog.Logger
	metrics  *metrics.Registry
	enc      encoderIface
	exact    *index
	wildcard *wildcardIndex
}

// Deps bundles Broadcaster collaborators. Metrics + Encoder required (panic on nil);
// Logger is value-type with usable Nop zero value.
type Deps struct {
	// Logger is the structured logger; zero value is a usable Nop.
	Logger zerolog.Logger
	// Metrics is the typed Prometheus registry. Required.
	Metrics *metrics.Registry
	// Encoder produces SSE wire bytes from a router.Event. Required. Production
	// passes sse.NewEncoder(cfg.Http.MaxPayloadBytes); tests stub.
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

// New constructs a Broadcaster with empty indexes. No error return — there
// are no failure modes at construction. Pre-touches the three walera_tx_dropped_total
// reason series ("slow_consumer", "tx_too_large", "multi_root") so they appear in
// Gather() from t=0; see INVARIANTS.md §Concurrency for the multi_root pre-touch site.
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

// Register inserts sub into the matching index and increments
// walera_subscribers_active{type=<kind>}. Called by the SSE handler;
// Deregister is the writer-side counterpart (single-owner cleanup, doc.go #7).
// Never logs row data. Approved fields: subscriber_id, kind, channel, start_lsn.
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

// Deregister removes sub from the matching index and decrements
// walera_subscribers_active{type=<kind>}. Single-owner cleanup: invoked
// ONLY by the SSE writer goroutine's defer (doc.go #7). Reason logged is
// sub.Reason() — empty string on a clean client close.
func (b *Broadcaster) Deregister(sub *Subscriber) {
	reason := sub.Reason()
	switch sub.Kind() {
	case KindExact:
		key := sub.Schema() + "." + sub.Table() + ":" + sub.PK()
		b.exact.Remove(key)
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

// Ingest is the single-goroutine consumer of txCh. Returns nil on txCh
// close, ctx.Err() on cancellation. Single-reader invariant (doc.go #1) —
// callers MUST not invoke Ingest from more than one goroutine.
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

func (b *Broadcaster) matchExact(key string) *Subscriber {
	return b.exact.Lookup(key)
}

func (b *Broadcaster) matchWildcard(key string) []*Subscriber {
	return b.wildcard.Lookup(key)
}

func (b *Broadcaster) mergeMatches(tx wal.Tx, matched map[*Subscriber][]int) {
	for i, ch := range tx.Changes {
		if sub := b.matchExact(ch.Key()); sub != nil {
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

// ExactLen returns the total number of registered exact subscribers across
// all 8 shards. Sampled by the metrics-sampler to update
// walera_routing_index_size{index_kind="exact"}.
// Metrics returns the registry this Broadcaster publishes counters into;
// exposed for the composition-root singleton-identity test.
func (b *Broadcaster) Metrics() *metrics.Registry { return b.metrics }

func (b *Broadcaster) ExactLen() int { return b.exact.Len() }

// WildcardLen returns the total number of registered wildcard subscribers
// across every "<schema>.<table>" key. Sampled by the metrics-sampler to
// update walera_routing_index_size{index_kind="wildcard"}.
func (b *Broadcaster) WildcardLen() int { return b.wildcard.Len() }

// Shutdown drains every active subscriber by calling sub.Drop("shutdown")
// and waiting for each subscriber's writer to complete (sub.Done() closing).
// Returns nil on clean drain, context.DeadlineExceeded on drainDeadline
// expiry, or ctx.Err() if the parent context is cancelled first.
//
// Snapshots both indexes once under copy-before-unlock, fans out per-sub via
// safego.Go (doc.go #8), and bounds total wait by drainDeadline + ctx.Done().
// Called CONCURRENTLY with http.Server.Shutdown in cmd/cdc-sse/main.go —
// the shutdown frame + conn close lets srv.Shutdown's StateActive→StateIdle
// poller make progress on otherwise-pinned SSE connections.
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
