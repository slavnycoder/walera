// Package router — subscriber.go: per-connection Subscriber lifecycle. See doc.go invariants 5-7.
package router

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"

	"github.com/jackc/pglogrepl"

	"github.com/walera/walera/internal/wal"
)

// Kind discriminates exact vs wildcard subscribers. The string value doubles
// as the "type" label value on subscriber/event metrics.
type Kind string

const (
	// KindExact is the single-PK subscription kind ("/sse/v1/{table}/{pk}").
	KindExact Kind = "exact"
	// KindWildcard is the whole-table subscription kind ("/sse/v1/{table}/all").
	KindWildcard Kind = "wildcard"
)

// SubscriberConfig holds the value-type fields for NewSubscriber. All
// fields are required except ID (auto-generated when empty) and PK (empty
// for KindWildcard).
type SubscriberConfig struct {
	// ID is the subscriber identifier. When empty, NewSubscriber generates
	// a 32-char hex value via crypto/rand.
	ID string
	// Kind discriminates exact vs wildcard (KindExact | KindWildcard).
	Kind Kind
	// Schema is the PostgreSQL schema name (hardcoded to "public" by the
	// SSE handler — see spec §3.2).
	Schema string
	// Table is the bare table name.
	Table string
	// PK is the primary-key value for exact subscriptions; empty for wildcards.
	PK string
	// StartLSN is the lower bound for tx delivery — tx.CommitLSN > StartLSN
	// is required for the router to send.
	StartLSN pglogrepl.LSN
	// BufferCap is INFORMATIONAL ONLY — the pool owns the per-sub queue via
	// PoolConfig.SubQueueSize (http.sub_queue_size). Silently ignored here.
	BufferCap int
}

// SubscriberDeps holds the collaborator-type fields for NewSubscriber.
type SubscriberDeps struct {
	// Parent is the context whose cancellation propagates to the subscriber.
	// nil falls back to context.Background.
	Parent context.Context
}

// Subscriber owns one SSE connection's lifecycle state inside the router.
// Construct via NewSubscriber. Drop is idempotent (sync.Once) and stores reason
// BEFORE cancel so observers of ctx.Done() can read Reason() reliably.
// Single-owner cleanup (doc.go #7): the router never deregisters; cancel-not-close
// discipline (doc.go #6): Drop never closes a channel.
type Subscriber struct {
	id       string
	kind     Kind
	schema   string
	table    string
	pk       string
	startLSN pglogrepl.LSN

	sendFunc atomic.Value // holds func([]byte) bool

	ctx    context.Context
	cancel context.CancelFunc

	reasonOnce sync.Once
	// see INVARIANTS.md §Concurrency for the sticky-reason atomic.Pointer rationale.
	reasonPtr atomic.Pointer[string]

	// Filter is the optional per-change authorization filter. When nil (fast
	// path) the router preserves the original tx.Changes slice identity in the
	// emitted Event (zero extra allocation). When non-nil the router invokes
	// Filter on each matched change, dropping fully-filtered subscribers
	// silently (no metric, no log) and otherwise cloning the tx with the
	// retained changes. Production wiring is `sub.Filter = authSub.FilterWithLSN`.
	//
	// Concurrency: assign BEFORE Broadcaster.Register. Subsequent permission
	// refreshes go through auth.Subscriber.swapMap (atomic.Pointer back-buffer);
	// do NOT swap Filter itself after Register.
	Filter func(c wal.Change, txCommitLSN pglogrepl.LSN) (wal.Change, bool)
}

// NewSubscriber constructs a Subscriber. Empty cfg.ID auto-generates a
// 32-char hex id via crypto/rand; nil deps.Parent falls back to context.Background.
// cfg.BufferCap is IGNORED (pool owns the queue). Panics only on crypto/rand failure.
func NewSubscriber(cfg SubscriberConfig, deps SubscriberDeps) *Subscriber {
	id := cfg.ID
	if id == "" {
		var buf [16]byte
		if _, err := rand.Read(buf[:]); err != nil {
			panic("router: crypto/rand.Read failed: " + err.Error())
		}
		id = hex.EncodeToString(buf[:])
	}
	parent := deps.Parent
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &Subscriber{
		id:       id,
		kind:     cfg.Kind,
		schema:   cfg.Schema,
		table:    cfg.Table,
		pk:       cfg.PK,
		startLSN: cfg.StartLSN,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// ID returns the subscriber's identifier (hex-encoded 16 random bytes when
// auto-generated).
func (s *Subscriber) ID() string { return s.id }

// Kind returns KindExact or KindWildcard.
func (s *Subscriber) Kind() Kind { return s.kind }

// KindString returns the Kind as a plain string, for metric-label callers
// (notably the SSE pool) that should not import the router.Kind type.
func (s *Subscriber) KindString() string { return string(s.kind) }

// Schema returns the PostgreSQL schema for this subscription ("public").
func (s *Subscriber) Schema() string { return s.schema }

// Table returns the bare table name for this subscription.
func (s *Subscriber) Table() string { return s.table }

// PK returns the primary-key value for an exact subscription, or empty for
// a wildcard subscription.
func (s *Subscriber) PK() string { return s.pk }

// StartLSN returns the lower bound for tx delivery. The router silently
// skips any tx whose CommitLSN <= StartLSN.
func (s *Subscriber) StartLSN() pglogrepl.LSN { return s.startLSN }

// WireSendFunc installs the per-sub frame-delivery closure (returns false
// on queue-full → router emits Drop("slow_consumer")). MUST be installed
// BEFORE broadcaster.Register; atomic.Value store makes test-side rewires
// race-clean (production wires exactly once via pool.Attach).
func (s *Subscriber) WireSendFunc(fn func(frame []byte) bool) {
	s.sendFunc.Store(fn)
}

func (s *Subscriber) send(frame []byte) bool {
	v := s.sendFunc.Load()
	if v == nil {
		return false
	}
	return v.(func([]byte) bool)(frame)
}

// Done returns the channel that fires when the subscriber's context is
// cancelled (equivalent to s.ctx.Done()). Writer reads Reason() on Done()
// to decide whether to emit a best-effort error frame.
func (s *Subscriber) Done() <-chan struct{} { return s.ctx.Done() }

// Reason returns the sticky disconnect reason set by Drop, or "" if the
// subscriber has not been dropped. Safe to call from any goroutine — backed
// by atomic.Pointer[string].
func (s *Subscriber) Reason() string {
	p := s.reasonPtr.Load()
	if p == nil {
		return ""
	}
	return *p
}

// Drop signals teardown with the given reason. Reason stored via
// atomic.Pointer[string] BEFORE cancel; sync.Once guards against double-drop
// (router slow_consumer racing writer client_closed). Drop NEVER closes any
// channel — pool worker owns the per-sub queue (doc.go #6).
func (s *Subscriber) Drop(reason string) {
	s.reasonOnce.Do(func() {
		r := reason
		s.reasonPtr.Store(&r)
		s.cancel()
	})
}
