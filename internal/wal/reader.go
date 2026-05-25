// Package wal — reader.go implements the pglogrepl-based WAL replication reader.
//
// Reader connects to PostgreSQL in replication mode, creates a temporary logical
// replication slot with pgoutput, decodes the WAL stream into typed Tx values, and
// sends them on a channel for the consumer (cmd/cdc-sse/main.go) to process.
//
// Key invariants enforced here:
//  1. PrimaryKeepaliveMessage.ReplyRequested=true → immediate StandbyStatusUpdate.
//  2. proto_version '1' pinned in StartReplication PluginArgs.
//  3. TOAST/PK enforcement — handled by relationCache.Update; errors logged, table skipped.
//  4. safego.Go for the standby ticker goroutine.
//  5. txBuilder.Reset() + fresh relationCache on every Run() entry.
package wal

import (
	"context"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/safego"
)

// lastCommittedLSN is the LSN of the last successfully committed transaction.
// Stored as an atomic.Uint64 (pglogrepl.LSN is uint64 under the hood).
// Written by the reader goroutine after each Commit; read by the standby
// ticker goroutine and the inline ReplyRequested ACK path — both on
// separate goroutines.
var lastCommittedLSN atomic.Uint64

// CurrentLSN returns the LSN of the last successfully committed transaction.
// Safe to call from any goroutine.
func CurrentLSN() pglogrepl.LSN {
	return pglogrepl.LSN(lastCommittedLSN.Load())
}

// setLSN atomically updates lastCommittedLSN.
func setLSN(lsn pglogrepl.LSN) {
	lastCommittedLSN.Store(uint64(lsn))
}

// replConn is the minimal seam the Reader needs from its PostgreSQL
// replication connection. Production wires pgConnAdapter (which wraps
// *pgconn.PgConn); the test-only fakeReplConn in reader_test.go is the
// second implementation, driving the message-queue + ACK harness used by
// reader_test.go and reader_reconnect_test.go without a real PG
// replication connection. The interface also crosses the external network
// boundary (PostgreSQL replication protocol). IFACE-01 (b)+(c).
type replConn interface {
	// ReceiveMessage waits for the next backend message from PostgreSQL.
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
	// SendACK sends a StandbyStatusUpdate to PostgreSQL. Protected by r.connWriteMu
	// in the Reader; callers must NOT call this without holding the mutex.
	SendACK(ctx context.Context, ssu pglogrepl.StandbyStatusUpdate) error
}

// pgConnAdapter wraps a *pgconn.PgConn and adds the SendACK method to satisfy replConn.
type pgConnAdapter struct {
	conn *pgconn.PgConn
}

func (a *pgConnAdapter) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	return a.conn.ReceiveMessage(ctx)
}

func (a *pgConnAdapter) SendACK(ctx context.Context, ssu pglogrepl.StandbyStatusUpdate) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, a.conn, ssu)
}

// Reader decodes the PostgreSQL WAL stream and emits typed Tx values on a channel.
//
// Lifecycle:
//  1. Create with New(cfg, logger) — returns a *Reader and the read-only <-chan Tx.
//  2. Call Run(ctx) to connect, start replication, and block until ctx is cancelled
//     or an unrecoverable error occurs.
//  3. On Run() return (any reason), txCh is closed — the consumer's range loop exits.
type Reader struct {
	cfg  Config
	log  zerolog.Logger
	txCh chan Tx

	// replConn is the active replication connection. Reset between Run() calls.
	// Protected from concurrent writes by connWriteMu.
	replConn replConn

	// connWriteMu guards all writes to the replication connection. Both the
	// standby ticker goroutine and the inline ReplyRequested ACK path
	// acquire this mutex before calling SendACK.
	connWriteMu sync.Mutex

	// connected reflects whether the replication connection is currently
	// established. Toggled atomically inside runOnce: Store(true) after the
	// pgConn adapter is installed; deferred Store(false) on runOnce exit —
	// the outer Run sleep window must show /healthz "disconnected" while we
	// are backing off.
	connected atomic.Bool

	// metrics is the typed accessor registry threaded through from main.go.
	// Used by the outer Run to increment walera_pg_reconnects_total on every
	// transient-error retry, by processWALMessage to observe per-message
	// decode duration (walera_wal_decode_duration_seconds), and by
	// HandleCommit-adjacent code to observe tx size
	// (walera_wal_tx_size_changes). Always non-nil in production; tests use
	// metrics.New() which never registers on prometheus.DefaultRegisterer.
	metrics *metrics.Registry

	// rng is the per-Reader jitter source for computeBackoff. Seeded with
	// PCG (math/rand/v2) at construction so two Readers in the same process
	// do not share jitter. The Reader is single-goroutine for its own state
	// (Run is called once); rng access is not concurrent.
	rng *mathrand.Rand

	// runOnceFn is the inner-loop callback the outer Run drives. Defaults to
	// r.runOnce in production (set in New); unit tests inject a stub so the
	// reconnect / backoff behaviour can be exercised without a real PG
	// connection. The field is a tiny test-seam, not a public API.
	runOnceFn func(context.Context) error

	// computeBackoffFn returns the sleep duration for a given attempt index.
	// Defaults to r.computeBackoff in production (curve hard-coded); tests
	// substitute a short-curve version to keep wall-clock runtime bounded.
	computeBackoffFn func(attempt int) time.Duration
}

// Deps bundles the collaborators wal.New requires at construction time.
// Required fields panic on nil — constructor invariant. Logger is the
// value-type exception (zerolog.Logger zero value is a usable Nop logger).
type Deps struct {
	// Logger is the structured logger; zero value is a usable Nop logger so
	// this field has no nil-check.
	Logger zerolog.Logger
	// Metrics is the typed Prometheus registry. Required — the reader
	// observes decode duration, tx size, reconnect count, and ACK
	// failures via this registry.
	Metrics *metrics.Registry
}

// validateDeps panics with the canonical "wal.New: Deps.<Field> is required"
// message when any required Deps field is nil. Logger is exempt (value-type
// with usable zero value).
func validateDeps(d Deps) {
	if d.Metrics == nil {
		panic("wal.New: Deps.Metrics is required")
	}
}

// New creates a Reader and returns it together with a read-only channel that the
// caller ranges over to receive decoded Tx values.
//
// txCh is buffered at 128: the single WAL reader produces Tx values, the
// buffered channel absorbs brief router stalls, and a single router-ingest
// goroutine consumes from this channel and fans out to N SSE writers.
//
// deps.Metrics is the typed Prometheus registry threaded through from
// main.go. Must be non-nil (panics at construction); pass metrics.New() in
// tests if metric observation is not under test.
func New(cfg Config, deps Deps) (*Reader, <-chan Tx) {
	validateDeps(deps)
	txCh := make(chan Tx, 128)
	r := &Reader{
		cfg:     cfg,
		log:     deps.Logger,
		txCh:    txCh,
		metrics: deps.Metrics,
		// Seed jitter via math/rand/v2 PCG (deterministic per Reader,
		// distinct across Readers via (time.UnixNano, pid)).
		rng: mathrand.New(mathrand.NewPCG(uint64(time.Now().UnixNano()), uint64(os.Getpid()))),
	}
	r.runOnceFn = r.runOnce
	r.computeBackoffFn = r.computeBackoff
	return r, txCh
}

// IsConnected reports whether the replication connection is currently
// established. Safe to call from any goroutine. Consumed by /healthz.
// Returns false at construction and after Run exits; returns true while
// Run is actively executing replication.
func (r *Reader) IsConnected() bool {
	return r.connected.Load()
}

// Metrics returns the registry this Reader publishes counters into. Exposed
// so the composition-root singleton-identity test can compare the pointer
// every consumer received against the registry the composition root built.
func (r *Reader) Metrics() *metrics.Registry { return r.metrics }

// CheckPG reports the replication connection's health. Returns nil when
// the replication connection is currently open; returns ErrNotConnected
// otherwise. Implements internal/health.PgChecker.
//
// Currently a thin wrapper over IsConnected() so the prober can call
// through a context-aware interface without taking a wal dependency.
// The ctx parameter is accepted for forward compatibility (future:
// round-trip ping) but unused today.
func (r *Reader) CheckPG(_ context.Context) error {
	if r.connected.Load() {
		return nil
	}
	return ErrNotConnected
}

// newReaderForTest creates a Reader with a pre-supplied replConn for unit testing.
// The txCh has capacity 8 to avoid blocking in tests.
//
// metrics is metrics.New() — keeps the production accessor surface live so
// tests that call runLoop / runOnce don't panic on nil dereference.
func newReaderForTest(conn replConn, log zerolog.Logger) *Reader {
	txCh := make(chan Tx, 8)
	return &Reader{
		cfg:      Config{},
		log:      log,
		txCh:     txCh,
		replConn: conn,
		metrics:  metrics.New(),
		rng:      mathrand.New(mathrand.NewPCG(1, 2)),
	}
}

// Run is the outer backoff loop: on every transient error returned by
// runOnce it sleeps a jittered backoff and retries. The loop only exits
// when ctx is cancelled — k8s liveness (`/healthz` 503 once IsConnected()
// flips false) is the outer bound on the silent-gap window.
//
// CRITICAL INVARIANTS:
//   - `defer close(r.txCh)` lives HERE on outer Run, NEVER on runOnce.
//     Closing txCh on a transient error would EOF the router-ingest
//     goroutine and drop every subscriber.
//   - `connected.Store(false)` is deferred INSIDE runOnce so that the
//     outer-loop sleep window observes /healthz "disconnected".
//
// On ctx cancellation Run returns ctx.Err() WITHOUT incrementing
// reconnects — clean shutdown is not a transient failure.
func (r *Reader) Run(ctx context.Context) error {
	defer close(r.txCh)

	attempt := 0
	for {
		attemptStartedAt := time.Now()
		err := r.runOnceFn(ctx)

		// Clean shutdown — ctx cancellation propagated up.
		if ctx.Err() != nil {
			r.log.Info().Msg("WAL reader: clean shutdown")
			return ctx.Err()
		}

		// Reset rule: if the just-finished runOnce streamed for at least
		// ResetAfterSuccessDuration before failing, treat this transient blip
		// as fresh — next sleep starts at the curve's first step (1s).
		// IMPORTANT: apply BEFORE the inc/log so the "attempt" field in logs
		// reflects the post-reset value the operator will see.
		if r.cfg.Reconnect.ResetAfterSuccessDuration > 0 &&
			time.Since(attemptStartedAt) >= r.cfg.Reconnect.ResetAfterSuccessDuration {
			attempt = 0
		}

		r.metrics.PGReconnects().Inc()
		backoff := r.computeBackoffFn(attempt)
		// PII-safe fields only: attempt, backoff, err — NO DSN.
		r.log.Warn().Err(err).
			Int("attempt", attempt).
			Dur("backoff", backoff).
			Msg("WAL reader: transient error; reconnecting")

		// time.NewTimer + Stop is mandatory — `time.After` leaks a goroutine
		// per attempt until the timer fires.
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		}
		attempt++
	}
}

// computeBackoff returns the backoff duration for the given attempt index:
// curve `[1s, 2s, 4s, 8s, 16s, 30s]`, clamped at the last element, with
// ±25% full-jitter from r.rng. The curve lives in code (not config)
// because the values are tuned to typical PG reconnect RTT.
func (r *Reader) computeBackoff(attempt int) time.Duration {
	curve := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
	}
	idx := attempt
	if idx < 0 {
		idx = 0
	}
	if idx >= len(curve) {
		idx = len(curve) - 1
	}
	base := curve[idx]
	// Full-jitter ±25%: factor ∈ [0.75, 1.25].
	factor := 0.75 + r.rng.Float64()*0.5
	return time.Duration(float64(base) * factor)
}

// runOnce performs one complete connect → start replication → drain cycle.
// Returns nil only when runLoop returns nil (which it never does in
// practice — it only returns on ctx cancellation or terminal error). The
// caller (outer Run) loops on transient errors.
//
// Per-attempt invariants:
//   - Fresh PG connection, fresh slot, fresh txBuilder + relationCache (the
//     latter two are allocated inside runLoop already).
//   - `r.connected.Store(true)` flips AFTER the adapter is installed.
//   - `r.connected.Store(false)` is the FIRST deferred call so it runs LAST
//     (defer LIFO) — but more importantly, it covers every exit path
//     including the pgConn.Close defer below.
func (r *Reader) runOnce(ctx context.Context) error {
	// Any exit path (success, error, ctx cancel) flips the gauge.
	defer r.connected.Store(false)

	// Connect to PostgreSQL in replication mode. Cast walconn.ReplicationDSN
	// to string at the boundary — pgconn.Connect wants a bare string, and the
	// named-type discipline keeps the cast locally visible. The DSN already
	// carries the replication runtime parameter (DeriveDSNs sets it
	// unconditionally), so no further query-string manipulation is needed.
	connStr := string(r.cfg.ReplicationDSN)
	pgConn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("wal: replication connect: %w", err)
	}
	defer pgConn.Close(ctx) //nolint:errcheck

	r.replConn = &pgConnAdapter{conn: pgConn}
	// Mark connected — observable to scrapers (health.Server) from now
	// until runOnce exits via the deferred Store(false) above.
	r.connected.Store(true)

	// Slot bootstrap: IdentifySystem → CreateReplicationSlot → ParseLSN →
	// StartReplication. setLSN is invoked inside bootstrapSlot BEFORE
	// StartReplication so the standby ticker never observes a zero LSN.
	slotName, _, err := bootstrapSlot(ctx, pgConn, r.cfg, r.log)
	if err != nil {
		return err
	}
	r.log.Info().Str("slot", slotName).Msg("starting WAL reader")

	return r.runLoop(ctx)
}

// tickStandby performs one standby-ticker iteration: under the write lock
// it sends a StandbyStatusUpdate keepalive and, on any error other than
// context cancellation, increments the
// walera_wal_standby_ack_failures_total counter BEFORE the Warn log so the
// counter increments even if the logger misbehaves. The ticker continues
// running on transient errors. Factored out of runLoop's anonymous
// goroutine so unit tests can drive a single iteration without waiting on
// the 5-second wall-clock ticker.
//
// context.Canceled and context.DeadlineExceeded are NOT counted as ACK
// failures. They fire on graceful shutdown and on every reconnect
// (runLoop's defer tickerCancel()), and a few clean reconnects in quick
// succession would otherwise trigger the WaleraStandbyAckFailures alert
// which is calibrated for real ACK-failure runs.
func (r *Reader) tickStandby(ctx context.Context) {
	r.connWriteMu.Lock()
	defer r.connWriteMu.Unlock()
	if err := r.replConn.SendACK(ctx, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: CurrentLSN(),
		WALFlushPosition: CurrentLSN(),
		WALApplyPosition: CurrentLSN(),
		ClientTime:       time.Now(),
	}); err != nil {
		// ctx errors are normal shutdown/reconnect — log at Debug and do
		// not advance the standby-ack-failures counter.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			r.log.Debug().Err(err).Msg("standby ticker: SendACK cancelled (shutdown/reconnect)")
			return
		}
		r.metrics.WALStandbyACKFailures().Inc()
		r.log.Warn().Err(err).Msg("standby ticker: SendACK failed")
	}
}

// runLoop is the inner message-processing loop, separated for testability.
// It can be called directly with a pre-configured replConn for unit tests.
// It initialises fresh txBuilder and relationCache state and spawns the
// standby ticker goroutine.
func (r *Reader) runLoop(ctx context.Context) error {
	// Fresh state every time we enter the loop. This ensures no partial
	// transaction or stale relation info survives a reconnect.
	txBld := newTxBuilder()
	relCache := newRelationCache()

	// Spawn the standby ticker as a separate goroutine via safego.Go. The
	// ticker acquires connWriteMu before each ACK to prevent concurrent
	// writes with the inline ReplyRequested ACK path in the receive loop.
	tickerCtx, tickerCancel := context.WithCancel(ctx)
	defer tickerCancel()

	safego.Go("wal-standby-ticker", func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-tickerCtx.Done():
				return
			case <-ticker.C:
				r.tickStandby(tickerCtx)
			}
		}
	})

	// Receive loop.
	for {
		receiveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		rawMsg, err := r.replConn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			// pgconn.Timeout means the 10s window elapsed without a message — that is fine,
			// the ticker has already sent a keepalive. Continue waiting.
			if pgconn.Timeout(err) {
				continue
			}
			// Any other error (including ctx cancellation) is terminal for this run.
			txBld.Reset()
			return fmt.Errorf("wal: ReceiveMessage: %w", err)
		}

		// Only CopyData frames carry replication data.
		cd, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		if len(cd.Data) == 0 {
			continue
		}

		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				r.log.Warn().Err(err).Msg("parse PrimaryKeepaliveMessage failed")
				continue
			}
			handleKeepaliveMsg(pkm, r.sendACK)

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				r.log.Warn().Err(err).Msg("parse XLogData failed")
				continue
			}
			if err := r.processWALMessage(ctx, xld.WALData, txBld, relCache); err != nil {
				txBld.Reset()
				return fmt.Errorf("wal: processWALMessage: %w", err)
			}
		}
	}
}

// sendACK sends a StandbyStatusUpdate to PostgreSQL.
// Protected by connWriteMu — both the ticker goroutine and the inline
// ReplyRequested ACK path call this, so the mutex is acquired here.
func (r *Reader) sendACK(ctx context.Context) error {
	r.connWriteMu.Lock()
	defer r.connWriteMu.Unlock()
	return r.replConn.SendACK(ctx, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: CurrentLSN(),
		WALFlushPosition: CurrentLSN(),
		WALApplyPosition: CurrentLSN(),
		ClientTime:       time.Now(),
	})
}

// handleKeepaliveMsg handles a PrimaryKeepaliveMessage. If ReplyRequested
// is true, it calls sendACK immediately. Returns true if an ACK was sent.
//
// Extracted as a package-level function (unexported) so that
// TestReplyRequestedACK can call it directly without needing a live PG
// connection.
func handleKeepaliveMsg(pkm pglogrepl.PrimaryKeepaliveMessage, sendACK func(context.Context) error) bool {
	if pkm.ReplyRequested {
		// Send ACK inline — do not defer to the next ticker interval.
		_ = sendACK(context.Background())
		return true
	}
	return false
}

// processWALMessage decodes a WAL data payload from pglogrepl.Parse and dispatches
// it to the appropriate txBuilder handler.
//
// Observes walera_wal_decode_duration_seconds for every message via
// `prometheus.NewTimer(...).ObserveDuration()`. The timer covers parse +
// dispatch — i.e., the full per-message hot path.
func (r *Reader) processWALMessage(ctx context.Context, walData []byte, txBld *txBuilder, relCache *relationCache) error {
	// One observation per WAL message.
	decodeTimer := prometheus.NewTimer(r.metrics.WALDecodeDuration())
	defer decodeTimer.ObserveDuration()

	if len(walData) == 0 {
		r.log.Warn().Msg("processWALMessage: empty WAL data; skipping")
		return nil
	}
	msg, err := pglogrepl.Parse(walData)
	if err != nil {
		r.log.Warn().Err(err).Msg("pglogrepl.Parse failed; skipping WAL message")
		return nil
	}

	switch m := msg.(type) {
	case *pglogrepl.BeginMessage:
		txBld.HandleBegin(m)

	case *pglogrepl.RelationMessage:
		if err := txBld.HandleRelation(m, relCache); err != nil {
			// Relation errors (errCompositePK, errUnsupportedPKType) are non-fatal:
			// the table is not supported by Walera and will be skipped.
			r.log.Warn().
				Err(err).
				Str("table", m.Namespace+"."+m.RelationName).
				Msg("relation skipped: PK constraint not met")
		}

	case *pglogrepl.InsertMessage:
		if err := txBld.HandleInsert(m, relCache); err != nil {
			r.log.Warn().Err(err).Msg("HandleInsert error; change skipped")
		}

	case *pglogrepl.UpdateMessage:
		if err := txBld.HandleUpdate(m, relCache); err != nil {
			r.log.Warn().Err(err).Msg("HandleUpdate error; change skipped")
		}

	case *pglogrepl.DeleteMessage:
		if err := txBld.HandleDelete(m, relCache); err != nil {
			r.log.Warn().Err(err).Msg("HandleDelete error; change skipped")
		}

	case *pglogrepl.CommitMessage:
		tx := txBld.HandleCommit(m)
		if tx != nil {
			setLSN(m.CommitLSN)
			// Observe per-tx change count BEFORE the channel send. The send
			// may block on ctx.Done; the observation should reflect what we
			// decoded regardless of delivery success.
			r.metrics.WALTxSizeChanges().Observe(float64(len(tx.Changes)))
			select {
			case r.txCh <- *tx:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

	case *pglogrepl.TypeMessage:
		// Type messages are informational; no action needed.
		r.log.Debug().Msg("TypeMessage received; ignored")

	case *pglogrepl.OriginMessage:
		// Origin messages indicate the origin server for cascaded replication; ignored.
		r.log.Debug().Msg("OriginMessage received; ignored")

	case *pglogrepl.TruncateMessage:
		// TRUNCATE is not a DML change we track.
		r.log.Debug().Int("relation_count", len(m.RelationIDs)).Msg("TruncateMessage received; ignored")

	default:
		r.log.Debug().Str("type", fmt.Sprintf("%T", msg)).Msg("unhandled WAL message type")
	}

	return nil
}
