package wal

import (
	"context"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"
)

var lastCommittedLSN atomic.Uint64

func CurrentLSN() pglogrepl.LSN {
	return pglogrepl.LSN(lastCommittedLSN.Load())
}

func setLSN(lsn pglogrepl.LSN) {
	lastCommittedLSN.Store(uint64(lsn))
}

type replConn interface {
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)

	SendACK(ctx context.Context, ssu pglogrepl.StandbyStatusUpdate) error
}

type pgConnAdapter struct {
	conn *pgconn.PgConn
}

func (a *pgConnAdapter) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	return a.conn.ReceiveMessage(ctx)
}

// standbyWriteTimeout bounds a standby update write: pglogrepl ignores ctx, so
// a stuck socket would otherwise block the reader (and shutdown) forever.
var standbyWriteTimeout = 10 * time.Second

func (a *pgConnAdapter) SendACK(ctx context.Context, ssu pglogrepl.StandbyStatusUpdate) error {
	nc := a.conn.Conn()
	_ = nc.SetWriteDeadline(time.Now().Add(standbyWriteTimeout))
	defer func() { _ = nc.SetWriteDeadline(time.Time{}) }()
	return pglogrepl.SendStandbyStatusUpdate(ctx, a.conn, ssu)
}

type Reader struct {
	cfg  Config
	log  zerolog.Logger
	txCh chan Tx

	replConn replConn

	standbyInterval time.Duration

	connected atomic.Bool

	rng *mathrand.Rand

	runOnceFn func(context.Context) error

	computeBackoffFn func(attempt int) time.Duration
}

type Deps struct {
	Logger zerolog.Logger
}

func New(cfg Config, deps Deps) (*Reader, <-chan Tx) {
	txCh := make(chan Tx, 128)
	r := &Reader{
		cfg:  cfg,
		log:  deps.Logger,
		txCh: txCh,

		rng: mathrand.New(mathrand.NewPCG(uint64(time.Now().UnixNano()), uint64(os.Getpid()))), //nolint:gosec // G404: reconnect backoff jitter, not security-sensitive
	}
	r.runOnceFn = r.runOnce
	r.computeBackoffFn = r.computeBackoff
	return r, txCh
}

func (r *Reader) IsConnected() bool {
	return r.connected.Load()
}

func (r *Reader) CheckPG(_ context.Context) error {
	if r.connected.Load() {
		return nil
	}
	return ErrNotConnected
}

func newReaderForTest(conn replConn, log zerolog.Logger) *Reader {
	txCh := make(chan Tx, 8)
	return &Reader{
		cfg:      Config{},
		log:      log,
		txCh:     txCh,
		replConn: conn,
		rng:      mathrand.New(mathrand.NewPCG(1, 2)), //nolint:gosec // G404: deterministic seed for test reproducibility, not security-sensitive
	}
}

func (r *Reader) Run(ctx context.Context) error {
	defer close(r.txCh)

	attempt := 0
	for {
		attemptStartedAt := time.Now()
		err := r.runOnceFn(ctx)

		if ctx.Err() != nil {
			r.log.Info().Msg("WAL reader: clean shutdown")
			return ctx.Err()
		}

		if r.cfg.Reconnect.ResetAfterSuccessDuration > 0 &&
			time.Since(attemptStartedAt) >= r.cfg.Reconnect.ResetAfterSuccessDuration {
			attempt = 0
		}

		backoff := r.computeBackoffFn(attempt)

		r.log.Warn().Err(err).
			Int("attempt", attempt).
			Dur("backoff", backoff).
			Msg("WAL reader: transient error; reconnecting")

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

	factor := 0.75 + r.rng.Float64()*0.5
	return time.Duration(float64(base) * factor)
}

func (r *Reader) runOnce(ctx context.Context) error {

	defer r.connected.Store(false)

	connStr := string(r.cfg.ReplicationDSN)
	pgConn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("wal: replication connect: %w", err)
	}
	defer pgConn.Close(ctx) //nolint:errcheck

	r.replConn = &pgConnAdapter{conn: pgConn}

	r.connected.Store(true)

	slotName, _, err := bootstrapSlot(ctx, pgConn, r.cfg, r.log)
	if err != nil {
		return err
	}
	r.log.Info().Str("slot", slotName).Msg("starting WAL reader")

	return r.runLoop(ctx)
}

const defaultStandbyInterval = 5 * time.Second

func (r *Reader) standbyEvery() time.Duration {
	if r.standbyInterval > 0 {
		return r.standbyInterval
	}
	return defaultStandbyInterval
}

// runLoop is the only goroutine that touches replConn: PgConn is not safe for
// concurrent use, and a receive deadline firing breaks any in-flight write.
func (r *Reader) runLoop(ctx context.Context) error {

	txBld := newTxBuilderWithConfig(r.cfg)
	relCache := newRelationCache()
	nextStandby := time.Now().Add(r.standbyEvery())

	for {
		if !time.Now().Before(nextStandby) {
			r.sendStandby(ctx)
			nextStandby = time.Now().Add(r.standbyEvery())
		}

		receiveCtx, cancel := context.WithDeadline(ctx, nextStandby)
		rawMsg, err := r.replConn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if receiveTimedOut(ctx, err) {
				continue
			}

			txBld.Reset()
			return fmt.Errorf("wal: ReceiveMessage: %w", err)
		}

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
			if handleKeepaliveMsg(pkm, r.sendStandby) {
				nextStandby = time.Now().Add(r.standbyEvery())
			}

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

// receiveTimedOut is true when our own standby deadline fired; the connection
// is still usable then, unlike shutdown or a broken socket.
func receiveTimedOut(ctx context.Context, err error) bool {
	return ctx.Err() == nil && (pgconn.Timeout(err) || errors.Is(err, context.DeadlineExceeded))
}

func (r *Reader) sendStandby(ctx context.Context) {
	err := r.replConn.SendACK(ctx, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: CurrentLSN(),
		WALFlushPosition: CurrentLSN(),
		WALApplyPosition: CurrentLSN(),
		ClientTime:       time.Now(),
	})
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		r.log.Debug().Err(err).Msg("standby status update cancelled (shutdown/reconnect)")
		return
	}
	r.log.Warn().Err(err).Msg("standby status update failed")
}

// deliver hands a committed tx to the router. While the router is busy the
// reader still owes Postgres standby updates, so they go out from here (same
// goroutine; the connection keeps a single owner).
func (r *Reader) deliver(ctx context.Context, tx Tx) error {
	t := time.NewTicker(r.standbyEvery())
	defer t.Stop()
	for {
		select {
		case r.txCh <- tx:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			r.sendStandby(ctx)
		}
	}
}

func handleKeepaliveMsg(pkm pglogrepl.PrimaryKeepaliveMessage, sendStandby func(context.Context)) bool {
	if pkm.ReplyRequested {
		sendStandby(context.Background())
		return true
	}
	return false
}

func (r *Reader) processWALMessage(ctx context.Context, walData []byte, txBld *txBuilder, relCache *relationCache) error {
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
			if err := r.deliver(ctx, *tx); err != nil {
				return err
			}
		}

	case *pglogrepl.TypeMessage:

		r.log.Debug().Msg("TypeMessage received; ignored")

	case *pglogrepl.OriginMessage:

		r.log.Debug().Msg("OriginMessage received; ignored")

	case *pglogrepl.TruncateMessage:

		r.log.Debug().Int("relation_count", len(m.RelationIDs)).Msg("TruncateMessage received; ignored")

	default:
		r.log.Debug().Str("type", fmt.Sprintf("%T", msg)).Msg("unhandled WAL message type")
	}

	return nil
}
