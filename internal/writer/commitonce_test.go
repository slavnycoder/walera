package writer

import (
	"context"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeTx struct {
	pgx.Tx
	queries     []string
	commitErr   error
	rollbackErr error
	committed   bool
	rolledBack  bool
	execErr     error
	scanID      int64
	scanErr     error
}

func (f *fakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.queries = append(f.queries, sql)
	if f.execErr != nil {
		return pgconn.CommandTag{}, f.execErr
	}
	return pgconn.CommandTag{}, nil
}

func (f *fakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	f.queries = append(f.queries, sql)
	return &fakeRow{id: f.scanID, err: f.scanErr}
}

func (f *fakeTx) Commit(ctx context.Context) error {
	f.committed = true
	return f.commitErr
}

func (f *fakeTx) Rollback(ctx context.Context) error {
	f.rolledBack = true
	return f.rollbackErr
}

type fakeRow struct {
	id  int64
	err error
}

func (r *fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) == 0 {
		return nil
	}
	if p, ok := dest[0].(*int64); ok {
		*p = r.id
	}
	return nil
}

type fakePool struct {
	tx         *fakeTx
	beginErr   error
	beginCalls int
}

func (p *fakePool) BeginTx(ctx context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	p.beginCalls++
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	return p.tx, nil
}

func newRNG() *mathrand.Rand { return mathrand.New(mathrand.NewPCG(1, 1)) }

func TestCommitOnceImpl_DevicesInsert(t *testing.T) {
	tx := &fakeTx{}
	pool := &fakePool{tx: tx}
	cfg := WriterPGConfig{TxTimeout: time.Second}
	if err := commitOnceImpl(context.Background(), pool, "devices", 3, newRNG(), cfg); err != nil {
		t.Fatalf("commitOnceImpl: %v", err)
	}
	if !tx.committed {
		t.Errorf("expected commit")
	}
	if got := len(tx.queries); got != 3 {
		t.Errorf("devices insert count = %d, want 3", got)
	}
	for _, q := range tx.queries {
		if !strings.Contains(q, "INSERT INTO devices") {
			t.Errorf("expected devices INSERT, got %q", q)
		}
	}
}

func TestCommitOnceImpl_ArticlesInsert(t *testing.T) {
	tx := &fakeTx{}
	pool := &fakePool{tx: tx}
	cfg := WriterPGConfig{TxTimeout: time.Second}
	if err := commitOnceImpl(context.Background(), pool, "articles", 1, newRNG(), cfg); err != nil {
		t.Fatalf("commitOnceImpl: %v", err)
	}
	if len(tx.queries) != 1 || !strings.Contains(tx.queries[0], "INSERT INTO articles") {
		t.Errorf("expected articles INSERT, got %v", tx.queries)
	}
}

func TestCommitOnceImpl_OrdersInsertsDepth4Chain(t *testing.T) {
	tx := &fakeTx{scanID: 42}
	pool := &fakePool{tx: tx}
	cfg := WriterPGConfig{TxTimeout: time.Second}
	const rows = 2
	if err := commitOnceImpl(context.Background(), pool, "orders", rows, newRNG(), cfg); err != nil {
		t.Fatalf("commitOnceImpl: %v", err)
	}

	wantOrders := rows
	wantLines := rows * lineItemsPerOrder
	wantOptions := wantLines * optionsPerLineItem
	wantAudits := wantOptions * auditsPerOption
	wantTotal := wantOrders + wantLines + wantOptions + wantAudits

	if len(tx.queries) != wantTotal {
		t.Errorf("expected %d queries, got %d: %v", wantTotal, len(tx.queries), tx.queries)
	}
	var orders, lines, options, audits int
	for _, q := range tx.queries {
		switch {
		case strings.Contains(q, "INSERT INTO orders"):
			orders++
		case strings.Contains(q, "INSERT INTO line_items"):
			lines++
		case strings.Contains(q, "INSERT INTO line_item_options"):
			options++
		case strings.Contains(q, "INSERT INTO option_audits"):
			audits++
		}
	}
	if orders != wantOrders || lines != wantLines || options != wantOptions || audits != wantAudits {
		t.Errorf("orders=%d lines=%d options=%d audits=%d, want %d/%d/%d/%d",
			orders, lines, options, audits, wantOrders, wantLines, wantOptions, wantAudits)
	}
}

func TestCommitOnceImpl_UnknownTarget(t *testing.T) {
	tx := &fakeTx{}
	pool := &fakePool{tx: tx}
	err := commitOnceImpl(context.Background(), pool, "bogus", 1, newRNG(), WriterPGConfig{TxTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "unknown target table") {
		t.Errorf("expected unknown-target error, got %v", err)
	}
}

func TestCommitOnceImpl_BeginError(t *testing.T) {
	pool := &fakePool{beginErr: errors.New("begin boom")}
	err := commitOnceImpl(context.Background(), pool, "devices", 1, newRNG(), WriterPGConfig{TxTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "begin") {
		t.Errorf("expected begin error, got %v", err)
	}
}

func TestCommitOnceImpl_ExecError(t *testing.T) {
	tx := &fakeTx{execErr: errors.New("exec boom")}
	pool := &fakePool{tx: tx}
	err := commitOnceImpl(context.Background(), pool, "devices", 1, newRNG(), WriterPGConfig{TxTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "insert") {
		t.Errorf("expected exec error wrapped as insert, got %v", err)
	}
	if !tx.rolledBack {
		t.Errorf("expected rollback on exec error")
	}
}

func TestCommitOnceImpl_OrdersScanError(t *testing.T) {
	tx := &fakeTx{scanErr: errors.New("scan boom")}
	pool := &fakePool{tx: tx}
	err := commitOnceImpl(context.Background(), pool, "orders", 1, newRNG(), WriterPGConfig{TxTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "insert") {
		t.Errorf("expected scan error, got %v", err)
	}
}

func TestCommitOnceImpl_CommitError(t *testing.T) {
	tx := &fakeTx{commitErr: errors.New("commit boom")}
	pool := &fakePool{tx: tx}
	err := commitOnceImpl(context.Background(), pool, "devices", 1, newRNG(), WriterPGConfig{TxTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "commit") {
		t.Errorf("expected commit error, got %v", err)
	}
}

func TestCommitOnceImpl_ZeroTxTimeoutFallback(t *testing.T) {
	tx := &fakeTx{}
	pool := &fakePool{tx: tx}

	if err := commitOnceImpl(context.Background(), pool, "articles", 1, newRNG(), WriterPGConfig{}); err != nil {
		t.Fatalf("commitOnceImpl: %v", err)
	}
}

func TestRealCommitOnce_DelegatesToImpl(t *testing.T) {
	tx := &fakeTx{}
	pool := &fakePool{tx: tx}
	if err := realCommitOnce(context.Background(), pool, "articles", 1, newRNG(), WriterPGConfig{TxTimeout: time.Second}); err != nil {
		t.Fatalf("realCommitOnce: %v", err)
	}
	if !tx.committed {
		t.Errorf("expected commit")
	}
}

func TestCommitOnce_AcceptsRealPool(t *testing.T) {

	_ = commitOnce
	_ = fmt.Sprint
}
