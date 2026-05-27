package router

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/wal"
)

type stubEncoder struct {
	overflow bool
	mu       sync.Mutex
	last     Event
}

func (e *stubEncoder) Encode(ev Event) ([]byte, bool) {
	if e.overflow {
		return nil, true
	}
	e.mu.Lock()
	e.last = ev
	e.mu.Unlock()
	pks := make([]string, 0, len(ev.MatchedIndices))
	for _, i := range ev.MatchedIndices {
		if i >= 0 && i < len(ev.Tx.Changes) {
			pks = append(pks, ev.Tx.Changes[i].PK)
		}
	}
	body, _ := json.Marshal(map[string]any{
		"tx_id":      ev.Tx.ID,
		"commit_lsn": ev.Tx.CommitLSN.String(),
		"pks":        pks,
	})
	return body, false
}

func (e *stubEncoder) lastEvent() Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.last
}

type recordedSub struct {
	sub    *Subscriber
	mu     sync.Mutex
	frames [][]byte
	events []Event
	limit  int
	enc    *stubEncoder
}

func (r *recordedSub) useEncoder(enc *stubEncoder) {
	r.mu.Lock()
	r.enc = enc
	r.mu.Unlock()
}

func mkChange(op wal.Op, schema, table, pk string) wal.Change {
	c := wal.Change{
		Schema: schema,
		Table:  table,
		Op:     op,
		PK:     pk,
		PKCol:  "id",
	}
	switch op {
	case wal.OpInsert:
		c.Data = map[string]any{"id": pk}
	case wal.OpUpdate:
		c.Changed = map[string]any{"id": pk}
	}
	return c
}

func mkTx(commitLSN pglogrepl.LSN, txID uint32, changes ...wal.Change) wal.Tx {
	return wal.Tx{
		ID:        txID,
		CommitLSN: commitLSN,
		CommitTS:  time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC),
		Changes:   changes,
	}
}

var (
	recordersMu sync.Mutex
	recorders   = map[*Subscriber]*recordedSub{}
)

func mkExactSub(schema, table, pk string, startLSN pglogrepl.LSN, bufCap int) *Subscriber {
	rec := newRecordedExactSubWithPK(schema, table, pk, startLSN, bufCap)
	return rec.sub
}

func mkWildcardSub(schema, table string, startLSN pglogrepl.LSN, bufCap int) *Subscriber {
	rec := newRecordedWildcardSubWithCap(schema, table, startLSN, bufCap)
	return rec.sub
}

func newRecordedExactSubWithPK(schema, table, pk string, startLSN pglogrepl.LSN, limit int) *recordedSub {
	s := NewSubscriber(
		SubscriberConfig{
			Kind:     KindExact,
			Schema:   schema,
			Table:    table,
			PK:       pk,
			StartLSN: startLSN,
		},
		SubscriberDeps{Parent: context.Background()},
	)
	rec := &recordedSub{sub: s, limit: limit}
	s.WireSendFunc(recordingSendFunc(rec))
	recordersMu.Lock()
	recorders[s] = rec
	recordersMu.Unlock()
	return rec
}

func newRecordedWildcardSubWithCap(schema, table string, startLSN pglogrepl.LSN, limit int) *recordedSub {
	s := NewSubscriber(
		SubscriberConfig{
			Kind:     KindWildcard,
			Schema:   schema,
			Table:    table,
			StartLSN: startLSN,
		},
		SubscriberDeps{Parent: context.Background()},
	)
	rec := &recordedSub{sub: s, limit: limit}
	s.WireSendFunc(recordingSendFunc(rec))
	recordersMu.Lock()
	recorders[s] = rec
	recordersMu.Unlock()
	return rec
}

func recordingSendFunc(rec *recordedSub) func(frame []byte) bool {
	return func(frame []byte) bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if rec.limit > 0 && len(rec.frames) >= rec.limit {
			return false
		}
		fc := make([]byte, len(frame))
		copy(fc, frame)
		rec.frames = append(rec.frames, fc)
		if rec.enc != nil {
			rec.events = append(rec.events, rec.enc.lastEvent())
		}
		return true
	}
}

func recordedFor(t *testing.T, sub *Subscriber) *recordedSub {
	t.Helper()
	recordersMu.Lock()
	rec, ok := recorders[sub]
	recordersMu.Unlock()
	if !ok {
		t.Fatalf("recordedFor: no recorder for subscriber %s — was the sub built via NewSubscriber directly?", sub.ID())
	}
	return rec
}

func mkBroadcaster(maxChanges int) (*Broadcaster, *metrics.Registry) {
	m := metrics.New()
	b := New(Config{
		ExactBuffer:       64,
		WildcardBuffer:    512,
		MaxChangesPerTx:   maxChanges,
		HeartbeatInterval: 15 * time.Second,
	}, Deps{
		Logger:  zerolog.Nop(),
		Metrics: m,
		Encoder: &stubEncoder{},
	})
	return b, m
}

func gatherCounter(t *testing.T, reg *metrics.Registry, name, labelKey, labelVal string) float64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if matchLabel(m, labelKey, labelVal) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func gatherGauge(t *testing.T, reg *metrics.Registry, name, labelKey, labelVal string) float64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if matchLabel(m, labelKey, labelVal) {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

func gatherHasSeries(t *testing.T, reg *metrics.Registry, name, labelKey, labelVal string) bool {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if matchLabel(m, labelKey, labelVal) {
				return true
			}
		}
	}
	return false
}

func matchLabel(m *dto.Metric, key, val string) bool {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == key && lp.GetValue() == val {
			return true
		}
	}
	return false
}

func drainOne(t *testing.T, sub *Subscriber, timeout time.Duration) Event {
	t.Helper()
	rec := recordedFor(t, sub)
	deadline := time.Now().Add(timeout)
	for {
		rec.mu.Lock()
		framesLen := len(rec.frames)
		eventsLen := len(rec.events)
		rec.mu.Unlock()

		if framesLen > 0 {
			rec.mu.Lock()
			var ev Event
			if eventsLen > 0 {
				ev = rec.events[0]
				rec.events = rec.events[1:]
			}
			frame := rec.frames[0]
			rec.frames = rec.frames[1:]
			rec.mu.Unlock()
			if eventsLen > 0 {
				return ev
			}

			return synthesizeEvent(t, frame)
		}
		if time.Now().After(deadline) {
			t.Fatalf("subscriber %s: no event within %v", sub.ID(), timeout)
			return Event{}
		}
		time.Sleep(time.Millisecond)
	}
}

func synthesizeEvent(t *testing.T, frame []byte) Event {
	t.Helper()
	var m struct {
		TxID      uint32   `json:"tx_id"`
		CommitLSN string   `json:"commit_lsn"`
		PKs       []string `json:"pks"`
	}
	if err := json.Unmarshal(frame, &m); err != nil {
		t.Fatalf("synthesizeEvent: unmarshal: %v (raw=%q)", err, frame)
	}
	lsn, err := pglogrepl.ParseLSN(m.CommitLSN)
	if err != nil {
		t.Fatalf("synthesizeEvent: ParseLSN(%q): %v", m.CommitLSN, err)
	}
	indices := make([]int, len(m.PKs))
	for i := range m.PKs {
		indices[i] = i
	}
	return Event{
		Tx:             wal.Tx{ID: m.TxID, CommitLSN: lsn},
		MatchedIndices: indices,
	}
}

func expectNoFrame(t *testing.T, sub *Subscriber, timeout time.Duration) {
	t.Helper()
	rec := recordedFor(t, sub)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		got := len(rec.frames)
		rec.mu.Unlock()
		if got > 0 {
			t.Errorf("subscriber %s: unexpected frame delivered (%d total)", sub.ID(), got)
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func runIngest(b *Broadcaster, ctx context.Context, txCh <-chan wal.Tx) (<-chan struct{}, *error) {
	done := make(chan struct{})
	var retErr error
	go func() {
		retErr = b.Ingest(ctx, txCh)
		close(done)
	}()
	return done, &retErr
}

func sendTx(t *testing.T, txCh chan<- wal.Tx, tx wal.Tx) {
	t.Helper()
	select {
	case txCh <- tx:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("timeout pushing tx %d onto txCh", tx.ID)
	}
}

func TestBroadcaster_ExactFanOut_SingleMatch(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	b.Register(sub)

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	tx := mkTx(pglogrepl.LSN(0x100), 1, mkChange(wal.OpInsert, "public", "users", "42"))
	sendTx(t, txCh, tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
	if got, want := len(ev.MatchedIndices), 1; got != want {
		t.Errorf("MatchedIndices length: got %d; want %d", got, want)
	}
	if len(ev.MatchedIndices) > 0 && ev.MatchedIndices[0] != 0 {
		t.Errorf("MatchedIndices[0]: got %d; want 0", ev.MatchedIndices[0])
	}
	if ev.Tx.CommitLSN != tx.CommitLSN {
		t.Errorf("Tx.CommitLSN: got %s; want %s", ev.Tx.CommitLSN, tx.CommitLSN)
	}

	for _, reason := range []string{"slow_consumer", "tx_too_large", "multi_root"} {
		if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", reason); v != 0 {
			t.Errorf("tx_dropped_total{reason=%s}: got %v; want 0", reason, v)
		}
	}

	cancel()
	<-done
}

func TestBroadcaster_ExactFanOut_MultipleSubscribersSameRow(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	a := mkExactSub("public", "users", "42", 0, 4)
	c := mkExactSub("public", "users", "42", 0, 4)
	recordedFor(t, a).useEncoder(b.enc.(*stubEncoder))
	recordedFor(t, c).useEncoder(b.enc.(*stubEncoder))
	b.Register(a)
	b.Register(c)

	if got, want := b.ExactLen(), 2; got != want {
		t.Fatalf("ExactLen after two exact subscribers: got %d; want %d", got, want)
	}
	if v := gatherGauge(t, reg, "walera_subscribers_active", "type", "exact"); v != 2 {
		t.Fatalf("subscribers_active{type=exact} after two Registers: got %v; want 2", v)
	}

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x110), 11, mkChange(wal.OpInsert, "public", "users", "42")))

	for _, sub := range []*Subscriber{a, c} {
		ev := drainOne(t, sub, 200*time.Millisecond)
		if got, want := ev.MatchedIndices, []int{0}; !equalInts(got, want) {
			t.Errorf("subscriber %s MatchedIndices: got %v; want %v", sub.ID(), got, want)
		}
	}

	b.Deregister(a)
	if got, want := b.ExactLen(), 1; got != want {
		t.Fatalf("ExactLen after deregistering one exact subscriber: got %d; want %d", got, want)
	}
	if v := gatherGauge(t, reg, "walera_subscribers_active", "type", "exact"); v != 1 {
		t.Fatalf("subscribers_active{type=exact} after one Deregister: got %v; want 1", v)
	}

	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x120), 12, mkChange(wal.OpUpdate, "public", "users", "42")))

	expectNoFrame(t, a, 50*time.Millisecond)
	ev := drainOne(t, c, 200*time.Millisecond)
	if got, want := ev.MatchedIndices, []int{0}; !equalInts(got, want) {
		t.Errorf("remaining subscriber MatchedIndices: got %v; want %v", got, want)
	}

	b.Deregister(c)
	if got, want := b.ExactLen(), 0; got != want {
		t.Errorf("ExactLen after deregistering both exact subscribers: got %d; want %d", got, want)
	}
	if v := gatherGauge(t, reg, "walera_subscribers_active", "type", "exact"); v != 0 {
		t.Errorf("subscribers_active{type=exact} after both Deregisters: got %v; want 0", v)
	}

	cancel()
	<-done
}

func TestBroadcaster_WildcardFanOut_MultiChange(t *testing.T) {
	t.Parallel()

	b, _ := mkBroadcaster(10000)
	sub := mkWildcardSub("public", "users", 0, 8)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	b.Register(sub)

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	tx := mkTx(pglogrepl.LSN(0x200), 2,
		mkChange(wal.OpInsert, "public", "users", "1"),
		mkChange(wal.OpUpdate, "public", "users", "1"),
		mkChange(wal.OpInsert, "public", "orders", "999"),
	)
	sendTx(t, txCh, tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
	// Under per-tx semantics: wildcard subscriber on users becomes eligible once
	// any users change matches; fullIndices covers the whole tx [0,1,2] including
	// the orders:999 change. With nil Filter (no whitelist), all 3 changes are
	// delivered. The tx touches only one users PK ("1"), so the multi_root guard
	// does not fire — multiple changes for the SAME anchor PK are normal.
	want := []int{0, 1, 2}
	if got := ev.MatchedIndices; !equalInts(got, want) {
		t.Errorf("MatchedIndices: got %v; want %v", got, want)
	}

	expectNoFrame(t, sub, 50*time.Millisecond)

	cancel()
	<-done
}

func TestBroadcaster_ExactAndWildcardSameTx_SingleEventPerSub(t *testing.T) {
	t.Parallel()

	b, _ := mkBroadcaster(10000)
	exact := mkExactSub("public", "users", "42", 0, 8)
	wild := mkWildcardSub("public", "users", 0, 8)
	recordedFor(t, exact).useEncoder(b.enc.(*stubEncoder))
	recordedFor(t, wild).useEncoder(b.enc.(*stubEncoder))
	b.Register(exact)
	b.Register(wild)

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	// Tx touches a single anchor PK ("42") with three changes — multi_root guard
	// does not fire, both subscribers receive one event with all three changes.
	// (Multi-PK same-table txs are covered by TestBroadcaster_MultiRoot_*.)
	tx := mkTx(pglogrepl.LSN(0x300), 3,
		mkChange(wal.OpInsert, "public", "users", "42"),
		mkChange(wal.OpUpdate, "public", "users", "42"),
		mkChange(wal.OpUpdate, "public", "users", "42"),
	)
	sendTx(t, txCh, tx)

	exactEv := drainOne(t, exact, 200*time.Millisecond)
	if got, want := exactEv.MatchedIndices, []int{0, 1, 2}; !equalInts(got, want) {
		t.Errorf("exact MatchedIndices: got %v; want %v (per-tx: full tx delivered once eligible)", got, want)
	}

	wildEv := drainOne(t, wild, 200*time.Millisecond)
	if got, want := wildEv.MatchedIndices, []int{0, 1, 2}; !equalInts(got, want) {
		t.Errorf("wildcard MatchedIndices: got %v; want %v", got, want)
	}

	expectNoFrame(t, exact, 50*time.Millisecond)
	expectNoFrame(t, wild, 50*time.Millisecond)

	cancel()
	<-done
}

func TestBroadcaster_StartLSN_FilterBelow(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", pglogrepl.LSN(0x100), 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	b.Register(sub)

	txCh := make(chan wal.Tx, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x100), 1, mkChange(wal.OpInsert, "public", "users", "42")))

	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x101), 2, mkChange(wal.OpInsert, "public", "users", "42")))

	ev := drainOne(t, sub, 200*time.Millisecond)
	if ev.Tx.CommitLSN != pglogrepl.LSN(0x101) {
		t.Errorf("delivered tx CommitLSN: got %s; want 0/101", ev.Tx.CommitLSN)
	}

	expectNoFrame(t, sub, 50*time.Millisecond)

	for _, reason := range []string{"slow_consumer", "tx_too_large", "multi_root"} {
		if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", reason); v != 0 {
			t.Errorf("tx_dropped_total{reason=%s}: got %v; want 0", reason, v)
		}
	}

	cancel()
	<-done
}

func TestBroadcaster_TxTooLarge_ExactCap(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(3)
	sub := mkExactSub("public", "users", "42", 0, 16)
	b.Register(sub)

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	tx := mkTx(pglogrepl.LSN(0x400), 4,
		mkChange(wal.OpUpdate, "public", "users", "42"),
		mkChange(wal.OpUpdate, "public", "users", "42"),
		mkChange(wal.OpUpdate, "public", "users", "42"),
		mkChange(wal.OpUpdate, "public", "users", "42"),
	)
	sendTx(t, txCh, tx)

	select {
	case <-sub.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("subscriber not dropped within 500ms")
	}
	if got, want := sub.Reason(), "tx_too_large"; got != want {
		t.Errorf("Reason: got %q; want %q", got, want)
	}

	expectNoFrame(t, sub, 50*time.Millisecond)

	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "tx_too_large"); v != 1 {
		t.Errorf("tx_dropped_total{reason=tx_too_large}: got %v; want 1", v)
	}

	cancel()
	<-done
}

func TestBroadcaster_TxTooLarge_WildcardCap(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(2)
	sub := mkWildcardSub("public", "users", 0, 16)
	b.Register(sub)

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	// All changes target the same anchor PK so the multi_root guard does NOT
	// trip — the test isolates the post-filter cap path; three changes for
	// users:1 exceed cap=2 → tx_too_large drop.
	tx := mkTx(pglogrepl.LSN(0x500), 5,
		mkChange(wal.OpInsert, "public", "users", "1"),
		mkChange(wal.OpUpdate, "public", "users", "1"),
		mkChange(wal.OpUpdate, "public", "users", "1"),
	)
	sendTx(t, txCh, tx)

	select {
	case <-sub.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("wildcard subscriber not dropped within 500ms")
	}
	if got, want := sub.Reason(), "tx_too_large"; got != want {
		t.Errorf("Reason: got %q; want %q", got, want)
	}

	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "tx_too_large"); v != 1 {
		t.Errorf("tx_dropped_total{reason=tx_too_large}: got %v; want 1", v)
	}

	cancel()
	<-done
}

func TestBroadcaster_TxTooLarge_ExactRelaxed_GEN03(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 0)
	b.Register(sub)

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	const nChanges = 1500
	changes := make([]wal.Change, nChanges)
	for i := range changes {
		changes[i] = mkChange(wal.OpUpdate, "public", "users", "42")
	}
	tx := mkTx(pglogrepl.LSN(0x600), 6, changes...)
	sendTx(t, txCh, tx)

	ev := drainOne(t, sub, 500*time.Millisecond)
	if got, want := len(ev.MatchedIndices), nChanges; got != want {
		t.Errorf("MatchedIndices length: got %d; want %d", got, want)
	}

	for _, reason := range []string{"slow_consumer", "tx_too_large", "multi_root"} {
		if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", reason); v != 0 {
			t.Errorf("tx_dropped_total{reason=%s}: got %v; want 0 (GEN-03 relaxation)", reason, v)
		}
	}

	cancel()
	<-done
}

func TestBroadcaster_SlowConsumerDisconnect(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 2)
	b.Register(sub)

	txCh := make(chan wal.Tx, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x100), 1, mkChange(wal.OpInsert, "public", "users", "42")))
	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x200), 2, mkChange(wal.OpInsert, "public", "users", "42")))
	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x300), 3, mkChange(wal.OpInsert, "public", "users", "42")))

	select {
	case <-sub.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("subscriber not dropped within 500ms (slow-consumer regression)")
	}
	if got, want := sub.Reason(), "slow_consumer"; got != want {
		t.Errorf("Reason: got %q; want %q", got, want)
	}
	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "slow_consumer"); v < 1 {
		t.Errorf("tx_dropped_total{reason=slow_consumer}: got %v; want >= 1", v)
	}

	cancel()
	<-done
}

func TestBroadcaster_Ingest_ExitsOnCtxCancel(t *testing.T) {
	t.Parallel()

	b, _ := mkBroadcaster(10000)
	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())

	done, errPtr := runIngest(b, ctx, txCh)
	cancel()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Ingest did not exit within 200ms of ctx cancel")
	}
	if *errPtr != context.Canceled {
		t.Errorf("Ingest return error: got %v; want context.Canceled", *errPtr)
	}
}

func TestBroadcaster_Ingest_ExitsOnTxChClose(t *testing.T) {
	t.Parallel()

	b, _ := mkBroadcaster(10000)
	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done, errPtr := runIngest(b, ctx, txCh)
	close(txCh)

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Ingest did not exit within 200ms of txCh close")
	}
	if *errPtr != nil {
		t.Errorf("Ingest return error: got %v; want nil", *errPtr)
	}
}

func TestBroadcaster_DeregisterDecrementsGauge(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)

	b.Register(sub)
	if v := gatherGauge(t, reg, "walera_subscribers_active", "type", "exact"); v != 1 {
		t.Errorf("after Register: subscribers_active{type=exact}: got %v; want 1", v)
	}

	b.Deregister(sub)
	if v := gatherGauge(t, reg, "walera_subscribers_active", "type", "exact"); v != 0 {
		t.Errorf("after Deregister: subscribers_active{type=exact}: got %v; want 0", v)
	}
}

func TestBroadcaster_MultiRootCounter_RegisteredAtZero(t *testing.T) {
	t.Parallel()

	_, reg := mkBroadcaster(10000)

	if !gatherHasSeries(t, reg, "walera_tx_dropped_total", "reason", "multi_root") {
		t.Fatal("walera_tx_dropped_total{reason=multi_root} series missing from Gather() — sentinel regressed")
	}
	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "multi_root"); v != 0 {
		t.Errorf("multi_root counter initial value: got %v; want 0", v)
	}

	for _, reason := range []string{"slow_consumer", "tx_too_large"} {
		if !gatherHasSeries(t, reg, "walera_tx_dropped_total", "reason", reason) {
			t.Errorf("walera_tx_dropped_total{reason=%s} series missing from Gather()", reason)
		}
	}
}

func TestRouterSubscriberFilterFieldDefaultNil(t *testing.T) {
	t.Parallel()
	sub := NewSubscriber(
		SubscriberConfig{
			Kind:      KindExact,
			Schema:    "public",
			Table:     "users",
			PK:        "42",
			BufferCap: 4,
		},
		SubscriberDeps{Parent: context.Background()},
	)
	if sub.Filter != nil {
		t.Fatalf("Filter at construction: got non-nil; want nil")
	}
}

func TestRouteTxFilterAllDroppedSilently(t *testing.T) {
	t.Parallel()
	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)
	sub.Filter = func(c wal.Change, _ pglogrepl.LSN) (wal.Change, bool) { return c, true }
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x100), 1,
		mkChange(wal.OpInsert, "public", "users", "42"),
	)
	b.routeTx(tx)

	expectNoFrame(t, sub, 100*time.Millisecond)

	if sub.Reason() != "" {
		t.Errorf("Reason: got %q; want empty (silent drop must not call sub.Drop)", sub.Reason())
	}

	for _, reason := range []string{"slow_consumer", "tx_too_large", "multi_root"} {
		if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", reason); v != 0 {
			t.Errorf("tx_dropped_total{reason=%s}: got %v; want 0", reason, v)
		}
	}
}

func TestRouteTxFilterPartialKept(t *testing.T) {
	t.Parallel()
	b, _ := mkBroadcaster(10000)
	sub := mkWildcardSub("public", "users", 0, 8)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))

	sub.Filter = func(c wal.Change, _ pglogrepl.LSN) (wal.Change, bool) {
		c2 := c
		if c.Data != nil {
			c2.Data = map[string]any{"id": c.Data["id"]}
		}
		return c2, false
	}
	b.Register(sub)

	// Two changes for the same anchor PK keep the multi_root guard out of
	// the way — this test exercises the Filter-clone path.
	tx := mkTx(pglogrepl.LSN(0x200), 2,
		mkChange(wal.OpInsert, "public", "users", "1"),
		mkChange(wal.OpUpdate, "public", "users", "1"),
	)
	b.routeTx(tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
	if got, want := len(ev.Tx.Changes), 2; got != want {
		t.Errorf("ev.Tx.Changes length: got %d; want %d", got, want)
	}
	if got, want := len(ev.MatchedIndices), 2; got != want {
		t.Errorf("ev.MatchedIndices length: got %d; want %d", got, want)
	}
	if !equalInts(ev.MatchedIndices, []int{0, 1}) {
		t.Errorf("ev.MatchedIndices: got %v; want [0 1] (clone uses new indices)", ev.MatchedIndices)
	}

	if reflect.ValueOf(ev.Tx.Changes).Pointer() == reflect.ValueOf(tx.Changes).Pointer() {
		t.Error("ev.Tx.Changes shares backing array with input tx.Changes — Filter path failed to clone")
	}
}

func TestRouteTxPassesCommitLSNToFilter(t *testing.T) {
	t.Parallel()
	b, _ := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)

	var captured pglogrepl.LSN
	sub.Filter = func(c wal.Change, txCommitLSN pglogrepl.LSN) (wal.Change, bool) {
		captured = txCommitLSN
		return c, false
	}
	b.Register(sub)

	want := pglogrepl.LSN(0x4A2)
	tx := mkTx(want, 1, mkChange(wal.OpInsert, "public", "users", "42"))
	b.routeTx(tx)

	_ = drainOne(t, sub, 200*time.Millisecond)

	if captured != want {
		t.Errorf("Filter received txCommitLSN = %s; want %s", captured, want)
	}
}

func TestRouteTxNilFilterUnchangedFastPath(t *testing.T) {
	t.Parallel()
	b, _ := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))

	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x300), 3,
		mkChange(wal.OpInsert, "public", "users", "42"),
	)
	b.routeTx(tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
	if reflect.ValueOf(ev.Tx.Changes).Pointer() != reflect.ValueOf(tx.Changes).Pointer() {
		t.Error("ev.Tx.Changes does NOT share backing array with input tx.Changes — fast path regressed")
	}
}

func gatherHistogramCount(t *testing.T, reg *metrics.Registry, name string) uint64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		var total uint64
		for _, m := range fam.GetMetric() {
			total += m.GetHistogram().GetSampleCount()
		}
		return total
	}
	return 0
}

func TestBroadcaster_RoutingFanOut_ObservedPerTx(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)
	b.Register(sub)

	txCh := make(chan wal.Tx, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x100), 1, mkChange(wal.OpInsert, "public", "users", "42")))
	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x101), 2, mkChange(wal.OpInsert, "public", "orders", "99")))
	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x102), 3, mkChange(wal.OpInsert, "public", "users", "42")))

	_ = drainOne(t, sub, 200*time.Millisecond)
	_ = drainOne(t, sub, 200*time.Millisecond)

	time.Sleep(20 * time.Millisecond)

	if got := gatherHistogramCount(t, reg, "walera_routing_fan_out"); got != 3 {
		t.Errorf("walera_routing_fan_out SampleCount = %d; want 3", got)
	}

	cancel()
	<-done
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestBroadcaster_WholeTransactionEligibility:
// A subscriber to todo_lists:42 becomes eligible for the whole tx once
// the todo_lists:42 UPDATE matches. The tx also contains a tasks:99 INSERT.
// Under per-tx semantics the subscriber must receive BOTH changes (not only
// the anchor change).
func TestBroadcaster_WholeTransactionEligibility(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "todo_lists", "42", 0, 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	b.Register(sub)

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	// Tx contains both the anchor change and a co-transactional change.
	tx := mkTx(pglogrepl.LSN(0x1000), 100,
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpInsert, "public", "tasks", "99"),
	)
	sendTx(t, txCh, tx)

	// Subscriber must receive an event once the anchor matches.
	ev := drainOne(t, sub, 200*time.Millisecond)

	// Event must contain BOTH changes (indices 0 and 1) because the
	// subscriber is eligible for the whole tx.
	if got, want := len(ev.MatchedIndices), 2; got != want {
		t.Errorf("MatchedIndices length: got %d; want %d (both tx changes expected)", got, want)
	}
	if !equalInts(ev.MatchedIndices, []int{0, 1}) {
		t.Errorf("MatchedIndices: got %v; want [0 1] (full tx in commit order)", ev.MatchedIndices)
	}

	for _, reason := range []string{"slow_consumer", "tx_too_large", "multi_root"} {
		if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", reason); v != 0 {
			t.Errorf("tx_dropped_total{reason=%s}: got %v; want 0", reason, v)
		}
	}

	cancel()
	<-done
}

// TestBroadcaster_CoTxWhitelistedDelivery:
// A subscriber Filter admits both todo_lists and tasks changes from the same tx.
// The single delivered Event must contain BOTH changes (not only the anchor change).
func TestBroadcaster_CoTxWhitelistedDelivery(t *testing.T) {
	t.Parallel()

	b, _ := mkBroadcaster(10000)
	sub := mkExactSub("public", "todo_lists", "42", 0, 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))

	// Filter admits all changes (drop=false for every change).
	sub.Filter = func(c wal.Change, _ pglogrepl.LSN) (wal.Change, bool) {
		return c, false // admit everything
	}
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x1010), 101,
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpInsert, "public", "tasks", "99"),
	)
	b.routeTx(tx)

	ev := drainOne(t, sub, 200*time.Millisecond)

	// Both changes must appear in the delivered event.
	if got, want := len(ev.Tx.Changes), 2; got != want {
		t.Errorf("ev.Tx.Changes length: got %d; want %d (both changes must be delivered)", got, want)
	}
	if got, want := len(ev.MatchedIndices), 2; got != want {
		t.Errorf("ev.MatchedIndices length: got %d; want %d", got, want)
	}

	// Verify both tables appear in the delivered changes.
	tables := make(map[string]bool)
	for _, ch := range ev.Tx.Changes {
		tables[ch.Table] = true
	}
	if !tables["todo_lists"] {
		t.Error("delivered event missing todo_lists change")
	}
	if !tables["tasks"] {
		t.Error("delivered event missing co-transactional tasks change")
	}
}

// TestBroadcaster_CoTxRequiresSurvivingAnchor:
// A raw channel match is not enough to authorize transaction-scoped fan-out.
// If the subscriber's Filter drops the exact anchor change, co-transactional
// changes that would otherwise pass the Filter must not be delivered.
func TestBroadcaster_CoTxRequiresSurvivingAnchor(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "todo_lists", "42", 0, 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	sub.Filter = func(c wal.Change, _ pglogrepl.LSN) (wal.Change, bool) {
		if c.Table == "todo_lists" {
			return wal.Change{}, true
		}
		return c, false
	}
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x1011), 111,
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpInsert, "public", "tasks", "99"),
	)
	b.routeTx(tx)

	expectNoFrame(t, sub, 100*time.Millisecond)
	for _, reason := range []string{"slow_consumer", "tx_too_large", "multi_root"} {
		if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", reason); v != 0 {
			t.Errorf("tx_dropped_total{reason=%s}: got %v; want 0", reason, v)
		}
	}
	if got := gatherCounterNoLabel(t, reg, "walera_co_tx_beyond_anchor_total"); got != 0 {
		t.Errorf("walera_co_tx_beyond_anchor_total = %v; want 0", got)
	}
}

// TestBroadcaster_SingleEventPerSubDedup:
// A wildcard subscriber on todo_lists is matched by multiple changes in one tx.
// The subscriber must receive exactly ONE event containing all changes in commit order.
// The eligible-set deduplicates multiple matches to one dispatch.
func TestBroadcaster_SingleEventPerSubDedup(t *testing.T) {
	t.Parallel()

	b, _ := mkBroadcaster(10000)
	// Wildcard subscriber so multiple changes in the tx match.
	sub := mkWildcardSub("public", "todo_lists", 0, 8)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	b.Register(sub)

	// All three changes target the same anchor PK so the multi_root guard
	// does not fire — dedup is about a single subscriber matching multiple
	// rows of the same tx, not about cross-root delivery.
	tx := mkTx(pglogrepl.LSN(0x1020), 102,
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
	)
	b.routeTx(tx)

	// Exactly one event: dedup via eligible set.
	ev := drainOne(t, sub, 200*time.Millisecond)
	expectNoFrame(t, sub, 50*time.Millisecond)

	// All three changes delivered in commit order.
	if got, want := len(ev.MatchedIndices), 3; got != want {
		t.Errorf("MatchedIndices length: got %d; want %d", got, want)
	}
	if !equalInts(ev.MatchedIndices, []int{0, 1, 2}) {
		t.Errorf("MatchedIndices: got %v; want [0 1 2] (commit order)", ev.MatchedIndices)
	}
}

// TestBroadcaster_NonMatchingTxNoDelivery:
// Subscriber to todo_lists:42; tx touches only tasks table.
// No event must be sent.
func TestBroadcaster_NonMatchingTxNoDelivery(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "todo_lists", "42", 0, 4)
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x1030), 103,
		mkChange(wal.OpInsert, "public", "tasks", "1"),
		mkChange(wal.OpInsert, "public", "tasks", "2"),
	)
	b.routeTx(tx)

	// No event must be delivered.
	expectNoFrame(t, sub, 100*time.Millisecond)

	for _, reason := range []string{"slow_consumer", "tx_too_large"} {
		if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", reason); v != 0 {
			t.Errorf("tx_dropped_total{reason=%s}: got %v; want 0", reason, v)
		}
	}
}

// TestBroadcaster_TxTooLarge_PostFilterCap:
// Filter admits more than cap changes → Drop("tx_too_large"), TxDropped incremented.
// cap=3; tx has 5 changes all admitted by Filter → subscriber dropped.
func TestBroadcaster_TxTooLarge_PostFilterCap(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(3) // cap = 3
	sub := mkWildcardSub("public", "todo_lists", 0, 16)

	// Filter admits all changes (no drop).
	sub.Filter = func(c wal.Change, _ pglogrepl.LSN) (wal.Change, bool) {
		return c, false
	}
	b.Register(sub)

	// All five changes target the same anchor PK so the multi_root guard
	// does not pre-empt the post-filter cap path — the test isolates the
	// cap-overflow drop.
	tx := mkTx(pglogrepl.LSN(0x1040), 104,
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
	)
	b.routeTx(tx)

	// Subscriber must be dropped (5 post-filter changes > cap 3).
	select {
	case <-sub.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("subscriber not dropped within 500ms (post-filter cap)")
	}
	if got, want := sub.Reason(), "tx_too_large"; got != want {
		t.Errorf("Reason: got %q; want %q", got, want)
	}
	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "tx_too_large"); v != 1 {
		t.Errorf("tx_dropped_total{reason=tx_too_large}: got %v; want 1", v)
	}
}

// TestBroadcaster_TxTooLarge_PreFilterNoFalsePositive:
// Regression-critical: tx has more changes than cap but Filter admits only <=cap changes.
// The subscriber must NOT be dropped; it receives only the whitelisted changes.
// The old pre-filter cap (checking len(indices) before the filter) would falsely drop
// this subscriber. This FAILS against the old code.
func TestBroadcaster_TxTooLarge_PreFilterNoFalsePositive(t *testing.T) {
	t.Parallel()

	const cap = 3
	b, reg := mkBroadcaster(cap)
	sub := mkExactSub("public", "todo_lists", "42", 0, 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))

	// Filter only admits todo_lists changes; drops everything else.
	sub.Filter = func(c wal.Change, _ pglogrepl.LSN) (wal.Change, bool) {
		if c.Table == "todo_lists" {
			return c, false // admit
		}
		return c, true // drop non-todo_lists
	}
	b.Register(sub)

	// Tx has 5 changes total (> cap=3), but only 2 pass the filter (< cap=3).
	// Old code: len(indices)=5 > cap=3 → drop subscriber (false positive).
	// New code: len(filtered)=2 <= cap=3 → deliver without drop.
	tx := mkTx(pglogrepl.LSN(0x1050), 105,
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"), // passes filter
		mkChange(wal.OpInsert, "public", "tasks", "1"),       // dropped by filter
		mkChange(wal.OpInsert, "public", "tasks", "2"),       // dropped by filter
		mkChange(wal.OpInsert, "public", "tasks", "3"),       // dropped by filter
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"), // passes filter
	)
	b.routeTx(tx)

	// Subscriber must NOT be dropped.
	ev := drainOne(t, sub, 200*time.Millisecond)
	if sub.Reason() != "" {
		t.Errorf("subscriber was unexpectedly dropped: reason=%q", sub.Reason())
	}
	// Only 2 changes pass the filter.
	if got, want := len(ev.Tx.Changes), 2; got != want {
		t.Errorf("ev.Tx.Changes length: got %d; want %d (only whitelisted changes)", got, want)
	}
	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "tx_too_large"); v != 0 {
		t.Errorf("tx_dropped_total{reason=tx_too_large}: got %v; want 0 (no false-positive drop)", v)
	}
}

// TestBroadcaster_TxFanOutWork_ObservedPerTx:
// After routing a tx with 2 eligible wildcard subscribers each receiving 3 changes,
// TxFanOutWork histogram is observed once more (one observation per tx with non-zero work).
// The old code does not observe TxFanOutWork in routeTx at all; this test FAILS.
func TestBroadcaster_TxFanOutWork_ObservedPerTx(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)

	// Two wildcard subscribers on todo_lists (K=2).
	sub1 := mkWildcardSub("public", "todo_lists", 0, 8)
	sub2 := mkWildcardSub("public", "todo_lists", 0, 8)
	b.Register(sub1)
	b.Register(sub2)

	// Capture histogram sample count before routing (registry pre-touch adds 1).
	countBefore := gatherHistogramCount(t, reg, "walera_tx_fan_out_work")

	// Tx with 3 changes matching both subscribers (M=3, total work = 2*3 = 6).
	// All three changes target the same anchor PK so the multi_root guard
	// stays out of the way of the fan-out-work observation.
	tx := mkTx(pglogrepl.LSN(0x1060), 106,
		mkChange(wal.OpUpdate, "public", "todo_lists", "1"),
		mkChange(wal.OpUpdate, "public", "todo_lists", "1"),
		mkChange(wal.OpUpdate, "public", "todo_lists", "1"),
	)
	b.routeTx(tx)

	// Drain both subscribers to ensure dispatch completed.
	_ = drainOne(t, sub1, 200*time.Millisecond)
	_ = drainOne(t, sub2, 200*time.Millisecond)

	// TxFanOutWork must have been observed once more (one Observe() call per tx).
	countAfter := gatherHistogramCount(t, reg, "walera_tx_fan_out_work")
	if countAfter != countBefore+1 {
		t.Errorf("walera_tx_fan_out_work SampleCount: got %d; want %d (one observation per routed tx)",
			countAfter, countBefore+1)
	}
}

// gatherCounterNoLabel reads a no-label counter value from the registry by metric family name.
func gatherCounterNoLabel(t *testing.T, reg *metrics.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		ms := fam.GetMetric()
		if len(ms) == 0 {
			return 0
		}
		return ms[0].GetCounter().GetValue()
	}
	return 0
}

// TestBroadcaster_CoBeyondAnchorCounter covers the three SAFE-02 sub-cases:
//
// 1. "exact co-tx": exact subscriber whitelisted for anchor + co-tx table receives both
// in one tx → beyond-anchor delta = 1.
//
// 2. "exact anchor-only": exact subscriber whose tx delivers only the anchor-keyed change
// → beyond-anchor delta = 0.
//
// 3. "wildcard single-table": wildcard subscriber receiving a tx where all changes share
// the subscribed table's WildcardKey → beyond-anchor delta = 0 (every change matches the
// anchor key for a wildcard sub).
//
// 4. "wildcard multi-table": wildcard subscriber receiving a tx with 1 change on the
// subscribed table + N-1 changes on OTHER tables (distinct WildcardKeys) → delta = N-1.
func TestBroadcaster_CoBeyondAnchorCounter(t *testing.T) {
	t.Parallel()

	t.Run("exact co-tx", func(t *testing.T) {
		t.Parallel()
		b, reg := mkBroadcaster(10000)

		sub := mkExactSub("public", "todo_lists", "42", 0, 4)
		// Filter admits both todo_lists and tasks changes.
		sub.Filter = func(c wal.Change, _ pglogrepl.LSN) (wal.Change, bool) {
			return c, false // admit all
		}
		b.Register(sub)

		before := gatherCounterNoLabel(t, reg, "walera_co_tx_beyond_anchor_total")

		// Tx: anchor change (todo_lists:42) + 1 co-tx change (tasks:99).
		tx := mkTx(pglogrepl.LSN(0x3000), 300,
			mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
			mkChange(wal.OpInsert, "public", "tasks", "99"),
		)
		b.routeTx(tx)

		_ = drainOne(t, sub, 200*time.Millisecond)

		after := gatherCounterNoLabel(t, reg, "walera_co_tx_beyond_anchor_total")
		if delta := after - before; delta != 1 {
			t.Errorf("beyond-anchor delta for exact co-tx: got %v; want 1", delta)
		}
	})

	t.Run("exact anchor-only", func(t *testing.T) {
		t.Parallel()
		b, reg := mkBroadcaster(10000)

		sub := mkExactSub("public", "todo_lists", "42", 0, 4)
		b.Register(sub)

		before := gatherCounterNoLabel(t, reg, "walera_co_tx_beyond_anchor_total")

		// Tx: only the anchor change, no co-tx changes.
		tx := mkTx(pglogrepl.LSN(0x3001), 301,
			mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		)
		b.routeTx(tx)

		_ = drainOne(t, sub, 200*time.Millisecond)

		after := gatherCounterNoLabel(t, reg, "walera_co_tx_beyond_anchor_total")
		if delta := after - before; delta != 0 {
			t.Errorf("beyond-anchor delta for exact anchor-only: got %v; want 0", delta)
		}
	})

	t.Run("wildcard single-table", func(t *testing.T) {
		t.Parallel()
		b, reg := mkBroadcaster(10000)

		// Wildcard subscriber on public.todo_lists — anchor WildcardKey = "public.todo_lists".
		sub := mkWildcardSub("public", "todo_lists", 0, 8)
		b.Register(sub)

		before := gatherCounterNoLabel(t, reg, "walera_co_tx_beyond_anchor_total")

		// All changes share the subscribed table's WildcardKey → every delivered
		// change equals the anchor key → delta = 0. Single PK keeps the
		// multi_root guard out of this test's scope.
		tx := mkTx(pglogrepl.LSN(0x3002), 302,
			mkChange(wal.OpUpdate, "public", "todo_lists", "1"),
			mkChange(wal.OpUpdate, "public", "todo_lists", "1"),
			mkChange(wal.OpUpdate, "public", "todo_lists", "1"),
		)
		b.routeTx(tx)

		_ = drainOne(t, sub, 200*time.Millisecond)

		after := gatherCounterNoLabel(t, reg, "walera_co_tx_beyond_anchor_total")
		if delta := after - before; delta != 0 {
			t.Errorf("beyond-anchor delta for wildcard single-table: got %v; want 0", delta)
		}
	})

	t.Run("wildcard multi-table", func(t *testing.T) {
		t.Parallel()
		b, reg := mkBroadcaster(10000)

		// Wildcard subscriber on public.todo_lists (nil Filter — admits all tables).
		sub := mkWildcardSub("public", "todo_lists", 0, 8)
		b.Register(sub)

		before := gatherCounterNoLabel(t, reg, "walera_co_tx_beyond_anchor_total")

		// Tx: 1 change on the subscribed table (WildcardKey = "public.todo_lists") +
		// 2 changes on OTHER tables (distinct WildcardKeys) → delta = 2.
		tx := mkTx(pglogrepl.LSN(0x3003), 303,
			mkChange(wal.OpUpdate, "public", "todo_lists", "42"), // anchor WildcardKey
			mkChange(wal.OpInsert, "public", "tasks", "99"),      // beyond anchor
			mkChange(wal.OpInsert, "public", "tags", "7"),        // beyond anchor
		)
		b.routeTx(tx)

		_ = drainOne(t, sub, 200*time.Millisecond)

		after := gatherCounterNoLabel(t, reg, "walera_co_tx_beyond_anchor_total")
		// 2 changes have WildcardKey != "public.todo_lists" → delta = 2.
		if delta := after - before; delta != 2 {
			t.Errorf("beyond-anchor delta for wildcard multi-table: got %v; want 2", delta)
		}
	})
}

// TestBroadcaster_FanOutRaceStress (SAFE-01 race):
// Many subscribers + large tx routed under -race with no data race.
// All routing occurs in the single routeTx goroutine (WAL ingest goroutine).
// fullIndices is read-only across sequential dispatchEvent calls; no new goroutines.
func TestBroadcaster_FanOutRaceStress(t *testing.T) {
	t.Parallel()

	const (
		nSubscribers = 50
		nChanges     = 20
		nTxns        = 10
	)

	b, _ := mkBroadcaster(10000)

	// Register many wildcard subscribers.
	subs := make([]*Subscriber, nSubscribers)
	for i := range subs {
		subs[i] = mkWildcardSub("public", "todo_lists", 0, nChanges*nTxns+8)
		b.Register(subs[i])
	}

	txCh := make(chan wal.Tx, nTxns)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	// Send nTxns transactions each with nChanges changes.
	for txIdx := range nTxns {
		changes := make([]wal.Change, nChanges)
		for j := range changes {
			changes[j] = mkChange(wal.OpUpdate, "public", "todo_lists", "42")
		}
		lsn := pglogrepl.LSN(uint64(0x2000) + uint64(txIdx))
		sendTx(t, txCh, mkTx(lsn, uint32(200+txIdx), changes...))
	}

	// Drain all subscribers (nTxns events per subscriber).
	for _, sub := range subs {
		for range nTxns {
			_ = drainOne(t, sub, 500*time.Millisecond)
		}
	}

	cancel()
	<-done

	// If -race detected a data race the test binary would have already panicked.
	// Reaching here confirms no race was detected.
}

// TestRouteTxSchemaScoped_FilterPath: when the publication contains a same-named
// table in another schema and a tx touches both, an exact subscriber on
// public.<table>:<pk> with a non-nil Filter must NOT see the private row even
// if the Filter accepts every change (auth whitelist keys only on table name).
func TestRouteTxSchemaScoped_FilterPath(t *testing.T) {
	t.Parallel()
	b, _ := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	sub.Filter = func(c wal.Change, _ pglogrepl.LSN) (wal.Change, bool) { return c, false }
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x500), 5,
		mkChange(wal.OpInsert, "public", "users", "42"),
		mkChange(wal.OpInsert, "private", "users", "99"),
	)
	b.routeTx(tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
	if got, want := len(ev.Tx.Changes), 1; got != want {
		t.Fatalf("ev.Tx.Changes length: got %d; want %d (cross-schema row must be stripped)", got, want)
	}
	if ev.Tx.Changes[0].Schema != "public" || ev.Tx.Changes[0].PK != "42" {
		t.Errorf("delivered change: got %+v; want public.users:42", ev.Tx.Changes[0])
	}
}

// TestRouteTxSchemaScoped_NilFilterStripsCrossSchema: nil-Filter path must
// also schema-scope (clones MatchedIndices but keeps tx.Changes backing).
func TestRouteTxSchemaScoped_NilFilterStripsCrossSchema(t *testing.T) {
	t.Parallel()
	b, _ := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x501), 6,
		mkChange(wal.OpInsert, "private", "users", "99"),
		mkChange(wal.OpInsert, "public", "users", "42"),
	)
	b.routeTx(tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
	if got, want := len(ev.MatchedIndices), 1; got != want {
		t.Fatalf("ev.MatchedIndices length: got %d; want %d", got, want)
	}
	if got := ev.MatchedIndices[0]; got != 1 {
		t.Errorf("ev.MatchedIndices[0]: got %d; want 1 (index into original tx.Changes)", got)
	}
	if reflect.ValueOf(ev.Tx.Changes).Pointer() != reflect.ValueOf(tx.Changes).Pointer() {
		t.Error("ev.Tx.Changes should still share backing array with tx.Changes (only indices rescoped)")
	}
}

// TestBroadcaster_MultiRoot_ExactDropsOnTwoPKs:
// Exact subscriber on todo_lists:42 must NOT receive a tx that touches both
// todo_lists:42 and todo_lists:99 in one commit. Per spec §1.6, this is a
// writer-side discipline violation; the broker drops the tx for this
// subscriber, increments tx_dropped_total{reason="multi_root"}, but keeps
// the connection alive (no sub.Drop, no Reason set).
func TestBroadcaster_MultiRoot_ExactDropsOnTwoPKs(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "todo_lists", "42", 0, 4)
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x4000), 400,
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpUpdate, "public", "todo_lists", "99"),
	)
	b.routeTx(tx)

	expectNoFrame(t, sub, 100*time.Millisecond)

	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "multi_root"); v != 1 {
		t.Errorf("tx_dropped_total{reason=multi_root}: got %v; want 1", v)
	}
	if got := sub.Reason(); got != "" {
		t.Errorf("Reason: got %q; want empty (multi_root must not disconnect the subscriber)", got)
	}
	select {
	case <-sub.Done():
		t.Error("subscriber unexpectedly disconnected on multi_root")
	default:
	}
}

// TestBroadcaster_MultiRoot_WildcardDropsOnTwoPKs:
// Wildcard subscribers are the primary signal of writer-side bulk-operation
// bugs (spec §4.4). Same per-subscriber drop semantics as exact.
func TestBroadcaster_MultiRoot_WildcardDropsOnTwoPKs(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkWildcardSub("public", "todo_lists", 0, 8)
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x4010), 401,
		mkChange(wal.OpInsert, "public", "todo_lists", "1"),
		mkChange(wal.OpInsert, "public", "todo_lists", "2"),
	)
	b.routeTx(tx)

	expectNoFrame(t, sub, 100*time.Millisecond)

	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "multi_root"); v != 1 {
		t.Errorf("tx_dropped_total{reason=multi_root}: got %v; want 1", v)
	}
	if got := sub.Reason(); got != "" {
		t.Errorf("Reason: got %q; want empty (multi_root must not disconnect the subscriber)", got)
	}
}

// TestBroadcaster_MultiRoot_SamePKMultipleChangesAllowed:
// Multiple changes for the SAME anchor PK in one tx (e.g., INSERT then UPDATE
// of users:42, or any "bump updated_at" pattern) is normal — the guard counts
// distinct PKs, not changes.
func TestBroadcaster_MultiRoot_SamePKMultipleChangesAllowed(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "todo_lists", "42", 0, 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x4020), 402,
		mkChange(wal.OpInsert, "public", "todo_lists", "42"),
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
	)
	b.routeTx(tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
	if got, want := len(ev.MatchedIndices), 3; got != want {
		t.Errorf("MatchedIndices length: got %d; want %d", got, want)
	}
	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "multi_root"); v != 0 {
		t.Errorf("tx_dropped_total{reason=multi_root}: got %v; want 0 (same PK is not multi-root)", v)
	}
}

// TestBroadcaster_MultiRoot_ChildOfOtherTableNotGuarded:
// The multi_root guard counts distinct PKs of the SUBSCRIBER's anchor table
// only. It does NOT inspect FK relationships between tables — a tx that
// touches todo_lists:42 + tasks(todo_list_id=99) is delivered. Closing this
// cross-table leak requires writer-side discipline (one root per tx,
// including its children), documented in README.
func TestBroadcaster_MultiRoot_ChildOfOtherTableNotGuarded(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "todo_lists", "42", 0, 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x4030), 403,
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpInsert, "public", "tasks", "777"),
	)
	b.routeTx(tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
	if got, want := len(ev.MatchedIndices), 2; got != want {
		t.Errorf("MatchedIndices length: got %d; want %d (child-of-other-root leak is writer-side discipline, not broker-enforced)", got, want)
	}
	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "multi_root"); v != 0 {
		t.Errorf("tx_dropped_total{reason=multi_root}: got %v; want 0 (guard scoped to anchor table)", v)
	}
}

// TestBroadcaster_MultiRoot_OneCounterPerSubscriber:
// Each eligible subscriber gets its own multi_root drop; the counter
// increments per (tx, subscriber) pair, not per tx (spec §8.2).
func TestBroadcaster_MultiRoot_OneCounterPerSubscriber(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	a := mkExactSub("public", "todo_lists", "42", 0, 4)
	c := mkExactSub("public", "todo_lists", "99", 0, 4)
	b.Register(a)
	b.Register(c)

	tx := mkTx(pglogrepl.LSN(0x4040), 404,
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpUpdate, "public", "todo_lists", "99"),
	)
	b.routeTx(tx)

	expectNoFrame(t, a, 50*time.Millisecond)
	expectNoFrame(t, c, 50*time.Millisecond)

	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "multi_root"); v != 2 {
		t.Errorf("tx_dropped_total{reason=multi_root}: got %v; want 2 (one per matched subscriber)", v)
	}
}

// TestBroadcaster_MultiRoot_NextTxStillDelivered:
// After a multi_root drop, the subscriber's connection stays open and the
// next well-formed tx is delivered normally.
func TestBroadcaster_MultiRoot_NextTxStillDelivered(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "todo_lists", "42", 0, 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	b.Register(sub)

	bad := mkTx(pglogrepl.LSN(0x4050), 405,
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
		mkChange(wal.OpUpdate, "public", "todo_lists", "99"),
	)
	b.routeTx(bad)
	expectNoFrame(t, sub, 50*time.Millisecond)

	good := mkTx(pglogrepl.LSN(0x4051), 406,
		mkChange(wal.OpUpdate, "public", "todo_lists", "42"),
	)
	b.routeTx(good)

	ev := drainOne(t, sub, 200*time.Millisecond)
	if got, want := len(ev.MatchedIndices), 1; got != want {
		t.Errorf("MatchedIndices length: got %d; want %d", got, want)
	}
	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "multi_root"); v != 1 {
		t.Errorf("tx_dropped_total{reason=multi_root}: got %v; want 1", v)
	}
}
