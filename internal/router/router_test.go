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
		mkChange(wal.OpInsert, "public", "users", "2"),
		mkChange(wal.OpInsert, "public", "users", "3"),
		mkChange(wal.OpInsert, "public", "orders", "999"),
	)
	sendTx(t, txCh, tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
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

	tx := mkTx(pglogrepl.LSN(0x300), 3,
		mkChange(wal.OpInsert, "public", "users", "42"),
		mkChange(wal.OpInsert, "public", "users", "99"),
		mkChange(wal.OpInsert, "public", "users", "100"),
	)
	sendTx(t, txCh, tx)

	exactEv := drainOne(t, exact, 200*time.Millisecond)
	if got, want := exactEv.MatchedIndices, []int{0}; !equalInts(got, want) {
		t.Errorf("exact MatchedIndices: got %v; want %v", got, want)
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

	tx := mkTx(pglogrepl.LSN(0x500), 5,
		mkChange(wal.OpInsert, "public", "users", "1"),
		mkChange(wal.OpInsert, "public", "users", "2"),
		mkChange(wal.OpInsert, "public", "users", "3"),
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

	tx := mkTx(pglogrepl.LSN(0x200), 2,
		mkChange(wal.OpInsert, "public", "users", "1"),
		mkChange(wal.OpInsert, "public", "users", "2"),
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
