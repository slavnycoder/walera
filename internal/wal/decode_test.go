package wal

import (
	"testing"

	"github.com/jackc/pglogrepl"
)

func fakeBeginMsg(xid uint32, finalLSN pglogrepl.LSN) *pglogrepl.BeginMessage {
	return &pglogrepl.BeginMessage{
		FinalLSN: finalLSN,
		Xid:      xid,
	}
}

func fakeCommitMsg(commitLSN pglogrepl.LSN) *pglogrepl.CommitMessage {
	return &pglogrepl.CommitMessage{
		CommitLSN: commitLSN,
	}
}

func fakeRelationCache() (*relationCache, *pglogrepl.RelationMessage) {
	relMsg := &pglogrepl.RelationMessage{
		RelationID:   42,
		Namespace:    "public",
		RelationName: "test_table",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: OIDInt4, Flags: 0x01},
			{Name: "name", DataType: OIDText, Flags: 0x00},
		},
	}
	cache := newRelationCache()
	if err := cache.Update(relMsg); err != nil {
		panic("fakeRelationCache: Update failed: " + err.Error())
	}
	return cache, relMsg
}

func fakeTupleData(values []fakeCol) *pglogrepl.TupleData {
	td := &pglogrepl.TupleData{}
	for _, v := range values {
		col := &pglogrepl.TupleDataColumn{}
		if v.isNull {
			col.DataType = pglogrepl.TupleDataTypeNull
		} else if v.isToast {
			col.DataType = pglogrepl.TupleDataTypeToast
		} else {
			col.DataType = pglogrepl.TupleDataTypeText
			col.Data = []byte(v.text)
		}
		td.Columns = append(td.Columns, col)
	}
	return td
}

type fakeCol struct {
	text    string
	isNull  bool
	isToast bool
}

func textCol(v string) fakeCol { return fakeCol{text: v} }
func nullCol() fakeCol         { return fakeCol{isNull: true} }
func toastCol() fakeCol        { return fakeCol{isToast: true} }

func TestTxBufferResetOnReconnect(t *testing.T) {
	cache, _ := fakeRelationCache()

	b := newTxBuilder()

	b.HandleBegin(fakeBeginMsg(1, pglogrepl.LSN(100)))
	insMsg := &pglogrepl.InsertMessage{
		RelationID: 42,
		Tuple:      fakeTupleData([]fakeCol{textCol("1"), textCol("alice")}),
	}
	if err := b.HandleInsert(insMsg, cache); err != nil {
		t.Fatalf("HandleInsert: %v", err)
	}

	b.Reset()

	if tx := b.HandleCommit(fakeCommitMsg(pglogrepl.LSN(100))); tx != nil {
		t.Fatalf("expected nil Tx after Reset, got Tx with ID=%d", tx.ID)
	}

	b.HandleBegin(fakeBeginMsg(2, pglogrepl.LSN(200)))
	insMsg2 := &pglogrepl.InsertMessage{
		RelationID: 42,
		Tuple:      fakeTupleData([]fakeCol{textCol("2"), textCol("bob")}),
	}
	if err := b.HandleInsert(insMsg2, cache); err != nil {
		t.Fatalf("HandleInsert #2: %v", err)
	}
	tx := b.HandleCommit(fakeCommitMsg(pglogrepl.LSN(200)))
	if tx == nil {
		t.Fatal("expected a Tx after clean begin+change+commit, got nil")
	}
	if tx.ID != 2 {
		t.Errorf("expected Tx.ID=2, got %d", tx.ID)
	}
	if len(tx.Changes) != 1 {
		t.Errorf("expected 1 change, got %d", len(tx.Changes))
	}

	if tx.Changes[0].PK != "2" {
		t.Errorf("expected Change.PK=2 from second tx, got %q", tx.Changes[0].PK)
	}
}

func TestHandleInsertMapsTypes(t *testing.T) {
	cache, _ := fakeRelationCache()
	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(10, pglogrepl.LSN(1000)))

	insMsg := &pglogrepl.InsertMessage{
		RelationID: 42,
		Tuple:      fakeTupleData([]fakeCol{textCol("99"), textCol("hello")}),
	}
	if err := b.HandleInsert(insMsg, cache); err != nil {
		t.Fatalf("HandleInsert: %v", err)
	}

	tx := b.HandleCommit(fakeCommitMsg(pglogrepl.LSN(1000)))
	if tx == nil || len(tx.Changes) != 1 {
		t.Fatal("expected one change")
	}
	ch := tx.Changes[0]

	if ch.PK != "99" {
		t.Errorf("expected PK=99, got %q", ch.PK)
	}
	if ch.PKCol != "id" {
		t.Errorf("expected PKCol=id, got %q", ch.PKCol)
	}
	if ch.Op != OpInsert {
		t.Errorf("expected OpInsert, got %q", ch.Op)
	}

	idVal, ok := ch.Data["id"]
	if !ok {
		t.Error("Data[id] missing")
	}
	if idVal != 99 {
		t.Errorf("Data[id] expected 99 (int), got %v (%T)", idVal, idVal)
	}

	nameVal, ok := ch.Data["name"]
	if !ok {
		t.Error("Data[name] missing")
	}
	if nameVal != "hello" {
		t.Errorf("Data[name] expected \"hello\", got %v", nameVal)
	}

	expectedKey := "public.test_table:99"
	if ch.Key() != expectedKey {
		t.Errorf("Key() expected %q, got %q", expectedKey, ch.Key())
	}
}

func TestHandleUpdateAbsenceNotNull(t *testing.T) {

	relMsg := &pglogrepl.RelationMessage{
		RelationID:   99,
		Namespace:    "public",
		RelationName: "multi_col",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: OIDInt4, Flags: 0x01},
			{Name: "a", DataType: OIDText, Flags: 0x00},
			{Name: "b", DataType: OIDText, Flags: 0x00},
		},
	}
	cache := newRelationCache()
	if err := cache.Update(relMsg); err != nil {
		t.Fatalf("cache.Update: %v", err)
	}

	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(20, pglogrepl.LSN(2000)))

	updMsg := &pglogrepl.UpdateMessage{
		RelationID: 99,

		NewTuple: fakeTupleData([]fakeCol{textCol("1"), toastCol(), textCol("new_b")}),
	}
	if err := b.HandleUpdate(updMsg, cache); err != nil {
		t.Fatalf("HandleUpdate: %v", err)
	}

	tx := b.HandleCommit(fakeCommitMsg(pglogrepl.LSN(2000)))
	if tx == nil || len(tx.Changes) != 1 {
		t.Fatal("expected one change")
	}
	ch := tx.Changes[0]

	if ch.Op != OpUpdate {
		t.Errorf("expected OpUpdate, got %q", ch.Op)
	}

	if _, hasA := ch.Changed["a"]; hasA {
		t.Error("Changed should NOT contain key 'a' (TOAST column, absence semantics)")
	}

	bVal, hasB := ch.Changed["b"]
	if !hasB {
		t.Error("Changed must contain key 'b'")
	}
	if bVal != "new_b" {
		t.Errorf("Changed[b] expected \"new_b\", got %v", bVal)
	}

	if ch.Data != nil {
		t.Error("Data should be nil for UPDATE")
	}
}

func TestHandleDeletePKOnly(t *testing.T) {
	cache, _ := fakeRelationCache()
	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(30, pglogrepl.LSN(3000)))

	delMsg := &pglogrepl.DeleteMessage{
		RelationID: 42,
		OldTuple:   fakeTupleData([]fakeCol{textCol("77"), textCol("")}),
	}
	if err := b.HandleDelete(delMsg, cache); err != nil {
		t.Fatalf("HandleDelete: %v", err)
	}

	tx := b.HandleCommit(fakeCommitMsg(pglogrepl.LSN(3000)))
	if tx == nil || len(tx.Changes) != 1 {
		t.Fatal("expected one change")
	}
	ch := tx.Changes[0]

	if ch.Op != OpDelete {
		t.Errorf("expected OpDelete, got %q", ch.Op)
	}
	if ch.PK != "77" {
		t.Errorf("expected PK=77, got %q", ch.PK)
	}
	if ch.Data != nil {
		t.Errorf("expected Data nil for DELETE, got %v", ch.Data)
	}
	if ch.Changed != nil {
		t.Errorf("expected Changed nil for DELETE, got %v", ch.Changed)
	}
}

func TestHandleCommitNilIfNoBegin(t *testing.T) {
	b := newTxBuilder()
	tx := b.HandleCommit(fakeCommitMsg(pglogrepl.LSN(999)))
	if tx != nil {
		t.Errorf("expected nil from HandleCommit with no Begin, got Tx{ID=%d}", tx.ID)
	}
}

func TestHandleUpdatePKToast(t *testing.T) {
	cache, _ := fakeRelationCache()
	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(40, pglogrepl.LSN(4000)))

	updMsg := &pglogrepl.UpdateMessage{
		RelationID: 42,
		NewTuple:   fakeTupleData([]fakeCol{toastCol(), textCol("new_name")}),
		OldTuple:   fakeTupleData([]fakeCol{textCol("7"), textCol("")}),
	}
	if err := b.HandleUpdate(updMsg, cache); err != nil {
		t.Fatalf("HandleUpdate with TOAST PK: %v", err)
	}

	tx := b.HandleCommit(fakeCommitMsg(pglogrepl.LSN(4000)))
	if tx == nil || len(tx.Changes) != 1 {
		t.Fatal("expected one change")
	}
	ch := tx.Changes[0]
	if ch.PK != "7" {
		t.Errorf("expected PK=7 from OldTuple fallback, got %q", ch.PK)
	}
}

func TestHandleUpdateNullValue(t *testing.T) {
	cache, _ := fakeRelationCache()
	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(50, pglogrepl.LSN(5000)))

	updMsg := &pglogrepl.UpdateMessage{
		RelationID: 42,
		NewTuple:   fakeTupleData([]fakeCol{textCol("3"), nullCol()}),
	}
	if err := b.HandleUpdate(updMsg, cache); err != nil {
		t.Fatalf("HandleUpdate with NULL: %v", err)
	}

	tx := b.HandleCommit(fakeCommitMsg(pglogrepl.LSN(5000)))
	if tx == nil || len(tx.Changes) != 1 {
		t.Fatal("expected one change")
	}
	ch := tx.Changes[0]
	nameVal, hasName := ch.Changed["name"]
	if !hasName {
		t.Error("Changed should contain 'name' (NULL is different from absent)")
	}
	if nameVal != nil {
		t.Errorf("Changed[name] should be nil for NULL, got %v", nameVal)
	}
}

func TestHandleUpdateNoOldTuple(t *testing.T) {
	cache, _ := fakeRelationCache()
	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(60, pglogrepl.LSN(6000)))

	updMsg := &pglogrepl.UpdateMessage{
		RelationID: 42,
		NewTuple:   fakeTupleData([]fakeCol{textCol("9"), textCol("updated")}),
		OldTuple:   nil,
	}
	if err := b.HandleUpdate(updMsg, cache); err != nil {
		t.Fatalf("HandleUpdate with nil OldTuple: %v", err)
	}
	tx := b.HandleCommit(fakeCommitMsg(pglogrepl.LSN(6000)))
	if tx == nil || len(tx.Changes) != 1 {
		t.Fatal("expected one change")
	}
	if tx.Changes[0].PK != "9" {
		t.Errorf("expected PK=9, got %q", tx.Changes[0].PK)
	}
}

func TestHandleDeleteNilOldTuple(t *testing.T) {
	cache, _ := fakeRelationCache()
	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(70, pglogrepl.LSN(7000)))

	delMsg := &pglogrepl.DeleteMessage{
		RelationID: 42,
		OldTuple:   nil,
	}
	if err := b.HandleDelete(delMsg, cache); err != nil {
		t.Fatalf("HandleDelete with nil OldTuple: %v", err)
	}
	tx := b.HandleCommit(fakeCommitMsg(pglogrepl.LSN(7000)))
	if tx == nil || len(tx.Changes) != 1 {
		t.Fatal("expected one change")
	}
	if tx.Changes[0].Op != OpDelete {
		t.Errorf("expected OpDelete, got %q", tx.Changes[0].Op)
	}
}

func TestHandleUpdatePKNullInNewTuple(t *testing.T) {
	cache, _ := fakeRelationCache()
	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(90, pglogrepl.LSN(9000)))

	updMsg := &pglogrepl.UpdateMessage{
		RelationID: 42,
		NewTuple:   fakeTupleData([]fakeCol{nullCol(), textCol("updated")}),
		OldTuple:   nil,
	}
	if err := b.HandleUpdate(updMsg, cache); err != nil {
		t.Fatalf("HandleUpdate with NULL PK: %v", err)
	}
	tx := b.HandleCommit(fakeCommitMsg(pglogrepl.LSN(9000)))
	if tx == nil || len(tx.Changes) != 1 {
		t.Fatal("expected one change")
	}

	if tx.Changes[0].PK != "" {
		t.Errorf("expected empty PK for NULL, got %q", tx.Changes[0].PK)
	}
}

func TestHandleInsertNilTuple(t *testing.T) {
	cache, _ := fakeRelationCache()
	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(80, pglogrepl.LSN(8000)))

	insMsg := &pglogrepl.InsertMessage{
		RelationID: 42,
		Tuple:      nil,
	}
	if err := b.HandleInsert(insMsg, cache); err != nil {
		t.Fatalf("HandleInsert with nil Tuple: %v", err)
	}
	tx := b.HandleCommit(fakeCommitMsg(pglogrepl.LSN(8000)))
	if tx == nil || len(tx.Changes) != 1 {
		t.Fatal("expected one change with nil Data")
	}
	if tx.Changes[0].Data != nil {
		t.Errorf("expected nil Data for nil Tuple, got %v", tx.Changes[0].Data)
	}
}

func TestHandleRelationDelegates(t *testing.T) {
	cache := newRelationCache()
	b := newTxBuilder()

	relMsg := &pglogrepl.RelationMessage{
		RelationID:   55,
		Namespace:    "myschema",
		RelationName: "mytable",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "pk", DataType: OIDInt8, Flags: 0x01},
		},
	}
	if err := b.HandleRelation(relMsg, cache); err != nil {
		t.Fatalf("HandleRelation: %v", err)
	}
	info, ok := cache.Get(55)
	if !ok {
		t.Error("expected relation 55 to be in cache after HandleRelation")
	}
	if info.Table != "mytable" {
		t.Errorf("expected table=mytable, got %q", info.Table)
	}
}

func TestHandleInsertUnknownRelation(t *testing.T) {
	cache := newRelationCache()
	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(1, pglogrepl.LSN(100)))

	insMsg := &pglogrepl.InsertMessage{
		RelationID: 9999,
		Tuple:      fakeTupleData([]fakeCol{textCol("1")}),
	}
	err := b.HandleInsert(insMsg, cache)
	if err == nil {
		t.Error("expected error for unknown relation OID, got nil")
	}
}

func TestHandleInsertWithoutBegin(t *testing.T) {
	cache, _ := fakeRelationCache()
	b := newTxBuilder()

	insMsg := &pglogrepl.InsertMessage{
		RelationID: 42,
		Tuple:      fakeTupleData([]fakeCol{textCol("1"), textCol("x")}),
	}
	if err := b.HandleInsert(insMsg, cache); err != nil {
		t.Errorf("HandleInsert without Begin should return nil, got %v", err)
	}
}

func TestHandleUpdateWithoutBegin(t *testing.T) {
	cache, _ := fakeRelationCache()
	b := newTxBuilder()
	updMsg := &pglogrepl.UpdateMessage{
		RelationID: 42,
		NewTuple:   fakeTupleData([]fakeCol{textCol("1"), textCol("x")}),
	}
	if err := b.HandleUpdate(updMsg, cache); err != nil {
		t.Errorf("HandleUpdate without Begin should return nil, got %v", err)
	}
}

func TestHandleDeleteWithoutBegin(t *testing.T) {
	cache, _ := fakeRelationCache()
	b := newTxBuilder()
	delMsg := &pglogrepl.DeleteMessage{
		RelationID: 42,
		OldTuple:   fakeTupleData([]fakeCol{textCol("1"), textCol("")}),
	}
	if err := b.HandleDelete(delMsg, cache); err != nil {
		t.Errorf("HandleDelete without Begin should return nil, got %v", err)
	}
}

func TestHandleUpdateUnknownRelation(t *testing.T) {
	cache := newRelationCache()
	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(1, pglogrepl.LSN(100)))
	updMsg := &pglogrepl.UpdateMessage{
		RelationID: 9999,
		NewTuple:   fakeTupleData([]fakeCol{textCol("1")}),
	}
	if err := b.HandleUpdate(updMsg, cache); err == nil {
		t.Error("expected error for unknown relation OID in HandleUpdate, got nil")
	}
}

func TestHandleDeleteUnknownRelation(t *testing.T) {
	cache := newRelationCache()
	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(1, pglogrepl.LSN(100)))
	delMsg := &pglogrepl.DeleteMessage{
		RelationID: 9999,
		OldTuple:   fakeTupleData([]fakeCol{textCol("1")}),
	}
	if err := b.HandleDelete(delMsg, cache); err == nil {
		t.Error("expected error for unknown relation OID in HandleDelete, got nil")
	}
}

func TestBuildDataMapMalformedValue(t *testing.T) {

	relMsg := &pglogrepl.RelationMessage{
		RelationID:   100,
		Namespace:    "public",
		RelationName: "err_table",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: OIDInt4, Flags: 0x01},
		},
	}
	cache := newRelationCache()
	if err := cache.Update(relMsg); err != nil {
		t.Fatalf("cache.Update: %v", err)
	}

	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(200, pglogrepl.LSN(20000)))

	insMsg := &pglogrepl.InsertMessage{
		RelationID: 100,
		Tuple:      fakeTupleData([]fakeCol{textCol("not_an_int")}),
	}
	err := b.HandleInsert(insMsg, cache)
	if err == nil {
		t.Error("expected error for malformed int4 value, got nil")
	}
}

func TestBuildChangedMapNilTuple(t *testing.T) {
	cache, _ := fakeRelationCache()
	b := newTxBuilder()
	b.HandleBegin(fakeBeginMsg(300, pglogrepl.LSN(30000)))

	updMsg := &pglogrepl.UpdateMessage{
		RelationID: 42,
		NewTuple:   nil,
		OldTuple:   nil,
	}
	if err := b.HandleUpdate(updMsg, cache); err != nil {
		t.Fatalf("HandleUpdate with nil NewTuple: %v", err)
	}
	tx := b.HandleCommit(fakeCommitMsg(pglogrepl.LSN(30000)))
	if tx == nil || len(tx.Changes) != 1 {
		t.Fatal("expected one change")
	}
	if tx.Changes[0].Changed != nil {
		t.Errorf("expected nil Changed for nil NewTuple, got %v", tx.Changes[0].Changed)
	}
}

func TestExtractPKFromTupleNotFound(t *testing.T) {

	rel := &relationInfo{
		PKCols: []string{"missing_pk"},
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "other_col", DataType: OIDInt4},
		},
	}
	tuple := fakeTupleData([]fakeCol{textCol("1")})
	_, _, err := extractPKFromTuple(tuple, rel)
	if err == nil {
		t.Error("expected error when PK column not found in tuple, got nil")
	}
}
