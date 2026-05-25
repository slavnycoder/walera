// Package wal — decode.go decodes pgoutput Begin/Insert/Update/Delete/Commit
// messages into typed Tx values via the txBuilder accumulator.
//
// txBuilder accumulates Begin → Change → Commit messages into a Tx value.
// Call Reset() whenever the replication connection is dropped so that no
// partial transaction survives across reconnects.
package wal

import (
	"fmt"

	"github.com/jackc/pglogrepl"
)

// inFlightTx holds the incomplete state for a transaction that has begun but not
// yet committed.
type inFlightTx struct {
	xid      uint32
	finalLSN pglogrepl.LSN
	changes  []Change
}

// txBuilder accumulates pglogrepl decoded messages into a Tx.
//
// Usage sequence per transaction:
//
//	HandleBegin → HandleRelation / HandleInsert / HandleUpdate / HandleDelete → HandleCommit
//
// On reconnect, call Reset() before re-entering the read loop.
type txBuilder struct {
	inFlight *inFlightTx
}

// newTxBuilder returns a fresh, empty txBuilder.
func newTxBuilder() *txBuilder {
	return &txBuilder{}
}

// Reset discards any in-flight transaction state. Call this on every reconnect
// path so that no half-assembled transaction bleeds into the next replication session.
func (b *txBuilder) Reset() {
	b.inFlight = nil
}

// HandleBegin starts accumulating a new transaction.
// Any existing in-flight state is discarded (defensive; PG guarantees no nested Begin).
func (b *txBuilder) HandleBegin(msg *pglogrepl.BeginMessage) {
	b.inFlight = &inFlightTx{
		xid:      msg.Xid,
		finalLSN: msg.FinalLSN,
	}
}

// HandleRelation delegates to cache.Update and returns any resulting error.
// The caller (reader) should log non-fatal errors (errCompositePK / errUnsupportedPKType)
// and skip streaming that table.
func (b *txBuilder) HandleRelation(msg *pglogrepl.RelationMessage, cache *relationCache) error {
	return cache.Update(msg)
}

// HandleInsert builds and appends a Change{Op: OpInsert} to the in-flight transaction.
//
// If the relation OID is not yet in cache (e.g. the Relation message was skipped due
// to an unsupported PK), the change is skipped and an error is returned.
func (b *txBuilder) HandleInsert(msg *pglogrepl.InsertMessage, cache *relationCache) error {
	if b.inFlight == nil {
		return nil // no active tx; skip (defensive)
	}

	rel, ok := cache.Get(msg.RelationID)
	if !ok {
		return fmt.Errorf("wal: HandleInsert: unknown relation OID %d", msg.RelationID)
	}

	ch := Change{
		Schema: rel.Schema,
		Table:  rel.Table,
		Op:     OpInsert,
	}

	data, pk, pkCol, err := buildDataMap(msg.Tuple, rel)
	if err != nil {
		return fmt.Errorf("wal: HandleInsert %s.%s: %w", rel.Schema, rel.Table, err)
	}
	ch.Data = data
	ch.PK = pk
	ch.PKCol = pkCol

	b.inFlight.changes = append(b.inFlight.changes, ch)
	return nil
}

// HandleUpdate builds and appends a Change{Op: OpUpdate} to the in-flight transaction.
//
// Only columns present in NewTuple with a non-TOAST data type are included in
// Change.Changed. Absent columns (TOAST) are not included — absence means "not
// changed", not null (spec §5 "absence ≠ null" / D-13).
func (b *txBuilder) HandleUpdate(msg *pglogrepl.UpdateMessage, cache *relationCache) error {
	if b.inFlight == nil {
		return nil
	}

	rel, ok := cache.Get(msg.RelationID)
	if !ok {
		return fmt.Errorf("wal: HandleUpdate: unknown relation OID %d", msg.RelationID)
	}

	ch := Change{
		Schema: rel.Schema,
		Table:  rel.Table,
		Op:     OpUpdate,
	}

	// Build Changed map from NewTuple, skipping TOAST columns (absence = not changed).
	changed, pk, pkCol, err := buildChangedMap(msg.NewTuple, rel)
	if err != nil {
		return fmt.Errorf("wal: HandleUpdate %s.%s: %w", rel.Schema, rel.Table, err)
	}
	ch.Changed = changed
	ch.PK = pk
	ch.PKCol = pkCol

	// If the PK column was TOAST in NewTuple, fall back to OldTuple for the PK value.
	if ch.PK == "" && msg.OldTuple != nil {
		oldPK, oldPKCol, err := extractPKFromTuple(msg.OldTuple, rel)
		if err == nil {
			ch.PK = oldPK
			ch.PKCol = oldPKCol
		}
	}

	b.inFlight.changes = append(b.inFlight.changes, ch)
	return nil
}

// HandleDelete builds and appends a Change{Op: OpDelete} to the in-flight transaction.
//
// Under REPLICA IDENTITY DEFAULT, PG sends only the PK column in OldTuple.
// Data and Changed are intentionally nil for DELETE.
func (b *txBuilder) HandleDelete(msg *pglogrepl.DeleteMessage, cache *relationCache) error {
	if b.inFlight == nil {
		return nil
	}

	rel, ok := cache.Get(msg.RelationID)
	if !ok {
		return fmt.Errorf("wal: HandleDelete: unknown relation OID %d", msg.RelationID)
	}

	ch := Change{
		Schema: rel.Schema,
		Table:  rel.Table,
		Op:     OpDelete,
	}

	if msg.OldTuple != nil {
		pk, pkCol, err := extractPKFromTuple(msg.OldTuple, rel)
		if err == nil {
			ch.PK = pk
			ch.PKCol = pkCol
		}
	}

	b.inFlight.changes = append(b.inFlight.changes, ch)
	return nil
}

// HandleCommit finalises the in-flight transaction and returns it. Returns nil if no
// transaction is in progress (defensive — should not happen with a well-behaved PG).
func (b *txBuilder) HandleCommit(msg *pglogrepl.CommitMessage) *Tx {
	if b.inFlight == nil {
		return nil
	}
	tx := &Tx{
		ID:        b.inFlight.xid,
		CommitLSN: msg.CommitLSN,
		CommitTS:  msg.CommitTime,
		Changes:   b.inFlight.changes,
	}
	b.inFlight = nil
	return tx
}

// --- helpers ---

// buildDataMap converts all columns in a TupleData (from an InsertMessage) into a
// map[string]any using mapValue, and returns the primary key value and column name.
func buildDataMap(tuple *pglogrepl.TupleData, rel *relationInfo) (data map[string]any, pk, pkCol string, err error) {
	if tuple == nil {
		return nil, "", "", nil
	}

	data = make(map[string]any, len(tuple.Columns))
	pkColName := ""
	if len(rel.PKCols) > 0 {
		pkColName = rel.PKCols[0]
	}

	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		relCol := rel.Columns[i]

		isNull := col.DataType == pglogrepl.TupleDataTypeNull
		v, mapErr := mapValue(relCol.DataType, col.Data, isNull)
		if mapErr != nil {
			return nil, "", "", fmt.Errorf("column %q: %w", relCol.Name, mapErr)
		}
		data[relCol.Name] = v

		// Capture PK value as its text representation.
		if relCol.Name == pkColName {
			pk = textPK(col, relCol)
			pkCol = relCol.Name
		}
	}
	return data, pk, pkCol, nil
}

// buildChangedMap converts non-TOAST columns in a NewTuple (from an UpdateMessage)
// into the Changed map. TOAST columns are skipped (absence = not changed).
func buildChangedMap(tuple *pglogrepl.TupleData, rel *relationInfo) (changed map[string]any, pk, pkCol string, err error) {
	if tuple == nil {
		return nil, "", "", nil
	}

	changed = make(map[string]any)
	pkColName := ""
	if len(rel.PKCols) > 0 {
		pkColName = rel.PKCols[0]
	}

	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		relCol := rel.Columns[i]

		// Skip TOAST columns — they are unchanged, and we must not include them.
		if col.DataType == pglogrepl.TupleDataTypeToast {
			continue
		}

		isNull := col.DataType == pglogrepl.TupleDataTypeNull
		v, mapErr := mapValue(relCol.DataType, col.Data, isNull)
		if mapErr != nil {
			return nil, "", "", fmt.Errorf("column %q: %w", relCol.Name, mapErr)
		}
		changed[relCol.Name] = v

		// Capture PK if present in NewTuple (non-TOAST).
		if relCol.Name == pkColName {
			pk = textPK(col, relCol)
			pkCol = relCol.Name
		}
	}
	return changed, pk, pkCol, nil
}

// extractPKFromTuple scans a TupleData for the PK column and returns its text value.
// Used for UPDATE (fallback when PK was TOAST in NewTuple) and DELETE.
func extractPKFromTuple(tuple *pglogrepl.TupleData, rel *relationInfo) (pk, pkCol string, err error) {
	if tuple == nil || len(rel.PKCols) == 0 {
		return "", "", nil
	}
	pkColName := rel.PKCols[0]

	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		relCol := rel.Columns[i]
		if relCol.Name == pkColName {
			return textPK(col, relCol), relCol.Name, nil
		}
	}
	return "", "", fmt.Errorf("PK column %q not found in tuple", pkColName)
}

// textPK returns the PK value as a string suitable for Change.PK.
// For non-NULL text-mode columns, this is simply the raw text bytes.
func textPK(col *pglogrepl.TupleDataColumn, relCol *pglogrepl.RelationMessageColumn) string {
	if col.DataType == pglogrepl.TupleDataTypeNull {
		return ""
	}
	if col.DataType == pglogrepl.TupleDataTypeToast {
		return "" // TOAST PK → caller falls back to OldTuple
	}
	// mapValue with text representation — but for PK we just want the raw text.
	return string(col.Data)
}
