package wal

import (
	"fmt"

	"github.com/jackc/pglogrepl"
)

type inFlightTx struct {
	xid      uint32
	finalLSN pglogrepl.LSN
	changes  []Change
}

type txBuilder struct {
	inFlight *inFlightTx
	mapper   valueMapper
}

func newTxBuilder() *txBuilder {
	return newTxBuilderWithConfig(Config{NaiveTimestampAssumeUTC: defaultNaiveTimestampAssumeUTC})
}

func newTxBuilderWithConfig(cfg Config) *txBuilder {
	return &txBuilder{mapper: newValueMapper(cfg)}
}

func (b *txBuilder) Reset() {
	b.inFlight = nil
}

func (b *txBuilder) HandleBegin(msg *pglogrepl.BeginMessage) {
	b.inFlight = &inFlightTx{
		xid:      msg.Xid,
		finalLSN: msg.FinalLSN,
	}
}

func (b *txBuilder) HandleRelation(msg *pglogrepl.RelationMessage, cache *relationCache) error {
	return cache.Update(msg)
}

func (b *txBuilder) HandleInsert(msg *pglogrepl.InsertMessage, cache *relationCache) error {
	if b.inFlight == nil {
		return nil
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

	data, pk, pkCol, err := buildDataMap(msg.Tuple, rel, b.mapper)
	if err != nil {
		return fmt.Errorf("wal: HandleInsert %s.%s: %w", rel.Schema, rel.Table, err)
	}
	ch.Data = data
	ch.PK = pk
	ch.PKCol = pkCol

	b.inFlight.changes = append(b.inFlight.changes, ch)
	return nil
}

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

	changed, pk, pkCol, err := buildChangedMap(msg.NewTuple, rel, b.mapper)
	if err != nil {
		return fmt.Errorf("wal: HandleUpdate %s.%s: %w", rel.Schema, rel.Table, err)
	}
	ch.Changed = changed
	ch.PK = pk
	ch.PKCol = pkCol

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

func buildDataMap(tuple *pglogrepl.TupleData, rel *relationInfo, mapper valueMapper) (data map[string]any, pk, pkCol string, err error) {
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
		v, mapErr := mapper.mapValue(relCol.DataType, col.Data, isNull)
		if mapErr != nil {
			return nil, "", "", fmt.Errorf("column %q: %w", relCol.Name, mapErr)
		}
		data[relCol.Name] = v

		if relCol.Name == pkColName {
			pk = textPK(col, relCol)
			pkCol = relCol.Name
		}
	}
	return data, pk, pkCol, nil
}

func buildChangedMap(tuple *pglogrepl.TupleData, rel *relationInfo, mapper valueMapper) (changed map[string]any, pk, pkCol string, err error) {
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

		if col.DataType == pglogrepl.TupleDataTypeToast {
			continue
		}

		isNull := col.DataType == pglogrepl.TupleDataTypeNull
		v, mapErr := mapper.mapValue(relCol.DataType, col.Data, isNull)
		if mapErr != nil {
			return nil, "", "", fmt.Errorf("column %q: %w", relCol.Name, mapErr)
		}
		changed[relCol.Name] = v

		if relCol.Name == pkColName {
			pk = textPK(col, relCol)
			pkCol = relCol.Name
		}
	}
	return changed, pk, pkCol, nil
}

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

func textPK(col *pglogrepl.TupleDataColumn, relCol *pglogrepl.RelationMessageColumn) string {
	if col.DataType == pglogrepl.TupleDataTypeNull {
		return ""
	}
	if col.DataType == pglogrepl.TupleDataTypeToast {
		return ""
	}

	return string(col.Data)
}
