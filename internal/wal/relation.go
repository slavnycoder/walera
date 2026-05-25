package wal

import (
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
)

var allowedPKOIDs = map[uint32]bool{
	OIDInt2: true,
	OIDInt4: true,
	OIDInt8: true,
	OIDUUID: true,
	OIDText: true,
}

var errUnsupportedPKType = errors.New("relation: PK column OID not in allowed scalar set (int2/int4/int8/uuid/text)")

var errCompositePK = errors.New("relation: composite PK not supported — single-column scalar PK required (ENT-02)")

type relationInfo struct {
	OID uint32

	Schema string

	Table string

	PKCols []string

	PKColOID uint32

	Columns []*pglogrepl.RelationMessageColumn
}

type relationCache struct {
	m map[uint32]*relationInfo
}

func newRelationCache() *relationCache {
	return &relationCache{m: make(map[uint32]*relationInfo)}
}

func (c *relationCache) Update(msg *pglogrepl.RelationMessage) error {

	var pkCols []*pglogrepl.RelationMessageColumn
	for _, col := range msg.Columns {
		if col.Flags&0x01 != 0 {
			pkCols = append(pkCols, col)
		}
	}

	if len(pkCols) > 1 {
		return fmt.Errorf("%w: table %q has %d PK columns",
			errCompositePK, msg.Namespace+"."+msg.RelationName, len(pkCols))
	}

	if len(pkCols) == 0 {
		return fmt.Errorf("%w: table %q has no PK column flagged (check REPLICA IDENTITY)",
			errCompositePK, msg.Namespace+"."+msg.RelationName)
	}

	pk := pkCols[0]
	if !allowedPKOIDs[pk.DataType] {
		return fmt.Errorf("%w: table %q column %q has OID %d",
			errUnsupportedPKType, msg.Namespace+"."+msg.RelationName, pk.Name, pk.DataType)
	}

	pkColNames := make([]string, len(pkCols))
	for i, col := range pkCols {
		pkColNames[i] = col.Name
	}

	info := &relationInfo{
		OID:      msg.RelationID,
		Schema:   msg.Namespace,
		Table:    msg.RelationName,
		PKCols:   pkColNames,
		PKColOID: pk.DataType,
		Columns:  msg.Columns,
	}

	c.m[msg.RelationID] = info
	return nil
}

func (c *relationCache) Get(oid uint32) (*relationInfo, bool) {
	info, ok := c.m[oid]
	return info, ok
}
