package wal

import (
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
)

// allowedPKOIDs is the set of PostgreSQL type OIDs that are permitted as the
// sole primary-key column on a replicated table (D-24, ENT-02).
//
// Rationale: PK values are used as subscription keys and written to the wire.
// Only scalar types whose text representation is stable, compact, and safe as a
// URL/key component are allowed:
//   - int2 (21): small integer
//   - int4 (23): integer
//   - int8 (20): bigint
//   - uuid (2950): universally unique identifier
//   - text (25): variable-length string (operator must ensure uniqueness)
//
// Notably absent: jsonb (3802), bytea (17), numeric (1700), citext, and any
// composite or custom types. These cannot serve as TOAST-safe identity columns
// for pgoutput logical replication.
var allowedPKOIDs = map[uint32]bool{
	OIDInt2: true,
	OIDInt4: true,
	OIDInt8: true,
	OIDUUID: true,
	OIDText: true,
}

// errUnsupportedPKType is returned by relationCache.Update when the declared PK
// column has a type OID that is not in the allowed scalar set.
// Per D-24: only int2/int4/int8/uuid/text are permitted.
var errUnsupportedPKType = errors.New("relation: PK column OID not in allowed scalar set (int2/int4/int8/uuid/text)")

// errCompositePK is returned by relationCache.Update when the relation message
// declares more than one column with the PK flag set.
// Per ENT-02: Walera only supports single-column scalar primary keys.
var errCompositePK = errors.New("relation: composite PK not supported — single-column scalar PK required (ENT-02)")

// relationInfo holds the decoded metadata for a single replicated relation
// (table), derived from a pglogrepl.RelationMessage.
type relationInfo struct {
	// OID is the PostgreSQL relation OID (RelationMessage.RelationID).
	OID uint32
	// Schema is the schema (namespace) name (e.g. "public").
	Schema string
	// Table is the relation name.
	Table string
	// PKCols holds the names of the column(s) flagged as PK (Flags & 0x01 != 0).
	// After a successful Update(), this slice always has exactly one entry.
	PKCols []string
	// PKColOID is the type OID of the single PK column.
	PKColOID uint32
	// Columns is the full column list from the Relation message, in order.
	// Stored by value so callers can iterate without holding any lock.
	Columns []*pglogrepl.RelationMessageColumn
}

// relationCache is an in-memory store of relation metadata, keyed by relation OID.
//
// Single-writer invariant: only the WAL reader goroutine calls Update().
// Multiple goroutines may call Get() concurrently.
//
// Note: no mutex is required today because the reader is the sole writer
// and reads happen only from assembly.go running on the same goroutine. If a
// future change introduces cross-goroutine access, add sync.RWMutex here.
type relationCache struct {
	m map[uint32]*relationInfo
}

// newRelationCache creates an empty relationCache.
func newRelationCache() *relationCache {
	return &relationCache{m: make(map[uint32]*relationInfo)}
}

// Update validates and stores the relation metadata from a pglogrepl.RelationMessage.
//
// Validation rules (D-17, D-24, ENT-02):
//  1. The relation must have exactly one PK column (Flags & 0x01 != 0) — composite
//     PKs return errCompositePK.
//  2. The single PK column's type OID must be in the allowed scalar set
//     {int2, int4, int8, uuid, text} — any other OID returns errUnsupportedPKType.
//
// If the same relation OID was previously cached (schema change / DDL), the old
// entry is replaced atomically.
func (c *relationCache) Update(msg *pglogrepl.RelationMessage) error {
	// Collect PK columns.
	var pkCols []*pglogrepl.RelationMessageColumn
	for _, col := range msg.Columns {
		if col.Flags&0x01 != 0 {
			pkCols = append(pkCols, col)
		}
	}

	// ENT-02: exactly one PK column.
	if len(pkCols) > 1 {
		return fmt.Errorf("%w: table %q has %d PK columns",
			errCompositePK, msg.Namespace+"."+msg.RelationName, len(pkCols))
	}

	// If there are no PK columns (no REPLICA IDENTITY or IDENTITY NOTHING),
	// we cannot route changes — reject.
	if len(pkCols) == 0 {
		return fmt.Errorf("%w: table %q has no PK column flagged (check REPLICA IDENTITY)",
			errCompositePK, msg.Namespace+"."+msg.RelationName)
	}

	// D-24: PK column OID must be in the allowed scalar set.
	pk := pkCols[0]
	if !allowedPKOIDs[pk.DataType] {
		return fmt.Errorf("%w: table %q column %q has OID %d",
			errUnsupportedPKType, msg.Namespace+"."+msg.RelationName, pk.Name, pk.DataType)
	}

	// Build PKCols name list.
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

// Get returns the cached relationInfo for the given relation OID.
// Returns (nil, false) if the OID is not in the cache.
func (c *relationCache) Get(oid uint32) (*relationInfo, bool) {
	info, ok := c.m[oid]
	return info, ok
}
