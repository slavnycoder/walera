// Package wal provides the shared types, PG→JSON type mapper, and relation cache
// for the Walera WAL pipeline.
//
// Import discipline (D-03, D-16): this package imports only the standard library
// and github.com/jackc/pglogrepl. It does NOT import any other Walera package.
package wal

import (
	"time"

	"github.com/jackc/pglogrepl"
)

// Op is the type of DML operation carried in a Change event.
type Op string

const (
	// OpInsert indicates a row was inserted.
	OpInsert Op = "insert"
	// OpUpdate indicates one or more columns of an existing row were changed.
	OpUpdate Op = "update"
	// OpDelete indicates a row was deleted.
	OpDelete Op = "delete"
)

// Change represents a single DML event within a transaction.
//
// For INSERT operations: Data contains the full new row as mapped JSON-safe values.
// For UPDATE operations: Changed contains only the columns that were modified.
//   - Absence of a column means "not changed" (not null). This distinction is critical
//     for downstream consumers (spec §5 "absence ≠ null" rule).
//
// For DELETE operations: only Schema, Table, Op, PK, and PKCol are populated.
type Change struct {
	// Schema is the PostgreSQL schema name (e.g. "public").
	Schema string
	// Table is the relation name.
	Table string
	// Op is the DML operation type (insert/update/delete).
	Op Op
	// PK is the primary-key column value serialized as text.
	PK string
	// PKCol is the primary-key column name.
	PKCol string
	// Data is the full new-row payload for INSERT operations.
	// nil for UPDATE and DELETE.
	Data map[string]any
	// Changed is the partial row payload for UPDATE operations, containing only
	// the columns whose values changed. Absent columns were not modified.
	// nil for INSERT and DELETE.
	Changed map[string]any
}

// Key returns the subscription-index lookup key for this change.
// Format: "<schema>.<table>:<pk>"
// Example: "public.users:42"
//
// This is the canonical key used by the router to dispatch changes to the
// correct subscriber set (ENT-02).
func (c Change) Key() string {
	return c.Schema + "." + c.Table + ":" + c.PK
}

// WildcardKey returns the wildcard-subscription lookup key for this change.
// Format: "<schema>.<table>"
// Example: "public.users"
//
// This is the canonical key used by the router to dispatch changes to
// wildcard ("table/all") subscribers (ROUTE-03, WC-01). It is the sibling of
// Key() for the exact-subscription path.
func (c Change) WildcardKey() string {
	return c.Schema + "." + c.Table
}

// Tx represents a fully-assembled PostgreSQL transaction.
//
// A Tx is emitted exactly once per COMMIT message received from the replication
// stream. It carries all DML changes that occurred within the transaction, in
// the order they were received.
type Tx struct {
	// ID is the PostgreSQL transaction ID (xid).
	ID uint32
	// CommitLSN is the LSN of the COMMIT record for this transaction.
	// Used for StandbyStatusUpdate acknowledgements.
	CommitLSN pglogrepl.LSN
	// CommitTS is the commit timestamp reported by PostgreSQL.
	CommitTS time.Time
	// Changes holds all DML events in this transaction, in replication order.
	Changes []Change
}
