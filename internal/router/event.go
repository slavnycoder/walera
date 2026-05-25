// Package router — event.go: Event payload type.
package router

import "github.com/walera/walera/internal/wal"

// Event is the per-subscriber tx payload. Carries the full wal.Tx plus the
// subset of change indices that matched this subscriber. Indices (not pre-filtered
// slices) bound per-(tx × subscriber) allocations to a single int slice; the
// shared tx.Changes is immutable once on the channel. Per-subscriber accumulation
// guarantees transactional atomicity (doc.go #2).
type Event struct {
	// Tx is the full transaction. Shared by reference across matched
	// subscribers — readers MUST treat it as immutable.
	Tx wal.Tx

	// MatchedIndices lists tx.Changes indices that matched this subscriber.
	// Always non-empty, ordered by replication order.
	MatchedIndices []int
}
