// Package router — router_bench_test.go: micro-benchmark coverage of
// (*Broadcaster).routeTx.
//
// BenchmarkRouteTx surfaces the per-tx allocation profile across four
// load shapes so the benchstat baseline can detect regressions introduced
// by the matchExact / matchWildcard / mergeMatches / dispatchEvent
// decomposition.
//
// Discipline:
//   - All fixtures (Broadcaster, subscribers, tx) are constructed ONCE in
//     setup. Only routeTx(tx) runs inside the timed b.Loop() body.
//   - sendFunc is overwritten with a no-op closure AFTER mkExactSub /
//     mkWildcardSub so the recorder slice does not grow per iteration.
//   - Bench fixtures do NOT mutate the package-global recordersMu /
//     recorders map; the workflow runs with -run=^$ so no prior test
//     populates that map either.
package router

import (
	"fmt"
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/walera/walera/internal/wal"
)

// noopSend is the rewired sendFunc used by every benchmarked subscriber. It
// always reports "delivered" without retaining the frame, so neither the
// router's slow_consumer path nor the recorder's frame slice grow during the
// timed loop.
func noopSend(_ []byte) bool { return true }

// BenchmarkRouteTx exercises one routeTx(tx) call per iteration against a
// pre-populated Broadcaster (matchExact + matchWildcard + mergeMatches + dispatchEvent).
//
// Four shapes:
//   - exact_1       — 1 exact sub,                  1 matching change.
//   - exact_10      — 10 exact subs,                10 matching changes.
//   - wildcard_100  — 100 wildcard subs (one table), 10 matching changes.
//   - mixed_50_50   — 25 exact + 25 wildcard subs,   10 changes
//     (5 exact-targeted + 5 wildcard-only).
//
// The Broadcaster uses cap=(1000, 10000) — matching the helper defaults in
// router_test.go::mkBroadcaster — and tx.CommitLSN is set above every sub's
// StartLSN (0) so the start_lsn filter is never the short-circuit.
func BenchmarkRouteTx(b *testing.B) {
	shapes := []struct {
		name    string
		nExact  int
		nWild   int
		changes int
	}{
		{"exact_1", 1, 0, 1},
		{"exact_10", 10, 0, 10},
		{"wildcard_100", 0, 100, 10},
		{"mixed_50_50", 25, 25, 10},
	}

	for _, shape := range shapes {
		shape := shape
		b.Run(shape.name, func(b *testing.B) {
			bc, _ := mkBroadcaster(10000)

			// Build subscribers ONCE in setup. Each sub's recorder
			// closure is overwritten with noopSend so the recorder
			// slice does not grow per iteration.
			subs := make([]*Subscriber, 0, shape.nExact+shape.nWild)
			for i := 0; i < shape.nExact; i++ {
				pk := fmt.Sprintf("%d", i+1)
				s := mkExactSub("public", "users", pk, 0, 0)
				s.WireSendFunc(noopSend)
				bc.Register(s)
				subs = append(subs, s)
			}
			for i := 0; i < shape.nWild; i++ {
				s := mkWildcardSub("public", "users", 0, 0)
				s.WireSendFunc(noopSend)
				bc.Register(s)
				subs = append(subs, s)
			}

			// Build the tx ONCE — reused across every iteration. Each
			// shape's change set is sized to surface the routing
			// allocation pattern for that fan-out shape.
			changes := buildChanges(shape.name, shape.changes, shape.nExact)
			tx := mkTx(pglogrepl.LSN(0x1000), 1, changes...)

			b.ReportAllocs()
			for b.Loop() {
				bc.routeTx(tx)
			}

			// Keep subs reachable for the duration of the loop — the
			// compiler can otherwise treat them as dead after Register.
			_ = subs
		})
	}
}

// buildChanges constructs the per-shape wal.Change slice once at setup. The
// distribution mirrors the named shape so routeTx exercises the intended
// matcher branches:
//
//   - exact_1     → one INSERT on public.users PK="1".
//   - exact_10    → 10 INSERTs on public.users PK="1".."10" (each hits one
//     distinct exact subscriber).
//   - wildcard_100 → 10 INSERTs on public.users (every change fans out to
//     all 100 wildcard subs).
//   - mixed_50_50 → 10 INSERTs on public.users PK="1".."10"; PKs 1..5 hit
//     one exact sub each, PKs 6..10 reach only the wildcard
//     subs. All 10 changes additionally fan out to every
//     wildcard sub.
func buildChanges(name string, n int, nExact int) []wal.Change {
	out := make([]wal.Change, 0, n)
	switch name {
	case "exact_1":
		out = append(out, mkChange(wal.OpInsert, "public", "users", "1"))
	case "exact_10":
		for i := 0; i < n; i++ {
			pk := fmt.Sprintf("%d", i+1)
			out = append(out, mkChange(wal.OpInsert, "public", "users", pk))
		}
	case "wildcard_100":
		for i := 0; i < n; i++ {
			pk := fmt.Sprintf("%d", i+1)
			out = append(out, mkChange(wal.OpInsert, "public", "users", pk))
		}
	case "mixed_50_50":
		for i := 0; i < n; i++ {
			pk := fmt.Sprintf("%d", i+1)
			out = append(out, mkChange(wal.OpInsert, "public", "users", pk))
		}
		_ = nExact
	}
	return out
}
