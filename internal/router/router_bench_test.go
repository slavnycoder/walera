package router

import (
	"fmt"
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/walera/walera/internal/wal"
)

func noopSend(_ []byte) bool { return true }

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

			changes := buildChanges(shape.name, shape.changes, shape.nExact)
			tx := mkTx(pglogrepl.LSN(0x1000), 1, changes...)

			b.ReportAllocs()
			for b.Loop() {
				bc.routeTx(tx)
			}

			_ = subs
		})
	}
}

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
