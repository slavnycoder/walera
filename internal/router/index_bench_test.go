// Package router — index_bench_test.go: in-package micro-benchmarks for the
// exact-index hot path (Add/Lookup-hit/Lookup-miss/Churn).
//
// These benchmarks isolate internal/router/index.go operations only — NOT the
// full routeTx pipeline. They establish the ≤ 3% allocs/op gate GEN-02 (shard
// reduction) must stay within. Run via:
//
//	go test -bench='^BenchmarkIndex' -benchmem -count=10 -run='^$' ./internal/router/
//
// Discipline (mirrors router_bench_test.go):
//   - All fixtures (index, keys, Subscribers) are built ONCE in setup.
//   - Only the measured index operation runs inside b.Loop().
//   - b.ReportAllocs() is called before the loop so -benchmem captures it.
//   - Synthetic keys only: fmt.Sprintf("public.users:%d", i) — no real tokens or PII.
//
// NOTE: -race is deliberately omitted from the capture command; race
// instrumentation invalidates allocation measurement (shadow memory doubles
// heap allocs/op). See testbench/bench-baseline-index-v2.5.txt Notes.
package router

import (
	"fmt"
	"testing"
)

// indexCardinalities are the sub-benchmark sizes exercised by each benchmark.
// n=1 isolates per-op cost; n=1000 mid-scale; n=10000 reflects the
// ~10k-subscriber single-instance target relevant to GEN-02's shard decision.
var indexCardinalities = []int{1, 1000, 10000}

// BenchmarkIndexAdd measures the register (Add) path. A fresh index is created
// per sub-benchmark size; all keys and Subscriber pointers are built in setup so
// only the Add call runs in the timed loop.
func BenchmarkIndexAdd(b *testing.B) {
	for _, n := range indexCardinalities {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			// Build keys and subscribers once in setup.
			keys := make([]string, n)
			subs := make([]*Subscriber, n)
			for i := 0; i < n; i++ {
				keys[i] = fmt.Sprintf("public.users:%d", i)
				subs[i] = &Subscriber{id: keys[i]}
			}

			b.ReportAllocs()
			idx := newIndex()
			var i int
			for b.Loop() {
				idx.Add(keys[i%n], subs[i%n])
				i++
			}
		})
	}
}

// BenchmarkIndexLookupHit measures the Lookup path for keys that ARE present
// in the index (100% hit rate). The index is pre-populated in setup; the timed
// loop only calls Lookup.
func BenchmarkIndexLookupHit(b *testing.B) {
	for _, n := range indexCardinalities {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			idx := newIndex()
			keys := make([]string, n)
			for i := 0; i < n; i++ {
				keys[i] = fmt.Sprintf("public.users:%d", i)
				idx.Add(keys[i], &Subscriber{id: keys[i]})
			}

			b.ReportAllocs()
			var i int
			for b.Loop() {
				_ = idx.Lookup(keys[i%n])
				i++
			}
		})
	}
}

// BenchmarkIndexLookupMiss measures the Lookup path for keys that are NOT
// present in the index (100% miss rate). The index is pre-populated with
// "public.users:%d" keys; the timed loop looks up "absent.users:%d" keys
// which hash into the same shards but will never be found.
func BenchmarkIndexLookupMiss(b *testing.B) {
	for _, n := range indexCardinalities {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			idx := newIndex()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("public.users:%d", i)
				idx.Add(key, &Subscriber{id: key})
			}
			// Absent keys — distinct namespace, guaranteed never present.
			absentKeys := make([]string, n)
			for i := 0; i < n; i++ {
				absentKeys[i] = fmt.Sprintf("absent.users:%d", i)
			}

			b.ReportAllocs()
			var i int
			for b.Loop() {
				_ = idx.Lookup(absentKeys[i%n])
				i++
			}
		})
	}
}

// BenchmarkIndexChurn measures the register/unregister churn path — one Add
// followed by one Remove per iteration on the same key. This models the
// subscribe/disconnect lifecycle at high turnover.
func BenchmarkIndexChurn(b *testing.B) {
	for _, n := range indexCardinalities {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			// Pre-populate to represent a live index of size n.
			idx := newIndex()
			keys := make([]string, n)
			subs := make([]*Subscriber, n)
			for i := 0; i < n; i++ {
				keys[i] = fmt.Sprintf("public.users:%d", i)
				subs[i] = &Subscriber{id: keys[i]}
				idx.Add(keys[i], subs[i])
			}

			// Churn keys are in a distinct namespace to avoid displacing the
			// pre-populated entries; they exist for the lifetime of the loop.
			churnKey := "churn.users:0"
			churnSub := &Subscriber{id: churnKey}

			b.ReportAllocs()
			for b.Loop() {
				idx.Add(churnKey, churnSub)
				idx.Remove(churnKey, churnSub)
			}
		})
	}
}
