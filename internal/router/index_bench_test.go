package router

import (
	"fmt"
	"testing"
)

var indexCardinalities = []int{1, 1000, 10000}

func BenchmarkIndexAdd(b *testing.B) {
	for _, n := range indexCardinalities {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {

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

func BenchmarkIndexLookupMiss(b *testing.B) {
	for _, n := range indexCardinalities {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			idx := newIndex()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("public.users:%d", i)
				idx.Add(key, &Subscriber{id: key})
			}

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

func BenchmarkIndexChurn(b *testing.B) {
	for _, n := range indexCardinalities {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {

			idx := newIndex()
			keys := make([]string, n)
			subs := make([]*Subscriber, n)
			for i := 0; i < n; i++ {
				keys[i] = fmt.Sprintf("public.users:%d", i)
				subs[i] = &Subscriber{id: keys[i]}
				idx.Add(keys[i], subs[i])
			}

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
