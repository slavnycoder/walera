// Package router — index.go: 8-shard exact-subscription index. See doc.go invariants 2 + 4.
package router

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

const numShards = 8

type shard struct {
	mu   sync.RWMutex
	subs map[string]*Subscriber
}

type index struct {
	shards [numShards]shard
}

func newIndex() *index {
	idx := &index{}
	for i := range idx.shards {
		idx.shards[i].subs = make(map[string]*Subscriber, 16)
	}
	return idx
}

func (idx *index) shardFor(key string) *shard {
	return &idx.shards[xxhash.Sum64String(key)%numShards]
}

func (idx *index) Add(key string, sub *Subscriber) {
	s := idx.shardFor(key)
	s.mu.Lock()
	s.subs[key] = sub
	s.mu.Unlock()
}

func (idx *index) Remove(key string) {
	s := idx.shardFor(key)
	s.mu.Lock()
	delete(s.subs, key)
	s.mu.Unlock()
}

func (idx *index) Lookup(key string) *Subscriber {
	s := idx.shardFor(key)
	s.mu.RLock()
	sub := s.subs[key]
	s.mu.RUnlock()
	return sub
}

func (idx *index) Len() int {
	total := 0
	for i := range idx.shards {
		s := &idx.shards[i]
		s.mu.RLock()
		total += len(s.subs)
		s.mu.RUnlock()
	}
	return total
}

func (idx *index) Snapshot() []*Subscriber {
	out := make([]*Subscriber, 0, idx.Len())
	for i := range idx.shards {
		s := &idx.shards[i]
		s.mu.RLock()
		for _, sub := range s.subs {
			out = append(out, sub)
		}
		s.mu.RUnlock()
	}
	return out
}
