// Package router — index.go: 8-shard exact-subscription index. See doc.go invariants 2 + 4.
package router

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

const numShards = 8

type shard struct {
	mu   sync.RWMutex
	subs map[string]map[*Subscriber]struct{}
}

type index struct {
	shards [numShards]shard
}

func newIndex() *index {
	idx := &index{}
	for i := range idx.shards {
		idx.shards[i].subs = make(map[string]map[*Subscriber]struct{}, 16)
	}
	return idx
}

func (idx *index) shardFor(key string) *shard {
	return &idx.shards[xxhash.Sum64String(key)%numShards]
}

func (idx *index) Add(key string, sub *Subscriber) {
	s := idx.shardFor(key)
	s.mu.Lock()
	if s.subs[key] == nil {
		s.subs[key] = make(map[*Subscriber]struct{}, 1)
	}
	s.subs[key][sub] = struct{}{}
	s.mu.Unlock()
}

func (idx *index) Remove(key string, sub *Subscriber) {
	s := idx.shardFor(key)
	s.mu.Lock()
	subs := s.subs[key]
	if len(subs) > 0 {
		delete(subs, sub)
		if len(subs) == 0 {
			delete(s.subs, key)
		}
	}
	s.mu.Unlock()
}

func (idx *index) Lookup(key string) []*Subscriber {
	s := idx.shardFor(key)
	s.mu.RLock()
	subs := s.subs[key]
	if len(subs) == 0 {
		s.mu.RUnlock()
		return nil
	}
	out := make([]*Subscriber, 0, len(subs))
	for sub := range subs {
		out = append(out, sub)
	}
	s.mu.RUnlock()
	return out
}

func (idx *index) Len() int {
	total := 0
	for i := range idx.shards {
		s := &idx.shards[i]
		s.mu.RLock()
		for _, subs := range s.subs {
			total += len(subs)
		}
		s.mu.RUnlock()
	}
	return total
}

func (idx *index) Snapshot() []*Subscriber {
	out := make([]*Subscriber, 0, idx.Len())
	for i := range idx.shards {
		s := &idx.shards[i]
		s.mu.RLock()
		for _, subs := range s.subs {
			for sub := range subs {
				out = append(out, sub)
			}
		}
		s.mu.RUnlock()
	}
	return out
}
