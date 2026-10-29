package router

import "sync"

// index maps an exact channel key to its subscribers. One RWMutex is enough
// at Walera's scale; lookups take the read lock and copy out.
type index struct {
	mu   sync.RWMutex
	subs map[string]map[*Subscriber]struct{}
}

func newIndex() *index {
	return &index{subs: make(map[string]map[*Subscriber]struct{})}
}

func (idx *index) Add(key string, sub *Subscriber) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.subs[key] == nil {
		idx.subs[key] = make(map[*Subscriber]struct{}, 1)
	}
	idx.subs[key][sub] = struct{}{}
}

func (idx *index) Remove(key string, sub *Subscriber) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	subs := idx.subs[key]
	delete(subs, sub)
	if len(subs) == 0 {
		delete(idx.subs, key)
	}
}

func (idx *index) Lookup(key string) []*Subscriber {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	subs := idx.subs[key]
	if len(subs) == 0 {
		return nil
	}
	out := make([]*Subscriber, 0, len(subs))
	for sub := range subs {
		out = append(out, sub)
	}
	return out
}

func (idx *index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	total := 0
	for _, subs := range idx.subs {
		total += len(subs)
	}
	return total
}

func (idx *index) Snapshot() []*Subscriber {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var out []*Subscriber
	for _, subs := range idx.subs {
		for sub := range subs {
			out = append(out, sub)
		}
	}
	return out
}
