package router

import "sync"

type wildcardIndex struct {
	mu sync.RWMutex
	m  map[string][]*Subscriber
}

func newWildcardIndex() *wildcardIndex {
	return &wildcardIndex{m: make(map[string][]*Subscriber)}
}

func (w *wildcardIndex) Add(key string, sub *Subscriber) {
	w.mu.Lock()
	w.m[key] = append(w.m[key], sub)
	w.mu.Unlock()
}

func (w *wildcardIndex) Remove(key string, sub *Subscriber) {
	w.mu.Lock()
	defer w.mu.Unlock()
	subs := w.m[key]
	for i, s := range subs {
		if s == sub {
			w.m[key] = append(subs[:i], subs[i+1:]...)
			if len(w.m[key]) == 0 {
				delete(w.m, key)
			}
			return
		}
	}
}

func (w *wildcardIndex) Lookup(key string) []*Subscriber {
	w.mu.RLock()
	defer w.mu.RUnlock()
	subs := w.m[key]
	if len(subs) == 0 {
		return nil
	}
	out := make([]*Subscriber, len(subs))
	copy(out, subs)
	return out
}

func (w *wildcardIndex) Len() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	total := 0
	for _, subs := range w.m {
		total += len(subs)
	}
	return total
}

func (w *wildcardIndex) Snapshot() []*Subscriber {
	w.mu.RLock()
	defer w.mu.RUnlock()
	total := 0
	for _, subs := range w.m {
		total += len(subs)
	}
	out := make([]*Subscriber, 0, total)
	for _, subs := range w.m {
		out = append(out, subs...)
	}
	return out
}
