# 4. Routing and WAL pipeline

## 4.1. Goroutine topology

```
[PG replication conn]
        │
        ▼
   reader goroutine ── tx assembled on COMMIT ──▶ txCh (buffered chan, cap=128)
                                                       │
                                                       ▼
                                                router goroutine
                                                       │
                                                  index lookup +
                                                  whitelist filter +
                                                  per-subscriber accumulator
                                                       │
                                                       ▼
                                              non-blocking send to
                                              each subscriber.ch
                                                       │
                                              ┌────────┴────────┬─────────┐
                                              ▼                 ▼         ▼
                                       writer goroutine    writer       writer
                                         (per sub)         (per sub)    (per sub)
                                              │
                                              ▼
                                         [TCP conn]
```

Other goroutines:
- **Standby ticker (1):** sends `StandbyStatusUpdate` to PG every 5s, reading `lastCommittedLSN` atomically.
- **Heartbeat ticker:** one per subscriber, emits SSE comment every 15s ([§6.2](06-network-behavior.md#62-heartbeat)).
- **Auth refresh ticker:** one per subscriber, period = `ttl_seconds`.
- **Metrics/health HTTP server:** standard.

Total: ~20k+ goroutines at full load. This is well within Go's capacity.

## 4.2. Subscription index (exact subscriptions)

Sharded hash map. Shard count: 64.

```go
type Shard struct {
    mu   sync.RWMutex
    subs map[string]map[*Subscriber]struct{} // key: "schema.table:pk"
}

type Index struct {
    shards [64]Shard
}

func (idx *Index) shardFor(key string) *Shard {
    return &idx.shards[xxhash.Sum64String(key)%64]
}
```

Lookup:
```go
s := idx.shardFor(key)
s.mu.RLock()
subsCopy := make([]*Subscriber, 0, len(s.subs[key]))
for sub := range s.subs[key] {
    subsCopy = append(subsCopy, sub)
}
s.mu.RUnlock()
// process subsCopy outside the lock
```

**Always release the shard lock BEFORE doing any work on subscribers (filtering, enqueuing).** The lock guards the index only.

Use `map[*Subscriber]struct{}` (not slice) for O(1) removal at unsubscribe.

## 4.3. Wildcard index

Single map, single RWMutex. Wildcard subscriptions are rare relative to exact ones; sharding is unnecessary.

```go
type WildcardIndex struct {
    mu   sync.RWMutex
    subs map[string]map[*Subscriber]struct{} // key: "schema.table"
}
```

## 4.4. Tx routing with root-entity membership

A subscription's routing key is the **root entity row** the tx touches, not the individual change ([§1.6](01-data-source-and-wal.md#16-entity-model)). The router determines, per subscriber, which root rows in the tx match that subscriber's channel and `authMap.roots`, then forwards the **entire tx** (whitelist-filtered) as one SSE event.

```go
// Phase 1: collect candidate subscribers via any change in the tx.
candidates := map[*Subscriber]struct{}{}
for _, change := range tx.Changes {
    subs := union(exactIndex.Lookup(change.Key), wildcardIndex.Lookup(change.Table))
    for _, sub := range subs {
        if tx.CommitLSN > sub.startLSN {
            candidates[sub] = struct{}{}
        }
    }
}

// Phase 2: per-subscriber root membership check.
accumulator := map[*Subscriber][]FilteredChange{}
for sub := range candidates {
    authMap := sub.authMap.Load()

    matchedRoots := map[ChangeKey]struct{}{}
    for _, change := range tx.Changes {
        if !authMap.IsRoot(change.Table)           { continue }
        if !sub.channelMatches(change.Table, change.PK) { continue }
        matchedRoots[change.Key] = struct{}{}
    }

    switch len(matchedRoots) {
    case 0:
        // No root row for this subscriber in this tx — silent skip (normal case).
        continue
    case 1:
        // Single root entity matched → deliver all whitelist-passing changes from the tx.
        for _, change := range tx.Changes {
            filtered, ok := applyWhitelist(change, authMap)
            if !ok { continue } // change fully hidden by whitelist
            accumulator[sub] = append(accumulator[sub], filtered)
        }
    default:
        // Backend discipline violation: tx touches 2+ root rows matching this subscriber.
        log.Warn("multi-root tx dropped for subscriber",
            "tx_id", tx.ID, "commit_lsn", tx.CommitLSN,
            "subscriber_id", sub.ID, "channel", sub.Channel,
            "matched_roots", keys(matchedRoots))
        metrics.TxDroppedTotal.WithLabelValues("multi_root").Inc()
        continue
    }
}

// Phase 3: send (existing logic).
for sub, changes := range accumulator {
    if len(changes) > sub.maxTxSize { kill(sub, "tx_too_large"); continue }
    event := buildTxEvent(tx, changes)
    select {
    case sub.ch <- event:
    default:
        kill(sub, "slow_consumer")
    }
}
```

Properties:
- **Atomicity:** a subscriber receives all changes from a tx (root + children, whitelisted) in one SSE event, or none — never a partial slice.
- **Multi-root drop is per-subscriber, not per-tx.** The same tx may be valid for exact subscribers (whose channel matches only one of the root rows) while being dropped for wildcard subscribers (whose channel matches all root rows). The wildcard case is the primary signal of a backend bulk-operation bug — see [§1.6](01-data-source-and-wal.md#16-entity-model) and alerts in [§8.4](08-observability.md#84-alerts).
- **No-root tx is silent.** It is the normal case for txs whose changes don't cross any subscriber's root set; nothing to log, nothing to drop.

## 4.5. Backpressure

**Router → subscriber:** non-blocking send. On full buffer, disconnect.

**Subscriber → router:** NEVER. A slow consumer must not block routing.

**Router → reader:** via buffered `txCh`. If the router falls behind, the reader blocks on send to `txCh` and stops reading from PG. This is the correct behavior — surfaces as `wal_lsn_lag_bytes` metric.

## 4.6. Subscriber cleanup

Do NOT close `sub.ch` explicitly. The classic Go pitfall: if the router sends to a closed channel, it panics.

Instead:
1. Cancel `sub.ctx` (causes writer goroutine to exit).
2. Acquire the shard lock, remove subscriber from the index.
3. After unlock, the router will no longer find this subscriber. Existing in-flight sends drain into the buffer; the GC collects the channel when no references remain.

The `select { case sub.ch <- ev: default: kill }` pattern means a stray send post-removal simply lands in the buffer harmlessly (no reader, but no panic either).
