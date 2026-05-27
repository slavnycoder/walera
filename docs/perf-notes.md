# Walera Performance Notes — depth-4 testbench profile

Captured 2026-05-27 against `master` at tag `v1.0.0` (merge of
`feature/transaction-scoped-delivery`), running the depth-4 testbench
workload (orders → line_items → line_item_options → option_audits with
chained bump triggers; writer fan-out 1 + 3 + 6 + 6 row inserts per tx).

## Measurement context

- **Walera limits:** `cpus: 4.0 / memory: 8G` (docker-compose deploy block,
  production parity per DOC-03).
- **Subscribers:** 2000 total — 1250 exact (`orders/1..5`) + 750 wildcard
  (`orders/all`, `devices/all`, `articles/all`), all using the
  `demo-alice` field whitelist.
- **Load:** writer driven through `POST /control` from 50 → 2000 tx/s burst.
- **pprof:** enabled via `WALERA_HTTP_PPROF_ADDR=0.0.0.0:6060` (loopback-
  exposed on host); 60 s CPU profile + heap + goroutine snapshot captured
  at writer 1500 tx/s.

### Throughput plateau

| Writer-requested tx/s | Writer actual tx/s | Walera observed tx/s | WAL lag after 45 s |
| ---:| ---:| ---:| ---:|
| 50    | 50    | 50    | 0.6 MB |
| 100   | 81    | 81    | 1.1 MB (steady) |
| 200   | 138   | 105   | 5.5 MB ↑ |
| 400   | 210   | 108   | 19 MB ↑ |
| 800   | 282   | 110   | 44 MB ↑ |
| 1500  | 332   | 110   | 85 MB ↑ |
| 2000  | burst | **109.5** | 103 MB ↑ |

**Sustained ceiling on 4 CPU / 8 GiB: ~110 tx/s of depth-4 chain commits**
under 2000 mixed subscribers. No `tx_dropped_total` and no
`slow_consumer` disconnects up to the burst. Walera CPU was 226% out of
400% available → 1.74 cores idle → the bottleneck is single-threaded, not
container-bound.

## CPU profile (60 s window, 130.1 s samples = 216% CPU)

| Symbol | cum % | Note |
| --- | ---:| --- |
| `router.(*Broadcaster).routeTx → dispatchEvent` | **45.8%** | Single broadcaster goroutine |
| `sse.(*Encoder).Encode → encoding/json.Marshal` | **33.0%** | Per-subscriber re-encode |
| `encoding/json.{struct,map,slice,array}Encoder.encode` | ~31% | Reflection-based path |
| `sse.(*poolWorker).run → drainSub → writeBuffers` | 40.4% | SSE writer side (spread over 8 workers) |
| `internal/runtime/syscall.Syscall6` (flat) | **18.0%** | Hundreds of thousands of writes/s |
| `runtime.selectgo` | 13.7% | Per-sub channel selects |
| `auth.(*Whitelist).Filter` | 9.3% | Per-(sub × row) map allocs |
| `runtime.mallocgc / scanobject` | ~17% | GC pressure from JSON + map allocs |

## Heap allocations (10.35 GB total over 60 s)

| Symbol | cum / flat | Share |
| --- | ---:| ---:|
| `router.dispatchEvent` (cum) | 9 773 MB | **94.4%** |
| `sse.Encoder.Encode` (cum) | 6 580 MB | 63.6% |
| `encoding/json.Marshal` (cum) | 4 494 MB | 43.4% |
| `auth.(*Whitelist).Filter` (flat) | 2 432 MB | **23.5%** |
| `json.mapEncoder.encode` (flat) | 1 611 MB | 15.6% |
| `sse.Encoder.Encode` (flat) | 1 602 MB | 15.5% |
| `reflect.copyVal` | 1 334 MB | 12.9% |

## Goroutine topology

- **1** broadcaster lane (`routeTx` — the bottleneck)
- 8  SSE pool workers
- 2 000 auth refresh loops (one per sub)
- 2 000 SSE writers (one per sub)
- ~20 system / supervisor goroutines

## Key insight

`router/router.go:295` — `b.enc.Encode(ev)` is invoked **per subscriber**
inside the per-tx fan-out. With ~250-sub fan-out per WAL row event and a
shared auth policy across most subs, the same JSON frame is computed
250× byte-identically. That is the single most wasted CPU cycle on the
hot path and the cheapest to fix.

## Recommendations (by expected payoff)

### 1. Memoize JSON frame per (tx, policy-hash) — top priority

Cache the encoded SSE frame inside `routeTx` keyed by the subscriber's
`AuthMap` pointer (or a stable column-set fingerprint). All wildcard
subs on the same policy reuse the same `[]byte`.

- **Where:** `internal/router/router.go:295`
- **Scope:** per-tx map; discarded when `routeTx` returns
- **Expected:** −25-30% CPU, plateau **110 → ~200 tx/s**
- **Risk:** low — read-only frame, no ordering implications

### 2. Pre-filter row once per policy in `routeTx`

`auth.(*Whitelist).Filter` allocates a fresh `map[string]any` on every
(sub × row). Compute the filtered `wal.Change` once per (row, policy)
and share by pointer across all subs in that policy class.

- **Where:** `internal/auth/map.go:34` plus a cache hop in
  `internal/router/router.go` before `dispatchEvent`
- **Expected:** −10% CPU, drops ~2.4 GB/min from the allocator
- **Synergy:** feeds the encoder in (#1) with a shared, already-filtered
  input → frame cache becomes keyed by (tx, policy) cleanly

### 3. Replace `encoding/json.Marshal` for `txToEvent`

The reflection path through `mapEncoder`/`structEncoder` is the slowest
JSON path. Options, in order of preference:

- `easyjson` / `go-json` codegen for the `txToEvent` types
- pre-marshal column values to `json.RawMessage` at WAL decode time
  (`internal/wal/...`) and emit a hand-rolled outer envelope
- as a quick win, replace `map[string]any` with `[]struct{Key, Value
  json.RawMessage}` to bypass the map-encoder

**Expected:** another −15-20% CPU once (#1) lowers the call rate.

### 4. Shard the broadcaster lane

The profile confirms `Ingest → routeTx → dispatchEvent → enqueue` runs
in **one** goroutine. CPU sat at 226% / 400% — sharding subscriber
fan-out by `xxhash(sub.ID)` across N workers unlocks the remaining
1.74 cores.

- **Cost:** 1-2 days; touches router invariants (`internal/router/INVARIANTS.md`)
- **Constraint:** per-sub ordering must hold; cross-sub ordering is not
  promised today, so shard boundaries are safe
- **Expected:** ×1.7 headroom — plateau **~250 → ~500 tx/s** after
  (#1)-(#3) are in

### 5. Coalesce SSE writes (low priority)

`writeBuffers → Syscall6` consumes 18% CPU on the syscall alone. The
worker already uses `net.Buffers`; raising `MaxBatchBytes` or merging
multiple subs into one epoll cycle saves ~5-10% syscall cost. Tail
latency benefit > throughput benefit.

- **Where:** `internal/sse/worker_loop.go`

## Ordered roadmap

| # | Change | Effort | Expected plateau |
| --- | --- | --- | ---:|
| 1 | Frame cache in `dispatchEvent`           | 1-2 h | ~200 tx/s |
| 2 | Per-policy row pre-filter                | 1-2 h | ~250 tx/s |
| 3 | Codegen / RawMessage JSON encoder        | 4-6 h | ~320 tx/s |
| 4 | Sharded broadcaster                      | 1-2 d | ~500 tx/s |
| 5 | Write coalescing tweaks                  | 2-4 h | tail-latency win |

All four primary changes target the same 4 CPU / 8 GiB envelope; (#4) is
what finally lets walera spend all four cores. Nothing here requires
relaxing the production resource cap.

## Reproducing the profile

```bash
# 1. raise admission limits + enable pprof (override file used during this run)
cat >testbench/docker-compose.override.yml <<'YAML'
services:
  walera:
    environment:
      WALERA_LIMITS_GLOBAL_CONCURRENT: "5000"
      WALERA_LIMITS_PER_USER_CONCURRENT: "5000"
      WALERA_LIMITS_PER_USER_RATE_PER_SECOND: "1000"
      WALERA_LIMITS_PER_USER_BURST: "2000"
      WALERA_LIMITS_PRE_AUTH_RATE_PER_SECOND: "1000"
      WALERA_LIMITS_PRE_AUTH_BURST: "2000"
      WALERA_HTTP_PPROF_ADDR: "0.0.0.0:6060"
      WALERA_PPROF_ALLOW_PUBLIC: "1"
    ports:
      - "127.0.0.1:6060:6060"
YAML

# 2. up the stack (depth-4 schema is already in testbench/migrations/002_demo_schema.sql)
docker compose -f testbench/docker-compose.yml \
  -f testbench/docker-compose.override.yml \
  --env-file testbench/.env up -d --build

# 3. spawn 2000 mixed subs
{ echo "orders/all"; echo "devices/all"; echo "articles/all";
  for i in 1 2 3 4 5; do echo "orders/$i"; done; } > /tmp/chans-mixed.txt
LOADGEN_AUTH_TOKEN=demo-alice ./loadgen \
  --target-url http://127.0.0.1:8080 \
  --concurrency 2000 --duration 10m --ramp-up 60s \
  --channels @/tmp/chans-mixed.txt --http-addr 127.0.0.1:9200 &

# 4. push writer past walera's plateau
curl -X POST http://127.0.0.1:9100/control \
  -H 'Content-Type: application/json' \
  -d '{"commit_rate":1500,"rows_per_tx":1,"scenario":"stress"}'

# 5. capture 60 s CPU + heap + goroutine
mkdir -p /tmp/walera-pprof
curl "http://127.0.0.1:6060/debug/pprof/profile?seconds=60" \
  -o /tmp/walera-pprof/cpu.pprof
curl "http://127.0.0.1:6060/debug/pprof/heap" \
  -o /tmp/walera-pprof/heap.pprof
curl "http://127.0.0.1:6060/debug/pprof/goroutine?debug=1" \
  -o /tmp/walera-pprof/goroutine.txt

# 6. inspect
go tool pprof -top -cum -nodecount=30 /tmp/walera-pprof/cpu.pprof
go tool pprof -alloc_space -top -nodecount=20 /tmp/walera-pprof/heap.pprof
```
