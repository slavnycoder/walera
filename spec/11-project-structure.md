# 11. Project structure (recommended)

```
/cmd/cdc-sse/main.go                 # entry point
/internal/config/                    # YAML + env config
/internal/wal/
    reader.go                        # pglogrepl wrapper, tx assembly
    relation.go                      # Relation message cache
    types.go                         # type mapper (PG → JSON)
/internal/router/
    router.go                        # tx → fan-out
    index.go                         # sharded subscription index
    wildcard.go                      # wildcard index
/internal/sse/
    handler.go                       # HTTP handler for /sse/v1/{table}/{pk}
    subscriber.go                    # subscriber struct, writer loop
    heartbeat.go                     # heartbeat ticker
    format.go                        # event serialization
/internal/auth/
    client.go                        # HTTP client to auth backend
    refresh.go                       # periodic refresh
    breaker.go                       # circuit breaker
    map.go                           # AuthMap type, whitelist filter
/internal/limits/
    global.go                        # global concurrency
    rate.go                          # per-user rate limit
    perуser.go                       # per-user connection count
/internal/metrics/                   # Prometheus metrics defs
/internal/health/                    # health endpoints
/migrations/                         # SQL for publication, slot, replication user
/test/integration/                   # testcontainers-based tests
/deploy/                             # k8s manifests, Dockerfile
```
