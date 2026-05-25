# 13. Implementation order (suggested)

For incremental delivery, build in this sequence:

1. **WAL reader skeleton** — connect to PG, read pgoutput, log received messages. Verify with manual INSERT/UPDATE/DELETE.
2. **Type mapper** — implement and unit-test PG → JSON for all relevant types.
3. **Transaction assembly** — buffer Begin/Change/Commit; emit `Transaction` structs.
4. **Subscription index** — sharded map, basic add/remove/lookup; unit tests with `-race`.
5. **Router** — consume txs, do lookup, build per-subscriber events; no auth filter yet.
6. **SSE handler** — minimal: open connection, register subscriber, write events. No auth, no heartbeat, no limits.
7. **Auth client + filter** — call backend, apply whitelist, drop hidden updates.
8. **Auth refresh** — periodic ticker, atomic pointer swap, revoke handling.
9. **Heartbeat** — SSE comment ticker.
10. **Limits** — global, per-user, rate, tx size.
11. **Wildcard subscriptions** — second index, routing changes.
12. **Backpressure / slow-consumer disconnect** — non-blocking send, kill on overflow.
13. **LSN ack** — standby status ticker; `lastCommittedLSN` atomic.
14. **Graceful shutdown** — signal handling, full sequence.
15. **Metrics, logs, health** — full observability layer.
16. **Integration test suite** — testcontainers, all scenarios from [§9.2](09-testing.md#92-integration-tests-testcontainers--pg).
17. **Load testing** — separate harness.
18. **Production hardening** — circuit breaker, pg reconnection backoff, error paths.

Each step should be a separate commit/PR with passing tests under `-race`.
