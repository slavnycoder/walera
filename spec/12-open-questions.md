# 12. Open questions and future work

- **Snapshot integration:** tighter coupling with the snapshot backend so it returns the LSN of the snapshot; clients then pass `?since_lsn=`.
- **Multi-channel per connection:** if HTTP/2 multiplexing proves insufficient, add a mode where one SSE stream carries many channels with REST-side subscription management.
- **WHERE-filter subscriptions:** reactive queries (`users:where(role='admin')`). Major effort — needs separate design.
- **Declarative composite-view mapping:** today composite views rely on backend discipline (root-bump in same tx). A future enhancement is declarative FK-aware routing — the service learns child→root relationships and infers the routing key without backend cooperation. Removes the multi-root-drop class of bugs at the cost of join logic in the hot path.
- **Active-passive HA:** for higher availability SLAs.
- **N-instance scale-out** via a message bus when single-instance capacity is exceeded.
- **Binary pgoutput mode:** CPU optimization if parsing becomes a bottleneck.
