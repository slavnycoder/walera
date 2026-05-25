// Package sse implements the Server-Sent Events delivery layer for
// Walera. It translates router.Event values into the spec §3.5 wire
// format and serves them to HTTP clients over text/event-stream. The
// Handler type registers Go 1.22 ServeMux patterns for exact and
// wildcard subscription routes; the Encoder produces SSE frames as
// pre-built byte slices.
//
// Design invariants (full register in internal/sse/INVARIANTS.md):
//  1. Single-writer-per-connection — the writer goroutine is the sole
//     owner of http.ResponseWriter for the connection's lifetime.
//  2. One Write + one Flush per frame via
//     http.NewResponseController(w).Flush().
//  3. Validate-then-execute — all input validation runs BEFORE the
//     first w.WriteHeader(200). Invalid input returns 400 JSON and
//     NEVER emits SSE response headers.
//  4. Vary: Origin unconditionally on every response (success, error,
//     preflight). CORS reflection fires only on allowlist match.
//  5. PII discipline — logs carry identifiers (subscriber_id,
//     channel, table, pk, commit_lsn, tx_id, reason), NEVER Data /
//     Changed maps or bearer tokens.
//  6. Connection-context-bound lifecycle — subscriber context derives
//     from r.Context(); client disconnect propagates Drop semantics
//     through the router.
//
// The package declares its own local broadcaster interface (Register /
// Deregister) so it can be developed and tested independently of
// *router.Broadcaster.
package sse
