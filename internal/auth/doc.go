// Package auth implements the authorization client, permission Whitelist +
// field-level Filter, the per-subscriber refresh Subscriber, and the
// auth circuit breaker for Walera.
//
// File layout:
//
//   - doc.go        — this file.
//   - config.go     — auth.Config + BreakerConfig.
//   - errors.go     — typed errors: ErrUnauthorized, ErrForbidden, ErrNotFound,
//     ErrUnavailable (carrying the upstream response body for
//     verbatim SSE forwarding), and the ErrBreakerOpen sentinel
//     used by background refresh skip-logic.
//   - map.go        — Whitelist (per-user permission snapshot) + Filter (field-level
//     redaction; PK always preserved) + ParseWhitelist wire validation.
//   - client.go     — Client (HTTP client + Permissions/Health) + BreakerHook
//     interface.
//   - breaker.go        — hand-rolled circuit-breaker FSM.
//   - breaker_window.go — sliding-window failure-rate calculator.
//   - subscriber.go     — per-subscriber refresh routine + Whitelist.Swap atomic
//     publication + LSN stamping.
//   - registry.go       — auth-subscriber registry + stale-subs sampler that
//     writes walera_auth_breaker_stale_subscribers.
//
// Security invariant: tokens never appear in any zerolog statement. Bearer
// tokens flow only through HTTP header parameters; capture into struct
// fields or local variables beyond the header set call is forbidden.
// The grep gate
//
//	! grep -nE 'Str\(.*[Tt]oken|Str\(.*Authorization' internal/auth/client.go
//
// enforces this.
//
// See also internal/auth/INVARIANTS.md for the canonical invariant list.
package auth
