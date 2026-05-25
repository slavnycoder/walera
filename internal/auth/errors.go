// Package auth — errors.go: typed errors returned by Client.Permissions.
// Body bytes carry the upstream response (bounded ≤ 64 KiB) so the SSE
// handler can forward verbatim.
package auth

import (
	"errors"
	"fmt"
)

// ErrBreakerOpen is the sentinel for background-refresh skip-logic. The live
// SSE handler does NOT consume this — new opens always attempt the auth call.
var ErrBreakerOpen = errors.New("auth: circuit breaker open")

// ErrUnauthorized is returned for HTTP 401 responses.
type ErrUnauthorized struct{ Body []byte }

func (e *ErrUnauthorized) Error() string { return "auth: unauthorized" }

// ErrForbidden is returned for HTTP 403 responses.
type ErrForbidden struct{ Body []byte }

func (e *ErrForbidden) Error() string { return "auth: forbidden" }

// ErrNotFound is returned for HTTP 404 responses.
type ErrNotFound struct{ Body []byte }

func (e *ErrNotFound) Error() string { return "auth: not found" }

// ErrUnavailable is returned for HTTP 5xx, network errors, timeouts, malformed
// JSON, and invalid-shape Whitelist responses. Wraps Cause for errors.Is/As.
type ErrUnavailable struct{ Cause error }

func (e *ErrUnavailable) Error() string {
	if e.Cause == nil {
		return "auth: unavailable"
	}
	return fmt.Sprintf("auth: unavailable: %s", e.Cause.Error())
}

// Unwrap returns the wrapped cause for errors.Is / errors.As.
func (e *ErrUnavailable) Unwrap() error { return e.Cause }
