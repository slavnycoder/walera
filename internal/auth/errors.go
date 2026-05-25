package auth

import (
	"errors"
	"fmt"
)

var ErrBreakerOpen = errors.New("auth: circuit breaker open")

type ErrUnauthorized struct{ Body []byte }

func (e *ErrUnauthorized) Error() string { return "auth: unauthorized" }

type ErrForbidden struct{ Body []byte }

func (e *ErrForbidden) Error() string { return "auth: forbidden" }

type ErrNotFound struct{ Body []byte }

func (e *ErrNotFound) Error() string { return "auth: not found" }

type ErrUnavailable struct{ Cause error }

func (e *ErrUnavailable) Error() string {
	if e.Cause == nil {
		return "auth: unavailable"
	}
	return fmt.Sprintf("auth: unavailable: %s", e.Cause.Error())
}

func (e *ErrUnavailable) Unwrap() error { return e.Cause }
