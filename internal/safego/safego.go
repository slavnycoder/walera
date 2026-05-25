// Package safego provides the canonical goroutine-spawn mechanism for Walera.
//
// Every long-lived goroutine in the codebase MUST be spawned via safego.Go — direct
// go fn() calls are forbidden in production code (D-05, enforced by code review).
// This ensures that a panic in any goroutine is logged and contained rather than
// crashing the entire process.
package safego

import (
	"fmt"
	"os"
	"runtime/debug"
)

// Go spawns a named goroutine that runs fn. If fn panics, the panic is recovered,
// a diagnostic message including the name, panic value, and stack trace is written
// to stderr, and the goroutine exits cleanly. The calling goroutine is unaffected.
//
// name is used purely for diagnostics — choose a stable, human-readable identifier
// such as "wal-reader" or "standby-ticker".
//
// Note: safego intentionally uses only stdlib (fmt, os, runtime/debug) to avoid
// import cycles — it is the lowest-level cross-cutting package in the codebase.
func Go(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "safego: goroutine %q panic: %v\nstack: %s\n",
					name, r, debug.Stack())
			}
		}()
		fn()
	}()
}
