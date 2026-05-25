// Package app — leak_main_test.go owns the goleak goroutine-leak guard
// for every test in this package. Mirrors internal/sse/leak_main_test.go.
//
// The pattern is from go.uber.org/goleak: a single TestMain wraps the
// test runner, calls VerifyTestMain at exit, and fails the run if any
// goroutine remains beyond the ignore set. Every test in this package
// that constructs *App is implicitly covered.
//
// VerifyTestMain is used (not per-test VerifyNone) because every test in
// this package calls t.Parallel(); goleak documents VerifyNone as
// incompatible with parallel tests.
package app

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreCurrent(),
		// Add process-scoped ignores here, each on its own line with a
		// comment naming the goroutine and the reason it is exempt.
		// Expected to be empty under default conditions; if a future
		// test surfaces a legitimate process-scoped goroutine, add an
		// explicit goleak.IgnoreTopFunction(...) entry with a
		// complete-sentence reason naming why the goroutine outlives
		// the test process.
	)
}
