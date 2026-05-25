// Package sse — leak_main_test.go owns the goleak goroutine-leak guard for
// every test in this package.
//
// The pattern is from go.uber.org/goleak: a single TestMain wraps the test
// runner, calls VerifyTestMain at exit, and fails the run if any goroutine
// remains beyond the ignore set. This catches lifecycle bugs in
// Pool / Attach / Drain / Stream without per-test boilerplate.
//
// Per-test goleak.VerifyNone(t) calls are deliberately NOT used — adding
// them risks masking real leaks behind ignore-list drift in individual
// tests. The TestMain pattern is the single source of leak truth.
package sse

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreCurrent(),
		// Add process-scoped ignores here, each on its own line with a
		// comment naming the goroutine and the reason it is exempt.
		// Expected to be empty under default conditions; if a future test
		// surfaces a legitimate process-scoped goroutine, add an explicit
		// goleak.IgnoreTopFunction(...) entry with a complete-sentence
		// reason naming why the goroutine outlives the test process.
	)
}
