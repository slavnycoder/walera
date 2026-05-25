// Package wal — leak_main_test.go owns the goleak goroutine-leak guard for
// every test in this package.
//
// Mirrors the pattern established in internal/sse/leak_main_test.go: a single
// TestMain wraps the test runner, calls VerifyTestMain at exit, and fails the
// run if any goroutine remains beyond the ignore set. This catches lifecycle
// bugs in reader.runOnce, the standby ticker, the lag sampler, and the
// reconnect/backoff loop without per-test boilerplate.
//
// Per-test goleak.VerifyNone(t) calls are deliberately NOT used — adding them
// risks masking real leaks behind ignore-list drift in individual tests. The
// TestMain pattern is the single source of leak truth.
//
// Every goleak.IgnoreTopFunction entry MUST carry a comment naming the
// goroutine and the reason it is exempt. The ignore list is expected to be
// empty under default conditions; if a run reports a leak, decide whether it
// is a real bug (fix when it is a one-line obvious correction, otherwise
// route via a follow-up) or a legitimate process-scoped goroutine (add an
// ignore entry with reason).
package wal

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
