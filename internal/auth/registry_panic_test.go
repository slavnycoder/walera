// Package auth — registry_panic_test.go covers the NewSubscribers
// construction gate: every required Deps field panics when nil.
package auth

import (
	"testing"

	"github.com/rs/zerolog"
)

func TestNewSubscribers_PanicsOnNilMetrics(t *testing.T) {
	t.Parallel()
	assertPanicsWithValue(t, "auth.NewSubscribers: Deps.Metrics is required", func() {
		_ = NewSubscribers(SubscribersDeps{
			Logger: zerolog.Nop(),
			// Metrics intentionally nil.
		})
	})
}
