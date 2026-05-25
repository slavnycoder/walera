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
		})
	})
}
