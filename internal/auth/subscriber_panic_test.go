// Package auth — subscriber_panic_test.go covers the NewSubscriber
// construction gate: every required Deps field panics when nil.
package auth

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
)

func validSubscriberConfig() SubscriberConfig {
	return SubscriberConfig{
		InitialMap: &Whitelist{
			UserID:     "u1",
			Tables:     map[string]map[string]struct{}{"orders": {"id": struct{}{}}},
			TTLSeconds: 60,
		},
		Token:      "tok",
		Channel:    "public.orders:1",
		DefaultTTL: 60 * time.Second,
	}
}

func validSubscriberDeps(t *testing.T) SubscriberDeps {
	t.Helper()
	rsub := router.NewSubscriber(
		router.SubscriberConfig{
			Kind:      router.KindExact,
			Schema:    "public",
			Table:     "orders",
			PK:        "1",
			BufferCap: 4,
		},
		router.SubscriberDeps{Parent: context.Background()},
	)
	// Build a Client with a syntactically valid backend URL — no
	// network call is made on construction.
	c := New(Config{
		BackendURL:     "http://127.0.0.1:1",
		ServiceToken:   "svc",
		RequestTimeout: time.Second,
		HealthChannel:  "_health",
	}, Deps{Logger: zerolog.Nop(), Metrics: metrics.New()})
	return SubscriberDeps{
		Sub:     rsub,
		Client:  c,
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
	}
}

func TestNewSubscriber_PanicsOnNilDeps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(d *SubscriberDeps)
		wantMsg string
	}{
		{
			name:    "Sub",
			mutate:  func(d *SubscriberDeps) { d.Sub = nil },
			wantMsg: "auth.NewSubscriber: Deps.Sub is required",
		},
		{
			name:    "Client",
			mutate:  func(d *SubscriberDeps) { d.Client = nil },
			wantMsg: "auth.NewSubscriber: Deps.Client is required",
		},
		{
			name:    "Metrics",
			mutate:  func(d *SubscriberDeps) { d.Metrics = nil },
			wantMsg: "auth.NewSubscriber: Deps.Metrics is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deps := validSubscriberDeps(t)
			tc.mutate(&deps)
			assertPanicsWithValue(t, tc.wantMsg, func() {
				_ = NewSubscriber(validSubscriberConfig(), deps)
			})
		})
	}
}
