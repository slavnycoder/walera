package app

import "context"

// Runnable describes one long-running goroutine spawned by (*App).Run via
// safego. Name surfaces in panic-recovery logs; Run receives the App's
// root context and MUST return when ctx is cancelled; OnError (optional)
// is invoked only on Run errors with ctx.Err() == nil (the gate is
// enforced by (*App).Run's spawn wrapper in lifecycle.go).
type Runnable struct {
	Name    string
	Run     func(ctx context.Context) error
	OnError func(error)
}
