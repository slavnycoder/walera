package app

import "context"

type Runnable struct {
	Name    string
	Run     func(ctx context.Context) error
	OnError func(error)
}
