// Package auth — assertions.go: compile-time interface satisfaction proofs.
// Kept in a production .go file (not _test.go) so `go build ./...` is the
// enforcement gate.
package auth

// Compile-time assertion that *Client satisfies the Prober interface natively
// via its CheckAuth(ctx) error method.
var _ Prober = (*Client)(nil)

// Compile-time assertion that *Breaker satisfies the BreakerHook contract
// from client.go.
var _ BreakerHook = (*Breaker)(nil)
