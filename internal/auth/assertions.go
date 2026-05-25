package auth

var _ Prober = (*Client)(nil)

var _ BreakerHook = (*Breaker)(nil)
