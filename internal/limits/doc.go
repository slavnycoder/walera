// Package limits implements admission-control gates for the SSE handshake.
//
// Construction: a single *Limits is created in cmd/cdc-sse/main.go and
// threaded into the SSE handler. Two gates fire BEFORE the auth backend
// call (global concurrency + per-IP rate); two gates fire AFTER the auth
// backend call (per-user concurrency + per-user rate).
//
//   - Global semaphore: a buffered chan struct{} with capacity
//     cfg.GlobalConcurrent (default 50000). Non-blocking acquire; rejected
//     dispatches increment walera_limit_rejected_total{kind="global_concurrent"}.
//   - Per-user concurrent counter: a sync.Map[string]*atomic.Int64 keyed
//     by userID. Acquire increments then re-checks against
//     cfg.PerUserConcurrentMax; on overflow it decrements and rejects.
//   - Per-IP token bucket (pre-auth): a sync.Map of *rate.Limiter
//     (golang.org/x/time/rate) with rate=cfg.PreAuthRatePerSecond,
//     burst=cfg.PreAuthBurst.
//   - Per-user token bucket (post-auth): same shape, keyed by userID,
//     with cfg.PerUserRatePerSecond + cfg.PerUserBurst.
//
// Sweeper: RunSweeper(ctx) is a long-lived goroutine spawned via safego.Go
// from cmd/cdc-sse/main.go. Every cfg.SweepInterval it walks the two rate
// maps and deletes entries whose lastSeen is older than
// cfg.SweepIdleThreshold (default 5m), preventing unbounded growth from
// rare or single-shot IPs/users.
package limits
