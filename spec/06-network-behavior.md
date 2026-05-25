# 6. Network behavior

## 6.1. Long-lived connections

SSE connections live indefinitely; there is no hard max-lifetime. Reliability mechanisms:
- TCP keepalive (kernel level).
- Application heartbeat (defeats proxy idle timeouts).
- EventSource auto-reconnect on the client side.

Monitor `subscriber_lifetime_seconds` (histogram) to catch anomalies.

## 6.2. Heartbeat

Every 15 seconds, the writer goroutine writes `":\n\n"` to the response and calls `Flush`. This:
- Is invisible to the EventSource client API (SSE spec: lines starting with `:` are comments).
- Defeats proxy idle timeouts (nginx, ALB default ~60s).
- Surfaces broken connections — `Write` returns an error when the client is gone.

Also enable TCP keepalive on the underlying connection:
```go
tcpConn.SetKeepAlive(true)
tcpConn.SetKeepAlivePeriod(30 * time.Second)
```

## 6.3. CORS

Required headers on SSE responses:
```
Access-Control-Allow-Origin: <from config; list of allowed origins>
Access-Control-Allow-Credentials: true   (if cookie auth is supported)
Access-Control-Allow-Headers: Authorization, X-Request-ID
```

Handle preflight `OPTIONS` requests explicitly for `/sse/*`.

## 6.4. HTTP/2

Enable HTTP/2 in `net/http` (effectively free for TLS-enabled servers). Lets clients multiplex many SSE streams over one TCP connection, dodging the HTTP/1.1 6-per-host limit.

## 6.5. Limits

| Limit | Default | Purpose |
|---|---|---|
| Global concurrent connections | 50,000 | OOM / EMFILE protection |
| Per-user concurrent connections | 10 | Anti-abuse |
| Per-user open rate | 5 rps, burst 10 | Anti reconnect-spam |
| Buffer size, exact sub | 64 events | Backpressure |
| Buffer size, wildcard sub | 512 events | Wildcard load tolerance |
| Max changes / tx (exact) | 1,000 | Anti monster-tx |
| Max changes / tx (wildcard) | 10,000 | Wildcard tolerance |
| Max payload bytes / event | 10 MB | Anti toxic payload |

All values in YAML config, overridable via env. Pre-auth checks run before auth call (cheap), per-user checks after (need `user_id`).

Exceeding global → 503 + `Retry-After`. Exceeding per-user → 429. Exceeding tx size → disconnect with `event: error`.
