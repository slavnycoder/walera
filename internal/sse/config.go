// Package sse — Config sub-struct loaded under the "http." key in koanf.
package sse

import (
	"time"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/router"
)

// Config holds SSE/HTTP-server tuning knobs. Field names map to koanf
// keys under "http.". Defaults are registered in
// internal/config/config.go's applyDefaults.
type Config struct {
	// Addr is the TCP listen address. Default ":8080".
	Addr string `koanf:"addr"`

	// CORSOrigins is the canonical-form Origin allowlist (lowercase
	// scheme + "://" + lowercase host; port preserved). Pre-canonicalised
	// by internal/config.Load — direct callers MUST pre-canonicalise.
	// Default nil (CORS reflection disabled).
	CORSOrigins []string `koanf:"cors_origins"`

	// HeartbeatInterval is the SSE keep-alive cadence. Default 15s.
	HeartbeatInterval time.Duration `koanf:"heartbeat_interval"`

	// MaxPayloadBytes caps serialized SSE data payload size per event;
	// overflow → slow_consumer drop. Default 10 MiB.
	MaxPayloadBytes int `koanf:"max_payload_bytes"`

	// WriteTimeout is the per-frame SetWriteDeadline budget. Default 5s.
	WriteTimeout time.Duration `koanf:"write_timeout"`

	// Router holds router.Config consumed when constructing each
	// router.Subscriber.
	Router router.Config

	// Auth holds auth.Config consumed by the per-subscriber refresh loop.
	Auth auth.Config

	// MaxHeaderBytes caps request-header byte size accepted by the
	// production *http.Server (enforced by stdlib BEFORE the handler
	// runs; oversized → 431). Default 16 KiB.
	MaxHeaderBytes int `koanf:"max_header_bytes"`
}
