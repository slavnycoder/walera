package sse

import (
	"time"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/router"
)

type Config struct {
	Addr string `koanf:"addr"`

	CORSOrigins []string `koanf:"cors_origins"`

	HeartbeatInterval time.Duration `koanf:"heartbeat_interval"`

	MaxPayloadBytes int `koanf:"max_payload_bytes"`

	WriteTimeout time.Duration `koanf:"write_timeout"`

	Router router.Config

	Auth auth.Config

	MaxHeaderBytes int `koanf:"max_header_bytes"`
}
