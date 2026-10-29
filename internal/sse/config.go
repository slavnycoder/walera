package sse

import (
	"time"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/router"
)

type Config struct {
	Addr string

	CORSOrigins []string

	HeartbeatInterval time.Duration

	MaxPayloadBytes int

	WriteTimeout time.Duration

	Router router.Config

	Auth auth.Config

	MaxHeaderBytes int
}
