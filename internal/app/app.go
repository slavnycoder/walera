package app

import (
	"context"
	"net/http"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/health"
	"github.com/walera/walera/internal/limits"
	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/sse"
	"github.com/walera/walera/internal/wal"
	"github.com/walera/walera/internal/walconn"
)

type App struct {
	Config *AppConfig

	Logger zerolog.Logger

	Metrics *metrics.Registry

	AdminConn walconn.AdminConn

	WalReader *wal.Reader

	TxCh <-chan wal.Tx

	AuthClient *auth.Client

	Breaker *auth.Breaker

	SubRegistry *auth.Subscribers

	Limits *limits.Limits

	Encoder *sse.Encoder

	RouterIndex *router.Broadcaster

	SSEPool *sse.WriterPool

	SSEHandler *sse.Handler

	HealthServer *health.Server

	HTTPServer *http.Server

	PProfServer *http.Server

	Runnables []Runnable

	cancel context.CancelFunc
}
