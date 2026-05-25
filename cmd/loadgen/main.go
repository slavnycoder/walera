// Command loadgen drives concurrent SSE subscribers against a running
// Walera instance for capacity / soak testing. Each subscriber holds one
// long-lived GET /sse/v1/<channel> connection with bearer auth, records
// per-frame counters to a Prometheus registry, and reconnects with
// full-jitter exponential backoff.
//
// Security: the auth token is NEVER written to logs — only its length
// (auth_token_len). Verified by TestSubscriber_DoesNotLogToken.
//
// Usage:
//
//	./loadgen \
//	  --target-url http://127.0.0.1:8080 \
//	  --concurrency 1000 \
//	  --channels orders/all,users/all \
//	  --duration 5m \
//	  --ramp-up 30s \
//	  --http-addr 127.0.0.1:9200 \
//	  --log-level info
//
// The --auth-token flag is honoured but LOADGEN_AUTH_TOKEN env var is
// preferred for CI / scripted runs (no token in shell history / ps output).
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"go.uber.org/automaxprocs/maxprocs"

	"github.com/walera/walera/internal/safego"
)

// loadgenFlags bundles the flag.* pointers populated by registerFlags.
type loadgenFlags struct {
	targetURL   *string
	concurrency *int
	channels    *string
	authToken   *string
	duration    *time.Duration
	rampUp      *time.Duration
	httpAddr    *string
	logLevel    *string
}

func registerFlags() *loadgenFlags {
	f := &loadgenFlags{
		targetURL:   flag.String("target-url", "http://127.0.0.1:8080", "Walera base URL (no trailing /sse path)"),
		concurrency: flag.Int("concurrency", 100, "number of concurrent SSE subscribers to open"),
		channels:    flag.String("channels", "", "comma-separated channels (e.g. orders/all,users/42) OR @path/to/file with one channel per line"),
		authToken:   flag.String("auth-token", "", "bearer credential (prefer the LOADGEN_AUTH_TOKEN env var; never logged)"),
		duration:    flag.Duration("duration", 0, "total run duration; 0 = run until SIGINT"),
		rampUp:      flag.Duration("ramp-up", 10*time.Second, "linear ramp-up window during which the N subscribers are spawned"),
		httpAddr:    flag.String("http-addr", "127.0.0.1:9200", "loadgen's own /metrics + /healthz HTTP listener"),
		logLevel:    flag.String("log-level", "info", "log level: debug|info|warn|error"),
	}
	flag.Parse()
	return f
}

func main() {
	f := registerFlags()

	logger := newLogger(*f.logLevel)
	setMaxprocs(logger)

	token := resolveToken(*f.authToken, logger)
	logger.Info().Int("auth_token_len", len(token)).Msg("auth token loaded (length only — never logged literally)")

	chans, err := loadChannels(*f.channels)
	if err != nil {
		logger.Fatal().Err(err).Msg("--channels: failed to load")
	}
	if len(chans) == 0 {
		logger.Fatal().Msg("--channels: at least one channel is required")
	}

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	runCtx, cancelRun := boundedRunContext(rootCtx, *f.duration)
	defer cancelRun()

	srv := startHTTPServer(*f.httpAddr, reg, logger, stop)
	httpClient := &http.Client{Timeout: 0} // no client-side timeout for long-lived SSE

	spawnSubscribers(runCtx, *f.concurrency, *f.rampUp, *f.targetURL, token, chans, httpClient, m, logger)

	<-runCtx.Done()
	logger.Info().Msg("run context cancelled; shutting down")
	gracefulShutdown(srv, logger)
}

func newLogger(level string) zerolog.Logger {
	logger := zerolog.New(os.Stderr).With().Timestamp().Caller().Logger()
	if lvl, err := zerolog.ParseLevel(level); err == nil {
		logger = logger.Level(lvl)
	}
	return logger
}

func setMaxprocs(logger zerolog.Logger) {
	_, err := maxprocs.Set(maxprocs.Logger(func(format string, args ...interface{}) {
		logger.Info().Msgf("maxprocs: "+format, args...)
	}))
	if err != nil {
		logger.Warn().Err(err).Msg("automaxprocs.Set failed")
	}
}

// resolveToken returns the bearer credential: --auth-token flag wins, env
// var falls back. Fatals when neither is set (token leakage avoided —
// never logged).
func resolveToken(flagToken string, logger zerolog.Logger) string {
	token := flagToken
	if token == "" {
		token = os.Getenv("LOADGEN_AUTH_TOKEN")
	}
	if token == "" {
		logger.Fatal().Msg("--auth-token (or LOADGEN_AUTH_TOKEN) is required")
	}
	return token
}

// boundedRunContext wraps the parent context with a --duration timeout
// when positive. Returns the context the subscribers observe plus a cancel
// func the caller must defer. When duration <= 0 the cancel is a no-op
// because the parent context owns lifetime.
func boundedRunContext(parent context.Context, duration time.Duration) (context.Context, context.CancelFunc) {
	if duration <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, duration)
}

// startHTTPServer launches the tiny /metrics + /healthz listener in a
// background goroutine and returns the *http.Server so main can Shutdown
// it after the run completes.
func startHTTPServer(addr string, reg *prometheus.Registry, logger zerolog.Logger, stop context.CancelFunc) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	safego.Go("loadgen-http", func() {
		logger.Info().Str("addr", addr).Msg("loadgen HTTP server listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("loadgen HTTP server exited with error")
			stop()
		}
	})
	return srv
}

// spawnSubscribers walks the linear ramp-up, launching one subscriber per
// slot. When rampUp <= 0 all subscribers spin up immediately. Each call
// reuses one *http.Client across subscribers.
func spawnSubscribers(
	ctx context.Context,
	concurrency int,
	rampUp time.Duration,
	targetURL, token string,
	chans []string,
	client *http.Client,
	m *metrics,
	logger zerolog.Logger,
) {
	spawnInterval := time.Duration(0)
	if concurrency > 0 && rampUp > 0 {
		spawnInterval = rampUp / time.Duration(concurrency)
	}
	logger.Info().
		Int("concurrency", concurrency).
		Int("channel_count", len(chans)).
		Dur("ramp_up", rampUp).
		Dur("spawn_interval", spawnInterval).
		Msg("starting subscribers")

	for i := 0; i < concurrency; i++ {
		if ctx.Err() != nil {
			return
		}
		ch := chans[i%len(chans)]
		sub := &Subscriber{
			URL:     targetURL,
			Channel: ch,
			Token:   token,
			Client:  client,
			M:       m,
			Log:     logger.With().Int("sub_id", i).Str("channel", ch).Logger(),
			Backoff: backoffCfg{Initial: 100 * time.Millisecond, Max: 30 * time.Second},
		}
		safego.Go(fmt.Sprintf("subscriber-%d", i), func() { sub.Run(ctx) })
		if spawnInterval > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(spawnInterval):
			}
		}
	}
}

func gracefulShutdown(srv *http.Server, logger zerolog.Logger) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn().Err(err).Msg("loadgen HTTP shutdown returned error")
	}
	logger.Info().Msg("loadgen exited")
}

// loadChannels resolves the --channels flag into a []string. Supported
// forms:
//
//   - "orders/42,users/all"   — comma-separated literal list
//   - "@/path/to/file"        — one channel per line ('#' comments + blanks stripped)
//
// Empty input returns an empty slice; the caller decides whether that's an error.
func loadChannels(s string) ([]string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	if strings.HasPrefix(s, "@") {
		return loadChannelsFromFile(strings.TrimPrefix(s, "@"))
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out, nil
}

func loadChannelsFromFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	return out, nil
}
