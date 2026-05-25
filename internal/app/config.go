// Package app — config.go owns the aggregate AppConfig assembled by
// LoadAppConfig. See internal/app/doc.go for the package narrative.
package app

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/config"
	"github.com/walera/walera/internal/health"
	"github.com/walera/walera/internal/limits"
	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/wal"
)

// stringToNamedDurationHook converts a string ("10s" / "8s") into
// ShutdownDeadline / DrainDeadline. koanf's default
// StringToTimeDurationHookFunc only matches the bare time.Duration type;
// this hook closes the gap for the named-duration wrappers.
func stringToNamedDurationHook() mapstructure.DecodeHookFuncType {
	shutdownT := reflect.TypeOf(ShutdownDeadline(0))
	drainT := reflect.TypeOf(DrainDeadline(0))
	return func(f, t reflect.Type, data any) (any, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t != shutdownT && t != drainT {
			return data, nil
		}
		// Belt-and-suspenders for hook-chain reordering; see IN-02.
		s, ok := data.(string)
		if !ok {
			return data, nil
		}
		d, err := time.ParseDuration(s)
		if err != nil {
			return data, err
		}
		if t == shutdownT {
			return ShutdownDeadline(d), nil
		}
		return DrainDeadline(d), nil
	}
}

// shutdownDecoderConfig returns a mapstructure.DecoderConfig with the
// named-duration hook for ShutdownDeadline / DrainDeadline plus the
// stdlib StringToTimeDuration hook. Used by LoadAppConfig's shutdown
// sub-unmarshal. The patched koanf textUnmarshalerHookFunc is
// package-private and the upstream version diverges; ShutdownConfig has
// no TextUnmarshaler fields so neither helper is needed today (see IN-01).
func shutdownDecoderConfig() *mapstructure.DecoderConfig {
	return &mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			stringToNamedDurationHook(),
			mapstructure.StringToTimeDurationHookFunc(),
		),
		Metadata:         nil,
		WeaklyTypedInput: true,
	}
}

// AppConfig is the aggregate root configuration for the cdc-sse binary,
// composing the per-package typed Config structs plus app-local
// Log/HTTP/Shutdown sub-configs.
type AppConfig struct {
	Log      LogConfig
	WAL      wal.Config
	HTTP     HTTPConfig
	Router   router.Config
	Auth     auth.Config
	Limits   limits.Config
	Health   health.Config
	Metrics  metrics.Config
	Shutdown ShutdownConfig
}

// LogConfig controls the structured logger (zerolog).
type LogConfig struct {
	// Level: debug | info | warn | error. Default: info.
	Level string `koanf:"level"`
	// DevMode enables the human-readable console writer. Disable in production.
	DevMode bool `koanf:"dev_mode"`
}

// HTTPConfig holds the HTTP server / SSE listener configuration.
// Field names map to koanf keys under the "http." prefix.
type HTTPConfig struct {
	// Addr is the TCP listen address. Default ":8080".
	Addr string `koanf:"addr"`

	// CORSOrigins is the allowlist of Origin values for which the SSE
	// handler reflects Access-Control-Allow-Origin + ACAC. Canonicalised
	// once at load time.
	CORSOrigins []string `koanf:"cors_origins"`

	// MaxPayloadBytes is the maximum serialized SSE payload size per event.
	// Default 10 MiB.
	MaxPayloadBytes int `koanf:"max_payload_bytes"`

	// WriteTimeout is the per-frame SSE write deadline. Default 5s.
	WriteTimeout time.Duration `koanf:"write_timeout"`

	// MaxHeaderBytes caps the request-header byte size. Default 16 KiB.
	MaxHeaderBytes int `koanf:"max_header_bytes"`

	// H2CEnabled toggles unencrypted HTTP/2 on the SSE listener. Default true.
	H2CEnabled bool `koanf:"h2c_enabled"`

	// PProfAddr is the bind address for the OPT-IN pprof listener.
	// Default "" (disabled); must bind loopback unless
	// WALERA_PPROF_ALLOW_PUBLIC=1.
	PProfAddr string `koanf:"pprof_addr"`

	// PoolFactor multiplies runtime.GOMAXPROCS(0). Default 2, must be >= 1.
	PoolFactor int `koanf:"pool_factor"`

	// SubQueueSize is the per-subscriber channel capacity. Default 32, >= 1.
	SubQueueSize int `koanf:"sub_queue_size"`

	// MaxWaitMs is the SSE-pool worker timer-armed batch-drain ceiling.
	// Default 2 ms, >= 0.
	MaxWaitMs int `koanf:"max_wait_ms"`

	// DrainThresholdSubs forces an immediate batch drain at this dirty-sub
	// count. Default 0 (built-in formula), >= 0.
	DrainThresholdSubs int `koanf:"drain_threshold_subs"`

	// MaxBatchBytesPerSub is the per-sub buffered-frame byte cap before
	// force-flush. Default 64 KiB, >= 1.
	MaxBatchBytesPerSub int `koanf:"max_batch_bytes_per_sub"`

	// BatchingDisabled forces drain every cycle. Default false.
	BatchingDisabled bool `koanf:"batching_disabled"`
}

// ShutdownConfig holds graceful-shutdown deadlines.
// Deadline (default 10s) is the hard cap; DrainDeadline (default 8s) is
// the inner broadcaster fan-out deadline and must be <= Deadline.
type ShutdownConfig struct {
	Deadline      ShutdownDeadline `koanf:"deadline"`
	DrainDeadline DrainDeadline    `koanf:"drain_deadline"`
}

// canonicalOrigin normalises a CORS allowlist entry to "scheme://host"
// (lower-cased). Returns ("", false) on parse failure or missing scheme/host.
func canonicalOrigin(s string) (string, bool) {
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", false
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host), true
}

// applyAppDefaults registers every koanf default: app-local Log/HTTP/Shutdown
// plus each internal/<pkg>.ApplyDefaults.
func applyAppDefaults(k *koanf.Koanf) {
	_ = k.Set("log.level", "info")
	_ = k.Set("log.dev_mode", false)
	_ = k.Set("http.addr", ":8080")
	_ = k.Set("http.cors_origins", []string{})
	_ = k.Set("http.max_payload_bytes", 10*1024*1024)
	_ = k.Set("http.write_timeout", "5s")
	_ = k.Set("http.max_header_bytes", 16*1024)
	_ = k.Set("http.h2c_enabled", true)
	_ = k.Set("http.pprof_addr", "")
	_ = k.Set("http.pool_factor", 2)
	_ = k.Set("http.sub_queue_size", 32)
	_ = k.Set("http.max_wait_ms", 2)
	_ = k.Set("http.drain_threshold_subs", 0)
	_ = k.Set("http.max_batch_bytes_per_sub", 65536)
	_ = k.Set("http.batching_disabled", false)
	_ = k.Set("shutdown.deadline", "10s")
	_ = k.Set("shutdown.drain_deadline", "8s")

	wal.ApplyDefaults(k)
	auth.ApplyDefaults(k)
	limits.ApplyDefaults(k)
	router.ApplyDefaults(k)
	health.ApplyDefaults(k)
	metrics.ApplyDefaults(k)
}

// LoadAppConfig reads the YAML at path, overlays WALERA_-prefixed env
// vars, dispatches to each internal/<pkg>.LoadConfig, and returns the
// aggregate AppConfig. Returns a non-nil error joining every validation
// failure. If path is empty / missing, env + defaults only.
func LoadAppConfig(path string) (*AppConfig, error) {
	k, err := config.LoadKoanf(path, applyAppDefaults)
	if err != nil {
		return nil, err
	}

	cfg := &AppConfig{}

	if err := k.UnmarshalWithConf("log", &cfg.Log, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return nil, fmt.Errorf("log config: unmarshal: %w", err)
	}
	if err := k.UnmarshalWithConf("http", &cfg.HTTP, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return nil, fmt.Errorf("http config: unmarshal: %w", err)
	}
	// shutdown uses the named-duration decoder (see shutdownDecoderConfig).
	if err := k.UnmarshalWithConf("shutdown", &cfg.Shutdown, koanf.UnmarshalConf{
		Tag:           "koanf",
		DecoderConfig: shutdownDecoderConfig(),
	}); err != nil {
		return nil, fmt.Errorf("shutdown config: unmarshal: %w", err)
	}

	// Collect per-package errors so misconfigs surface in one startup message.
	var errs []error
	walCfg, err := wal.LoadConfig(k)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.WAL = walCfg

	// The admin and replication DSNs both derive from the single top-level
	// database.url (env WALERA_DATABASE_URL). DeriveDSNs validates the base
	// and produces the replication variant by adding replication=database.
	adminDSN, replDSN, dsnErr := wal.DeriveDSNs(k.String("database.url"))
	if dsnErr != nil {
		errs = append(errs, dsnErr)
	}
	cfg.WAL.PostgresDSN = adminDSN
	cfg.WAL.ReplicationDSN = replDSN

	authCfg, err := auth.LoadConfig(k)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.Auth = authCfg

	limitsCfg, err := limits.LoadConfig(k)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.Limits = limitsCfg

	routerCfg, err := router.LoadConfig(k)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.Router = routerCfg

	healthCfg, err := health.LoadConfig(k)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.Health = healthCfg

	metricsCfg, err := metrics.LoadConfig(k)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.Metrics = metricsCfg

	if vErr := validateHTTP(&cfg.HTTP); vErr != nil {
		errs = append(errs, vErr)
	}
	if vErr := validateShutdown(&cfg.Shutdown); vErr != nil {
		errs = append(errs, vErr)
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("config: validation failed: %w", errors.Join(errs...))
	}

	// SEC-09 — canonicalise cors_origins once at load (validateHTTP ran first).
	canon := make([]string, 0, len(cfg.HTTP.CORSOrigins))
	for _, raw := range cfg.HTTP.CORSOrigins {
		if c, ok := canonicalOrigin(raw); ok {
			canon = append(canon, c)
		}
	}
	cfg.HTTP.CORSOrigins = canon

	return cfg, nil
}

// validateHTTP enforces the app-local HTTP invariants.
func validateHTTP(c *HTTPConfig) error {
	var errs []error
	if c.Addr == "" {
		errs = append(errs, errors.New("http.addr is required"))
	} else {
		_, portStr, splitErr := net.SplitHostPort(c.Addr)
		if splitErr != nil {
			errs = append(errs, config.FormatError(
				"http.addr",
				c.Addr,
				"is not a valid host:port: "+splitErr.Error(),
				`use ":8080" or "host:8080"`,
			))
		} else {
			port, perr := strconv.ParseUint(portStr, 10, 16)
			switch {
			case perr != nil:
				errs = append(errs, config.FormatError(
					"http.addr",
					c.Addr,
					"port is not a valid uint16: "+perr.Error(),
					"choose a port in [1, 65535]",
				))
			case port == 0:
				errs = append(errs, config.FormatError(
					"http.addr",
					c.Addr,
					"port must be in [1, 65535]",
					"choose a positive port number",
				))
			}
		}
	}
	if c.PProfAddr != "" {
		host, _, err := net.SplitHostPort(c.PProfAddr)
		if err != nil {
			errs = append(errs, fmt.Errorf("http.pprof_addr %q is not a valid host:port: %w", c.PProfAddr, err))
		} else {
			hl := strings.ToLower(host)
			isLoopback := hl == "127.0.0.1" || hl == "::1" || hl == "localhost"
			if !isLoopback && os.Getenv("WALERA_PPROF_ALLOW_PUBLIC") != "1" {
				errs = append(errs, fmt.Errorf("http.pprof_addr %q binds a non-loopback host; set WALERA_PPROF_ALLOW_PUBLIC=1 to override", c.PProfAddr))
			}
		}
	}
	if c.PoolFactor < 1 {
		errs = append(errs, fmt.Errorf("http.pool_factor must be >= 1 (got %d)", c.PoolFactor))
	}
	if c.SubQueueSize < 1 {
		errs = append(errs, fmt.Errorf("http.sub_queue_size must be >= 1 (got %d)", c.SubQueueSize))
	}
	if c.MaxWaitMs < 0 {
		errs = append(errs, fmt.Errorf("http.max_wait_ms must be >= 0 (got %d)", c.MaxWaitMs))
	}
	if c.DrainThresholdSubs < 0 {
		errs = append(errs, fmt.Errorf("http.drain_threshold_subs must be >= 0 (got %d)", c.DrainThresholdSubs))
	}
	if c.MaxBatchBytesPerSub < 1 {
		errs = append(errs, fmt.Errorf("http.max_batch_bytes_per_sub must be >= 1 (got %d)", c.MaxBatchBytesPerSub))
	}
	// SEC-09 — every cors_origins entry must parse with scheme + host.
	for i, raw := range c.CORSOrigins {
		if _, ok := canonicalOrigin(raw); !ok {
			errs = append(errs, fmt.Errorf("http.cors_origins[%d] (%q) is not a valid URL with scheme and host", i, raw))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("http config: validation failed: %w", errors.Join(errs...))
	}
	return nil
}

// validateShutdown enforces the app-local shutdown invariants.
func validateShutdown(c *ShutdownConfig) error {
	var errs []error
	if c.Deadline <= 0 {
		errs = append(errs, errors.New("shutdown.deadline must be > 0"))
	}
	if c.DrainDeadline <= 0 {
		errs = append(errs, errors.New("shutdown.drain_deadline must be > 0"))
	}
	if c.DrainDeadline.Duration() > c.Deadline.Duration() {
		errs = append(errs, errors.New("shutdown.drain_deadline must be <= shutdown.deadline"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("shutdown config: validation failed: %w", errors.Join(errs...))
	}
	return nil
}
