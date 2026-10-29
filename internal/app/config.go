package app

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"reflect"
	"slices"
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

func stringToNamedDurationHook() mapstructure.DecodeHookFuncType {
	shutdownT := reflect.TypeOf(ShutdownDeadline(0))
	return func(f, t reflect.Type, data any) (any, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t != shutdownT {
			return data, nil
		}

		s, ok := data.(string)
		if !ok {
			return data, nil
		}
		d, err := time.ParseDuration(s)
		if err != nil {
			return data, err
		}
		return ShutdownDeadline(d), nil
	}
}

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

type LogConfig struct {
	Level string `koanf:"level"`

	DevMode bool `koanf:"dev_mode"`
}

type HTTPConfig struct {
	Addr string `koanf:"addr"`

	CORSOrigins []string `koanf:"cors_origins"`

	MaxPayloadBytes int `koanf:"max_payload_bytes"`

	WriteTimeout time.Duration `koanf:"write_timeout"`

	MaxHeaderBytes int `koanf:"max_header_bytes"`

	H2CEnabled bool `koanf:"h2c_enabled"`

	PProfAddr string `koanf:"pprof_addr"`
}

type ShutdownConfig struct {
	Deadline ShutdownDeadline `koanf:"deadline"`
}

func canonicalOrigin(s string) (string, bool) {
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", false
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host), true
}

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
	_ = k.Set("shutdown.deadline", "10s")

	wal.ApplyDefaults(k)
	auth.ApplyDefaults(k)
	limits.ApplyDefaults(k)
	router.ApplyDefaults(k)
	health.ApplyDefaults(k)
	metrics.ApplyDefaults(k)
}

// removedConfigKeys are keys from earlier releases that no longer do anything.
// Setting one fails startup so nobody believes a removed control (HMAC
// signing, the circuit breaker, the writer pool) is still in effect. Prefix
// match covers both the YAML (auth.signing.secret) and the env
// (WALERA_AUTH_SIGNING_SECRET -> auth.signing_secret) spellings.
var removedConfigKeys = []string{
	"auth.signing",
	"auth.breaker",
	"auth.health_channel",
	"shutdown.drain_deadline",
	"http.pool_factor",
	"http.sub_queue_size",
	"http.max_wait_ms",
	"http.drain_threshold_subs",
	"http.max_batch_bytes_per_sub",
	"http.batching_disabled",
}

// rejectRemovedKeys names offending keys only; values may be secrets.
func rejectRemovedKeys(k *koanf.Koanf) error {
	var found []string
	for _, key := range k.Keys() {
		for _, p := range removedConfigKeys {
			if key == p || strings.HasPrefix(key, p+".") || strings.HasPrefix(key, p+"_") {
				found = append(found, key)
				break
			}
		}
	}
	if len(found) == 0 {
		return nil
	}
	slices.Sort(found)
	return fmt.Errorf("removed config keys are set: %s (YAML key or its WALERA_* env form) — delete them (see CHANGELOG.md)", strings.Join(found, ", "))
}

func LoadAppConfig(path string) (*AppConfig, error) {
	k, err := config.LoadKoanf(path, applyAppDefaults)
	if err != nil {
		return nil, err
	}
	if err := rejectRemovedKeys(k); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	cfg := &AppConfig{}

	if err := k.UnmarshalWithConf("log", &cfg.Log, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return nil, fmt.Errorf("log config: unmarshal: %w", err)
	}
	if err := k.UnmarshalWithConf("http", &cfg.HTTP, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return nil, fmt.Errorf("http config: unmarshal: %w", err)
	}

	if err := k.UnmarshalWithConf("shutdown", &cfg.Shutdown, koanf.UnmarshalConf{
		Tag:           "koanf",
		DecoderConfig: shutdownDecoderConfig(),
	}); err != nil {
		return nil, fmt.Errorf("shutdown config: unmarshal: %w", err)
	}

	var errs []error
	walCfg, err := wal.LoadConfig(k)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.WAL = walCfg

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

	canon := make([]string, 0, len(cfg.HTTP.CORSOrigins))
	for _, raw := range cfg.HTTP.CORSOrigins {
		if c, ok := canonicalOrigin(raw); ok {
			canon = append(canon, c)
		}
	}
	cfg.HTTP.CORSOrigins = canon

	return cfg, nil
}

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

func validateShutdown(c *ShutdownConfig) error {
	var errs []error
	if c.Deadline <= 0 {
		errs = append(errs, errors.New("shutdown.deadline must be > 0"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("shutdown config: validation failed: %w", errors.Join(errs...))
	}
	return nil
}
