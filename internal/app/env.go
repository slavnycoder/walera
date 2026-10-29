package app

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/config"
	"github.com/walera/walera/internal/health"
	"github.com/walera/walera/internal/limits"
	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/wal"
)

// envVars is every variable Walera reads. Any other WALERA_* variable fails
// startup, so a typo or a v3 name never silently falls back to a default.
var envVars = []string{
	"WALERA_DATABASE_URL", "WALERA_AUTH_URL", "WALERA_ADDR", "WALERA_PUBLICATION",
	"WALERA_TABLES", "WALERA_CORS_ORIGINS", "WALERA_AUTH_TTL",
	"WALERA_AUTH_FORWARD_COOKIES", "WALERA_AUTH_FORWARD_HEADERS",
	"WALERA_AUTH_ALLOW_PLAINTEXT", "WALERA_MAX_CONNECTIONS", "WALERA_LOG_LEVEL",
}

// LoadFromEnv builds the configuration from environ (os.Environ() format).
// Errors name variables, never their values.
func LoadFromEnv(environ []string) (*AppConfig, error) {
	env := map[string]string{}
	var unknown []string
	for _, kv := range environ {
		k, v, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(k, "WALERA_") {
			continue
		}
		if !slices.Contains(envVars, k) {
			unknown = append(unknown, k)
			continue
		}
		env[k] = v
	}
	var errs []error
	if len(unknown) > 0 {
		slices.Sort(unknown)
		errs = append(errs, fmt.Errorf("unknown environment variables: %s (see README for the v4 list)", strings.Join(unknown, ", ")))
	}

	cfg := &AppConfig{
		Log:      LogConfig{Level: "info"},
		HTTP:     defaultHTTP(),
		WAL:      wal.Defaults(),
		Auth:     auth.Defaults(),
		Limits:   limits.Defaults(),
		Router:   router.Defaults(),
		Health:   health.Defaults(),
		Shutdown: ShutdownConfig{Deadline: ShutdownDeadline(10 * time.Second)},
	}

	// DeriveDSNs reports a missing WALERA_DATABASE_URL itself.
	admin, repl, err := wal.DeriveDSNs(env["WALERA_DATABASE_URL"]) // redacts the DSN in its errors
	if err != nil {
		errs = append(errs, err)
	}
	cfg.WAL.PostgresDSN, cfg.WAL.ReplicationDSN = admin, repl
	cfg.Auth.BackendURL = env["WALERA_AUTH_URL"] // auth.Validate reports it missing
	if v := env["WALERA_ADDR"]; v != "" {
		cfg.HTTP.Addr = v
	}
	if v := env["WALERA_PUBLICATION"]; v != "" {
		cfg.WAL.PublicationName = wal.PublicationName(v)
	}
	cfg.WAL.Tables = splitList(env["WALERA_TABLES"])
	cfg.HTTP.CORSOrigins = splitList(env["WALERA_CORS_ORIGINS"])
	cfg.Auth.ForwardedCookies = splitList(env["WALERA_AUTH_FORWARD_COOKIES"])
	cfg.Auth.ForwardedHeaders = splitList(env["WALERA_AUTH_FORWARD_HEADERS"])

	if v := env["WALERA_AUTH_TTL"]; v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Second {
			errs = append(errs, config.FormatError("WALERA_AUTH_TTL", "", "must be a duration >= 1s", "e.g. 60s or 5m"))
		} else {
			cfg.Auth.DefaultTTLSeconds = int(d / time.Second)
		}
	}
	switch env["WALERA_AUTH_ALLOW_PLAINTEXT"] {
	case "", "0":
	case "1":
		cfg.Auth.AllowPlaintext = true
	default:
		errs = append(errs, config.FormatError("WALERA_AUTH_ALLOW_PLAINTEXT", "", "must be 0 or 1", ""))
	}
	if v := env["WALERA_MAX_CONNECTIONS"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			errs = append(errs, config.FormatError("WALERA_MAX_CONNECTIONS", "", "must be a positive integer", ""))
		} else {
			cfg.Limits.GlobalConcurrent = n
		}
	}
	if v := env["WALERA_LOG_LEVEL"]; v != "" {
		switch v {
		case "debug", "info", "warn", "error":
			cfg.Log.Level = v
		default:
			errs = append(errs, config.FormatError("WALERA_LOG_LEVEL", "", "must be debug, info, warn or error", ""))
		}
	}

	for _, v := range []interface{ Validate() error }{cfg.WAL, cfg.Auth, cfg.Limits, cfg.Router, cfg.Health} {
		if err := v.Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := validateHTTP(&cfg.HTTP); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("config: %w", errors.Join(errs...))
	}
	return cfg, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
