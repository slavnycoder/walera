package app

import (
	"errors"
	"fmt"
	"net"
	"net/url"
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

type AppConfig struct {
	Log      LogConfig
	WAL      wal.Config
	HTTP     HTTPConfig
	Router   router.Config
	Auth     auth.Config
	Limits   limits.Config
	Health   health.Config
	Shutdown ShutdownConfig
}

type LogConfig struct {
	Level string
}

type HTTPConfig struct {
	Addr string

	CORSOrigins []string

	MaxPayloadBytes int

	WriteTimeout time.Duration

	MaxHeaderBytes int

	H2CEnabled bool
}

type ShutdownConfig struct {
	Deadline ShutdownDeadline
}

// defaultHTTP holds the HTTP settings; only Addr and CORSOrigins are
// configurable.
func defaultHTTP() HTTPConfig {
	return HTTPConfig{
		Addr:            ":8080",
		MaxPayloadBytes: 10 << 20,
		WriteTimeout:    5 * time.Second,
		MaxHeaderBytes:  16 << 10,
		H2CEnabled:      true,
	}
}

func canonicalOrigin(s string) (string, bool) {
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", false
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host), true
}

// validateHTTP checks Addr and CORSOrigins and, when valid, rewrites
// CORSOrigins to canonical scheme://host form. Errors never echo values.
func validateHTTP(c *HTTPConfig) error {
	var errs []error
	if c.Addr == "" {
		errs = append(errs, errors.New("WALERA_ADDR is required"))
	} else if _, portStr, err := net.SplitHostPort(c.Addr); err != nil {
		errs = append(errs, config.FormatError("WALERA_ADDR", "", "is not a valid host:port", `use ":8080" or "host:8080"`))
	} else if port, err := strconv.ParseUint(portStr, 10, 16); err != nil || port == 0 {
		errs = append(errs, config.FormatError("WALERA_ADDR", "", "port must be in [1, 65535]", `use ":8080" or "host:8080"`))
	}
	canon := make([]string, 0, len(c.CORSOrigins))
	for i, raw := range c.CORSOrigins {
		o, ok := canonicalOrigin(raw)
		if !ok {
			errs = append(errs, config.FormatError(fmt.Sprintf("WALERA_CORS_ORIGINS entry %d", i+1), "", "is not a valid URL with scheme and host", "e.g. https://app.example"))
			continue
		}
		canon = append(canon, o)
	}
	if len(errs) > 0 {
		return fmt.Errorf("http config: validation failed: %w", errors.Join(errs...))
	}
	c.CORSOrigins = canon
	return nil
}
