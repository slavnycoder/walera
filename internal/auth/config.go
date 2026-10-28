package auth

import (
	"errors"
	"fmt"
	"net/textproto"
	"net/url"
	"os"
	"time"

	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/config"
)

// isFieldNameToken reports whether s is a non-empty RFC 6265 cookie-name /
// HTTP field-name token: letters, digits, and the punctuation
// ! # $ % & ' * + - . ^ _ | ~ and backtick.
func isFieldNameToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z':
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '!' || c == '#' || c == '$' || c == '%' || c == '&' ||
			c == '\'' || c == '*' || c == '+' || c == '-' || c == '.' ||
			c == '^' || c == '_' || c == '|' || c == '~' || c == '`':
		default:
			return false
		}
	}
	return true
}

type Config struct {
	BackendURL string `koanf:"backend_url"`

	DefaultTTLSeconds int `koanf:"default_ttl_seconds"`

	RequestTimeout time.Duration `koanf:"request_timeout"`

	// ForwardedCookies / ForwardedHeaders are allowlists of client-supplied
	// credentials threaded from the SSE handshake into the OpenSession backend
	// call. Empty (nil) means the feature is off for that kind — no defaults.
	ForwardedCookies []string `koanf:"forwarded_cookies"`

	ForwardedHeaders []string `koanf:"forwarded_headers"`
}

// DocsURL points operators at the auth-feature documentation. Included in
// log lines surfaced from the periodic-refresh loop so an on-call engineer
// can jump straight to the contract.
const DocsURL = "https://github.com/walera/walera/blob/master/docs/auth.md"

func ApplyDefaults(k *koanf.Koanf) {
	_ = k.Set("auth.default_ttl_seconds", 60)
	_ = k.Set("auth.request_timeout", "2s")
}

func LoadConfig(k *koanf.Koanf) (Config, error) {
	var cfg Config
	if err := k.UnmarshalWithConf("auth", &cfg, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return Config{}, fmt.Errorf("auth config: unmarshal: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("auth config: %w", err)
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var errs []error
	if c.BackendURL == "" {
		errs = append(errs, errors.New("auth.backend_url is required"))
	} else {
		u, err := url.Parse(c.BackendURL)
		switch {
		case err != nil:
			errs = append(errs, config.FormatError(
				"auth.backend_url",
				c.BackendURL,
				"URL parse failed: "+err.Error(),
				"provide an absolute https:// URL",
			))
		case u.Scheme != "http" && u.Scheme != "https":
			errs = append(errs, config.FormatError(
				"auth.backend_url",
				c.BackendURL,
				"scheme must be http or https",
				"use https://<host>/<base-path>",
			))
		case u.Host == "":
			errs = append(errs, config.FormatError(
				"auth.backend_url",
				c.BackendURL,
				"host is empty",
				"include host[:port] in the URL",
			))
		default:

			if u.Scheme != "https" && os.Getenv("WALERA_AUTH_ALLOW_PLAINTEXT") != "1" {
				errs = append(errs, errors.New("auth.backend_url must use https:// (override with WALERA_AUTH_ALLOW_PLAINTEXT=1 for dev)"))
			}
		}
	}
	if c.RequestTimeout <= 0 {
		errs = append(errs, errors.New("auth.request_timeout must be > 0"))
	}
	if c.DefaultTTLSeconds < 1 {
		errs = append(errs, config.FormatError(
			"auth.default_ttl_seconds",
			fmt.Sprintf("%d", c.DefaultTTLSeconds),
			"must be > 0: live streams must re-check permissions so revocations propagate",
			"omit the field (default 60) or use a positive integer",
		))
	}

	// Forwarded-credential allowlists. Cookie names follow RFC 6265 tokens;
	// header names use the same token class. Reserved headers are managed by
	// Walera and can never be forwarded.
	for _, name := range c.ForwardedCookies {
		if !isFieldNameToken(name) {
			errs = append(errs, config.FormatError(
				"auth.forwarded_cookies",
				name,
				"is not a valid RFC 6265 cookie name token",
				"use only letters, digits, and ! # $ % & ' * + - . ^ _ | ~ `",
			))
		}
	}
	for _, name := range c.ForwardedHeaders {
		if !isFieldNameToken(name) {
			errs = append(errs, config.FormatError(
				"auth.forwarded_headers",
				name,
				"is not a valid HTTP header field-name token",
				"use only letters, digits, and ! # $ % & ' * + - . ^ _ | ~ `",
			))
			continue
		}
		if _, reserved := reservedHeaders[textproto.CanonicalMIMEHeaderKey(name)]; reserved {
			errs = append(errs, config.FormatError(
				"auth.forwarded_headers",
				name,
				"is a reserved header managed by Walera and cannot be forwarded",
				"remove it from auth.forwarded_headers; Walera sets it on every backend call",
			))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %w", errors.Join(errs...))
	}
	return nil
}

var _ = time.Second
