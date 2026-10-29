package auth

import (
	"errors"
	"fmt"
	"net/textproto"
	"net/url"
	"time"

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
	BackendURL string

	DefaultTTLSeconds int

	RequestTimeout time.Duration

	// ForwardedCookies / ForwardedHeaders are allowlists of client-supplied
	// credentials threaded from the SSE handshake into the OpenSession backend
	// call. Empty (nil) means the feature is off for that kind — no defaults.
	ForwardedCookies []string

	ForwardedHeaders []string

	// AllowPlaintext permits an http:// BackendURL (WALERA_AUTH_ALLOW_PLAINTEXT=1, dev only).
	AllowPlaintext bool
}

// DocsURL points operators at the auth-feature documentation. Included in
// log lines surfaced from the periodic-refresh loop so an on-call engineer
// can jump straight to the contract.
const DocsURL = "https://github.com/slavnycoder/walera/blob/master/docs/auth.md"

// Defaults are the auth settings used when the corresponding WALERA_*
// variables are unset; RequestTimeout is not configurable.
func Defaults() Config {
	return Config{
		DefaultTTLSeconds: 60,
		RequestTimeout:    2 * time.Second,
	}
}

// Validate names the offending variable but never echoes its value: the
// backend URL may carry credentials.
func (c Config) Validate() error {
	var errs []error
	if c.BackendURL == "" {
		errs = append(errs, errors.New("WALERA_AUTH_URL is required"))
	} else {
		u, err := url.Parse(c.BackendURL)
		switch {
		case err != nil:
			errs = append(errs, config.FormatError(
				"WALERA_AUTH_URL",
				"",
				"is not a valid URL",
				"provide an absolute https:// URL",
			))
		case u.Scheme != "http" && u.Scheme != "https":
			errs = append(errs, config.FormatError(
				"WALERA_AUTH_URL",
				"",
				"scheme must be http or https",
				"use https://<host>/<base-path>",
			))
		case u.Host == "":
			errs = append(errs, config.FormatError(
				"WALERA_AUTH_URL",
				"",
				"host is empty",
				"include host[:port] in the URL",
			))
		default:
			if u.Scheme != "https" && !c.AllowPlaintext {
				errs = append(errs, errors.New("WALERA_AUTH_URL must use https:// (set WALERA_AUTH_ALLOW_PLAINTEXT=1 for dev)"))
			}
		}
	}
	if c.RequestTimeout <= 0 {
		errs = append(errs, errors.New("auth: request timeout must be > 0"))
	}
	if c.DefaultTTLSeconds < 1 {
		errs = append(errs, config.FormatError(
			"WALERA_AUTH_TTL",
			"",
			"must be >= 1s: live streams must re-check permissions so revocations propagate",
			"omit it (default 60s) or use a positive duration",
		))
	}

	// Forwarded-credential allowlists. Cookie names follow RFC 6265 tokens;
	// header names use the same token class. Reserved headers are managed by
	// Walera and can never be forwarded.
	for i, name := range c.ForwardedCookies {
		if !isFieldNameToken(name) {
			errs = append(errs, config.FormatError(
				fmt.Sprintf("WALERA_AUTH_FORWARD_COOKIES entry %d", i+1),
				"",
				"is not a valid RFC 6265 cookie name token",
				"use only letters, digits, and ! # $ % & ' * + - . ^ _ | ~ `",
			))
		}
	}
	for i, name := range c.ForwardedHeaders {
		key := fmt.Sprintf("WALERA_AUTH_FORWARD_HEADERS entry %d", i+1)
		if !isFieldNameToken(name) {
			errs = append(errs, config.FormatError(
				key,
				"",
				"is not a valid HTTP header field-name token",
				"use only letters, digits, and ! # $ % & ' * + - . ^ _ | ~ `",
			))
			continue
		}
		if _, reserved := reservedHeaders[textproto.CanonicalMIMEHeaderKey(name)]; reserved {
			errs = append(errs, config.FormatError(
				key,
				"",
				"is a reserved header managed by Walera and cannot be forwarded",
				"remove it; Walera sets it on every backend call",
			))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %w", errors.Join(errs...))
	}
	return nil
}
