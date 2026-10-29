package auth_test

import (
	"strings"
	"testing"

	"github.com/walera/walera/internal/auth"
)

func validCfg() auth.Config {
	cfg := auth.Defaults()
	cfg.BackendURL = "https://auth.example/test"
	return cfg
}

func TestDefaults(t *testing.T) {
	cfg := validCfg()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if cfg.DefaultTTLSeconds != 60 {
		t.Errorf("DefaultTTLSeconds = %d; want 60 (refresh is always on)", cfg.DefaultTTLSeconds)
	}
	if cfg.RequestTimeout.Seconds() != 2 {
		t.Errorf("RequestTimeout = %s; want 2s", cfg.RequestTimeout)
	}
	if cfg.AllowPlaintext || cfg.ForwardedCookies != nil || cfg.ForwardedHeaders != nil {
		t.Errorf("defaults = %+v; want plaintext off and no forwarding", cfg)
	}
}

func TestValidate_RejectsZeroTTL(t *testing.T) {
	cfg := validCfg()
	cfg.DefaultTTLSeconds = 0
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "WALERA_AUTH_TTL") {
		t.Fatalf("Validate: err = %v; want WALERA_AUTH_TTL rejected", err)
	}
}

func TestValidate_RequiresBackendURL(t *testing.T) {
	cfg := validCfg()
	cfg.BackendURL = ""
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "WALERA_AUTH_URL is required") {
		t.Fatalf("Validate: err = %v; want WALERA_AUTH_URL required", err)
	}
}

func TestValidate_RequiresHTTPS(t *testing.T) {
	cfg := validCfg()
	cfg.BackendURL = "http://insecure.example"
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "must use https://") || !strings.Contains(err.Error(), "WALERA_AUTH_ALLOW_PLAINTEXT=1") {
		t.Fatalf("Validate: err = %v; want https required with the override named", err)
	}
}

func TestValidate_AllowPlaintextAcceptsHTTP(t *testing.T) {
	cfg := validCfg()
	cfg.BackendURL = "http://insecure.example"
	cfg.AllowPlaintext = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with AllowPlaintext: %v", err)
	}
}

func TestValidate_ForwardedAllowlists_Valid(t *testing.T) {
	cfg := validCfg()
	cfg.ForwardedCookies = []string{"session", "csrf_token"}
	cfg.ForwardedHeaders = []string{"X-Tenant-Id", "X-Trace"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidate_ForwardedCookies_RejectsInvalidName(t *testing.T) {
	for _, val := range []string{"bad name", "bad:name", "", "a;b"} {
		t.Run(val, func(t *testing.T) {
			cfg := validCfg()
			cfg.ForwardedCookies = []string{val}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate: err = nil; want invalid cookie-name error")
			}
			if !strings.Contains(err.Error(), "WALERA_AUTH_FORWARD_COOKIES entry 1") || !strings.Contains(err.Error(), "RFC 6265") {
				t.Errorf("err = %q; want variable entry and RFC 6265 token message", err.Error())
			}
		})
	}
}

func TestValidate_ForwardedHeaders_RejectsInvalidName(t *testing.T) {
	cfg := validCfg()
	cfg.ForwardedHeaders = []string{"bad header"}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate: err = nil; want invalid header-name error")
	}
	if !strings.Contains(err.Error(), "WALERA_AUTH_FORWARD_HEADERS entry 1") || !strings.Contains(err.Error(), "field-name token") {
		t.Errorf("err = %q; want variable entry and field-name token message", err.Error())
	}
}

func TestValidate_ForwardedHeaders_RejectsReserved(t *testing.T) {
	// Reserved-name rejection is case-insensitive (canonicalized).
	for _, name := range []string{
		"Authorization", "authorization", "Cookie", "content-type",
		"X-Request-Id", "x-request-id", "Host", "Origin", "origin",
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validCfg()
			cfg.ForwardedHeaders = []string{name}
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), "reserved header managed by Walera") {
				t.Fatalf("Validate: err = %v; want reserved-header rejection for %q", err, name)
			}
		})
	}
}

// The backend URL may carry credentials, so errors name the variable only.
func TestValidate_BackendURLRules(t *testing.T) {
	cases := []struct {
		name string
		url  string
		want string
	}{
		{"bad scheme", "ftp://user:hunter2@example.com", "scheme must be"},
		{"empty host", "https://", "host is empty"},
		{"unparseable URL", "https://user:hunter2@example.com/\x7f", "is not a valid URL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validCfg()
			cfg.BackendURL = tc.url
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), "WALERA_AUTH_URL") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate: err = %v; want WALERA_AUTH_URL %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "hunter2") {
				t.Fatalf("err = %q; leaked URL credentials", err)
			}
		})
	}
}
