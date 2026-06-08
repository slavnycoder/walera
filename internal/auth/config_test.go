package auth_test

import (
	"strings"
	"testing"

	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/auth"
)

func newK(t *testing.T) *koanf.Koanf {
	t.Helper()
	k := koanf.New(".")
	auth.ApplyDefaults(k)
	_ = k.Set("auth.backend_url", "https://auth.example/test")
	_ = k.Set("auth.signing.secret", strings.Repeat("a", 32))
	return k
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := auth.LoadConfig(newK(t))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.DefaultTTLSeconds != 0 {
		t.Errorf("DefaultTTLSeconds = %d; want 0 (periodic refresh opt-in)", cfg.DefaultTTLSeconds)
	}
	if cfg.HealthChannel != "_health" {
		t.Errorf("HealthChannel = %q; want _health", cfg.HealthChannel)
	}
	if cfg.Breaker.WindowBuckets != 30 {
		t.Errorf("Breaker.WindowBuckets = %d; want 30", cfg.Breaker.WindowBuckets)
	}
}

func TestLoadConfig_RequiresBackendURL(t *testing.T) {
	k := newK(t)
	_ = k.Set("auth.backend_url", "")
	_, err := auth.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "auth.backend_url is required") {
		t.Fatalf("LoadConfig: err = %v; want backend_url required", err)
	}
}

func TestLoadConfig_RequiresHTTPS(t *testing.T) {
	k := newK(t)
	_ = k.Set("auth.backend_url", "http://insecure.example")
	_, err := auth.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "must use https://") {
		t.Fatalf("LoadConfig: err = %v; want https required", err)
	}
}

func TestLoadConfig_HTTPSOverrideAccepted(t *testing.T) {
	t.Setenv("WALERA_AUTH_ALLOW_PLAINTEXT", "1")
	k := newK(t)
	_ = k.Set("auth.backend_url", "http://insecure.example")
	if _, err := auth.LoadConfig(k); err != nil {
		t.Fatalf("LoadConfig with override: %v", err)
	}
}

func TestLoadConfig_BreakerCooldownLessThanRequestTimeout(t *testing.T) {
	k := newK(t)
	_ = k.Set("auth.request_timeout", "5s")
	_ = k.Set("auth.breaker.cooldown", "1s")
	_, err := auth.LoadConfig(k)
	if err == nil {
		t.Fatal("LoadConfig: err = nil; want cooldown<timeout error")
	}
	if !strings.Contains(err.Error(), "auth.breaker.cooldown vs auth.request_timeout") {
		t.Errorf("err = %q; want pair-comparison error", err.Error())
	}
}

func TestLoadConfig_RequiresSigningSecret(t *testing.T) {
	k := newK(t)
	_ = k.Set("auth.signing.secret", "")
	_, err := auth.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "auth.signing.secret is required") {
		t.Fatalf("LoadConfig: err = %v; want signing.secret required", err)
	}
}

func TestLoadConfig_RejectsShortSigningSecret(t *testing.T) {
	k := newK(t)
	_ = k.Set("auth.signing.secret", strings.Repeat("a", 31))
	_, err := auth.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "auth.signing.secret") {
		t.Fatalf("LoadConfig: err = %v; want too-short error", err)
	}
}

func TestLoadConfig_RequiresSigningKid(t *testing.T) {
	k := newK(t)
	_ = k.Set("auth.signing.kid", "")
	_, err := auth.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "auth.signing.kid is required") {
		t.Fatalf("LoadConfig: err = %v; want signing.kid required", err)
	}
}

func TestLoadConfig_DefaultKid(t *testing.T) {
	cfg, err := auth.LoadConfig(newK(t))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Signing.Kid != "v1" {
		t.Errorf("Signing.Kid = %q; want v1", cfg.Signing.Kid)
	}
}

func TestLoadConfig_ForwardedAllowlists_Valid(t *testing.T) {
	k := newK(t)
	_ = k.Set("auth.forwarded_cookies", []string{"session", "csrf_token"})
	_ = k.Set("auth.forwarded_headers", []string{"X-Tenant-Id", "X-Trace"})
	cfg, err := auth.LoadConfig(k)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.ForwardedCookies) != 2 {
		t.Errorf("ForwardedCookies = %v; want 2 entries", cfg.ForwardedCookies)
	}
	if len(cfg.ForwardedHeaders) != 2 {
		t.Errorf("ForwardedHeaders = %v; want 2 entries", cfg.ForwardedHeaders)
	}
}

func TestLoadConfig_ForwardedAllowlists_EmptyValidates(t *testing.T) {
	// Feature off (nil allowlists) must validate cleanly.
	cfg, err := auth.LoadConfig(newK(t))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.ForwardedCookies != nil {
		t.Errorf("ForwardedCookies = %v; want nil (feature off)", cfg.ForwardedCookies)
	}
	if cfg.ForwardedHeaders != nil {
		t.Errorf("ForwardedHeaders = %v; want nil (feature off)", cfg.ForwardedHeaders)
	}
}

func TestLoadConfig_ForwardedCookies_RejectsInvalidName(t *testing.T) {
	cases := []struct {
		name string
		val  string
	}{
		{"space", "bad name"},
		{"colon", "bad:name"},
		{"empty", ""},
		{"semicolon", "a;b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := newK(t)
			_ = k.Set("auth.forwarded_cookies", []string{tc.val})
			_, err := auth.LoadConfig(k)
			if err == nil {
				t.Fatalf("LoadConfig: err = nil; want invalid cookie-name error")
			}
			if !strings.Contains(err.Error(), "auth.forwarded_cookies") {
				t.Errorf("err = %q; want auth.forwarded_cookies field", err.Error())
			}
			if !strings.Contains(err.Error(), "RFC 6265") {
				t.Errorf("err = %q; want RFC 6265 token message", err.Error())
			}
		})
	}
}

func TestLoadConfig_ForwardedHeaders_RejectsInvalidName(t *testing.T) {
	k := newK(t)
	_ = k.Set("auth.forwarded_headers", []string{"bad header"})
	_, err := auth.LoadConfig(k)
	if err == nil {
		t.Fatal("LoadConfig: err = nil; want invalid header-name error")
	}
	if !strings.Contains(err.Error(), "auth.forwarded_headers") {
		t.Errorf("err = %q; want auth.forwarded_headers field", err.Error())
	}
	if !strings.Contains(err.Error(), "field-name token") {
		t.Errorf("err = %q; want field-name token message", err.Error())
	}
}

func TestLoadConfig_ForwardedHeaders_RejectsReserved(t *testing.T) {
	// Reserved-name rejection is case-insensitive (canonicalized). Cover a few
	// reserved names in assorted casings.
	cases := []string{
		"Authorization",
		"authorization",
		"Cookie",
		"content-type",
		"X-Request-Id",
		"x-request-id",
		"X-Walera-Sig",
		"X-Walera-Kid",
		"Host",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			k := newK(t)
			_ = k.Set("auth.forwarded_headers", []string{name})
			_, err := auth.LoadConfig(k)
			if err == nil {
				t.Fatalf("LoadConfig: err = nil; want reserved-header rejection for %q", name)
			}
			if !strings.Contains(err.Error(), "reserved header managed by Walera") {
				t.Errorf("err = %q; want reserved-header message for %q", err.Error(), name)
			}
		})
	}
}

func TestLoadConfig_BackendURLSchemaRules(t *testing.T) {
	cases := []struct {
		name        string
		url         string
		wantSubstrs []string
	}{
		{
			name:        "bad scheme",
			url:         "ftp://example.com",
			wantSubstrs: []string{"auth.backend_url", "scheme must be"},
		},
		{
			name:        "empty host",
			url:         "https://",
			wantSubstrs: []string{"auth.backend_url"},
		},
		{
			name:        "unparseable URL",
			url:         "https://example.com/\x7f",
			wantSubstrs: []string{"auth.backend_url"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := newK(t)
			_ = k.Set("auth.backend_url", tc.url)
			_, err := auth.LoadConfig(k)
			if err == nil {
				t.Fatalf("LoadConfig: err = nil; want error")
			}
			for _, s := range tc.wantSubstrs {
				if !strings.Contains(err.Error(), s) {
					t.Errorf("err = %q; want substring %q", err.Error(), s)
				}
			}
		})
	}
}
