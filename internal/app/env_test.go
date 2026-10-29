package app

import (
	"strings"
	"testing"
	"time"
)

func baseEnv() []string {
	return []string{
		"WALERA_DATABASE_URL=postgres://walera:s3cret@db:5432/app",
		"WALERA_AUTH_URL=https://auth.example",
		"PATH=/usr/bin", // non-WALERA vars are ignored
	}
}

func TestLoadFromEnv_Defaults(t *testing.T) {
	cfg, err := LoadFromEnv(baseEnv())
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.HTTP.Addr != ":8080" || string(cfg.WAL.PublicationName) != "walera_pub" ||
		cfg.Auth.DefaultTTLSeconds != 60 || cfg.Limits.GlobalConcurrent != 50000 ||
		cfg.Log.Level != "info" || cfg.Auth.AllowPlaintext || len(cfg.WAL.Tables) != 0 {
		t.Fatalf("defaults wrong: %+v", cfg)
	}
}

func TestLoadFromEnv_AllVars(t *testing.T) {
	env := append(baseEnv(),
		"WALERA_ADDR=127.0.0.1:9000",
		"WALERA_PUBLICATION=pub_x",
		"WALERA_TABLES=public.a, public.b",
		"WALERA_CORS_ORIGINS=https://App.example",
		"WALERA_AUTH_TTL=90s",
		"WALERA_AUTH_FORWARD_COOKIES=session",
		"WALERA_AUTH_FORWARD_HEADERS=X-Api-Key",
		"WALERA_AUTH_ALLOW_PLAINTEXT=1",
		"WALERA_MAX_CONNECTIONS=200",
		"WALERA_LOG_LEVEL=debug",
	)
	env[1] = "WALERA_AUTH_URL=http://auth:8000"
	cfg, err := LoadFromEnv(env)
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.HTTP.Addr != "127.0.0.1:9000" || string(cfg.WAL.PublicationName) != "pub_x" ||
		strings.Join(cfg.WAL.Tables, ",") != "public.a,public.b" ||
		strings.Join(cfg.HTTP.CORSOrigins, ",") != "https://app.example" ||
		cfg.Auth.DefaultTTLSeconds != 90 || cfg.Auth.ForwardedCookies[0] != "session" ||
		cfg.Auth.ForwardedHeaders[0] != "X-Api-Key" || !cfg.Auth.AllowPlaintext ||
		cfg.Limits.GlobalConcurrent != 200 || cfg.Log.Level != "debug" {
		t.Fatalf("parsed wrong: %+v", cfg)
	}
}

func TestLoadFromEnv_RequiredVars(t *testing.T) {
	_, err := LoadFromEnv([]string{"PATH=/usr/bin"})
	if err == nil || !strings.Contains(err.Error(), "WALERA_DATABASE_URL") || !strings.Contains(err.Error(), "WALERA_AUTH_URL") {
		t.Fatalf("err = %v; want both required vars named", err)
	}
}

// v3 names and config-file era variables must fail loudly, naming the
// variable but never echoing its value.
func TestLoadFromEnv_RejectsUnknownVars(t *testing.T) {
	env := append(baseEnv(),
		"WALERA_AUTH_BACKEND_URL=https://old.example",
		"WALERA_WAL_PUBLICATION_NAME=secretish-value",
	)
	_, err := LoadFromEnv(env)
	if err == nil {
		t.Fatal("err = nil; want unknown-variable rejection")
	}
	for _, name := range []string{"WALERA_AUTH_BACKEND_URL", "WALERA_WAL_PUBLICATION_NAME"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("err = %q; want it to name %s", err, name)
		}
	}
	if strings.Contains(err.Error(), "secretish-value") {
		t.Errorf("err = %q; must not echo values", err)
	}
}

func TestLoadFromEnv_InvalidDSNRedactsPassword(t *testing.T) {
	for _, dsn := range []string{
		"postgres://walera:s3cret@db:notaport/app",
		"postgres://walera@db:99999999/app?password=s3cret",
		"postgresql://walera@db/app?password=s3cret&sslmode=bogus",
		"host=db user=walera password=s3cret dbname=app",
		"mysql://walera:s3cret@db/app",
	} {
		t.Run(dsn, func(t *testing.T) {
			env := baseEnv()
			env[0] = "WALERA_DATABASE_URL=" + dsn
			_, err := LoadFromEnv(env)
			if err == nil {
				t.Fatal("err = nil; want invalid DSN error")
			}
			if strings.Contains(err.Error(), "s3cret") {
				t.Fatalf("err = %q; leaked password", err)
			}
			if !strings.Contains(err.Error(), "WALERA_DATABASE_URL") {
				t.Fatalf("err = %q; want the variable named", err)
			}
		})
	}
}

func TestLoadFromEnv_DSNRequiresPostgresScheme(t *testing.T) {
	for _, dsn := range []string{"host=db user=walera dbname=app", "mysql://db/app", "db:5432/app"} {
		env := baseEnv()
		env[0] = "WALERA_DATABASE_URL=" + dsn
		_, err := LoadFromEnv(env)
		if err == nil || !strings.Contains(err.Error(), "WALERA_DATABASE_URL must be a postgres:// URL") {
			t.Fatalf("%q: err = %v; want scheme error", dsn, err)
		}
	}
	for _, dsn := range []string{"postgres://db/app", "postgresql://db/app"} {
		env := baseEnv()
		env[0] = "WALERA_DATABASE_URL=" + dsn
		if _, err := LoadFromEnv(env); err != nil {
			t.Fatalf("%q: %v", dsn, err)
		}
	}
}

func TestLoadFromEnv_InvalidValues(t *testing.T) {
	for _, kv := range []string{
		"WALERA_AUTH_TTL=500ms",
		"WALERA_AUTH_TTL=soon",
		"WALERA_MAX_CONNECTIONS=0",
		"WALERA_MAX_CONNECTIONS=many",
		"WALERA_LOG_LEVEL=loud",
		"WALERA_AUTH_ALLOW_PLAINTEXT=yes",
		"WALERA_TABLES=no_schema",
		"WALERA_CORS_ORIGINS=not a url",
		"WALERA_ADDR=:99999",
		"WALERA_AUTH_FORWARD_HEADERS=Origin",
	} {
		t.Run(kv, func(t *testing.T) {
			if _, err := LoadFromEnv(append(baseEnv(), kv)); err == nil {
				t.Fatalf("%s: err = nil; want validation error", kv)
			}
		})
	}
}

func TestLoadFromEnv_TTLWholeSeconds(t *testing.T) {
	cfg, err := LoadFromEnv(append(baseEnv(), "WALERA_AUTH_TTL=2m"))
	if err != nil || time.Duration(cfg.Auth.DefaultTTLSeconds)*time.Second != 2*time.Minute {
		t.Fatalf("ttl = %v, %v; want 120s", cfg.Auth.DefaultTTLSeconds, err)
	}
}

func TestLoadFromEnv_PlaintextAuthURLNeedsOptIn(t *testing.T) {
	env := baseEnv()
	env[1] = "WALERA_AUTH_URL=http://auth:8000"
	_, err := LoadFromEnv(env)
	if err == nil || !strings.Contains(err.Error(), "WALERA_AUTH_ALLOW_PLAINTEXT=1") {
		t.Fatalf("err = %v; want https requirement naming WALERA_AUTH_ALLOW_PLAINTEXT=1", err)
	}
}

// Invalid values are reported by variable name only; the value itself could
// be a secret pasted into the wrong variable.
func TestLoadFromEnv_InvalidValuesNotEchoed(t *testing.T) {
	const secret = "hunter2secret"
	for _, kv := range []string{
		"WALERA_AUTH_URL=ftp://u:" + secret + "@auth",
		"WALERA_ADDR=" + secret,
		"WALERA_PUBLICATION=" + secret + "-x",
		"WALERA_TABLES=" + secret,
		"WALERA_CORS_ORIGINS=" + secret,
		"WALERA_AUTH_TTL=" + secret,
		"WALERA_AUTH_FORWARD_COOKIES=" + secret + ";",
		"WALERA_AUTH_FORWARD_HEADERS=" + secret + " x",
		"WALERA_AUTH_ALLOW_PLAINTEXT=" + secret,
		"WALERA_MAX_CONNECTIONS=" + secret,
		"WALERA_LOG_LEVEL=" + secret,
	} {
		name, _, _ := strings.Cut(kv, "=")
		t.Run(name, func(t *testing.T) {
			_, err := LoadFromEnv(append(baseEnv(), kv))
			if err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("err = %v; want an error naming %s", err, name)
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("err = %q; echoed the value", err)
			}
		})
	}
}

// Kubernetes injects service-link variables for a Service named "walera" into
// every pod; they must not trip the unknown-variable check.
func TestLoadFromEnv_IgnoresKubernetesServiceLinks(t *testing.T) {
	env := append(baseEnv(),
		"WALERA_SERVICE_HOST=10.0.0.1",
		"WALERA_SERVICE_PORT=8080",
		"WALERA_SERVICE_PORT_HTTP=8080",
		"WALERA_PORT=tcp://10.0.0.1:8080",
		"WALERA_PORT_8080_TCP=tcp://10.0.0.1:8080",
		"WALERA_PORT_8080_TCP_PROTO=tcp",
		"WALERA_PORT_8080_TCP_PORT=8080",
		"WALERA_PORT_8080_TCP_ADDR=10.0.0.1",
		"WALERA_PORT_53_UDP_ADDR=10.0.0.1",
		"WALERA_PORT_9_SCTP=sctp://10.0.0.1:9",
	)
	if _, err := LoadFromEnv(env); err != nil {
		t.Fatalf("LoadFromEnv with service links: %v", err)
	}
}

func TestLoadFromEnv_RejectsServiceLinkNearMisses(t *testing.T) {
	for _, name := range []string{
		"WALERA_PORTS", "WALERA_SERVICE", "WALERA_SERVICE_HOSTNAME",
		"WALERA_PORT_HTTP", "WALERA_PORT_8080_HTTP", "WALERA_PORT_8080_TCP_HOST",
		"WALERA_SERVICE_PORT_", "WALERA_PORT_8080_TCP_PORT_X",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := LoadFromEnv(append(baseEnv(), name+"=x"))
			if err == nil || !strings.Contains(err.Error(), "unknown environment variables: "+name) {
				t.Fatalf("err = %v; want %s rejected as unknown", err, name)
			}
		})
	}
}

func TestLoadFromEnv_LogLevelCaseInsensitive(t *testing.T) {
	cfg, err := LoadFromEnv(append(baseEnv(), "WALERA_LOG_LEVEL=INFO"))
	if err != nil || cfg.Log.Level != "info" {
		t.Fatalf("level = %v, %v; want info", cfg, err)
	}
}

func TestLoadFromEnv_ErrorPrefixOnce(t *testing.T) {
	_, err := LoadFromEnv(append(baseEnv(), "WALERA_AUTH_TTL=soon", "WALERA_PUBLICATION=bad-name"))
	if err == nil {
		t.Fatal("err = nil; want validation error")
	}
	if !strings.HasPrefix(err.Error(), "config: ") || strings.Count(err.Error(), "config: ") != 1 {
		t.Fatalf("err = %q; want exactly one leading \"config: \" prefix", err)
	}
}

func TestLoadFromEnv_FixedDefaults(t *testing.T) {
	cfg, err := LoadFromEnv(baseEnv())
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	h := cfg.HTTP
	if h.Addr != ":8080" || h.MaxPayloadBytes != 10<<20 || h.WriteTimeout != 5*time.Second ||
		h.MaxHeaderBytes != 16<<10 || !h.H2CEnabled {
		t.Fatalf("HTTP defaults = %+v", h)
	}
	if cfg.Shutdown.Deadline.Duration() != 10*time.Second {
		t.Fatalf("shutdown deadline = %v; want 10s", cfg.Shutdown.Deadline.Duration())
	}
}
