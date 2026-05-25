//go:build integration

// Package integration — harness.go combines PG container, mock auth backend,
// spawned binary, and SSE client into a single fixture that scenarios use
// via NewHarness(t). The four constructors are combined into one harness
// for test-author ergonomics; the inner types remain independently usable
// for scenarios that need finer control (e.g., the restart-resume test
// stops the PG container without touching the binary).
//
// Spawned-binary pattern: the harness expects ./testbin/cdc-sse to exist on
// disk. The Makefile target `make test-integration` builds the binary as a
// prerequisite. If the binary is missing the harness skips the test with a
// clear message; tests run individually via `go test -tags=integration
// -run …` should `make testbin` first.
//
// Cleanup ordering (t.Cleanup runs in LIFO):
//  1. SSE Client closeFn (in-test, via defer in the scenario).
//  2. Binary SIGTERM + 10s grace + SIGKILL.
//  3. MockAuth httptest.Server.Close.
//  4. PG container terminate.
//
// The Binary cleanup runs BEFORE the PG / MockAuth cleanups by construction
// (NewHarness registers t.Cleanup in the inverse order); SIGTERM the binary
// while PG and mock auth are still alive so the graceful-shutdown sequence
// can finish without spurious "auth/PG unreachable" log noise.
package integration

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// binaryPath returns the absolute path to the prebuilt cdc-sse binary.
// `go test` runs each package with CWD = the package directory, so we
// resolve the repo root (two levels up from this test package) and
// return its testbin/cdc-sse. Falls back to ./testbin/cdc-sse if the
// repo-root resolution fails (kept Skip semantics intact).
func binaryPath() string {
	if abs, err := filepath.Abs("../../testbin/cdc-sse"); err == nil {
		return abs
	}
	return "./testbin/cdc-sse"
}

// Harness is the combined integration-test fixture.
type Harness struct {
	PG     *PG
	Auth   *MockAuth
	Binary *Binary
	Client *Client
}

// SpawnBinaryOption tunes optional fields of the walera-test.yaml emitted by
// SpawnBinary. Multiple options compose; later options override earlier ones.
// Defaults documented in SpawnBinary's doc comment apply when no option
// touches the field. TEST-09 / SEC-01 regression anchor: the SEC-01 test
// needs a SHORT http.write_timeout so the writer-level per-frame
// SetWriteDeadline defence trips within the test budget.
type SpawnBinaryOption func(*spawnConfig)

// spawnConfig holds tunable defaults for the spawned binary. New options
// append fields here. The zero-value of each field means "use the production
// default from internal/config/config.go::applyDefaults".
type spawnConfig struct {
	writeTimeout        time.Duration // http.write_timeout; 0 → omit (use binary default 5s)
	withoutPlaintextEnv bool          // when true, do NOT inject WALERA_AUTH_ALLOW_PLAINTEXT=1
}

// WithWriteTimeout overrides the spawned binary's http.write_timeout
// (default 5s in internal/config/config.go::applyDefaults — set there via
// `_ = k.Set("http.write_timeout", "5s")`). SEC-01 (TEST-09.1) sets this to
// 200ms so a non-reading SSE client trips the per-frame SetWriteDeadline
// defence inside the test budget without bloating the integration test
// runtime.
func WithWriteTimeout(d time.Duration) SpawnBinaryOption {
	return func(c *spawnConfig) { c.writeTimeout = d }
}

// WithoutPlaintextAllow disables the harness's default injection of
// WALERA_AUTH_ALLOW_PLAINTEXT=1 into the spawned binary's environment.
//
// By default every integration-test binary is launched with this env-var
// set so the loopback httptest.Server (http://, no TLS) used as the mock
// auth backend is accepted at config-validation time — the SEC-04
// production guard in internal/config/config.go refuses non-https
// auth.backend_url values unless this override is present.
//
// A future end-to-end test that wants to exercise the SEC-04 binary-
// lifecycle gate itself (i.e., assert that the binary REFUSES to start
// when given an http URL and no override) must opt out of the auto-
// injection via this option. Such a test is the only black-box coverage
// that would catch a regression where the env-var lookup is bypassed in
// main() before config.Load runs — e.g., a hypothetical "load secrets
// from KMS" feature that re-orders startup.
//
// Existing call sites are unaffected: when this option is NOT passed,
// the env-var is injected exactly as before.
//
// WR-02 review-fix anchor; TEST-09 supporting harness change.
func WithoutPlaintextAllow() SpawnBinaryOption {
	return func(c *spawnConfig) { c.withoutPlaintextEnv = true }
}

// NewHarness boots PG, mock auth, the binary, and an SSE client. Calls
// t.Skip if ./testbin/cdc-sse does not exist (i.e., the operator forgot to
// run `make testbin` before `go test`). On any other failure t.Fatal is
// invoked.
//
// Construction order matters:
//  1. PG (slowest — testcontainers boot).
//  2. MockAuth (httptest — fast).
//  3. Binary (depends on PG.DSN + MockAuth.URL).
//  4. Client (depends on Binary.BaseURL).
//
// t.Cleanup is registered for each in inverse order so the binary is
// SIGTERM'd before PG / MockAuth shut down (avoids shutdown-time log noise
// from the binary about "auth backend gone" or "WAL conn lost").
//
// opts threads SpawnBinaryOption values through to SpawnBinary. Existing
// call sites (NewHarness(t)) continue to compile unchanged — Go's variadic
// rules guarantee source compatibility.
func NewHarness(t *testing.T, opts ...SpawnBinaryOption) *Harness {
	t.Helper()
	if _, err := os.Stat(binaryPath()); err != nil {
		t.Skipf("%s missing — run `make testbin` (or `go build -o testbin/cdc-sse ./cmd/cdc-sse`) first: %v", binaryPath(), err)
	}

	pg := NewPG(t)
	auth := NewMockAuth(t)
	bin := SpawnBinary(t, pg.DSN, pg.ReplicationDSN(), auth.URL(), opts...)
	client := NewClient(bin.BaseURL())

	return &Harness{
		PG:     pg,
		Auth:   auth,
		Binary: bin,
		Client: client,
	}
}

// syncBuffer is a bytes.Buffer with a mutex for safe concurrent reads from
// scenarios while the os/exec pipe goroutine appends bytes. testcontainers-go
// also reads container logs concurrently via its wait strategies, so the
// stderr surface must be race-free under the -race detector.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// Binary wraps the spawned ./testbin/cdc-sse process.
type Binary struct {
	cmd       *exec.Cmd
	stderrBuf *syncBuffer
	addr      string // "http://127.0.0.1:PORT"
}

// BaseURL returns "http://127.0.0.1:PORT" for SSE Client construction.
func (b *Binary) BaseURL() string { return b.addr }

// Stderr returns a snapshot of the binary's stderr buffer. Scenarios use
// this to assert log lines (e.g. "automaxprocs" smoke test for Pitfall G9).
//
// Returns a string copy — the underlying buffer is still being written to by
// the os/exec pipe goroutine, so callers must not retain references to the
// returned string and re-read.
func (b *Binary) Stderr() string { return b.stderrBuf.String() }

// Signal sends sig to the binary. Used by scenario 08 (graceful shutdown
// — SIGTERM mid-stream and assert event:shutdown frames).
func (b *Binary) Signal(sig syscall.Signal) error {
	if b.cmd == nil || b.cmd.Process == nil {
		return fmt.Errorf("binary: no process")
	}
	return b.cmd.Process.Signal(sig)
}

// SpawnBinary execs ./testbin/cdc-sse with a generated walera-test.yaml that
// points at the supplied PG DSN, replication DSN, and mock auth URL. Returns
// when the binary's HTTP listener is observed via TCP dial (10s budget), or
// t.Fatal on timeout.
//
// Config keys mirror config.yaml's full schema. All
// timing-sensitive defaults are tightened so the integration suite finishes
// quickly:
//   - wal.reconnect.reset_after_success_duration: 2s (vs prod 60s).
//   - wal.lag_sample_interval: 1s (vs prod 5s).
//   - metrics.sample_interval: 1s (vs prod 30s).
//   - shutdown.deadline: 5s, drain_deadline: 4s (vs prod 10s / 8s).
//   - auth.default_ttl_seconds: 1 (vs prod 60) — scenario 04 needs fast
//     refresh; SetTTL on the mock can further reduce per-scenario.
//   - router.heartbeat_interval: 1s (vs prod 15s) — keeps the stream alive
//     during fast assertions and surfaces heartbeats in tests that care.
//   - router.wildcard_buffer: 1 (vs prod default e.g. 512) — minimum-
//     non-zero buffer so Test06/SlowConsumer can saturate the per-
//     subscriber wildcard buffer with a small burst within 3s and observe
//     the `walera_tx_dropped_total{reason="slow_consumer"}` counter bump
//     deterministically. Localhost TCP backpressure is unreliable
//     (autotune absorbs hundreds of MiB even with SO_RCVBUF/
//     TCP_WINDOW_CLAMP capped), so the test relies on a small buffer +
//     fast burst rather than TCP-layer backpressure. Production retains
//     its tuned default in `internal/router/config.go` /
//     `internal/config/config.go`. This is a pure test-fixture knob; it
//     does NOT change production semantics. Other integration scenarios
//     that exercise wildcard fan-out (Test10) commit a single row per
//     assertion and remain safely under buffer=1.
//
// On t.Cleanup: SIGTERM, then 10s wait, then SIGKILL on timeout (Pitfall G6
// cleanup pattern from RESEARCH Pattern 9).
//
// opts apply optional tweaks to the emitted walera-test.yaml. See
// SpawnBinaryOption / WithWriteTimeout for the available knobs. The base
// template is the SAME for every call; options append/override specific keys
// only.
func SpawnBinary(t *testing.T, pgDSN, replicationDSN, mockAuthURL string, opts ...SpawnBinaryOption) *Binary {
	t.Helper()
	// replicationDSN is retained in the signature for caller compatibility but
	// is no longer written to the emitted config: the binary derives its
	// replication connection from the single top-level database.url (pgDSN).
	_ = replicationDSN
	port := freePort(t)
	addr := "127.0.0.1:" + port

	sc := spawnConfig{}
	for _, opt := range opts {
		opt(&sc)
	}

	// http.write_timeout — emitted ONLY when WithWriteTimeout(d) was passed;
	// otherwise the binary uses its production default (5s) from
	// internal/config/config.go::applyDefaults. time.Duration.String()
	// returns Go duration syntax ("200ms", "5s") which the koanf parser
	// accepts unquoted (mirrors heartbeat_interval: 1s below).
	writeTimeoutLine := ""
	if sc.writeTimeout > 0 {
		writeTimeoutLine = fmt.Sprintf("  write_timeout: %s\n", sc.writeTimeout)
	}

	cfg := fmt.Sprintf(`log:
  level: debug
  dev_mode: false
database:
  url: %q
wal:
  publication_name: cdc_sse_streamer
  slot_name_prefix: walera_test
  slot_headroom_min: 1
  naive_timestamp_assume_utc: true
  reconnect:
    reset_after_success_duration: 2s
  lag_sample_interval: 1s
http:
  addr: %q
  cors_origins: []
  max_payload_bytes: 1048576
%s
router:
  exact_buffer: 16
  wildcard_buffer: 1    # test-fixture knob — minimum-non-zero buffer so Test06/SlowConsumer triggers within a short burst even on hosts where loopback TCP backpressure is weak (see SpawnBinary doc comment). Production retains its tuned default.
  max_changes_per_tx: 1000  # test-fixture: keeps large-tx test split behaviour (harness value, not production default)
  heartbeat_interval: 1s
auth:
  backend_url: %q
  default_ttl_seconds: 1
  health_channel: _health
  request_timeout: 2s
  breaker:
    window_buckets: 30
    bucket_seconds: 1
    failure_rate_threshold: 0.5
    debounce_floor: 5
    cooldown: 2s
    stale_refresh_jitter: 100ms
limits:
  global_concurrent: 100
  per_user_concurrent: 10
  per_user_rate_per_second: 100.0
  per_user_burst: 100
  pre_auth_rate_per_second: 100.0
  pre_auth_burst: 100
  sweep_interval: 5s
  sweep_idle_threshold: 30s
health:
  readyz_probe_interval: 1s
metrics:
  sample_interval: 1s
shutdown:
  deadline: 5s
  drain_deadline: 4s
`, pgDSN, addr, writeTimeoutLine, mockAuthURL)

	cfgFile := filepath.Join(t.TempDir(), "walera-test.yaml")
	if err := os.WriteFile(cfgFile, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	stderrBuf := &syncBuffer{}
	cmd := exec.Command(binaryPath(), "--config", cfgFile)
	cmd.Stderr = stderrBuf
	cmd.Stdout = nil // walera writes only to stderr (D-26)
	// SEC-04 escape hatch — the mock auth backend is an httptest.Server that
	// binds an http:// URL on loopback (no TLS termination in tests). The
	// SEC-04 / F-P1-04 guard in internal/config/config.go refuses to start
	// the binary with a non-https auth.backend_url unless this env-var is
	// set. Setting it on the spawned binary keeps the production guard
	// intact while allowing the loopback-only test fixture to work. This
	// applies ONLY to the test harness; production deployments never see
	// this env-var.
	//
	// WR-02 (2026-05-18): WithoutPlaintextAllow() opts out of the auto-
	// injection so a future test can exercise the SEC-04 binary-lifecycle
	// gate (i.e., assert the binary refuses to start when given an http
	// URL with no override). Default behavior (env-var injected) is
	// unchanged — existing scenarios continue to compile and pass.
	cmd.Env = os.Environ()
	if !sc.withoutPlaintextEnv {
		cmd.Env = append(cmd.Env, "WALERA_AUTH_ALLOW_PLAINTEXT=1")
	}
	// Run in a fresh process group so SIGTERM doesn't escape to children of
	// the test runner. setpgid is Linux/macOS; harness is tagged
	// `integration` and the suite is intended for Linux CI.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("binary start: %v", err)
	}

	t.Cleanup(func() {
		if cmd.Process == nil {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		doneCh := make(chan error, 1)
		go func() { doneCh <- cmd.Wait() }()
		select {
		case <-doneCh:
		case <-time.After(10 * time.Second):
			t.Logf("binary did not exit on SIGTERM within 10s; sending SIGKILL\nstderr:\n%s", stderrBuf.String())
			_ = cmd.Process.Kill()
			<-doneCh
		}
	})

	// Wait up to 10s for the binary to start listening on its SSE port.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
			_ = conn.Close()
			return &Binary{cmd: cmd, stderrBuf: stderrBuf, addr: "http://" + addr}
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Capture stderr for the t.Fatalf message — invaluable when diagnosing
	// missing config keys or PG connect failures.
	t.Fatalf("binary did not start listening at %s within 10s\nstderr:\n%s", addr, stderrBuf.String())
	return nil
}

// freePort returns a TCP port that was just-now bindable on 127.0.0.1. The
// port is released before return, so a tight race remains where another
// process could grab it before the binary's net.Listen runs — acceptable for
// tests (the harness's 10s readiness loop would fail loudly if so).
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: listen: %v", err)
	}
	addr := l.Addr().(*net.TCPAddr)
	_ = l.Close()
	return strconv.Itoa(addr.Port)
}
