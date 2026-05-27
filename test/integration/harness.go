//go:build integration

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

func binaryPath() string {
	if abs, err := filepath.Abs("../../testbin/cdc-sse"); err == nil {
		return abs
	}
	return "./testbin/cdc-sse"
}

type Harness struct {
	PG     *PG
	Auth   *MockAuth
	Binary *Binary
	Client *Client
}

type SpawnBinaryOption func(*spawnConfig)

type spawnConfig struct {
	writeTimeout        time.Duration
	withoutPlaintextEnv bool
}

func WithWriteTimeout(d time.Duration) SpawnBinaryOption {
	return func(c *spawnConfig) { c.writeTimeout = d }
}

func WithoutPlaintextAllow() SpawnBinaryOption {
	return func(c *spawnConfig) { c.withoutPlaintextEnv = true }
}

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

type Binary struct {
	cmd       *exec.Cmd
	stderrBuf *syncBuffer
	addr      string
}

func (b *Binary) BaseURL() string { return b.addr }

func (b *Binary) Stderr() string { return b.stderrBuf.String() }

func (b *Binary) Signal(sig syscall.Signal) error {
	if b.cmd == nil || b.cmd.Process == nil {
		return fmt.Errorf("binary: no process")
	}
	return b.cmd.Process.Signal(sig)
}

func SpawnBinary(t *testing.T, pgDSN, replicationDSN, mockAuthURL string, opts ...SpawnBinaryOption) *Binary {
	t.Helper()

	_ = replicationDSN
	port := freePort(t)
	addr := "127.0.0.1:" + port

	sc := spawnConfig{}
	for _, opt := range opts {
		opt(&sc)
	}

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
  signing:
    secret: %q
    kid: %q
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
`, pgDSN, addr, writeTimeoutLine, mockAuthURL, IntegrationSigningSecret, IntegrationSigningKid)

	cfgFile := filepath.Join(t.TempDir(), "walera-test.yaml")
	if err := os.WriteFile(cfgFile, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	stderrBuf := &syncBuffer{}
	cmd := exec.Command(binaryPath(), "--config", cfgFile)
	cmd.Stderr = stderrBuf
	cmd.Stdout = nil

	cmd.Env = os.Environ()
	if !sc.withoutPlaintextEnv {
		cmd.Env = append(cmd.Env, "WALERA_AUTH_ALLOW_PLAINTEXT=1")
	}

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

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
			_ = conn.Close()
			return &Binary{cmd: cmd, stderrBuf: stderrBuf, addr: "http://" + addr}
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("binary did not start listening at %s within 10s\nstderr:\n%s", addr, stderrBuf.String())
	return nil
}

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
