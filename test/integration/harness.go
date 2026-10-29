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
	"strings"
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
	withoutPlaintextEnv bool
	globalConcurrent    int
	forwardedCookies    []string
	forwardedHeaders    []string
	bootstrapAutoTables []string
}

func WithoutPlaintextAllow() SpawnBinaryOption {
	return func(c *spawnConfig) { c.withoutPlaintextEnv = true }
}

// WithGlobalConcurrent sets WALERA_MAX_CONNECTIONS so the 503 admission
// path is reachable with a handful of held connections.
func WithGlobalConcurrent(n int) SpawnBinaryOption {
	return func(c *spawnConfig) { c.globalConcurrent = n }
}

// WithForwardedCookies sets WALERA_AUTH_FORWARD_COOKIES so the named client
// cookies are threaded from the SSE handshake into the OpenSession backend call.
func WithForwardedCookies(names ...string) SpawnBinaryOption {
	return func(c *spawnConfig) { c.forwardedCookies = append(c.forwardedCookies, names...) }
}

// WithForwardedHeaders sets WALERA_AUTH_FORWARD_HEADERS so the named client
// headers are threaded from the SSE handshake into the OpenSession backend call.
func WithForwardedHeaders(names ...string) SpawnBinaryOption {
	return func(c *spawnConfig) { c.forwardedHeaders = append(c.forwardedHeaders, names...) }
}

// WithBootstrapAuto sets WALERA_TABLES to the given explicit table list, for
// tests that exercise Walera's own publication-creation path (the publication
// they use must not exist yet).
func WithBootstrapAuto(tables ...string) SpawnBinaryOption {
	return func(c *spawnConfig) { c.bootstrapAutoTables = tables }
}

func NewHarness(t *testing.T, opts ...SpawnBinaryOption) *Harness {
	t.Helper()
	if _, err := os.Stat(binaryPath()); err != nil {
		t.Skipf("%s missing — run `make testbin` (or `go build -o testbin/cdc-sse ./cmd/cdc-sse`) first: %v", binaryPath(), err)
	}

	pg := NewPG(t)
	auth := NewMockAuth(t)
	bin := SpawnBinary(t, pg.DSN, pg.ReplicationDSN(), auth.URL(), opts...)
	pg.WaitForActiveSlot(t, "walera", 15*time.Second)
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
	if sc.globalConcurrent == 0 {
		sc.globalConcurrent = 100
	}

	// The fixture publication is created by testdata/001_publication.sql, so
	// Walera only verifies it unless WithBootstrapAuto lists tables.
	env := []string{
		"WALERA_DATABASE_URL=" + pgDSN,
		"WALERA_AUTH_URL=" + mockAuthURL,
		"WALERA_ADDR=" + addr,
		"WALERA_PUBLICATION=cdc_sse_streamer",
		"WALERA_AUTH_TTL=1s",
		"WALERA_MAX_CONNECTIONS=" + strconv.Itoa(sc.globalConcurrent),
		"WALERA_LOG_LEVEL=debug",
	}
	if !sc.withoutPlaintextEnv {
		env = append(env, "WALERA_AUTH_ALLOW_PLAINTEXT=1")
	}
	if len(sc.bootstrapAutoTables) > 0 {
		env = append(env, "WALERA_TABLES="+strings.Join(sc.bootstrapAutoTables, ","))
	}
	if len(sc.forwardedCookies) > 0 {
		env = append(env, "WALERA_AUTH_FORWARD_COOKIES="+strings.Join(sc.forwardedCookies, ","))
	}
	if len(sc.forwardedHeaders) > 0 {
		env = append(env, "WALERA_AUTH_FORWARD_HEADERS="+strings.Join(sc.forwardedHeaders, ","))
	}

	stderrBuf := &syncBuffer{}
	cmd := exec.Command(binaryPath()) //nolint:gosec // G204: fixed repo-relative test binary path, no external input
	cmd.Stderr = stderrBuf
	cmd.Stdout = nil
	// Host WALERA_* variables would either override the fixture or, being
	// unknown, fail startup.
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "WALERA_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, env...)

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
		case err := <-doneCh:
			if err != nil {
				t.Logf("binary exited with error after SIGTERM: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Logf("binary did not exit on SIGTERM within 10s; sending SIGKILL\nstderr:\n%s", stderrBuf.String())
			_ = cmd.Process.Kill()
			if err := <-doneCh; err != nil {
				t.Logf("binary exited with error after SIGKILL: %v", err)
			}
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
