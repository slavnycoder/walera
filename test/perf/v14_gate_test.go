//go:build perf_gate
// +build perf_gate

package perf

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

type Thresholds struct {
	SchemaVersion  int          `yaml:"schema_version"`
	Captured       string       `yaml:"captured"`
	CapturedCommit string       `yaml:"captured_commit"`
	BaselineStrace BaselineRefs `yaml:"baseline_strace"`
	Val02          ValBlock     `yaml:"val_02"`
	Val03          ValBlock     `yaml:"val_03"`
}

type BaselineRefs struct {
	Source1k string `yaml:"source_1k"`
	Source5k string `yaml:"source_5k"`
}

type ValBlock struct {
	Subscribers          int     `yaml:"subscribers"`
	Scenario             string  `yaml:"scenario"`
	CommitRate           int     `yaml:"commit_rate"`
	RowsPerTx            int     `yaml:"rows_per_tx"`
	Channels             string  `yaml:"channels"`
	DurationSeconds      int     `yaml:"duration_seconds"`
	WarmupDiscardSeconds int     `yaml:"warmup_discard_seconds"`
	EventsSentMinPerS    float64 `yaml:"events_sent_min_per_s"`
	EventsPerWritevMin   float64 `yaml:"events_per_writev_min"`
}

const (
	composeFile = "testbench/docker-compose.yml"

	healthzURL = "http://127.0.0.1:8080/healthz"

	metricsURL = "http://127.0.0.1:8080/metrics"

	pgDSN = "postgres://walera:walera@127.0.0.1:5432/walera?sslmode=disable"

	straceOffset = 45 * time.Second

	straceWindow = 30 * time.Second

	healthzTimeout = 90 * time.Second

	healthzPoll = 2 * time.Second

	expectedSchemaVersion = 1
)

func TestPerfGateV1k(t *testing.T) {
	th := loadThresholds(t)
	runGate(t, th.Val02, "1k", "02")
}

func TestPerfGateV5k(t *testing.T) {
	th := loadThresholds(t)
	runGate(t, th.Val03, "5k", "03")
}

func runGate(t *testing.T, block ValBlock, label, reqID string) {
	t.Helper()

	if block.EventsSentMinPerS <= 0 || block.EventsPerWritevMin <= 0 {
		t.Fatalf("VAL-%s thresholds malformed: events_sent_min_per_s=%.0f events_per_writev_min=%.2f (both must be > 0)",
			reqID, block.EventsSentMinPerS, block.EventsPerWritevMin)
	}

	if block.WarmupDiscardSeconds < 0 || block.WarmupDiscardSeconds >= block.DurationSeconds {
		t.Fatalf("VAL-%s warmup_discard_seconds=%d outside [0, duration_seconds=%d)",
			reqID, block.WarmupDiscardSeconds, block.DurationSeconds)
	}
	if int(straceOffset.Seconds()) < block.WarmupDiscardSeconds {
		t.Fatalf("VAL-%s straceOffset=%s starts before warmup_discard_seconds=%d ends (would sample ramp-up region)",
			reqID, straceOffset, block.WarmupDiscardSeconds)
	}

	root := repoRoot(t)
	ts := time.Now().UTC().Format("20060102T150405")
	outDir := filepath.Join("bench-out", "perf-gate-"+label+"-"+ts)
	absOut := filepath.Join(root, outDir)
	if err := os.MkdirAll(absOut, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", absOut, err)
	}

	project := os.Getenv("COMPOSE_PROJECT_NAME")
	if project == "" {

		sum := sha256.Sum256([]byte(t.Name() + "-" + ts))
		project = "walera-perf-gate-" + label + "-" + hex.EncodeToString(sum[:4])
	}
	t.Logf("VAL-%s: COMPOSE_PROJECT_NAME=%s", reqID, project)

	t.Cleanup(func() {
		cmd := exec.Command("docker", "compose", "--project-name", project, "-f", composeFile, "down", "-v", "--remove-orphans")
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Logf("compose down -v failed: %v\n%s", err, out)
		}
	})

	t.Logf("VAL-%s: docker compose --project-name %s -f %s up -d --build", reqID, project, composeFile)
	upCmd := exec.Command("docker", "compose", "--project-name", project, "-f", composeFile, "up", "-d", "--build")
	upCmd.Dir = root
	if out, err := upCmd.CombinedOutput(); err != nil {
		t.Fatalf("VAL-%s compose up failed: %v\n%s", reqID, err, out)
	}

	if err := waitHealthz(t, healthzTimeout); err != nil {
		t.Fatalf("VAL-%s walera healthz not reachable within %s: %v. See %s/", reqID, healthzTimeout, err, outDir)
	}

	token, err := loadAuthToken(filepath.Join(root, "testbench", ".env"))
	if err != nil {
		t.Fatalf("VAL-%s loadAuthToken: %v", reqID, err)
	}

	if err := os.Setenv("LOADGEN_AUTH_TOKEN", token); err != nil {
		t.Fatalf("VAL-%s setenv LOADGEN_AUTH_TOKEN: %v", reqID, err)
	}

	benchArgs := []string{
		"scripts/bench.sh",
		"--scenario", block.Scenario,
		"--subscribers", strconv.Itoa(block.Subscribers),
		"--duration", strconv.Itoa(block.DurationSeconds) + "s",
		"--commit-rate", strconv.Itoa(block.CommitRate),
		"--rows-per-tx", strconv.Itoa(block.RowsPerTx),
		"--channels", block.Channels,
		"--pg-dsn", pgDSN,
		"--pprof-addr", "127.0.0.1:6060",
		"--writer-bin", "./writer",
		"--loadgen-bin", "./loadgen",
		"--out-dir", outDir,
	}
	t.Logf("VAL-%s: bash %s", reqID, strings.Join(benchArgs, " "))

	benchCmd := exec.Command("bash", benchArgs...)
	benchCmd.Dir = root

	stdoutPipe, err := benchCmd.StdoutPipe()
	if err != nil {
		t.Fatalf("VAL-%s benchCmd.StdoutPipe: %v", reqID, err)
	}
	stderrPipe, err := benchCmd.StderrPipe()
	if err != nil {
		t.Fatalf("VAL-%s benchCmd.StderrPipe: %v", reqID, err)
	}

	if err := benchCmd.Start(); err != nil {
		t.Fatalf("VAL-%s benchCmd.Start: %v", reqID, err)
	}

	var logWG sync.WaitGroup
	logWG.Add(2)
	go streamLog(t, "bench[stdout]", stdoutPipe, &logWG)
	go streamLog(t, "bench[stderr]", stderrPipe, &logWG)

	stracePath := filepath.Join(absOut, "strace-sample-"+label+".txt")
	straceDone := make(chan error, 1)
	straceTimer := time.AfterFunc(straceOffset, func() {
		straceDone <- runStraceSample(t, reqID, stracePath)
	})

	defer straceTimer.Stop()

	warmupSnapshotPath := filepath.Join(absOut, "walera-metrics-warmup.txt")
	warmupDone := make(chan warmupSnapshot, 1)
	warmupTimer := time.AfterFunc(time.Duration(block.WarmupDiscardSeconds)*time.Second, func() {
		warmupDone <- captureWarmupSnapshot(t, reqID, warmupSnapshotPath)
	})
	defer warmupTimer.Stop()

	benchWaitTimeout := time.Duration(block.DurationSeconds+60) * time.Second
	benchDone := make(chan error, 1)
	go func() { benchDone <- benchCmd.Wait() }()

	select {
	case err := <-benchDone:
		logWG.Wait()
		if err != nil {
			t.Fatalf("VAL-%s bench.sh exited %v. See %s/", reqID, err, outDir)
		}
	case <-time.After(benchWaitTimeout):
		_ = benchCmd.Process.Kill()
		logWG.Wait()
		t.Fatalf("VAL-%s bench.sh did not exit within %s. See %s/", reqID, benchWaitTimeout, outDir)
	}

	select {
	case err := <-straceDone:
		if err != nil {
			t.Fatalf("VAL-%s strace sample failed: %v. See %s/", reqID, err, outDir)
		}
	case <-time.After(straceWindow + 10*time.Second):
		t.Fatalf("VAL-%s strace sample did not complete within %s. See %s/", reqID, straceWindow+10*time.Second, outDir)
	}

	walMetrics := filepath.Join(absOut, "walera-metrics.txt")
	loadMetrics := filepath.Join(absOut, "loadgen-metrics.txt")
	mustNonEmpty(t, reqID, walMetrics, outDir)
	mustNonEmpty(t, reqID, loadMetrics, outDir)
	mustNonEmpty(t, reqID, stracePath, outDir)

	var warmup warmupSnapshot
	select {
	case warmup = <-warmupDone:
		if warmup.err != nil {
			t.Fatalf("VAL-%s warmup-baseline scrape: %v. See %s/", reqID, warmup.err, outDir)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("VAL-%s warmup-baseline scrape did not complete (timer never fired). See %s/", reqID, outDir)
	}
	walBytes, err := os.ReadFile(walMetrics)
	if err != nil {
		t.Fatalf("VAL-%s read %s: %v", reqID, walMetrics, err)
	}
	if !bytes.Contains(walBytes, []byte("walera_events_sent_total")) {
		t.Fatalf("VAL-%s %s missing walera_events_sent_total. See %s/", reqID, walMetrics, outDir)
	}
	totalEvents, err := parseEventsSentTotal(walBytes)
	if err != nil {
		t.Fatalf("VAL-%s parseEventsSentTotal: %v. See %s/", reqID, err, outDir)
	}

	steadyDuration := block.DurationSeconds - block.WarmupDiscardSeconds
	if steadyDuration <= 0 {
		t.Fatalf("VAL-%s steady-state window is zero (duration=%d warmup=%d). See %s/",
			reqID, block.DurationSeconds, block.WarmupDiscardSeconds, outDir)
	}
	steadyEvents := totalEvents - warmup.total
	if steadyEvents < 0 {
		t.Fatalf("VAL-%s steady-events negative: final=%.0f warmup=%.0f (counter went backwards?). See %s/",
			reqID, totalEvents, warmup.total, outDir)
	}
	eventsPerSec := steadyEvents / float64(steadyDuration)
	t.Logf("VAL-%s steady-state window: events=%.0f (final=%.0f - warmup=%.0f) over %ds → %.0f/s",
		reqID, steadyEvents, totalEvents, warmup.total, steadyDuration, eventsPerSec)

	straceBytes, err := os.ReadFile(stracePath)
	if err != nil {
		t.Fatalf("VAL-%s read %s: %v", reqID, stracePath, err)
	}
	if !bytes.Contains(straceBytes, []byte("writev")) {
		t.Fatalf("VAL-%s %s missing writev row. See %s/", reqID, stracePath, outDir)
	}
	writevCalls, err := parseStraceWritevCalls(straceBytes)
	if err != nil {
		t.Fatalf("VAL-%s parseStraceWritevCalls: %v. See %s/", reqID, err, outDir)
	}
	writevPerSec := float64(writevCalls) / straceWindow.Seconds()
	if writevPerSec <= 0 {
		t.Fatalf("VAL-%s writev/s computed as %.2f (calls=%d, window=%s). See %s/",
			reqID, writevPerSec, writevCalls, straceWindow, outDir)
	}

	eventsPerWritev := eventsPerSec / writevPerSec

	t.Logf("VAL-%s measured: events_sent=%.0f/s (floor %.0f); writev=%.0f/s; events_per_writev=%.2f (floor %.2f)",
		reqID, eventsPerSec, block.EventsSentMinPerS, writevPerSec, eventsPerWritev, block.EventsPerWritevMin)

	if eventsPerSec < block.EventsSentMinPerS {
		t.Fatalf("VAL-%s events_sent_min_per_s %.0f not met: measured %.0f/s. See %s/",
			reqID, block.EventsSentMinPerS, eventsPerSec, outDir)
	}
	if eventsPerWritev < block.EventsPerWritevMin {
		t.Fatalf("VAL-%s events_per_writev_min %.2f not met: measured %.2f. See %s/",
			reqID, block.EventsPerWritevMin, eventsPerWritev, outDir)
	}

	t.Logf("VAL-%s PASS: events_sent=%.0f/s >= %.0f; events_per_writev=%.2f >= %.2f. Artifacts: %s/",
		reqID, eventsPerSec, block.EventsSentMinPerS, eventsPerWritev, block.EventsPerWritevMin, outDir)
}

func runStraceSample(t *testing.T, reqID, outPath string) error {
	t.Helper()
	t.Logf("VAL-%s: capturing %s strace sample → %s", reqID, straceWindow, outPath)

	straceShell := fmt.Sprintf(
		`apk add --no-cache strace >/dev/null 2>&1 && `+
			`WALERA_PID=$(pgrep -f /cdc-sse | head -1) && `+
			`strace -c -e trace=write,writev -p "$WALERA_PID" 2>&1 & `+
			`STRACE_PID=$!; sleep %d; kill -INT "$STRACE_PID"; wait "$STRACE_PID"`,
		int(straceWindow.Seconds()),
	)

	out, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", outPath, err)
	}
	defer out.Close()

	cmd := exec.Command("docker", "run", "--rm",
		"--pid=container:walera-app",
		"--cap-add", "SYS_PTRACE",
		"alpine",
		"sh", "-c", straceShell,
	)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("strace sidecar: %w", err)
	}
	return nil
}

type warmupSnapshot struct {
	total float64
	err   error
}

func captureWarmupSnapshot(t *testing.T, reqID, outPath string) warmupSnapshot {
	t.Helper()
	t.Logf("VAL-%s: warmup-baseline scrape (T+warmup) → %s", reqID, outPath)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(metricsURL)
	if err != nil {
		return warmupSnapshot{err: fmt.Errorf("scrape %s: %w", metricsURL, err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return warmupSnapshot{err: fmt.Errorf("scrape %s: status=%d", metricsURL, resp.StatusCode)}
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return warmupSnapshot{err: fmt.Errorf("read %s: %w", metricsURL, err)}
	}
	if err := os.WriteFile(outPath, b, 0o644); err != nil {
		return warmupSnapshot{err: fmt.Errorf("write %s: %w", outPath, err)}
	}
	total, err := parseEventsSentTotal(b)
	if err != nil {
		return warmupSnapshot{err: fmt.Errorf("parseEventsSentTotal: %w", err)}
	}
	return warmupSnapshot{total: total}
}

func streamLog(t *testing.T, tag string, r io.ReadCloser, wg *sync.WaitGroup) {
	defer wg.Done()
	defer r.Close()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		t.Logf("%s %s", tag, sc.Text())
	}
}

func mustNonEmpty(t *testing.T, reqID, path, outDir string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("VAL-%s missing artifact: %s (%v). See %s/", reqID, path, err, outDir)
	}
	if info.Size() == 0 {
		t.Fatalf("VAL-%s empty artifact: %s. See %s/", reqID, path, outDir)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, here, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller(0) failed")
	}
	dir := filepath.Dir(here)
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("repoRoot: no go.mod found walking up from %s", here)
	return ""
}

func loadThresholds(t *testing.T) Thresholds {
	t.Helper()
	root := repoRoot(t)
	path := filepath.Join(root, "test", "perf", "thresholds.yml")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("loadThresholds: read %s: %v", path, err)
	}
	var th Thresholds
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&th); err != nil {
		t.Fatalf("loadThresholds: yaml decode %s: %v", path, err)
	}
	if th.SchemaVersion != expectedSchemaVersion {
		t.Fatalf("loadThresholds: schema_version=%d, expected %d (test predates this thresholds.yml rev)",
			th.SchemaVersion, expectedSchemaVersion)
	}
	return th
}

func parseEventsSentTotal(b []byte) (float64, error) {
	const metric = "walera_events_sent_total"

	const typeLabel = `type="wildcard"`
	var total float64
	var matched int
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, metric) {
			continue
		}

		if strings.HasPrefix(line, "#") {
			continue
		}

		if !strings.Contains(line, typeLabel) {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			return 0, fmt.Errorf("parse value on line %q: %w", line, err)
		}
		total += v
		matched++
	}
	if err := sc.Err(); err != nil {
		return 0, fmt.Errorf("scan walera-metrics: %w", err)
	}
	if matched == 0 {
		return 0, fmt.Errorf("no %s{type=%q,...} lines found in walera-metrics", metric, "wildcard")
	}
	return total, nil
}

func parseStraceWritevCalls(b []byte) (int64, error) {
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}

		syscall := fields[len(fields)-1]
		if syscall != "writev" {
			continue
		}

		callsStr := fields[3]
		n, err := strconv.ParseInt(callsStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse writev calls %q: %w", callsStr, err)
		}
		return n, nil
	}
	if err := sc.Err(); err != nil {
		return 0, fmt.Errorf("scan strace sample: %w", err)
	}
	return 0, fmt.Errorf("no writev row found in strace sample")
}

func waitHealthz(t *testing.T, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(healthzURL)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			lastErr = fmt.Errorf("status=%d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(healthzPoll)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("timeout")
	}
	return lastErr
}

func loadAuthToken(envPath string) (string, error) {

	if v := os.Getenv("LOADGEN_AUTH_TOKEN"); v != "" {
		return v, nil
	}
	f, err := os.Open(envPath)
	if err != nil {
		return "", fmt.Errorf("open %s: %w (and LOADGEN_AUTH_TOKEN not in env)", envPath, err)
	}
	defer f.Close()
	var loadgenTok, mockTok string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if eq := strings.IndexByte(line, '='); eq > 0 {
			k := strings.TrimSpace(line[:eq])
			v := strings.TrimSpace(line[eq+1:])

			v = strings.Trim(v, `"'`)
			switch k {
			case "LOADGEN_AUTH_TOKEN":
				loadgenTok = v
			case "MOCK_AUTH_TOKEN":
				mockTok = v
			}
		}
	}
	if err := sc.Err(); err != nil {
		return "", fmt.Errorf("scan %s: %w", envPath, err)
	}
	if loadgenTok != "" {
		return loadgenTok, nil
	}
	if mockTok != "" {
		return mockTok, nil
	}
	return "", fmt.Errorf("LOADGEN_AUTH_TOKEN and MOCK_AUTH_TOKEN both unset in %s", envPath)
}
