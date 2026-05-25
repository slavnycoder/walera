//go:build perf_gate
// +build perf_gate

// Package perf — GATE-01: build-tagged perf-gate test that locks the
// ratio-based VAL-02 + VAL-03 thresholds (events_sent_min_per_s +
// events_per_writev_min) into a re-runnable check. Brings the testbench
// compose stack up, drives scripts/bench.sh at the spec-mandated rates for a
// 90 s window, captures a 30 s strace sample mid-run via the alpine sidecar
// pattern, computes the ratio + throughput, asserts against
// test/perf/thresholds.yml. ALWAYS tears the compose stack down via
// t.Cleanup, even on failure.
//
// Two top-level Test functions live here so each is independently filterable
// via `go test -run TestPerfGateV1k$` or `go test -run TestPerfGateV5k$`:
//
//  1. TestPerfGateV1k — 1k steady (500 cr × 5 rt) → asserts val_02 floors.
//  2. TestPerfGateV5k — 5k stress (2000 cr × 5 rt) → asserts val_03 floors.
//
// Both share neither state nor binaries; each owns its full compose lifecycle.
// They run sequentially (no t.Parallel) — the testbench compose stack has
// fixed host ports (127.0.0.1:8080, :5432) so parallel runs would conflict.
//
// Failure messages name the threshold key + measured value + artifact path:
//
//	VAL-02 events_per_writev_min 2.00 not met: measured 1.95. See bench-out/perf-gate-1k-<ts>/
//
// The artifact path on stdout is essential for the CI workflow's
// upload-artifact step (plan 20-02): the operator clicks the artifact link
// in the failed PR check and gets the full walera-metrics.txt + strace
// sample for post-mortem.
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

// Thresholds is the typed mirror of test/perf/thresholds.yml. The schema is
// versioned via SchemaVersion; the gate test refuses to run if the YAML's
// schema_version doesn't match the expected value (defense in depth — a
// future schema rev that silently dropped the EventsPerWritevMin field would
// otherwise pass with a zero floor).
type Thresholds struct {
	SchemaVersion  int          `yaml:"schema_version"`
	Captured       string       `yaml:"captured"`
	CapturedCommit string       `yaml:"captured_commit"`
	BaselineStrace BaselineRefs `yaml:"baseline_strace"`
	Val02          ValBlock     `yaml:"val_02"`
	Val03          ValBlock     `yaml:"val_03"`
}

// BaselineRefs holds the on-disk paths to the captured strace baseline
// artifacts referenced by the rebaseline workflow (scripts/perf-gate-regen.sh
// emits these). The fields are purely metadata for audit/PR-diff trails; the
// gate test does not read them. Declared as a struct (not omitted via
// KnownFields(false)) so the strict-decode invariant remains intact and any
// future field rename surfaces as a yaml unmarshal error.
type BaselineRefs struct {
	Source1k string `yaml:"source_1k"`
	Source5k string `yaml:"source_5k"`
}

// ValBlock encodes the per-scenario floors. Floors are non-pointer floats so
// yaml.v3 returns an error on type mismatch (rather than silently zeroing
// the field — which would make `eventsPerSec >= 0` trivially pass).
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
	// composeFile is relative to repo root; every subprocess sets cmd.Dir = repoRoot().
	composeFile = "testbench/docker-compose.yml"
	// healthzURL is the walera-app health probe (host-published on 127.0.0.1:8080).
	healthzURL = "http://127.0.0.1:8080/healthz"
	// metricsURL is the walera-app Prometheus scrape endpoint; the gate hits
	// this directly at T+warmup_discard_seconds to snapshot the counter
	// baseline so the events/s rate is computed over the steady-state window
	// only (previously divided cumulative-from-T0 by full duration, mixing
	// warmup ramp-up into the apples-to-apples comparison against the
	// captured steady-state baseline).
	metricsURL = "http://127.0.0.1:8080/metrics"
	// pgDSN — host-side bench connects to the compose-published PG port.
	pgDSN = "postgres://walera:walera@127.0.0.1:5432/walera?sslmode=disable"
	// straceOffset — when to start the strace sample inside the bench window
	// (post-warmup, mid-run); 45 s into a 90 s window gives a stable sample.
	straceOffset = 45 * time.Second
	// straceWindow — duration of the strace -c summary capture. Must be > 0
	// and < (block.DurationSeconds - straceOffset.Seconds()) so the sample
	// completes before the bench tears down.
	straceWindow = 30 * time.Second
	// healthzTimeout — upper bound for /healthz reachability after compose up.
	healthzTimeout = 90 * time.Second
	// healthzPoll — interval between /healthz polls.
	healthzPoll = 2 * time.Second
	// expectedSchemaVersion — the schema rev this test understands.
	expectedSchemaVersion = 1
)

// TestPerfGateV1k locks VAL-02: 1000 subscribers, steady scenario, 500
// commits/sec × 5 rows/tx — asserts events_sent_min_per_s and
// events_per_writev_min from thresholds.yml::val_02.
func TestPerfGateV1k(t *testing.T) {
	th := loadThresholds(t)
	runGate(t, th.Val02, "1k", "02")
}

// TestPerfGateV5k locks VAL-03: 5000 subscribers, stress scenario, 2000
// commits/sec × 5 rows/tx — asserts events_sent_min_per_s and
// events_per_writev_min from thresholds.yml::val_03.
func TestPerfGateV5k(t *testing.T) {
	th := loadThresholds(t)
	runGate(t, th.Val03, "5k", "03")
}

// runGate is the shared lifecycle: compose-up → wait /healthz → launch
// bench.sh (background) → strace sample at T+45s → wait bench → parse
// artifacts → assert floors. t.Cleanup runs `docker compose down -v
// --remove-orphans` regardless of pass/fail/panic.
//
// label is "1k" or "5k" — used for artifact-dir naming.
// reqID is "02" or "03" — used in failure messages so the operator can map
// a failure back to REQUIREMENTS.md::VAL-{reqID}.
func runGate(t *testing.T, block ValBlock, label, reqID string) {
	t.Helper()

	// Defense in depth — yaml.v3 already errored on the missing schema_version
	// at loadThresholds, but a future schema rev could keep the key while
	// silently dropping float fields. Asserting > 0 catches that.
	if block.EventsSentMinPerS <= 0 || block.EventsPerWritevMin <= 0 {
		t.Fatalf("VAL-%s thresholds malformed: events_sent_min_per_s=%.0f events_per_writev_min=%.2f (both must be > 0)",
			reqID, block.EventsSentMinPerS, block.EventsPerWritevMin)
	}
	// Timing-budget invariants. The strace sample must start
	// AFTER warmup completes so the writev rate is measured in steady state,
	// and the warmup-discard region must fit inside the bench duration.
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

	// WR-02: scope the compose project so the gate's `down -v --remove-
	// orphans` cannot clobber a long-running developer testbench. Prefer
	// COMPOSE_PROJECT_NAME from env (CI sets walera-perf-gate-ci), else
	// derive a per-test-+-timestamp project so two consecutive local
	// invocations don't share state. The testbench/docker-compose.yml
	// pins container_name: walera-app so the strace sidecar's
	// `--pid=container:walera-app` resolves regardless of project name.
	project := os.Getenv("COMPOSE_PROJECT_NAME")
	if project == "" {
		// Hash t.Name()+ts → short suffix; keeps the project name
		// docker-compliant (max 64 chars, lowercase, no whitespace).
		sum := sha256.Sum256([]byte(t.Name() + "-" + ts))
		project = "walera-perf-gate-" + label + "-" + hex.EncodeToString(sum[:4])
	}
	t.Logf("VAL-%s: COMPOSE_PROJECT_NAME=%s", reqID, project)

	// Register the cleanup BEFORE the compose-up so a panic mid-up still
	// tears the stack down. The cleanup is `docker compose down -v
	// --remove-orphans` scoped to the project so any stray service from a
	// prior aborted gate run is also reaped — and crucially, an unrelated
	// dev testbench (different project name) is NOT touched.
	t.Cleanup(func() {
		cmd := exec.Command("docker", "compose", "--project-name", project, "-f", composeFile, "down", "-v", "--remove-orphans")
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Logf("compose down -v failed: %v\n%s", err, out)
		}
	})

	// --- Compose up (foreground; --build covers stale images) -----------
	t.Logf("VAL-%s: docker compose --project-name %s -f %s up -d --build", reqID, project, composeFile)
	upCmd := exec.Command("docker", "compose", "--project-name", project, "-f", composeFile, "up", "-d", "--build")
	upCmd.Dir = root
	if out, err := upCmd.CombinedOutput(); err != nil {
		t.Fatalf("VAL-%s compose up failed: %v\n%s", reqID, err, out)
	}

	// --- Wait for /healthz ----------------------------------------------
	if err := waitHealthz(t, healthzTimeout); err != nil {
		t.Fatalf("VAL-%s walera healthz not reachable within %s: %v. See %s/", reqID, healthzTimeout, err, outDir)
	}

	// --- Load LOADGEN_AUTH_TOKEN from testbench/.env --------------------
	token, err := loadAuthToken(filepath.Join(root, "testbench", ".env"))
	if err != nil {
		t.Fatalf("VAL-%s loadAuthToken: %v", reqID, err)
	}
	// SECURITY: never log the token value. Set via os.Setenv so the bench.sh
	// child inherits it; bench.sh reads it via env (not CLI flag) so it does
	// not appear in any `ps` output.
	if err := os.Setenv("LOADGEN_AUTH_TOKEN", token); err != nil {
		t.Fatalf("VAL-%s setenv LOADGEN_AUTH_TOKEN: %v", reqID, err)
	}

	// --- Build the bench.sh arg list -----------------------------------
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

	// Pipe stdout + stderr to t.Log so a bench failure surfaces in the test
	// output. Use a small goroutine + bufio.Scanner — captured live, not
	// buffered to end-of-process.
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

	// --- Schedule strace sample at T+straceOffset -----------------------
	stracePath := filepath.Join(absOut, "strace-sample-"+label+".txt")
	straceDone := make(chan error, 1)
	straceTimer := time.AfterFunc(straceOffset, func() {
		straceDone <- runStraceSample(t, reqID, stracePath)
	})
	// Ensure the timer is stopped on early exit (cleanup runs regardless).
	defer straceTimer.Stop()

	// --- Schedule warmup-baseline scrape at T+warmup_discard_seconds ----
	// Snapshot walera_events_sent_total at the end of the warmup region so
	// the final rate is computed over the steady-state window only. Without
	// this, the cumulative-from-T0 counter is divided by the full duration
	// including warmup ramp-up — apples-to-oranges against the captured
	// steady-state baseline this gate is calibrated against. The snapshot
	// is written next to the artifacts for the failure-postmortem path.
	warmupSnapshotPath := filepath.Join(absOut, "walera-metrics-warmup.txt")
	warmupDone := make(chan warmupSnapshot, 1)
	warmupTimer := time.AfterFunc(time.Duration(block.WarmupDiscardSeconds)*time.Second, func() {
		warmupDone <- captureWarmupSnapshot(t, reqID, warmupSnapshotPath)
	})
	defer warmupTimer.Stop()

	// --- Wait for bench.sh to exit, bounded by duration + 60s --------
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

	// Wait for the strace sample to land (must complete BEFORE we parse).
	// straceOffset (45s) + straceWindow (30s) = 75s, well inside the 90s
	// bench window — the AfterFunc has fired by now. Block on the channel
	// up to straceWindow + 10s slack.
	select {
	case err := <-straceDone:
		if err != nil {
			t.Fatalf("VAL-%s strace sample failed: %v. See %s/", reqID, err, outDir)
		}
	case <-time.After(straceWindow + 10*time.Second):
		t.Fatalf("VAL-%s strace sample did not complete within %s. See %s/", reqID, straceWindow+10*time.Second, outDir)
	}

	// --- Verify artifact triplet --------------------------------------
	walMetrics := filepath.Join(absOut, "walera-metrics.txt")
	loadMetrics := filepath.Join(absOut, "loadgen-metrics.txt")
	mustNonEmpty(t, reqID, walMetrics, outDir)
	mustNonEmpty(t, reqID, loadMetrics, outDir)
	mustNonEmpty(t, reqID, stracePath, outDir)

	// --- Parse events_sent rate (steady-state only) -------------------
	// Wait for the warmup-snapshot AfterFunc to complete. By the time
	// bench.sh exits (which we already awaited above), T >= duration
	// seconds >> warmup_discard_seconds, so the AfterFunc has fired —
	// this is a sanity drain, not a real wait.
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
	// Steady-state window: subtract the warmup baseline from the final
	// counter and divide by (duration - warmup) so the rate reflects only
	// the post-warmup region — apples-to-apples with the captured
	// steady-state thresholds.
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

	// --- Parse writev_per_s -------------------------------------------
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

	// --- Compute ratio + assert both floors ---------------------------
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

// runStraceSample runs the alpine strace sidecar against the
// walera-app PID namespace and writes the strace -c summary to outPath.
// Uses `pgrep -f /cdc-sse | head -1` (matching the baseline-capture form)
// rather than -p 1 for robustness — the cdc-sse binary IS PID 1 in
// walera-app's namespace by virtue of distroless having no init shim, but
// pgrep is the form that produced the threshold-capture artifacts so the
// gate test uses it for behavioural parity.
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

// warmupSnapshot is the result of the post-warmup metrics scrape. `total`
// is the parseEventsSentTotal value (same arithmetic as the final scrape)
// at T+warmup_discard_seconds. `err` is non-nil if either the HTTP scrape
// failed or the on-disk write of the snapshot blob failed — runGate
// surfaces it via t.Fatalf so a flaky scrape never silently rounds to a
// zero baseline (which would inflate the measured rate and mask
// regressions, the inverse of the silent-zero-baseline failure mode).
type warmupSnapshot struct {
	total float64
	err   error
}

// captureWarmupSnapshot fetches /metrics from walera-app and parses
// walera_events_sent_total to a float, writing the full Prometheus text
// to outPath for the failure-postmortem path. The fetch reuses the same
// strict 2 s client timeout pattern as waitHealthz — a stalled scrape
// here should error fast, not block the bench window.
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

// streamLog drains a pipe line-by-line into t.Log with a tag prefix.
func streamLog(t *testing.T, tag string, r io.ReadCloser, wg *sync.WaitGroup) {
	defer wg.Done()
	defer r.Close()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		t.Logf("%s %s", tag, sc.Text())
	}
}

// mustNonEmpty fails the test if path does not exist or is zero-byte. Named
// in the failure path for operator clarity.
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

// repoRoot walks up from the test file's directory until it finds a sibling
// go.mod, then returns that directory. Used to set cmd.Dir for every
// subprocess. Panics via t.Fatalf if no go.mod is found within 8 levels.
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
	return "" // unreachable
}

// loadThresholds reads test/perf/thresholds.yml from repoRoot, unmarshals
// via yaml.v3, and validates the schema_version. Returns the typed struct.
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
	dec.KnownFields(true) // reject unknown keys — schema drift fails loudly
	if err := dec.Decode(&th); err != nil {
		t.Fatalf("loadThresholds: yaml decode %s: %v", path, err)
	}
	if th.SchemaVersion != expectedSchemaVersion {
		t.Fatalf("loadThresholds: schema_version=%d, expected %d (test predates this thresholds.yml rev)",
			th.SchemaVersion, expectedSchemaVersion)
	}
	return th
}

// parseEventsSentTotal sums the value column of every
// `walera_events_sent_total{type="wildcard",...} <value>` line in
// Prometheus text format. Filters to `type="wildcard"` because:
//
//  1. The bench subscribes to wildcard channels (orders/all, devices/all,
//     articles/all) — the captured thresholds were measured on this label
//     variant. Summing all label variants pollutes the gate with any
//     unrelated `type="exact"` increments from concurrent traffic.
//
//  2. Heartbeat frames also increment this counter on the same label as
//     the subscription kind they're emitted for (registry.go: counter is
//     incremented in pool.drainSubDeadline for every frame including the
//     `:` comment heartbeat). Filtering to wildcard keeps the measurement
//     apples-to-apples with the captured baseline — both baseline and gate
//     count wildcard frames including the wildcard-sub heartbeat cadence.
//
// Skips HELP/TYPE comment lines and any line not starting with the metric
// name. Returns the cumulative counter total (NOT a rate — the caller
// divides by the steady-state duration).
//
// NOTE: The floor values in test/perf/thresholds.yml are defined on this
// same wildcard-filtered counter. A future change to heartbeat cadence
// would still move both baseline and measured value in lockstep — the
// gate would not false-alarm on a heartbeat-cadence-only change.
func parseEventsSentTotal(b []byte) (float64, error) {
	const metric = "walera_events_sent_total"
	// Match `type="wildcard"` substring in the label block. Prom-text
	// labels are unordered, so a substring check is the simplest robust
	// form — full label parsing is unwarranted for one label.
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
		// Skip HELP/TYPE lines (they begin with `# HELP <metric>` or
		// `# TYPE <metric>`, NOT the metric name itself, so HasPrefix
		// above already filters them — but defense in depth).
		if strings.HasPrefix(line, "#") {
			continue
		}
		// WR-03: filter to type="wildcard" label only. A line without
		// the label block (no `{` — bare metric name) is unlabelled
		// and not what walera emits; skip it defensively.
		if !strings.Contains(line, typeLabel) {
			continue
		}
		// Format: walera_events_sent_total{type="wildcard",...} <value>
		// Split on whitespace; last field is the value.
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

// parseStraceWritevCalls scans a `strace -c` summary table and returns the
// `calls` column of the writev row. The strace -c table has columns:
//
//	% time | seconds | usecs/call | calls | [errors] | syscall
//
// Some strace builds emit an `errors` column only when errors > 0, so the
// `calls` column is at index 3 (4th field) when the row has 5 fields and
// index 3 again when the row has 6 fields (calls is always before the
// final syscall name). We find the row by its trailing "writev" token and
// then walk back from the end: syscall name is last; calls is the field
// before any non-numeric trailing field.
//
// To keep the parser simple AND robust, we locate the writev row, then
// take the last numeric field BEFORE the syscall name. That handles both
// 5-column (no errors) and 6-column (with errors) layouts.
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
		// Last field is the syscall name on data rows; on the totals
		// row it is also "total" — skip that.
		syscall := fields[len(fields)-1]
		if syscall != "writev" {
			continue
		}
		// Walk back from len-2 (just before syscall name) finding the
		// first all-integer field — that is the `calls` column. (The
		// `errors` column, when present, is also integer; but it sits
		// AFTER `calls` in the strace layout, so the rightmost integer
		// before the syscall name is `errors` when present, and we
		// want `calls` which is one further left. Strategy: take the
		// `calls` field as the 4th column from the LEFT — that is its
		// position in every strace -c layout we've seen, regardless of
		// whether the errors column is present.)
		//
		// % time | seconds | usecs/call | calls | [errors] | syscall
		//   0    |   1     |     2      |   3   |    4     |   5
		//
		// Index 3 is `calls` in both 5-field and 6-field layouts.
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

// waitHealthz polls healthzURL every healthzPoll until status 200 or
// timeout. The test fails with a clear message on timeout.
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

// loadAuthToken reads testbench/.env (KEY=VALUE format) and returns the
// LOADGEN_AUTH_TOKEN value if present, otherwise MOCK_AUTH_TOKEN. The
// value is never logged (CLAUDE.md security invariant: never log tokens).
// Returns an error if neither variable is set.
func loadAuthToken(envPath string) (string, error) {
	// Allow caller env to override (CI may inject LOADGEN_AUTH_TOKEN
	// directly without a .env file).
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
			// Strip surrounding quotes if present.
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
