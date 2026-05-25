//go:build golden_capture

// Package sse — golden_capture_test.go regenerates the checked-in
// SSE wire-parity fixture at scripts/golden/sse_v13_handshake.txt.
// Build-tagged with `golden_capture` so it is INVISIBLE to the normal
// `go test./...` run. The fixture is the CONTRACT; the parity test
// (golden_parity_test.go) is a binary diff against the file on every
// `go test./...` invocation. This generator is run ONLY when the wire
// intentionally changes, via `scripts/golden-capture.sh`.
// The capture must be deterministic: the synthetic tx uses fixed PKs,
// fixed timestamps, fixed commit_lsn, and a full-access (no-filter)
// subscriber. No goroutines, no clocks read by the production code path
// (the encoder uses tx.CommitTS verbatim; the pool's connectedAt is
// unused in the wire bytes).
package sse

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// TestGenerateGoldenFixture writes the deterministic golden fixture to
// `scripts/golden/sse_v13_handshake.txt.new` in the repository root.
// `scripts/golden-capture.sh` moves the `.new` file over the canonical
// file when content changes.
func TestGenerateGoldenFixture(t *testing.T) {
	got := captureGoldenBytes(t)

	// Locate the repo root by walking up from this test file's package
	// directory (internal/sse) until we hit a directory containing
	// `go.mod`. This keeps the script-runs-from-anywhere contract.
	root := repoRoot(t)
	outDir := filepath.Join(root, "scripts", "golden")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", outDir, err)
	}
	outPath := filepath.Join(outDir, "sse_v13_handshake.txt.new")
	if err := os.WriteFile(outPath, got, 0o644); err != nil {
		t.Fatalf("WriteFile(%q): %v", outPath, err)
	}
	t.Logf("wrote %d bytes to %s", len(got), outPath)
}

// repoRoot returns the absolute path of the directory containing go.mod,
// starting from the current working directory (which `go test` sets to
// the package directory).
func repoRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("repoRoot: walked past filesystem root from %q without finding go.mod", cwd)
		}
		dir = parent
	}
}

// captureGoldenBytes runs the deterministic synthetic tx through the
// production encoder + pool path and returns the recorded wire bytes:
//
//	retry: 15000\n\n (prelude, emitted by pool.Attach)
//	event: tx\n... (one tx frame: insert+update+delete)
//	:\n\n (one heartbeat frame)
//	event: shutdown\n... (shutdown frame, emitted by pool.Shutdown)
//
// The shape mirrors what a v1.3 client would have observed on the wire:
// a handshake (prelude), an event (the tx), a keep-alive (heartbeat),
// and a graceful close (shutdown).
// This is also the shared helper consumed by golden_parity_test.go's two
// variants (TestGoldenParity_HijackPath / TestGoldenParity_RespWriterPath).
// Keeping the producer in this build-tagged file would leave the parity
// test unable to compile; we therefore move the shared helper out to a
// non-tagged file (golden_fixture_helper_test.go) but the entry-point
// for *regenerating* the fixture remains here.
func captureGoldenBytes(t *testing.T) []byte {
	t.Helper()
	rw, err := runSyntheticTxThroughPoolViaRespWriter(zerolog.Nop())
	if err != nil {
		t.Fatalf("runSyntheticTxThroughPoolViaRespWriter: %v", err)
	}
	return rw.snapshot()
}
