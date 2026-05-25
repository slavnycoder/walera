//go:build golden_capture

package sse

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

func TestGenerateGoldenFixture(t *testing.T) {
	got := captureGoldenBytes(t)

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

func captureGoldenBytes(t *testing.T) []byte {
	t.Helper()
	rw, err := runSyntheticTxThroughPoolViaRespWriter(zerolog.Nop())
	if err != nil {
		t.Fatalf("runSyntheticTxThroughPoolViaRespWriter: %v", err)
	}
	return rw.snapshot()
}
