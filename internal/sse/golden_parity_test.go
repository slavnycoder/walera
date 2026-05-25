package sse

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

func loadGoldenFixture(t *testing.T) []byte {
	t.Helper()
	root := fixtureRepoRoot(t)
	path := filepath.Join(root, "scripts", "golden", "sse_v13_handshake.txt")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden fixture %q: %v\n(regenerate via: bash scripts/golden-capture.sh)", path, err)
	}
	if len(b) == 0 {
		t.Fatalf("golden fixture %q is empty\n(regenerate via: bash scripts/golden-capture.sh)", path)
	}
	return b
}

func fixtureRepoRoot(t *testing.T) string {
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

func assertGoldenEqual(t *testing.T, got []byte) {
	t.Helper()
	want := loadGoldenFixture(t)
	if bytes.Equal(got, want) {
		return
	}

	div := len(want)
	if len(got) < div {
		div = len(got)
	}
	for i := 0; i < div; i++ {
		if got[i] != want[i] {
			div = i
			break
		}
	}
	t.Fatalf("wire mismatch at byte offset %d (len got=%d, len want=%d)\n--- want (first 240 bytes):\n%q\n--- got (first 240 bytes):\n%q",
		div, len(got), len(want),
		truncate(want, 240),
		truncate(got, 240),
	)
}

func truncate(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[:n]
}

func TestGoldenParity_RespWriterPath(t *testing.T) {
	t.Parallel()
	rw, err := runSyntheticTxThroughPoolViaRespWriter(zerolog.Nop())
	if err != nil {
		t.Fatalf("runSyntheticTxThroughPoolViaRespWriter: %v", err)
	}
	assertGoldenEqual(t, rw.snapshot())
}

func TestGoldenParity_HijackPath(t *testing.T) {
	t.Parallel()
	got, err := runSyntheticTxThroughPoolViaTCPConn(zerolog.Nop())
	if err != nil {
		t.Fatalf("runSyntheticTxThroughPoolViaTCPConn: %v", err)
	}
	assertGoldenEqual(t, got)
}
