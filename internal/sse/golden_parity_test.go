// Package sse — golden_parity_test.go locks the SSE wire format byte-for
// -byte against scripts/golden/sse_v13_handshake.txt.
// Two variants:
//   - TestGoldenParity_HijackPath drives the synthetic tx through the
//     production encoder + pool with conn != nil, using a real loopback
//     TCP pair (net.Listen / net.Dial). This exercises the
//     (*net.Buffers).WriteTo writev(2) syscall path that will
//     re-enable in the handler. Tests today via direct pool.Attach so
//     the hijack-disabled 16-03 handler quirk does not block coverage.
//   - TestGoldenParity_RespWriterPath drives the same synthetic tx
//     through the respWriter+rc fallback (conn == nil), which is the
//     production code path for TLS / h2c clients AND for every
//     subscriber in 16-03 (where hijackTCPConn returns (nil, nil)).
//
// Both variants MUST produce byte-identical output. The fixture is the
// contract — any encoder or pool change that drifts the wire trips this
// test. Regenerate the fixture via scripts/golden-capture.sh ONLY when
// the wire is intentionally changing.
package sse

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// loadGoldenFixture reads scripts/golden/sse_v13_handshake.txt from the
// repo root. Returns the bytes verbatim; on missing/unreadable file the
// test FAILs with an instruction to regenerate.
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

// fixtureRepoRoot walks up from the test working dir (internal/sse) to
// find the directory containing go.mod.
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

// assertGoldenEqual compares got to the fixture bytes; on mismatch prints
// the first 240 bytes of each side + a marker showing the first divergent
// offset, so operators can see whether to regenerate (intentional change)
// or fix the regression.
func assertGoldenEqual(t *testing.T, got []byte) {
	t.Helper()
	want := loadGoldenFixture(t)
	if bytes.Equal(got, want) {
		return
	}
	// Find first divergent byte for a focused diff.
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

// TestGoldenParity_RespWriterPath asserts wire bytes through the
// respWriter+rc fallback path are byte-identical to the golden fixture.
// This is the production code path for TLS / h2c clients and for every
// 16-03 subscriber (hijack disabled).
func TestGoldenParity_RespWriterPath(t *testing.T) {
	t.Parallel()
	rw, err := runSyntheticTxThroughPoolViaRespWriter(zerolog.Nop())
	if err != nil {
		t.Fatalf("runSyntheticTxThroughPoolViaRespWriter: %v", err)
	}
	assertGoldenEqual(t, rw.snapshot())
}

// TestGoldenParity_HijackPath asserts wire bytes through the hijacked
// *net.TCPConn path (where pool.drainSub uses (*net.Buffers).WriteTo
// for the writev(2) syscall) are byte-identical to the golden fixture.
// Uses a real loopback TCP listener so the conn really is a *net.TCPConn,
// not a synthetic substitute. Calls pool.Attach directly so the
// 16-03 handler's hijack-disabled quirk does not block coverage —
// when re-enables hijack in the handler, the same bytes must
// still emerge from this code path, and this test continues to gate that.
func TestGoldenParity_HijackPath(t *testing.T) {
	t.Parallel()
	got, err := runSyntheticTxThroughPoolViaTCPConn(zerolog.Nop())
	if err != nil {
		t.Fatalf("runSyntheticTxThroughPoolViaTCPConn: %v", err)
	}
	assertGoldenEqual(t, got)
}
