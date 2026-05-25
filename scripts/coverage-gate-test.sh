#!/usr/bin/env bash
# scripts/coverage-gate-test.sh — fixture-based tests for scripts/coverage-gate.sh.
#
# Exercises three scenarios:
#   1. coverage-pass.log    — every internal/... package >= 85.0% → gate exits 0.
#   2. coverage-fail.log    — auth at 84.1%, sse at 70.5%        → gate exits 1
#                              and stderr lists both failing packages.
#   3. coverage-no-tests.log — packages with `[no test files]`   → gate exits 0,
#                              surfaces a WARN line, does NOT count as failure.
#
# Run from the repo root:
#   bash scripts/coverage-gate-test.sh
#
# This script is intentionally dependency-free: pure bash + POSIX awk + grep.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GATE="$SCRIPT_DIR/coverage-gate.sh"
FIX="$SCRIPT_DIR/testdata"

fail() { echo "TEST FAIL: $*" >&2; exit 1; }
pass() { echo "TEST PASS: $*"; }

[ -x "$GATE" ] || fail "gate script not executable: $GATE"

# --- Test 1: passing fixture exits 0 ---
out=$("$GATE" "$FIX/coverage-pass.log" 2>&1) && rc=0 || rc=$?
[ "$rc" -eq 0 ] || fail "expected exit 0 on coverage-pass.log, got $rc"
echo "$out" | grep -q "PASS: github.com/walera/walera/internal/auth" \
  || fail "expected PASS line for internal/auth in:\n$out"
echo "$out" | grep -q "PASS: github.com/walera/walera/internal/sse" \
  || fail "expected PASS line for internal/sse in:\n$out"
pass "coverage-pass.log → exit 0 with per-package PASS lines"

# --- Test 2: failing fixture exits 1 and lists both bad packages ---
out=$("$GATE" "$FIX/coverage-fail.log" 2>&1) && rc=0 || rc=$?
[ "$rc" -eq 1 ] || fail "expected exit 1 on coverage-fail.log, got $rc"
echo "$out" | grep -q "FAIL: github.com/walera/walera/internal/auth 84.1% < 85.0%" \
  || fail "expected FAIL line for internal/auth 84.1% in:\n$out"
echo "$out" | grep -q "FAIL: github.com/walera/walera/internal/sse 70.5% < 85.0%" \
  || fail "expected FAIL line for internal/sse 70.5% in:\n$out"
pass "coverage-fail.log → exit 1 listing internal/auth and internal/sse below 85.0%"

# --- Test 3: [no test files] packages produce WARN, not failure ---
out=$("$GATE" "$FIX/coverage-no-tests.log" 2>&1) && rc=0 || rc=$?
[ "$rc" -eq 0 ] || fail "expected exit 0 on coverage-no-tests.log, got $rc"
echo "$out" | grep -q "WARN: github.com/walera/walera/internal/empty" \
  || fail "expected WARN line for internal/empty in:\n$out"
pass "coverage-no-tests.log → exit 0, internal/empty surfaced as WARN"

# --- Test 4: threshold override via env var ---
out=$(COVERAGE_THRESHOLD=99.9 "$GATE" "$FIX/coverage-pass.log" 2>&1) && rc=0 || rc=$?
[ "$rc" -eq 1 ] || fail "expected exit 1 with COVERAGE_THRESHOLD=99.9, got $rc"
echo "$out" | grep -q "< 99.9%" \
  || fail "expected '< 99.9%' in failure output (threshold override) in:\n$out"
pass "COVERAGE_THRESHOLD=99.9 env var override → exit 1"

# --- Test 5: total: line from `go tool cover -func` is ignored ---
# If someone pipes `go tool cover -func` output (which ends with `total:`), the
# gate must not crash on it. Construct a hybrid fixture inline.
HYBRID=$(mktemp)
trap 'rm -f "$HYBRID"' EXIT
cat > "$HYBRID" <<'EOF'
ok  	github.com/walera/walera/internal/auth	0.123s	coverage: 90.0% of statements
total:	(statements)	88.7%
EOF
out=$("$GATE" "$HYBRID" 2>&1) && rc=0 || rc=$?
[ "$rc" -eq 0 ] || fail "expected exit 0 with trailing total: line, got $rc\n$out"
echo "$out" | grep -qv "FAIL.*total" \
  || fail "trailing total: line should be ignored, not parsed as a package"
pass "trailing total: line is ignored, not treated as a package"

echo
echo "All gate tests passed."
