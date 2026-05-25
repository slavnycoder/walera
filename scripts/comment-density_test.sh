#!/usr/bin/env bash
# =============================================================================
# scripts/comment-density_test.sh
#
# Shell-level unit test for scripts/comment-density.sh. Asserts the
# script's exit codes, ratio math, --max-pct, --json, --all, and the
# *_test.go exclusion against synthetic fixtures.
#
# Cases:
#   1. empty fixture package         -> exit 0, "comment-density: OK"
#   2. low-density (under ceiling)   -> exit 0, per-file table present
#   3. high-density (over ceiling)   -> exit 1, ::error annotation
#   4. --max-pct raises ceiling      -> 40% fixture passes at --max-pct 50
#   5. --json emits valid object     -> JSON shape with required keys
#   6. missing directory             -> exit 2 on stderr
#   7. *_test.go excluded            -> ratio computed only over non-test
#   8. --all on multi-pkg sandbox    -> per-package + overall summary, exit 1
#
# All synthetic fixtures live in a mktemp -d sandbox; nothing touches
# the repo. Fixture comment ratios are composed at runtime via printf
# so the test source itself stays clean.
#
# Exit codes:
#   0 — all cases pass.
#   1 — at least one case failed (descriptive error printed).
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$SCRIPT_DIR/comment-density.sh"

if [ ! -f "$SCRIPT" ]; then
  echo "test: error: comment-density.sh not found at $SCRIPT" >&2
  exit 1
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

pass_count=0
fail_count=0

assert_exit() {
  local got=$1
  local want=$2
  local label=$3
  if [ "$got" = "$want" ]; then
    pass_count=$((pass_count + 1))
    echo "PASS: $label (exit=$got)"
  else
    fail_count=$((fail_count + 1))
    echo "FAIL: $label — got exit=$got want=$want" >&2
  fi
}

assert_contains() {
  local haystack=$1
  local needle=$2
  local label=$3
  if printf '%s' "$haystack" | grep -qF "$needle"; then
    pass_count=$((pass_count + 1))
    echo "PASS: $label (contains $needle)"
  else
    fail_count=$((fail_count + 1))
    echo "FAIL: $label — output did not contain '$needle'" >&2
    echo "------ captured output ------" >&2
    printf '%s\n' "$haystack" >&2
    echo "-----------------------------" >&2
  fi
}

# Helper: write a Go file with N comment lines and M code lines.
write_go_file() {
  local path=$1
  local n_comments=$2
  local n_code=$3
  local pkg=$4
  {
    echo "package $pkg"
    echo ""
    local i
    for i in $(seq 1 "$n_comments"); do
      echo "// comment line $i"
    done
    for i in $(seq 1 "$n_code"); do
      echo "var V$i = $i"
    done
  } > "$path"
}

# ---- Case 1: empty fixture package (zero .go files) ----
case1="$tmpdir/case1"
mkdir -p "$case1"
# Add a marker file to make the dir non-empty but not a .go file.
echo "marker" > "$case1/README.md"

set +e
out1=$(bash "$SCRIPT" "$case1" 2>&1)
ec1=$?
set -e
assert_exit "$ec1" "0" "case1 (empty package — no go files)"
assert_contains "$out1" "comment-density: OK" "case1 (OK summary)"
assert_contains "$out1" "0.00%" "case1 (zero ratio)"

# ---- Case 2: low-density (under ceiling) ----
case2="$tmpdir/case2"
mkdir -p "$case2"
# 2 comment lines, 18 code lines => 10% ratio.
write_go_file "$case2/low.go" 2 18 x

set +e
out2=$(bash "$SCRIPT" "$case2" 2>&1)
ec2=$?
set -e
assert_exit "$ec2" "0" "case2 (low density, default 30%)"
assert_contains "$out2" "low.go" "case2 (per-file row present)"
assert_contains "$out2" "OK" "case2 (per-pkg OK)"

# ---- Case 3: high-density (over ceiling) ----
case3="$tmpdir/case3"
mkdir -p "$case3"
# 4 comment lines, 1 code line => 80% ratio.
write_go_file "$case3/high.go" 4 1 x

set +e
out3=$(bash "$SCRIPT" "$case3" 2>&1)
ec3=$?
set -e
assert_exit "$ec3" "1" "case3 (high density — over 30%)"
assert_contains "$out3" "::error file=" "case3 (annotation prefix)"
assert_contains "$out3" "Package comment density above ceiling" "case3 (annotation title)"
assert_contains "$out3" "FAIL" "case3 (FAIL summary)"

# ---- Case 4: --max-pct raises ceiling ----
case4="$tmpdir/case4"
mkdir -p "$case4"
# 2 comment lines, 3 code lines => 40% ratio (fails at 30, passes at 50).
write_go_file "$case4/mid.go" 2 3 x

set +e
out4a=$(bash "$SCRIPT" "$case4" 2>&1)
ec4a=$?
set -e
assert_exit "$ec4a" "1" "case4a (default 30%, 40% fixture fails)"

set +e
out4b=$(bash "$SCRIPT" --max-pct 50 "$case4" 2>&1)
ec4b=$?
set -e
assert_exit "$ec4b" "0" "case4b (--max-pct 50, 40% fixture passes)"
assert_contains "$out4b" "max=50" "case4b (ceiling reflected in output)"

# ---- Case 5: --json emits valid JSON ----
case5="$tmpdir/case5"
mkdir -p "$case5"
write_go_file "$case5/j.go" 2 8 x  # 20% — passes

set +e
out5=$(bash "$SCRIPT" --json "$case5" 2>&1)
ec5=$?
set -e
assert_exit "$ec5" "0" "case5 (json under ceiling)"
assert_contains "$out5" '"package":"' "case5 (package key present)"
assert_contains "$out5" '"ratio_pct":' "case5 (ratio_pct key present)"
assert_contains "$out5" '"files":[' "case5 (files array)"
assert_contains "$out5" '"ceiling_pct":30' "case5 (ceiling_pct present)"
assert_contains "$out5" '"status":"ok"' "case5 (status ok)"

# ---- Case 6: missing directory ----
case6_missing="$tmpdir/does-not-exist"

set +e
out6=$(bash "$SCRIPT" "$case6_missing" 2>&1)
ec6=$?
set -e
assert_exit "$ec6" "2" "case6 (missing directory)"
assert_contains "$out6" "is not a directory" "case6 (error message)"

# ---- Case 7: *_test.go excluded from ratio ----
case7="$tmpdir/case7"
mkdir -p "$case7"
# Production file: 1 comment, 9 code => 10% ratio.
write_go_file "$case7/prod.go" 1 9 x
# Test file: 99 comments, 1 code — if counted, would dominate the ratio.
write_go_file "$case7/prod_test.go" 99 1 x

set +e
out7=$(bash "$SCRIPT" "$case7" 2>&1)
ec7=$?
set -e
assert_exit "$ec7" "0" "case7 (test file excluded — under ceiling)"
assert_contains "$out7" "prod.go" "case7 (production file present)"
# Test file MUST NOT appear in the per-file rows.
if printf '%s' "$out7" | grep -qF "prod_test.go"; then
  fail_count=$((fail_count + 1))
  echo "FAIL: case7 — prod_test.go leaked into output" >&2
else
  pass_count=$((pass_count + 1))
  echo "PASS: case7 (test file not in output)"
fi

# ---- Case 8: --all on multi-pkg sandbox ----
# Simulate --all by invoking the script with the synthetic ./internal/* layout.
# We can't override --all's hard-coded targets, so test the equivalent contract
# by checking that --all on the REAL repo's hot packages returns a summary
# line and a numeric per-package report (exit code is "either 0 or 1" since
# the repo currently has packages above 30% — pre-sweep). Use --max-pct 999
# to force exit 0, asserting the multi-package report structure.
case8_root="$tmpdir/case8-multi"
mkdir -p "$case8_root/internal/sse" "$case8_root/internal/router" \
         "$case8_root/internal/app" "$case8_root/internal/auth"
write_go_file "$case8_root/internal/sse/x.go" 1 9 sse
write_go_file "$case8_root/internal/router/x.go" 4 1 router  # 80% — fails
write_go_file "$case8_root/internal/app/x.go" 1 9 app
write_go_file "$case8_root/internal/auth/x.go" 1 9 auth

# Run --all from inside the synthetic root so the hard-coded ./internal/* paths
# resolve to our fixture tree.
set +e
out8=$(cd "$case8_root" && bash "$SCRIPT" --all 2>&1)
ec8=$?
set -e
assert_exit "$ec8" "1" "case8 (--all with one package over ceiling)"
assert_contains "$out8" "./internal/sse" "case8 (sse pkg name in output)"
assert_contains "$out8" "./internal/router" "case8 (router pkg name in output)"
assert_contains "$out8" "./internal/app" "case8 (app pkg name in output)"
assert_contains "$out8" "./internal/auth" "case8 (auth pkg name in output)"
assert_contains "$out8" "comment-density: FAIL" "case8 (final FAIL summary)"

# ---- Summary ----
echo
total=$((pass_count + fail_count))
if [ "$fail_count" -eq 0 ]; then
  echo "comment-density_test: OK ($pass_count/$total assertions passed across 8 cases)"
  exit 0
else
  echo "comment-density_test: FAIL ($fail_count/$total assertions failed)" >&2
  exit 1
fi
