#!/usr/bin/env bash
# =============================================================================
# scripts/lint-comment-tokens_test.sh
#
# Shell-level unit test for scripts/lint-comment-tokens.sh. Asserts the
# script's exit codes and output annotations against synthetic fixtures.
#
# Cases:
#   1. clean fixture                 -> exit 0
#   2. single banned-token comment   -> exit 1, annotation prints file/line/token
#   3. two banned tokens on 2 lines  -> exit 1, summary reports 2 hits
#   4. banned token in non-.go file  -> exit 0 (lint scoped to *.go)
#   5. banned token in string literal -> exit 0 (lint is comment-only)
#
# All synthetic fixtures are written into a mktemp -d sandbox; nothing
# touches the repo. The fixture content is composed via printf %s so the
# test script itself contains zero banned tokens at the source-text level
# (the lint runs against this repo as well, including any *.sh additions
# — though *.sh is outside the --include="*.go" scope, the fixture data
# is kept in $tmpdir for cleanliness anyway).
#
# Exit codes:
#   0 — all cases pass.
#   1 — at least one case failed (descriptive error printed).
# =============================================================================

set -euo pipefail

# Locate the script under test relative to this test script.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LINT="$SCRIPT_DIR/lint-comment-tokens.sh"

if [ ! -x "$LINT" ]; then
  echo "test: error: lint script not executable at $LINT" >&2
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

# ---- Case 1: clean fixture ----
case1="$tmpdir/case1"
mkdir -p "$case1"
cat > "$case1/clean.go" <<'EOF'
package x

// Just a normal comment with no banned tokens.
var X = 1
EOF

set +e
out1=$(bash "$LINT" "$case1" 2>&1)
ec1=$?
set -e
assert_exit "$ec1" "0" "case1 (clean fixture)"
assert_contains "$out1" "lint-comment-tokens: OK" "case1 (OK summary)"

# ---- Case 2: single banned-token comment ----
case2="$tmpdir/case2"
mkdir -p "$case2"
# Compose the banned word at runtime to keep this test source clean.
forbidden=$(printf '%s %s' "Phase" "17")
{
  echo "package x"
  echo
  echo "// $forbidden / HB-01: this comment is forbidden."
  echo "var Y = 2"
} > "$case2/bad.go"

set +e
out2=$(bash "$LINT" "$case2" 2>&1)
ec2=$?
set -e
assert_exit "$ec2" "1" "case2 (single banned-token)"
assert_contains "$out2" "::error file=" "case2 (annotation prefix)"
assert_contains "$out2" "line=3" "case2 (line number)"
assert_contains "$out2" "$forbidden" "case2 (matched token)"
assert_contains "$out2" "lint-comment-tokens: FAIL (1 hit" "case2 (FAIL summary, 1 hit)"

# ---- Case 3: two banned tokens on two lines ----
case3="$tmpdir/case3"
mkdir -p "$case3"
t1=$(printf '%s %s' "Phase" "17")
t2=$(printf '%s' "PITFALL")
{
  echo "package x"
  echo
  echo "// $t1: forbidden one."
  echo "// $t2: forbidden two."
  echo "var Z = 3"
} > "$case3/multi.go"

set +e
out3=$(bash "$LINT" "$case3" 2>&1)
ec3=$?
set -e
assert_exit "$ec3" "1" "case3 (two banned tokens)"
assert_contains "$out3" "lint-comment-tokens: FAIL (2 hit" "case3 (FAIL summary, 2 hits)"

# ---- Case 4: banned token in non-.go file is ignored ----
case4="$tmpdir/case4"
mkdir -p "$case4"
t4=$(printf '%s %s' "Phase" "17")
echo "$t4 — markdown reference" > "$case4/note.md"
{
  echo "package x"
  echo
  echo "// clean go file"
  echo "var W = 4"
} > "$case4/clean.go"

set +e
out4=$(bash "$LINT" "$case4" 2>&1)
ec4=$?
set -e
assert_exit "$ec4" "0" "case4 (non-.go file ignored)"
assert_contains "$out4" "lint-comment-tokens: OK" "case4 (OK summary)"

# ---- Case 5: banned token in a string literal is ignored ----
# This mirrors the real-world false-positive shape from Plan 03-04:
#   t.Skip("flaky on slow CI hardware; tracked as Phase 11 follow-up.")
case5="$tmpdir/case5"
mkdir -p "$case5"
t5=$(printf '%s %s' "Phase" "11")
{
  echo "package x"
  echo
  echo "import \"testing\""
  echo
  echo "func TestS(t *testing.T) {"
  echo "    t.Skip(\"flaky on slow CI hardware; tracked as $t5 follow-up.\")"
  echo "}"
} > "$case5/skip.go"

set +e
out5=$(bash "$LINT" "$case5" 2>&1)
ec5=$?
set -e
assert_exit "$ec5" "0" "case5 (string literal ignored)"
assert_contains "$out5" "lint-comment-tokens: OK" "case5 (OK summary)"

# ---- Case 6: Phase 3 SC #3 alternations (one fixture per new alternation) ----
# The widened pattern (scripts/lint-comment-tokens.sh line ~72) adds
# Pitfall, DRAIN-NN, SSE-NN, IFACE-NN, D-NN, WRITER-NN, DECOMP-NN,
# SWEEP-NN, FLAKE-NN, BENCH-NN, LIFE-NN, DI-NN, READ-NN, WRITER-CLN-NN,
# WR-NN. Each must trigger the lint with exit 1 and surface the token
# in the captured annotation. Fixture tokens are composed at runtime
# via printf so the test source itself stays clean of banned tokens.
case6_dir="$tmpdir/case6"
mkdir -p "$case6_dir"
case6_pass=0
case6_fail=0
# token-stem and number per alternation
declare -a alternations=(
  "Pitfall:7"
  "DRAIN:05"
  "SSE:06"
  "IFACE:01"
  "D:06"
  "WRITER:03"
  "DECOMP:02"
  "SWEEP:05"
  "FLAKE:04"
  "BENCH:01"
  "LIFE:02"
  "DI:03"
  "READ:01"
  "WRITER-CLN:01"
  "WR:02"
)
for spec in "${alternations[@]}"; do
  stem=${spec%%:*}
  num=${spec##*:}
  # Two formats: "Pitfall 7" (space) for the literal Pitfall alternation,
  # and "STEM-NN" for everything else.
  if [ "$stem" = "Pitfall" ]; then
    token=$(printf '%s %s' "$stem" "$num")
  else
    token=$(printf '%s-%s' "$stem" "$num")
  fi
  fname="$case6_dir/${stem//-/_}.go"
  {
    echo "package x"
    echo
    echo "// $token: forbidden token fixture."
    echo "var V = 1"
  } > "$fname"
  set +e
  out=$(bash "$LINT" "$case6_dir" 2>&1)
  ec=$?
  set -e
  label="case6 alternation $stem (token=$token)"
  # The annotation surfaces the matched alternation, not the full fixture
  # token. For "Pitfall 7" the matched alternation is the literal "Pitfall"
  # (single word, no number), so we assert on the alternation stem rather
  # than the composed fixture token.
  needle="$token"
  if [ "$stem" = "Pitfall" ]; then
    needle="Pitfall"
  fi
  if [ "$ec" = "1" ] && printf '%s' "$out" | grep -qF "$needle"; then
    pass_count=$((pass_count + 1))
    case6_pass=$((case6_pass + 1))
    echo "PASS: $label (exit=1, token surfaced)"
  else
    fail_count=$((fail_count + 1))
    case6_fail=$((case6_fail + 1))
    echo "FAIL: $label — exit=$ec, output:" >&2
    printf '%s\n' "$out" >&2
  fi
  rm -f "$fname"
done
echo "case6 alternation coverage: ${case6_pass} pass / ${case6_fail} fail"

# ---- Summary ----
echo
total=$((pass_count + fail_count))
if [ "$fail_count" -eq 0 ]; then
  echo "lint-comment-tokens_test: OK ($pass_count/$total assertions passed across 6 cases)"
  exit 0
else
  echo "lint-comment-tokens_test: FAIL ($fail_count/$total assertions failed)" >&2
  exit 1
fi
