#!/usr/bin/env bash
# =============================================================================
# scripts/check-changelog-date_test.sh
#
# Shell-level unit test for scripts/check-changelog-date.sh. Asserts the
# guard's exit codes and output annotations against synthetic fixtures
# in a mktemp -d sandbox — nothing touches the repo's real CHANGELOG.md.
#
# Cases:
#   1. clean fixture with concrete date         -> exit 0, OK summary
#   2. single placeholder on a known line       -> exit 1, line=N annotation, FAIL 1 hit
#   3. mixed sections: 1 concrete + 1 placeholder -> exit 1, FAIL 1 hit
#   4. empty file                               -> exit 0 (degenerate, not a failure)
#   5. missing file                             -> exit 2 (usage error)
#
# Exit codes:
#   0 — all cases pass.
#   1 — at least one assertion failed (descriptive error printed).
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARD="$SCRIPT_DIR/check-changelog-date.sh"

if [ ! -x "$GUARD" ]; then
  echo "test: error: guard script not executable at $GUARD" >&2
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

# The template marker for the placeholder — composed at runtime so the
# test source text itself does not duplicate the literal `YYYY-MM-DD`
# in a way that could surprise repo-wide greps.
PLACEHOLDER=$(printf '%s-%s-%s' "YYYY" "MM" "DD")

# ---- Case 1: clean fixture with concrete date ----
case1="$tmpdir/case1"
mkdir -p "$case1"
{
  echo "## [2.0.0] - 2026-05-23"
  echo
  echo "### Added"
  echo "- thing"
} > "$case1/clean.md"

set +e
out1=$(bash "$GUARD" "$case1/clean.md" 2>&1)
ec1=$?
set -e
assert_exit "$ec1" "0" "case1 (clean concrete date)"
assert_contains "$out1" "check-changelog-date: OK" "case1 (OK summary)"

# ---- Case 2: single placeholder on a known line ----
case2="$tmpdir/case2"
mkdir -p "$case2"
{
  echo "## [Unreleased]"          # line 1
  echo                            # line 2
  echo "## [2.0.0] - $PLACEHOLDER" # line 3 — the placeholder lives here
  echo                            # line 4
  echo "### Added"                # line 5
  echo "- thing"                  # line 6
} > "$case2/bad.md"

set +e
out2=$(bash "$GUARD" "$case2/bad.md" 2>&1)
ec2=$?
set -e
assert_exit "$ec2" "1" "case2 (single placeholder)"
assert_contains "$out2" "::error file=" "case2 (annotation prefix)"
assert_contains "$out2" "line=3" "case2 (line number)"
assert_contains "$out2" "$PLACEHOLDER" "case2 (annotation names placeholder)"
assert_contains "$out2" "check-changelog-date: FAIL (1 hit" "case2 (FAIL summary, 1 hit)"

# ---- Case 3: mixed sections (1 concrete + 1 placeholder) ----
case3="$tmpdir/case3"
mkdir -p "$case3"
{
  echo "## [Unreleased]"             # line 1
  echo                               # line 2
  echo "## [2.0.0] - 2026-05-23"     # line 3 — concrete, no hit
  echo                               # line 4
  echo "## [3.0.0] - $PLACEHOLDER"   # line 5 — placeholder, hit here
  echo                               # line 6
  echo "### Added"                   # line 7
} > "$case3/mixed.md"

set +e
out3=$(bash "$GUARD" "$case3/mixed.md" 2>&1)
ec3=$?
set -e
assert_exit "$ec3" "1" "case3 (mixed: one placeholder)"
assert_contains "$out3" "line=5" "case3 (line number of placeholder)"
assert_contains "$out3" "check-changelog-date: FAIL (1 hit" "case3 (FAIL summary, exactly 1 hit)"

# ---- Case 4: empty file ----
case4="$tmpdir/case4"
mkdir -p "$case4"
: > "$case4/empty.md"

set +e
out4=$(bash "$GUARD" "$case4/empty.md" 2>&1)
ec4=$?
set -e
assert_exit "$ec4" "0" "case4 (empty file)"
assert_contains "$out4" "check-changelog-date: OK" "case4 (OK summary)"

# ---- Case 5: missing file (usage error) ----
set +e
out5=$(bash "$GUARD" "$tmpdir/case5/does-not-exist.md" 2>&1)
ec5=$?
set -e
assert_exit "$ec5" "2" "case5 (missing file -> usage error)"
assert_contains "$out5" "is not readable" "case5 (error names the missing path)"

# ---- Summary ----
echo
total=$((pass_count + fail_count))
if [ "$fail_count" -eq 0 ]; then
  echo "check-changelog-date_test: OK ($pass_count/$total assertions passed across 5 cases)"
  exit 0
else
  echo "check-changelog-date_test: FAIL ($fail_count/$total assertions failed)" >&2
  exit 1
fi
