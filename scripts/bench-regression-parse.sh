#!/usr/bin/env bash
# =============================================================================
# scripts/bench-regression-parse.sh
#
# Failure-detection parser for benchstat-format delta output. Closes
# REQUIREMENTS.md BENCH-02 (CI regression gate). Invoked from
# .github/workflows/bench-regression.yml after the workflow has run
# benchstat against the committed baseline and the PR's bench run.
#
# What it does:
#   Reads a benchstat output file (default: delta.txt) and tracks the
#   current metric-unit section header that benchstat emits above each
#   sub-table (sec/op | B/op | allocs/op). For every row in a sec/op or
#   allocs/op section whose "vs base" column reports +N.NN% with N > 3.0
#   AND p-value < 0.05, the script emits a ::error:: annotation naming
#   the offending sub-benchmark and prints the full benchstat row verbatim
#   so the operator can triage from the PR Checks tab without downloading
#   the artifact.
#
#   Gating policy (per BENCH-02):
#     - sec/op    regression > 3% with p < 0.05  -> FAIL
#     - allocs/op regression > 3% with p < 0.05  -> FAIL
#     - B/op      rows are informational only    -> skip
#     - Improvements (negative %) are NEVER a failure; they are not
#       parsed (the regex only matches a literal '+').
#
# Usage:
#   bench-regression-parse.sh [delta.txt]
#
# Exit codes:
#   0 - no regressions detected (or empty / unreadable input -> no rows).
#   1 - at least one regression detected; ::error:: annotations were
#       emitted and a summary line printed.
#
# Self-test (manual; NOT wired into the workflow):
#   bash scripts/bench-regression-parse.sh /dev/null              # -> exit 0
#   See the workflow's Plan-time fixture for the perturbed-input check.
#
# Project conventions honoured:
#   - shebang  : /usr/bin/env bash
#   - strict   : set -euo pipefail
#   - style    : mirrors scripts/lint-comment-tokens.sh and
#                scripts/check-changelog-date_test.sh (header block,
#                ::error:: annotation format, no `set -x`).
#   - no deps  : pure bash + GNU awk for float comparison; the script
#                does NOT invoke benchstat itself (that is the workflow's
#                job — this script consumes benchstat's stdout).
#   - HYGIENE  : comment tokens are safe (BENCH-02 / DECOMP-XX are not
#                banned per RESEARCH.md Q8).
# =============================================================================

set -euo pipefail

delta="${1:-delta.txt}"

if [ ! -r "$delta" ]; then
  echo "bench-regression-parse: error: input '$delta' is not readable" >&2
  exit 2
fi

regressions=0
current_unit=""

# benchstat emits a per-metric grid where each metric section is introduced
# by a header row containing the unit name (sec/op, B/op, allocs/op). Data
# rows below the header carry the same unit until the next header is seen.
# The "vs base" column is also in a header row, so we must distinguish:
#   - unit-only header  : "│ sec/op   │"           <- set current_unit
#   - vs-base header    : "│ sec/op   vs base │"   <- still a header, skip
#   - data row          : "BenchName  base ± n%   new ± n%   +N.NN% (p=...)"
# We treat any line containing the unit token but NOT containing
# "vs base" as a unit-anchor (sets current_unit) AND continue. A line
# that contains BOTH the unit and "vs base" is also a header (the
# second-row header benchstat prints), so we also continue. Only after
# we have a current_unit do we attempt to match a regression row.

while IFS= read -r line; do
  # Unit-anchor detection. The order matters because "allocs/op" contains
  # "B/op" as a substring would NOT (allocs/op vs B/op are disjoint), but
  # we still check allocs/op before B/op for safety.
  if [[ "$line" == *"sec/op"* ]]; then
    current_unit="sec/op"
    continue
  fi
  if [[ "$line" == *"allocs/op"* ]]; then
    current_unit="allocs/op"
    continue
  fi
  if [[ "$line" == *"B/op"* ]]; then
    current_unit="B/op"
    continue
  fi

  # Only gate on sec/op and allocs/op; B/op rows are informational.
  if [[ "$current_unit" != "sec/op" && "$current_unit" != "allocs/op" ]]; then
    continue
  fi

  # Extract +N.NN% and p=N.NNN from the row. The leading literal '+' means
  # improvements (negative deltas) are silently skipped by this regex.
  if [[ "$line" =~ \+([0-9]+\.[0-9]+)%[[:space:]]*\(p=([0-9]+\.[0-9]+) ]]; then
    delta_pct="${BASH_REMATCH[1]}"
    pval="${BASH_REMATCH[2]}"

    # bash has no native float compare; delegate to awk.
    if awk -v d="$delta_pct" -v p="$pval" 'BEGIN { exit !(d > 3.0 && p < 0.05) }'; then
      regressions=$((regressions + 1))
      # First whitespace-delimited token is the bench name (e.g.
      # "RouteTx/exact_1-4"). awk gives us the same field robustly
      # regardless of the surrounding column padding benchstat emits.
      bench_row=$(printf '%s' "$line" | awk '{print $1}')
      printf '::error::Benchmark regression on %s: %s +%s%% (p=%s)\n' \
        "$current_unit" "$bench_row" "$delta_pct" "$pval"
      # Print the offending row verbatim so the operator sees the full
      # benchstat output line (base value, new value, CI, n) without
      # downloading the new.txt/delta.txt artifact.
      printf '  %s\n' "$line"
    fi
  fi
done < "$delta"

if [ "$regressions" -gt 0 ]; then
  printf '::error::%d benchmark regression(s) detected (> 3%% on sec/op or allocs/op, p < 0.05)\n' "$regressions"
  echo "bench-regression-parse: FAIL ($regressions regression(s))"
  exit 1
fi

echo "bench-regression-parse: OK (no regressions detected)"
exit 0
