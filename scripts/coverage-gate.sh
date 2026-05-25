#!/usr/bin/env bash
# scripts/coverage-gate.sh — per-package coverage enforcer for internal/...
#
# Promotes the v1.0/v1.1 "> 85 % per package" audit rule into a hard gate.
# Reads `go test ... -cover` summary lines and FAILs (exit 1) when any package
# under `github.com/walera/walera/internal/` falls below the threshold.
#
# Input  : a `go test` log on stdin OR a path passed as $1 (default: coverage.log).
#          The expected format is the canonical `ok  <pkg>  <time>  coverage: N.N% of statements`
#          lines that `go test -cover` emits on stdout. The script also tolerates
#          (and ignores) any `total:` aggregate line that `go tool cover -func`
#          appends — the gate is per-package, not per-binary.
#
# Output : one PASS / FAIL / WARN line per internal package to stdout/stderr,
#          plus a final summary; exit 0 if every internal package with tests
#          meets the threshold, exit 1 otherwise.
#
# Threshold: defined ONCE here as `THRESHOLD`, overridable via the
#            `COVERAGE_THRESHOLD` env var. Default 85.0.
#
# Convention: this script is POSIX bash with `set -euo pipefail` and no
#             GNU-only flags so it runs on macOS dev boxes and CI runners alike.
#             Numeric comparisons go through `awk` because POSIX `test` is
#             integer-only and percentages have decimals.
#
# Usage:
#   go test -race -coverprofile=coverage.out -covermode=atomic ./... 2>&1 \
#     | tee coverage.log
#   bash scripts/coverage-gate.sh coverage.log
#
# Or pipe directly:
#   go test ... | bash scripts/coverage-gate.sh
set -euo pipefail

THRESHOLD="${COVERAGE_THRESHOLD:-85.0}"
INTERNAL_PREFIX="github.com/walera/walera/internal/"

# --- Input source: $1 if present and exists, else stdin -------------------
INPUT_PATH="${1:-}"
if [ -n "$INPUT_PATH" ]; then
    [ -r "$INPUT_PATH" ] || { echo "coverage-gate: cannot read '$INPUT_PATH'" >&2; exit 2; }
    SOURCE_CMD=(cat "$INPUT_PATH")
else
    SOURCE_CMD=(cat -)
fi

# --- Parse the log --------------------------------------------------------
# We classify each line into one of three buckets:
#   - ok   ...internal/<pkg>... coverage: N.N% of statements  → measured
#   - ?    ...internal/<pkg>... [no test files]               → warn-only
# Anything else (FAIL lines, `total:` from cover -func, build output) is
# silently ignored — the gate is a coverage check, not a test-runner.

failures=()   # "pkg pct" pairs for packages below threshold
warns=()      # packages with no test files
passes=()    # "pkg pct" pairs that met the bar

while IFS= read -r line; do
    case "$line" in
        ok*${INTERNAL_PREFIX}*coverage:*)
            # Extract package: word 2 of the `ok  <pkg>  <time>  coverage:...` shape.
            pkg=$(awk '{print $2}' <<<"$line")
            # Extract percentage: token after `coverage:`, strip trailing %.
            pct=$(awk -F'coverage:' '{print $2}' <<<"$line" | awk '{print $1}' | tr -d '%')
            # Numeric comparison via awk (floats; `test` would be integer-only).
            if awk -v p="$pct" -v t="$THRESHOLD" 'BEGIN{exit !(p+0 < t+0)}'; then
                failures+=("$pkg $pct")
            else
                passes+=("$pkg $pct")
            fi
            ;;
        \?*${INTERNAL_PREFIX}*\[no\ test\ files\]*)
            pkg=$(awk '{print $2}' <<<"$line")
            warns+=("$pkg")
            ;;
        *)
            : # ignore everything else (including `total:` from cover -func)
            ;;
    esac
done < <("${SOURCE_CMD[@]}")

# --- Report ---------------------------------------------------------------
for entry in "${passes[@]}"; do
    pkg=${entry% *}
    pct=${entry##* }
    printf 'PASS: %s %s%% >= %s%%\n' "$pkg" "$pct" "$THRESHOLD"
done

for pkg in "${warns[@]}"; do
    printf 'WARN: %s [no test files] — not counted as failure\n' "$pkg" >&2
done

if [ "${#failures[@]}" -gt 0 ]; then
    for entry in "${failures[@]}"; do
        pkg=${entry% *}
        pct=${entry##* }
        printf 'FAIL: %s %s%% < %s%%\n' "$pkg" "$pct" "$THRESHOLD" >&2
    done
    printf '\ncoverage-gate: %d package(s) below %s%% threshold\n' \
        "${#failures[@]}" "$THRESHOLD" >&2
    exit 1
fi

if [ "${#passes[@]}" -eq 0 ]; then
    # Defensive: zero packages parsed = caller fed us the wrong file.
    echo "coverage-gate: no 'ok  ${INTERNAL_PREFIX}...coverage:' lines found in input — refusing to pass vacuously" >&2
    exit 2
fi

printf '\ncoverage-gate: all %d internal package(s) >= %s%%\n' \
    "${#passes[@]}" "$THRESHOLD"
