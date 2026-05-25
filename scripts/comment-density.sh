#!/usr/bin/env bash
# =============================================================================
# scripts/comment-density.sh
#
# Per-package comment-to-code ratio measurement. Closes REQUIREMENTS.md
# SWEEP-05 (CI gate). Run on every PR via .github/workflows/checks.yml
# (`comment-hygiene` job, alongside lint-comment-tokens.sh); also exposed
# locally as `make comment-density`.
#
# What it does:
#   Walks every non-test *.go file under the given package path (or, with
#   --all, the five hot packages internal/{sse,router,app,auth,writer})
#   and computes a deterministic per-file and per-package ratio:
#
#       ratio_pct = 100 * comment_lines / (comment_lines + code_lines)
#
#   Blank lines are excluded from both numerator and denominator. The
#   package ratio sums comment and code lines across all files in the
#   package (NOT the arithmetic mean of per-file ratios — files with more
#   total lines weight the package ratio more heavily). The default
#   ceiling is 30%; override with --max-pct N.
#
# Scope:
#   - Only non-test *.go files. *_test.go is EXCLUDED unconditionally
#     from the ratio (the SWEEP-XX requirements measure production-code
#     comment density only; test files commonly use long table-driven
#     fixtures that would skew the ratio). This is documented in --help
#     and is NOT configurable.
#   - Excludes vendor/ and .git/ (belt-and-suspenders; the default --all
#     glob already filters them).
#
# Comment-line definition (matches --help text):
#   A line is a "comment line" if its first non-whitespace characters are
#   // or *, OR if the line is bounded by /* ... */ and contains no Go
#   code on the same line. A "code line" is any non-blank, non-comment
#   line. Pure-blank lines (whitespace-only) are excluded from both.
#
# CLI surface:
#   bash scripts/comment-density.sh [--max-pct N] [--json] [--help] <package-path>
#   bash scripts/comment-density.sh [--max-pct N] [--json] [--help] --all
#
# Output (human mode):
#   Per-file lines sorted descending by ratio, then a per-package summary,
#   then (in --all mode) a final overall summary. Single failing package
#   emits one GitHub-Actions ::error annotation pointing at the worst
#   file in the package (per-package, not per-file — keeps PR annotation
#   surface actionable).
#
# Output (JSON mode):
#   Single JSON object per invocation. For single-package mode:
#     {"package":"...","ratio_pct":N.NN,"files":[...],"ceiling_pct":N,"status":"ok"|"fail"}
#   For --all mode:
#     {"packages":[...],"status":"ok"|"fail"}
#
# Portability:
#   Pure bash + awk + grep + find; no PCRE required. The CI runner is
#   ubuntu-latest. macOS developers can run as-is (the awk dialect is
#   POSIX).
#
# Exit codes:
#   0 — every measured package is at or below the ceiling.
#   1 — at least one measured package is above the ceiling.
#   2 — usage / argument error (missing dir, bad flag, etc.).
# =============================================================================

set -euo pipefail

usage() {
  cat <<'USAGE' >&2
Usage:
  bash scripts/comment-density.sh [--max-pct N] [--json] <package-path>
  bash scripts/comment-density.sh [--max-pct N] [--json] --all
  bash scripts/comment-density.sh --help

Flags:
  --max-pct N   Per-package ceiling in percent (integer). Default: 30.
  --json        Emit machine-readable JSON instead of human text.
  --all         Sweep the canonical hot packages:
                  ./internal/sse ./internal/router ./internal/app
                  ./internal/auth ./internal/writer
  --help        Show this message and exit 0.

Notes:
  *_test.go files are EXCLUDED from the ratio unconditionally. The metric
  measures production-code comment density only; test files commonly use
  long table-driven fixtures that would otherwise skew the ratio.

Exit codes:
  0   all measured packages at or below ceiling
  1   at least one package above ceiling
  2   usage / argument error
USAGE
}

ceiling=30
json_mode=0
all_mode=0
target=""

# Hand-rolled flag loop matching scripts/lint-comment-tokens.sh terse style.
while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h)
      usage
      exit 0
      ;;
    --max-pct)
      if [ "$#" -lt 2 ]; then
        echo "comment-density: error: --max-pct requires an integer argument" >&2
        exit 2
      fi
      ceiling="$2"
      if ! printf '%s' "$ceiling" | grep -qE '^[0-9]+$'; then
        echo "comment-density: error: --max-pct must be a non-negative integer (got: $ceiling)" >&2
        exit 2
      fi
      shift 2
      ;;
    --json)
      json_mode=1
      shift
      ;;
    --all)
      all_mode=1
      shift
      ;;
    --*)
      echo "comment-density: error: unknown flag '$1'" >&2
      usage
      exit 2
      ;;
    *)
      if [ -n "$target" ]; then
        echo "comment-density: error: multiple package paths given ('$target' and '$1')" >&2
        exit 2
      fi
      target="$1"
      shift
      ;;
  esac
done

if [ "$all_mode" -eq 0 ] && [ -z "$target" ]; then
  echo "comment-density: error: no package path given; use --all or supply <package-path>" >&2
  usage
  exit 2
fi

if [ "$all_mode" -eq 1 ] && [ -n "$target" ]; then
  echo "comment-density: error: --all and an explicit package path are mutually exclusive" >&2
  exit 2
fi

# -----------------------------------------------------------------------------
# measure_file <path>
#   prints "<comment_lines> <code_lines>" for the given file via awk.
# -----------------------------------------------------------------------------
measure_file() {
  local path="$1"
  awk '
    BEGIN { c = 0; k = 0; in_block = 0 }
    {
      line = $0
      # Strip leading whitespace for prefix detection.
      stripped = line
      sub(/^[ \t]+/, "", stripped)
      # Blank line: neither comment nor code.
      if (stripped == "") { next }

      if (in_block) {
        c++
        if (index(stripped, "*/") > 0) { in_block = 0 }
        next
      }

      # Single-line block comment: starts with /* and contains matching */ on
      # the same line and nothing else after */ besides whitespace.
      if (substr(stripped, 1, 2) == "/*") {
        end = index(stripped, "*/")
        if (end > 0) {
          # Confirm no non-whitespace after */
          tail = substr(stripped, end + 2)
          gsub(/[ \t]+/, "", tail)
          if (tail == "") { c++; next }
          # Code follows the block close — treat as code line.
          k++; next
        }
        # Opens a block; no close on this line.
        in_block = 1
        c++
        next
      }

      # Line-comment prefix.
      if (substr(stripped, 1, 2) == "//") { c++; next }

      # Continuation of a /* */ block (line begins with *) — treat as comment.
      if (substr(stripped, 1, 1) == "*") { c++; next }

      # Anything else with non-whitespace is a code line.
      k++
    }
    END { printf "%d %d\n", c, k }
  ' "$path"
}

# -----------------------------------------------------------------------------
# measure_package <path>
#   Scans non-test *.go files under <path> (non-recursive into vendor/),
#   prints lines of the form:
#       <ratio_pct> <comment_lines> <code_lines> <file_path>
#   one per file, sorted DESCENDING by ratio_pct. Stores a summary line
#   in the global vars pkg_comment / pkg_code / pkg_ratio / pkg_files_count.
# -----------------------------------------------------------------------------
pkg_comment=0
pkg_code=0
pkg_ratio="0.00"
pkg_files_count=0
declare -a pkg_file_lines=()

measure_package() {
  local pkg="$1"
  pkg_comment=0
  pkg_code=0
  pkg_ratio="0.00"
  pkg_files_count=0
  pkg_file_lines=()

  if [ ! -d "$pkg" ]; then
    echo "comment-density: error: search root '$pkg' is not a directory" >&2
    return 2
  fi

  # Collect non-test *.go files. Use a portable find invocation: depth 1
  # is wrong because packages have subpackages — but the SWEEP measurement
  # is per-package, so we want only files at depth 1 of the given dir.
  local files=()
  while IFS= read -r f; do
    [ -n "$f" ] && files+=("$f")
  done < <(find "$pkg" -maxdepth 1 -type f -name '*.go' ! -name '*_test.go' 2>/dev/null | sort)

  pkg_files_count="${#files[@]}"

  local f comment code ratio
  for f in "${files[@]}"; do
    read -r comment code < <(measure_file "$f")
    pkg_comment=$((pkg_comment + comment))
    pkg_code=$((pkg_code + code))
    # Per-file ratio (printf-formatted; awk handles divide-by-zero).
    ratio=$(awk -v c="$comment" -v k="$code" 'BEGIN {
      denom = c + k
      if (denom == 0) { printf "0.00"; exit }
      printf "%.2f", 100.0 * c / denom
    }')
    pkg_file_lines+=("$ratio $comment $code $f")
  done

  pkg_ratio=$(awk -v c="$pkg_comment" -v k="$pkg_code" 'BEGIN {
    denom = c + k
    if (denom == 0) { printf "0.00"; exit }
    printf "%.2f", 100.0 * c / denom
  }')

  return 0
}

# -----------------------------------------------------------------------------
# emit_human_package — print the human-readable per-package report.
# -----------------------------------------------------------------------------
emit_human_package() {
  local pkg="$1"
  local status="$2"   # "ok" or "fail"
  # Sort file lines descending by ratio.
  if [ "${#pkg_file_lines[@]}" -gt 0 ]; then
    printf '%s\n' "${pkg_file_lines[@]}" | sort -k1,1 -gr | while read -r ratio comment code file; do
      printf '  %s  %s%%  (%s cmt / %s code)\n' "$file" "$ratio" "$comment" "$code"
    done
  fi
  if [ "$status" = "fail" ]; then
    printf 'package %s: %s%% (max=%d%%) — FAIL\n' "$pkg" "$pkg_ratio" "$ceiling"
  else
    printf 'package %s: %s%% (max=%d%%) — OK\n' "$pkg" "$pkg_ratio" "$ceiling"
  fi
}

# -----------------------------------------------------------------------------
# emit_annotation — single GitHub-Actions error annotation per failing pkg.
# Anchors to the file with the highest ratio in the package.
# -----------------------------------------------------------------------------
emit_annotation() {
  local pkg="$1"
  local worst_file=""
  if [ "${#pkg_file_lines[@]}" -gt 0 ]; then
    worst_file=$(printf '%s\n' "${pkg_file_lines[@]}" | sort -k1,1 -gr | head -n 1 | awk '{ print $4 }')
  fi
  [ -z "$worst_file" ] && worst_file="$pkg"
  printf '::error file=%s,title=Package comment density above ceiling::package %s ratio %s%% > ceiling %d%%\n' \
    "$worst_file" "$pkg" "$pkg_ratio" "$ceiling"
}

# -----------------------------------------------------------------------------
# emit_json_files — print the inner [...] array for one package's file list.
# -----------------------------------------------------------------------------
emit_json_files() {
  if [ "${#pkg_file_lines[@]}" -eq 0 ]; then
    printf '[]'
    return
  fi
  local first=1 entry
  printf '['
  # Sort descending by ratio for stable output.
  while IFS= read -r entry; do
    local ratio comment code file
    read -r ratio comment code file <<<"$entry"
    if [ "$first" -eq 1 ]; then
      first=0
    else
      printf ','
    fi
    printf '{"path":"%s","ratio_pct":%s,"comment_lines":%s,"code_lines":%s}' \
      "$file" "$ratio" "$comment" "$code"
  done < <(printf '%s\n' "${pkg_file_lines[@]}" | sort -k1,1 -gr)
  printf ']'
}

# -----------------------------------------------------------------------------
# compare ratio vs ceiling — uses awk for float comparison.
# returns 0 if ratio > ceiling (i.e. FAIL), 1 otherwise.
# -----------------------------------------------------------------------------
ratio_over_ceiling() {
  local r="$1" c="$2"
  awk -v r="$r" -v c="$c" 'BEGIN { exit (r > c) ? 0 : 1 }'
}

# -----------------------------------------------------------------------------
# Main dispatch.
# -----------------------------------------------------------------------------
if [ "$all_mode" -eq 1 ]; then
  packages=(./internal/sse ./internal/router ./internal/app ./internal/auth ./internal/writer)
else
  packages=("$target")
fi

any_fail=0
overall_fail_count=0

# JSON accumulator (string fragments). We build the package array up front.
json_pkg_chunks=()

for pkg in "${packages[@]}"; do
  if ! measure_package "$pkg"; then
    # measure_package already wrote a diagnostic.
    exit 2
  fi

  status="ok"
  if ratio_over_ceiling "$pkg_ratio" "$ceiling"; then
    status="fail"
    any_fail=1
    overall_fail_count=$((overall_fail_count + 1))
  fi

  if [ "$json_mode" -eq 1 ]; then
    files_json=$(emit_json_files)
    pkg_chunk=$(printf '{"package":"%s","ratio_pct":%s,"files":%s,"ceiling_pct":%d,"status":"%s"}' \
      "$pkg" "$pkg_ratio" "$files_json" "$ceiling" "$status")
    json_pkg_chunks+=("$pkg_chunk")
  else
    emit_human_package "$pkg" "$status"
    if [ "$status" = "fail" ]; then
      emit_annotation "$pkg"
    fi
    # Blank line between packages in --all mode.
    if [ "$all_mode" -eq 1 ]; then
      printf '\n'
    fi
  fi
done

# -----------------------------------------------------------------------------
# Final summary.
# -----------------------------------------------------------------------------
if [ "$json_mode" -eq 1 ]; then
  overall_status="ok"
  [ "$any_fail" -eq 1 ] && overall_status="fail"
  if [ "$all_mode" -eq 1 ]; then
    # Emit a wrapper object with packages[] and overall status.
    printf '{"packages":['
    sep=""
    for chunk in "${json_pkg_chunks[@]}"; do
      printf '%s%s' "$sep" "$chunk"
      sep=","
    done
    printf '],"status":"%s"}\n' "$overall_status"
  else
    # Single-package JSON object (already in chunk form).
    printf '%s\n' "${json_pkg_chunks[0]}"
  fi
else
  if [ "$any_fail" -eq 1 ]; then
    if [ "$overall_fail_count" -eq 1 ]; then
      printf 'comment-density: FAIL (1 package over ceiling)\n'
    else
      printf 'comment-density: FAIL (%d packages over ceiling)\n' "$overall_fail_count"
    fi
  else
    printf 'comment-density: OK\n'
  fi
fi

if [ "$any_fail" -eq 1 ]; then
  exit 1
fi
exit 0
