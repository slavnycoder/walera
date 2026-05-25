#!/usr/bin/env bash
# =============================================================================
# scripts/check-changelog-date.sh
#
# CHANGELOG date-placeholder guard. Closes REQUIREMENTS.md RELEASE-02
# (ROADMAP Phase 4 SC1 + SC2). Run on every PR via
# .github/workflows/checks.yml (`changelog-date-test` job), invoked at
# tag-cut time by .github/workflows/release.yml, and exposed locally as
# `make check-changelog-date`.
#
# What it does:
#   Scans the supplied file (default `CHANGELOG.md`) for the literal
#   substring `YYYY-MM-DD`. On each hit, emits one GitHub-Actions
#   `::error file=...,line=N,...` annotation naming the offending line.
#   On clean input, prints a one-line OK summary and exits 0.
#
#   Why this exists: the release.yml flow extracts the
#   `## [<version>]` section from CHANGELOG.md and publishes it as the
#   GitHub Release notes. Tagging a release while the section header
#   still reads `## [X.Y.Z] - YYYY-MM-DD` would publish an obviously
#   un-dated release; the guard refuses to proceed and tells the
#   operator exactly which line to fix.
#
# Usage:
#   scripts/check-changelog-date.sh [PATH]
#     PATH defaults to `CHANGELOG.md`. release.yml passes a tempfile
#     holding the extracted `## [<version>]` section so the annotation
#     line numbers correspond to lines within that section.
#
# Matching:
#   Literal substring `YYYY-MM-DD`, case-sensitive. Implemented via
#   `grep -nF` so the hyphen-rich, template-marker token is matched
#   verbatim (no regex interpretation). The annotation path is the
#   caller-supplied PATH; we deliberately do NOT canonicalize via
#   `realpath` so an annotation pointing at `CHANGELOG.md` stays
#   relative to the repo root for the operator.
#
# Portability:
#   POSIX `grep -nF`. No PCRE required. The CI runner is ubuntu-latest
#   (GNU grep) but the script also works under macOS BSD grep and
#   busybox grep — `-n` and `-F` are POSIX-mandated flags.
#
# Exit codes:
#   0 — clean (no `YYYY-MM-DD` substring in the file).
#   1 — at least one placeholder occurrence found.
#   2 — usage / argument error (no file at the supplied path, or path
#       is not readable).
# =============================================================================

set -euo pipefail

path="${1:-CHANGELOG.md}"

if [ ! -r "$path" ]; then
  echo "check-changelog-date: error: file '$path' is not readable" >&2
  exit 2
fi

# Literal substring (NOT a regex). The token contains hyphens and an
# uppercase template marker; we want verbatim matching.
needle='YYYY-MM-DD'

# Use a tempfile to avoid set -e tripping when grep exits 1 on no-match.
tmp_hits="$(mktemp)"
trap 'rm -f "$tmp_hits"' EXIT

grep -nF "$needle" "$path" > "$tmp_hits" 2>/dev/null || true

hit_count=0
while IFS= read -r line; do
  [ -z "$line" ] && continue
  linenum=$(printf '%s' "$line" | cut -d: -f1)
  printf '::error file=%s,line=%s,title=CHANGELOG date placeholder::Literal "YYYY-MM-DD" placeholder found at line %s. Update CHANGELOG.md before pushing the v* tag.\n' \
    "$path" "$linenum" "$linenum"
  hit_count=$((hit_count + 1))
done < "$tmp_hits"

if [ "$hit_count" -eq 0 ]; then
  echo "check-changelog-date: OK (no YYYY-MM-DD placeholder in $path)"
  exit 0
fi

echo "check-changelog-date: FAIL ($hit_count hit(s) in $path)"
exit 1
