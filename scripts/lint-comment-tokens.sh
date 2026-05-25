#!/usr/bin/env bash
# =============================================================================
# scripts/lint-comment-tokens.sh
#
# Banned-token lint for Go-source comments. Closes REQUIREMENTS.md HYGIENE-04
# and HYGIENE-05 (CI gate). Run on every PR via .github/workflows/checks.yml
# (`comment-hygiene` job); also exposed locally as `make lint-comments`.
#
# What it does:
#   Scans every *.go file under the search root (default ".") for a
#   canonical set of banned tokens that, by project convention, are
#   GSD-planning artifacts and MUST NOT survive in production Go comments.
#   On hit, prints one GitHub-Actions error annotation per occurrence and
#   exits 1. On clean, prints a one-line OK summary and exits 0.
#
# Canonical banned-token regex (PCRE):
#   Plan [0-9]+-[0-9]+
#     | Phase [0-9]+
#     | PITFALL
#     | D[0-9]+-[0-9]+
#     | LIM-[0-9]+
#     | Added for future plan
#     | CR-[0-9]+
#     | HB-[0-9]+
#     | OBS-[0-9]+
#     | SC#[0-9]+
#
# Source: .planning/milestones/v2.0-phases/v2.0-MILESTONE-AUDIT.md, HYGIENE-01.
#
# Scope:
#   - Only *.go files (production AND test). The repository convention is
#     that .planning/, *.md, *.yml, and shell-script files freely reference
#     plan/phase IDs because they ARE the planning artifacts. The lint
#     enforces a Go-source-only ban.
#   - Excludes vendor/ and .git/ (belt-and-suspenders; --include already
#     filters them).
#
# Comment-only matching:
#   The lint restricts hits to lines that look like Go comment lines:
#     - line starts (after whitespace) with "//"   (line comments)
#     - line starts (after whitespace) with "*"    (continuation of /* */)
#     - line contains an inline " //" after code   (rare but legal)
#   This deliberately ignores banned tokens that appear inside string
#   literals (e.g. t.Skip("flaky on slow CI hardware; tracked as Phase 11
#   follow-up. ") in test/integration/06_slow_consumer_test.go) — those
#   are runtime operator-facing strings, not maintainer-facing comments.
#   Plan 03-04 SUMMARY documents the two known string-literal hits.
#
# Portability:
#   Uses GNU grep -P (PCRE). The CI runner is ubuntu-latest and ships
#   GNU grep natively. macOS developers need `brew install grep` and to
#   invoke this script via `PATH=/opt/homebrew/opt/grep/libexec/gnubin:$PATH`
#   or simply use `ggrep` indirectly through the system bash.
#
# Exit codes:
#   0 — clean (no banned tokens in any Go comment under the search root).
#   1 — at least one banned token found.
#   2 — usage / argument error.
# =============================================================================

set -euo pipefail

root="${1:-.}"

if [ ! -d "$root" ]; then
  echo "lint-comment-tokens: error: search root '$root' is not a directory" >&2
  exit 2
fi

# Canonical banned-token PCRE. Single-quoted so backslashes survive intact
# to grep -P. See header for the canonical token enumeration. The Phase 3
# additions (Pitfall, DRAIN-NN, SSE-NN, IFACE-NN, D-NN, WRITER-NN,
# DECOMP-NN, SWEEP-NN, FLAKE-NN, BENCH-NN, LIFE-NN, DI-NN, READ-NN,
# WRITER-CLN-NN, WR-NN) close the ROADMAP Phase 3 SC #3 coverage gap.
pattern='Plan [0-9]+-[0-9]+|Phase [0-9]+|PITFALL|Pitfall|D[0-9]+-[0-9]+|LIM-[0-9]+|Added for future plan|CR-[0-9]+|HB-[0-9]+|OBS-[0-9]+|SC#[0-9]+|DRAIN-[0-9]+|SSE-[0-9]+|IFACE-[0-9]+|D-[0-9]+|WRITER-CLN-[0-9]+|WRITER-[0-9]+|DECOMP-[0-9]+|SWEEP-[0-9]+|FLAKE-[0-9]+|BENCH-[0-9]+|LIFE-[0-9]+|DI-[0-9]+|READ-[0-9]+|WR-[0-9]+'

# Comment-line pre-filter PCRE: a line is "comment-ish" if it begins
# (modulo leading whitespace) with "//" or "*", OR if it contains an
# inline " //" comment marker followed by anything. This matches:
#   //  bare line comment
#       // indented line comment
#    *  block-comment continuation line
#   code // trailing inline comment
# It rejects string literals like `t.Skip("...Phase 11...")` because
# such lines neither start with // nor contain a // marker.
comment_pat='^\s*(//|\*)|//'

# Compose the full match: line must look like a comment AND contain a
# banned token. We do this with a two-stage grep so we can still emit
# the annotation containing the matched token (single-stage `grep -P`
# with two lookaheads would lose the captured-token text).
#
# Stage 1: --include="*.go" + comment-line filter, with file:line:content.
# Stage 2: awk to filter lines that ALSO contain a banned token; extract
# the first matched token via a separate grep -oP per line.

# Use a tempfile to avoid set -e issues with the empty-grep-result branch.
tmp_hits="$(mktemp)"
trap 'rm -f "$tmp_hits"' EXIT

# Collect all comment-prefixed lines with file:line:content prefix.
# grep -P matches the comment_pat; --include filters to .go files;
# --exclude-dir keeps vendor and .git out (belt-and-suspenders).
grep -rPn \
  --include='*.go' \
  --exclude-dir='vendor' \
  --exclude-dir='.git' \
  "$comment_pat" "$root" > "$tmp_hits" 2>/dev/null || true

hit_count=0
while IFS= read -r line; do
  # Split file:linenum:content. Filenames may contain ':' on weird
  # systems but our tree has none; use cut for clarity.
  file=$(printf '%s' "$line" | cut -d: -f1)
  linenum=$(printf '%s' "$line" | cut -d: -f2)
  content=$(printf '%s' "$line" | cut -d: -f3-)

  # Does this comment-line contain a banned token?
  token=$(printf '%s' "$content" | grep -oP "$pattern" | head -n 1 || true)
  if [ -z "$token" ]; then
    continue
  fi

  printf '::error file=%s,line=%s,title=Banned comment token::Token "%s" is forbidden in .go comments. See REQUIREMENTS.md HYGIENE-04.\n' \
    "$file" "$linenum" "$token"
  hit_count=$((hit_count + 1))
done < "$tmp_hits"

if [ "$hit_count" -eq 0 ]; then
  echo "lint-comment-tokens: OK (0 hits across .go files in $root)"
  exit 0
fi

echo "lint-comment-tokens: FAIL ($hit_count hit(s) across .go files in $root)"
exit 1
