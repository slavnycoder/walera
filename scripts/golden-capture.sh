#!/usr/bin/env bash
# scripts/golden-capture.sh — regenerate the SSE wire-parity golden fixture.
#
# The fixture at scripts/golden/sse_v13_handshake.txt is the CONTRACT — the
# parity test in internal/sse/golden_parity_test.go is a binary diff against
# this file on every `go test ./...` invocation. Operators run this script
# ONLY when the wire intentionally changes (e.g., a planned encoder evolution).
#
# Workflow:
#   1. Build + run the build-tagged generator (TestGenerateGoldenFixture)
#      which writes scripts/golden/sse_v13_handshake.txt.new.
#   2. Compare the new bytes to the existing committed fixture.
#   3. If different, move .new over the committed file and print a notice.
#      If identical, remove .new and report no-op.
#
# Idempotent: re-running against a clean repo is a no-op (same bytes).
#
# Exit codes:
#   0 — script completed (either no change OR fixture regenerated)
#   non-zero — the build-tagged test failed; nothing was rewritten.
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p scripts/golden

OUT="scripts/golden/sse_v13_handshake.txt"
NEW="${OUT}.new"

# Remove any stale .new from a previous failed run so we never miss a diff.
rm -f "$NEW"

echo "regenerating $OUT via go test -tags golden_capture ..."
go test -tags golden_capture -count=1 -run TestGenerateGoldenFixture ./internal/sse/

if [ ! -f "$NEW" ]; then
  echo "ERROR: $NEW was not produced by the generator" >&2
  exit 1
fi

if [ -f "$OUT" ] && cmp -s "$OUT" "$NEW"; then
  rm -f "$NEW"
  echo "no changes — fixture is up to date ($(wc -c < "$OUT") bytes)"
else
  mv "$NEW" "$OUT"
  echo "fixture regenerated — review the diff before committing ($(wc -c < "$OUT") bytes)"
fi
