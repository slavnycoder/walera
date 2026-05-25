#!/usr/bin/env bash
# deploy/scripts/preflight.sh — pre-deploy guard against un-substituted marker tokens.
#
# Purpose
#   Walera ships deploy/ artefacts (PrometheusRule, ServiceMonitor, ...) with
#   a deliberate operator-substitution tripwire on operator-configurable labels
#   (notably the `release:` label that Prometheus Operator's `ruleSelector` /
#   `serviceMonitorSelector` matches against — Pitfall G7). The tripwire lives
#   in source so an operator who blindly `kubectl apply`s the unmodified
#   manifests gets alerts that silently never fire.
#
#   This script is the deterministic guard: AFTER the operator runs their
#   substitution step (sed / kustomize / helm overlay) AND BEFORE
#   `kubectl apply`, run this to confirm no marker survived.
#
# Exit codes
#   0  — clean: no un-substituted marker under the deploy tree (excluding README
#        files and this script itself).
#   1  — un-substituted markers found; each offending path:lineno is printed to
#        stderr, followed by a one-line remediation hint pointing operators at
#        deploy/prometheus/README.md.
#
# Idempotency
#   Pure read-only over the filesystem; repeated invocations against the same
#   tree produce byte-identical output. Safe to call from CI and from operator
#   workstations.
#
# Usage
#   deploy/scripts/preflight.sh                # auto-discovers the deploy/ tree
#   deploy/scripts/preflight.sh /path/to/deploy  # explicit tree path

set -euo pipefail

# The marker token is constructed at runtime rather than hard-coded so that the
# preflight script itself does not match its own search pattern. Operators
# never need to read this constant — it must remain "REPLACE" + "_ME".
MARKER="REPLACE""_ME"

# Discover the deploy/ tree:
#   - If $1 is given, treat it as the deploy-tree root.
#   - Otherwise, walk up one level from this script's own directory
#     (deploy/scripts/preflight.sh → deploy/). This lets operators invoke the
#     script from the repo root, from inside deploy/, or by absolute path.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DIR="${1:-${DEFAULT_DIR}}"

if [ ! -d "${DIR}" ]; then
  echo "preflight: ERROR — deploy tree not found at ${DIR}" >&2
  exit 1
fi

# Recursively search for the marker token, excluding README files
# (case-insensitive) and the preflight script itself. `|| true` is required
# because grep exits 1 when there are zero matches and `set -e` would otherwise
# abort here.
matches="$(grep -rn \
  --exclude='README*' --exclude='readme*' --exclude='Readme*' \
  --exclude='preflight.sh' \
  "${MARKER}" "${DIR}" || true)"

if [ -n "${matches}" ]; then
  printf '%s\n' "${matches}" >&2
  echo "preflight: FAIL — un-substituted ${MARKER} markers found under ${DIR}" >&2
  echo "preflight: see deploy/prometheus/README.md (Required Label Customization) for the substitution workflow." >&2
  exit 1
fi

echo "preflight: OK — no ${MARKER} markers detected under ${DIR}"
exit 0
