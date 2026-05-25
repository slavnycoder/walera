#!/usr/bin/env bash
# =============================================================================
# testbench/scripts/smoke-ci.sh — Umbrella runner for all per-phase smokes.
#
# Runs smoke-{05,06,07,08,09}.sh in order, aggregates pass/fail, exits 0 only
# if all five pass. This is the test-runner invoked by the GitHub Actions
# workflow (.github/workflows/testbench-smoke.yml) AFTER the workflow has
# brought the compose stack up.
#
# Ordering rationale (matches the phase ship order):
#   smoke-05 — substrate (postgres + mock-auth)
#   smoke-06 — walera (SSE + CORS + h2c)
#   smoke-07 — writer (Poisson load + ±2 % parity)
#   smoke-08 — demo UI (frontend + CORS + visibility wiring)
#   smoke-09 — observability (Grafana + Prometheus + failure-mode artefacts + CI workflow)
#
# Failure semantics:
#   - Each smoke runs independently; one smoke's failure does NOT short-circuit
#     the others. Running all five gives the operator the full regression
#     picture in one CI run.
#   - Per-smoke exit code captured under `set +e; …; rc=$?; set -e`.
#   - Aggregate exit = 0 iff every smoke exited 0; otherwise 1.
#
# This script does NOT manage the compose lifecycle. The workflow (or the
# operator running it locally) owns `docker compose up -d` and
# `docker compose down`. Keeping the lifecycle out of the umbrella means
# `smoke-ci.sh` runs identically against an already-warm dev stack and a
# fresh CI runner.
#
# Usage:
#   bash testbench/scripts/smoke-ci.sh
#
# Env overrides (all optional):
#   SMOKE_SUBSET   comma-separated list of smoke names to run (e.g.
#                  "smoke-05,smoke-09"); default = run all five. Useful for
#                  local iteration ("just re-run the observability one").
#
#   Env vars consumed by individual smokes (TOKEN, WALERA_BASE_URL,
#   WRITER_BASE_URL, ORIGIN, GRAFANA_BASE_URL, PROM_BASE_URL, …) propagate
#   to children naturally — no extra plumbing needed here.
#
# Exit codes:
#   0 = all selected smokes PASS
#   1 = at least one selected smoke FAILED (see the aggregate table at the end)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Canonical ordered list of smoke names.
ALL_SMOKES=(smoke-05 smoke-06 smoke-07 smoke-08 smoke-09)

# Resolve the subset to run.
if [[ -n "${SMOKE_SUBSET:-}" ]]; then
  IFS=',' read -ra REQUESTED <<< "${SMOKE_SUBSET}"
  SMOKES=()
  for n in "${REQUESTED[@]}"; do
    # Trim whitespace.
    n="${n#"${n%%[![:space:]]*}"}"
    n="${n%"${n##*[![:space:]]}"}"
    [[ -z "${n}" ]] && continue
    SMOKES+=("${n}")
  done
else
  SMOKES=("${ALL_SMOKES[@]}")
fi

# ANSI colors only on TTY.
if [[ -t 1 ]]; then
  C_PASS=$'\033[32m'; C_FAIL=$'\033[31m'; C_INFO=$'\033[36m'; C_HEAD=$'\033[1;35m'; C_OFF=$'\033[0m'
else
  C_PASS=""; C_FAIL=""; C_INFO=""; C_HEAD=""; C_OFF=""
fi

banner() {
  echo
  echo "${C_HEAD}============================================================${C_OFF}"
  echo "${C_HEAD}  $1${C_OFF}"
  echo "${C_HEAD}============================================================${C_OFF}"
}

# Parallel arrays — bash 3.2 portable (the macOS system bash and some
# minimal Linux containers do not ship associative arrays).
NAMES=()
RCS=()
DURATIONS=()

TOTAL_START="$(date +%s)"

# -----------------------------------------------------------------------------
# Run each smoke in order. Capture exit code without aborting the umbrella.
# -----------------------------------------------------------------------------
for name in "${SMOKES[@]}"; do
  script="${SCRIPT_DIR}/${name}.sh"

  banner "Running ${name} — $(date -u +"%Y-%m-%dT%H:%M:%SZ")"

  if [[ ! -x "${script}" ]]; then
    echo "${C_FAIL}MISSING${C_OFF} ${script} not found or not executable"
    NAMES+=("${name}")
    RCS+=(127)
    DURATIONS+=(0)
    continue
  fi

  start_epoch="$(date +%s)"
  set +e
  bash "${script}"
  rc=$?
  set -e
  end_epoch="$(date +%s)"
  elapsed=$(( end_epoch - start_epoch ))

  NAMES+=("${name}")
  RCS+=("${rc}")
  DURATIONS+=("${elapsed}")

  if (( rc == 0 )); then
    echo "${C_PASS}>>> ${name} PASSED in ${elapsed}s${C_OFF}"
  else
    echo "${C_FAIL}>>> ${name} FAILED (exit=${rc}) in ${elapsed}s${C_OFF}"
  fi
done

TOTAL_END="$(date +%s)"
TOTAL_ELAPSED=$(( TOTAL_END - TOTAL_START ))

# -----------------------------------------------------------------------------
# Aggregate summary table + exit decision.
# -----------------------------------------------------------------------------
banner "Umbrella smoke summary (smoke-ci.sh)"

printf '  %-12s  %-7s  %-10s\n' "smoke" "result" "elapsed"
printf '  %-12s  %-7s  %-10s\n' "-----" "------" "-------"

all_pass=1
for i in "${!NAMES[@]}"; do
  if (( RCS[i] == 0 )); then
    result="${C_PASS}PASS${C_OFF}"
  else
    result="${C_FAIL}FAIL${C_OFF}"
    all_pass=0
  fi
  printf '  %-12s  %b  %4ss\n' "${NAMES[i]}" "${result}" "${DURATIONS[i]}"
done

echo
echo "  total elapsed: ${TOTAL_ELAPSED}s"

if (( all_pass == 1 )); then
  echo
  echo "${C_PASS}smoke-ci PASSED${C_OFF} — all ${#SMOKES[@]} smoke(s) green"
  exit 0
else
  echo
  echo "${C_FAIL}smoke-ci FAILED${C_OFF} — at least one smoke red (see table above)"
  exit 1
fi
