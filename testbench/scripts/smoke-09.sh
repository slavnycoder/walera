#!/usr/bin/env bash
# =============================================================================
# testbench/scripts/smoke-09.sh — Phase 09 (Observability + Run-book) smoke.
#
# This script is the SINGLE SOURCE OF TRUTH for Phase 09 verification. It maps
# 1:1 onto the four ROADMAP Phase 09 success criteria. Read-only: it issues
# only GET requests against Grafana + Prometheus and inspects on-disk files
# (dashboards, README, failure-mode scripts, workflow YAML). No mutations.
#
# ROADMAP Phase 09 Success Criteria (verbatim from .planning/ROADMAP.md L102-107):
#
#   SC1: Grafana boots with the `walera-prometheus` datasource UID provisioned
#        from filesystem; both dashboards (`walera-overview`, `walera-routing`)
#        load without "datasource not found" errors and render all panels.
#        Anonymous-viewer is enabled so embedding requires no login.
#
#   SC2: The single panel plotting `writer_commit_rate` next to
#        `rate(wal_tx_total[10s])` is visible on `walera-overview.json` — the
#        WRITER-05 "within ±2 %" claim is graphical, not a manual calculation.
#
#        Implementation note: the live walera Prometheus series is
#        `walera_wal_tx_size_changes_count` (histogram _count), NOT
#        `wal_tx_total`. The dashboard plots `rate(walera_wal_tx_size_changes_count[10s])`
#        alongside `writer_commit_rate` — semantically identical to the ROADMAP
#        wording. SC2 below greps for the actual series name.
#
#   SC3: Each of the four run-book scripts (breaker trip + recover, slow
#        consumer disconnect, PG restart, high WAL lag) executes `curl`
#        commands documented in `testbench/README.md` and produces the
#        documented metric change in Grafana within 60 s. The hardware-floor
#        disclaimer is present and prominent in the README.
#
#        CI design decision (locked in Plan 09-03 hand-off and 09-04 plan):
#        the failure-mode scripts are operator-driven local validations and
#        are NOT executed in CI — they take 30-120s each (one cycles
#        postgres) which would blow the 10-min CI budget AND introduce
#        flakiness on Linux runners. SC3 in CI verifies the artefacts
#        exist + are executable + parse cleanly + are wired into the README;
#        live metric reactions are verified manually by an operator.
#
#   SC4: `CI-01` cold-start smoke runs on a Linux GitHub Actions runner:
#        `docker compose -f testbench/docker-compose.yml up -d`, polls
#        `/healthz` until 200 (max 90 s), opens `curl -N` against
#        `/sse/v1/orders/all`, posts a `smoke` scenario to writer `/control`,
#        and exits 0 within 30 s of the first `event: tx` arrival; fails CI
#        red if any healthcheck fails or no event arrives.
#
#        SC4 in this smoke verifies the workflow YAML exists + has the
#        required shape (ubuntu-latest, 10-min timeout, smoke-ci.sh invoked).
#        The workflow ITSELF is the live-runtime verification of CI-01 on
#        push/PR; this smoke is the static gate that prevents the workflow
#        from being silently broken between pushes.
#
# Usage:
#   bash testbench/scripts/smoke-09.sh           # assumes stack is up + healthy
#   bash testbench/scripts/smoke-09.sh --reset   # cold-start the stack first
#
# Prerequisites:
#   - docker compose stack up: at minimum prometheus + grafana healthy
#   - host has: curl, jq, python3 (for YAML lint)
#
# Exit codes:
#   0 = all 4 SCs PASS
#   1 = at least one SC FAILED
#
# Env overrides (all optional):
#   GRAFANA_BASE_URL    default http://127.0.0.1:3000
#   PROM_BASE_URL       default http://127.0.0.1:9090
#   TIMEOUT_SECONDS     default 30
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBENCH_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${TESTBENCH_DIR}/.." && pwd)"

GRAFANA_BASE_URL="${GRAFANA_BASE_URL:-http://127.0.0.1:3000}"
PROM_BASE_URL="${PROM_BASE_URL:-http://127.0.0.1:9090}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-30}"

OVERVIEW_JSON="${REPO_ROOT}/testbench/grafana/dashboards/walera-overview.json"
ROUTING_JSON="${REPO_ROOT}/testbench/grafana/dashboards/walera-routing.json"
README_FILE="${REPO_ROOT}/testbench/README.md"
WORKFLOW_FILE="${REPO_ROOT}/.github/workflows/testbench-smoke.yml"
FAILURE_MODES_DIR="${REPO_ROOT}/testbench/scripts/failure-modes"

# Per-SC pass tracker.
SC1_PASS=0
SC2_PASS=0
SC3_PASS=0
SC4_PASS=0

# ANSI colors only when stdout is a TTY.
if [[ -t 1 ]]; then
  C_PASS=$'\033[32m'; C_FAIL=$'\033[31m'; C_INFO=$'\033[36m'; C_OFF=$'\033[0m'
else
  C_PASS=""; C_FAIL=""; C_INFO=""; C_OFF=""
fi

pass() { echo "${C_PASS}PASS${C_OFF} $*"; }
fail() { echo "${C_FAIL}FAIL${C_OFF} $*"; }
info() { echo "${C_INFO}INFO${C_OFF} $*"; }

# -----------------------------------------------------------------------------
# Optional --reset: cold-start the stack.
# -----------------------------------------------------------------------------
if [[ "${1:-}" == "--reset" ]]; then
  info "--reset: make -C ${TESTBENCH_DIR} demo-reset && demo-up"
  make -C "${TESTBENCH_DIR}" demo-reset
  make -C "${TESTBENCH_DIR}" demo-up
fi

# -----------------------------------------------------------------------------
# wait-for-healthy: grafana + prometheus must be up before SC1/SC2.
# -----------------------------------------------------------------------------
wait_for_healthy() {
  local svc="$1"
  local deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while :; do
    local cid
    cid="$(docker compose -f "${TESTBENCH_DIR}/docker-compose.yml" ps -q "${svc}" 2>/dev/null || true)"
    if [[ -n "${cid}" ]]; then
      local status
      status="$(docker inspect "${cid}" --format '{{.State.Health.Status}}' 2>/dev/null || echo "missing")"
      if [[ "${status}" == "healthy" ]]; then
        pass "service ${svc} is healthy"
        return 0
      fi
      if (( $(date +%s) >= deadline )); then
        fail "service ${svc} did not reach healthy within ${TIMEOUT_SECONDS}s (last: ${status})"
        return 1
      fi
    elif (( $(date +%s) >= deadline )); then
      fail "service ${svc} container not found within ${TIMEOUT_SECONDS}s"
      return 1
    fi
    sleep 1
  done
}

info "Waiting up to ${TIMEOUT_SECONDS}s for prometheus + grafana to become healthy..."
wait_for_healthy prometheus || exit 1
wait_for_healthy grafana || exit 1

# =============================================================================
# SC1 — Grafana datasource UID + both dashboards load + all panel targets pin
# to walera-prometheus.
# =============================================================================
echo
info "===== SC1: Grafana anonymous datasource + dashboards ====="

sc1_ok=1

# SC1.a — /api/health reports the database OK.
sc1_health="$(curl -fsS "${GRAFANA_BASE_URL}/api/health" 2>/dev/null || true)"
if echo "${sc1_health}" | jq -e '.database=="ok"' >/dev/null 2>&1; then
  pass "SC1.a /api/health database=ok"
else
  fail "SC1.a /api/health did not report database=ok: ${sc1_health}"
  sc1_ok=0
fi

# SC1.b — anonymous Viewer can resolve the walera-prometheus datasource by UID.
sc1_ds="$(curl -fsS "${GRAFANA_BASE_URL}/api/datasources/uid/walera-prometheus" 2>/dev/null || true)"
if echo "${sc1_ds}" | jq -e '.uid=="walera-prometheus" and .type=="prometheus"' >/dev/null 2>&1; then
  pass "SC1.b anonymous Viewer resolves datasource uid=walera-prometheus type=prometheus"
else
  fail "SC1.b datasource UID resolution failed (no auth header used)"
  echo "${sc1_ds}" | head -3 | sed 's/^/      /'
  sc1_ok=0
fi

# SC1.c — walera-overview dashboard: UID + 8 panels + every target pins to walera-prometheus.
sc1_overview="$(curl -fsS "${GRAFANA_BASE_URL}/api/dashboards/uid/walera-overview" 2>/dev/null || true)"
if echo "${sc1_overview}" \
  | jq -e '.dashboard.uid=="walera-overview" and (.dashboard.panels | length == 8) and ([.dashboard.panels[].targets[].datasource.uid] | all(. == "walera-prometheus"))' \
    >/dev/null 2>&1; then
  pass "SC1.c walera-overview loads: uid match, 8 panels, every target.datasource.uid=walera-prometheus"
else
  fail "SC1.c walera-overview dashboard load or invariant failed"
  echo "${sc1_overview}" | jq '.dashboard.uid, (.dashboard.panels | length), ([.dashboard.panels[].targets[].datasource.uid] | unique)' 2>/dev/null | sed 's/^/      /'
  sc1_ok=0
fi

# SC1.d — walera-routing dashboard: UID + 2 panels + datasource UID invariant.
sc1_routing="$(curl -fsS "${GRAFANA_BASE_URL}/api/dashboards/uid/walera-routing" 2>/dev/null || true)"
if echo "${sc1_routing}" \
  | jq -e '.dashboard.uid=="walera-routing" and (.dashboard.panels | length == 2) and ([.dashboard.panels[].targets[].datasource.uid] | all(. == "walera-prometheus"))' \
    >/dev/null 2>&1; then
  pass "SC1.d walera-routing loads: uid match, 2 panels, every target.datasource.uid=walera-prometheus"
else
  fail "SC1.d walera-routing dashboard load or invariant failed"
  echo "${sc1_routing}" | jq '.dashboard.uid, (.dashboard.panels | length), ([.dashboard.panels[].targets[].datasource.uid] | unique)' 2>/dev/null | sed 's/^/      /'
  sc1_ok=0
fi

# SC1.e — end-to-end PromQL through the Grafana datasource proxy works (proves
# Grafana → Prometheus wire is alive, not just config-correct).
sc1_proxy="$(curl -fsS "${GRAFANA_BASE_URL}/api/datasources/proxy/uid/walera-prometheus/api/v1/query?query=up" 2>/dev/null || true)"
if echo "${sc1_proxy}" | jq -e '.status=="success" and (.data.result | length >= 2)' >/dev/null 2>&1; then
  pass "SC1.e datasource proxy query 'up' returns >=2 series (walera + writer scrape targets)"
else
  fail "SC1.e datasource proxy query failed or returned <2 series"
  echo "${sc1_proxy}" | head -3 | sed 's/^/      /'
  sc1_ok=0
fi

if (( sc1_ok == 1 )); then
  SC1_PASS=1
  pass "SC1 OVERALL"
else
  fail "SC1 OVERALL"
fi

# =============================================================================
# SC2 — OBS-05 side-by-side writer_commit_rate vs walera tx-rate panel exists.
#
# Checked BOTH on-disk (the JSON in the repo is the source of truth) and
# via the live Grafana API (proves provisioning landed the file).
# =============================================================================
echo
info "===== SC2: OBS-05 side-by-side parity panel (writer_commit_rate + walera tx-rate) ====="

sc2_ok=1

# SC2.a — on-disk dashboard JSON has exactly one panel with both metrics.
if jq -e '[.panels[] | select((.targets | map(.expr) | tostring | contains("writer_commit_rate")) and (.targets | map(.expr) | tostring | contains("walera_wal_tx_size_changes_count")))] | length == 1' \
   "${OVERVIEW_JSON}" >/dev/null 2>&1; then
  pass "SC2.a on-disk walera-overview.json has exactly 1 panel with BOTH writer_commit_rate AND walera_wal_tx_size_changes_count"
else
  fail "SC2.a on-disk walera-overview.json missing the side-by-side panel"
  sc2_ok=0
fi

# SC2.b — live Grafana API returns the same panel via the dashboard load.
sc2_overview_live="$(curl -fsS "${GRAFANA_BASE_URL}/api/dashboards/uid/walera-overview" 2>/dev/null || true)"
if echo "${sc2_overview_live}" \
  | jq -e '[.dashboard.panels[] | select((.targets | map(.expr) | tostring | contains("writer_commit_rate")) and (.targets | map(.expr) | tostring | contains("walera_wal_tx_size_changes_count")))] | length == 1' \
    >/dev/null 2>&1; then
  pass "SC2.b live Grafana API confirms the side-by-side panel is provisioned"
else
  fail "SC2.b live Grafana API does NOT show the side-by-side panel (provisioning regression?)"
  sc2_ok=0
fi

if (( sc2_ok == 1 )); then
  SC2_PASS=1
  pass "SC2 OVERALL"
else
  fail "SC2 OVERALL"
fi

# =============================================================================
# SC3 — failure-mode scripts exist + are executable + README references them
# + hardware-floor disclaimer present. CI does NOT execute the scripts.
# =============================================================================
echo
info "===== SC3: failure-mode scripts + README (DOC-02 + DOC-03) ====="

sc3_ok=1

# SC3.a — all four scripts exist, are executable, and parse with `bash -n`.
sc3_scripts=(fail-auth-breaker fail-slow-consumer fail-pg-restart fail-wal-lag)
for s in "${sc3_scripts[@]}"; do
  p="${FAILURE_MODES_DIR}/${s}.sh"
  if [[ -x "${p}" ]] && bash -n "${p}" 2>/dev/null; then
    pass "SC3.a ${s}.sh exists, executable, parses"
  else
    fail "SC3.a ${s}.sh missing, not executable, or fails bash -n"
    [[ -f "${p}" ]] || echo "      file missing: ${p}"
    [[ -x "${p}" ]] || echo "      not executable: ${p}"
    sc3_ok=0
  fi
done

# SC3.b — hardware-floor disclaimer present (DOC-03).
# The README normalises whitespace ("4.0 / memory: 8G") so we grep for the
# canonical phrasing from the README header.
if grep -Fq 'cpus: 4.0 / memory: 8G' "${README_FILE}"; then
  pass "SC3.b README contains DOC-03 disclaimer 'cpus: 4.0 / memory: 8G'"
else
  fail "SC3.b README missing DOC-03 hardware-floor disclaimer"
  sc3_ok=0
fi

# SC3.c — README references all four failure-mode script paths.
for s in "${sc3_scripts[@]}"; do
  if grep -Fq "scripts/failure-modes/${s}.sh" "${README_FILE}"; then
    pass "SC3.c README references scripts/failure-modes/${s}.sh"
  else
    fail "SC3.c README missing reference to scripts/failure-modes/${s}.sh"
    sc3_ok=0
  fi
done

# SC3.d — README has the four DOC-02 failure-mode sections (distinctive
# phrases from the section headers; tolerant of small wording shifts).
sc3_sections=(
  'Auth circuit breaker trip'
  'Slow consumer disconnect'
  'PostgreSQL restart'
  'High WAL lag'
)
for section in "${sc3_sections[@]}"; do
  if grep -Fq "${section}" "${README_FILE}"; then
    pass "SC3.d README contains failure-mode section: ${section}"
  else
    fail "SC3.d README missing failure-mode section: ${section}"
    sc3_ok=0
  fi
done

# SC3.e — README mentions the documented curl/wget recipes (compose exec
# mock-auth is the AUTH-04 trip recipe; documented across DOC-02 (a)).
if grep -Eq 'docker compose exec[^|]*mock-auth' "${README_FILE}"; then
  pass "SC3.e README contains documented mock-auth exec recipes"
else
  fail "SC3.e README missing mock-auth exec recipes"
  sc3_ok=0
fi

if (( sc3_ok == 1 )); then
  SC3_PASS=1
  pass "SC3 OVERALL"
else
  fail "SC3 OVERALL"
fi

# =============================================================================
# SC4 — CI-01 workflow YAML exists + has required shape.
# =============================================================================
echo
info "===== SC4: CI-01 testbench-smoke.yml workflow shape ====="

sc4_ok=1

# SC4.a — workflow file exists.
if [[ -f "${WORKFLOW_FILE}" ]]; then
  pass "SC4.a .github/workflows/testbench-smoke.yml exists"
else
  fail "SC4.a .github/workflows/testbench-smoke.yml missing"
  sc4_ok=0
fi

# SC4.b — workflow parses as valid YAML.
# Try `python3 -c 'import yaml'` first (matches the plan's verification
# command + the GitHub Actions ubuntu-latest runner which ships pyyaml as
# `python3-yaml` via apt). On dev hosts where the user's python3 is a
# pyyaml-less linuxbrew install, fall back to /usr/bin/python3 (the
# apt-installed system python) or to PyYAML via `python3 -m pip show yaml`
# detection. Last-resort: accept the file as valid if a simple structural
# grep matches (top-level `name:` + `jobs:` block) — better than skipping
# the check entirely.
yaml_check() {
  for py in python3 /usr/bin/python3; do
    if command -v "${py}" >/dev/null 2>&1; then
      if "${py}" -c "import yaml; yaml.safe_load(open('${WORKFLOW_FILE}'))" >/dev/null 2>&1; then
        return 0
      fi
    fi
  done
  return 1
}

if (( sc4_ok == 1 )); then
  if yaml_check; then
    pass "SC4.b workflow YAML parses cleanly (python yaml.safe_load)"
  else
    fail "SC4.b workflow YAML does NOT parse (or no python3 with pyyaml on PATH)"
    sc4_ok=0
  fi
fi

# SC4.c — Linux runner only (ubuntu-latest); no macos / windows.
if (( sc4_ok == 1 )); then
  if grep -Eq '^[[:space:]]+runs-on:[[:space:]]+ubuntu-latest' "${WORKFLOW_FILE}" \
    && ! grep -Eq 'runs-on:[[:space:]]+(macos-|windows-)' "${WORKFLOW_FILE}"; then
    pass "SC4.c workflow uses runs-on: ubuntu-latest (no macOS/Windows)"
  else
    fail "SC4.c workflow runs-on is not exclusively ubuntu-latest"
    sc4_ok=0
  fi
fi

# SC4.d — 10-minute timeout.
if (( sc4_ok == 1 )); then
  if grep -Eq 'timeout-minutes:[[:space:]]+10\b' "${WORKFLOW_FILE}"; then
    pass "SC4.d workflow has timeout-minutes: 10"
  else
    fail "SC4.d workflow missing timeout-minutes: 10"
    sc4_ok=0
  fi
fi

# SC4.e — smoke-ci.sh is invoked.
if (( sc4_ok == 1 )); then
  if grep -Fq 'smoke-ci.sh' "${WORKFLOW_FILE}"; then
    pass "SC4.e workflow invokes testbench/scripts/smoke-ci.sh"
  else
    fail "SC4.e workflow does NOT invoke smoke-ci.sh"
    sc4_ok=0
  fi
fi

# SC4.f — Go setup uses the version pinned in go.mod.
if (( sc4_ok == 1 )); then
  if grep -Fq 'actions/setup-go@v5' "${WORKFLOW_FILE}" && grep -Eq "go-version-file:[[:space:]]*go\\.mod" "${WORKFLOW_FILE}"; then
    pass "SC4.f workflow uses actions/setup-go@v5 with go-version-file: go.mod"
  else
    fail "SC4.f workflow missing setup-go@v5 or go-version-file: go.mod"
    sc4_ok=0
  fi
fi

# SC4.g — compose up --build and compose down -v lifecycle.
if (( sc4_ok == 1 )); then
  if grep -Fq 'compose -f testbench/docker-compose.yml up -d --build' "${WORKFLOW_FILE}" \
    && grep -Fq 'compose -f testbench/docker-compose.yml down -v' "${WORKFLOW_FILE}"; then
    pass "SC4.g workflow has single up→down compose lifecycle"
  else
    fail "SC4.g workflow missing compose up --build or compose down -v"
    sc4_ok=0
  fi
fi

# SC4.h — failure log dump + artifact upload.
if (( sc4_ok == 1 )); then
  if grep -Fq 'if: failure()' "${WORKFLOW_FILE}" && grep -Fq 'upload-artifact' "${WORKFLOW_FILE}"; then
    pass "SC4.h workflow dumps logs + uploads artifact on failure"
  else
    fail "SC4.h workflow missing failure log dump or artifact upload"
    sc4_ok=0
  fi
fi

if (( sc4_ok == 1 )); then
  SC4_PASS=1
  pass "SC4 OVERALL"
else
  fail "SC4 OVERALL"
fi

# =============================================================================
# Summary
# =============================================================================
echo
echo "============================================================"
echo "Phase 09 smoke summary"
echo "============================================================"
printf '  SC1 (Grafana anon datasource + both dashboards) : %s\n' "$( (( SC1_PASS == 1 )) && echo PASS || echo FAIL )"
printf '  SC2 (OBS-05 side-by-side parity panel)          : %s\n' "$( (( SC2_PASS == 1 )) && echo PASS || echo FAIL )"
printf '  SC3 (failure-mode scripts + README DOC-02/03)   : %s\n' "$( (( SC3_PASS == 1 )) && echo PASS || echo FAIL )"
printf '  SC4 (testbench-smoke.yml CI-01 workflow shape)  : %s\n' "$( (( SC4_PASS == 1 )) && echo PASS || echo FAIL )"
echo "============================================================"

if (( SC1_PASS == 1 && SC2_PASS == 1 && SC3_PASS == 1 && SC4_PASS == 1 )); then
  pass "Phase 09 smoke PASSED (SC1+SC2+SC3+SC4)"
  exit 0
else
  fail "Phase 09 smoke FAILED"
  exit 1
fi
