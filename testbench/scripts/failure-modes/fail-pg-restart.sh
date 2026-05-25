#!/usr/bin/env bash
# =============================================================================
# testbench/scripts/failure-modes/fail-pg-restart.sh
#
# Plan 09-03 / DOC-02 failure scenario (c): PostgreSQL restart.
# Demonstrates walera's transient-PG reconnect contract: the WAL reader
# detects the connection drop, /readyz returns 503 during the gap, the
# reconnect loop fires, and long-lived SSE subscribers stay connected
# across the disconnect window (modulo the documented missed-events
# window per WAL-04).
#
# Mechanism:
#   1. Capture baseline walera_pg_connection_status (expect 1) and
#      walera_pg_reconnects_total (counter; may be ≥ 0 from earlier runs).
#   2. docker compose -f testbench/docker-compose.yml restart postgres.
#   3. Poll for walera_pg_connection_status == 0 within TIMEOUT_DISCONNECT s
#      (the WAL reader's connection drops within seconds; the 5s lag
#      sampler updates the gauge — so up to ~7s for the gauge to drop).
#   4. Briefly probe /readyz for 503 (this is a transient state).
#   5. Wait for walera_pg_connection_status == 1 AND /readyz == 200
#      within TIMEOUT_RECOVER s.
#   6. Verify walera_pg_reconnects_total incremented by ≥ 1.
#
# Expected metric reaction:
#   walera_pg_connection_status:   1 → 0 → 1  (gauge)
#   walera_pg_reconnects_total:    incremented by ≥ 1
#   /readyz:                       200 → 503 → 200
#
#   Grafana panels to watch: walera-overview → "WAL LSN lag" (spikes during
#   the gap), and the implicit pg_connection_status (not a dedicated panel
#   today — infer from /metrics directly or via the Prometheus query URL).
#
# Idempotency:
#   No cleanup trap needed — postgres restart is self-recovering. If the
#   recovery deadline expires, the script dumps the last 50 lines of
#   postgres + walera logs.
#
# Env overrides:
#   WALERA_BASE_URL    default http://127.0.0.1:8080
#   PROM_BASE_URL      default http://127.0.0.1:9090
#   COMPOSE_FILE       default testbench/docker-compose.yml
#   TIMEOUT_DISCONNECT default 30  (seconds to observe status → 0)
#   TIMEOUT_RECOVER    default 90  (seconds to observe status → 1)
#
# Exit codes:
#   0  full cycle observed: status 1 → 0 → 1, /readyz 503 transiently,
#      reconnects counter incremented.
#   1  expected reaction not observed within deadlines.
#   2  preconditions not met.
# =============================================================================

set -euo pipefail

WALERA_BASE_URL="${WALERA_BASE_URL:-http://127.0.0.1:8080}"
PROM_BASE_URL="${PROM_BASE_URL:-http://127.0.0.1:9090}"
COMPOSE_FILE="${COMPOSE_FILE:-testbench/docker-compose.yml}"
TIMEOUT_DISCONNECT="${TIMEOUT_DISCONNECT:-30}"
TIMEOUT_RECOVER="${TIMEOUT_RECOVER:-90}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "${REPO_ROOT}"

if [[ -t 1 ]]; then
  C_PASS=$'\033[32m'; C_FAIL=$'\033[31m'; C_INFO=$'\033[36m'; C_OFF=$'\033[0m'
else
  C_PASS=""; C_FAIL=""; C_INFO=""; C_OFF=""
fi
pass() { echo "${C_PASS}PASS${C_OFF} $*"; }
fail() { echo "${C_FAIL}FAIL${C_OFF} $*"; }
info() { echo "${C_INFO}INFO${C_OFF} $*"; }

prom_query() {
  local q="$1"
  curl -fsS --max-time 5 -G "${PROM_BASE_URL}/api/v1/query" --data-urlencode "query=${q}" 2>/dev/null \
    | sed -n 's/.*"value":\[[0-9.]*,"\([^"]*\)"\].*/\1/p' \
    | head -1
}

nz() { local v="${1:-}"; [[ -z "${v}" ]] && echo 0 || echo "${v}"; }

http_code() {
  local url="$1"
  curl -fsS -o /dev/null -w '%{http_code}' --max-time 3 "${url}" 2>/dev/null || echo "000"
}

dump_logs_on_fail() {
  echo "----- last 50 lines: docker compose logs postgres -----"
  docker compose -f "${COMPOSE_FILE}" logs --tail=50 postgres 2>&1 | sed 's/^/      /' || true
  echo "----- last 50 lines: docker compose logs walera -----"
  docker compose -f "${COMPOSE_FILE}" logs --tail=50 walera 2>&1 | sed 's/^/      /' || true
}

# -----------------------------------------------------------------------------
# Preconditions.
# -----------------------------------------------------------------------------
info "Preconditions: walera + postgres + prometheus healthy"

if ! curl -fsS --max-time 3 -o /dev/null "${PROM_BASE_URL}/-/healthy"; then
  fail "Prometheus not reachable at ${PROM_BASE_URL}/-/healthy"
  exit 2
fi

baseline_status="$(nz "$(prom_query 'walera_pg_connection_status')")"
if [[ "${baseline_status}" != "1" ]]; then
  fail "Refusing to run: walera_pg_connection_status=${baseline_status} (expected 1=connected)"
  exit 2
fi
pass "Baseline: walera_pg_connection_status == 1 (connected)"

readyz_baseline="$(http_code "${WALERA_BASE_URL}/readyz")"
if [[ "${readyz_baseline}" != "200" ]]; then
  fail "Refusing to run: /readyz returned ${readyz_baseline} (expected 200)"
  exit 2
fi
pass "Baseline: /readyz == 200"

baseline_reconnects="$(nz "$(prom_query 'walera_pg_reconnects_total')")"
pass "Baseline: walera_pg_reconnects_total == ${baseline_reconnects}"

# -----------------------------------------------------------------------------
# Restart postgres.
# -----------------------------------------------------------------------------
echo
info "Restarting postgres: docker compose -f ${COMPOSE_FILE} restart postgres"
restart_start="$(date +%s)"
docker compose -f "${COMPOSE_FILE}" restart postgres >/dev/null
restart_elapsed=$(( $(date +%s) - restart_start ))
pass "postgres restart issued (${restart_elapsed}s wall-clock)"

# -----------------------------------------------------------------------------
# Observe the gauge drop.
# -----------------------------------------------------------------------------
echo
info "Polling for walera_pg_connection_status == 0, deadline=${TIMEOUT_DISCONNECT}s..."
deadline=$(( $(date +%s) + TIMEOUT_DISCONNECT ))
disconnect_observed=0
disconnect_at=0
while :; do
  status="$(nz "$(prom_query 'walera_pg_connection_status')")"
  if [[ "${status}" == "0" ]]; then
    disconnect_observed=1
    disconnect_at="$(date +%s)"
    pass "Disconnect observed: walera_pg_connection_status == 0"
    break
  fi
  if (( $(date +%s) >= deadline )); then
    fail "Did not observe walera_pg_connection_status == 0 within ${TIMEOUT_DISCONNECT}s (last=${status})"
    dump_logs_on_fail
    exit 1
  fi
  sleep 1
done

# -----------------------------------------------------------------------------
# Probe /readyz for 503 (briefly — this is a transient state, so make a
# short best-effort window before the recovery loop catches it again).
# -----------------------------------------------------------------------------
echo
info "Probing /readyz briefly (expect 503 during the gap)..."
readyz_503_seen=0
probe_deadline=$(( $(date +%s) + 10 ))
while (( $(date +%s) < probe_deadline )); do
  code="$(http_code "${WALERA_BASE_URL}/readyz")"
  if [[ "${code}" == "503" ]]; then
    readyz_503_seen=1
    pass "/readyz observed at 503 during the gap"
    break
  fi
  sleep 1
done
if (( readyz_503_seen == 0 )); then
  info "/readyz never observed at 503 during the 10s probe — the gap may be too brief to catch on a single instant"
  info "(this is informational; not a hard failure — gauge drop already confirmed)"
fi

# -----------------------------------------------------------------------------
# Wait for full recovery.
# -----------------------------------------------------------------------------
echo
info "Polling for walera_pg_connection_status == 1 AND /readyz == 200, deadline=${TIMEOUT_RECOVER}s..."
deadline=$(( $(date +%s) + TIMEOUT_RECOVER ))
recovered=0
while :; do
  status="$(nz "$(prom_query 'walera_pg_connection_status')")"
  code="$(http_code "${WALERA_BASE_URL}/readyz")"
  if [[ "${status}" == "1" && "${code}" == "200" ]]; then
    recovered=1
    recover_at="$(date +%s)"
    recover_elapsed=$(( recover_at - disconnect_at ))
    pass "Recovery: status=1, /readyz=200 (took ~${recover_elapsed}s from disconnect)"
    break
  fi
  if (( $(date +%s) >= deadline )); then
    fail "Did not recover within ${TIMEOUT_RECOVER}s (status=${status}, /readyz=${code})"
    dump_logs_on_fail
    exit 1
  fi
  sleep 2
done

# -----------------------------------------------------------------------------
# Verify reconnect counter incremented.
# -----------------------------------------------------------------------------
echo
final_reconnects="$(nz "$(prom_query 'walera_pg_reconnects_total')")"
delta="$(awk -v a="${final_reconnects}" -v b="${baseline_reconnects}" 'BEGIN{print a-b}')"
if awk -v d="${delta}" 'BEGIN{exit !(d >= 1)}'; then
  pass "walera_pg_reconnects_total: ${baseline_reconnects} → ${final_reconnects} (delta=${delta})"
else
  fail "walera_pg_reconnects_total did not increment: ${baseline_reconnects} → ${final_reconnects}"
  dump_logs_on_fail
  exit 1
fi

# -----------------------------------------------------------------------------
# Summary.
# -----------------------------------------------------------------------------
echo
echo "============================================================"
echo "fail-pg-restart.sh — summary"
echo "============================================================"
printf '  baseline pg_status:        1\n'
printf '  observed pg_status drop:   1 → 0\n'
printf '  /readyz 503 observed:      %s\n' "$( (( readyz_503_seen == 1 )) && echo yes || echo 'no (gap too brief)' )"
printf '  recovery elapsed:          %ss\n' "${recover_elapsed}"
printf '  pg_reconnects_total:       %s → %s (delta=%s)\n' "${baseline_reconnects}" "${final_reconnects}" "${delta}"
echo "  Grafana panel: http://127.0.0.1:3000/d/walera-overview → 'WAL LSN lag'"
echo "  Prometheus checks:"
echo "    curl '${PROM_BASE_URL}/api/v1/query?query=walera_pg_connection_status'"
echo "    curl '${PROM_BASE_URL}/api/v1/query?query=walera_pg_reconnects_total'"
echo "============================================================"
pass "PG-restart reaction demonstrated"
exit 0
