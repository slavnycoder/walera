#!/usr/bin/env bash
# =============================================================================
# testbench/scripts/failure-modes/fail-auth-breaker.sh
#
# Plan 09-03 / DOC-02 failure scenario (a): AUTH-04 auth circuit breaker trip
# + recover. Demonstrates walera's bounded fail-open / fail-closed semantics:
# when the auth backend fails >50% of requests over the breaker's rolling
# window, the breaker opens (new opens fail-closed, existing subs hold);
# when the backend recovers, half-open trial successes close the breaker.
#
# Mechanism:
#   1. POST /_admin/fail-on against mock-auth so every /auth/permissions
#      returns 500.
#   2. Drive ~20 short SSE opens against walera over ~20s — each open
#      triggers an auth call that now fails 500, filling the breaker's
#      failure window.
#   3. Poll Prometheus for walera_auth_circuit_breaker_state == 1 (Open).
#   4. POST /_admin/fail-off, then poll for state == 0 (Closed).
#
# Expected metric reaction:
#   walera_auth_circuit_breaker_state transitions 0 → 1 within TIMEOUT_TRIP s,
#   then 1 → 0 within TIMEOUT_RECOVER s after fail-off. Grafana panel to
#   watch: walera-overview → "Auth circuit breaker state".
#
# Prometheus check URL:
#   http://127.0.0.1:9090/api/v1/query?query=walera_auth_circuit_breaker_state
#
# Idempotency:
#   On any exit (success, failure, signal), an EXIT trap unconditionally
#   issues /_admin/fail-off so the system is left in a known-good state.
#   Safe to re-run.
#
# Env overrides:
#   WALERA_BASE_URL    default http://127.0.0.1:8080
#   PROM_BASE_URL      default http://127.0.0.1:9090
#   TOKEN              default demo-alice
#   ORIGIN             default http://localhost:8081
#   TIMEOUT_TRIP       default 90    (seconds to wait for state → 1)
#   TIMEOUT_RECOVER    default 120   (seconds to wait for state → 0)
#   COMPOSE_FILE       default testbench/docker-compose.yml
#
# Exit codes:
#   0  breaker tripped 0→1 and recovered 1→0 within the configured timeouts.
#   1  expected metric reaction not observed within the deadline.
#   2  preconditions not met (stack not healthy, or breaker already non-zero).
# =============================================================================

set -euo pipefail

WALERA_BASE_URL="${WALERA_BASE_URL:-http://127.0.0.1:8080}"
PROM_BASE_URL="${PROM_BASE_URL:-http://127.0.0.1:9090}"
TOKEN="${TOKEN:-demo-alice}"
ORIGIN="${ORIGIN:-http://localhost:8081}"
TIMEOUT_TRIP="${TIMEOUT_TRIP:-90}"
TIMEOUT_RECOVER="${TIMEOUT_RECOVER:-120}"
COMPOSE_FILE="${COMPOSE_FILE:-testbench/docker-compose.yml}"

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

# Prometheus query helper — returns the scalar value of the first series, or
# empty on no-data / error.
prom_query() {
  local q="$1"
  curl -fsS --max-time 5 -G "${PROM_BASE_URL}/api/v1/query" --data-urlencode "query=${q}" 2>/dev/null \
    | sed -n 's/.*"value":\[[0-9.]*,"\([^"]*\)"\].*/\1/p' \
    | head -1
}

mock_auth_admin() {
  local action="$1"   # fail-on | fail-off
  # busybox wget in python:alpine does NOT support --method=POST; the
  # canonical busybox idiom is `--post-data` (empty value sends an empty
  # body, which is what the mock-auth admin endpoints expect). Use
  # 127.0.0.1 explicitly so the v4 bind always wins over a potentially
  # broken `localhost` AAAA resolution inside the container.
  docker compose -f "${COMPOSE_FILE}" exec -T mock-auth \
    wget -q -O- --post-data='' "http://127.0.0.1:9000/_admin/${action}" \
    >/dev/null 2>&1
}

dump_logs_on_fail() {
  echo "----- last 30 lines: docker compose logs walera -----"
  docker compose -f "${COMPOSE_FILE}" logs --tail=30 walera 2>&1 | sed 's/^/      /' || true
  echo "----- last 30 lines: docker compose logs mock-auth -----"
  docker compose -f "${COMPOSE_FILE}" logs --tail=30 mock-auth 2>&1 | sed 's/^/      /' || true
}

# Idempotency: EXIT trap unconditionally clears fail-on so the system is left
# in a known-good state regardless of how the script exits.
cleanup() {
  local ec=$?
  info "EXIT trap: ensuring mock-auth fail-off (script exit=${ec})"
  mock_auth_admin fail-off || info "fail-off best-effort suppressed"
  exit $ec
}
trap cleanup EXIT

# -----------------------------------------------------------------------------
# Preconditions.
# -----------------------------------------------------------------------------
info "Preconditions: walera + mock-auth + prometheus reachable, breaker == 0"

if ! curl -fsS --max-time 3 -o /dev/null "${PROM_BASE_URL}/-/healthy"; then
  fail "Prometheus not reachable at ${PROM_BASE_URL}/-/healthy"
  exit 2
fi
if ! curl -fsS --max-time 3 -o /dev/null "${WALERA_BASE_URL}/healthz"; then
  fail "walera not reachable at ${WALERA_BASE_URL}/healthz"
  exit 2
fi
if ! docker compose -f "${COMPOSE_FILE}" ps --status running --services 2>/dev/null | grep -Fxq mock-auth; then
  fail "mock-auth container not running"
  exit 2
fi

baseline_state="$(prom_query 'walera_auth_circuit_breaker_state')"
if [[ "${baseline_state}" != "0" ]]; then
  fail "Refusing to run: walera_auth_circuit_breaker_state=${baseline_state:-<empty>} (expected 0=Closed)"
  fail "Either an earlier failure-mode run left it non-zero or auth is already failing."
  exit 2
fi
pass "Baseline: walera_auth_circuit_breaker_state == 0 (Closed)"

# -----------------------------------------------------------------------------
# Trip the breaker.
# -----------------------------------------------------------------------------
echo
info "Tripping: docker compose exec mock-auth POST /_admin/fail-on"
if ! mock_auth_admin fail-on; then
  fail "mock-auth /_admin/fail-on POST failed"
  exit 1
fi
pass "mock-auth FAIL_MODE = on"

info "Driving ~20 short SSE opens to fill the breaker's failure window..."
trip_start="$(date +%s)"
for i in $(seq 1 20); do
  curl -sN --max-time 1 \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Origin: ${ORIGIN}" \
    "${WALERA_BASE_URL}/sse/v1/orders/${i}" >/dev/null 2>&1 || true
done
info "Drove 20 short opens in $(( $(date +%s) - trip_start ))s"

info "Polling walera_auth_circuit_breaker_state for value 1 (Open), deadline=${TIMEOUT_TRIP}s..."
deadline=$(( $(date +%s) + TIMEOUT_TRIP ))
peak_state=""
while :; do
  state="$(prom_query 'walera_auth_circuit_breaker_state')"
  peak_state="${state}"
  if [[ "${state}" == "1" || "${state}" == "2" ]]; then
    pass "Breaker observed in non-Closed state: walera_auth_circuit_breaker_state=${state}"
    break
  fi
  if (( $(date +%s) >= deadline )); then
    fail "Breaker did not open within ${TIMEOUT_TRIP}s (last observed: ${state:-<empty>})"
    # Keep extra context: how many auth calls did mock-auth log as failing?
    info "mock-auth recent log (last 15 lines):"
    docker compose -f "${COMPOSE_FILE}" logs --tail=15 mock-auth 2>&1 | sed 's/^/      /' || true
    dump_logs_on_fail
    exit 1
  fi
  # Drive a few more opens to keep failures flowing while we wait
  for i in 1 2 3; do
    curl -sN --max-time 1 \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Origin: ${ORIGIN}" \
      "${WALERA_BASE_URL}/sse/v1/orders/${i}" >/dev/null 2>&1 || true
  done
  sleep 2
done

# -----------------------------------------------------------------------------
# Recover.
# -----------------------------------------------------------------------------
echo
info "Recovering: docker compose exec mock-auth POST /_admin/fail-off"
if ! mock_auth_admin fail-off; then
  fail "mock-auth /_admin/fail-off POST failed"
  exit 1
fi
pass "mock-auth FAIL_MODE = off"

info "Polling walera_auth_circuit_breaker_state for value 0 (Closed), deadline=${TIMEOUT_RECOVER}s..."
recover_start="$(date +%s)"
deadline=$(( recover_start + TIMEOUT_RECOVER ))
final_state=""
while :; do
  state="$(prom_query 'walera_auth_circuit_breaker_state')"
  final_state="${state}"
  if [[ "${state}" == "0" ]]; then
    pass "Breaker closed: walera_auth_circuit_breaker_state == 0"
    break
  fi
  if (( $(date +%s) >= deadline )); then
    fail "Breaker did not close within ${TIMEOUT_RECOVER}s (last observed: ${state:-<empty>})"
    dump_logs_on_fail
    exit 1
  fi
  # Drive a few opens — successes feed half-open trial transitions.
  for i in 1 2 3; do
    curl -sN --max-time 1 \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Origin: ${ORIGIN}" \
      "${WALERA_BASE_URL}/sse/v1/orders/${i}" >/dev/null 2>&1 || true
  done
  sleep 2
done
recover_elapsed=$(( $(date +%s) - recover_start ))

# -----------------------------------------------------------------------------
# Summary.
# -----------------------------------------------------------------------------
echo
echo "============================================================"
echo "fail-auth-breaker.sh — summary"
echo "============================================================"
printf '  baseline state:    0 (Closed)\n'
printf '  peak state:        %s\n' "${peak_state}"
printf '  final state:       %s (Closed)\n' "${final_state}"
printf '  recover elapsed:   %ds\n' "${recover_elapsed}"
echo "  Grafana panel: http://127.0.0.1:3000/d/walera-overview"
echo "                  → 'Auth circuit breaker state'"
echo "  Prometheus check:"
echo "    curl '${PROM_BASE_URL}/api/v1/query?query=walera_auth_circuit_breaker_state'"
echo "============================================================"
pass "AUTH-04 breaker trip + recover demonstrated"
exit 0
