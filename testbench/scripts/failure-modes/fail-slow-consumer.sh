#!/usr/bin/env bash
# =============================================================================
# testbench/scripts/failure-modes/fail-slow-consumer.sh
#
# Plan 09-03 / DOC-02 failure scenario (b): slow_consumer disconnect.
# Demonstrates the per-subscriber bounded-channel contract: a client that
# cannot drain the SSE channel fast enough is disconnected with
# `event: error reason=slow_consumer` rather than blocking the broadcaster.
#
# Mechanism:
#   1. Switch writer to spike@500 tx/s × 10 rows so walera fans out heavy
#      tx traffic.
#   2. Launch an SSE curl in the background that writes to a temp file
#      whose reader is never drained — the OS pipe buffer (~64 KiB on
#      Linux) fills, the curl recv blocks, walera's per-subscriber
#      bounded channel fills, walera kicks the client.
#   3. Poll Prometheus for either
#        walera_subscriber_disconnects_total{reason="slow_consumer"}
#      OR walera_tx_dropped_total{reason="slow_consumer"}
#      to increment vs baseline. Both are documented reactions.
#
# Expected metric reaction:
#   At least one of the two counters above increments by ≥ 1 within
#   TIMEOUT_SECONDS. Grafana panel to watch: walera-overview →
#   "Tx dropped by reason" (slow_consumer series).
#
# Prometheus check URLs:
#   http://127.0.0.1:9090/api/v1/query?query=walera_subscriber_disconnects_total{reason="slow_consumer"}
#   http://127.0.0.1:9090/api/v1/query?query=walera_tx_dropped_total{reason="slow_consumer"}
#
# Idempotency:
#   EXIT trap kills the background slow-consumer curl and restores the
#   writer to smoke@5×1 so the bench returns to baseline. Safe to re-run.
#
# Env overrides:
#   WALERA_BASE_URL    default http://127.0.0.1:8080
#   PROM_BASE_URL      default http://127.0.0.1:9090
#   WRITER_BASE_URL    default http://127.0.0.1:9100
#   TOKEN              default demo-alice
#   ORIGIN             default http://localhost:8081
#   TIMEOUT_SECONDS    default 90
#   COMPOSE_FILE       default testbench/docker-compose.yml
#
# Exit codes:
#   0  at least one slow_consumer counter incremented within the deadline.
#   1  neither counter incremented within the deadline.
#   2  preconditions not met (stack not healthy).
# =============================================================================

set -euo pipefail

WALERA_BASE_URL="${WALERA_BASE_URL:-http://127.0.0.1:8080}"
PROM_BASE_URL="${PROM_BASE_URL:-http://127.0.0.1:9090}"
WRITER_BASE_URL="${WRITER_BASE_URL:-http://127.0.0.1:9100}"
TOKEN="${TOKEN:-demo-alice}"
ORIGIN="${ORIGIN:-http://localhost:8081}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-90}"
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

prom_query() {
  local q="$1"
  curl -fsS --max-time 5 -G "${PROM_BASE_URL}/api/v1/query" --data-urlencode "query=${q}" 2>/dev/null \
    | sed -n 's/.*"value":\[[0-9.]*,"\([^"]*\)"\].*/\1/p' \
    | head -1
}

# numeric-or-zero — empty string from prom_query (no series) is treated as 0.
nz() { local v="${1:-}"; [[ -z "${v}" ]] && echo 0 || echo "${v}"; }

set_writer_scenario() {
  local name="$1" rate="$2" rows="$3"
  curl -fsS --max-time 5 -X POST "${WRITER_BASE_URL}/control" \
    -H 'Content-Type: application/json' \
    -d "{\"commit_rate\":${rate},\"rows_per_tx\":${rows},\"scenario\":\"${name}\"}" \
    >/dev/null
}

CLIENT_PID=""
TMP_SSE="$(mktemp)"

cleanup() {
  local ec=$?
  info "EXIT trap: cleaning up (script exit=${ec})"
  if [[ -n "${CLIENT_PID}" ]] && kill -0 "${CLIENT_PID}" 2>/dev/null; then
    # The curl was deliberately SIGSTOP'd to fill its socket recv buffer.
    # SIGTERM cannot interrupt a stopped process — it queues. Send SIGCONT
    # first to un-pause, then SIGKILL (more reliable than SIGTERM against a
    # process that may be blocked in recv).
    kill -CONT "${CLIENT_PID}" 2>/dev/null || true
    kill -KILL "${CLIENT_PID}" 2>/dev/null || true
    wait "${CLIENT_PID}" 2>/dev/null || true
    info "killed background SSE client pid=${CLIENT_PID}"
  fi
  rm -f "${TMP_SSE}"
  info "restoring writer to smoke@5×1"
  set_writer_scenario smoke 5 1 || info "writer scenario restore best-effort suppressed"
  exit $ec
}
trap cleanup EXIT

# -----------------------------------------------------------------------------
# Preconditions.
# -----------------------------------------------------------------------------
info "Preconditions: walera + writer + prometheus reachable"

if ! curl -fsS --max-time 3 -o /dev/null "${PROM_BASE_URL}/-/healthy"; then
  fail "Prometheus not reachable at ${PROM_BASE_URL}/-/healthy"
  exit 2
fi
if ! curl -fsS --max-time 3 -o /dev/null "${WALERA_BASE_URL}/healthz"; then
  fail "walera not reachable at ${WALERA_BASE_URL}/healthz"
  exit 2
fi
if ! curl -fsS --max-time 3 -o /dev/null "${WRITER_BASE_URL}/healthz"; then
  fail "writer not reachable at ${WRITER_BASE_URL}/healthz"
  exit 2
fi

baseline_disc="$(nz "$(prom_query 'walera_subscriber_disconnects_total{reason="slow_consumer"}')")"
baseline_drop="$(nz "$(prom_query 'walera_tx_dropped_total{reason="slow_consumer"}')")"
info "Baseline counters:"
info "  walera_subscriber_disconnects_total{reason=slow_consumer} = ${baseline_disc}"
info "  walera_tx_dropped_total{reason=slow_consumer}            = ${baseline_drop}"

# -----------------------------------------------------------------------------
# Spike writer so walera has tx to fan out.
# -----------------------------------------------------------------------------
echo
info "Switching writer to spike@500×10 to drive heavy fan-out"
set_writer_scenario spike 500 10
pass "writer scenario = spike"

# Give the spike a moment to materialise so the very first event the slow
# client receives is part of the high-rate stream.
sleep 2

# -----------------------------------------------------------------------------
# Launch slow consumer (curl that never reads its own stdout).
# -----------------------------------------------------------------------------
info "Launching background SSE client whose recv will stall (OS pipe fills)"
(
  curl -sN --max-time "${TIMEOUT_SECONDS}" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Origin: ${ORIGIN}" \
    "${WALERA_BASE_URL}/sse/v1/orders/all" \
    > "${TMP_SSE}" 2>&1
) &
CLIENT_PID=$!
info "slow-consumer SSE client pid=${CLIENT_PID}"

# The script intentionally does NOT read ${TMP_SSE}. Curl writes events into
# the file; ${TMP_SSE} is on a regular filesystem so it does NOT fill the way
# a pipe does. To make this a real slow-consumer simulation, suspend the curl
# process with SIGSTOP so its socket-recv stalls — that's what walera observes
# from the kernel side as "client not draining".
sleep 1
if kill -0 "${CLIENT_PID}" 2>/dev/null; then
  kill -STOP "${CLIENT_PID}" 2>/dev/null || true
  info "Suspended curl pid=${CLIENT_PID} (SIGSTOP) — its socket recv buffer will fill"
fi

# -----------------------------------------------------------------------------
# Poll Prometheus for either counter to increment.
# -----------------------------------------------------------------------------
echo
info "Polling for slow_consumer reactions, deadline=${TIMEOUT_SECONDS}s..."
deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
peak_disc="${baseline_disc}"
peak_drop="${baseline_drop}"
reaction=0
while :; do
  cur_disc="$(nz "$(prom_query 'walera_subscriber_disconnects_total{reason="slow_consumer"}')")"
  cur_drop="$(nz "$(prom_query 'walera_tx_dropped_total{reason="slow_consumer"}')")"
  peak_disc="${cur_disc}"
  peak_drop="${cur_drop}"
  # Use awk for the comparison so non-integer (e.g. "0") values still compare cleanly.
  if awk -v a="${cur_disc}" -v b="${baseline_disc}" 'BEGIN{exit !(a > b)}'; then
    pass "walera_subscriber_disconnects_total{reason=slow_consumer}: ${baseline_disc} → ${cur_disc}"
    reaction=1
    break
  fi
  if awk -v a="${cur_drop}" -v b="${baseline_drop}" 'BEGIN{exit !(a > b)}'; then
    pass "walera_tx_dropped_total{reason=slow_consumer}: ${baseline_drop} → ${cur_drop}"
    reaction=1
    break
  fi
  if (( $(date +%s) >= deadline )); then
    fail "No slow_consumer reaction observed within ${TIMEOUT_SECONDS}s"
    fail "  disconnects: ${baseline_disc} → ${cur_disc} (delta=$(awk -v a=${cur_disc} -v b=${baseline_disc} 'BEGIN{print a-b}'))"
    fail "  drops:       ${baseline_drop} → ${cur_drop} (delta=$(awk -v a=${cur_drop} -v b=${baseline_drop} 'BEGIN{print a-b}'))"
    info "last 30 lines: docker compose logs walera"
    docker compose -f "${COMPOSE_FILE}" logs --tail=30 walera 2>&1 | sed 's/^/      /' || true
    exit 1
  fi
  sleep 2
done

# -----------------------------------------------------------------------------
# Summary.
# -----------------------------------------------------------------------------
echo
echo "============================================================"
echo "fail-slow-consumer.sh — summary"
echo "============================================================"
printf '  baseline disconnects: %s\n' "${baseline_disc}"
printf '  peak disconnects:     %s\n' "${peak_disc}"
printf '  baseline drops:       %s\n' "${baseline_drop}"
printf '  peak drops:           %s\n' "${peak_drop}"
echo "  Grafana panel: http://127.0.0.1:3000/d/walera-overview"
echo "                  → 'Tx dropped by reason' (slow_consumer series)"
echo "  Prometheus check:"
echo "    curl '${PROM_BASE_URL}/api/v1/query?query=walera_tx_dropped_total{reason=%22slow_consumer%22}'"
echo "============================================================"
pass "slow_consumer disconnect reaction demonstrated"
exit 0
