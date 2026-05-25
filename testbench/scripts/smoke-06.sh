#!/usr/bin/env bash
# =============================================================================
# testbench/scripts/smoke-06.sh — Phase 06 end-to-end success-criteria smoke.
#
# This script is the SINGLE SOURCE OF TRUTH for Phase 06 verification. It is
# read-only against the running compose stack (the optional --reset flag is the
# one mutation, and it is gated behind the explicit flag). It is idempotent:
# running it twice in a row against the same healthy stack succeeds twice.
#
# ROADMAP Phase 06 Success Criteria (verbatim from .planning/ROADMAP.md L48-52):
#
#   SC1: `walera` container builds from the repo-root `Dockerfile` (no parallel
#        image) and reports `GOMAXPROCS=4` in its startup logs;
#        `deploy.resources.limits` of `cpus: '4.0', memory: 8G` are applied
#        (verifiable via `docker inspect`).
#
#   SC2: `curl -N -H 'Authorization: Bearer demo-alice'
#              -H 'Origin: http://localhost:8081'
#              http://localhost:8080/sse/v1/orders/1`
#        opens an SSE stream, the response contains
#        `Access-Control-Allow-Origin: http://localhost:8081`, and the response
#        body's first non-heartbeat line is `retry: 15000`.
#
#   SC3: `curl -I -X OPTIONS -H 'Origin: http://localhost:8081'
#              -H 'Access-Control-Request-Method: GET'
#              http://localhost:8080/sse/v1/orders/1`
#        (preflight) returns 204 with the matching `Access-Control-Allow-*`
#        headers.
#
#   SC4: A browser successfully negotiates HTTP/2 via h2c against
#        `walera:8080`, visible via `chrome://net-export/` or equivalent —
#        supports >6 concurrent EventSource connections from one tab without
#        HTTP/1.1 head-of-line blocking.
#        Phase 06 verification: `curl --http2-prior-knowledge` succeeds and -v
#        output shows `HTTP/2 200`. Browser-side verification (chrome://
#        net-export, >6 EventSource concurrency) is DEFERRED to Phase 08 Demo
#        UI per .planning/phases/06-walera-wired-into-compose/06-CONTEXT.md.
#
# Usage:
#   bash testbench/scripts/smoke-06.sh           # assumes stack is up
#   bash testbench/scripts/smoke-06.sh --reset   # cold-start the stack first
#
# Env overrides (all optional):
#   WALERA_BASE_URL    default http://localhost:8080
#   TOKEN              default demo-alice            (seeded full-whitelist user)
#   ORIGIN             default http://localhost:8081 (Phase 08 demo UI origin)
#   CONTAINER          default walera-app
#   TIMEOUT_SECONDS    default 30                    (wait-for-healthy upper bound)
# =============================================================================

set -euo pipefail

WALERA_BASE_URL="${WALERA_BASE_URL:-http://localhost:8080}"
TOKEN="${TOKEN:-demo-alice}"
ORIGIN="${ORIGIN:-http://localhost:8081}"
CONTAINER="${CONTAINER:-walera-app}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-30}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBENCH_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Per-SC pass tracker (1=pass, 0=fail). Aggregated at the end.
SC1_PASS=0
SC2_PASS=0
SC3_PASS=0
SC4_PASS=0

# ANSI colors only when stdout is a TTY (CI logs stay clean).
if [[ -t 1 ]]; then
  C_PASS=$'\033[32m'; C_FAIL=$'\033[31m'; C_INFO=$'\033[36m'; C_OFF=$'\033[0m'
else
  C_PASS=""; C_FAIL=""; C_INFO=""; C_OFF=""
fi

pass() { echo "${C_PASS}PASS${C_OFF} $*"; }
fail() { echo "${C_FAIL}FAIL${C_OFF} $*"; }
info() { echo "${C_INFO}INFO${C_OFF} $*"; }

# -----------------------------------------------------------------------------
# Optional --reset: cold-start the stack via the Makefile.
# -----------------------------------------------------------------------------
if [[ "${1:-}" == "--reset" ]]; then
  info "--reset: make -C ${TESTBENCH_DIR} demo-reset && demo-up"
  make -C "${TESTBENCH_DIR}" demo-reset
  make -C "${TESTBENCH_DIR}" demo-up
fi

# -----------------------------------------------------------------------------
# wait-for-healthy: poll docker inspect until the walera container reports
# "healthy" or TIMEOUT_SECONDS elapses. On timeout, dump tail logs and exit 1.
# -----------------------------------------------------------------------------
info "Waiting up to ${TIMEOUT_SECONDS}s for ${CONTAINER} to become healthy..."
deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
while :; do
  status="$(docker inspect "${CONTAINER}" --format '{{.State.Health.Status}}' 2>/dev/null || echo "missing")"
  if [[ "${status}" == "healthy" ]]; then
    pass "container ${CONTAINER} is healthy"
    break
  fi
  if (( $(date +%s) >= deadline )); then
    fail "container ${CONTAINER} did not reach healthy within ${TIMEOUT_SECONDS}s (last status: ${status})"
    echo "----- docker logs --tail 50 ${CONTAINER} -----"
    docker logs --tail 50 "${CONTAINER}" 2>&1 || true
    exit 1
  fi
  sleep 1
done

# =============================================================================
# SC1 — BENCH-04 (production-Dockerfile reuse) + BENCH-05 (resource limits).
#   - `walera` service is defined in the compose file (not a parallel image)
#   - `docker logs walera-app` contains `GOMAXPROCS=4` (automaxprocs log)
#   - `docker inspect walera-app` shows HostConfig.NanoCpus=4000000000,
#     HostConfig.Memory=8589934592 (= 4 CPU / 8 GiB)
# =============================================================================
echo
info "===== SC1: BENCH-04 + BENCH-05 (build + resource limits) ====="

sc1_ok=1

# SC1.a: walera service exists in compose config (one canonical definition).
compose_file="${TESTBENCH_DIR}/docker-compose.yml"
if docker compose -f "${compose_file}" config --services 2>/dev/null | grep -Fxq walera; then
  pass "SC1.a compose config lists service: walera"
else
  fail "SC1.a compose config does not list service: walera"
  sc1_ok=0
fi

# SC1.b: automaxprocs log line contains GOMAXPROCS=4.
gomaxprocs_line="$(docker logs "${CONTAINER}" 2>&1 | grep -F 'GOMAXPROCS=' | head -1 || true)"
if [[ -n "${gomaxprocs_line}" ]] && echo "${gomaxprocs_line}" | grep -Fq 'GOMAXPROCS=4'; then
  pass "SC1.b GOMAXPROCS log line: ${gomaxprocs_line}"
else
  fail "SC1.b no log line matching 'GOMAXPROCS=4' found in docker logs ${CONTAINER}"
  [[ -n "${gomaxprocs_line}" ]] && echo "      actual line: ${gomaxprocs_line}"
  sc1_ok=0
fi

# SC1.c: HostConfig.NanoCpus + HostConfig.Memory match the deploy.resources.limits.
inspect_out="$(docker inspect "${CONTAINER}" --format '{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}' 2>/dev/null || true)"
expected_inspect="4000000000 8589934592"
if [[ "${inspect_out}" == "${expected_inspect}" ]]; then
  pass "SC1.c docker inspect HostConfig.{NanoCpus,Memory} = ${inspect_out} (= 4 CPU / 8 GiB)"
else
  fail "SC1.c docker inspect HostConfig.{NanoCpus,Memory} mismatch"
  echo "      expected: ${expected_inspect}"
  echo "      actual:   ${inspect_out}"
  sc1_ok=0
fi

if (( sc1_ok == 1 )); then
  SC1_PASS=1
  pass "SC1 OVERALL"
else
  fail "SC1 OVERALL"
fi

# =============================================================================
# SC2 — WALERA-01 (retry: prelude) + WALERA-02 (CORS reflected on SSE GET).
#   curl -N -H 'Authorization: Bearer demo-alice' \
#           -H 'Origin: http://localhost:8081' \
#           http://localhost:8080/sse/v1/orders/1
#   - response headers include `Access-Control-Allow-Origin: http://localhost:8081`
#   - first non-blank, non-comment (`:`) body line is exactly `retry: 15000`
#
# curl exits 28 (Operation timed out) on --max-time elapse — this is EXPECTED
# for a long-lived SSE stream; we accept exit 28 as success and treat any
# other non-zero exit as failure.
# =============================================================================
echo
info "===== SC2: WALERA-01 retry prelude + WALERA-02 CORS-on-GET ====="

sc2_ok=1

sc2_headers="$(mktemp)"
sc2_body="$(mktemp)"
trap 'rm -f "${sc2_headers}" "${sc2_body}"' EXIT

set +e
curl -sS -N --max-time 3 \
     -H "Authorization: Bearer ${TOKEN}" \
     -H "Origin: ${ORIGIN}" \
     -D "${sc2_headers}" \
     "${WALERA_BASE_URL}/sse/v1/orders/1" \
     > "${sc2_body}" 2>/dev/null
sc2_exit=$?
set -e

if (( sc2_exit == 0 || sc2_exit == 28 )); then
  pass "SC2.a curl exit=${sc2_exit} (0 or 28=timeout both acceptable for SSE)"
else
  fail "SC2.a curl exit=${sc2_exit} (expected 0 or 28); aborting SC2 checks"
  sc2_ok=0
fi

# SC2.b: ACAO header matches Origin.
if (( sc2_ok == 1 )); then
  acao_line="$(grep -i '^Access-Control-Allow-Origin:' "${sc2_headers}" | tr -d '\r' | head -1 || true)"
  if [[ "${acao_line}" == "Access-Control-Allow-Origin: ${ORIGIN}" ]]; then
    pass "SC2.b ${acao_line}"
  else
    fail "SC2.b Access-Control-Allow-Origin mismatch"
    echo "      expected: Access-Control-Allow-Origin: ${ORIGIN}"
    echo "      actual:   ${acao_line:-<missing>}"
    sc2_ok=0
  fi
fi

# SC2.c: first non-blank, non-comment body line is exactly `retry: 15000`.
if (( sc2_ok == 1 )); then
  # Strip CRs (SSE uses bare LF but be defensive); ignore blank lines and
  # comment lines (those starting with `:`); take the first remaining line.
  first_line="$(tr -d '\r' < "${sc2_body}" | awk 'NF && !/^:/' | head -1 || true)"
  if [[ "${first_line}" == "retry: 15000" ]]; then
    pass "SC2.c first non-comment SSE line = '${first_line}'"
  else
    fail "SC2.c first non-comment SSE line mismatch"
    echo "      expected: retry: 15000"
    echo "      actual:   ${first_line:-<empty>}"
    echo "      ----- body (first 5 lines, CR-stripped) -----"
    tr -d '\r' < "${sc2_body}" | head -5 | sed 's/^/      /'
    sc2_ok=0
  fi
fi

if (( sc2_ok == 1 )); then
  SC2_PASS=1
  pass "SC2 OVERALL"
else
  fail "SC2 OVERALL"
fi

# =============================================================================
# SC3 — WALERA-02 (CORS preflight on OPTIONS).
#   curl -I -X OPTIONS -H 'Origin: http://localhost:8081' \
#                      -H 'Access-Control-Request-Method: GET' \
#                      http://localhost:8080/sse/v1/orders/1
#   - status 204
#   - Access-Control-Allow-Origin: http://localhost:8081
#   - Access-Control-Allow-Methods includes GET and OPTIONS
#   - Access-Control-Allow-Headers includes Authorization
#   - Vary header includes Origin
# =============================================================================
echo
info "===== SC3: WALERA-02 CORS preflight (OPTIONS) ====="

sc3_ok=1
sc3_headers="$(mktemp)"
trap 'rm -f "${sc2_headers}" "${sc2_body}" "${sc3_headers}"' EXIT

set +e
curl -sS -I -X OPTIONS \
     -H "Origin: ${ORIGIN}" \
     -H "Access-Control-Request-Method: GET" \
     "${WALERA_BASE_URL}/sse/v1/orders/1" \
     > "${sc3_headers}" 2>/dev/null
sc3_exit=$?
set -e

if (( sc3_exit != 0 )); then
  fail "SC3.a curl OPTIONS exit=${sc3_exit} (expected 0); aborting SC3"
  sc3_ok=0
fi

if (( sc3_ok == 1 )); then
  status_line="$(head -1 "${sc3_headers}" | tr -d '\r' || true)"
  if echo "${status_line}" | grep -qE '^HTTP/[12](\.[01])? 204( |$)'; then
    pass "SC3.a status line: ${status_line}"
  else
    fail "SC3.a status line missing 204"
    echo "      actual: ${status_line}"
    sc3_ok=0
  fi
fi

if (( sc3_ok == 1 )); then
  acao_line="$(grep -i '^Access-Control-Allow-Origin:' "${sc3_headers}" | tr -d '\r' | head -1 || true)"
  if [[ "${acao_line}" == "Access-Control-Allow-Origin: ${ORIGIN}" ]]; then
    pass "SC3.b ${acao_line}"
  else
    fail "SC3.b Access-Control-Allow-Origin mismatch"
    echo "      expected: Access-Control-Allow-Origin: ${ORIGIN}"
    echo "      actual:   ${acao_line:-<missing>}"
    sc3_ok=0
  fi
fi

if (( sc3_ok == 1 )); then
  methods_line="$(grep -i '^Access-Control-Allow-Methods:' "${sc3_headers}" | tr -d '\r' | head -1 || true)"
  if echo "${methods_line}" | grep -qiE 'GET' && echo "${methods_line}" | grep -qiE 'OPTIONS'; then
    pass "SC3.c ${methods_line}"
  else
    fail "SC3.c Access-Control-Allow-Methods missing GET and/or OPTIONS"
    echo "      actual: ${methods_line:-<missing>}"
    sc3_ok=0
  fi
fi

if (( sc3_ok == 1 )); then
  hdrs_line="$(grep -i '^Access-Control-Allow-Headers:' "${sc3_headers}" | tr -d '\r' | head -1 || true)"
  if echo "${hdrs_line}" | grep -qiE 'Authorization'; then
    pass "SC3.d ${hdrs_line}"
  else
    fail "SC3.d Access-Control-Allow-Headers missing Authorization"
    echo "      actual: ${hdrs_line:-<missing>}"
    sc3_ok=0
  fi
fi

if (( sc3_ok == 1 )); then
  vary_line="$(grep -i '^Vary:' "${sc3_headers}" | tr -d '\r' | head -1 || true)"
  if echo "${vary_line}" | grep -qiE 'Origin'; then
    pass "SC3.e ${vary_line}"
  else
    fail "SC3.e Vary header missing Origin"
    echo "      actual: ${vary_line:-<missing>}"
    sc3_ok=0
  fi
fi

if (( sc3_ok == 1 )); then
  SC3_PASS=1
  pass "SC3 OVERALL"
else
  fail "SC3 OVERALL"
fi

# =============================================================================
# SC4 — WALERA-03 (h2c prior-knowledge negotiation).
#   curl --http2-prior-knowledge -v -N --max-time 3 \
#        -H 'Authorization: Bearer demo-alice' \
#        http://localhost:8080/sse/v1/orders/1
#   - exit 0 or 28 (timeout) acceptable
#   - -v trace contains `< HTTP/2 200` (proves server actually negotiated h2c)
#   - -v trace does NOT contain `HTTP/1.1 200` for this request (mutual exclusion)
#
# Browser-side h2c verification (chrome://net-export, >6 EventSource
# concurrency) is DEFERRED to Phase 08 Demo UI per 06-CONTEXT.md.
# =============================================================================
echo
info "===== SC4: WALERA-03 h2c prior-knowledge negotiation ====="

sc4_ok=1
sc4_trace="$(mktemp)"
sc4_body="$(mktemp)"
trap 'rm -f "${sc2_headers}" "${sc2_body}" "${sc3_headers}" "${sc4_trace}" "${sc4_body}"' EXIT

# Capability gate: ensure the host curl supports --http2-prior-knowledge.
if ! curl --help all 2>/dev/null | grep -Fq -- '--http2-prior-knowledge'; then
  fail "SC4.precheck curl on this host lacks --http2-prior-knowledge support"
  echo "      Install a curl built against nghttp2, or run smoke from a"
  echo "      container: docker run --rm --network=host curlimages/curl:8 ..."
  sc4_ok=0
fi

if (( sc4_ok == 1 )); then
  set +e
  curl -vsS -N --max-time 3 --http2-prior-knowledge \
       -H "Authorization: Bearer ${TOKEN}" \
       "${WALERA_BASE_URL}/sse/v1/orders/1" \
       2>"${sc4_trace}" >"${sc4_body}"
  sc4_exit=$?
  set -e

  if (( sc4_exit == 0 || sc4_exit == 28 )); then
    pass "SC4.a curl --http2-prior-knowledge exit=${sc4_exit} (0 or 28 acceptable)"
  else
    fail "SC4.a curl --http2-prior-knowledge exit=${sc4_exit} (expected 0 or 28)"
    echo "      ----- last 10 trace lines -----"
    tail -10 "${sc4_trace}" | sed 's/^/      /'
    sc4_ok=0
  fi
fi

if (( sc4_ok == 1 )); then
  h2_line="$(grep -E '^< HTTP/2 200' "${sc4_trace}" | head -1 || true)"
  if [[ -n "${h2_line}" ]]; then
    pass "SC4.b h2c negotiated: ${h2_line}"
  else
    fail "SC4.b no '< HTTP/2 200' line in curl -v trace"
    echo "      ----- HTTP-status-line trace excerpts -----"
    grep -E '^< HTTP' "${sc4_trace}" | head -5 | sed 's/^/      /' || echo "      <none>"
    sc4_ok=0
  fi
fi

if (( sc4_ok == 1 )); then
  # Mutual-exclusion guard: if HTTP/1.1 200 also appears as a response status for
  # THIS request, something is off (e.g., proxy intercept downgraded the
  # protocol). Record discrepancy but do not necessarily fail — print as a
  # warning if both appear.
  if grep -qE '^< HTTP/1\.1 200' "${sc4_trace}"; then
    fail "SC4.c discrepancy: both HTTP/2 200 and HTTP/1.1 200 appear in trace"
    echo "      ----- response-status excerpts -----"
    grep -E '^< HTTP' "${sc4_trace}" | sed 's/^/      /'
    sc4_ok=0
  else
    pass "SC4.c no HTTP/1.1 fallback observed (mutual exclusion holds)"
  fi
fi

if (( sc4_ok == 1 )); then
  SC4_PASS=1
  pass "SC4 OVERALL"
else
  fail "SC4 OVERALL"
fi

info "SC4 note: browser-side h2c verification (chrome://net-export, >6"
info "          concurrent EventSources) is DEFERRED to Phase 08 Demo UI."

# =============================================================================
# Summary
# =============================================================================
echo
echo "============================================================"
echo "Phase 06 smoke summary"
echo "============================================================"
printf '  SC1 (BENCH-04/05 build+limits)      : %s\n' "$( (( SC1_PASS == 1 )) && echo PASS || echo FAIL )"
printf '  SC2 (WALERA-01 retry + WALERA-02 GET): %s\n' "$( (( SC2_PASS == 1 )) && echo PASS || echo FAIL )"
printf '  SC3 (WALERA-02 OPTIONS preflight)    : %s\n' "$( (( SC3_PASS == 1 )) && echo PASS || echo FAIL )"
printf '  SC4 (WALERA-03 h2c prior-knowledge)  : %s\n' "$( (( SC4_PASS == 1 )) && echo PASS || echo FAIL )"
echo "============================================================"

if (( SC1_PASS == 1 && SC2_PASS == 1 && SC3_PASS == 1 && SC4_PASS == 1 )); then
  pass "Phase 06 smoke PASSED (SC1+SC2+SC3+SC4)"
  exit 0
else
  fail "Phase 06 smoke FAILED"
  exit 1
fi
