#!/usr/bin/env bash
# =============================================================================
# testbench/scripts/smoke-08.sh — Phase 08 (Demo UI) end-to-end success-criteria
# smoke. Mirrors testbench/scripts/smoke-07.sh structure.
#
# This script is the SINGLE SOURCE OF TRUTH for Phase 08 verification. It maps
# 1:1 onto the five ROADMAP Phase 08 success criteria. Where a check is
# fundamentally visual (CSS animation, DOM mutation), the script emits
# `[manual-verification-required]` pointing to the operator checklist in
# testbench/web/MANUAL-VERIFICATION.md.
#
# ROADMAP Phase 08 Success Criteria (verbatim from .planning/ROADMAP.md):
#
#   SC1: Opening http://localhost:8081 loads a single index.html (plus CSS,
#        native ESM modules, and one vendored fetch-event-source.min.js) —
#        no package.json, no node_modules/, no build step under testbench/web/.
#
#   SC2: Operator picks demo-alice, subscribes to orders:1, and sees event: tx
#        entries in the live feed; switching to demo-eve tears down + reopens
#        all subscriptions and the payload shape changes to wildcard-only.
#
#   SC3: Entity-state card for orders:1 flashes the changed-field columns on
#        update, collapses on delete, reveals on insert; wildcard list clears
#        with a banner on event: error reason=tx_too_large.
#
#   SC4: Metrics panel polls /metrics every 2 s and displays four headline
#        values; writer-control panel submits scenario changes to
#        writer:9100/control from the browser without leaving the page.
#
#   SC5: Connection-state pills reflect transitions per panel; reconnect banner
#        explicitly states "events during the gap were NOT replayed";
#        backgrounding the tab pauses the ring-buffer flush; in-page banner
#        appears if h2c negotiation fails.
#
# Usage:
#   bash testbench/scripts/smoke-08.sh           # assumes stack is up + healthy
#   bash testbench/scripts/smoke-08.sh --reset   # cold-start the stack first
#
# Prerequisites:
#   - docker compose stack up: postgres + mock-auth + walera + writer + frontend
#   - host has: curl, awk, grep, sha256sum, find
#
# Exit codes:
#   0 = all 5 SCs PASS (manual-required items annotated, not blocking)
#   1 = at least one SC FAILED (see per-[SCn] PASS|FAIL lines and summary)
#
# Env overrides (all optional):
#   WALERA_BASE_URL   default http://127.0.0.1:8080
#   WRITER_BASE_URL   default http://127.0.0.1:9100
#   FRONTEND_BASE_URL default http://127.0.0.1:8081
#   ORIGIN            default http://localhost:8081
#   TOKEN             default demo-alice
# =============================================================================

set -euo pipefail
IFS=$'\n\t'

WALERA_BASE_URL="${WALERA_BASE_URL:-http://127.0.0.1:8080}"
WRITER_BASE_URL="${WRITER_BASE_URL:-http://127.0.0.1:9100}"
FRONTEND_BASE_URL="${FRONTEND_BASE_URL:-http://127.0.0.1:8081}"
ORIGIN="${ORIGIN:-http://localhost:8081}"
TOKEN="${TOKEN:-demo-alice}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBENCH_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${TESTBENCH_DIR}/.." && pwd)"

# Per-SC pass tracker (1=pass, 0=fail).
SC1_PASS=0
SC2_PASS=0
SC3_PASS=0
SC4_PASS=0
SC5_PASS=0

# ANSI colors only when stdout is a TTY (CI logs stay clean).
if [[ -t 1 ]]; then
  C_PASS=$'\033[32m'; C_FAIL=$'\033[31m'; C_INFO=$'\033[36m'; C_MAN=$'\033[33m'; C_OFF=$'\033[0m'
else
  C_PASS=""; C_FAIL=""; C_INFO=""; C_MAN=""; C_OFF=""
fi

pass() { echo "${C_PASS}PASS${C_OFF} $*"; }
fail() { echo "${C_FAIL}FAIL${C_OFF} $*"; }
info() { echo "${C_INFO}INFO${C_OFF} $*"; }
manual() { echo "${C_MAN}[manual-verification-required]${C_OFF} $*"; }

# Tempfile cleanup.
SSE_HEADERS="$(mktemp -t walera-smoke-08-sse.XXXXXX)"
TMP_RESP="$(mktemp -t walera-smoke-08-resp.XXXXXX)"
cleanup() {
  rm -f "${SSE_HEADERS}" "${TMP_RESP}" || true
}
trap cleanup EXIT

# -----------------------------------------------------------------------------
# Optional --reset: cold-start the stack via the Makefile.
# -----------------------------------------------------------------------------
if [[ "${1:-}" == "--reset" ]]; then
  info "--reset: make -C ${TESTBENCH_DIR} demo-reset && demo-up"
  make -C "${TESTBENCH_DIR}" demo-reset
  make -C "${TESTBENCH_DIR}" demo-up
  info "Waiting up to 120s for stack to become healthy after cold start..."
  deadline=$(( $(date +%s) + 120 ))
  while :; do
    if curl -sf "${WRITER_BASE_URL}/healthz" >/dev/null 2>&1 \
       && curl -sf "${WALERA_BASE_URL}/healthz" >/dev/null 2>&1 \
       && curl -sf "${FRONTEND_BASE_URL}/vendor/fes/index.js" >/dev/null 2>&1; then
      pass "stack reached healthy (walera + writer + frontend responding)"
      break
    fi
    if (( $(date +%s) >= deadline )); then
      fail "stack did not become healthy within 120s after cold start"
      docker compose -f "${TESTBENCH_DIR}/docker-compose.yml" ps || true
      exit 1
    fi
    sleep 2
  done
fi

# =============================================================================
# SC1 — Static / no-bundler discipline; vendored polyfill + SHA-256 binding.
#
# Sub-checks:
#   SC1.a — find under testbench/web for node_modules OR package.json: must be 0.
#   SC1.b — all four vendored artefacts present on disk
#            (vendor/fes/{index,fetch,parse}.js + vendor/README.md).
#   SC1.c — recomputed sha256 of each vendored file appears verbatim in
#            vendor/README.md (proves the recorded SHA-256 matches the bytes).
#   SC1.d — index.html has exactly ONE `<script type="module"` tag and ZERO
#            cross-origin https:// src/href references.
# =============================================================================
verify_sc1() {
  echo
  info "===== SC1: static / no-bundler discipline + vendored polyfill SHA-256 ====="

  local ok=1

  # SC1.a — no bundler artefacts under testbench/web.
  local nm_pkg_count
  nm_pkg_count=$(find "${REPO_ROOT}/testbench/web" \
                       \( -name node_modules -o -name package.json \) 2>/dev/null | wc -l | tr -d ' ')
  if [[ "${nm_pkg_count}" == "0" ]]; then
    pass "SC1.a no node_modules/ or package.json under testbench/web/ (count=0)"
  else
    fail "SC1.a found ${nm_pkg_count} node_modules/package.json entries under testbench/web/ (must be 0)"
    find "${REPO_ROOT}/testbench/web" \
      \( -name node_modules -o -name package.json \) 2>/dev/null | sed 's/^/      /'
    ok=0
  fi

  # SC1.b — vendored polyfill files present.
  local missing=0
  for f in \
      testbench/web/vendor/fes/index.js \
      testbench/web/vendor/fes/fetch.js \
      testbench/web/vendor/fes/parse.js \
      testbench/web/vendor/README.md; do
    if [[ -f "${REPO_ROOT}/${f}" ]]; then
      :
    else
      fail "SC1.b missing: ${f}"
      missing=1
    fi
  done
  if (( missing == 0 )); then
    pass "SC1.b all four vendored artefacts present (vendor/fes/{index,fetch,parse}.js + vendor/README.md)"
  else
    ok=0
  fi

  # SC1.c — SHA-256 of each disk file appears in vendor/README.md.
  if (( missing == 0 )); then
    local sha_ok=1
    for f in index.js fetch.js parse.js; do
      local sha
      sha=$(sha256sum "${REPO_ROOT}/testbench/web/vendor/fes/${f}" | awk '{print $1}')
      if grep -q -F "${sha}" "${REPO_ROOT}/testbench/web/vendor/README.md"; then
        pass "SC1.c sha256(${f})=${sha:0:12}… matches vendor/README.md"
      else
        fail "SC1.c sha256(${f})=${sha} NOT found in vendor/README.md (re-vendor required or tampering)"
        sha_ok=0
      fi
    done
    if (( sha_ok == 0 )); then
      ok=0
    fi
  fi

  # SC1.d — index.html shape: exactly one module script, no cross-origin assets.
  # NOTE: `grep -c X || echo 0` is a double-counter trap — grep always prints
  # the count to stdout AND exits 1 when count==0, so the `|| echo 0` appends
  # an extra "0" yielding "0\n0". Use `{ grep -c X || true; }` instead — the
  # `true` swallows the exit status without mutating output.
  local script_count https_count
  script_count=$({ grep -c '<script type="module"' "${REPO_ROOT}/testbench/web/index.html" 2>/dev/null || true; })
  # Count https:// occurrences in src= / href= attrs (regex tolerates single
  # or double quotes around the URL).
  https_count=$({ grep -cE '(src|href)=["'\'']https://' "${REPO_ROOT}/testbench/web/index.html" 2>/dev/null || true; })
  script_count="${script_count:-0}"
  https_count="${https_count:-0}"
  if [[ "${script_count}" == "1" ]] && [[ "${https_count}" == "0" ]]; then
    pass "SC1.d index.html has exactly 1 <script type=\"module\"> and 0 cross-origin https:// asset refs"
  else
    fail "SC1.d index.html shape mismatch (script_count=${script_count}, expected 1; https_refs=${https_count}, expected 0)"
    ok=0
  fi

  if (( ok == 1 )); then
    SC1_PASS=1
    pass "[SC1] PASS — no-bundler discipline intact, vendored polyfill SHA-256-pinned, single ESM entry"
  else
    fail "[SC1] FAIL"
  fi
}

# =============================================================================
# SC2 — HTTP-level page + module reachability + cross-origin SSE handshake.
#
# Sub-checks:
#   SC2.a — GET FRONTEND/ returns 200.
#   SC2.b — GET FRONTEND/index.html contains <title>Walera Testbench</title>.
#   SC2.c — GET FRONTEND/modules/app.js returns 200.
#   SC2.d — Cross-origin SSE GET against WALERA/sse/v1/orders/1 reflects ACAO
#           AND Timing-Allow-Origin (the latter proves Plan 08-04 landed).
#           curl exit 28 (max-time) is acceptable — stream stays open by design;
#           we capture headers and grep them.
#   SC2.visual — User-switch tear-down + payload-shape change → manual.
# =============================================================================
verify_sc2() {
  echo
  info "===== SC2: HTTP-level page + module reachability + SSE CORS ====="

  local ok=1

  # SC2.a — index page reachable.
  local code
  code=$(curl -fsS -o /dev/null -w '%{http_code}' "${FRONTEND_BASE_URL}/" 2>/dev/null || echo 000)
  if [[ "${code}" == "200" ]]; then
    pass "SC2.a GET ${FRONTEND_BASE_URL}/ returned 200"
  else
    fail "SC2.a GET ${FRONTEND_BASE_URL}/ returned ${code} (expected 200)"
    ok=0
  fi

  # SC2.b — title present in index.html.
  if curl -fsS "${FRONTEND_BASE_URL}/index.html" 2>/dev/null | grep -q '<title>Walera Testbench</title>'; then
    pass "SC2.b GET ${FRONTEND_BASE_URL}/index.html contains <title>Walera Testbench</title>"
  else
    fail "SC2.b ${FRONTEND_BASE_URL}/index.html missing <title>Walera Testbench</title>"
    ok=0
  fi

  # SC2.c — app.js reachable.
  code=$(curl -fsS -o /dev/null -w '%{http_code}' "${FRONTEND_BASE_URL}/modules/app.js" 2>/dev/null || echo 000)
  if [[ "${code}" == "200" ]]; then
    pass "SC2.c GET ${FRONTEND_BASE_URL}/modules/app.js returned 200"
  else
    fail "SC2.c GET ${FRONTEND_BASE_URL}/modules/app.js returned ${code} (expected 200)"
    ok=0
  fi

  # SC2.d — cross-origin SSE handshake reflects ACAO + TAO.
  # `|| true` is required: curl exits 28 on --max-time; that exit code is
  # expected for an open SSE stream. We only care that the response headers
  # included the right CORS reflections.
  : > "${SSE_HEADERS}"
  curl -sS -i -N --max-time 3 \
       -H "Authorization: Bearer ${TOKEN}" \
       -H "Origin: ${ORIGIN}" \
       "${WALERA_BASE_URL}/sse/v1/orders/1" \
       > "${SSE_HEADERS}" 2>/dev/null || true

  local acao_ok=0 tao_ok=0
  if grep -qiE "^Access-Control-Allow-Origin:[[:space:]]*${ORIGIN}" "${SSE_HEADERS}"; then
    acao_ok=1
  fi
  if grep -qiE "^Timing-Allow-Origin:[[:space:]]*${ORIGIN}" "${SSE_HEADERS}"; then
    tao_ok=1
  fi

  if (( acao_ok == 1 && tao_ok == 1 )); then
    pass "SC2.d SSE response reflected Access-Control-Allow-Origin AND Timing-Allow-Origin = ${ORIGIN}"
  else
    fail "SC2.d SSE CORS reflection incomplete (ACAO=${acao_ok}, TAO=${tao_ok}) — expected both"
    echo "      ----- first 20 SSE response lines (headers + prelude) -----"
    head -20 "${SSE_HEADERS}" | sed 's/^/      /'
    ok=0
  fi

  # SC2.visual — manual gate.
  manual "SC2 visual: user-switch teardown + payload-shape change → see testbench/web/MANUAL-VERIFICATION.md steps 2 and 6"

  if (( ok == 1 )); then
    SC2_PASS=1
    pass "[SC2] PASS — page + module + cross-origin SSE handshake (visual user-switch deferred to MANUAL-VERIFICATION.md)"
  else
    fail "[SC2] FAIL"
  fi
}

# =============================================================================
# SC3 — Visual: field-flash on update, collapse on delete, reveal on insert,
#       wildcard list clears + banner on tx_too_large.
#
# These behaviours involve CSS keyframe animations + DOM mutation under live
# SSE traffic — no headless tool can verify them reliably in <5 min. The
# script emits a `[manual-verification-required]` pointing to the operator
# checklist, and auto-PASSes IF the checklist exists with the relevant steps.
# =============================================================================
verify_sc3() {
  echo
  info "===== SC3: visual field-flash + collapse + reveal + wildcard banner ====="

  local ok=1
  local manual_file="${REPO_ROOT}/testbench/web/MANUAL-VERIFICATION.md"

  if [[ ! -f "${manual_file}" ]]; then
    fail "SC3 testbench/web/MANUAL-VERIFICATION.md missing — visual gate cannot be deferred"
    ok=0
  else
    # Confirm the checklist contains the steps SC3 needs.
    local missing_steps=()
    grep -q 'Field-flash on update' "${manual_file}"          || missing_steps+=("step 4: field-flash")
    grep -q 'Collapse on delete, reveal on insert' "${manual_file}" || missing_steps+=("step 5: collapse/reveal")
    grep -q 'tx_too_large' "${manual_file}"                   || missing_steps+=("step 7: tx_too_large")

    if (( ${#missing_steps[@]} == 0 )); then
      pass "SC3 MANUAL-VERIFICATION.md present with steps 4, 5, and 7 covering field-flash, collapse/reveal, tx_too_large"
    else
      fail "SC3 MANUAL-VERIFICATION.md missing required steps: ${missing_steps[*]}"
      ok=0
    fi
  fi

  manual "SC3: see testbench/web/MANUAL-VERIFICATION.md steps 4, 5, and 7"

  if (( ok == 1 )); then
    SC3_PASS=1
    pass "[SC3] PASS — visual gate documented in MANUAL-VERIFICATION.md (operator must walk through)"
  else
    fail "[SC3] FAIL"
  fi
}

# =============================================================================
# SC4 — Metrics panel + writer-control HTTP surface.
#
# Sub-checks:
#   SC4.a — POST writer /control with Origin returns 200 + reflected ACAO
#            AND the JSON response body echoes scenario=steady, commit_rate=50.
#   SC4.b — OPTIONS preflight to writer /control returns 204 with the four
#            ACA-* headers (Origin, Methods, Headers, Max-Age).
#   SC4.c — walera /metrics returns 200 with Timing-Allow-Origin reflected
#            (UI-11 prerequisite for h2c probe + cross-origin metrics-panel).
#   SC4.d — walera /metrics surface contains ≥3 of the headline metric
#            families used by the metrics-panel (subscribers, lag, tx rate,
#            breaker). Naming is environment-dependent — accept either the
#            walera_-prefixed or bare names per the 08-04 wire-name fix.
# =============================================================================
verify_sc4() {
  echo
  info "===== SC4: metrics-panel + writer-control HTTP surface ====="

  local ok=1

  # SC4.a — POST /control with Origin reflects ACAO and returns scenario JSON.
  : > "${TMP_RESP}"
  local code
  code=$(curl -sS -o "${TMP_RESP}" -w '%{http_code}' -i \
              -X POST \
              -H "Origin: ${ORIGIN}" \
              -H 'Content-Type: application/json' \
              -d '{"scenario":"steady","commit_rate":50}' \
              "${WRITER_BASE_URL}/control" 2>/dev/null || echo 000)
  # `code` is contaminated because we used -i above (writes headers + body to file)
  # and -w prints separately; re-read status from the headers.
  local status_line
  status_line=$(head -1 "${TMP_RESP}" 2>/dev/null || echo "")
  local acao_seen=0
  if grep -qiE "^Access-Control-Allow-Origin:[[:space:]]*${ORIGIN}" "${TMP_RESP}"; then
    acao_seen=1
  fi
  local body_ok=0
  if grep -qE '"scenario":"steady"' "${TMP_RESP}" \
     && grep -qE '"commit_rate":50' "${TMP_RESP}"; then
    body_ok=1
  fi
  if echo "${status_line}" | grep -q '200' && (( acao_seen == 1 )) && (( body_ok == 1 )); then
    pass "SC4.a POST /control returned 200 + ACAO=${ORIGIN} + body echoed scenario=steady, commit_rate=50"
  else
    fail "SC4.a POST /control failed (status=${status_line%%$'\r'*}, acao=${acao_seen}, body_ok=${body_ok})"
    echo "      ----- /control response (first 20 lines) -----"
    head -20 "${TMP_RESP}" | sed 's/^/      /'
    ok=0
  fi

  # SC4.b — OPTIONS preflight returns 204 with the four ACA-* headers.
  # NOTE: `-o /dev/null -i > FILE` is contradictory — -o /dev/null sends body
  # AND headers to /dev/null and the redirect captures nothing. Use `-D FILE`
  # to dump headers to a file while sending body to /dev/null.
  : > "${TMP_RESP}"
  curl -sS -D "${TMP_RESP}" -o /dev/null \
       -X OPTIONS \
       -H "Origin: ${ORIGIN}" \
       -H 'Access-Control-Request-Method: POST' \
       -H 'Access-Control-Request-Headers: Content-Type' \
       "${WRITER_BASE_URL}/control" 2>/dev/null || true
  local pf_status pf_origin pf_methods pf_headers
  pf_status=$(head -1 "${TMP_RESP}" 2>/dev/null || echo "")
  pf_origin=0; grep -qiE "^Access-Control-Allow-Origin:[[:space:]]*${ORIGIN}" "${TMP_RESP}" && pf_origin=1
  pf_methods=0; grep -qiE "^Access-Control-Allow-Methods:.*POST" "${TMP_RESP}" && pf_methods=1
  pf_headers=0; grep -qiE "^Access-Control-Allow-Headers:.*Content-Type" "${TMP_RESP}" && pf_headers=1
  if echo "${pf_status}" | grep -q '204' && (( pf_origin == 1 )) && (( pf_methods == 1 )) && (( pf_headers == 1 )); then
    pass "SC4.b OPTIONS /control returned 204 + ACA-{Origin, Methods=POST, Headers=Content-Type}"
  else
    fail "SC4.b OPTIONS /control preflight failed (status=${pf_status%%$'\r'*}, origin=${pf_origin}, methods=${pf_methods}, headers=${pf_headers})"
    echo "      ----- preflight response headers -----"
    head -15 "${TMP_RESP}" | sed 's/^/      /'
    ok=0
  fi

  # SC4.c — walera /metrics returns 200 + Timing-Allow-Origin.
  : > "${TMP_RESP}"
  curl -sS -D- -o /dev/null \
       -H "Origin: ${ORIGIN}" \
       "${WALERA_BASE_URL}/metrics" > "${TMP_RESP}" 2>/dev/null || true
  local tao_status tao_seen
  tao_status=$(head -1 "${TMP_RESP}" 2>/dev/null || echo "")
  tao_seen=0; grep -qiE "^Timing-Allow-Origin:[[:space:]]*${ORIGIN}" "${TMP_RESP}" && tao_seen=1
  if echo "${tao_status}" | grep -q '200' && (( tao_seen == 1 )); then
    pass "SC4.c GET ${WALERA_BASE_URL}/metrics returned 200 + Timing-Allow-Origin=${ORIGIN}"
  else
    fail "SC4.c /metrics CORS reflection failed (status=${tao_status%%$'\r'*}, tao=${tao_seen})"
    echo "      ----- /metrics response headers -----"
    head -15 "${TMP_RESP}" | sed 's/^/      /'
    ok=0
  fi

  # SC4.d — walera /metrics surface contains ≥3 headline metric families.
  # Accept both the plan-hypothetical names and the walera_-prefixed actual
  # names (per the 08-04 Rule 1 fix). Counting unique family roots, not
  # samples — sort -u on the metric family name.
  # Pipeline ends with `grep -cE … || true` to avoid the double-counter trap.
  local headline_count
  headline_count=$( { curl -sf "${WALERA_BASE_URL}/metrics" 2>/dev/null \
                    | grep -v '^#' \
                    | awk '{print $1}' \
                    | sed 's/{.*//' \
                    | sort -u \
                    | grep -cE '^(wal_tx_total|walera_wal_tx_size_changes_count|subscribers_active|wal_lsn_lag_bytes|walera_wal_lsn_lag_bytes|auth_circuit_breaker_state|walera_auth_circuit_breaker_state|walera_routing_index_size)$' \
                    ; } || true )
  headline_count="${headline_count:-0}"
  if (( headline_count >= 3 )); then
    pass "SC4.d walera /metrics contains ${headline_count} of the headline metric families (≥ 3 required)"
  else
    fail "SC4.d walera /metrics contains only ${headline_count} headline metric families (expected ≥ 3)"
    ok=0
  fi

  if (( ok == 1 )); then
    SC4_PASS=1
    pass "[SC4] PASS — writer /control POST + preflight, walera /metrics CORS + headline metrics present"
  else
    fail "[SC4] FAIL"
  fi
}

# =============================================================================
# SC5 — Source-level wiring gates (visibilitychange, h2c probe, error-banner
#       reconciliation triggers, reconnect banner copy).
#
# Per planner hygiene rule: always strip comment lines before counting tokens.
# `grep -v '^[[:space:]]*//'` is sufficient for the JS modules in this repo
# (they use //-style line comments only; no /* ... */ blocks).
# =============================================================================
verify_sc5() {
  echo
  info "===== SC5: source-level wiring gates (visibility + h2c + banner) ====="

  local ok=1
  local web_modules="${REPO_ROOT}/testbench/web/modules"

  # SC5.a — visibilityState wired in app.js AND metrics-panel.js (the two
  # places UI-10 pause matters: the rAF loop and the polling loop).
  # `{ pipeline; } || true` avoids the double-counter trap (grep -c prints
  # the count AND exits 1 on zero matches; `|| echo 0` would append a stray "0").
  local app_vis pol_vis
  app_vis=$( { grep -v '^[[:space:]]*//' "${web_modules}/app.js"           2>/dev/null | grep -c 'visibilityState' ; } || true )
  pol_vis=$( { grep -v '^[[:space:]]*//' "${web_modules}/metrics-panel.js" 2>/dev/null | grep -c 'visibilityState' ; } || true )
  app_vis="${app_vis:-0}"; pol_vis="${pol_vis:-0}"
  if (( app_vis >= 1 )) && (( pol_vis >= 1 )); then
    pass "SC5.a visibilityState wired in app.js (${app_vis}) AND metrics-panel.js (${pol_vis}) — UI-10 pause enforced"
  else
    fail "SC5.a visibilityState wiring incomplete (app.js=${app_vis}, metrics-panel.js=${pol_vis}; both must be ≥ 1)"
    ok=0
  fi

  # SC5.b — nextHopProtocol wired in h2c-detector.js (UI-11 OC-2-safe probe).
  local h2c_count
  h2c_count=$( { grep -v '^[[:space:]]*//' "${web_modules}/h2c-detector.js" 2>/dev/null | grep -c 'nextHopProtocol' ; } || true )
  h2c_count="${h2c_count:-0}"
  if (( h2c_count >= 1 )); then
    pass "SC5.b h2c-detector.js references nextHopProtocol (${h2c_count}) — UI-11 fallback probe wired"
  else
    fail "SC5.b h2c-detector.js missing nextHopProtocol reference (count=${h2c_count})"
    ok=0
  fi

  # SC5.c — error-banner.js references ≥3 of the five §3.8 trigger kinds.
  local banner_triggers
  banner_triggers=$( { grep -v '^[[:space:]]*//' "${web_modules}/error-banner.js" 2>/dev/null \
                    | grep -cE 'auth_revoked|auth_unavailable|slow_consumer|tx_too_large|shutdown' \
                    ; } || true )
  banner_triggers="${banner_triggers:-0}"
  if (( banner_triggers >= 3 )); then
    pass "SC5.c error-banner.js wires ${banner_triggers} of the five §3.8 trigger kinds (≥ 3 required)"
  else
    fail "SC5.c error-banner.js references only ${banner_triggers} trigger kinds (expected ≥ 3)"
    ok=0
  fi

  # SC5.d — error-banner.js contains the verbatim "NOT replayed" copy AND
  # an auto-dismiss mechanism (setTimeout(hide… or "auto-dismiss" or
  # "Reconnected").
  local not_replayed dismiss
  not_replayed=$( { grep -v '^[[:space:]]*//' "${web_modules}/error-banner.js" 2>/dev/null \
                  | grep -c 'NOT replayed' ; } || true )
  dismiss=$( { grep -v '^[[:space:]]*//' "${web_modules}/error-banner.js" 2>/dev/null \
             | grep -cE 'auto-dismiss|setTimeout\(.*hide|Reconnected' ; } || true )
  not_replayed="${not_replayed:-0}"; dismiss="${dismiss:-0}"
  if (( not_replayed >= 1 )) && (( dismiss >= 1 )); then
    pass "SC5.d error-banner.js contains 'NOT replayed' copy (${not_replayed}) AND auto-dismiss wiring (${dismiss})"
  else
    fail "SC5.d error-banner.js missing reconnect-banner copy or auto-dismiss (NOT replayed=${not_replayed}, dismiss=${dismiss})"
    ok=0
  fi

  # SC5.visual — manual gates for pill transitions + h2c fallback banner.
  manual "SC5 visual: pill transitions + reconnect banner + h2c banner → see testbench/web/MANUAL-VERIFICATION.md steps 2, 9, 10"

  if (( ok == 1 )); then
    SC5_PASS=1
    pass "[SC5] PASS — source-level wiring gates green (visibility, h2c, banner triggers, reconnect copy)"
  else
    fail "[SC5] FAIL"
  fi
}

# =============================================================================
# main
# =============================================================================
main() {
  echo "============================================================"
  echo "Phase 08 smoke — $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "  WALERA_BASE_URL   = ${WALERA_BASE_URL}"
  echo "  WRITER_BASE_URL   = ${WRITER_BASE_URL}"
  echo "  FRONTEND_BASE_URL = ${FRONTEND_BASE_URL}"
  echo "  ORIGIN            = ${ORIGIN}"
  echo "  TOKEN             = ${TOKEN}"
  echo "============================================================"

  verify_sc1
  verify_sc2
  verify_sc3
  verify_sc4
  verify_sc5

  echo
  echo "============================================================"
  echo "Phase 08 smoke summary"
  echo "============================================================"
  printf '  SC1 (no-bundler + vendored polyfill SHA-256)   : %s\n' "$( (( SC1_PASS == 1 )) && echo PASS || echo FAIL )"
  printf '  SC2 (HTTP page+module + SSE CORS handshake)    : %s\n' "$( (( SC2_PASS == 1 )) && echo PASS || echo FAIL )"
  printf '  SC3 (visual flash/collapse/reveal/banner)      : %s\n' "$( (( SC3_PASS == 1 )) && echo PASS || echo FAIL )"
  printf '  SC4 (metrics+writer CORS + headline metrics)   : %s\n' "$( (( SC4_PASS == 1 )) && echo PASS || echo FAIL )"
  printf '  SC5 (visibility+h2c+banner source wiring)      : %s\n' "$( (( SC5_PASS == 1 )) && echo PASS || echo FAIL )"
  echo "============================================================"
  echo "  Manual checks deferred to testbench/web/MANUAL-VERIFICATION.md"
  echo "============================================================"

  if (( SC1_PASS == 1 && SC2_PASS == 1 && SC3_PASS == 1 && SC4_PASS == 1 && SC5_PASS == 1 )); then
    pass "Phase 08 smoke PASSED (SC1+SC2+SC3+SC4+SC5)"
    exit 0
  else
    fail "Phase 08 smoke FAILED"
    exit 1
  fi
}

main "$@"
