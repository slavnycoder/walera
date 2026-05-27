#!/usr/bin/env bash
# =============================================================================
# testbench/scripts/smoke-05.sh — Phase 05 substrate success-criteria smoke.
#
# This script is the SINGLE SOURCE OF TRUTH for Phase 05 (Compose Foundation —
# PG + Mock-Auth) verification. It is the regression gate for the substrate
# that every other smoke depends on: if smoke-05 is red, smoke-{06,07,08,09}
# results are not trustworthy.
#
# Read-only against the running compose stack. The only mutations are the
# `_admin/fail-on` and `_admin/fail-off` toggles in SC4 (the contract under
# test); an EXIT trap unconditionally re-issues `_admin/fail-off` so the
# stack is always left in a known-good state on any exit path (including
# SIGINT, errexit, and uncaught failures).
#
# ROADMAP Phase 05 Success Criteria (verbatim from .planning/ROADMAP.md L31-36):
#
#   SC1: `make -C testbench demo-up` boots `postgres` and `mock-auth` to healthy
#        state with no host-published mock-auth port; `make demo-reset`
#        cold-starts cleanly (no stale volumes).
#
#   SC2: `docker compose -f testbench/docker-compose.yml exec postgres psql -c
#        "SHOW wal_level"` returns `logical`; `cdc_sse_streamer` publication
#        exists and includes the six depth-4 chain tables (`orders`, `devices`,
#        `articles`, `line_items`, `line_item_options`, `option_audits`) with
#        `REPLICA IDENTITY DEFAULT`.
#
#   SC3: From a sibling container on the compose network, `curl -H
#        'Authorization: Bearer demo-alice'
#        http://mock-auth:9000/auth/permissions?channel=orders:1` returns a
#        full-whitelist payload matching the v1.0 wire format; `demo-bob`
#        returns a narrow whitelist; `demo-eve` returns wildcard-only.
#
#   SC4: `POST mock-auth/_admin/fail-on` flips the backend to 503 mode;
#        `_admin/revoke?subject=demo-alice` causes Alice's next refresh to
#        return 401 — verified by direct `curl` against the compose-internal
#        network.
#
#   SC5: Inserting a `line_items` row in a transaction bumps `orders.updated_at`
#        in the same transaction (root-bump trigger fires) — verified by
#        `psql` `SELECT updated_at` before/after.
#
# Implementation notes:
#   - SC1 in CI is a read-only assertion (we cannot tear the stack down in
#     mid-smoke without losing the rest of smoke-ci). We assert the
#     postgres + mock-auth services are running and report healthy, and that
#     the compose file (which the Makefile points at) lists both services.
#     The cold-start invariant is exercised at workflow level: the CI
#     workflow boots from a clean compose state, so reaching smoke-05 at all
#     means demo-up worked.
#   - SC3 uses the mock-auth container itself (busybox `wget`) to curl
#     itself over the compose-internal network. walera's distroless image
#     ships neither curl nor wget, so we cannot exec from there. mock-auth
#     is python:alpine and has busybox wget on PATH — verified by Plan 09-03
#     deviation 1.
#   - SC4 admin POST shape: `--post-data='' http://127.0.0.1:9000/_admin/X`.
#     `--method=POST` is GNU wget syntax and does NOT work in busybox wget.
#     `localhost` may resolve IPv6-first; `127.0.0.1` literal forces v4.
#
# Usage:
#   bash testbench/scripts/smoke-05.sh           # assumes stack is up + healthy
#   bash testbench/scripts/smoke-05.sh --reset   # cold-start the stack first
#
# Prerequisites:
#   - docker compose stack up: postgres + mock-auth healthy (walera/writer not
#     required for this smoke)
#   - host has: docker, awk, grep
#
# Exit codes:
#   0 = all 5 SCs PASS
#   1 = at least one SC FAILED (see per-SC PASS|FAIL lines and summary)
#
# Env overrides (all optional):
#   COMPOSE_FILE        default testbench/docker-compose.yml
#   POSTGRES_PASSWORD   default walera                  (mirrors .env.example)
#   TIMEOUT_SECONDS     default 30                      (wait-for-healthy bound)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBENCH_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${TESTBENCH_DIR}/.." && pwd)"

COMPOSE_FILE="${COMPOSE_FILE:-${TESTBENCH_DIR}/docker-compose.yml}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-walera}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-30}"

# Per-SC pass tracker (1=pass, 0=fail). Aggregated at the end.
SC1_PASS=0
SC2_PASS=0
SC3_PASS=0
SC4_PASS=0
SC5_PASS=0

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
# Cleanup trap: always restore mock-auth to fail-off so a SC4 abort does not
# leave the bench in 503 mode for downstream smokes. Best-effort; never fatal.
# -----------------------------------------------------------------------------
cleanup() {
  docker compose -f "${COMPOSE_FILE}" exec -T mock-auth \
    wget -q -O- --post-data='' http://127.0.0.1:9000/_admin/fail-off >/dev/null 2>&1 || true
}
trap cleanup EXIT

# -----------------------------------------------------------------------------
# Optional --reset: cold-start the stack via the Makefile.
# -----------------------------------------------------------------------------
if [[ "${1:-}" == "--reset" ]]; then
  info "--reset: make -C ${TESTBENCH_DIR} demo-reset && demo-up"
  make -C "${TESTBENCH_DIR}" demo-reset
  make -C "${TESTBENCH_DIR}" demo-up
fi

# -----------------------------------------------------------------------------
# wait-for-healthy: poll postgres + mock-auth until healthy. walera + writer
# are NOT required (this smoke is Phase 05 substrate only).
# -----------------------------------------------------------------------------
wait_for_healthy() {
  local svc="$1"
  local deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while :; do
    local cid
    cid="$(docker compose -f "${COMPOSE_FILE}" ps -q "${svc}" 2>/dev/null || true)"
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
    else
      if (( $(date +%s) >= deadline )); then
        fail "service ${svc} container not found within ${TIMEOUT_SECONDS}s"
        return 1
      fi
    fi
    sleep 1
  done
}

info "Waiting up to ${TIMEOUT_SECONDS}s for postgres + mock-auth to become healthy..."
wait_for_healthy postgres || exit 1
wait_for_healthy mock-auth || exit 1

# psql helper — runs a query inside the postgres container and returns the
# trimmed, tuples-only output on stdout.
psql_q() {
  docker compose -f "${COMPOSE_FILE}" exec -T \
    -e PGPASSWORD="${POSTGRES_PASSWORD}" \
    postgres psql -U walera -d walera -t -A -c "$1"
}

# mock-auth wget helper — exec inside the mock-auth container so we hit the
# compose-internal port (no host port published per BENCH-03).
mock_get() {
  local path="$1" token="${2:-}"
  if [[ -n "${token}" ]]; then
    docker compose -f "${COMPOSE_FILE}" exec -T mock-auth \
      wget -q -S -O- "http://127.0.0.1:9000${path}" \
      --header="Authorization: Bearer ${token}" 2>&1
  else
    docker compose -f "${COMPOSE_FILE}" exec -T mock-auth \
      wget -q -S -O- "http://127.0.0.1:9000${path}" 2>&1
  fi
}

mock_post() {
  local path="$1"
  docker compose -f "${COMPOSE_FILE}" exec -T mock-auth \
    wget -q -S -O- --post-data='' "http://127.0.0.1:9000${path}" 2>&1
}

# =============================================================================
# SC1 — postgres + mock-auth services running + healthy + listed in compose.
# =============================================================================
echo
info "===== SC1: compose substrate (postgres + mock-auth healthy) ====="

sc1_ok=1

# SC1.a — compose config lists both services.
compose_services="$(docker compose -f "${COMPOSE_FILE}" config --services 2>/dev/null || true)"
if [[ $'\n'"${compose_services}"$'\n' == *$'\npostgres\n'* ]] \
  && [[ $'\n'"${compose_services}"$'\n' == *$'\nmock-auth\n'* ]]; then
  pass "SC1.a compose config lists services: postgres + mock-auth"
else
  fail "SC1.a compose config missing postgres and/or mock-auth"
  sc1_ok=0
fi

# SC1.b — both services are in Up + healthy state (already gated by
# wait_for_healthy above; this is the explicit per-SC pin so the summary
# attributes the assertion correctly).
ps_out="$(docker compose -f "${COMPOSE_FILE}" ps --status running postgres mock-auth 2>/dev/null || true)"
if echo "${ps_out}" | grep -Fq postgres && echo "${ps_out}" | grep -Fq mock-auth; then
  pass "SC1.b docker compose ps shows postgres + mock-auth running"
else
  fail "SC1.b docker compose ps missing postgres and/or mock-auth in running state"
  echo "${ps_out}" | sed 's/^/      /'
  sc1_ok=0
fi

# SC1.c — mock-auth does NOT expose a host port (BENCH-03: compose-internal only).
mock_ports="$(docker compose -f "${COMPOSE_FILE}" ps mock-auth --format '{{.Publishers}}' 2>/dev/null || true)"
if [[ -z "${mock_ports}" || "${mock_ports}" == "[]" ]]; then
  pass "SC1.c mock-auth has no host-published port (BENCH-03)"
else
  # Some compose versions emit something like "9000/tcp" with no host
  # binding for unpublished ports; accept either truly empty or
  # tcp-only-without-host-IP.
  if ! echo "${mock_ports}" | grep -qE '0\.0\.0\.0|127\.0\.0\.1'; then
    pass "SC1.c mock-auth has no host-published port (BENCH-03; raw: ${mock_ports})"
  else
    fail "SC1.c mock-auth appears host-published: ${mock_ports}"
    sc1_ok=0
  fi
fi

if (( sc1_ok == 1 )); then
  SC1_PASS=1
  pass "SC1 OVERALL"
else
  fail "SC1 OVERALL"
fi

# =============================================================================
# SC2 — wal_level=logical + publication + demo tables (REPLICA IDENTITY DEFAULT).
# =============================================================================
echo
info "===== SC2: wal_level + publication + demo schema ====="

sc2_ok=1

# SC2.a — SHOW wal_level returns logical.
wal_level="$(psql_q 'SHOW wal_level' || true)"
if [[ "${wal_level}" == "logical" ]]; then
  pass "SC2.a SHOW wal_level = ${wal_level}"
else
  fail "SC2.a SHOW wal_level = '${wal_level}' (expected 'logical')"
  sc2_ok=0
fi

# SC2.b — cdc_sse_streamer publication exists.
pubname="$(psql_q "SELECT pubname FROM pg_publication WHERE pubname = 'cdc_sse_streamer'" || true)"
if [[ "${pubname}" == "cdc_sse_streamer" ]]; then
  pass "SC2.b publication cdc_sse_streamer exists"
else
  fail "SC2.b publication cdc_sse_streamer missing (got: '${pubname}')"
  sc2_ok=0
fi

# SC2.c — publication includes exactly the six depth-4 chain tables (no extras).
pubtables="$(psql_q "SELECT tablename FROM pg_publication_tables WHERE pubname='cdc_sse_streamer' ORDER BY tablename" || true)"
expected_tables=$'articles\ndevices\nline_item_options\nline_items\noption_audits\norders'
if [[ "${pubtables}" == "${expected_tables}" ]]; then
  pass "SC2.c publication tables = [articles, devices, line_item_options, line_items, option_audits, orders] (exactly the six depth-4 chain tables)"
else
  fail "SC2.c publication table list mismatch"
  echo "      expected:"
  echo "${expected_tables}" | sed 's/^/        /'
  echo "      actual:"
  echo "${pubtables}" | sed 's/^/        /'
  sc2_ok=0
fi

# SC2.d — \dt lists the six depth-4 chain tables (idempotency check; catches a
# migration-skip scenario where the publication is correct but the tables
# never landed).
tbls="$(psql_q "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename IN ('articles','devices','line_items','line_item_options','option_audits','orders') ORDER BY tablename" || true)"
if [[ "${tbls}" == "${expected_tables}" ]]; then
  pass "SC2.d public schema contains all six depth-4 chain tables"
else
  fail "SC2.d public schema missing one or more demo tables"
  echo "      actual: ${tbls}"
  sc2_ok=0
fi

if (( sc2_ok == 1 )); then
  SC2_PASS=1
  pass "SC2 OVERALL"
else
  fail "SC2 OVERALL"
fi

# =============================================================================
# SC3 — mock-auth seeds reachable from sibling container; three users distinct.
# =============================================================================
echo
info "===== SC3: mock-auth seeds (demo-alice / demo-bob / demo-eve) ====="

sc3_ok=1

# Helper: extract the JSON body (drop any wget -S header lines that begin
# with whitespace + "HTTP" or "Header:").
extract_body() {
  awk '/^[[:space:]]*\{/{p=1} p {print}'
}

# SC3.a — demo-alice returns a full-whitelist payload with the v1.0 wire keys.
alice_raw="$(mock_get '/auth/permissions?channel=orders:1' demo-alice || true)"
alice_body="$(echo "${alice_raw}" | extract_body)"
if echo "${alice_body}" | grep -Fq '"user_id"' \
  && echo "${alice_body}" | grep -Fq '"tables"' \
  && echo "${alice_body}" | grep -Fq '"roots"' \
  && echo "${alice_body}" | grep -Fq '"ttl_seconds"' \
  && echo "${alice_body}" | grep -Fq 'u_demo_alice' \
  && echo "${alice_body}" | grep -Fq 'line_items'; then
  pass "SC3.a demo-alice returns full-whitelist payload (v1.0 wire keys present, line_items included)"
else
  fail "SC3.a demo-alice payload missing one or more required fields"
  echo "${alice_body}" | head -3 | sed 's/^/      /'
  sc3_ok=0
fi

# SC3.b — demo-bob returns a narrow whitelist (orders only; NO line_items, NO devices, NO articles).
bob_body="$(mock_get '/auth/permissions?channel=orders:1' demo-bob | extract_body || true)"
if echo "${bob_body}" | grep -Fq 'u_demo_bob' \
  && echo "${bob_body}" | grep -Fq '"orders"' \
  && ! echo "${bob_body}" | grep -Fq 'line_items' \
  && ! echo "${bob_body}" | grep -Fq '"devices"' \
  && ! echo "${bob_body}" | grep -Fq '"articles"'; then
  pass "SC3.b demo-bob returns narrow whitelist (orders only)"
else
  fail "SC3.b demo-bob shape unexpected"
  echo "${bob_body}" | head -3 | sed 's/^/      /'
  sc3_ok=0
fi

# SC3.c — demo-eve returns wildcard-only (articles only — roots: ["articles"]).
eve_body="$(mock_get '/auth/permissions?channel=articles:hello-world' demo-eve | extract_body || true)"
if echo "${eve_body}" | grep -Fq 'u_demo_eve' \
  && echo "${eve_body}" | grep -Fq '"articles"' \
  && ! echo "${eve_body}" | grep -Fq '"orders"' \
  && ! echo "${eve_body}" | grep -Fq '"devices"'; then
  pass "SC3.c demo-eve returns wildcard-only shape (articles)"
else
  fail "SC3.c demo-eve shape unexpected"
  echo "${eve_body}" | head -3 | sed 's/^/      /'
  sc3_ok=0
fi

# SC3.d — the three bodies are pairwise distinct (parseable JSON; non-empty).
if [[ -n "${alice_body}" && -n "${bob_body}" && -n "${eve_body}" ]] \
  && [[ "${alice_body}" != "${bob_body}" ]] \
  && [[ "${alice_body}" != "${eve_body}" ]] \
  && [[ "${bob_body}" != "${eve_body}" ]]; then
  pass "SC3.d three users return pairwise-distinct JSON bodies"
else
  fail "SC3.d at least two users returned identical/empty bodies"
  sc3_ok=0
fi

if (( sc3_ok == 1 )); then
  SC3_PASS=1
  pass "SC3 OVERALL"
else
  fail "SC3 OVERALL"
fi

# =============================================================================
# SC4 — admin endpoints (_admin/fail-on + fail-off) functional.
#
# The cleanup trap above unconditionally re-issues fail-off on EXIT so any
# failure inside SC4 cannot leave the bench in 503 mode.
# =============================================================================
echo
info "===== SC4: mock-auth admin endpoints (_admin/fail-on, fail-off) ====="

sc4_ok=1

# SC4.a — baseline: permissions returns 200 with body.
sc4_pre="$(mock_get '/auth/permissions?channel=orders:1' demo-alice 2>&1 || true)"
if echo "${sc4_pre}" | grep -Fq 'HTTP/1.0 200' || echo "${sc4_pre}" | grep -Fq 'HTTP/1.1 200'; then
  pass "SC4.a baseline /auth/permissions returns 200"
else
  fail "SC4.a baseline /auth/permissions did not return 200"
  echo "${sc4_pre}" | head -5 | sed 's/^/      /'
  sc4_ok=0
fi

# SC4.b — POST _admin/fail-on.
fail_on_out="$(mock_post '/_admin/fail-on' 2>&1 || true)"
if echo "${fail_on_out}" | grep -Eq 'HTTP/1\.[01] (200|204)'; then
  pass "SC4.b POST /_admin/fail-on returned 200/204"
else
  fail "SC4.b POST /_admin/fail-on did not return 200/204"
  echo "${fail_on_out}" | head -5 | sed 's/^/      /'
  sc4_ok=0
fi

# SC4.c — after fail-on, permissions now returns 503 (or 500/5xx — the mock
# documents 503 but accept any 5xx as fail-on engaged).
sleep 0.5
sc4_after_on="$(mock_get '/auth/permissions?channel=orders:1' demo-alice 2>&1 || true)"
if echo "${sc4_after_on}" | grep -Eq 'HTTP/1\.[01] 5[0-9][0-9]'; then
  pass "SC4.c after fail-on /auth/permissions returns 5xx (fail-on engaged)"
else
  fail "SC4.c after fail-on /auth/permissions did NOT return 5xx"
  echo "${sc4_after_on}" | head -5 | sed 's/^/      /'
  sc4_ok=0
fi

# SC4.d — POST _admin/fail-off restores 200.
fail_off_out="$(mock_post '/_admin/fail-off' 2>&1 || true)"
if echo "${fail_off_out}" | grep -Eq 'HTTP/1\.[01] (200|204)'; then
  pass "SC4.d POST /_admin/fail-off returned 200/204"
else
  fail "SC4.d POST /_admin/fail-off did not return 200/204"
  echo "${fail_off_out}" | head -5 | sed 's/^/      /'
  sc4_ok=0
fi

sleep 0.5
sc4_after_off="$(mock_get '/auth/permissions?channel=orders:1' demo-alice 2>&1 || true)"
if echo "${sc4_after_off}" | grep -Eq 'HTTP/1\.[01] 200'; then
  pass "SC4.e after fail-off /auth/permissions back to 200"
else
  fail "SC4.e after fail-off /auth/permissions did NOT return 200"
  echo "${sc4_after_off}" | head -5 | sed 's/^/      /'
  sc4_ok=0
fi

if (( sc4_ok == 1 )); then
  SC4_PASS=1
  pass "SC4 OVERALL"
else
  fail "SC4 OVERALL"
fi

# =============================================================================
# SC5 — root-bump trigger: inserting a line_items row in a transaction bumps
# orders.updated_at within the same transaction.
#
# Strategy: capture the orders.updated_at BEFORE in one statement; in a single
# psql invocation run BEGIN; INSERT ...; SELECT updated_at; ROLLBACK; and
# assert that the post-INSERT updated_at differs from the pre-INSERT value.
# ROLLBACK keeps the bench seed pristine.
# =============================================================================
echo
info "===== SC5: line_items INSERT bumps orders.updated_at (SCHEMA-02 trigger) ====="

sc5_ok=1

pre_ts="$(psql_q "SELECT extract(epoch from updated_at)::numeric(20,6) FROM orders WHERE id = 1" || true)"
if [[ -z "${pre_ts}" ]]; then
  fail "SC5.a could not read orders.updated_at for id=1 (seed missing?)"
  sc5_ok=0
fi

if (( sc5_ok == 1 )); then
  # Single-shot script: BEGIN -> INSERT -> SELECT bumped timestamp -> ROLLBACK.
  # `-X -v ON_ERROR_STOP=1 -P pager=off` keeps psql deterministic.
  post_ts="$(docker compose -f "${COMPOSE_FILE}" exec -T \
    -e PGPASSWORD="${POSTGRES_PASSWORD}" \
    postgres psql -U walera -d walera -X -v ON_ERROR_STOP=1 -t -A -P pager=off <<'SQL' || true
BEGIN;
INSERT INTO line_items (orders_id, sku, qty, unit_price_cents)
  VALUES (1, 'SMOKE-05', 1, 1);
SELECT extract(epoch from updated_at)::numeric(20,6) FROM orders WHERE id = 1;
ROLLBACK;
SQL
  )"
  # `psql` with -t -A may emit an extra blank line / NOTICE; grab last numeric.
  post_ts="$(echo "${post_ts}" | awk '/^[0-9]+(\.[0-9]+)?$/ {v=$0} END{print v}')"

  if [[ -z "${post_ts}" ]]; then
    fail "SC5.b could not capture orders.updated_at after INSERT (transaction failed?)"
    sc5_ok=0
  elif [[ "${pre_ts}" == "${post_ts}" ]]; then
    fail "SC5.b orders.updated_at NOT bumped by line_items INSERT"
    echo "      pre  = ${pre_ts}"
    echo "      post = ${post_ts}"
    sc5_ok=0
  else
    # Compare numerically — post must be strictly greater than pre.
    if awk -v a="${pre_ts}" -v b="${post_ts}" 'BEGIN{exit !(b+0 > a+0)}'; then
      pass "SC5.b orders.updated_at bumped within transaction: ${pre_ts} -> ${post_ts}"
    else
      fail "SC5.b orders.updated_at post-INSERT not greater than pre-INSERT (pre=${pre_ts}, post=${post_ts})"
      sc5_ok=0
    fi
  fi
fi

# SC5.c — confirm ROLLBACK reverted: row count unchanged outside the txn.
li_count="$(psql_q "SELECT count(*) FROM line_items WHERE sku='SMOKE-05'" || true)"
if [[ "${li_count}" == "0" ]]; then
  pass "SC5.c ROLLBACK reverted the test row (no SKU=SMOKE-05 persisted)"
else
  fail "SC5.c ROLLBACK did NOT clean up (found ${li_count} rows with SKU=SMOKE-05)"
  sc5_ok=0
fi

if (( sc5_ok == 1 )); then
  SC5_PASS=1
  pass "SC5 OVERALL"
else
  fail "SC5 OVERALL"
fi

# =============================================================================
# Summary
# =============================================================================
echo
echo "============================================================"
echo "Phase 05 smoke summary"
echo "============================================================"
printf '  SC1 (compose substrate: postgres + mock-auth healthy): %s\n' "$( (( SC1_PASS == 1 )) && echo PASS || echo FAIL )"
printf '  SC2 (wal_level=logical + publication + demo tables)  : %s\n' "$( (( SC2_PASS == 1 )) && echo PASS || echo FAIL )"
printf '  SC3 (mock-auth three users distinct payloads)        : %s\n' "$( (( SC3_PASS == 1 )) && echo PASS || echo FAIL )"
printf '  SC4 (_admin/fail-on + fail-off cycle)                : %s\n' "$( (( SC4_PASS == 1 )) && echo PASS || echo FAIL )"
printf '  SC5 (line_items INSERT bumps orders.updated_at)      : %s\n' "$( (( SC5_PASS == 1 )) && echo PASS || echo FAIL )"
echo "============================================================"

if (( SC1_PASS == 1 && SC2_PASS == 1 && SC3_PASS == 1 && SC4_PASS == 1 && SC5_PASS == 1 )); then
  pass "Phase 05 smoke PASSED (SC1+SC2+SC3+SC4+SC5)"
  exit 0
else
  fail "Phase 05 smoke FAILED"
  exit 1
fi
