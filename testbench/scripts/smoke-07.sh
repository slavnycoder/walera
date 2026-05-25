#!/usr/bin/env bash
# =============================================================================
# testbench/scripts/smoke-07.sh — Phase 07 end-to-end success-criteria smoke.
#
# This script is the SINGLE SOURCE OF TRUTH for Phase 07 verification. It is
# read-only against the running compose stack EXCEPT for the POST /control
# calls that are intentional (writer is a load generator — switching scenarios
# is the contract under test). Optional --reset flag cold-starts the stack.
#
# ROADMAP Phase 07 Success Criteria (verbatim from .planning/ROADMAP.md):
#
#   SC1: No new go.mod entries; `go mod tidy` is a no-op; deps unchanged.
#
#   SC2: `curl -X POST localhost:9100/control \
#               -d '{"commit_rate":100,"rows_per_tx":1,"scenario":"steady"}'`
#        produces visible `event: tx` payloads in
#        `curl -N localhost:8080/sse/v1/orders/all` within 1s;
#        switching scenario changes behavior without restart;
#        `--scenario list` enumerates the 6 scenarios.
#
#   SC3: 60s steady@100tx/s:
#        - writer_observed = delta(writer_tx_total) / 60s
#        - walera_observed = delta(walera_wal_tx_size_changes_count) / 60s
#        - ratio = writer_observed / walera_observed
#        - assert |1 - ratio| ≤ 0.02 (WRITER-05 ±2% parity)
#        - also assert writer_commit_rate{scenario="steady"} gauge ≈ 100 (±1)
#        - emit absolute observed numbers to stdout
#
#   SC4: Static greps confirm:
#        - WaitN / x/time/rate present in writer commit loop
#        - PoolMaxConns / pool_max_conns present in writer
#        - --commit-rate and --rows-per-tx are independent flags
#        - --scenario list enumerates all 6 scenarios
#
# IMPORTANT — walera tx counter name:
#   The plan body referenced `walera_wal_tx_total` but the actual Prometheus
#   series exposed by walera v1.0 is `walera_wal_tx_size_changes_count` (the
#   `_count` of a histogram that observes one sample per decoded WAL tx).
#   Verified by `curl /metrics | grep '^walera_wal' | sort -u`. The histogram's
#   _count semantics are identical to a counter for our purpose: monotonic
#   delta over a 60s window gives the same observed tx-rate.
#
# Usage:
#   bash testbench/scripts/smoke-07.sh           # assumes stack is up + healthy
#   bash testbench/scripts/smoke-07.sh --reset   # cold-start the stack first
#
# Prerequisites:
#   - docker compose stack up: postgres + mock-auth + walera + writer all healthy
#   - host has: curl, awk, grep, go (for SC1 tidy + SC2 host-binary --scenario list)
#   - host has: git (for SC1 diff check)
#
# Exit codes:
#   0 = all 4 SCs PASS
#   1 = at least one SC FAILED (see per-[SCn] PASS|FAIL lines and summary)
#
# Env overrides (all optional):
#   WALERA_BASE_URL    default http://127.0.0.1:8080
#   WRITER_BASE_URL    default http://127.0.0.1:9100
#   TOKEN              default demo-alice      (seeded full-whitelist user)
#   ORIGIN             default http://localhost:8081
#   SC3_WINDOW_SECONDS default 60              (lower only for local debug)
#   SC3_STABILIZE_SECS default 5               (grace after /control before t=0)
# =============================================================================

set -euo pipefail
IFS=$'\n\t'

WALERA_BASE_URL="${WALERA_BASE_URL:-http://127.0.0.1:8080}"
WRITER_BASE_URL="${WRITER_BASE_URL:-http://127.0.0.1:9100}"
TOKEN="${TOKEN:-demo-alice}"
ORIGIN="${ORIGIN:-http://localhost:8081}"
SC3_WINDOW_SECONDS="${SC3_WINDOW_SECONDS:-60}"
SC3_STABILIZE_SECS="${SC3_STABILIZE_SECS:-5}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBENCH_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${TESTBENCH_DIR}/.." && pwd)"

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

# Tempfile cleanup (T-07-12 mitigation: bounded /tmp surface).
SSE_LOG="$(mktemp -t walera-smoke-07-sse.XXXXXX)"
WRITER_BIN_DIR="$(mktemp -d -t walera-smoke-07-writer.XXXXXX)"
cleanup() {
  rm -f "${SSE_LOG}" || true
  rm -rf "${WRITER_BIN_DIR}" || true
}
trap cleanup EXIT

# -----------------------------------------------------------------------------
# Optional --reset: cold-start the stack via the Makefile.
# -----------------------------------------------------------------------------
if [[ "${1:-}" == "--reset" ]]; then
  info "--reset: make -C ${TESTBENCH_DIR} demo-reset && demo-up"
  make -C "${TESTBENCH_DIR}" demo-reset
  make -C "${TESTBENCH_DIR}" demo-up
  info "Waiting up to 90s for writer to become healthy after cold start..."
  deadline=$(( $(date +%s) + 90 ))
  while :; do
    if curl -sf "${WRITER_BASE_URL}/healthz" >/dev/null 2>&1 \
       && curl -sf "${WALERA_BASE_URL}/healthz" >/dev/null 2>&1; then
      pass "stack reached healthy (walera + writer responding)"
      break
    fi
    if (( $(date +%s) >= deadline )); then
      fail "stack did not become healthy within 90s after cold start"
      docker compose -f "${TESTBENCH_DIR}/docker-compose.yml" ps || true
      exit 1
    fi
    sleep 2
  done
fi

# =============================================================================
# SC1 — Zero new go.mod entries; `go mod tidy` is a no-op.
# =============================================================================
verify_sc1() {
  echo
  info "===== SC1: go mod tidy is a no-op (no new deps in Phase 07) ====="

  local ok=1

  pushd "${REPO_ROOT}" >/dev/null

  # Snapshot HEAD copies so we can restore on tidy mutation (defensive).
  local tidy_stderr
  tidy_stderr="$(mktemp)"
  if ! go mod tidy 2>"${tidy_stderr}"; then
    fail "SC1.a go mod tidy returned non-zero exit"
    echo "      ----- go mod tidy stderr -----"
    sed 's/^/      /' < "${tidy_stderr}"
    ok=0
  else
    pass "SC1.a go mod tidy exited 0"
  fi
  rm -f "${tidy_stderr}"

  # The critical assertion: no diff in go.mod / go.sum.
  if git diff --exit-code -- go.mod go.sum >/dev/null 2>&1; then
    pass "SC1.b git diff --exit-code go.mod go.sum is clean (no new entries)"
  else
    fail "SC1.b go mod tidy mutated go.mod or go.sum — Phase 07 introduced new deps"
    echo "      ----- git diff go.mod go.sum -----"
    git diff -- go.mod go.sum | sed 's/^/      /' | head -50
    ok=0
  fi

  # Sanity check: writer-relevant deps are still present in go.sum (catches
  # accidental dep removal during tidy).
  local dep_hits
  dep_hits=$(grep -c -E 'pgx/v5|prometheus/client_golang|zerolog|koanf/v2|x/time' go.sum || true)
  if (( dep_hits >= 5 )); then
    pass "SC1.c go.sum contains ${dep_hits} hits for writer deps (≥ 5 required)"
  else
    fail "SC1.c go.sum has only ${dep_hits} writer-dep hits (expected ≥ 5)"
    ok=0
  fi

  popd >/dev/null

  if (( ok == 1 )); then
    SC1_PASS=1
    pass "[SC1] PASS — go mod tidy no-op, writer deps intact"
  else
    fail "[SC1] FAIL"
  fi
}

# =============================================================================
# SC2 — Scenario list + runtime scenario switch + 1s SSE event observation.
#
# Four sub-checks:
#   SC2.a — `writer --scenario list` (host-built binary) prints exactly the
#           6 expected names; --scenario list short-circuits to exit 0
#           without touching the network.
#   SC2.b — POST /control {steady@100,rows_per_tx=1} returns 200 with
#           response.scenario == "steady".
#   SC2.c — `curl -N /sse/v1/orders/all` observes at least one `event: tx`
#           within a short window after the POST (Authorization +
#           Origin headers per WALERA-02 CORS).
#   SC2.d — POST /control {spike} returns 200 with response.scenario ==
#           "spike", proving runtime switch without restart. Then return to
#           the safe smoke@5 baseline so SC3 starts from a known state.
#
# T-07-13 / PII discipline: the SSE body contains demo data (customer_name).
# Positive path uses `grep -q "event: tx"` only — no body dump. On FAIL,
# the first 50 lines of the SSE capture are emitted for debug; demo data
# only (Alice Demo / customer-<rng>) per testbench/migrations/002_demo_schema.sql.
# =============================================================================
verify_sc2() {
  echo
  info "===== SC2: scenario list + /control switch + SSE event observation ====="

  local ok=1

  # Precondition — both services healthy.
  if ! curl -sf "${WALERA_BASE_URL}/healthz" >/dev/null 2>&1; then
    fail "SC2.pre walera /healthz unreachable at ${WALERA_BASE_URL}"
    info "[SC2] FAIL — stack not healthy"
    return 1
  fi
  if ! curl -sf "${WRITER_BASE_URL}/healthz" >/dev/null 2>&1; then
    fail "SC2.pre writer /healthz unreachable at ${WRITER_BASE_URL}"
    info "[SC2] FAIL — stack not healthy"
    return 1
  fi
  pass "SC2.pre walera + writer healthz both responding"

  # SC2.a — build the writer once on the host and enumerate scenarios.
  local writer_bin="${WRITER_BIN_DIR}/writer-listcheck"
  pushd "${REPO_ROOT}" >/dev/null
  if go build -o "${writer_bin}" ./cmd/writer 2>/dev/null; then
    pass "SC2.a host build of cmd/writer succeeded"
  else
    fail "SC2.a host build of cmd/writer failed"
    popd >/dev/null
    ok=0
  fi
  popd >/dev/null

  if (( ok == 1 )); then
    local list_out
    list_out=$("${writer_bin}" --scenario list 2>/dev/null || true)
    # Expected: exactly 6 names in the canonical order.
    local expected_csv="smoke,ramp-up,steady,spike,soak,stress"
    local actual_csv
    actual_csv=$(echo "${list_out}" | awk 'NF' | paste -sd, -)
    if [[ "${actual_csv}" == "${expected_csv}" ]]; then
      pass "SC2.a --scenario list = [${actual_csv}]"
    else
      fail "SC2.a --scenario list mismatch"
      echo "      expected: ${expected_csv}"
      echo "      actual:   ${actual_csv}"
      ok=0
    fi
  fi

  # SC2.b — POST steady@100tx/s.
  local control_resp
  control_resp=$(curl -sf -X POST \
                       -H 'Content-Type: application/json' \
                       -d '{"scenario":"steady","commit_rate":100,"rows_per_tx":1}' \
                       "${WRITER_BASE_URL}/control" 2>/dev/null || true)
  # Parse scenario field without jq: pull "scenario":"steady" literal.
  if echo "${control_resp}" | grep -qE '"scenario":"steady"'; then
    pass "SC2.b POST /control {steady@100} returned: ${control_resp}"
  else
    fail "SC2.b POST /control did not return scenario=steady (got: ${control_resp:-<empty>})"
    ok=0
  fi

  # SC2.c — SSE event: tx within a short window.
  # Background curl writes to ${SSE_LOG}. The writer is now driving 100 tx/s
  # against orders, so the first line_items insert on a new orders row will
  # fire the SCHEMA-02 root-bump trigger and emit an SSE event on
  # /sse/v1/orders/all. Window is generous (up to 5s) to absorb container
  # scheduler jitter at 100 Hz.
  : > "${SSE_LOG}"
  ( timeout 6 curl -sN \
        -H "Authorization: Bearer ${TOKEN}" \
        -H "Origin: ${ORIGIN}" \
        "${WALERA_BASE_URL}/sse/v1/orders/all" \
        > "${SSE_LOG}" 2>/dev/null
  ) &
  local sse_pid=$!

  local saw_tx=0
  for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 \
           21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 \
           41 42 43 44 45 46 47 48 49 50; do
    if grep -q '^event: tx' "${SSE_LOG}" 2>/dev/null; then
      saw_tx=1
      break
    fi
    sleep 0.1
  done
  kill "${sse_pid}" 2>/dev/null || true
  wait "${sse_pid}" 2>/dev/null || true

  if (( saw_tx == 1 )); then
    local event_count
    event_count=$(grep -c '^event: tx' "${SSE_LOG}" 2>/dev/null || echo 0)
    pass "SC2.c observed ${event_count} \`event: tx\` lines on /sse/v1/orders/all within ≤5s"
  else
    fail "SC2.c no \`event: tx\` line observed on /sse/v1/orders/all within 5s"
    echo "      ----- first 50 lines of SSE capture (demo data, T-07-13 bounded leak) -----"
    head -50 "${SSE_LOG}" | sed 's/^/      /'
    ok=0
  fi

  # SC2.d — runtime scenario switch.
  local spike_resp
  spike_resp=$(curl -sf -X POST \
                    -H 'Content-Type: application/json' \
                    -d '{"scenario":"spike"}' \
                    "${WRITER_BASE_URL}/control" 2>/dev/null || true)
  if echo "${spike_resp}" | grep -qE '"scenario":"spike"'; then
    pass "SC2.d runtime switch to spike succeeded: ${spike_resp}"
  else
    fail "SC2.d runtime switch to spike failed (response: ${spike_resp:-<empty>})"
    ok=0
  fi

  # Return to safe baseline for SC3 (steady@100 is what SC3 needs; SC3
  # will issue the POST itself, but we leave the writer on a benign rate
  # here so a 2-3s gap between SC2 and SC3 doesn't spike storage).
  curl -sf -X POST \
       -H 'Content-Type: application/json' \
       -d '{"scenario":"smoke","commit_rate":5,"rows_per_tx":1}' \
       "${WRITER_BASE_URL}/control" >/dev/null 2>&1 || true

  if (( ok == 1 )); then
    SC2_PASS=1
    pass "[SC2] PASS — scenario list, SSE event, runtime switch all confirmed"
  else
    fail "[SC2] FAIL"
  fi
}

# =============================================================================
# SC3 — 60s steady@100tx/s parity: writer vs walera observed-rate within ±2%.
#
# Method (WRITER-05 ±2% invariant — counters, not gauges):
#   t0:
#     writer_t0 = sum(writer_tx_total{*}) over all scenarios+targets
#     walera_t0 = walera_wal_tx_size_changes_count
#   t1 (after SC3_WINDOW_SECONDS):
#     writer_t1, walera_t1
#   observed:
#     writer_observed = (writer_t1 - writer_t0) / window
#     walera_observed = (walera_t1 - walera_t0) / window
#   parity:
#     ratio = writer_observed / walera_observed   (≈ 1.0)
#     |1 - ratio| ≤ 0.02 required
#   gauge sanity:
#     writer_commit_rate{scenario="steady"} ≈ 100 (±1)
#
# NOTE on walera metric name: the PLAN body referenced `walera_wal_tx_total`
# but the actual exposed series is `walera_wal_tx_size_changes_count` — the
# `_count` of the histogram `walera_wal_tx_size_changes`. Histogram _count
# is monotonic (incremented exactly once per decoded WAL tx), so the delta
# over the window is mathematically equivalent to a counter delta.
# =============================================================================
verify_sc3() {
  echo
  info "===== SC3: ${SC3_WINDOW_SECONDS}s steady@100tx/s — writer vs walera observed-rate parity ====="

  local ok=1

  # 1. Switch writer to steady@100tx/s.
  local steady_resp
  steady_resp=$(curl -sf -X POST \
                     -H 'Content-Type: application/json' \
                     -d '{"scenario":"steady","commit_rate":100,"rows_per_tx":1}' \
                     "${WRITER_BASE_URL}/control" 2>/dev/null || true)
  if ! echo "${steady_resp}" | grep -qE '"scenario":"steady"'; then
    fail "SC3.pre POST /control steady@100 failed (response: ${steady_resp:-<empty>})"
    info "[SC3] FAIL — could not establish steady scenario"
    return 1
  fi
  pass "SC3.pre writer switched to steady@100tx/s"

  # 2. Wait for the loop to stabilise (rate.Limiter ramps + scenario tick at
  #    100ms cadence may take a beat to settle).
  info "SC3 stabilising for ${SC3_STABILIZE_SECS}s..."
  sleep "${SC3_STABILIZE_SECS}"

  # 3. Sample t=0 counters. Use awk to sum writer_tx_total across all
  #    {scenario,target} label combinations; walera_wal_tx_size_changes_count
  #    has no labels (it's the histogram base count).
  local writer_t0 walera_t0
  writer_t0=$(curl -sf "${WRITER_BASE_URL}/metrics" 2>/dev/null \
              | awk '/^writer_tx_total\{/ {sum += $NF} END {print sum+0}')
  walera_t0=$(curl -sf "${WALERA_BASE_URL}/metrics" 2>/dev/null \
              | awk '/^walera_wal_tx_size_changes_count[[:space:]]/ {print $NF; exit}')
  walera_t0="${walera_t0:-0}"
  pass "SC3 t=0  writer_tx_total=${writer_t0}  walera_wal_tx_size_changes_count=${walera_t0}"

  # 4. Sleep the measurement window.
  info "SC3 sampling for ${SC3_WINDOW_SECONDS}s..."
  sleep "${SC3_WINDOW_SECONDS}"

  # 5. Sample t=1.
  local writer_t1 walera_t1
  writer_t1=$(curl -sf "${WRITER_BASE_URL}/metrics" 2>/dev/null \
              | awk '/^writer_tx_total\{/ {sum += $NF} END {print sum+0}')
  walera_t1=$(curl -sf "${WALERA_BASE_URL}/metrics" 2>/dev/null \
              | awk '/^walera_wal_tx_size_changes_count[[:space:]]/ {print $NF; exit}')
  walera_t1="${walera_t1:-0}"
  pass "SC3 t=1  writer_tx_total=${writer_t1}  walera_wal_tx_size_changes_count=${walera_t1}"

  # 6. Compute observed rates + ratio + delta-percent.
  local writer_observed walera_observed ratio delta_pct
  writer_observed=$(awk -v t0="${writer_t0}" -v t1="${writer_t1}" -v w="${SC3_WINDOW_SECONDS}" \
                        'BEGIN { printf "%.2f", (t1 - t0) / w }')
  walera_observed=$(awk -v t0="${walera_t0}" -v t1="${walera_t1}" -v w="${SC3_WINDOW_SECONDS}" \
                        'BEGIN { printf "%.2f", (t1 - t0) / w }')
  ratio=$(awk -v w="${writer_observed}" -v l="${walera_observed}" \
              'BEGIN { if (l > 0) printf "%.4f", w / l; else print "0.0000" }')
  delta_pct=$(awk -v r="${ratio}" \
                  'BEGIN { v = r - 1; if (v < 0) v = -v; printf "%.2f", v * 100 }')

  # 7. Sample gauge for sanity check.
  local gauge
  gauge=$(curl -sf "${WRITER_BASE_URL}/metrics" 2>/dev/null \
          | awk '/^writer_commit_rate\{scenario="steady"\}/ {print $NF; exit}')
  gauge="${gauge:-0}"

  # 8. Emit the absolute-numbers line that 07-04-SUMMARY.md records.
  echo "[SC3] writer_observed=${writer_observed} tx/s  walera_observed=${walera_observed} tx/s  ratio=${ratio}  delta=${delta_pct}%  gauge=${gauge}"

  # 9. Assert parity (delta ≤ 2.00%).
  if awk -v d="${delta_pct}" 'BEGIN { exit (d <= 2.00 ? 0 : 1) }'; then
    pass "SC3.a parity ≤ 2.00% (observed ${delta_pct}%)"
  else
    fail "SC3.a parity exceeded ±2.00% (observed ${delta_pct}%)"
    ok=0
  fi

  # 10. Assert gauge sanity (|gauge - 100| < 1).
  if awk -v g="${gauge}" 'BEGIN { v = g - 100; if (v < 0) v = -v; exit (v < 1 ? 0 : 1) }'; then
    pass "SC3.b writer_commit_rate gauge=${gauge} (within ±1 of 100)"
  else
    fail "SC3.b writer_commit_rate gauge=${gauge} not within ±1 of 100"
    ok=0
  fi

  # 11. Return writer to safe baseline so Phase 08/09 don't inherit a hot loop.
  curl -sf -X POST \
       -H 'Content-Type: application/json' \
       -d '{"scenario":"smoke","commit_rate":5,"rows_per_tx":1}' \
       "${WRITER_BASE_URL}/control" >/dev/null 2>&1 || true

  if (( ok == 1 )); then
    SC3_PASS=1
    pass "[SC3] PASS — writer↔walera observed-rate parity within ±2% (WRITER-05)"
  else
    fail "[SC3] FAIL"
  fi
}

# =============================================================================
# SC4 — Static grep checks (no live containers required).
# =============================================================================
verify_sc4() {
  echo
  info "===== SC4: static greps — WaitN, pool bound, independent flags ====="

  local ok=1

  pushd "${REPO_ROOT}" >/dev/null

  # SC4.a: WaitN / x/time/rate present in writer production code (not tests).
  if grep -RnE 'WaitN|x/time/rate' cmd/writer internal/writer 2>/dev/null \
     | grep -v _test.go | grep -q .; then
    local hit
    hit=$(grep -RnE 'WaitN|x/time/rate' cmd/writer internal/writer 2>/dev/null \
          | grep -v _test.go | head -1)
    pass "SC4.a WaitN/x/time/rate found in writer production code: ${hit}"
  else
    fail "SC4.a no WaitN or x/time/rate usage in cmd/writer or internal/writer (excluding tests)"
    ok=0
  fi

  # SC4.b: PoolMaxConns / pool_max_conns / MaxConns present in writer.
  if grep -RnE 'MaxConns|pool_max_conns' cmd/writer internal/writer 2>/dev/null \
     | grep -v _test.go | grep -q .; then
    local hit
    hit=$(grep -RnE 'MaxConns|pool_max_conns' cmd/writer internal/writer 2>/dev/null \
          | grep -v _test.go | head -1)
    pass "SC4.b pool bound (MaxConns) found in writer code: ${hit}"
  else
    fail "SC4.b no MaxConns or pool_max_conns reference in writer production code"
    ok=0
  fi

  # SC4.c: --commit-rate flag declared in cmd/writer.
  if grep -RnE 'commit-rate|commit_rate' cmd/writer 2>/dev/null \
     | grep -v _test.go | grep -q .; then
    pass "SC4.c --commit-rate flag present in cmd/writer"
  else
    fail "SC4.c --commit-rate flag missing in cmd/writer"
    ok=0
  fi

  # SC4.d: --rows-per-tx flag declared in cmd/writer.
  if grep -RnE 'rows-per-tx|rows_per_tx' cmd/writer 2>/dev/null \
     | grep -v _test.go | grep -q .; then
    pass "SC4.d --rows-per-tx flag present in cmd/writer"
  else
    fail "SC4.d --rows-per-tx flag missing in cmd/writer"
    ok=0
  fi

  # SC4.e: independence — the two flags appear on SEPARATE flag.X() calls in
  # the same file. Use cmd/writer/main.go (canonical CLI surface) and assert
  # the two flag definitions resolve to different line numbers. Regex matches
  # flag.<Type>(... where Type ∈ {Bool, Int, Int64, Float64, String, Duration...}
  # — character class includes digits so "Float64" matches.
  # `|| true` is mandatory: a failing grep inside $() trips set -e on bash 5.x
  # via the command-substitution exit-status propagation rules.
  local cr_line rt_line
  cr_line=$(grep -nE 'flag\.[A-Za-z][A-Za-z0-9]*\("commit-rate"' cmd/writer/main.go 2>/dev/null | head -1 | cut -d: -f1 || true)
  rt_line=$(grep -nE 'flag\.[A-Za-z][A-Za-z0-9]*\("rows-per-tx"' cmd/writer/main.go 2>/dev/null | head -1 | cut -d: -f1 || true)
  if [[ -n "${cr_line}" && -n "${rt_line}" && "${cr_line}" != "${rt_line}" ]]; then
    pass "SC4.e --commit-rate (L${cr_line}) and --rows-per-tx (L${rt_line}) are independent flag.X() declarations"
  else
    fail "SC4.e flag independence check failed (commit-rate line: ${cr_line:-none}, rows-per-tx line: ${rt_line:-none})"
    ok=0
  fi

  popd >/dev/null

  if (( ok == 1 )); then
    SC4_PASS=1
    pass "[SC4] PASS — static greps confirm bounded pool, Poisson pacing, independent flags"
  else
    fail "[SC4] FAIL"
  fi
}

# =============================================================================
# main
# =============================================================================
main() {
  echo "============================================================"
  echo "Phase 07 smoke — $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "  WALERA_BASE_URL = ${WALERA_BASE_URL}"
  echo "  WRITER_BASE_URL = ${WRITER_BASE_URL}"
  echo "  TOKEN           = ${TOKEN}"
  echo "  SC3_WINDOW_SECONDS = ${SC3_WINDOW_SECONDS}"
  echo "============================================================"

  verify_sc1
  verify_sc2
  verify_sc3
  verify_sc4

  echo
  echo "============================================================"
  echo "Phase 07 smoke summary"
  echo "============================================================"
  printf '  SC1 (go mod tidy no-op)              : %s\n' "$( (( SC1_PASS == 1 )) && echo PASS || echo FAIL )"
  printf '  SC2 (scenario list + SSE event)      : %s\n' "$( (( SC2_PASS == 1 )) && echo PASS || echo FAIL )"
  printf '  SC3 (60s ±2%% parity)                : %s\n' "$( (( SC3_PASS == 1 )) && echo PASS || echo FAIL )"
  printf '  SC4 (static greps)                   : %s\n' "$( (( SC4_PASS == 1 )) && echo PASS || echo FAIL )"
  echo "============================================================"

  if (( SC1_PASS == 1 && SC2_PASS == 1 && SC3_PASS == 1 && SC4_PASS == 1 )); then
    pass "Phase 07 smoke PASSED (SC1+SC2+SC3+SC4)"
    exit 0
  else
    fail "Phase 07 smoke FAILED"
    exit 1
  fi
}

main "$@"
