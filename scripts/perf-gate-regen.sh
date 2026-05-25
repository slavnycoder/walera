#!/usr/bin/env bash
# scripts/perf-gate-regen.sh — Phase 20 GATE-03 baseline rebuilder.
#
# Re-runs the FULL 5-minute Phase 19 bench cycle (1k steady 500cr×5rt +
# 5k stress 2000cr×5rt) on deeb007, parses the measured events_sent/s and
# events-per-writev ratios from each run, applies a 20% safety margin
# (floor = measured × 0.8) to absorb short-bench variance, then rewrites
# `test/perf/thresholds.yml` with the new floors plus refreshed `captured`
# (today's UTC date) and `captured_commit` (current HEAD short SHA)
# metadata. The operator reviews the resulting PR diff before committing.
#
# USE THIS WHEN:
#   A deliberate architectural improvement on the v1.4 perf surface (pool /
#   drain / router) raises the events_sent/s OR events-per-writev floor,
#   AND you have a real bench measurement proving the new floor is stable.
#   The intentional rebaseline is the ONLY sanctioned path for raising the
#   thresholds.yml floors.
#
# DO NOT use this to silence a regression. The PR-diff trail produced by
# this script (refreshed `captured_commit` + new floor values) is exactly
# the audit surface T-20-01 was designed to mitigate against. Lowering a
# floor here without architectural justification is the regression-masking
# pattern this gate exists to prevent.
#
# Wall-clock budget: ~13 minutes
#   compose-up + /healthz wait     ~ 30 s
#   1k bench (5-min steady)         ~ 5 m
#   5k bench (5-min stress)         ~ 5 m
#   tear-down + YAML compute        ~ 1 m
#
# Usage:
#   ./scripts/perf-gate-regen.sh
#       Default: 1k + 5k benches + YAML rewrite. REUSES the existing
#       VAL-01 strace baseline (bench-out/strace-baseline-*/) — does
#       NOT regenerate it.
#
#   ./scripts/perf-gate-regen.sh --regen-strace
#       Also re-runs the VAL-01 1k strace baseline. NOTE: the script
#       does NOT auto-update `baseline_strace.source_1k` in the
#       rewritten YAML — the operator updates that path manually in
#       the PR review. 5k strace regen is a v1.5 follow-up; this
#       script regenerates the 1k baseline only when --regen-strace
#       is set.
#
#   ./scripts/perf-gate-regen.sh --dry-run
#       Print the YAML that WOULD be written (between dry-run sentinel
#       lines) to stdout. Does NOT bring compose up, does NOT run the
#       bench, does NOT modify disk. Validates the script's schema
#       composition + flag parsing.
#
# Preflight:
#   - testbench compose stack must NOT already be up (the script brings
#     it up and tears it down with `down -v --remove-orphans`)
#   - The host should be quiet (no other CPU-heavy workloads); the
#     measurement basis is GOMAXPROCS=4, identical to Phase 19's reckon
#   - LOADGEN_AUTH_TOKEN must be exported, OR the script reads it from
#     testbench/.env (LOADGEN_AUTH_TOKEN= or MOCK_AUTH_TOKEN= line).
#     The script awk-extracts the value rather than `source`-ing the
#     file (avoids leaking every var in .env into the shell env).

set -euo pipefail

# --- Defaults (initialised per invocation, T-19-06 mitigation) ------------
regen_strace=false
dry_run=false

# --- Flag parser ----------------------------------------------------------
while [ $# -gt 0 ]; do
    case "$1" in
        --regen-strace) regen_strace=true; shift ;;
        --dry-run)      dry_run=true;      shift ;;
        -h|--help)
            sed -n '2,55p' "$0"
            exit 0
            ;;
        *)
            echo "perf-gate-regen.sh: unknown flag $1" >&2
            exit 2
            ;;
    esac
done

# --- Resolve repo root ----------------------------------------------------
script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
cd "$repo_root"

# --- Bench timing constants ----------------------------------------------
# bench_duration_seconds drives the bench.sh --duration flag AND the awk
# divisor that converts the cumulative events_sent_total counter to a
# per-second rate. The two MUST stay in lock-step — if a future operator
# raises the bench length, the divisor below adjusts automatically.
# strace_window_seconds drives the `timeout N strace -c ...` window AND
# the corresponding awk divisor for writev/s. Same coupling.
bench_duration_seconds=300
bench_duration_arg="5m"
strace_window_seconds=30

# --- Capture metadata up-front (T-20-16: captured_commit pinned at start) -
captured_date=$(date -u +%Y-%m-%d)
captured_commit=$(git rev-parse --short HEAD)
ts_1k=$(date -u +%Y%m%dT%H%M%S)
sleep 1
ts_5k=$(date -u +%Y%m%dT%H%M%S)
out_1k="bench-out/perf-gate-baseline-1k-${ts_1k}"
out_5k="bench-out/perf-gate-baseline-5k-${ts_5k}"

# --- Pre-existing strace baseline paths (overridden by --regen-strace) ----
# These are the read-only Phase 19 baseline artifacts referenced by the
# current test/perf/thresholds.yml. The script preserves them by default;
# --regen-strace re-runs the 1k baseline (5k regen is a v1.5 follow-up).
baseline_strace_1k_path="bench-out/strace-baseline-133455/strace-1k.txt"
baseline_strace_5k_path="bench-out/strace-baseline-133654/strace-5k.txt"

# ==========================================================================
# DRY-RUN PATH: compose stub YAML with placeholder measured values, print,
# exit. Does NOT bring compose up, does NOT modify disk.
# ==========================================================================
if [ "$dry_run" = "true" ]; then
    # Stub measured values — the dry-run validates the YAML composition
    # and flag-parsing pathway, NOT the actual bench math.
    events_1k=158286
    writev_1k=48114
    ratio_1k=3.29
    events_5k=209175
    writev_5k=45872
    ratio_5k=4.56
    floor_events_1k=$(awk -v e="$events_1k" 'BEGIN {printf "%.0f", e * 0.8}')
    floor_ratio_1k=$(awk  -v r="$ratio_1k"  'BEGIN {printf "%.1f", r * 0.8}')
    floor_events_5k=$(awk -v e="$events_5k" 'BEGIN {printf "%.0f", e * 0.8}')
    floor_ratio_5k=$(awk  -v r="$ratio_5k"  'BEGIN {printf "%.1f", r * 0.8}')

    cat <<EOF
=== DRY RUN: thresholds.yml would be ===
# test/perf/thresholds.yml — perf-gate floors for Phase 20.
# Regenerate with \`make perf-gate-baseline\` on deeb007.
schema_version: 1
captured: ${captured_date}
captured_commit: ${captured_commit}  # HEAD at regen time
baseline_strace:
  source_1k: ${baseline_strace_1k_path}
  source_5k: ${baseline_strace_5k_path}
val_02:
  subscribers: 1000
  scenario: steady
  commit_rate: 500
  rows_per_tx: 5
  channels: orders/all,devices/all,articles/all
  duration_seconds: 90
  warmup_discard_seconds: 30
  events_sent_min_per_s: ${floor_events_1k}        # measured ${events_1k}/s × 0.8 (DRY-RUN stub)
  events_per_writev_min: ${floor_ratio_1k}          # measured ${ratio_1k}:1 × 0.8 (DRY-RUN stub)
val_03:
  subscribers: 5000
  scenario: stress
  commit_rate: 2000
  rows_per_tx: 5
  channels: orders/all,devices/all,articles/all
  duration_seconds: 90
  warmup_discard_seconds: 30
  events_sent_min_per_s: ${floor_events_5k}        # measured ${events_5k}/s × 0.8 (DRY-RUN stub)
  events_per_writev_min: ${floor_ratio_5k}          # measured ${ratio_5k}:1 × 0.8 (DRY-RUN stub)
=== END DRY RUN ===
EOF
    echo "perf-gate-regen.sh: dry-run mode — test/perf/thresholds.yml NOT modified." >&2
    exit 0
fi

# ==========================================================================
# LIVE PATH: full bench cycle on deeb007.
# ==========================================================================

# --- Resolve LOADGEN_AUTH_TOKEN -------------------------------------------
# Prefer env; fall back to awk-extract from testbench/.env. Do NOT `source`
# testbench/.env (T-20-13: would leak every var, incl. any POSTGRES_PASSWORD
# override the operator set locally).
if [ -z "${LOADGEN_AUTH_TOKEN:-}" ]; then
    if [ -f testbench/.env ]; then
        LOADGEN_AUTH_TOKEN=$(awk -F= '/^LOADGEN_AUTH_TOKEN=/ {sub(/^LOADGEN_AUTH_TOKEN=/, ""); print; exit}' testbench/.env)
        if [ -z "$LOADGEN_AUTH_TOKEN" ]; then
            LOADGEN_AUTH_TOKEN=$(awk -F= '/^MOCK_AUTH_TOKEN=/ {sub(/^MOCK_AUTH_TOKEN=/, ""); print; exit}' testbench/.env)
        fi
    fi
fi
if [ -z "${LOADGEN_AUTH_TOKEN:-}" ]; then
    echo "perf-gate-regen.sh: LOADGEN_AUTH_TOKEN is unset and not found in testbench/.env" >&2
    echo "perf-gate-regen.sh: export it OR add LOADGEN_AUTH_TOKEN=... to testbench/.env" >&2
    exit 2
fi
export LOADGEN_AUTH_TOKEN

# --- Compose lifecycle ----------------------------------------------------
# T-20-14: install the trap BEFORE `up` so a partial up still tears down.
cleanup() {
    set +e
    docker compose -f testbench/docker-compose.yml down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "perf-gate-regen.sh: bringing testbench compose stack up..."
docker compose -f testbench/docker-compose.yml up -d --build

# --- Wait for /healthz (90s polling) --------------------------------------
echo "perf-gate-regen.sh: waiting for walera /healthz..."
ready=false
for i in $(seq 1 90); do
    if curl -fs --max-time 2 http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 1
done
if [ "$ready" != "true" ]; then
    echo "perf-gate-regen.sh: walera did not become healthy within 90s" >&2
    exit 1
fi

# --- Build bench binaries -------------------------------------------------
echo "perf-gate-regen.sh: building ./writer + ./loadgen..."
go build -o ./writer ./cmd/writer
go build -o ./loadgen ./cmd/loadgen

# --- Optional --regen-strace branch ---------------------------------------
# Re-runs the 1k strace baseline against a quiescent walera. The script
# does NOT auto-update baseline_strace.source_1k in the rewritten YAML —
# the operator points to the new file manually in the PR review (T-20-12:
# auto-update would couple a timestamp-dependent path into the YAML write
# step, which is auditing-hostile). 5k baseline regen is a v1.5 follow-up.
if [ "$regen_strace" = "true" ]; then
    strace_ts=$(date -u +%H%M%S)
    strace_dir="bench-out/strace-baseline-${strace_ts}"
    mkdir -p "$strace_dir"
    echo "perf-gate-regen.sh: capturing fresh 1k strace baseline (${strace_window_seconds}s sample)..."
    # 30s sample inside the alpine sidecar against the running walera-app
    # container (matches Phase 19 plan 19-01 pattern verbatim). The
    # `timeout N strace -c ...` form delivers SIGTERM at N seconds; strace
    # catches it and prints its -c summary on exit. This is the same
    # behaviour pattern the gate test itself uses (sleep N; kill -INT) and
    # the canonical recipe for "strace for N seconds and dump the table".
    # Drop `|| true` — a strace failure here must abort the regen, NOT
    # silently produce a zero-floor YAML (CR-02 regression-masking guard).
    docker run --rm \
        --pid=container:walera-app \
        --cap-add SYS_PTRACE \
        alpine sh -c "apk add --no-cache strace >/dev/null && WALERA_PID=\$(pgrep -f /cdc-sse | head -1) && timeout ${strace_window_seconds} strace -f -c -e trace=write,writev -p \$WALERA_PID 2>&1" \
        >"$strace_dir/strace-1k.txt"
    if ! grep -q 'writev' "$strace_dir/strace-1k.txt"; then
        echo "perf-gate-regen.sh: 1k strace baseline missing writev row — refusing to write YAML" >&2
        exit 1
    fi
    echo "perf-gate-regen.sh: 1k strace baseline captured to $strace_dir/strace-1k.txt"
    echo "perf-gate-regen.sh: NOTE — update baseline_strace.source_1k in test/perf/thresholds.yml manually if you want this baseline locked in."
fi

# --- 1k bench (FULL 5-min) ------------------------------------------------
echo "perf-gate-regen.sh: launching 1k steady bench (${bench_duration_arg})..."
bash scripts/bench.sh \
    --scenario steady \
    --subscribers 1000 \
    --duration "$bench_duration_arg" \
    --commit-rate 500 \
    --rows-per-tx 5 \
    --channels orders/all,devices/all,articles/all \
    --pg-dsn "postgres://walera:walera@127.0.0.1:5432/walera?sslmode=disable" \
    --pprof-addr 127.0.0.1:6060 \
    --writer-bin ./writer \
    --loadgen-bin ./loadgen \
    --out-dir "$out_1k" &
bench_pid=$!
sleep 90
echo "perf-gate-regen.sh: capturing 1k strace sample (${strace_window_seconds}s, mid-bench)..."
docker run --rm \
    --pid=container:walera-app \
    --cap-add SYS_PTRACE \
    alpine sh -c "apk add --no-cache strace >/dev/null && WALERA_PID=\$(pgrep -f /cdc-sse | head -1) && timeout ${strace_window_seconds} strace -f -c -e trace=write,writev -p \$WALERA_PID 2>&1" \
    >"$out_1k/strace-sample-1k.txt"
if ! grep -q 'writev' "$out_1k/strace-sample-1k.txt"; then
    echo "perf-gate-regen.sh: 1k strace sample missing writev row — refusing to write YAML" >&2
    exit 1
fi
wait "$bench_pid"

# --- 5k bench (FULL 5-min) ------------------------------------------------
echo "perf-gate-regen.sh: launching 5k stress bench (${bench_duration_arg})..."
bash scripts/bench.sh \
    --scenario stress \
    --subscribers 5000 \
    --duration "$bench_duration_arg" \
    --commit-rate 2000 \
    --rows-per-tx 5 \
    --channels orders/all,devices/all,articles/all \
    --pg-dsn "postgres://walera:walera@127.0.0.1:5432/walera?sslmode=disable" \
    --pprof-addr 127.0.0.1:6060 \
    --writer-bin ./writer \
    --loadgen-bin ./loadgen \
    --out-dir "$out_5k" &
bench_pid=$!
sleep 90
echo "perf-gate-regen.sh: capturing 5k strace sample (${strace_window_seconds}s, mid-bench)..."
docker run --rm \
    --pid=container:walera-app \
    --cap-add SYS_PTRACE \
    alpine sh -c "apk add --no-cache strace >/dev/null && WALERA_PID=\$(pgrep -f /cdc-sse | head -1) && timeout ${strace_window_seconds} strace -f -c -e trace=write,writev -p \$WALERA_PID 2>&1" \
    >"$out_5k/strace-sample-5k.txt"
if ! grep -q 'writev' "$out_5k/strace-sample-5k.txt"; then
    echo "perf-gate-regen.sh: 5k strace sample missing writev row — refusing to write YAML" >&2
    exit 1
fi
wait "$bench_pid"

# --- Parse measured ratios -----------------------------------------------
# events_sent/s averaged over the bench window (cumulative counter /
# bench_duration_seconds). Filter on `type="wildcard"` to mirror the gate
# test (WR-03: heartbeats land on the same counter, but the comparison is
# apples-to-apples so a heartbeat-cadence change cannot false-alarm the
# gate). writev calls extracted from `strace -c` summary (column 4 == calls;
# stable across 5- and 6-field layouts) divided by strace_window_seconds.
# Both divisors are sourced from the variables declared at the top of the
# live path — change the bench duration there and the divisors follow.
events_1k=$(awk -v d="$bench_duration_seconds" '/^walera_events_sent_total\{[^}]*type="wildcard"/ {s+=$2} END {if (s>0) printf "%.0f", s/d; else print "0"}' "$out_1k/walera-metrics.txt")
writev_1k=$(awk -v w="$strace_window_seconds" '/writev$/ {print $4; exit}' "$out_1k/strace-sample-1k.txt" 2>/dev/null | awk -v w="$strace_window_seconds" '{if ($1>0) printf "%.0f", $1/w; else print "0"}')
ratio_1k=$(awk -v e="$events_1k" -v w="$writev_1k" 'BEGIN {if (w>0) printf "%.2f", e/w; else print "0"}')

events_5k=$(awk -v d="$bench_duration_seconds" '/^walera_events_sent_total\{[^}]*type="wildcard"/ {s+=$2} END {if (s>0) printf "%.0f", s/d; else print "0"}' "$out_5k/walera-metrics.txt")
writev_5k=$(awk -v w="$strace_window_seconds" '/writev$/ {print $4; exit}' "$out_5k/strace-sample-5k.txt" 2>/dev/null | awk -v w="$strace_window_seconds" '{if ($1>0) printf "%.0f", $1/w; else print "0"}')
ratio_5k=$(awk -v e="$events_5k" -v w="$writev_5k" 'BEGIN {if (w>0) printf "%.2f", e/w; else print "0"}')

# --- Apply 20% safety margin ----------------------------------------------
floor_events_1k=$(awk -v e="$events_1k" 'BEGIN {printf "%.0f", e * 0.8}')
floor_ratio_1k=$(awk  -v r="$ratio_1k"  'BEGIN {printf "%.1f", r * 0.8}')
floor_events_5k=$(awk -v e="$events_5k" 'BEGIN {printf "%.0f", e * 0.8}')
floor_ratio_5k=$(awk  -v r="$ratio_5k"  'BEGIN {printf "%.1f", r * 0.8}')

# --- Compose the new YAML -------------------------------------------------
new_yaml=$(cat <<EOF
# test/perf/thresholds.yml — perf-gate floors for Phase 20.
# Regenerate with \`make perf-gate-baseline\` on deeb007.
schema_version: 1
captured: ${captured_date}
captured_commit: ${captured_commit}  # HEAD at regen time
baseline_strace:
  source_1k: ${baseline_strace_1k_path}
  source_5k: ${baseline_strace_5k_path}
val_02:
  subscribers: 1000
  scenario: steady
  commit_rate: 500
  rows_per_tx: 5
  channels: orders/all,devices/all,articles/all
  duration_seconds: 90
  warmup_discard_seconds: 30
  events_sent_min_per_s: ${floor_events_1k}        # measured ${events_1k}/s × 0.8
  events_per_writev_min: ${floor_ratio_1k}          # measured ${ratio_1k}:1 × 0.8
val_03:
  subscribers: 5000
  scenario: stress
  commit_rate: 2000
  rows_per_tx: 5
  channels: orders/all,devices/all,articles/all
  duration_seconds: 90
  warmup_discard_seconds: 30
  events_sent_min_per_s: ${floor_events_5k}        # measured ${events_5k}/s × 0.8
  events_per_writev_min: ${floor_ratio_5k}          # measured ${ratio_5k}:1 × 0.8
EOF
)

# --- Write or print -------------------------------------------------------
# WR-05: atomic rename. Write the rendered YAML to a sibling tempfile and
# `mv` it into place — POSIX rename(2) within the same directory is atomic,
# so an interrupt between the printf and the mv leaves the previous YAML
# intact. Without this, a SIGINT mid-write would land a zero-byte
# thresholds.yml on disk and break every subsequent CI run with a
# misleading "yaml: unmarshal" error.
tmp_yaml=$(mktemp test/perf/thresholds.yml.XXXXXX)
trap 'rm -f "$tmp_yaml"; cleanup' EXIT INT TERM
printf '%s\n' "$new_yaml" > "$tmp_yaml"
mv -f "$tmp_yaml" test/perf/thresholds.yml
echo "perf-gate-regen.sh: rewrote test/perf/thresholds.yml"
echo "  captured: ${captured_date}"
echo "  captured_commit: ${captured_commit}"
echo "  val_02 floor: events_sent_min_per_s=${floor_events_1k} events_per_writev_min=${floor_ratio_1k} (measured: ${events_1k}/s, ${ratio_1k}:1)"
echo "  val_03 floor: events_sent_min_per_s=${floor_events_5k} events_per_writev_min=${floor_ratio_5k} (measured: ${events_5k}/s, ${ratio_5k}:1)"
echo "perf-gate-regen.sh: review the PR diff before committing."
