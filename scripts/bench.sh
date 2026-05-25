#!/usr/bin/env bash
# scripts/bench.sh — load-test + profiling orchestrator for Walera.
#
# Quick task 260518-lh1 / Task 3. Spins up the writer (test data source)
# and the cmd/loadgen binary against a running testbench compose stack,
# captures periodic pprof snapshots (heap / cpu profile / goroutine) from
# Walera's opt-in pprof listener, then dumps both /metrics endpoints and
# prints a one-line summary on exit.
#
# Strict-mode bash, no GNU-only flags (works on macOS dev boxes and Linux
# CI runners alike). Uses only POSIX-portable utilities (awk, sed, curl,
# grep, date, kill, sleep, mktemp without the -p flag); deliberately avoids
# basic-calc, GNU-only date flags, recursive-symlink readlink, and the
# GNU-only mktemp parent-dir flag (project NOT-USE list).
#
# Usage:
#   ./scripts/bench.sh \
#     --scenario steady \
#     --subscribers 1000 \
#     --duration 5m \
#     --pprof-addr 127.0.0.1:6060 \
#     --writer-bin ./writer \
#     --loadgen-bin ./loadgen \
#     --commit-rate 500 \
#     --rows-per-tx 5 \
#     --channels orders/all,devices/all,articles/all \
#     --pg-dsn 'postgres://walera:walera@127.0.0.1:5432/walera?sslmode=disable'
#
# Flags:
#   --scenario <name>      One of smoke|steady|spike|soak|stress. Required.
#   --subscribers <N>      Loadgen concurrency (default 1000).
#   --duration <D>         Run length (e.g. 5m, 30s). Default 5m.
#   --pprof-addr <addr>    Walera pprof listener (default 127.0.0.1:6060).
#   --writer-bin <path>    Writer binary (default ./writer).
#   --loadgen-bin <path>   Loadgen binary (default ./loadgen).
#   --out-dir <path>       Artefact directory (default bench-out/<ts>).
#   --commit-rate <float>  Override the scenario's default commit rate
#                          (commits/sec). Empty / unset → use the scenario
#                          default baked into the writer. Passed through to
#                          the writer binary as --commit-rate <N>.
#   --rows-per-tx <int>    Override the scenario's default rows-per-tx.
#                          Empty / unset → use the scenario default.
#                          Passed through to the writer as --rows-per-tx <N>.
#   --pg-dsn <DSN>         Override the writer's Postgres DSN (e.g. when the
#                          writer runs on the host against a published PG
#                          port rather than from inside the compose net).
#                          Empty / unset → use WRITER_PG_DSN env var if set,
#                          otherwise fall through to the writer's own
#                          config-file / env-var resolution. Passed to the
#                          writer as --pg-dsn <DSN>.
#   --channels <spec>      Comma-separated channel list (e.g.
#                          orders/all,devices/all) OR @path/to/file. Passed
#                          to loadgen as --channels <spec>. Required when
#                          launching loadgen — the binary fatals without it.
#                          Default: orders/all,devices/all,articles/all
#                          (matches the testbench mock-auth demo-alice
#                          whitelist).
#
# Auth: the loadgen reads its bearer credential from LOADGEN_AUTH_TOKEN
# (env var). Export it before invoking this script; the value is never
# echoed by this script.
#
# Preflight: the testbench compose stack MUST be up (the script aborts
# otherwise). pprof MUST be enabled on Walera — set
# http.pprof_addr: 127.0.0.1:6060 in the Walera config OR pass
# WALERA_HTTP_PPROF_ADDR=127.0.0.1:6060 on the compose env. The script
# tolerates a missing pprof endpoint (skips that snapshot) so a run
# without --pprof-addr still produces metrics output.

set -euo pipefail

# --- Defaults --------------------------------------------------------------
scenario=""
subscribers=1000
duration="5m"
pprof_addr="127.0.0.1:6060"
writer_bin="./writer"
loadgen_bin="./loadgen"
out_dir=""
# Empty-string sentinel = "do not pass to writer/loadgen; let its default win".
# Initialised per-invocation so prior bench.sh runs cannot leak state into
# the next one (no persisted state between runs — see T-19-06 in 19-01-PLAN).
commit_rate=""
rows_per_tx=""
pg_dsn=""
channels="orders/all,devices/all,articles/all"

# --- Flag parsing (POSIX while+case loop; mirrors coverage-gate.sh) -------
while [ $# -gt 0 ]; do
    case "$1" in
        --channels)      channels="$2"; shift 2 ;;
        --commit-rate)   commit_rate="$2"; shift 2 ;;
        --duration)      duration="$2"; shift 2 ;;
        --loadgen-bin)   loadgen_bin="$2"; shift 2 ;;
        --out-dir)       out_dir="$2"; shift 2 ;;
        --pg-dsn)        pg_dsn="$2"; shift 2 ;;
        --pprof-addr)    pprof_addr="$2"; shift 2 ;;
        --rows-per-tx)   rows_per_tx="$2"; shift 2 ;;
        --scenario)      scenario="$2"; shift 2 ;;
        --subscribers)   subscribers="$2"; shift 2 ;;
        --writer-bin)    writer_bin="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,60p' "$0"
            exit 0
            ;;
        *)
            echo "bench.sh: unknown flag $1" >&2
            exit 2
            ;;
    esac
done

# --- Required-flag check --------------------------------------------------
case "$scenario" in
    smoke|steady|spike|soak|stress) : ;;
    "")
        echo "bench.sh: --scenario is required (one of: smoke|steady|spike|soak|stress)" >&2
        exit 2
        ;;
    *)
        echo "bench.sh: --scenario must be one of: smoke|steady|spike|soak|stress (got '$scenario')" >&2
        exit 2
        ;;
esac

# --- Duration validation (WR-03) ------------------------------------------
# Accept exactly <n>s, <n>m, <n>h, or a bare integer (treated as seconds).
# Reject anything else (e.g. `2d`, `5w`, `100ms`, `m5`) loudly rather than
# silently treating it as N seconds — the previous awk parser fell through
# any unknown suffix to "seconds", which turned a fat-fingered
# `--duration 2d` (intended: 2 days of soak) into a ~2-second run that
# produced unintentionally tiny artefacts. Validate up-front before any
# background launch so a typo aborts before the script forks anything.
#
# POSIX-shell-pattern strategy: split duration into <prefix><last-char>;
# require last-char ∈ {0-9, s, m, h} and require prefix to be all digits
# (which also rules out "ms" — the trailing "s" is fine but the prefix
# "100m" would have to be all-digit, and "100m" contains a non-digit 'm',
# so "100ms" is rejected as expected).
case "$duration" in
    ''|*' '*|*$'\t'*)
        echo "bench.sh: --duration must not be empty or contain whitespace (got '$duration')" >&2
        exit 2
        ;;
esac
_dur_last="${duration#"${duration%?}"}"   # last char
_dur_prefix="${duration%?}"               # everything except last char
case "$_dur_last" in
    [0-9])
        # Bare integer: every character must be a digit.
        case "$duration" in
            *[!0-9]*)
                echo "bench.sh: --duration looks numeric but contains non-digits (got '$duration')" >&2
                exit 2
                ;;
        esac
        ;;
    s|m|h)
        # Unit suffix: prefix must be non-empty and all-digit.
        if [ -z "$_dur_prefix" ]; then
            echo "bench.sh: --duration must include a numeric prefix (got '$duration')" >&2
            exit 2
        fi
        case "$_dur_prefix" in
            *[!0-9]*)
                echo "bench.sh: --duration prefix must be all-digit; multi-letter units like 'ms' are rejected (got '$duration')" >&2
                exit 2
                ;;
        esac
        ;;
    *)
        echo "bench.sh: --duration must match <n>[s|m|h] or bare integer seconds (got '$duration'); units other than s/m/h (e.g. d, w, ms) are rejected" >&2
        exit 2
        ;;
esac
unset _dur_last _dur_prefix

# --- Resolve out_dir (portable timestamp) ---------------------------------
if [ -z "$out_dir" ]; then
    ts=$(date +%Y%m%dT%H%M%S)
    out_dir="bench-out/$ts"
fi

# --- Resolve script dir via cd + pwd (portable; no symlink-resolving flag) ----
script_dir=$(cd "$(dirname "$0")" && pwd)
repo_dir=$(cd "$script_dir/.." && pwd)

# --- Preflight: testbench compose stack must be up ------------------------
testbench_dir="$repo_dir/testbench"
if [ ! -d "$testbench_dir" ]; then
    echo "bench.sh: expected testbench dir at $testbench_dir" >&2
    exit 2
fi

if ! ( cd "$testbench_dir" && docker compose ps ) >/dev/null 2>&1; then
    echo "bench.sh: testbench compose stack is not up (cd $testbench_dir && docker compose up -d first)" >&2
    exit 2
fi

# --- Artefact dir ---------------------------------------------------------
mkdir -p "$out_dir"
echo "bench.sh: writing artefacts to $out_dir"

# --- Build writer arg list (conditional passthrough) ----------------------
# Empty-string sentinel means "do not pass; let the writer's own config
# resolution decide". 0 is NOT used as a sentinel — the writer treats 0 as
# "use scenario default" but an explicit 0 on the CLI vs. an absent flag is
# the same to it; keeping "" here just keeps the bench-side state explicit.
writer_args=(--scenario "$scenario")
if [ -n "$commit_rate" ]; then
    writer_args+=(--commit-rate "$commit_rate")
fi
if [ -n "$rows_per_tx" ]; then
    writer_args+=(--rows-per-tx "$rows_per_tx")
fi
# --pg-dsn: explicit flag wins; otherwise inherit WRITER_PG_DSN from env if
# the operator exported it (the host-side bench typically targets the
# compose-published 127.0.0.1:5432 rather than the in-network postgres:5432
# DSN baked into the compose env).
if [ -n "$pg_dsn" ]; then
    writer_args+=(--pg-dsn "$pg_dsn")
elif [ -n "${WRITER_PG_DSN:-}" ]; then
    writer_args+=(--pg-dsn "$WRITER_PG_DSN")
fi

# --- Trap: best-effort cleanup on exit ------------------------------------
# WR-02: declare PIDs and install the cleanup trap BEFORE either
# background launch. Earlier this trap was installed only AFTER both
# launches, leaving a window where a failure between writer-launch and
# trap-install (e.g. missing loadgen_bin under `set -e`, or a SIGINT
# from the operator) would orphan the writer. The cleanup function
# guards each kill on a non-empty PID so it is safe to invoke even if
# only the writer (or neither) has been launched yet.
writer_pid=""
loadgen_pid=""
cleanup() {
    set +e
    if [ -n "${writer_pid:-}" ] && [ -n "${loadgen_pid:-}" ]; then
        kill -TERM "$writer_pid" "$loadgen_pid" 2>/dev/null || true
    elif [ -n "${writer_pid:-}" ]; then
        kill -TERM "$writer_pid" 2>/dev/null || true
    elif [ -n "${loadgen_pid:-}" ]; then
        kill -TERM "$loadgen_pid" 2>/dev/null || true
    fi
    wait 2>/dev/null || true
    # Final metrics snapshot (best-effort — loadgen may already be down by
    # the time cleanup runs, since loadgen exits on its --duration timer
    # ~30s before this script's snapshot loop finishes). Use .tmp + size
    # guard to preserve the in-loop scrape if the post-EXIT scrape fails;
    # without this, an empty post-EXIT scrape would silently destroy the
    # final-iteration good data.
    curl -s --max-time 5 http://127.0.0.1:8080/metrics >"$out_dir/walera-metrics.txt.tmp" 2>/dev/null || true
    if [ -s "$out_dir/walera-metrics.txt.tmp" ]; then
        mv "$out_dir/walera-metrics.txt.tmp" "$out_dir/walera-metrics.txt"
    else
        rm -f "$out_dir/walera-metrics.txt.tmp"
    fi
    curl -s --max-time 5 http://127.0.0.1:9200/metrics >"$out_dir/loadgen-metrics.txt.tmp" 2>/dev/null || true
    if [ -s "$out_dir/loadgen-metrics.txt.tmp" ]; then
        mv "$out_dir/loadgen-metrics.txt.tmp" "$out_dir/loadgen-metrics.txt"
    else
        rm -f "$out_dir/loadgen-metrics.txt.tmp"
    fi
    # Summary: counter sums via awk (portable arithmetic; see NOT-USE list).
    if [ -s "$out_dir/loadgen-metrics.txt" ]; then
        events=$(awk '/^loadgen_events_received_total/ {s+=$2} END {printf "%.0f", s}' "$out_dir/loadgen-metrics.txt")
        errs=$(awk '/^loadgen_connection_errors_total/ {s+=$2} END {printf "%.0f", s}' "$out_dir/loadgen-metrics.txt")
        echo "bench.sh: loadgen — events=$events connection_errors=$errs"
    fi
    if [ -s "$out_dir/walera-metrics.txt" ]; then
        sent=$(awk '/^walera_events_sent_total/ {s+=$2} END {printf "%.0f", s}' "$out_dir/walera-metrics.txt")
        echo "bench.sh: walera — events_sent=$sent"
    fi
    # Dump raw histogram buckets for event_lag (raw — percentile math
    # would require non-portable arithmetic utilities; see NOT-USE list).
    if [ -s "$out_dir/loadgen-metrics.txt" ]; then
        awk '/^loadgen_event_lag_seconds_bucket/ || /^loadgen_event_lag_seconds_sum/ || /^loadgen_event_lag_seconds_count/ {print}' \
            "$out_dir/loadgen-metrics.txt" >"$out_dir/loadgen-lag-buckets.txt"
    fi
    echo "bench.sh: artefacts in $out_dir"
}
trap cleanup EXIT INT TERM

# --- Start writer (background) --------------------------------------------
echo "bench.sh: starting writer ($writer_bin ${writer_args[*]})"
"$writer_bin" "${writer_args[@]}" >"$out_dir/writer.log" 2>&1 &
writer_pid=$!

# --- Start loadgen (background) -------------------------------------------
# Loadgen reads its bearer token from LOADGEN_AUTH_TOKEN (preferred over
# --auth-token because the env var avoids leaking the token into the
# process listing).
if [ -z "${LOADGEN_AUTH_TOKEN:-}" ]; then
    echo "bench.sh: WARNING — LOADGEN_AUTH_TOKEN not set; loadgen will fail to authenticate" >&2
fi
echo "bench.sh: starting loadgen ($loadgen_bin --concurrency $subscribers --duration $duration --channels $channels)"
"$loadgen_bin" \
    --target-url http://127.0.0.1:8080 \
    --concurrency "$subscribers" \
    --duration "$duration" \
    --channels "$channels" \
    --http-addr 127.0.0.1:9200 \
    >"$out_dir/loadgen.log" 2>&1 &
loadgen_pid=$!

# --- Warmup ---------------------------------------------------------------
echo "bench.sh: warmup sleep 30s"
sleep 30

# --- Capture loop: periodic pprof snapshots -------------------------------
# Use the bash SECONDS builtin for elapsed-time math (avoids non-portable
# arithmetic helpers and GNU-only timestamp parsing; see NOT-USE list).
# Loop until --duration elapses (loadgen exits on its own --duration timer
# in parallel; this loop is the snapshot driver).
total_secs=$(awk -v d="$duration" 'BEGIN {
    # Parse "5m", "30s", "2h" — minimal subset.
    # Use POSIX awk only: substr/index/length (the 3-arg match() with
    # capture-array form is a gawk extension and is NOT portable).
    # WR-03: belt-and-braces with the up-front shell validation — if the
    # last char is not in {s, m, h, digit}, exit non-zero so we cannot
    # silently mis-parse (e.g. treating "2d" as 2 seconds). Stay POSIX:
    # no gensub/match-with-array, no regex extensions.
    n = d + 0          # leading numeric prefix
    last = substr(d, length(d), 1)
    if      (last == "m") print n * 60
    else if (last == "h") print n * 3600
    else if (last == "s") print n
    else if (last >= "0" && last <= "9") print n  # bare integer = seconds
    else {
        printf "bench.sh: awk: unrecognized duration unit %s in %s\n", last, d > "/dev/stderr"
        exit 1
    }
}') || {
    echo "bench.sh: failed to parse --duration '$duration' (see error above)" >&2
    exit 2
}

SECONDS=0
snapshot_interval=30
while [ "$SECONDS" -lt "$total_secs" ]; do
    ts=$(date +%H%M%S)
    # heap snapshot (fast).
    curl -s --max-time 10 \
        "http://$pprof_addr/debug/pprof/heap" \
        >"$out_dir/heap-$ts.pprof" 2>/dev/null || true
    # goroutine snapshot (fast, text format).
    curl -s --max-time 10 \
        "http://$pprof_addr/debug/pprof/goroutine?debug=1" \
        >"$out_dir/goroutine-$ts.txt" 2>/dev/null || true
    # Walera + loadgen /metrics scrape on EVERY iteration — loadgen runs
    # for --duration starting from t=0 while this loop runs from t=+30s
    # (post-warmup) for $total_secs. Without per-iteration scrape, the
    # cleanup-on-EXIT scrape would race with loadgen's --duration timer
    # and frequently catch a dead process (loadgen-metrics.txt = 0 bytes).
    # Overwrite is fine; the last successful scrape before loadgen exits
    # is the operative one (Prometheus counters are cumulative).
    curl -s --max-time 5 \
        "http://127.0.0.1:8080/metrics" \
        >"$out_dir/walera-metrics.txt" 2>/dev/null || true
    curl -s --max-time 5 \
        "http://127.0.0.1:9200/metrics" \
        >"$out_dir/loadgen-metrics.txt.tmp" 2>/dev/null || true
    if [ -s "$out_dir/loadgen-metrics.txt.tmp" ]; then
        mv "$out_dir/loadgen-metrics.txt.tmp" "$out_dir/loadgen-metrics.txt"
    else
        rm -f "$out_dir/loadgen-metrics.txt.tmp"
    fi
    # CPU profile — the curl call itself blocks for the seconds= window
    # so this is also the loop's pacing primitive.
    curl -s --max-time 45 \
        "http://$pprof_addr/debug/pprof/profile?seconds=$snapshot_interval" \
        >"$out_dir/profile-$ts.pprof" 2>/dev/null || true
done

# Cleanup runs via trap EXIT.
