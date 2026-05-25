// testbench/web/modules/metrics-panel.js — Plan 08-04
//
// Polls walera /metrics every 2 s, parses the Prometheus text format inline
// (no external dep), and displays four headline values per UI-SPEC §3.6:
//
//   subscribers_active           — sum(walera_routing_index_size{index_kind})
//                                  (no single subscribers_active gauge exists
//                                  in the actual /metrics surface; the routing
//                                  index size is the authoritative count of
//                                  registered subscribers — Plan 08-04 fix vs
//                                  PLAN.md interfaces block which named a
//                                  hypothetical subscribers_active metric)
//   wal_lsn_lag_bytes            — walera_wal_lsn_lag_bytes (gauge)
//   wal_tx_total                 — walera_wal_tx_size_changes_count (counter)
//                                  sliding 10 s rate (/s); the prefix-less
//                                  wal_tx_total mentioned in the plan does
//                                  not exist on the actual surface
//   auth_circuit_breaker_state   — walera_auth_circuit_breaker_state
//                                  (gauge: 0/1/2) → closed | open | half-open
//
// Wire details:
//   • Each poll uses AbortController so a stalled fetch is cancelled when the
//     next interval fires (avoids stacking in-flight requests on a slow walera).
//   • walera's /metrics is unauthenticated (served by promhttp.Handler from
//     internal/health/server.go). NO Authorization header is sent — the demo
//     pattern is "Bearer header for /sse only".
//   • Visibility-pause: the rAF flush is paused by app.js on hidden; here we
//     additionally skip the polling iteration when document.visibilityState ===
//     "hidden" so a backgrounded tab doesn't accumulate /metrics traffic.
//   • Stale / errored states surface inline on the four <dd> values (UI-SPEC
//     §3.6: "stale if last poll older than 6 s; errored if last response was
//     non-2xx").
//   • Counter-rate computation: 5-sample ring buffer (one per 2 s = 10 s
//     window). Rate = (latest.count - oldest.count) / (latest.ts - oldest.ts)
//     in seconds, displayed to one decimal place.
//   • Metric-name fallback: if `wal_tx_total` is absent, prefer
//     `walera_wal_tx_size_changes_count` per the smoke-07 note in the plan.
//
// Console discipline: warn/error only.

import { mount as mountPill } from "./connection-pill.js";

const MY_PANEL_ID = "metrics-panel";

const POLL_INTERVAL_MS = 2000;
const RATE_WINDOW_MS = 10_000;
const RATE_RING_CAP = 6;             // 6 samples × 2 s = 12 s of headroom
const STALE_THRESHOLD_MS = 6_000;    // UI-SPEC §3.6 — "stale if last poll > 6 s ago"

// Prometheus text-format regex per UI-SPEC §3.6 / RESEARCH §5.
// Single line: `metric_name{label="value",…} 123.45[ 1640000000000]`.
// The optional trailing column is the OpenMetrics sample timestamp; we
// accept and ignore it. The value group accepts the spec-defined sentinels
// `+Inf / -Inf / NaN` that Prometheus emits for histogram extremes; the
// previous loose char-class `[0-9eE+\-.]+` accepted garbage like `++.eE-`.
const PROM_LINE_RE = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{[^}]*\})?\s+([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?|[+-]?Inf|NaN)(?:\s+\d+)?$/;

const BREAKER_LABELS = {
  0: "closed",
  1: "open",
  2: "half-open",
};

/**
 * Parses Prometheus text format into a flat { metricName: aggregatedValue } map.
 * Aggregation is "sum of all label combinations" for the gauges / counters we
 * care about (subscribers_active is per-channel-shard; the headline is the
 * total across shards). Lines starting with `#` (HELP / TYPE comments) and
 * blank lines are skipped.
 */
function parsePrometheus(text) {
  const out = Object.create(null);
  const lines = text.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line || line.charCodeAt(0) === 35 /* '#' */) continue;
    const m = PROM_LINE_RE.exec(line);
    if (!m) continue;
    const name = m[1];
    const value = Number(m[2]);
    if (!Number.isFinite(value)) continue;
    out[name] = (out[name] || 0) + value;
  }
  return out;
}

/**
 * Formats a large integer with thin spaces every 3 digits (e.g. 1 234 567).
 * Used for wal_lsn_lag_bytes per UI-SPEC §3.6.
 */
function formatThousands(n) {
  if (!Number.isFinite(n)) return "—";
  const sign = n < 0 ? "-" : "";
  const abs = Math.abs(Math.trunc(n));
  const s = String(abs);
  let out = "";
  for (let i = 0; i < s.length; i++) {
    if (i > 0 && (s.length - i) % 3 === 0) out += "\u2009"; // thin space
    out += s[i];
  }
  return sign + out;
}

export function mount(rootEl, deps) {
  const urls = (deps && deps.urls) || { walera: "http://localhost:8080" };

  // Build DOM from UI-SPEC §3.6.
  const section = document.createElement("section");
  section.className = "panel metrics";

  const header = document.createElement("header");
  header.className = "panel__hdr";
  const h2 = document.createElement("h2");
  h2.textContent = "Metrics";
  header.appendChild(h2);
  // Per-panel pill placeholder — Plan 12-01 (UI-12): metrics-panel previously
  // had no pill slot. mountPill() appends its own <span class="pill pill--{state}">
  // into this slot; state is dispatched from poll() below over the document
  // CustomEvent bus and consumed by our own filtered listener.
  const pillSlot = document.createElement("span");
  pillSlot.dataset.slot = "pill";
  header.appendChild(pillSlot);
  section.appendChild(header);

  const dl = document.createElement("dl");
  dl.className = "metrics__grid";

  const slots = {};
  const rows = [
    ["subscribers_active",       "subscribers_active"],
    ["wal_lsn_lag_bytes",        "wal_lsn_lag_bytes"],
    ["wal_tx_total (/s)",        "wal_tx_total_rate"],
    ["auth_breaker",             "auth_breaker"],
  ];
  for (const [label, slot] of rows) {
    const dt = document.createElement("dt");
    dt.textContent = label;
    const dd = document.createElement("dd");
    dd.setAttribute("data-metric", slot);
    dd.textContent = "\u2014"; // em-dash placeholder
    dl.appendChild(dt);
    dl.appendChild(dd);
    slots[slot] = dd;
  }
  section.appendChild(dl);
  rootEl.appendChild(section);

  // ── Per-panel ConnectionPill (Plan 12-01, UI-12) ────────────────
  // metrics-panel is BOTH producer and consumer over the bus: it dispatches
  // pill state from the poll loop and its own filtered listener routes that
  // back to pillHandle.update. app.js does NOT dispatch metrics-panel state
  // (would race the producer).
  const pillHandle = mountPill(pillSlot);

  const onSubStateChange = (ev) => {
    const d = ev && ev.detail;
    if (!d || d.panelId !== MY_PANEL_ID) return;
    pillHandle.update(d.state, d.reason);
  };
  document.addEventListener("subscription:state:change", onSubStateChange);

  function dispatchPillState(state, reason) {
    document.dispatchEvent(new CustomEvent("subscription:state:change", {
      detail: { panelId: MY_PANEL_ID, state, reason },
    }));
  }

  // Sliding-window state for wal_tx_total rate.
  const txRing = [];            // [{ ts: epoch_ms, count: scalar }, …]
  let lastSuccessTs = 0;
  let lastStatus = null;        // "ok" | "errored" | null
  let abortCtl = null;
  let intervalId = null;
  let destroyed = false;
  let firstPoll = true;

  function renderErrored() {
    for (const slot of Object.values(slots)) {
      slot.textContent = "\u2014";
      slot.classList.add("metrics__value--errored");
      slot.classList.remove("metrics__value--stale");
    }
    section.classList.add("metrics--errored");
  }

  function renderStale() {
    for (const slot of Object.values(slots)) {
      slot.classList.add("metrics__value--stale");
    }
  }

  function clearStateClasses() {
    section.classList.remove("metrics--errored");
    for (const slot of Object.values(slots)) {
      slot.classList.remove("metrics__value--errored");
      slot.classList.remove("metrics__value--stale");
    }
  }

  function renderValues(parsed, now) {
    // subscribers_active — synthesised from walera_routing_index_size (the
    // actual metric surface has no scalar subscribers_active gauge; the
    // routing index size summed across {exact, wildcard} labels IS the
    // count of registered subscribers). Plan 08-04 fix vs PLAN.md
    // interfaces block.
    let subs = parsed["walera_routing_index_size"];
    if (!Number.isFinite(subs)) subs = parsed["subscribers_active"]; // fallback
    slots["subscribers_active"].textContent =
      Number.isFinite(subs) ? String(Math.trunc(subs)) : "\u2014";

    // wal_lsn_lag_bytes — prefer walera-prefixed name (actual surface).
    let lag = parsed["walera_wal_lsn_lag_bytes"];
    if (!Number.isFinite(lag)) lag = parsed["wal_lsn_lag_bytes"]; // fallback
    slots["wal_lsn_lag_bytes"].textContent = formatThousands(lag);

    // wal_tx_total — counter, sliding-window rate.
    // Actual surface exposes walera_wal_tx_size_changes_count; the plan's
    // wal_tx_total / walera_wal_tx_total names are aspirational.
    let txCount = parsed["walera_wal_tx_size_changes_count"];
    if (!Number.isFinite(txCount)) txCount = parsed["wal_tx_total"];
    if (!Number.isFinite(txCount)) txCount = parsed["walera_wal_tx_total"];
    if (Number.isFinite(txCount)) {
      txRing.push({ ts: now, count: txCount });
      // Drop samples older than the window.
      while (txRing.length > 0 && now - txRing[0].ts > RATE_WINDOW_MS + POLL_INTERVAL_MS) {
        txRing.shift();
      }
      // Cap the ring (defensive — shouldn't exceed RATE_RING_CAP given the
      // window-drop above, but keeps the array bounded in any edge case).
      while (txRing.length > RATE_RING_CAP) txRing.shift();

      if (txRing.length >= 2) {
        const oldest = txRing[0];
        const latest = txRing[txRing.length - 1];
        const dtSec = (latest.ts - oldest.ts) / 1000;
        if (dtSec > 0) {
          const rate = (latest.count - oldest.count) / dtSec;
          slots["wal_tx_total_rate"].textContent = rate.toFixed(1);
        } else {
          slots["wal_tx_total_rate"].textContent = "0.0";
        }
      } else {
        // Single sample — cannot compute rate yet.
        slots["wal_tx_total_rate"].textContent = "\u2014";
      }
    } else {
      slots["wal_tx_total_rate"].textContent = "\u2014";
    }

    // auth_circuit_breaker_state — gauge: 0/1/2 → label.
    // Actual surface uses walera_auth_circuit_breaker_state; keep the
    // unprefixed name as a fallback in case the metric is ever renamed.
    let breaker = parsed["walera_auth_circuit_breaker_state"];
    if (!Number.isFinite(breaker)) breaker = parsed["auth_circuit_breaker_state"];
    if (Number.isFinite(breaker) && BREAKER_LABELS[breaker] !== undefined) {
      slots["auth_breaker"].textContent = BREAKER_LABELS[breaker];
    } else {
      slots["auth_breaker"].textContent = "\u2014";
    }
  }

  async function poll() {
    if (destroyed) return;
    if (document.visibilityState === "hidden") return;

    // First poll → emit "connecting" so the pill leaves its default "closed"
    // state before the fetch resolves. Subsequent polls dispatch from the
    // success/error branches below.
    if (firstPoll) {
      dispatchPillState("connecting");
    }

    // Abort any in-flight request before starting a new one.
    if (abortCtl) {
      try { abortCtl.abort(); } catch (_e) {}
    }
    abortCtl = new AbortController();
    const signal = abortCtl.signal;
    const url = urls.walera + "/metrics";

    try {
      const response = await fetch(url, { method: "GET", mode: "cors", signal });
      if (!response.ok) {
        lastStatus = "errored";
        renderErrored();
        // "reconnecting" rather than "closed" — the poll loop will retry
        // every 2 s; "closed" is reserved for destroy().
        dispatchPillState("reconnecting", "metrics poll failed");
        firstPoll = false;
        return;
      }
      const text = await response.text();
      if (destroyed) return;
      const parsed = parsePrometheus(text);
      const now = Date.now();
      clearStateClasses();
      renderValues(parsed, now);
      lastSuccessTs = now;
      lastStatus = "ok";
      dispatchPillState("open");
      firstPoll = false;
    } catch (err) {
      // AbortError → next interval fired; suppress.
      if (err && err.name === "AbortError") return;
      lastStatus = "errored";
      renderErrored();
      dispatchPillState("reconnecting", "metrics poll failed");
      firstPoll = false;
      console.warn("[metrics-panel] poll failed:", err && err.message ? err.message : err);
    }

    // Stale-detection (covers the case where renderValues succeeded but the
    // subsequent poll was hidden-skipped for too long — the next visible poll
    // will clear the stale class).
    if (lastStatus === "ok" && Date.now() - lastSuccessTs > STALE_THRESHOLD_MS) {
      renderStale();
    }
  }

  // Initial poll on mount (don't wait for the first interval tick) +
  // setInterval(2000) per UI-SPEC §3.6. Fire-and-forget; rejection is
  // already handled inside poll().
  poll();
  intervalId = setInterval(poll, POLL_INTERVAL_MS);

  return {
    destroy() {
      // Best-effort: dispatch "closed" BEFORE removing our own listener so
      // any other consumer (today there is none for metrics-panel, but the
      // bus contract is uniform) still sees the final transition.
      dispatchPillState("closed", "panel destroyed");
      destroyed = true;
      if (intervalId !== null) clearInterval(intervalId);
      if (abortCtl) {
        try { abortCtl.abort(); } catch (_e) {}
      }
      document.removeEventListener("subscription:state:change", onSubStateChange);
      pillHandle.destroy();
      if (rootEl.contains(section)) rootEl.removeChild(section);
    },
    flushPending() {
      // No-op: metrics-panel is poll-driven, not rAF-driven. The fn is
      // exposed so app.js's flushPending list contract is uniform.
    },
  };
}

export default { mount };
