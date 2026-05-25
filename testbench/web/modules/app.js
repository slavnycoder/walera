// testbench/web/modules/app.js — Plan 08-03
//
// ESM entry point for the Walera testbench UI.
//
// Responsibilities (UI-SPEC §6 Performance Contract + §Appendix A):
//   • Own the shared eventBus (EventTarget) and subscriptions registry (Map).
//   • Install a single document `visibilitychange` listener — pauses the rAF
//     flush when the tab is backgrounded (UI-10). The SSE connection itself
//     stays open (sse-client passes openWhenHidden:true to the polyfill);
//     pause is purely a DOM-flush concern.
//   • Own a single requestAnimationFrame loop that calls flushPending() on
//     every mounted panel exposing it. No panel calls rAF itself.
//   • Bootstrap on DOMContentLoaded: mount user-picker, connection-pill,
//     error-banner (08-02) and event-feed, entity-card, wildcard-list (08-03).
//   • Wire the user-picker `user:change` event to subscription teardown +
//     reopen using the per-user channel map (UI-SPEC §3.10).
//   • Fire the h2c probe once on boot (fire-and-forget — do not delay panel
//     mount on probe latency).
//
// Console discipline: warn/error only.

import { mount as mountPicker }   from "./user-picker.js";
import { mount as mountPill }     from "./connection-pill.js";
import { mount as mountBanner }   from "./error-banner.js";
import { mount as mountFeed }     from "./event-feed.js";
import { mount as mountCard }     from "./entity-card.js";
import { mount as mountWildcard } from "./wildcard-list.js";
import { mount as mountMetrics }  from "./metrics-panel.js";
import { mount as mountWriter }   from "./writer-control.js";
import { open as sseOpen }        from "./sse-client.js";
import { probe as probeH2c }      from "./h2c-detector.js";

// ── Constants (UI-SPEC §Appendix A) ──────────────────────────────────────
const URLS = Object.freeze({
  walera: "http://localhost:8080",
  writer: "http://localhost:9100",
});

// Per-user channel map (UI-SPEC §3.10).
// "exact" channels feed the entity-card; "wildcard" channels feed wildcard-list.
// PK for exact channels chosen per plan brief:
//   demo-alice: orders:1, devices:00000000-0000-0000-0000-000000000001, articles:intro-to-cdc
//   demo-bob:   orders:1
//   demo-eve:   articles:all (wildcard only)
// Plan-brief overrides UI-SPEC §3.10 (which lists only the headline channel
// per user) — explicitly enumerated below.
const USER_CHANNELS = Object.freeze({
  "demo-alice": [
    { kind: "exact",    channel: "orders:1",                                            panel: "entity-card",   pkColumn: "id" },
    { kind: "exact",    channel: "devices:00000000-0000-0000-0000-000000000001",        panel: null,            pkColumn: "id" },
    { kind: "exact",    channel: "articles:intro-to-cdc",                               panel: null,            pkColumn: "slug" },
    { kind: "wildcard", channel: "articles:all",                                        panel: "wildcard-list", pkColumn: "slug" },
  ],
  "demo-bob": [
    { kind: "exact", channel: "orders:1", panel: "entity-card", pkColumn: "id" },
  ],
  "demo-eve": [
    { kind: "wildcard", channel: "articles:all", panel: "wildcard-list", pkColumn: "slug" },
  ],
});

// ── Shared state owned by app.js ─────────────────────────────────────────
const eventBus     = new EventTarget();
const subscriptions = new Map(); // channel → SseSubscription handle

let paused = document.visibilityState === "hidden";
const mountedPanels = []; // { handle, name } — flushPending() targets
let rafId = 0;
let destroyed = false;

// Panel handles surfaced for the per-user wiring below.
let feedHandle      = null;
let cardHandle      = null;
let wildcardHandle  = null;
let pillHandle      = null;
let cardMountEl     = null;
let wildcardMountEl = null;
let currentToken    = null;

// ── Visibility listener — UI-10 ──────────────────────────────────────────
const onVisibilityChange = () => {
  paused = document.visibilityState === "hidden";
};
document.addEventListener("visibilitychange", onVisibilityChange);

// ── rAF loop — UI-SPEC §6 ────────────────────────────────────────────────
function tick() {
  if (destroyed) {
    rafId = 0;
    return;
  }
  if (!paused && document.visibilityState === "visible") {
    for (const { handle, name } of mountedPanels) {
      if (typeof handle.flushPending !== "function") continue;
      try {
        handle.flushPending();
      } catch (err) {
        console.error(`[app] flushPending failed for ${name}:`, err);
      }
    }
  }
  rafId = requestAnimationFrame(tick);
}

// ── Mount helper ─────────────────────────────────────────────────────────
function mountInto(selector, name, mountFn) {
  const rootEl = document.querySelector(selector);
  if (!rootEl) {
    console.warn(`[app] mount target not found for ${name}: ${selector}`);
    return { handle: null, rootEl: null };
  }
  const deps = { eventBus, subscriptions, userToken: null, urls: URLS };
  try {
    const handle = mountFn(rootEl, deps);
    if (handle && typeof handle === "object") {
      mountedPanels.push({ handle, name });
      return { handle, rootEl };
    }
    return { handle: null, rootEl };
  } catch (err) {
    console.error(`[app] mount failed for ${name}:`, err);
    return { handle: null, rootEl };
  }
}

// ── Connection-pill aggregation ──────────────────────────────────────────
// The global pill in the header reflects the aggregate state across all
// active subscriptions. Rules (UI-SPEC §3.2 / §9 UI-09):
//   • any subscription "reconnecting"               → reconnecting
//   • any subscription "connecting" and no "open"   → connecting
//   • any subscription "open" (and none of the above worse) → open
//   • subscriptions Map empty                       → closed
//
// Per-panel pills (UI-SPEC §3.3/3.4/3.5, WR-VR-01):
//   • event-feed pill reflects the global aggregate — the feed renders every
//     channel.
//   • entity-card pill reflects the state of its bound exact channel only.
//   • wildcard-list pill reflects the state of its bound wildcard channel.
//   • Unbound panels (no current binding) render "closed".
function aggregateState() {
  if (subscriptions.size === 0) return "closed";
  let hasOpen = false;
  let hasReconnecting = false;
  let hasConnecting = false;
  for (const sub of subscriptions.values()) {
    const s = sub && sub.state;
    if (s === "reconnecting") hasReconnecting = true;
    else if (s === "connecting") hasConnecting = true;
    else if (s === "open") hasOpen = true;
  }
  if (hasReconnecting) return "reconnecting";
  if (hasConnecting && !hasOpen) return "connecting";
  if (hasOpen) return "open";
  return "closed";
}

function channelState(channel) {
  if (!channel) return "closed";
  const sub = subscriptions.get(channel);
  return (sub && sub.state) || "closed";
}

// Per-panel pill state dispatch — Plan 12-01 (UI-12). The document-level
// CustomEvent bus replaces the previous direct *Handle.setPillState() calls;
// each panel module owns its own filtered listener (detail.panelId === MY_ID).
// metrics-panel is NOT dispatched here — it produces its own state from
// poll() and a duplicate emit would race that producer.
function dispatchPanelState(panelId, state, reason) {
  document.dispatchEvent(new CustomEvent("subscription:state:change", {
    detail: { panelId, state, reason },
  }));
}

// Tracks which channel each per-panel pill mirrors. Updated on each
// openSubscriptionsFor() call; null means the panel is unbound for the
// current user and renders "closed".
let cardPillChannel     = null;
let wildcardPillChannel = null;

function recomputePill() {
  const agg = aggregateState();
  // Global header pill stays inline — CONTEXT D-02 explicitly keeps this off
  // the bus (the bus is for PER-PANEL pills only; the global pill IS the
  // aggregate, not a panel).
  if (pillHandle) pillHandle.update(agg);

  // Per-panel pills dispatched over the document CustomEvent bus. SC #4 of
  // v1.2 phase 12: the affected panel-pill must flip before the global pill
  // — that ordering falls out naturally because each per-panel pill's state
  // is computed from a single channel (or the aggregate for event-feed),
  // while the global pill only flips once subscriptions.size reaches 0.
  //
  // event-feed mirrors the global aggregate — it renders every channel.
  dispatchPanelState("event-feed", agg);
  dispatchPanelState("entity-card", channelState(cardPillChannel));
  dispatchPanelState("wildcard-list", channelState(wildcardPillChannel));
  // metrics-panel is dispatched from metrics-panel.js#poll — NOT here.
}

// ── Per-user subscription lifecycle ─────────────────────────────────────
function closeAllSubscriptions(reason) {
  for (const sub of subscriptions.values()) {
    try { sub.close(); } catch (err) { console.warn("[app] sub.close() threw:", err); }
  }
  subscriptions.clear();
  // Per-panel pills lose their binding when subscriptions clear; recomputePill
  // will read these as "closed".
  cardPillChannel = null;
  wildcardPillChannel = null;
  recomputePill();
  if (reason) {
    // No-op for now; downstream may consume.
  }
}

function channelToUrlPath(channel) {
  // "orders:1" → "/sse/v1/orders/1"
  // "articles:all" → "/sse/v1/articles/all"
  const sep = channel.indexOf(":");
  if (sep < 0) return `/sse/v1/${channel}`;
  const table = channel.slice(0, sep);
  const pk = channel.slice(sep + 1);
  return `/sse/v1/${encodeURIComponent(table)}/${encodeURIComponent(pk)}`;
}

function openSubscriptionsFor(token) {
  const entries = USER_CHANNELS[token] || [];
  // Show/hide the side-panels per user (UI-SPEC §3.10):
  //   demo-alice: entity-card visible, wildcard-list visible
  //   demo-bob:   entity-card visible, wildcard-list hidden
  //   demo-eve:   entity-card hidden,  wildcard-list visible
  const hasEntityCard   = entries.some((e) => e.panel === "entity-card");
  const hasWildcardList = entries.some((e) => e.panel === "wildcard-list");
  if (cardMountEl)     cardMountEl.hidden     = !hasEntityCard;
  if (wildcardMountEl) wildcardMountEl.hidden = !hasWildcardList;

  // Bind the entity-card / wildcard-list to their channel BEFORE opening
  // streams, so the very first received tx already has a binding to apply to.
  const exact = entries.find((e) => e.panel === "entity-card");
  const wild  = entries.find((e) => e.panel === "wildcard-list");
  cardPillChannel     = exact ? exact.channel : null;
  wildcardPillChannel = wild  ? wild.channel  : null;
  if (cardHandle) {
    cardHandle.bindChannel(exact ? exact.channel : null);
  }
  if (wildcardHandle) {
    if (wild) wildcardHandle.bindChannel(wild.channel, wild.pkColumn || "pk");
    else      wildcardHandle.bindChannel(null, "pk");
  }

  for (const entry of entries) {
    const url = URLS.walera + channelToUrlPath(entry.channel);
    const handle = sseOpen({
      url,
      token,
      channel: entry.channel,
      handlers: {
        onOpen: ({ reconnected }) => {
          recomputePill();
          if (reconnected) {
            eventBus.dispatchEvent(
              new CustomEvent("sse:reconnect", {
                detail: { channel: entry.channel },
              }),
            );
          }
        },
        onTx: (payload) => {
          // Fan out the parsed payload to the appropriate panel(s) AND the
          // event-feed (which renders every tx regardless of panel binding).
          if (entry.panel === "entity-card" && cardHandle) {
            cardHandle.onTx(payload);
          } else if (entry.panel === "wildcard-list" && wildcardHandle) {
            wildcardHandle.onTx(payload);
          }
          if (feedHandle) {
            feedHandle.onTx({ channel: entry.channel, payload });
          }
          // Surface raw event on the bus for any downstream listener.
          eventBus.dispatchEvent(
            new CustomEvent("tx", { detail: { channel: entry.channel, payload } }),
          );
        },
        onError: (err) => {
          eventBus.dispatchEvent(
            new CustomEvent("sse:error", {
              detail: { channel: entry.channel, kind: err.kind, raw: err.raw },
            }),
          );
          // Reconcile wildcard-list on tx_too_large / slow_consumer.
          if (wildcardHandle && (err.kind === "tx_too_large" || err.kind === "slow_consumer")) {
            wildcardHandle.clearOnError(err.kind);
          }
        },
        onShutdown: () => {
          eventBus.dispatchEvent(
            new CustomEvent("sse:error", {
              detail: { channel: entry.channel, kind: "shutdown" },
            }),
          );
        },
        onClose: (info) => {
          subscriptions.delete(entry.channel);
          recomputePill();
          if (info && info.reason === "error") {
            // sse-client.js translates HTTP status → banner kind at the
            // source (401/403 → auth_revoked; 5xx/network → auth_unavailable),
            // so app.js stays free of CORS/auth-policy knowledge. Fall back
            // to auth_unavailable if no kind was passed (defensive).
            const kind = (info.kind) || "auth_unavailable";
            eventBus.dispatchEvent(
              new CustomEvent("sse:error", {
                detail: { channel: entry.channel, kind, status: info.status },
              }),
            );
          }
        },
      },
    });
    subscriptions.set(entry.channel, handle);
  }
  recomputePill();
}

function onUserChange(evt) {
  const token = evt && evt.detail && evt.detail.token;
  if (!token || token === currentToken) return;
  currentToken = token;
  closeAllSubscriptions("user-switch");
  // Clear all panels so stale data from previous user does not bleed across.
  if (cardHandle)     cardHandle.bindChannel(null);
  if (wildcardHandle) wildcardHandle.clearOnError("user-switch");
  // Event-feed listens for "clear" on the bus.
  eventBus.dispatchEvent(new CustomEvent("clear"));
  openSubscriptionsFor(token);
}

// ── Bootstrap ────────────────────────────────────────────────────────────
function bootstrap() {
  mountInto('[data-mount="user-picker"]', "user-picker", mountPicker);

  const pillResult = mountInto('[data-mount="connection-pill"]', "connection-pill", mountPill);
  pillHandle = pillResult.handle;

  mountInto('[data-mount="error-banner"]', "error-banner", mountBanner);

  const feedResult     = mountInto('[data-mount="event-feed"]',     "event-feed",     mountFeed);
  feedHandle = feedResult.handle;

  const cardResult     = mountInto('[data-mount="entity-card"]',    "entity-card",    mountCard);
  cardHandle  = cardResult.handle;
  cardMountEl = cardResult.rootEl;

  const wildcardResult = mountInto('[data-mount="wildcard-list"]',  "wildcard-list",  mountWildcard);
  wildcardHandle  = wildcardResult.handle;
  wildcardMountEl = wildcardResult.rootEl;

  // Plan 08-04: metrics-panel + writer-control. Both panels are operator-
  // facing (not per-user-token); they live below the side-panel column on
  // the grid and run for the page lifetime.
  mountInto('[data-mount="metrics-panel"]',  "metrics-panel",  mountMetrics);
  mountInto('[data-mount="writer-control"]', "writer-control", mountWriter);

  // Wire user-picker.
  eventBus.addEventListener("user:change", onUserChange);

  // Initial pill state is "closed" (no subscriptions yet).
  recomputePill();

  // Fire-and-forget h2c probe (UI-11). Probe latency must NOT delay first
  // paint or panel mount; we explicitly do NOT await this.
  probeH2c(URLS.walera, eventBus).catch((err) => {
    console.warn("[app] h2c probe rejected:", err);
  });

  rafId = requestAnimationFrame(tick);
}

// WR-CR-07: explicit teardown for tests and clean page unload. Cancels the
// rAF loop, iterates every mounted panel calling its destroy() if exposed,
// closes every in-flight subscription, and removes the visibility +
// user-change listeners. Idempotent.
function destroy() {
  if (destroyed) return;
  destroyed = true;
  if (rafId !== 0) {
    cancelAnimationFrame(rafId);
    rafId = 0;
  }
  document.removeEventListener("visibilitychange", onVisibilityChange);
  eventBus.removeEventListener("user:change", onUserChange);
  // Close all subscriptions before destroying panels — panels may receive a
  // final onClose callback during the close() sweep.
  closeAllSubscriptions("destroy");
  for (const { handle, name } of mountedPanels) {
    if (handle && typeof handle.destroy === "function") {
      try { handle.destroy(); } catch (err) {
        console.warn(`[app] destroy() threw for ${name}:`, err);
      }
    }
  }
  mountedPanels.length = 0;
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", bootstrap, { once: true });
} else {
  bootstrap();
}

// Exported for tests / future plans that need to peek at the bus/registry.
export { eventBus, subscriptions, URLS, destroy };
