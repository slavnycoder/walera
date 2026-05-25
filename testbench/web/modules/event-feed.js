// testbench/web/modules/event-feed.js — Plan 08-03
//
// Live append-only event feed with a bounded ring buffer (200 entries),
// rAF-batched DOM updates, and pause/resume/clear controls.
//
// Performance contract (UI-SPEC §3.3 + §6):
//   • Inbound SSE events MUST be pushed into an in-memory queue from onTx.
//     NO DOM operation may run inline in the SSE message handler.
//   • flushPending() (called once per rAF tick by app.js — app.js owns the
//     single requestAnimationFrame loop; this module never calls rAF itself,
//     per the 08-02 pattern) drains the queue into the ring buffer, trims to
//     200, and diff-renders only the newly added entries. The "+N events
//     not shown" line updates only when the dropped count actually changes.
//   • When paused (toggle button) or when app.js stops calling flushPending
//     (tab hidden), the queue still accumulates with a hard ceiling of 200
//     (older queue entries dropped FIFO so a backgrounded tab cannot grow
//     unbounded).
//
// Per UI-SPEC §3.3 each entry renders as:
//   <li class="feed__entry feed__entry--{op}">
//     <time>{HH:MM:SS}</time>
//     <span class="op-chip op-chip--{op}">{glyph} {OP}</span>
//     <span class="table">{table}:{pk}</span>
//     <details><summary>Show JSON</summary><pre>{raw}</pre></details>
//   </li>
//
// Console discipline: warn/error only.

import { mount as mountPill } from "./connection-pill.js";

const MY_PANEL_ID = "event-feed";

const RING_CAP = 200;
const QUEUE_HARD_CAP = 200; // protects backgrounded tab from unbounded growth

const OP_GLYPHS = {
  insert: "\u25B2", // ▲
  update: "\u25CF", // ●
  delete: "\u25BC", // ▼
};

function fmtTime(ts) {
  const d = new Date(ts);
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${hh}:${mm}:${ss}`;
}

export function mount(rootEl, _deps) {
  // ── DOM construction ────────────────────────────────────────────
  const panel = document.createElement("section");
  panel.className = "panel feed";

  const hdr = document.createElement("header");
  hdr.className = "panel__hdr";

  const title = document.createElement("h2");
  title.textContent = "Live Event Feed";

  const toggleBtn = document.createElement("button");
  toggleBtn.type = "button";
  toggleBtn.dataset.action = "toggle";
  toggleBtn.textContent = "\u23F8"; // ⏸
  toggleBtn.setAttribute("aria-label", "Pause feed");

  const clearBtn = document.createElement("button");
  clearBtn.type = "button";
  clearBtn.dataset.action = "clear";
  clearBtn.textContent = "\u23F9 Clear"; // ⏹

  // Per-panel pill placeholder — UI-SPEC §3.3 declares `<span data-slot="pill">`
  // in each panel header. Plan 12-01 (UI-12): the actual pill DOM and state
  // rendering is owned by connection-pill.js; this placeholder is the slot
  // mountPill() appends its own <span class="pill pill--{state}"> into. State
  // is fed via the document-level `subscription:state:change` CustomEvent bus
  // (filtered by panelId === "event-feed").
  const pillSlot = document.createElement("span");
  pillSlot.dataset.slot = "pill";

  hdr.appendChild(title);
  hdr.appendChild(pillSlot);
  hdr.appendChild(toggleBtn);
  hdr.appendChild(clearBtn);

  const list = document.createElement("ol");
  list.className = "feed__list";
  list.dataset.slot = "entries";
  list.setAttribute("aria-live", "off");

  const droppedLine = document.createElement("p");
  droppedLine.className = "feed__dropped";
  droppedLine.dataset.slot = "dropped";
  droppedLine.hidden = true;
  const droppedCountSpan = document.createElement("span");
  droppedCountSpan.textContent = "0";
  droppedLine.appendChild(document.createTextNode("+ "));
  droppedLine.appendChild(droppedCountSpan);
  droppedLine.appendChild(document.createTextNode(" events not shown"));

  panel.appendChild(hdr);
  panel.appendChild(list);
  panel.appendChild(droppedLine);
  rootEl.appendChild(panel);

  // ── Per-panel ConnectionPill (Plan 12-01, UI-12) ────────────────
  // mountPill APPENDS its own <span class="pill pill--closed"> into the slot;
  // the slot's `data-slot="pill"` attribute is preserved for selector queries.
  const pillHandle = mountPill(pillSlot);

  // Bus subscriber — only react to events targeting this panelId.
  const onSubStateChange = (ev) => {
    const d = ev && ev.detail;
    if (!d || d.panelId !== MY_PANEL_ID) return;
    pillHandle.update(d.state, d.reason);
  };
  document.addEventListener("subscription:state:change", onSubStateChange);

  // ── In-memory state ─────────────────────────────────────────────
  // queue: events received since last flush (drained per rAF tick)
  // ringBuffer: the most recent RING_CAP entries currently in the DOM model
  // droppedCount: total events dropped (queue overflow + ring trim)
  let queue = [];
  const ringBuffer = []; // newest at end
  let droppedCount = 0;
  let lastRenderedDroppedCount = 0;
  let userPaused = false;

  // ── Public callback used by app.js inside the SSE onTx handler ──
  // CONTRACT: only push to in-memory queue. NO DOM ops allowed here.
  function onTx({ channel, payload } = {}) {
    if (!payload) return;
    // Walera tx payload: { tx_lsn, root, changes: [{op, table, pk, before?, after?, changed?}] }
    const changes = Array.isArray(payload.changes) ? payload.changes : [];
    for (const change of changes) {
      const entry = {
        ts: Date.now(),
        op: change.op,
        table: change.table,
        pk: change.pk,
        channel,
        raw: payload, // we serialise lazily at flush time
      };
      queue.push(entry);
      // Hard cap on queue size — protects backgrounded tab from unbounded growth.
      // FIFO drop oldest queued events; each drop counts toward droppedCount.
      if (queue.length > QUEUE_HARD_CAP) {
        const overflow = queue.length - QUEUE_HARD_CAP;
        queue.splice(0, overflow);
        droppedCount += overflow;
      }
    }
  }

  // ── DOM render for one entry ────────────────────────────────────
  function renderEntry(entry) {
    const op = entry.op || "update";
    const li = document.createElement("li");
    li.className = `feed__entry feed__entry--${op}`;

    const time = document.createElement("time");
    time.textContent = fmtTime(entry.ts);

    const chip = document.createElement("span");
    chip.className = `op-chip op-chip--${op}`;
    chip.textContent = `${OP_GLYPHS[op] || "?"} ${op.toUpperCase()}`;

    const tbl = document.createElement("span");
    tbl.className = "table";
    tbl.textContent = `${entry.table || "?"}:${entry.pk == null ? "?" : entry.pk}`;

    const details = document.createElement("details");
    const summary = document.createElement("summary");
    summary.textContent = "Show JSON";
    const pre = document.createElement("pre");
    try {
      pre.textContent = JSON.stringify(entry.raw, null, 2);
    } catch (_err) {
      pre.textContent = "<unserializable payload>";
    }
    details.appendChild(summary);
    details.appendChild(pre);

    li.appendChild(time);
    li.appendChild(document.createTextNode(" "));
    li.appendChild(chip);
    li.appendChild(document.createTextNode(" "));
    li.appendChild(tbl);
    li.appendChild(document.createTextNode(" "));
    li.appendChild(details);
    return li;
  }

  // ── flushPending — called by app.js rAF loop ────────────────────
  function flushPending() {
    // WR-01: even while paused, repaint the dropped-count line so the operator
    // sees overflow accumulating in real time. The queue itself is NOT drained
    // while paused — entries continue to accrue with the hard cap protecting
    // the backgrounded tab.
    if (droppedCount !== lastRenderedDroppedCount) {
      lastRenderedDroppedCount = droppedCount;
      droppedCountSpan.textContent = String(droppedCount);
      droppedLine.hidden = droppedCount === 0;
    }
    if (userPaused) return;
    if (queue.length === 0) {
      return;
    }
    // Drain queue into ring buffer.
    if (queue.length > 0) {
      const draining = queue;
      queue = [];
      // Append the new entries to the ring buffer.
      for (const entry of draining) {
        ringBuffer.push(entry);
      }
      // Trim to ring cap. Oldest entries are at the front (FIFO).
      if (ringBuffer.length > RING_CAP) {
        const overflow = ringBuffer.length - RING_CAP;
        ringBuffer.splice(0, overflow);
        droppedCount += overflow;
        // Remove the matching oldest DOM nodes from the front of <ol>.
        for (let i = 0; i < overflow && list.firstChild; i++) {
          list.removeChild(list.firstChild);
        }
      }
      // Append the new entries to the DOM. Note: after the trim above the
      // ring buffer contains at most RING_CAP entries; the newly drained
      // ones are the last (`draining.length` capped to RING_CAP) elements.
      // Compute how many of `draining` survived the trim.
      const survivors = Math.min(draining.length, RING_CAP);
      const startIdx = ringBuffer.length - survivors;
      const frag = document.createDocumentFragment();
      for (let i = startIdx; i < ringBuffer.length; i++) {
        frag.appendChild(renderEntry(ringBuffer[i]));
      }
      // Auto-follow: stick to the newest entry only when the operator is
      // already pinned to the bottom (allow a 16 px slack for sub-pixel
      // layout). Operators who scrolled up to read history are NOT yanked.
      const wasAtBottom = list.scrollHeight - list.clientHeight - list.scrollTop <= 16;
      list.appendChild(frag);
      if (wasAtBottom) {
        list.scrollTop = list.scrollHeight;
      }
    }
    // Trim may have bumped droppedCount above; mirror the same repaint logic
    // used at the top of flushPending so a drain that triggered a ring overflow
    // refreshes the line within the same tick.
    if (droppedCount !== lastRenderedDroppedCount) {
      lastRenderedDroppedCount = droppedCount;
      droppedCountSpan.textContent = String(droppedCount);
      droppedLine.hidden = droppedCount === 0;
    }
  }

  // ── Controls ────────────────────────────────────────────────────
  function setPaused(next) {
    userPaused = next;
    toggleBtn.textContent = userPaused ? "\u25B6" : "\u23F8"; // ▶ / ⏸
    toggleBtn.setAttribute(
      "aria-label",
      userPaused ? "Resume feed" : "Pause feed",
    );
  }

  function clear() {
    queue = [];
    ringBuffer.length = 0;
    while (list.firstChild) list.removeChild(list.firstChild);
    droppedCount = 0;
    lastRenderedDroppedCount = 0;
    droppedCountSpan.textContent = "0";
    droppedLine.hidden = true;
  }

  const onToggle = () => setPaused(!userPaused);
  toggleBtn.addEventListener("click", onToggle);
  clearBtn.addEventListener("click", clear);

  // Also listen for an app.js-level "clear" event so user-switches reset the feed.
  const onBusClear = () => clear();
  if (_deps && _deps.eventBus) {
    _deps.eventBus.addEventListener("clear", onBusClear);
  }

  // Per-panel pill state — accepted values match connection-pill module
  // (connecting / open / reconnecting / closed). Plan 12-01: thin delegate
  // to pillHandle.update; the PILL_STATES guard still rejects unknown values
  // silently so legacy direct callers can't corrupt the pill DOM.
  const PILL_STATES = new Set(["connecting", "open", "reconnecting", "closed"]);
  function setPillState(state) {
    if (!PILL_STATES.has(state)) return;
    pillHandle.update(state);
  }

  return {
    destroy() {
      toggleBtn.removeEventListener("click", onToggle);
      clearBtn.removeEventListener("click", clear);
      if (_deps && _deps.eventBus) {
        _deps.eventBus.removeEventListener("clear", onBusClear);
      }
      document.removeEventListener("subscription:state:change", onSubStateChange);
      pillHandle.destroy();
      rootEl.removeChild(panel);
    },
    flushPending,
    onTx,
    clear,
    setPillState,
  };
}

export default { mount };
