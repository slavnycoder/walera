// testbench/web/modules/entity-card.js — Plan 08-03
//
// Renders the latest known row state for a single (table, pk) channel.
//
// UI-SPEC §3.4:
//   • insert  → reveal the card; render all known fields
//   • update  → update field values; toggle .field-changed for ~300 ms on each
//               key listed in `change.data` (UPDATE carries only changed cols)
//   • delete  → collapse the card (hide <dl>; show "row deleted at {ts}")
//   • whitelist behaviour: <dt>/<dd> pairs are created lazily on first
//     observed key so a demo-bob subscriber (id+status whitelist) never shows
//     `customer_name` in the DOM.
//
// Performance contract (UI-SPEC §6):
//   • onTx() runs from the SSE handler — pushes pending mutations only;
//     NO DOM ops allowed.
//   • flushPending() (rAF tick) applies queued mutations to the DOM and
//     manages the field-flash class.
//   • The field-flash retrigger requires a void offsetWidth reflow when the
//     same field updates twice within a frame (CSS animation will not
//     re-run otherwise — well-known browser behaviour).
//
// Console discipline: warn/error only.

import { mount as mountPill } from "./connection-pill.js";

const MY_PANEL_ID = "entity-card";

const FLASH_REMOVE_MS = 350;
// UI-IN-03: under prefers-reduced-motion the CSS replaces the keyframe with
// a 1 s background→transparent transition (style.css §3 .field-changed under
// the media query). A 350 ms class removal would snap the cell back before
// the transition completes, defeating the static-color fallback.
const FLASH_REMOVE_REDUCED_MS = 1100;
function flashRemoveDelay() {
  return (typeof matchMedia === "function"
    && matchMedia("(prefers-reduced-motion: reduce)").matches)
    ? FLASH_REMOVE_REDUCED_MS
    : FLASH_REMOVE_MS;
}

function fmtTime(ts) {
  const d = new Date(ts);
  return d.toISOString().replace("T", " ").replace(/\.\d+Z$/, "Z");
}

export function mount(rootEl, _deps) {
  // ── DOM construction ────────────────────────────────────────────
  const panel = document.createElement("section");
  panel.className = "panel card";

  const hdr = document.createElement("header");
  hdr.className = "panel__hdr";
  const title = document.createElement("h2");
  title.textContent = "Entity-State Card";
  // Per-panel pill placeholder — UI-SPEC §3.4 declares `<span data-slot="pill">`
  // in the panel header. Plan 12-01 (UI-12): pill DOM and state rendering are
  // owned by connection-pill.js; updates flow through the document-level
  // `subscription:state:change` CustomEvent bus (filtered by panelId).
  const pillSlot = document.createElement("span");
  pillSlot.dataset.slot = "pill";
  const channelSpan = document.createElement("span");
  channelSpan.className = "card__channel";
  channelSpan.textContent = "(no channel)";
  hdr.appendChild(title);
  hdr.appendChild(pillSlot);
  hdr.appendChild(channelSpan);

  const placeholder = document.createElement("p");
  placeholder.className = "card__placeholder";
  placeholder.textContent = "Awaiting first event…";

  const fields = document.createElement("dl");
  fields.className = "card__fields";
  fields.dataset.slot = "fields";
  fields.hidden = true;

  const deletedNote = document.createElement("p");
  deletedNote.className = "card__deleted";
  deletedNote.hidden = true;

  panel.appendChild(hdr);
  panel.appendChild(placeholder);
  panel.appendChild(fields);
  panel.appendChild(deletedNote);
  rootEl.appendChild(panel);

  // ── Per-panel ConnectionPill (Plan 12-01, UI-12) ────────────────
  const pillHandle = mountPill(pillSlot);

  const onSubStateChange = (ev) => {
    const d = ev && ev.detail;
    if (!d || d.panelId !== MY_PANEL_ID) return;
    pillHandle.update(d.state, d.reason);
  };
  document.addEventListener("subscription:state:change", onSubStateChange);

  // ── In-memory state ─────────────────────────────────────────────
  // boundChannel: "{table}:{pk}" — only events matching this channel are applied
  // rowFields: latest known field values (map: key → value)
  // dtElems / ddElems: lazily-created <dt>/<dd> nodes keyed by field name
  // deleted: whether the row is currently in the deleted state
  // pending: { op, changedFields:Set<string>, deletedAtTs:number|null }
  //   — accumulated mutations to apply on the next flushPending()
  let boundChannel = null;
  const rowFields = new Map();
  const ddElems = new Map();
  let deleted = false;
  let pending = newPending();
  // WR-CR-04: track active field-flash timers so a bindChannel / user-switch
  // can cancel them; otherwise pending setTimeouts continue to fire ~350 ms
  // later against detached DOM nodes (no-op) or, worse, against freshly-
  // created <dd> nodes that happen to live in the same Map key.
  const flashTimers = new Set();

  function newPending() {
    return { op: null, changedFields: new Set(), deletedAtTs: null };
  }

  // ── bindChannel — called by app.js on user-switch ───────────────
  function bindChannel(channel) {
    // Cancel any pending field-flash removals so they don't fire against
    // freshly-created <dd> nodes (or leak GC roots on the old ones).
    for (const t of flashTimers) clearTimeout(t);
    flashTimers.clear();
    boundChannel = channel;
    rowFields.clear();
    ddElems.clear();
    while (fields.firstChild) fields.removeChild(fields.firstChild);
    fields.hidden = true;
    deletedNote.hidden = true;
    placeholder.hidden = false;
    deleted = false;
    pending = newPending();
    channelSpan.textContent = channel ? `Channel: ${channel}` : "(no channel)";
  }

  // ── onTx — runs from SSE handler; NO DOM ops ────────────────────
  function onTx(payload) {
    if (!boundChannel || !payload) return;
    const changes = Array.isArray(payload.changes) ? payload.changes : [];
    for (const change of changes) {
      const chKey = `${change.table}:${change.pk}`;
      if (chKey !== boundChannel) continue;

      if (change.op === "delete") {
        pending.op = "delete";
        pending.deletedAtTs = Date.now();
        // Clear rowFields — but defer DOM clear to flushPending.
        rowFields.clear();
        pending.changedFields.clear();
        continue;
      }
      if (change.op === "insert") {
        // Fresh row state — `data` is the canonical full-row map (Walera
        // wire format, encoder.go §changeEvent: Data for INSERT).
        const data = change.data || {};
        rowFields.clear();
        for (const k of Object.keys(data)) {
          rowFields.set(k, data[k]);
          pending.changedFields.add(k);
        }
        // Insert overrides any prior delete/update intent in this flush window.
        pending.op = "insert";
        continue;
      }
      // Update — Walera sends `data` as a key→new-value map containing
      // ONLY the columns whose values changed (encoder.go §changeEvent:
      // unified `data` field; op disambiguates INSERT vs UPDATE shape;
      // absence ≠ null per wal/types.go §"absence ≠ null").
      const updated = change.data && typeof change.data === "object" ? change.data : {};
      for (const k of Object.keys(updated)) {
        rowFields.set(k, updated[k]);
        pending.changedFields.add(k);
      }
      if (pending.op !== "insert" && pending.op !== "delete") {
        pending.op = "update";
      }
    }
  }

  // ── DOM application — runs once per rAF tick ────────────────────
  function flushPending() {
    if (!pending.op) return;
    const { op, changedFields, deletedAtTs } = pending;
    pending = newPending();

    if (op === "delete") {
      deleted = true;
      fields.hidden = true;
      placeholder.hidden = true;
      deletedNote.hidden = false;
      deletedNote.textContent = `row deleted at ${fmtTime(deletedAtTs || Date.now())}`;
      // Clear field DOM so a future insert starts fresh.
      while (fields.firstChild) fields.removeChild(fields.firstChild);
      ddElems.clear();
      return;
    }

    // Insert and update both render the current rowFields.
    if (deleted && op === "insert") {
      deleted = false;
      deletedNote.hidden = true;
    }
    placeholder.hidden = true;
    fields.hidden = false;

    for (const key of changedFields) {
      let dd = ddElems.get(key);
      if (!dd) {
        // Lazily create <dt>/<dd> pair for this newly observed key.
        const dt = document.createElement("dt");
        dt.textContent = key;
        dd = document.createElement("dd");
        dd.dataset.field = key;
        fields.appendChild(dt);
        fields.appendChild(dd);
        ddElems.set(key, dd);
      }
      const newVal = rowFields.has(key) ? rowFields.get(key) : "";
      dd.textContent = newVal == null ? "" : String(newVal);

      // Toggle .field-changed for the flash animation. The void-offsetWidth
      // trick retriggers the keyframe when the same field updates within a
      // single frame (the browser otherwise coalesces and skips the rerun).
      dd.classList.remove("field-changed");
      // eslint-disable-next-line no-unused-expressions
      void dd.offsetWidth; // force reflow
      dd.classList.add("field-changed");
      const timer = setTimeout(() => {
        flashTimers.delete(timer);
        dd.classList.remove("field-changed");
      }, flashRemoveDelay());
      flashTimers.add(timer);
    }
  }

  // Per-panel pill state — UI-SPEC §3.4. Plan 12-01: thin delegate to
  // pillHandle.update (DOM owned by connection-pill.js).
  const PILL_STATES = new Set(["connecting", "open", "reconnecting", "closed"]);
  function setPillState(state) {
    if (!PILL_STATES.has(state)) return;
    pillHandle.update(state);
  }

  return {
    destroy() {
      for (const t of flashTimers) clearTimeout(t);
      flashTimers.clear();
      document.removeEventListener("subscription:state:change", onSubStateChange);
      pillHandle.destroy();
      rootEl.removeChild(panel);
    },
    flushPending,
    onTx,
    bindChannel,
    setPillState,
  };
}

export default { mount };
