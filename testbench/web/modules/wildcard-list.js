// testbench/web/modules/wildcard-list.js — Plan 08-03
//
// Deduplicated table view for a wildcard subscription (e.g. articles:all).
//
// UI-SPEC §3.5:
//   • Keyed by (table, pk); insertion order preserved (Map iteration order).
//   • insert / update → upsert the row; cell-level .field-changed flash.
//   • delete         → remove the row.
//   • On `event: error reason in {tx_too_large, slow_consumer}`, clear the
//     dedup Map AND the DOM <tbody>. Banner is shown by error-banner via
//     its own eventBus subscription; this module's clearOnError() handles
//     the local reconciliation only.
//
// Performance contract (UI-SPEC §6):
//   • onTx() runs from the SSE handler — only mutates the in-memory Map and
//     records dirty keys; NO DOM ops allowed.
//   • flushPending() (rAF tick) diff-renders only the rows whose keys are
//     in the dirty set since the last flush.
//
// Console discipline: warn/error only.

import { mount as mountPill } from "./connection-pill.js";

const MY_PANEL_ID = "wildcard-list";

const FLASH_REMOVE_MS = 350;
// UI-IN-03: see entity-card.js — under prefers-reduced-motion the CSS uses a
// 1 s background transition, so removal must be deferred until that finishes.
const FLASH_REMOVE_REDUCED_MS = 1100;
function flashRemoveDelay() {
  return (typeof matchMedia === "function"
    && matchMedia("(prefers-reduced-motion: reduce)").matches)
    ? FLASH_REMOVE_REDUCED_MS
    : FLASH_REMOVE_MS;
}

export function mount(rootEl, _deps) {
  // ── DOM construction ────────────────────────────────────────────
  const panel = document.createElement("section");
  panel.className = "panel wildcard";

  const hdr = document.createElement("header");
  hdr.className = "panel__hdr";
  const title = document.createElement("h2");
  title.textContent = "Wildcard List";
  // Per-panel pill placeholder — UI-SPEC §3.5 declares `<span data-slot="pill">`
  // in the panel header. Plan 12-01 (UI-12): pill DOM and state rendering are
  // owned by connection-pill.js; updates flow through the document-level
  // `subscription:state:change` CustomEvent bus (filtered by panelId).
  const pillSlot = document.createElement("span");
  pillSlot.dataset.slot = "pill";
  const channelSpan = document.createElement("span");
  channelSpan.className = "wildcard__channel";
  channelSpan.textContent = "(no channel)";
  hdr.appendChild(title);
  hdr.appendChild(pillSlot);
  hdr.appendChild(channelSpan);

  const placeholder = document.createElement("p");
  placeholder.className = "wildcard__placeholder";
  placeholder.textContent = "Awaiting first event…";

  const table = document.createElement("table");
  table.className = "wildcard__table";
  table.hidden = true;
  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  thead.appendChild(headRow);
  const tbody = document.createElement("tbody");
  tbody.dataset.slot = "rows";
  table.appendChild(thead);
  table.appendChild(tbody);

  panel.appendChild(hdr);
  panel.appendChild(placeholder);
  panel.appendChild(table);
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
  // boundChannel: "{table}:all" — only events for this channel apply
  // pkColumn: name of the PK field (string) — set by bindChannel
  // rows: Map<pk, { row: object, prev: object|null }>
  // dirtyKeys: Set<pk> — keys whose DOM needs (re)render at next flush
  // columns: ordered list of column names (derived from the first observed row)
  let boundChannel = null;
  let pkColumn = "pk";
  let tableName = "";
  const rows = new Map();
  const dirtyKeys = new Set();
  const deletedKeys = new Set();
  let columns = null; // null = not yet observed
  const trElems = new Map(); // pk → <tr>
  // WR-CR-04: cancel pending field-flash removals on bindChannel /
  // clearOnError so they don't fire against rebuilt rows. Each entry is a
  // setTimeout handle returned by setTimeout().
  const flashTimers = new Set();

  function bindChannel(channel, pkCol) {
    for (const t of flashTimers) clearTimeout(t);
    flashTimers.clear();
    boundChannel = channel;
    pkColumn = pkCol || "pk";
    tableName = (channel || "").split(":")[0] || "";
    rows.clear();
    dirtyKeys.clear();
    deletedKeys.clear();
    trElems.clear();
    columns = null;
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    while (headRow.firstChild) headRow.removeChild(headRow.firstChild);
    table.hidden = true;
    placeholder.hidden = false;
    channelSpan.textContent = channel ? `Channel: ${channel}` : "(no channel)";
  }

  // ── onTx — SSE handler; NO DOM ──────────────────────────────────
  function onTx(payload) {
    if (!boundChannel || !payload) return;
    const changes = Array.isArray(payload.changes) ? payload.changes : [];
    for (const change of changes) {
      // Wildcard channel matches every PK on its table; filter by table.
      if (tableName && change.table !== tableName) continue;

      const pkVal = change.pk;
      if (pkVal == null) continue;
      const key = String(pkVal);

      if (change.op === "delete") {
        rows.delete(key);
        deletedKeys.add(key);
        dirtyKeys.add(key);
        continue;
      }

      // Walera wire format (encoder.go §changeEvent — unified `data`):
      //   INSERT → change.data is the full new row map
      //   UPDATE → change.data is a partial map of changed keys only
      //   DELETE → change.data is absent
      //   absence ≠ null (wal/types.go) — merge UPDATE into prior row state
      const existing = rows.get(key);
      const prev = existing ? existing.row : null;
      let nextRow;
      if (change.op === "insert" || !prev) {
        const data = change.data || {};
        nextRow = { ...data };
        // PK is carried out-of-band on `change.pk`; ensure it's in the row
        // map so the table cell renders correctly even if the PK column
        // is also the conceptual identifier.
        if (pkColumn && !Object.prototype.hasOwnProperty.call(nextRow, pkColumn)) {
          nextRow[pkColumn] = pkVal;
        }
      } else {
        // UPDATE — merge the partial `data` over the prior row.
        const partial = change.data || {};
        nextRow = { ...prev, ...partial };
      }
      // Establish column order on first observed row. For wildcard lists,
      // prefer the PK column first if present.
      if (!columns) {
        const cols = Object.keys(nextRow);
        if (pkColumn && cols.includes(pkColumn)) {
          columns = [pkColumn, ...cols.filter((c) => c !== pkColumn)];
        } else {
          columns = cols;
        }
      }
      rows.set(key, { row: nextRow, prev });
      deletedKeys.delete(key);
      dirtyKeys.add(key);
    }
  }

  // ── flushPending — rAF tick ─────────────────────────────────────
  function flushPending() {
    if (dirtyKeys.size === 0) return;

    // First render? Build the <thead> from the discovered column order.
    if (columns && headRow.childNodes.length === 0) {
      for (const col of columns) {
        const th = document.createElement("th");
        th.textContent = col;
        headRow.appendChild(th);
      }
    }
    if (rows.size > 0 || dirtyKeys.size > 0) {
      table.hidden = false;
      placeholder.hidden = true;
    }

    for (const key of dirtyKeys) {
      if (deletedKeys.has(key)) {
        const tr = trElems.get(key);
        if (tr && tr.parentNode === tbody) tbody.removeChild(tr);
        trElems.delete(key);
        deletedKeys.delete(key);
        continue;
      }
      const entry = rows.get(key);
      if (!entry) continue;
      const { row, prev } = entry;
      let tr = trElems.get(key);
      let isNew = false;
      if (!tr) {
        tr = document.createElement("tr");
        tr.dataset.pk = key;
        // Build cells in column order.
        for (const col of columns) {
          const td = document.createElement("td");
          td.dataset.field = col;
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
        trElems.set(key, tr);
        isNew = true;
      }
      // Update each cell's text; flash if the value changed (or is brand new).
      const cells = tr.children;
      for (let i = 0; i < columns.length; i++) {
        const col = columns[i];
        const td = cells[i];
        if (!td) continue;
        const newVal = Object.prototype.hasOwnProperty.call(row, col) ? row[col] : "";
        const oldVal = prev && Object.prototype.hasOwnProperty.call(prev, col) ? prev[col] : undefined;
        td.textContent = newVal == null ? "" : String(newVal);
        if (isNew || newVal !== oldVal) {
          td.classList.remove("field-changed");
          // eslint-disable-next-line no-unused-expressions
          void td.offsetWidth; // force reflow so the animation retriggers
          td.classList.add("field-changed");
          const timer = setTimeout(() => {
            flashTimers.delete(timer);
            td.classList.remove("field-changed");
          }, flashRemoveDelay());
          flashTimers.add(timer);
        }
      }
      // Snapshot prev = row so the next update diff is correct.
      entry.prev = row;
    }
    dirtyKeys.clear();

    if (rows.size === 0) {
      table.hidden = true;
      placeholder.hidden = false;
    }
  }

  // ── clearOnError — banner-reconcile hook ────────────────────────
  // Called by app.js from the sse:error listener for tx_too_large /
  // slow_consumer. Also useful for "user-switch" resets.
  function clearOnError(_reason) {
    for (const t of flashTimers) clearTimeout(t);
    flashTimers.clear();
    rows.clear();
    dirtyKeys.clear();
    deletedKeys.clear();
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    trElems.clear();
    placeholder.hidden = false;
    table.hidden = true;
  }

  // Per-panel pill state — UI-SPEC §3.5. Plan 12-01: thin delegate to
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
    clearOnError,
    setPillState,
  };
}

export default { mount };
