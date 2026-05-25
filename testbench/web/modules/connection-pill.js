// testbench/web/modules/connection-pill.js — Plan 08-02 (skeleton)
//
// Stateless renderer for one of four connection states:
//   connecting / open / reconnecting / closed
// UI-SPEC §3.2. Initial render is the default "closed" state per plan brief
// (no live connection wiring yet — that arrives in plan 08-03).
//
// Returns { destroy, flushPending, update } so the future sse-client can
// drive state transitions by calling handle.update("open"|"reconnecting"|…).

const GLYPHS = {
  connecting:   "○",
  open:         "●",
  reconnecting: "◆",
  closed:       "▼",
};

const VALID_STATES = new Set(Object.keys(GLYPHS));

export function mount(rootEl, _deps) {
  const pill = document.createElement("span");
  pill.setAttribute("role", "status");

  const dot = document.createElement("span");
  dot.className = "pill__dot";

  const text = document.createElement("span");
  text.className = "pill__text";

  pill.appendChild(dot);
  pill.appendChild(document.createTextNode(" "));
  pill.appendChild(text);
  rootEl.appendChild(pill);

  let currentState = null;

  function update(state, reason) {
    if (!VALID_STATES.has(state)) {
      console.warn(`[connection-pill] invalid state: ${state}`);
      return;
    }
    if (currentState) pill.classList.remove(`pill--${currentState}`);
    currentState = state;
    pill.className = `pill pill--${state}`;
    pill.setAttribute("aria-label", `Connection ${state}`);
    dot.textContent = GLYPHS[state];
    text.textContent = reason ? `${state}: ${reason}` : state;
  }

  // Initial render: default to "closed" until the sse-client wires up.
  update("closed");

  return {
    destroy() {
      rootEl.removeChild(pill);
    },
    flushPending() {
      // Pill is event-driven, not rAF-batched.
    },
    update,
  };
}

export default { mount };
