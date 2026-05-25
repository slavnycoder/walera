// testbench/web/modules/user-picker.js — Plan 08-02 (skeleton)
//
// Renders the three-demo-user dropdown and emits "user:change" on the shared
// eventBus when a selection is made. Full teardown/reopen logic for active
// subscriptions arrives in plan 08-03 (sse-client wiring).
//
// UI-SPEC §3.1: initial state is "errored" — no user selected. Options match
// the §3.10 mapping table verbatim.

const USERS = [
  { token: "demo-alice", label: "demo-alice (full whitelist)" },
  { token: "demo-bob",   label: "demo-bob (id + status only)" },
  { token: "demo-eve",   label: "demo-eve (articles:all wildcard)" },
];

export function mount(rootEl, deps) {
  // Build DOM: <label>User <select id="user-picker">…</select></label>
  const label = document.createElement("label");
  label.textContent = "User ";

  const select = document.createElement("select");
  select.id = "user-picker";
  select.setAttribute("aria-label", "Active demo user");
  // UI-AC-05: surface the "no user picked" errored state to AT — UI-SPEC §3.1
  // declares the initial state is errored. Cleared on first valid pick.
  select.setAttribute("aria-invalid", "true");
  select.setAttribute("aria-required", "true");

  // Placeholder option for the initial "no user selected" state.
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "— select a user —";
  placeholder.disabled = true;
  placeholder.selected = true;
  select.appendChild(placeholder);

  for (const { token, label: optLabel } of USERS) {
    const opt = document.createElement("option");
    opt.value = token;
    opt.textContent = optLabel;
    select.appendChild(opt);
  }

  label.appendChild(select);
  rootEl.appendChild(label);

  // Dispatch "user:change" on the shared eventBus when selection changes.
  const onChange = () => {
    const token = select.value;
    if (!token) return;
    // First valid pick clears the errored state (UI-AC-05).
    select.removeAttribute("aria-invalid");
    deps.eventBus.dispatchEvent(
      new CustomEvent("user:change", { detail: { token } })
    );
  };
  select.addEventListener("change", onChange);

  return {
    destroy() {
      select.removeEventListener("change", onChange);
      rootEl.removeChild(label);
    },
    flushPending() {
      // Picker has no rAF-batched work.
    },
  };
}

export default { mount };
