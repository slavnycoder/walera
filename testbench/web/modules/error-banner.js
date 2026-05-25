// testbench/web/modules/error-banner.js — Plan 08-02 (skeleton)
//
// Banner state machine consuming three event-bus messages:
//   • sse:error              — verbatim message lookup by detail.kind
//   • sse:reconnect          — orange banner, auto-dismisses after 8000 ms
//   • h2c:negotiation-failed — yellow banner, persists until close clicked
//
// Initial state is hidden. Exposes show(message, kind?) / hide() helpers on
// the returned handle for future modules that want to drive it directly.
//
// UI-SPEC §3.8 — verbatim message table.

const ERROR_MESSAGES = {
  auth_revoked:     "Authorization revoked. Subscription closed.",
  auth_unavailable: "Auth backend unavailable. Subscription closed.",
  slow_consumer:    "Slow consumer: server dropped this subscription. Wildcard list cleared.",
  tx_too_large:     "Transaction exceeded the per-tx size limit and was not delivered.",
  shutdown:         "Server is shutting down. Reconnect will be attempted automatically.",
};

const RECONNECT_MESSAGE =
  "Reconnected — events during the gap were NOT replayed.";
const H2C_FALLBACK_MESSAGE =
  "HTTP/2 (h2c) not negotiated — browser limited to 6 concurrent SSE " +
  "connections per tab. Multi-panel demos may stall.";

const ICONS = {
  error:          "⚠",
  reconnect:      "↻",
  "h2c-fallback": "⚠",
};

const RECONNECT_AUTO_DISMISS_MS = 8000;

export function mount(rootEl, deps) {
  // Build banner DOM (initially hidden).
  // UI-AC-04: assign a stable id so the close button can aria-controls the
  // banner. Without it, AT users hear "Dismiss button" with no association
  // to the banner content.
  const bannerId = "error-banner-region";
  const banner = document.createElement("section");
  banner.id = bannerId;
  banner.className = "banner";
  banner.setAttribute("role", "alert");
  banner.setAttribute("data-slot", "banner");
  banner.hidden = true;

  const icon = document.createElement("span");
  icon.className = "banner__icon";

  const msg = document.createElement("p");
  msg.className = "banner__msg";

  const closeBtn = document.createElement("button");
  closeBtn.className = "banner__close";
  closeBtn.setAttribute("aria-label", "Dismiss banner");
  closeBtn.setAttribute("aria-controls", bannerId);
  closeBtn.type = "button";
  closeBtn.textContent = "×";

  banner.appendChild(icon);
  banner.appendChild(msg);
  banner.appendChild(closeBtn);
  rootEl.appendChild(banner);

  let dismissTimer = null;
  let currentKind = null;

  function hide() {
    if (dismissTimer !== null) {
      clearTimeout(dismissTimer);
      dismissTimer = null;
    }
    banner.hidden = true;
    // WR-CR-02: preserve the prior kind as `banner--<kind>-dismissed` so any
    // future CSS fade-out transition still has a kind selector to hook into.
    // Setting className to bare "banner" (the previous behaviour) wiped the
    // state-transition styling mid-animation.
    if (currentKind) {
      banner.className = `banner banner--${currentKind}-dismissed`;
    } else {
      banner.className = "banner";
    }
  }

  function show(message, kind = "error", { autoDismissMs } = {}) {
    if (dismissTimer !== null) {
      clearTimeout(dismissTimer);
      dismissTimer = null;
    }
    currentKind = kind;
    banner.className = `banner banner--${kind}`;
    banner.hidden = false;
    icon.textContent = ICONS[kind] || "⚠";
    msg.textContent = message;
    if (autoDismissMs && autoDismissMs > 0) {
      dismissTimer = setTimeout(hide, autoDismissMs);
    }
  }

  closeBtn.addEventListener("click", hide);

  // ── Event-bus subscriptions ───────────────────────────────────────
  const onSseError = (evt) => {
    const kind = evt && evt.detail && evt.detail.kind;
    const message = ERROR_MESSAGES[kind]
      || (evt && evt.detail && evt.detail.reason)
      || "Subscription error.";
    show(message, "error");
  };

  const onSseReconnect = () => {
    show(RECONNECT_MESSAGE, "reconnect", {
      autoDismissMs: RECONNECT_AUTO_DISMISS_MS,
    });
  };

  const onH2cFailed = () => {
    show(H2C_FALLBACK_MESSAGE, "h2c-fallback");
  };

  deps.eventBus.addEventListener("sse:error",              onSseError);
  deps.eventBus.addEventListener("sse:reconnect",          onSseReconnect);
  deps.eventBus.addEventListener("h2c:negotiation-failed", onH2cFailed);

  return {
    destroy() {
      deps.eventBus.removeEventListener("sse:error",              onSseError);
      deps.eventBus.removeEventListener("sse:reconnect",          onSseReconnect);
      deps.eventBus.removeEventListener("h2c:negotiation-failed", onH2cFailed);
      closeBtn.removeEventListener("click", hide);
      if (dismissTimer !== null) clearTimeout(dismissTimer);
      rootEl.removeChild(banner);
    },
    flushPending() {
      // Banner is event-driven, not rAF-batched.
    },
    show,
    hide,
  };
}

export default { mount };
