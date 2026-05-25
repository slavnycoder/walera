// testbench/web/modules/sse-client.js — Plan 08-03
//
// Thin wrapper around the vendored @microsoft/fetch-event-source polyfill.
// Native EventSource cannot send an Authorization header, hence the polyfill.
//
// Responsibilities (UI-SPEC §3.9, plan 08-03 brief):
//   • One open() call per channel — returns a subscription handle { close, channel, state }.
//   • Inject `Authorization: Bearer <token>` per request.
//   • `openWhenHidden: true` — the connection stays alive when the tab is
//     backgrounded. UI-10 pausing happens in the rAF flush layer (event-feed),
//     NOT at the connection layer; closing the stream on every tab-switch
//     would cause a reconnect storm and lose in-flight events.
//   • Translate the polyfill's onmessage callback into typed handlers
//     (onTx / onError / onShutdown) for `event: tx`, `event: error`,
//     `event: shutdown`. Heartbeats (empty events) are silently dropped.
//   • Honour Walera's `retry: 15000` prelude (WALERA-01) — on transient
//     network errors the polyfill retries with the prelude-provided cadence;
//     on terminal errors (4xx auth/permission), throw to stop retrying.
//   • Track a four-state lifecycle: connecting → open → reconnecting → closed.
//     State transitions are surfaced via handlers.onOpen / handlers.onClose.
//     NO eventBus calls — this module is pure; the caller (app.js) drives the
//     bus, keeping a single source of truth for cross-panel wiring.
//
// Console discipline: warn/error only.

import { fetchEventSource, EventStreamContentType } from "../vendor/fes/index.js";

// Walera SSE prelude is `retry: 15000` (WALERA-01). The polyfill's
// onmessage `retry` field overrides our default; we keep this as the
// fall-back retry interval for transient errors before any retry directive
// has been observed.
const DEFAULT_RETRY_MS = 15000;

/**
 * Open one SSE subscription.
 *
 * @param {Object} opts
 * @param {string} opts.url       full URL (e.g. http://localhost:8080/sse/v1/orders/1)
 * @param {string} opts.token     bearer token (e.g. "demo-alice")
 * @param {string} opts.channel   "{table}:{pk}" identifier for logging / handle.channel
 * @param {Object} opts.handlers  callback bag (all optional except onTx)
 *   - onOpen({ channel })                : connection just transitioned to open
 *   - onTx(payload)                      : `event: tx` (parsed JSON object)
 *   - onError({ kind, raw, channel })    : `event: error` (Walera-typed)
 *   - onShutdown({ channel })            : `event: shutdown`
 *   - onClose({ reason, channel })       : terminal close (reason: "error" | "abort" | "complete")
 * @returns {{ close(): void, channel: string, get state(): string }}
 */
export function open({ url, token, channel, handlers }) {
  const ctrl = new AbortController();
  let state = "connecting";
  let terminalResponseStatus = null; // captured by onopen for the onerror branch

  function setState(next) {
    state = next;
  }

  // onopen — content-type + status gate. Throw to mark fatal (the polyfill's
  // onerror will then receive the thrown error and we decide retry/terminate).
  async function onopen(response) {
    if (!response.ok) {
      terminalResponseStatus = response.status;
      throw new Error(
        `[sse-client] non-OK response for ${channel}: HTTP ${response.status}`,
      );
    }
    const ct = response.headers.get("content-type");
    if (!ct || !ct.startsWith(EventStreamContentType)) {
      terminalResponseStatus = response.status;
      throw new Error(
        `[sse-client] unexpected content-type for ${channel}: ${ct}`,
      );
    }
    // Successful (re)open transition.
    const wasReconnecting = state === "reconnecting";
    setState("open");
    try {
      handlers && typeof handlers.onOpen === "function" && handlers.onOpen({ channel, reconnected: wasReconnecting });
    } catch (cbErr) {
      console.error(`[sse-client] onOpen handler threw for ${channel}:`, cbErr);
    }
  }

  // onmessage — dispatch by event type. The polyfill normalises the default
  // event name to the empty string; Walera always sets an explicit event name.
  function onmessage(msg) {
    // Heartbeat / keepalive comments arrive as empty event + ":" or empty data.
    if (!msg || !msg.event || msg.event === "") {
      // Walera does not send unnamed data events on hot path; ignore silently.
      return;
    }
    const evt = msg.event;
    if (evt === "tx") {
      let payload;
      try {
        payload = JSON.parse(msg.data);
      } catch (parseErr) {
        console.warn(`[sse-client] tx parse failed for ${channel}:`, parseErr);
        return;
      }
      try {
        handlers && typeof handlers.onTx === "function" && handlers.onTx(payload);
      } catch (cbErr) {
        console.error(`[sse-client] onTx handler threw for ${channel}:`, cbErr);
      }
      return;
    }
    if (evt === "error") {
      let parsed = {};
      try {
        parsed = msg.data ? JSON.parse(msg.data) : {};
      } catch (parseErr) {
        // Walera always sends JSON; if parse fails, surface the raw payload.
        console.warn(`[sse-client] error parse failed for ${channel}:`, parseErr);
      }
      try {
        handlers && typeof handlers.onError === "function"
          && handlers.onError({ kind: parsed.reason, raw: msg.data, channel });
      } catch (cbErr) {
        console.error(`[sse-client] onError handler threw for ${channel}:`, cbErr);
      }
      return;
    }
    if (evt === "shutdown") {
      try {
        handlers && typeof handlers.onShutdown === "function"
          && handlers.onShutdown({ channel });
      } catch (cbErr) {
        console.error(`[sse-client] onShutdown handler threw for ${channel}:`, cbErr);
      }
      return;
    }
    if (evt === "heartbeat") {
      // Silent drop; presence is enough — no caller wiring needed.
      return;
    }
    // Unknown event name — log once at warn so we notice if Walera adds events.
    console.warn(`[sse-client] unknown event "${evt}" on ${channel}`);
  }

  // Map HTTP status (captured in onopen on a non-OK response) to the
  // error-banner kind so app.js does not embed CORS/auth-policy knowledge.
  //   401 / 403       → auth_revoked       (user lost permission)
  //   5xx / network   → auth_unavailable   (auth backend / upstream sick)
  //   other 4xx       → auth_unavailable   (best-effort fallback so the
  //                      operator still sees a banner; UI-SPEC §3.8 has no
  //                      "unknown 4xx" kind, and silently swallowing the
  //                      close would mis-direct debugging)
  function statusToKind(status) {
    if (status === 401 || status === 403) return "auth_revoked";
    return "auth_unavailable";
  }

  // onerror — return number = retry-ms; throw = fatal (stops the polyfill).
  function onerror(err) {
    setState("reconnecting");
    // Auth/permission failures (4xx) are terminal. terminalResponseStatus is
    // populated by onopen() before it throws.
    if (terminalResponseStatus !== null && terminalResponseStatus < 500) {
      setState("closed");
      const kind = statusToKind(terminalResponseStatus);
      try {
        handlers && typeof handlers.onClose === "function"
          && handlers.onClose({ reason: "error", channel, status: terminalResponseStatus, kind });
      } catch (cbErr) {
        console.error(`[sse-client] onClose handler threw for ${channel}:`, cbErr);
      }
      // Throw → polyfill stops retrying.
      throw err;
    }
    // Transient (network reset, 5xx, content-type mismatch on retry). Keep
    // retrying at Walera's prelude cadence. Reset the captured status so a
    // subsequent successful onopen() doesn't carry stale state.
    terminalResponseStatus = null;
    return DEFAULT_RETRY_MS;
  }

  // onclose — the polyfill calls this only on a clean server-side close
  // (rare for SSE; typically the connection dies and onerror fires instead).
  function onclose() {
    setState("closed");
    try {
      handlers && typeof handlers.onClose === "function"
        && handlers.onClose({ reason: "complete", channel });
    } catch (cbErr) {
      console.error(`[sse-client] onClose handler threw for ${channel}:`, cbErr);
    }
  }

  // Fire the fetch. We do NOT await — the polyfill returns a Promise that
  // resolves when the stream terminates (abort or onclose).
  fetchEventSource(url, {
    method: "GET",
    headers: { Authorization: "Bearer " + token },
    signal: ctrl.signal,
    openWhenHidden: true, // UI-10: connection-layer must NOT pause on hidden tab
    onopen,
    onmessage,
    onerror,
    onclose,
  }).catch((err) => {
    // Terminal (thrown from onerror). State already transitioned in onerror.
    if (state !== "closed") {
      setState("closed");
      // If a terminal status was captured but onerror's onClose has not yet
      // run for some pathological path, surface a best-effort kind so the
      // banner still classifies correctly.
      const kind = terminalResponseStatus !== null
        ? statusToKind(terminalResponseStatus)
        : "auth_unavailable";
      try {
        handlers && typeof handlers.onClose === "function"
          && handlers.onClose({ reason: "error", channel, error: err, status: terminalResponseStatus, kind });
      } catch (cbErr) {
        console.error(`[sse-client] onClose handler threw for ${channel}:`, cbErr);
      }
    }
  });

  return {
    channel,
    get state() { return state; },
    close() {
      if (state === "closed") return;
      const prev = state;
      setState("closed");
      ctrl.abort();
      // Only call onClose if we transitioned from a non-terminal state; if
      // the polyfill already fired onclose/onerror, state was already "closed".
      if (prev !== "closed") {
        try {
          handlers && typeof handlers.onClose === "function"
            && handlers.onClose({ reason: "abort", channel });
        } catch (cbErr) {
          console.error(`[sse-client] onClose handler threw for ${channel}:`, cbErr);
        }
      }
    },
  };
}

export default { open };
