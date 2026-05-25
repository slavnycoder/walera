// testbench/web/modules/h2c-detector.js — Plan 08-03
//
// One-shot probe for HTTP/2 (h2c) negotiation against Walera.
//
// UI-SPEC §4.5 + Open Conflict OC-2:
//   Browsers do not expose ALPN negotiation results directly. The closest
//   proxy is `PerformanceResourceTiming.nextHopProtocol`, populated for
//   network-fetched resources. Values we care about:
//     "h2c" / "h2"   → success (no banner)
//     "http/1.1"     → definitive negative (fire banner)
//     "" / undefined → unknown (Safari, privacy mode) — silently skip
//
// Probe choice (CONTEXT decision Q3): GET /healthz, not HEAD.
//   Some browsers elide resource-timing entries for HEAD; GET is universal.
//
// Probe failure ≠ h2c negotiation failure. A network error means the user
// has a bigger problem than h2c; we warn and skip — do NOT show the banner
// (would mis-attribute the cause).
//
// Console discipline: warn/error only.

/**
 * @param {string} walera_url   base URL of walera (e.g. "http://localhost:8080")
 * @param {EventTarget} eventBus  shared app event bus
 * @returns {Promise<void>}     resolves after probe completes (or fails silently)
 */
export async function probe(walera_url, eventBus) {
  const probeUrl = walera_url.replace(/\/$/, "") + "/healthz";
  try {
    // WR-CR-05: deterministic resource-timing read via PerformanceObserver.
    // The previous code called performance.getEntriesByType("resource")
    // immediately after `await response.text()`; per the Resource Timing
    // spec the entry is added when "the resource's fetch is complete", but
    // browsers may post-finalize the timing entry into the buffer one
    // microtask after the body-bytes promise resolves. On the affected
    // browsers, the entry lookup returned null and the banner could
    // silently never fire even on a definitive http/1.1 negotiation.
    //
    // Attach the observer FIRST so we cannot miss the entry, then fire the
    // fetch. Resolve with the observed nextHopProtocol (or "" on timeout).
    const proto = await new Promise((resolve) => {
      let settled = false;
      let timer = 0;
      const finish = (val) => {
        if (settled) return;
        settled = true;
        try { obs.disconnect(); } catch (_e) {}
        if (timer) clearTimeout(timer);
        resolve(val);
      };
      // PerformanceObserver may not exist in very old browsers — fall back
      // to the old immediate-read path so we still try.
      if (typeof PerformanceObserver !== "function") {
        fetch(probeUrl, { method: "GET", mode: "cors" })
          .then((r) => r.text().catch(() => ""))
          .then(() => {
            const entries = performance.getEntriesByType
              ? performance.getEntriesByType("resource")
              : [];
            for (let i = entries.length - 1; i >= 0; i--) {
              if (entries[i].name && entries[i].name.endsWith("/healthz")) {
                resolve(entries[i].nextHopProtocol || "");
                return;
              }
            }
            resolve("");
          })
          .catch(() => resolve(""));
        return;
      }
      // Standard path.
      // eslint-disable-next-line no-var
      var obs = new PerformanceObserver((list) => {
        for (const e of list.getEntries()) {
          if (e.name && e.name.endsWith("/healthz")) {
            finish(e.nextHopProtocol || "");
            return;
          }
        }
      });
      try {
        obs.observe({ type: "resource", buffered: true });
      } catch (_observeErr) {
        // `type` may not be supported (older spec form); try entryTypes.
        try {
          obs.observe({ entryTypes: ["resource"] });
        } catch (_e2) {
          finish("");
          return;
        }
      }
      // Fire the fetch AFTER the observer is attached.
      fetch(probeUrl, { method: "GET", mode: "cors" })
        .then((r) => r.text().catch(() => ""))
        .catch(() => {
          // Network error — finish "" so we behave as unknown (no banner)
          // per the OC-2 zero-false-positive policy.
          finish("");
        });
      // Safety timeout — if no entry materializes in 2 s, treat as unknown.
      timer = setTimeout(() => finish(""), 2000);
    });

    if (proto === "h2c" || proto === "h2") {
      return; // happy path — silent
    }
    if (proto === "http/1.1") {
      eventBus.dispatchEvent(
        new CustomEvent("h2c:negotiation-failed", {
          detail: { observed: "http/1.1" },
        }),
      );
      return;
    }
    // Empty string, undefined, or any other value: unknown — skip.
    return;
  } catch (err) {
    // Probe failure ≠ h2c failure. Warn but do NOT fire the banner.
    console.warn(`[h2c-detector] probe to ${probeUrl} failed:`, err);
  }
}

export default { probe };
