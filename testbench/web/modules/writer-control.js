// testbench/web/modules/writer-control.js — Plan 08-04
//
// Form that POSTs JSON to writer:9100/control to change the live load
// scenario / commit rate / rows-per-tx without restarting the writer
// container. UI-08 / UI-SPEC §3.7.
//
// Wire details:
//   • Writer CORS landed in Plan 08-04 Task 1: POST /control reflects ACAO
//     for http://localhost:8081 and the OPTIONS preflight returns 204 with
//     ACA-Methods=POST,OPTIONS. So this module uses `mode: "cors"` (NOT
//     "no-cors") and reads the JSON response body to echo the effective
//     config back to the operator.
//   • Pre-fill on mount: GET /healthz returns { status, uptime_seconds,
//     scenario } so the scenario <select> can default to the writer's
//     current state instead of the static "smoke" first option.
//   • Apply button disabled during in-flight POST to prevent double-submit.
//   • No Authorization header — writer /control is unauthenticated by
//     design (T-07-05, testbench-internal endpoint).
//
// Console discipline: warn/error only.

const SCENARIOS = ["smoke", "ramp-up", "steady", "spike", "soak", "stress"];

export function mount(rootEl, deps) {
  const urls = (deps && deps.urls) || { writer: "http://localhost:9100" };

  // Build DOM from UI-SPEC §3.7. rootEl is already a <form data-area="writer">
  // child in index.html, so we attach the inputs directly to it (but wrapped
  // in a fragment for the panel-header consistency the other panels use).
  const wrap = document.createElement("div");
  wrap.className = "panel writer";

  const header = document.createElement("header");
  header.className = "panel__hdr";
  const h2 = document.createElement("h2");
  h2.textContent = "Writer Control";
  header.appendChild(h2);
  wrap.appendChild(header);

  // Inputs row.
  const scenarioLabel = document.createElement("label");
  scenarioLabel.textContent = "Scenario ";
  const scenarioSelect = document.createElement("select");
  scenarioSelect.name = "scenario";
  for (const name of SCENARIOS) {
    const opt = document.createElement("option");
    opt.value = name;
    opt.textContent = name;
    scenarioSelect.appendChild(opt);
  }
  scenarioLabel.appendChild(scenarioSelect);
  wrap.appendChild(scenarioLabel);

  const rateLabel = document.createElement("label");
  rateLabel.textContent = "commit_rate ";
  const rateInput = document.createElement("input");
  rateInput.type = "number";
  rateInput.name = "commit_rate";
  rateInput.min = "0";
  rateInput.step = "0.1";
  rateInput.value = "100";
  rateLabel.appendChild(rateInput);
  wrap.appendChild(rateLabel);

  const rowsLabel = document.createElement("label");
  rowsLabel.textContent = "rows_per_tx ";
  const rowsInput = document.createElement("input");
  rowsInput.type = "number";
  rowsInput.name = "rows_per_tx";
  rowsInput.min = "1";
  rowsInput.step = "1";
  rowsInput.value = "1";
  rowsLabel.appendChild(rowsInput);
  wrap.appendChild(rowsLabel);

  const button = document.createElement("button");
  button.type = "submit";
  button.textContent = "Apply";
  wrap.appendChild(button);

  const status = document.createElement("p");
  status.className = "writer__status";
  status.setAttribute("data-slot", "status");
  wrap.appendChild(status);

  rootEl.appendChild(wrap);

  // Pre-fill the scenario select from GET /healthz (CONTEXT §Claude's
  // Discretion — recommended). Fire-and-forget; on failure we just keep
  // the default first option.
  (async () => {
    try {
      const response = await fetch(urls.writer + "/healthz", {
        method: "GET",
        mode: "cors",
      });
      if (!response.ok) return;
      const body = await response.json();
      if (body && typeof body.scenario === "string" && SCENARIOS.includes(body.scenario)) {
        scenarioSelect.value = body.scenario;
      }
    } catch (err) {
      console.warn("[writer-control] pre-fill /healthz failed:", err && err.message ? err.message : err);
    }
  })();

  // Submit handler. rootEl is a <form>, so we listen on it directly.
  function setStatus(text, kind) {
    status.textContent = text;
    status.className = "writer__status writer__status--" + kind;
  }

  async function onSubmit(evt) {
    evt.preventDefault();
    if (button.disabled) return;

    // WR-CR-06: parseFloat/parseInt on an empty input returned NaN, which
    // JSON.stringify serialised as `null`. The writer's Go *float64/*int
    // pointer semantics treats `null` as 'leave unchanged', so an empty
    // rate field silently dropped the rows_per_tx update too (any partial-
    // update intent was collapsed). Validate before submit and only
    // include explicitly-provided numeric fields in the payload.
    const rateRaw = rateInput.value;
    const rowsRaw = rowsInput.value;
    let rate = null;
    let rows = null;
    if (rateRaw !== "") {
      rate = Number(rateRaw);
      if (!Number.isFinite(rate) || rate <= 0) {
        setStatus("commit_rate must be a positive number", "error");
        return;
      }
    }
    if (rowsRaw !== "") {
      rows = Number(rowsRaw);
      if (!Number.isFinite(rows) || !Number.isInteger(rows) || rows < 1) {
        setStatus("rows_per_tx must be a positive integer", "error");
        return;
      }
    }

    button.disabled = true;
    setStatus("submitting…", "pending");

    const payload = { scenario: scenarioSelect.value };
    if (rate !== null) payload.commit_rate = rate;
    if (rows !== null) payload.rows_per_tx = rows;

    try {
      const response = await fetch(urls.writer + "/control", {
        method: "POST",
        mode: "cors",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        setStatus(`POST failed: ${response.status} ${response.statusText}`, "error");
        return;
      }
      const body = await response.json();
      if (body && typeof body === "object") {
        const sc = body.scenario || payload.scenario;
        // Server echoes the effective values; only display fields the server
        // actually returned (don't print `undefined` when the user submitted
        // a partial update — see WR-CR-06).
        const echoedRate = (typeof body.commit_rate === "number")
          ? body.commit_rate
          : (typeof payload.commit_rate === "number" ? payload.commit_rate : null);
        const echoedRows = (typeof body.rows_per_tx === "number")
          ? body.rows_per_tx
          : (typeof payload.rows_per_tx === "number" ? payload.rows_per_tx : null);
        let line = `applied: ${sc}`;
        if (echoedRate !== null) line += ` @ ${echoedRate} tx/s`;
        if (echoedRows !== null) line += ` × ${echoedRows}`;
        setStatus(line, "success");
      } else {
        setStatus("applied", "success");
      }
    } catch (err) {
      setStatus("network error", "error");
      console.warn("[writer-control] POST /control failed:", err && err.message ? err.message : err);
    } finally {
      button.disabled = false;
    }
  }

  rootEl.addEventListener("submit", onSubmit);

  return {
    destroy() {
      rootEl.removeEventListener("submit", onSubmit);
      if (rootEl.contains(wrap)) rootEl.removeChild(wrap);
    },
    flushPending() {
      // No-op: writer-control is event-driven (form submit), not rAF-driven.
    },
  };
}

export default { mount };
