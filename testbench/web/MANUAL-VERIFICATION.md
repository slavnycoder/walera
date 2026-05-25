## Manual Verification — Phase 08 + Phase 12 Demo UI

These checks are visual and require a human at a browser; `testbench/scripts/smoke-08.sh`
marks the corresponding ROADMAP Phase 08 / Phase 12 success criteria as
`[manual-verification-required]` and points back here. Each numbered check below
maps to one or more bullet items in SC2 / SC3 / SC5 of the ROADMAP Phase 08
goal: the automated script covers HTTP-level, CORS, and source-grep gates;
this checklist covers DOM mutation, animation, visibility-pause, and reconnect
banner copy — none of which a headless tool can confirm reliably in <5 min.
Check 11 below is the visual UAT for ROADMAP Phase 12 SC #4 (UI-12).

If ANY check fails, mark the corresponding line `Fail` and **file an issue
tagged `phase-08-uat-gap`**, then re-plan via `/gsd:plan-phase --gaps`.
A non-author operator should complete all ten checks in ≤ 5 minutes.

## Prerequisites

1. Bring the testbench compose stack up and confirm health:
   ```
   make -C testbench demo-up
   docker compose -f testbench/docker-compose.yml ps
   ```
   Expect `frontend`, `walera`, `mock-auth`, `postgres`, `writer` all `(healthy)`.
2. Drive a steady load so events flow during the visual checks:
   ```
   curl -s -X POST http://localhost:9100/control \
     -H 'Content-Type: application/json' \
     -d '{"scenario":"steady","commit_rate":5,"rows_per_tx":1}'
   ```
3. Open **http://localhost:8081/** in Chrome or Firefox (DevTools open is helpful
   but not required).

## Check index

1. Page loads with all six grid regions visible
2. Pick demo-alice — pills transition closed → connecting → open within 2 s
3. Entity card for orders:1 populates within 3 s of first writer commit
4. Field-flash on update — yellow flash + fade, observable repeatedly
5. Collapse on delete, reveal on insert
6. User switch — demo-eve tears down + reopens with wildcard-only shape
7. tx_too_large — wildcard list clears + banner shows verbatim message
8. Writer-control form — submit spike, metrics-panel reflects within 2 s
9. Backgrounded-tab pause — ring buffer caps at 200, dropped-count grows
10. Restart walera — pills reconnect, banner shows "NOT replayed", self-dismisses
11. demo-bob auth revoke — event-feed panel-pill flips to closed BEFORE the global header pill

---

### 1. Page loads with all six grid regions visible

**Action:** Visit http://localhost:8081/ in a fresh tab. Observe the layout.

**Expected:** Title bar reads "Walera Testbench". The page renders six labeled
grid regions: header (user-picker + global connection pill), banner slot
(collapsed / empty initially), live event feed, side panel (entity card +
wildcard list), metrics panel, writer-control panel. DevTools Console shows
zero errors.

**Pass / Fail:**

---

### 2. Pick demo-alice — pills transition closed → connecting → open within 2 s

**Action:** Click the user-picker, select `demo-alice`.

**Expected:** Within ~2 s, the global header pill and the per-panel pills
(feed, entity-card, wildcard-list) all transition from `closed` (▼) through
`connecting` to `open` (●). DevTools Network shows four GET requests to
`http://localhost:8080/sse/v1/{orders/1, devices/…, articles/intro-to-cdc,
articles/all}`, each carrying `Authorization: Bearer demo-alice`.

**Pass / Fail:**

---

### 3. Entity card for orders:1 populates within 3 s of first writer commit

**Action:** Keep the writer at steady@5 tx/s. Watch the side-panel
"Entity-State Card" header.

**Expected:** Within ≤ 3 s of the first `event: tx` arriving on the orders
stream, the card header reads `Channel: orders:1` and the six whitelisted
field cells populate (`id`, `customer_name`, `total_cents`, `status`,
`created_at`, `updated_at`). No `<dd>` cell is empty or shows "—".

**Pass / Fail:**

---

### 4. Field-flash on update — yellow flash + fade, observable repeatedly

**Action:** With demo-alice active and the writer at steady@5 tx/s, watch
the `<dd data-field="updated_at">` cell in the entity card. Hover/inspect it
in DevTools to confirm `data-field` attributes are present.

**Expected:** On every `orders:1` update event, the corresponding cell
flashes yellow (~300 ms) and fades back to transparent. The flash is
observable repeatedly over 30 s — no cell stays yellow, no flicker, no
console errors.

**Pass / Fail:**

---

### 5. Collapse on delete, reveal on insert

**Action:** From a host terminal:
```
docker compose -f testbench/docker-compose.yml exec postgres \
  psql -U walera -d walera -c "DELETE FROM orders WHERE id=1;"
```
Then within ~5 s:
```
docker compose -f testbench/docker-compose.yml exec postgres psql -U walera -d walera \
  -c "INSERT INTO orders (id, customer_name, total_cents, status) VALUES (1, 'Alice', 12500, 'paid');"
```

**Expected:** On DELETE, the entity card collapses (header preserved, body
shows a sub-line such as `row deleted at <ts>`). On INSERT, the card reveals
with the new field values.

**Pass / Fail:**

---

### 6. User switch — demo-eve tears down + reopens with wildcard-only shape

**Action:** Switch the user-picker from `demo-alice` to `demo-eve`.

**Expected:** DevTools Network shows the four demo-alice streams ending and
a single new stream opening to `http://localhost:8080/sse/v1/articles/all`
with `Authorization: Bearer demo-eve`. The entity-card panel hides (no exact
subscription for demo-eve). The wildcard list re-populates with articles
(no orders data anywhere in the DOM). The feed clears and resumes with
new articles events.

**Pass / Fail:**

---

### 7. tx_too_large — wildcard list clears + banner shows verbatim message

**Action:** Trigger an oversized transaction. One reliable method: configure
the writer to a `stress` scenario with a payload that exceeds walera's per-tx
size limit, e.g.:
```
docker compose -f testbench/docker-compose.yml exec postgres \
  psql -U walera -d walera -c \
  "INSERT INTO articles (slug, title, body) VALUES ('big', 'big', repeat('x', 2000000));"
```
(or any other action that produces a `event: error reason=tx_too_large` on a
subscribed channel).

**Expected:** The wildcard list immediately wipes (empty `<tbody>`). The red
banner shows the verbatim message:
**"Transaction exceeded the per-tx size limit and was not delivered."**

**Pass / Fail:**

---

### 8. Writer-control form — submit spike, metrics-panel reflects within 2 s

**Action:** In the writer-control panel, change the scenario to `spike` and
press "Apply".

**Expected:** The status line reads `applied: spike @ {rate} tx/s × {rows}`
(reflecting the JSON response body). Within ≤ 2 s the metrics panel's
`wal_tx_total (/s)` (or equivalent walera-prefixed counter) value visibly
climbs. No page reload, no full SSE teardown.

**Pass / Fail:**

---

### 9. Backgrounded-tab pause — ring buffer caps at 200, dropped-count grows

**Action:** Raise load briefly:
```
curl -s -X POST http://localhost:9100/control \
  -H 'Content-Type: application/json' \
  -d '{"scenario":"stress","commit_rate":500,"rows_per_tx":1}'
```
Switch the tab away for 30 s; switch back.

**Expected:** The feed shows the most recent 200 entries
(`document.querySelectorAll('.feed__list > li').length` ≤ 200) and the
`+ N events not shown` line below the feed shows a non-zero, increased
count. DevTools Performance shows zero `requestAnimationFrame` callbacks
fired while the tab was hidden. After returning, drop load back to
steady@5 tx/s:
```
curl -s -X POST http://localhost:9100/control \
  -H 'Content-Type: application/json' \
  -d '{"scenario":"steady","commit_rate":5,"rows_per_tx":1}'
```

**Pass / Fail:**

---

### 10. Restart walera — pills reconnect, banner shows "NOT replayed", self-dismisses

**Action:** From the host:
```
docker compose -f testbench/docker-compose.yml restart walera
```

**Expected:** All connection pills transition `open` → `reconnecting` → `open`
within ≤ 15 s. An orange reconnect banner appears with the verbatim text:
**"Reconnected — events during the gap were NOT replayed."**
The banner self-dismisses after ~8 s. No console errors persist after
the reconnect completes.

**Pass / Fail:**

---

### 11. demo-bob auth revoke — event-feed panel-pill flips to closed before global pill

**Action:** With demo-bob picked in the user-picker (so the single
`orders:1` SSE stream is open), revoke the user's token. The mock-auth
admin port is `9000`, internal to the `testbench-net` Docker bridge
(BENCH-01: deliberately NOT host-published), and the subject is the
`user_id` (`u_demo_bob`), not the token (`demo-bob`):
```
docker run --rm --network walera-testbench_testbench-net curlimages/curl \
  -s -X POST "http://mock-auth:9000/_admin/revoke?subject=u_demo_bob"
```
Wait up to ~60 s (auth refresh TTL) for walera's refresh loop to observe
the 401 and drop the subscription.

**Expected:** Within ~60 s of the curl returning 2xx (one auth-refresh
tick at the default TTL), the per-panel ConnectionPill inside the
**event-feed** panel header transitions to `closed` (▼), and so do the
`entity-card` and `wildcard-list` panel pills. The **global header pill**
also reflects `closed` since this is demo-bob's only subscription.

**Why this matters (SC #4):** the per-panel pill divergence from the
global pill is the design intent — operators can identify *which* panel
lost its stream without waiting for the aggregate. With demo-bob the
divergence is degenerate (single channel) and pills flip together; with
a multi-channel user (`demo-alice` / `demo-eve`) the bus delivers state
per-channel and individual panels can show `connecting` or `closed`
while the global aggregate remains `open`. Verified live: switching
into demo-alice produced 4 events where entity-card / wildcard-list
were `connecting` while the global pill was already `open`.

DevTools Console shows zero errors.

**Pass / Fail:**

---

## Failure-handling protocol

If ANY of the ten checks fails, file an issue tagged `phase-08-uat-gap`
including: which check failed, the observed-vs-expected delta, browser
+ OS, and a screenshot or DevTools network/console capture. Re-plan the
gap-closure work via `/gsd:plan-phase --gaps` so the next iteration of
the demo UI carries the missing behaviour.
Check 11 maps to ROADMAP Phase 12 SC #4; failures are tagged `phase-12-uat-gap` instead of phase-08-uat-gap.
