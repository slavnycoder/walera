# Vendored third-party assets

This directory contains third-party browser code checked into the Walera repo
verbatim. Per UI-01 (Phase 08 — Demo UI), the testbench frontend MUST contain
no `node_modules/` and no `package.json`. Any third-party library used by the
demo UI is vendored here, source-traceable, and SHA-256-pinned so an auditor
can prove no upstream tampering occurred between retrieval and commit.

---

## fes/ — @microsoft/fetch-event-source

**Library:** `@microsoft/fetch-event-source`
**Version:** `2.0.1`
**License:** MIT — https://github.com/Azure/fetch-event-source/blob/main/LICENSE
**Upstream URL prefix:** https://unpkg.com/@microsoft/fetch-event-source@2.0.1/lib/esm/
**Retrieval date:** 2026-05-17
**Retrieval command:**

```sh
mkdir -p testbench/web/vendor/fes
cd testbench/web/vendor/fes
curl -fsSL -o index.js https://unpkg.com/@microsoft/fetch-event-source@2.0.1/lib/esm/index.js
curl -fsSL -o fetch.js https://unpkg.com/@microsoft/fetch-event-source@2.0.1/lib/esm/fetch.js
curl -fsSL -o parse.js https://unpkg.com/@microsoft/fetch-event-source@2.0.1/lib/esm/parse.js
sha256sum index.js fetch.js parse.js
```

**Purpose:** Native browser `EventSource` cannot set custom request headers,
so it cannot carry the `Authorization: Bearer <token>` that Walera requires
for SSE subscriptions. This polyfill replaces `EventSource` with a fetch-based
equivalent that accepts custom headers (including Authorization), exposes the
same connection-state semantics, and honors the `retry: <ms>` prelude.

**SHA-256 (binding, post-[browser-compat] patch):**

| File       | Size (bytes) | SHA-256                                                            |
| ---------- | ------------ | ------------------------------------------------------------------ |
| `index.js` | 105          | `4956c8a85aebf18c91e2b9a9605db85c3e5d1dee2dd5a3b031f37592ea544be2` |
| `fetch.js` | 4089         | `ed67747d149013d7e5761f8143bf12162c7d4f93cdbeb165afcf35dce344d436` |
| `parse.js` | 3570         | `8e136b9d47df94b18490bdd347b3029f0ecffe96bd052dd0263143df8aac5e0b` |

**Verify locally:**

```sh
( cd testbench/web/vendor/fes && sha256sum -c <<'EOF'
4956c8a85aebf18c91e2b9a9605db85c3e5d1dee2dd5a3b031f37592ea544be2  index.js
ed67747d149013d7e5761f8143bf12162c7d4f93cdbeb165afcf35dce344d436  fetch.js
8e136b9d47df94b18490bdd347b3029f0ecffe96bd052dd0263143df8aac5e0b  parse.js
EOF
)
```

**Local modifications — [browser-compat] only:**

The vendored files have ONE class of modification applied on top of the
upstream ESM build at https://unpkg.com/@microsoft/fetch-event-source@2.0.1/lib/esm/ :

- `index.js:1` — `from './fetch'`  → `from './fetch.js'`
- `fetch.js:12` — `from './parse'` → `from './parse.js'`

**Rationale:** Upstream targets Node module resolution, which silently
implies the `.js` extension. Native browser ES-module resolution (per HTML
spec) rejects extensionless relative specifiers with `TypeError: Failed to
resolve module specifier "./fetch"`. The change is byte-equivalent to the
upstream JS logic — only the import-specifier suffix is altered so the
graph resolves under a vanilla `<script type="module">` with no import map
and no bundler. `parse.js` is unmodified.

**Re-vendor procedure (when upstream ships a security patch):**

1. Re-run the `curl` block above to fetch fresh upstream bytes.
2. Re-apply the two `.js` suffix patches in `index.js` and `fetch.js` listed
   under "Local modifications" above.
3. Re-run `sha256sum index.js fetch.js parse.js` and update the SHA-256
   table in this README in the same commit so the recorded digests always
   match the bytes on disk.

The `.js`-suffix patch is the ONLY change permitted on top of upstream; any
behavioural delta must be filed upstream first.
