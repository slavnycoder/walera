# Walera — PostgreSQL Setup Guide for Operators

This document is the DBA playbook for deploying Walera against a real
PostgreSQL cluster. It covers required PostgreSQL configuration, role
creation, publication migration, OS-level tuning (DEP-04), Kubernetes
Secret management (DEP-02), and a troubleshooting cookbook.

Walera is designed for **a single Kubernetes instance** streaming from
**one PostgreSQL database** at a time. If you operate multiple instances
(staging + prod, multiple PGs), each needs its own publication + slot
namespace.

> **Credentials are supplied once via `WALERA_DATABASE_URL`
> (`database.url`).** This single base URL IS the admin/startup-check
> connection; Walera derives the replication connection from it automatically
> by adding `replication=database` at load time. Create ONE database role,
> give it the `REPLICATION` attribute, and put it in `WALERA_DATABASE_URL`.
> If `replication` is present by accident, Walera strips it from the admin
> connection and sets the derived replication connection to
> `replication=database`.

---

## 1. PostgreSQL Version and Configuration

**Required:** PostgreSQL **14 or newer** with `wal_level=logical`.

### postgresql.conf

```ini
# Required for Walera (logical replication via pgoutput).
wal_level = logical

# Each Walera instance opens ONE temporary replication slot.
# Spec §10.4 formula:
#   max_replication_slots >= ceil(N * 1.25) + reserved_slots + 2
# where N is the number of expected concurrent Walera instances and
# reserved_slots is any slots used by other consumers (Debezium, pgbackrest,
# physical replicas, etc.).
#
# Worked example for 1 Walera + 2 slots reserved for other consumers + 2 safety:
#   max_replication_slots >= ceil(1 * 1.25) + 2 + 2 = 6
# A practical floor is 10 for any non-trivial deployment.
max_replication_slots = 10

# max_wal_senders must be >= max_replication_slots for logical replication
# to be able to acquire a sender per slot.
max_wal_senders = 10

# Optional but recommended: increase WAL retention so brief Walera outages
# do not cause WAL recycling to remove unstreamed segments. Tune to your
# disk budget and typical Walera downtime tolerance.
# max_slot_wal_keep_size = 2GB
```

After editing, **restart** PostgreSQL (most of these settings require a
restart, not just a reload).

### Verifying

```sql
SHOW wal_level;                   -- expect: logical
SHOW max_replication_slots;       -- expect: >= 10
SHOW max_wal_senders;             -- expect: >= max_replication_slots
```

---

## 2. Database Role

Walera uses **one** database role for everything. That single role backs
both connections derived from `WALERA_DATABASE_URL`:

- the **replication connection** (derived: base URL + `replication=database`),
  which runs `CREATE_REPLICATION_SLOT ... TEMPORARY`, `IDENTIFY_SYSTEM`, and
  `START_REPLICATION`; and
- the **admin/startup-check connection** (the base URL as-is), which verifies
  the publication exists (`pg_publication_tables`), checks replication-slot
  headroom (`pg_replication_slots`), and runs the lag sampler
  (`pg_wal_lsn_diff(...)`) every 5s.

Because the same role serves the replication connection, it **must** hold the
`REPLICATION` attribute. This requirement is **not** validated at config load
— config load only checks that the URL parses. A role that is missing the
`REPLICATION` attribute fails at **runtime**, on `START_REPLICATION`.

```sql
-- Create the single role with REPLICATION and a strong password.
CREATE ROLE walera WITH REPLICATION LOGIN PASSWORD 'change-me';

-- Grant database-level connect.
GRANT CONNECT ON DATABASE app TO walera;

-- The role must be able to SELECT from every table in the publication so
-- that pgoutput can decode INSERT/UPDATE/DELETE column values. Grant
-- per-table or via a role:
GRANT USAGE ON SCHEMA public TO walera;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO walera;
-- For tables created later:
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO walera;

-- Required for the pg_replication_slots view + WAL LSN functions used by the
-- startup headroom check and the lag sampler.
GRANT pg_monitor TO walera;

-- Required for the publication-existence check at startup.
GRANT SELECT ON pg_publication_tables TO walera;
```

The single base DSN passed via `WALERA_DATABASE_URL` looks like:

```
postgres://walera:change-me@pg.example.com:5432/app?sslmode=require
```

Walera derives the replication form of this URL automatically. If the URL
already carries a `replication` parameter, Walera removes it from the admin
connection and applies `replication=database` only to the derived replication
connection.

---

## 4. Publication Migration

Walera streams only what is in a DBA-owned PostgreSQL publication. The
template lives at `migrations/001_publication.sql.example`. **Copy to
`migrations/001_publication.sql`** with your real table list and apply
once via your existing migration tool.

```sql
-- Excerpted from migrations/001_publication.sql.example.
-- See that file for full constraints (PK type, REPLICA IDENTITY, TRUNCATE, …).

CREATE PUBLICATION cdc_sse_streamer
    FOR TABLE
        public.users
        , public.orders
    WITH (publish = 'insert, update, delete');
```

### Hard constraints (enforced by Walera at runtime)

| Constraint                  | Why                                                                                  |
| --------------------------- | ------------------------------------------------------------------------------------ |
| `REPLICA IDENTITY DEFAULT`  | `FULL` doubles/triples WAL volume; `NOTHING` means DELETE has no PK — spec rejects.   |
| Single-column scalar PK     | `int2/4/8`, `uuid`, or `text` only. Composite PK and TOAST-unsafe PK types rejected. |
| `publish = 'insert, update, delete'` | TRUNCATE is excluded — semantics don't map to per-row CDC.                   |
| Never `FOR ALL TABLES`      | Risks streaming audit/migration tables — explicit table list only.                   |

The publication is **DBA-owned** by default: Walera assumes it exists at
startup and fails fast if missing when `wal.bootstrap.mode = verify`.

For dev/testbench Walera ships an opt-in auto-bootstrap path
(`wal.bootstrap.mode = auto`, default) — see §4a below.

### 4a. Optional: auto-bootstrap (dev / demo only)

Walera can create the publication (and optionally the single database
role) on its own. Three things still require operator action regardless of
the bootstrap mode — they cannot be changed from SQL:

| Setting                 | Why Walera can't fix it                              |
| ----------------------- | ---------------------------------------------------- |
| `wal_level = logical`   | `postgresql.conf` + server restart.                  |
| `max_replication_slots` | Same — restart required.                             |
| `max_wal_senders`       | Same — restart required.                             |

Walera **verifies all three** on the admin connection at startup and
fails fast with the offending value + the value it needs.

The bootstrap modes:

| `wal.bootstrap.mode` | Behavior on the publication                                                                                                                                       |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `auto` (default)     | Creates the publication if missing. If it exists and `bootstrap.tables` is set, verifies the table list and warns on mismatch. Never mutates an existing publication. |
| `verify`             | Fails fast if the publication is missing or empty. The original DBA-managed posture.                                                                              |
| `off`                | Skips the publication check entirely.                                                                                                                             |

```yaml
# config.yaml
wal:
  bootstrap:
    mode: auto
    tables:                       # schema-qualified; preferred over FOR ALL TABLES
      - public.orders
      - public.devices
    create_roles: true            # opt-in role provisioning
```

Equivalent env vars:

```sh
WALERA_WAL_BOOTSTRAP_MODE=auto
WALERA_WAL_BOOTSTRAP_TABLES=public.orders,public.devices
WALERA_WAL_BOOTSTRAP_CREATE_ROLES=true
```

When `bootstrap.tables` is non-empty and the publication is missing,
Walera issues:

```sql
CREATE PUBLICATION <name>
  FOR TABLE public.orders, public.devices
  WITH (publish = 'insert, update, delete');
```

When `bootstrap.tables` is empty (the default), Walera falls back to
`FOR ALL TABLES` for backward compatibility — but you should set the
list explicitly in any environment that streams real data.

`bootstrap.create_roles: true` makes Walera probe `pg_catalog.pg_roles`
for the username in `database.url`. Both derived DSNs share that single
username, so this provisions exactly **one** role: if the role does not
exist Walera runs `CREATE ROLE <user> WITH LOGIN REPLICATION PASSWORD
'<dsn-password>'`; the subsequent admin pass sees the role already exists
and skips. Net effect: one role with the `REPLICATION` attribute. Failures
are downgraded to warnings — the WAL or admin connection will surface the
real error on its own attempt.

**Security:**
- `create_roles: true` requires the connecting role to carry `CREATEROLE`
  or superuser, which is **higher** than the day-to-day posture documented
  in §2. Use it only in environments where that elevation is acceptable
  (dev, testbench, demos).
- Passwords are sourced from the DSN and never logged.
- For production, prefer `mode: verify` plus the DBA-owned role created
  out-of-band per §2.

---

## 5. No PgBouncer in the Replication Path

> **PgBouncer does not support the PostgreSQL replication protocol.**

There is a single DSN (`WALERA_DATABASE_URL`), and Walera uses it for the
replication protocol. It **must** therefore connect directly to PostgreSQL.
If it goes through PgBouncer you will see `unsupported startup parameter:
replication` or similar at startup.

---

## 6. OS Tuning (DEP-04)

These knobs matter on the **PostgreSQL host AND the Kubernetes node that
runs Walera**. At ~10 000 concurrent SSE subscribers, the kernel TCP
accept queue and per-process FD limit become the bottleneck before the Go
runtime does.

### File-descriptor limit

Walera holds one TCP FD per subscriber + a handful for PG connections,
metrics scraping, etc. With 10k subscribers, set the soft and hard FD
limits to **32768 or higher**.

```sh
# Per-shell or per-service via PAM:
ulimit -n 32768

# Persistent via /etc/security/limits.d/99-walera.conf:
*  soft  nofile  32768
*  hard  nofile  65536

# Persistent via systemd unit override:
sudo systemctl edit walera     # if running outside k8s
# Add:
#   [Service]
#   LimitNOFILE=32768
```

In Kubernetes, the container inherits the node's FD limit unless an
admission webhook explicitly restricts it. If you see
`WaleraFDPressure` firing in clusters where ulimit was supposedly raised,
verify with `kubectl exec walera-<pod> -- cat /proc/self/limits`.

### TCP accept-queue / SYN-backlog

```ini
# /etc/sysctl.d/99-walera.conf
net.core.somaxconn         = 4096
net.ipv4.tcp_max_syn_backlog = 4096
```

Apply with:

```sh
sudo sysctl --system
sysctl net.core.somaxconn          # verify
sysctl net.ipv4.tcp_max_syn_backlog
```

Kubernetes nodes inherit these from the host. The container's listen
backlog is `min(somaxconn, net.core.somaxconn-as-seen-from-the-container)`,
so the node-level setting is what matters.

### Confirming inside the running pod

```sh
kubectl exec -n walera deploy/walera -- cat /proc/sys/net/core/somaxconn
kubectl exec -n walera deploy/walera -- cat /proc/self/limits | grep -i 'open files'
```

---

## 7. Secrets Management (DEP-02)

> **Create-then-apply contract (SEC-06 / F-P2-09):** The `walera-secrets`
> Secret MUST exist in the cluster BEFORE `kubectl apply -k deploy/k8s/`
> is run. The bundle's `kustomization.yaml` deliberately does NOT
> include `secret.yaml` as a resource — that file is a template/
> reference for operator use only and contains no `stringData:` values.
> Walera's startup validation fails fast if the required `database_url` value
> is missing or empty.
>
> Use one of the three workflows below (§7a, §7b, §7c) to create the
> Secret first, then apply the bundle.

`deploy/k8s/secret.yaml` ships as a TEMPLATE / OPERATOR REFERENCE
only — its body is entirely commented out so `kubectl apply -f
deploy/k8s/secret.yaml` is a no-op. The three operator workflows
below remain the canonical Secret-creation paths.

### 7a. One-shot kubectl create

```sh
kubectl create secret generic walera-secrets \
  --namespace=walera \
  --from-literal=database_url='postgres://walera:...@pg:5432/app?sslmode=require'
```

Pros: simplest. Cons: not GitOps-friendly; rotation requires manual `kubectl
delete` + `create`.

### 7b. sealed-secrets (GitOps-friendly)

```sh
# Write a raw Secret YAML locally (NOT committed).
cat > /tmp/raw-secret.yaml <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: walera-secrets
  namespace: walera
type: Opaque
stringData:
  # Single Postgres DSN; the role MUST have the REPLICATION attribute.
  database_url: 'postgres://walera:...@pg:5432/app?sslmode=require'
EOF

# Seal it. The resulting sealed-secret.yaml IS safe to commit.
kubeseal -o yaml < /tmp/raw-secret.yaml > deploy/k8s/sealed-secret.yaml
rm /tmp/raw-secret.yaml
```

Then reference the sealed Secret from `kustomization.yaml` (replace
`secret.yaml` with `sealed-secret.yaml` in the `resources:` list).

### 7c. external-secrets-operator (Vault / AWS SM / GCP SM)

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: walera-secrets
  namespace: walera
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: ClusterSecretStore
  target:
    name: walera-secrets
    creationPolicy: Owner
  data:
    - secretKey: database_url
      remoteRef:
        key: secret/walera
        property: database_url
```

The ExternalSecret resource creates and refreshes the `walera-secrets`
Secret in the cluster, sourcing values from your Vault / cloud Secret
manager. See https://external-secrets.io/ for backend-specific config.

---

## 8. Troubleshooting

### Slot orphan recovery

A "stuck" replication slot (e.g., from a crashed prior Walera instance
that never cleaned up its temporary slot — should be impossible since the
slot is `TEMPORARY`, but operator error during dev can leave debris):

```sql
-- List inactive logical slots:
SELECT slot_name, plugin, slot_type, active, restart_lsn
FROM pg_replication_slots
WHERE active = false AND slot_type = 'logical';

-- Drop the orphan after confirming it's not in use by any other consumer:
SELECT pg_drop_replication_slot('walera_some-host_12345');
```

### Investigating WAL lag

```sql
-- Per-slot lag in bytes:
SELECT
    slot_name,
    active,
    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
FROM pg_replication_slots
ORDER BY lag_bytes DESC;
```

This is the same query Walera's lag sampler runs every 5s to populate
`walera_wal_lsn_lag_bytes`. If the Walera-reported lag and the DBA-side
lag diverge significantly, the sampler may be failing — check Walera logs
for `wal lag sample skipped` debug entries.

### Replication connection diagnostics

```sql
-- Active replication connections (one row per Walera instance):
SELECT
    application_name,
    client_addr,
    state,
    sent_lsn,
    write_lsn,
    flush_lsn,
    replay_lsn,
    sync_state
FROM pg_stat_replication;
```

A row missing for a Walera pod that claims to be `pg_connection_status=1`
indicates a network problem (replication conn dropped silently). Walera's
reconnect loop should fire within seconds; if not, check pod logs.

### Verifying `wal_level`

```sql
SHOW wal_level;            -- must be 'logical' (not 'replica' or 'minimal')
```

If `wal_level` is wrong, Walera fails fast at startup with
`replication slot creation requires wal_level=logical`. Fix by editing
`postgresql.conf` and restarting PostgreSQL.

### Verifying the publication

```sql
SELECT pubname, tablename, schemaname
FROM pg_publication_tables
WHERE pubname = 'cdc_sse_streamer'
ORDER BY schemaname, tablename;
```

A zero-row result means Walera will refuse to start with
`publication "cdc_sse_streamer" not found`.

---

## See Also

- `deploy/k8s/` — Kubernetes manifests (Deployment, Service, ConfigMap, Secret, ServiceMonitor).
- `deploy/prometheus/alerts.yaml` — PrometheusRule (8 alerts; see `deploy/prometheus/README.md`).
- `migrations/001_publication.sql.example` — publication template (DBA-owned).
- `spec/10-resources-and-deployment.md` — authoritative resource/deployment spec.
- `CLAUDE.md` — project constraints (`§Constraints`).
