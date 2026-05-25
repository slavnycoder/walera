# Walera Prometheus Alerts — Operator Guide

This directory ships the `PrometheusRule` manifest (`alerts.yaml`) that
implements the OBS-03 alert set (D4-14, D4-15). Eight rules cover the
P1/P2/P3 severities across the WAL, auth, subscriber, and Go-runtime
surface areas.

> No Grafana dashboard JSON ships in the MVP. The alert rules ARE the
> binding contract; dashboards are operator preference (D4-15).

---

## Installation

```sh
# Prerequisites: Prometheus Operator (kube-prometheus-stack or equivalent)
# is already installed in the cluster.

kubectl apply -f deploy/prometheus/alerts.yaml
```

The manifest creates a single `PrometheusRule` resource in the `monitoring`
namespace. The Prometheus Operator's controller picks it up automatically
via label selection (see below).

If your Prometheus Operator runs in a namespace other than `monitoring`,
edit `metadata.namespace` first.

---

## Required Label Customization (Pitfall G7)

**Without this step, alerts will not fire even after `kubectl apply` reports
success.** The Prometheus Operator uses `spec.ruleSelector` to discover
`PrometheusRule` resources, and the default selectors filter by a
`release:` label. Our manifest ships with `release: REPLACE_ME` as a
deliberate tripwire.

### Step 1 — discover your operator's expected label value

```sh
kubectl get prometheus -n monitoring -o yaml | grep -A 5 ruleSelector
```

Typical output:

```yaml
ruleSelector:
  matchLabels:
    release: kube-prometheus-stack
```

Common values seen in the wild:

| Operator deployment           | `release:` label             |
| ----------------------------- | ---------------------------- |
| `kube-prometheus-stack` helm  | `kube-prometheus-stack`      |
| `prometheus-community` helm   | `prometheus`                 |
| `kube-prometheus` manifests   | `kube-prometheus`            |
| Custom / homegrown            | Whatever your manifest sets  |

### Step 2 — run preflight to verify substitution

After you have chosen your `release:` value and applied it via `sed` (Step 3
below) or via a kustomize/helm overlay, run the preflight guard BEFORE
`kubectl apply`:

```sh
deploy/scripts/preflight.sh
```

Exit code `0` means no `REPLACE_ME` marker survived under `deploy/`. Exit
code `1` lists every offending `path:lineno` on stderr — fix those and
re-run before continuing to `kubectl apply`. The preflight is the
deterministic guard that Walera ships in place of a templating layer; it
covers `deploy/prometheus/alerts.yaml` and `deploy/k8s/servicemonitor.yaml`
in one pass and is safe to wire into CI.

### Step 3 — apply the substitution

```sh
sed -i 's/release: REPLACE_ME/release: kube-prometheus-stack/' \
  deploy/prometheus/alerts.yaml

kubectl apply -f deploy/prometheus/alerts.yaml
```

### Step 4 — confirm the rules are loaded

```sh
# Either via the Prometheus web UI (/rules) or:
kubectl exec -n monitoring sts/prometheus-kube-prometheus-stack-prometheus -c prometheus -- \
  wget -qO- localhost:9090/api/v1/rules | jq '.data.groups[].name'
```

You should see `walera.wal`, `walera.auth`, `walera.subscribers`, `walera.process`.

The same `release:` workaround applies to `deploy/k8s/servicemonitor.yaml`
(same Pitfall G7 surface) — apply the equivalent sed there if your operator's
`serviceMonitorSelector` also filters by `release:`. `deploy/scripts/preflight.sh`
(Step 2 above) walks the entire `deploy/` tree and catches a missed substitution
in `servicemonitor.yaml` exactly the same way it catches one in `alerts.yaml`.

---

## Per-Alert Reference

| Alert                                                | Severity | Intent                                                                | Mitigation Steps                                                                                                                          | Dashboard Panel (future) |
| ---------------------------------------------------- | -------- | --------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| `WaleraWALLagHigh`                                   | P1       | WAL replay falling behind during live session                         | Investigate router/writer throughput; check `walera_subscriber_queue_depth`; look for a slow-consumer storm                                | "WAL Lag (bytes)"        |
| `WaleraPGDisconnected`                               | P1       | Replication connection to PostgreSQL lost                             | Reconnect loop auto-retries; k8s liveness evicts after ~6s if `/healthz` stays 503; check PG availability, network policy, slot limits      | "PG Connection Status"   |
| `WaleraAuthBreakerOpen`                              | P1       | Auth backend breaker tripped (bounded fail-open active)               | Up to ~2min stale-permissions window for existing subscribers. Investigate auth backend logs / latency / 5xx rate                          | "Auth Breaker State"     |
| `WaleraAuthBreakerStaleSubscribersOutsideBreaker`    | P1       | Stale subscribers exist but breaker is closed (refresh logic bug)     | Inspect `walera_auth_refresh_total{result}`; check Walera logs for refresh exceptions                                                       | "Stale Subscribers"      |
| `WaleraSlowConsumerRate`                             | P2       | Slow-consumer disconnect rate > 6/min sustained                       | Investigate downstream client backpressure; consider raising per-subscriber buffer; check network path to slow clients                     | "Disconnects by Reason"  |
| `WaleraMultiRootDrops`                               | P2       | Auth backend allowing same tx visible under multiple roots            | Coordinate with auth-backend owner; same tx touching multiple roots for one subscriber violates the spec's root discipline                | "Tx Drops by Reason"     |
| `WaleraFDPressure`                                   | P2       | Open FDs > 80% of soft limit                                          | Inspect for socket leaks or undersized ulimit; cross-check with `walera_subscribers_active`; raise k8s `securityContext` / node ulimit     | "FD Usage"               |
| `WaleraHeapGrowth`                                   | P3       | Heap allocated grew > 100 MiB over 30m (suspected leak)               | Capture `/debug/pprof/heap` if pprof endpoint enabled; correlate with `walera_subscribers_active` and `walera_routing_index_size` trends   | "Heap Allocated (delta)" |

---

## Grafana Dashboards

Not shipped in the MVP (D4-15). The alert expressions above ARE the binding
contract for SRE alerting. Each alert has a "Dashboard Panel (future)" hint
intended for operators authoring their own dashboards — naming alignment
makes future runbook references stable.

A reasonable starter dashboard would include one row per alert:
- single-stat (current value)
- 1h time-series of the expr (without the threshold)
- threshold line annotated

---

## Verifying alert wiring end-to-end

For a smoke test of the WAL-lag alert path:

```sh
# 1. Stop the upstream postgres for >1 minute (e.g., scale to 0 if managed by you):
kubectl scale -n walera deploy/postgres --replicas=0   # only if PG is in-cluster

# 2. Wait 70s. WaleraPGDisconnected should fire (severity P1).

# 3. Bring PG back:
kubectl scale -n walera deploy/postgres --replicas=1

# 4. Within ~30s the alert should resolve.
```

For managed PostgreSQL (RDS, CloudSQL, Supabase), use a security-group /
network-policy rule instead of scaling.
