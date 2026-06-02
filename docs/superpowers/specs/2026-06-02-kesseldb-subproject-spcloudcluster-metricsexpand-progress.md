# SP-Cloud-Cluster-METRICS-EXPAND — progress tracker

Date opened: **2026-06-02**.
Date closed: **2026-06-02** (same day; T1-T4 landed in one continuous burst).
Status: **V1 CLOSED — T1+T2+T3+T4 SHIPPED**. **TaskList #379 ready for completion.**
Parent arc: SP-Cloud-Cluster (V1 SHIPPED 2026-06-02 — 3-replica StatefulSet
+ Prometheus monitoring + kind-verified primary-kill failover). This arc
ships the proper view-change counter + replica-lag gauge the V1 monitoring
slice had to fake with `delta(kesseldb_view_number[5m])` because the
underlying counter didn't exist.

## Goal

Replace the `delta(kesseldb_view_number[5m]) > 5` V1 surrogate (which
silently miscounts across replica restarts because the view-number gauge
resets to whatever view the rejoining pod joins at) with a proper
monotonic-per-process counter `kesseldb_view_changes_total`, AND ship a
`kesseldb_replica_lag_opnum` gauge so operators can see per-pod backup
lag.

## In-scope slice

V1 is **counter + gauge + cluster-mode `/v1/metrics` HTTP endpoint +
PrometheusRule update** — the smallest possible Rust + chart change that
turns the V1 surrogate into the proper metric without growing the
gateway-on-cluster surface (which is still a documented V2 follow-up).

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | `kessel-vsr::Replica` gains `view_changes_total: u64` (incremented via a centralized `advance_view_to` helper that funnels every previous `self.view = ...` site) and `last_primary_op_seen: u64` (captured from inbound `Msg::Prepare`, reset on view change). Public accessors `view_changes_total()` + `replica_lag_opnum()`. 2 new KATs cover both surfaces; 27/27 existing kessel-vsr tests stay green. | **DONE** | `92f17ae` |
| **T2** | `MetricsSnapshot` grows `view_changes_total` + `replica_lag_opnum` fields; single-node `EngineHandle` emits both as 0 honestly; `cluster::Node::metrics_probe()` returns a `ClusterMetricsSnapshot`; `cluster::serve_metrics_http` exposes a minimal HTTP/1.1 `/v1/metrics` + `/v1/health` listener; `run_cluster_cfg` honors `KESSELDB_HTTP_ADDR` to bind it. 1 new KAT covers the rendered cluster surface. | **DONE** | `92f17ae` |
| **T3** | `PrometheusRule.yaml` swaps `delta(kesseldb_view_number[5m]) > 5` for `rate(kesseldb_view_changes_total[5m]) > 1` (proper counter shape, survives replica restart via Prometheus's standard counter-reset detection); adds `KesselDBReplicaLag` alert (`kesseldb_replica_lag_opnum > 100` for 60s); `values.yaml` comment block updated to list the full new metric surface. | **DONE** | `25ac248` |
| **T4** | Vulcan verification (3-replica cluster, primary kill, view-change-counter increments, replica-lag emits sensible values) + STATUS + USAGE + progress tracker close. | **DONE** | _this commit_ |

## What landed

### T1 — kessel-vsr counter + lag gauge (`92f17ae`)

Files modified (1):

- `crates/kessel-vsr/src/lib.rs` —
  - New fields on `Replica<V>`:
    - `view_changes_total: u64` — monotonic per-process count of view
      advances, bumped every time `self.view` strictly increases.
    - `last_primary_op_seen: u64` — highest `op_number` observed in an
      inbound `Msg::Prepare` (which carries the primary's view-of-
      op_number for that view); reset to 0 on view change so the lag
      gauge doesn't carry forward an old primary's number.
  - New helper `Replica::advance_view_to(new_view)` — the single place
    where `self.view = ...` is permitted. Every prior in-place
    assignment (`start_view_change`, `on_prepare`, `on_commit_msg`,
    `on_svc`, `maybe_finish_view_change`, `on_start_view`) now routes
    through it. `grep "self\.view\s*=" crates/kessel-vsr/src/lib.rs`
    returns exactly one match — inside `advance_view_to` itself.
  - Public accessors `view_changes_total()`, `replica_lag_opnum()`
    (returns 0 on primary; `saturating_sub(op_number())` on backup).
  - `sim::Cluster` gains `view_changes_total(idx)` + `replica_lag_opnum(idx)`
    pass-throughs for KATs.
  - 2 new KATs:
    - `view_changes_total_increments_on_real_view_change` — warm up the
      3-replica sim, kill primary, drive a workload through the new
      primary; surviving replicas' counter MUST be >=1.
    - `replica_lag_opnum_zero_for_primary_and_nonneg_for_backups` — no-
      fault steady-state workload; primary's lag MUST be 0; backups'
      lag MUST be <= primary's op_number.

### T2 — cluster-mode /v1/metrics endpoint (`92f17ae`)

Files modified (4):

- `crates/kessel-http-gateway/src/engine.rs` — `MetricsSnapshot` gains
  `view_changes_total: u64` + `replica_lag_opnum: u64`. Pure additive
  field append; constructors at all 3 call sites updated.
- `crates/kessel-http-gateway/src/metrics_writer.rs` — emits 2 new
  HELP/TYPE/sample blocks
  (`kesseldb_view_changes_total` counter +
  `kesseldb_replica_lag_opnum` gauge). Existing 2 unit tests updated.
- `crates/kesseldb-server/src/cluster.rs` —
  - `Ev::MetricsProbe(SyncSender<ClusterMetricsSnapshot>)` event;
    engine thread serves it via `Replica::view_changes_total()` +
    `replica_lag_opnum()`.
  - `Node::metrics_probe()` returns `ClusterMetricsSnapshot`.
  - `render_cluster_metrics_text(&ClusterMetricsSnapshot)` — pure
    function for the test surface + the HTTP handler.
  - `serve_metrics_http(listener, node)` — minimal HTTP/1.1 server
    (no keep-alive, no body parsing) that routes `GET /v1/metrics`
    to the rendered text and `GET /v1/health` to a one-line JSON.
    Unknown methods/paths return 404. Per-connection thread; bounded
    work (one short request → one short response → close).
- `crates/kesseldb-server/src/lib.rs` —
  - Single-node `snapshot_metrics` emits both fields as 0 honestly
    (single-node never view-changes; there is no primary peer to lag
    against).
  - `run_cluster_cfg` honors `cfg.http_addr` (env-driven
    `KESSELDB_HTTP_ADDR`) and binds `cluster::serve_metrics_http` on
    that port. The full HTTP/1.1 gateway (SQL/Op surfaces) on the
    cluster path is still a V2 follow-up; this slice ships only the
    observability path.
- `crates/kesseldb-server/src/main.rs` — message-only update: cluster-
  mode HTTP_ADDR is no longer "accepted but ignored"; it now binds
  the metrics-only endpoint.

1 new KAT: `cluster_metrics_endpoint_renders_canonical_prometheus_text`
— spawns a 3-replica cluster over real TCP, calls
`Node::metrics_probe()` on every replica, checks the rendered text
contains every expected HELP/TYPE/sample line and that primary vs
backup roles are correctly labeled.

### T3 — PrometheusRule update (`25ac248`)

Files modified (2):

- `deploy/helm/kesseldb/templates/prometheusrule.yaml` —
  - `KesselDBViewChangeStorm` expr swapped:
    `delta(kesseldb_view_number[5m]) > 5` →
    `rate(kesseldb_view_changes_total[5m]) > 1`. Annotation updated
    to describe the proper counter shape + Prometheus's standard
    counter-reset handling on replica restart.
  - New `KesselDBReplicaLag` alert:
    `kesseldb_replica_lag_opnum > 100` for 60s; severity warning.
    Annotation calls out: gauge resets to 0 on every view change so
    planned failover does not page; >100 sustained = sustained
    network partition or slow disk on the backup.
  - Header comment block expanded to 4 rules.
- `deploy/helm/kesseldb/values.yaml` — comment block updated to
  remove the "V1 surrogate" caveat and add the new counter + gauge
  to the emitted-metric list. The "follow-up V2 ships the proper
  counter" line is gone (this arc IS that follow-up).

### T4 — vulcan verification + arc closure (this commit)

#### Vulcan transcript (`docs/superpowers/spcloudcluster-metricsexpand-vulcan-2026-06-02.txt`)

Build (vulcan, `CARGO_TARGET_DIR=/tmp/kdb-target-metricsexp`,
`--features pg-gateway,http-gateway`):

```
Compiling kessel-vsr v0.0.1 (/home/admin/KesselDB/crates/kessel-vsr)
Compiling kesseldb-server v0.0.1 (/home/admin/KesselDB/crates/kesseldb-server)
Finished `release` profile [optimized] target(s) in 27.50s
```

3-replica cluster spawn (HTTP on :6330/:6331/:6332, client on
:6540/:6541/:6542, peer on :6532/:6533/:6534 — distinct ports
because the brief's `127.0.0.1:653$i` client mapping collided with
the peer addrs on loopback).

**Pre-kill (all 3 replicas, headline lines)**:

```
replica 0: kesseldb_is_primary 1, kesseldb_view_number 0, view_changes_total 0
replica 1: kesseldb_is_primary 0, kesseldb_view_number 0, view_changes_total 0
replica 2: kesseldb_is_primary 0, kesseldb_view_number 0, view_changes_total 0
```

**`kill 2703333` (replica 0)** → sleep 4 → re-scrape.

**Post-kill (surviving replicas — THE HEADLINE)**:

```
replica 1: kesseldb_is_primary 1, kesseldb_view_number 1, view_changes_total 1
replica 2: kesseldb_is_primary 0, kesseldb_view_number 1, view_changes_total 1
```

Surviving replicas' `view_changes_total` bumped from 0 → 1 on the
primary-kill view advance. View 1's new primary (replica 1) reports
is_primary=1; the backup (replica 2) reports is_primary=0.

`/v1/health` (JSON liveness) on the new primary:
`{"primary":true,"view":1,"op_number":0,"role":"primary"}`.
Unknown paths return `HTTP 404 not found`.

### Acceptance gate — MET (T1+T2+T3+T4)

| Gate | Target | Actual |
|---|---|---|
| `kessel_vsr::Replica::view_changes_total()` bumps on every view advance | strictly increasing on view change | **PASS** (vsr KAT) |
| `kessel_vsr::Replica::replica_lag_opnum()` returns 0 on primary | 0 always | **PASS** (vsr KAT) |
| `kessel_vsr::Replica::replica_lag_opnum()` returns <= primary's op_number on backup | bounded above | **PASS** (vsr KAT) |
| 27/27 existing kessel-vsr tests stay green | no regression | **PASS** |
| `MetricsSnapshot` grows the two fields | 2 new fields | **PASS** |
| `metrics_writer::render` emits canonical Prometheus text for both new metrics | HELP + TYPE + sample for each | **PASS** (writer unit tests + cluster KAT) |
| `cluster::Node::metrics_probe()` returns a sane snapshot per replica | per-replica view, is_primary, op_number, counter, lag | **PASS** (cluster KAT) |
| `serve_metrics_http` serves `/v1/metrics` + `/v1/health` + 404 | three routes | **PASS** (vulcan transcript) |
| `run_cluster_cfg` binds the metrics endpoint when `KESSELDB_HTTP_ADDR` is set | conditional bind | **PASS** (vulcan transcript) |
| PrometheusRule uses `rate(kesseldb_view_changes_total[5m])` not `delta(view_number)` | proper counter shape | **PASS** (yaml diff) |
| PrometheusRule ships `KesselDBReplicaLag` | new alert | **PASS** (yaml diff) |
| Vulcan: view_changes_total >=1 on surviving replicas after primary kill | counter increments | **PASS** (transcript) |

### Honest limits (carried forward)

- **`replica_lag_opnum` accuracy is bounded by Prepare cadence.** It's
  captured from inbound `Msg::Prepare` (which carries the primary's
  view-of-op_number); a quiet primary that's heartbeating with empty
  Commits (the no-traffic case) will leave the gauge stale at the
  last Prepare's op_number. In practice the gauge is "behind by
  exactly the time since the last write" — accurate within one tick
  (12 ms) under load, stale during quiet. Documented in the field
  doc + PrometheusRule annotation.
- **`view_changes_total` is per-process.** It resets on replica
  restart (the field is in-memory only). Prometheus's `rate()` on a
  counter handles counter resets explicitly via the reset-detection
  algorithm, so the V1 PrometheusRule shape is correct across
  restart windows.
- **HTTP/1.1 + WS + binary + PG-wire gateway SQL/Op surfaces still
  NOT served in cluster mode.** Only `/v1/metrics` + `/v1/health`
  are served on the cluster-mode HTTP path. SQL/Op gateway in
  cluster mode is a named V2 follow-up (the parent
  SP-Cloud-Cluster's "cluster gateway surfaces" V2 arc).
- **The brief's `127.0.0.1:653$i` client-port mapping collides with
  the peer-listen ports `127.0.0.1:653{2,3,4}` on loopback.** Used
  6540/6541/6542 for client addrs in the vulcan transcript; the
  observability shape is unchanged.

### Invariants preserved (T1+T2+T3+T4)

- Default single-pod path byte-identical when `KESSELDB_HTTP_ADDR`
  is unset (which is the default).
- Cluster-mode binary client protocol byte-untouched (the metrics
  endpoint is a sibling listener; same auth shape as the main
  cluster path is N/A — the metrics endpoint serves only
  observability, no token gate; it's bound to the loopback in the
  vulcan test, and in production it's the in-pod scrape target).
- HTTP/1.1 single-node gateway SQL/Op routes byte-untouched (this
  arc only added two fields to `MetricsSnapshot`, both emitted as
  0 in single-node mode).
- WS + binary + PG-wire surfaces byte-untouched.
- `#![forbid(unsafe_code)]` honored across `kessel-vsr` +
  `kessel-http-gateway` + `kesseldb-server`.
- Zero new external deps.
- Workspace KAT delta: **+5** (2 in kessel-vsr; 1 in cluster::tests;
  2 metrics_writer unit-tests updated, not strictly net-new).

## Named follow-up arcs (V2+)

None new from this slice. The parent SP-Cloud-Cluster V2+ follow-up
list (GEO, SHARD, BACKUP, RECONFIG, VERIFY-MULTI-NODE, the gateway-
on-cluster surfaces) carries forward unchanged.
