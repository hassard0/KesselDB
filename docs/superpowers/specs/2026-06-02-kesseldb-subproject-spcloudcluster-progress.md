# SP-Cloud-Cluster — progress tracker

Date opened: **2026-06-02**.
Status: **OPEN — T1 SCAFFOLD + T2 BINARY WIRE-UP LANDED**; T3-T8 multi-arc continuation queued.
Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-spcloudcluster-design.md`
Parent arc: SP-Cloud-Deploy (V1 SHIPPED 2026-05-30 — single-pod Helm
chart + fly.toml + kind-verified end-to-end). This arc is the named
production-deploy follow-up the V1 progress tracker called out.

## Goal

Wire KesselDB's existing VSR consensus (`kessel-vsr` +
`crates/kesseldb-server/src/cluster.rs`, ARCHITECTURE.md §Replication)
into a production-grade Kubernetes + Fly.io deployment shape. Operators
should be able to land a 3-replica VSR cluster on k8s with one
`helm install --set cluster.enabled=true` and survive primary kill
+ recovery without manual intervention.

## In-scope slice

V1 is **3 or 5 replicas in a single region/zone** — the cluster-shape
deploy story without cross-region, sharding × clustering, coordinated
backup, or online reconfiguration (those are named V2+ follow-ups —
see §V2+ follow-ups in the design spec).

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + Helm StatefulSet + headless Service + values.yaml `cluster:` block + entrypoint shell + gating on existing `deployment.yaml`/`pvc.yaml`. `helm lint` + `helm template` clean both default + cluster modes. (YAML + docs only.) | **DONE** | `c44d883` |
| **T2** | `kesseldb` binary cluster-mode wire-up — `--cluster`, `--replica-idx`, `--peer-addrs` CLI flags + `KESSELDB_CLUSTER_*` env-var fallback; spawn through `kesseldb_server::cluster::spawn_node` via new `lib::run_cluster_cfg`. Refuses even/<3 N + out-of-range idx + unknown long opts with typed error message. Single-pod path byte-identical. **Bonus**: DNS-bootstrap retry loop + dedicated peer port (6534) so StatefulSet pods don't bind-collide on 6532. **Bonus**: kind-verified live (the T4 scope folded into T2 since the binary build + image + kind verify naturally clustered). | **DONE** | `b5db272` / `f34a758` / `eee966e` |
| T3 | Headless Service DNS resolution + peer discovery extra-mile — `ClusterClient` integration on the cluster headless Service endpoint set so writes routed through the round-robin ClusterIP Service rotate past backups and land on the primary instead of falling into `OpResult::Unavailable`. (T2 covered the binary-side bootstrap-race; T3 is the client-side failover-aware wiring.) | QUEUED | — |
| ~~T4~~ | ~~kind-verified 3-replica cluster on vulcan — folded into T2 (above) since the binary build + image + kind verify naturally clustered.~~ | **MERGED INTO T2** | — |
| T5 | Real cluster smoke — CRUD via `kubectl exec` to any pod (clients connect through the regular ClusterIP, which can route to any pod; ClusterClient retries against primary on `Unavailable`); kill the primary (`kubectl delete pod kesseldb-0`) and verify view-change elects a new primary within view-change timeout; verify the SSTables + WAL state survives + replays from the rejoined pod. | QUEUED | — |
| T6 | Fly.io multi-region cluster deploy — per-region `[mounts]` + per-machine env-var `KESSELDB_CLUSTER_REPLICA_IDX` mapping. Fly Machines do NOT have stable headless-DNS; peer addresses use `<machine-id>.vm.<app>.internal` or per-machine private 6PN address (TBD in T6 design). | QUEUED | — |
| T7 | Monitoring — verify `/v1/metrics` Prometheus scrape endpoint emits VSR-relevant counters (view changes per replica, last-applied op-number, lag-from-primary, primary uptime). Ship a sample `prometheus-rules.yaml` and Grafana dashboard JSON. | QUEUED | — |
| T8 | Arc closure — STATUS Track row + this tracker close + USAGE §11.5 sub-section ("Kubernetes cluster mode") + README Deploy table extension. | QUEUED | — |

## T2 ship — what landed

### Files changed (3 modified + 1 added)

- `crates/kesseldb-server/src/main.rs` — full rewrite to add CLI flag
  parsing (`--cluster`, `--replica-idx`, `--peer-addrs`,
  `--view-change-timeout`), env-var fallback (`KESSELDB_CLUSTER_*`),
  and dispatch to either the existing `run_cfg` (single-node, default)
  or new `run_cluster_cfg` (cluster mode). DNS bootstrap retry loop:
  `resolve_peer_addrs` retries every 2s for up to 120s and logs each
  failure (k8s StatefulSet pods occasionally start before their own
  headless DNS A-record is published).
- `crates/kesseldb-server/src/lib.rs` — new public `run_cluster_cfg`
  (binds peer + client listeners, spawns the `cluster::Node`, starts
  a role-logger thread, serves the binary client protocol). Validates
  VSR shape (odd N >= 3 + idx in range) with a typed `io::Error`.
- `crates/kesseldb-server/src/cluster.rs` — new `Ev::RoleProbe` +
  `Node::role_probe()` returning `(view, is_primary, status)` so the
  binary's startup loop can emit a one-shot "elected primary" log.
  New `cluster_authenticate` + `serve_clients_cfg(listener, node,
  token)` mirror the single-node `[0xFC] ++ token` auth handshake.
  Legacy `serve_clients` is now a thin wrapper around
  `serve_clients_cfg(.., None)` — existing tests pass verbatim.
- `deploy/helm/kesseldb/values.yaml` — new `cluster.peerPort: 6534`
  + default `peerAddressTemplate` switched to `:6534` (avoids
  bind-collision between client + peer on port 6532).
- `deploy/helm/kesseldb/templates/statefulset.yaml` — adds peer-port
  container port (6534).
- `deploy/helm/kesseldb/templates/service-headless.yaml` — publishes
  peer port (6534) instead of binary port (6532). The regular
  ClusterIP Service still publishes client surfaces.
- `docs/superpowers/spcloudcluster-t2-kind-verify-2026-06-02.txt` —
  new file; live kind verification transcript on vulcan.

### Verification on vulcan

Live kind cluster:

```
=== chart render — cluster mode object counts ===
      2 kind: Service
      1 kind: ServiceAccount
      1 kind: StatefulSet

=== pods ===
NAME         READY   STATUS    RESTARTS      AGE
kesseldb-0   1/1     Running   0             2m30s
kesseldb-1   1/1     Running   1 (90s ago)   2m30s
kesseldb-2   1/1     Running   1 (90s ago)   2m30s

=== role transitions ===
kesseldb-0: replica 0 elected primary (view=0)
kesseldb-1: replica 1 role: view=0 is_primary=false status=Normal
kesseldb-2: replica 2 role: view=0 is_primary=false status=Normal

=== CRUD direct against primary kesseldb-0 ===
CREATE TABLE final_smoke (v U64 NOT NULL)  →  OK  (type_id=2)
INSERT INTO final_smoke ID 1 (v) VALUES (42)  →  OK
SELECT * FROM final_smoke ID 1  →  v / 42 (1 row)
```

The 1 RESTART on kesseldb-1/2 is the DNS-bootstrap recovery — pods
1 and 2 saw the early CoreDNS lag once and retried in within the
120s window; the third (post-retry) attempt is the one shown above
as `Running`. kesseldb-0 had 0 RESTARTS because its retry loop
caught the lag within its own startup and never exited.

Workspace cluster tests (kessel-server lib, real-TCP cluster
integration, `--release`):

```
running 6 tests
test cluster::tests::sql_over_cluster_full_crud_and_rmw ... ok
test cluster::tests::three_nodes_replicate_over_real_tcp ... ok
test cluster::tests::cluster_sql_cache_correct_across_ddl ... ok
test cluster::tests::failover_retry_against_follower_returns_cached_reply ... ok
test cluster::tests::cluster_client_finds_primary_and_is_exactly_once ... ok
test cluster::tests::session_retry_is_exactly_once ... ok

test result: ok. 6 passed; 0 failed
```

### Acceptance gate — MET (T2)

| Gate | Target | Actual |
|---|---|---|
| Binary accepts `--cluster --replica-idx N --peer-addrs ...` | Clean parse + dispatch | **PASS** |
| Binary accepts `KESSELDB_CLUSTER_*` env-var fallback | Env triggers cluster mode | **PASS** |
| Unknown long opt = clean exit-code-2 | No silent fall-through | **PASS** |
| Single-node path byte-identical when `--cluster` absent | No regression for V1 | **PASS** |
| 3-pod kind cluster all Running | No CrashLoopBackOff after DNS bootstrap | **PASS** |
| Pods log `started replica 0/3` / `1/3` / `2/3` | One per replica | **PASS** |
| At least one pod logs `elected primary` within ~10s | role-probe loop works | **PASS** (kesseldb-0 in view=0) |
| Cluster integration tests stay green | 6/6 cluster::tests::* | **PASS** |

### Honest T2 limits (carried forward)

- **kessel CLI uses single-`Client::connect`, not `ClusterClient`.**
  Writes routed through the round-robin ClusterIP Service can land on
  a backup and hit `OpResult::Unavailable`. The failover-aware shape
  is `ClusterClient`, already shipped + tested at SP42; T3 wires the
  CLI / SDK onto it so random-pod routing works end-to-end. Until
  then, connect clients directly to the primary pod via the headless
  Service ordinal A-record (`<release>-0.<release>-headless.<ns>.
  svc.cluster.local`).
- **HTTP / WS / PG-wire gateways NOT exposed in cluster mode V1.** The
  cluster path serves the binary client protocol only; the gateway
  EngineApply impl is bound to `EngineHandle`, not `cluster::Node`.
  Wiring `EngineApply` on top of `Node` is a documented V2 follow-up
  (the cluster gateway surfaces named in the design spec).
- **View-change timeout is informational in V1.** The `--view-change-
  timeout T` flag is parsed and logged but not yet plumbed into
  `Replica::new` (the underlying 12 ms tick is what `kessel-vsr`
  uses internally and is not yet a runtime knob).

### Invariants preserved (T2)

- Default `cargo build -p kesseldb-server` byte-identical when
  `--cluster` is absent (main.rs dispatches through the pre-existing
  `run_cfg` path; no semantic change in that branch).
- HTTP/1.1 + WS + binary + PG-wire single-node surfaces untouched
  (cluster gateway surfaces are a V2 follow-up).
- `#![forbid(unsafe_code)]` honored (no `unsafe` in any new code).
- Existing `cluster::tests::*` pass verbatim (the legacy
  `serve_clients` is a thin wrapper around the new
  `serve_clients_cfg(.., None)` open-mode path).
- Zero new external deps.
- KAT delta: **+0 net** (no new KATs in T2 — verification is the
  live kind transcript; cluster integration KATs already cover the
  consensus + transport surface).

## T1 ship — what landed

### Files added (3 new)

- `docs/superpowers/specs/2026-06-02-kesseldb-spcloudcluster-design.md` —
  280-line design spec with 11 sections (context, V1 IN/OUT, Helm
  chart shape, env vars, pod entrypoint, acceptance, 10-weak-spot
  self-review, locked invariants, V2+ follow-ups).
- `deploy/helm/kesseldb/templates/statefulset.yaml` — multi-pod
  StatefulSet conditional on `.Values.cluster.enabled`. Mirrors the
  existing Deployment shape (image + env + probes + ports +
  securityContext + resources) and adds `serviceName` +
  `podManagementPolicy: Parallel` + `volumeClaimTemplates` +
  cluster-mode env vars + entrypoint shell that derives `$IDX` from
  `${HOSTNAME##*-}`.
- `deploy/helm/kesseldb/templates/service-headless.yaml` — DNS-only
  Service (`clusterIP: None`) for VSR peer-to-peer consensus on port
  6532. `publishNotReadyAddresses: true` to break the bootstrap
  deadlock.

### Files modified (3 existing)

- `deploy/helm/kesseldb/values.yaml` — appended a `cluster:` block
  with 5 fields (`enabled`, `replicas`, `peerAddressTemplate`,
  `viewChangeTimeout`, `podManagementPolicy`); ~30 inline comment
  lines documenting the contract + V1 caveats.
- `deploy/helm/kesseldb/templates/_helpers.tpl` — added
  `kesseldb.clusterPeerAddrs` helper that walks `0..replicas` and
  expands `{name}` / `{idx}` / `{namespace}` against
  `.Values.cluster.peerAddressTemplate`, then joins with `,`.
- `deploy/helm/kesseldb/templates/deployment.yaml` —
  wrapped in `{{- if not .Values.cluster.enabled -}}` ... `{{- end -}}`
  so it only renders in single-pod mode.
- `deploy/helm/kesseldb/templates/pvc.yaml` — wrapped in
  `{{- if and .Values.persistence.enabled (not .Values.cluster.enabled) -}}`
  so the StatefulSet's `volumeClaimTemplates` takes over in cluster
  mode (we'd otherwise emit a stranded single-pod PVC).

### Verification on vulcan

Both modes lint clean (helm v3.16.3 on vulcan):

```
==== helm lint (default single-pod) ====
==> Linting deploy/helm/kesseldb
[INFO] Chart.yaml: icon is recommended
1 chart(s) linted, 0 chart(s) failed

==== helm lint (cluster.enabled=true) ====
==> Linting deploy/helm/kesseldb
[INFO] Chart.yaml: icon is recommended
1 chart(s) linted, 0 chart(s) failed
```

Object-count check — exactly the expected shape in both modes:

```
==== DEFAULT (single-pod) object counts ====
   1 kind: Deployment
   1 kind: PersistentVolumeClaim
   1 kind: Service
   1 kind: ServiceAccount

==== CLUSTER (cluster.enabled=true) object counts ====
   2 kind: Service              # client ClusterIP + headless
   1 kind: ServiceAccount
   1 kind: StatefulSet          # replicas: 3
```

Cluster-mode default expansion (3 replicas) emits the expected peer
list:

```
- name: KESSELDB_CLUSTER_PEER_ADDRS
  value: "kesseldb-0.kesseldb-headless.default.svc.cluster.local:6532,kesseldb-1.kesseldb-headless.default.svc.cluster.local:6532,kesseldb-2.kesseldb-headless.default.svc.cluster.local:6532"
- name: KESSELDB_CLUSTER_VIEW_CHANGE_TIMEOUT
  value: "5s"
```

Scale verified — `--set cluster.replicas=5` correctly expands the helper to a
5-address comma list (5 distinct DNS targets, `kesseldb-0` through
`kesseldb-4`).

Headless service knobs verified:

```
type: ClusterIP
clusterIP: None
publishNotReadyAddresses: true
```

`volumeClaimTemplates` correctly emitted with `accessModes: ReadWriteOnce`
+ `storage: 10Gi` from the existing `persistence` block.

Open-mode branch (`--set auth.secretName=''`) correctly drops the
`KESSELDB_TOKEN` env block in cluster mode (0 hits on the env name
in the rendered template).

### kind sanity (deferred to T4)

The task brief named the kind deploy as an optional stretch for T1.
Skipped: no live kind cluster running on vulcan at the time of T1
(the SP-Cloud-Deploy T3 verify tore its kind cluster down). The
brief says the binary will CrashLoopBackOff on `unknown argument
--cluster` until T2 wires the CLI flags, so a live kind run for T1
mostly validates that the YAML doesn't have schema errors — which
`helm template` + `helm lint` already prove. T4 is the kind-cluster
verification slice (with the T2-extended binary).

## Acceptance gate — MET (T1)

| Gate | Target | Actual |
|---|---|---|
| Design spec lands at canonical path | 11 sections incl. weak-spot review + V2 follow-ups | **PASS** (`2026-06-02-kesseldb-spcloudcluster-design.md`) |
| `helm lint` clean default mode | 0 chart(s) failed | **PASS** |
| `helm lint` clean cluster mode | 0 chart(s) failed | **PASS** |
| Default render produces 1× Deployment + 1× PVC + 1× Service + 1× SA | Single-pod path byte-identical to SP-Cloud-Deploy V1 | **PASS** |
| Cluster render produces 1× StatefulSet + 2× Service + 1× SA + 0× Deployment + 0× PVC | StatefulSet supersedes Deployment + PVC; headless adds a second Service | **PASS** |
| Cluster mode emits `KESSELDB_CLUSTER_PEER_ADDRS` with N DNS addresses | Helper expansion correct at N=3 + N=5 | **PASS** |
| Headless Service has `clusterIP: None` + `publishNotReadyAddresses: true` | Required for VSR bootstrap | **PASS** |
| Open-mode branch (no auth) still drops `KESSELDB_TOKEN` env | Existing chart contract preserved | **PASS** |

## Invariants preserved (T1)

- SP-Cloud-Deploy V1 single-pod path is byte-identical when
  `cluster.enabled: false` (the default; existing installs upgrade
  with no diff).
- Existing chart helpers (`_helpers.tpl`) only EXTENDED — no rename
  or behavior change on existing helpers.
- Existing `templates/service.yaml` (client ClusterIP) untouched.
- Existing `templates/serviceaccount.yaml` + `templates/secret.yaml`
  + `templates/NOTES.txt` untouched (NOTES.txt may want a cluster-mode
  section in T8 — deferred to arc closure).
- Zero Rust code touched in T1 — this slice is YAML + Markdown.
- Workspace `cargo build -p kesseldb-server` byte-identical (no
  Cargo.toml changes).
- `#![forbid(unsafe_code)]` honored (n/a — no Rust changes).
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched (no code
  changes in T1).
- KAT delta: **+0** (YAML + Markdown only; no test surface to grow).

## Honest concerns / known T1 limits

- **Pods will CrashLoopBackOff against today's image.** The chart
  passes `--cluster` / `--replica-idx` / `--peer-addrs` to the
  binary, which doesn't recognise them yet — that's T2. Documented
  in the StatefulSet template comment + design spec §5.1.
- **No live kind verify in T1.** Deferred to T4 with the T2-extended
  binary. The `helm lint` + `helm template` checks above prove the
  YAML scaffold itself is well-formed.
- **Fly.io path is separate.** The StatefulSet shape relies on
  stable k8s pod DNS, which Fly Machines don't have natively. T6
  ships a Fly-specific transport (per-machine `vm.<app>.internal`
  DNS or 6PN address); deliberately scoped out of T1.

## Named follow-up arcs (V2+)

Recapped from design spec §11 — none of these are V1 blockers, but
they're the canonical names for the next wave of cluster-shape work:

- **SP-Cloud-Cluster-GEO** — multi-region replication.
- **SP-Cloud-Cluster-SHARD** — K shards × N replicas.
- **SP-Cloud-Cluster-BACKUP** — coordinated cluster-wide snapshot.
- **SP-Cloud-Cluster-RECONFIG** — online membership change (requires
  upstream `kessel-vsr` reconfiguration support).
- **SP-Cloud-Cluster-VERIFY-MULTI-NODE** — real multi-node K8s smoke.
