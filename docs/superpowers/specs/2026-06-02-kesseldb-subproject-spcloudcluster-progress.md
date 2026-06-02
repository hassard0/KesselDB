# SP-Cloud-Cluster — progress tracker

Date opened: **2026-06-02**.
Status: **OPEN — T1 SCAFFOLD LANDED**; T2-T8 multi-arc continuation queued.
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
| **T1** | Design spec + Helm StatefulSet + headless Service + values.yaml `cluster:` block + entrypoint shell + gating on existing `deployment.yaml`/`pvc.yaml`. `helm lint` + `helm template` clean both default + cluster modes. (THIS SLICE — YAML + docs only.) | **DONE** | `c44d883` |
| T2 | `kesseldb` binary cluster-mode wire-up — `--cluster`, `--replica-idx`, `--peer-addrs` CLI flags + `KESSELDB_CLUSTER_*` env-var fallback; spawn through `kesseldb_server::cluster::spawn_node(idx, listener, addrs, dir)` instead of `run_cfg`. Refuse to start on even N or N<3 with a typed error message. Refuse `--cluster` without `--replica-idx`/`--peer-addrs`. Single-pod path byte-identical. | QUEUED | — |
| T3 | Headless Service DNS resolution + peer discovery — verify the binary tolerates `getaddrinfo` returning empty for the first 30-60s of pod bootstrap (the writer-thread already redials lazily; this slice just confirms the failure mode is clean). Add a small "waiting for peers" log line. | QUEUED | — |
| T4 | kind-verified 3-replica cluster on vulcan — fresh `kind create cluster` + `kubectl create secret kesseldb-token` + `helm install --set cluster.enabled=true` + `kind load docker-image` for the T2-extended binary + `kubectl rollout status statefulset/kesseldb`. All 3 pods reach Ready, primary elects, follower replicas catch up. Transcript at `docs/superpowers/spcloudcluster-t4-kind-verify-2026-XX-XX.txt`. | QUEUED | — |
| T5 | Real cluster smoke — CRUD via `kubectl exec` to any pod (clients connect through the regular ClusterIP, which can route to any pod; ClusterClient retries against primary on `Unavailable`); kill the primary (`kubectl delete pod kesseldb-0`) and verify view-change elects a new primary within view-change timeout; verify the SSTables + WAL state survives + replays from the rejoined pod. | QUEUED | — |
| T6 | Fly.io multi-region cluster deploy — per-region `[mounts]` + per-machine env-var `KESSELDB_CLUSTER_REPLICA_IDX` mapping. Fly Machines do NOT have stable headless-DNS; peer addresses use `<machine-id>.vm.<app>.internal` or per-machine private 6PN address (TBD in T6 design). | QUEUED | — |
| T7 | Monitoring — verify `/v1/metrics` Prometheus scrape endpoint emits VSR-relevant counters (view changes per replica, last-applied op-number, lag-from-primary, primary uptime). Ship a sample `prometheus-rules.yaml` and Grafana dashboard JSON. | QUEUED | — |
| T8 | Arc closure — STATUS Track row + this tracker close + USAGE §11.5 sub-section ("Kubernetes cluster mode") + README Deploy table extension. | QUEUED | — |

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
