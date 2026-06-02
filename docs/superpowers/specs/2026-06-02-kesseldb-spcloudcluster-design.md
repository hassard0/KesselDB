# SP-Cloud-Cluster — design spec (T1 scaffold)

Date opened: **2026-06-02**.
Status: **OPEN** — T1 scaffold landing; T2-T8 multi-arc continuation queued.
Parent arc: SP-Cloud-Deploy (V1 SHIPPED 2026-05-30 — single-pod Helm chart
+ fly.toml + kind-verified end-to-end). The SP-Cloud-Deploy progress
tracker (`docs/superpowers/specs/2026-05-30-kesseldb-spclouddeploy-progress.md`)
called this out as the named follow-up.

## 1. Context

SP-Cloud-Deploy V1 ships a single-pod / single-VM cloud-deploy story:
the Helm chart at `deploy/helm/kesseldb/` is a `Deployment` with
`replicas: 1` + `strategy: Recreate` because the engine is single-writer
and the data PVC is ReadWriteOnce. A second pod would split-brain.

That single-pod story is intentional for V1 (it matches the engine's
single-writer assumption when run without VSR), but real production
deployments need replicated state for availability + durability. KesselDB
already supports multi-node clustering through `kessel-vsr` consensus
(N=3 or N=5 with view-change + crash recovery + a real-TCP socket
transport in `crates/kesseldb-server/src/cluster.rs`); see ARCHITECTURE.md
§Replication. SP-Cloud-Cluster wires that existing capability into a
production-grade k8s + Fly.io deployment shape.

## 2. Scope V1 (this arc — ~6-8 task slices)

| T# | Scope | Status |
|---|---|---|
| **T1** | Design spec + Helm StatefulSet + headless Service + env-based config (THIS SLICE — YAML + docs only) | **DONE** (this commit) |
| T2 | `kesseldb` binary cluster-mode wire-up (read `KESSELDB_CLUSTER_REPLICA_IDX` + `KESSELDB_CLUSTER_PEER_ADDRS` env; spawn through `kesseldb_server::cluster::spawn_node`) | QUEUED |
| T3 | Headless Service DNS resolution + peer discovery (init-container or in-binary `getaddrinfo` retry loop until all peers resolvable) | QUEUED |
| T4 | kind-verified 3-replica cluster on a single node (real K8s smoke) | QUEUED |
| T5 | Real cluster smoke (CRUD + primary-kill survives + new primary takes over) | QUEUED |
| T6 | Fly.io multi-region cluster deploy (per-region Machine + private 6PN address mesh) | QUEUED |
| T7 | Monitoring (`/v1/metrics` Prometheus scrape + alerting rules for view-change storms / lagging replicas) | QUEUED |
| T8 | Arc closure (STATUS row + progress tracker close + USAGE §11.5 Cluster sub-section) | QUEUED |

## 3. V1 scope IN

- **StatefulSet with 3 or 5 replicas (configurable)** — deterministic
  pod naming (`kesseldb-0`, `kesseldb-1`, `kesseldb-2`), stable DNS,
  ordinal-derived replica idx.
- **VSR replication over k8s pod DNS** — peer addresses constructed
  from headless-Service DNS template
  (`{name}-{idx}.{name}.{namespace}.svc.cluster.local:6532`).
- **Per-pod PVC** (`volumeClaimTemplates`) — every replica owns its own
  data volume; the StatefulSet controller binds them deterministically
  across pod restarts.
- **Primary election + failover** — view-change runs whenever the
  primary becomes unreachable (12 ms tick + configurable timeout); the
  new primary takes over without operator intervention.
- **ClusterClient connection model** — clients connect to any of the N
  endpoints; reads/writes that hit a backup return `Unavailable` and
  the SP42 client-side `ClusterClient` retries against the discovered
  primary.

## 4. V1 out-of-scope (named follow-up arcs)

- **Cross-region replication** → **SP-Cloud-Cluster-GEO**. V1 keeps the
  3 or 5 replicas inside a single region/zone group. Multi-region adds
  WAN-latency-tolerant view-change timeouts + per-region quorum reads.
- **Sharding × clustering** → SP-Cloud-Cluster-SHARD (V2). Each shard
  is its own VSR group; combining K shards × N replicas means K×N pods
  and K independent leader elections. V1 ships clustering, V2 adds the
  shard × cluster matrix.
- **Backup/restore across cluster** → SP-Cloud-Cluster-BACKUP. V1
  per-pod snapshots are uncoordinated. A coordinated cluster-wide
  snapshot (quiesce at a known op-number, snapshot all PVCs, replay
  from snapshot on restore) is its own design.
- **Online cluster reconfiguration (add/remove replicas)** →
  SP-Cloud-Cluster-RECONFIG. VSR core doesn't support membership
  change in V1; static N is the contract. Scaling means a fresh cluster
  + a one-shot data migration.

## 5. Helm chart additions (this slice)

Path: `deploy/helm/kesseldb/`.

### 5.1 NEW: `templates/statefulset.yaml`

Conditional on `.Values.cluster.enabled`. Mirrors the existing
`deployment.yaml` (image, env, probes, ports, securityContext) but
substitutes the StatefulSet-specific shape:

- `apiVersion: apps/v1`, `kind: StatefulSet`.
- `spec.replicas: {{ .Values.cluster.replicas }}` — default 3.
- `spec.serviceName: {{ fullname }}-headless` — required for stable
  pod DNS.
- `spec.podManagementPolicy: Parallel` — VSR doesn't require ordered
  startup (the protocol bootstraps when f+1 replicas reach each other,
  in any order). Parallel speeds up rolling restart.
- `spec.updateStrategy.type: RollingUpdate` with
  `partition: 0` — rolling restart proceeds replica-by-replica, with
  the partition knob available for canary tier upgrades.
- `spec.template` matches the Deployment's pod-spec for image / env /
  ports / probes / securityContext, EXTENDED with cluster env vars:
  - `KESSELDB_CLUSTER_REPLICA_IDX` derived in the entrypoint command
    from `$HOSTNAME` (`${HOSTNAME##*-}` → 0/1/2/...).
  - `KESSELDB_CLUSTER_PEER_ADDRS` rendered from
    `.Values.cluster.peerAddressTemplate` for each ordinal.
  - `KESSELDB_CLUSTER_VIEW_CHANGE_TIMEOUT` from
    `.Values.cluster.viewChangeTimeout` (default `5s`).
- `spec.volumeClaimTemplates` — one entry named `data`, sized by
  `.Values.persistence.size`, ReadWriteOnce. The Deployment-path PVC
  in `templates/pvc.yaml` is gated off when `.Values.cluster.enabled`
  so we don't create both shapes.

Container `command` overrides the image's `kesseldb LISTEN_ADDR DATA_DIR`
default with a small shell wrapper that derives the replica index from
the pod hostname (T2 will optionally move this into the binary itself
via a `--cluster` flag; the wrapper is enough for T1's
`helm template`-clean output):

```sh
#!/bin/sh
set -eu
IDX="${HOSTNAME##*-}"
echo "kesseldb cluster pod: idx=$IDX peers=$KESSELDB_CLUSTER_PEER_ADDRS"
exec /usr/local/bin/kesseldb \
  --cluster \
  --replica-idx "$IDX" \
  --peer-addrs "$KESSELDB_CLUSTER_PEER_ADDRS" \
  0.0.0.0:6532 \
  /data
```

The `--cluster` / `--replica-idx` / `--peer-addrs` CLI args do NOT yet
exist on the binary — T2 adds them. T1's StatefulSet renders cleanly
but the pods will CrashLoopBackOff on `unknown argument --cluster`.
That failure mode is intentional + documented in §8 acceptance.

### 5.2 NEW: `templates/service-headless.yaml`

Conditional on `.Values.cluster.enabled`. The standard StatefulSet
headless-Service shape:

- `metadata.name: {{ fullname }}-headless`.
- `spec.clusterIP: None` — turns this into a DNS-only Service; each
  pod gets an A record at
  `{{ fullname }}-{ord}.{{ fullname }}-headless.{ns}.svc.cluster.local`.
- `spec.publishNotReadyAddresses: true` — VSR needs to talk to peers
  during bootstrap BEFORE they're "ready" by k8s probe standards
  (peers need each other to reach the steady state). Without this,
  initial cluster bootstrap deadlocks (every pod waits for every
  other pod to be Ready, but nobody is Ready because nobody can talk).
- `spec.ports`: only the binary port (6532) — the headless service is
  for peer-to-peer consensus, not client traffic. The existing
  `templates/service.yaml` (ClusterIP) is still rendered and stays the
  client-facing endpoint.
- `spec.selector` matches the StatefulSet selector.

### 5.3 `values.yaml` extension

Append the `cluster:` block AFTER the existing `persistence:` / `service:` /
`resources:` blocks:

```yaml
# ─────────────────────────────────────────────────────────────────────
# Cluster mode — replicated VSR consensus across N pods (V1: N=3 or 5).
# Set cluster.enabled=true to switch from single-pod Deployment to
# multi-pod StatefulSet + headless Service + per-pod PVCs.
#
# This is the SP-Cloud-Cluster opt-in. SP-Cloud-Deploy V1 single-pod
# remains the default; cluster mode is the production shape.
# ─────────────────────────────────────────────────────────────────────
cluster:
  # OFF by default so existing SP-Cloud-Deploy V1 installs are
  # byte-identical when the chart is upgraded.
  enabled: false
  # VSR is fixed-size; legal values are 3 (tolerates 1 failure) or
  # 5 (tolerates 2 failures). The chart does not enforce this — the
  # binary will refuse to bootstrap a cluster with even N or N<3 at
  # T2. Documented here so the operator knows the contract.
  replicas: 3
  # DNS template for peer discovery. `{name}` expands to the chart
  # fullname (e.g. "kesseldb"), `{idx}` to the ordinal (0..N-1),
  # `{namespace}` to the release namespace. Defaults match the
  # headless Service `metadata.name` ({fullname}-headless).
  peerAddressTemplate: "{name}-{idx}.{name}-headless.{namespace}.svc.cluster.local:6532"
  # View-change timeout — how long a backup waits without hearing
  # from the primary before initiating a view-change. Default 5s
  # matches the engine's `Replica` default; tune higher for higher-
  # latency networks (e.g. multi-zone) and lower for tight LANs.
  viewChangeTimeout: 5s
  # podManagementPolicy: Parallel (default) bootstraps faster.
  # Set to OrderedReady if you need strict-order pod creation
  # (rare; only useful when peer addresses depend on prior pods
  # being Ready, which VSR does not require).
  podManagementPolicy: Parallel
```

### 5.4 Existing `templates/deployment.yaml` + `templates/pvc.yaml` gating

Both files get an outer `{{- if not .Values.cluster.enabled -}} ... {{- end -}}`
wrapper so they DO NOT render when cluster mode is on. Cluster mode
uses the StatefulSet + its `volumeClaimTemplates` instead.

The existing single-pod path is byte-identical when `cluster.enabled: false`
(default) — every existing SP-Cloud-Deploy V1 install upgrades without
diff.

## 6. Binary env vars (T2 wires these)

| Env var | T1 default value (from chart) | Meaning |
|---|---|---|
| `KESSELDB_CLUSTER_REPLICA_IDX` | derived in entrypoint shell from `${HOSTNAME##*-}` | This pod's index into the peer list (0..N-1) |
| `KESSELDB_CLUSTER_PEER_ADDRS` | comma-separated render of `peerAddressTemplate` for each ordinal | All N peer addresses in deterministic order |
| `KESSELDB_CLUSTER_VIEW_CHANGE_TIMEOUT` | `5s` | View-change timeout (passed to `Replica::new`) |

T2 also adds the matching CLI flags (`--cluster`, `--replica-idx`,
`--peer-addrs`) and the parser that builds `addrs: Vec<SocketAddr>`
and invokes `kesseldb_server::cluster::spawn_node(idx, listener, addrs, dir)`
instead of `kesseldb_server::run_cfg(addr, dir, cfg)`.

## 7. Pod startup shell (T1 scaffold)

Embedded in the StatefulSet's `containers[0].command` as a small inline
shell script (no separate ConfigMap needed):

```sh
#!/bin/sh
set -eu
IDX="${HOSTNAME##*-}"
echo "kesseldb cluster pod: idx=$IDX peers=$KESSELDB_CLUSTER_PEER_ADDRS"
exec /usr/local/bin/kesseldb \
  --cluster \
  --replica-idx "$IDX" \
  --peer-addrs "$KESSELDB_CLUSTER_PEER_ADDRS" \
  0.0.0.0:6532 \
  /data
```

The `${HOSTNAME##*-}` POSIX-shell parameter-expansion suffix strip
yields the trailing integer from a `kesseldb-0` / `kesseldb-1` pod
name. The Dockerfile's `kessel` user has `/bin/sh` available
(busybox-style `sh` is fine — no bashisms).

## 8. Acceptance criteria

### T1 (this slice)

- `deploy/helm/kesseldb/templates/statefulset.yaml` exists, renders only
  when `cluster.enabled=true`.
- `deploy/helm/kesseldb/templates/service-headless.yaml` exists, renders
  only when `cluster.enabled=true`.
- `values.yaml` documents the `cluster:` block + every field.
- `helm template ./deploy/helm/kesseldb` (default values) emits exactly
  one `kind: Deployment` (no StatefulSet) — V1 single-pod default is
  byte-identical to SP-Cloud-Deploy V1.
- `helm template ./deploy/helm/kesseldb --set cluster.enabled=true` emits:
  - 0× `kind: Deployment` (single-pod path gated off)
  - 0× `kind: PersistentVolumeClaim` (volumeClaimTemplates supersedes)
  - 1× `kind: StatefulSet` (3 replicas by default)
  - 1× headless `kind: Service` named `*-headless` with `clusterIP: None`
  - 1× existing `kind: Service` (client-facing, ClusterIP, unchanged)
  - 1× `kind: ServiceAccount`
- `helm lint ./deploy/helm/kesseldb` and
  `helm lint ./deploy/helm/kesseldb --set cluster.enabled=true` both pass.
- The rendered pod-spec includes the cluster env vars + the entrypoint
  shell snippet derives the replica index from `$HOSTNAME`.
- kind-deploying with `cluster.enabled=true` produces pods that
  CrashLoopBackOff on `unknown argument --cluster` (expected — T2 wires
  the CLI; T1 confirms a CLEAN failure mode, not a stuck pending state).

### T8 (arc closure — informational here; will be re-stated in progress tracker)

- Real 3-pod kind cluster survives primary-kill + new primary elected
  within view-change timeout.
- CRUD round-trip via `kessel` CLI through the headless Service.
- USAGE §11.5 Cluster sub-section added.

## 9. Six-plus weak-spot self-review

1. **StatefulSet pod naming relies on stable DNS.** Works in k8s but
   NOT in Fly.io (Fly Machines don't have predictable headless-Service-
   style DNS; instead the per-machine 6PN address is the identity).
   T6 will use a different transport: Fly's `<machine-id>.vm.<app>.internal`
   per-machine DNS + explicit per-region region/machine-id ↔ idx
   mapping in fly.toml `[env]` or a small bootstrap script.
2. **Per-pod PVC sizing assumes uniform data per replica.** True for
   VSR (every replica has every byte of the committed log), so per-pod
   storage sizing equals total data size. This breaks the per-shard
   model in V2 (SP-Cloud-Cluster-SHARD); documented here so V2 can
   change the per-shard PVC story without surprising V1 operators.
3. **kind single-node deploys all pods on one node.** Fine for T4-T5
   smoke (we're testing the YAML + binary, not real cross-node
   failure modes), but it misses real failure scenarios: node drain,
   network partition between nodes, PVC re-attach to a different node.
   T8 explicitly notes this as a `kind` limitation; real-cluster
   verification on multi-node K8s (3+ vulcan workers, or a real
   cloud cluster) is a SP-Cloud-Cluster-VERIFY-MULTI-NODE follow-up.
4. **Helm chart only supports VSR-3 or VSR-5.** The chart doesn't
   enforce odd N or N=3|5 (operator may set `replicas: 4`); the
   binary will refuse at T2 (`kessel-vsr` panics on even N at
   `Replica::new`). Documented in `values.yaml` comments + the
   T2 spec will add a clean error message.
5. **Binary reads env vars at startup.** Runtime reconfig (add/remove
   replicas without restart) is OUT — see §4 SP-Cloud-Cluster-RECONFIG.
   An operator scaling 3 → 5 replicas today needs to take the cluster
   down, redeploy with N=5, and restore from snapshot or replay the
   log on the two new pods (which is the V1 contract — fresh cluster
   bootstrap).
6. **Backup of clustered data is per-pod-PVC.** Each replica has the
   full data, so a single-pod snapshot is sufficient for restore-to-
   single-pod, but cross-cluster restore (snapshot from cluster A,
   restore into cluster B at the same op-number) needs the
   coordinated quiesce. SP-Cloud-Cluster-BACKUP is the named follow-up.
7. **Image pull secrets for private ghcr.io are operator's
   responsibility.** Same as SP-Cloud-Deploy V1 (`image.pullSecrets`
   in values.yaml); cluster mode inherits the existing knob.
8. **SCRAM token rotation across replicas requires synchronized k8s
   Secret update.** When the operator rotates `KESSELDB_TOKEN`, all
   N pods need the new token simultaneously (otherwise mid-rotation
   clients see auth failures from some replicas). k8s Secret updates
   are eventually consistent across pods; for cleanest rotation,
   roll the StatefulSet (T2 should document `kubectl rollout restart
   statefulset/kesseldb` as the rotation procedure).
9. **Replica startup deadlock if headless DNS resolves before all
   pods exist.** `publishNotReadyAddresses: true` on the headless
   Service mitigates by publishing pod IPs as soon as they're
   scheduled (NOT when they're Ready). The binary at T2 must
   tolerate `getaddrinfo` returning empty results for the first
   30-60s of bootstrap; a small `connect-retry-with-backoff` loop
   in `spawn_node`'s writer thread is enough (already implemented —
   `cluster.rs` lazily redials on every send, so transient DNS
   failures self-heal).
10. **Two services in cluster mode (client + headless) — operator
    confusion.** Documented in NOTES.txt: client traffic uses the
    existing `{{ fullname }}` Service (ClusterIP, ports 6532/6533/5432);
    peer traffic uses `{{ fullname }}-headless` (DNS-only, port 6532).
    External clients never touch the headless service.

## 10. Locked invariants

- SP-Cloud-Deploy V1 single-pod path is byte-identical when
  `cluster.enabled: false` (the default).
- Existing chart helpers (`_helpers.tpl`) untouched.
- Existing `templates/service.yaml` (client ClusterIP) untouched.
- Existing `templates/serviceaccount.yaml` + `templates/secret.yaml`
  + `templates/NOTES.txt` untouched.
- Zero Rust code touched in T1 — this slice is YAML + Markdown.
- Workspace `cargo build -p kesseldb-server` byte-identical (no
  Cargo.toml changes).
- `#![forbid(unsafe_code)]` honored (n/a — no Rust changes).
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched (no code
  changes in T1).
- KAT delta: **+0** (YAML + Markdown only).

## 11. Named follow-up arcs (V2+)

- **SP-Cloud-Cluster-GEO** — multi-region replication with WAN-tolerant
  view-change.
- **SP-Cloud-Cluster-SHARD** — K shards × N replicas, K independent
  leader elections.
- **SP-Cloud-Cluster-BACKUP** — coordinated cluster-wide quiesce +
  snapshot.
- **SP-Cloud-Cluster-RECONFIG** — online membership change (requires
  upstream `kessel-vsr` reconfiguration support, which V1 lacks).
- **SP-Cloud-Cluster-VERIFY-MULTI-NODE** — real multi-node K8s smoke
  (currently T4-T5 are single-node kind).
