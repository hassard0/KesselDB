# SP-Cloud-Deploy — progress tracker

Date opened: **2026-05-30**.
Date closed: **2026-05-30**.
Status: **V1 SHIPPED — arc closed**.

## Goal

Build the production-deploy story on top of SP-DX-superior's
Dockerfile + ghcr.io push. Operators should be able to land a running
KesselDB node on Kubernetes or Fly.io in a single command sequence,
with documented one-liner shapes for Docker single-host + any other
OCI runtime (Nomad / ECS / Cloud Run / systemd-nspawn).

## In-scope slice

V1 is **single-pod / single-VM** by design — the engine is single-
writer and the data volume is single-attach. Replicated VSR
clustering on k8s + Fly is the named follow-up arc **SP-Cloud-
Cluster** (StatefulSet + per-replica PVCs + headless Service +
ClusterClient endpoints).

| Item | Status | Notes |
|---|---|---|
| 1. Helm chart skeleton | **SHIPPED — T1** | Single-pod, ClusterIP, RWO PVC, kessel:1100 non-root, secret-ref env. `helm lint`: 0 failed. |
| 2. fly.toml + deploy/fly/README.md | **SHIPPED — T2** | Pinned to ghcr.io image; 3 TCP services; single-attach volume; tomllib well-formed. |
| 3. kind verify on vulcan | **SHIPPED — T3** | Real K8s cluster (kind v0.24.0 + K8s v1.31.0 + helm v3.16.3); binary + HTTP smoke GREEN; transcript file. |
| 4. USAGE §11 deploy section | **SHIPPED — T4** | 11.1 Docker / 11.2 Helm / 11.3 Fly / 11.4 Custom; former §11-13 renumbered to §12-14. |
| 5. README Deploy section | **SHIPPED — T5** | 4-row table (shape | one-liner | reference); points at USAGE §11. |
| 6. STATUS + progress tracker | **SHIPPED — T6** | Track K row in STATUS.md; this tracker file. |

## Task tracker

### T1 — Helm chart skeleton (DONE 2026-05-30, commit `e3eca27`)

`deploy/helm/kesseldb/` ships the standard Helm v3 layout:

```
deploy/helm/kesseldb/
  Chart.yaml              # apiVersion v2, type application, appVersion=latest
  values.yaml             # all knobs documented inline (~250 lines incl. comments)
  .helmignore             # standard
  templates/
    _helpers.tpl          # fullname / labels / selectorLabels / serviceAccountName
    deployment.yaml       # replicas:1 (single-writer), Recreate strategy, kessel:1100
    service.yaml          # ClusterIP, three named ports (binary, http, postgres)
    pvc.yaml              # 10Gi RWO, default StorageClass
    secret.yaml           # DELIBERATELY EMPTY — token via pre-created Secret only
    serviceaccount.yaml   # minimal SA per release, no RBAC
    NOTES.txt             # post-install: create token Secret, connect samples
```

Key design decisions (called out in values.yaml + template comments):

- **Single-pod by design.** `replicas: 1` + `strategy: Recreate`
  because the engine is single-writer and the data PVC is
  ReadWriteOnce. A second pod would split-brain. Multi-replica is
  SP-Cloud-Cluster.
- **Image defaults to `ghcr.io/hassard0/kesseldb:latest`** — the
  tag pushed by `.github/workflows/release.yml`'s docker job.
  Operators pin via `--set image.tag=v1.2.3` for reproducible
  rollouts.
- **KESSELDB_TOKEN via Secret, not values.yaml.** The Deployment
  references `.Values.auth.secretName` (default `kesseldb-token`,
  key `token`). Token values must never land in helm-rendered YAML
  or release-history Secret backing. Set `auth.secretName=""` to
  run in open mode (matches Dockerfile default behaviour).
- **kessel:1100 non-root** — matches the Dockerfile's UID/GID.
  `fsGroup: 1100` makes the PVC writable; `containerSecurityContext`
  drops ALL caps + disallows privilege escalation.
- **TCP probes on `:6532`** — the binary protocol port being up is
  sufficient liveness/readiness signal (HTTP + PG gateways are
  opt-in via env vars; can't probe what may not be enabled).
- **Resources**: 500m/512Mi requests, 4/4Gi limits. The 4 CPU
  ceiling matches SP-Hash-Agg V1's 4-way parallel target.

Verified on vulcan:

```
$ helm lint deploy/helm/kesseldb
==> Linting deploy/helm/kesseldb
[INFO] Chart.yaml: icon is recommended
1 chart(s) linted, 0 chart(s) failed

$ helm template kesseldb deploy/helm/kesseldb
# renders 4 K8s objects (ServiceAccount + PVC + Service + Deployment).

$ helm template kesseldb deploy/helm/kesseldb --set auth.secretName=''
# KESSELDB_TOKEN env block correctly OMITTED — open-mode branch verified.
```

### T2 — fly.toml + deploy/fly/README.md (DONE 2026-05-30, commit `449929d`)

`deploy/fly/fly.toml`:

- `[build].image = "ghcr.io/hassard0/kesseldb:latest"` — no Fly.io
  builder step (we use the multi-arch image release.yml already pushes).
- `[env]` sets `KESSELDB_HTTP_ADDR` + `KESSELDB_PG_ADDR` to 0.0.0.0
  (Dockerfile defaults already do this; restated for clarity).
- `[[mounts]]` `kesseldb_data` -> `/data` (matches Dockerfile VOLUME).
- Three `[[services]]` stanzas — one per wire surface (binary 6532,
  HTTP 6533, PG 5432). All raw TCP, `tcp_checks` every 10s.
- `auto_stop_machines = "off"` + `min_machines_running = 1` —
  KesselDB is stateful; autostop would suspend the VM and break
  long-lived connections.
- `[deploy].strategy = "immediate"` — single-attach volume can't
  be re-attached to a new machine before the old one releases it.

`deploy/fly/README.md` — the four-command deploy walkthrough
(`fly launch --no-deploy` → `fly secrets set KESSELDB_TOKEN=...` →
`fly volumes create` → `fly deploy`) + verify section
(`fly status` / `fly logs` / `fly ssh console` / `fly proxy`) +
connect-from-outside section (binary + HTTP + psql against
`<app>.fly.dev`) + backups (`fly volumes snapshots create`) +
roll-back/wipe commands + V1 caveats (single-VM; no public TLS;
auto_stop off; single-attach volume).

Validation:

```
$ python -c "import tomllib; tomllib.loads(open('deploy/fly/fly.toml','rb').read().decode()); print('TOML OK')"
TOML OK
```

`flyctl config validate` deferred (flyctl not installed on vulcan;
the structure follows the canonical shape published at
<https://fly.io/docs/reference/configuration/>).

### T3 — kind verify on vulcan (DONE 2026-05-30, commit `1a7ceb9`)

Stack: `kind v0.24.0` (Kubernetes v1.31.0) + `kubectl v1.31.0` +
`helm v3.16.3`, all installed user-local to vulcan (`~/bin`); no
sudo needed.

Sequence (full transcript at
`docs/superpowers/spclouddeploy-t3-kind-verify-2026-05-30.txt`):

```
1. helm lint deploy/helm/kesseldb         -> 0 chart(s) failed
2. helm template kesseldb ... [--set auth.secretName='']
                                          -> renders 4 objects; open-mode branch drops env
3. kind create cluster --name kesseldb-test --wait 90s  (17s)
4. kubectl create secret generic kesseldb-token --from-literal=token=smoketest
5. helm install kesseldb ./deploy/helm/kesseldb         -> deployed
6. kind load docker-image (workaround for private GHCR package — see Caveats)
7. kubectl rollout status deploy/kesseldb --timeout=120s
                                          -> deployment successfully rolled out
8. Smoke (binary):
   kubectl exec deploy/kesseldb -- kessel --addr 127.0.0.1:6532 --token smoketest 'CREATE TABLE smoke (v U64 NOT NULL)'
                                          -> OK (table created, type_id=1)
   kubectl exec ... -- kessel ... 'INSERT INTO smoke ID 1 (v) VALUES (42)'
                                          -> OK
   kubectl exec ... -- kessel ... 'SELECT SUM(v) FROM smoke'
                                          -> = 42 (16 bytes)
9. Smoke (HTTP via port-forward):
   curl -H 'Authorization: Bearer smoketest' http://127.0.0.1:16533/v1/health
                                          -> {"status":"ok","primary":true,"view":0,"op_number":4,"role":"primary"}
   curl -X POST -H 'Authorization: Bearer smoketest' -H 'Content-Type: text/plain' \
        --data-binary 'SELECT * FROM smoke' http://127.0.0.1:16533/v1/sql
                                          -> {"status":"ok","bytes":36}
                                             (4-byte LE len prefix + 32-byte encoded row)
10. helm uninstall + kind delete cluster  -> clean teardown
```

Proven invariants:

- PVC bound (10 Gi RWO, default StorageClass)
- Deployment ready (1/1 pod, kessel:1100 non-root)
- Liveness + readiness TCP:6532 probes green
- Service exposes all three wire surfaces
- Binary protocol + HTTP gateway both serve correctly
- Token-mode auth wired Secret -> env -> server

### T4 — USAGE §11 deploy section (DONE 2026-05-30, commit `a3b7d0f`)

New §11 'Deploying to the cloud' inserted; former §11/§12/§13
(Backup & monitoring / Wire protocol / Troubleshooting) renumbered
to §12/§13/§14.

§11 sub-sections:

- **11.1 Docker (single-host)** — one-liner `docker run` with all
  three ports + persistent volume + auth env.
- **11.2 Kubernetes (Helm chart)** — 3-step pre-create Secret +
  helm install + kubectl-exec smoke; cross-refs to values.yaml
  + the T3 verify transcript.
- **11.3 Fly.io (fly.toml)** — 4-command launch + secret + volume
  + deploy; cross-refs to deploy/fly/README.md.
- **11.4 Custom (any container runtime)** — image/entrypoint/args/
  env/volume/ports reference table for Nomad / ECS / Cloud Run /
  systemd-nspawn.

V1 caveats sub-section names all three known limits:
single-pod/single-VM by design; no public TLS in v1 image; GHCR
package visibility (currently private).

### T5 — README Deploy section (DONE 2026-05-30, commit `4c5e793`)

New top-level 'Deploy' section between 'Or from Rust' and
'PostgreSQL client compatibility'. Pointer-only (the existing
Quick start already covers the Docker one-liner).

Four-row table — shape | copy-pasteable one-liner | reference link.
Trailing paragraph points at USAGE §11 and the kind-verify transcript.

### T6 — STATUS row + progress tracker close (this commit)

- `docs/STATUS.md` Track K row added covering the V1 ship.
- This file CLOSED.

## Invariants preserved

- Workspace zero-dep stance: zero Rust code touched; no Cargo.toml
  changes; no new external deps.
- `#![forbid(unsafe_code)]` honored (n/a — no Rust changes).
- Default `cargo build -p kesseldb-server` byte-identical.
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched at the
  wire boundary.
- KAT delta: **+0** (this slice is YAML + Markdown only; no test
  surface to grow). Per the brief's "+0-3" target band.

## Honest concerns

- **GHCR package currently private.** The `ghcr.io/hassard0/kesseldb`
  package was published by SP-DX-superior's release.yml docker job,
  but GitHub defaults new ghcr packages to private — the kind
  verify side-loaded a locally built image instead of pulling from
  ghcr. One-time fix: flip the package to Public in the GitHub UI
  (repo Packages -> kesseldb -> Settings -> Change visibility ->
  Public). Documented in:
  - `docs/USAGE.md` §11 V1 caveats sub-section
  - `deploy/helm/kesseldb/templates/NOTES.txt` (referenced
    indirectly via the install instructions)
  - The T3 verify transcript's follow-up section
- **kind validate-against-real-cluster** is the harder smoke; we
  ran the Helm install end-to-end on real K8s (kind), not just
  `helm template` lint. The transcript captures the proof.
- **No fly.io live verify.** flyctl is not installed on vulcan and
  a real Fly deploy requires Fly account credentials that this
  agent does not have. fly.toml is TOML-well-formed and follows
  the canonical fly.io reference; the four-command sequence is
  the standard Fly.io shape documented at
  <https://fly.io/docs/reference/configuration/>. Listed as a
  follow-up.

## Named follow-ups

- **SP-Cloud-Cluster** — replicated VSR clustering on k8s + Fly.io.
  Bigger surface (StatefulSet + per-replica PVCs + headless
  Service + ClusterClient endpoints + Fly multi-region Wireguard
  mesh). Deserves its own design pass.
- **SP-Cloud-Deploy-Verify-Fly** — once flyctl + a Fly.io account
  is available on the verify host, run the same shape of smoke
  test that T3 ran for Helm (fly deploy → fly ssh exec kessel SQL
  round-trip + curl against `<app>.fly.dev:6533/v1/health`).
- **GHCR package visibility flip** — one-time manual step in the
  GitHub UI; not really a follow-up arc, just a checklist item
  for next time someone runs the helm install path.
- **SP-Cloud-Deploy-TLS** — add an optional `tls.enabled` value to
  the Helm chart that ships a cert-manager Issuer + Certificate
  pair and switches the HTTP gateway to `--features tls`. Out of
  scope here because the v1 ghcr.io image doesn't include the
  `tls` feature; needs a new release.yml job that pushes a
  `:<version>-tls` image variant.
