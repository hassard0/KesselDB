# Deploying KesselDB to Fly.io

A single-VM KesselDB deployment on Fly.io takes about five minutes.
The `fly.toml` in this directory bundles the
already-published `ghcr.io/hassard0/kesseldb:latest` multi-arch
image — Fly.io pulls it directly, no local build step.

> V1 ships single-VM only (matches the Helm chart's single-pod
> posture). Multi-region / multi-replica VSR clustering on Fly.io
> is SP-Cloud-Cluster — a separate arc.

## Prerequisites

- A Fly.io account + `flyctl` installed locally
  (`brew install flyctl` or `curl -L https://fly.io/install.sh | sh`).
- `fly auth login` once.
- The app name `kesseldb` is taken on Fly's global namespace; pick
  your own (`fly launch --name <yours>`).

## Deploy sequence

```bash
cd deploy/fly

# 1. Initialise the app (copies fly.toml into Fly's state).
#    --no-deploy because we need to create the volume + secret first.
fly launch --no-deploy --copy-config --name <your-app>

# 2. Set the auth token (the password operators present on Bearer +
#    SCRAM-SHA-256 auth). Use a strong random value.
fly secrets set KESSELDB_TOKEN=$(openssl rand -hex 32)

# 3. Create the persistent volume. Size in GiB; bump for production.
#    Region MUST match `primary_region` in fly.toml.
fly volumes create kesseldb_data --size 10 --region iad

# 4. Deploy.
fly deploy
```

## Verify

```bash
# Status:
fly status

# Tail logs:
fly logs

# Open a shell inside the VM:
fly ssh console

# From inside the VM — smoke test:
kessel --addr 127.0.0.1:6532 --token "$KESSELDB_TOKEN" 'SELECT 1'

# From your laptop — port-forward over WireGuard:
fly proxy 5432:5432
# In another shell:
PGPASSWORD="$(fly secrets list --json | jq -r '.[]|select(.Name=="KESSELDB_TOKEN").Digest')" \
  psql -h 127.0.0.1 -p 5432 -U kessel "SELECT 1"
#   NOTE: `fly secrets list` does NOT expose plaintext values (only digests).
#   Re-use whatever value you piped into `fly secrets set` above.
```

## Connect from outside Fly

Fly's `fly.toml` publishes raw TCP ports — they're reachable on
`<your-app>.fly.dev` once `fly ips allocate-v6` (auto-done by
`fly launch`) completes:

```bash
# Binary protocol:
kessel --addr <your-app>.fly.dev:6532 --token "$KESSELDB_TOKEN" 'SELECT 1'

# HTTP:
curl -X POST \
  -H "Authorization: Bearer $KESSELDB_TOKEN" \
  --data-binary 'SELECT 1' \
  http://<your-app>.fly.dev:6533/v1/sql

# psql:
PGPASSWORD="$KESSELDB_TOKEN" psql \
  -h <your-app>.fly.dev -p 5432 -U kessel "SELECT 1"
```

## Backups

KesselDB ships a hot snapshot primitive that's safe to call against
a live engine (see `docs/USAGE.md` §12 for the binary-protocol shape,
or use the `0xFA` admin frame). For Fly volume snapshots:

```bash
fly volumes snapshots create kesseldb_data
fly volumes snapshots list   kesseldb_data
```

## Roll back / wipe

```bash
fly deploy --image ghcr.io/hassard0/kesseldb:v1.0.0     # pin to a known-good tag
fly machine destroy <id>                                # nuke a stuck VM
fly volumes destroy kesseldb_data                       # WIPES DATA
fly apps destroy <your-app>                             # tear down the whole app
```

## Caveats

- **Single-VM only.** Multi-region / multi-replica clustering is
  SP-Cloud-Cluster (separate arc; will require Fly's WireGuard mesh
  + ClusterClient endpoints + per-replica volumes).
- **No public TLS** in the V1 image. The ghcr.io image is built
  with `--features pg-gateway,http-gateway` only; `--features tls`
  is opt-in (rustls). Pair with Fly's `fly certs` + a fronting
  proxy (e.g. caddy) if you need HTTPS in front of `:6533`.
- **`auto_stop_machines = "off"`** — KesselDB is a stateful engine
  with a single attached volume; Fly's autostop suspends the VM
  and breaks long-lived connections. Pin `min_machines_running = 1`.
- **Single-attach volume.** `strategy = "immediate"` is required
  because Fly's rolling strategy can't attach the volume to a new
  machine before the old one releases it.
