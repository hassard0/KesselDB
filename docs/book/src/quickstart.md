# Quick start

Five minutes from download to a real SQL query, including the PostgreSQL
wire. Full details live in the [Usage guide](usage/full-usage.md) — this
chapter is the README's quick-start section, kept verbatim so you don't
need to dig.

For the README-side context (pitch, capability matrix, performance log
links), see the [Introduction](introduction.md). For the full operator
manual including auth, quotas, clustering, backups, and every wire
protocol, see [Usage guide (full)](usage/full-usage.md) §1–§13.

## Download a prebuilt Linux binary

```bash
VER=v1.0.0   # see https://github.com/hassard0/KesselDB/releases for the latest
curl -L https://github.com/hassard0/KesselDB/releases/download/$VER/kesseldb-$VER-x86_64-unknown-linux-gnu \
  -o kesseldb && chmod +x kesseldb
curl -L https://github.com/hassard0/KesselDB/releases/download/$VER/kessel-$VER-x86_64-unknown-linux-gnu \
  -o kessel    && chmod +x kessel
```

The release workflow builds these with `cargo build --release
--features pg-gateway,http-gateway` so the PostgreSQL, HTTP, and
WebSocket gateways are wired in.

## Or build from source

```bash
git clone https://github.com/hassard0/KesselDB && cd KesselDB
cargo build --release                                       # binary protocol only
cargo build --release --features pg-gateway,http-gateway    # all wire surfaces
cargo test  --workspace --release                           # 1792 default tests
```

Requires Rust stable 1.95+. No system libraries, no native build steps.

## Start a node

```bash
# kesseldb [LISTEN_ADDR] [DATA_DIR]
./kesseldb 127.0.0.1:7878 ./data

# All wire surfaces (binary + HTTP + PG) on one node:
KESSELDB_TOKEN=mysecret \
KESSELDB_HTTP_ADDR=127.0.0.1:8080 \
KESSELDB_PG_ADDR=127.0.0.1:5432 \
  ./kesseldb 127.0.0.1:7878 ./data
```

## Connect

```bash
# Binary protocol via the kessel CLI:
./kessel "CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)"
./kessel "INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)"
./kessel "SELECT SUM(bal) FROM acct WHERE owner = 100"     # => = 50

# HTTP/1.1 + JSON:
curl -s -X POST --data-binary 'SELECT * FROM acct' \
  -H 'Content-Type: text/plain' \
  -H 'Authorization: Bearer mysecret' \
  http://127.0.0.1:8080/v1/sql

# PostgreSQL wire (any libpq client):
PGPASSWORD=mysecret psql -h 127.0.0.1 -p 5432 -U test "SELECT SUM(bal) FROM acct"
```

Next steps: [CLI](usage/cli.md) · [SQL surface](usage/sql-surface.md)
· [HTTP gateway](usage/http-gateway.md) · [PostgreSQL wire](usage/postgres-wire.md)
· [Running a cluster](operations/clustering.md).
