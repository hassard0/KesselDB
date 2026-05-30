################################################################################
# KesselDB — minimal single-binary image.
#
# Two stages:
#   1. builder — rust:1.83-slim, compiles `kesseldb` (server) with the
#      `pg-gateway` + `http-gateway` features and the `kessel` CLI client.
#      No system libraries needed — the kernel + all gateways are pure Rust.
#   2. runtime — debian:bookworm-slim with just the two binaries, the
#      LICENSE, README, and USAGE doc, and a `/data` volume.
#
# Default ENTRYPOINT exposes all three wire surfaces:
#   * 6532 — binary protocol (kessel CLI native)
#   * 6533 — HTTP/1.1 + WebSocket (browsers, curl)
#   * 5432 — PostgreSQL Frontend/Backend v3.0 (psql, JDBC, psycopg…)
#
# Quickstart:
#   docker run --rm -p 6532:6532 -p 6533:6533 -p 5432:5432 \
#     -v $PWD/kesseldb-data:/data \
#     -e KESSELDB_TOKEN=changeme \
#     ghcr.io/hassard0/kesseldb:latest
#
# Then from the host:
#   kessel --addr 127.0.0.1:6532 --token changeme "CREATE TABLE acct (id U64 NOT NULL)"
#
# Image build is intentionally NOT part of the workspace default cargo
# build — the Dockerfile composes the existing release binaries with the
# already-shipped pg-gateway + http-gateway feature flags. The workspace
# zero-dep stance is preserved (no new runtime deps added to any crate).
################################################################################

FROM rust:1-slim AS builder
WORKDIR /src

# Copy the source tree. .dockerignore keeps target/ + git data out of the
# build context.
COPY . .

# Build the two release binaries. Pin CARGO_INCREMENTAL=0 + a fresh
# CARGO_TARGET_DIR so layer caching reuses the registry+git caches but
# not stale incremental artifacts (which would defeat reproducibility).
ENV CARGO_INCREMENTAL=0
ENV CARGO_TERM_COLOR=always
RUN cargo build --release \
        --bin kesseldb -p kesseldb-server \
        --features pg-gateway,http-gateway && \
    cargo build --release \
        --bin kessel -p kessel-client

# Strip debug info to keep the runtime image small (~6-8 MiB each).
RUN strip target/release/kesseldb target/release/kessel || true

################################################################################
FROM debian:bookworm-slim AS runtime

# Run as a non-root user — defensive default. Most KesselDB deploys mount
# /data, so we chown it to the runtime user at startup via the entrypoint.
RUN groupadd --system --gid 1100 kessel && \
    useradd --system --uid 1100 --gid kessel --home /var/lib/kesseldb \
        --shell /usr/sbin/nologin kessel && \
    mkdir -p /data && chown kessel:kessel /data

# Just the binaries + the docs an operator wants on-image.
COPY --from=builder /src/target/release/kesseldb /usr/local/bin/kesseldb
COPY --from=builder /src/target/release/kessel   /usr/local/bin/kessel
COPY --from=builder /src/README.md               /usr/share/kesseldb/README.md
COPY --from=builder /src/docs/USAGE.md           /usr/share/kesseldb/USAGE.md
COPY --from=builder /src/LICENSE                 /usr/share/kesseldb/LICENSE

# Default gateway addresses: all three surfaces on 0.0.0.0 so the
# container is useful out of the box. Operators override via -e or a
# kubectl env: stanza.
ENV KESSELDB_HTTP_ADDR=0.0.0.0:6533
ENV KESSELDB_PG_ADDR=0.0.0.0:5432
# KESSELDB_TOKEN is intentionally NOT set by default — the image runs
# in open mode unless the operator passes one. This mirrors the bare
# binary's behaviour.

EXPOSE 6532 6533 5432
VOLUME ["/data"]

USER kessel
WORKDIR /var/lib/kesseldb

ENTRYPOINT ["/usr/local/bin/kesseldb"]
CMD ["0.0.0.0:6532", "/data"]
