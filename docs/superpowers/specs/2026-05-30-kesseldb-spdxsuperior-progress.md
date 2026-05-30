# SP-DX-superior — progress tracker

Date opened: **2026-05-30**.
Date closed: **2026-05-30**.
Status: **V1 SHIPPED — arc closed**.

## Goal

Bring the developer experience to match the engineering wins shipped in
the SP-PG-EXTQ / SP-Perf-A / SP-Analytic-Plan arcs. The user mandate:
"superior speed, DX, and general functionality." The first five minutes
of using KesselDB should feel as polished as the perf numbers.

## In-scope slice

Five DX wins were proposed in the kickoff brief. This V1 ships the
three with the highest leverage-per-LoC and defers the two larger
shapes to focused later slices:

| Item | Status | Notes |
|---|---|---|
| 1. `kessel init` quickstart scaffolder | **DEFERRED** | Separate slice: **SP-DX-INIT**. Larger surface (env file generator, compose template, README scaffold); the bare `kesseldb` + `kessel` command + Docker image already cover the start-a-node arc end-to-end. |
| 2. Better error messages | **SHIPPED — T1** | Did-you-mean for unknown table + unknown column; CLI connect/auth diagnosis; SchemaError prefix stripping. |
| 3. Dockerfile + ghcr.io push | **SHIPPED — T2** | Multi-arch `linux/amd64` + `linux/arm64`; release.yml job; verified end-to-end on vulcan. |
| 4. Embedded Rust example | **SHIPPED — T3** | `crates/kesseldb-server/examples/embedded.rs`; new `EngineHandle::sql` convenience. |
| 5. `kessel sql` REPL polish | **DEFERRED** | Separate slice: **SP-DX-REPL**. Multi-line editing, history, wider `\?` meta-commands. Adding rustyline would break the zero-dep stance; a feature-gated optional dep is the right shape but deserves its own design pass. |

## Task tracker

### T1 — better error messages (DONE 2026-05-30, commit `c65b010`)

Audit + improvement of the highest-traffic user-visible error paths.

- `kessel-sql` gains a `suggest(name, candidates) -> Option<&str>`
  helper with a deterministic ranking: exact-case-insensitive →
  prefix-or-superstring (length ≥ 3) → bounded Damerau-Levenshtein
  with cap `max(1, len/4)`. Tie-break on lexicographic first.
- `kessel-sql` gains a `unknown_column_err(col, ot)` helper used at
  every "unknown column" site (10 sites; all updated). Output:
  `unknown column \`owne\` on table \`acct\` — did you mean \`owner\`?`
  or `unknown column \`zzz\` on table \`acct\`; have: \`owner\`, \`bal\``
  when no near-match exists.
- `kessel-sql::P::type_named` now emits did-you-mean OR the educational
  empty-catalog hint ("no tables defined yet — use CREATE TABLE first").
- `kessel-client::bin::kessel` differentiates
  `ConnectionRefused` / `PermissionDenied` / `TimedOut` / fall-through
  on connect failure. Each branch points at the env var / flag that
  actually controls the surface (CLI: `--token` / `$KESSELDB_TOKEN`;
  server: `kesseldb <addr> ./data` and `KESSELDB_TOKEN`).
- `kessel-client::format_result` + `format_result_json` strip the
  duplicative server-side `"sql: "` prefix on SchemaError so users see
  the friendly inner message directly.
- KATs +3: `unknown_table_suggests_near_match`,
  `unknown_column_includes_table_context`, `suggest_helper_basic_shape`.

### T2 — Dockerfile + multi-arch ghcr.io push (DONE 2026-05-30, commits `e52e9da` + `85b8d90`)

- `Dockerfile` at repo root. Two-stage:
  - builder: `rust:1-slim` (latest stable 1.x, matches the runner's
    rustc 1.95+ MSRV in release.yml; pinning to 1.83 broke
    `unsigned_is_multiple_of` in kessel-crypto).
  - runtime: `debian:bookworm-slim` with stripped `kesseldb` + `kessel`
    binaries, README, USAGE, LICENSE, `/data` volume, non-root
    `kessel:1100` UID.
- `.dockerignore` keeps `target/`, `.git/`, `docs/superpowers/`,
  `docs/book/`, `kesseldb-tla/` out of the build context (~25 MiB
  context, not ~700 MiB).
- Default ENTRYPOINT exposes all three wire surfaces:
  `6532` binary, `6533` HTTP+WS, `5432` PostgreSQL.
- `release.yml` gains a `docker` job:
  - QEMU + Buildx multi-arch (`linux/amd64,linux/arm64`)
  - Logs in to `ghcr.io` with the workflow's `GITHUB_TOKEN`
  - Builds + pushes `:<version>`, `v<version>` (both raw + leading `v`),
    AND `:latest` for non-prerelease tags (skipped for tags containing
    `-`, e.g. `v1.1.0-rc1`)
  - Cached via `cache-from: type=gha` so subsequent tag builds reuse
    layers (first build of a clean repo is ~15 min for the arm64
    cross-compile; cached rebuild is ~3 min)
  - Best-effort (`continue-on-error: true`) so a transient registry /
    QEMU blip can't gate the binary release; the existing publish:
    step is unchanged.
- Verified end-to-end on vulcan:
  - `docker build -t kesseldb:dx-smoke .` → exit 0
  - `docker run --rm -d -e KESSELDB_TOKEN=smoketest -p 26532:6532 -p 26533:6533 kesseldb:dx-smoke`
  - HTTP `POST /v1/sql` `CREATE TABLE t (v U64 NOT NULL)` →
    `{"status":"ok","type_id":1}`
  - HTTP `POST /v1/sql` `SELECT COUNT(*) FROM t` →
    `{"status":"ok","value":0}`
  - Image size: **77.2 MiB stripped**.

### T3 — embedded Rust example + EngineHandle::sql (DONE 2026-05-30, commit `33d21c7`)

- `EngineHandle::sql(&str) -> OpResult` — inherent method; equivalent
  to `apply_raw([0xFE] ++ sql)` with a named entry point. The
  `EngineApply::apply_sql` trait method is gated behind
  `--features http-gateway` (it's the gateway-side bridge); embedders
  who do NOT enable that feature get a clean inherent surface.
- `crates/kesseldb-server/examples/embedded.rs` walks the public API
  end-to-end:
  1. Spawn engine with `read_workers = Some(0)` (Perf-A bypass on).
  2. SQL CREATE TABLE + 2× INSERT + SUM via `engine.sql(...)`.
  3. Typed `Op::GetById` via `engine.apply(...)`.
  4. Typed `Op::Create` after building the record with
     `kessel_codec::encode` and `kessel_catalog::ObjectType::from_def`
     (mirrors what `parallel_reads_oracle.rs` does, but as a
     user-facing example).
  5. Hot `engine.snapshot(...)`.
  6. Stats summary.
- Verified on vulcan:
  ```
  $ cargo run --release --example embedded -p kesseldb-server
  → data dir: /tmp/kesseldb-embedded-example-621964
  → creating table via SQL …
     SUM(bal) WHERE owner=100 = 1049
  → direct Op::GetById …
     raw row bytes: 32 bytes
  → typed Op::Create via the codec …
     round-tripped kv row → [Uint(7), Uint(42)]
  → taking on-disk snapshot …
     snapshot dir contains 3 files at /tmp/kesseldb-embedded-example-621964.snapshot
  → stats: applied_ops=6  digest=0x2e94ea3e  uptime=0s  read_pool=0
  ✓ embedded example complete
  ```

## Invariants preserved

- Workspace zero-dep stance: zero new external dependency added to any
  crate. The Dockerfile composes existing release binaries; the
  embedded example only uses already-pinned workspace crates.
- `#![forbid(unsafe_code)]` honored.
- Default `cargo build -p kesseldb-server` byte-identical (the new
  `EngineHandle::sql` is additive).
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched at the
  wire boundary. SchemaError variant + payload bytes are byte-identical
  at the binary boundary; the CLI rewordings happen on the
  client-side text/JSON render path.
- KAT delta +3 (all in kessel-sql) — within the brief's `+0-5` target
  band.

## Deferred follow-ups

- **SP-DX-INIT** — `kessel init <name>` scaffolder: emits a directory
  with `docker-compose.yml`, generated `KESSELDB_TOKEN`, empty data
  dir, README quickstart. Bigger surface than this slice fits.
- **SP-DX-REPL** — `kessel sql` REPL with multi-line editing, history,
  expanded `\?` meta-commands. Adding line-editing requires either a
  feature-gated rustyline dep (breaks the default zero-dep stance) or
  a from-scratch zero-dep readline (substantial). Right shape: a
  feature-gated `cli-readline` flag, designed in its own slice.
- **SP-DX-WIDER-ERROR-AUDIT** — the SQL parser error sites
  (`expected \`,\` or \`)\``, `expected size`, etc.) could include the
  offending token's position (`at column 14: expected \`,\`…`). The
  current parser is char-stream-based with no span tracking; threading
  spans through every parse arm is a larger refactor and deserves its
  own design pass.
