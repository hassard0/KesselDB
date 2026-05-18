# KesselDB — External Sources (EXT), slice 1: design

**Date:** 2026-05-18  **Status:** design approved, pre-implementation.

This is the first sub-project of a larger strategic decomposition
(see "Context & decomposition"). It designs **EXT slice 1**:
registered external JSON/CSV-over-HTTP sources, materialized into
normal KesselDB types via an explicit, replicated `REFRESH`.

## Context & decomposition

A strategic direction was proposed: decouple storage from runtime /
run from object storage; read JSON/CSV over APIs; compile the
expr-VM to WASM; take inspiration from Apache Iceberg. These are
**3–4 independent sub-projects**, each its own spec → plan → build:

- **OBJ** — object-store-backed `Vfs` + stateless readers +
  Iceberg-style snapshots (foundational; mostly zero-dep core).
- **EXT** — external JSON/CSV foreign sources over HTTP (this doc).
- **WASM** — reframed: an *offline* AOT compiler (guest → expr-VM,
  stays deterministic) or a fenced, gas-metered optional UDF crate.
  Highest design risk; a later research spike.

**North-Star stance (decided):** the kernel (`kessel-sm`,
`kessel-storage`, `kessel-vsr`) stays **zero-external-dependency and
bit-for-bit deterministic**. Optional, off-by-default cargo-feature
crates *may* pull networking/TLS (and later a wasm runtime); they
are explicitly "not the pure kernel." The seed-7 corpus and the
determinism digest must remain unaffected when the feature is off.

**Order (decided):** EXT is designed and built first (the headline
"read JSON/CSV over APIs" capability), then OBJ, then WASM.

## 1. Architecture & North-Star fences

- Kernel crates gain **no external I/O and no nondeterminism**.
  External I/O never enters the replicated state machine; only
  *captured rows* do, as ordinary `Create`/`Update` ops. The one
  kernel-side change is a **backward-compatible, deterministic**
  catalog field (the external recipe, §2) — the same kind of
  additive change SP86 made for column defaults; it does not affect
  the digest for existing types or the seed-7 corpus.
- New optional crate **`kessel-fetch`** behind cargo feature
  **`external-sources`** (default off): pure-Rust HTTP/1.1 client +
  JSON/CSV parsers. Feature-off ⇒ the engine is byte-identical to
  today; corpus/seed-7/digest unaffected (the crate is not
  compiled).
- The **router** (`kesseldb-server::router`) is the only seam: it
  gains a `REFRESH` handler that calls `kessel-fetch`, then submits
  a deterministic upsert batch through the existing replicated-log
  path (the same path cross-shard txns use).

## 2. SQL surface & catalog recipe

```
CREATE EXTERNAL SOURCE <name> (
    <col> <TYPE> FROM '<json.dotted.path>' | CSV '<header-name>',
    ...
) FROM '<url>' FORMAT JSON|CSV KEY <col>
  [AUTH BEARER ENV '<VARNAME>' | HEADER '<H>' ENV '<VARNAME>']

REFRESH <name>
DROP EXTERNAL SOURCE <name>
```

`CREATE EXTERNAL SOURCE` creates a **normal KesselDB type** named
`<name>` with the declared columns, plus a catalog "external recipe"
record: `url`, `format`, per-column source mapping, the `KEY`
column, and an auth **reference** (`ENV 'NAME'` — never a value).
`SELECT … FROM <name>` is a plain select over that type, so all
existing SQL / index / scatter machinery applies unchanged. The
recipe is persisted via the existing backward-compatible catalog
trailer mechanism (the technique SP86 used for column defaults), so
catalog decode for all existing callers is untouched.

## 3. REFRESH execution (router op)

1. Router loads the recipe from the catalog (catalog is global —
   one `Describe`-class read).
2. Resolves the auth secret from **its own** environment
   (`ENV 'NAME'`) — never logged, never placed in an op.
3. `kessel-fetch` performs **one** HTTP GET (bounded body size,
   default 64 MiB, configurable; oversize ⇒ error, prior data
   intact); parses the whole body.
4. Each parsed record → declared columns → a codec record; the
   row's `ObjectId` is derived from the `KEY` value (§4).
5. Router submits the result as **one atomic batch** (`Op::Txn` of
   upserts, reusing the existing cross-shard-aware path) into the
   replicated log. Replicas replay the *captured* rows identically
   ⇒ determinism preserved.

The fetch happens exactly once, on the router; only its result is
replicated. A failed/partial fetch submits **nothing**
(all-or-nothing per refresh).

## 4. Row identity & upsert

- `ObjectId` = first 16 bytes of a domain-separated hash
  (`kessel-crypto`) of the canonicalized `KEY` value. Deterministic
  and stable across refreshes: the same upstream key always maps to
  the same row.
- Upsert = create-if-absent, else update, at that id. Realized by
  the router resolving each row to a `Create` (id absent) or
  `Update` (id present) via a point existence check, batched into
  the single `Op::Txn`; whether to instead add a dedicated
  idempotent upsert op is an implementation-plan decision, not a
  design variable. Re-`REFRESH` is idempotent for unchanged rows
  (same id + same record ⇒ identical effect; consistent with the
  SP94 monotonic-op discipline).
- **Slice 1 = upsert only.** Rows deleted upstream are *not*
  auto-pruned (documented). `REFRESH … MODE REPLACE`
  (prune/replace-all) is an explicit follow-on slice.

## 5. Parsing & schema mapping

- **Explicit mapping only.** No inference (fragile,
  nondeterministic across fetches, collides with strong typing).
  JSON: array-of-objects, dotted path to a scalar. CSV: RFC 4180,
  by header name.
- Each value is parsed into the column's declared KesselDB type
  (U*/I*/CHAR(n)/BYTES(n)/Bool/Timestamp). A value that does not fit
  ⇒ **typed parse error; REFRESH aborts; prior data intact**.
- Nested objects/arrays as a column value, NDJSON, and pagination
  are **non-goals for slice 1** (documented; clean follow-ons).

## 6. Transport & TLS

- `kessel-fetch` slice 1: pure-Rust **HTTP/1.1** (GET, headers,
  chunked decode, redirects off by default).
- **TLS decision for slice 1: plain-HTTP + a documented
  TLS-terminating sidecar/proxy.** A from-scratch or vendored
  minimal TLS stack is a large, security-sensitive effort; it is
  deferred to its own slice behind the same feature. Slice 1 stays
  small and ships no unaudited crypto.

## 7. Failure modes & limits

DNS/connect failure, HTTP status ≠ 2xx, body-too-large, parse/type
error, missing or duplicate `KEY`, missing env secret → each
returns a clear `OpResult::SchemaError`/`Constraint` to the caller
and **mutates nothing**. Refresh is atomic at the log level (one
`Op::Txn`).

## 8. Determinism & consistency boundary (documented, not faked)

A source reflects only its **last successful `REFRESH`**; queries
read the materialized snapshot, never live upstream; `REFRESH` is
the sole point external data enters — captured once, then
replicated. This boundary is stated wherever external sources are
documented, exactly like the scatter "no cross-shard snapshot" and
cross-shard-transaction boundaries.

## 9. Testing / oracle strategy

- `kessel-fetch` unit tests: JSON/CSV parse + type coercion + every
  error case, against an in-process stub HTTP server (no real
  network; deterministic).
- Router oracle: a stub source serving fixed bytes → `CREATE
  EXTERNAL SOURCE` + `REFRESH` → assert resulting rows == an
  independent model; idempotent re-`REFRESH` (digest unchanged);
  changed-row upsert; abort-on-bad-row leaves prior data intact.
- Feature-off build: the full existing suite stays green, seed-7
  intact (crate not compiled).

## 10. Slice-1 scope vs. follow-ons

**In:** `CREATE`/`DROP EXTERNAL SOURCE`, `REFRESH` (upsert),
JSON-array + CSV, explicit flat mapping, env-reference auth, HTTP,
atomic refresh, the oracle.

**Follow-ons (each its own slice):** TLS in `kessel-fetch`;
`REFRESH … MODE REPLACE` / prune; NDJSON / pagination / nested
extraction; scheduled auto-refresh; the OBJ object-store substrate;
WASM.

## Non-goals (explicit)

Live per-query federation (breaks determinism — rejected by
design); schema inference; cross-source joins as a special case
(ordinary SQL joins over materialized types already work); secrets
stored in the catalog (only env-references are persisted);
in-engine external I/O (router-only).
