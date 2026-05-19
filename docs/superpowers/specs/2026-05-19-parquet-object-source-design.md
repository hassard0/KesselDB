# KesselDB — Parquet Object Sources (OBJ-2 slice 1 = OBJ-2a): design

**Date:** 2026-05-19  **Status:** design approved under the standing
KesselDB autonomous-build mandate (`feedback_kesseldb_autonomous_build`;
user asleep — decisions made deliberately and documented here). The
brainstorming user-review gate is satisfied by the standing mandate.
Pre-implementation.

A follow-on to the shipped Object-Store External Sources line
(subproject100 / OBJ-1: `s3://`/`az://` JSON/CSV/NDJSON via signed
HTTPS GET). It adds a **pure-Rust columnar Parquet reader** so
`CREATE EXTERNAL SOURCE … FROM 's3://…'|'az://…' FORMAT PARQUET`
materializes rows from a Parquet object — flipping the OBJ-1
CREATE-time rejection ("FORMAT PARQUET over object store is OBJ-2
(not yet shipped)") to **supported**. Every OBJ-1 invariant is
preserved by construction: router-capture → one atomic `Op::Txn` →
replicate → materialize, deterministic `ObjectId`, fail-closed,
secret-reference (env-NAME-only), HTTPS-only / no-bypass, the
deterministic kernel zero-dependency, the default workspace build
pulling no new external deps, seed-7 untouched when the feature is
off.

## 0. Scope & decomposition (Parquet is large — this slice is OBJ-2a)

- **OBJ-2a (this slice):** parse the Parquet footer (`PAR1` magic +
  Thrift-compact `FileMetaData`), iterate **all** row groups, read
  **only the recipe-mapped leaf columns**, decoding **`PLAIN`
  encoding** in **`UNCOMPRESSED`** column chunks, for **flat
  `REQUIRED`** columns of the primitive physical types
  `BOOLEAN | INT32 | INT64 | FLOAT | DOUBLE | BYTE_ARRAY`. Any file
  using dictionary/RLE encoding, any compression codec, any
  `OPTIONAL`/`REPEATED` (definition/repetition levels), or a nested
  schema is **rejected with a precise typed error** naming the
  OBJ-2b/2c follow-on — the honest boundary.
- **OBJ-2b (follow-on):** `RLE`/bit-packed **dictionary** encoding +
  `RLE`-encoded definition levels (→ `OPTIONAL` columns) + **Snappy**
  block decompression (hand-rolled, no codec crate).
- **OBJ-2c (follow-on):** `GZIP`/`ZSTD` codecs; `INT96`/`DECIMAL`/
  logical-type rendering niceties; nested-column skip; statistics /
  predicate pushdown.

Decomposition rationale: a full Parquet implementation is multi-slice
on its own; OBJ-2a delivers end-to-end "read a (PLAIN, uncompressed)
Parquet object" — the realistic first cut a producer can target
(`parquet-tools`/pyarrow can write PLAIN+uncompressed) — and is the
substrate every later Parquet slice extends. Multi-row-group is
**included** in 2a (real files have many; iterating them is cheap
once chunk navigation works).

## 1. Architecture & invariants

The deterministic kernel, WAL, `kessel-sm`, `kessel-vsr`, `kessel-io`,
`kessel-codec`, and the core of `kessel-proto`/`kessel-catalog` are
**untouched** — Parquet is just a third on-object body format,
parallel to JSON/CSV/NDJSON, decoded **router-side** out of the
already-captured object bytes.

New optional crate **`kessel-parquet`** — pure Rust, **zero external
dependencies** (in-tree only; it needs none of kessel-crypto/catalog
for 2a — it is self-contained). It exposes:

```rust
pub enum PqValue { Null, Bool(bool), I64(i64), F64(f64), Bytes(Vec<u8>) }
pub enum PqError { Bad(String), Unsupported(String) }      // + Display
/// Decode the mapped leaf columns from a whole Parquet object.
/// `wanted` = leaf column names in the caller's desired output order.
/// Returns rows × columns (in `wanted` order); REQUIRED flat columns
/// only in 2a.
pub fn extract(bytes: &[u8], wanted: &[&str])
    -> Result<Vec<Vec<PqValue>>, PqError>;
```

`kessel-fetch`'s existing `object-store` cargo feature gains
`dep:kessel-parquet`. `kessel-fetch::Format` gains a `Parquet`
variant. `rows_from_body` gains one arm:

```rust
Format::Parquet => {
    let names: Vec<&str> = cols.iter().map(|c| c.source.as_str()).collect();
    kessel_parquet::extract(body, &names)?
        .into_iter()
        .map(|row| row.into_iter().map(pq_to_cell).collect())
        .collect()
}
```

`pq_to_cell(PqValue) -> json::Cell` maps a Parquet physical value to
the **existing** `Cell` representation the JSON path already produces
for the same logical value — `Null→Cell::Null`, `Bool→Cell::Bool`,
`I64→Cell::Text(itoa)`, `F64→Cell::Text(canonical float, same
formatting JSON numbers use)`, `Bytes→Cell::Text(utf8-lossy)` — so
the **existing `coerce::to_field_bytes` path is reused byte-for-byte
unchanged**. No new coercion logic, no new determinism surface: the
same logical value yields the same `FieldKind` bytes whether it
arrived as JSON or Parquet. (kessel-parquet does NOT depend on
kessel-fetch — no dependency cycle; the `PqValue→Cell` mapping lives
in kessel-fetch.)

`do_refresh` / `do_refresh_objstore` already resolve `recipe.format`
(0 Json / 1 Csv / 2 Ndjson, else `SchemaError "unknown format
code"`). Add `3 => Format::Parquet`. Everything downstream
(deterministic `ObjectId`, atomic `Op::Txn`, fail-closed) is the
unchanged OBJ-1/EXT path. The whole object is fetched once at the
router and decoded once; only the materialized rows enter the
replicated log.

Rejected alternatives: (a) a Parquet module inside `kessel-fetch`
(bloats it; a self-contained ~kLOC format parser is its own
testable unit — the established pattern is a sibling optional crate
like `kessel-objstore`); (b) pulling the `parquet`/`arrow` crates
(violates the zero-external-dependency ethos and is enormous);
(c) streaming/lazy column readers (over-engineered — we materialize
+ concat then one atomic `Txn`, identical to every other format).

## 2. The reader (`kessel-parquet`) — files, each one concern

- `src/lib.rs` — `PqValue`/`PqError` + `pub fn extract` orchestration
  (footer → metadata → per-row-group, per-wanted-column chunk →
  page decode → assemble rows in `wanted` order; arity/row-count
  consistency checks; the 2a support-matrix gate with precise
  `Unsupported(...)` errors).
- `src/thrift.rs` — minimal **Thrift Compact Protocol** decoder
  (varint zig-zag, field-delta headers, the wire types Parquet's
  metadata uses: bool/i32/i64/binary/list/struct). Only what
  `FileMetaData` needs; KAT-pinned against hand-built byte vectors.
- `src/meta.rs` — Parquet `FileMetaData` structs (schema elements,
  row groups, column chunks, `Encoding`/`CompressionCodec`/`Type`
  enums, data-page header) decoded via `thrift.rs`. Maps the flat
  schema's leaf names → column-chunk descriptors.
- `src/plain.rs` — `PLAIN` page decoder per physical type:
  `BOOLEAN` (bit-packed 1-bit), `INT32`/`INT64` (LE), `FLOAT`/
  `DOUBLE` (LE IEEE-754), `BYTE_ARRAY` (4-byte LE len prefix + bytes)
  → `Vec<PqValue>`. Bounded reads (every length/offset checked
  against the slice; a malformed file ⇒ `PqError::Bad`, never a
  panic).
- `src/footer.rs` — `PAR1` magic header+trailer check, trailing
  `[u32 LE metadata_len][PAR1]` → the Thrift `FileMetaData` byte
  range; size/sanity bounds (reject absurd `metadata_len`).

The crate `#![forbid(unsafe_code)]`. No `unwrap`/`expect`/panic on
file-controlled bytes — every decode step returns `Result`.

## 3. Support matrix & fail-closed errors (OBJ-2a)

Decoded from `FileMetaData`, before any page is read:

- physical type ∈ `{BOOLEAN,INT32,INT64,FLOAT,DOUBLE,BYTE_ARRAY}`
  for every **wanted** column; other types (`INT96`, `FIXED_LEN…`)
  ⇒ `Unsupported("INT96/FIXED_LEN_BYTE_ARRAY: OBJ-2c")`.
- every wanted column's `repetition_type == REQUIRED` and the
  schema is flat (one level of leaves); any `OPTIONAL`/`REPEATED`
  or nested group on a wanted column ⇒ `Unsupported("OPTIONAL/
  REPEATED/nested columns: OBJ-2b")`.
- every wanted column chunk: `codec == UNCOMPRESSED` (else
  `Unsupported("compression <X>: OBJ-2b/2c")`) and the only data-
  page encoding is `PLAIN` (dictionary/`RLE`/`DELTA_*` ⇒
  `Unsupported("<enc> encoding: OBJ-2b")`).
- a recipe-mapped `source` name absent from the file schema ⇒
  `Bad("column `<name>` not found in Parquet schema")`.
- all wanted columns must report the **same total value count**
  across row groups (a flat REQUIRED file guarantees this; a
  mismatch ⇒ `Bad("column length mismatch")`).
- only **V1 data pages** (`PageType::DATA_PAGE` with
  `DataPageHeader`), `PLAIN` encoding, `UNCOMPRESSED`. **V2 data
  pages** (`DATA_PAGE_V2`) ⇒ `Unsupported("Parquet V2 data pages:
  OBJ-2b")`; an index/dictionary page where a data page was
  expected ⇒ the dictionary `Unsupported`. (The checked-in fixtures
  are written with the V1 data-page version + `use_dictionary=False`
  + `compression=None` — the documented producer settings 2a
  targets; this is the minimal unambiguous first cut.)

Any `PqError` ⇒ `kessel-fetch` maps it to `FetchError::Parse(...)`
⇒ `do_refresh` returns `OpResult::SchemaError("refresh: …")` and
submits **nothing** (OBJ-1 all-or-nothing abort, unchanged). The
error text carries only the parse reason / column name — never
object bytes or credentials.

## 4. SQL surface (flip the OBJ-1 rejection)

`kessel-sql` `CREATE EXTERNAL SOURCE`: `FORMAT PARQUET` is already
parsed (format code `3`). OBJ-1 rejects it. OBJ-2a changes the
validation so `format == 3` is **accepted iff** the URL is
`s3://` or `az://`. Still rejected (clear messages, CREATE-time):

- `FORMAT PARQUET` with an `http://`/`https://` source ⇒
  `"FORMAT PARQUET is only supported for object-store (s3://|az://)
  sources"` (Parquet over plain HTTP is an OBJ follow-on; keep the
  boundary honest).
- `FORMAT PARQUET` with any `PAGE …` clause ⇒ `"PAGE clauses are
  not supported with FORMAT PARQUET"` (a Parquet object is one
  object; pagination is OBJ-4 listing territory).
- `FORMAT PARQUET` with a `ROWS '<path>'` clause ⇒ `"ROWS is not
  applicable to FORMAT PARQUET"` (no JSON envelope).

The `ColumnMap.source` for a Parquet column is the **flat leaf
column name** in the Parquet schema (same slot JSON uses for a
dotted path / CSV uses for a header) — documented in USAGE.

## 5. Determinism, security, backward-compat

- **Determinism:** Parquet decode is a pure function of the object
  bytes (captured once at the router, identical on every replica —
  exactly the JSON/OBJ-1 boundary). `pq_to_cell` renders each value
  to the same `Cell` the JSON path would for that logical value, so
  `coerce::to_field_bytes` produces byte-identical `FieldKind` bytes
  → identical `ObjectId`/codec record → identical replicated `Txn`.
  No floating-point nondeterminism is introduced: `F64` is rendered
  with the existing deterministic number formatting the JSON `Cell`
  path already uses (no locale, no platform `printf`).
- **Security:** unchanged from OBJ-1 — HTTPS-only / webpki full-
  verify / no bypass; secret-reference env-NAME-only; the Parquet
  bytes are attacker-influenceable (operator-declared object), so
  the reader is hardened: every offset/length bounds-checked,
  `#![forbid(unsafe_code)]`, no panic path, hard caps (reject
  `metadata_len` > a sane bound and a row-count/value-count ceiling
  to bound memory; reuse `kessel-fetch`'s existing `DEFAULT_MAX_BODY`
  for the object size — the object is already capped by the fetch
  path). A pentest task locks: malformed/truncated footer, lying
  `metadata_len`, oversized `BYTE_ARRAY` length, value-count
  overflow ⇒ typed `PqError`, never panic/OOM.
- **Backward-compat:** purely additive. `Format` gains a variant
  (a Rust enum addition — `rows_from_body` is exhaustive, all arms
  updated); no on-disk catalog/proto/WAL format changes (format
  code `3` already exists in the recipe/op wire and was previously
  rejected only at the SQL layer + `do_refresh` "unknown code" —
  now mapped). Existing EXT/TLS/OBJ-1 oracles must stay green
  unchanged. seed-7 untouched (feature-off ⇒ kessel-parquet not
  compiled).

## 6. Testing

- `kessel-parquet` unit (KAT): hand-built Thrift-compact byte
  vectors for the metadata structs (varint/zigzag/field-delta);
  `PLAIN` page decode per physical type from hand-built bytes;
  footer parse incl. malformed/truncated/lying-`metadata_len`
  (→ typed error, no panic); the support-matrix rejections
  (dictionary/compressed/OPTIONAL/INT96/missing-column → the exact
  `Unsupported`/`Bad`).
- **Checked-in tiny fixtures**: 1–2 minimal real Parquet files
  (PLAIN, UNCOMPRESSED, flat REQUIRED, a few rows, mixed primitive
  types; one with multiple row groups) under
  `crates/kessel-parquet/tests/fixtures/`, with a `README`
  documenting the exact `pyarrow`/`parquet-tools` regen command and
  the expected logical rows (fixed bytes ⇒ no test-time toolchain;
  same model as the SP99 TLS fixture). End-to-end `extract()` over
  the fixture asserts the exact `PqValue` rows + the mapped-subset/
  column-order behavior.
- `kessel-fetch` (`#[cfg(feature="object-store")]`): `rows_from_body`
  with `Format::Parquet` over the fixture bytes ⇒ the expected
  coerced rows (proves `pq_to_cell` + the reused coerce path).
- `kessel-sql`: parse tests — `FORMAT PARQUET` accepted for
  `s3://`/`az://`; rejected for `http(s)://`, with `PAGE`, with
  `ROWS` (the three messages).
- Server e2e (`#[cfg(feature="external-sources-objstore")]`):
  mirror the OBJ-1 `external_source_objstore_oracle` — a localhost
  stub serves the Parquet fixture bytes; production webpki-roots
  rejects the self-signed localhost cert so REFRESH is **fail-
  closed** (typed `SchemaError`, prior state intact), exactly the
  SP100 precedent. The trusted decode happy-path is proven at the
  `kessel-fetch` layer over the fixture (not via a router bypass —
  no fixture trust injected into production).
- **Gate:** `cargo test --workspace --release` FAILED=0 + seed-7
  (`large_seed_corpus_is_deterministic_and_converges`) green. The
  new `kessel-parquet` crate is a workspace member so its unit
  tests run under `cargo test --workspace` — the default-build
  total rises (honest, NOT a zero delta; same accounting as
  SP100's kessel-objstore). The kernel pulls no new external dep;
  `cargo tree` default stays rustls/objstore/parquet-free; the
  plan records the exact measured new total and the docs reconcile
  it honestly.

## 7. Non-goals (explicit, kept honest in docs & at CREATE)

Dictionary/RLE/DELTA encodings, ALL compression codecs, OPTIONAL/
REPEATED/nested columns, definition/repetition levels, `INT96`/
`FIXED_LEN_BYTE_ARRAY`/`DECIMAL` logical rendering, statistics/
predicate pushdown, column-index/bloom-filter, encryption,
Parquet over plain `http(s)://` (object-store only), pagination/
multi-object (OBJ-4), streaming. Each is rejected at CREATE or with
a typed `PqError` naming the OBJ-2b/2c follow-on — never silently
mis-decoded.

## 8. Process note (autonomous mandate)

Per `feedback_kesseldb_autonomous_build` the user is unavailable and
delegated decisions; the brainstorming user-review gate is satisfied
by the standing mandate. Implementation still goes through
`writing-plans` → `subagent-driven-development` with the full
two-stage (spec-compliance then code-quality) review per task + a
final whole-implementation review. KesselDB non-negotiables hold:
zero-dep deterministic kernel, seed-7 green, honest docs, single-
branch commits straight to `main` (no Co-Authored-By, no signing,
matching `git log` style), full `cargo test --workspace --release`
each kernel-adjacent task, pentest pass on the new attacker-facing
parser.
