# KesselDB — Subproject 105: OBJ-2b-4 Parquet OPTIONAL/nullable columns

**Date:** 2026-05-19  **Status:** done — code + tests committed and passing.

Builds on:
- Subproject 100 — Object-store external sources (OBJ-1):
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject100-objstore.md`
- Subproject 101 — Parquet OBJ-2a:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject101-parquet.md`
- Subproject 102 — RLE/bit-packing hybrid:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject102-rle.md`
- Subproject 103 — Parquet dictionary encoding:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject103-dict.md`
- Subproject 104 — Parquet Snappy decompression:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject104-snappy.md`

Design document:
`docs/superpowers/specs/2026-05-19-parquet-optional-design.md`

Plan document:
`docs/superpowers/plans/2026-05-19-parquet-optional.md`

---

## What shipped

`kessel-parquet::extract()` now decodes flat **OPTIONAL (nullable)**
Parquet columns via V1 definition levels — pyarrow's true default
`pq.write_table(df)` (OPTIONAL + dictionary + Snappy, V1, with NULLs)
now reads with **zero special flags**. This is the capstone of the
OBJ-2b arc.

- **`meta.rs` flat-schema detection** — `FileMetaData.flat_schema: bool`
  (true iff the schema node list is root-group + leaves only, no
  intermediate groups). New `SchemaNode::{Leaf, Group}` enum replaces
  the prior silent group-drop; `leaves` population unchanged for all
  consumers.
- **`lib.rs` `max_def_level` + gate flip** — per-leaf `max_def_level`
  computed from `Repetition`: `Required → 0`, `Optional → 1`,
  `Repeated/Other → Unsupported(OBJ-2c)`. The prior blanket
  OPTIONAL/REPEATED gate is replaced by the `max_def_level` dispatch.
  A new flat-schema guard rejects non-flat files up front
  (`Unsupported("nested schema: OBJ-2c")`).
- **`decode_page` null-scatter** — free fn threading `max_def_level`
  through the per-page decode. For `max_def_level == 0` (REQUIRED):
  byte-identical to the prior path. For `max_def_level == 1`
  (flat OPTIONAL): decodes the 4-byte-length-prefixed RLE/bit-packing
  def-level stream via the existing SP102 `rle::decode_level_v1`
  (unchanged, reused as-is), decodes only the present values from the
  remaining payload, then scatters `PqValue::Null` for absent rows.
- **REQUIRED path byte-unchanged** — all prior REQUIRED tests pass
  unmodified; the REQUIRED code path in `decode_page` is a direct
  pass-through.

**Supported matrix after OBJ-2b arc:**

| Axis | Supported |
|---|---|
| Schema shape | Flat (root group + leaves only) |
| Column repetition | REQUIRED or OPTIONAL |
| Compression | UNCOMPRESSED, SNAPPY (raw block; pages ≤ 64 MiB) |
| Encoding | PLAIN, dictionary (PLAIN_DICTIONARY / RLE_DICTIONARY) |
| Data page version | V1 (`DATA_PAGE`) |
| Null handling | `PqValue::Null` for OPTIONAL def-level 0 rows |

**This is vanilla `pq.write_table(df)` with zero flags — OBJ-2b arc
COMPLETE.**

---

## Latent OBJ-2a flat-schema tightening (honest disclosure)

The new flat-schema guard (`!md.flat_schema →
Unsupported("nested schema: OBJ-2c")`) **tightens a latent OBJ-2a
behavior**: previously, `meta.rs::decode_schema_element` silently
dropped intermediate group nodes (returning `None` for any element with
`num_children > 0`), so a leaf under a nested group would be treated as
a top-level leaf. This was harmless for REQUIRED flat files (all real
pyarrow fixtures are flat), but would mis-compute `max_def_level` for a
nested OPTIONAL leaf — a silent correctness hazard for OBJ-2c schemas
reaching this code path.

The new code is validated **non-self-referentially**: all real pyarrow
fixtures (`golden.parquet`, `dict_int64.parquet`, `snappy_dict.parquet`,
`snappy_plain.parquet`, `nullable.parquet`, `nullable_plain.parquet`)
are flat and still round-trip through `extract()`. The nested-schema
reject is covered by a dedicated KAT (`flat_schema_true_for_root_plus_leaves_false_for_nested_group`
in `meta.rs` and `extract_rejects_nested_schema_obj2c` in `lib.rs`).

This is the same disclosure discipline as SP104's PageHeader field-ID
fix: the prior code was self-consistently wrong in a way the
hand-built-bytes tests could not catch; the fix is validated by
real-fixture round-trips.

---

## Verification

- **Spec KATs** (hand-derived from parquet-format Encodings.md / the
  V1 page-payload layout):
  - OPTIONAL PLAIN `[7, null, -2]`: defs `[1,0,1]`, present `[7,-2]` →
    `[I64(7), Null, I64(-2)]`.
  - All-null page (defs all 0, no present values).
  - All-present page (defs all 1, no nulls).
  - OPTIONAL + dictionary: `[7, null, 7]` via dict `[7]`, data-page
    def-stream + bit-packed index 0 → `[I64(7), Null, I64(7)]`.
  - `rle::decode_level_v1` is SP102-KAT'd (unchanged, reused as-is);
    here the test coverage is the null-scatter correctness (correct
    placement of `Null` vs decoded values).
- **Real pyarrow fixtures** (pyarrow 24.0.0):
  - `nullable.parquet` — VANILLA default (`pq.write_table(df)`, no
    flags): OPTIONAL + dictionary + Snappy, with NULLs. Round-trips via
    production `extract()`.
  - `nullable_plain.parquet` — OPTIONAL + PLAIN + UNCOMPRESSED, with
    NULLs. Round-trips via production `extract()`.
  - Expected rows: `id=[7,7,null,-2,100]`, `s=["a",null,"b","c","a"]`.
  - Both fixtures metadata-verified (OPTIONAL schema, correct
    compression/encoding) before being committed.
- **e2e fail-closed** — `refresh_nullable_parquet_from_s3_fails_closed_and_state_intact`
  oracle via the SP101 `tls_stub_with_fixture` harness pointed at
  `nullable.parquet`; REFRESH returns a typed error, prior materialized
  data intact.
- **Source-format-independence pin** — `pq_to_cell(PqValue::Null)`
  produces the same `Cell` a JSON `null` produces; `coerce::to_field_bytes`
  yields identical bytes for both. The same `coerce` path used by the
  JSON decoder is reused unchanged for Parquet OPTIONAL nulls — the
  SP101 source-format-independence invariant is maintained for the null
  case.
- **Pentest** (9 `catch_unwind` locks — no panic, no OOM, typed errors):
  - Truncated def-level stream → `Err(Bad)`.
  - Lying 4-byte length prefix → `Err(Bad)`.
  - def-level value > 1 (prefix-undercount, distinct from past-EOF) →
    `Err(Bad("definition level exceeds max"))`.
  - Value section shorter than present count → `Err(Bad)`.
  - OPTIONAL + dict with out-of-range index → `Err(Bad)`.
  - Non-flat schema (root→group→leaf) → `Err(Unsupported)` (no panic).
  - Positive correctness locks: all-null page → all `PqValue::Null`;
    all-present page → no nulls; mixed `[1,0,1,1,0]` scatter → exact
    placement.

---

## Intended behavior change (reviewed — NOT a regression)

The test `extract_rejects_optional_repetition` (which asserted that
OPTIONAL columns are rejected with `Unsupported`) is intentionally
replaced by:

- `extract_decodes_optional_int64_with_nulls` — OPTIONAL now decoded.
- `extract_rejects_repeated_obj2c` — REPEATED still `Unsupported(OBJ-2c)`.
- `extract_rejects_nested_schema_obj2c` — non-flat schema still
  `Unsupported(OBJ-2c)`.

All other OBJ-2a / OBJ-2b tests are unchanged (REQUIRED path
byte-identical; existing flat REQUIRED files still round-trip).

---

## Honest gate accounting

Default-build total: **348 → 365** (+17) — new meta flat-schema test,
OPTIONAL extract/fixture/e2e/source-indep/pentest tests, minus 1
intentionally-removed optional-reject test. **NOT a zero-delta** (same
corrected stance as SP100–104). (Individual `#[test]` functions contain
multiple assertions; the +17 is the measured `cargo test --workspace
--release` delta, the authoritative figure.)

Invariants that hold:
- Deterministic kernel pulls no new external dependency.
- `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty.
- Default `cargo tree -p kesseldb-server` links no
  parquet/objstore/rustls/webpki.
- `large_seed_corpus_is_deterministic_and_converges` green (seed-7).
- EXT/TLS/OBJ-1 oracles (2/1/1) unchanged.
- REQUIRED path byte-unchanged; all OBJ-2a/2b REQUIRED tests pass.

---

## Deferred (OBJ-2c)

- **REPEATED columns / repetition levels** — `max_rep_level > 0`,
  LIST/MAP logical types.
- **Nested / optional groups** — intermediate group nodes with
  `max_def_level > 1` (multi-level definition levels).
- **Compression** — gzip, zstd, lz4, brotli.
- **Physical types** — `INT96`, `FIXED_LEN_BYTE_ARRAY`, `DECIMAL`.
- **V2 data pages** (`DATA_PAGE_V2`).
- **Large Snappy pages** — decompressed size > 64 MiB.
