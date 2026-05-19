# KesselDB — Subproject 107: OBJ-2c-3 Parquet V2 data pages

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
- Subproject 105 — Parquet OPTIONAL/nullable columns:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject105-optional.md`
- Subproject 106 — Parquet GZIP page decompression:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject106-gzip.md`

Design document:
`docs/superpowers/specs/2026-05-19-parquet-v2pages-design.md`

Plan document:
`docs/superpowers/plans/2026-05-19-parquet-v2pages.md`

---

## What shipped

`kessel-parquet` now decodes `DATA_PAGE_V2` Parquet data pages (pyarrow
`data_page_version='2.0'`). No external crate is added; no server, kernel,
or SQL layer is changed.

- **`meta.rs` field-8 `DataPageHeaderV2`** — new Thrift struct decoded
  alongside the existing `DataPageHeader` (field 2 = V1, field 8 = V2).
  The `PageHeader` enum gains a `DataV2` variant carrying
  `DataPageHeaderV2` (num_values, num_nulls, num_rows, encoding,
  definition_levels_byte_length, repetition_levels_byte_length,
  is_compressed, statistics). `PageType` enum gains `DataPageV2 = 3`.

- **`lib.rs` `decode_data_page_v2`** — new function invoked when
  `page_type == 3`. The V2 layout splits the page payload into three
  contiguous sections: `[rep_levels (raw, uncompressed)][def_levels (raw,
  uncompressed)][value-section (optionally compressed)]`. The key design
  difference from V1: the level bytes are **never compressed** — they are
  passed through raw, before `page_payload` decompression. Only the
  value-section is decompressed (via the existing `page_payload` seam with
  `definition_levels_byte_length` as the split boundary). This is NOT the
  whole-page `page_payload` seam used by V1; the raw-level split happens
  first.

- **Shared `scatter_nulls`** — the null-scatter helper (def-level 0 →
  `PqValue::Null`) is now shared between V1-OPTIONAL and V2. The V1
  OPTIONAL path is byte-identical to before: the refactor extracted the
  function without changing its behavior, so all OBJ-2a/2b/2c-1 OPTIONAL
  tests pass unchanged.

- **`page_type == 3` gate flip** — the `lib.rs` decode dispatch now routes
  `PageType::DataPageV2` to `decode_data_page_v2` instead of returning
  `Unsupported("non-V1 data page (V2/index): OBJ-2c")`.

**Supported matrix after OBJ-2c-3:**

| Axis | Supported |
|---|---|
| Schema shape | Flat (root group + leaves only) |
| Column repetition | REQUIRED or OPTIONAL |
| Compression | UNCOMPRESSED, SNAPPY (raw block; ≤ 64 MiB), or GZIP (RFC 1952; pages ≤ 64 MiB decompressed) |
| Encoding | PLAIN, dictionary (PLAIN_DICTIONARY / RLE_DICTIONARY) |
| Data page version | **V1 and V2** (`DATA_PAGE` and `DATA_PAGE_V2`) |
| Null handling | `PqValue::Null` for OPTIONAL def-level 0 rows (V1 and V2) |

---

## Resequencing & T1 disclosures

**OBJ-2c-2 zstd resequenced/deferred:** The original OBJ-2c plan ordered
zstd (codec 6) before V2 data pages. That ordering was reversed: V2 pages
are more broadly exercised by pyarrow's default writer (any file written
with `data_page_version='2.0'`), whereas zstd requires an explicit codec
flag and is relatively rare in practice. Delivering V2 first maximises
compatibility with real-world Parquet files sooner. OBJ-2c-2 (zstd) is
deferred, not dropped — it is tracked as the next OBJ-2c arc objective.

**T1 behavior-preserving e2e-helper extraction:** T1 of this slice
extracted a shared `run_fail_closed_parquet_e2e(...)` helper from the five
near-identical fail-closed e2e functions accumulated across SP101/SP104/
SP105/SP105-nullable/SP106. This was the SP106-tracked follow-on task (see
SP106's "Tracked follow-up" section): "Do this at the next e2e addition
(OBJ-2c-2)." The extraction was deliberately brought forward to T1 of this
slice (rather than deferred again) as a behavior-preserving refactor — all
5 prior e2e observable assertions are preserved byte-for-byte (same inputs,
same error variant assertions, same prior-state-intact checks). The T1
delta is **net-0** to the default-build test count (the helper is not itself
a test; it reduces duplication in the existing tests without removing any
assertion).

**Gate-caught V1-ordering regression fix:** During T3 development, a V1
byte-identity defect was introduced mid-slice (the shared `scatter_nulls`
refactor initially mis-ordered a step for the V1 path). The gate caught
this via the existing V1-OPTIONAL regression tests. A permanent regression
KAT (`v1_check_order_num_values_before_comp_size_unchanged`) was added to lock the
correct V1 output order permanently. This is an honest disclosure: the gate
is working as intended, the defect was corrected before any commit to main,
and the regression test ensures it cannot recur silently.

---

## Verification

### Hand-derived V2 KATs (from parquet.thrift)

V2 data pages have a distinct binary layout specified in `parquet.thrift`.
The following KATs were derived directly from the spec, not from pyarrow
output, providing a non-self-referential basis:

- **PLAIN REQUIRED V2** — a minimal `DataPageHeaderV2` for INT64 REQUIRED
  column: `num_values=2`, `num_nulls=0`, `num_rows=2`,
  `definition_levels_byte_length=0`, `repetition_levels_byte_length=0`,
  `is_compressed=false`. Value section = two raw INT64 LE 8-byte values.
  Verified the decoder emits the correct `[I64(a), I64(b)]` without
  invoking decompression.

- **PLAIN OPTIONAL V2 `[7, null, -2]`** — `DataPageHeaderV2` with
  `num_values=3`, `num_nulls=1`, `definition_levels_byte_length=N`
  (the raw, non-compressed def section is NOT 4-byte-length-prefixed in
  V2, unlike V1's RLE level encoding). The def section uses the V1
  RLE/bit-packing encoding but is placed raw before the value section.
  The decoder correctly reads `def_len` bytes raw, decodes them via
  `rle::decode_level_v1`, then decompresses only the value section.

- **V2 + dict** — a dictionary-page followed by a `DATA_PAGE_V2` index
  page: `DataPageHeaderV2` with `PLAIN_DICTIONARY` encoding,
  `is_compressed=false`. Verifies the dict ∘ V2 composition path.

### Real pyarrow V2 fixtures (pyarrow 24.0.0, metadata-verified genuine DATA_PAGE_V2)

All four fixtures were verified before commit using pyarrow's metadata
API to confirm `data_page_header_v2` is present (not `data_page_header`):

- **`v2_plain.parquet`** — `data_page_version='2.0'`, PLAIN encoding,
  UNCOMPRESSED. Flat REQUIRED `id` (INT64) + `s` (BYTE_ARRAY). Round-trips
  via production `extract()`. V2 × Uncompressed exercised.

- **`v2_dict.parquet`** — `data_page_version='2.0'`, dictionary encoding,
  UNCOMPRESSED. Flat REQUIRED. Round-trips via production `extract()`.
  V2 × dict × Uncompressed exercised.

- **`v2_gzip.parquet`** — `data_page_version='2.0'`, PLAIN encoding,
  `compression='gzip'`. Flat REQUIRED. Round-trips via production
  `extract()`. **V2 × GZIP exercised** — the raw-level split happens
  before gzip decompression of the value section.

- **`v2_nullable.parquet`** — `data_page_version='2.0'`, default encoding
  (dict + SNAPPY). Flat OPTIONAL (nullable rows). Round-trips via
  production `extract()`. **V2 × SNAPPY × OPTIONAL exercised** — the
  raw def-level section is read before Snappy decompression of the value
  section; `scatter_nulls` scatters nulls correctly.

V2 is exercised across all three supported compression codecs:
Uncompressed (v2_plain, v2_dict), Snappy (v2_nullable), and GZIP (v2_gzip).

### Source-format-independence pin

`extract_v2_v1_source_independence_pin`: the same logical values extracted
from a V2-paged file and a V1-paged file (both PLAIN, REQUIRED,
UNCOMPRESSED) are byte-identical. The same `coerce::to_field_bytes` path
is reused — the SP101 invariant holds for V2 pages.

### e2e fail-closed (6th, via shared helper)

`refresh_v2_parquet_from_s3_fails_closed_and_state_intact`: a V2 Parquet
file via the `tls_stub_with_fixture` harness (same style as SP101/SP104/
SP105/SP106 e2e oracles). REFRESH returns a typed error when the server
rejects the request; prior materialized data remains intact. This is the
6th fail-closed e2e oracle and uses the shared `run_fail_closed_parquet_e2e`
helper extracted in T1.

### Pentest — `mod pentest_v2` (17 locks; no vuln found)

All 17 pentest cases run under `catch_unwind`; no panic, no OOM, typed
errors only:

- **Lying `def_len` (underflow):** `definition_levels_byte_length` larger
  than the available payload → `Bad`.
- **`rep_len > 0` (OBJ-2c-5 scope):** `repetition_levels_byte_length > 0`
  → `Unsupported("REPEATED columns: OBJ-2c")` — correct gating; REPEATED
  V2 is not decoded.
- **Value-section truncation:** `def_len` split leaves too few bytes for
  the value section → `Bad`.
- **`def > 1` (invalid for flat schema):** def-level value > max_def_level
  in the level stream → `Bad`.
- **`num_nulls` cross-check mismatch:** `num_nulls` in the header claims
  N but the def-level stream produces a different null count → `Bad`.
- **Raw-len mismatch:** value-section byte count inconsistent with
  `num_values` after null scatter → `Bad`.
- **Truncated page (no data):** empty payload with `num_values > 0` → `Bad`.
- **V2 + GZIP corrupt:** valid-looking `DataPageHeaderV2` + corrupt gzip
  bytes in the value section → `Bad` (gzip::decompress error).
- **Positive locks (8):** V2 PLAIN REQUIRED (2 values), V2 PLAIN OPTIONAL
  `[7, null, -2]`, V2 dict REQUIRED, V2 `is_compressed=false` explicit,
  v2_plain fixture roundtrip, v2_dict fixture roundtrip, v2_gzip fixture
  roundtrip, v2_nullable fixture roundtrip — all assert `Ok(exact_rows)`.

---

## Honest gate accounting

Default-build total: **397 → 425** (+28) — new V2 KATs (PLAIN REQUIRED,
PLAIN OPTIONAL, dict) + meta field-8 test + extract decode tests
(v2_plain/v2_dict/v2_gzip/v2_nullable roundtrips + source-independence pin)
+ V1-ordering regression KAT + e2e fail-closed (6th) + 17 pentest_v2
locks. **NOT a zero-delta** (same corrected stance as SP100–106; the
per-slice +28 is the authoritative figure per the tracked nit). T1 was
net-0 (behavior-preserving e2e-helper extraction; the helper is not itself
a test).

Invariants that hold:
- Deterministic kernel pulls no new external dependency.
- `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty.
- Default `cargo tree -p kesseldb-server` links no
  parquet/objstore/rustls/webpki.
- `large_seed_corpus_is_deterministic_and_converges` green (seed-7).
- EXT/TLS/OBJ-1 oracles (2/1/1) unchanged.
- REQUIRED path byte-unchanged; all OBJ-2a/2b/2c-1 REQUIRED tests pass.
- All OBJ-2b OPTIONAL tests pass unchanged (V1 OPTIONAL path byte-identical).
- All V1 OBJ-2a/2b/2c-1 paths byte-unchanged.

---

## Deferred (OBJ-2c-2+)

- **OBJ-2c-2** — ZSTD compression (codec 6). Resequenced: was planned
  before V2 pages, deferred to prioritise broader pyarrow compatibility.
- **OBJ-2c-4** — `INT96` / `FIXED_LEN_BYTE_ARRAY` / `DECIMAL` physical types.
- **OBJ-2c-5** — REPEATED columns / repetition levels (LIST/MAP, nested
  groups), including V2 `rep_len > 0` pages.
- **lz4 / brotli** compression (also OBJ-2c follow-on).
- **GZIP / Snappy pages > 64 MiB** — the 64 MiB cap mirrors SP104/SP106;
  lifting it requires a streaming decompressor or a tunable cap.
