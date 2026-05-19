# OBJ-2c-4 — Parquet INT96 timestamps + DECIMAL: Design

**Date:** 2026-05-19
**Status:** Approved (autonomous mandate — see Process Note)
**Subproject:** 108 (third built sub-slice of the OBJ-2c arc)
**Builds on:** subproject 97/98/99/100/101/102/103/104/105/106/107

## Process Note (autonomy + resequencing)

Produced under the standing overnight autonomous-build mandate
(`feedback_kesseldb_autonomous_build`): "build the backlog
autonomously, BOLD choices, don't wait for approval, keep the
two-stage review gate + full tests + pentest passes." The
brainstorming user-review gate is satisfied by this documented
decision record. All other rigor retained: two-stage subagent review
per task (spec then code-quality), a final whole-implementation
review, full `cargo test --workspace --release` + seed-7 + a pentest
pass each kernel-adjacent task.

**OBJ-2c arc state.** After SP107 (V2 data pages), this slice ships
OBJ-2c-4 (INT96 + DECIMAL) as the **third** built sub-slice. Remaining
after this: OBJ-2c-2 zstd (resequenced from SP107), OBJ-2c-5
REPEATED/nested. Arc state after SP108 = 3/5.

## Problem

`kessel-parquet` currently rejects `INT96` and `FIXED_LEN_BYTE_ARRAY`
(`meta::Type::Int96`/`Type::FixedLenByteArray`) at the `extract()`
support-matrix gate (`lib.rs:506`) with `Unsupported("physical type
…: OBJ-2c")`. The `DECIMAL` logical type (the converted_type=DECIMAL
+ precision + scale + (optionally) LogicalType union from the
SchemaElement) is not decoded at all — pyarrow writes DECIMAL as
FLBA-backed, so a DECIMAL column even at the schema level surfaces
as the FLBA-Unsupported reject.

INT96 is **the de-facto Spark/Hive/Hadoop legacy timestamp** — a
nanos-of-day (8 bytes LE) + Julian-day (4 bytes LE) = 12 bytes total,
deprecated by the spec but ubiquitous in real-world data. DECIMAL is
the canonical exact-numeric type; pyarrow writes it for any `pa.decimal128()`
array. Both are common in real datasets and currently fail-closed.

This slice flips both on, end-to-end, for the existing flat
REQUIRED|OPTIONAL × UNCOMPRESSED|Snappy|GZIP × V1|V2 × PLAIN|dict
matrix.

## Decisions (bold choices, documented)

### Decision 1 — INT96 representation: **typed `PqValue::Timestamp(i64)` (bold A)**

Three options weighed; the strongest one-of-a-kind path is A, taken.

- **(A) New `PqValue::Timestamp(i64)`** carrying **nanoseconds since
  the Unix epoch**. The 12 on-disk bytes decode as
  `(nanos_of_day: u64 LE, julian_day: u32 LE)`. Conversion
  (Julian day 2440588 == 1970-01-01 00:00:00 UTC):
  ```text
  let day_offset = i64::from(julian_day) - 2_440_588_i64;
  let nanos = day_offset
      .checked_mul(86_400_000_000_000)   // ns/day
      .and_then(|d| d.checked_add(i64::try_from(nanos_of_day)?))
      .ok_or_else(|| PqError::Bad("INT96 ns overflow"))?;
  Ok(PqValue::Timestamp(nanos))
  ```
  All checked-arithmetic on attacker bytes: a hostile `julian_day`
  near `u32::MAX` would overflow `i64` after multiply by 86_400e9; the
  `.checked_mul` returns Bad rather than wrapping. `nanos_of_day` is
  cross-checked: ≥ 86_400_000_000_000 is malformed (a normalized INT96
  has nanos_of_day in `[0, 86_400_000_000_000)`); reject as Bad
  (defense-in-depth — some producers store unnormalized but the
  checked-add still bounds the result).

  **Rationale (why bold over safe):** Catalog already has
  `FieldKind::Timestamp` (u64 nanos, 8 bytes, tag 15 — `kessel-catalog/src/lib.rs:39`).
  The fetch-boundary `coerce::to_field_bytes` routes `Timestamp` →
  `int(false, 8)` — i.e. text-parse to u64. A typed Timestamp value
  flowing through `pq_to_cell` → `i64.to_string()` → `Cell::Text` →
  `coerce(Timestamp, …)` arrives as 8 LE bytes — the existing storage
  format, byte-identical to the JSON path for the same logical value.
  This is the kernel-correct outcome the user expects ("FROM 'event_ts'
  INT96 → Timestamp NOT NULL" becomes a real range-comparable
  timestamp). Pushing it to caller-side bytes (option B) abandons that
  alignment.

- **(B) Opaque `PqValue::Int96([u8;12])`** — smaller blast radius;
  caller-interprets. Rejected: forces the SQL layer (eventually) to
  re-decode what the parquet layer already understands; misses the
  catalog alignment.

- **(C) `PqValue::I64(nanos_of_day)` (date lost)** — rejected on
  correctness grounds (silent date truncation).

**Honest negative-nanos boundary.** A pre-1970 timestamp produces a
negative `i64`. `i64.to_string()` of `-12345` is `"-12345"`, which
`u128::parse` in `coerce::int(false, 8)` rejects with FetchError::Type.
This is a **documented deferred**: pre-1970 INT96 values today produce
a typed coerce-time error (no silent corruption); a future
follow-up either (1) extends the catalog to a signed `Timestamp` /
`TimestampNs` accepting negative nanos, or (2) reinterprets the i64
bits as u64 in the coerce text path (round-trips correctly because
the storage is 8 little-endian bytes either way). For typical Spark
data (≥1970 timestamps), the bold-A path works end-to-end as-is.
**This boundary is documented in the slice's record and USAGE deferred
list; positive-nanos correctness locks pin the working path.**

### Decision 2 — DECIMAL representation: **typed `PqValue::Decimal { unscaled: i128, scale: i32 }` (bold A)**

- **(A) New `PqValue::Decimal { unscaled: i128, scale: i32 }`.**
  Parquet DECIMAL physical representations (spec — apache parquet-format
  `LogicalTypes.md`):
  - INT32 (precision ≤ 9): signed 32-bit LE → widen to i128.
  - INT64 (precision ≤ 18): signed 64-bit LE → widen to i128.
  - FLBA(N) (precision > 18, N ≤ 16): big-endian signed integer of N
    bytes → sign-extend to i128.
  - BYTE_ARRAY: same big-endian sign-extended decode as FLBA, with a
    per-value length (length stored in the BYTE_ARRAY u32-LE prefix).

  Logical value = `unscaled / 10^scale`. `i128` covers up to 38
  decimal digits (Parquet's max DECIMAL precision); precision > 38 →
  `Unsupported("DECIMAL precision > 38: OBJ-2c")`.

  **Rationale.** First-class typed Decimal that downstream SQL can
  range-compare/aggregate; matches the catalog's `FieldKind::Fixed {
  scale: u8 }` shape (decimal stored as i64 * 10^-scale). At the
  fetch-boundary today, `coerce::to_field_bytes` rejects `Fixed`
  (internal-only); the natural integration is to render the typed
  `PqValue::Decimal` as a Cell::Text containing the unscaled integer
  (decimal digits, no exponent), which a future Fixed-coerce path
  parses → i64 scaled bytes. For this slice the decoder ships the
  typed variant and `pq_to_cell` adds the text-rendering arm; the
  Fixed-coerce path is the **immediate follow-up** (logged in this
  spec's Deferred section). Mapping a DECIMAL → `FieldKind::I128`
  works today (the text unscaled integer parses via `int(true, 16)`),
  so a user who declares the kessel column as I128 sees a working
  end-to-end path. The DECIMAL → Fixed (decimal-preserving) path is
  the documented follow-on.

- **(B) Decode to `PqValue::Bytes` raw BE unscaled** — loses scale,
  pushes everything to caller. Rejected as weak.

- **(C) Decode to `PqValue::I64` iff precision ≤ 18 and scale==0** —
  partial / fragile. Rejected.

**Sign-extension (FLBA/BYTE_ARRAY).** The N raw bytes are big-endian
two's complement of width N (N ≤ 16). To widen to i128:

```text
let mut buf = if (raw[0] & 0x80) != 0 { [0xFF; 16] } else { [0x00; 16] };
buf[16 - n..].copy_from_slice(raw);   // n = raw.len(), MSB-aligned BE
let unscaled = i128::from_be_bytes(buf);
```

Bounds: `n > 16` (would overflow i128) → `Bad("DECIMAL FLBA exceeds 16 bytes")`.

**Cross-physical-type determinism pin.** The same logical decimal
value via INT32, INT64, FLBA, or BYTE_ARRAY MUST decode to identical
`PqValue::Decimal { unscaled, scale }` after sign-extension. Pinned by
real-pyarrow fixtures (Decision 7) + cross-physical hand KAT.

### Decision 3 — FLBA (non-DECIMAL): **`PqValue::Bytes(Vec<u8>)`**

Parquet FLBA is used for raw fixed-width payloads (UUIDs, fixed
hashes, ML feature vectors) where no LogicalType is set. Cheap and
natural — ship alongside.

- PLAIN encoding: N consecutive bytes per value (N = SchemaElement
  `type_length`, parquet.thrift field 7). No 4-byte length prefix
  (unlike BYTE_ARRAY).
- Dict encoding: dict entries themselves are N-byte blobs; the data
  page is the standard `[bit_width byte][RLE-hybrid indices]`.
- Bounds: `type_length` must be > 0 and < 65 KiB (sanity cap; > 65 KiB
  → Bad — no legitimate FLBA is that wide). `count * n ≤ data.len()`.

A leaf is FLBA-DECIMAL iff `converted_type == DECIMAL(5)` OR
`logical_type == Decimal{precision, scale}` (Decision 4) is set on
the SchemaElement; otherwise FLBA → opaque `PqValue::Bytes`.

### Decision 4 — Schema LogicalType decode: **converted_type + precision + scale + type_length + LogicalType union (both, with precedence)**

The prompt asks: converted_type (older, universal) vs LogicalType
union (newer). Empirically pyarrow 24.0.0 writes **both** for DECIMAL.
The chosen path:

- `meta::SchemaLeaf` gains `precision: i32`, `scale: i32`,
  `type_length: i32`, and `logical_decimal: bool` (a single bool —
  true iff the SchemaElement has `converted_type == DECIMAL(5)` OR a
  `LogicalType { 5:DECIMAL{1:scale,2:precision} }` union arm —
  parquet.thrift `LogicalType` union, DecimalType is union arm 5).
  Storing the boolean (rather than re-deriving the union variant)
  keeps the decoder seam simple.

- `decode_schema_element` decodes:
  - **field 6 converted_type (i32)** — DECIMAL = 5. Mirror the
    existing field-1/3/4/5 nibble-coded reads.
  - **field 7 type_length (i32)** — required for FLBA width
    (`type_length` in parquet.thrift). Recorded even for non-FLBA
    leaves (harmless).
  - **field 9 scale (i32)** — DECIMAL scale.
  - **field 10 precision (i32)** — DECIMAL precision.
  - **field 12 logical_type (LogicalType union)** — newer/broader.
    For this slice we decode only enough to set `logical_decimal=true`
    if the union arm-5 (DecimalType) is present; other arms (UUID
    field 14, TimestampType field 8, etc.) are skipped via
    `s.skip(f.ctype)` — no behavioral effect on this slice. The
    DecimalType arm itself contains `1:i32 scale, 2:i32 precision`;
    we extract those if the converted_type path didn't already set
    them (some writers set logical_type only). One-source-of-truth
    rule: if BOTH are present, they must agree (Bad on disagreement —
    defense-in-depth).

- `meta::Type::Int96` and `meta::Type::FixedLenByteArray` already exist
  in the `Type` enum (`meta.rs:17/21`) and map physical type values 3
  and 7 respectively. **No new enum variants needed.** The Type
  variants are already correctly routed at the gate; we lift them out
  of the Unsupported arm at `lib.rs:506`.

**Why both, not just converted_type:** Some producers (Spark 3.x with
`spark.sql.parquet.writeLegacyFormat=false`) write **only** the
LogicalType union and omit converted_type. Reading both ensures
SP108 supports Spark-written DECIMAL even with the modern path.
Decoding the union arm is bounded (a sibling `decode_logical_type_union`
helper, ~30 lines, mirroring `decode_schema_element` bracketing).

### Decision 5 — T1 helper-struct conversion: **yes (do it before the 7th call-site)**

The SP107 code-quality review explicitly deferred converting
`run_fail_closed_parquet_e2e`'s 9 positional params to a
`FailClosedCase` struct **until the 7th call-site is written** (the
trigger condition the reviewer named). THIS SLICE adds the 7th
call-site (a DECIMAL or INT96 fail-closed e2e). T1 of this slice
performs the conversion **before** adding the 7th call-site, as a
deliberate behavior-preserving refactor — same discipline as SP107-T1.

Form:

```rust
struct FailClosedCase {
    fixture: &'static [u8],
    tag: &'static str,
    keyid_env: &'static str,
    secret_env: &'static str,
    keyid_val: &'static str,
    secret_val: &'static str,
    source: &'static str,
    ddl_cols: &'static str,
    s3_path: &'static str,
}

fn run_fail_closed_parquet_e2e(c: FailClosedCase) { /* body unchanged */ }
```

All 6 existing call-sites rewritten to struct-literal form, e.g.:

```rust
run_fail_closed_parquet_e2e(FailClosedCase {
    fixture: V2_DICT_PARQUET_FIXTURE,
    tag: "v2pq",
    keyid_env: "OBJ_V2PQ_KEYID",
    secret_env: "OBJ_V2PQ_SECRET",
    keyid_val: "AKIAEXAMPLE6",
    secret_val: "secretexamplekey6",
    source: "v2feed",
    ddl_cols: "id U64 NOT NULL FROM 'id', s CHAR(4) NOT NULL FROM 's'",
    s3_path: "v2dict.parquet",
});
```

Every test's observable behavior is byte-identical (same fixtures,
same env vars, same assertions, same `tls_stub_with_fixture` flow).
T1 net-0 to the default-build test count (refactor, not new tests).

### Decision 6 — Cross-crate impact (kessel-fetch): **add `pq_to_cell` arms; flag deeper coerce as immediate follow-up**

`kessel_fetch::pq_to_cell` (`crates/kessel-fetch/src/lib.rs:258`) is
**exhaustive over `PqValue` variants** — no wildcard arm. Adding
`PqValue::Timestamp(i64)` and `PqValue::Decimal { unscaled, scale }`
fails to compile until `pq_to_cell` adds matching arms. THIS SLICE
adds those arms so the workspace compiles:

```rust
// kessel-fetch/src/lib.rs pq_to_cell
Timestamp(ns) => json::Cell::Text(ns.to_string()),       // i64-decimal text
Decimal{ unscaled, scale: _ } => json::Cell::Text(unscaled.to_string()),
// scale is on the catalog FieldKind side (Fixed{scale}); the unscaled
// integer text round-trips through int(true, 16) for I128 columns and
// through the future Fixed-coerce path for Fixed{scale} columns.
```

What works end-to-end after this slice:
- INT96 → `FieldKind::Timestamp` (ns ≥ 0) → 8 LE bytes via
  `coerce::int(false, 8)`.
- INT96 → `FieldKind::I64` → 8 LE bytes via `int(true, 8)` (any sign;
  unit is nanos-since-Unix-epoch).
- DECIMAL → `FieldKind::I128` → 16 LE unscaled bytes via `int(true,
  16)` (scale is lost at the fetch boundary; the user's responsibility
  if they map to I128 vs Fixed).
- DECIMAL → `FieldKind::I64` (precision ≤ 18) → 8 LE unscaled bytes.

What is **immediate follow-up** (out of scope for SP108):
- DECIMAL → `FieldKind::Fixed { scale }` end-to-end. Requires
  extending `coerce::to_field_bytes` to accept Fixed (currently
  rejected as "internal-only"); coerce-time scale agreement check
  (PqValue::Decimal.scale must equal FieldKind::Fixed.scale or be
  rescaled losslessly). This is a one-fn coerce extension in
  `kessel-fetch/src/coerce.rs` plus a SQL grammar verification —
  ~50-line follow-up slice (call it SP109 or fold into OBJ-2c-5's
  USAGE update).
- INT96 → signed-Timestamp for pre-1970 timestamps. Either extend
  `FieldKind::Timestamp` to signed-i64 (add a tagged variant or
  reinterpret) or extend `coerce::int` to round-trip negative-i64
  through a u64 reinterpret. Documented; not in this slice.

The fetch-boundary thus ships a **typed, decoder-correct** Timestamp
and Decimal today, working for the dominant cases (≥1970 timestamps;
I128-mapped decimals). The decimal-preserving Fixed-coerce path is a
two-arm coerce extension; the negative-timestamp boundary is a one-fn
coerce extension. Both are flagged honestly in the deferred section.

### Decision 7 — Fixtures (real pyarrow 24.0.0, BLOCKED-not-faked discipline)

Pyarrow 24.0.0 behaviour confirmed at this slice's planning time:

- **INT96:** `pq.write_table(..., use_deprecated_int96_timestamps=True)`
  writes INT96 (physical_type=='INT96'). Verified.
- **DECIMAL:** default writer produces FIXED_LEN_BYTE_ARRAY for ALL
  precisions (verified). Passing `store_decimal_as_integer=True` (a
  pyarrow ParquetWriter option) emits INT32 for prec ≤ 9 and INT64
  for 9 < prec ≤ 18; FLBA still for prec > 18. Verified.
- **BYTE_ARRAY DECIMAL:** pyarrow never writes it. Honest deferred:
  decoder supports it (hand-KAT only); real-pyarrow fixture not
  generable in pyarrow 24.0.0.

Fixtures to ship:

1. `int96_plain.parquet` — `data_page_version='1.0'`,
   `use_deprecated_int96_timestamps=True`, REQUIRED `ts:timestamp[ns]`,
   3 rows around 1970-01-01 (zero, +1 day, -1 day proves the Julian-day
   subtraction).
2. `int96_dict.parquet` — same but `use_dictionary=True` (proves
   INT96 ∘ dict).
3. `int96_v2_snappy.parquet` — `data_page_version='2.0'`,
   `compression='snappy'`, INT96 (proves INT96 ∘ V2 ∘ Snappy).
4. `int96_optional.parquet` — OPTIONAL INT96 with one null row
   (proves INT96 ∘ null scatter).
5. `decimal_int32.parquet` — `store_decimal_as_integer=True`,
   `pa.decimal128(5, 2)`, REQUIRED, 3 rows (incl. negative).
6. `decimal_int64.parquet` — `store_decimal_as_integer=True`,
   `pa.decimal128(18, 3)`, REQUIRED, 3 rows.
7. `decimal_flba.parquet` — default writer (FLBA), `pa.decimal128(30,
   5)`, REQUIRED, 3 rows.
8. `decimal_flba_optional.parquet` — same shape but OPTIONAL with one
   null (proves DECIMAL ∘ FLBA ∘ null scatter).
9. `decimal_int32_dict.parquet` — `store_decimal_as_integer=True` +
   `use_dictionary=True` (proves DECIMAL ∘ dict).
10. `flba_uuid.parquet` — FLBA(16) **without** a DECIMAL annotation
    (proves the non-DECIMAL FLBA → Bytes path). Pyarrow's
    `pa.binary(16)` writes this directly (verified separately).

Metadata-verify each before commit: `pf.metadata.schema.column(0).physical_type`
matches the expected `INT96`/`INT32`/`INT64`/`FIXED_LEN_BYTE_ARRAY`,
and `converted_type` / `logical_type` match expectations. BLOCKED-not-faked
if any regen step fails or produces an unexpected physical type (SP101
T7 stance).

**BYTE_ARRAY DECIMAL** is honestly disclosed as
**hand-KAT-tested only** (the spec allows it; pyarrow doesn't write
it). The README notes this. A hand-built KAT in `mod tests` (built
from parquet.thrift bytes) covers the BYTE_ARRAY DECIMAL decode path.

### Decision 8 — Pentest scope

Match the SP107 `mod pentest_v2` convention (`#[cfg(test)] mod
pentest_int96_decimal` in `lib.rs`). All cases run under
`catch_unwind`; typed Err only; no panic/OOM/hang.

**Hostile INT96:**
- 12-byte payload but data slice has < 12 bytes → `Bad("INT96 truncated")`.
- `julian_day = u32::MAX` (overflows i64 ns) → `Bad("INT96 ns overflow")`.
- `nanos_of_day ≥ 86_400_000_000_000` → `Bad("INT96 nanos-of-day out of range")`.
- Dict-encoded INT96 with index out of range → typed `Bad` (existing
  dict-index OOB path applies; verify it composes).
- INT96 in V2 with `is_compressed=true` + corrupt Snappy → typed `Bad`
  (codec composition).

**Hostile DECIMAL:**
- precision > 38 → `Unsupported("DECIMAL precision > 38: OBJ-2c-4")`.
- precision < 1 → `Bad("DECIMAL precision range")`.
- scale < 0 OR scale > precision → `Bad("DECIMAL scale range")`.
- FLBA `type_length > 16` for a DECIMAL leaf → `Bad("DECIMAL FLBA
  exceeds 16 bytes")`.
- FLBA `type_length == 0` → `Bad`.
- INT32-physical DECIMAL with precision > 9 → `Bad("DECIMAL precision
  exceeds physical-type range")` (defense-in-depth schema check).
- INT64-physical DECIMAL with precision > 18 → same `Bad`.
- BYTE_ARRAY DECIMAL with per-value length > 16 → `Bad`.
- converted_type=DECIMAL on a `Type::Boolean`/`Float`/`Double` leaf →
  `Bad("DECIMAL on incompatible physical type")`.
- converted_type=DECIMAL and logical_type=Decimal both present, with
  mismatched precision/scale → `Bad("DECIMAL converted_type vs
  logical_type disagree")`.

**Hostile FLBA non-DECIMAL:**
- `type_length` huge (≥ 65 KiB sanity cap) → `Bad`.
- truncated payload (count * N > data.len()) → `Bad`.

**Positive correctness locks (assert exact `Ok`):**
- INT96 PLAIN REQUIRED → matching `PqValue::Timestamp` vector.
- INT96 PLAIN OPTIONAL with one null → scatter correct.
- INT96 dict-encoded → same value vector as PLAIN (source-format
  independence within the page).
- INT96 V2 → same as V1 for the same logical data.
- DECIMAL via INT32 / INT64 / FLBA all produce IDENTICAL
  `PqValue::Decimal { unscaled, scale }` for the same logical value
  (the four-way determinism pin).
- DECIMAL FLBA dict → same as PLAIN.
- DECIMAL FLBA OPTIONAL with null → scatter correct.
- FLBA non-DECIMAL (UUID-like) → `PqValue::Bytes(Vec<u8>)` of width N.

## Architecture

Decode-only change inside `kessel-parquet` + a 2-line additive change
in `kessel-fetch::pq_to_cell` (mandatory for workspace compilation).
Reuses every shipped primitive (`rle::decode_hybrid`,
`dict::resolve_dict_indices`, `snappy::decompress`, `gzip::decompress`,
`scatter_nulls`, `decode_data_page_v2`). The V1 and V2 decode paths
stay **byte-identical** for the already-supported types.

### Components

1. **`meta.rs` SchemaElement extensions.** `decode_schema_element`
   gains arms:
   - field 6 (i32 converted_type) — record DECIMAL(5) flag.
   - field 7 (i32 type_length) — for FLBA width.
   - field 9 (i32 scale).
   - field 10 (i32 precision).
   - field 12 (LogicalType union) — minimal decode: nested struct,
     iterate union arms, only `5:DecimalType{1:scale,2:precision}`
     produces effect; others skipped. Sub-helper
     `decode_logical_type_union` mirrors `decode_schema_element`'s
     bracketing.
   `SchemaLeaf` struct gains 4 fields: `precision: i32` (default 0),
   `scale: i32` (default 0), `type_length: i32` (default 0),
   `logical_decimal: bool` (default false). The hand-built thrift KAT
   in `meta.rs#tests` proves all four decode paths.

2. **`lib.rs` `decode_plain` extensions (in `plain.rs`).** Add per-type
   arms:
   - `Type::Int96` (12 bytes per value): read `nanos_of_day` (u64 LE,
     8 bytes) + `julian_day` (u32 LE, 4 bytes); apply Julian-to-Unix
     conversion (Decision 1's checked arithmetic); push
     `PqValue::Timestamp(ns)`.
   - `Type::FixedLenByteArray`: SIGNATURE CHANGE for plain.rs —
     `decode_plain` needs to know the per-value width. Two paths to
     choose:
     - **(a)** Add a `flba_len: Option<usize>` parameter to
       `decode_plain`. Touches every call-site in lib.rs (~5
       call-sites). Behaviour-preserving for non-FLBA (None).
     - **(b)** Pass a typed `PlainSpec { ptype, flba_len, decimal:
       Option<DecimalSpec> }` enum. Cleaner but more refactor.

     **Choice: (a)** — minimal-impact for this slice. Add
     `flba_len: Option<usize>` as a trailing param. Default `None`
     (non-FLBA types). All non-FLBA call-sites pass `None`. FLBA
     call-site (new) passes `Some(n)`.

   The FLBA decode arm reads `n` bytes per value; if a DECIMAL spec is
   present (Decision 4) it sign-extends to i128 and produces
   `PqValue::Decimal`; otherwise produces `PqValue::Bytes(Vec<u8>)`.
   BUT: deciding DECIMAL vs not is leaf-level metadata. `decode_plain`
   doesn't have the leaf — it has `Type`. So the decimal flag must be
   threaded too. **Refined choice: a single `PlainSpec` struct** (the
   (b) option) — `decode_plain(data: &[u8], spec: PlainSpec, count:
   usize) -> Result<Vec<PqValue>, PqError>`:

   ```rust
   pub struct PlainSpec {
       pub ptype: meta::Type,
       pub flba_len: Option<usize>,     // Some(n) iff ptype == FixedLenByteArray
       pub decimal: Option<DecimalSpec>, // Some iff this leaf is a DECIMAL
   }
   pub struct DecimalSpec { pub precision: u32, pub scale: u32 }
   ```

   This is the cleaner refactor. Touch-radius (all call-sites in
   lib.rs + plain.rs + the dict path that calls `decode_plain` for
   the dictionary itself) is the same regardless — the only choice is
   one trailing param vs a struct; struct wins on call-site clarity.
   **Final architecture: PlainSpec struct.** A `PlainSpec::plain(ptype)`
   constructor produces the `{ ptype, flba_len:None, decimal:None }`
   used by every existing non-FLBA non-DECIMAL site (preserving V1
   byte-identity by code-construction). The new INT96/FLBA/DECIMAL
   sites pass the populated spec.

3. **`lib.rs` `extract()` support-matrix gate (line ~506).** Lift
   `Type::Int96` and `Type::FixedLenByteArray` out of the
   Unsupported arm. Build the per-leaf `PlainSpec` (decimal arm from
   leaf metadata Decision 4; flba_len from `leaf.type_length`).
   Validate `flba_len > 0 && flba_len ≤ 65_536` (sanity cap); reject
   precision > 38 (Unsupported) and precision/scale range (Bad).
   Cross-check precision vs physical-type bounds (Decision 8 hostiles).

4. **`lib.rs` `decode_page` + `decode_data_page_v2`.** Replace the
   `wp: meta::Type` parameter with `spec: &PlainSpec` and forward to
   `plain::decode_plain(spec, ..)` for the PLAIN arm. The dict path
   (`dict::resolve_dict_indices`) is unchanged at the call-site, but
   the dict ITSELF is decoded via `decode_plain` over the dictionary
   page — that call also needs the PlainSpec so dict-encoded INT96 /
   FLBA / DECIMAL works. Trace: in `read_chunk_values`, the
   `plain::decode_plain(&payload, want_ptype, dn)?` (line 341) at
   dict-page time needs a PlainSpec too — change is uniform.

5. **`kessel-fetch::pq_to_cell` (REQUIRED for workspace compile).**
   Add the two new arms (Decision 6).

### V1 & V2 byte-identity

The PlainSpec refactor is a **purely additive** signature change: the
per-call-site `PlainSpec::plain(ptype)` constructor produces the same
behavior as the prior `(ptype)` parameter for INT32/INT64/Float/
Double/Boolean/ByteArray. The decode arms inside `decode_plain` for
those types are byte-unchanged (same `chunks_exact(N)` + `from_le_bytes`
sequences). The new INT96 / FLBA / DECIMAL arms are gated by the
`PlainSpec` fields; they are unreachable for the existing types →
provably no V1/V2 byte change for SP100–107 paths.

A V1-and-V2 byte-identity regression KAT (mirror of SP107's
`v1_check_order_num_values_before_comp_size_unchanged`):
`extract_no_op_for_int64_plain_after_plainspec_refactor` — re-run the
existing PLAIN-INT64 golden over the SP101 builder and assert the
output is byte-identical. This is a permanent regression test (any
future PlainSpec mutation that breaks INT64 trips this KAT).

## Source-format independence pin

For every supported physical encoding of a logical value, `extract()`
returns byte-identical `PqValue`. Pinned by:

1. **DECIMAL 4-way determinism** — `pa.decimal128(5,2)` value
   `Decimal('1.23')` via INT32 (`store_decimal_as_integer=True`), via
   INT64 (precision widened to 15), via FLBA (default), and via
   BYTE_ARRAY (hand-KAT) all yield `PqValue::Decimal { unscaled: 123,
   scale: 2 }`.
2. **INT96 PLAIN vs dict** — same INT96 timestamps via
   `use_dictionary=False` and `use_dictionary=True` yield identical
   `Vec<PqValue::Timestamp(_)>`.
3. **INT96 V1 vs V2** — same data with `data_page_version='1.0'` vs
   `'2.0'` yields identical output.
4. **FLBA non-DECIMAL** — identical Bytes vectors regardless of dict
   on/off.

## Security posture (pentest)

`catch_unwind`-bracketed, all hostile vectors typed `Bad`/`Unsupported`,
no panic / OOM / stack overflow / hang. Full list in Decision 8. Cap
for FLBA `type_length`: 65 KiB (no legitimate FLBA exceeds this; cap
is a constant in `lib.rs`). Bounded INT96 arithmetic: `checked_mul` +
`checked_add` over the Julian-day path. Bounded DECIMAL: precision >
38 → Unsupported; FLBA byte width > 16 → Bad.

## Determinism / invariants gate (every task)

- `cargo test --workspace --release` `FAILED=0`; seed-7
  (`large_seed_corpus_is_deterministic_and_converges`) green.
- Honest gate accounting: baseline = measured post-SP107 425
  (recorded in Task 0); new meta / decode / fixture / pentest tests
  raise the default-build total honestly; per-slice +DELTA is the
  authoritative figure. T1 is behavior-preserving → net-0.
- Kernel pulls no new external dependency; `kessel-parquet/Cargo.toml`
  `[dependencies]` stays empty; default `cargo tree -p kesseldb-server`
  links no parquet / objstore / rustls / webpki.
- `#![forbid(unsafe_code)]`; no `unwrap`/`expect`/`panic`/raw-index on
  input bytes; only the statically-infallible fixed-size slice→`[u8;N]`
  `try_into().unwrap()` after a length-checked `get`.
- Existing oracles green: `external_source_oracle` (2),
  `external_source_tls_oracle` (1), `external_source_objstore_oracle`
  (1); SP101/103/104/105/106/107 e2e cases preserved (now via the
  struct-form FailClosedCase helper, observable behavior identical);
  all V1 OBJ-2a/2b/2c-1/2c-3 decode+gate tests byte-unchanged.

## Honest deferred set

- **DECIMAL → `FieldKind::Fixed { scale }` coerce path** at the fetch
  boundary (decimal-preserving end-to-end). Today: I128/I64
  text-coerce works (unscaled integer); Fixed-coerce is the immediate
  follow-up (~50-line `kessel-fetch::coerce::to_field_bytes` arm +
  scale agreement check).
- **Pre-1970 INT96 timestamps** through the fetch boundary. Today:
  decoder produces correct negative-`Timestamp(i64)`; the
  coerce-to-`FieldKind::Timestamp` path rejects negative text. Mapping
  to `FieldKind::I64` works regardless. The fix is a one-line coerce
  extension or a signed-Timestamp FieldKind variant.
- **DECIMAL BYTE_ARRAY** real-pyarrow fixture (pyarrow doesn't write
  it). Hand-KAT covers the decode path; honest README disclosure.
- **DECIMAL precision > 38** → `Unsupported`. Lifting requires a
  bigint type; out of scope.
- **INT96 nanos overflow boundaries** (`julian_day > ~292K years from
  epoch`) → `Bad` (correct fail-closed; no overflow into wrap).
- **REPEATED INT96 / DECIMAL** → still `Unsupported("REPEATED
  columns: OBJ-2c")` (OBJ-2c-5 still open).
- **V2 `rep_len > 0` with INT96 / DECIMAL** — same OBJ-2c-5 deferred.
- **LogicalType union arms beyond DecimalType** (UUID logical type,
  TimestampType, etc.) — currently skipped; not yet a typed
  KesselDB-level logical-type representation. Future enhancement.
- **`store_decimal_as_integer` is a pyarrow option that isn't on by
  default** — typical real-world pyarrow-written DECIMAL is FLBA; the
  INT32/INT64 paths cover Spark and parquet-mr writers, plus the
  store-as-integer pyarrow override.

After this slice the supported matrix = **flat REQUIRED|OPTIONAL ×
UNCOMPRESSED|Snappy|GZIP × V1|V2 × PLAIN|dict × {INT32, INT64, FLOAT,
DOUBLE, BOOLEAN, BYTE_ARRAY, INT96, FixedLenByteArray, DECIMAL via
INT32/INT64/FLBA/BYTE_ARRAY}**.

## Task decomposition (one coherent slice)

- **T0** determinism baseline (record measured post-SP107 425).
- **T1** convert `run_fail_closed_parquet_e2e`'s 9 positional params
  to a `FailClosedCase` struct (deliberate refactor at all 6 existing
  call-sites; SP107-tracked follow-up done now per the 7th call-site
  trigger). Behavior-preserving (each test's observable
  SchemaError/empty-state assertions identical); gate green. A
  reviewed refactor, NOT a behavior change. Net-0 to test count.
- **T2** `meta.rs` SchemaElement extensions (converted_type field 6,
  type_length field 7, scale field 9, precision field 10,
  logical_type field 12) + `SchemaLeaf` field additions
  (precision/scale/type_length/logical_decimal); hand-built thrift
  KATs for each new field-id arm AND a multi-field KAT for
  pyarrow-shaped DECIMAL(15,3) FLBA leaf with all five fields set; V1
  PageHeader / column-meta tests unchanged (still pass byte-identically).
- **T3** `plain.rs` `PlainSpec` refactor + `decode_plain` signature
  change (PlainSpec) + INT96 arm + FLBA arm (Bytes or Decimal
  per-spec) + DECIMAL widening for INT32/INT64 paths (when
  spec.decimal is set); `lib.rs` updated to build `PlainSpec` from
  `SchemaLeaf` and thread it through `decode_page` +
  `decode_data_page_v2` + the dict-page decode call; gate-flip
  removing INT96/FLBA from the Unsupported arm; hand-built KATs:
  INT96 plain (zero, +1 day, -1 day Julian conversion); FLBA(4) non-
  DECIMAL → Bytes; FLBA(8) DECIMAL(15,3) sign-extension; INT32-
  DECIMAL widening; cross-physical 4-way determinism pin (hand-KAT);
  ALL V1 OBJ-2a/2b/2c-1 + V2 OBJ-2c-3 tests byte-unchanged (run
  `cargo test -p kessel-parquet` and confirm zero regressions plus
  the new tests pass).
- **T4** real pyarrow fixtures (10 fixtures per Decision 7);
  metadata-verify physical_type / converted_type / logical_type
  before commit (BLOCKED-not-faked); roundtrips via production
  `extract()`; INT96 PLAIN-vs-dict-vs-V2 source-independence pin;
  DECIMAL INT32-vs-INT64-vs-FLBA 4-way pin (the BYTE_ARRAY arm of
  the 4-way pin uses the T3 hand-KAT — disclosed in the test);
  add the 7th e2e fail-closed via the T1-struct helper (a DECIMAL
  fixture as the e2e payload — the typed-error path doesn't care
  what's in the bytes).
- **T5** pentest pass (all enumerated hostile vectors from Decision
  8 + positive correctness locks); `#[cfg(test)] mod
  pentest_int96_decimal` in `lib.rs`, `catch_unwind`-bracketed; no
  panic / OOM / hang; fast (each case sub-100ms).
- **T6** docs: `docs/superpowers/specs/2026-05-19-kesseldb-subproject108-int96-decimal.md`
  record (SP107 convention exactly) + STATUS row after SP107 (numeric
  order, gate numbers + `Record:` backlink) + USAGE §7g note +
  cumulative Parquet-scope-table update (retitle heading `(OBJ-2a →
  OBJ-2c-4)`; add Physical-types row → `BOOLEAN, INT32, INT64, FLOAT,
  DOUBLE, BYTE_ARRAY, INT96 (→ Timestamp), FixedLenByteArray, DECIMAL
  (via INT32, INT64, FLBA, BYTE_ARRAY*)`; add a Logical-types row →
  `DECIMAL{precision ≤ 38, scale} (typed PqValue::Decimal)` and a
  Temporal row → `INT96 → PqValue::Timestamp (Unix ns; ≥1970
  end-to-end today)`; update the NOT-supported list to drop INT96/
  FLBA/DECIMAL and add the deferred sub-items: `DECIMAL precision >
  38; DECIMAL → FieldKind::Fixed end-to-end (immediate follow-up);
  pre-1970 INT96 through the fetch-boundary coerce-to-Timestamp path`)
  + gate reconciliation + auto-memory (SP108 block + MEMORY.md line,
  outside repo, never git-add).

## Internal record

`docs/superpowers/specs/2026-05-19-kesseldb-subproject108-int96-decimal.md`
at docs time, mirroring the SP107 record convention exactly (KesselDB
H1, `**Status:**` line, bare-backtick-path Builds-on, `---`
separators, honest gate reconciliation, the
resequencing-still-present + T1-struct-conversion disclosure +
cross-crate kessel-fetch `pq_to_cell` impact + deferred list incl.
the Fixed-coerce and pre-1970 boundaries).
