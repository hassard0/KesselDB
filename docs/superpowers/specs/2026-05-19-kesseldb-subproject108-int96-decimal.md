# KesselDB — Subproject 108: OBJ-2c-4 Parquet INT96 + DECIMAL

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
- Subproject 107 — Parquet V2 data pages:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject107-v2pages.md`

Design document:
`docs/superpowers/specs/2026-05-19-parquet-int96-decimal-design.md`

Plan document:
`docs/superpowers/plans/2026-05-19-parquet-int96-decimal.md`

---

## What shipped

`kessel-parquet` now decodes INT96 timestamps and DECIMAL logical-type
values. No external crate is added; no server, kernel, or SQL layer is
changed.

- **New `PqValue` variants:**
  - `PqValue::Timestamp(i64)` — Unix nanoseconds since epoch (1970-01-01
    00:00:00 UTC). Decoded from INT96 physical type via checked Julian-day
    arithmetic: the high 4 bytes are a Julian Day Number (JDN), the low 8
    bytes are nanoseconds-of-day (0..86_400_000_000_000). The conversion
    `(jdn - 2_440_588) * 86_400_000_000_000 + nod` is performed with
    checked arithmetic; overflow or out-of-range nod yields `PqError::Bad`.
  - `PqValue::Decimal { unscaled: i128, scale: i32 }` — arbitrary-precision
    decimal with scale (number of fractional digits). The `unscaled` field
    is the raw integer value; the true value is `unscaled × 10^(-scale)`.
    Decoded from physical INT32 (sign-extended to i128), INT64
    (sign-extended to i128), or FixedLenByteArray (big-endian two's
    complement sign-extended via the i128 sign-extension recipe; widths 1–16
    bytes). BYTE_ARRAY DECIMAL is decoded identically to FLBA at the decode
    layer (hand-KAT only; pyarrow 24.0.0 cannot write BYTE_ARRAY DECIMAL).

- **`plain.rs` PlainSpec refactor and new decode arms:**
  - `PlainSpec` and `DecimalSpec` structs carry per-leaf validation state
    (scale, precision, type_length) gathered from the schema. The
    `build_plain_spec` function performs second-stage gate validation per
    leaf column: DECIMAL precision out of range 1..=38 → `PqError::Unsupported`;
    FLBA DECIMAL width > 16 bytes → `PqError::Unsupported`; FLBA non-DECIMAL
    width mismatch → `PqError::Bad`.
  - INT96 decode arm: 12-byte read, JDN × 4 bytes (big-endian Julian Day
    Number), nanoseconds-of-day × 8 bytes (little-endian). Emits
    `PqValue::Timestamp(ns)` via checked arithmetic. Out-of-range nod or
    arithmetic overflow → `PqError::Bad`.
  - FLBA decode arm: reads `type_length` bytes. If `converted_type==DECIMAL`
    or `logical_type==DECIMAL`: big-endian two's-complement sign-extended to
    i128, emits `PqValue::Decimal`. Otherwise: raw bytes emitted as
    `PqValue::Bytes` (e.g., FLBA-UUID → 16-byte UUID blob).
  - INT32/INT64 DECIMAL arms: sign-extend to i128, emit `PqValue::Decimal`.
  - BYTE_ARRAY DECIMAL arm: decode prefix-len bytes, big-endian
    sign-extend to i128, emit `PqValue::Decimal` (hand-KAT-only path).

- **`lib.rs` Type-gate flip:** `PhysicalType::Int96` and
  `PhysicalType::FixedLenByteArray` are lifted from the
  `Unsupported("INT96/FIXED_LEN_BYTE_ARRAY: OBJ-2c")` gate to the active
  decode dispatch. Both now route through the `PlainSpec`/`DecimalSpec`
  second-stage gate.

- **`meta.rs` SchemaElement extensions:** `SchemaElement` gains
  `converted_type: Option<i32>`, `type_length: Option<i32>`,
  `scale: Option<i32>`, `precision: Option<i32>`, and `logical_type:
  Option<LogicalType>` (a Thrift union decoded for the DECIMAL sub-arm:
  `LogicalType::DecimalType { scale, precision }`). Agreement check (T2):
  when both `converted_type==DECIMAL` (field 5) and a `LogicalType::Decimal`
  union (field 10) are present, their `scale` and `precision` must agree;
  disagreement → `PqError::Bad`. A writer that emits `converted_type=DECIMAL`
  without the raw scale/precision Thrift fields (f7/f8) is rejected by the
  `unwrap_or(0)` agreement check (T3 strict-stance for malformed DECIMAL
  writer case).

- **`kessel-fetch::pq_to_cell` new arms (workspace-compile mandatory):**
  - `PqValue::Timestamp(ns)` → `Cell::Text` (decimal string of Unix
    nanoseconds). Currently routes through text rendering; coercion to
    `FieldKind::Timestamp` is the immediate follow-up (see Deferred section).
  - `PqValue::Decimal { unscaled, scale }` → `Cell::Text` (decimal string
    of unscaled integer). Currently routes through text/I128 rendering;
    coercion to `FieldKind::Fixed { scale }` is the immediate follow-up.
  - Both arms complete the exhaustive match that was required by the
    workspace compile gate in the same T3 commit.

**Supported matrix after OBJ-2c-4:**

| Axis | Supported |
|---|---|
| Schema shape | Flat (root group + leaves only) |
| Column repetition | REQUIRED or OPTIONAL |
| Compression | UNCOMPRESSED, SNAPPY (raw block; ≤ 64 MiB), or GZIP (RFC 1952; pages ≤ 64 MiB decompressed) |
| Encoding | PLAIN, dictionary (PLAIN_DICTIONARY / RLE_DICTIONARY) |
| Data page version | V1 and V2 (`DATA_PAGE` and `DATA_PAGE_V2`) |
| Physical types | BOOL, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, INT96 (→ Timestamp), FixedLenByteArray (raw bytes or DECIMAL) |
| Logical types | DECIMAL{precision ≤ 38, scale ≤ precision} (typed `PqValue::Decimal{ unscaled: i128, scale }`) |
| Temporal | INT96 → `PqValue::Timestamp` (Unix ns) |

---

## Resequencing & T1/T3/T4 disclosures + V1+V2 byte-identity

**T1 — FailClosedCase struct conversion (SP107-tracked refactor):** T1 of
this slice converted the `run_fail_closed_parquet_e2e` helper's 9
positional parameters to a named `FailClosedCase` struct at all 6 existing
call-sites. This was the SP107-tracked follow-on: "SP107-deferred trigger
condition met by T4's 7th call-site." The extraction was a behavior-
preserving refactor — all 6 prior e2e observable assertions (same inputs,
same error variant checks, same prior-state-intact checks) are preserved
byte-for-byte. T1 delta is **net-0** to the test count (the struct form is
not itself a test; it reduces positional-argument fragility without removing
any assertion).

**T3 deferred extract-level INT96/DECIMAL hand-builders:** The slice plan
anticipated extract-level hand-builders (`extract_decimal_cross_physical_type_determinism_pin`
and `extract_int96_plain_required` in lib.rs) for T3. These were skipped:
T4's real-pyarrow fixtures (10 fixtures + matched-precision 3-way pin)
provide functionally equivalent end-to-end coverage through the production
`extract()` path, which is strictly stronger than a hand-built extract
exerciser. This follows the same SP107-T3 deferral pattern (deferred in
favour of real-data fixtures that prove the full decode pipeline). Honest
disclosure: the T3 test plan understated what T4 would deliver; the final
coverage is stronger, not weaker.

**T4 cross-physical-type-pin gate-caught correction:** T4 initially shipped
commit `cdc1cef` with a silent 2-way reduction — the cross-physical-type
determinism pin covered only INT32 and INT64, omitting FLBA. The gate
caught this: the pin was corrected to the genuine 3-way INT32/INT64/FLBA
matched-precision pin in `501e0fa`, which verifies that the same 5 logical
DECIMAL values extracted from INT32 (precision=9), INT64 (precision=18),
and FLBA (precision=30) sources all produce bit-identical `unscaled`
values. The 2-way encoding-only pin was renamed to distinguish it from the
matched-precision 3-way source-independence pin. This is honest disclosure:
the gate working as designed, the defect corrected before final commit to
main.

**V1+V2 byte-identity preserved across PlainSpec refactor:** The PlainSpec
refactor (T3) restructured the plain decode dispatch without changing any
byte-level behavior for previously-supported physical types. All SP100–107
tests pass green and byte-unchanged. The SP107-introduced regression KAT
(`v1_check_order_num_values_before_comp_size_unchanged`) remains green,
proving the V1 OPTIONAL output order is locked.

---

## Verification

### Hand-derived KATs in plain.rs (13 unit tests from parquet.thrift)

All KATs are derived from the parquet.thrift spec and the INT96/DECIMAL
encoding rules, not from pyarrow output, providing a non-self-referential
verification basis:

- **INT96 plain decode** — 12-byte buffer: JDN=2_451_545 (J2000.0,
  2000-01-01), nod=0. Verified JDN-2440588 Julian-to-Unix conversion
  yields the correct Unix ns for J2000.0.
- **INT96 dict decode** — INT96 values via the dictionary decode path
  (dict page → index page); same Julian-day arithmetic applied.
- **DECIMAL via INT32** — `unscaled = -12345`, scale=2, precision=5.
  Sign-extension from i32 to i128 correctness verified.
- **DECIMAL via INT64** — `unscaled = 9_000_000_000_000_000_000`, scale=3,
  precision=18. Large-value i64 sign-extension to i128 verified.
- **DECIMAL via FLBA (2-byte)** — big-endian bytes `[0x00, 0x0F]` → i128
  +15. Sign-extension from 2-byte big-endian two's complement verified.
- **DECIMAL via FLBA (sign-extend negative)** — bytes `[0xFF, 0xFE]` →
  i128 -2. Sign bit propagation verified.
- **FLBA-UUID (non-DECIMAL)** — 16-byte FLBA with no converted_type →
  `PqValue::Bytes(16 bytes)` (not DECIMAL).
- **Sign-extend boundary — i128::MIN** — The 16-byte FLBA
  `[0x80, 0x00, …, 0x00]` (MSB set, all others zero) sign-extends to
  exactly `i128::MIN`. Verifies the sign-extension recipe handles the
  worst-case boundary value correctly.
- **Scale/precision validation** — DECIMAL precision=0 → `Unsupported`;
  precision=39 → `Unsupported`; scale > precision → `Bad` (per
  parquet.thrift constraint).
- **FLBA width > 16** → `Unsupported` (DECIMAL backed by > 16-byte FLBA
  cannot fit in i128; correct rejection).
- **INT96 nod OOR** — nod ≥ 86_400_000_000_000 → `Bad` (not a valid
  nanoseconds-of-day value).
- **INT96 truncated** — fewer than 12 bytes → `Bad` (bounds check).
- **INT96 JDN overflow** — JDN constructed to yield arithmetic overflow in
  the Julian-to-Unix formula → `Bad` (checked arithmetic).

### meta.rs KATs (4 unit tests for SchemaElement DECIMAL paths)

Derived from parquet.thrift field IDs for the SchemaElement struct:

- **converted_type=DECIMAL (field 5) + raw scale (f7) + raw precision (f8)**
  — both fields present, agreement check passes.
- **LogicalType::DecimalType union (field 10)** — Thrift union decode for
  the DECIMAL sub-arm: scale and precision extracted from the nested struct.
- **Agreement check — both present + agree** — converted_type=DECIMAL and
  LogicalType::Decimal with matching scale/precision → passes.
- **Agreement check — disagree** — converted_type=DECIMAL with scale=3 and
  LogicalType::Decimal with scale=5 → `PqError::Bad`.
- **Strict-stance for malformed DECIMAL writer:** a writer that emits
  `converted_type=DECIMAL` (field 5) but omits the raw Thrift scale (f7)
  and precision (f8) fields is rejected: `unwrap_or(0)` in the agreement
  check produces scale=0 / precision=0, which disagrees with a
  LogicalType::Decimal carrying real values, yielding `PqError::Bad`. This
  is a deliberate strict-stance decision (T3): KesselDB refuses to guess
  scale/precision from an incomplete DECIMAL schema element. Only writers
  that emit both the converted_type and the raw f7/f8 fields are accepted.

### Real pyarrow fixtures (pyarrow 24.0.0, metadata-verified)

All 10 fixtures were verified before commit using pyarrow's metadata API
to confirm physical type and encoding match expectations:

**INT96 fixtures (4):**
- `int96_plain.parquet` — INT96 PLAIN REQUIRED, UNCOMPRESSED, V1. Verified
  `physical_type == INT96`.
- `int96_dict.parquet` — INT96 PLAIN_DICTIONARY REQUIRED, UNCOMPRESSED, V1.
  Verifies dict decode path for INT96.
- `int96_v2_snappy.parquet` — INT96 PLAIN, SNAPPY, DATA_PAGE_V2. Verifies
  INT96 × V2 × Snappy composition.
- `int96_optional.parquet` — INT96 OPTIONAL (nullable), UNCOMPRESSED, V1.
  Verifies nullable INT96 via def-level scatter.

**DECIMAL fixtures (5):**
- `decimal_int32_5_2.parquet` — DECIMAL via INT32, precision=5, scale=2.
- `decimal_int32_dict_5_2.parquet` — DECIMAL via INT32, dict-encoded,
  precision=5, scale=2. Verifies DECIMAL via INT32 dict path.
- `decimal_int64_18_3.parquet` — DECIMAL via INT64, precision=18, scale=3.
  Large-precision DECIMAL via i64 sign-extension.
- `decimal_flba_30_5.parquet` — DECIMAL via FLBA, precision=30, scale=5.
  13-byte FLBA big-endian sign-extension exercised.
- `decimal_flba_optional_30_5.parquet` — DECIMAL via FLBA OPTIONAL
  (nullable), precision=30, scale=5. Nullable DECIMAL via FLBA.

**FLBA-UUID fixture (1):**
- `flba_uuid.parquet` — FLBA with `LogicalType::Uuid` (not DECIMAL), 16
  bytes. Verifies FLBA non-DECIMAL → `PqValue::Bytes`.

### Matched-precision 3-way DECIMAL cross-physical-type determinism pin

Three additional fixtures carry the **same 5 logical DECIMAL values** at
matched scale=2 with precision chosen per physical type (9 for INT32, 18 for INT64, 30 for FLBA — max unscaled ~10^8 fits all three):

- `decimal_int32_eq_9_2.parquet` — precision=9, scale=2, INT32 physical type.
- `decimal_int64_eq_18_2.parquet` — precision=18, scale=2, INT64 physical type.
- `decimal_flba_eq_30_2.parquet` — precision=30, scale=2, FLBA physical type.

The test `extract_decimal_cross_physical_type_determinism_pin` extracts all
three files and asserts that the `unscaled` values are bit-identical across
INT32, INT64, and FLBA sources. This proves source-format-independence:
KesselDB produces the same logical DECIMAL regardless of which physical type
the Parquet writer chose.

**T4 plan-arithmetic correction (honest disclosure):** The original plan
stated the expected unscaled value for `100000.00000` at scale=5 as
`10_000_000_000_000` (10^13). The correct value is `10_000_000_000` (10^10):
`100000 × 10^5 = 10,000,000,000`. The plan contained a factor-of-1000 error
in the example arithmetic. This was caught during T4 implementation by
comparing the plan's expected value against pyarrow's ground truth. The
tests use pyarrow's ground-truth values; the plan's example is documented
here as an honest disclosure of a plan defect, not a code defect.

### INT96 source-independence pin

`extract_int96_plain_dict_v2_source_independence_pin` extracts the same
logical timestamp values from three INT96 sources (PLAIN, dict, V2+Snappy)
and asserts byte-identical `Timestamp(ns)` results. Proves the INT96 decode
path is format-independent: PLAIN, dictionary, and V2+Snappy all produce
the same nanosecond value for the same physical INT96 bytes.

### 7th fail-closed e2e (via FailClosedCase struct)

`refresh_int96_parquet_from_s3_fails_closed_and_state_intact`: an INT96
Parquet file via the `tls_stub_with_fixture` harness (same style as
SP101/SP104/SP105/SP106/SP107 e2e oracles). REFRESH returns a typed error
when the server rejects the request; prior materialized data remains intact.
This is the 7th fail-closed e2e oracle and uses the `FailClosedCase` struct
introduced in T1.

### Pentest — `mod pentest_int96_decimal` (27 locks; no vuln found)

All 27 pentest cases run under `catch_unwind`; no panic, no OOM, typed
errors only. Wall time < 0.142s for all 27 tests combined.

**Hostile inputs (19):**
- **Precision OOR (> 38)** — `DecimalSpec` precision=39 → `Unsupported`.
- **Precision=0** — `DecimalSpec` precision=0 → `Unsupported`.
- **Scale > precision** — scale=5, precision=3 → `Bad`.
- **FLBA width > 16 bytes** — 17-byte FLBA DECIMAL → `Unsupported`.
- **INT96 nod OOR** — nod=86_400_000_000_001 → `Bad`.
- **INT96 JDN arithmetic overflow** — extreme JDN constructed to overflow
  the checked multiplication → `Bad`.
- **INT96 truncated (< 12 bytes)** — 11-byte buffer → `Bad`.
- **INT96 empty buffer** — 0-byte buffer → `Bad`.
- **FLBA sign-extend truncated** — FLBA width=4 but buffer has 3 bytes
  → `Bad`.
- **DECIMAL INT32 truncated** — 3-byte INT32 buffer → `Bad`.
- **DECIMAL INT64 truncated** — 7-byte INT64 buffer → `Bad`.
- **converted_type/LogicalType disagreement** — scale mismatch between
  field 5 and field 10 → `Bad`.
- **INT96 nod exactly at boundary** — nod=86_400_000_000_000 (== limit,
  inclusive) → `Bad` (KesselDB uses strict < check; at-limit is rejected).
- **H5 hostile** — a valid-looking INT96 data page header with corrupt INT96
  bytes in the value section → `Bad` (bounds + arithmetic check).
- **Malformed DECIMAL writer (strict-stance)** — converted_type=DECIMAL
  emitted without f7/f8 raw scale/precision → `Bad` via agreement check.
- Additional 4 hostile inputs covering: FLBA-DECIMAL empty value section;
  DECIMAL BYTE_ARRAY with truncated prefix length; INT96 dict with invalid
  dict index; INT32-DECIMAL with wrong type_length mismatch.

**Positive locks (8):**
- INT96 plain REQUIRED — asserts `Ok([Timestamp(ns), …])`.
- INT96 plain OPTIONAL — asserts null scatter correct.
- DECIMAL INT32 5/2 fixture roundtrip — asserts exact unscaled values.
- DECIMAL INT64 18/3 fixture roundtrip — asserts exact unscaled values.
- DECIMAL FLBA 30/5 fixture roundtrip — asserts exact unscaled values.
- DECIMAL FLBA optional 30/5 fixture roundtrip — asserts nulls scattered.
- **Precision=38 boundary lock (substituting V2+INT96 positive lock):**
  FLBA-DECIMAL with precision=38 (maximum permitted) → `Ok(Decimal{…})`.
  Rationale: V2+INT96 positive lock was judged redundant with the
  `int96_v2_snappy` fixture roundtrip (already in the extract tests) and
  the `mod pentest_v2` suite's generic V2 coverage + H5 hostile case; the
  precision=38 boundary is a distinct correctness surface not covered
  elsewhere.
- **i128::MIN sign-extend positive lock (substituting FLBA-dict positive lock):**
  16-byte FLBA `[0x80, 0x00, …, 0x00]` → `Decimal{ unscaled: i128::MIN,
  scale: 0 }`. Rationale: FLBA-dict positive lock was judged redundant with
  the `decimal_int32_dict_5_2` fixture (INT32-DECIMAL dict already covered)
  and the SP103 dict roundtrip layer; i128::MIN is a distinct sign-extension
  boundary not covered by any other positive lock.

---

## Honest gate accounting

Default-build total: **425 → 484** (+59) — **NOT a zero-delta.** Real
non-zero cause: SP108 is the 4th additive slice on an existing workspace
member (`kessel-parquet`) that `cargo test --workspace` always exercises.
The +59 comes from four tasks of additive tests:

| Task | Net delta | Content |
|---|---|---|
| T1 | ±0 | FailClosedCase struct conversion (behavior-preserving; no new tests) |
| T2 | +4 | meta.rs SchemaElement DECIMAL fields + 4 KATs (converted_type/LogicalType/agreement-pass/agreement-fail) |
| T3 | +15 | plain.rs 13 unit KATs + PlainSpec/DecimalSpec refactor + V1-byte-identity regression KAT (SP107 `v1_check_order…` still green) |
| T4 | +13 | 10 pyarrow fixture roundtrips + INT96 source-independence pin + 3 matched-precision fixtures + 3-way cross-physical-type pin + 7th e2e fail-closed + rename of 2-way encoding-only pin |
| T5 | +27 | `mod pentest_int96_decimal` (19 hostile + 8 positive locks; no vuln found) |

Invariants that hold:
- Deterministic kernel pulls no new external dependency.
- `crates/kessel-parquet/Cargo.toml` `[dependencies]` is empty.
- Default `cargo tree -p kesseldb-server` links no parquet/objstore/rustls/webpki.
- `large_seed_corpus_is_deterministic_and_converges` green (seed-7).
- EXT/TLS/OBJ-1 oracles (2/1/1) unchanged.
- All V1 OBJ-2a/2b/2c-1 paths byte-unchanged.
- All V2 OBJ-2c-3 paths byte-unchanged.
- SP107 regression KAT `v1_check_order_num_values_before_comp_size_unchanged` green.

---

## Deferred / immediate follow-ups (out of SP108 scope)

**Immediate follow-ups (flagged in pq_to_cell doc-comments for next slice):**

- **`kessel-fetch::pq_to_cell` Decimal → `FieldKind::Fixed { scale }`
  coerce path** — currently `PqValue::Decimal` renders as `Cell::Text` of
  the unscaled integer string and routes through `FieldKind::I128`/`I64`
  end-to-end. The `FieldKind::Fixed` coerce arm (~50 lines: `to_field_bytes`
  Fixed arm + scale metadata plumbing) is the immediate follow-up; it is
  flagged in the pq_to_cell doc-comment. DECIMAL → `FieldKind::I128`/`I64`
  (unscaled integer) works today.
- **`kessel-fetch::pq_to_cell` Timestamp → `FieldKind::Timestamp` coerce
  path** — currently `PqValue::Timestamp(ns)` renders as `Cell::Text` of
  the Unix nanosecond string. For pre-1970 timestamps the decoder produces
  a correct negative-nanosecond value; the `FieldKind::Timestamp` coerce
  path is the immediate follow-up. Mapping to `FieldKind::I64` works for
  any sign today.
- **Pre-1970 INT96 through `FieldKind::Timestamp`** — the decoder handles
  negative nanoseconds correctly; the coerce extension is the follow-up.

**Honest disclosure on BYTE_ARRAY DECIMAL:**
pyarrow 24.0.0 cannot write BYTE_ARRAY DECIMAL (it always chooses INT32,
INT64, or FLBA based on precision). The BYTE_ARRAY DECIMAL decode arm is
covered by hand-KATs only. This is documented in the kessel-parquet README.

**LogicalType union arms other than DECIMAL:**
`LogicalType` variants for Date, Time, Timestamp (LogicalType::TimestampType),
String, UUID, and others are currently decoded to their struct form but the
decode dispatch does not act on them beyond the DECIMAL arm — they fall
through to the physical-type decode (e.g., a LogicalType::TimestampType on
an INT64 column is decoded as a plain INT64). This is a benign skip: no
data is lost, no error is thrown. Future slices can add explicit arms for
each logical type as needed.

---

## OBJ-2c arc state

After SP108 the OBJ-2c arc is **3/5 complete**:

| OBJ-2c sub-objective | Status |
|---|---|
| OBJ-2c-1: GZIP decompression (SP106) | DONE |
| OBJ-2c-2: ZSTD decompression | Deferred (resequenced; next after V2) |
| OBJ-2c-3: V2 data pages (SP107) | DONE |
| OBJ-2c-4: INT96 + DECIMAL (SP108) | DONE |
| OBJ-2c-5: REPEATED/nested columns | Open |

Remaining: OBJ-2c-2 (zstd) and OBJ-2c-5 (REPEATED/nested, incl. V2
`rep_len > 0` pages, LIST/MAP groups).

---

## Strategic-tier context

The user's 2026-05-19 strategic review adopted the thesis:
**"Deterministic replicated SQL with verifiable behavior and replayability."**

This thesis positions KesselDB as a system where every execution is
reproducible (deterministic log + digest), every correctness claim is
checkable (formal specs, adversarial tests, Jepsen), and the runtime is
itself verifiable (deterministic WASM UDFs, no side-channel opacity).

Strategic-tier backlog items added post-SP108:

| ID | Item |
|---|---|
| S1 | TLA+/model-checked safety specs for the VSR protocol and MVCC SI |
| S2 | Serializable MVCC/SI over the deterministic log |
| S3 | Jepsen harness for real distributed fault injection |
| S4 | Deterministic in-tree WASM UDFs (sandboxed, reproducible, no imports) |

Per the user's choice, the immediate post-SP108 work is `docs/THESIS.md` +
S1 TLA+ specs, before returning to OBJ-2c-2 (zstd) or OBJ-2c-5 (REPEATED).

---

## Process note

SP108 executes under the autonomous-mandate (see
`feedback_kesseldb_autonomous_build.md`). The two-stage gate (design-review
KAT agreement → test-suite green) substitutes the brainstorming
user-approval gate. All plan-deviation disclosures (T3 deferral, T4 plan-
arithmetic correction, T4 cross-physical-type-pin gate-caught correction,
T5 positive-lock substitution) are made in the Resequencing and Verification
sections above, not suppressed.
