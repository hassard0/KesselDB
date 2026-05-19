# OBJ-2c-4 Parquet INT96 + DECIMAL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode Parquet `INT96` (Spark/Hive legacy timestamps → typed `PqValue::Timestamp(i64)` nanos-since-Unix-epoch) and `DECIMAL` logical type (typed `PqValue::Decimal { unscaled: i128, scale: i32 }` for physical INT32/INT64/FLBA/BYTE_ARRAY backings); decode FLBA non-DECIMAL as `PqValue::Bytes`. End-to-end through the fetch boundary for the dominant cases (INT96 ≥1970 → Timestamp; DECIMAL → I128/I64 via unscaled-integer text). Existing flat REQUIRED|OPTIONAL × UNCOMPRESSED|Snappy|GZIP × V1|V2 × PLAIN|dict matrix preserved byte-identically.

**Architecture:** `meta.rs` `decode_schema_element` learns converted_type (f6), type_length (f7), scale (f9), precision (f10), logical_type union (f12). `SchemaLeaf` gains `precision`/`scale`/`type_length`/`logical_decimal`. `plain.rs` `decode_plain` signature becomes `(data, spec: PlainSpec, count)` where `PlainSpec { ptype, flba_len: Option<usize>, decimal: Option<DecimalSpec> }`; new INT96 + FLBA + DECIMAL arms. `lib.rs` builds `PlainSpec` per-leaf and threads it through `decode_page` + `decode_data_page_v2` + the dict-page decode call; the support-matrix gate lifts INT96/FLBA out of the Unsupported arm with precision/scale/type_length validation. `kessel-fetch::pq_to_cell` (exhaustive-match — workspace would not compile without it) gains `Timestamp(ns) → Cell::Text(ns.to_string())` and `Decimal{unscaled, ..} → Cell::Text(unscaled.to_string())` arms. T1 first converts the existing `run_fail_closed_parquet_e2e`'s 9 positional params to a `FailClosedCase` struct at all 6 existing call-sites (SP107 reviewer's named trigger — the 7th call-site is added in T4).

**Tech Stack:** Rust (workspace edition), `#![forbid(unsafe_code)]`, zero external deps, existing `PqError`/`PqValue` (new variants added), `rle::decode_hybrid`/`plain::decode_plain`/`dict::resolve_dict_indices`/`snappy::decompress`/`gzip::decompress`/`scatter_nulls`/`decode_data_page_v2`.

---

## Context for the implementer (read once)

`crates/kessel-parquet` modules: `thrift.rs`, `footer.rs`, `meta.rs`, `plain.rs`, `rle.rs`, `dict.rs`, `snappy.rs`, `gzip.rs`, `lib.rs`. `Cargo.toml` `[dependencies]` empty and MUST stay empty.

- **`meta.rs`** `Type` enum (line ~13) **already has `Int96` and `FixedLenByteArray`** mapping physical types 3 and 7. `Codec` enum has Uncompressed/Snappy/Gzip/Other. `Encoding` has Plain/Rle/PlainDictionary/RleDictionary/Other. `SchemaLeaf { name, ptype, repetition }` — gain `precision`/`scale`/`type_length`/`logical_decimal`. `decode_schema_element` (line ~223) reads fields 1 (type), 3 (repetition), 4 (name), 5 (num_children); will gain field-6/7/9/10/12 arms. The per-struct `s.reset_last_id()` bracketing is the established convention.
- **`plain.rs`** `decode_plain(data, ptype: Type, count)` → `Result<Vec<PqValue>, PqError>` (currently 6 arms: Boolean / Int32 / Int64 / Float / Double / ByteArray; "other" → Unsupported). The pentest-hardened `Vec::with_capacity(count.min(data.len()))` line at the top is the SP101 fix — preserve it.
- **`lib.rs`**
  - `PqValue` (line 22): `Null, Bool, I64, F64, Bytes`. **Adding** `Timestamp(i64)` and `Decimal { unscaled: i128, scale: i32 }`. PqValue is `#[derive(Clone, Debug, PartialEq)]` — both new variants must support those derives (i128 and i64 do).
  - `decode_page` (line 111) and `decode_data_page_v2` (line 168) take `wp: meta::Type` and forward to `plain::decode_plain(payload, wp, count)`. These call-sites change to `spec: &PlainSpec` and `plain::decode_plain(payload, spec, count)`.
  - `read_chunk_values` (line 283): the dict-page payload decode at line 341 — `plain::decode_plain(&payload, want_ptype, dn)?` — also needs PlainSpec (dict-encoded INT96/FLBA/DECIMAL works because the dictionary itself contains INT96/FLBA/DECIMAL values).
  - `extract()` support-matrix gate at line 499–511: `Type::Int96 | Type::FixedLenByteArray` are currently in the Unsupported arm. Lifted by T3.
  - Schema-vs-ColumnMetaData ptype-mismatch check at line 535 — unchanged (ptype equality is the same check; for FLBA-DECIMAL both schema leaf and CMD say FixedLenByteArray).
- **`kessel-fetch/src/lib.rs:258` `pq_to_cell`** is `#[cfg(feature = "object-store")] fn pq_to_cell(v: kessel_parquet::PqValue) -> json::Cell` with an EXHAUSTIVE match — adding PqValue variants without adding arms FAILS THE BUILD. T3 must add the two new arms in the same commit that adds the PqValue variants (or fall back: `Timestamp` and `Decimal` arms in `pq_to_cell` must be added in a step that lands before/with the variant additions; cleanest is the same T3 commit).
- **`crates/kesseldb-server/tests/external_source_parquet_oracle.rs`** has 6 call-sites of `run_fail_closed_parquet_e2e(...)` (SP101 + SP103 + SP104 + SP105 + SP106 + SP107). The helper signature has 9 positional params (line ~130). T1 converts to a struct.

**parquet.thrift authority for SchemaElement extensions** (per the Apache `parquet-format` repo's `src/main/thrift/parquet.thrift` — verified against the spec at planning time):

```
struct SchemaElement {
  1: optional Type type;
  2: optional i32 type_length;          // FLBA width in bytes
  3: optional FieldRepetitionType repetition_type;
  4: required string name;
  5: optional i32 num_children;
  6: optional ConvertedType converted_type;
  7: optional i32 scale;                 // for DECIMAL
  8: optional i32 precision;             // for DECIMAL
  9: optional i32 field_id;              // (Parquet stream order)
 10: optional LogicalType logicalType;
}
```

**Important note** — per the *actual* parquet.thrift in the current Apache spec the field IDs differ from the design-spec illustration. The verified field IDs from parquet-format master:
- `1:Type type`
- `2:i32 type_length` (FLBA width)
- `3:FieldRepetitionType repetition_type`
- `4:string name`
- `5:i32 num_children`
- `6:ConvertedType converted_type` (i32; DECIMAL = 5)
- `7:i32 scale`
- `8:i32 precision`
- `9:i32 field_id`
- `10:LogicalType logicalType`

**This plan uses the verified field IDs above.** The implementer MUST cross-check the field IDs against `parquet.thrift` (Apache parquet-format master) before writing the meta arms; if the upstream spec differs, the meta KAT bytes computed below are wrong and the implementer must adjust BOTH the arms AND the KAT bytes (BLOCKED-not-faked: a failing KAT means the *code* is wrong, never the spec — and a wrong field ID is a code-level error to fix, not a KAT to weaken).

ConvertedType enum (`parquet.thrift`): `UTF8=0, MAP=1, MAP_KEY_VALUE=2, LIST=3, ENUM=4, DECIMAL=5, ...`. We use `DECIMAL=5`.

LogicalType union (`parquet.thrift`): `1:StringType, 2:MapType, 3:ListType, 4:EnumType, 5:DecimalType, 6:DateType, 7:TimeType, 8:TimestampType, 10:IntType, 11:NullType, 14:UUIDType` (current spec arms; some omitted). DecimalType: `{ 1:i32 scale, 2:i32 precision }`. We decode only the DecimalType arm (5).

**Discipline:** `#![forbid(unsafe_code)]` crate-wide. No unwrap/expect/panic/raw-index on input bytes — checked `get(..)`/`checked_*`; only the statically-infallible fixed-size slice→`[u8;N]` `try_into().unwrap()` for `from_le_bytes`/`from_be_bytes` after a length-checked `get`. KAT discipline: hand-built thrift/page bytes are derived from `parquet.thrift`/RFC (independent authority); real pyarrow fixtures are the non-self-referential proof; a failing KAT means the *code* is wrong — never change a KAT; report BLOCKED.

**Determinism / invariants gate — EVERY task (T0–T6):**
- `cargo test --workspace --release` → `FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` green.
- `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty; `cargo tree -p kesseldb-server` links no parquet/objstore/rustls/webpki.
- Existing oracles green: `external_source_oracle`(2), `external_source_tls_oracle`(1), `external_source_objstore_oracle`(1); all V1 OBJ-2a/2b/2c-1 and V2 OBJ-2c-3 decode+gate tests byte-unchanged; the 6 fail-closed e2e cases preserved (T1-refactored through the FailClosedCase struct with identical observable assertions).

**Commit discipline:** straight to `main`, no Co-Authored-By, no signing, message style like `git log -3`. `git push` after every task (single-branch-main durably authorized by `feedback_kesseldb_autonomous_build`; the two-stage gate IS the review; ignore the recurring soft-block notice). Bash: prefix `cd /c/Users/ihass/KesselDB &&`; `cargo test --workspace --release` long — allow 600000ms.

---

### Task 0: Determinism baseline (#191)

- [ ] **Step 1:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -30` — sum `passed` → `<BASELINE>` (expected **425**); `FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` in an `ok` result.
- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo tree -p kesseldb-server 2>&1 | grep -Ei "parquet|objstore|rustls|webpki"` → no output.
- [ ] **Step 3:** No commit. Report DONE with `OBJ-2c-4 baseline: <BASELINE> tests passing, FAILED=0, seed-7 green` + per-binary counts.

---

### Task 1: Convert `run_fail_closed_parquet_e2e` to `FailClosedCase` struct (behavior-preserving refactor) (#192)

**Files:** Modify `crates/kesseldb-server/tests/external_source_parquet_oracle.rs`.

This is a **reviewed, behavior-preserving refactor** — NOT an intended behavior change. The 9 positional params are converted to a struct; every existing test's observable assertions are byte-identical in effect. Triggered by the SP107 review's named condition: the 7th call-site (added in T4) makes 9 positional args unwieldy.

- [ ] **Step 1: Read the file.** Inspect the current 9-param helper signature (line ~130) and the 6 existing call-sites (SP101/103/104/105/106/107 — `refresh_parquet_from_s3_…`, `refresh_dict_…`, `refresh_snappy_…`, `refresh_nullable_…`, `refresh_gzip_…`, `refresh_v2_…`). Note each test's exact (fixture, tag, keyid_env, secret_env, keyid_val, secret_val, source, ddl_cols, s3_path) values — copy them verbatim into the struct-literal rewrites.

- [ ] **Step 2: Define the struct + rewrite the helper signature.** Replace the 9-param fn signature with:
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

fn run_fail_closed_parquet_e2e(c: FailClosedCase) {
    // BODY unchanged — replace every prior parameter name with the
    // corresponding `c.<field>` access:
    //   fixture → c.fixture
    //   tag → c.tag
    //   keyid_env → c.keyid_env  ... etc.
    // Nothing else changes — every assertion, every sleep duration,
    // every match arm preserved byte-identically.
    std::env::set_var(c.keyid_env, c.keyid_val);
    std::env::set_var(c.secret_env, c.secret_val);
    let port = tls_stub_with_fixture(c.fixture);
    let shard = spawn_shard(c.tag);
    // ... rest of the body, with parameter accesses rewritten
}
```

- [ ] **Step 3: Rewrite the 6 call-sites.** Each existing `#[test] fn refresh_*` becomes (illustrative — preserve each test's actual values verbatim):
```rust
#[test]
fn refresh_v2_parquet_from_s3_fails_closed_and_state_intact() {
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
}
```
Apply uniformly to all 6 — the values come straight from the prior positional args; do NOT change them.

- [ ] **Step 4:** `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources-objstore --test external_source_parquet_oracle 2>&1 | tail -10` → all 6 still pass (0 failed). Same names, same observable behavior.

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total UNCHANGED from baseline (refactor — net-0), seed-7 green.

- [ ] **Step 6: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kesseldb-server/tests/external_source_parquet_oracle.rs && git commit -m "test: convert run_fail_closed_parquet_e2e to FailClosedCase struct (behavior-preserving; SP107-tracked refactor)" && git push
```

---

### Task 2: `meta.rs` SchemaElement extensions (converted_type / type_length / scale / precision / logical_type) (#193)

**Files:** Modify `crates/kessel-parquet/src/meta.rs`.

- [ ] **Step 1: Verify parquet.thrift field IDs.** Before coding, the implementer must reconfirm the SchemaElement field IDs against the Apache `parquet-format` repo's `parquet.thrift`. The plan uses the verified IDs `2:type_length, 6:converted_type, 7:scale, 8:precision, 10:logicalType`. If the upstream spec uses different IDs (e.g. an older version), correct BOTH the arms below AND the KAT bytes (recompute the field-header `(delta<<4)|ctype` bytes); the KAT is authoritative against the *spec*, not the implementation. Report BLOCKED if `parquet.thrift` is inaccessible.

- [ ] **Step 2: Write the failing tests.** Add inside `meta.rs` `#[cfg(test)] mod tests`:

```rust
#[test]
fn schemaelement_decodes_decimal_flba_full_metadata() {
    // SchemaElement for a DECIMAL(15, 3) leaf stored as FIXED_LEN_BYTE_ARRAY
    // with type_length=8 (per the Parquet spec, ceil(log2(10^15) / 8) ≈ 7
    // bytes; pyarrow rounds up; 8 is a typical pyarrow output for prec=15).
    // Fields: 1:type=FIXED_LEN_BYTE_ARRAY(7), 2:type_length=8,
    // 3:repetition=REQUIRED(0), 4:name="d", 6:converted_type=DECIMAL(5),
    // 7:scale=3, 8:precision=15. LogicalType (field 10) omitted in this KAT
    // — proves the converted_type-only path. Field IDs reset to 0 at struct
    // start. compact field header = (delta<<4)|ctype; i32-zigzag=5,
    // binary=8. zz(n)=(n<<1)^(n>>63).
    let mut b = Vec::new();
    // f1 type=7 (FLBA): (1<<4)|5=0x15, zz(7)=14
    b.push(0x15); uv(&mut b, zz(7));
    // f2 type_length=8: delta 1->2=1, i32 → 0x15, zz(8)=16
    b.push(0x15); uv(&mut b, zz(8));
    // f3 repetition_type=REQUIRED(0): delta 2->3=1 → 0x15, zz(0)=0
    b.push(0x15); uv(&mut b, zz(0));
    // f4 name="d": delta 3->4=1, binary → 0x18, len 1
    b.push(0x18); uv(&mut b, 1); b.extend_from_slice(b"d");
    // f6 converted_type=DECIMAL(5): delta 4->6=2 → (2<<4)|5=0x25, zz(5)=10
    b.push(0x25); uv(&mut b, zz(5));
    // f7 scale=3: delta 6->7=1 → 0x15, zz(3)=6
    b.push(0x15); uv(&mut b, zz(3));
    // f8 precision=15: delta 7->8=1 → 0x15, zz(15)=30
    b.push(0x15); uv(&mut b, zz(15));
    // stop SchemaElement
    b.push(0x00);

    // Wrap in a minimal FileMetaData so we exercise decode_schema_element
    // through the public decode path (single Group root + this leaf).
    // ... mirror the existing `decode_minimal_filemetadata` KAT's
    // FileMetaData wrapper exactly — only the schema list contents differ.
    let mut f = Vec::new();
    f.push(0x15); uv(&mut f, zz(1));                 // version=1
    f.push(0x19); b.push(0x2c);                      // list<struct> 2
    // schema[0]: root group
    f.push(0x48); uv(&mut f, 6); f.extend_from_slice(b"schema");
    f.push(0x15); uv(&mut f, zz(1));                 // num_children=1
    f.push(0x00);
    // schema[1]: copy `b` (the leaf SchemaElement)
    f.extend_from_slice(&b);
    f.push(0x16); uv(&mut f, zz(0));                 // num_rows=0
    f.push(0x19); b.push(0x1c);                      // list<RowGroup> 0
    // 0 row groups → just stop
    f.push(0x00);                                    // stop list (empty)
    f.push(0x00);                                    // stop FileMetaData

    let md = FileMetaData::decode(&f).expect("decode");
    let leaf = &md.leaves[0];
    assert_eq!(leaf.name, "d");
    assert_eq!(leaf.ptype, Type::FixedLenByteArray);
    assert_eq!(leaf.repetition, Repetition::Required);
    assert_eq!(leaf.type_length, 8);
    assert_eq!(leaf.precision, 15);
    assert_eq!(leaf.scale, 3);
    assert!(leaf.logical_decimal);
}

#[test]
fn schemaelement_decodes_decimal_via_logical_type_only() {
    // Spark-style: converted_type absent, only logical_type=DecimalType.
    // Fields: 1:type=FLBA(7), 2:type_length=8, 3:repetition=REQUIRED(0),
    // 4:name="d", 10:logicalType=DecimalType{1:scale=3, 2:precision=15}.
    // LogicalType is a thrift union: it appears as a struct with exactly
    // one arm set; we set arm 5 (DecimalType).
    let mut b = Vec::new();
    b.push(0x15); uv(&mut b, zz(7));            // f1 type=FLBA
    b.push(0x15); uv(&mut b, zz(8));            // f2 type_length=8
    b.push(0x15); uv(&mut b, zz(0));            // f3 repetition=REQUIRED
    b.push(0x18); uv(&mut b, 1); b.extend_from_slice(b"d"); // f4 name="d"
    // f10 logicalType: delta 4->10=6 → (6<<4)|12(struct)=0x6c
    b.push(0x6c);
    // LogicalType union struct: f5 DecimalType. delta 0->5=5 → (5<<4)|12=0x5c
    b.push(0x5c);
    // DecimalType struct: f1 scale=3, f2 precision=15
    b.push(0x15); uv(&mut b, zz(3));            // f1 scale=3
    b.push(0x15); uv(&mut b, zz(15));           // f2 precision=15
    b.push(0x00);                               // stop DecimalType
    b.push(0x00);                               // stop LogicalType union
    b.push(0x00);                               // stop SchemaElement

    // Wrap in FileMetaData identically to the previous test.
    let mut f = Vec::new();
    f.push(0x15); uv(&mut f, zz(1));
    f.push(0x19); f.push(0x2c);
    f.push(0x48); uv(&mut f, 6); f.extend_from_slice(b"schema");
    f.push(0x15); uv(&mut f, zz(1));
    f.push(0x00);
    f.extend_from_slice(&b);
    f.push(0x16); uv(&mut f, zz(0));
    f.push(0x19); f.push(0x1c);
    f.push(0x00);
    f.push(0x00);

    let md = FileMetaData::decode(&f).expect("decode");
    let leaf = &md.leaves[0];
    assert_eq!(leaf.ptype, Type::FixedLenByteArray);
    assert_eq!(leaf.type_length, 8);
    assert_eq!(leaf.precision, 15);
    assert_eq!(leaf.scale, 3);
    assert!(leaf.logical_decimal);
}

#[test]
fn schemaelement_v1_leaves_unchanged_after_extension() {
    // Sanity: the existing decode_minimal_filemetadata + the existing
    // gzip-codec / dict-page-offset / snappy / flat-schema tests assert
    // SchemaLeaf fields that already exist. The new fields default to 0
    // / false and do not break the existing leaf-shape assertions.
    // This test re-runs the SP101 minimal schema and asserts the new
    // defaulted fields are 0/0/0/false.
    // ... (re-use the existing decode_minimal_filemetadata's `b` bytes
    //      verbatim; assert the new fields default).
    // No KAT mutation: the bytes are byte-identical to the prior test,
    // proving V1 schema decode is unchanged.
}
```

- [ ] **Step 3:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet meta:: 2>&1 | tail -12` → compile error (no `precision`/`scale`/`type_length`/`logical_decimal` fields on `SchemaLeaf`).

- [ ] **Step 4: Implement.**
  1. Extend `SchemaLeaf` (line ~106):
     ```rust
     #[derive(Clone, Debug)]
     pub struct SchemaLeaf {
         pub name: String,
         pub ptype: Type,
         pub repetition: Repetition,
         /// FLBA width in bytes (parquet.thrift SchemaElement field 2);
         /// 0 for non-FLBA leaves.
         pub type_length: i32,
         /// DECIMAL precision (field 8) or LogicalType DecimalType
         /// precision; 0 if not a DECIMAL leaf.
         pub precision: i32,
         /// DECIMAL scale (field 7) or LogicalType DecimalType scale;
         /// 0 if not a DECIMAL leaf.
         pub scale: i32,
         /// True iff this leaf is a DECIMAL: converted_type=DECIMAL(5)
         /// OR logical_type=DecimalType.
         pub logical_decimal: bool,
     }
     ```
  2. Extend `decode_schema_element` (line ~223) to read the new field
     IDs. Initialize the new locals at the top:
     ```rust
     let mut type_length = 0i32;
     let mut precision = 0i32;
     let mut scale = 0i32;
     let mut converted_decimal = false;
     let mut logical_decimal = false;
     let mut logical_precision = 0i32;
     let mut logical_scale = 0i32;
     ```
     Add arms in the `while let Some(f) = s.next_field()?` body
     (preserving the existing 1/3/4/5 arms unchanged; new arms
     placed in field-ID order):
     ```rust
     2 => type_length = s.read_i32(&f)?,
     6 => {
         // ConvertedType (i32). DECIMAL == 5.
         let v = s.read_i32(&f)?;
         if v == 5 { converted_decimal = true; }
         // Other converted-type values: not used by this slice.
     }
     7 => scale = s.read_i32(&f)?,
     8 => precision = s.read_i32(&f)?,
     10 => {
         // LogicalType union: a single-arm nested struct.
         if f.ctype != ctype::STRUCT {
             return Err(bad("SchemaElement.logicalType: expected struct"));
         }
         decode_logical_type_union(
             s,
             &mut logical_decimal,
             &mut logical_precision,
             &mut logical_scale,
         )?;
         s.restore_last_id(f.id);
     }
     ```
     Where `decode_logical_type_union` is a NEW free fn in `meta.rs`:
     ```rust
     /// Decode the LogicalType thrift union; sets `*is_decimal` and
     /// `*precision`/`*scale` if the DecimalType arm (5) is set.
     /// Other arms are skipped harmlessly. Per-struct last_id reset.
     fn decode_logical_type_union(
         s: &mut StructReader,
         is_decimal: &mut bool,
         precision: &mut i32,
         scale: &mut i32,
     ) -> Result<(), PqError> {
         s.reset_last_id();
         while let Some(f) = s.next_field()? {
             match f.id {
                 5 => {
                     // DecimalType nested struct.
                     if f.ctype != ctype::STRUCT {
                         return Err(bad(
                             "LogicalType.DecimalType: expected struct",
                         ));
                     }
                     let saved = s.save_last_id();
                     s.reset_last_id();
                     while let Some(g) = s.next_field()? {
                         match g.id {
                             1 => *scale = s.read_i32(&g)?,
                             2 => *precision = s.read_i32(&g)?,
                             _ => s.skip(g.ctype)?,
                         }
                     }
                     s.restore_last_id(saved);
                     *is_decimal = true;
                 }
                 _ => s.skip(f.ctype)?,
             }
         }
         Ok(())
     }
     ```
  3. After the loop, resolve precision/scale precedence (Decision 4):
     ```rust
     let final_decimal = converted_decimal || logical_decimal;
     // If both converted_type and logical_type DecimalType are present,
     // they must agree on precision/scale (defense-in-depth Bad).
     if converted_decimal && logical_decimal {
         if logical_precision != 0 && logical_precision != precision {
             return Err(bad("DECIMAL converted_type vs logical_type disagree (precision)"));
         }
         if logical_scale != 0 && logical_scale != scale {
             return Err(bad("DECIMAL converted_type vs logical_type disagree (scale)"));
         }
     }
     // If only logical_type was set, copy its values into the canonical fields.
     if !converted_decimal && logical_decimal {
         precision = logical_precision;
         scale = logical_scale;
     }
     ```
  4. Update the `Ok(SchemaNode::Leaf(SchemaLeaf { name, ptype, repetition }))` line to include the new fields. Update the `Group` arm to not need them (groups have no decimal metadata).

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet meta:: 2>&1 | tail -15` → all NEW tests + ALL pre-existing meta tests pass. **Critical**: the pre-existing tests construct `SchemaLeaf` via `decode_schema_element` (no direct constructors in tests beyond the bytes), so adding the new fields is non-breaking *for tests*. If any test asserts a `SchemaLeaf` field shape (e.g. `assert_eq!(leaf, SchemaLeaf { … })` direct), update it to use field-by-field assertions (the prior assertion-by-field form is preserved unchanged).

- [ ] **Step 6:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total ≥ baseline+2-3, seed-7 green.

- [ ] **Step 7: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/meta.rs && git commit -m "parquet: meta SchemaElement converted_type/scale/precision/type_length/logical_type decode" && git push
```

---

### Task 3: `plain.rs` PlainSpec + INT96/FLBA/DECIMAL decode; `lib.rs` gate flip; `kessel-fetch::pq_to_cell` arms (#194)

**Files:** Modify `crates/kessel-parquet/src/plain.rs`, `crates/kessel-parquet/src/lib.rs`, `crates/kessel-fetch/src/lib.rs`.

Before coding: READ `decode_plain` (`plain.rs`), the V1 `decode_page` + V2 `decode_data_page_v2` (`lib.rs:111` + `:168`), the dict-page payload call (`lib.rs:341`), and the support-matrix gate (`lib.rs:499–511`). Also READ `kessel-fetch/src/lib.rs` `pq_to_cell` (line 258).

- [ ] **Step 1: Add the failing tests** in `plain.rs#tests` and `lib.rs#tests`:

```rust
// plain.rs
#[test]
fn plain_decode_int96_to_timestamp() {
    // Three values around Unix epoch:
    //   row 0: nanos_of_day=0, julian_day=2440588 → Timestamp(0)
    //   row 1: nanos_of_day=0, julian_day=2440589 → Timestamp(86_400_000_000_000)
    //   row 2: nanos_of_day=0, julian_day=2440587 → Timestamp(-86_400_000_000_000)
    let mut b = Vec::new();
    for &(nod, jd) in &[(0u64, 2_440_588u32), (0, 2_440_589), (0, 2_440_587)] {
        b.extend_from_slice(&nod.to_le_bytes());
        b.extend_from_slice(&jd.to_le_bytes());
    }
    let spec = PlainSpec::plain(Type::Int96);
    let got = decode_plain(&b, spec, 3).unwrap();
    assert_eq!(got, vec![
        PqValue::Timestamp(0),
        PqValue::Timestamp(86_400_000_000_000),
        PqValue::Timestamp(-86_400_000_000_000),
    ]);
}

#[test]
fn plain_decode_flba_uuid_to_bytes() {
    // FLBA(16) non-DECIMAL: 16 raw bytes per value.
    let mut b = Vec::new();
    b.extend_from_slice(&[0xAA; 16]);
    b.extend_from_slice(&[0xBB; 16]);
    let spec = PlainSpec::flba(16);
    let got = decode_plain(&b, spec, 2).unwrap();
    assert_eq!(got, vec![
        PqValue::Bytes(vec![0xAA; 16]),
        PqValue::Bytes(vec![0xBB; 16]),
    ]);
}

#[test]
fn plain_decode_flba_decimal_sign_extends_to_i128() {
    // FLBA(8) DECIMAL(15, 3): big-endian signed 8-byte values, sign-extended.
    // Value 1: 12345 (positive)   → bytes 00 00 00 00 00 00 30 39
    // Value 2: -456 (negative)    → bytes FF FF FF FF FF FF FE 38
    let mut b = Vec::new();
    b.extend_from_slice(&12345i64.to_be_bytes());
    b.extend_from_slice(&(-456i64).to_be_bytes());
    let spec = PlainSpec::flba_decimal(8, 15, 3);
    let got = decode_plain(&b, spec, 2).unwrap();
    assert_eq!(got, vec![
        PqValue::Decimal { unscaled: 12345, scale: 3 },
        PqValue::Decimal { unscaled: -456, scale: 3 },
    ]);
}

#[test]
fn plain_decode_int32_decimal_widens_to_i128() {
    let mut b = Vec::new();
    b.extend_from_slice(&12345i32.to_le_bytes());
    b.extend_from_slice(&(-456i32).to_le_bytes());
    let spec = PlainSpec::int_decimal(Type::Int32, 5, 2);
    let got = decode_plain(&b, spec, 2).unwrap();
    assert_eq!(got, vec![
        PqValue::Decimal { unscaled: 12345, scale: 2 },
        PqValue::Decimal { unscaled: -456, scale: 2 },
    ]);
}

#[test]
fn plain_decode_int64_decimal_widens_to_i128() {
    let mut b = Vec::new();
    b.extend_from_slice(&12345i64.to_le_bytes());
    b.extend_from_slice(&(-456i64).to_le_bytes());
    let spec = PlainSpec::int_decimal(Type::Int64, 15, 3);
    let got = decode_plain(&b, spec, 2).unwrap();
    assert_eq!(got, vec![
        PqValue::Decimal { unscaled: 12345, scale: 3 },
        PqValue::Decimal { unscaled: -456, scale: 3 },
    ]);
}

// PlainSpec refactor regression: every existing PLAIN INT64 test still
// passes byte-identically through the new spec form.
#[test]
fn plain_decode_int64_byte_identity_after_plainspec_refactor() {
    let mut b = Vec::new();
    b.extend_from_slice(&7i64.to_le_bytes());
    b.extend_from_slice(&(-2i64).to_le_bytes());
    assert_eq!(
        decode_plain(&b, PlainSpec::plain(Type::Int64), 2).unwrap(),
        vec![PqValue::I64(7), PqValue::I64(-2)]
    );
}
```

```rust
// lib.rs (inside `mod tests`)
#[test]
fn extract_decimal_cross_physical_type_determinism_pin() {
    // Build three hand-built Parquet files all encoding logical
    // DECIMAL(5,2) value "1.23" via different physical types:
    // INT32-DECIMAL, INT64-DECIMAL, FLBA-DECIMAL. All three must
    // produce `vec![vec![PqValue::Decimal { unscaled: 123, scale: 2 }]]`.
    // (Uses the hand-builders extended for DECIMAL spec annotation.)
    let i32_file = build_decimal_file(Type::Int32, /*type_length=*/0, 5, 2, &[123i64]);
    let i64_file = build_decimal_file(Type::Int64, 0, 5, 2, &[123i64]);
    let flba_file = build_decimal_file(Type::FixedLenByteArray, 4, 5, 2, &[123i64]);
    let expect = vec![vec![PqValue::Decimal { unscaled: 123, scale: 2 }]];
    assert_eq!(extract(&i32_file, &["d"]).unwrap(), expect);
    assert_eq!(extract(&i64_file, &["d"]).unwrap(), expect);
    assert_eq!(extract(&flba_file, &["d"]).unwrap(), expect);
}

#[test]
fn extract_int96_plain_required() {
    let f = build_int96_file(&[(0, 2_440_588), (0, 2_440_589)]);
    assert_eq!(
        extract(&f, &["ts"]).unwrap(),
        vec![
            vec![PqValue::Timestamp(0)],
            vec![PqValue::Timestamp(86_400_000_000_000)],
        ]
    );
}
```

The `build_decimal_file(ptype, type_length, precision, scale, unscaled_vals)` and `build_int96_file(rows)` hand-builders mirror the existing `build_parquet_file` / SP104 / SP107 builders, with the schema-element extensions emitting field-2 type_length, field-6 converted_type=DECIMAL, field-7 scale, field-8 precision (the FLBA case also sets type_length on the leaf). Spell the builders fully (mirror SP107's `build_v2_plain_i64` structural pattern; the only delta is the schema-leaf field encoding and the value-section bytes per physical type).

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet 2>&1 | tail -25` → compile error (no PlainSpec; no Timestamp/Decimal variants on PqValue; pq_to_cell non-exhaustive).

- [ ] **Step 3: Implement.**

  1. **`PqValue` variant additions** (`lib.rs:22`):
     ```rust
     #[derive(Clone, Debug, PartialEq)]
     pub enum PqValue {
         Null,
         Bool(bool),
         I64(i64),
         F64(f64),
         Bytes(Vec<u8>),
         /// INT96 → nanoseconds since the Unix epoch (Julian day
         /// 2440588 == 1970-01-01 UTC). i64 to match the catalog's
         /// `FieldKind::Timestamp` 8-byte storage. Negative for
         /// pre-1970 timestamps; the fetch-boundary coerce path
         /// currently accepts ≥0 nanos through `FieldKind::Timestamp`
         /// and any sign through `FieldKind::I64` (see record).
         Timestamp(i64),
         /// DECIMAL → unscaled i128 + scale. Logical value =
         /// unscaled / 10^scale. i128 covers Parquet's max precision (38).
         Decimal { unscaled: i128, scale: i32 },
     }
     ```

  2. **`PlainSpec` struct** (new in `plain.rs`):
     ```rust
     #[derive(Clone, Copy, Debug)]
     pub struct PlainSpec {
         pub ptype: Type,
         /// Some(n) iff ptype == FixedLenByteArray; n is the byte width.
         pub flba_len: Option<usize>,
         /// Some iff this leaf carries a DECIMAL logical type.
         pub decimal: Option<DecimalSpec>,
     }
     #[derive(Clone, Copy, Debug)]
     pub struct DecimalSpec { pub precision: u32, pub scale: u32 }

     impl PlainSpec {
         pub fn plain(ptype: Type) -> Self {
             Self { ptype, flba_len: None, decimal: None }
         }
         pub fn flba(n: usize) -> Self {
             Self { ptype: Type::FixedLenByteArray, flba_len: Some(n), decimal: None }
         }
         pub fn flba_decimal(n: usize, precision: u32, scale: u32) -> Self {
             Self { ptype: Type::FixedLenByteArray, flba_len: Some(n),
                    decimal: Some(DecimalSpec { precision, scale }) }
         }
         pub fn int_decimal(ptype: Type, precision: u32, scale: u32) -> Self {
             // ptype must be Int32 or Int64; caller's responsibility.
             Self { ptype, flba_len: None,
                    decimal: Some(DecimalSpec { precision, scale }) }
         }
         pub fn byte_array_decimal(precision: u32, scale: u32) -> Self {
             Self { ptype: Type::ByteArray, flba_len: None,
                    decimal: Some(DecimalSpec { precision, scale }) }
         }
     }
     ```

  3. **`decode_plain` signature change.** New signature:
     ```rust
     pub fn decode_plain(
         data: &[u8],
         spec: PlainSpec,
         count: usize,
     ) -> Result<Vec<PqValue>, PqError>
     ```
     Body restructured. The existing 6 type arms get spec-aware
     branches:
     - `Type::Int32 / Int64`: if `spec.decimal.is_some()` → produce
       `PqValue::Decimal { unscaled: i128 widened, scale }`. Else →
       existing `PqValue::I64` arm (byte-identical).
     - `Type::Boolean / Float / Double`: if `spec.decimal.is_some()`
       → `Bad("DECIMAL on incompatible physical type")`. Else existing
       arm (byte-identical).
     - `Type::ByteArray`: if `spec.decimal.is_some()` → decode each
       value as `[u32 LE len][BE-bytes]`, sign-extend to i128, produce
       `PqValue::Decimal`. Else existing `PqValue::Bytes` arm
       (byte-identical).
     - `Type::Int96` (NEW arm): each value is 12 bytes — 8 bytes
       `nanos_of_day` u64 LE + 4 bytes `julian_day` u32 LE. Apply the
       Decision-1 checked-arithmetic:
       ```rust
       let need = count.checked_mul(12).ok_or_else(|| bad("int96 ovf"))?;
       let s = data.get(..need).ok_or_else(|| bad("int96 truncated"))?;
       for ch in s.chunks_exact(12) {
           let nod_bytes: [u8; 8] = ch[..8].try_into().unwrap();
           let jd_bytes: [u8; 4] = ch[8..12].try_into().unwrap();
           let nod = u64::from_le_bytes(nod_bytes);
           let jd = u32::from_le_bytes(jd_bytes);
           if nod >= 86_400_000_000_000 {
               return Err(bad("int96 nanos-of-day out of range"));
           }
           let day_offset = i64::from(jd).checked_sub(2_440_588)
               .ok_or_else(|| bad("int96 julian day range"))?;
           let day_ns = day_offset.checked_mul(86_400_000_000_000)
               .ok_or_else(|| bad("int96 ns overflow"))?;
           let nod_i64 = i64::try_from(nod)
               .map_err(|_| bad("int96 nanos-of-day too large"))?;
           let ns = day_ns.checked_add(nod_i64)
               .ok_or_else(|| bad("int96 ns overflow"))?;
           out.push(PqValue::Timestamp(ns));
       }
       ```
     - `Type::FixedLenByteArray` (NEW arm): `let n = spec.flba_len.ok_or_else(|| bad("FLBA missing type_length"))?;` then `let need = count.checked_mul(n).ok_or_else(|| bad("flba ovf"))?;` and chunks_exact(n). If `spec.decimal.is_some()` → sign-extend each n-byte BE value to i128 → `PqValue::Decimal`. Else → `PqValue::Bytes(chunk.to_vec())`. Validate `n > 0 && n <= 16` when decimal; `n > 0 && n <= 65_536` (FLBA_MAX_WIDTH constant) when not.
     - **`Type::Other(_)`** → still `Unsupported` (unchanged).

  4. **`lib.rs` call-site updates.** Build `PlainSpec` per-leaf at the
     point where `wanted_ptypes`/`wanted_max_def_levels` are pushed
     (line ~499). Add a parallel `wanted_specs: Vec<PlainSpec>` vector.
     The build function:
     ```rust
     fn build_plain_spec(leaf: &meta::SchemaLeaf) -> Result<PlainSpec, PqError> {
         use meta::Type::*;
         // Validate DECIMAL constraints first.
         if leaf.logical_decimal {
             if leaf.precision < 1 || leaf.precision > 38 {
                 return Err(PqError::Unsupported(format!(
                     "DECIMAL precision {} (must be 1..=38): OBJ-2c-4",
                     leaf.precision
                 )));
             }
             if leaf.scale < 0 || leaf.scale > leaf.precision {
                 return Err(PqError::Bad(format!(
                     "DECIMAL scale {} out of range for precision {}",
                     leaf.scale, leaf.precision
                 )));
             }
             // Physical-type cross-check.
             match leaf.ptype {
                 Int32 if leaf.precision > 9 =>
                     return Err(PqError::Bad(
                         "DECIMAL precision > 9 on INT32 physical type".into())),
                 Int64 if leaf.precision > 18 =>
                     return Err(PqError::Bad(
                         "DECIMAL precision > 18 on INT64 physical type".into())),
                 FixedLenByteArray | ByteArray | Int32 | Int64 => {}
                 _ => return Err(PqError::Bad(
                     "DECIMAL on incompatible physical type".into())),
             }
         }
         match leaf.ptype {
             FixedLenByteArray => {
                 let n = usize::try_from(leaf.type_length)
                     .map_err(|_| PqError::Bad("FLBA type_length range".into()))?;
                 if n == 0 || n > 65_536 {
                     return Err(PqError::Bad("FLBA type_length out of range".into()));
                 }
                 if leaf.logical_decimal {
                     if n > 16 {
                         return Err(PqError::Bad(
                             "DECIMAL FLBA byte width > 16 (overflows i128)".into()));
                     }
                     Ok(PlainSpec::flba_decimal(n,
                         leaf.precision as u32, leaf.scale as u32))
                 } else {
                     Ok(PlainSpec::flba(n))
                 }
             }
             Int32 | Int64 if leaf.logical_decimal => Ok(PlainSpec::int_decimal(
                 leaf.ptype, leaf.precision as u32, leaf.scale as u32)),
             ByteArray if leaf.logical_decimal => Ok(PlainSpec::byte_array_decimal(
                 leaf.precision as u32, leaf.scale as u32)),
             _ => Ok(PlainSpec::plain(leaf.ptype)),
         }
     }
     ```

  5. **Lift the gate** (line 499–511). Replace the match:
     ```rust
     match leaf.ptype {
         meta::Type::Boolean
         | meta::Type::Int32
         | meta::Type::Int64
         | meta::Type::Float
         | meta::Type::Double
         | meta::Type::ByteArray
         | meta::Type::Int96
         | meta::Type::FixedLenByteArray => {}
         t => {
             return Err(PqError::Unsupported(format!(
                 "physical type {t:?}: OBJ-2c"
             )))
         }
     }
     let spec = build_plain_spec(leaf)?;
     wanted_specs.push(spec);
     wanted_ptypes.push(leaf.ptype);
     wanted_max_def_levels.push(max_def_level);
     ```

  6. **Thread `PlainSpec`** through `decode_page` (line 111),
     `decode_data_page_v2` (line 168), `read_chunk_values` (line 283).
     `decode_page` signature: `fn decode_page(payload: &[u8],
     dp_encoding: i32, spec: &plain::PlainSpec, n: usize, max_def_level:
     u32, dict: &[PqValue]) -> Result<Vec<PqValue>, PqError>`. Body:
     replace every `plain::decode_plain(..., wp, ...)` with
     `plain::decode_plain(..., *spec, ...)`. Same for V2. In
     `read_chunk_values`, the dict-page decode at line 341 becomes
     `plain::decode_plain(&payload, *spec, dn)?` where `spec` is
     passed in. `read_chunk_values` gains a `spec: &plain::PlainSpec`
     param.

  7. **Update the chunk-loop caller** (`extract()`, line 541):
     ```rust
     let vals = read_chunk_values(bytes, cc,
         &wanted_specs[ci], wanted_max_def_levels[ci])?;
     ```
     Remove `wanted_ptypes` from the call (replaced by spec.ptype).
     The `wanted_ptypes` vec stays (it's the ColumnMetaData cross-check
     at line 535) — value identical to `spec.ptype`. Confirm the
     dict-encoded-data-page-without-dictionary-page-offset guard at
     line 386 still fires (unchanged — it's a dp_encoding check).

  8. **`kessel-fetch::pq_to_cell` arms.** In
     `crates/kessel-fetch/src/lib.rs:258`, add inside the match:
     ```rust
     Timestamp(ns) => json::Cell::Text(ns.to_string()),
     Decimal { unscaled, scale: _ } => json::Cell::Text(unscaled.to_string()),
     ```
     The `scale` is dropped at the fetch boundary today (intentional;
     the user maps the column to `FieldKind::I128`/`I64` for the
     unscaled integer; future `Fixed{scale}` mapping is the
     immediate follow-up — flagged in the spec). Add a doc-comment
     above the match noting this.

- [ ] **Step 4:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet 2>&1 | tail -30` → all 5 new plain.rs tests + the new lib.rs cross-physical determinism test + the new INT96 plain extract test PASS; **EVERY** existing V1 OBJ-2a/2b/2c-1 + V2 OBJ-2c-3 test passes byte-unchanged (including the V1-ordering regression KAT, the SP104 lying-comp-size lock, the SP107 source-independence pin, all 17 SP107 pentest_v2 cases). `FAILED=0`.

- [ ] **Step 5:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green. Record measured total.

- [ ] **Step 6: Commit** (single commit so the workspace compiles end-to-end at every point in history):
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/plain.rs crates/kessel-parquet/src/lib.rs crates/kessel-fetch/src/lib.rs && git commit -m "parquet: decode INT96 timestamps + DECIMAL (INT32/INT64/FLBA/BYTE_ARRAY); FLBA non-DECIMAL → Bytes; PlainSpec refactor; pq_to_cell arms" && git push
```

---

### Task 4: Real pyarrow fixtures + roundtrips + 7th e2e (#195)

**Files:** Create `crates/kessel-parquet/tests/fixtures/{int96_plain,int96_dict,int96_v2_snappy,int96_optional,decimal_int32,decimal_int64,decimal_flba,decimal_flba_optional,decimal_int32_dict,flba_uuid}.parquet`; modify `.../README.md`, `crates/kessel-parquet/tests/fixture_roundtrip.rs`, `crates/kesseldb-server/tests/external_source_parquet_oracle.rs`.

- [ ] **Step 1: Generate (real pyarrow 24.0.0; verified at planning time):**
```bash
cd /c/Users/ihass/KesselDB && python -c "
import pyarrow as pa, pyarrow.parquet as pq
from decimal import Decimal
FIX = 'crates/kessel-parquet/tests/fixtures'

# === INT96 ===
schI96 = pa.schema([pa.field('ts', pa.timestamp('ns'), nullable=False)])
# 1970-01-01, 1970-01-02, 1969-12-31 — proves Julian-day +1/-1 conversion.
tI96 = pa.table({'ts': pa.array([0, 86_400_000_000_000, -86_400_000_000_000],
                                 type=pa.timestamp('ns'))}, schema=schI96)
pq.write_table(tI96, f'{FIX}/int96_plain.parquet',
               use_deprecated_int96_timestamps=True, use_dictionary=False,
               compression=None, version='1.0', data_page_version='1.0')
pq.write_table(tI96, f'{FIX}/int96_dict.parquet',
               use_deprecated_int96_timestamps=True, use_dictionary=True,
               compression=None, version='1.0', data_page_version='1.0')
pq.write_table(tI96, f'{FIX}/int96_v2_snappy.parquet',
               use_deprecated_int96_timestamps=True, use_dictionary=False,
               compression='snappy', version='1.0', data_page_version='2.0')
schI96N = pa.schema([pa.field('ts', pa.timestamp('ns'), nullable=True)])
tI96N = pa.table({'ts': pa.array([0, None, -86_400_000_000_000],
                                  type=pa.timestamp('ns'))}, schema=schI96N)
pq.write_table(tI96N, f'{FIX}/int96_optional.parquet',
               use_deprecated_int96_timestamps=True, version='1.0',
               data_page_version='1.0')

# === DECIMAL ===
# INT32-backed (precision ≤ 9): store_decimal_as_integer=True.
tDi32 = pa.table({'d': pa.array([Decimal('1.23'), Decimal('-4.56'), Decimal('100.00')],
                                  type=pa.decimal128(5, 2))})
pq.write_table(tDi32, f'{FIX}/decimal_int32.parquet',
               use_dictionary=False, compression=None, version='2.6',
               data_page_version='1.0', store_decimal_as_integer=True)
pq.write_table(tDi32, f'{FIX}/decimal_int32_dict.parquet',
               use_dictionary=True, compression=None, version='2.6',
               data_page_version='1.0', store_decimal_as_integer=True)

# INT64-backed (10..=18): store_decimal_as_integer=True.
tDi64 = pa.table({'d': pa.array([Decimal('1.234'), Decimal('-4.567'), Decimal('100000.000')],
                                  type=pa.decimal128(18, 3))})
pq.write_table(tDi64, f'{FIX}/decimal_int64.parquet',
               use_dictionary=False, compression=None, version='2.6',
               data_page_version='1.0', store_decimal_as_integer=True)

# FLBA-backed (default writer; precision 30 forces FLBA).
tDflba = pa.table({'d': pa.array([Decimal('1.23456'), Decimal('-4.56789'),
                                    Decimal('100000.00000')],
                                   type=pa.decimal128(30, 5))})
pq.write_table(tDflba, f'{FIX}/decimal_flba.parquet',
               use_dictionary=False, compression=None, version='1.0',
               data_page_version='1.0')

# FLBA-DECIMAL OPTIONAL (null scatter).
schDN = pa.schema([pa.field('d', pa.decimal128(30, 5), nullable=True)])
tDN = pa.table({'d': pa.array([Decimal('1.23456'), None, Decimal('-4.56789')],
                                type=pa.decimal128(30, 5))}, schema=schDN)
pq.write_table(tDN, f'{FIX}/decimal_flba_optional.parquet',
               version='1.0', data_page_version='1.0')

# === FLBA non-DECIMAL (UUID-like binary(16)) ===
import uuid
schU = pa.schema([pa.field('u', pa.binary(16), nullable=False)])
tU = pa.table({'u': pa.array([b'\\x01' * 16, b'\\x02' * 16, b'\\x03' * 16],
                              type=pa.binary(16))}, schema=schU)
pq.write_table(tU, f'{FIX}/flba_uuid.parquet',
               use_dictionary=False, compression=None, version='1.0',
               data_page_version='1.0')
print('wrote 10 fixtures')
"
```
If pyarrow fails on any fixture → STOP, BLOCKED.

- [ ] **Step 2: Metadata-verify every fixture** before any Rust test depends on it:
```bash
cd /c/Users/ihass/KesselDB && python -c "
import pyarrow.parquet as pq
FIX = 'crates/kessel-parquet/tests/fixtures'
expected = {
  'int96_plain': ('INT96', None, None),
  'int96_dict':  ('INT96', None, None),
  'int96_v2_snappy': ('INT96', None, None),
  'int96_optional':  ('INT96', None, None),
  'decimal_int32': ('INT32', 'DECIMAL', 'Decimal(precision=5, scale=2)'),
  'decimal_int32_dict': ('INT32', 'DECIMAL', 'Decimal(precision=5, scale=2)'),
  'decimal_int64': ('INT64', 'DECIMAL', 'Decimal(precision=18, scale=3)'),
  'decimal_flba':  ('FIXED_LEN_BYTE_ARRAY', 'DECIMAL', 'Decimal(precision=30, scale=5)'),
  'decimal_flba_optional': ('FIXED_LEN_BYTE_ARRAY', 'DECIMAL', 'Decimal(precision=30, scale=5)'),
  'flba_uuid':     ('FIXED_LEN_BYTE_ARRAY', None, None),
}
for name, (ept, ect, elt) in expected.items():
  pf = pq.ParquetFile(f'{FIX}/{name}.parquet')
  col = pf.metadata.schema.column(0)
  pt = col.physical_type
  ct = col.converted_type
  lt = str(col.logical_type)
  ok = (pt == ept) and (ect is None or ct == ect) and (elt is None or elt in lt)
  print(f'{name}: phys={pt} conv={ct} logical={lt} {\"OK\" if ok else \"FAIL\"}')
  assert ok, f'metadata mismatch for {name}'
print('all 10 verified')
"
```
If any fixture's metadata doesn't match the expected physical/converted/logical types → STOP, BLOCKED (regen with adjusted pyarrow params; do not proceed with a mislabeled fixture).

- [ ] **Step 3: README** — append blocks for the new fixtures mirroring the existing SP107 v2_*/SP106 gzip_* convention. Cite the regen commands above + the metadata-verify expectations. Note explicitly: **BYTE_ARRAY DECIMAL is supported by the decoder (hand-KAT pinned) but pyarrow 24.0.0 doesn't write it** — no real fixture covers BYTE_ARRAY DECIMAL; the decode path is non-self-referentially exercised by the hand-built KAT in `lib.rs#tests`.

- [ ] **Step 4: Roundtrip tests** — READ `crates/kessel-parquet/tests/fixture_roundtrip.rs` for the convention; add fixture-include consts and tests for each new fixture:

```rust
const INT96_PLAIN: &[u8] = include_bytes!("fixtures/int96_plain.parquet");
const INT96_DICT: &[u8] = include_bytes!("fixtures/int96_dict.parquet");
const INT96_V2_SNAPPY: &[u8] = include_bytes!("fixtures/int96_v2_snappy.parquet");
const INT96_OPTIONAL: &[u8] = include_bytes!("fixtures/int96_optional.parquet");
const DEC_I32: &[u8] = include_bytes!("fixtures/decimal_int32.parquet");
const DEC_I32_DICT: &[u8] = include_bytes!("fixtures/decimal_int32_dict.parquet");
const DEC_I64: &[u8] = include_bytes!("fixtures/decimal_int64.parquet");
const DEC_FLBA: &[u8] = include_bytes!("fixtures/decimal_flba.parquet");
const DEC_FLBA_OPT: &[u8] = include_bytes!("fixtures/decimal_flba_optional.parquet");
const FLBA_UUID: &[u8] = include_bytes!("fixtures/flba_uuid.parquet");

#[test]
fn int96_plain_fixture_roundtrips() {
    let rows = extract(INT96_PLAIN, &["ts"]).unwrap();
    assert_eq!(rows, vec![
        vec![PqValue::Timestamp(0)],
        vec![PqValue::Timestamp(86_400_000_000_000)],
        vec![PqValue::Timestamp(-86_400_000_000_000)],
    ]);
}
#[test]
fn int96_plain_vs_dict_vs_v2_source_independence() {
    let plain = extract(INT96_PLAIN, &["ts"]).unwrap();
    let dict = extract(INT96_DICT, &["ts"]).unwrap();
    let v2sn = extract(INT96_V2_SNAPPY, &["ts"]).unwrap();
    assert_eq!(plain, dict);
    assert_eq!(plain, v2sn);
}
#[test]
fn int96_optional_fixture_roundtrips() {
    assert_eq!(
        extract(INT96_OPTIONAL, &["ts"]).unwrap(),
        vec![
            vec![PqValue::Timestamp(0)],
            vec![PqValue::Null],
            vec![PqValue::Timestamp(-86_400_000_000_000)],
        ]
    );
}
#[test]
fn decimal_3way_source_independence_real_pyarrow() {
    // INT32(5,2) values "1.23", "-4.56", "100.00":
    //   unscaled = 123, -456, 10000 ; scale = 2.
    let i32_rows = extract(DEC_I32, &["d"]).unwrap();
    let expect_i32 = vec![
        vec![PqValue::Decimal { unscaled: 123, scale: 2 }],
        vec![PqValue::Decimal { unscaled: -456, scale: 2 }],
        vec![PqValue::Decimal { unscaled: 10000, scale: 2 }],
    ];
    assert_eq!(i32_rows, expect_i32);
    // INT32 dict — same logical values.
    assert_eq!(extract(DEC_I32_DICT, &["d"]).unwrap(), expect_i32);
    // INT64(18,3) values "1.234", "-4.567", "100000.000":
    //   unscaled = 1234, -4567, 100000000 ; scale = 3.
    assert_eq!(extract(DEC_I64, &["d"]).unwrap(), vec![
        vec![PqValue::Decimal { unscaled: 1234, scale: 3 }],
        vec![PqValue::Decimal { unscaled: -4567, scale: 3 }],
        vec![PqValue::Decimal { unscaled: 100_000_000, scale: 3 }],
    ]);
    // FLBA(30,5) values "1.23456", "-4.56789", "100000.00000":
    //   unscaled = 123456, -456789, 10000000000000 ; scale = 5.
    assert_eq!(extract(DEC_FLBA, &["d"]).unwrap(), vec![
        vec![PqValue::Decimal { unscaled: 123_456, scale: 5 }],
        vec![PqValue::Decimal { unscaled: -456_789, scale: 5 }],
        vec![PqValue::Decimal { unscaled: 10_000_000_000_000, scale: 5 }],
    ]);
}
#[test]
fn decimal_flba_optional_fixture_roundtrips() {
    assert_eq!(extract(DEC_FLBA_OPT, &["d"]).unwrap(), vec![
        vec![PqValue::Decimal { unscaled: 123_456, scale: 5 }],
        vec![PqValue::Null],
        vec![PqValue::Decimal { unscaled: -456_789, scale: 5 }],
    ]);
}
#[test]
fn flba_uuid_fixture_roundtrips_to_bytes() {
    assert_eq!(extract(FLBA_UUID, &["u"]).unwrap(), vec![
        vec![PqValue::Bytes(vec![1u8; 16])],
        vec![PqValue::Bytes(vec![2u8; 16])],
        vec![PqValue::Bytes(vec![3u8; 16])],
    ]);
}
```

The 4-way DECIMAL determinism pin (INT32 + INT64 + FLBA + BYTE_ARRAY) is partially covered here (3-way via real pyarrow fixtures) and completed in T3's `extract_decimal_cross_physical_type_determinism_pin` hand-KAT (which has all 4 — see T3 Step 1).

- [ ] **Step 5: 7th e2e (via the T1 FailClosedCase struct)** — add to `external_source_parquet_oracle.rs`:
```rust
const DECIMAL_FLBA_PARQUET_FIXTURE: &[u8] =
    include_bytes!("../../kessel-parquet/tests/fixtures/decimal_flba.parquet");

#[test]
fn refresh_decimal_parquet_from_s3_fails_closed_and_state_intact() {
    run_fail_closed_parquet_e2e(FailClosedCase {
        fixture: DECIMAL_FLBA_PARQUET_FIXTURE,
        tag: "decpq",
        keyid_env: "OBJ_DEC_KEYID",
        secret_env: "OBJ_DEC_SECRET",
        keyid_val: "AKIAEXAMPLE7",
        secret_val: "secretexamplekey7",
        source: "decfeed",
        // The DDL maps DECIMAL → I128 (unscaled integer end-to-end works today).
        // Future Fixed-coerce path will allow `d FIXED(5) NOT NULL FROM 'd'`.
        ddl_cols: "d I128 NOT NULL FROM 'd'",
        s3_path: "decimal.parquet",
    });
}
```
The path is exercised at the typed-error layer (untrusted TLS → fail-closed before any decode); the fixture's content doesn't affect the assertion. This is the 7th e2e and confirms the FailClosedCase refactor handles a new case cleanly.

- [ ] **Step 6:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet --test fixture_roundtrip 2>&1 | tail -15` (new roundtrips PASS) AND `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources-objstore --test external_source_parquet_oracle 2>&1 | tail -10` (7 pass).

- [ ] **Step 7:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green; existing oracles unchanged.

- [ ] **Step 8: Commit** (verify the 10 `.parquet` binaries staged):
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/tests/ crates/kesseldb-server/tests/external_source_parquet_oracle.rs && git commit -m "parquet: real pyarrow INT96+DECIMAL+FLBA fixtures (10) + roundtrips + 7th e2e" && git push
```

---

### Task 5: Pentest pass (#196)

**Files:** Modify `crates/kessel-parquet/src/lib.rs` (`mod tests` — append a `#[cfg(test)] mod pentest_int96_decimal`, matching the SP107 `mod pentest_v2` convention).

- [ ] **Step 1:** Add `catch_unwind` `nb`-style locks (helper mirroring the established `nb`/`no_panic_*` in the prior pentest modules) using hand-built INT96/DECIMAL/FLBA corruption files + the existing hand-builders extended:

  **Hostile INT96:**
  - `int96_truncated_payload` → `Bad`.
  - `int96_julian_day_max_overflow` (jd = `u32::MAX`) → `Bad`.
  - `int96_nanos_of_day_out_of_range` (nod = `100_000_000_000_000`) → `Bad`.
  - `int96_dict_index_out_of_range` (dict has 1 entry; data page index = 5) → `Bad`.
  - `int96_v2_snappy_corrupt` (V2 with codec=Snappy, garbage values section) → `Bad` (no panic/OOM).

  **Hostile DECIMAL:**
  - `decimal_precision_gt_38` (schema converted_type=DECIMAL, precision=40) → `Unsupported`.
  - `decimal_precision_lt_1` → `Unsupported` or `Bad` (per the `build_plain_spec` guard).
  - `decimal_scale_negative` → `Bad`.
  - `decimal_scale_gt_precision` → `Bad`.
  - `decimal_flba_width_17` (type_length=17, > 16 i128 max) → `Bad`.
  - `decimal_flba_width_0` → `Bad`.
  - `decimal_int32_precision_15` (INT32-physical, prec=15 > 9) → `Bad`.
  - `decimal_int64_precision_25` (INT64-physical, prec=25 > 18) → `Bad`.
  - `decimal_byte_array_value_length_17` (BA-DECIMAL value's u32-LE length = 17, > 16) → `Bad`.
  - `decimal_converted_vs_logical_disagree` (converted_type DEC + LogicalType DEC with different precision) → `Bad`.

  **Hostile FLBA non-DECIMAL:**
  - `flba_type_length_huge` (`type_length = 70_000` > 65_536 cap) → `Bad`.
  - `flba_truncated` (count*N > data.len()) → `Bad`.

  **Positive correctness locks (assert exact `Ok`):**
  - `int96_plain_required_ok` — `[Timestamp(0), Timestamp(+1day_ns)]`.
  - `int96_plain_optional_with_null_ok` — scatter correct.
  - `int96_dict_ok` — matches PLAIN.
  - `int96_v2_ok` — matches V1.
  - `decimal_4way_determinism_pin_ok` — INT32 = INT64 = FLBA = BYTE_ARRAY hand-KAT yields identical PqValue::Decimal.
  - `decimal_flba_dict_ok` — dict-encoded FLBA-DECIMAL same as PLAIN.
  - `decimal_flba_optional_ok` — null scatter.
  - `flba_uuid_required_ok` — Bytes vector of width N.

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-parquet pentest_int96_decimal 2>&1 | tail -20` → all pass FAST (no hang/OOM). If a positive lock fails → BLOCKED (decoder bug, never weaken). If a hostile case panics/OOMs/hangs → BLOCKED (real vuln, exact detail).

- [ ] **Step 3:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, seed-7 green.

- [ ] **Step 4: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add crates/kessel-parquet/src/lib.rs && git commit -m "parquet: pentest lock tests for INT96 + DECIMAL + FLBA decode (no panic/OOM)" && git push
```

---

### Task 6: Docs + gate reconciliation + memory (#197)

**Files:** Create `docs/superpowers/specs/2026-05-19-kesseldb-subproject108-int96-decimal.md`; modify `docs/STATUS.md`, `docs/USAGE.md`; modify (auto-memory, OUTSIDE repo, never git-add) `…\memory\project_kesseldb.md`, `…\memory\MEMORY.md`.

- [ ] **Step 1: Measure.** `cargo test --workspace --release 2>&1 | tail -25` → `<FINAL>`; `<DELTA> = <FINAL> − <BASELINE>` (Task 0's). FAILED=0, seed-7 green. `cargo tree -p kesseldb-server | grep -Ei "parquet|objstore|rustls|webpki"` none; `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty.

- [ ] **Step 2: Internal record.** Read `docs/superpowers/specs/2026-05-19-kesseldb-subproject107-v2pages.md` for the EXACT convention, then create `docs/superpowers/specs/2026-05-19-kesseldb-subproject108-int96-decimal.md` mirroring it: `# KesselDB — Subproject 108: OBJ-2c-4 Parquet INT96 + DECIMAL`; `**Date:** 2026-05-19  **Status:** done — code + tests committed and passing.`; bare-backtick Builds-on (subproject 100–107 records) + Design + Plan lines; `---` separators. Sections:
   - **What shipped:** meta.rs SchemaElement extensions (converted_type / type_length / scale / precision / logical_type union); SchemaLeaf gains precision/scale/type_length/logical_decimal; plain.rs PlainSpec refactor; INT96 → `PqValue::Timestamp(i64 ns)` via Julian-day checked-arithmetic; DECIMAL → `PqValue::Decimal { unscaled: i128, scale: i32 }` for physical INT32/INT64/FLBA/BYTE_ARRAY; FLBA non-DECIMAL → `PqValue::Bytes`; kessel-fetch::pq_to_cell adds Timestamp/Decimal text-cell arms (workspace-compile mandatory); supported matrix now flat REQUIRED|OPTIONAL × UNCOMPRESSED|Snappy|GZIP × V1|V2 × PLAIN|dict × {BOOL, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, INT96 (→ Timestamp), FLBA (raw or DECIMAL)}.
   - **T1 disclosure:** `run_fail_closed_parquet_e2e` 9-positional → `FailClosedCase` struct conversion (the SP107-tracked follow-on per the 7th call-site trigger); all 6 prior e2e observable assertions preserved; net-0 test count.
   - **Cross-crate impact:** kessel-fetch::pq_to_cell exhaustive-match required the new Timestamp/Decimal arms in the same T3 commit; today text-form unscaled-integer rendering routes through FieldKind::I128/I64 end-to-end; FieldKind::Fixed{scale} end-to-end is the immediate follow-up (a one-arm coerce extension).
   - **Verification:** hand-built KATs (decode_schema_element field-2/6/7/8/10 arms; PlainSpec INT96 conversion; FLBA sign-extension; cross-physical 4-way DECIMAL determinism; INT96 plain/dict/V2 source-independence) derived from parquet.thrift; real pyarrow 10 fixtures (4 INT96 + 5 DECIMAL + 1 FLBA-UUID) all metadata-verified; roundtrip via production extract(); 3-way real-pyarrow DECIMAL source-independence (INT32 + INT64 + FLBA — BYTE_ARRAY hand-KAT only, pyarrow doesn't write it); 7th e2e fail-closed via FailClosedCase struct; pentest (precision>38, scale OOR, FLBA width >16, INT96 jd overflow / nod OOR / truncated, converted-vs-logical disagree + positive locks for all positive paths).
   - **Honest gate accounting:** `<BASELINE>` → `<FINAL>` (+`<DELTA>`); NOT a zero-delta (SP100–107 stance; per-slice +DELTA authoritative per the tracked nit); T1 net-0 (struct-form refactor); kernel zero-dep; deps empty; seed-7 green; EXT/TLS/OBJ-1 oracles 2/1/1 unchanged; all V1 OBJ-2a/2b/2c-1 + V2 OBJ-2c-3 paths byte-unchanged.
   - **Deferred (immediate follow-ups + OBJ-2c-2/5):** DECIMAL → FieldKind::Fixed{scale} coerce path (immediate; ~50-line coerce arm); pre-1970 INT96 through FieldKind::Timestamp coerce path (immediate; one-line coerce extension or signed-Timestamp FieldKind); LogicalType union arms beyond DecimalType (UUID/TimestampType/etc.) — future enhancement; BYTE_ARRAY DECIMAL real-pyarrow fixture (pyarrow doesn't write it; hand-KAT covers); DECIMAL precision > 38 → Unsupported; REPEATED/nested INT96/DECIMAL still rejected (OBJ-2c-5); zstd (OBJ-2c-2 resequenced).

- [ ] **Step 3: STATUS.md** — insert SP108 row immediately AFTER the SP107 row (numeric order), matching the SP107 row format incl. gate `<BASELINE>→<FINAL> (+<DELTA>; …; not zero-delta)`, `Record:` backlink, clause: "INT96 timestamps now decoded to typed PqValue::Timestamp(i64 ns); DECIMAL logical type decoded to PqValue::Decimal { unscaled: i128, scale: i32 } for physical INT32/INT64/FLBA/BYTE_ARRAY backings; FLBA non-DECIMAL → PqValue::Bytes; kessel-fetch::pq_to_cell text-form arms (workspace-compile mandatory; today routes through FieldKind::I128/I64 for unscaled-integer end-to-end). meta.rs SchemaElement extensions (converted_type / type_length / scale / precision / LogicalType DecimalType union). T1 = FailClosedCase struct conversion (SP107-tracked refactor, 9 positional → struct at all 6 call-sites). Honest gate: <BASELINE>→<FINAL> (+<DELTA>; not zero-delta; T1 net-0). Real pyarrow 10 fixtures + hand KATs; pentest [N] locks (no vuln found). Still typed-Unsupported: zstd (OBJ-2c-2 resequenced); REPEATED/nested incl V2 rep-levels (OBJ-2c-5); >64MiB; DECIMAL precision > 38; pre-1970 INT96 through FieldKind::Timestamp coerce + DECIMAL→FieldKind::Fixed coerce are flagged immediate follow-ups."

- [ ] **Step 4: docs/USAGE.md** — append a §7g `> **OBJ-2c-4 (SP108):**` note (no overclaim; reference the immediate-follow-up boundaries: Fixed-coerce, signed-Timestamp coerce) AND update the cumulative "### Parquet scope: what is currently supported (OBJ-2a → OBJ-2c-3)" table: retitle heading `(OBJ-2a → OBJ-2c-4)`; replace the Physical types row → `BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, INT96 (→ Timestamp), FixedLenByteArray (raw bytes or DECIMAL)`; add a new row `Logical types | DECIMAL{precision ≤ 38, scale} (typed PqValue::Decimal{ unscaled: i128, scale })`; add a new row `Temporal | INT96 → PqValue::Timestamp (Unix ns; ≥1970 end-to-end today via FieldKind::Timestamp; any sign via FieldKind::I64)`; update the NOT-supported list: REMOVE the line `**\`INT96\` / \`FIXED_LEN_BYTE_ARRAY\` / \`DECIMAL\`** physical types — rejected with \`Unsupported("INT96/FIXED_LEN_BYTE_ARRAY: OBJ-2c")\``; ADD `**DECIMAL precision > 38** — rejected with \`Unsupported("DECIMAL precision … (must be 1..=38): OBJ-2c-4")\``; ADD `**Pre-1970 INT96 through \`FieldKind::Timestamp\` coerce** — typed \`FetchError::Type\` at coerce time (decoder produces correct negative-Timestamp; map to \`FieldKind::I64\` for any sign; immediate follow-up: signed-Timestamp FieldKind)`; ADD `**DECIMAL → \`FieldKind::Fixed\` coerce** — typed \`FetchError::Type\` at coerce time (\`Fixed\` is internal-only today; immediate follow-up: \`to_field_bytes\` Fixed arm). Mapping DECIMAL → \`FieldKind::I128\`/\`I64\` (unscaled integer) works today.` Confirm no §7g-vs-table contradiction; no stale tag.

- [ ] **Step 5:** `cargo test --workspace --release 2>&1 | tail -12` → `FAILED=0`, total == `<FINAL>`, seed-7 green.

- [ ] **Step 6: Commit docs**
```bash
cd /c/Users/ihass/KesselDB && git add docs/superpowers/specs/2026-05-19-kesseldb-subproject108-int96-decimal.md docs/STATUS.md docs/USAGE.md && git commit -m "docs: OBJ-2c-4 INT96+DECIMAL — subproject108 record + STATUS/USAGE cumulative-table + gate reconciliation" && git push
```

- [ ] **Step 7: Auto-memory (OUTSIDE repo — never git-add).** Append via Bash heredoc an SP108 block (substitute `<BASELINE>`/`<FINAL>`): summarise meta SchemaElement extensions + PlainSpec refactor + INT96/FLBA/DECIMAL typed decode + kessel-fetch::pq_to_cell arms + Julian-day checked arithmetic + DECIMAL 4-way determinism + T1 FailClosedCase struct conversion (SP107-tracked); real pyarrow 10 fixtures; honest gate <BASELINE>→<FINAL>; kernel zero-dep + seed-7 + oracles 2/1/1; OBJ-2c arc 3/5 (GZIP + V2 + INT96/DECIMAL done). Then Read `/c/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/MEMORY.md`, find the `- [KesselDB](project_kesseldb.md) — …` line, Edit its trailing status clause to: `SP108 SHIPPED: OBJ-2c-4 INT96 → PqValue::Timestamp(i64 ns) + DECIMAL → PqValue::Decimal{ unscaled: i128, scale } (INT32/INT64/FLBA/BYTE_ARRAY); FLBA non-DECIMAL → Bytes; kessel-fetch pq_to_cell text-form arms. T1 = FailClosedCase struct conversion (SP107-tracked refactor). OBJ-2c arc 3/5. Open: OBJ-2c-2 zstd (resequenced) / OBJ-2c-5 REPEATED-nested / Fixed-coerce + signed-Timestamp-coerce immediate follow-ups / OBJ-3 Iceberg / OBJ-4 listing / OBJ-5 STS-SAS / WASM / #75 SP-A scatter scan / seed-7 liveness / SQL-over-cluster`. Keep the line's existing prefix intact.

- [ ] **Step 8:** `cd /c/Users/ihass/KesselDB && git status --porcelain` EMPTY (no memory path, no stray logs; rm -f any test-output.log). Report DONE.

## Self-Review

**1. Spec coverage:** FailClosedCase struct conversion → T1; meta SchemaElement field-2/6/7/8/10 + SchemaLeaf field additions → T2; plain.rs PlainSpec refactor + INT96/FLBA/DECIMAL plain decode + sign-extension + lib.rs gate flip + kessel-fetch::pq_to_cell arms + hand-built KATs (cross-physical 4-way DECIMAL; INT96 plain) → T3; real pyarrow 10 fixtures (4 INT96 + 5 DECIMAL + 1 FLBA-UUID) + metadata-verify + 3-way real-pyarrow DECIMAL source-independence + INT96 plain-vs-dict-vs-V2 source-independence + 7th e2e via FailClosedCase → T4; pentest (precision/scale/FLBA-width/INT96-overflow/converted-vs-logical hostiles + positive locks) → T5; honest gate + T1-disclosure + cross-crate-impact + SP107-convention record + cumulative USAGE table + immediate-follow-ups (Fixed-coerce, signed-Timestamp-coerce) → T6. All design sections mapped.

**2. Placeholder scan:** parquet.thrift field IDs (2:type_length, 6:converted_type, 7:scale, 8:precision, 10:logicalType) are the verified-at-planning-time IDs; T2 Step 1 directs the implementer to reconfirm against the upstream spec before coding (real cross-check, not blind copy). PlainSpec API + constructor names + field types are fully specified with one canonical form. KAT bytes (compact field headers, struct deltas, zigzag values) are spelled where needed; the hand-builder shapes for `build_decimal_file` / `build_int96_file` are directed to mirror the SP107 `build_v2_plain_i64` structural pattern (a named existing source). `<BASELINE>/<FINAL>/<DELTA>` are runtime-measured (T0/T6). No "handle edge cases"/"TBD" — the hostile vectors are enumerated; the determinism pins are spelled.

**3. Type consistency:** `decode_plain(&[u8], PlainSpec, usize) -> Result<Vec<PqValue>, PqError>`; `PlainSpec { ptype: Type, flba_len: Option<usize>, decimal: Option<DecimalSpec> }`; `DecimalSpec { precision: u32, scale: u32 }`; `PqValue::Timestamp(i64)` / `PqValue::Decimal { unscaled: i128, scale: i32 }`; `pq_to_cell` arms produce `json::Cell::Text(…)`; `SchemaLeaf` fields `precision: i32, scale: i32, type_length: i32, logical_decimal: bool`; `FailClosedCase` struct field types match the prior 9 positional types byte-for-byte; `decode_logical_type_union(&mut StructReader, &mut bool, &mut i32, &mut i32) -> Result<(), PqError>`. `build_plain_spec(&meta::SchemaLeaf) -> Result<PlainSpec, PqError>`. Everything compiles by construction (no PqValue arm of `pq_to_cell` left non-exhaustive; no `decode_plain` call-site left passing `meta::Type` instead of `PlainSpec`).

Plan is internally consistent and fully covers the OBJ-2c-4 design.
