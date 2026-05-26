# KesselDB — Subproject 144: Parquet nested decode — Map + struct columns

**Status:** design — approved by autonomous mandate substitution; implementation plan to follow. Second slice of the 3-slice OBJ-2c-5 arc (SP143 List → **SP144 Map+struct** → SP145 deep nesting).

(Note: SP144 here is the **OBJ-2c-5 SP144** for Parquet Map+struct. The HTTP gateway gap-closure slice was named **SP144H** with an "H" suffix to disambiguate.)

**Builds on:**
- `docs/superpowers/specs/2026-05-25-kesseldb-subproject143-parquet-nested-list.md` (SP143 — schema tree + multi-bit rep/def + LIST<primitive> + Dremel assembler).
- `crates/kessel-parquet/src/assembly.rs::assemble_list_primitive` — the SP143 assembler that this slice generalizes.

**Process note.** Per `feedback_kesseldb_autonomous_build`, the brainstorm gate is substituted. The user explicitly chose "LIST + struct + Map (full nested)" in the OBJ-2c-5 scoping question (see SP143 spec). This is the second slice of that arc.

---

## 1. Problem

SP143 added `List<primitive>` decode for the canonical 3-node Parquet LIST encoding. Real analytics Parquet files also use:

1. **`Map<K, V>`** — encoded as the canonical 3-node MAP pattern:
   ```
   [OPT|REQ] group <name> (MAP) {
     REPEATED group key_value {
       REQUIRED <PRIMITIVE_TYPE> key
       [OPT|REQ] <PRIMITIVE_TYPE> value
     }
   }
   ```
2. **struct columns** — any non-LIST/non-MAP group with multiple children, e.g.:
   ```
   [OPT|REQ] group user {
     REQUIRED INT64 id
     OPTIONAL BYTE_ARRAY name
     REQUIRED INT64 created_at
   }
   ```

Today `classify_column_plan` in `crates/kessel-parquet/src/lib.rs` rejects both with named SP144 errors:
- Group without `LogicalType::List` → `Unsupported("struct column without LIST annotation: SP144 follow-up (...)")` at `lib.rs:~lines_to_find`
- `Map<K,V>` is rejected upstream because the schema-tree's `LogicalType` only has `List` (no `Map` variant yet)

SP144 lifts both rejections.

---

## 2. Goals and non-goals

**Goals (V1).**

- Add `LogicalType::Map` variant + recognize the canonical 3-node MAP pattern in `meta::recognize_logical_type`.
- Add `PqValue::Map(Vec<(PqValue, PqValue)>)` variant (key-value pairs as a Vec so insertion order is preserved — Parquet MAP preserves the wire order even though Map semantics are usually unordered).
- Add `PqValue::Struct(Vec<(String, PqValue)>)` variant (named fields in schema-declared order).
- Add `assembly::assemble_map_kv` — Dremel assembler for `Map<K, V>` columns. The MAP pattern has the same rep/def shape as a LIST except the "item" at max_def is a key-value PAIR (two parallel value streams: key column + value column). Cleanest implementation: assemble two separate primitive streams (one for keys, one for values, both with the SAME rep/def streams from the SAME column-chunk pair) and zip them into pairs.
- Add `assembly::assemble_struct` — for struct columns, the rep/def levels of any single leaf within the struct apply identically to ALL leaves (REPEATED ancestors are shared, struct fields share def levels for the OPTIONAL group ancestor). Assembly is essentially: decode each leaf's column chunk independently, then zip them into a `PqValue::Struct(Vec<(String, PqValue)>)` per output row. The trick: when the outer struct group is OPTIONAL, def=0 means the struct is null (all leaves emit nothing).
- Extend `classify_column_plan` in `lib.rs::extract_nested` to recognize Map and struct patterns, dispatch to the right assembler.
- Reject `List<struct>`, `List<Map>`, `Map<K, List>`, `Map<K, struct>`, struct containing nested LIST/MAP/struct, etc. with typed `Unsupported(...)` errors naming SP145 (deep nesting).
- 4-5 real pyarrow fixtures: `map_string_i64`, `map_string_string`, `optional_map_string_i64`, `struct_i64_string`, `optional_struct`.
- Pentest matrix for the new code paths: malformed MAP encoding (REPEATED middle not named "key_value", wrong child count), struct rejected when nested-LIST-inside, MAP key REQUIRED violated (keys must be REQUIRED per spec), etc.
- Honest docs reconciliation in T-final.

**Non-goals (named, deferred to SP145).**

- `List<struct<...>>`, `List<Map<...>>` — cross-nested types. Defer to SP145.
- `Map<K, struct<...>>`, `Map<K, List<...>>` — Map with nested values. Defer to SP145.
- Struct containing LIST/MAP/struct children — defer to SP145.
- `max_rep_level ≥ 2` columns (which is what cross-nesting requires). Defer to SP145.
- Map with OPTIONAL key (Parquet spec forbids; rejected at validation time as malformed, not deferred).
- Any new compression codec, encoding, or page format.

---

## 3. Architecture

### 3.1 PqValue extensions

In `crates/kessel-parquet/src/lib.rs::PqValue`:

```rust
pub enum PqValue {
    // ... existing ...
    /// SP143: List<primitive>
    List(Vec<PqValue>),
    /// SP144: Map<K, V> — key/value pairs preserving wire order.
    /// Each pair is (key, value). Key is never Null (Parquet MAP keys are
    /// REQUIRED by spec); value may be Null when element_optional=true.
    Map(Vec<(PqValue, PqValue)>),
    /// SP144: Struct — named fields in schema-declared order. An OPTIONAL
    /// struct group whose def-level says "null" produces PqValue::Null at
    /// the column position, NOT PqValue::Struct with all-Null fields.
    Struct(Vec<(String, PqValue)>),
}
```

`kessel-fetch::pq_to_cell`'s exhaustive match (T2 SP143 added the `List` arm) needs new arms for `Map` and `Struct`. Both serialize to JSON via the existing `pqvalue_list_to_json` pattern (extend the helper or add siblings).

### 3.2 Schema-tree extensions

In `crates/kessel-parquet/src/meta.rs`:

```rust
pub enum LogicalType {
    List,
    /// SP144: MAP / MAP_KEY_VALUE (deprecated legacy alias). Per parquet
    /// LogicalTypes.md, the converted_type is MAP (1) — and the
    /// MAP_KEY_VALUE legacy variant (2) is accepted as an alias.
    Map,
}
```

`recognize_logical_type` extends:

```rust
fn recognize_logical_type(name: &str, children: &[SchemaNode], elem: &RawSchemaElement) -> Option<LogicalType> {
    if let Some(ct) = elem.converted_type {
        match ct {
            3 => return Some(LogicalType::List),  // LIST
            1 | 2 => return Some(LogicalType::Map),  // MAP, MAP_KEY_VALUE
            _ => {}
        }
    }
    // Structural LIST pattern (SP143): existing.
    // Structural MAP pattern (SP144): outer group → exactly one REPEATED
    // middle group with EXACTLY 2 children (key REQUIRED, value
    // OPT/REQ). Names by convention: middle=key_value, fields=key+value.
    if children.len() == 1 {
        if let SchemaNode::Group { repetition: Repetition::Repeated, children: gc, .. } = &children[0] {
            if gc.len() == 2 {
                // Strong signal: it's MAP. The first child should be a
                // REQUIRED leaf named "key" by Parquet convention but we
                // accept any name.
                if let (SchemaNode::Leaf { repetition: Repetition::Required, .. }, SchemaNode::Leaf { .. }) = (&gc[0], &gc[1]) {
                    return Some(LogicalType::Map);
                }
            }
            // Existing structural LIST (single leaf child of middle group)
            if gc.len() == 1 {
                if let SchemaNode::Leaf { .. } = &gc[0] {
                    return Some(LogicalType::List);
                }
            }
        }
    }
    None
}
```

### 3.3 classify_column_plan extensions

In `crates/kessel-parquet/src/lib.rs::classify_column_plan`:

Add two new ColumnKind variants:

```rust
enum ColumnKind {
    Flat { ... },
    NestedListPrimitive { ... },
    /// SP144: canonical 3-node MAP<K, V>.
    NestedMapKV {
        key_ptype: meta::Type,
        value_ptype: meta::Type,
        key_path: Vec<String>,
        value_path: Vec<String>,
        max_def_level: u32,
        max_rep_level: u32,
        outer_optional: bool,
        value_optional: bool,  // key is always REQUIRED per spec
    },
    /// SP144: struct column — fan out into N flat ColumnPlans for the N
    /// children, then zip back into PqValue::Struct per row.
    NestedStruct {
        fields: Vec<StructField>,  // one per child leaf
        outer_optional: bool,
        max_def_at_struct_level: u32,  // for the outer-null check
    },
}

struct StructField {
    name: String,
    /// Plan to read this field's column chunk (always Flat in V1; nested
    /// fields rejected with SP145 follow-up).
    plan: Box<ColumnPlan>,
}
```

For struct: each field gets its own per-row Vec<PqValue> via the existing flat path. After all fields are read, zip:

```rust
for row in 0..num_rows {
    let struct_value = if outer_optional && def_at_struct_for_this_row == 0 {
        PqValue::Null
    } else {
        let fields: Vec<(String, PqValue)> = (0..N).map(|i|
            (fields[i].name.clone(), per_field_rows[i][row].clone())
        ).collect();
        PqValue::Struct(fields)
    };
    out.push(struct_value);
}
```

The struct's outer-OPTIONAL-null case is the tricky bit: we need to know the def_level at the struct group level. For SP144 V1 simplification: only support REQUIRED struct (outer_optional=false → always present → always wrap fields). Reject OPTIONAL struct with SP145 follow-up if too complex. ACTUALLY: pyarrow's `pa.struct(...)` produces nullable structs by default. So we DO need to handle OPTIONAL struct.

Strategy: when outer struct is OPTIONAL, read ANY one of the field columns' def-levels (they'll all be ≥1 at the outer-OPTIONAL level). For def=0 at the outer level, ALL fields read PqValue::Null at that row (the existing flat OPTIONAL path handles this). So zipping all-Null fields produces a "struct of all nulls" — which we then convert to PqValue::Null at the struct level.

Cleanest rule: post-zip, if ALL fields in a row are PqValue::Null AND outer was declared OPTIONAL, emit PqValue::Null. This is a slight aliasing risk (a non-null struct with all-Null OPTIONAL fields would also be flattened to Null) but it's the simplest V1 implementation. Alternative: read the def-level of one field at the outer-OPTIONAL level explicitly — more code but unambiguous. **BOLD V1 pick: explicit def-level read for outer_optional cases, simple zip otherwise.**

### 3.4 Map assembler

`assembly::assemble_map_kv`:

```rust
/// SP144: Dremel-style assembler for Map<K, V> columns. Input is two
/// parallel decode streams (key + value) from the two leaf columns under
/// the canonical 3-node MAP encoding's key_value REPEATED group. Both
/// leaves share IDENTICAL rep/def streams (they're under the same
/// REPEATED parent).
///
/// Algorithm: extract a single rep/def view from either column (they
/// match by spec); use SP143's assemble_list_primitive logic to identify
/// (rep=0 record-start, rep=1 continuation, def levels for null/empty)
/// but produce PqValue::Map(Vec<(K, V)>) where K is consumed from the
/// key stream and V from the value stream at every (def == max_def) slot.
pub fn assemble_map_kv(
    rep_levels: &[u32],
    def_levels: &[u32],
    keys: &[PqValue],
    values: &[PqValue],
    max_def_level: u32,
    outer_optional: bool,
    value_optional: bool,
) -> Result<Vec<PqValue>, PqError> { ... }
```

The structural shape is IDENTICAL to `assemble_list_primitive` — the difference is each "item slot" is a pair `(key, value)` instead of a single value. Both K and V cursors advance together for present-item slots.

Note: per Parquet spec, MAP keys are REQUIRED — there's no "key null" case. Only the value can be null (when value_optional). max_def_level = ancestors-with-OPT-or-REP. For OPT-REP-REQ-REQ (outer OPT, key REQ, value REQ): max_def=2. For OPT-REP-REQ-OPT (outer OPT, key REQ, value OPT): max_def=3.

Reuse assemble_list_primitive's classify(def) → DefCase enum, then specialize the "ItemPresent" arm to consume from both streams.

### 3.5 Struct "assembler"

There's no Dremel-style stream-based struct assembler in V1 — struct is a SHALLOW grouping where each child column is decoded independently and then zipped. The "assembler" is just a zip function:

```rust
pub fn assemble_struct(
    field_names: &[String],
    field_columns: &[Vec<PqValue>],  // one Vec<PqValue> per struct field
    outer_optional: bool,
    outer_null_def_levels: Option<&[u32]>,  // None if outer is REQUIRED
) -> Result<Vec<PqValue>, PqError> {
    // Validation: all field_columns must have same length = num_rows.
    // If outer_optional and outer_null_def_levels says def==0 for row i,
    // emit PqValue::Null for that row.
    // Else emit PqValue::Struct(zip of field_names + field_columns[*][i]).
}
```

### 3.6 Files

| Path | Responsibility | Touched in task |
|---|---|---|
| `crates/kessel-parquet/src/lib.rs::PqValue` | Add Map + Struct variants | T1 |
| `crates/kessel-fetch/src/lib.rs::pq_to_cell` | Add Map/Struct → JSON arms | T1 |
| `crates/kessel-parquet/src/meta.rs::LogicalType` | Add Map variant + recognition | T2 |
| `crates/kessel-parquet/src/assembly.rs::assemble_map_kv` | NEW + 6-8 KATs | T3 |
| `crates/kessel-parquet/src/assembly.rs::assemble_struct` | NEW + 4-6 KATs | T4 |
| `crates/kessel-parquet/src/lib.rs::classify_column_plan` | Add Map + struct dispatch | T5 |
| `crates/kessel-parquet/src/lib.rs::extract_nested` | Route Map/struct to new assemblers | T5 |
| `crates/kessel-parquet/src/lib.rs` (tests) | 2-3 inline hand-built roundtrips | T6 |
| `crates/kessel-parquet/tests/fixtures/map_*.parquet` + `struct_*.parquet` | 4-5 pyarrow fixtures | T7 |
| `crates/kessel-parquet/tests/fixture_roundtrip.rs` | 4-5 roundtrip tests | T7 |
| `crates/kessel-parquet/src/lib.rs::sp144_pentest` | NEW pentest module | T8 |
| `docs/STATUS.md` / `docs/USAGE.md` / `README.md` / internal record / memory | T9 |

### 3.7 Task decomposition (T0–T9)

- **T0**: Baseline (already 978/1007).
- **T1**: Extend `PqValue` with `Map(Vec<(PqValue, PqValue)>)` + `Struct(Vec<(String, PqValue)>)`. Update kessel-fetch `pq_to_cell` exhaustive match (JSON serialization). Unit tests.
- **T2**: `LogicalType::Map` variant in meta.rs + extend `recognize_logical_type` for the structural MAP pattern + converted_type 1/2 (MAP / MAP_KEY_VALUE). KAT for hand-built MAP-annotated schema.
- **T3**: `assembly::assemble_map_kv` + 6-8 Dremel KATs (REQ-REP-REQ-REQ, REQ-REP-REQ-OPT, OPT-REP-REQ-REQ, OPT-REP-REQ-OPT × empty map / null map / mixed null values / multi-record).
- **T4**: `assembly::assemble_struct` + 4-6 KATs (REQ struct, OPT struct null, OPT struct present, 2-field, 3-field).
- **T5**: Extend `classify_column_plan` to recognize Map (via `LogicalType::Map`) and struct (any non-LIST/non-MAP group with ≥2 children, all leaves). Add dispatch in `extract_nested` to call `read_chunk_values_nested_map` / `read_chunk_values_nested_struct`. Reject deep-nesting (List<struct>, struct<struct>, Map<K,struct>, etc.) with named SP145 errors.
- **T6**: 2-3 inline hand-built roundtrip tests (REQ struct, OPT struct, REQ-REP-REQ-REQ Map).
- **T7**: 4-5 pyarrow fixtures + roundtrip tests (`map_string_i64`, `optional_map_string_i64`, `struct_i64_string`, `optional_struct`, `map_string_string`).
- **T8**: Pentest matrix (~10 rows): malformed MAP (3-leaf key_value, missing key field), OPTIONAL key in MAP, struct with nested LIST child rejected as SP145, etc.
- **T9**: Docs slice — STATUS row, USAGE.md update, README.md update, internal record, memory.

### 3.8 What stays UNCHANGED

- All SP143 fixture roundtrips green untouched.
- Flat decode path byte-identical.
- All 17 SP141 pentests, 14 SP143 pentests green untouched.
- `kessel-parquet/Cargo.toml [dependencies]` stays empty.
- Binary protocol UNCHANGED.
- Default `cargo tree -p kesseldb-server` UNCHANGED.
- seed-7 GREEN.

---

## 4. Security posture (T8 pentest matrix)

| Adversarial input | Expected typed error |
|---|---|
| MAP with REPEATED middle that has 1 child (not 2) | `Unsupported("non-canonical MAP encoding (key_value children != 2): SP145")` |
| MAP with REPEATED middle that has 3 children | `Unsupported("non-canonical MAP encoding (key_value children != 2): SP145")` |
| MAP with OPTIONAL key (spec violation) | `Bad("MAP key must be REQUIRED per Parquet spec")` |
| MAP key is itself a group (struct-as-key, deep nesting) | `Unsupported("MAP<group, _>: SP145")` |
| MAP value is itself a group (List/Map/struct value) | `Unsupported("MAP<_, group>: SP145")` |
| struct with a nested LIST child | `Unsupported("struct containing nested type: SP145")` |
| struct with a nested struct child | `Unsupported("struct containing nested type: SP145")` |
| struct with a nested MAP child | `Unsupported("struct containing nested type: SP145")` |
| `assemble_map_kv` value-stream truncation | `Bad("value stream exhausted at position N")` |
| `assemble_map_kv` key-stream truncation | `Bad("key stream exhausted at position N")` |
| `assemble_struct` field-column length mismatch | `Bad("struct field 'X' length L1 != row count L2")` |

---

## 5. Test plan

Pre-T0 baseline: 978 default / 1007 featured (post-SP144H).

Expected DELTA after T9:
- T1: +3-5 unit tests (Map + Struct variant construction + JSON serialization)
- T2: +1 KAT (Map schema recognition)
- T3: +6-8 KATs (Map assembler matrix)
- T4: +4-6 KATs (struct assembler)
- T5: +1-2 unit (classify dispatch + rejection)
- T6: +3 inline roundtrips
- T7: +4-5 pyarrow fixture roundtrips
- T8: +11 pentest tests
- T9: +0 docs

Sum: ~33-40 new tests. Honest measurement in T9.

Determinism gate every task:
- `cargo test --workspace --release` FAILED=0
- seed-7 GREEN
- Default `cargo tree -p kesseldb-server` empty
- All SP140-SP144H oracles green untouched

---

## 6. Acceptance criteria

1. `PqValue::Map(Vec<(PqValue, PqValue)>)` and `PqValue::Struct(Vec<(String, PqValue)>)` variants present, kessel-fetch consumer updated.
2. `LogicalType::Map` recognized via converted_type AND structural pattern.
3. `assemble_map_kv` correctly assembles Map<K, V> per Dremel semantics, with REQUIRED key enforcement.
4. `assemble_struct` zips field columns, handles OPTIONAL outer-null correctly.
5. `extract()` returns `PqValue::Map(...)` / `PqValue::Struct(...)` for canonical Map/struct columns.
6. 4-5 pyarrow fixtures roundtrip byte-identically.
7. Pentest matrix (11 rows) passes — no panic, typed errors only.
8. SP143 + SP141 + SP140 oracles untouched.
9. Default `cargo build -p kesseldb-server` byte-identical to SP144H ship.
10. seed-7 GREEN.
11. T9 docs include honest gate reconciliation.
