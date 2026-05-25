# KesselDB — Subproject 143: Parquet nested decode — schema tree + multi-bit rep/def levels + `List<primitive>`

**Status:** design — approved by autonomous mandate substitution; implementation plan to follow. First slice of the 3-slice OBJ-2c-5 arc (SP143 List → SP144 Map+struct → SP145 deep nesting).

**Builds on:**
- `crates/kessel-parquet/` (the shipped Parquet decoder: 7 pyarrow e2e oracles, flat REQUIRED|OPTIONAL × UNCOMPRESSED|Snappy|Gzip|Zstd × PLAIN|dict × V1+V2 pages).
- `crates/kessel-parquet/src/meta.rs::FileMetaData.flat_schema` (the existing nested-schema rejection gate).
- `crates/kessel-parquet/src/rle.rs::decode_hybrid` (already supports arbitrary bit widths — reused for multi-bit rep/def level streams).

**Process note.** Per `feedback_kesseldb_autonomous_build`, the brainstorming user-review gate is substituted by this written spec. The OBJ-2c-5 scope decision (full nested LIST+Map+struct) was confirmed by the user; the 3-slice decomposition (SP143/SP144/SP145) is the BOLD call documented here. The two-stage spec-then-quality subagent review gate during implementation is retained.

---

## 1. Problem

The shipped Parquet decoder rejects nested schemas (`flat_schema == false` → `Unsupported("nested schema: OBJ-2c")` at `lib.rs:633-637`) and REPEATED columns (`leaf.repetition == REPEATED` → `Unsupported("REPEATED columns: OBJ-2c")` at `lib.rs:657-661`). V2 page decode rejects any non-zero `repetition_levels_byte_length` (`lib.rs:209`).

Real analytics Parquet files almost always contain nested types: `List<i64>` for arrays of values, `Map<String, double>` for typed dictionaries, struct columns for sub-records. Today KesselDB cannot ingest any of them.

OBJ-2c-5 in full generality (LIST + Map + struct + arbitrary nesting depth via Dremel record assembly) is a multi-week effort. SP143 is the first slice: it ships the foundational infrastructure (schema tree, multi-bit rep/def level decode, single-level `List<primitive>`), which is ~80% of the real-world data-engineering use case. SP144 and SP145 build on this foundation.

---

## 2. Goals and non-goals

**Goals (SP143 V1).**

- Add a `SchemaTree` (recursive `SchemaNode`) in `FileMetaData` alongside the existing flat `leaves: Vec<LeafInfo>`. The flat list stays unchanged so the existing decode path is byte-identical.
- For each leaf column, compute `max_rep_level` and `max_def_level` from the schema tree (currently both are implicitly 0 or 1).
- Extend `decode_page_v1` and `decode_data_page_v2` to decode multi-bit rep/def level streams via the existing `rle::decode_hybrid(bit_width)`. The bit width is `ceil(log2(max_X_level + 1))`.
- Recognize the canonical Parquet **LIST<primitive>** 3-node group pattern:
  ```
  optional|required group <name> (LIST) {
    repeated group list {
      optional|required <PRIMITIVE_TYPE> element;
    }
  }
  ```
  And produce `PqValue::List(Vec<PqValue>)` for each output row's value at that column.
- Extend `PqValue` enum (`crates/kessel-codec/src/lib.rs` or wherever `PqValue` lives — verify) with `List(Vec<PqValue>)` variant (additive; existing variants byte-untouched).
- Reuse `plain::decode_plain` and `dict::resolve_dict_indices` for the leaf-value decode (no new primitive decoding needed).
- Write the Dremel-style assembly algorithm in a new module `crates/kessel-parquet/src/assembly.rs` — generic enough to be extended in SP144 (Map+struct) and SP145 (deep nesting) without rewriting.
- 4-5 real pyarrow fixtures: `list_i64_required`, `list_i64_optional`, `list_string`, `optional_list_i64`, `list_with_null_items`. Each is roundtrip-tested through `extract()`.
- Pentest matrix for rep/def-level adversarial inputs: bit-width overflow, level value > max, mismatched rep/def stream lengths, OOM via lying num_values, empty rep stream with non-zero rep_levels_byte_length, etc.
- Honest docs reconciliation in T-final: STATUS row + USAGE §Parquet update + internal record.

**Non-goals (named, deferred to SP144/SP145).**

- `Map<K,V>` columns (3-node MAP encoding). Defer to SP144.
- struct columns (multiple primitive children grouped under a non-LIST/non-MAP group). Defer to SP144.
- `List<List<T>>` and other deep nesting (max_rep_level ≥ 2). Defer to SP145.
- `List<struct<...>>` and `List<Map<...>>` cross-product. Defer to SP145.
- Logical-type annotations beyond LIST detection (Decimal, UTF8, etc. — already handled by primitive type detection from SP108).
- Anything that requires a new compression codec, encoding (DELTA/BYTE_STREAM_SPLIT), or page format. Stays in OBJ-2c-2/2c-4 follow-ups.

---

## 3. Architecture

### 3.1 Schema tree

Currently `FileMetaData` has:
- `leaves: Vec<LeafInfo>` — flat list of leaf-column metadata
- `flat_schema: bool` — true iff schema is `root(group, num_children=N)` followed by N leaves only

The flat representation is sufficient for REQUIRED|OPTIONAL columns at any depth (the gate rejects intermediate groups), but for LIST/struct/Map we need the parent-child relationships. Add an additive field:

```rust
pub struct FileMetaData {
    pub version: i32,
    pub num_rows: i64,
    pub leaves: Vec<LeafInfo>,
    pub row_groups: Vec<RowGroup>,
    pub flat_schema: bool,
    /// SP143: full schema tree, populated alongside `leaves`. Used by the
    /// nested-decode path. For flat files (`flat_schema == true`), this
    /// is unused — the existing flat path takes precedence.
    pub schema_tree: SchemaTree,
}

pub struct SchemaTree {
    pub root: SchemaNode,
}

pub enum SchemaNode {
    Group {
        name: String,
        repetition: Repetition,  // REQUIRED, OPTIONAL, REPEATED
        children: Vec<SchemaNode>,
        /// Parquet ConvertedType / LogicalType annotation (e.g. LIST, MAP).
        /// V1 SP143: only `Some(LogicalType::List)` matters for the
        /// LIST<primitive> recognition; SP144 adds Map.
        logical_type: Option<LogicalType>,
    },
    Leaf {
        name: String,
        repetition: Repetition,
        physical_type: PhysicalType,  // already exists as `LeafInfo.ptype`
        max_def_level: u32,           // computed from path
        max_rep_level: u32,           // computed from path
        path: Vec<String>,            // dotted-path for diagnostics + col matching
    },
}

pub enum LogicalType {
    List,
    // SP144: Map, SP145: more
}
```

`meta::decode_filemetadata` is extended to build the tree alongside the flat leaves (already walks the SchemaElement list in DFS order — same walk produces both). The `max_def_level` and `max_rep_level` on each `Leaf` are computed during the walk by accumulating `+1` per OPTIONAL ancestor (for def) and per REPEATED ancestor (for rep).

### 3.2 Decode-path dispatch

In `lib.rs::extract`:

```rust
if md.flat_schema {
    // Existing path (byte-identical) — handles REQUIRED|OPTIONAL flat columns.
    extract_flat(file, &md, wanted)
} else {
    // NEW path — handles LIST<primitive> via schema_tree + assembly.
    extract_nested(file, &md, wanted)
}
```

The gate flip removes the `Unsupported("nested schema: OBJ-2c")` rejection at line 637. `extract_nested` is a new function that walks the schema tree and dispatches per-leaf.

`extract_nested` first validates that every "wanted" column path is recognized:
- Either a flat REQUIRED|OPTIONAL column (handled via the existing per-leaf decode, returning `PqValue::I64(_)` etc.)
- Or a LIST<primitive> column (handled via the new assembly path, returning `PqValue::List(Vec<PqValue>)`)
- Or **rejected with typed error** (`Unsupported("Map columns: OBJ-2c-5b/SP144")`, `Unsupported("struct columns: OBJ-2c-5b/SP144")`, `Unsupported("deep nesting: OBJ-2c-5c/SP145")`).

The rejection messages explicitly name which future slice will lift them, so the user sees a roadmap.

### 3.3 Multi-bit rep/def level decode

Today `decode_page_v1` reads def-levels via `rle::decode_level_v1(payload, 1, n)` (bit width hardcoded to 1, length-prefixed). For nested:
- bit_width = `ceil(log2(max_def_level + 1))` for def — e.g. `List<Optional<i64>>` has max_def_level = 3 → bit_width = 2
- Similarly for rep

V1 page layout: `[4-byte u32 LE rep_len][rep_data][4-byte u32 LE def_len][def_data][values]`. (Or the SP102 RLE-hybrid form; reuse the existing decode_level_v1 with the wider bit width.)

V2 page layout: `[rep_data: rep_levels_byte_length bytes RAW RLE-hybrid][def_data: def_levels_byte_length bytes RAW RLE-hybrid][values]`. The rep/def byte lengths are in the V2 page header (already decoded in `meta::PageHeader.v2_rep_levels_byte_length` and `v2_def_levels_byte_length`). Today the V2 decode rejects `rep_len > 0` (lib.rs:209) — flip that to actually decode.

Refactor `rle::decode_level_v1(payload, bit_width, n)` if needed to accept arbitrary bit_width (likely it already does — verify).

### 3.4 The Dremel assembly algorithm

For each LIST<primitive> column with max_rep_level=1 and max_def_level∈{2,3,4} (depending on optional/required at the LIST and ITEM levels):

Input: parallel streams of `(rep_levels: Vec<u32>, def_levels: Vec<u32>, values: Vec<PqValue>)` for that leaf. Each entry's value is only present when `def == max_def` (the deepest-defined case).

Output: `Vec<PqValue::List(Vec<PqValue>)>` — one entry per output row.

Algorithm (forward streaming, single pass):

```
let mut rows = Vec::with_capacity(num_rows);
let mut current_list: Option<Vec<PqValue>> = None;
let mut value_iter = values.into_iter();

for (rep, def) in rep_levels.iter().zip(def_levels.iter()) {
    if rep == 0 {
        // New record. Flush previous.
        if let Some(list) = current_list.take() {
            rows.push(PqValue::List(list));
        }
        // Start the new record.
        if def == 0 {
            // List itself is null (only possible if outer is OPTIONAL).
            current_list = None;  // will become PqValue::Null below
            rows.push(PqValue::Null);
            continue;
        } else if def == 1 {
            // Empty list (outer is non-null but no items).
            current_list = Some(Vec::new());
        } else {
            // First item of new list. Value present iff def == max_def.
            let mut new_list = Vec::new();
            if def == max_def {
                new_list.push(value_iter.next().expect("value present"));
            } else {
                new_list.push(PqValue::Null);  // item is null (def == max_def - 1)
            }
            current_list = Some(new_list);
        }
    } else {
        // rep == 1: continuing current list.
        let list = current_list.as_mut().expect("rep==1 without active list");
        if def == max_def {
            list.push(value_iter.next().expect("value present"));
        } else {
            list.push(PqValue::Null);
        }
    }
}

// Flush trailing list.
if let Some(list) = current_list.take() {
    rows.push(PqValue::List(list));
}
```

(The actual implementation uses bounds-checked iteration with typed `PqError` returns, not `expect()`. The pseudocode shows the structural shape.)

The algorithm is `O(num_levels + num_values)`. No backtracking. No buffering beyond `current_list`.

### 3.5 Files

| Path | Responsibility | Touched in task |
|---|---|---|
| `crates/kessel-codec/src/lib.rs` (or where PqValue lives) | Add `List(Vec<PqValue>)` variant | T2 |
| `crates/kessel-parquet/src/meta.rs` | Add `SchemaTree`, `SchemaNode`, `LogicalType` types; build the tree during footer decode; compute max_def/max_rep per leaf | T3 |
| `crates/kessel-parquet/src/assembly.rs` | NEW — `pub fn assemble_list_primitive(rep_levels, def_levels, values, max_rep, max_def) -> Vec<PqValue>` + Dremel unit tests | T5 |
| `crates/kessel-parquet/src/lib.rs::extract` | Dispatch flat vs nested; `extract_nested` implementation | T7, T8 |
| `crates/kessel-parquet/src/lib.rs::decode_page_v1` and `decode_data_page_v2` | Multi-bit rep/def level decode | T4 |
| `crates/kessel-parquet/tests/fixtures/list_i64_required.parquet` etc. | 5 pyarrow fixtures | T9 |
| `crates/kessel-parquet/tests/list_roundtrip.rs` | 5 e2e roundtrip tests through extract() | T9 |
| `crates/kessel-parquet/src/lib.rs::pentest` tests | Pentest matrix for adversarial rep/def inputs | T10 |
| `docs/STATUS.md` / `docs/USAGE.md` / `README.md` / internal record / memory | Docs slice | T11 |

### 3.6 Task decomposition (T0..T11)

- **T0**: Baseline (record measured workspace tests + featured + seed-7 + tree-grep).
- **T1**: Refactor `rle::decode_level_v1` to support arbitrary bit_width if it doesn't already; add KAT for bit_width=2 (4-level rep/def).
- **T2**: Extend `PqValue` with `List(Vec<PqValue>)` variant. Update any pattern-matches that exhaust the enum (compiler-driven). Add unit tests for List in `kessel-codec`.
- **T3**: Add `SchemaTree`, `SchemaNode`, `LogicalType` types in `meta.rs`. Extend `decode_filemetadata` to build the tree alongside the flat leaves. Compute `max_def_level` and `max_rep_level` per leaf during the walk. Add KAT for a hand-built nested-schema thrift blob proving the tree + level computations are correct.
- **T4**: Extend V1+V2 page decode to read multi-bit rep/def level streams via `rle::decode_hybrid` with the correct bit_width derived from the leaf's max_X_level. Add unit tests for multi-bit decode on hand-built page bytes.
- **T5**: Write `assembly.rs::assemble_list_primitive` per §3.4 algorithm + 6-8 hand-built Dremel KATs covering: required-list-of-required-items, required-list-of-optional-items, optional-list-of-required-items, optional-list-of-optional-items, empty list, all-null list, mixed-null items, multiple records.
- **T6**: Extend `extract` to dispatch flat vs nested via the `flat_schema` flag. `extract_nested` recognizes LIST<primitive> pattern + dispatches each leaf through the assembly path. Reject Map/struct/deep-nesting with typed errors naming SP144/SP145.
- **T7**: Build a hand-built nested-schema Parquet file inline (in tests) and prove `extract()` returns `[PqValue::List([...]), ...]` correctly.
- **T8**: Update the existing `extract_rejects_nested_schema_obj2c` test to assert the SPECIFIC rejections that remain (Map, struct, deep-nesting) — the LIST rejection is now lifted.
- **T9**: Generate 5 pyarrow fixtures + 5 roundtrip tests. Each fixture is a real `pyarrow.parquet.write_table` with a List<T> column; the roundtrip asserts `extract()` returns the exact expected `Vec<PqValue::List>` values.
- **T10**: Pentest matrix — adversarial rep/def inputs (per spec §4 below).
- **T11**: Docs + STATUS row + USAGE §Parquet nested update + internal record + memory.

### 3.7 What stays UNCHANGED

- The existing flat decode path (REQUIRED|OPTIONAL × UNCOMPRESSED|Snappy|Gzip|Zstd × PLAIN|dict × V1+V2): byte-identical. The dispatch in T6 routes `flat_schema == true` files to the existing `extract_flat` (renamed if needed but logically unchanged).
- All 7 existing pyarrow e2e oracles: green untouched.
- `kessel-parquet/Cargo.toml [dependencies]`: still empty (zero external deps).
- `#![forbid(unsafe_code)]`: honored.
- Binary protocol bytes UNCHANGED.
- Default `cargo tree -p kesseldb-server` UNCHANGED.
- seed-7 GREEN.

---

## 4. Security posture (T10 pentest matrix)

Each row asserts typed `PqError` (no panic, no OOM, no infinite loop), under `catch_unwind`:

| Adversarial input | Expected typed error |
|---|---|
| `max_def_level=2` but def stream contains value `3` | `PqError::Bad("def level > max_def_level")` |
| `max_rep_level=1` but rep stream contains value `2` | `PqError::Bad("rep level > max_rep_level")` |
| `bit_width=33` for a level stream | `PqError::Bad("bit_width too large")` |
| rep_levels_byte_length > total page payload | `PqError::Bad("rep section overrun")` |
| def_levels_byte_length + rep_levels_byte_length > uncompressed_page_size | `PqError::Bad("level sections overrun")` |
| Levels say "value present" but values vec is shorter than required | `PqError::Bad("value stream truncated")` |
| Values vec longer than levels imply | Accepts (extra values are unused) OR `PqError::Bad` — design pick; spec lock: accept extras silently (matches the existing flat decoder's tolerance for over-provisioned pages) |
| num_values mismatch between levels and values | `PqError::Bad("level/value count mismatch")` |
| Deeply nested rep_level value (e.g. 5 on a 1-level LIST) | `PqError::Bad("rep level > max_rep_level")` |
| Empty rep stream but non-zero `rep_levels_byte_length` | `PqError::Bad("rep section length lies")` |
| Schema tree says LIST but leaf path doesn't match the 3-node pattern | `PqError::Unsupported("non-canonical LIST encoding: SP145")` |
| LIST element type is itself a LIST (deep nesting) | `PqError::Unsupported("List<List<…>>: OBJ-2c-5c/SP145")` |
| LIST element type is a group (struct) | `PqError::Unsupported("List<struct<…>>: OBJ-2c-5b/SP144")` |
| Cycle in schema (impossible in valid Parquet but adversarial test) | `PqError::Bad("schema cycle")` (depth-limit recursion to 64) |

All pentests via `catch_unwind` + `well_behaved` pattern (existing in `kessel-parquet/src/lib.rs::pentest`).

---

## 5. Test plan

Pre-T0 baseline: 932 default / 959 featured (post-SP142).

Expected DELTA after T11:
- T1: +1 KAT (bit_width=2 decode_level_v1)
- T2: +1-3 unit tests (PqValue::List variant)
- T3: +1 KAT (schema tree + level computation)
- T4: +1-2 KAT (multi-bit V1+V2 page decode)
- T5: +6-8 KAT (Dremel assembly)
- T6: +1-2 unit (dispatch + Map/struct/deep-nesting rejection)
- T7: +1 (inline hand-built file roundtrip)
- T8: +0 (modifies existing tests)
- T9: +5 (pyarrow roundtrip + e2e oracle)
- T10: +13 (pentest matrix from §4)
- T11: +0 (docs)

Sum: ~32-37 new tests. Honest reconciliation in T11 — measure the actual count, no rounding.

Determinism gate every task:
- `cargo test --workspace --release` FAILED=0
- seed-7 GREEN
- Default tree-grep EMPTY
- All 7 existing Parquet pyarrow e2e oracles still pass
- All 38 KATs + 8 e2e + 17 pentest + 2 metrics_e2e + 2 metrics_writer unit + applied_ops test + 1 SP143 = baseline + delta

---

## 6. Acceptance criteria

1. `FileMetaData.schema_tree: SchemaTree` populated for every Parquet file (flat or nested).
2. Per-leaf `max_def_level` and `max_rep_level` computed correctly per Parquet spec.
3. V1+V2 page decode handles multi-bit rep/def level streams via `rle::decode_hybrid`.
4. `assembly::assemble_list_primitive` correctly reconstructs `Vec<PqValue::List>` from rep/def/value triples per the Dremel algorithm.
5. `extract()` returns `PqValue::List(Vec<PqValue>)` for LIST<primitive> columns.
6. 5 pyarrow fixtures roundtrip byte-identically through `extract()`.
7. Pentest matrix: 13 rows pass (no panic/OOM under `catch_unwind`).
8. All 7 existing Parquet pyarrow e2e oracles still green.
9. Map/struct/deep-nesting rejections name SP144/SP145 in error messages.
10. Default `cargo build -p kesseldb-server` byte-identical to SP142 ship.
11. seed-7 GREEN.
12. Docs reconciliation honest in T11 internal record.
