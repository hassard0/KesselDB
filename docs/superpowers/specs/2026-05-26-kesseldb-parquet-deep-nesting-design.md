# KesselDB — Subproject 145: Parquet deep nesting (3rd OBJ-2c-5 slice)

**Status:** design — approved by autonomous mandate substitution. Final slice of the 3-slice OBJ-2c-5 arc (SP143 List ✓ → SP144 Map+struct ✓ → **SP145 deep nesting**).

**Builds on:**
- `docs/superpowers/specs/2026-05-25-kesseldb-subproject143-parquet-nested-list.md` (SP143 — schema tree + multi-bit rep/def + LIST<primitive> + Dremel assembler).
- `docs/superpowers/specs/2026-05-25-kesseldb-subproject144-parquet-map-struct.md` (SP144 — Map<K,V> + struct of primitives).

**Process note.** Per `feedback_kesseldb_autonomous_build`, brainstorm gate substituted. User explicitly requested "do the remaining obj-2c-5 item". This closes the entire OBJ-2c-5 arc.

---

## 1. Problem

SP143 ships `List<primitive>`. SP144 ships `Map<K, V>` and `struct<primitives>`. **Everything else is rejected** with named SP145 errors:

| Today's rejection | What SP145 lifts |
|---|---|
| `Unsupported("List<group> (List of struct or List<List<...>>): SP145")` | `List<List<T>>`, `List<struct<...>>`, `List<Map<...>>` |
| `Unsupported("MAP<group, _>: SP145")` | `Map<struct<...>, V>`, `Map<List<...>, V>` (rare in practice) |
| `Unsupported("MAP<_, group>: SP145")` | `Map<K, struct<...>>`, `Map<K, List<...>>`, `Map<K, Map<...>>` |
| `Unsupported("struct containing nested group: SP145")` | `struct<List<T>>`, `struct<Map<K,V>>`, `struct<struct<...>>` |

Common shape: every rejection involves a nested LIST/MAP/struct **inside** another LIST/MAP/struct. The defining technical property: **`max_rep_level >= 2`** (multiple REPEATED ancestors in the path).

After SP145 closes, KesselDB can ingest any nested Parquet pyarrow writes.

## 2. Goals and non-goals

**Goals (V1).**

- Generalize the SP143 List assembler from "single-level rep_level=1" to **arbitrary `max_rep_level`** via a stack-based incremental builder.
- Same for the SP144 Map assembler (3-node MAP can appear inside a List/struct, e.g. `List<Map<String, i64>>`).
- Allow struct fields to be themselves nested types (List/Map/struct) — recursively assemble inner values, then zip per row.
- Lift the 4 specific SP145 rejections in `classify_column_plan`:
  - `List<group>` — recurse into the inner group's own classification (could be List, Map, or struct)
  - `Map<_, group>` — same
  - `Map<group, _>` — accept group keys IF the group is a struct of primitives (rare; defer further if it's a List/Map key — that's pathological)
  - `struct<group>` — recurse into each non-leaf child
- Schema-tree-walk continues to compute correct `max_rep_level` and `max_def_level` per leaf (already correct from SP143 T3 — REPEATED contributes +1 to both).
- Per-leaf decode unchanged: `decode_page_v1_nested` + `decode_data_page_v2_nested` already handle arbitrary `max_rep_level` via `rle::decode_hybrid` with `bit_width = ceil(log2(max_rep_level + 1))`.
- Assembly is where the work is.

**Non-goals (named, deferred to future slices).**

- Parquet logical types beyond LIST and MAP (e.g. interval, JSON, BSON). Those are physical-layer concerns handled at extract time; SP145 doesn't touch them.
- New compression codecs (lz4, brotli) — separate OBJ-2c-2 slice.
- `> 64 MiB` page payload cap — separate OBJ-2c-4 slice.
- `INT96` / `DECIMAL` refinements — already handled in SP108.

## 3. Architecture

### 3.1 The generalized assembler

SP143's `assemble_list_primitive` and SP144's `assemble_map_kv` are special-case 1-level assemblers. They classify each (rep, def) tuple into a small set of cases ({OuterNull, EmptyList, ItemNull, ItemPresent}) based on a 1-deep nesting structure.

For arbitrary depth, the **structural model** is a tree of "build stages", each corresponding to a schema-tree node from outermost LIST/MAP/struct down to the deepest primitive leaf. Each stage has its own "active accumulator" (a Vec<PqValue> for List, Vec<(K,V)> for Map, Vec<(name,PqValue)> for struct).

The Dremel algorithm walks the (rep, def, value) triples. For each tuple:
- The `rep` level tells us **which stage to "reset to"** (rep=0 → outermost record, rep=k → stage at depth k).
- The `def` level tells us **how deep the value is defined** (def=0 → outermost is null, def=max_def → fully-present leaf).

The classic Dremel paper describes this as a state machine. For SP145's V1, we implement it as a **recursive descent assembler** keyed off the schema tree:

```
assemble_recursive(schema_node, leaf_streams, cursors) -> PqValue
```

But this gets complicated when a single column-chunk's stream needs to feed multiple stages. A cleaner **V1 BOLD design**: per-leaf-stream Dremel reconstruction (not per-record). Each leaf produces a Vec<PqValue> of length `num_records` (top-level rows) where each entry is the fully-nested PqValue at that leaf's column position. Then `extract_nested` combines them per top-level row.

Wait — that doesn't quite work either, because the "nested PqValue at that leaf's column position" depends on the schema tree structure between root and leaf (which List/Map/struct nodes are along the way).

**The honest design**: a generalized Dremel assembler that takes a schema sub-tree + parallel decoded streams from each leaf within that sub-tree, and produces `Vec<PqValue>` (one per top-level record) where each PqValue follows the schema sub-tree's structure.

### 3.2 The stack-based assembler (pseudo-code)

```rust
/// SP145 generalized Dremel assembler. Input: the schema sub-tree for one
/// "wanted" top-level column, plus the parallel leaf streams (rep, def,
/// values) for every leaf reachable from this sub-tree's root.
///
/// Output: Vec<PqValue> with length = number of top-level records (= count
/// of rep_level=0 entries in any leaf stream).
pub fn assemble_nested(
    node: &SchemaNode,
    leaf_streams: &HashMap<LeafPath, (Vec<u32>, Vec<u32>, Vec<PqValue>)>,
) -> Result<Vec<PqValue>, PqError> {
    // Algorithm sketch:
    // 1. Determine the leaves under this node and their paths.
    // 2. Track cursors per leaf (index into rep/def/value vecs).
    // 3. Iterate by record (advance all leaf cursors past rep=0 positions in lockstep).
    // 4. For each record, recursively build the PqValue tree by walking the schema
    //    sub-tree top-down, consuming from leaf cursors as needed.
}
```

Concretely, for each top-level record:
- Walk the schema tree depth-first.
- At each Group node, determine if it's a LIST/MAP/struct/primitive-pass-through, and use the corresponding accumulator.
- At each Leaf, consume one (rep, def, value) tuple from that leaf's cursor; emit PqValue at the correct depth based on the def level.

This is **non-trivial** but well-defined. The Dremel paper has the canonical algorithm in §4 (record assembly automaton).

### 3.3 Simplification: Per-shape composition (BOLD V1)

The full Dremel automaton is overkill if we restrict to the **structural patterns Parquet actually produces**:

| Outer shape | Inner can be | Implementation |
|---|---|---|
| `List<X>` | List, Map, struct, primitive | Generalize `assemble_list_primitive`: instead of consuming a primitive value at max_def, RECURSE into inner-shape assembler for the sub-record. |
| `Map<K, X>` | struct, List, Map, primitive | Generalize `assemble_map_kv`: V value at max_def becomes recursive call. |
| `struct<fields>` | each field can be any shape | `assemble_struct`: each field column already produces its own assembled Vec<PqValue>; struct zip is unchanged. |

**The trick:** the inner shape's assembly happens on a **subset** of the parallel streams — only the leaves under that inner sub-tree, with rep/def levels OFFSET to be "relative to the inner sub-tree's root".

For example, in `List<struct<id: i64, name: string>>`:
- The "outer List" sees rep=0 (new record) and rep=1 (new item in list).
- The "inner struct" doesn't care about rep at all (struct has no REPEATED inner) — for each item slot the outer List allocates, the struct's `id` and `name` columns each emit one value.

So `assemble_list_of_struct` becomes:
```rust
// For each rep=0 (new record):
//   Identify how many "items" this record contains (count rep=1's that follow before next rep=0)
//   For each item:
//     Build a struct by zipping the relevant slice of each field's column
```

But this only works if all leaves under the outer List have IDENTICAL rep-level streams (they share the same REPEATED ancestor). That's true by Parquet construction.

**V1 implementation plan:** add 4 generalized assemblers, each handling one outer shape with arbitrary inner:

1. `assemble_list_nested(outer_rep, outer_def, inner_assembler_fn) -> Vec<PqValue>`
2. `assemble_map_nested(outer_rep, outer_def, key_stream, value_assembler_fn) -> Vec<PqValue>`
3. `assemble_struct_nested(field_columns) -> Vec<PqValue>` (same as SP144, unchanged)
4. Plus a top-level dispatcher that picks the right outer assembler and recurses for inner shapes

This avoids the full Dremel automaton complexity while supporting all common nested types.

### 3.4 The 4 specific cases SP145 must support

Per the SP145 rejections to lift:

1. **`List<List<T>>`** (`max_rep_level=2`) — list of lists of primitive. Pyarrow shape:
   ```
   outer (LIST) -> repeated list_outer -> element (LIST) -> repeated list_inner -> element primitive
   ```
   Implementation: outer rep=0 starts new record; rep=1 starts new inner-list within the same outer; rep=2 continues current inner-list.

2. **`List<struct<...>>`** — list of structs. Pyarrow shape:
   ```
   outer (LIST) -> repeated list -> element (group with N field children)
   ```
   Implementation: outer rep handles list boundaries; for each list item, zip the N field columns at the right slice.

3. **`Map<K, struct<...>>`** — map of struct values. Pyarrow shape:
   ```
   outer (MAP) -> repeated key_value -> [key, value (group with N field children)]
   ```
   Implementation: outer rep handles map boundaries; for each pair, key is a primitive, value is a struct zipped from N field columns.

4. **`struct<List<T>>` and `struct<struct<...>>`** — struct containing a List or struct field.
   Implementation: assemble_struct receives recursively-built field columns (so the inner List/struct is already a Vec<PqValue::List>/etc.).

### 3.5 V1 simplification — start with these 4 shapes

Don't try to support all combinatorial cross-products in T1. Build the 4 specific shapes above, then T-late tasks extend to:
- `Map<K, List<T>>`
- `List<Map<K,V>>`
- `Map<K, Map<K2,V2>>`
- struct containing Map

These can be added as new dispatch arms; the generalized assemblers from T3-T5 should already handle them by composition.

### 3.6 Files

| Path | Change | Task |
|---|---|---|
| `crates/kessel-parquet/src/assembly.rs` | NEW `assemble_list_of_struct`, `assemble_list_of_list_primitive`, `assemble_map_of_struct`, `assemble_struct_with_nested` | T2-T5 |
| `crates/kessel-parquet/src/lib.rs::classify_column_plan` | Lift the 4 SP145 rejections — recursive classification | T6 |
| `crates/kessel-parquet/src/lib.rs::extract_nested` | Route new shapes to right assembler | T6 |
| `crates/kessel-parquet/src/lib.rs` (tests) | Hand-built inline roundtrips for the 4 shapes | T7 |
| `crates/kessel-parquet/tests/fixtures/list_of_*.parquet` etc. | Pyarrow fixtures + tests | T8 |
| `crates/kessel-parquet/src/lib.rs` (pentest) | New rejection paths now lifted; pentest the recursive validation | T9 |
| `docs/STATUS.md` / `docs/USAGE.md` / internal record / memory | T10 docs | T10 |

## 4. Task decomposition (T0–T10)

- **T0**: Baseline.
- **T1**: Add `List<List<primitive>>` recursive support. Generalize `assemble_list_primitive` to call a leaf-or-recursive callback at max_def. Add KATs.
- **T2**: Add `List<struct<primitives>>` support — list of structs. Build hand-built nested-streams test.
- **T3**: Add `Map<K, struct<primitives>>` support — map with struct values.
- **T4**: Add `struct<List<primitive>>` and `struct<struct<primitives>>` support — struct containing nested types.
- **T5**: classify_column_plan recursive validation — lift the 4 SP145 rejections; reject pathological cases (List<Map<group,_>>, struct<REPEATED leaf without parent group>, etc.) with `Unsupported("non-canonical: ...")`.
- **T6**: Inline hand-built roundtrips for each of the 4 shapes (T1-T4 assemblers x integration with classify_column_plan).
- **T7**: Pyarrow fixtures for the 4 shapes + 1-2 more common cross-product shapes:
  - `list_of_list_i64.parquet`
  - `list_of_struct.parquet`
  - `map_string_struct.parquet`
  - `struct_with_list_field.parquet`
  - (BOLD: include `map_string_list_string.parquet` if pyarrow makes it easy)
- **T8**: Pentest matrix (~15 rows) for the new deep-nesting paths.
- **T9**: Cross-product matrix completion (Map<K, List>, List<Map>, struct<Map>) — additive on top of T1-T5 generalized assemblers.
- **T10**: Docs, STATUS row, USAGE update, internal record, memory, OBJ-2c-5 arc CLOSED marker.

## 5. Acceptance criteria

1. `assemble_list_*` generalizes to `max_rep_level >= 2` for nested LIST.
2. `assemble_map_*` accepts struct/List/Map values via recursive composition.
3. `assemble_struct_with_nested` accepts List/struct fields.
4. classify_column_plan lifts the 4 SP145 rejections; recursive validation handles further nesting.
5. 5-6 pyarrow fixtures roundtrip successfully (all 4 shapes + extras).
6. Pentest matrix (15+ rows) green.
7. OBJ-2c-5 arc CLOSED — STATUS marker.
8. Workspace baseline + ~40-50 new tests; default tree-grep EMPTY; binary protocol UNCHANGED.

## 6. Honest estimate

SP145 is the largest single Parquet slice (~15-20 tasks if done granularly; ~10 if combined like SP147). Real-world data engineers most commonly use `List<struct>` and `Map<K, struct>` — these are the priority. `List<List>` and other rarely-seen cross-products are still BOLD V1 included.

**The full Dremel automaton would be more general but considerably more complex code.** Per the V1 simplification in §3.3, we ship a per-shape compositional design that handles every common case. If a future pyarrow file exercises a shape the compositional design can't handle, that becomes a documented "SP146-or-later" follow-up.
