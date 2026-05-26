# KesselDB — Subproject 144: Parquet nested decode — Map<K,V> + struct columns

**Status:** done — code + tests committed and passing.

**Builds on:**
- `docs/superpowers/specs/2026-05-25-kesseldb-parquet-map-struct-design.md` (SP144 design spec).
- `docs/superpowers/specs/2026-05-25-kesseldb-subproject143-parquet-nested-list.md` (SP143 — schema tree + multi-bit rep/def + LIST<primitive> + Dremel assembler).

(Note: SP144 here is the **OBJ-2c-5 SP144** for Parquet Map+struct. The HTTP gateway gap-closure slice was named **SP144H** with an "H" suffix to disambiguate.)

---

## Outcome

Second slice of the 3-slice OBJ-2c-5 arc (SP143 List ✓ → **SP144 Map+struct** ✓ → SP145 deep nesting). Adds production-quality `Map<K, V>` and `struct` Parquet decode covering the canonical 3-node MAP encoding and the struct-of-primitives pattern.

Capabilities added:
- `PqValue::Map(Vec<(PqValue, PqValue)>)` + `PqValue::Struct(Vec<(String, PqValue)>)` variants (additive).
- Centralized `pqvalue_to_json` helper in kessel-parquet for List/Map/Struct serialization. `kessel-fetch::pq_to_cell` consumer updated.
- `LogicalType::Map` variant in `meta.rs` + `recognize_logical_type` extended to recognize via `converted_type=1` (MAP) or `=2` (MAP_KEY_VALUE legacy alias) AND structural pattern (REPEATED middle with 2 children, first REQUIRED).
- `assembly::assemble_map_kv` Dremel-style assembler — consumes from parallel key+value streams at every `def == max_def` slot; classify into 4 cases per outer/value optionality.
- `assembly::assemble_struct` zip helper — combines N field columns into PqValue::Struct rows; OPT outer-null detected via all-fields-Null heuristic (V1 trade-off documented).
- `classify_column_plan` extended with `NestedMapKV` + `NestedStruct` variants; MAP-key REQUIRED enforcement; deep-nesting (List<struct>, Map<K,struct>, struct<group>, etc.) rejected with typed `Unsupported(SP145)`.
- `read_chunk_values_nested_map` + `read_chunk_values_nested_struct` glue between extract_nested and the assemblers. Page-loop helper (`read_chunk_levels_and_values`) factored out so List + Map share the V1/V2/dict/codec dispatch.

End-to-end validation:
- 3 inline hand-built roundtrip tests (REQ struct, OPT struct with all-Null row, REQ-REP-REQ-REQ Map<String, i64>) — all passed FIRST TRY.
- 5 real pyarrow 24.0.0 fixtures (map_string_i64, optional_map_string_i64, map_string_string, struct_i64_string, optional_struct) — all passed FIRST TRY. pyarrow output matched our 4-shape model exactly; no quirks.
- 15-row pentest matrix — every adversarial input typed PqError, no panic / no OOM. ZERO production bugs (T3/T4/T5 were written after SP143 T10's lessons and entered T8 with clean discipline).

Binary wire byte-identical. Default `cargo build -p kesseldb-server` byte-identical to SP144H ship. `cargo tree -p kesseldb-server` (no features) empty for HTTP/gateway crates.

---

## Slice arc (9 production commits + 1 design spec)

| Commit | Task | Summary |
|---|---|---|
| `dd2397c` | spec | SP144 design spec (Map<K,V> + struct columns) |
| `a52998e` | T1 | PqValue::Map + PqValue::Struct variants + pqvalue_to_json centralized helper + kessel-fetch::pq_to_cell consumer |
| `5873e3f` | T2 | LogicalType::Map + recognize_logical_type extension (annotation + structural) |
| `4f099aa` | T3 | assemble_map_kv Dremel assembler + 8 KATs |
| `c4b6709` | T4 | assemble_struct zip helper + 7 KATs (with outer-OPT all-Null heuristic) — **1000/0/0 milestone** |
| `6024d23` | T5 | classify_column_plan + extract_nested Map/struct dispatch + page-loop helper refactor (List+Map share path) |
| `0e3612c` | T6 | 3 inline hand-built Map+struct roundtrip tests (all passed FIRST TRY) |
| `5e2f3a1` | T7 | 5 real pyarrow Map+struct fixtures + roundtrips (all passed FIRST TRY) |
| `007505b` | T8 | 15-row pentest matrix (ZERO production bugs) |

---

## Gate reconciliation (honest)

- Before (SP144H ship): 978 PASSED / 0 / 0 default; 1007 / 0 / 0 featured.
- After T9 (measured): **1023** PASSED / 0 / 0 default (+45); **1052** PASSED / 0 / 0 featured (+45).
- Per-slice delta:
  - T1 PqValue + helper: +5 unit tests
  - T2 LogicalType::Map: +2 KATs (annotation + structural)
  - T3 assemble_map_kv: +8 KATs
  - T4 assemble_struct: +7 KATs
  - T5 dispatch: +0 (wiring; updated existing rejection tests)
  - T6 inline roundtrips: +3
  - T7 pyarrow fixtures: +5
  - T8 pentest: +15
  - T9 docs: +0
  - Sum: 5+2+8+7+0+3+5+15+0 = **+45** ✓
- `cargo tree -p kesseldb-server --no-default-features | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp|kessel-http-gateway"`: empty.
- `cargo build -p kesseldb-server` (no features) byte-identical to SP144H ship.
- `kessel-vsr::large_seed_corpus_is_deterministic_and_converges`: GREEN.
- All SP140-SP144H oracles (7 Parquet pyarrow, 5 SP143 List, 2 external-source, 1 TLS, 1 objstore, 17 pentest, 8 SP141 e2e, 2 SP141 metrics e2e, 14 SP143 pentest, 3 SP143 inline-nested, plus SP144H's): green untouched.

---

## Remaining OBJ-2c-5 arc

After SP144 closes the 2nd slice, only **SP145 deep nesting** remains:
- `List<List<T>>`, `List<Map<K,V>>`, `List<struct<...>>` — cross-nested List
- `Map<K, struct<...>>`, `Map<K, List<T>>` — Map with nested values
- struct containing LIST/MAP/struct children
- All require `max_rep_level >= 2` columns with a generalized stack-based assembler

Estimated SP145 scope: comparable to SP143 + SP144 combined (~15-20 tasks). After SP145, the entire OBJ-2c-5 Parquet nested-decode arc closes and KesselDB can ingest any flat or nested analytics Parquet file pyarrow writes.

Other open Parquet items (lz4/brotli codecs, >64MiB cap) remain in their respective OBJ-2c-2 / OBJ-2c-4 follow-up tracks.

---

## Cross-links

- STATUS row: `docs/STATUS.md` (SP144 row, after SP144H).
- USAGE note: `docs/USAGE.md` §Parquet support matrix.
- README capability matrix bullet.
- ARCHITECTURE: `docs/ARCHITECTURE.md` §kessel-parquet.
- Design spec: `docs/superpowers/specs/2026-05-25-kesseldb-parquet-map-struct-design.md`.
- Memory: `memory/project_kesseldb.md` (SP144 block) + `MEMORY.md` (KesselDB line).
