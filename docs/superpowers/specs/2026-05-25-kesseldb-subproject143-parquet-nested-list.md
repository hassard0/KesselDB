# KesselDB — Subproject 143: Parquet nested decode — schema tree + multi-bit rep/def levels + `List<primitive>`

**Status:** done — code + tests committed and passing.

**Builds on:**
- `docs/superpowers/specs/2026-05-25-kesseldb-parquet-nested-list-design.md` (SP143 design spec).
- Shipped Parquet decoder (`crates/kessel-parquet`): 7 pyarrow e2e oracles, flat REQUIRED|OPTIONAL × UNCOMPRESSED|Snappy|Gzip|Zstd × PLAIN|dict × V1+V2 pages.
- Shipped flat-schema gate (`FileMetaData.flat_schema`) — preserved untouched for backward compatibility.
- Shipped `rle::decode_hybrid` arbitrary-bit-width support — reused for multi-bit rep/def streams.

---

## Outcome

First slice of the 3-slice OBJ-2c-5 arc (SP143 List → SP144 Map+struct → SP145 deep nesting). Adds production-quality `List<primitive>` decode covering ~80% of real-world analytics Parquet data.

Capabilities added:
- `PqValue::List(Vec<PqValue>)` variant + JSON serialization at the `kessel-fetch::pq_to_cell` consumer boundary (preserves binary `Cell` enum byte-untouched)
- `SchemaTree` + `LogicalType::List` in `meta.rs` (additive on `FileMetaData`; flat path unchanged)
- Per-leaf `max_def_level` and `max_rep_level` computation via DFS walk (REQUIRED adds 0/0; OPTIONAL adds 1/0; REPEATED adds 1/1)
- `decode_page_v1_nested` + `decode_data_page_v2_nested` sibling helpers for multi-bit rep/def level streams (level value range-validated against schema-computed max)
- `assembly::assemble_list_primitive` Dremel-style record assembler with standard Parquet def-level semantics (no look-ahead heuristic)
- `extract()` dispatches to `extract_nested` for non-flat schemas; canonical 3-node `LIST<primitive>` recognition via both `LogicalType::List` annotation and structural pattern; Map/struct/deep-nesting rejected with typed `Unsupported` errors naming SP144 / SP145

End-to-end validation:
- 3 inline hand-built roundtrip tests (REQ-REP-REQ, REQ-REP-OPT, OPT-REP-REQ) — all passed FIRST TRY
- 5 real pyarrow 24.0.0 fixtures (list_i64_required, list_i64_optional, list_string, optional_list_i64, list_with_null_items) — all passed FIRST TRY
- 14-row pentest matrix — every adversarial input typed `PqError`, no panic / no OOM

Binary wire byte-identical. Default `cargo build -p kesseldb-server` byte-identical to SP142 ship. Default `cargo tree -p kesseldb-server --no-default-features` empty for HTTP/gateway/external crates.

---

## Slice arc (10 production commits + 1 design spec)

| Commit | Task | Summary |
|---|---|---|
| `a36a371` | spec | SP143 design spec (3-slice arc decomposition) |
| `8fdd2c9` | T1 | rle::decode_level_v1 multi-bit (bit_width=2) KAT — no production change |
| `54bc8a7` | T2 | PqValue::List variant + kessel-fetch JSON mapping (Cell::Text preserves binary protocol) |
| `c42aa6f` | T3 | SchemaTree + LogicalType + max_def/max_rep computation |
| `26e1625` | T4 | decode_page_v1_nested + decode_data_page_v2_nested sibling helpers |
| `0654fa5` | T5 | Dremel assembler (initial — used look-ahead heuristic, would have failed on pyarrow data) |
| `d4f3209` | T5 fix | Rewrote assembler to use STANDARD Parquet def-level semantics (no look-ahead) |
| `b15bc7d` | T6 + T8 | extract() dispatches flat vs nested + LIST<primitive> recognition + T8 inline-folded |
| `217ecc8` | T7 | 3 inline hand-built roundtrip tests (all passed first try) |
| `563f672` | T9 | 5 real pyarrow fixtures + roundtrip tests (all passed first try) |
| `10a4e05` | T10 | 14-row pentest matrix — caught and fixed 2 real CVEs (see below) |

---

## Production bugs caught by pentest matrix

1. **`rle::decode_hybrid` OOM vector.** `Vec::with_capacity(num_values)` blindly trusted the caller-supplied `num_values`. A direct call with `num_values = 1_000_000_000` requested 8 GB and would OOM-abort the process. Fixed by capping the initial reservation at 64 K elements; the bit-packed/RLE run loop is already bounded by input data length via existing `data.get(..)?` checks.
2. **`assembly::assemble_list_primitive` silent value-discard.** The `n == 0` short-circuit returned `Ok(vec![])` without validating `values.is_empty()`. Hostile input with empty level streams but a non-empty values vec would be silently accepted. Fix: validate values are empty in the n==0 fast path; otherwise return `PqError::Bad`.

Both fixes are in the T10 commit (`10a4e05`).

---

## Gate reconciliation (honest)

- Before (T0 baseline post-SP142): 932 PASSED / 0 / 0 default; 959 / 0 / 0 featured.
- After T11 (measured): **976** PASSED / 0 / 0 default; **1003** PASSED / 0 / 0 featured. **+44** in both modes.
- Per-slice delta:
  - T1: +1 KAT
  - T2: +8 unit tests (PqValue::List variant + JSON helper round-trips)
  - T3: +1 KAT (schema tree + level computation)
  - T4: +2 KATs (V1 + V2 nested page decode)
  - T5 (+ fix): +10 KATs (4-shape matrix + empty + multi-record + 3 error-path locks + bonus error case)
  - T6 (+ T8 inline): +0 (wiring-only; test adjustments to existing rejection tests)
  - T7: +3 inline roundtrip tests
  - T9: +5 pyarrow fixture roundtrips
  - T10: +14 pentest tests
  - T11 (this task): +0 (docs)
  - Sum: 1+8+1+2+10+0+3+5+14+0 = **+44** ✓
- `cargo tree -p kesseldb-server --no-default-features | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp|kessel-http-gateway"`: empty.
- `cargo build -p kesseldb-server` (no features) byte-identical to SP142 ship.
- `kessel-vsr::large_seed_corpus_is_deterministic_and_converges`: GREEN.
- All 7 existing Parquet pyarrow e2e oracles + 2 external-source + 1 TLS + 1 objstore + all SP141 HTTP gateway tests + all SP142 hardening tests: green untouched.

---

## Remaining OBJ-2c-5 arc (next slices)

- **SP144**: `Map<K, V>` columns (3-node MAP encoding) + struct columns (multiple primitive children grouped under a non-LIST/non-MAP group). Builds on SP143's `assemble_list_primitive` pattern; needs new assemblers `assemble_map_kv` and `assemble_struct`.
- **SP145**: arbitrarily-deep nesting (`List<List<T>>`, `List<Map<K, V>>`, `List<struct<…>>`, `Map<K, struct<…>>`, etc.) with multi-level rep/def streams (max_rep_level ≥ 2). Generalizes the assembler stack.

After SP143, the typical analytics Parquet file with `List<primitive>` columns works end-to-end through KesselDB. Map and struct columns surface clear "SP144 follow-up" errors instead of silent failure.

---

## Cross-links

- STATUS row: `docs/STATUS.md` (SP143 row, after SP142).
- USAGE note: `docs/USAGE.md` §Parquet support matrix.
- README capability matrix bullet.
- ARCHITECTURE: `docs/ARCHITECTURE.md` §kessel-parquet.
- Design spec: `docs/superpowers/specs/2026-05-25-kesseldb-parquet-nested-list-design.md`.
- Memory: `memory/project_kesseldb.md` (SP143 block) + `MEMORY.md` (KesselDB line).
