# KesselDB — Subproject 102: OBJ-2b-1 RLE/bit-packing hybrid decoder

**Date:** 2026-05-19  **Status:** done — code + tests committed and passing.

Builds on:
- Subproject 97 — External sources (EXT slice 1):
  `docs/superpowers/specs/2026-05-18-external-sources-design.md`
- Subproject 98 — External sources: pagination + NDJSON:
  `docs/superpowers/specs/2026-05-18-external-sources-pagination-design.md`
- Subproject 99 — External sources: HTTPS/TLS:
  `docs/superpowers/specs/2026-05-18-external-sources-tls-design.md`
- Subproject 100 — Object-store external sources (OBJ-1):
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject100-objstore.md`
- Subproject 101 — Parquet object sources (OBJ-2a):
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject101-parquet.md`

Design document:
`docs/superpowers/specs/2026-05-19-parquet-rle-hybrid-design.md`

Plan document:
`docs/superpowers/plans/2026-05-19-parquet-rle-hybrid.md`

---

## What shipped

`crates/kessel-parquet/src/rle.rs` — a pure, bounds-checked Apache
Parquet RLE/bit-packing-hybrid decoder, KAT-pinned to the published
`parquet-format/Encodings.md` grammar (the independent authority):

- `decode_hybrid(data, bit_width, num_values) -> Result<Vec<u64>, PqError>`
  — framing-agnostic hybrid `<encoded-data>` decode (bit-packed +
  RLE runs, LSB-of-stream-first packing, bit_width 0..=64,
  over-production truncation).
- `decode_level_v1(data, bit_width, num_values) -> Result<(Vec<u64>, usize), PqError>`
  — the V1 4-byte-u32-LE-length-prefixed level-stream wrapper;
  returns levels + total bytes consumed (incl. prefix).

This is the shared primitive the next sub-slices consume. **No wiring
and no support-matrix gate changed in this slice** — dictionary,
Snappy, and OPTIONAL columns are still rejected with the exact same
typed `Unsupported` errors as OBJ-2a, until OBJ-2b-2/3/4 flip them.

---

## Verification

- KATs hand-derived from `parquet-format/Encodings.md` (canonical
  bit-packed 0..=7 width-3 example = `[0x03,0x88,0xC6,0xFA]`; RLE
  run; bit_width=0; mixed; wide-value width-17; V1 prefix framing).
- Independent-encoder round-trip (separate code path) over bit
  widths 1..=32 — non-self-referential.
- Pentest: catch_unwind lock tests prove no panic/OOM/stack-overflow
  on hostile headers (run_len≈2^63, groups≈2^63, truncated runs,
  bit_width=64 tiny slice, bit_width=65, oversized V1 prefix, empty
  slice) — typed `PqError::Bad` or exactly-num_values `Ok`.

---

## Honest gate accounting

`kessel-parquet` is an existing workspace member; its tests run under
`cargo test --workspace`. Default-build total: **293 → 310**
(+17), entirely the 17 new `rle` module tests (6 KAT + 1
decode_level_v1 KAT + 2 round-trip + 8 pentest). NOT a zero-delta
(same corrected stance as SP100/SP101). The deterministic kernel pulls
no new external dependency; `kessel-parquet/Cargo.toml` `[dependencies]`
stays empty; default `cargo tree -p kesseldb-server` links no
parquet/objstore/rustls/webpki; `large_seed_corpus_is_deterministic_and_converges`
green; existing EXT/TLS/OBJ-1 oracles (2/1/1) unchanged.

---

## Deferred (next OBJ-2b sub-slices)

- OBJ-2b-2: dictionary page + index resolution (flips dict gate).
- OBJ-2b-3: Snappy block decompression (flips Snappy gate).
- OBJ-2b-4: OPTIONAL/def-levels + nullable wiring + support-matrix
  flips + pyarrow-default fixtures + e2e.
