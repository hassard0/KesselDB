# KesselDB — Subproject 103: OBJ-2b-2 Parquet dictionary encoding

**Date:** 2026-05-19  **Status:** done — code + tests committed and passing.

Builds on:
- Subproject 99 — External Sources TLS:
  `docs/superpowers/specs/2026-05-18-external-sources-tls-design.md`
- Subproject 100 — Object-Store sources (OBJ-1):
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject100-objstore.md`
- Subproject 101 — Parquet OBJ-2a:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject101-parquet.md`
- Subproject 102 — RLE/bit-packing hybrid (OBJ-2b-1):
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject102-rle.md`

Design document:
`docs/superpowers/specs/2026-05-19-parquet-dictionary-design.md`

Plan document:
`docs/superpowers/plans/2026-05-19-parquet-dictionary.md`

---

## What shipped

`kessel-parquet::extract()` now decodes **dictionary-encoded flat
REQUIRED, UNCOMPRESSED, V1** columns (pyarrow default
`use_dictionary=True`):

- `meta.rs`: `Encoding::PlainDictionary(2)`/`RleDictionary(8)`;
  `ColumnChunk.dictionary_page_offset` (CMD field 11);
  `PageHeader.dict_num_values/dict_encoding` + field-7
  `DictionaryPageHeader` decode (per-struct `last_id` bracketing).
- `dict.rs` (new, pure, zero-dep): `resolve_dict_indices` — reads
  the data-page bit-width byte, decodes indices via SP102
  `rle::decode_hybrid`, resolves against the PLAIN-decoded
  dictionary with bounds-checked lookups.
- `extract()`: per-chunk read refactored into `read_chunk_values`
  — optional leading DICTIONARY_PAGE then a multi-DATA_PAGE loop;
  each data page dispatched PLAIN (dictionary-fallback) or
  PLAIN_DICTIONARY/RLE_DICTIONARY.

Still rejected with typed errors: compression (OBJ-2b-3),
OPTIONAL/levels (OBJ-2b-4), DELTA/BYTE_STREAM_SPLIT/INT96/V2 (OBJ-2c).

---

## Verification

- Spec KATs hand-derived from parquet-format (dict index hybrid
  stream `[0x02,0x03,0x58,0x00]`→`[a,c,b,b]`; bit_width=0→all
  `dict[0]`; field-11/field-7 thrift KATs).
- Real pyarrow 24.0.0 `use_dictionary=True, compression=None`
  fixture (`dict_flat.parquet`, REQUIRED via nullable=False) —
  metadata-verified PLAIN_DICTIONARY both columns — round-trips;
  e2e via the SP101 oracle harness (fail-closed, no router
  fixture-trust bypass).
- Determinism pin: same logical values are byte-identical
  `PqValue` whether PLAIN- or dictionary-encoded
  (`pq_to_cell`/coerce unchanged).
- Pentest: catch_unwind locks (empty payload, OOB index, huge
  bit-width, truncated stream, lying n, dict-page-offset past EOF
  via a valid-footer file, truncated-file) → typed errors, no
  panic/OOM; plus a positive lock proving `bit_width==0` valid
  input is NOT over-rejected.

---

## Intended behavior change (reviewed — NOT a regression)

OBJ-2a's `extract_rejects_dict_columnmeta_encoding` and
`extract_rejects_dict_data_page_encoding` asserted dictionary is
rejected. This slice intentionally supports dictionary, so those two
tests were replaced: `extract_rejects_delta_encoding` (asserts
DELTA_BINARY_PACKED still Unsupported) and
`extract_decodes_dictionary_int64` (positive decode). All other
OBJ-2a gate tests (snappy/optional/schema-mismatch/missing-column/
golden) remain unchanged and green. Deliberate, reviewed change,
not a silent test weakening.

---

## Honest gate accounting

`kessel-parquet` is an existing workspace member; its new tests run
under `cargo test --workspace`. Default-build total: **310 → 326**
(+16) — new meta/dict/extract/fixture/pentest tests minus the 2
intentionally-removed dict-reject tests. NOT a zero-delta (same
corrected stance as SP100/101/102). Kernel pulls no new external
dependency; `kessel-parquet/Cargo.toml` `[dependencies]` empty;
default `cargo tree -p kesseldb-server` links no
parquet/objstore/rustls/webpki;
`large_seed_corpus_is_deterministic_and_converges` green; existing
EXT/TLS/OBJ-1 oracles (2/1/1) unchanged.

---

## Deferred (next OBJ-2b / OBJ-2c)

- OBJ-2b-3: Snappy block decompression (flips the Snappy gate).
- OBJ-2b-4: OPTIONAL/def-levels + nullable wiring + e2e.
- OBJ-2c: gzip/zstd, INT96/DECIMAL, REPEATED/nested, V2 pages.
