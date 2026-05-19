# OBJ-2b-2 — Parquet dictionary encoding: Design

**Date:** 2026-05-19
**Status:** Approved (autonomous mandate — see Process Note)
**Subproject:** 103 (second sub-slice of the OBJ-2b arc)
**Builds on:** subproject 97/98/99/100/101/102

## Process Note (autonomy)

Produced under the standing overnight autonomous-build mandate
(`feedback_kesseldb_autonomous_build`): the user directed "keep going"
and is intermittently away. The brainstorming skill's user-review gate
is satisfied by this documented decision record. All other rigor is
retained: two-stage subagent review per task (spec then code-quality),
a final whole-implementation review, full
`cargo test --workspace --release` + seed-7 + a pentest pass each
kernel-adjacent task. Genuine design alternatives are recorded with the
chosen option and the reason.

## Problem

SP102 shipped the pure `kessel-parquet::rle` hybrid decoder
(`decode_hybrid`/`decode_level_v1`) but flipped **no** support-matrix
gate. OBJ-2a still rejects dictionary-encoded Parquet — which is
pyarrow's **default** (`use_dictionary=True`) — in two places with
typed `Unsupported`:

- the `ColumnMetaData` encodings-list gate (only `Plain`/`Rle`
  allowed);
- the data-page gates `ph.page_type != 0` and `ph.dp_encoding != 0`.

OBJ-2b-2 makes `extract()` decode dictionary-encoded **flat REQUIRED,
UNCOMPRESSED, V1** columns by consuming the SP102 `decode_hybrid`
primitive, and flips exactly those gates. Compression (OBJ-2b-3),
OPTIONAL/definition-levels (OBJ-2b-4), and OBJ-2c features stay
rejected with their existing typed errors.

## Parquet dictionary layout (authority: `parquet-format`)

A dictionary-encoded column chunk is:

```
[DICTIONARY_PAGE]  PageHeader{ type=DICTIONARY_PAGE(2),
                               DictionaryPageHeader{1:num_values,
                                                    2:encoding,
                                                    3:is_sorted} }
                   payload = PLAIN-encoded `num_values` values of the
                             column's physical type.
[DATA_PAGE]*       PageHeader{ type=DATA_PAGE(0),
                               DataPageHeader{1:num_values,2:encoding} }
                   payload (flat REQUIRED, no rep/def levels) =
                     <1 byte: bit_width>
                     <RLE/bit-packing hybrid stream of `num_values`
                      dictionary indices, NOT length-prefixed>
```

- `ColumnMetaData.dictionary_page_offset` is **field id 11** (i64),
  currently *not* decoded by `meta.rs`. It points at the dictionary
  page header.
- `PageHeader.dictionary_page_header` is **field id 7**;
  `DictionaryPageHeader` is `{1:i32 num_values, 2:Encoding encoding,
  3:bool is_sorted}`.
- `PageType`: `DATA_PAGE=0`, `INDEX_PAGE=1`, `DICTIONARY_PAGE=2`,
  `DATA_PAGE_V2=3`.
- `Encoding`: `PLAIN=0`, `PLAIN_DICTIONARY=2`, `RLE=3`,
  `RLE_DICTIONARY=8`.
- The data-page index stream's bit width is the **first payload
  byte** (read it; do **not** compute `ceil(log2(dict_len))`). The
  remaining payload is exactly the framing-agnostic hybrid stream
  SP102's `decode_hybrid` consumes.

## Architecture

Decode-only change inside `kessel-parquet`. No `kessel-fetch` /
`kessel-sql` / server / kernel change. New cohesive module
`crates/kessel-parquet/src/dict.rs` owns dictionary-index resolution;
`meta.rs` learns the two new metadata fields and the dictionary page
header; `lib.rs::extract()`'s per-chunk read is refactored into a
page-loop helper.

### Components / data flow

1. **`meta.rs`**
   - `Encoding`: add `PlainDictionary` (2) and `RleDictionary` (8).
   - `ColumnChunk`: add `dictionary_page_offset: Option<i64>` (decode
     `ColumnMetaData` field 11; `None` if absent).
   - `PageHeader`: add `dict_num_values: i32`, `dict_encoding: i32`
     (default −1). `decode_page_header` decodes field 7
     (`DictionaryPageHeader`) like it already does field 5
     (`DataPageHeader`), with the same per-struct `last_id`
     save/reset/restore bracketing (the SP101 Fix-4 discipline).
2. **`dict.rs`** — pure, zero-dep:
   ```rust
   /// Resolve a dictionary-encoded data-page payload to values.
   /// `payload` is the WHOLE data-page payload (its first byte is the
   /// bit width; the rest is the non-length-prefixed hybrid stream).
   /// `dict` is the already-PLAIN-decoded dictionary. `n` is the
   /// data page's num_values. Every index is bounds-checked against
   /// `dict.len()` → typed Bad on OOB. Never panics/OOMs.
   pub fn resolve_dict_indices(
       payload: &[u8],
       dict: &[PqValue],
       n: usize,
   ) -> Result<Vec<PqValue>, PqError>;
   ```
   Reads `payload[0]` as `bit_width` (Bad if payload empty), calls
   `rle::decode_hybrid(&payload[1..], bit_width as u32, n)`, then maps
   each `idx` → `dict.get(usize::try_from(idx)?)` (OOB → Bad), cloning
   the `PqValue`.
3. **`lib.rs::extract()`** — the per-row-group, per-wanted-column body
   currently reads exactly one page at `data_page_offset`. Refactor
   that into:
   ```rust
   fn read_chunk_values(file: &[u8], cc: &ColumnChunk, ptype: Type)
       -> Result<Vec<PqValue>, PqError>
   ```
   which:
   - keeps the SP101 strict guards: `cc.codec == Uncompressed`
     (else Unsupported "compression: OBJ-2b-3"), schema/chunk ptype
     match (Fix-1), encodings-list gate now allows
     `Plain|Rle|PlainDictionary|RleDictionary` (else Unsupported);
   - if any data-page will be dictionary-encoded, require
     `cc.dictionary_page_offset` present (else **Bad**
     "dictionary-encoded column without dictionary_page_offset");
     read the page header there, require `page_type ==
     DICTIONARY_PAGE`, require its `dict_encoding ∈ {PLAIN(0),
     PLAIN_DICTIONARY(2)}` (else Unsupported), PLAIN-decode
     `dict_num_values` values via `plain::decode_plain` → `dict`;
   - then iterate **data pages** from the first data-page offset,
     accumulating values until the accumulated count equals
     `cc.num_values`; per data page dispatch on `dp_encoding`:
     `PLAIN(0)` → `plain::decode_plain` (pyarrow dictionary-fallback
     pages are legal and nearly free to support);
     `PLAIN_DICTIONARY(2)`/`RLE_DICTIONARY(8)` →
     `dict::resolve_dict_indices`; anything else → Unsupported;
     `page_type` must be `DATA_PAGE(0)` (V2 still Unsupported).
   - bounds: each page advance is `header_len + uncompressed_size`
     via `checked_add`, slice via `get(..)`; a page count that would
     exceed `cc.num_values` or never reach it → Bad.

### Bold, documented decisions (options → choice)

- **Support multiple DATA_PAGEs per chunk + PLAIN
  dictionary-fallback pages in THIS slice.** Option A: single
  data-page only (toy). Option B (chosen): a page loop until
  `num_values` rows, dispatching each page by its own encoding
  (PLAIN or dict). Reason: this is pyarrow's *actual* output for any
  non-tiny column (it splits at `data_page_size` and falls back to
  PLAIN when the dictionary grows past `dictionary_pagesize_limit`).
  The marginal cost is one bounded loop reusing `decode_plain` /
  `resolve_dict_indices` — it makes the reader work on real files
  rather than only hand-tuned tiny ones, directly serving the
  "mind-blowing, not toy" mandate. Still strictly bounded (no
  compression, no levels, V1 only).
- **Require an explicit `dictionary_page_offset` for dict columns.**
  Rejected the "dict page conventionally precedes `data_page_offset`"
  heuristic — it is not guaranteed and invites a misparse. If a data
  page is dict-encoded and `dictionary_page_offset` is absent →
  typed **Bad** (malformed), not a guess.
- **Bit width is read from the payload's first byte**, never
  computed. `bit_width == 0` is **valid** (all indices 0 → every row
  is `dict[0]`; in-bounds whenever `dict` is non-empty) and decodes
  correctly — it is *not* an error case. `decode_hybrid` already caps
  `bit_width ≤ 64`; the `idx < dict.len()` check is the real safety
  net, so no extra bit-width upper gate is needed.
- **`dict.rs` is framing-aware about the 1 bit-width byte; `rle.rs`
  stays framing-agnostic.** The leading byte is part of the Parquet
  *data-page* layout, not the hybrid grammar — keeping it out of
  `rle.rs` preserves that module's clean boundary (also used by the
  future level path).
- **Dictionary page `encoding` accepted set = {PLAIN(0),
  PLAIN_DICTIONARY(2)}.** pyarrow historically tags the dict page
  `PLAIN_DICTIONARY`; the values are PLAIN-laid-out in both cases.
  Anything else (e.g. a dict page that is itself dict-encoded) →
  Unsupported/Bad, pentested.

## Determinism / source-format independence

Dictionary encoding only deduplicates; the resolved `PqValue`
sequence is bit-identical to what the PLAIN path yields for the same
logical data. `pq_to_cell` → `coerce::to_field_bytes` is unchanged, so
existing `extract()` callers get byte-identical `FieldKind` bytes
regardless of PLAIN vs dict. Pinned by a test that encodes the same
logical column both ways and asserts `extract()` returns an identical
`Vec<Vec<PqValue>>`. `dict.rs`/`meta.rs`/`extract()` are pure functions
of the file bytes — deterministic by construction; no clock/env/IO.

## Security posture (pentest, extends SP101 Task-12 / SP102)

Parquet bytes are operator-source-controlled. Lock tests
(`catch_unwind`, typed `Err`, no panic/OOM/stack-overflow):

- lying `DictionaryPageHeader.num_values` (huge) vs a tiny dict page →
  `decode_plain`'s existing `count.min(data.len())` bound holds → Bad;
- out-of-range dictionary index (`idx ≥ dict.len()`) → Bad (not a
  panic / wrong value);
- `dictionary_page_offset` past EOF / overlapping the footer → Bad;
- dict-encoded data page with **no** `dictionary_page_offset` → Bad;
- empty data-page payload (no bit-width byte) → Bad;
- huge `bit_width` byte (e.g. 200) → `decode_hybrid` rejects
  (`> 64` → Bad) or the index bound rejects → Bad;
- dictionary page that is itself `RLE_DICTIONARY` → Unsupported/Bad;
- index hybrid stream truncated mid-run → Bad (from `decode_hybrid`);
- multi-page accumulated count `< or > cc.num_values` → Bad;
- `bit_width == 0` with a multi-entry dict → **decodes correctly to
  all `dict[0]`** (asserted Ok, not an error — this is a correctness
  lock, proving we don't over-reject valid Parquet).

## Testing

TDD per task. Layers:

1. **Spec KATs** (hand-derived from `parquet-format`, independent
   authority; reviewer re-derives): a one-byte-bit-width + hybrid
   index stream resolving against a known PLAIN dictionary
   (e.g. dict = `["a","b","c"]` BYTE_ARRAY, indices via an RLE run
   and a bit-packed run, bit_width derived by hand) → expected
   `PqValue`s; `meta.rs` field-11 / field-7 decode KAT with
   hand-built compact-thrift bytes.
2. **Real-pyarrow fixtures** (the realism win): regenerate the SP101
   fixtures with `use_dictionary=True, compression=None` (pyarrow
   default dictionary; still uncompressed until OBJ-2b-3). If pyarrow
   is unavailable the implementer reports **BLOCKED**, never
   hand-fakes a dict file only the reader can parse (SP101 T7
   stance). pyarrow 24.0.0 is present in this environment.
3. **Determinism pin**: same logical column PLAIN vs dict → identical
   `extract()` output.
4. **e2e**: mirror the SP101 `external_source_parquet_oracle` /
   SP100 objstore-oracle harness — fail-closed, no router
   fixture-trust bypass; the trusted decode happy-path proven at the
   `kessel-fetch` layer over the dict fixture.
5. **Pentest** lock tests as enumerated above.

## Intended behavior change (call out — NOT a regression)

OBJ-2a shipped two tests asserting dictionary is rejected:
`extract_rejects_dict_columnmeta_encoding` and
`extract_rejects_dict_data_page_encoding` (in `lib.rs`). This slice
**intentionally** makes dictionary supported, so those two tests are
**updated**: the column-meta one is repurposed to assert a *still*-
unsupported encoding (e.g. `DELTA_BINARY_PACKED=5`) is rejected; the
data-page one becomes a **positive** decode assertion (dict data page
decodes to the expected values). The docs/record task states this
explicitly as a deliberate, reviewed behavior change — not a silently
weakened test. All other OBJ-2a gate tests (compression, OPTIONAL,
schema/chunk mismatch, missing column, V2) stay green unchanged.

## Determinism / invariants gate (every task)

- `cargo test --workspace --release` `FAILED=0`;
  `large_seed_corpus_is_deterministic_and_converges` green.
- Honest gate accounting: `kessel-parquet` is an existing workspace
  member; new `dict`/`meta`/`extract` tests **raise the default-build
  total honestly** (no false zero-delta; SP100/101/102 stance). The
  two repurposed dict-rejection tests are a net-neutral count change
  plus new positive/pentest tests — the docs task reconciles the
  measured before→after (baseline 310) with the real reason and the
  intended-behavior-change note.
- Kernel pulls no new external dependency;
  `kessel-parquet/Cargo.toml` `[dependencies]` stays **empty**;
  default `cargo tree -p kesseldb-server` links no
  parquet/objstore/rustls/webpki.
- `#![forbid(unsafe_code)]`; no unwrap/expect/panic/raw-index on
  input bytes (checked `get`/`checked_*`; the only allowed
  `try_into().unwrap()` is the 4-byte→`[u8;4]` infallible pattern).
- Existing oracles green unchanged: `external_source_oracle` (2),
  `external_source_tls_oracle` (1), `external_source_objstore_oracle`
  (1); SP101 OBJ-2a behavior unchanged except the two intentionally
  updated dict tests.

## Out of scope (deferred)

- Snappy / any compression → **OBJ-2b-3**.
- OPTIONAL/REPEATED, definition/repetition levels, nullable coerce →
  **OBJ-2b-4**.
- gzip/zstd, INT96/DECIMAL, nested, V2 data pages → **OBJ-2c**.

The schema `repetition == Required` gate and the `codec ==
Uncompressed` gate are **unchanged** — OPTIONAL and compressed dict
files still get their existing typed `Unsupported` until their owning
sub-slice.

## Task decomposition (one coherent slice)

- **T0** determinism baseline (record 310).
- **T1** `meta.rs`: `Encoding::PlainDictionary/RleDictionary`;
  `ColumnChunk.dictionary_page_offset` (field 11);
  `PageHeader.dict_num_values/dict_encoding` + field-7
  `DictionaryPageHeader` decode (per-struct `last_id` bracketing).
  Hand-built compact-thrift KATs.
- **T2** `dict.rs`: `resolve_dict_indices` (bit-width byte +
  `rle::decode_hybrid` + bounds-checked dict lookup). Spec KAT
  (hand-derived index stream + PLAIN dict).
- **T3** `lib.rs`: refactor per-chunk read into `read_chunk_values`
  (dict-page load + multi-data-page loop + per-page PLAIN/dict
  dispatch); flip the encodings-list + page gates; **update** the two
  OBJ-2a dict-rejection tests (documented intended change) + add the
  PLAIN-vs-dict determinism pin.
- **T4** real-pyarrow `use_dictionary=True, compression=None`
  fixtures (regen command + expected rows in the fixtures README) +
  e2e via the SP101 oracle harness (fail-closed).
- **T5** pentest pass (the enumerated hostile vectors).
- **T6** docs: `docs/superpowers/specs/2026-05-19-kesseldb-subproject103-dict.md`
  record + STATUS/USAGE honest lines + gate reconciliation + the
  intended-behavior-change call-out + auto-memory (SP103 block +
  MEMORY.md line).

## Internal record

`docs/superpowers/specs/2026-05-19-kesseldb-subproject103-dict.md` at
docs time, mirroring the SP100/101/102 record convention (KesselDB H1
prefix, Status line, linked Builds-on, `---` separators, honest gate
reconciliation, intended-behavior-change note).
