# SP154 — Zero-dep Brotli (RFC 7932) Decoder SP-arc — Progress Tracker

Date: 2026-05-26
Status: **IN PROGRESS — Layers 1-5 of ~12 shipped**

## Mission

Implement a pure-Rust zero-dependency Brotli (Parquet codec id 4)
decoder per RFC 7932, mirroring the SP125-SP140 zstd arc. Pre-SP154,
codec id 4 was recognized at meta-decode time (SP150) but
`page_payload`'s Brotli arm always returned typed Unsupported pointing
at this follow-up arc. Post-SP154 the boundary is refined: streams
composed of only uncompressed metablocks decode successfully; streams
needing compressed metablocks (= the pyarrow shape) still reject, but
with a more specific SP154-followup pointer.

## Layers — Honest Scope

| Layer | Description | Status | Commits |
|-------|-------------|--------|---------|
| L1 | LSB-first bit reader | DONE | `fa7a030` |
| L2 | Stream header (WBITS) | DONE | `fa7a030` |
| L3 | Metablock framing (ISLAST / MNIBBLES / MLEN / ISUNCOMPRESSED + skip-region) | DONE | `fa7a030` |
| L4 | Uncompressed metablock body | DONE | `fa7a030` |
| L5 | Huffman trees — SIMPLE prefix codes (§3.4) + canonical reconstruction (§3.3) | DONE | `4753fad` |
| L5b | Huffman trees — COMPLEX prefix codes (§3.5) | DEFERRED | — |
| L6 | Block-type / block-length codes | DEFERRED | — |
| L7 | Distance code parameters (NPOSTFIX, NDIRECT) | DEFERRED | — |
| L8 | Context modes (CMODE / IMTF transform) | DEFERRED | — |
| L9 | Insert-and-copy commands | DEFERRED | — |
| L10 | Static dictionary (~122 KB constants + transforms) | DEFERRED | — |
| L11 | Compressed metablock orchestration loop | DEFERRED | — |
| L12 | Ring buffer with wraparound | DEFERRED | — |

## Code Locations

- `crates/kessel-parquet/src/brotli_bit_reader.rs` — Layer 1 (LSB-first bit reader, 14 KATs)
- `crates/kessel-parquet/src/brotli.rs` — Layers 2-4 (stream header, metablock framing, uncompressed body, dispatch loop) + 18 KATs
- `crates/kessel-parquet/src/brotli_huffman.rs` — Layer 5 (simple prefix code + canonical code construction) + 10 KATs
- `crates/kessel-parquet/src/lib.rs` — 5 wire sites: page_payload arm + 2 V2 data-page arms + 2 pre-flight gates

## What Works Right Now

- A Brotli stream that contains ONLY uncompressed metablocks (rare but
  valid per RFC 7932 §9.2) decodes to the original bytes
- A Brotli stream with skip-region metablocks (MNIBBLES=0) is skipped
  correctly (does not block the loop)
- A simple prefix code (NSYM ≤ 4 symbols, RFC §3.4) can be decoded
  in isolation via `brotli_huffman::decode_simple_prefix_code`
- Canonical prefix codes (RFC §3.3) reconstruct correctly from
  `(symbol, length)` pairs (including the zero-bit NSYM=1 case)
- Bomb defense: `BROTLI_MAX_DECOMP = 256 MiB` cap matches SP151
  zstd/gzip/lz4/snappy caps
- All errors typed (`BrotliError` + `HuffmanError` + `BitReaderError`);
  no panics on attacker bytes; `#![forbid(unsafe_code)]` honored

## What Doesn't Yet Work

- Any pyarrow-emitted Brotli file (pyarrow always emits compressed
  metablocks via insert-and-copy commands over Huffman-coded literals)
  → surfaces typed `Unsupported("Brotli compressed metablock: SP154-followup. Workaround — zstd/lz4")`
- Complex prefix codes (RFC §3.5) — needed before ANY compressed
  metablock can be decoded
- Static-dictionary back-references — required for the most common
  compression patterns (e.g. shared strings in JSON-like Parquet
  columns)

## Remaining Layer Estimates

Each layer is a self-contained commit. Per-layer task counts assume
the SP125-SP140 zstd-arc cadence (~1 layer per session-slice):

- **L5b complex prefix codes** — ~2 sessions. Needs:
  - HSKIP (4 bits) leading skip-count
  - 18-entry code-length code table (= a small prefix code over
    `0..=17` whose own lengths are read directly from the bit stream)
  - RLE-coded run of alphabet code lengths (symbols 0..=15 are
    direct lengths; symbol 16 is a 2-bit-extended repeat; symbol 17
    is a 3-bit-extended zero-repeat)
  - Pin via KATs hand-derived from the RFC §3.5 worked example
- **L6 block-type/block-length codes** — ~1 session. Per RFC §6,
  there are 3 block-type structures (literal, insert-copy, distance);
  each has NBLTYPES + a block-type prefix code + a block-length
  prefix code. For V1 we can reject NBLTYPES > 1 with a typed
  Unsupported and only support the single-block case.
- **L7 distance code parameters** — ~0.5 session. NPOSTFIX (0..=3)
  and NDIRECT (0..=15) shape the distance alphabet. V1 can reject
  non-default values.
- **L8 context modes** — ~1 session. CMODE selects one of LSB6 /
  MSB6 / UTF8 / Signed for literal context; for V1 we can support
  only CMODE=0 (a single context) and reject the others.
- **L9 insert-and-copy commands** — ~2 sessions. The insert-copy
  alphabet is 704 symbols; each command produces (insert_length,
  copy_length, distance_code). Substantial table-driven work.
- **L10 static dictionary** — ~2 sessions. ~122 KB of constants
  (Appendix A) plus 121 word-length-buckets indexing into them,
  plus 121 transforms (Appendix B). The transforms alone are a
  ~500-line table.
- **L11 compressed metablock orchestration** — ~1 session. Ties
  the above layers together into the actual decode loop.
- **L12 ring buffer** — ~0.5 session. The output buffer wraps at
  `1 << WBITS`; back-references can reach across the wraparound.

**Total remaining estimate: ~7-10 sessions** (Layer 5b + 6 + 7 + 8 +
9 + 10 + 11 + 12 + buffer for KAT-derivation surprises). The full
Brotli decoder is genuinely a multi-week sub-project, matching the
SP125-SP140 zstd arc length.

## Test Counts

- Pre-SP154: workspace 1138/0/1 default, 1171/0/1 featured
- Post-SP154 L1-L4: workspace 1170/0/1, 1203/0/1
- Post-SP154 L5: workspace 1180/0/1, 1213/0/1 (current)

All seed-7 GREEN; tree-grep EMPTY across all commits.

## RFC 7932 Ambiguities / Surprises Encountered

1. **MNIBBLES encoding is a FIXED-LENGTH non-monotonic code, not a
   straight LSB-first integer.** RFC 7932 §9.2 says: '00' → 4 nibbles,
   '01' → 5, '10' → 6, '11' → 0 (skip-region). My first implementation
   used the obvious 0/1/2/3 → 0/4/5/6 mapping and failed against the
   pyarrow fixture with a misleading `NonLastMetablockMustHaveLength`
   error. Surfaced by re-checking against the actual RFC table.

2. **Skip-region metablocks (MNIBBLES=0) are independent of ISLAST.**
   A stream can have an ISLAST=1 skip metablock followed by EOF; or a
   mid-stream skip metablock that doesn't terminate the loop.

3. **NSYM=3 simple prefix code lengths are IN ORDER OF APPEARANCE.**
   RFC 7932 §3.4 says "code lengths for the symbols are 1, 2, 2 in
   the order they appear in the representation of the simple prefix
   code", so the FIRST symbol declared gets length 1, NOT the
   sorted-smallest one. Canonical assignment within each length
   then orders by symbol value.

4. **ISUNCOMPRESSED is only present when !ISLAST.** An ISLAST
   metablock is ALWAYS compressed. So a hand-crafted Brotli stream
   that contains a single uncompressed metablock MUST have that
   metablock be non-last + ISUNCOMPRESSED=1, followed by a final
   ISLAST+ISLASTEMPTY=1 marker.

## Open Questions for Future Implementers

1. **Static dictionary**: should the ~122 KB of Brotli Appendix A
   constants live in `brotli_dict_data.rs` as a single `&[u8]`
   include, or split per-word-length-bucket? The zstd FSE tables
   precedent (in `zstd_fse.rs`) suggests a single flat array with
   index lookups.

2. **Insert-copy alphabet**: should the 704-entry insert-and-copy
   command alphabet be table-driven (matching the zstd
   `zstd_sequences.rs` precedent) or computed from the §5 formulas
   on the fly? The latter is easier to verify against the RFC.

3. **Test fixture strategy**: pyarrow can't emit uncompressed-only
   metablocks; the existing brotli_flat.parquet fixture covers only
   the compressed-rejection path. Once Layer 5b lands, we should
   add a hand-crafted single-compressed-metablock fixture (small
   alphabet, simple prefix code, no commands) as the FIRST positive
   compressed-path KAT. This can be derived by hand from the RFC
   without needing a Brotli encoder. Once Layer 9-11 land, we can
   add a real pyarrow fixture as the integration test.

4. **Wire-up timing**: the current page_payload arm calls
   `brotli::decompress` and surfaces compressed-metablock errors as
   typed Unsupported. Layer 5b doesn't change this — the metablock
   body decoder still rejects compressed metablocks at the
   `if !mb.is_uncompressed` check in `brotli::decompress_inner`.
   The wire-up flips at Layer 11 (compressed metablock orchestration)
   when the body decoder can actually consume the compressed payload.

## Sources

- [RFC 7932 - Brotli Compressed Data Format](https://datatracker.ietf.org/doc/html/rfc7932)
- Google's reference brotli decoder: https://github.com/google/brotli (consulted for §9.2 grammar ambiguity)
- SP125-SP140 zstd arc precedent: `crates/kessel-parquet/src/zstd*.rs`, `docs/STATUS.md` rows
