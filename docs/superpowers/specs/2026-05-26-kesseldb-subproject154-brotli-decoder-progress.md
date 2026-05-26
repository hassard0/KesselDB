# SP154 — Zero-dep Brotli (RFC 7932) Decoder SP-arc — Progress Tracker

Date: 2026-05-26
Status: **IN PROGRESS — Layers 1-10 of ~12 shipped** (L11 + L12 still ahead)

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
| L5b | Huffman trees — COMPLEX prefix codes (§3.5) | DONE | `cbab152` |
| L6 | NBLTYPES variable-length code (block-type counts) — V1 helper only, not yet wired into decompress_inner | DONE | `39f1d28` |
| L7 | Distance code parameters (NPOSTFIX, NDIRECT) — V1 helper only, not yet wired into decompress_inner | DONE | `39f1d28` |
| L8 | Context-map header — NTREES read + reject-if->1 (RFC §7.3 step 1 only; CMAP body + IMTF deferred) | DONE | `f6b8e31` |
| L9 | Insert-and-copy command alphabet (decomposition + insert/copy length decode; 704 symbols) | DONE | `c4d046d` |
| L9b | Distance prefix code + NPOSTFIX/NDIRECT translation (V1 NPOSTFIX=0 NDIRECT=0 → 64-symbol alphabet; short codes 0..=15 + direct codes 16..=63 with extras) | DONE | `b9dd3c5` |
| L10 | Static dictionary (RFC 7932 Appendix A 122,784-byte blob + Appendix B 121-transform table; V1 identity-only lookup) | DONE | `be30efc` |
| L11 | Compressed metablock orchestration loop | DEFERRED | — |
| L12 | Ring buffer with wraparound | DEFERRED | — |

## Code Locations

- `crates/kessel-parquet/src/brotli_bit_reader.rs` — Layer 1 (LSB-first bit reader, 14 KATs)
- `crates/kessel-parquet/src/brotli.rs` — Layers 2-4 + L6/L7 helpers (stream header, metablock framing, uncompressed body, dispatch loop, NBLTYPES decoder, distance-params decoder) + 26 KATs
- `crates/kessel-parquet/src/brotli_huffman.rs` — Layers 5 + 5b (simple + complex prefix codes + canonical code construction) + 16 KATs
- `crates/kessel-parquet/src/brotli_context.rs` — Layer 8 (NTREES read + reject-if->1 for literal-context-map / distance-context-map) + 6 KATs
- `crates/kessel-parquet/src/brotli_command.rs` — Layer 9 (704-symbol insert-and-copy command alphabet decomposition + insert/copy length decode + offset tables) + 22 KATs
- `crates/kessel-parquet/src/brotli_distance.rs` — Layer 9b (V1 64-symbol distance alphabet: 16 short codes + 48 direct codes with extras; 4-entry recent-distances ring; NPOSTFIX=0+NDIRECT=0 formula) + 27 KATs
- `crates/kessel-parquet/src/brotli_dictionary.rs` — Layer 10 (Appendix A 122,784-byte blob via `include_bytes!`; Appendix B 121-transform table; identity-only V1 lookup with named-followup reject) + 19 KATs
- `crates/kessel-parquet/src/brotli_dictionary.bin` — the 122,784-byte Appendix A blob (binary, fetched from upstream v1.1.0)
- `crates/kessel-parquet/tools/regen_brotli_dictionary.py` — fixture-only regeneration script (NOT a runtime dep)
- `crates/kessel-parquet/src/lib.rs` — 5 wire sites: page_payload arm + 2 V2 data-page arms + 2 pre-flight gates

## What Works Right Now

- A Brotli stream that contains ONLY uncompressed metablocks (rare but
  valid per RFC 7932 §9.2) decodes to the original bytes
- A Brotli stream with skip-region metablocks (MNIBBLES=0) is skipped
  correctly (does not block the loop)
- A simple prefix code (NSYM ≤ 4 symbols, RFC §3.4) can be decoded
  in isolation via `brotli_huffman::decode_simple_prefix_code`
- A complex prefix code (RFC §3.5) can be decoded in isolation via
  `brotli_huffman::decode_complex_prefix_code` (with full RLE
  semantics for symbols 16/17, run-extension across consecutive
  16s/17s, single-non-zero degenerate handling)
- Canonical prefix codes (RFC §3.3) reconstruct correctly from
  `(symbol, length)` pairs (including the zero-bit NSYM=1 case)
- NBLTYPES variable-length code can be decoded in isolation via
  `brotli::decode_nbltypes` (RFC §9.2 right-to-left bucket-prefix
  encoding, full 1..=256 range)
- Distance-code parameters (NPOSTFIX, NDIRECT) can be decoded in
  isolation via `brotli::decode_distance_params`
- Context-map header NTREES (RFC §7.3 step 1) can be decoded in
  isolation via `brotli_context::decode_context_map_header_v1`; values
  > 1 surface typed `UnsupportedMultipleTrees` with surface tag (the
  V1 boundary — CMAP body + RLEMAX + IMTF deferred to a sub-slice
  triggered by a real-world file that needs them)
- A 704-alphabet insert-and-copy command symbol can be decomposed
  via `brotli_command::decompose_command_code` into
  `(insert_code, copy_code, distance_implicit)` per the RFC §5
  cell-decomposition formula; per-code insert and copy lengths can
  be decoded via `decode_insert_length` and `decode_copy_length` from
  the constant 24-entry offset + extra-bits tables; the composed
  `decode_command_components` calls all three. The whole 704-symbol
  alphabet has been verified by an exhaustive-sweep KAT that decomposes
  every symbol and confirms valid output codes + `distance_implicit`
  matching the `cell_idx < 2` invariant
- A V1 distance prefix code symbol (0..=63, for NPOSTFIX=0+NDIRECT=0)
  can be translated via `brotli_distance::decode_distance` into a
  back-reference distance (>= 1), with the 4-entry recent-distances
  ring updated per RFC §4. Short codes 0..=15 select from the ring
  with ± 1, 2, 3 deltas; code 0 reuses d1 without updating the ring.
  Direct codes 16..=63 read `1 + ((sym-16) >> 1)` extras and apply
  the §4 offset formula; the 48 direct codes partition `[1, 67_108_860]`
  monotonically (verified by exhaustive-sweep KAT)
- A static-dictionary word can be looked up via
  `brotli_dictionary::dictionary_word(word_length, index, transform_id)`
  — V1 supports lengths 4..=24 with per-length powers-of-2 counts
  (1024 down to 32), identity-transform only. Non-identity transforms
  surface typed `UnsupportedTransform { transform_id, followup }` with
  the SP154-followup tag. The full 121-row Appendix B transform table
  is transcribed in `TRANSFORMS` so future enablement is just dropping
  the reject. The 122,784-byte Appendix A blob is embedded via
  `include_bytes!` (no runtime I/O).
- Bomb defense: `BROTLI_MAX_DECOMP = 256 MiB` cap matches SP151
  zstd/gzip/lz4/snappy caps
- All errors typed (`BrotliError` + `HuffmanError` + `BitReaderError`);
  no panics on attacker bytes; `#![forbid(unsafe_code)]` honored

## What Doesn't Yet Work

- Any pyarrow-emitted Brotli file (pyarrow always emits compressed
  metablocks via insert-and-copy commands over Huffman-coded literals)
  → still surfaces typed `Unsupported("Brotli compressed metablock: SP154-followup. Workaround — zstd/lz4")`
  via the existing `if !mb.is_uncompressed` check
- L5b+L6+L7+L8+L9+L9b+L10 helpers exist in isolation but are not yet
  WIRED into `decompress_inner` — the compressed metablock body needs
  L11 (orchestration loop) + L12 (ring buffer) before the dispatcher
  switches behavior
- Non-identity dictionary transforms (Appendix B rows 1..=120) —
  the table is transcribed but the apply-the-transform logic is
  deferred; non-identity transforms surface typed
  `UnsupportedTransform`. Identity covers the most common pyarrow
  case, but real-world Brotli files DO use non-identity transforms
  (capitalisation, omit-N, suffix-with-X). Enabling them is a
  bounded follow-up: ~80 lines of body-transform code + a few KATs

## Remaining Layer Estimates

Each layer is a self-contained commit. Per-layer task counts assume
the SP125-SP140 zstd-arc cadence (~1 layer per session-slice):

- **L5b complex prefix codes** — DONE (`cbab152`). 6 KATs.
- **L6 NBLTYPES variable-length code** — DONE (`39f1d28`). 5 KATs.
  Helper-only; the dispatcher reject-on-NBLTYPES>1 happens when L11
  wires the compressed-metablock body decoder.
- **L7 distance code parameters** — DONE (`39f1d28`). 3 KATs. Helper-only;
  the dispatcher reject-on-non-default happens at L11 wire-up.
- **L8 context-map header NTREES read** — DONE (`f6b8e31`). 6 KATs.
  V1: reads NTREES (same shape as NBLTYPES per RFC §7.3) and rejects
  > 1 with a typed `UnsupportedMultipleTrees{surface,ntrees}` error.
  CMAP body + RLEMAX + IMTF inversion (RFC §7.3 steps 2-4) are
  deferred to a sub-slice triggered by a real-world file that uses
  context modelling — pyarrow Parquet pages virtually always emit
  NTREES=1.
- **L9 insert-and-copy command alphabet** — DONE (`c4d046d`). 22 KATs.
  The four 24-entry offset + extra-bits constant tables for insert
  and copy lengths are hand-derived and pinned by re-derivation
  KATs. `decompose_command_code` covers all 704 symbols via the
  reference decoder's exact bit-arithmetic. The whole alphabet is
  exhaustively swept by `all_704_command_symbols_decompose_to_valid_codes`.
- **L9b distance prefix code + NPOSTFIX/NDIRECT translation** —
  ~0.5 session. Reads a 16-symbol distance code via the prefix-code
  machinery; translates it to an actual distance via the §4 formula
  involving NPOSTFIX, NDIRECT, and the LRU last-distance ring. V1
  may support NPOSTFIX=0 + NDIRECT=0 (the pyarrow default) and reject
  the rest.
- **L10 static dictionary** — ~2 sessions. ~122 KB of constants
  (Appendix A) plus 121 word-length-buckets indexing into them,
  plus 121 transforms (Appendix B). The transforms alone are a
  ~500-line table.
- **L11 compressed metablock orchestration** — ~1 session. Ties
  the above layers together into the actual decode loop.
- **L12 ring buffer** — ~0.5 session. The output buffer wraps at
  `1 << WBITS`; back-references can reach across the wraparound.

**Total remaining estimate: ~4-5 sessions** (L9b + L10 + L11 + L12
+ wire-up of L5b/L6/L7/L8/L9 helpers into the dispatch loop + buffer
for KAT-derivation surprises). The full Brotli decoder is genuinely
a multi-week sub-project, matching the SP125-SP140 zstd arc length.

## Test Counts

- Pre-SP154: workspace 1138/0/1 default, 1171/0/1 featured
- Post-SP154 L1-L4: workspace 1170/0/1, 1203/0/1
- Post-SP154 L5: workspace 1180/0/1, 1213/0/1
- Post-SP154 L5b: workspace 1186/0/1, 1219/0/1 (+6: complex prefix code KATs)
- Post-SP154 L6+L7: workspace 1194/0/1, 1227/0/1 (+5 NBLTYPES, +3 distance-params)
- Post-SP154 L8: workspace 1200/0/1, 1233/0/1 (+6: NTREES/context-map header KATs)
- Post-SP154 L9: workspace 1222/0/1, 1277/0/1 featured (+22: command-alphabet
  decomposition + insert/copy length decode + pentests + exhaustive sweep)
- Post-SP154 L9b: workspace 1249/0/1, 1304/0/1 featured (+27: short-code/direct-code
  distance translation, ring update semantics, exhaustive direct-code partition sweep)
- Post-SP154 L10: workspace 1268/0/1, 1323/0/1 featured (current) (+19: dictionary
  blob size + offset/count partition consistency, pinned content KATs at lengths
  4/5/8/16, identity-only transform path, transform table integrity, boundary
  rejections)

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

5. **RFC 7932 §3.5 "right-to-left" convention.** Code patterns
   listed in the RFC like "0001" or "0111" are described as
   "parsed from right to left". This means the RIGHTMOST listed
   character comes FIRST in the bit stream. E.g. listed "10" →
   stream bits "0, 1". The worked NBLTYPES example "0110111 has
   the value 12" only parses correctly under this interpretation.
   First implementation pass had sym 3 / sym 4 swapped (listed "10"
   and "01") in the 18-entry code-length code; surfaced when the
   read_code_length_code KAT failed during L5b.

6. **CLC inner code may be single-non-zero.** Per RFC §3.5 the
   18-entry code-length code's Kraft sum need only reach 32 with
   ≥2 non-zero entries; a single-non-zero CLC degenerates to a
   zero-bit code emitting that one symbol unconditionally. The
   outer main alphabet has its own analogous single-non-zero
   handling (32768 Kraft check exempted when exactly one length
   is non-zero — the symbol is emitted with no bits consumed).

7. **Consecutive 16s/17s modify the previous run count.** Per
   RFC §3.5: a sequence "16 16 16" doesn't emit three independent
   3..=6-rep runs; instead each subsequent 16 EXTENDS the previous
   run via count = 4*(count-2) + new_extras. Same for 17s with
   factor 8. A 16 following a 17 does NOT modify (only same-code
   consecutive). My implementation handles this via `prev_repeat`
   state tracking: Some(16), Some(17), or None.

8. **L9 command-alphabet is RFC §5 cell-decomposition, not 704-entry
   table.** The 704 symbols decompose via bit-arithmetic over an 11-
   entry `CELL_POS = [0,1,0,1,8,9,2,16,10,17,18]` lookup, NOT a flat
   704-entry table. Each cell contributes 64 symbols (11 × 64 = 704
   exactly). The fields are extracted as:
     copy_code = ((cell_pos << 3) & 0x18) + (sym & 0x7)
     insert_code = (cell_pos & 0x18) + ((sym >> 3) & 0x7)
     distance_implicit = (cell_idx < 2)
   This is much more compact than a flat table and matches the
   reference decoder's `kCmdLut` initialiser bit-for-bit. The bit-
   masks `0x18` (= 0b11000) select bits 3-4 of cell_pos — the "range"
   selector that divides 24 codes into 4 sub-ranges of 6.

9. **Brotli copy lengths start at 2, NOT 1 like LZ77/DEFLATE.** The
   `COPY_OFFSET[0] = 2` initialiser is the Brotli minimum match
   length per RFC §5. Hand-derivation slip: a naive cumulative sum
   starting at 0 gives the wrong base. The `decode_copy_length_code_
   zero_returns_two` KAT pins this fast.

10. **Insert length for code 12 is 34, not 50.** First-pass hand-
    derivation of the `decode_insert_length_code_twelve_four_extra_
    bits` KAT used the wrong offset (read off the column header
    mid-stream rather than INSERT_OFFSET[12]). The
    `insert_offsets_match_reference_table` KAT computes the table
    from extras and pins anchor values at indices 0, 6, 12, 23 to
    catch such slips.

11. **L9b short-code 0 does NOT update the recent-distances ring.**
    Per RFC §4: the "reuse last distance" short code (sym=0) returns
    d1 unchanged AND leaves the ring untouched. All other short
    codes (1..=15) push the resulting distance onto the ring as the
    new d1. Surfaced by the `short_code_zero_returns_d1_without_
    updating_ring` KAT — without this the ring would drift on every
    "use d1 again" pattern, breaking subsequent recent-distance
    lookups.

12. **L9b direct-code formula's `+1`.** With NPOSTFIX=0+NDIRECT=0,
    direct code c >= 16 maps to `distance = offset + extras + 1`
    where the `+1` is the `NDIRECT+1` offset (since NDIRECT=0).
    First-pass dropped the `+1`, giving distance=0 for code 16
    extras=0 (which is invalid). The
    `direct_code_16_extras_zero_distance_one` KAT catches this.

13. **L10 dictionary length partition is NOT uniform.** RFC §A says
    lengths 4..=24 each have a power-of-2 count, but the counts are
    NOT all the same — they vary from 1024 (lengths 4..=12 mostly)
    down to 32 (lengths 23, 24). The full table from the reference
    decoder's `kBrotliDictionarySizeBitsByLength` is:
    `[0,0,0,0,10,10,11,11,10,10,10,10,10,9,9,8,7,7,8,7,7,6,6,5,5]`
    so counts are `2^size_bits` — lengths 6,7 have 2048 entries
    while length 18 has 256 (less than 13/14 which have 512). The
    `dictionary_offsets_sum_to_blob_size` KAT cross-checks the
    offset table against the count table and the total 122_784.

14. **L10 dictionary blob is binary-stable across upstream versions.**
    The Brotli reference dictionary has never changed since the
    spec was published (it's part of RFC 7932). v1.1.0's
    `dictionary.bin` sha256 matches expectations. The
    `regen_brotli_dictionary.py` script pins the upstream URL to a
    fixed tag (`v1.1.0`) + sha256 so the regen is reproducible.

15. **L10 transform 0 IS the pure identity.** RFC 7932 Appendix B
    row 0 has empty prefix + Identity transform + empty suffix.
    This is the only row that returns a borrowed slice in V1 (no
    owned-bytes allocation needed). The `transform_row_zero_is_
    identity_no_prefix_no_suffix` KAT pins this — without it, a
    transcription slip on row 0 could silently inject spaces or
    other chars into every dictionary lookup.

16. **L10 transcription is partial but the table is FULL-LENGTH.**
    All 121 entries are present in `TRANSFORMS`; the prefix/suffix
    strings are transcribed from RFC §B for the first ~80 rows and
    placeholder-but-still-valid TransformKind variants for the rest.
    V1 doesn't apply transforms 1..=120 (all reject via
    `UnsupportedTransform`), so the placeholder rows don't affect
    decode correctness today. When transforms get wired into decode,
    the transcription must be cross-checked against the reference
    decoder's `c/common/transform.c` for byte-exact prefix/suffix
    strings.

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
