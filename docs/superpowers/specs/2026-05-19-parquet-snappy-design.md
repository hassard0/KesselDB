# OBJ-2b-3 — Parquet Snappy block decompression: Design

**Date:** 2026-05-19
**Status:** Approved (autonomous mandate — see Process Note)
**Subproject:** 104 (third sub-slice of the OBJ-2b arc)
**Builds on:** subproject 97/98/99/100/101/102/103

## Process Note (autonomy)

Produced under the standing overnight autonomous-build mandate
(`feedback_kesseldb_autonomous_build`): the user directed "keep going"
and is intermittently away. The brainstorming user-review gate is
satisfied by this documented decision record. All other rigor retained:
two-stage subagent review per task (spec then code-quality), a final
whole-implementation review, full `cargo test --workspace --release` +
seed-7 + a pentest pass each kernel-adjacent task. Genuine design
alternatives are recorded with the chosen option and the reason.

## Problem

SP103 shipped dictionary decode but `read_chunk_values`
(`crates/kessel-parquet/src/lib.rs:55`) still rejects **any**
compression with `Unsupported("compression: OBJ-2b-3")`. pyarrow's
**default** is `compression='snappy'`. So OBJ-2b-3 is the last big
realism gap: with Snappy + the already-shipped dictionary support,
KesselDB reads pyarrow's *true default output*. This slice hand-rolls a
pure zero-dependency Snappy decompressor and flips the Snappy codec
gate; all other gates (OPTIONAL → OBJ-2b-4; gzip/zstd/INT96/V2 →
OBJ-2c) stay rejected with typed errors.

## Snappy format (authority: `google/snappy` `format_description.txt`)

Parquet stores each page's compressed bytes as a single **raw Snappy
block** — *not* the Snappy stream/framing format (no `0xff`
stream-identifier chunk, no per-chunk CRC). The page header's
`uncompressed_page_size` is the decompressed length;
`compressed_page_size` is the on-disk byte length. Decision: implement
and KAT the **raw block** format only (justified: that is exactly what
Parquet uses; the stream format is irrelevant here and adding it would
be unused scope).

Raw block layout:

- **Preamble:** uncompressed length as a little-endian base-128 varint.
- **Elements** until the source is exhausted; each starts with a tag
  byte, `tag & 0b11` selects the type:
  - `00` **literal.** `len1 = tag >> 2`. If `len1 < 60`, literal
    length = `len1 + 1`. If `len1 ∈ {60,61,62,63}`, the next
    `len1 - 59` bytes are a little-endian integer = (length − 1);
    length = that + 1. Then `length` bytes are copied verbatim from
    src to output.
  - `01` **copy, 1-byte offset.** length = `4 + ((tag >> 2) & 0b111)`
    (4..=11). offset = `((tag >> 5) << 8) | next_byte` (11-bit).
  - `10` **copy, 2-byte offset.** length = `1 + (tag >> 2)` (1..=64).
    offset = next 2 bytes little-endian.
  - `11` **copy, 4-byte offset.** length = `1 + (tag >> 2)` (1..=64).
    offset = next 4 bytes little-endian.
- A copy back-references already-produced output: it requires
  `1 <= offset <= output.len()` and copies `length` bytes starting at
  `output.len() - offset`. **Overlapping copies (`offset < length`)
  are legal** and produce a repeating pattern — they MUST be
  implemented byte-by-byte (RLE-style), not via a single
  `copy_within`/slice copy. This is the classic Snappy correctness
  trap and gets an explicit positive KAT.
- Decompression ends when src is exhausted; the produced length MUST
  equal the preamble length.

## Architecture

Decode-only change inside `kessel-parquet`. New pure module
`crates/kessel-parquet/src/snappy.rs`; `meta.rs` learns
`Codec::Snappy`; `lib.rs::read_chunk_values` gains a small page-payload
helper and the codec-gate flip. No `kessel-fetch`/`kessel-sql`/server/
kernel change.

### Components

1. **`snappy.rs`** (pure, zero-dep):
   ```rust
   /// Hard cap on a single decompressed Parquet page. Real writers
   /// (pyarrow default data_page_size ~1 MiB) stay far below this;
   /// the cap defeats a decompression-bomb (tiny input claiming a
   /// multi-GB uncompressed_page_size). Pages above it are rejected
   /// as Unsupported (OBJ-2c may revisit).
   pub(crate) const SNAPPY_MAX_DECOMP: usize = 64 << 20; // 64 MiB

   /// Decompress one raw Snappy block. `expected_len` is the page
   /// header's uncompressed_page_size (the authority). The block's
   /// own varint preamble MUST equal `expected_len` (defense in
   /// depth). Output is bounded; every offset/length checked; never
   /// panics / OOM-aborts.
   pub fn decompress(src: &[u8], expected_len: usize)
       -> Result<Vec<u8>, PqError>;
   ```
   Behavior: reject `expected_len > SNAPPY_MAX_DECOMP` as
   `Unsupported("snappy page exceeds {SNAPPY_MAX_DECOMP} cap: OBJ-2c")`
   *before any allocation*. Read the preamble varint; if it ≠
   `expected_len` → `Bad`. `Vec::with_capacity(expected_len)` (now
   provably ≤ 64 MiB — safe). Decode elements; every literal-length /
   copy-length / offset is `checked_*` and bounds-checked against src
   and current output; output length is never allowed to exceed
   `expected_len` (overproduction → `Bad`); a copy with
   `offset == 0 || offset > out.len()` → `Bad`; overlapping copy done
   byte-by-byte. At end: `out.len() == expected_len` else `Bad`;
   trailing garbage after the last element with output already full →
   `Bad`. Unknown/zero-length malformed → `Bad`. `Unsupported` is used
   *only* for the over-cap case (a valid-but-too-big page); all
   malformed inputs are `Bad`.

2. **`meta.rs`** — `Codec`: add `Snappy`. `from_i32`: `0 →
   Uncompressed`, `1 → Snappy`, `o → Other(o)` (parquet
   `CompressionCodec` SNAPPY = 1).

3. **`lib.rs::read_chunk_values`** — two changes:
   - **Codec gate flip** (line 55): replace
     `if cc.codec != Uncompressed { Unsupported }` with
     `match cc.codec { Uncompressed | Snappy => {}, _ =>
     return Err(Unsupported("compression codec (gzip/zstd/lz4/brotli): OBJ-2c")) }`.
   - **Unified page-payload helper.** Today the dict-page arm and the
     data-page loop each slice `[dstart .. dstart + uncompressed_size]`
     and advance `off` by `uncompressed_size`. That is a *latent
     OBJ-2a assumption* (`compressed == uncompressed`) — correct only
     for uncompressed pages. Introduce:
     ```rust
     fn page_payload<'a>(
         file: &'a [u8],
         dstart: usize,
         comp: usize,        // ph.compressed_size
         uncomp: usize,      // ph.uncompressed_size
         codec: meta::Codec,
     ) -> Result<std::borrow::Cow<'a, [u8]>, PqError>;
     ```
     It slices the **on-disk** region `[dstart .. dstart + comp]`
     (checked), then: `Uncompressed → Cow::Borrowed(slice)` (zero-copy,
     no perf regression for the common path; also asserts
     `comp == uncomp`-consistency is *not* required — uncompressed
     pages set them equal but we slice by `comp` and decode `uncomp`
     bytes, which are equal); `Snappy → Cow::Owned(snappy::decompress(
     slice, uncomp)?)`. The existing PLAIN/dict decode then runs over
     `&payload` with `uncomp`/`num_values` exactly as today. **`off`
     advances by `comp`** (the on-disk size), for *both* the dict page
     and every data page.
   - The dictionary page is itself Snappy-compressed when
     `codec == Snappy`; it flows through the same `page_payload`
     helper before `plain::decode_plain`. Decision: one helper, both
     page kinds — no special-casing.

### Why offset-advance-by-`compressed_size` is safe for existing tests

All hand-built fixtures (SP101/103 builders) and the OBJ-2a path set
`compressed_page_size == uncompressed_page_size`, so `comp == uncomp`
and the slice/advance are byte-identical to today — every existing
OBJ-2a/2b test stays green unchanged. For real Snappy pages
`comp < uncomp` and the new path is exercised. This change also
*corrects* the latent OBJ-2a assumption rather than introducing risk.

## Bold, documented decisions (options → choice)

- **Hand-roll, no `snappy`/`snap` crate.** The zero-external-dependency
  kernel invariant is non-negotiable; `kessel-parquet/Cargo.toml`
  `[dependencies]` stays empty. Snappy raw-block decode is ~120 lines.
- **Raw block, not stream framing.** Parquet uses the raw block format;
  the stream format (`0xff` identifier + CRC chunks) never appears in a
  Parquet page. Implementing it would be dead scope. KAT the raw block.
- **`Cow<[u8]>` page payload.** Rejected: always copying (an
  allocation + memcpy per uncompressed page = a real perf regression on
  the existing hot path). Rejected: duplicating the slice/decompress
  logic inline in both the dict and data arms (DRY violation, two
  places to get the `comp`-vs-`uncomp` distinction wrong). The `Cow`
  helper is zero-copy for uncompressed, owned for Snappy, single
  source of truth.
- **64 MiB hard decompressed-page cap (`SNAPPY_MAX_DECOMP`).** A page
  header `uncompressed_page_size` is an `i32` (≤ ~2.1 GB) and is
  attacker-influenceable; a tiny compressed input claiming a 2 GB
  uncompressed size is a classic decompression bomb. Rejected:
  bounding by `compressed_len * ratio` (Snappy RLE copies legitimately
  expand unboundedly, so no honest ratio bound exists). Rejected:
  trusting the upstream page-size gate (it bounds the *on-disk* size,
  not the decompressed size). Chosen: a fixed 64 MiB cap — ~64× the
  pyarrow default `data_page_size` (1 MiB), comfortably above any sane
  real page, far below a memory-DoS; over-cap pages are typed
  `Unsupported(... OBJ-2c)` (a future slice can raise it for genuine
  large-page use). Combined with "output never exceeds
  `expected_len`", the eager `Vec::with_capacity(expected_len)` is
  provably ≤ 64 MiB.
- **Preamble must equal `expected_len`.** Defense in depth: the page
  header is the allocation authority, but a self-consistent block must
  also declare the same length internally; a mismatch is a malformed
  block → `Bad`.
- **Overlapping copies byte-by-byte.** The single most common Snappy
  decoder bug. Explicit positive KAT (`"aaaaaa"` from a 1-byte literal
  + an offset-1 length-5 copy).

## Determinism / source-format independence

Decompressed bytes feed the *same* `plain::decode_plain` /
`dict::resolve_dict_indices` → `PqValue` → `pq_to_cell` →
`coerce::to_field_bytes` path, unchanged. Same logical data is
byte-identical whether stored Snappy or uncompressed. Pinned by a test:
hand-build the same logical column uncompressed and Snappy and assert
`extract()` returns an identical `Vec<Vec<PqValue>>`. `snappy.rs` /
`page_payload` are pure functions of bytes — deterministic by
construction; no clock/env/IO.

## Security posture (pentest — extends SP101 T12 / SP102 / SP103)

Parquet bytes are operator-source-controlled. Lock tests
(`catch_unwind`, typed `Err`, no panic/OOM/stack-overflow):

- preamble varint huge / `expected_len > 64 MiB` → `Unsupported`
  **before allocation** (no OOM);
- decompression bomb: tiny `src`, `expected_len` = 2 GB →
  `Unsupported`/`Bad`, no multi-GB allocation;
- preamble varint ≠ `expected_len` → `Bad`;
- copy `offset == 0` → `Bad`; `offset > out.len()` → `Bad`;
- copy length would push output past `expected_len` → `Bad`;
- literal length past end of `src` → `Bad`;
- truncated tag / truncated extra-length / truncated offset → `Bad`;
- trailing bytes after output already == `expected_len` → `Bad`;
- final produced length ≠ `expected_len` → `Bad`;
- **positive correctness lock:** overlapping copy (`offset < length`)
  decodes to the correct RLE expansion (asserted `Ok`, NOT rejected —
  proves we don't mishandle valid Snappy);
- extract-level: a Snappy fixture whose page `compressed_size` lies
  (points past EOF) → typed `Bad`, no panic.

## Testing

TDD per task. Layers:

1. **Spec KATs** hand-derived from `format_description.txt` (the
   independent authority; reviewer re-derives): `"abc"` literal;
   1/2/4-byte-offset copies; the `"aaaaaa"` overlapping-copy RLE lock;
   the multi-byte literal-length (≥ 60) encoding; malformed → `Bad`.
2. **Real pyarrow `compression='snappy'` fixtures** (the capstone
   realism win): two fixtures — `snappy_dict.parquet`
   (`use_dictionary=True` — pyarrow's true default) and
   `snappy_plain.parquet` (`use_dictionary=False`), both
   `nullable=False` (REQUIRED — OBJ-2b-3 scope; OPTIONAL is OBJ-2b-4).
   pyarrow 24.0.0 is present; if regen fails the implementer reports
   **BLOCKED**, never hand-fakes (SP101 T7 stance). Roundtrip via
   `extract()`; metadata-verify the columns are actually SNAPPY +
   dict/plain.
3. **Determinism pin**: same logical column uncompressed vs Snappy →
   identical `extract()` output.
4. **e2e**: mirror the SP101 `external_source_parquet_oracle` harness
   (fail-closed, no router fixture-trust bypass) over a Snappy fixture.
5. **Pentest** lock tests as enumerated.

## Determinism / invariants gate (every task)

- `cargo test --workspace --release` `FAILED=0`;
  `large_seed_corpus_is_deterministic_and_converges` green.
- Honest gate accounting: `kessel-parquet` existing member; new
  snappy/meta/extract/fixture/pentest tests **raise the default-build
  total honestly** (baseline 326; no false zero-delta — SP100/101/
  102/103 stance); docs task reconciles measured before→after.
- Kernel pulls no new external dependency;
  `kessel-parquet/Cargo.toml` `[dependencies]` stays **empty**;
  default `cargo tree -p kesseldb-server` links no
  parquet/objstore/rustls/webpki.
- `#![forbid(unsafe_code)]`; no unwrap/expect/panic/raw-index on input
  bytes (checked `get`/`checked_*`; only the statically-infallible
  4-byte→`[u8;4]` `try_into().unwrap()` is allowed).
- Existing oracles green unchanged: `external_source_oracle` (2),
  `external_source_tls_oracle` (1), `external_source_objstore_oracle`
  (1); ALL OBJ-2a/2b gate + decode tests green unchanged (they use
  `comp == uncomp`).

## Out of scope (deferred)

- OPTIONAL/REPEATED, definition/repetition levels → **OBJ-2b-4**.
- gzip/zstd/lz4/brotli, INT96/DECIMAL, REPEATED/nested, V2 data pages,
  pages above the 64 MiB Snappy cap → **OBJ-2c**.

The schema `repetition == Required` gate is unchanged — OPTIONAL Snappy
files still get their existing typed `Unsupported` until OBJ-2b-4.

## Task decomposition (one coherent slice)

- **T0** determinism baseline (record 326).
- **T1** `snappy.rs`: `decompress` + `SNAPPY_MAX_DECOMP`; spec KATs
  (literal, 1/2/4-byte-offset copy, overlapping-copy RLE lock,
  multi-byte literal length, malformed→Bad).
- **T2** `meta.rs`: `Codec::Snappy` (`from_i32` 1); KAT (codec field
  decodes Snappy; `Other` unchanged).
- **T3** `lib.rs`: `page_payload` `Cow` helper + codec-gate flip +
  advance-by-`compressed_size` (both dict & data pages); hand-built
  Snappy dict+plain decode test + the uncompressed-vs-Snappy
  determinism pin; all OBJ-2a/2b tests stay green.
- **T4** real pyarrow `compression='snappy'` fixtures (dict + plain,
  REQUIRED) + roundtrip + e2e (SP101 harness, fail-closed); README
  regen + metadata verification.
- **T5** pentest pass (all enumerated hostile vectors + the
  overlapping-copy positive lock + the lying-`compressed_size`
  extract-level lock).
- **T6** docs: `docs/superpowers/specs/2026-05-19-kesseldb-subproject104-snappy.md`
  record (SP103 convention exactly) + STATUS row after SP103 (numeric
  order, gate numbers + `Record:` backlink) + USAGE §7f note (no
  overclaim) + gate reconciliation (326 → measured) + auto-memory
  (SP104 block + MEMORY.md line, outside repo, never git-add).

## Internal record

`docs/superpowers/specs/2026-05-19-kesseldb-subproject104-snappy.md`
at docs time, mirroring the SP103 record convention exactly (KesselDB
H1, `**Status:**` line, bare-backtick-path Builds-on, `---`
separators, honest gate reconciliation, deferred list).
