# OBJ-2b — Real-World Parquet (dictionary + Snappy + nullable): Design

**Date:** 2026-05-19
**Status:** Approved (autonomous mandate — see Process Note)
**Subproject:** 102 (first sub-slice of the OBJ-2b arc)
**Builds on:** subproject 97/98/99/100/101 (External Sources → TLS → Object-Store OBJ-1 → Parquet OBJ-2a)

## Process Note (autonomy)

This design was produced under the standing overnight autonomous-build
mandate (`feedback_kesseldb_autonomous_build`): the user is away and has
directed continuous, bold, independent progress without per-slice
approval. The brainstorming skill's **user-review gate is therefore
satisfied by this documented decision record** rather than an
interactive approval. All other rigor is retained unchanged: the
two-stage subagent review gate (spec-compliance then code-quality) per
task, a final whole-implementation review, full
`cargo test --workspace --release` + seed-7 + a pentest pass each
kernel-adjacent task. Design options that had genuine alternatives are
recorded below with the chosen option and the reason.

## Problem

OBJ-2a (subproject 101) shipped a pure-Rust, zero-external-dependency
Parquet reader, but only for **PLAIN encoding, UNCOMPRESSED, flat
REQUIRED columns, V1 data pages**. Real-world Parquet — in particular
pyarrow's *default* output (`compression='snappy'`,
`use_dictionary=True`, plus `OPTIONAL` columns for any nullable field)
— is rejected by OBJ-2a with precise typed `Unsupported` errors. The
OBJ-2a fixtures had to be written with the non-default
`use_dictionary=False, compression=NONE`. To make KesselDB read Parquet
that real producers actually emit, OBJ-2b adds the dominant subset:
RLE/bit-packing-hybrid decoding, dictionary encoding, Snappy block
decompression, and OPTIONAL (nullable) flat columns.

## Scope decomposition (the OBJ-2b arc)

OBJ-2b is too large for one spec/plan/review cycle. It decomposes into
four independently-shippable, independently-reviewable sub-slices, each
its own spec → plan → subagent-driven-development cycle:

| Sub-slice | Adds | Gate(s) flipped |
|---|---|---|
| **OBJ-2b-1** (this spec, SP102) | `rle.rs`: the Parquet RLE/bit-packing-hybrid decoder — the shared primitive for dictionary indices **and** definition/repetition levels. Pure function; **no wiring, no gate flips**. | none (pure primitive) |
| OBJ-2b-2 | Dictionary-page decode (PLAIN values via existing `decode_plain`) + data-page index stream via `rle.rs` → resolved values. | dictionary encoding (`PLAIN_DICTIONARY`/`RLE_DICTIONARY`) |
| OBJ-2b-3 | Hand-rolled Snappy raw-block decompression (`snappy.rs`), KAT-pinned to the Snappy format spec; decompress page bytes before decode. | `SNAPPY` codec |
| OBJ-2b-4 | OPTIONAL flat columns: V1 RLE-encoded definition levels → `PqValue::Null` where def-level < max; nullable wiring through `pq_to_cell`/coerce; support-matrix flips; **pyarrow-default fixtures**; fail-closed e2e. | `OPTIONAL` repetition; removes the OBJ-2a non-default-producer fixture constraint |

**Why this order:** `rle.rs` is the foundational primitive that both
the dictionary path (OBJ-2b-2) and the level path (OBJ-2b-4) consume.
Building and hardening it in isolation first — with zero wiring risk and
no support-matrix changes — means the subsequent slices integrate a
*proven, pentested* primitive rather than co-developing it with their
integration logic. This is the established KesselDB "one concern per
slice" discipline.

This document fully designs **OBJ-2b-1**. OBJ-2b-2/3/4 get their own
specs when they are scheduled.

---

# OBJ-2b-1: RLE / bit-packing hybrid decoder

## Goal

Add `crates/kessel-parquet/src/rle.rs`: a pure, bounds-checked decoder
for the Apache Parquet **RLE / bit-packing hybrid** run-length encoding,
KAT-pinned to the published `parquet-format` `Encodings.md` grammar (the
independent authority — *not* self-referential). No public-API change to
`kessel-parquet`, no `extract` change, no support-matrix gate change,
no `kessel-fetch`/`kessel-sql`/server change. The crate stays
zero-external-dependency; the default KesselDB build and seed-7 corpus
are byte-untouched (the crate is still only compiled by
`kessel-fetch`'s `object-store` feature).

## The encoding (authority: `parquet-format` `Encodings.md`)

```
rle/bit-packed-hybrid:
  encoded-data := <run>*
  run          := <bit-packed-run> | <rle-run>

  bit-packed-run := <bit-packed-header> <bit-packed-values>
    bit-packed-header := varint( (number_of_groups_of_8 << 1) | 1 )   # LSB = 1
    bit-packed-values := number_of_groups_of_8 * 8 values, each
                         `bit_width` bits, packed LSB-of-stream-first;
                         within a value, bits in normal MSB→LSB order.

  rle-run := <rle-header> <repeated-value>
    rle-header     := varint( run_length << 1 )                       # LSB = 0
    repeated-value := the value, fixed width = ceil(bit_width/8) bytes,
                      little-endian.

  # In the V1 definition/repetition-level context the stream is framed:
  #   <length:u32 LE> <encoded-data of exactly `length` bytes>
  # In the dictionary-data-page index context it is NOT length-framed;
  # a single leading bit-width byte precedes <encoded-data> and is the
  # CALLER's concern (OBJ-2b-2), not this module's.
```

Subtleties this decoder must get exactly right (each pinned by a KAT):

1. **Header LSB discriminates the run kind**: `header & 1 == 1` →
   bit-packed run; `== 0` → RLE run.
2. **Bit-packed length is in groups of 8**: `header >> 1` is the number
   of 8-value groups; the run yields `groups * 8` values. A run may
   over-produce past the requested count — trailing padding values in
   the final group are **discarded** (caller-requested `num_values`
   wins).
3. **LSB-of-stream-first bit packing** (the Parquet order, *not* the
   deprecated `BIT_PACKED` MSB order). Canonical spec example: bit
   width 3, values `0,1,2,3,4,5,6,7` encode to the three bytes
   `0x88 0xC6 0xFA`. This is the load-bearing KAT.
4. **RLE repeated-value width** = `ceil(bit_width / 8)` bytes,
   little-endian, zero-extended into a `u64`.
5. **`bit_width == 0` is legal** (single-entry dictionary; all
   def-levels equal). Every decoded value is `0`; **no value bytes are
   consumed** (an RLE run still has its header; a bit-packed run still
   has its header but zero value bytes).
6. **Stream/`num_values` reconciliation**: decode runs until
   `>= num_values` produced, then truncate to exactly `num_values`. If
   the stream is exhausted before `num_values` are produced →
   `PqError::Bad`. Bytes required by any run that exceed the slice →
   `PqError::Bad`.

## Public surface (within the crate)

`rle.rs` is a private module (`mod rle;` in `lib.rs`, like
`thrift`/`plain`); nothing is added to the crate's public API in this
slice.

```rust
// crates/kessel-parquet/src/rle.rs

/// Decode exactly `num_values` from a Parquet RLE/bit-packing-hybrid
/// stream of fixed `bit_width` (0..=64). Values returned as `u64`; the
/// caller narrows (dictionary index / definition level / repetition
/// level). Consumes only the bytes the runs require; bit-packed
/// over-production past `num_values` is discarded. Never panics,
/// never OOM-aborts on hostile input — returns `PqError::Bad`.
pub fn decode_hybrid(
    data: &[u8],
    bit_width: u32,
    num_values: usize,
) -> Result<Vec<u64>, PqError>;

/// V1 level stream: a 4-byte little-endian length prefix followed by
/// exactly that many bytes of hybrid `<encoded-data>`. Decodes
/// `num_values` levels of `bit_width` and returns
/// `(levels, total_consumed)` where `total_consumed` includes the
/// 4-byte prefix (so the caller can advance to the value section).
pub fn decode_level_v1(
    data: &[u8],
    bit_width: u32,
    num_values: usize,
) -> Result<(Vec<u64>, usize), PqError>;
```

### Design decisions (options → choice)

- **Return type `Vec<u64>`** (vs. a generic `T` or `Vec<i64>`): chosen
  for simplicity and YAGNI. Dictionary indices and Parquet level values
  are non-negative and fit `u64`; callers narrow trivially. A generic
  numeric parameter buys nothing here and complicates the bounds proofs.
- **`decode_hybrid` is framing-agnostic** (raw `<encoded-data>`, caller
  supplies `bit_width` and `num_values`). The two real framings differ:
  V1 level streams are `u32`-LE-length-prefixed (handled by the thin
  `decode_level_v1` wrapper here); the dictionary index stream has a
  single leading bit-width byte and **no** length prefix — that byte is
  decoded by OBJ-2b-2 and is deliberately *not* this module's concern,
  to keep the unit boundary crisp (one module = the hybrid grammar
  only). Rejected alternative: a single do-everything function with a
  `framing` enum — it would entangle three independent concerns and
  make the bounds reasoning harder to audit.
- **`bit_width` accepted range `0..=64`**; `> 64` → `PqError::Bad`
  (cannot fit a value in the `u64` accumulator; rejecting early is
  safer than silently masking). `bit_width == 0` is a valid fast path
  (all zeros, zero value bytes).
- **Over-production**: decode whole runs, then `truncate(num_values)`.
  Simpler and spec-faithful vs. trying to stop mid-bit-packed-group.

## Error handling

Reuses the existing `PqError` (`Bad` / `Unsupported`). Every
malformed/hostile input yields `PqError::Bad` with a short static-ish
message. `Unsupported` is **not** used here — the hybrid grammar has no
"valid but unsupported" sub-cases at the primitive level (feature gating
stays in `lib.rs`). `#![forbid(unsafe_code)]` is already crate-wide; no
`unwrap`/`expect`/`panic`/indexing on input bytes — every read is a
checked `get(..)` / `checked_*`, exactly matching `plain.rs`/`thrift.rs`
discipline.

## Security posture (pentest, extends the SP101 Task-12 stance)

The Parquet bytes are operator-declared-source-controlled =
attacker-influenceable. The RLE header varints are attacker values up to
`u64::MAX`. Hardening rules (each pinned by a `pentest` lock test
wrapped in `catch_unwind`, asserting a typed `Err` and *no*
panic/OOM/stack-overflow):

- **Reservation bound**: never `Vec::with_capacity(run_len_from_header)`.
  Bound the output `Vec` reservation by `num_values` (the caller's
  expected count, itself bounded upstream by the page header's
  `dp_num_values`, which SP101 Task-12 already caps). A hostile RLE
  header `run_length = u64::MAX` is harmless: the loop only ever pushes
  up to `num_values` (+ ≤7 final-group padding) then truncates.
- **Bit-packed group bound**: before iterating a bit-packed run, require
  `groups * 8 ... ` value bytes — i.e. `groups * bit_width` must be a
  `checked_mul`, must round to a byte count that **fits the remaining
  slice**, and the produced count must not be required to exceed
  `num_values + 7`. A hostile `number_of_groups_of_8 = u64::MAX` fails
  the `checked_mul`/slice-fit check → `Bad`, no allocation.
- **RLE value-width bound**: `ceil(bit_width/8)` bytes via
  `checked_*`; a truncated repeated-value slice → `Bad`.
- **`decode_level_v1`**: the `u32` length prefix is bounds-checked
  against the slice before the inner `decode_hybrid` is called on the
  exact sub-slice; a lying/oversized prefix → `Bad`, no allocation.
- Specific lock tests: `run_len = u64::MAX`; `groups = u64::MAX`;
  truncated final bit-packed group; truncated RLE repeated-value;
  `bit_width = 64` with a tiny slice; `bit_width = 65` → `Bad`;
  `decode_level_v1` length prefix > slice; empty slice with
  `num_values > 0`.

## Testing

TDD per task. Three test layers:

1. **Spec KATs** (the independent authority — hand-derived from
   `parquet-format` `Encodings.md`, *not* produced by the decoder under
   test; the spec reviewer independently re-derives the bytes):
   - The canonical doc example: `bit_width=3`, values `0..=7` ⇒ bytes
     `[0x03, 0x88, 0xC6, 0xFA]` (header `0x03` = `(1 groups << 1)|1`;
     one group of 8). `decode_hybrid(&bytes, 3, 8)` ⇒ `0..=7`.
   - An RLE run: value `5`, run length `8`, `bit_width=3` ⇒ header
     `varint(8<<1)=0x10`, repeated-value `0x05` (1 byte =
     `ceil(3/8)`). `decode_hybrid` ⇒ eight `5`s.
   - `bit_width=0`: RLE header `varint(4<<1)=0x08`, **no** value byte ⇒
     four `0`s; bit-packed `bit_width=0` ⇒ zeros, no value bytes.
   - Mixed stream: an RLE run followed by a bit-packed run in one
     buffer; truncation when `num_values` < produced.
   - RLE repeated-value width: `bit_width=17` ⇒ 3-byte
     (`ceil(17/8)`) little-endian repeated value round-trips.
   - `decode_level_v1`: a `u32`-LE length prefix wrapping a known
     hybrid stream; assert returned `total_consumed == 4 + length`.
2. **Property/round-trip** (cross-check, still spec-independent): a
   small in-test hybrid *encoder* written directly from the grammar
   (independent code path from the decoder) → `decode_hybrid` →
   original, across random widths `1..=32` and counts. This is a
   non-self-referential round trip (encoder and decoder are separate
   implementations of the published grammar).
3. **Pentest lock tests** as enumerated under Security posture.

## Determinism / invariants gate (every task)

- `cargo test --workspace --release` → `FAILED=0`; the seed-7 corpus
  test `large_seed_corpus_is_deterministic_and_converges` green.
- **Honest gate accounting**: `kessel-parquet` is an existing workspace
  member; its unit tests already run under `cargo test --workspace`.
  Adding `rle.rs` tests **raises the default-build test total honestly**
  (no false zero-delta — the same corrected stance as SP100/SP101). The
  docs/record task records the measured before→after with the real
  reason (new `rle` module tests).
- The deterministic kernel pulls **no** new external dependency;
  `cargo tree` for the default build links no parquet/objstore/rustls;
  `kessel-parquet/Cargo.toml` `[dependencies]` stays empty.
- Existing oracles unchanged: `external_source_oracle` (2),
  `external_source_tls_oracle` (1), `external_source_objstore_oracle`
  (1), and all SP101 `kessel-parquet` tests still green.
- `decode_hybrid`/`decode_level_v1` are pure functions of their byte
  inputs (no clock/env/IO) → deterministic by construction; this slice
  adds no new value into the materialize path yet (no `pq_to_cell`
  change), so source-format-independence is trivially preserved.

## Out of scope (deferred to later OBJ-2b sub-slices / OBJ-2c)

- Dictionary page decode + index→value resolution (OBJ-2b-2).
- Snappy / any compression (OBJ-2b-3).
- OPTIONAL/def-levels wiring, nullable coerce, support-matrix flips,
  pyarrow-default fixtures, e2e (OBJ-2b-4).
- gzip/zstd, INT96/DECIMAL, REPEATED/nested, V2 data pages (OBJ-2c).

No support-matrix gate in `lib.rs` changes in this slice; the existing
typed `Unsupported` rejections (dictionary, compression, OPTIONAL, V2)
remain exactly as shipped in OBJ-2a until their owning sub-slice flips
them.

## Internal record

This slice gets its own internal record at
`docs/superpowers/specs/2026-05-19-kesseldb-subproject102-rle.md` at
docs time (mirrors the SP99/100/101 records), reconciling the measured
gate delta with the real reason.
