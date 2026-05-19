# KesselDB — Subproject 106: OBJ-2c-1 Parquet GZIP page decompression

**Date:** 2026-05-19  **Status:** done — code + tests committed and passing.

Builds on:
- Subproject 100 — Object-store external sources (OBJ-1):
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject100-objstore.md`
- Subproject 101 — Parquet OBJ-2a:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject101-parquet.md`
- Subproject 102 — RLE/bit-packing hybrid:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject102-rle.md`
- Subproject 103 — Parquet dictionary encoding:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject103-dict.md`
- Subproject 104 — Parquet Snappy decompression:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject104-snappy.md`
- Subproject 105 — Parquet OPTIONAL/nullable columns:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject105-optional.md`

Design document:
`docs/superpowers/specs/2026-05-19-parquet-gzip-design.md`

Plan document:
`docs/superpowers/plans/2026-05-19-parquet-gzip.md`

---

## What shipped

`kessel-parquet` now decompresses GZIP-compressed Parquet pages (pyarrow
`compression='gzip'`) via a pure zero-dependency RFC 1952 + RFC 1951
implementation. No external crate is added; no server, kernel, or SQL
layer is changed.

- **`crates/kessel-parquet/src/gzip.rs`** — new module:
  - **RFC 1952 wrapper parse** (`decompress`): validates the 10-byte
    gzip member header (`ID1/ID2` magic, `CM=8` method check), skips
    FLG-gated optional fields (`FEXTRA`, `FNAME`, `FCOMMENT`, `FHCRC`)
    with bounds-checked advancement, reads the 8-byte trailer
    (`CRC32` + `ISIZE`), extracts the embedded DEFLATE stream, and
    verifies `ISIZE == expected_len as u32` as defense-in-depth.
  - **RFC 1951 inflate** (`inflate`): fully iterative (no recursion →
    no stack-overflow vector). Supports all three DEFLATE block types:
    stored blocks (BTYPE=00, NLEN validation), fixed Huffman (BTYPE=01,
    RFC §3.2.6 literal/length + 5-bit distance tables), and dynamic
    Huffman (BTYPE=10, full code-length canonical Huffman decode,
    `hlit`/`hdist`/`hclen` header, symbols 16/17/18 for run-length
    expansion). A `BitReader` struct reads bits LSB-first with
    bounds-checked byte access. The canonical Huffman decoder (`Huff`)
    reads one bit at a time, matching against per-length first-code
    ranges; over-subscribed and incomplete tables are rejected (Kraft
    over-subscription check, `RFC 1951` §3.2.2 discipline). Back-reference
    copies are byte-wise so overlapping distances (`distance < length`)
    are handled correctly without `copy_within`. Overproduce and
    under-produce are both guarded (`Bad`). The full RFC 1951 §3.2.5
    length/distance base + extra-bit tables are included as `const`
    arrays.
  - **CRC32 verify** (`crc32`): table-driven CRC-32/ISO-HDLC
    (poly `0xEDB88320`, verified against the universal check value
    `crc32(b"123456789") == 0xCBF43926`). Checked after inflate, before
    returning the decompressed bytes.
  - **64 MiB GZIP_MAX_DECOMP cap**: pre-alloc size check (before any
    `Vec` allocation) that returns `Err(PqError::Unsupported(...))` for
    pages exceeding 64 MiB — mirrors `snappy::SNAPPY_MAX_DECOMP`.
- **`meta.rs` `Codec::Gzip`** — new enum variant for Parquet
  `CompressionCodec` id 2. `from_i32(2) => Codec::Gzip`;
  all other mappings unchanged.
- **`lib.rs` `page_payload` Gzip arm** — the single decompression seam
  added a `Codec::Gzip` arm: `gzip::decompress(on_disk, uncomp)?`
  returning `Cow::Owned`. Because every data page (REQUIRED, OPTIONAL,
  dictionary, multi-page, multi-row-group) flows through `page_payload`,
  GZIP composes automatically with dictionary pages, OPTIONAL/def-level
  decoding, and multi-page row groups — no other code path required
  changes.
- **Codec gate flip** — `lib.rs` codec gate changed from
  `Uncompressed | Snappy => {}` to `Uncompressed | Snappy | Gzip => {}`.
  The `Other(_)` arm message remains `"compression codec (zstd/lz4/brotli): OBJ-2c"`.

**Supported matrix after OBJ-2c-1:**

| Axis | Supported |
|---|---|
| Schema shape | Flat (root group + leaves only) |
| Column repetition | REQUIRED or OPTIONAL |
| Compression | UNCOMPRESSED, SNAPPY (raw block; ≤ 64 MiB), or GZIP (RFC 1952; pages ≤ 64 MiB decompressed) |
| Encoding | PLAIN, dictionary (PLAIN_DICTIONARY / RLE_DICTIONARY) |
| Data page version | V1 (`DATA_PAGE`) |
| Null handling | `PqValue::Null` for OPTIONAL def-level 0 rows |

---

## Intended behavior change (disclosed)

The test `extract_rejects_gzip_codec_obj2c` (which asserted that GZIP
codec is rejected with `Unsupported`) is intentionally replaced by:

- `extract_rejects_zstd_codec_obj2c` — ZSTD (codec 6) is still
  `Unsupported(OBJ-2c)`. GZIP (codec 2) is now supported.

All other OBJ-2a / OBJ-2b tests are unchanged. The REQUIRED path is
byte-identical to before; all existing flat REQUIRED and OPTIONAL files
still round-trip. The existing `gzip_plain` / `gzip_dict` /
`gzip_nullable` fixture round-trips are new additions, not replacements.

---

## Verification

- **Hand-derived RFC KATs:**
  - STORED block (`kat_inflate_stored_block`): BFINAL=1, BTYPE=00,
    LEN=5/NLEN=!5, then 5 raw bytes — derived directly from RFC 1951
    §3.2.4, no tool needed.
  - Canonical CRC-32 check value (`kat_crc32_canonical_check_value`):
    `crc32(b"123456789") == 0xCBF43926` — the universal RFC 3309 / zlib
    published check value; independent published authority, non-self-referential.

- **Python stdlib reference vectors** (zlib / gzip = the zlib reference
  C implementation; definitively NOT the code under test):
  - Fixed Huffman (`kat_inflate_fixed_huffman`): `zlib.compressobj`
    level-9, `-15` window (raw DEFLATE), `b"hello world"`.
  - Dynamic Huffman (`kat_inflate_dynamic_huffman`): same compressor,
    `bytes((i*7+3)%251 for i in range(400))`.
  - Overlapping back-reference / RLE (`kat_inflate_overlapping_backref`):
    `b"a" * 8` — a literal plus a distance=1/length>1 back-reference;
    proves byte-wise copy correctness.
  - Full gzip member wrapper (`kat_decompress_gzip_member`):
    `gzip.compress(b"AB")` — proves header parse + DEFLATE + CRC32 +
    ISIZE end-to-end.

- **Real pyarrow fixtures** (pyarrow 24.0.0, metadata-verified GZIP
  before commit):
  - `gzip_dict.parquet` — GZIP + dictionary, `id=[7,7,-2,7,100]`,
    `s=["a","a","b","c","a"]` (flat REQUIRED). Round-trips via
    production `extract()`.
  - `gzip_plain.parquet` — GZIP + PLAIN, same data (flat REQUIRED).
    Round-trips via production `extract()`.
  - `gzip_nullable.parquet` — GZIP + dictionary + OPTIONAL, nullable
    rows: `id=[7,7,null,-2,100]`, `s=["a",null,"b","c","a"]`. Round-trips
    via production `extract()`. **This fixture proves gzip ∘ def-levels
    ∘ dict composition through the page_payload seam** — no special
    wiring; the architecture composes.

- **Source-format-independence pin**
  (`extract_gzip_uncompressed_snappy_identical`): the same logical
  `[I64(7), I64(-2)]` values extracted from a gzip-compressed, an
  uncompressed, and a Snappy-compressed file are byte-identical. The
  same `coerce::to_field_bytes` path is reused — the SP101 invariant
  holds for GZIP.

- **e2e fail-closed oracle**
  (`refresh_gzip_parquet_from_s3_fails_closed_and_state_intact`): GZIP
  Parquet via the `tls_stub_with_fixture` harness (same as SP101/SP104/SP105
  e2e style); REFRESH returns a typed error when the server rejects the
  request; prior materialized data remains intact.

- **Pentest** (18 `catch_unwind` locks — no panic, no OOM, typed errors):
  - Negative locks: over-cap pre-alloc → `Unsupported`; bomb bounded (ISIZE
    inconsistent) → `Bad`; bad magic → `Bad`; CM != deflate → `Unsupported`;
    truncated header (2 bytes) → `Bad`; lying FEXTRA (huge XLEN) → `Bad`;
    unterminated FNAME (no NUL) → `Bad`; truncated DEFLATE → `Bad`; reserved
    BTYPE (11) → `Bad`; bad dynamic Huffman (overrun HLIT+HDIST) → `Bad`;
    distance before output → `Bad`; stored NLEN mismatch → `Bad`; ISIZE
    mismatch → `Bad`; CRC mismatch → `Bad`.
  - Positive correctness locks (assert `Ok` exact): STORED block, fixed
    Huffman, dynamic Huffman, overlapping back-reference (reuse T1 KAT
    vectors); plus a tiny gzip member of the 16-byte PLAIN int64 payload.
  - All 22 pentest results are typed `Err(Bad(...))` / `Err(Unsupported(...))`
    or `Ok(exact_bytes)` — no panic, no stack overflow, no OOM.
- **Lying-compressed-size page_payload-bounds lock**: a page whose
  `compressed_page_size` header field lies beyond the file bounds is
  caught by the existing `page_payload` bounds check before reaching
  `gzip::decompress` — typed `Bad`, not a panic.

---

## Honest gate accounting

Default-build total: **365 → 397** (+32) — new gzip KATs + meta codec
test + extract gzip tests (PLAIN int64 e2e + source-format-indep pin +
repurposed zstd-reject) + fixture roundtrips (gzip_dict + gzip_plain +
gzip_nullable) + e2e fail-closed + 18 gzip pentest locks + the
lying-comp-size lock. **NOT a zero-delta** (same corrected stance as
SP100–105; the per-slice +32 is the authoritative figure per the tracked
nit).

Invariants that hold:
- Deterministic kernel pulls no new external dependency.
- `crates/kessel-parquet/Cargo.toml` `[dependencies]` empty.
- Default `cargo tree -p kesseldb-server` links no
  parquet/objstore/rustls/webpki.
- `large_seed_corpus_is_deterministic_and_converges` green (seed-7).
- EXT/TLS/OBJ-1 oracles (2/1/1) unchanged.
- REQUIRED path byte-unchanged; all OBJ-2a/2b REQUIRED tests pass.
- All OBJ-2b OPTIONAL tests pass unchanged.

---

## Tracked follow-up

The `external_source_parquet_oracle` now has 5 near-identical fail-closed
e2e functions (SP101/SP104/SP105/SP105-nullable/SP106). A dedicated refactor
task should extract a shared `run_fail_closed_parquet_e2e(...)` helper to
reduce duplication. Deferred from OBJ-2c-1 T4 review to preserve the
byte-unchanged invariant for the existing tests; do this at the next e2e
addition (OBJ-2c-2).

---

## Deferred (OBJ-2c-2+)

- **OBJ-2c-2** — ZSTD compression (codec 6).
- **OBJ-2c-3** — V2 data pages (`DATA_PAGE_V2`).
- **OBJ-2c-4** — `INT96` / `FIXED_LEN_BYTE_ARRAY` / `DECIMAL` physical types.
- **OBJ-2c-5** — REPEATED columns / repetition levels (LIST/MAP, nested groups).
- **lz4 / brotli** compression (also OBJ-2c follow-on).
- **GZIP pages > 64 MiB** — the 64 MiB cap mirrors Snappy; lifting it
  requires a streaming decompressor or a tunable cap.
