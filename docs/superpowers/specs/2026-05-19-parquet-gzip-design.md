# OBJ-2c-1 — Parquet GZIP page decompression (RFC 1952 + RFC 1951 inflate): Design

**Date:** 2026-05-19
**Status:** Approved (autonomous mandate — see Process Note)
**Subproject:** 106 (first sub-slice of the OBJ-2c arc)
**Builds on:** subproject 97/98/99/100/101/102/103/104/105

## Process Note (autonomy)

Produced under the standing overnight autonomous-build mandate
(`feedback_kesseldb_autonomous_build`): the user directed "keep going"
and is intermittently away. The brainstorming user-review gate is
satisfied by this documented decision record. All other rigor retained:
two-stage subagent review per task (spec then code-quality), a final
whole-implementation review, full `cargo test --workspace --release` +
seed-7 + a pentest pass each kernel-adjacent task. Genuine design
alternatives are recorded with the chosen option and the reason.

## Problem & where this sits

The OBJ-2b arc (SP99–105) shipped: KesselDB reads vanilla
`pq.write_table(df)` (flat REQUIRED|OPTIONAL, UNCOMPRESSED|Snappy,
PLAIN|dictionary, V1). The remaining Parquet frontier is **OBJ-2c**.
`gzip` is the #2 Parquet compression after Snappy (Spark/Hive/older
tooling default; pyarrow `compression='gzip'`); `read_chunk_values`'
codec gate (`lib.rs:154`) and the `page_payload` Cow helper
(`lib.rs:50`) currently reject any non-`Uncompressed|Snappy` codec with
typed `Unsupported`. This slice flips GZIP on.

## OBJ-2c arc decomposition (documented; this doc designs sub-slice 1)

OBJ-2c is too large for one spec. Sub-slices, each its own spec→plan→
build cycle:

| Sub-slice | Scope |
|---|---|
| **OBJ-2c-1 (SP106, this doc)** | GZIP page decompression (RFC 1952 wrapper + RFC 1951 inflate), zero-dep |
| OBJ-2c-2 | zstd page decompression (zero-dep RFC 8878 inflate — large; its own slice) |
| OBJ-2c-3 | V2 data pages (`DATA_PAGE_V2` header: separately-stored rep/def level byte-lengths) |
| OBJ-2c-4 | INT96 / DECIMAL / other logical–physical types |
| OBJ-2c-5 | REPEATED / nested / LIST / MAP (repetition levels + Dremel record assembly) — a mini-arc |
| deferred | lz4 / brotli (rare in Parquet), >64 MiB pages |

Sequencing rationale: gzip first — highest realism value of the
remaining items, fully self-contained, hand-rollable zero-dep from
published RFCs, and mirrors the **proven SP104 Snappy slice template
exactly** (new pure module + `Codec` variant + the single
`page_payload` decompression seam + real-pyarrow fixtures + pentest +
docs), so it composes automatically with dictionary, OPTIONAL/
def-levels, and multi-page (all already routed through `page_payload`).

## Architecture

Decode-only change inside `kessel-parquet`, isomorphic to SP104:

1. **`crates/kessel-parquet/src/gzip.rs`** (new, pure, zero external
   deps; `#![forbid(unsafe_code)]` crate-wide; `#![allow(dead_code)]`
   header like siblings):
   ```rust
   /// Hard cap on a single decompressed Parquet page (mirrors
   /// snappy::SNAPPY_MAX_DECOMP; same value & rationale, separate
   /// const so gzip.rs stays self-contained / zero cross-module
   /// coupling — the established sibling-module convention).
   pub(crate) const GZIP_MAX_DECOMP: usize = 64 << 20; // 64 MiB

   /// Decompress one Parquet GZIP page (RFC 1952 member: 0x1f 0x8b,
   /// CM=8, FLG-gated optional fields, RFC 1951 DEFLATE body, CRC32
   /// + ISIZE trailer). `expected_len` is the page header's
   /// uncompressed_page_size (the authority). Bounded, bounds-checked
   /// everywhere; never panics / OOM-aborts / stack-overflows
   /// (iterative inflate, no recursion).
   pub fn decompress(src: &[u8], expected_len: usize)
       -> Result<Vec<u8>, PqError>;
   ```
   Internals:
   - **RFC 1952 wrapper:** require `src[0]==0x1f && src[1]==0x8b`
     (else `Bad("gzip magic")`); `CM` (`src[2]`) `==8` else
     `Unsupported("gzip method != deflate: OBJ-2c")`; read `FLG`
     (`src[3]`); skip `MTIME/XFL/OS` (6 bytes); then FLG-gated,
     fully bounds-checked: `FEXTRA` (2-byte LE `XLEN` + `XLEN`
     bytes), `FNAME` (NUL-terminated), `FCOMMENT` (NUL-terminated),
     `FHCRC` (2 bytes). The DEFLATE stream begins right after. The
     last 8 bytes of `src` are the trailer: `CRC32` (4 LE) + `ISIZE`
     (4 LE). `ISIZE != (expected_len as u32)` → `Bad` (defense in
     depth; the page header is the alloc authority — mirrors SP104).
   - **RFC 1951 inflate** — LSB-first bit reader over the DEFLATE
     bytes (between wrapper end and the 8-byte trailer); loop on
     blocks: read `BFINAL` (1 bit) + `BTYPE` (2 bits):
     - `00` stored: align to byte, read `LEN`/`NLEN` (2 LE each),
       require `NLEN == !LEN`, copy `LEN` raw bytes.
     - `01` fixed Huffman: the RFC §3.2.6 fixed literal/length code
       lengths + the 5-bit fixed distance codes.
     - `10` dynamic Huffman: `HLIT`(5)+257, `HDIST`(5)+1,
       `HCLEN`(4)+4; read `HCLEN` 3-bit code-length-code lengths in
       the RFC permutation order; build the code-length canonical
       Huffman; decode `HLIT+HDIST` code lengths (with repeat codes
       16/17/18 and their extra bits, bounds-checked against the
       total); build the literal/length and distance canonical
       Huffman trees.
     - `11` → `Bad("deflate reserved block type")`.
     Symbol loop: literal `<256` → push; `256` → end of block;
     `257..=285` → length (base+extra-bits per RFC §3.2.5) then a
     distance symbol → distance (base+extra-bits) → an LZ77
     back-reference: require `1 <= distance <= out.len()`; copy
     `length` bytes **byte-by-byte** from `out[out.len()-distance]`
     (overlapping back-references `distance < length` are legal &
     common — same correctness trap as Snappy's overlapping copy;
     handled byte-wise). `BFINAL==1` ends the stream.
   - **Canonical Huffman decode — decided approach (bold but
     disciplined):** per-symbol *bit-at-a-time* canonical decoding
     (RFC §3.2.2: from the code lengths compute `bl_count` →
     `next_code` first-code-per-length → walk input bits accumulating
     a code, matching it against the per-length code ranges). Chosen
     over the speed-optimized lookup-table decoder: it is directly
     auditable, every step bounds-checkable, zero-dep, and adequate
     for page-sized data. Rejected the table decoder for OBJ-2c-1
     (harder to bounds-prove, premature optimization for the
     external-source read path); a future OBJ-2c task may add the
     table fast-path behind the same function signature.
   - **CRC32 verification — decided (build for safety, strong
     solution):** compute CRC-32/ISO-HDLC (poly `0xEDB88320`, a
     256-entry table built once at call time; ~15 lines) over the
     inflated output and require it `== trailer CRC32` else
     `Bad("gzip crc mismatch")`. Rejected skipping CRC (relying only
     on ISIZE + structural validation): the mandate is explicit —
     "strong solutions, don't take the easy way out, build for
     safety". CRC catches silent corruption the structural checks
     miss, costs ~15 lines and one O(n) pass, and a mismatch is a
     clean typed `Bad`. Documented as a deliberate
     defense-in-depth choice.
   - **Bomb / OOM cap (mirrors SP104 exactly):**
     `expected_len > GZIP_MAX_DECOMP` → `Unsupported("gzip page
     {n} exceeds {cap} cap: OBJ-2c")` **before any allocation**;
     `Vec::with_capacity(expected_len)` provably ≤ 64 MiB; the
     inflate loop refuses to push past `expected_len` (overproduce →
     `Bad`); at end `out.len() == expected_len` else `Bad`. Inflate
     is iterative (no recursion) — no stack-overflow vector.

2. **`meta.rs` `Codec`:** add `Gzip`; `from_i32`: `0=>Uncompressed,
   1=>Snappy, 2=>Gzip, o=>Other(o)` (parquet `CompressionCodec`
   `GZIP=2`).

3. **`lib.rs::page_payload`** (the single decompression seam,
   `lib.rs:50`): add a `meta::Codec::Gzip => Cow::Owned(gzip::
   decompress(on_disk, uncomp)?)` arm next to the Snappy arm; the
   codec gate at `lib.rs:154` adds `Gzip` to the accepted match
   (`Uncompressed | Snappy | Gzip => {}`); `Other(_)` still
   `Unsupported("compression codec (zstd/lz4/brotli): OBJ-2c")`.
   **Nothing downstream changes** — decompressed bytes feed the same
   PLAIN/dict/def-level `decode_page` path, so GZIP composes
   automatically with dictionary, OPTIONAL/def-levels and multi-page,
   and the dictionary **page** (also gzip-compressed when
   `codec=GZIP`) flows through the same `page_payload`. This
   single-seam elegance is why the slice is small and low-risk.

## Intended behavior change (disclosed — NOT a regression)

`lib.rs:601 extract_rejects_gzip_codec_obj2c` currently asserts GZIP
(codec 2) → `Unsupported`. This slice intentionally **supports** GZIP,
so that test is **repurposed**: `extract_rejects_zstd_codec_obj2c`
asserting codec `6` (ZSTD) → `Unsupported("compression codec
(zstd/lz4/brotli): OBJ-2c")` (still unsupported). All other OBJ-2a/2b
codec/decode/gate tests stay unchanged & green. Recorded explicitly in
the SP106 record + STATUS + memory (same disclosure discipline as
SP103's dict-reject repurpose and SP105's optional-reject split).

## Source-format independence

gzip-decompressed bytes feed the *same* `decode_page` →
`plain::decode_plain`/`dict::resolve_dict_indices`/def-level scatter →
`PqValue` → `pq_to_cell` → `coerce` path, unchanged. Same logical data
stored gzip vs uncompressed vs Snappy → byte-identical FieldKind bytes.
Pinned by a test (a hand-built gzip-wrapped PLAIN page decodes to the
exact same rows as the existing uncompressed/Snappy equivalents) and by
the real-pyarrow gzip fixture roundtrip matching the Snappy/plain
fixtures' logical rows.

## Security posture (pentest — extends SP101 T12 / SP104 / SP105)

`catch_unwind`, typed `Err`, no panic/OOM/stack-overflow:

- `expected_len > GZIP_MAX_DECOMP` → `Unsupported` **before alloc**;
- decompression bomb: tiny `src`, expected_len ≤ cap but the inflate
  legitimately can't reach it → `Bad`, no multi-GB alloc;
- bad magic / `CM != 8` / truncated 10-byte header → `Bad`/`Unsupported`;
- lying/oversized `FEXTRA XLEN`, unterminated `FNAME`/`FCOMMENT`,
  truncated trailer → `Bad`;
- truncated DEFLATE mid-block / mid-symbol → `Bad`;
- reserved `BTYPE==11` → `Bad`;
- malformed dynamic-Huffman (code-length sequence overruns
  `HLIT+HDIST`, an over-subscribed/incomplete Huffman) → `Bad`;
- distance back-reference with `distance == 0` or `distance >
  out.len()` (before-output-start / beyond window) → `Bad`;
- stored block `NLEN != !LEN` → `Bad`;
- ISIZE mismatch → `Bad`; CRC32 mismatch → `Bad`;
- **positive correctness locks (assert `Ok`, NOT error):** an
  overlapping back-reference (`distance < length`, RLE expansion)
  decodes correctly; a stored-block stream; a fixed-Huffman stream; a
  dynamic-Huffman stream; the full vanilla-pyarrow gzip+dict (and
  gzip+OPTIONAL) stack.

## Testing

TDD per task. Layers:

1. **Spec KATs** hand-derived from RFC 1952 + RFC 1951 (the
   independent authority; reviewer re-derives): a gzip member with a
   **stored** DEFLATE block wrapping known bytes; a **fixed-Huffman**
   gzip of a short string; a **dynamic-Huffman** gzip of a short
   string; an **overlapping back-reference** (RLE) case; CRC32 of a
   known vector; an ISIZE-mismatch → Bad; over-cap → Unsupported.
   These pin both the RFC 1952 wrapper parse and the RFC 1951 inflate
   independently of any pyarrow output.
2. **Real pyarrow `compression='gzip'` fixtures** (the realism win):
   `gzip_dict.parquet` (pyarrow default `use_dictionary=True`),
   `gzip_plain.parquet` (`use_dictionary=False`), and
   `gzip_nullable.parquet` (vanilla nullable — proves gzip ∘
   def-levels ∘ dict composition through `page_payload`). pyarrow
   24.0.0 present; BLOCKED-not-faked if regen fails or a column is
   not GZIP (SP101 T7 stance). Roundtrip via production `extract()`;
   metadata-verify codec==GZIP.
3. **Determinism / source-independence pin:** a hand-built
   gzip-wrapped PLAIN INT64 page decodes to the exact rows the
   existing uncompressed (`build_parquet_file`) and Snappy
   (`build_snappy_plain_int64_file`) builders produce.
4. **e2e:** mirror the SP104/SP105 `external_source_parquet_oracle`
   harness over `gzip_dict.parquet` (fail-closed, no router
   fixture-trust bypass).
5. **Pentest** lock tests as enumerated.

## Determinism / invariants gate (every task)

- `cargo test --workspace --release` `FAILED=0`; seed-7
  (`large_seed_corpus_is_deterministic_and_converges`) green.
- Honest gate accounting: `kessel-parquet` existing member; new
  gzip/meta/page/fixture/pentest tests **raise the default-build total
  honestly** (baseline = the measured post-SP105 total recorded in
  Task 0; no false zero-delta — SP100–105 stance); the docs task
  reconciles measured before→after with the real reason + the
  intended-change disclosure. (Per the SP100–105 tracked nit: the
  per-slice +DELTA is the authoritative figure; absolute baseline is
  whatever Task 0 measures via `cargo test --workspace --release`.)
- Kernel pulls no new external dependency;
  `kessel-parquet/Cargo.toml` `[dependencies]` stays **empty**;
  default `cargo tree -p kesseldb-server` links no
  parquet/objstore/rustls/webpki.
- `#![forbid(unsafe_code)]`; no unwrap/expect/panic/raw-index on input
  bytes (checked `get`/`checked_*`; only the statically-infallible
  fixed-size slice→`[u8;N]` `try_into().unwrap()` for `from_le_bytes`);
  inflate iterative — no recursion.
- Existing oracles green unchanged: `external_source_oracle` (2),
  `external_source_tls_oracle` (1), `external_source_objstore_oracle`
  (1); SP101/103/104/105 e2e unchanged; all OBJ-2a/2b decode+gate
  tests unchanged except the intentionally-repurposed gzip-reject
  test.

## Out of scope (deferred → later OBJ-2c sub-slices)

zstd/lz4/brotli (OBJ-2c-2/deferred); INT96/DECIMAL (OBJ-2c-4);
DATA_PAGE_V2 (OBJ-2c-3); REPEATED/nested/LIST/MAP (OBJ-2c-5); pages
> 64 MiB decompressed. After this slice the supported matrix =
**flat schema, REQUIRED|OPTIONAL, UNCOMPRESSED|Snappy|GZIP,
PLAIN|dictionary, V1**.

## Task decomposition (one coherent slice)

- **T0** determinism baseline (record the measured post-SP105 total).
- **T1** `gzip.rs`: RFC-1952 wrapper parse + RFC-1951 inflate
  (stored/fixed/dynamic, byte-wise overlapping back-ref) + CRC32 +
  `GZIP_MAX_DECOMP` cap; spec KATs hand-derived from the RFCs
  (stored, fixed-Huffman, dynamic-Huffman, overlapping back-ref,
  CRC, ISIZE-mismatch→Bad, over-cap→Unsupported).
- **T2** `meta.rs`: `Codec::Gzip` (`from_i32` 2); KAT (codec 2 →
  `Gzip`; codec 6 → `Other(6)`; existing 0/1 unchanged).
- **T3** `lib.rs`: `page_payload` `Gzip` arm + codec-gate flip;
  repurpose `extract_rejects_gzip_codec_obj2c` →
  `extract_rejects_zstd_codec_obj2c`; a hand-built gzip-wrapped
  PLAIN-page decode test + the gzip-vs-uncompressed-vs-snappy
  determinism pin; all other OBJ-2a/2b tests unchanged.
- **T4** real pyarrow `compression='gzip'` fixtures
  (`gzip_dict`/`gzip_plain`/`gzip_nullable`) + roundtrips + e2e
  (SP104/105 harness, fail-closed).
- **T5** pentest pass (all enumerated hostile vectors + the positive
  stored/fixed/dynamic/overlapping/vanilla-stack locks).
- **T6** docs: `docs/superpowers/specs/2026-05-19-kesseldb-subproject106-gzip.md`
  record (SP105 convention exactly) + STATUS row after SP105
  (numeric order, gate numbers + `Record:` backlink + the intended
  GZIP-now-supported disclosure) + USAGE §7f note + cumulative
  Parquet-scope-table update (Compression row → "UNCOMPRESSED,
  SNAPPY, or GZIP (RFC 1952; pages ≤ 64 MiB decompressed)") + gate
  reconciliation + auto-memory (SP106 block + MEMORY.md line, outside
  repo, never git-add).

## Internal record

`docs/superpowers/specs/2026-05-19-kesseldb-subproject106-gzip.md` at
docs time, mirroring the SP105 record convention exactly (KesselDB
H1, `**Status:**` line, bare-backtick-path Builds-on, `---`
separators, honest gate reconciliation, the GZIP-now-supported
intended-behavior-change disclosure, deferred list).
