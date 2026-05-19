# KesselDB — Subproject 104: OBJ-2b-3 Parquet Snappy block decompression

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

Design document:
`docs/superpowers/specs/2026-05-19-parquet-snappy-design.md`

Plan document:
`docs/superpowers/plans/2026-05-19-parquet-snappy.md`

---

## What shipped

`kessel-parquet::extract()` now decodes **Snappy-compressed** flat
REQUIRED, V1 Parquet (dictionary OR PLAIN pages) — pyarrow's true
default `compression='snappy'`:

- `snappy.rs` (new, pure, zero-dep): raw-block Snappy `decompress`
  with a 64 MiB hard decompressed-page cap; overlapping copies done
  byte-by-byte; every length/offset bounds-checked.
- `meta.rs`: `Codec::Snappy` (CompressionCodec id 1).
- `lib.rs`: `page_payload` `Cow` helper — slices the on-disk page by
  `compressed_size` (Uncompressed → borrowed, Snappy → decompressed
  to `uncompressed_size`); the codec gate now accepts
  Uncompressed|Snappy (Other → Unsupported OBJ-2c); the file offset
  advances by `compressed_size` for both the dictionary page and
  every data page (this also corrects a latent OBJ-2a
  compressed==uncompressed assumption — safe because all prior
  fixtures set them equal).

Still rejected with typed errors: OPTIONAL/levels (OBJ-2b-4),
gzip/zstd/lz4/brotli, INT96/DECIMAL, REPEATED/nested, V2 pages, and
Snappy pages above the 64 MiB cap (OBJ-2c).

---

## Latent SP101 bug found & fixed (honest disclosure)

`meta.rs decode_page_header` had read PageHeader thrift field **3 →
uncompressed_size, 4 → compressed_size**, but the published
parquet.thrift is **2: uncompressed_page_size, 3: compressed_page_size,
4: crc**. This was latent since SP101: OBJ-2a/2b advanced the page
offset by `uncompressed_size` and never consumed `compressed_size`,
and the hand-built test page-header encoders were self-consistently
wrong with the decoder — a self-referential blind spot. OBJ-2b-3's
advance-by-`compressed_size` surfaced it (the decoder would have
mis-read `compressed_size` as 0 on real pyarrow files (it was decoding
thrift field 4 = crc, absent ⇒ default 0)). Fixed: decode field 2→uncompressed,
3→compressed (4 crc skipped); the hand-built test encoders corrected
to the true field deltas. Validated **non-self-referentially**: the
real-pyarrow SP101/SP103/SP104 fixtures (written with the true field
IDs) still round-trip through `extract()` only because the decoder now
reads the correct fields. All stale field-ID doc comments corrected.
This is exactly the class of defect the real-fixture + two-stage-gate
discipline exists to catch (cf. SP101's silent-data-corruption catch).

---

## Verification

- Spec KATs hand-derived from google/snappy `format_description.txt`
  (literal; 1/2/4-byte-offset copies; the `"aaaaaa"`
  overlapping-copy RLE lock; multi-byte literal length; malformed →
  Bad; over-cap → Unsupported).
- Real pyarrow 24.0.0 `compression='snappy'` fixtures
  (`snappy_dict.parquet` use_dictionary=True, `snappy_plain.parquet`
  use_dictionary=False, both REQUIRED) — metadata-verified SNAPPY —
  round-trip; e2e via the SP101 oracle harness (fail-closed, no
  router fixture-trust bypass).
- Determinism pin: same logical column is byte-identical `PqValue`
  whether Snappy or uncompressed (`pq_to_cell`/coerce unchanged).
- Pentest: catch_unwind locks (over-cap pre-alloc, decompression
  bomb, preamble mismatch, offset 0 / past output, overproduce,
  literal/offset truncation, trailing-after-full, lying
  compressed_size) → typed errors no panic/OOM; positive
  overlapping-copy correctness lock.

---

## Honest gate accounting

`kessel-parquet` is an existing workspace member; its new tests run
under `cargo test --workspace`. Default-build total: **326 → 348**
(+22) — new snappy/meta/extract/fixture/pentest tests. NOT a
zero-delta (same corrected stance as SP100/101/102/103). Kernel pulls
no new external dependency; `kessel-parquet/Cargo.toml`
`[dependencies]` empty; default `cargo tree -p kesseldb-server` links
no parquet/objstore/rustls/webpki;
`large_seed_corpus_is_deterministic_and_converges` green; existing
EXT/TLS/OBJ-1 oracles (2/1/1) unchanged; all OBJ-2a/2b decode+gate
tests unchanged (they use compressed==uncompressed).

---

## Deferred (next OBJ-2b / OBJ-2c)

- OBJ-2b-4: OPTIONAL/def-levels + nullable wiring + e2e.
- OBJ-2c: gzip/zstd/lz4/brotli, INT96/DECIMAL, REPEATED/nested, V2
  pages, Snappy pages above the 64 MiB cap.
