# OBJ-2c-3 — Parquet V2 data pages (DATA_PAGE_V2): Design

**Date:** 2026-05-19
**Status:** Approved (autonomous mandate — see Process Note)
**Subproject:** 107 (second built sub-slice of the OBJ-2c arc)
**Builds on:** subproject 97/98/99/100/101/102/103/104/105/106

## Process Note (autonomy + resequencing)

Produced under the standing overnight autonomous-build mandate
(`feedback_kesseldb_autonomous_build`): the user directed "keep going"
and is intermittently away. The brainstorming user-review gate is
satisfied by this documented decision record. All other rigor retained:
two-stage subagent review per task (spec then code-quality), a final
whole-implementation review, full `cargo test --workspace --release` +
seed-7 + a pentest pass each kernel-adjacent task.

**Resequencing decision (bold, documented):** SP106's OBJ-2c
decomposition listed OBJ-2c-2 = zstd as next. A zero-dependency zstd
inflater (FSE + Huffman + sequence decoding + the zstd frame format)
is materially larger than gzip's DEFLATE and warrants a dedicated
focused slice. It is **resequenced to remain OBJ-2c-2 (deferred)**.
This slice is **OBJ-2c-3 = V2 data pages** — smaller, self-contained,
high real-world value (modern pyarrow `data_page_version='2.0'`), fits
the one-slice cadence, and reuses the entire shipped decode stack.
Remaining OBJ-2c after this: OBJ-2c-2 zstd, OBJ-2c-4 INT96/DECIMAL,
OBJ-2c-5 REPEATED/nested (incl. V2 repetition levels), lz4/brotli,
>64 MiB pages.

## Problem

SP99–106 made KesselDB read flat REQUIRED|OPTIONAL ×
UNCOMPRESSED|Snappy|GZIP × PLAIN|dict Parquet — but **V1 data pages
only**. `read_chunk_values` (`lib.rs:224`) rejects any non-V1 data
page: `if ph.page_type != 0 { Unsupported("non-V1 data page
(V2/index): OBJ-2c") }`. Modern pyarrow with `data_page_version='2.0'`
(and Spark, increasingly the default) writes `DATA_PAGE_V2`. This slice
flips `page_type == 3` (DATA_PAGE_V2) on for the *same* matrix already
supported in V1.

## V1 vs V2 (authority: `parquet.thrift`)

`PageHeader.data_page_header_v2` is thrift field id **8**;
`DataPageHeaderV2 { 1:i32 num_values, 2:i32 num_nulls, 3:i32 num_rows,
4:Encoding encoding, 5:i32 definition_levels_byte_length, 6:i32
repetition_levels_byte_length, 7:optional bool is_compressed
(default true) }`. `PageType DATA_PAGE_V2 = 3` (V1 `DATA_PAGE = 0`,
`DICTIONARY_PAGE = 2`).

The V2 page byte layout (decisively different from V1):

- V1: the **whole** page payload (levels + values) is one
  compression unit; def-levels are a 4-byte-u32-LE-length-prefixed
  RLE-hybrid stream *inside* the (decompressed) payload.
- V2: the on-disk page = `[repetition levels: rep_len bytes][definition
  levels: def_len bytes][values: compressed_page_size − rep_len −
  def_len bytes]`. The **level sections are ALWAYS stored RAW
  (uncompressed)** with their byte-lengths in the header (NOT
  length-prefixed); only the **values** section is compressed (per
  `is_compressed`, default true), decompressing to
  `uncompressed_page_size − rep_len − def_len` bytes. V2 def-levels
  are RLE-hybrid but **not** 4-byte-prefixed (the length is `def_len`).
- For a flat leaf: `rep_len == 0` (max_rep_level 0, non-REPEATED);
  `num_rows == num_values`; `num_nulls == #(def < max_def_level)`.

## Architecture

Decode-only change inside `kessel-parquet`, reusing every shipped
primitive (`rle::decode_hybrid`, `plain::decode_plain`,
`dict::resolve_dict_indices`, `snappy::decompress`,
`gzip::decompress`). The V1 decode path stays **byte-identical**.

### Components

1. **`meta.rs` — field-8 `DataPageHeaderV2` decode.** Add a field-8
   nested-struct arm mirroring the existing field-5 (`DataPageHeader`)
   / field-7 (`DictionaryPageHeader`) pattern *with the SP101
   per-struct `last_id` save/reset/restore bracketing*. Extend
   `PageHeader` with: `v2_num_values: i32`, `v2_num_nulls: i32`,
   `v2_encoding: i32` (default −1), `v2_def_len: i32`,
   `v2_rep_len: i32`, `v2_is_compressed: bool` (default **true** if
   field 7 absent). `page_type == 3` is the V1-vs-V2 discriminator.
   `v2_num_rows` is decoded for completeness but a flat leaf uses
   `v2_num_values` as the row count. V1 fields (`dp_num_values`,
   `dp_encoding`, `page_type`, sizes, dict fields) unchanged → V1
   decode byte-identical.

2. **`lib.rs` — V2 decode path + shared null-scatter.** In the
   `read_chunk_values` data-page loop, replace the
   `if ph.page_type != 0 { Unsupported }` gate with:
   - `page_type == 0` → the **existing V1 path** (unchanged: the
     existing `page_payload` whole-page decompress → `decode_page`).
   - `page_type == 3` → a new `decode_data_page_v2(...)`.
   - else (`1` INDEX_PAGE, other) → `Unsupported("non-V1/V2 data
     page (index): OBJ-2c")`.
   `decode_data_page_v2(region, ph, codec, want_ptype, max_def_level,
   &dict) -> Result<Vec<PqValue>, PqError>` where `region` is the raw
   on-disk `file[dstart..dstart+compressed_size]` slice (V2 does
   **not** go through `page_payload` — the decompression boundary is
   inside the page, not the whole page):
   - `rep_len = usize::try_from(ph.v2_rep_len)` (checked); if
     `rep_len > 0` → `Unsupported("REPEATED/nested V2: OBJ-2c-5")`
     (flat non-REPEATED ⇒ pyarrow writes 0).
   - `def_len = usize::try_from(ph.v2_def_len)` (checked);
     require `rep_len.checked_add(def_len)` exists and `≤
     region.len()` else `Bad`.
   - `levels = &region[rep_len .. rep_len+def_len]` (RAW — never
     decompressed); `values_section = &region[rep_len+def_len ..]`.
   - `n = usize::try_from(ph.v2_num_values)` (the row count incl
     nulls).
   - def-levels: if `max_def_level == 1` →
     `defs = rle::decode_hybrid(levels, 1, n)?` (require
     `defs.len() == n`; reject any `d > 1` → `Bad`); `present =
     defs.iter().filter(|&&d| d==1).count()`. Defense-in-depth: if
     `ph.v2_num_nulls >= 0`, require `present == n -
     usize::try_from(ph.v2_num_nulls)?` else `Bad("V2 num_nulls vs
     def-levels mismatch")` (mirrors SP104's ISIZE cross-check
     stance). If `max_def_level == 0` → `def_len` must be 0, `present
     = n`, `defs` implicitly all-present (no def stream / no
     scatter).
   - values target length: `vt = uncompressed_page_size − rep_len −
     def_len` (checked: `uncompressed_page_size ≥ rep_len+def_len`;
     `vt ≤ 64 MiB` — the existing decompress caps apply via the
     codec functions). `values_raw`:
     `Uncompressed` **or** `!v2_is_compressed` → `values_section`
     (must be exactly `vt` bytes — check); `Snappy` →
     `snappy::decompress(values_section, vt)`; `Gzip` →
     `gzip::decompress(values_section, vt)`; `Other(_)` →
     `Unsupported("compression codec (zstd/lz4/brotli): OBJ-2c")`.
   - decode `present` values from `values_raw` by `ph.v2_encoding`:
     `0` (PLAIN) → `plain::decode_plain(values_raw, want_ptype,
     present)`; `2|8` (PLAIN_DICTIONARY/RLE_DICTIONARY) →
     `dict::resolve_dict_indices(values_raw, &dict, present)`;
     else → `Unsupported("data page encoding ...: OBJ-2c")`.
   - if `max_def_level == 1`: null-scatter `vals` against `defs`
     (d==1 → next value, d==0 → `PqValue::Null`); else return `vals`
     directly. The scatter is the **exact same logic** the V1
     OPTIONAL arm uses — factor it: extract the V1 `decode_page`
     scatter into `fn scatter_nulls(defs:&[u64], vals:Vec<PqValue>,
     n:usize) -> Result<Vec<PqValue>,PqError>` and call it from
     **both** the V1 OPTIONAL arm and the V2 path. The V1
     REQUIRED/OPTIONAL observable behavior MUST stay byte-identical
     (the factored fn is the same code moved, no logic change).
   - the `decode_data_page_v2` result has length `n`; the
     multi-page accumulation to `cc.num_values` is unchanged (V2
     `num_values` is the row count incl nulls, exactly like V1
     `dp_num_values`).

3. **Dictionary page unchanged.** Even in V2 files pyarrow writes a
   **V1-style `DICTIONARY_PAGE`** (`page_type == 2`,
   `DictionaryPageHeader`, whole-page compressed) followed by
   `DATA_PAGE_V2` data pages. The existing dict-page handling
   (`lib.rs:187`, whole-page `page_payload` decompress) is correct and
   stays unchanged. V2 + dict: after the V2 level-split +
   values-decompress, `values_raw` is the bit-width-byte +
   RLE-hybrid dict-index stream → `dict::resolve_dict_indices` exactly
   as V1 dict.

### Bold, documented decisions (options → choice)

- **Dedicated `decode_data_page_v2` (not branch inside `decode_page`
  or `page_payload`).** The V2 decompression boundary is *inside* the
  page (levels raw, values compressed to a different target length);
  forcing it through the whole-page `page_payload` seam would require
  contorting that helper. A sibling fn keeps the V1 path provably
  byte-identical and isolates the V2 split. Rejected: a `payload`
  variant returning two slices (more API churn, leaks V2 structure
  into the seam).
- **Factor `scatter_nulls` shared by V1-OPTIONAL and V2.** The
  null-scatter is identical; duplicating it would risk drift between
  the two paths (a real correctness hazard). One source of truth;
  V1 behavior byte-identical (same code, relocated).
- **`num_nulls` defense-in-depth cross-check.** V2 carries
  `num_nulls` explicitly; cross-checking it against the decoded
  def-level present-count (Bad on mismatch) is the "build for safety
  / strong solution" stance (mirrors SP104's ISIZE check). Rejected
  trusting one source silently.
- **e2e-helper extraction as this slice's T1.** The SP106 review
  explicitly named the trigger: the `external_source_parquet_oracle`
  now has 5 near-identical fail-closed e2e fns and this slice adds a
  6th. Doing the extraction as a dedicated **behavior-preserving
  refactor** first task (extract `run_fail_closed_parquet_e2e`,
  rewrite the 5 call-sites, gate green, observable assertions
  preserved — verified by the spec reviewer as NOT an intended
  behavior change) keeps the byte-unchanged concern manageable; the
  V2 e2e then just adds a 6th call. Chosen over deferring again (the
  trigger is unambiguous and the scoped-task approach is clean).
- **`is_compressed == false`**: the values section is raw even when
  the chunk codec is Snappy/GZIP (V2 per-page opt-out). Handled
  explicitly (raw `values_section` when `!v2_is_compressed`).

## Source-format independence

A V2 page and a V1 page of the **same logical data** decode to
byte-identical `PqValue` → identical `pq_to_cell`/`coerce` FieldKind
bytes. Pinned: pyarrow writes the same logical column once with
`data_page_version='1.0'` and once with `'2.0'`; `extract()` returns
an identical `Vec<Vec<PqValue>>`. The shared `scatter_nulls` and the
unchanged downstream guarantee this structurally.

## Security posture (pentest — extends SP101 T12 / SP104/105/106)

`catch_unwind`, typed `Err`, no panic/OOM/stack-overflow:

- lying `def_len`/`rep_len` (sum > `compressed_size`, or
  individually huge) → `Bad`, no OOB slice;
- `uncompressed_page_size < rep_len + def_len` (underflow of `vt`)
  → `Bad`;
- `rep_len > 0` → `Unsupported("REPEATED/nested V2: OBJ-2c-5")`,
  no panic;
- a def-level value `> 1` → `Bad`;
- `present` (from def-levels) ≠ `num_values − num_nulls` → `Bad`;
- values section length ≠ `vt` when uncompressed/`!is_compressed`
  → `Bad`;
- V2 + Snappy/GZIP with a corrupt values section → the codec's
  existing typed `Bad`/cap, no panic/OOM (cap still 64 MiB);
- truncated V2 page (region shorter than rep_len+def_len) → `Bad`;
- **positive correctness locks (assert `Ok`):** V2 PLAIN REQUIRED;
  V2 PLAIN OPTIONAL with nulls (scatter); V2 + dict; V2 +
  Snappy/GZIP (per-section decompression composes); `is_compressed
  == false` under a Snappy/GZIP chunk codec → raw values decode.

## Testing

TDD per task. Layers:

1. **Spec KATs** — hand-built V2 page files (mirroring the existing
   SP104/SP105 hand-builders) derived from `parquet.thrift`: a V2
   PLAIN REQUIRED INT64 (`def_len=0`, `rep_len=0`), a V2 PLAIN
   OPTIONAL `[7,null,-2]` (`def_len` = the RLE-hybrid bytes for
   `[1,0,1]` bit_width 1, NOT 4-byte-prefixed; `num_nulls=1`), and a
   V2 + dict. Reviewer independently re-derives the thrift field-8
   bytes + the non-prefixed def-level layout.
2. **Real pyarrow `data_page_version='2.0'` fixtures** —
   `v2_plain.parquet`, `v2_dict.parquet`, `v2_nullable.parquet`, and
   a `v2_gzip.parquet` (or snappy) to prove per-section
   decompression. **Metadata-verify the data page is genuinely
   `DataPageHeaderV2`** (not silently V1) before any Rust test runs;
   BLOCKED-not-faked if regen fails / isn't V2 (SP101 T7 stance).
   pyarrow 24.0.0 present.
3. **V2-vs-V1 source-independence pin** — same logical column written
   `'1.0'` and `'2.0'` → identical `extract()` output.
4. **e2e** — a 6th `external_source_parquet_oracle` case over
   `v2_dict.parquet`, via the **T1-extracted** shared helper
   (fail-closed, no router fixture-trust bypass).
5. **Pentest** lock tests as enumerated.

## Determinism / invariants gate (every task)

- `cargo test --workspace --release` `FAILED=0`; seed-7
  (`large_seed_corpus_is_deterministic_and_converges`) green.
- Honest gate accounting: `kessel-parquet` existing member; new
  meta/v2/fixture/pentest tests **raise the default-build total
  honestly** (baseline = measured post-SP106 397, recorded in Task 0;
  no false zero-delta — SP100–106 stance; the per-slice +DELTA is the
  authoritative figure per the tracked nit). T1 is a
  behavior-preserving refactor → net-0 test-count, all 5 e2e green
  with preserved observable assertions (disclosed as a refactor, not
  a behavior change).
- Kernel pulls no new external dependency;
  `kessel-parquet/Cargo.toml` `[dependencies]` stays **empty**;
  default `cargo tree -p kesseldb-server` links no
  parquet/objstore/rustls/webpki.
- `#![forbid(unsafe_code)]`; no unwrap/expect/panic/raw-index on
  input bytes (checked `get`/`checked_*`; only the
  statically-infallible fixed-size slice→`[u8;N]` `try_into().unwrap`).
- Existing oracles green unchanged: `external_source_oracle` (2),
  `external_source_tls_oracle` (1), `external_source_objstore_oracle`
  (1); SP101/103/104/105/106 e2e cases preserved (refactored through
  the T1 helper, observable behavior identical); all V1 OBJ-2a/2b/2c-1
  decode+gate tests byte-unchanged.

## Out of scope (deferred → later OBJ-2c)

- zstd (OBJ-2c-2), INT96/DECIMAL (OBJ-2c-4), REPEATED/nested incl.
  V2 repetition levels `rep_len>0` (OBJ-2c-5), lz4/brotli, pages
  > 64 MiB decompressed. Non-flat schema still rejected by the
  unchanged `md.flat_schema` guard.

After this slice the supported matrix = **flat REQUIRED|OPTIONAL ×
UNCOMPRESSED|Snappy|GZIP × PLAIN|dict × V1 *and* V2 data pages**.

## Task decomposition (one coherent slice)

- **T0** determinism baseline (record measured post-SP106 397).
- **T1** extract `run_fail_closed_parquet_e2e(fixture, tag, keyid,
  secret, source)` in `external_source_parquet_oracle.rs`; rewrite
  the 5 existing call-sites (SP101/103/104/105/106) to use it;
  behavior-preserving (each test's observable
  SchemaError/empty-state assertions identical); gate green. A
  reviewed refactor, NOT a behavior change.
- **T2** `meta.rs` field-8 `DataPageHeaderV2` decode + the new
  `PageHeader` V2 fields (`v2_*`); hand-built thrift KAT (page_type 3
  + field-8 fields decode; V1 page_type-0/2 unchanged).
- **T3** `lib.rs` `decode_data_page_v2` + the shared `scatter_nulls`
  factoring + flip the `page_type==3` gate; hand-built V2 KAT files
  (PLAIN REQUIRED, PLAIN OPTIONAL `[7,null,-2]`, V2+dict); the
  V2-vs-V1 determinism pin; ALL V1 OBJ-2a/2b/2c-1 tests
  byte-unchanged.
- **T4** real pyarrow `data_page_version='2.0'` fixtures (plain,
  dict, nullable, gzip-or-snappy) — metadata-verified
  `DataPageHeaderV2` — + roundtrips + the 6th e2e via the T1 helper.
- **T5** pentest pass (all enumerated hostile vectors + the positive
  V2 PLAIN/OPTIONAL/dict/compressed/`is_compressed=false` locks).
- **T6** docs: `docs/superpowers/specs/2026-05-19-kesseldb-subproject107-v2pages.md`
  record (SP106 convention exactly, incl. the resequencing note +
  the T1 refactor disclosure) + STATUS row after SP106 (numeric
  order, gate numbers + `Record:` backlink) + USAGE §7f note +
  cumulative Parquet-scope-table update (Data-page-version row →
  "V1 and V2 (DATA_PAGE_V2)") + gate reconciliation + auto-memory
  (SP107 block + MEMORY.md line, outside repo, never git-add).

## Internal record

`docs/superpowers/specs/2026-05-19-kesseldb-subproject107-v2pages.md`
at docs time, mirroring the SP106 record convention exactly (KesselDB
H1, `**Status:**` line, bare-backtick-path Builds-on, `---`
separators, honest gate reconciliation, the resequencing +
T1-e2e-helper-refactor disclosures, deferred list).
