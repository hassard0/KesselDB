# OBJ-2b-4 — Parquet OPTIONAL/nullable columns (V1 definition levels): Design

**Date:** 2026-05-19
**Status:** Approved (autonomous mandate — see Process Note)
**Subproject:** 105 (capstone sub-slice of the OBJ-2b arc)
**Builds on:** subproject 97/98/99/100/101/102/103/104

## Process Note (autonomy)

Produced under the standing overnight autonomous-build mandate
(`feedback_kesseldb_autonomous_build`): the user directed "keep going"
and is intermittently away. The brainstorming user-review gate is
satisfied by this documented decision record. All other rigor retained:
two-stage subagent review per task (spec then code-quality), a final
whole-implementation review, full `cargo test --workspace --release` +
seed-7 + a pentest pass each kernel-adjacent task. Genuine design
alternatives are recorded with the chosen option and the reason.

## Problem & capstone significance

OBJ-2a→2b-3 read flat **REQUIRED**, UNCOMPRESSED|Snappy, PLAIN|
dictionary, V1 Parquet. But pyarrow makes every column **nullable
(OPTIONAL) by default** — `pq.write_table(df)` with no flags produces
OPTIONAL columns. `extract()` rejects them at `lib.rs:226`
(`if leaf.repetition != Repetition::Required { Unsupported }`). This
slice flips that: with flat OPTIONAL support, KesselDB reads **vanilla
pyarrow output with zero special flags** — the realism capstone of the
OBJ-2b arc.

## Parquet V1 OPTIONAL data-page layout (authority: parquet-format)

For a **flat** leaf (schema = root group + leaves only, no intermediate
groups):

- `max_rep_level = 0` for any non-REPEATED leaf ⇒ **no repetition-level
  bytes** in the V1 page.
- `max_def_level = 0` for REQUIRED, `1` for OPTIONAL.
- A REQUIRED leaf page (`max_def_level == 0`) has **no level bytes** —
  payload is values directly (the OBJ-2a/2b path, unchanged).
- An OPTIONAL leaf page (`max_def_level == 1`) payload is:
  `[definition levels: 4-byte-u32-LE-length-prefixed RLE/bit-packing
  hybrid, bit_width = ceil(log2(max_def_level+1)) = 1, dp_num_values
  entries][values: only for rows whose def-level == 1 (non-null);
  count = #(def==1)]`.
- `dp_num_values` is the **row count including nulls**; `cc.num_values`
  (chunk) is likewise the row count including nulls (so the existing
  multi-data-page accumulation to `cc.num_values` counts rows
  correctly, unchanged).

SP102 already shipped (KAT'd, unwired) exactly the decoder for the
def-level stream:
`rle::decode_level_v1(data, bit_width, num_values) -> Result<(Vec<u64>,
usize /*bytes consumed incl. 4-byte prefix*/), PqError>`.

## Architecture

Decode-only change inside `kessel-parquet`. `meta.rs` learns whether
the schema is flat; `lib.rs` computes per-leaf `max_def_level`, flips
the OPTIONAL gate, and adds a `decode_page` helper that does
def-level-decode + value-decode + null-scatter for OPTIONAL leaves
(REQUIRED path byte-unchanged). No kessel-fetch/kessel-sql/server/
kernel production change. `rle::decode_level_v1` is reused as-is.

### Components

1. **`meta.rs` — flat-schema detection.** The schema element list is
   `[root group (num_children>0)] + leaves`. Currently
   `decode_schema_element` returns `Some(leaf)` for `num_children==0`
   and `None` for any group, so intermediate groups are silently
   dropped (a latent flatten — a leaf nested under an OPTIONAL group
   would get `max_def_level` mis-computed as 1 when it should be 2).
   Change `decode_schema_element` to return an enum
   `SchemaNode::{Leaf(SchemaLeaf), Group{num_children:i32}}`.
   `FileMetaData::decode` then derives
   `pub flat_schema: bool` = `true` iff: exactly one node is a
   `Group` (the root), it is element 0, every other node is a `Leaf`,
   and `root.num_children == leaves.len()`. Otherwise `false`
   (intermediate/nested groups present). `leaves` is still the
   flat `Vec<SchemaLeaf>` as today (no consumer change for the
   leaves themselves).

2. **`lib.rs` — `max_def_level`, gate flip, flat-schema guard.** In
   `extract()`'s per-wanted-leaf resolution (currently `lib.rs:226`):
   - Replace `if leaf.repetition != Required { Unsupported("OPTIONAL/
     REPEATED columns: OBJ-2b") }` with:
     - `Required` → `max_def_level = 0` (unchanged path).
     - `Optional` → require `md.flat_schema` else
       `Unsupported("nested schema: OBJ-2c")`; `max_def_level = 1`.
     - `Repeated` → `Unsupported("REPEATED columns: OBJ-2c")`.
     - `Other(_)` → `Unsupported("unknown repetition: OBJ-2c")`.
   - **Flat-schema guard applies to REQUIRED too** (defense in depth /
     honest correctness): if `!md.flat_schema`, even a REQUIRED leaf
     under a nested group has the wrong implicit level structure;
     reject any non-flat schema with `Unsupported("nested schema:
     OBJ-2c")` regardless of repetition. This *tightens* a latent
     OBJ-2a flat-only assumption (same spirit as the SP104 field-ID
     fix) — disclosed honestly in the record. Existing tests/fixtures
     are all flat (root group + leaves) so they stay green.
   - Thread `max_def_level` to `read_chunk_values` (per wanted column;
     it's `0` or `1`).

3. **`lib.rs` — `decode_page` helper.** The data-page loop in
   `read_chunk_values` currently does, per page,
   `match dp_encoding { 0 => plain::decode_plain(&payload,wp,n),
   2|8 => dict::resolve_dict_indices(&payload,&dict,n), _ =>
   Unsupported }`. Factor that into:
   ```rust
   fn decode_page(
       payload: &[u8],
       dp_encoding: i32,
       wp: meta::Type,
       n: usize,              // dp_num_values = ROW count incl nulls
       max_def_level: u32,    // 0 = REQUIRED, 1 = flat OPTIONAL
       dict: &[PqValue],
   ) -> Result<Vec<PqValue>, PqError>;
   ```
   - `max_def_level == 0`: exactly today's `match dp_encoding` over
     the whole `payload` for `n` values — **byte-for-byte unchanged**
     behavior for every REQUIRED file.
   - `max_def_level == 1`:
     1. `let (defs, consumed) = rle::decode_level_v1(payload, 1, n)?;`
        require `defs.len() == n`.
     2. Validate every `d` in `defs` is `0` or `1`
        (`> max_def_level` ⇒ `Bad("definition level exceeds max")`).
     3. `present = defs.iter().filter(|&&d| d == 1).count()`.
     4. `let body = payload.get(consumed..).ok_or(Bad(...))?;`
     5. Decode exactly `present` values from `body` by `dp_encoding`
        (`0 => decode_plain(body,wp,present)`,
        `2|8 => resolve_dict_indices(body,&dict,present)`,
        else `Unsupported`).
     6. Scatter: build `Vec<PqValue>` of length `n`; walk `defs`,
        `d==1` ⇒ next decoded value, `d==0` ⇒ `PqValue::Null`. The
        present-iterator must be exactly exhausted (count mismatch ⇒
        `Bad("value/def-level count mismatch")`).
     7. Return the length-`n` row vector (nulls in place).
   - The dictionary **page** itself (the dictionary entries) is never
     level-encoded — its decode path is unchanged (REQUIRED-style
     PLAIN over the dict page). Only **data pages** of an OPTIONAL
     leaf carry def-levels.

### Source-format independence (decisive design choice)

`extract()` stays **recipe-agnostic**: it emits `PqValue::Null` for
null rows and does **not** consult any target nullability.
`PqValue::Null` already maps (in `kessel-fetch`, unchanged) via
`pq_to_cell` → `json::Cell::Null` → `coerce::to_field_bytes`, the
*same* path a JSON `null` takes for the same logical column.
Therefore a Parquet-OPTIONAL null and a JSON null for the same column
produce **byte-identical FieldKind bytes** — no `kessel-fetch` change
needed. Pinned by a cross-format test (a JSON source with a `null` vs
a Parquet OPTIONAL source with a def==0 row → identical materialized
field bytes). Rejected alternative: teaching `kessel-parquet` the
recipe's column nullability — that would couple the decoder to the
catalog, break the clean `extract(bytes,&[names])` contract, and
risk a Parquet-vs-JSON divergence. Emitting `PqValue::Null` and
reusing the unchanged coerce is correct and minimal.

## Bold, documented decisions (options → choice)

- **Flat-schema guard for ALL files (not just OPTIONAL).** A nested
  schema mis-computes levels for any leaf; OBJ-2a silently flattened
  it. Enforcing root-group+leaves-only with a typed
  `Unsupported("nested schema: OBJ-2c")` is the honest correctness
  boundary, hardens a latent assumption, and is green for every
  existing flat test/fixture. Rejected: guarding only OPTIONAL (would
  leave the REQUIRED-under-nested-group latent bug live).
- **`max_def_level` hardcoded to 1 for flat OPTIONAL.** It is exactly
  `ceil(log2(1+1)) = 1` for the only OPTIONAL shape this slice
  supports (single-level flat). No general nested-depth computation
  (that is OBJ-2c). Documented as the scope boundary.
- **One `decode_page` helper, REQUIRED path byte-identical.** Rejected
  inlining the OPTIONAL branch into the existing match (would tangle
  level logic with encoding logic and risk perturbing the REQUIRED
  path). The helper isolates the new concern; `max_def_level==0`
  returns the exact prior code path.
- **Reuse `rle::decode_level_v1` unchanged** (SP102, already KAT'd to
  the parquet-format hybrid spec). This slice adds the *null-scatter*
  reconstruction and its KAT; the level-decode primitive is not
  re-implemented or modified.
- **REPEATED and `max_def_level>1` stay `Unsupported(... OBJ-2c)`.**
  Repetition levels / nested optional groups are out of scope; flat
  single-level OPTIONAL only.

## Determinism / invariants

- `decode_page` / flat-schema detection are pure functions of the
  file bytes — deterministic by construction; no clock/env/IO.
- REQUIRED path byte-unchanged ⇒ all OBJ-2a/2b decode + gate tests
  green unchanged (they use flat REQUIRED files; comp==uncomp).
- Source-format independence pinned (Parquet-null FieldKind ==
  JSON-null FieldKind).
- Kernel zero-dep; `kessel-parquet/Cargo.toml` `[dependencies]`
  stays **empty**; default `cargo tree -p kesseldb-server` links no
  parquet/objstore/rustls/webpki; seed-7
  (`large_seed_corpus_is_deterministic_and_converges`) green.
- `#![forbid(unsafe_code)]`; no unwrap/expect/panic/raw-index on
  input bytes (checked `get`/`checked_*`; only the
  statically-infallible 4-byte→`[u8;4]` `try_into().unwrap()`).

## Security posture (pentest — extends SP101 T12 / SP102/103/104)

Lock tests (`catch_unwind`, typed `Err`, no panic/OOM/stack-overflow):

- def-level stream truncated / lying 4-byte length prefix → `Bad`;
- a def-level value `> 1` (> max_def_level) → `Bad`;
- `present` (count of def==1) > `dp_num_values` impossible by
  construction but a crafted stream yielding `defs.len() != n` →
  `Bad`;
- decoded value count ≠ `present` (short value section) → `Bad`;
- OPTIONAL + dictionary with an out-of-range index → `Bad`;
- non-flat schema (intermediate group) → `Unsupported("nested
  schema: OBJ-2c")`, no panic;
- **positive correctness locks (must decode `Ok`, NOT over-reject):**
  all-null page (def-levels all 0, zero value bytes → all
  `PqValue::Null`); all-present page (def-levels all 1 → equivalent
  to REQUIRED-with-a-def-stream, no nulls); a mixed page exercising
  the scatter; an OPTIONAL+dict+Snappy page (the full vanilla-pyarrow
  stack).

## Testing

TDD per task. Layers:

1. **Spec KATs** hand-derived from parquet-format: a flat OPTIONAL
   PLAIN INT64 page, logical `[7, null, -2]` — def-levels `[1,0,1]`
   bit_width 1 → V1 stream `[len u32 LE][0x03,0x05]` (1 bit-packed
   group of 8, LSB-first `1,0,1,0…` ⇒ byte `0x05`), then PLAIN
   `7i64,-2i64`; assert `extract()` → `[[I64(7)],[Null],[I64(-2)]]`.
   Plus an OPTIONAL+dictionary KAT and the all-null / all-present
   edge KATs. (`rle::decode_level_v1` itself is already SP102-KAT'd;
   here the KAT pins the **null-scatter reconstruction**.) `meta.rs`
   flat-schema KAT: flat → `flat_schema==true`; an intermediate
   group → `false`.
2. **Real pyarrow fixtures (the capstone):** `nullable.parquet`
   written by **vanilla** `pq.write_table(df)` (pyarrow default:
   OPTIONAL + dictionary + Snappy) with **actual NULLs**; plus
   `nullable_plain.parquet` (uncompressed, PLAIN, OPTIONAL, with
   NULLs). pyarrow 24.0.0 present; BLOCKED-not-faked if regen fails
   (SP101 T7 stance). Roundtrip asserts `PqValue::Null` at the null
   positions; metadata-verify OPTIONAL + (for the vanilla one)
   SNAPPY+dictionary.
3. **Source-format independence pin** (cross-format): same logical
   nullable column via a JSON source (`null`) vs the Parquet OPTIONAL
   fixture → byte-identical materialized FieldKind (a test-only
   addition in the kessel-fetch/oracle test crate; no production
   change).
4. **e2e:** mirror the SP101/SP103/SP104 `external_source_parquet_
   oracle` harness over `nullable.parquet` (fail-closed, no router
   fixture-trust bypass).
5. **Pentest** lock tests as enumerated.

## Intended behavior change (disclosed — NOT a regression)

`extract_rejects_optional_repetition` (OBJ-2a) asserted OPTIONAL is
rejected. This slice intentionally supports flat OPTIONAL, so that
test is **split/repurposed**: `extract_rejects_repeated_obj2c`
(REPEATED still `Unsupported`) + `extract_decodes_optional_int64_with_
nulls` (positive). Plus a new `extract_rejects_nested_schema_obj2c`
(the flat-schema guard). All other OBJ-2a/2b gate tests
(snappy/gzip/schema-mismatch/missing-column/dict/delta/golden) stay
unchanged and green. Recorded explicitly as a deliberate, reviewed
change. Additionally the flat-schema guard tightens a latent OBJ-2a
nested-schema mis-flatten — disclosed in the record like SP104's
field-ID fix.

## Determinism / invariants gate (every task)

- `cargo test --workspace --release` `FAILED=0`; seed-7 green.
- Honest gate accounting: `kessel-parquet` existing member; new
  meta/optional/fixture/pentest tests **raise the default-build total
  honestly** (baseline 348; no false zero-delta — SP100–104 stance);
  docs task reconciles measured before→after + the intended-change +
  flat-schema-tightening disclosure.
- Kernel zero-dep; deps empty; default `cargo tree` clean; seed-7
  green; existing EXT/TLS/OBJ-1 oracles (2/1/1) + SP101/103/104 e2e
  unchanged; all OBJ-2a/2b REQUIRED decode+gate tests unchanged.

## Out of scope (deferred → OBJ-2c)

- REPEATED columns, repetition levels, nested/optional **groups**
  (`max_def_level > 1`), LIST/MAP logical types.
- gzip/zstd/lz4/brotli, INT96/DECIMAL, V2 data pages, Snappy pages
  > 64 MiB.

After this slice the supported matrix = **flat schema, REQUIRED *or*
OPTIONAL leaves, UNCOMPRESSED|Snappy, PLAIN|dictionary, V1** — i.e.
**vanilla `pq.write_table(df)` with no flags**.

## Task decomposition (one coherent slice)

- **T0** determinism baseline (record 348).
- **T1** `meta.rs`: `SchemaNode` Group/Leaf + `FileMetaData.flat_
  schema`; hand-built thrift KATs (flat→true; intermediate group→
  false; existing leaf decode unchanged).
- **T2** `lib.rs`: gate flip + per-leaf `max_def_level` + flat-schema
  guard + `decode_page` helper (REQUIRED byte-unchanged; OPTIONAL
  def-level decode via `rle::decode_level_v1` + validation + value
  decode over `payload[consumed..]` + null scatter); hand-built
  OPTIONAL PLAIN + OPTIONAL+dict KATs incl. all-null/all-present;
  split/repurpose `extract_rejects_optional_repetition` →
  `extract_rejects_repeated_obj2c` + `extract_decodes_optional_int64_
  with_nulls` + `extract_rejects_nested_schema_obj2c`; all other
  OBJ-2a/2b tests unchanged.
- **T3** real pyarrow fixtures (`nullable.parquet` vanilla default +
  `nullable_plain.parquet`) + roundtrip (PqValue::Null asserted) +
  e2e (SP101 harness, fail-closed) + the cross-format
  source-independence pin.
- **T4** pentest pass (all enumerated hostile vectors + the positive
  all-null/all-present/mixed/dict+snappy correctness locks).
- **T5** docs: `docs/superpowers/specs/2026-05-19-kesseldb-subproject105-optional.md`
  record (SP104 convention exactly) + STATUS row after SP104
  (numeric order, gate numbers + `Record:` backlink + intended-change
  + flat-schema-tightening disclosure) + USAGE §7f note + the
  cumulative Parquet-scope-table update ("REQUIRED or OPTIONAL";
  "vanilla pyarrow default") + gate reconciliation (348 → measured) +
  auto-memory (SP105 block + MEMORY.md line, outside repo, never
  git-add).

## Internal record

`docs/superpowers/specs/2026-05-19-kesseldb-subproject105-optional.md`
at docs time, mirroring the SP104 record convention exactly (KesselDB
H1, `**Status:**` line, bare-backtick-path Builds-on, `---`
separators, honest gate reconciliation, intended-behavior-change +
flat-schema-tightening disclosure, deferred list).
