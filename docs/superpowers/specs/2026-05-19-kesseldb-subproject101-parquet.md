# KesselDB — Subproject 101: Parquet Object Sources (OBJ-2a)

**Date:** 2026-05-19  **Status:** done — code + tests committed and passing.

Builds on:
- Subproject 97 — External sources (EXT slice 1):
  `docs/superpowers/specs/2026-05-18-external-sources-design.md`
- Subproject 98 — External sources: pagination + NDJSON:
  `docs/superpowers/specs/2026-05-18-external-sources-pagination-design.md`
- Subproject 99 — External sources: HTTPS/TLS:
  `docs/superpowers/specs/2026-05-18-external-sources-tls-design.md`
- Subproject 100 — Object-store external sources (OBJ-1):
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject100-objstore.md`

Design document:
`docs/superpowers/specs/2026-05-19-parquet-object-source-design.md`

---

## What shipped

### `kessel-parquet` — new workspace-member crate

A new pure-Rust crate with zero external dependencies. It exposes a
single public API:

```rust
pub enum PqValue { Null, Bool(bool), I64(i64), F64(f64), Bytes(Vec<u8>) }
pub enum PqError { Bad(String), Unsupported(String) }  // + Display
pub fn extract(bytes: &[u8], wanted: &[&str])
    -> Result<Vec<Vec<PqValue>>, PqError>;
```

The crate has five source files, each owning one concern:

**`src/thrift.rs`** — Thrift Compact Protocol reader: varint (unsigned
and zigzag-signed), field-delta headers, wire-type dispatch for the
types Parquet metadata uses (bool/i32/i64/binary/list/struct). Provides
a `skip` function (with a recursion-depth cap — see Security posture)
for advancing past unrecognised fields.

**`src/footer.rs`** — `PAR1` magic header + trailing
`[u32 LE metadata_len][PAR1]` framing. Enforces hard size-sanity bounds
(`metadata_len` must fit inside the file with 12 bytes of framing
overhead; absurd values are rejected before any allocation).

**`src/meta.rs`** — `FileMetaData` structs decoded via `thrift.rs`:
schema elements (`SchemaElement` with `Type`, `RepetitionType`, name,
type-length, num_children), row groups, column chunks, column metadata
(`ColumnMetaData` with Encoding, CompressionCodec, data-page offset,
num-values), and `DataPageHeader`. Also includes the `PageType` enum.
The decoder maps the flat schema's leaf names to column-chunk
descriptors and validates the OBJ-2a support matrix before any page is
read (see below).

**`src/plain.rs`** — PLAIN page decoder per physical type:
`BOOLEAN` (1-bit packed, LSB-first within each byte), `INT32`/`INT64`
(little-endian), `FLOAT`/`DOUBLE` (little-endian IEEE-754),
`BYTE_ARRAY` (4-byte LE length prefix + bytes) → `Vec<PqValue>`. Every
offset and length is bounds-checked against the slice; a malformed file
yields `PqError::Bad`, never a panic.

**`src/lib.rs`** — `extract` orchestration: footer → metadata → for
each row group, for each wanted column, locate the column chunk, seek
to the data-page, call the PLAIN decoder, assemble a `Vec<Vec<PqValue>>`
in the caller's `wanted` order; arity/row-count consistency check across
row groups.

### Thrift per-struct `last_id` correctness fix

The initial Thrift reader shared a single `last_id` accumulator across
the entire decode of a `FileMetaData` message, so field-delta values
were computed against the last field number of the *previous* struct
rather than `0` (the correct base for each new struct). This caused
multi-struct decodes (`ColumnMetaData` following `SchemaElement`, etc.)
to misread field IDs and either skip fields or decode the wrong value.
The fix resets `last_id` to `0` at the start of each struct decode.
This is a correctness fix for the published Thrift Compact Protocol
spec, not a security issue.

### Recursion-depth cap on Thrift `skip`

The `skip` function for advancing past unrecognised fields is
recursive over struct nesting. A hostile file with deeply nested structs
would produce unbounded recursion leading to a stack overflow. A hard
depth limit (`MAX_SKIP_DEPTH = 64`) was added; exceeding it yields
`PqError::Bad("thrift skip depth exceeded")`.

### Schema / chunk physical-type strict guard

The initial implementation read the wanted column's physical type from
the schema element and used it to dispatch the PLAIN decoder, but did
not cross-check that the column's `ColumnMetaData.type_` (the chunk's
declared type) matched the schema declaration. A hostile file could
declare `INT64` in the schema but write `BYTE_ARRAY` bytes in the chunk;
the decoder would then interpret the `BYTE_ARRAY` 4-byte length prefix
as an INT64 LE value — a silent data-corruption vector. The fix adds a
strict guard: `schema_type == chunk_type` is checked before any page
decode; a mismatch yields `PqError::Bad("column type mismatch between
schema and chunk metadata")`.

### Pentest-fixed remote OOM / DoS

`PLAIN BYTE_ARRAY` decoding reads a 4-byte LE length prefix per value
and then allocates a buffer of that size. A hostile file can set
`num_values` to a large number while providing far fewer actual bytes.
The initial code allocated `Vec::with_capacity(num_values)` before
reading any elements, so a file with `num_values = 2^30` would cause a
multi-GB allocation before the first read attempt. The fix bounds the
pre-allocation: `count.min(data.len())` — the allocation is capped to
the number of bytes actually available, which is always a valid upper
bound on how many 4-byte-minimum elements can exist. The
`vec_with_capacity_bounds_hostile_count` pentest locks this property.

### `kessel-fetch` `object-store` feature extension

The `object-store` feature (already in `kessel-fetch`) gains
`dep:kessel-parquet`. New in `kessel-fetch`:

- **`Format::Parquet` variant** — the fourth body format alongside
  `Json`, `Csv`, `Ndjson`.
- **`rows_from_body` Parquet arm** — calls `kessel_parquet::extract`
  with the recipe-mapped column source names, then maps each row through
  `pq_to_cell`.
- **`pq_to_cell(PqValue) -> json::Cell`** — maps a Parquet physical
  value to the same `Cell` representation the JSON path produces for the
  same logical value: `Null→Cell::Null`, `Bool→Cell::Bool`,
  `I64→Cell::Text(itoa)`, `F64→Cell::Text(canonical_f64)`,
  `Bytes→Cell::Text(utf8-lossy)`. The existing `coerce::to_field_bytes`
  path is reused byte-for-byte unchanged — no new coercion logic, no new
  determinism surface.
- **`canonical_f64`** — formats an `f64` identically to how JSON
  numbers are serialized (the same formatting the JSON decoder would
  produce for the equivalent value). This function has a unit test that
  runs in the default build.

`kessel-parquet` does not depend on `kessel-fetch`; the `PqValue→Cell`
mapping lives in `kessel-fetch` to avoid a dependency cycle.

### `do_refresh` and `do_refresh_objstore` — format code 3

Both `do_refresh` (HTTP path) and `do_refresh_objstore` (S3/Azure path)
already switch on `recipe.format` (0 Json / 1 Csv / 2 Ndjson). OBJ-2a
adds `3 => Format::Parquet`. The HTTP path (`do_refresh`) maps format 3
to an immediate `SchemaError("FORMAT PARQUET is only supported for
object-store sources (s3:// or az://)")` — fail-closed before any
fetch. The S3/Azure path (`do_refresh_objstore`) routes format 3 through
the `rows_from_body(Format::Parquet, …)` arm, then through the
unchanged `materialize_external_rows` / atomic `Op::Txn` upsert.

### `kessel-sql` grammar — OBJ-1 `FORMAT PARQUET` rejection flipped

The SQL parser previously rejected `FORMAT PARQUET` for all sources at
`CREATE` time with "FORMAT PARQUET over object store is OBJ-2 (not yet
shipped)". OBJ-2a flips this:

- `FORMAT PARQUET` with an `s3://` or `az://` URL: **accepted**.
- `FORMAT PARQUET` with an `http://` or `https://` URL: **rejected** at
  `CREATE` with "FORMAT PARQUET is only supported for object-store
  sources (s3:// or az://); use FORMAT JSON/CSV/NDJSON for HTTP(S)".
- `FORMAT PARQUET` combined with `PAGE` or `ROWS`: **rejected** at
  `CREATE` with "PAGE and ROWS are not applicable to FORMAT PARQUET
  sources".
- Iceberg, prefix-listing, and STS/SAS/IMDS rejections: **unchanged**.

### Feature-gated fail-closed e2e oracle

`cargo test -p kesseldb-server --features external-sources-objstore
--test external_source_parquet_oracle`

The test points the production router at an `s3://` source with
`FORMAT PARQUET` backed by a stub HTTPS server. The oracle asserts that
`REFRESH` returns an appropriate error (the stub server does not present
a webpki-trusted certificate) and that prior materialized data is intact.
The trusted happy-path for `FORMAT PARQUET` decode is covered at the
`kessel-fetch` layer (feature-gated `tests/parquet_decode.rs` over the
real pyarrow fixture files).

---

## Known-answer vectors / fixture provenance

### Thrift Compact Protocol KATs

| Primitive | Independent authority | Byte vector |
|---|---|---|
| Varint (unsigned) | Thrift Compact Protocol spec §3.1: "the standard variable-length integer encoding, MSB first with the low 7 bits of each byte carrying data" (continuation bit in the MSB of each byte; value bits are LSB-first in byte order — i.e. standard LEB128/protobuf varint) — hand-computed for values 0, 1, 127, 128, 16384 | `[0x00]`, `[0x01]`, `[0x7F]`, `[0x80, 0x01]`, `[0x80, 0x80, 0x01]` |
| Zigzag varint (signed) | Thrift spec §3.1 zigzag mapping `n → (n<<1)^(n>>63)` — hand-computed for 0, -1, 1, -64, 63 | as specified |
| Field-delta header (type 5 = i32, delta 1) | Thrift spec §3.2: `(delta << 4) | wire_type` — hand-computed | `0x15` |
| Binary field (type 8) | Thrift spec §3.3 — hand-computed for `b"hello"` | `[0x18, 0x05, b'h', b'e', b'l', b'l', b'o']` |

### Parquet footer KAT

| Component | Independent authority | Value |
|---|---|---|
| `PAR1` magic | Apache Parquet format spec §1: "Files start and end with a 4-byte magic number `PAR1`" | `b"PAR1"` |
| Footer framing | Parquet spec §1: `[PAR1][row groups][metadata_len u32 LE][PAR1]` | hand-computed for a 6-byte metadata blob: `b"PAR1" ++ b"\x06\x00\x00\x00" ++ b"PAR1"` with the blob in the middle |
| Size-sanity rejection | Derived from the framing spec: a `metadata_len` that would extend beyond the file after accounting for 12 bytes of framing must be rejected | verified: `metadata_len = file_len - 12 + 1` ⇒ `PqError::Bad` |

### PLAIN decoder KATs

| Physical type | Authority | Test vector |
|---|---|---|
| `INT32` LE | Parquet format spec §1.4 "PLAIN encoding for INT32 is a sequence of 4-byte LE integers" | `[0x01, 0x00, 0x00, 0x00]` → 1; `[0xFF, 0xFF, 0xFF, 0xFF]` → -1 (as i32 → widened to i64) |
| `INT64` LE | Same spec, 8-byte LE | `[0x01, 0x00, ..., 0x00]` → 1 |
| `FLOAT` | IEEE-754 binary32 LE | `[0x00, 0x00, 0x80, 0x3F]` → 1.0f32 |
| `DOUBLE` | IEEE-754 binary64 LE | hand-computed 8-byte vector → 1.0f64 |
| `BOOLEAN` | Parquet spec: 1-bit packed, LSB-first | `[0b00001101]` → `[true, false, true, true, false, false, false, false]` |
| `BYTE_ARRAY` | Parquet spec: 4-byte LE length + bytes | `[0x03, 0x00, 0x00, 0x00, b'f', b'o', b'o']` → `"foo"` |

### Real fixture files (non-self-referential)

| File | Producer | Contents |
|---|---|---|
| `crates/kessel-parquet/tests/fixtures/flat_required.parquet` | pyarrow 24.0.0, `use_dictionary=False`, `compression="none"`, `data_page_version="1.0"` | 3 rows × 3 columns (`id: INT64`, `value: INT64`, `label: BYTE_ARRAY`), 1 row group |
| `crates/kessel-parquet/tests/fixtures/flat_multirg.parquet` | Same pyarrow settings | 6 rows × same schema, 3 row groups (2 rows each) |

The fixtures are non-self-referential because every PLAIN decode primitive
is independently pinned to hand-computed spec-derived KATs above. pyarrow
re-reads both fixture files via its own independent implementation and
asserts the same row counts/values, providing a second independent
read-back.

---

## Tests and which build each runs in

### Default build (`cargo test --workspace --release`)

- **`kessel-parquet` unit tests (KAT)** — all of `thrift.rs`, `footer.rs`,
  `plain.rs`, `meta.rs` primitives pinned to the public spec byte vectors.
- **`kessel-parquet` fixture tests** — `extract` over `flat_required.parquet`
  and `flat_multirg.parquet` (single- and multi-row-group paths).
- **`kessel-parquet` pentest / adversarial tests:**
  - `hostile_count_does_not_oom` — `BYTE_ARRAY` page with `num_values` ≫
    data length; asserts `PqError::Bad`, no OOM.
  - `hostile_metadata_len_rejected` — absurd `metadata_len` in the footer;
    asserts `PqError::Bad`.
  - `hostile_deep_skip_rejected` — struct nesting beyond `MAX_SKIP_DEPTH`;
    asserts `PqError::Bad`.
  - `type_mismatch_schema_vs_chunk_rejected` — schema says INT64, chunk
    metadata says BYTE_ARRAY; asserts `PqError::Bad` (no silent decode).
- **`kessel-fetch` `canonical_f64` test** — verifies the float-formatting
  function against known values (runs in the default build, not
  feature-gated).
- **`kessel-sql` 2 new parquet-parse tests** — `FORMAT PARQUET` with
  `s3://` accepted at `CREATE`; `FORMAT PARQUET` with `https://` rejected
  with the correct error.

### Feature-on: `--features external-sources-objstore`

- **`kessel-fetch` `tests/parquet_decode.rs`** — `rows_from_body(
  Format::Parquet, …)` over the fixture files; asserts correct rows,
  canonical float rendering, and `pq_to_cell` coerce path.
- **`external_source_parquet_oracle`** — fail-closed e2e (described above).

---

## Honest gate accounting: 267 → 293 (+26)

**The delta is NOT zero — the design document anticipated this explicitly.**

The design notes: "`kessel-parquet` is a workspace member ⇒ its unit tests
run under `cargo test --workspace`; the default-build total rises honestly
(not a zero-delta — Task 13 reconciles README/STATUS to the measured
number with the real reason, exactly as SP100 did)."

The 26 new tests that appear in the default-build total break down as:

1. **`kessel-parquet` — KAT/unit/fixture/pentest tests** — all new; this
   is a new workspace member with no prior baseline. These tests cover
   the Thrift Compact Protocol reader, the footer parser, the PLAIN
   decoder (all six physical types), the `FileMetaData` decoder, and the
   adversarial / pentest cases (OOM-DoS fix, deep-skip cap, schema/chunk
   type-mismatch guard, hostile footer bounds).
2. **`kessel-fetch` — `canonical_f64` default test** — 1 test. This
   function formats f64 values identically to JSON numbers; the test
   runs in the default build (the function is not feature-gated).
3. **`kessel-sql` — 2 new Parquet parse tests** — FORMAT PARQUET +
   `s3://` accepted; FORMAT PARQUET + `https://` rejected. Both compile
   in the default build (the SQL parser's feature-independent layer).

**Measured total: `cargo test --workspace --release` ⇒ TOTAL=293, seed-7
(`large_seed_corpus_is_deterministic_and_converges`) green, no REALFAIL.**

**The invariants that DO hold (these are the correct claims):**

- The deterministic kernel, WAL, `kessel-sm`, `kessel-vsr`, `kessel-io`,
  `kessel-codec`, and the core of `kessel-proto`/`kessel-catalog` are
  byte-identical and pull zero new dependencies in the default build.
- `cargo tree -p kesseldb-server -e normal` and
  `cargo tree -p kessel-fetch -e normal` show no rustls, webpki,
  objstore, or parquet entries in the default build graph — both return
  `DEFAULT CLEAN`.
- Feature-OFF Parquet code is not compiled into the default binary.
- seed-7 (`large_seed_corpus_is_deterministic_and_converges`) is green.
- Default-build total: **293** (measured; seed-7 green; no REALFAIL).

---

## Security posture

**`#![forbid(unsafe_code)]`** in `kessel-parquet`. No unsafe code anywhere
in the new crate.

**Every offset and length bounds-checked.** Every read into a byte slice
is preceded by a bounds check against the slice length. A malformed file
yields a typed `PqError::Bad` or `PqError::Unsupported` — never a panic,
never an index-out-of-bounds abort, never an OOM.

**Demonstrated and fixed remote OOM / DoS (pentest).** During internal
review, a hostile `BYTE_ARRAY` page with `num_values = 2^30` (a value
plausible in a small Parquet file header) caused `Vec::with_capacity(
num_values)` to attempt a multi-GB allocation before reading a single
byte. The fix: `count.min(data.len())` caps the pre-allocation to the
number of bytes actually present in the data slice — always a valid upper
bound. The `hostile_count_does_not_oom` pentest locks this property.

**Silent-data-corruption vector closed.** The schema/chunk physical-type
strict guard (`schema_type == chunk_type`) was added after it was
observed that a hostile file could declare one type in the schema
(`FileMetaData.schema`) and a different type in `ColumnMetaData.type_`.
Without the guard, the decoder would use the schema type to dispatch the
PLAIN decoder but read bytes laid out for the chunk type — producing
garbage values with no error. The `type_mismatch_schema_vs_chunk_rejected`
pentest locks the rejection.

**Recursion cap on `skip`.** The Thrift `skip` function is recursive over
struct nesting. A hard depth limit (`MAX_SKIP_DEPTH = 64`) prevents a
stack overflow on hostile deeply-nested Parquet metadata.

**Fail-closed on any `PqError`.** `PqError` → `kessel-fetch` maps it to
`FetchError::Parse(…)` → `do_refresh_objstore` returns
`OpResult::SchemaError("refresh: …")` and submits nothing to the
replicated log. Prior materialized rows are intact (all-or-nothing
abort, unchanged from OBJ-1).

**Error messages are clean.** Only the parse reason and column name appear
in `PqError` strings — never object bytes, credential values, or
secrets.

**HTTPS-only, no bypass.** Parquet objects are fetched via the same
`kessel-fetch` object-store machinery as OBJ-1: rustls + webpki roots,
full certificate and hostname verification, no bypass flag.

**Secret-reference only.** The OBJ-1 secret-handling invariant is
unchanged: only env-var NAME strings are in the catalog/WAL/op;
values resolved at REFRESH never appear in logs, errors, or digest
output.

---

## Determinism boundary

Parquet decode is pure: given the same captured object bytes and the
same `wanted` column list, `kessel_parquet::extract` returns the
same `Vec<Vec<PqValue>>`, deterministically. The captured bytes are
the router's fetched object body — the same captured-once/replicate
boundary as JSON/CSV/NDJSON and the OBJ-1 SigV4/Azure timestamp.

`pq_to_cell` renders each `PqValue` to the same `Cell` the JSON path
produces for the equivalent logical value:
- `PqValue::I64(n)` → `Cell::Text(itoa(n))` — same as a JSON integer.
- `PqValue::F64(f)` → `Cell::Text(canonical_f64(f))` — the same
  formatting JSON numbers use (matches the `canonical_f64` unit test).
- `PqValue::Bytes(b)` → `Cell::Text(utf8-lossy(b))` — same as a JSON
  string field.
- `PqValue::Bool(v)` → `Cell::Bool(v)` — same as a JSON boolean.

These `Cell` values then enter `coerce::to_field_bytes` — the
**identical** path the JSON decoder uses. A source that happens to
produce the same logical values in both JSON and Parquet will yield
byte-identical `FieldKind` bytes after coercion, and therefore an
identical `ObjectId` and an identical `Op::Txn` payload. The
replicated-log entry and the WAL/digest output are source-format-
independent for the same logical row set.

---

## Deferred follow-ons

### OBJ-2b — dictionary / RLE + Snappy + OPTIONAL columns

- `PLAIN_DICTIONARY` and `RLE_DICTIONARY` encoding (the dictionary
  page decode + index page decode + value materialisation).
- `RLE`-encoded definition levels for `OPTIONAL` columns (allowing
  nullable fields in Parquet schemas).
- Snappy block decompression (hand-rolled, no codec crate).

Currently rejected at `REFRESH` with `Unsupported("… : OBJ-2b")`.

### OBJ-2c — gzip / zstd + INT96 / DECIMAL + nested-skip + V2 pages

- Gzip and Zstd column-chunk decompression.
- `INT96` physical type rendering (typically a timestamp — logical-type
  annotation needed to interpret it).
- `FIXED_LEN_BYTE_ARRAY` / `DECIMAL` logical-type niceties.
- Nested-column skip (for non-wanted nested groups in a non-flat schema).
- Parquet V2 data pages (`DATA_PAGE_V2` with the different header
  layout and optional repetition/definition level encodings).

Currently rejected at `REFRESH` with `Unsupported("… : OBJ-2c")`.

### OBJ-3 — Iceberg table manifests

`FORMAT ICEBERG`: resolve the current snapshot from a table metadata
JSON file, enumerate manifest files, enumerate data file paths, fetch
and decode each Parquet data file. Depends on OBJ-2.
Rejected at `CREATE` with a clear error message.

### OBJ-4 — Prefix / multi-object listing

Allow a source URL to be a prefix (`s3://bucket/prefix/`) and have
`REFRESH` enumerate all matching objects, fetch each, and materialize
the union. Rejected at `CREATE` (`PAGE` on object-store sources).

### OBJ-5 — STS / SAS / IMDS credential providers

AWS STS session tokens (assumed-role, web identity), Azure SAS tokens,
AWS IMDS / Azure IMDS workload-identity credential resolution.
Rejected at `CREATE`.

### Task-9 M3: `build_cols` / `resolve_format` DRY

The `build_cols` and `resolve_format` logic is partially duplicated
between `do_refresh` and `do_refresh_objstore`. A follow-on can factor
these into a shared helper, reducing the maintenance surface.

### Residual (accepted for OBJ-2a, not a bug)

`BYTE_ARRAY` values are decoded via a per-element copy: each element
requires a `Vec<u8>` allocation bounded by the 4-byte length prefix in
the Parquet page. For a row group containing many large binary columns,
the total allocation is bounded by the fetched object body, which is in
turn bounded by `DEFAULT_MAX_BODY` (64 MiB by default). This is
acceptable for OBJ-2a; a zero-copy `Bytes`-slice approach is an
efficiency follow-on once the overall Parquet pipeline is stable.

### Carried EXT/TLS/OBJ deferrals (still open from prior slices)

- **Unify `fetch_rows_paginated` decode tail** (SP98/SP99 carry):
  the paginated path has an inline decode+coerce tail that duplicates
  the logic in `rows_from_body`.
- **Trusted multi-page HTTPS test** (SP99 carry): paginated TLS test is
  fail-closed only; a trusted multi-page happy-path test is outstanding.
- **`test_config_trusting` visibility** (SP99 carry): currently `pub`;
  could be narrowed to `pub(crate)`.
- **Gitleaks allow-list for test key fixture** (SP99 carry): the
  `localhost.key.pem` fixture will trip secret scanners if CI secret
  scanning is added.
