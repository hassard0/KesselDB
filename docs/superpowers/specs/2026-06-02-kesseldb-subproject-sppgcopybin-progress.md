# SP-PG-COPY-BIN — PostgreSQL `COPY ... WITH (FORMAT binary)` — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T3 (2026-06-02).** Real psql 16.14
smoke on vulcan: `CREATE TABLE` + INSERT seed + `COPY bin_smoke TO
STDOUT WITH (FORMAT binary) > /tmp/binary-export.bin` (89 bytes; canonical
`PGCOPY\n\xff\r\n\0` signature + 3 length-prefixed binary rows + `\xff\xff`
EOD marker) + `COPY bin_smoke2 FROM STDIN WITH (FORMAT binary)` returns
COPY 3 + follow-up SELECT returns the same row set + re-export
byte-equal (`md5sum` match `d4df79da25448ee783bbe6ce0caad181`). NUMERIC
columns pre-rejected at COPY-start with precise `0A000` + SP-PG-COPY-BIN-NUMERIC
pointer; session stays alive. TaskList #360 ready for completion.

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgcopybin-design.md`
Smoke transcript: `docs/superpowers/sppgcopybin-t3-smoke-2026-06-02.txt`

Parent SP-arc: SP-PG-COPY (V1 closed 2026-05-30, text) + SP-PG-COPY-CSV
(V1 closed 2026-06-01, CSV). Both progress trackers named SP-PG-COPY-BIN
as the deferred V2 follow-up for `WITH (FORMAT binary)` — this is the
realisation.

## What this SP-arc ships

V1 = "`pg_dump --format=custom` restore + JDBC binary `CopyManager` +
every binary-format ETL bulk-loader work against KesselDB." After V1
(T1..T4), a PG client speaking the protocol can:

1. Send `Q` with `COPY <table> FROM STDIN WITH (FORMAT binary)`; server
   replies `CopyInResponse` (`G`) with overall format byte = 1 (binary)
   + per-column format codes = 1.
2. Stream `CopyData` (`d`) frames containing: (first frame's prefix
   bytes) 19-byte PG binary header — `PGCOPY\n\xff\r\n\0` signature +
   4-byte BE flags (V1: 0) + 4-byte BE header extension length (V1: 0)
   — then per-row records: 2-byte BE i16 field count, per-field 4-byte
   BE i32 length (`-1` = NULL), then `length` bytes of binary-encoded
   value. Records can span multiple CopyData frames via the carry
   buffer.
3. End with `CopyDone` (`c`) → server emits `CommandComplete` (`COPY N`)
   + `ReadyForQuery` (`Z 'I'`).
4. OR send the optional `\xff\xff` end-of-data marker followed by
   CopyDone (V1 tolerates both).
5. Send `Q` with `COPY <table> TO STDOUT WITH (FORMAT binary)`; server
   emits `CopyOutResponse` (`H`) with format=1 + first CopyData with the
   19-byte signature header + N × CopyData with per-row binary records
   + CopyData with the `\xff\xff` end-of-data marker + CopyDone +
   CommandComplete(`COPY N`) + RFQ.
6. Coexist text + CSV + binary COPY arbitrarily on the same connection
   (each Q dispatch returns the connection to the Idle copy state).

**Out-of-scope (named, deferred — each is its own arc):**

- **SP-PG-COPY-BIN-NUMERIC (V2)** — binary NUMERIC encoding (variable-
  length base-10000 digits + sign + scale). The most complex per-type
  binary representation in the PG wire protocol. V1 pre-rejects NUMERIC
  / I128 / U128 / Fixed columns at COPY-start with precise V2-arc-pointing
  `0A000` messages.
- **SP-PG-COPY-BIN-OID (V2)** — the optional OID-column flag bit
  (header flag bit 16). PG ≤11 `WITH OIDS` tables — legacy. V1 rejects
  non-zero header flags with precise `0A000`.
- **SP-PG-COPY-BIN-EXTRA (V2)** — binary UUID / JSONB / ARRAY. Same V2
  surface as SP-PG-EXTQ-BIN-EXTRA (the param + result + COPY-binary paths
  support the same OID set by construction).
- **SP-PG-COPY-BIN-DIRECT (V2)** — bypass the per-value binary→text
  round trip with typed parameter binding (5-10× throughput win for
  binary-heavy workloads).

## Slice plan (mirrors design spec §7)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1+T2** | Design spec (~454 LoC, 8 weak-spots + 3 open questions + 4 V2 follow-up arcs named) + `copy::binary` module (`PG_BINARY_SIGNATURE` constant + `BinaryDecoder` streaming parser + `encode_binary_header/_row/_end_of_data` helpers — 28 KATs) + `copy::CopyFormat::Binary` variant + `is_binary()` selector + `CopyInState::binary_header_consumed` flag + `command::parse_with_options` flip from `Rejected(BinaryFormat)` to `Ok(CopyFormat::Binary)` + flipped parser KAT + `proto::encode_copy_in_response_binary` / `_out_response_binary` (format=1, 3 KATs) + `dispatch::dispatch_copy_in_start` NUMERIC pre-reject + binary CopyInResponse emit + `dispatch::process_copy_data` binary branch (decodes binary → text → BULKAPPLY V1 fold) + `dispatch::dispatch_copy_to` binary branch (binary `H` + signature header CopyData + per-row binary CopyData + EOD CopyData + CopyDone + COPY N tag + RFQ) + flipped `server::run_session` test. | **DONE** | `c523339` (+1705 LoC, +31 KATs) |
| **T3** | Real psql 16.14 smoke on vulcan + USAGE update. CREATE TABLE + INSERT 3 rows + COPY TO STDOUT binary → file (89 bytes, canonical wire shape verified via `hexdump`) + COPY FROM STDIN binary into fresh table → COPY 3 + SELECT * shows the same rows + re-export byte-equal (md5sum match). NUMERIC pre-reject verified with precise V2-pointing message + session-alive check. | **DONE** | `8519902` (smoke + USAGE) |
| **T4** | Arc closure — STATUS.md row + progress tracker → CLOSED + TaskList #360 ready. | **DONE** (this commit) | (final docs commit) |

## T1+T2 — what landed (2026-06-02, commit `c523339`)

**One commit, +1705 LoC across 7 files:**

### Design spec (`docs/superpowers/specs/2026-06-02-kesseldb-sppgcopybin-design.md`, 454 LoC):

- **§1 Context** — the failing `pg_dump --format=custom` /
  `pg_basebackup` / `pg_bulkload` / `pgloader` / JDBC
  `CopyManager.copyIn(PGCopyOutputStream)` workflows captured against
  pre-arc KesselDB; 6-row ecosystem table.
- **§2 Scope** — V1 in (8 items: COPY FROM/TO binary + 10 type codec
  reuse from SP-PG-EXTQ-BIN + signature/flags/extension parsing/emission
  + EOD marker + connection state machine + 16 MiB carry cap + streaming
  per-row processing + zero-dep stance); V1 out (4 named V2 arcs).
- **§3 Binary codec module** — `BinaryDecoder` API (Header / Body /
  EndOfData states, `consume_header` + `next_row`); `encode_binary_*`
  helpers.
- **§4 State machine + integration** — `CopyFormat::Binary` variant +
  `CopyInState::binary_header_consumed` flag; parser flip; dispatch
  branches; binary-variant `encode_copy_in_response_binary` /
  `_out_response_binary`.
- **§5 Memory + flow bounds** — 16 MiB inherited; header extension
  capped at the same.
- **§6 Error semantics** — 8-row SQLSTATE table covering bad signature
  (`08P01`), unsupported flags (`0A000`), oversized header extension
  (`08P01`), field count mismatch (`22023`), bad field length (`08P01`),
  truncated (`22023`), NUMERIC at COPY-start (`0A000` +
  SP-PG-COPY-BIN-NUMERIC), unsupported OID (`0A000` +
  SP-PG-COPY-BIN-EXTRA).
- **§7 Task decomposition** — T1+T2 / T3 / T4 with KAT-delta estimates.
- **§8 Acceptance criteria** — 9 items including byte-equal psql
  round-trip + session alive + format coexistence + zero-dep invariant
  + surface-byte-untouched.
- **§9 Self-review — 8 weak spots**: per-value text round trip cost
  (V2 SP-PG-COPY-BIN-DIRECT); binary header inside first CopyData
  (V1 carry handles); end-of-data marker tolerance (V1 tolerant); NUMERIC
  rejection placement at COPY-start vs at-row-time; no streaming of
  100 MiB+ blobs; per-column re-encoding cost on COPY TO; PG-reserved
  flag bits 1-15; no partial-COPY recovery semantics.
- **§10 Out-of-scope hard passes** — PROGRAM (permanent); FILE
  (operator opt-in only).
- **§11 Open questions** — per-column format codes (V1: all 1 for
  binary); NoticeResponse for NUMERIC rejection (V1: hard fail
  matching PG); per-value V1 vs V2 DIRECT.

### Code (`crates/kessel-pg-gateway/src/copy/binary.rs`, ~770 LoC):

- **`PG_BINARY_SIGNATURE`** — 11-byte canonical constant
  `b"PGCOPY\n\xff\r\n\0"`.
- **`PG_BINARY_END_OF_DATA = -1`** — 2-byte i16 EOD marker constant.
- **`BinaryDecoder<'a>`** — streaming decoder with `BinaryState`
  lifecycle (Header → Body → EndOfData). `consume_header` validates
  signature + flags + extension area length; `next_row` parses per-row
  records; returns `Ok(None)` for partial input (caller carries) and
  `Err(...)` for malformed.
- **`BinaryDecodeError`** — 6-variant error enum: BadSignature,
  UnsupportedFlags, HeaderExtensionTooLarge, FieldCountMismatch,
  BadFieldLength, Truncated.
- **`encode_binary_header() -> Vec<u8>`** — emits the canonical 19-byte
  header (signature + 0 flags + 0 extension length).
- **`encode_binary_end_of_data() -> Vec<u8>`** — emits the 2-byte
  `\xff\xff` EOD marker.
- **`encode_binary_row(values) -> Vec<u8>`** — emits a single row's wire
  shape (2-byte field count + per-field 4-byte length + binary bytes).

### KATs (`copy::binary` — 28):

- `t1_signature_constant_byte_locked` — `PG_BINARY_SIGNATURE` exact
  11-byte equality.
- `t1_encode_binary_header_byte_locked` — 19-byte canonical header.
- `t1_encode_binary_end_of_data_byte_locked` — `\xff\xff`.
- `t1_encode_binary_row_single_int8_byte_locked` — single-column INT8
  row exact wire bytes.
- `t1_encode_binary_row_int8_and_text` — multi-column row.
- `t1_encode_binary_row_null_column_byte_locked` — NULL = `-1` length,
  no value bytes.
- `t1_encode_decode_round_trip_mixed_null` — encode + decode identity.
- `t1_consume_header_valid_advances_cursor` — happy path.
- `t1_consume_header_truncated_returns_false` — partial header.
- `t1_consume_header_bad_signature_rejected` — bad signature → error.
- `t1_consume_header_non_zero_flags_rejected` — flag bit 16 → error
  with the flag value preserved.
- `t1_consume_header_oversized_extension_rejected` — 32 MiB extension
  → error.
- `t1_consume_header_with_extension_advances_past_extension` — proper
  4-byte extension consumed.
- `t1_consume_header_with_partial_extension_needs_more` — partial
  extension → Ok(false).
- `t1_decode_empty_stream_yields_zero_rows` — header + EOD only.
- `t1_decode_field_count_mismatch` — wrong column count.
- `t1_decode_truncated_mid_field_length_needs_more` — partial length.
- `t1_decode_truncated_mid_value_needs_more` — partial value.
- `t1_decode_bad_field_length_rejected` — negative non-`-1` length.
- `t1_round_trip_int_widths` — INT2/INT4/INT8.
- `t1_round_trip_float_widths` — FLOAT4/FLOAT8.
- `t1_round_trip_bool` — BOOL 0x00/0x01.
- `t1_round_trip_text_multibyte` — multi-byte UTF-8.
- `t1_round_trip_bytea_with_zero_bytes` — embedded zero bytes (binary
  BYTEA is raw bytes, not `\x` hex).
- `t1_round_trip_timestamptz` — 8-byte BE i64.
- `t1_round_trip_zero_column_row` — `field_count = 0` distinct from
  `-1` EOD marker.
- `t1_round_trip_three_rows_with_eod` — multi-row + state transitions.
- `t1_new_in_body_skips_header_consumption` — resume across CopyData
  boundaries.

### KATs (`copy::proto::binv1_*` — 3):

- `binv1_copy_in_response_binary_two_cols_byte_locked` — format=1 in
  G frame.
- `binv1_copy_out_response_binary_two_cols_byte_locked` — format=1 in
  H frame.
- `binv1_binary_vs_text_response_shape_identical_modulo_format_bytes`
  — invariant: binary differs from text ONLY in format byte +
  per-column codes (across ncols=0/1/2/7).

### KATs (`copy::command::t1_parse_copy_binary_format_accepted_in_v1`):

- 1 KAT: `COPY t FROM STDIN WITH (FORMAT binary)` → `From { format:
  Binary }` (was Rejected pre-arc).

### KATs (`server::tests::t2_run_session_copy_binary_format_accepted_v1`):

- 1 KAT (flipped from V1 rejection): full Q → CopyInResponse(format=1)
  → CopyData(header + EOD) → CopyDone → COPY 0 + RFQ. Locks the
  end-to-end binary session shape.

### Test counts (release on vulcan, 2026-06-02 after T1+T2):

| Surface | Before | After T1+T2 | Delta |
|---|---|---|---|
| `kessel-pg-gateway` lib | 744 | 775 | +31 |

`#![forbid(unsafe_code)]` honored across all new modules. Zero new
external deps — `copy::binary` is pure Rust + std + the existing
extq encoders.

## T3 — real psql smoke on vulcan (2026-06-02)

**Setup**:
- Server: `kesseldb` built from commit `c523339` with
  `--features pg-gateway`.
- Listener: `127.0.0.1:5532` (PG-wire), `127.0.0.1:6532` (binary).
- Token: `admin`.
- Client: PostgreSQL 16.14 psql.

**Headline results** (full transcript:
`docs/superpowers/sppgcopybin-t3-smoke-2026-06-02.txt`):

1. ✅ `CREATE TABLE bin_smoke (id BIGINT, name CHAR(32))` →
   `CREATE TABLE`.
2. ✅ `INSERT (1, 'alpha'), (2, 'beta'), (3, 'gamma')` → `INSERT 0 3`.
3. ✅ `COPY bin_smoke TO STDOUT WITH (FORMAT binary)` → 89 bytes
   with canonical `PGCOPY\n\xff\r\n\0` signature (verified via
   `hexdump -C`) + 3 length-prefixed binary rows + `\xff\xff` EOD.
4. ✅ `COPY bin_smoke2 FROM STDIN WITH (FORMAT binary)` (into fresh
   table) → `COPY 3`. Follow-up `SELECT *` returns the same row set.
5. ✅ Re-export `COPY bin_smoke2 TO STDOUT WITH (FORMAT binary)` →
   `diff -q` byte-equal vs original (md5sum match
   `d4df79da25448ee783bbe6ce0caad181`).
6. ✅ `COPY bin_num TO STDOUT WITH (FORMAT binary)` against I128
   column → `ERROR: COPY binary: column "amount" type OID 1700 not
   supported in V1 (SP-PG-COPY-BIN-NUMERIC)`. Session stays alive
   for the next Q.

**Headline question — does psql binary COPY round-trip work
against the binary?** YES. The full round-trip (export → import → re-
export) is byte-equal across the wire, signaling that every layer
(parser → state machine → binary codec → BULKAPPLY fold → engine →
SELECT path → binary value encoder → output framing) is internally
consistent.

## V2 follow-ups (each its own arc — listed here for the SP-PG
parent arc's running follow-up index)

- **`SP-PG-COPY-BIN-NUMERIC`** — binary NUMERIC encoding.
- **`SP-PG-COPY-BIN-OID`** — optional OID-column flag bit.
- **`SP-PG-COPY-BIN-EXTRA`** — UUID / JSONB / ARRAY binary encoding.
- **`SP-PG-COPY-BIN-DIRECT`** — typed parameter binding (skip the
  binary→text→SQL round trip for 5-10× throughput).

## Closure notes

- TaskList #360 ready for completion.
- HTTP/1.1 + WS + binary + PG-wire-Simple + PG-wire-Extended +
  PG-wire-COPY-text + PG-wire-COPY-CSV surfaces byte-untouched.
- `#![forbid(unsafe_code)]` honored across the new `copy::binary`
  submodule.
- Zero new external deps.
- USAGE.md §9 — new SP-PG-COPY-BIN subsection covering wire shape,
  supported type set, codec reuse, and V2 follow-ups.
- STATUS.md current-capabilities header updated with the Track A.2.2
  SP-PG-COPY-BIN entry.
