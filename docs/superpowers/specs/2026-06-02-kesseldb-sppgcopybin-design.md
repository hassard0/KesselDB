> # SP-PG-COPY-BIN — PostgreSQL `COPY` binary format
>
> Status: T1 — design spec + binary codec scaffold + KATs (this commit).
> T2 (parser + dispatch wiring), T3 (vulcan smoke), T4 (arc closure) widen.
>
> SP-arc parent: SP-PG-COPY (V1 closed 2026-05-30, text-only) + SP-PG-COPY-CSV
> (V1 closed 2026-05-30, CSV format). Both progress trackers named
> `SP-PG-COPY-BIN` as the deferred V2 follow-up for `WITH (FORMAT binary)`.
>
> Companion progress tracker:
> `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgcopybin-progress.md`
> (created at T1; updated each slice).
>
> Date: 2026-06-02

## §1. Context — why binary COPY is the next adoption multiplier

After SP-PG-COPY (V1, text) + SP-PG-COPY-CSV (V1, CSV), four canonical
workflows still trip at the protocol layer because they all hard-require
`COPY ... WITH (FORMAT binary)`:

1. **`pg_dump --format=custom`** — the default `pg_dump` output mode
   for non-text dumps emits per-table COPY frames in PG binary format.
   The custom format is the recommended format for production use
   (compressed + selective restore via `pg_restore`); text format is the
   "for compatibility" fallback. Without binary COPY, KesselDB can only
   restore from `pg_dump --format=plain`, the slowest and largest dump
   shape.
2. **`pg_basebackup` / replication-stream COPY** — physical replication
   wraps WAL segments in COPY binary frames. KesselDB doesn't ship a
   physical replication surface (that's a separate arc — SP-PG-REPL),
   but the wire shape for the COPY framing is the same one this arc
   ships, so SP-PG-REPL inherits the codec for free.
3. **High-volume ETL tools** — `pg_bulkload`, `pgloader`, Stitch, Fivetran,
   Airbyte's `airbyte-source-postgres` all use binary COPY for the
   throughput win (no per-value text→binary conversion at the server).
   Text COPY parses each integer / float / timestamp from decimal text;
   binary COPY skips the parser entirely and writes the wire bytes
   directly into the engine's storage representation.
4. **JDBC `CopyManager.copyIn(stream, ...)` with binary-format stream** —
   the modern JDBC driver's bulk-load path uses binary by default when
   the caller wraps a `PGCopyOutputStream`. Today this fails at KesselDB
   with the V2-pointing `0A000` reject.

After SP-PG-COPY-BIN V1 closes, all four workflows succeed against
KesselDB end-to-end.

| Surface | Today (text+csv only) | After SP-PG-COPY-BIN V1 |
|---|---|---|
| `pg_dump --format=custom` restore | FAIL — first COPY binary frame rejects | works |
| `pg_basebackup` | N/A (separate arc — SP-PG-REPL; this arc unlocks its codec) | unblocks |
| `pg_bulkload` | FAIL — bulk-loader uses binary | works |
| `pgloader` | FAIL — bulk-loader uses binary | works |
| JDBC `CopyManager.copyIn(PGCopyOutputStream...)` | FAIL — driver default = binary | works |
| `psql \copy ... WITH (FORMAT binary)` | FAIL — psql passes through | works |

The headline acceptance test for V1: a `psql` round-trip via
`COPY t TO STDOUT WITH (FORMAT binary) > out.bin` followed by
`COPY t FROM STDIN WITH (FORMAT binary) < out.bin` into a fresh table
preserves the rows byte-equal.

## §2. Scope

### §2.1. V1 in-scope

1. **`COPY <table> FROM STDIN WITH (FORMAT binary)`** — server enters
   CopyIn state; client streams binary-format frames; server applies
   each parsed row through the existing engine path (per-row INSERT
   synthesis through `dispatch::dispatch_query`, same shape as
   SP-PG-COPY text V1).
2. **`COPY <table> TO STDOUT WITH (FORMAT binary)`** — server drives
   the existing `SELECT * FROM <table>` path under the hood and reframes
   each row's DataRow as binary-format CopyData per PG §55.2.7.
3. **PG binary COPY wire format** per PG §55.2.7:
   - 11-byte signature `PGCOPY\n\xff\r\n\0`
   - 4-byte BE flags (V1: 0; V1 rejects any non-zero flag with
     `0A000 feature_not_supported` + a precise V2-pointing message
     because bit 16 is the OID column which V1 doesn't carry).
   - 4-byte BE header extension area length (V1 reads + discards extension
     bytes if present; emits zero-length extension on COPY TO).
   - Per row: 2-byte BE i16 field count, then per field: 4-byte BE i32
     length (-1 = NULL), then `length` bytes of binary-encoded value.
   - End-of-data marker: 2-byte i16 -1 (`\xff\xff`).
4. **Per-column binary codec** — REUSES the existing encode + decode
   helpers from SP-PG-EXTQ-BIN-RESULTS (`extq::binary_results::
   encode_binary_value`) and SP-PG-EXTQ-BIN (`extq::substitute::
   decode_binary_param`). Supports the same 10 types: BOOL, INT2, INT4,
   INT8, FLOAT4, FLOAT8, TEXT, VARCHAR, BYTEA, TIMESTAMPTZ. No new
   per-type codec code lands in this arc — only the framing layer.
5. **Connection state machine** — same `CopyState::In(...)` per-connection
   state as SP-PG-COPY text. The CopyInState's `format` field already
   carries the CSV variant from SP-PG-COPY-CSV; this arc adds a third
   variant `CopyFormat::Binary` (no options struct — V1 has no
   binary-specific options).
6. **`MAX_COPY_DATA_BUFFER`** = 16 MiB carry buffer (inherited from
   SP-PG-COPY V1). Binary records can technically be larger than text
   rows (a single BYTEA column with a 32 MiB blob), but V1 inherits the
   text path's cap — V2 raises if needed.
7. **Streaming row processing** — binary CopyData frames are parsed
   incrementally; the carry buffer stashes the trailing partial row
   when a CopyData boundary lands mid-row.
8. **No-extra-deps invariant preserved.** Binary codec lives in
   `kessel-pg-gateway::copy::binary` (new submodule). Pure Rust + std.
   `#![forbid(unsafe_code)]`. No new workspace crates.

### §2.2. V1 out-of-scope (named V2 follow-ups)

- **`SP-PG-COPY-BIN-NUMERIC` (V2)** — binary NUMERIC. PG's binary
  NUMERIC is the most complex per-type encoding in the wire protocol
  (variable-length base-10000 digits + sign + scale + display scale).
  V1 rejects NUMERIC columns at COPY-start time with a precise
  V2-pointing `0A000` if the schema has any. Same V2-deferral shape as
  SP-PG-EXTQ-BIN-NUMERIC.
- **`SP-PG-COPY-BIN-OID` (V2)** — the optional OID column variant
  (header flag bit 16). PG used to emit this for tables with OIDs;
  modern PG (12+) defaults to `WITHOUT OIDS` so this is mostly legacy.
  V1 rejects non-zero header flags with a precise `0A000`. ~1 slice if
  ever needed.
- **`SP-PG-COPY-BIN-EXTRA` (V2)** — UUID, JSONB, ARRAY binary formats.
  Same set as SP-PG-EXTQ-BIN-EXTRA (the param + result + COPY-binary
  paths support the same OIDs by construction).

### §2.3. Wire dispatch surface

Same wire surface as SP-PG-COPY text + CSV (the framing is identical;
the codec swaps). The only new shapes:

| Frontend tag | Direction | V1 disposition |
|---|---|---|
| `Q` with `COPY ... WITH (FORMAT binary)` | client→server | parse + dispatch through `command::parse_copy_command` (already recognizes `FORMAT binary` — currently routes to `Rejected { BinaryFormat }`; this arc flips to `From/To { format: CopyFormat::Binary }`). |
| `d` CopyData payloads | client→server | per-record framing: 2-byte field count, per-field 4-byte length + binary value. Decode header on the FIRST CopyData payload, then per-record from there. |
| End-of-data marker (`\xff\xff`) | client→server | V1 reads + tolerates; CopyDone is the authoritative end-of-stream signal. |

`G` CopyInResponse / `H` CopyOutResponse still emit per the existing
`encode_copy_in_response` / `encode_copy_out_response` helpers in
`copy::proto`. The overall format byte in those frames is supposed to
flip from 0 (text) to 1 (binary) per PG §55.2.7 — V1 emits format=1 in
those slots when the parsed format is `CopyFormat::Binary`. Per-column
format codes also flip to 1.

## §3. Binary codec module

New file `crates/kessel-pg-gateway/src/copy/binary.rs`:

```rust
pub const PG_BINARY_SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";
pub const PG_BINARY_END_OF_DATA: i16 = -1;

/// Streaming binary COPY decoder. Stateful across CopyData frame
/// boundaries — the carry buffer (managed at the caller in
/// `CopyInState::carry`) is passed in as a `&[u8]` slice each call.
#[derive(Debug)]
pub struct BinaryDecoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
    state: BinaryState,
}

#[derive(Debug, PartialEq, Eq)]
pub enum BinaryState {
    /// Header not yet consumed — call `consume_header` first.
    Header,
    /// Header consumed — `next_row` returns rows until end-of-data.
    Body,
    /// End-of-data marker seen — no more rows.
    EndOfData,
}

#[derive(Debug, PartialEq, Eq)]
pub enum BinaryDecodeError {
    /// Signature mismatch (not the canonical `PGCOPY\n\xff\r\n\0`).
    BadSignature,
    /// Header flag bits other than 0 set. V1 only supports flags=0.
    UnsupportedFlags { flags: u32 },
    /// Header extension area > 0 bytes — V1 reads + discards but caps
    /// at the MAX_COPY_DATA_BUFFER. >cap is a protocol violation.
    HeaderExtensionTooLarge { length: u32 },
    /// A row's field count differs from the expected column count.
    FieldCountMismatch { expected: usize, actual: usize },
    /// A field's length prefix is negative (other than -1 which means
    /// NULL) — V1 treats as a wire-shape error.
    BadFieldLength { length: i32 },
    /// Truncated frame — ran out of bytes mid-row / mid-field.
    Truncated,
}

impl<'a> BinaryDecoder<'a> {
    pub fn new(bytes: &'a [u8]) -> Self;
    /// Consume the 19-byte (or longer if header extension) header.
    /// Validates signature + flags + extension area length. On success
    /// transitions to `Body`. Returns `Ok(true)` if the header was
    /// successfully consumed, `Ok(false)` if `bytes` doesn't yet hold
    /// the full header (carry + wait for more data), `Err(...)` on
    /// malformed.
    pub fn consume_header(&mut self) -> Result<bool, BinaryDecodeError>;
    /// Parse the next row. Returns `Ok(Some(fields))` on a complete
    /// row, `Ok(None)` if more bytes are needed OR the end-of-data
    /// marker was consumed (caller checks `state()` to distinguish).
    /// `Err(...)` on malformed.
    pub fn next_row(
        &mut self,
        expected_cols: usize,
    ) -> Result<Option<Vec<Option<&'a [u8]>>>, BinaryDecodeError>;
    pub fn state(&self) -> &BinaryState;
    pub fn cursor(&self) -> usize;
}

/// Encode a single binary COPY row. `values` is one per column:
/// `Some(&binary_bytes)` for non-NULL, `None` for NULL.
pub fn encode_binary_row(values: &[Option<&[u8]>]) -> Vec<u8>;
/// Encode the 19-byte canonical binary COPY header (signature + 0 flags
/// + 0-length extension area).
pub fn encode_binary_header() -> Vec<u8>;
/// Encode the 2-byte i16 -1 end-of-data marker.
pub fn encode_binary_end_of_data() -> Vec<u8>;
```

The decoder is a one-shot streaming type — each CopyData frame
constructs a fresh `BinaryDecoder` over the carry+frame bytes,
consumes whatever it can, and reports back how many bytes were
consumed so the caller can update the carry buffer.

## §4. State machine + integration

### §4.1. CopyFormat variant

`copy::CopyFormat` (currently `Text` + `Csv(CsvOptions)`) gains a third
variant `Binary`. No options struct — V1 has no binary-specific options.

```rust
pub enum CopyFormat {
    Text,
    Csv(CsvOptions),
    Binary,
}
```

`is_csv()` semantics unchanged; new `is_binary()` for the dispatch
branch.

### §4.2. CopyInState

`CopyInState` gains a `binary_header_consumed: bool` field. Per the spec
the header is sent as part of the first CopyData payload, not as a
separate message. The dispatcher tracks "have we consumed the header
yet" so subsequent CopyData payloads parse rows starting from the
right offset.

### §4.3. parse_copy_command flip

The recognizer in `command.rs::parse_with_options` currently returns
`Err(RejectReason::BinaryFormat)` for `WITH (FORMAT binary)`. This arc
flips that to `Ok(CopyFormat::Binary)`. The dispatcher then routes
through the new binary path.

V1 still rejects any binary-specific options (the spec lists none, but
we defensively reject `FORCE_QUOTE`, `HEADER`, etc. when paired with
binary — those are CSV-only).

### §4.4. process_copy_data binary branch

`process_copy_data` (currently text + CSV branches) gains a binary
branch. The branch:

1. If `!state.binary_header_consumed`, attempt `consume_header` over
   `state.carry + data`. If `Ok(false)` (need more data), update carry
   and return `Continue`. If `Ok(true)`, flip the flag.
2. Loop: `next_row(state.column_count)` — buffer to `state.pending_rows`
   (same BULKAPPLY V1 fold from SP-PG-COPY V1). Flush at `batch_size`.
3. If `Ok(None)` with `state.state() == BinaryState::EndOfData` — break.
4. If `Ok(None)` with `state.state() == BinaryState::Body` — partial row;
   save trailing bytes to carry and return `Continue`.

The per-row INSERT synthesis reuses the existing
`synthesize_insert_sql` / `synthesize_multi_row_insert_sql` — the
incoming binary bytes are first decoded to text via the existing
`decode_binary_param` (from SP-PG-EXTQ-BIN), then handed to the same
text-format SQL synthesizer. Trade-off: V1 pays the binary→text round
trip per value for code reuse. V2 `SP-PG-COPY-BIN-DIRECT` could
bypass the text round trip with typed parameter binding (~5-10× faster
for binary-heavy workloads), but V1 prioritizes "make pg_dump custom +
JDBC binary CopyManager work at all."

### §4.5. dispatch_copy_to binary branch

`dispatch_copy_to` currently switches on `CopyFormat::Text` vs
`CopyFormat::Csv(opts)`. This arc adds the `CopyFormat::Binary` arm:

1. Emit `H` CopyOutResponse with `format=1` + per-column format=1.
2. Emit `d` CopyData with `encode_binary_header()` payload.
3. For each engine row (from the underlying `SELECT * FROM <table>`):
   - Per column, decode text → re-encode binary via
     `extq::binary_results::encode_binary_value`.
   - Frame as `encode_binary_row(&binary_cols)`.
   - Emit as a `d` CopyData.
4. Emit `d` CopyData with `encode_binary_end_of_data()` payload.
5. Emit `c` CopyDone + `CommandComplete("COPY N")` + RFQ.

### §4.6. encode_copy_in_response / encode_copy_out_response

The existing helpers hard-code `format=0` (text) for the overall format
byte + per-column codes. This arc adds a parameter (or new helpers) so
the caller can request `format=1`. New helpers:

```rust
pub fn encode_copy_in_response_binary(ncols: u16) -> Vec<u8>;
pub fn encode_copy_out_response_binary(ncols: u16) -> Vec<u8>;
```

Same wire shape as the text variants modulo the format-byte +
per-column-code values.

## §5. Memory + flow bounds

- `MAX_COPY_DATA_BUFFER = 16 MiB` (inherited).
- A single binary row's serialized size is bounded by sum-of-column
  binary-width caps. V1 doesn't enforce a per-row cap beyond the
  carry-buffer cap.
- The header extension area is capped at MAX_COPY_DATA_BUFFER too;
  any header advertising a larger extension is rejected with
  `08P01 protocol_violation`.

## §6. Error semantics

| Trigger | SQLSTATE | Message | State after |
|---|---|---|---|
| Bad signature | `08P01` | `COPY binary: bad signature (expected PGCOPY\n\xff\r\n\0)` | In→Idle |
| Non-zero flags (V1 doesn't support OID column) | `0A000` | `COPY binary: header flags ${flags:#010X} not supported in V1 (SP-PG-COPY-BIN-OID)` | In→Idle |
| Header extension > MAX_COPY_DATA_BUFFER | `08P01` | `COPY binary: header extension length exceeds 16 MiB` | close |
| Field count mismatch | `22023` | `COPY binary row N: expected ${expected} columns, got ${actual}` | In→Idle |
| Negative field length other than -1 | `08P01` | `COPY binary row N: bad field length ${length}` | In→Idle |
| Truncated row | `22023` | `COPY binary row N: truncated` | In→Idle |
| NUMERIC column at COPY-start (V1 deferred type) | `0A000` | `COPY binary: NUMERIC columns not supported in V1 (SP-PG-COPY-BIN-NUMERIC)` | Idle |
| Unsupported OID at column | `0A000` | `COPY binary: column "<col>" type OID ${oid} not supported in V1 (SP-PG-COPY-BIN-EXTRA)` | Idle |

V1 atomicity: same per-row dispatch model as SP-PG-COPY text V1 (rows
already committed STAY committed on mid-COPY abort). SP-PG-COPY-BULKAPPLY
batches into multi-row INSERTs — same fold applies to binary.

## §7. Task decomposition (T1..T4)

| T# | Scope | KAT delta |
|---|---|---|
| **T1+T2** | Design spec (this file) + `copy::binary` codec module + KATs locking the wire shapes + parser flip + dispatch wiring. Header signature/flags/extension parsing + encoding. Encode + decode round-trip for the 10 supported types. `CopyFormat::Binary` variant. `process_copy_data` binary branch. `dispatch_copy_to` binary branch. `encode_copy_in_response_binary` + `encode_copy_out_response_binary`. | +15-20 |
| **T3** | Real psql 16.14 smoke on vulcan + USAGE update. `CREATE TABLE` + INSERT seed rows + `COPY TO STDOUT WITH (FORMAT binary)` to a file + `COPY FROM STDIN WITH (FORMAT binary)` into a fresh table + `SELECT *` byte-equal verification. | (smoke) |
| **T4** | Arc closure — STATUS.md row + USAGE §9 binary subsection + progress tracker → CLOSED + TaskList #360 ready. | (docs) |

Total estimate: ~15-20 new KATs.

## §8. Acceptance criteria

1. `psql -h vulcan -p 5532 -U test -c "COPY t TO STDOUT WITH (FORMAT
   binary)" > out.bin` exports binary-format bytes starting with the
   canonical `PGCOPY\n\xff\r\n\0` signature.
2. `psql ... -c "COPY t FROM STDIN WITH (FORMAT binary)" < out.bin`
   into a fresh table re-produces the same row set (verified via
   `SELECT *` byte-equal).
3. Connection state model: a malformed binary COPY frame emits the
   right SQLSTATE per §6 and clears state (connection STAYS ALIVE).
4. Mixed-format coexistence on the same connection: a text COPY can
   follow a binary COPY can follow a CSV COPY arbitrarily.
5. The 10 supported types round-trip byte-equal at the engine layer
   (INSERT via binary COPY → SELECT → COPY TO binary → byte-equal).
6. `#![forbid(unsafe_code)]` honored across the new `copy::binary` module.
7. Zero new external deps — `cargo tree -p kessel-pg-gateway -e
   normal` shows workspace-only.
8. seed-7 GREEN; tree-grep EMPTY; CI green.
9. HTTP/1.1 + WS + binary + PG-wire-Simple + PG-wire-Extended +
   PG-wire-COPY-text + PG-wire-COPY-CSV surfaces byte-untouched.

## §9. Self-review — weak spots

1. **Per-value text round trip cost.** V1's per-row INSERT synthesizer
   takes text input; binary COPY data must be decoded to text via
   `decode_binary_param` before re-encoding as a SQL literal. For a
   1M-row binary COPY of all INT8 columns, that's 1M binary→text →
   SQL synthesis → SQL parser → engine round trips. V2
   `SP-PG-COPY-BIN-DIRECT` would bypass via typed parameter binding,
   landing the 5-10× throughput win. V1 prioritizes correctness over
   throughput.
2. **Binary header on the first CopyData frame, not on its own
   message.** Per PG §55.2.7 the 19-byte header is part of the first
   CopyData payload, not a separate frame. V1's `process_copy_data`
   binary branch checks a per-state flag to decide whether the next
   bytes are header or row. The wrinkle: a single CopyData frame
   smaller than 19 bytes (pathological — a client batching one byte
   per frame) would force header parsing across many frames. V1 carry
   buffer handles correctly but adds latency.
3. **End-of-data marker placement.** PG places `\xff\xff` at the end
   of the row stream as a synchronization marker. V1 reads + tolerates
   it (CopyDone is the authoritative end-of-stream signal per v3
   protocol), but if a malicious client puts `\xff\xff` MID-stream and
   continues sending CopyData, V1 silently skips the marker and keeps
   going. This is technically wrong per PG semantics (the marker should
   end the stream) but matches the SP-PG-COPY text V1 tolerance shape.
   V2 could flip to strict.
4. **NUMERIC rejection at COPY-start vs at-row-time.** V1 rejects at
   COPY-start by walking the schema and checking for NUMERIC columns.
   The trade-off: an early reject saves work but a per-row reject
   would let mixed schemas (most non-NUMERIC + 1 NUMERIC col) still
   work for the other columns. V1 picks early-reject for simplicity;
   V2 SP-PG-COPY-BIN-NUMERIC ships the per-value codec.
5. **No FROM-STDIN streaming over very large blobs.** A single BYTEA
   column of 100 MiB in a binary COPY exceeds the 16 MiB carry buffer.
   V1 rejects with `54000 program_limit_exceeded`. V2 could chunk-decode
   the BYTEA across CopyData boundaries.
6. **`encode_binary_value` per-column re-encoding cost on COPY TO.**
   The engine emits text-format DataRow; the COPY TO binary path
   decodes back to a column value then re-encodes binary. V2 could
   stream binary-format engine rows directly (when the engine learns
   to emit binary).
7. **Header flag bits 1-15 reserved by PG.** PG reserves flag bits
   1-15 in the binary header for future extensions; V1 rejects any
   set bit with `UnsupportedFlags`. If PG ever uses bit 0 for a new
   feature this V1 will need a wider mask.
8. **No partial-COPY recovery semantics.** A binary COPY that fails
   mid-stream leaves the engine in the "rows-already-committed-stay-
   committed" state from SP-PG-COPY text V1. PG's all-or-nothing
   semantics for binary COPY (when the row count is non-trivial) is
   landed by V2 SP-PG-COPY-BULKAPPLY (already applies to text + CSV).

## §10. Out-of-scope hard passes (permanent)

- Same `COPY ... FROM PROGRAM '...'` / `COPY ... FROM '/path'` as the
  parent SP-PG-COPY arc — server-side file/program access is a
  permanent hard pass.

## §11. Open questions

1. Should the per-column format codes in `H`/`G` be format=1 across
   the board, or per-column? PG sets them all to 1 for `FORMAT binary`
   even if specific columns can't be represented in binary (the wire
   just rejects at the value layer). V1: emit all 1s; the
   `0A000` reject at COPY-start time prevents the column-level reject
   from being reachable.
2. Should V1 emit a NoticeResponse when a client requests `FORMAT
   binary` against a table with a NUMERIC column? PG fails hard; V1
   matches.
3. Per-value V1 `encode_binary_value` vs V2 `SP-PG-COPY-BIN-DIRECT`
   typed parameter binding — same trade-off as the corresponding
   SP-PG-EXTQ-BIN-DIRECT arc.

## §12. References

- PostgreSQL §55.2.7 "COPY Operations — Binary Format":
  https://www.postgresql.org/docs/current/sql-copy.html#id-1.9.3.55.9.4
- PostgreSQL src/backend/commands/copyfromparse.c — reference
  implementation.
- PostgreSQL src/include/catalog/pg_type.dat — per-type binary
  representations.
- libpq's `PQputCopyData` for binary clients — reference for what
  real clients send.
- pgwire crate (Rust) — reference implementation; V1 doesn't depend
  but cross-validates byte shapes against its tests.
