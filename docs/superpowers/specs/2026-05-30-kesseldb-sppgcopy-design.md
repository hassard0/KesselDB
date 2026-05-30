> # SP-PG-COPY — PostgreSQL `COPY FROM STDIN` / `COPY TO STDOUT` bulk-load protocol
>
> Status: T1 — design spec + scaffold (this commit). T2..T5 widen.
>
> SP-arc parent: SP-PG (V1 closed 2026-05-27 — Simple Query) + SP-PG-EXTQ
> (V1 closed 2026-05-29 — Extended Query). Named as a deferred V2 follow-up
> in both `2026-05-28-kesseldb-sppgextq-extended-query-design.md` §2 and
> the SP-PG-EXTQ progress tracker §V2 follow-ups list.
>
> Companion progress tracker:
> `docs/superpowers/specs/2026-05-30-kesseldb-subproject-sppgcopy-progress.md`
> (created at T1; updated each slice).
>
> Date: 2026-05-30

## §1. Context — why COPY is the next adoption multiplier

After SP-PG (V1) + SP-PG-EXTQ (V1), every modern PG ORM connects to
KesselDB and runs prepared-statement workloads end-to-end. But three
adjacent workloads still fail at the protocol layer because they all
hard-require COPY:

1. **`pg_dump` / `pg_restore`** — the canonical PG backup tool emits
   `COPY <table> FROM STDIN` per restored table (it never uses INSERT
   for bulk data; INSERT is reserved for schema + sequence ops).
   KesselDB cannot accept a `pg_dump`-shaped restore today.
2. **`sysbench --db-driver=pgsql prepare`** — the standard PG
   benchmark harness seeds rows via `COPY FROM STDIN` for the same
   reason: INSERT round-trip latency dominates at 1M+ row seeds, and
   COPY is 10-100× faster on real PG. Without it, a sysbench compare
   against KesselDB is unfair (the seed phase, not the OLTP phase,
   dominates wall-clock).
3. **CSV ingest via `psql -c "\copy t FROM 'data.csv' CSV"`** — the
   ergonomic CSV bulk-load every analyst muscle-memory uses. `\copy`
   is a psql client-side wrapper that wires the file's bytes into a
   `COPY <table> FROM STDIN` server-side, so the wire protocol is
   identical to what `pg_dump` emits.

Listed adoption surfaces unlocked by SP-PG-COPY V1 (text format):

| Surface | Today (without COPY) | After SP-PG-COPY V1 |
|---|---|---|
| `pg_dump` restore | FAIL — `0A000 feature_not_supported` on first COPY frame | works for text-format dumps (the default) |
| `sysbench prepare` | FAIL — seed phase rejects | works (text-format) |
| `psql \copy ... FROM 'file.csv'` | FAIL | works (text-format; CSV-format deferred) |
| `psql \copy ... TO 'file.csv'` | FAIL | works (text-format; CSV deferred) |
| psycopg2 `cursor.copy_from(...)` | FAIL | works |
| psycopg3 `cursor.copy(...)` | text-format only — works |
| JDBC `CopyManager.copyIn(...)` | text-format only — works |
| asyncpg `connection.copy_from_query(...)` / `copy_records_to_table(...)` | text-format only — works |

The headline acceptance test for V1: after this arc closes, a
sysbench `prepare` phase seeds 100K rows via COPY against a running
`kesseldb-server` with `--features pg-gateway`, and a subsequent
`COPY t TO STDOUT` exports them byte-equal.

## §2. Scope

### §2.1. V1 in-scope

1. **`COPY <table> FROM STDIN`** — server replies with
   `CopyInResponse` (`G`) advertising text format and column count;
   client streams one or more `CopyData` (`d`) chunks of newline-
   delimited tab-separated rows; client ends with `CopyDone` (`c`)
   for success or `CopyFail` (`f`) to abort; server replies with
   `CommandComplete` (`COPY N` tag) + `ReadyForQuery`.
2. **`COPY <table> TO STDOUT`** — server replies with
   `CopyOutResponse` (`H`) advertising text format and column count;
   server streams `CopyData` per row (one row per CopyData frame, or
   batched — V1 emits one row per frame for simplicity);
   server ends with `CopyDone` (`c`) + `CommandComplete("COPY N")` +
   `ReadyForQuery`.
3. **Text format only.** PG's text format per §COPY-FORMATS:
   - Rows separated by `\n`.
   - Fields within a row separated by `\t`.
   - NULL is the literal `\N`.
   - Backslash escapes in field bytes: `\b` `\f` `\n` `\r` `\t` `\v`
     `\\` (and `\N` only at the field-byte boundary, not embedded).
   - End-of-data marker (single line `\.\n`) is OPTIONAL in v3
     protocol because `CopyDone` framing is unambiguous; V1 tolerates
     the marker if present (skip the trailing `\.\n` line on FROM).
4. **Connection state machine.** A connection that has sent a COPY
   FROM `Query` enters `CopyIn` state. While in `CopyIn`, only
   `CopyData` (`d`), `CopyDone` (`c`), `CopyFail` (`f`), and
   `Terminate` (`X`) are valid frontend tags; ANY other tag
   (including `Q`, `P`, `B`, etc.) is a protocol error (08P01) and
   the dispatcher emits the canonical PG response `EFATAL 08P01
   "unexpected message tag in COPY mode"` then closes the connection.
   COPY TO does NOT enter a special state — once the server has
   emitted `CopyOutResponse` + all `CopyData` + `CopyDone`, normal
   `Query` dispatch resumes (`ReadyForQuery` is the boundary).
5. **Per-row dispatch through `Op::Create`-shaped INSERT.** V1 COPY
   FROM does not bypass the engine — each parsed row is dispatched
   via the existing Simple Query path as a synthetic `INSERT INTO
   <table> (<columns>) VALUES (<values>)` (the same path
   psycopg/SQLAlchemy already exercises in production). The row-count
   reported in `COPY N` is the number of rows successfully ingested
   (any constraint-failed row aborts the whole COPY per
   PG-`ON_ERROR_STOP` default).
6. **`MAX_COPY_DATA_BUFFER`** = 16 MiB per `CopyData` frame
   (inherits the `PG_MAX_MESSAGE_SIZE` cap). A client streaming a
   3 GB dataset chunks into multiple `CopyData` frames; V1 processes
   each frame fully before reading the next.
7. **Streaming row processing.** COPY FROM does not buffer the whole
   dataset before INSERTing — rows are parsed + applied as soon as
   their complete newline-terminated record is in hand. The
   per-frame parser keeps a per-connection "carry" buffer for the
   trailing incomplete row at the frame boundary (because PG
   `CopyData` is purely a binary framing, not a logical row
   framing — a row can span multiple CopyData frames).
8. **No-extra-deps invariant preserved.** The COPY codec lives in
   `kessel-pg-gateway::copy` (new submodule). Pure Rust + std + the
   existing workspace crates. `#![forbid(unsafe_code)]`.

### §2.2. V1 out-of-scope (named V2 follow-ups)

Each is its own arc — listed by name so the closure note in T5 can
point clients at the named gap rather than handwaving:

- **`SP-PG-COPY-BIN` (V2)** — binary format (`WITH (FORMAT binary)`).
  PG binary COPY uses a 19-byte signature header (`PGCOPY\n\xFF\r\n\0`
  + flags u32 + header-extension-len u32) and 2-byte field-count +
  per-field length-prefixed binary values + 2-byte `-1` end-of-data
  marker. The binary protocol is type-OID-aware: int4 is 4 bytes BE,
  text is varlena, etc. SP-PG-CAT's OID table + the engine's
  `kessel-codec::encode/decode` round-trip would already cover most
  types; V2 wires the format codes + per-OID binary encoders.
  ~2 slices.
- **`SP-PG-COPY-CSV` (V2)** — CSV format (`WITH (FORMAT csv)`).
  Adds quoting (`"`), embedded-quote escapes (`""`), embedded-comma
  handling, optional `HEADER` directive, custom `DELIMITER` /
  `NULL` / `QUOTE` / `ESCAPE` parameters. The codec is otherwise
  similar to text — same line-by-line + field-by-field parsing but
  with CSV-specific quote handling. ~2 slices.
- **`SP-PG-COPY-FILE` (V2)** — `COPY <table> FROM '/path/to/file'`
  / `COPY <table> TO '/path/to/file'` (server-side file access).
  HARD PASS for V1 because of the security implications — a
  client able to read/write any path on the server is a privilege
  escalation. PG itself requires superuser for this form and warns
  prominently in the docs. KesselDB will likely never ship this
  because operator-side file access from a multi-tenant DB is a
  permanent footgun; the V2 arc, if it ships, would be opt-in
  per-tenant with a strict allowlist.
- **`SP-PG-COPY-BULKAPPLY` (V2)** — bulk `Op::Txn` fold so the COPY
  FROM path batches N rows into a single engine round-trip instead
  of one Op::Create per row. The V1 per-row pattern is simple +
  correct but slow at scale; V2 batches into the existing
  `Op::Txn` shape (the same `apply_sql` interface, but with an
  `INSERT INTO t VALUES (r1), (r2), ..., (rN)` multi-row INSERT
  per batch of e.g. 1000 rows). Spec §11 weak-spot #2 quantifies
  the V1 cost; V2 is a 10-50× throughput win at sysbench-sized
  workloads.
- **`SP-PG-COPY-PROGRAM` (V2)** — `COPY ... FROM PROGRAM '...'` /
  `TO PROGRAM '...'`. Hard pass alongside `SP-PG-COPY-FILE` for the
  same security reasons; lists for completeness so a future operator
  pull cannot drift into shelling out without a deliberate audit.
- **`SP-PG-COPY-FREEZE` (V2)** — the `FREEZE` option on `COPY FROM`
  that PG uses to skip the visibility map on bulk loads. KesselDB
  doesn't have a visibility-map equivalent (every committed write is
  visible to every reader immediately), so the option is silently a
  no-op. V2 may emit a notice; V1 just ignores.

### §2.3. Wire dispatch surface

| Frontend tag | Direction | V1 disposition |
|---|---|---|
| `Q` with `COPY ... FROM STDIN` | client→server | enter `CopyIn` state; emit `G` CopyInResponse |
| `Q` with `COPY ... TO STDOUT` | client→server | stream rows; emit `H` CopyOutResponse + N `d` CopyData + `c` CopyDone + `C` CommandComplete + `Z` RFQ |
| `d` CopyData | client→server | parse text-format chunks (only in CopyIn state) |
| `c` CopyDone | client→server | finalize; emit `C` CommandComplete + `Z` RFQ |
| `f` CopyFail | client→server | abort; emit `E` ErrorResponse 57014 query_canceled (with the reason from the CopyFail payload) + `Z` RFQ |

| Backend tag | Direction | When |
|---|---|---|
| `G` CopyInResponse | server→client | first reply to a `COPY FROM STDIN` Query |
| `H` CopyOutResponse | server→client | first reply to a `COPY TO STDOUT` Query |
| `d` CopyData | server→client | one per row in `COPY TO STDOUT`; payload is the text-format row including trailing `\n` |
| `c` CopyDone | server→client | end of `COPY TO STDOUT` row stream |

`G` and `H` share the same wire shape (5 + 1 + 2 + 2N bytes):
- byte tag (`G` or `H`)
- u32 BE total length (includes itself, excludes tag)
- u8 overall format (0 = text — V1's only emit)
- u16 BE column count
- u16 BE per-column format codes × column count

V1 emits format=0 for every column. For COPY FROM, the column-format
list is advisory (the client uses the same text format regardless);
for COPY TO, V1 actually emits text-format bytes per column.

## §3. State machine

A per-connection `CopyState` enum is added next to the existing
`extq::SessionState`. Three variants:

```rust
enum CopyState {
    /// Default — not in a COPY exchange.
    Idle,
    /// COPY FROM STDIN — the server has emitted CopyInResponse and is
    /// awaiting CopyData / CopyDone / CopyFail from the client. The
    /// `table` field carries the target table (for INSERT dispatch);
    /// `column_count` is the wire-advertised count (for the
    /// CopyInResponse echo); `carry` is the trailing-incomplete-row
    /// buffer (one or zero \n-terminated rows worth of bytes carried
    /// over from the previous CopyData frame); `rows_ingested` is
    /// the running count that becomes `COPY N` at CopyDone.
    In {
        table: String,
        column_count: u16,
        carry: Vec<u8>,
        rows_ingested: u64,
    },
}
```

COPY TO does NOT enter a state — the whole exchange happens within
a single `Q` dispatch (the row stream is generated synchronously
from the engine's response to the existing
`SELECT * FROM <table>` dispatch path, then framed as CopyOutResponse
+ CopyData × N + CopyDone + CommandComplete + RFQ before the byte
sequence is written to the wire).

The `server::run_session` loop branches on `copy_state` BEFORE
inspecting the frontend tag:

```text
read tag
if copy_state.is_in() {
    match tag {
        FE_COPY_DATA => extend carry, parse complete rows, dispatch as INSERTs
        FE_COPY_DONE => emit CommandComplete + RFQ, clear copy_state
        FE_COPY_FAIL => emit ErrorResponse 57014 + RFQ, clear copy_state
        FE_TERMINATE => clean close
        other        => protocol violation 08P01, close
    }
} else {
    // existing Q / extq dispatch
    if Q matches `COPY ... FROM STDIN` → set copy_state, emit CopyInResponse
    if Q matches `COPY ... TO STDOUT` → run COPY TO inline, emit full reply
    else → existing dispatch
}
```

This isolates the CopyIn-state-machine to the `server::run_session`
boundary — the `dispatch::dispatch_query` path is byte-untouched for
non-COPY SQL.

## §4. COPY parser

The text-format COPY parser lives in a new submodule
`kessel-pg-gateway::copy`. It exposes:

```rust
pub fn parse_copy_command(sql: &str) -> Option<ParsedCopy>;

pub enum ParsedCopy {
    From { table: String, columns: Option<Vec<String>>, format: CopyFormat },
    To   { table: String, columns: Option<Vec<String>>, format: CopyFormat },
}

pub enum CopyFormat { Text, Csv, Binary } // V1 accepts only Text

pub fn parse_text_row_bytes(line: &[u8], ncols: usize) -> Result<Vec<Option<Vec<u8>>>, CopyParseError>;

pub fn encode_text_row(values: &[Option<&[u8]>]) -> Vec<u8>;
```

`parse_copy_command` is intentionally lenient — leading whitespace +
comments stripped, trailing `;` tolerated. Recognizes:
- `COPY <ident> FROM STDIN`
- `COPY <ident> (col1, col2) FROM STDIN`
- `COPY <ident> FROM STDIN WITH (FORMAT text)` (also `WITH (FORMAT
  csv|binary)` but those return `Err(...)` → 0A000 at the dispatcher)
- `COPY <ident> TO STDOUT [WITH (FORMAT text)]`
- `COPY <ident> (col1, col2) TO STDOUT`

`parse_text_row_bytes` splits a `\n`-terminated row on `\t` then
unescapes per-field per the §2.1 table. Returns `Vec<Option<Vec<u8>>>`
with `None` for the `\N` NULL sentinel; on a single-line input with
exactly `ncols` fields, returns the field values. Mismatched count
→ `CopyParseError::FieldCountMismatch { expected, actual }` → SQLSTATE
22023 invalid_parameter_value (the canonical PG SQLSTATE for "COPY
data does not match table column count").

`encode_text_row` is the inverse: given a row's text-format byte
slices, encode them with tab separators + the necessary backslash
escapes + the trailing `\n`. Used by COPY TO to render engine row
bytes as wire bytes.

### Edge cases

1. **NULL** — wire `\N`; programmatic `None`.
2. **Empty field** — wire `<empty>` between tabs (i.e. `\t\t` is two
   tabs with an empty field between); programmatic `Some(b"".to_vec())`.
   Distinct from NULL.
3. **Tab in field value** — must be escaped as `\t` (backslash-t); a
   bare tab would be a field separator.
4. **Newline in field value** — must be escaped as `\n`.
5. **Backslash in field value** — must be doubled to `\\`.
6. **Field that starts with `\N`** — escaped as `\\N` (so it doesn't
   collide with the NULL sentinel).
7. **End-of-data marker** — `\.\n` on its own line in COPY FROM is
   the optional v2-protocol marker; v3 protocol uses `CopyDone`
   framing instead and the marker is unnecessary, but psql still
   emits it via `\copy`. V1 silently drops a line containing only
   `\.\n`.

## §5. Memory + flow bounds

- `MAX_COPY_DATA_BUFFER` = `PG_MAX_MESSAGE_SIZE = 16 MiB` per
  CopyData frame. Per-frame validation happens at `read_message`
  before allocation.
- The per-connection `carry` buffer is capped at the same 16 MiB
  (a row spanning a multi-MiB CopyData frame is pathological — V1
  rejects with SQLSTATE `54000 program_limit_exceeded` rather than
  unbounded buffering).
- The CopyIn dispatcher emits one INSERT per parsed row through
  the existing engine path. No batching in V1 (V2 SP-PG-COPY-
  BULKAPPLY) — at 1000 r/s throughput this is acceptable for V1
  acceptance, and the per-row pattern is the simplest correct shape.
- Rows are processed eagerly per CopyData arrival. The TCP read
  buffer + the CopyData payload + the carry buffer together cap
  per-connection RSS at `2 * 16 MiB + cargo overhead ≈ 35 MiB`.

## §6. Error semantics

| Trigger | SQLSTATE | Message | State after |
|---|---|---|---|
| `COPY ... FROM '/file'` | `0A000` | `COPY FROM file path not supported in V1; use COPY FROM STDIN` | Idle |
| `COPY ... WITH (FORMAT binary)` | `0A000` | `COPY binary format not supported in V1 (SP-PG-COPY-BIN)` | Idle |
| `COPY ... WITH (FORMAT csv)` | `0A000` | `COPY csv format not supported in V1 (SP-PG-COPY-CSV)` | Idle |
| Unknown table | `42P01` | `relation "<name>" does not exist` | Idle |
| Field count mismatch | `22023` | `extra/missing column data` | In→Idle (abort, no rows ingested) |
| Constraint failure mid-COPY | passes through engine SQLSTATE (e.g. `23505` UNIQUE / `23502` NOT NULL) | passes through engine message + `(at row <n>)` suffix | In→Idle |
| Frame >16 MiB | `08P01` | `protocol violation: message length exceeds limit` | close |
| `CopyFail` from client | `57014` | `COPY from stdin failed: <client reason>` | In→Idle |
| Non-CopyData/Done/Fail tag in CopyIn state | `08P01` | `unexpected message tag 0x<NN> in COPY mode` | close |

V1's atomicity model for COPY FROM: per PG semantics, the default is
"all-or-nothing on error" — if any row fails, the whole COPY rolls
back. KesselDB V1 does NOT meet this — rows are dispatched per-row
through the existing `Op::Create` path which auto-commits each one.
This is a documented divergence (spec §11 weak-spot #4) — V2
SP-PG-COPY-BULKAPPLY's `Op::Txn` fold restores PG-compatible
all-or-nothing semantics by wrapping the whole COPY in a transaction.
For V1, a constraint failure mid-COPY surfaces immediately as an
ErrorResponse and the COPY aborts at that point; rows already
ingested STAY committed.

## §7. Task decomposition (T1..T5)

| T# | Scope | KAT delta |
|---|---|---|
| **T1** | This commit — design spec (you are here) + scaffold (`crate::copy::{mod,proto,response}` with `parse_copy_command`, encode/decode helpers, byte-locked encoders, all 5 KATs locking spec invariants; new `proto::FE_COPY_DATA` etc. constants already exist from SP-PG T1; `extq::SessionState`-adjacent `CopyState` enum scaffolded behind a `copy_state` field on a new per-connection `PgSessionState` struct OR added directly to `server::run_session`'s loop). | +12 |
| **T2** | COPY FROM STDIN end-to-end. New `copy::dispatch_copy_in_start(sql, engine) -> CopyOutcome` that recognizes `COPY ... FROM STDIN`, returns `CopyInResponse` + sets state. New `copy::process_copy_data(data, state, engine) -> CopyOutcome` that handles incoming CopyData payloads. `server::run_session` loop branches on `copy_state`. Headline KAT: a full Q (`COPY t FROM STDIN`) + CopyData (3 rows) + CopyDone session emits the expected `G` + `C COPY 3` + RFQ sequence. | +10 |
| **T3** | COPY TO STDOUT end-to-end. New `copy::dispatch_copy_to(sql, engine) -> Vec<u8>` that drives the existing SELECT path, frames each result row as `CopyData`, ends with `CopyDone` + `CommandComplete("COPY N") + RFQ`. Headline KAT: a full Q (`COPY t TO STDOUT`) against a 3-row engine emits the expected `H` + 3×`d` + `c` + `C COPY 3` + RFQ sequence. | +8 |
| **T4** | Real psql smoke on vulcan + USAGE update. Bring up `kesseldb-server --features pg-gateway`, drive psql through `CREATE TABLE` + `COPY FROM STDIN` + `SELECT *` + `COPY TO STDOUT`. Capture results. Lift any inline bugs found. | (smoke) |
| **T5** | Arc closure — STATUS.md row + USAGE §9 expansion + BENCHMARKS row (if measured) + progress tracker → CLOSED + TaskList #350 ready. | (docs) |

Total estimate: ~30-40 new KATs.

## §8. Acceptance criteria

1. `psql -h vulcan -p 5532 -U test -c "COPY t FROM STDIN" < data.tsv`
   ingests rows into table `t` (V1 user-visible: the row count appears
   in the `COPY N` reply and the rows are visible to a subsequent
   `SELECT *`).
2. `psql -h vulcan -p 5532 -U test -c "COPY t TO STDOUT" > out.tsv`
   exports the rows in PG text format (rows separated by `\n`, fields
   by `\t`, NULL as `\N`).
3. The exported `out.tsv` byte-equal round-trips: re-ingesting via
   `COPY ... FROM STDIN < out.tsv` re-produces the same row set.
4. `pg_dump`-shaped restore of a single table works end-to-end (the
   `\copy <table> FROM '/path/to/dump.tsv'` shape).
5. Connection state model: a malformed `CopyData` frame in CopyIn
   mode emits ErrorResponse 22023 + RFQ + clears state (the
   connection STAYS ALIVE so the client can retry; PG itself
   sometimes closes here — V1 picks "tolerant survive" matching the
   SP-PG-EXTQ tolerant probe-then-fall-back contract).
6. Concurrent connection isolation: two concurrent connections each
   running COPY against the same table don't interfere — each
   connection's CopyIn state is independent.
7. Memory bound: a 50 MiB single CopyData frame is rejected at the
   framing layer (16 MiB cap) with 08P01 + clean close.
8. `#![forbid(unsafe_code)]` honored across the new `copy` submodule.
9. Zero new external deps — `cargo tree -p kessel-pg-gateway -e
   normal` shows workspace-only.
10. seed-7 GREEN; tree-grep EMPTY (no leaked TODO/FIXME); CI green.
11. PG-wire-Simple + PG-wire-Extended + HTTP/1.1 + WS surfaces
    byte-untouched.

## §9. Self-review — weak spots

1. **Per-row Op::Create dispatch is slow at scale.** Each parsed
   COPY FROM row goes through `dispatch::dispatch_query` (which
   compiles + applies + commits). At 1000 rows/sec the per-row
   pattern is fine; at 1M rows it's a 16-minute COPY. V2
   `SP-PG-COPY-BULKAPPLY` lifts via per-batch `Op::Txn` fold.
   *Severity*: medium-high — names the win headlining the V2 arc.
   *Mitigation*: V1 acceptance picks 100K rows so the per-row pattern
   is workable (90s ish on V100-class hardware); benches over 1M are
   labeled V2.
2. **Per-row SQL synthesis is brittle for unusual field values.**
   The `INSERT INTO t VALUES (...)` we synthesize from the parsed
   row uses `'` quoting for text values + `''` escaping for embedded
   single quotes. A binary blob ingested via text-format COPY (PG
   allows that — bytes are `\x` hex-encoded inside text format)
   round-trips correctly because `kessel-sql`'s lexer accepts the
   `\x` prefix. But weird unicode (composed characters, RTL marks)
   could trip a `kessel-sql` parse error V1 surfaces as a row-level
   ErrorResponse mid-COPY. *Mitigation*: V1 documents the constraint;
   V2 SP-PG-COPY-BULKAPPLY's batched Op::Txn fold can use a typed
   parameter binding instead of SQL-text synthesis.
3. **No mid-COPY transaction rollback (atomicity gap vs PG).** PG's
   default is "if any row fails, the whole COPY rolls back." V1
   commits each row immediately because the engine has no notion of
   "tentative writes." A NOT NULL violation at row 500 of 1000 leaves
   the first 499 committed and aborts the next 501. *Mitigation*:
   document. V2 SP-PG-COPY-BULKAPPLY's transaction wrap restores
   PG semantics.
4. **No COPY FROM CSV (the most ergonomic CSV-load shape).** CSV
   format is by far the most common shape an analyst uses
   (`\copy t FROM 'data.csv' WITH CSV HEADER`). V1 only supports
   text format. V2 SP-PG-COPY-CSV lifts. *Severity*: medium — the
   pg_dump + sysbench paths use text format so V1's adoption
   headline still lands; SP-PG-COPY-CSV is the right next slice.
5. **COPY TO STDOUT loads the entire result set into memory** before
   framing. A 10M-row table would peak RSS at ~10×row-size bytes.
   V1 should be honest about the cap; the OOM-or-not call lands on
   the operator's row-count expectation. *Mitigation*: V1 documents
   the constraint; V2 streams per-row from the engine's
   `Op::Select`-style streamer when SP-A T14 streaming lands.
6. **No CopyFail mid-COPY-TO support.** PG allows the server to
   abort a COPY TO mid-stream by emitting ErrorResponse + RFQ. V1
   doesn't because we precompute the whole row set before framing.
   *Mitigation*: V1 documents; V2 streams.
7. **Encoding/decoding allocs per row.** Each `encode_text_row`
   allocates a fresh `Vec<u8>`; each `parse_text_row_bytes`
   allocates a `Vec<Option<Vec<u8>>>`. At 100K rows that's 200K
   allocations. *Mitigation*: V1 acceptable; V2 could reuse buffers
   via a per-connection allocator pool.
8. **`carry` buffer + multi-CopyData rows.** The carry is correctly
   handled (incomplete trailing row carries over), but a pathological
   client sending one byte per CopyData frame would exercise the
   carry path 16M times for a 16 MiB load. Per-frame overhead is
   bounded but not zero. *Mitigation*: V1 acceptable; clients in
   practice batch into reasonable-sized CopyData frames (libpq's
   default is 8 KiB-ish per frame).
9. **No `pg_dump --inserts` fallback test.** A user who runs
   `pg_dump --inserts` (which emits INSERT instead of COPY) already
   works on V1 — but a user who DOESN'T pass that flag and gets
   COPY by default benefits from this arc. V1 KAT in T4 covers the
   `pg_dump`-default path explicitly.
10. **The 16 MiB CopyData cap interacts with very wide rows.** A
    table with 1000 text columns of 32 KiB each would have rows
    just under 32 MiB — already over the cap. V1 surfaces this as
    a framing 08P01 + close. *Mitigation*: V1 documents; V2 raises
    the cap or chunks rows transparently.

## §10. Out-of-scope hard passes (permanent)

- `COPY ... FROM PROGRAM '...'` / `TO PROGRAM '...'` — shells out
  to an OS command. Security disaster, no good operator story.
- `COPY ... FROM '/local/file'` (server-side file access) — same
  privilege-escalation concern.
- The legacy v2 protocol's `\.\n` end-of-data marker as REQUIRED
  signal — V1 tolerates it but doesn't require it (v3 framing).

## §11. Open questions

1. Should COPY FROM emit one INSERT per row (V1 current plan) or
   batch into chunks of N rows in a single multi-row INSERT? The
   batched shape would be 3-5× faster but exposes a partial-failure
   surface (one bad row aborts the whole chunk). V1: per-row; V2:
   batched.
2. Should a malformed CopyData frame keep the session alive (V1
   plan — tolerant) or close it (PG default — strict)? V1 picks
   tolerant for symmetry with the SP-PG-EXTQ probe-then-fall-back
   contract; if real-world clients have trouble with the tolerant
   shape we flip in a patch.
3. Should `COPY (SELECT ... )` form (COPY query result to STDOUT)
   be in V1 scope? Many real-world uses of COPY TO are
   `COPY (SELECT * FROM t WHERE x > 100) TO STDOUT`. V1 plan: NO
   (only `COPY <table> TO STDOUT`), V2: the form lands in
   SP-PG-COPY-CSV which adds the same parser surface.
4. Should V1 emit a NoticeResponse when accepting a deprecated /
   ignored option (e.g. `WITH (FREEZE)`)? PG itself emits notices
   for such cases. V1 silently ignores; V2 may add notices.

## §12. References

- PostgreSQL §55.2.5 "COPY Operations":
  https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-COPY
- PostgreSQL §55.7 "Message Formats" — `CopyData`, `CopyDone`,
  `CopyFail`, `CopyInResponse`, `CopyOutResponse` shapes.
- PostgreSQL §SQL-COPY — the SQL command itself.
- libpq's `PQputCopyData` / `PQputCopyEnd` / `PQgetCopyData` —
  reference for what real clients send/receive.
- pgwire crate (Rust) — reference implementation of the protocol;
  V1 doesn't depend on it but cross-validates our byte shapes
  against its tests.
