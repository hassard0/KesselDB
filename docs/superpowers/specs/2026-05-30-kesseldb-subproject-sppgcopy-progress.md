# SP-PG-COPY — PostgreSQL COPY FROM STDIN / COPY TO STDOUT — SP-arc Progress Tracker

Date created: 2026-05-30
**Status: CLOSED — V1 SHIPPED at T4 (2026-05-30).** Real psql 16.14
smoke on vulcan: `CREATE TABLE` + `COPY FROM STDIN` (3 rows) + `SELECT *`
+ `COPY TO STDOUT` (3 rows on the wire) round-trip byte-equal. NULL
round-trip via `\N` sentinel works. 1k-row ingest in 3.89s (~257
rows/sec — comparable to SP-PG-EXTQ INSERT loop; lifts in V2
`SP-PG-COPY-BULKAPPLY`). Binary / CSV / file / program variants reject
with precise V2-pointing `0A000` messages. Connection stays alive
across COPY rejections (matches SP-PG-EXTQ tolerant probe contract).
TaskList #350 ready for completion.

Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-sppgcopy-design.md`
Smoke transcript: `docs/superpowers/sppgcopy-t4-smoke-2026-05-30.txt`

Parent SP-arc: SP-PG (closed 2026-05-27, Simple Query) + SP-PG-EXTQ
(closed 2026-05-29, Extended Query). Both arc progress trackers named
SP-PG-COPY as a deferred V2 follow-up — this is the realisation.

## What this SP-arc ships

V1 = "`pg_dump` + sysbench-style + psql `\copy` bulk-load all work
against KesselDB." After V1 (T1..T5), a PG client speaking the
PostgreSQL Frontend/Backend protocol v3.0 can:

1. Send `Q` with `COPY <table> FROM STDIN [WITH (FORMAT text)]`;
   server replies `CopyInResponse` (`G`) advertising text-format +
   column count.
2. Stream one or more `CopyData` (`d`) frames containing
   newline-delimited tab-separated rows; the server parses each
   complete row and dispatches it as a synthetic
   `INSERT INTO <table> VALUES (...)` through the existing engine
   path (carry buffer handles partial trailing rows at frame
   boundaries).
3. End with `CopyDone` (`c`) → server emits `CommandComplete`
   (`COPY N`) + `ReadyForQuery` (`Z 'I'`).
4. Or abort with `CopyFail` (`f` payload = reason cstring) → server
   emits `ErrorResponse 57014 query_canceled` (with the client's
   reason in the message) + `ReadyForQuery`.
5. Send `Q` with `COPY <table> TO STDOUT [WITH (FORMAT text)]`;
   server emits `CopyOutResponse` (`H`) + N × `CopyData` (`d`) (one
   per row, payload = text-format row including trailing `\n`) +
   `CopyDone` (`c`) + `CommandComplete("COPY N")` + `ReadyForQuery`.
6. Coexist Simple Query + Extended Query + COPY arbitrarily on the
   same connection (each Q dispatch returns the connection to the
   Idle copy state).

**Out-of-scope (named, deferred — each is its own arc):**

- **SP-PG-COPY-BIN (V2)** — binary format (`WITH (FORMAT binary)`).
  ~2 slices. Detected at parse time; emits a precise V2-pointing
  `0A000` rejection today.
- **SP-PG-COPY-CSV (V2)** — CSV format with quoting / `HEADER` /
  custom delimiter. ~2 slices. Detected at parse time; emits a
  precise V2-pointing rejection today.
- **SP-PG-COPY-BULKAPPLY (V2)** — batched `Op::Txn` fold for
  10-50× throughput win + PG-compatible all-or-nothing atomicity.
  ~2 slices.
- **SP-PG-COPY-FILE (V2)** — `COPY ... FROM '/path/to/file'`. Hard
  pass without an opt-in operator surface (security).
- **SP-PG-COPY-PROGRAM (V2)** — `COPY ... FROM PROGRAM '...'`.
  Permanent hard pass.

## Slice plan (mirrors design spec §7)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec (~440 LoC, 10 weak-spots + 4 open questions + 5 V2 follow-up arcs named) + scaffold: `copy` submodule with `parse_copy_command` SQL recognizer (V1 + V2-rejected variants), `parse_text_row_bytes` + `encode_text_row` text-format codec (7 PG-canonical escapes + `\N` NULL + `\.` EOD marker), `encode_copy_in_response('G')` + `encode_copy_out_response('H')` + `encode_copy_data('d')` + `encode_copy_done('c')` byte-locked encoders + `decode_copy_fail('f')` helper + `copy_tag(N)` "COPY N" builder, `CopyState`/`CopyInState` per-connection state. All locked via 44 KATs. | **DONE** | `7a4768c` (spec + scaffold) |
| **T2** | COPY FROM STDIN end-to-end: `dispatch_copy_in_start` (table-lookup + column validation + 'G' CopyInResponse emit + state transition); `process_copy_data` (carry buffer + line splitting + per-row `INSERT INTO t [(cols)] VALUES (...)` synthesis through `dispatch::dispatch_query`); `finalize_copy_in_success` + `finalize_copy_in_failure`. Wired into `server::run_session`: CopyIn state branch BEFORE the existing tag match; Q-dispatch branch recognizes COPY before DISCARD / tx-control. Plus T2 fixes: schema-aware INSERT literal rendering (numeric kinds bare, string kinds quoted) and NULL handling via column-omit (kessel-sql has no NULL keyword in INSERT VALUES; rely on SP86 default-fill semantics for omitted nullable columns). | **DONE** | `9bb5db3` (dispatchers + run_session wiring + KATs) + `483fd11` (compile fix) + `52730ad` (schema-aware INSERT) + `a3ab0da` (struct literal fix) + `3a6557b` (NULL drops from INSERT) |
| **T3** | COPY TO STDOUT end-to-end: `dispatch_copy_to` runs the existing `SELECT * FROM <table>` path through `dispatch_query`, parses the DataRow frames out of the response, and reframes each row as a CopyData with text-format payload (tab-separated, `\N` for NULL, backslash-escape for the 7 special chars). Inline within the Q-dispatch — connection stays in Idle the whole time. Full reply sequence: `H` CopyOutResponse + N×`d` CopyData + `c` CopyDone + CommandComplete("COPY N") + RFQ. | **DONE** (shipped together with T2 in commit `9bb5db3`) | (folded into T2 commit) |
| **T4** | Real psql 16.14 smoke on vulcan + USAGE update + smoke transcript. CREATE TABLE + COPY FROM (3 rows + NULL row) + SELECT * + COPY TO (round-trip byte-equal). 1k-row throughput run (3.89s = ~257 rows/sec). Binary / CSV reject error paths verified to surface precise V2-pointer messages AND keep the session alive for follow-up Qs. | **DONE** | (smoke transcript + USAGE entry) |
| **T5** | Arc closure — STATUS.md row + USAGE §9 expansion + progress tracker → CLOSED + TaskList #350 ready. | **DONE** (this commit) | (final docs commit) |

## T1 — what landed (2026-05-30, commit `7a4768c`)

**One commit, +2032 LoC across 7 files:**

### Design spec (`docs/superpowers/specs/2026-05-30-kesseldb-sppgcopy-design.md`, 440 LoC):

- **§1 Context** — the failing pg_dump / sysbench / `\copy`
  workflows captured against pre-arc KesselDB; full 8-row ecosystem
  table (pg_dump / sysbench / psql `\copy` / psycopg2 `copy_from` /
  psycopg3 `copy` / JDBC `CopyManager` / asyncpg `copy_from_query` /
  asyncpg `copy_records_to_table`).
- **§2 Scope** — V1 in (8 items: COPY FROM STDIN + COPY TO STDOUT
  + text format only + connection state machine + per-row Op::Create
  dispatch + MAX_COPY_DATA_BUFFER cap + streaming row processing +
  zero-dep stance); V1 out (6 named V2+ arcs).
- **§3 State machine** — per-connection `CopyState ::= Idle |
  In(CopyInState)`; CopyIn carries `table` + `columns` +
  `column_count` + `column_kinds` + `carry: Vec<u8>` +
  `rows_ingested: u64`.
- **§4 COPY parser** — `parse_copy_command` SQL recognizer +
  `parse_text_row_bytes` + `encode_text_row` text-format codec.
  Edge case table (7 entries): NULL + empty + tab-in-field +
  newline-in-field + backslash-in-field + field-starts-with-`\N` +
  `\.` end-of-data marker.
- **§5 Memory + flow bounds** — `MAX_COPY_DATA_BUFFER = 16 MiB`
  (inherits `PG_MAX_MESSAGE_SIZE`); per-row dispatch in V1 (V2
  SP-PG-COPY-BULKAPPLY batches).
- **§6 Error semantics** — 8-row table mapping triggers to SQLSTATEs
  (`0A000`, `42P01`, `22023`, `08P01`, `54000`, `57014`).
- **§7 Task decomposition** — T1..T5 with per-slice KAT-delta
  estimates.
- **§8 Acceptance criteria** — 11 items including real psql
  ingest + export round-trip + memory bound + concurrent isolation.
- **§9 Self-review — 10 weak spots**: (1) per-row Op::Create slow at
  scale (lifts in V2 SP-PG-COPY-BULKAPPLY); (2) per-row SQL
  synthesis brittle for unusual unicode; (3) no mid-COPY transaction
  rollback (V1 atomicity gap vs PG); (4) no COPY FROM CSV (V2);
  (5) COPY TO loads all rows into memory before framing; (6) no
  CopyFail mid-COPY-TO support; (7) per-row alloc; (8) carry buffer
  + many tiny CopyData frames; (9) no `pg_dump --inserts` fallback
  test; (10) 16 MiB CopyData cap interacts with very wide rows.
- **§10 Out-of-scope hard passes** — PROGRAM (permanent); FILE
  (without operator opt-in); v2 `\.` marker as REQUIRED signal.
- **§11 Open questions** — per-row vs batched dispatch (V1: per-row);
  tolerant-survive vs strict-close on malformed CopyData (V1:
  tolerant); `COPY (SELECT ... )` query form (V1: NO); NoticeResponse
  for deprecated options (V1: silent).

### Scaffold (`crates/kessel-pg-gateway/src/copy/{mod,proto,text,command}.rs`):

- **mod.rs** — `CopyState { Idle, In(CopyInState) }` per-connection
  state; `CopyInState { table, columns, column_count, column_kinds,
  carry, rows_ingested }`; `MAX_COPY_DATA_BUFFER` lock vs
  `PG_MAX_MESSAGE_SIZE`. **4 + 1 KATs** (default Idle, In reports
  is_in true, cap matches PG_MAX, `CopyInState::new` initial values,
  `new_with_kinds` carries kinds).
- **proto.rs** — `encode_copy_in_response('G')` /
  `encode_copy_out_response('H')` byte-locked; `encode_copy_data('d')`
  / `encode_copy_done('c')` envelope shapes; `decode_copy_fail('f')`
  cstring reader; `copy_tag(N)` "COPY N" CommandComplete tag builder.
  **13 KATs** (G/H zero/two-col + G/H symmetry across 5 ncols, d/c
  envelopes, CopyFail decoder happy path + 4 error paths +
  zero-length, copy_tag canonical shape, tag-distinctness lock).
- **text.rs** — `parse_text_row_bytes` + `encode_text_row` codec
  handling 7 PG-canonical escapes + `\N` NULL + `\.` EOD marker.
  **13 KATs** (2-field round-trip, NULL sentinel, empty vs NULL
  distinction, 7-escape canonical decode, mid-field \N is literal,
  embedded-tab escape, field-count mismatch, trailing backslash,
  unknown escape, encode-2-field, encode-NULL, encode-special-chars,
  byte-vector round-trip property with 7 corpora + NULL mixed, EOD
  marker recognition).
- **command.rs** — `parse_copy_command` SQL recognizer (lenient on
  leading whitespace + comments + trailing `;`) covering V1-supported
  shapes + V2-only rejection variants. **14 KATs** (basic FROM STDIN
  / TO STDOUT, column list, case-insensitive verbs, trailing `;`,
  leading comments, explicit text accepted, binary/csv rejected with
  precise reason, file source / program source rejected, non-COPY
  SQL → None, COPY-in-string-literal NOT matched, quoted table name,
  WITH FORMAT among other options, WITH FREEZE silently accepted,
  unknown FROM target rejected, TO STDIN invalid).

### Test counts (release on vulcan, 2026-05-30 after T1):

| Surface | Before T1 | After T1 | Delta |
|---|---|---|---|
| `kessel-pg-gateway` lib | 511 | 555 | +44 |

`kessel-sim` seed-7 GREEN (3 / 3); CI green at commit
`7a4768c`; `#![forbid(unsafe_code)]` honored across all new modules.

## T2 + T3 — what landed (2026-05-30, commit `9bb5db3` + fix-ups)

**One main commit + 4 small fix-ups, +1356 LoC net:**

### Commit `9bb5db3` — dispatchers + run_session wiring + KATs

`crates/kessel-pg-gateway/src/copy/dispatch.rs` (NEW, ~600 LoC code +
~500 LoC KATs):

- **`dispatch_copy_in_start(parsed, engine) -> CopyInStartOutcome`**:
  validates table via `engine.describe_table` (42P01 if missing);
  validates supplied column list against the schema (42703 if any
  column missing); captures per-column `FieldKind` for the per-row
  INSERT-synthesizer; emits `CopyInResponse(G)` with the chosen
  column count; returns `CopyInState` for the caller to install.
- **`process_copy_data(data, state, engine) -> CopyDataOutcome`**:
  extends the carry buffer, splits on `\n` (tolerating `\r\n`);
  for each complete row: skip if it's the legacy `\.` end-of-data
  marker; parse via `parse_text_row_bytes` (22023 on field-count
  mismatch); synthesize `INSERT INTO <table> [(cols)] VALUES (...)`
  with kind-aware literal rendering (bare for numeric, quoted for
  string, NULL columns dropped); dispatch through
  `dispatch::dispatch_query`. If the dispatch response contains an
  ErrorResponse, surface it with a row-number-tagged message and
  abort the COPY (caller transitions back to Idle).
- **`finalize_copy_in_success(state)`** — emits
  `CommandComplete("COPY N") + RFQ`.
- **`finalize_copy_in_failure(reason)`** — emits
  `ErrorResponse 57014 + RFQ` with the client's CopyFail reason.
- **`dispatch_copy_to(parsed, engine) -> Vec<u8>`** (T3 inline):
  drives `SELECT * FROM <table>` through `dispatch_query`, parses
  the DataRow frames out of the response, reframes each row as a
  CopyData with text-format payload, emits the full reply sequence
  `CopyOutResponse + N×CopyData + CopyDone + CommandComplete("COPY N")
  + RFQ`.

`crates/kessel-pg-gateway/src/server.rs` (~142 LoC added):

- **CopyIn state branch** added BEFORE the existing tag match: when
  `copy_state.is_in()`, only `d`/`c`/`f`/`X` are valid; any other
  tag = 08P01 + state clear + stay alive.
- **Q-dispatch branch** updated to recognize COPY via
  `parse_copy_command` FIRST (before DISCARD / tx-control / generic
  dispatch_query). Routes ParsedCopy::From → dispatch_copy_in_start
  + state transition; ParsedCopy::To → dispatch_copy_to inline;
  ParsedCopy::Rejected → precise error via dispatch_copy_in_start
  failure path.

**+30 dispatch.rs KATs** (T2 dispatch_copy_in_start: 4; T2
process_copy_data: 8 — three-rows happy path, partial-row carry
across frames, NULL field drops, EOD marker skipped, field-count
mismatch with row number, engine error with row number, CRLF
tolerated, finalize success, finalize failure; T3 dispatch_copy_to:
3 — empty table, 3-row full sequence with payload byte-equality,
unknown table 42P01, NULL field emits \N).

**+6 server.rs integration KATs** (T2/T3/T4): full session-loop
COPY FROM (3 rows), CopyFail mid-stream → 57014 + alive, unknown
table 42P01 + no state change, binary format precise reject with
SP-PG-COPY-BIN pointer, COPY TO STDOUT (3 rows full sequence with
byte-equal first CopyData payload), stray CopyData in Idle =
unsupported message tag.

### Fix-ups

- `483fd11` — compile fix (dead `Vec` construction).
- `52730ad` — schema-aware INSERT literal rendering (numeric kinds
  bare, string kinds quoted). Threads per-column `FieldKind` through
  `CopyInState::new_with_kinds`. Adds +1 KAT.
- `a3ab0da` — missing `column_kinds` in literal CopyInState test.
- `3a6557b` — NULL columns dropped from synthesized INSERT
  (kessel-sql has no NULL keyword in VALUES; rely on SP86
  auto-NULL-fill for omitted nullable columns). Updates 2 KATs to
  match new behaviour.

### Test counts (release on vulcan, 2026-05-30 after T2+T3 + fix-ups):

| Surface | Before T1 | After T2+T3 (final) | Delta |
|---|---|---|---|
| `kessel-pg-gateway` lib | 511 | 587 | +76 |

`kessel-sim` seed-7 GREEN (3 / 3); CI green at every commit;
`#![forbid(unsafe_code)]` honored across all touched modules;
HTTP/1.1 + WS + binary + PG-wire-Simple + PG-wire-Extended surfaces
byte-untouched.

## T4 — real psql smoke on vulcan (2026-05-30)

**Setup**:
- Server: `kesseldb` built from commit `3a6557b` with
  `--features pg-gateway,http-gateway`.
- Listener: `127.0.0.1:5532` (PG-wire), `127.0.0.1:6532` (binary).
- Token: `admin`.
- Client: PostgreSQL 16.14 psql.

**Headline results** (full transcript:
`docs/superpowers/sppgcopy-t4-smoke-2026-05-30.txt`):

1. ✅ `CREATE TABLE copy_smoke (id BIGINT, name CHAR(32))` →
   `CREATE TABLE`.
2. ✅ `COPY copy_smoke FROM STDIN < 3-rows.tsv` → `COPY 3`.
3. ✅ `SELECT * FROM copy_smoke` → 3 rows with the right values.
4. ✅ `COPY copy_smoke TO STDOUT` → 3 rows on the wire
   byte-equal to the input.
5. ✅ NULL round-trip: `\N` on input → engine NULL → `\N` on output.
6. ✅ Round-trip via file: COPY TO STDOUT > file; COPY FROM STDIN <
   file → identical row set.
7. ✅ 1000-row ingest in 3.89s (~257 rows/sec — comparable to
   SP-PG-EXTQ INSERT loop at T8).
8. ✅ Binary format → `ERROR: COPY binary format not supported in
   V1 (SP-PG-COPY-BIN)`. Session stays alive for the next Q.
9. ✅ CSV format → `ERROR: COPY csv format not supported in V1
   (SP-PG-COPY-CSV)`. Session stays alive.

**Headline question — does `psql COPY FROM STDIN` work end-to-end
against the binary?** YES. Three rows ingested end-to-end via real
psql 16.14 against the running `kesseldb` binary; subsequent
`SELECT *` returns the rows; subsequent `COPY TO STDOUT` exports
them byte-equal.

**Throughput vs INSERT?** The V1 per-row dispatch pattern means COPY
and N×INSERT have the same throughput (~257 r/s on vulcan for a
small-row schema). V2 `SP-PG-COPY-BULKAPPLY` is where the headline
COPY-vs-INSERT win lands; V1 is the "make pg_dump / sysbench work
at all" milestone, not the throughput milestone.

## V2 follow-ups (each its own arc — listed here for the SP-PG
parent arc's running follow-up index)

- **`SP-PG-COPY-BIN`** — binary format (`WITH (FORMAT binary)`).
- **`SP-PG-COPY-CSV`** — CSV format with quoting + `HEADER` + custom
  delimiter.
- **`SP-PG-COPY-BULKAPPLY`** — per-batch Op::Txn fold for 10-50×
  throughput win + PG-compatible all-or-nothing atomicity.
- **`SP-PG-COPY-FILE`** — server-side file access (operator-opt-in
  only — security).
- **`SP-PG-COPY-PROGRAM`** — `COPY ... FROM PROGRAM '...'`
  (permanent hard pass).

## Closure notes

- TaskList #350 ready for completion.
- HTTP/1.1 + WS + binary + PG-wire-Simple + PG-wire-Extended
  surfaces byte-untouched.
- `#![forbid(unsafe_code)]` honored across the new `copy` submodule.
- Zero new external deps.
- USAGE.md §9 (PostgreSQL clients) updated with the COPY usage
  recipe + the V1 caveats + the named V2 follow-up arcs.
- STATUS.md current-capabilities header updated with the
  Track A.2 SP-PG-COPY entry.
