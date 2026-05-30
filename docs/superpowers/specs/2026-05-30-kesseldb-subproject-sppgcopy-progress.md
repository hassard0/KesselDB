# SP-PG-COPY — PostgreSQL COPY FROM STDIN / COPY TO STDOUT — SP-arc Progress Tracker

Date created: 2026-05-30
**Status: IN PROGRESS — T1 scaffold + design landing.**

Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-sppgcopy-design.md`

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
   same connection (each `Sync` boundary returns the connection to
   the Idle copy state).

**Out-of-scope (named, deferred — each is its own arc):**

- **SP-PG-COPY-BIN (V2)** — binary format. ~2 slices.
- **SP-PG-COPY-CSV (V2)** — CSV format with quoting / `HEADER` /
  custom delimiter. ~2 slices.
- **SP-PG-COPY-BULKAPPLY (V2)** — batched Op::Txn fold for 10-50×
  throughput win. ~2 slices.
- **SP-PG-COPY-FILE (V2)** — `COPY ... FROM '/path/to/file'`. Hard
  pass without an opt-in operator surface (security).
- **SP-PG-COPY-PROGRAM (V2)** — `COPY ... FROM PROGRAM '...'`.
  Permanent hard pass.

## Slice plan (mirrors design spec §7)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + scaffold: `copy` submodule with `parse_copy_command`, `parse_text_row_bytes`, `encode_text_row`, `encode_copy_in_response`, `encode_copy_out_response`, `encode_copy_done` byte-locked encoders; `CopyState` enum with `Idle` / `In` variants; recognize-COPY interceptor stubs; KATs locking spec invariants. | **IN PROGRESS** | — |
| **T2** | COPY FROM STDIN dispatcher + connection state + KATs. | **PLANNED** | — |
| **T3** | COPY TO STDOUT dispatcher + KATs. | **PLANNED** | — |
| **T4** | Real psql smoke on vulcan + USAGE update. | **PLANNED** | — |
| **T5** | STATUS + arc closure. | **PLANNED** | — |

Estimated KAT delta: +15-25.

## Standing rules

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-pgcopy`
- Direct commits to main, no Co-Authored-By
- HTTP/1.1 + WS + binary + PG-wire-Simple + PG-wire-Extended surfaces byte-untouched
- `#![forbid(unsafe_code)]` honored
- No new external deps
- CI green at every commit
