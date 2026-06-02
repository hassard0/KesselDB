# SP-PG-COPY-ABORT-DONE-TAIL — drain CopyDone/CopyFail tail after a COPY abort — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T4 (2026-06-02).** Drain-flag in
`server::run_session` silently discards trailing `d`/`c`/`f` after
a `CopyDataOutcome::Failed` per PG §55.2.7. Defensive `08P01` for
stray `c`/`f` in pristine Idle preserved. vulcan psql 16 smoke
(`docs/superpowers/sppgcopyaborttail-t3-smoke-2026-06-02.txt`)
confirms zero spurious `unsupported message tag` lines AND a
single-session SELECT + bad COPY + SELECT round-trip completing
on the SAME connection. TaskList #383 ready for completion.

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgcopyaborttail-design.md`

Parent SP-arcs:
- SP-PG-COPY V1 (closed 2026-05-30 — text format COPY).
- SP-PG-COPY-CSV V1 (closed 2026-06-01 — CSV format).
- SP-PG-COPY-CSV-NUMERIC V1 (closed 2026-06-02 — NUMERIC validator)
  — the abort-tail bug was surfaced in this arc's smoke transcript
  as a pre-existing protocol-violation footnote independent of the
  validator landing.

## What this SP-arc ships

V1 = "an ErrorResponse mid-COPY leaves the connection alive and
silently drains any trailing CopyData / CopyDone / CopyFail frames
the client had already sent before observing the error, so the
next Query on the same connection works without reconnection."

1. `expecting_copy_tail: bool` local in `server::run_session`.
2. `process_copy_data` Failed-outcome branch sets the flag.
3. Idle-state pre-dispatch arm: when flag is set AND tag is
   `d`/`c`/`f`, silently consume the body and continue. `c`/`f`
   clears the flag; `d` keeps it set (more pre-error bytes may
   follow).
4. Defensive `c`/`f` rejection preserved when flag is false.

## Slice plan — ALL CLOSED

- **T1+T2 (commit `5c6156d`)** — design spec + progress tracker +
  drain flag in `server::run_session` + 5 KATs in
  `crates/kessel-pg-gateway/src/server.rs`. 924 pg-gateway lib
  tests pass (was 919 + 5 new).
- **T3 (commit `0ece79b`)** — vulcan psql 16 smoke confirms
  malformed-CSV COPY fires the existing 22023 batch-flush error
  with zero `unsupported message tag` lines in the gateway log,
  AND a single-session `SELECT 1` + bad `\copy` + `SELECT * FROM
  abort_smoke` completes all three statements on the SAME TCP
  connection. USAGE §9 documents the abort-tail drain shape.
- **T4 (this commit)** — STATUS row + tracker CLOSED + TaskList
  #383 ready.

## Out-of-scope (named, deferred)

- `SP-PG-COPY-ABORT-DONE-TAIL-PRE-AUTH` — abort drain during
  pre-auth phase (impossible in practice; client cannot enter
  COPY mode before AuthOk).
- `SP-PG T24` — protocol-level CancelRequest mid-COPY.

## KAT count target

+5 KATs in `crates/kessel-pg-gateway/src/server.rs`.
