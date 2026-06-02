# SP-PG-COPY-ABORT-DONE-TAIL — drain CopyDone/CopyFail tail after a COPY abort — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: T1+T2 IN FLIGHT.** Drain-flag added to `server::run_session`;
silent-discard arm honors PG §55.2.7 tail semantics. Defensive
stray-`c`/`f` rejection preserved. T3 (vulcan smoke) + T4 (USAGE +
STATUS) close the arc.

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

## Slice plan

- **T1 (this commit)** — design spec + progress tracker + drain
  flag + 5 KATs in `crates/kessel-pg-gateway/src/server.rs`.
  Build green, all gateway tests pass.
- **T2 (same commit batch)** — KAT integration verified;
  in-process server-loop tests cover all 5 transitions.
- **T3** — vulcan psql 16 smoke:
  CREATE TABLE + COPY FROM with malformed CSV +
  follow-up SELECT works on the same server (no reconnect).
  Transcript at `docs/superpowers/sppgcopyaborttail-t3-smoke-2026-06-02.txt`.
- **T4** — USAGE §9 note + STATUS row + tracker CLOSED + TaskList
  #383 ready.

## Out-of-scope (named, deferred)

- `SP-PG-COPY-ABORT-DONE-TAIL-PRE-AUTH` — abort drain during
  pre-auth phase (impossible in practice; client cannot enter
  COPY mode before AuthOk).
- `SP-PG T24` — protocol-level CancelRequest mid-COPY.

## KAT count target

+5 KATs in `crates/kessel-pg-gateway/src/server.rs`.
