# SP-PG-COPY-CSV — CSV format for COPY FROM/TO — SP-arc Progress Tracker

Date created: 2026-06-01
**Status: T1 IN-FLIGHT (this commit) — design spec + CSV codec
scaffold + parser KATs.**

Design spec: `docs/superpowers/specs/2026-06-01-kesseldb-sppgcopycsv-design.md`

Parent SP-arc: SP-PG-COPY (closed 2026-05-30, text format). Named as
a V2 follow-up in the SP-PG-COPY design spec §2.2 and progress
tracker §V2 follow-ups list — this is the realisation.

## What this SP-arc ships

V1 = "`pg_dump --csv` + `psql \copy ... CSV HEADER` end-to-end work
against KesselDB."

1. `WITH (FORMAT csv [, DELIMITER 'X'] [, QUOTE 'X'] [, ESCAPE 'X']
   [, NULL 'string'] [, HEADER])` parsed at the COPY command
   recognizer.
2. CSV-format `COPY ... FROM STDIN` decodes per-record + dispatches
   each row through the existing engine path (BULKAPPLY V1 batching
   still applies — CSV is just a different payload codec).
3. CSV-format `COPY ... TO STDOUT` emits a CSV record stream with the
   resolved delimiter/quote/null/header options.
4. Quoted-field semantics per RFC 4180 + PG superset: embedded
   delimiter/quote/newline → quoted; doubled-quote escape (or custom
   ESCAPE char); HEADER consumed on input + emitted on output.

## Slice plan (mirrors design spec §7)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + CSV codec module + command-parser option extension + dispatch-branch on format + CopyInState format field + KATs. | **IN PROGRESS** | (this commit) |
| **T2** | Real psql vulcan smoke + USAGE update + smoke transcript. | PENDING | — |
| **T3** | STATUS row + USAGE §9 expansion + progress tracker → CLOSED + TaskList #358 ready. | PENDING | — |

## Closure notes

(filled in at T3)
