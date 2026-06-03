# SP-PG-ORM-RELATIONSHIPS — SQLAlchemy 2.0 multi-table FK relationships — SP-arc Progress Tracker

Date created: 2026-06-03

**Status: CLOSED — DONE (2026-06-03).** A real SQLAlchemy 2.0 **two-model
FK-relationship** workload (`Author` 1—N `Book`, declarative
`relationship()` + `ForeignKey`) now composes END-TO-END against KesselDB
over the PG wire: **4/4 stages PASS** on vulcan (FK DDL / cascade insert /
JOIN query / lazy-load navigation). Two surgical fixes, each unblocking a
major path. The relational core (foreign keys + inner equi-joins) works
through a real ORM. Determinism preserved. No NEW blocking gap surfaced; the
named follow-ups (FK enforcement, OUTER/filtered/multi joins) are honest
extensions, not regressions. TaskList ready for completion.

Smoke harness:    `scripts/sppgormrelationships-smoke.py`
Smoke transcript: `docs/superpowers/sppgormrelationships-smoke-2026-06-03.txt`
Design:           `docs/superpowers/specs/2026-06-03-kesseldb-sppgormrelationships-design.md`

## Slice plan

| T# | Scope | Status |
|---|---|---|
| **T1** | Design doc (validation arc, recon: FK DDL unwired + gateway has no JOIN render path). | **DONE** |
| **T2** | Fix A — kessel-sql accept-and-skips `FOREIGN KEY(col) REFERENCES tbl(col)` table-constraint + inline `REFERENCES …` column modifier + `ON DELETE/UPDATE` actions (`skip_referential_actions`). Compiles to byte-identical `CreateType`. | **DONE** (995b867, fixup 3e78512) |
| **T2** | Fix B — kessel-sql `join_projection()` recovers a JOIN's qualified projection; gateway `render_join_result` decodes the `KTR1` typed result + the embedded combined schema, maps the projection, emits projected DataRows. | **DONE** (995b867) |
| **T3** | vulcan smoke run → **4/4 PASS**; direct psql verification of both JOIN render shapes (qualified projection + `SELECT *`). | **DONE** |
| **T4** | USAGE §9 row, STATUS rows, this tracker → CLOSED, smoke transcript, vulcan cleanup, TaskList. | **DONE** |

## Verification (on vulcan @ 3e78512)

- Smoke: **4/4** (`create_all_fk_ddl`, `cascade_insert`, `join_query`, `lazy_load_nav`).
- Unit tests: kessel-sql **1002 passed / 0 failed** (incl. new
  `fk_table_constraint_ddl_parses`, `join_projection_extracts_qualified_cols`),
  kessel-pg-gateway **125 passed / 0 failed**.
- Determinism: VSR seed-7 oracle
  `large_seed_corpus_is_deterministic_and_converges` → **PASS** (Fix A
  compiles byte-identical, Fix B is pure render — no engine/Op change).
- Gateway log clean; no ErrorResponse / panic for any ORM-emitted statement.

No PG-wire / HTTP / WS / binary surface byte touched outside the new JOIN
SELECT render; `#![forbid(unsafe_code)]` honored; no new external deps.

## Named follow-ups (out of scope this arc — honest extensions, no regression)

- `SP-PG-DDL-FK-ENFORCE` — wire the parsed FK to engine `Op::AddForeignKey`
  + referential-integrity enforcement (V1 parse-and-skips at the DDL layer).
- `SP-PG-SQL-OUTER-JOIN` — LEFT/RIGHT/FULL OUTER JOIN (`Op::Join` is
  inner-only; an unmatched author returns no row today).
- `SP-PG-SQL-JOIN-WHERE` — `JOIN … WHERE` / filtered joins.
- `SP-PG-SQL-MULTI-JOIN` — 3+ table joins / join chains.
- `SP-PG-SQL-JOIN-ALIAS` — `FROM a AS x JOIN b AS y`.
- `SP-PG-SQL-JOIN-AGG` — aggregates over a JOIN.
