# SP-PG-SQL-JOIN-WHERE — filtered inner joins (`JOIN … WHERE`) — SP-arc Progress Tracker

Date created: 2026-06-03

**Status: CLOSED — DONE (2026-06-03).** A filtered inner equi-join
(`SELECT a.name, b.title FROM a JOIN b ON a.id = b.aid WHERE b.title = $1`)
now composes END-TO-END against KesselDB over the PG wire. `Op::Join` carries
an OPTIONAL `kessel-expr` filter program over the COMBINED `(a ++ b)` join
schema; the engine joins (unchanged), builds each combined record, runs the
predicate, and keeps only matching rows. kessel-sql compiles the qualified
`WHERE` against the same synthetic combined `ObjectType` the engine builds, so
field offsets line up by construction. The gateway render is byte-untouched
(it simply receives fewer rows). Determinism preserved (the filter is a pure
function of the combined record + predicate; no ordering / clock / RNG). No new
blocking gap surfaced; the named follow-ups are honest extensions. TaskList
ready for completion.

Design:           `docs/superpowers/specs/2026-06-03-kesseldb-sppgsqljoinwhere-design.md`
Smoke script:     `scripts/sppgsqljoinwhere-smoke.py`
Smoke transcript: `docs/superpowers/sppgsqljoinwhere-smoke-2026-06-03.txt`

## Slice plan

| T# | Scope | Status |
|---|---|---|
| **T1** | Design doc (Path A — filter in the engine; combined-schema offset resolution; 7 weak spots). | **DONE** |
| **T2** | proto — `Op::Join` gains an OPTIONAL trailing `filter: Vec<u8>` (emitted only when non-empty ⇒ byte-identical bare-join frame; older frame decodes to empty). | **DONE** (77cf7e0) |
| **T2** | engine (kessel-sm) — apply the combined-schema filter in BOTH the main apply arm and the RO-`Op::Txn` bypass arm (identical bodies). Non-matching rows dropped; `limit` caps emitted rows. | **DONE** (77cf7e0, fixup 76719c3 borrow `&[u8]` for `kessel_expr::eval`) |
| **T2** | kessel-sql — `combined_join_type()` + `compile_join_where()`: qualified `a.x`/`b.y` resolve by combined name, bare `col` by suffix with ambiguity error; the existing `compile_where` grammar then yields AND/OR/NOT/IN/BETWEEN/LIKE for free. | **DONE** (77cf7e0, generic fixup d1c3399) |
| **T3** | vulcan green (kessel-sql 127 / kessel-pg-gateway 1002 / kessel-sm 176 / kessel-vsr 28, incl. seed-7 oracle) + filtered-join smoke → **1 row** (`tolkien, lotr`). | **DONE** |
| **T4** | USAGE §9 + grammar rows, STATUS rows, CHANGELOG, this tracker → CLOSED, smoke transcript, vulcan cleanup, TaskList. | **DONE** (2199224 + this commit) |

## Verification (on vulcan @ 2199224)

- Unit/integration (release): kessel-sql **127 passed / 0 failed**
  (incl. `inner_equi_join`, `join_where_filters_combined_rows`,
  `join_where_bare_ambiguous_column_errors`,
  `join_projection_extracts_qualified_cols`), kessel-pg-gateway
  **1002 passed / 0 failed**, kessel-sm **176 passed / 0 failed**,
  kessel-vsr **28 passed / 0 failed**.
- Determinism: VSR seed-7 oracle
  `large_seed_corpus_is_deterministic_and_converges` → **PASS**. The filter
  is a pure function of the combined record + predicate bytes (left-key
  order preserved; filter only drops, never reorders) — seed-7 + 3-replica
  convergence holds.
- Smoke (psql direct, port 5551): bare join → 2 rows (`tolkien,hobbit`),
  (`tolkien,lotr`); `… WHERE b.title = 'lotr'` → **1 row** (`tolkien,lotr`).
- Gateway log clean; no ErrorResponse / panic.

No PG-wire / HTTP / WS / binary surface byte touched (the wire change is an
optional trailing field on `Op::Join`, absent for every existing bare join);
`#![forbid(unsafe_code)]` honored; no new external deps.

## Named follow-ups (out of scope this arc — honest extensions, no regression)

- `SP-PG-SQL-OUTER-JOIN` — LEFT/RIGHT/FULL OUTER JOIN (`Op::Join` is
  inner-only; an unmatched left row returns no row today). **← next arc.**
- `SP-PG-SQL-JOIN-ORDERBY` — `JOIN … WHERE … ORDER BY / LIMIT` post-filter
  ordering of combined rows.
- `SP-PG-SQL-MULTI-JOIN` — 3+ table joins / join chains.
- `SP-PG-SQL-JOIN-AGG` — aggregates over a JOIN.
- `SP-PG-SQL-JOIN-ALIAS` — `FROM a AS x JOIN b AS y`.
