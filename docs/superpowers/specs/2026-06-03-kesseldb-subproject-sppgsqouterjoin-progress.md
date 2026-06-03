# SP-PG-SQL-OUTER-JOIN — `LEFT [OUTER] JOIN` — SP-arc Progress Tracker

Date created: 2026-06-03

**Status: CLOSED — DONE (2026-06-03).** `LEFT [OUTER] JOIN`
(`SELECT a.name, b.title FROM a LEFT JOIN b ON a.id = b.aid`) now composes
END-TO-END against KesselDB over the PG wire — the join every real ORM emits
for an OPTIONAL relationship (SQLAlchemy `isouter=True`). `Op::Join` gained a
`join_type` field (`Inner | Left`); LEFT mode emits EVERY left row, and a left
row with no matching right row comes back ONCE with all right (`b.*`) fields
NULL. The combined `KTR1` null bitmap carries the NULLs, so the gateway renders
the PG `i32 -1` NULL sentinel with ZERO render change. Determinism preserved.
No new blocking gap surfaced; the named follow-ups (RIGHT / FULL / MULTI) are
honest extensions. TaskList ready for completion.

Design:           `docs/superpowers/specs/2026-06-03-kesseldb-sppgsqouterjoin-design.md`
Smoke transcript: `docs/superpowers/sppgsqouterjoin-smoke-2026-06-03.txt`

## Slice plan

| T# | Scope | Status |
|---|---|---|
| **T1** | Design doc (engine outer-join; combined-schema NULL marking; LEFT+WHERE = PG semantics; additive 2-trailing-field wire; 7 weak spots). | **DONE** |
| **T2** | proto — `JoinType { Inner, Left }` + `Op::Join.join_type`; wire tag appended only when non-Inner (INNER byte-identical); unknown tag rejected at decode. | **DONE** (9aebf10) |
| **T2** | engine (kessel-sm) — LEFT mode in BOTH apply arms (main + RO-Txn bypass, identical): unmatched left row emits left ++ `[Null; nR]`; combined type marks right fields nullable in LEFT mode (INNER unchanged). | **DONE** (9aebf10) |
| **T2** | kessel-sql — parse `LEFT [OUTER] JOIN` → `Op::Join { join_type: Left }`; teach `join_projection` + the two bare-projection detectors the LEFT prefix. | **DONE** (9aebf10) |
| **T2** | gateway — NO change needed: combined-record NULL bitmap → `decode_record` `None` → `encode_data_row` i32 -1 sentinel (already wired + KAT-covered). | **DONE** (verified, no edit) |
| **T3** | vulcan green (kessel-proto 20 / kessel-sql 129 / kessel-pg-gateway 1002 / kessel-sm 176 / kessel-vsr 28, incl. seed-7 oracle) + LEFT JOIN smoke → **2 rows** incl. `(orphan, NULL)`. | **DONE** |
| **T4** | USAGE + grammar rows, STATUS row, CHANGELOG, this tracker → CLOSED, smoke transcript, vulcan cleanup, TaskList. | **DONE** |

## KATs (5 new)

- `kessel-proto::inner_join_no_filter_wire_byte_identical` — an INNER bare join
  encodes to the exact 17-byte pre-arc frame (no trailing tag) — REGRESSION.
- `kessel-proto::left_join_no_filter_carries_tag` — a LEFT join with no filter
  appends an empty filter (len-0) + the tag, round-trips to `Left`.
- `kessel-proto::unknown_join_type_tag_rejected` — a bogus tag fails decode.
- `kessel-sql::left_join_parses_to_left_join_type` — `LEFT JOIN` and
  `LEFT OUTER JOIN` → `Left`; bare `JOIN` → `Inner`; qualified-projection LEFT
  join parses; `join_projection` recognises both LEFT shapes.
- `kessel-sql::left_join_emits_unmatched_left_with_null_right` — INNER drops the
  orphan (2 rows); LEFT emits it (3 rows) with exactly one NULL-right row;
  `LEFT … WHERE ord.amt = 200` drops the orphan (PG semantics); `LEFT … WHERE
  usr.uid = 2` keeps the orphan.

(plus the pre-existing `Op::Join` round-trip set, now exercising both
`JoinType::Inner` and `JoinType::Left` fixtures, and the
`kessel-pg-gateway::t8_select_null_column_emits_negative_one_sentinel` NULL→-1
render KAT which the LEFT path reuses.)

## Verification (on vulcan @ 9aebf10)

- Unit/integration (release): kessel-proto **20 passed / 0 failed**,
  kessel-sql **129 passed / 0 failed** (was 127 → +2 OUTER-JOIN KATs),
  kessel-pg-gateway **1002 passed / 0 failed**, kessel-sm **176 passed /
  0 failed**, kessel-vsr **28 passed / 0 failed**.
- Determinism: VSR seed-7 oracle
  `large_seed_corpus_is_deterministic_and_converges` → **PASS**. Unmatched
  left rows emit at their position in the deterministic left-key scan; the
  proto change is additive and INNER joins are wire-identical, so replay is
  unaffected.
- Smoke (psql direct, port 5552): `SELECT a.name, b.title FROM a LEFT JOIN b
  ON a.id = b.aid` over `a = {1:tolkien, 2:orphan}`, `b = {(aid 1, lotr)}` →
  **2 rows**: `(tolkien, lotr)` and `(orphan, <NULL>)`.
- Gateway log clean; no ErrorResponse / panic.

No PG-wire / HTTP / WS / binary surface byte touched outside the additive
`Op::Join` join-type tag (absent for every INNER join); `#![forbid(unsafe_code)]`
honored; no new external deps.

## Named follow-ups (out of scope this arc — honest extensions, no regression)

- `SP-PG-SQL-RIGHT-JOIN` — `RIGHT [OUTER] JOIN` (a RIGHT join is a LEFT join
  with the operands swapped — a thin compile-side rewrite, or a
  `JoinType::Right` to keep operand order in the result schema).
- `SP-PG-SQL-FULL-JOIN` — `FULL [OUTER] JOIN` (needs BOTH-sides-unmatched:
  unmatched-left-with-NULL-right AND unmatched-right-with-NULL-left; the right
  side must track which keys a left row consumed).
- `SP-PG-SQL-MULTI-JOIN` — 3+ table / chained outer joins.
- `SP-PG-SQL-JOIN-ALIAS` — `FROM a AS x LEFT JOIN b AS y`.
