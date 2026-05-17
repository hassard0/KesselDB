# KesselDB Sub-project 32 — Index-accelerated SQL queries

**Date:** 2026-05-17  **Status:** spec + build. On-mission: *Postgres
flexibility at TigerBeetle speed* — SQL queries no longer always full-scan.

## Change

- New `Op::QueryRows { type_id, eq_preds, program, limit }`: the planner
  intersects the SP3 index for any `eq_preds` on indexed fields to narrow
  candidates; the full WHERE `program` (kessel-expr) then verifies every
  candidate, so the result is **identical to `Select`** — the index only
  accelerates (correctness never depends on the hint). Falls back to a full
  scan when no predicate is on an indexed field.
- `kessel-sql`: the restricted grammar
  `SELECT * FROM t [WHERE c=v [AND c=v]*] [LIMIT n]` compiles to
  `QueryRows`; index-hint bytes are encoded to exactly match the record's
  stored field bytes (so narrowing is correct). Anything outside it (OR,
  NOT, ranges, parens, ORDER/GROUP/projection) cleanly restores the cursor
  and falls back to the general scan planner — no behavior change there.

So `SELECT * FROM t WHERE owner = 100 AND kind = 2` is sub-linear when
`owner`/`kind` are indexed, and still exactly correct (the VM re-checks).

## Non-goals (honest)

Equality-only index use (range/OR index planning is future); `SELECT *`
restricted form only (projected/ordered/aggregate SQL still scans —
documented, layered atop existing correct paths).

## Tests

`kessel-sql::select_star_eq_compiles_to_query_rows_and_is_correct`
(compiles to QueryRows; OR falls back to Select; 4-row correctness;
multi-eq AND). `kessel-proto` round-trips. 123 tests total green.
