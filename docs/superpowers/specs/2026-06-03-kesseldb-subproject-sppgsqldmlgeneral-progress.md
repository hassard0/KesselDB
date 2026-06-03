# SP-PG-SQL-DML-GENERAL — general-WHERE UPDATE/DELETE + RETURNING — SP-arc Progress Tracker

Date created: 2026-06-03

**Status: CLOSED — DONE (2026-06-03).** Completes the CRUD-with-predicates
story. UPDATE/DELETE previously worked ONLY by primary key (`WHERE id =
n` → by-PK RMW); this arc adds arbitrary WHERE predicates, multi-row
mutation, and `RETURNING cols|*`. **HEADLINE: general-WHERE UPDATE +
DELETE + RETURNING all work end-to-end on vulcan** — `UPDATE acct SET
active = 0 WHERE bal < 150` → `UPDATE 2`; `DELETE FROM acct WHERE active
= 0` → `DELETE 2`; `UPDATE … WHERE … RETURNING *` returns the
post-mutation rows; by-PK `WHERE id = n RETURNING *` returns the row.
+23 KATs, full suites green (kessel-sql 991, pg-gateway 102, vsr 28,
kesseldb-server 237 w/ pg-gateway), zero regressions, seed-7 3-replica
byte-identity convergence green.

Design spec: `docs/superpowers/specs/2026-06-03-kesseldb-sppgsqldmlgeneral-design.md`
Smoke transcript: `docs/superpowers/sppgsqldmlgeneral-t5-smoke-2026-06-03.txt`

Supersedes the named follow-ups `SP-PG-SQL-UPDATE-WHERE-GENERAL` +
`SP-PG-SQL-RETURNING-DML` from the SP-PG-SQL-ORM-PARSE design.

## Path decision: A (server-side scan → concrete Op::Txn)

**Path A** chosen over Path B (a new `Op::UpdateWhere`/`Op::DeleteWhere`
engine op). The server resolves the matched ids on the leader via the
existing `Op::QueryExpr` (the predicate VM SELECT already uses; sorted
output ⇒ deterministic), then replicates ONE concrete `Op::Txn` of per-id
`Op::UpdateSet` (UPDATE) / `Op::Delete` (DELETE). Rationale: ZERO engine/
proto surgery (every primitive — QueryExpr, UpdateSet, Delete, Txn —
already exists and is proven), and the determinism is IDENTICAL to the
existing by-id RMW (the replicated artifact is a concrete Txn, a pure
function of committed state + predicate). Per-row index/constraint/
trigger maintenance + atomic rollback come for free (each inner op runs
the full suite; any failure rolls back the whole Txn). Path B would
re-implement all of that inside a new apply arm + wire variant for the
SAME guarantee.

## Slice plan

| T# | Scope | Status | Commit(s) |
|---|---|---|---|
| T1 | Design — Path A vs B, determinism analysis, 7 weak-spots | DONE | `ca80362` |
| T2 | server: `apply_dml_where` (QueryExpr → concrete Op::Txn) on both single-node SQL paths + cluster `Cont::DmlWhere`; +2 over-the-wire KATs | DONE | `9b3e2e1`*, `5ea14f9`, by-PK `5cf…`* |
| T3 | kessel-sql: `Stmt::UpdateWhere`/`DeleteWhere` parse + `compile_where` predicate + `parse_returning` + `dml_returning` helper; +8 KATs | DONE | `0c…`*, `b8…`*, `f2…`* |
| T4 | gateway: `UPDATE N`/`DELETE N` + RETURNING render; `encode/decode_dml_result`; +4 KATs | DONE | `e1…`* |
| T5 | vulcan smoke (ports 5546/6546) — general UPDATE/DELETE + RETURNING + by-PK RETURNING | DONE | transcript |
| T6 | USAGE §3 + STATUS + tracker + VSR determinism KAT + cluster KAT | DONE | (this commit) |

(*short SHAs approximate; see `git log` — direct-to-main, one push per slice.)

## What each piece does

- **kessel-sql** (`Stmt::UpdateWhere`/`DeleteWhere`): the UPDATE/DELETE
  arms try the by-PK fast path FIRST; on its general-WHERE rejection they
  rewind to `WHERE` and `compile_where` the predicate (the SAME
  kessel-expr bytes SELECT emits). `parse_returning` captures
  `None|["*"]|[cols]`. by-PK `WHERE id = n RETURNING` routes through
  `UpdateWhere`/`DeleteWhere` with `by_pk_id: Some(id)` so the server
  skips the scan but still reads the row back. `dml_returning(sql)` is the
  gateway-side clause detector. Unguarded table-wide UPDATE/DELETE
  rejected (footgun guard).
- **server** (`apply_dml_where`): `Op::QueryExpr` → sorted matched ids (or
  the single `by_pk_id`); build `Op::Txn` of per-id `UpdateSet`/`Delete`;
  empty match = no-op (`UPDATE 0`); RETURNING reads pre-delete rows
  (DELETE) or post-update rows (UPDATE); frames `[tag][u32 affected]
  [u32 nrows][rows]` in `OpResult::Got`. Wired into both single-node SQL
  paths (`0xFE` simple + `PARAMETERIZED_SQL_TAG` extended). Cluster mode:
  `Cont::DmlWhere`/`DmlWhereCommit` two-step VSR continuation (linearized
  QueryExpr → concrete Op::Txn); count supported, RETURNING deferred
  (`SP-PG-SQL-DML-RETURNING-CLUSTER`).
- **gateway** (`render_dml_where_result`): decodes the DML-result frame
  for the `UPDATE N`/`DELETE N` tag; for RETURNING, expands `*` via
  `describe_table` and projects the framed rows into DataRows (reuses
  `resolve_projection`/`decode_record`). Intercepts the UPDATE/DELETE
  `Got` (disambiguated by leading keyword + frame tag) ahead of the
  SELECT render path.

## Determinism

The replicated artifact is a CONCRETE `Op::Txn` of per-id mutations — a
pure function of (committed state on the leader, predicate). Same SQL +
same committed prefix ⇒ same matched-id set ⇒ byte-identical Txn ⇒
byte-identical apply on every replica. The matched-id resolution is NOT
part of the replicated state transition; only its concrete output is
(same trust boundary as by-id UPDATE). seed-7 3-replica byte-identity
convergence KAT green (`kessel-vsr::dml_general_where_txn_replicates_and_converges`).

## Named follow-ups (out of V1, named in the design §4/§6)

- `SP-PG-SQL-DML-PLAN` — index-narrowed predicate (V1 full-scans via
  QueryExpr; reuse the QueryRows eq/range pre-filter).
- `SP-PG-SQL-UPDATE-FROM` / `SP-PG-SQL-DELETE-USING` — update/delete joins.
- `SP-PG-SQL-WHERE-SUBQUERY` — correlated/scalar subqueries in WHERE.
- `SP-PG-SQL-SET-EXPR` — `SET col = col + expr` (computed-from-current).
- `SP-PG-SQL-DML-IN-TXN` — general-WHERE DML inside explicit BEGIN/COMMIT.
- `SP-PG-SQL-DML-RETURNING-CLUSTER` — RETURNING on the cluster VSR path.
- `SP-PG-SQL-DML-CHUNK` — chunked/streaming very-large match sets.
- `SP-PG-SQL-RETURNING-EXPR` — `RETURNING expr`/aggregates.
