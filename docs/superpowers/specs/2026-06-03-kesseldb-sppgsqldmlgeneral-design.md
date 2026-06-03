# SP-PG-SQL-DML-GENERAL — general-WHERE UPDATE/DELETE + RETURNING — design

Date: 2026-06-03

Supersedes the named follow-ups `SP-PG-SQL-UPDATE-WHERE-GENERAL` and
`SP-PG-SQL-RETURNING-DML` from
`docs/superpowers/specs/2026-06-02-kesseldb-sppgsqlormparse-design.md`
(§6 weak-spot #6, §3 RETURNING note).

## 1. Context

SP-PG-SQL-ORM-PARSE made `SELECT` + `INSERT` ORM-complete and wired
`UPDATE`/`DELETE` **by primary key only**: the parser maps `WHERE id = n`
(qualifier stripped) to the existing id-based read-modify-write
(`Stmt::Update` → server RMW → `Op::Update`; `Op::Delete`). Any
non-`id` column, non-`=` operator, or multi-predicate WHERE returns a
precise "by-PK only (SP-PG-SQL-UPDATE-WHERE-GENERAL)" error.

Real apps + ORMs need arbitrary WHERE predicates and multi-row mutation:

```
UPDATE users SET active = false WHERE last_login < $1
DELETE FROM t WHERE status = 'expired'
UPDATE acct SET bal = 0 WHERE bal < 100 RETURNING *   -- optimistic concurrency
```

This arc closes the CRUD-with-predicates story: general WHERE on
UPDATE/DELETE, multi-row, atomic, plus `RETURNING cols | *`.

## 2. Path A vs Path B — decision: **Path A** (server-side scan → concrete Txn)

UPDATE/DELETE by arbitrary WHERE is a read-then-mutate. Two paths:

- **Path B** — a new engine op `Op::UpdateWhere`/`Op::DeleteWhere` that
  scans + evaluates the predicate + mutates each match inside `apply`,
  on every replica. One replicated op, atomic by construction.

- **Path A** — the SERVER runs the WHERE query (an existing read op),
  resolves the concrete matching-id list, then builds a concrete
  `Op::Txn` of per-id mutations and replicates THAT. This is the SP30 /
  SP84 pattern (UPDATE-by-id was already a server-side RMW that
  replicates a concrete follow-up op) generalized from 1 id to N ids.

**Path A chosen.** Rationale:

1. **Maximal reuse, zero engine/proto surgery.** Every primitive
   already exists and is proven:
   - `Op::QueryExpr { type_id, program }` scans the type's rows,
     evaluates the kessel-expr predicate per row, returns the matched
     object ids **already `sort_unstable()`-sorted** (deterministic
     order, `kessel-sm/src/lib.rs` ~L2550).
   - `Op::UpdateSet { type_id, id, sets }` does the full per-row RMW:
     decode → splice set-fields → re-encode → delegate to `Op::Update`,
     which runs triggers / NOT NULL / UNIQUE / FK / CHECK / index
     maintenance / overflow GC (`kessel-sm` ~L4062, ~L3983).
   - `Op::Delete { type_id, id }` does the full cascade closure + index
     maintenance (`kessel-sm` ~L4120).
   - `Op::Txn { ops }` wraps the inner mutations atomically
     (`begin_txn`/`commit`, read-your-writes overlay); any inner failure
     rolls the whole batch back (`Op::proto` L72-74, L844).
   No new `Op` variant, no new wire tag, no `kessel-proto` encode/decode
   change, no `kessel-sm` apply arm.

2. **Determinism is identical to the existing by-id path.** The
   replicated artifact is a CONCRETE `Op::Txn` carrying a fixed, sorted
   id list — a pure function of (committed state read on the
   leader, predicate). Same predicate over the same committed prefix →
   same matched-id set → byte-identical Txn → byte-identical apply on
   every replica. This is exactly the determinism story SP84 already
   ships for `UpdateSet`-inside-`Txn`; we only widen the id count.

3. **Constraint re-check + atomicity come for free.** Each inner
   `UpdateSet`/`Delete` runs the full constraint suite. A UNIQUE
   violation in ANY matched row aborts the whole `Op::Txn` → none
   applied (atomic), satisfying the acceptance KAT directly. No new
   constraint plumbing.

Path B would re-implement scan + predicate-eval + per-row
constraint/index/trigger maintenance inside a new apply arm and add a
wire variant — strictly more surgery for the SAME determinism guarantee.
Path A is the correct V1 (and likely permanent) shape.

### 2.1 The leader-read subtlety (why this is still deterministic)

The leader reads committed state to resolve the matched ids, then
replicates a concrete Txn. A replica replaying the log NEVER re-runs the
predicate scan — it applies the concrete Txn. So the matched-id
resolution is NOT part of the replicated state transition; only its
concrete output is. This is the same trust boundary as by-id UPDATE
(the leader reads the current record, then replicates the patched
`Op::Update`). No clock, no RNG, no replica divergence surface.

## 3. Scope

- `UPDATE t SET c1 = v1, c2 = v2 WHERE <general predicate>` — multi-row.
- `DELETE FROM t WHERE <general predicate>` — multi-row.
- `UPDATE/DELETE ... RETURNING cols | *` — the affected rows
  (post-mutation for UPDATE; the deleted rows for DELETE).
- The by-PK fast path (`WHERE id = n` → single `Op::Update`/`Op::Delete`)
  STAYS as-is (regression-locked); the general path only fires when the
  WHERE is not the by-PK shape.
- WHERE predicate grammar is the existing `compile_where` grammar (the
  same one SELECT uses): AND/OR/NOT, `=`/`<`/`<=`/`>`/`>=`/`<>`, IN,
  BETWEEN, IS [NOT] NULL — over a single table's columns (qualifier
  stripped, lenient).

## 4. V1 out of scope (named follow-ups)

- `UPDATE t SET ... FROM other` (update-with-join) — `SP-PG-SQL-UPDATE-FROM`.
- `DELETE FROM t USING other` — `SP-PG-SQL-DELETE-USING`.
- Correlated / scalar subqueries inside the WHERE — `SP-PG-SQL-WHERE-SUBQUERY`.
- `SET col = col + expr` (computed-from-current SET) — `SP-PG-SQL-SET-EXPR`;
  V1 SET RHS is a literal/param, identical to by-id UPDATE today.
- Index-narrowed predicate evaluation — V1 does a FULL SCAN via
  `Op::QueryExpr` (no `eq_preds`/`range_preds` planner narrowing).
  `SP-PG-SQL-DML-PLAN` (see §6.4).

## 5. Acceptance

- `UPDATE t SET x = $1 WHERE y = $2` mutates EVERY matching row
  atomically; CommandComplete tag = `UPDATE N` with the real N.
- `DELETE FROM t WHERE pred` removes every match; tag = `DELETE N`.
- `WHERE` matching 0 rows → `UPDATE 0` / `DELETE 0`, no state change.
- `UPDATE/DELETE ... RETURNING *` emits RowDescription + N DataRows.
- A UNIQUE-violating general UPDATE is rejected with ZERO rows applied
  (atomic).
- By-PK `WHERE id = n` UPDATE/DELETE compiles byte-identically to before
  (regression).
- seed-7 green + 3-replica byte-identity green.

## 6. Weak spots (named, not all fixed in V1)

1. **Full-scan predicate cost.** V1 routes the match-resolution through
   `Op::QueryExpr`, which scans EVERY row of the type and runs the
   expr-VM per row — O(rows), no index narrowing even when the predicate
   is `col = const` on an indexed column. Acceptable for V1 (correct,
   deterministic); the planner narrowing is `SP-PG-SQL-DML-PLAN` — reuse
   the `Op::QueryRows` `eq_preds`/`range_preds` machinery to pre-filter
   the candidate set before the row-by-row program filter.

2. **Index maintenance on multi-row update.** Each matched row's
   `Op::UpdateSet`→`Op::Update` runs `idx_maintain` for the (old, new)
   record pair, so equality/ordered/composite indexes are updated for
   every mutated row. Covered by reuse; the KAT
   `update_indexed_column_where` locks it.

3. **Constraint re-check per row + atomic rollback.** Each inner op runs
   NOT NULL / UNIQUE / FK / CHECK. Because all inner ops ride one
   `Op::Txn`, a violation anywhere rolls back the WHOLE statement (none
   applied). Covered; KAT `update_where_unique_violation_atomic`.

4. **Trigger firing per matched row.** `Op::Update` runs `run_triggers`
   per row, so before-write triggers fire once per mutated row (PG
   semantics for row-level triggers). Covered by reuse.

5. **Determinism of scan order.** `Op::QueryExpr` sorts the matched ids
   (`sort_unstable`) before returning, so the inner-op order in the Txn
   is a deterministic function of the id set, independent of storage
   iteration order. The RETURNING row order follows this sorted id order
   (documented; PG makes no RETURNING-order promise without ORDER BY).

6. **RETURNING * column expansion.** `RETURNING *` expands to every table
   column in declared order (reusing the `insert_returning` star-sentinel
   + `describe_table` expansion the gateway already has). A RETURNING
   list that names a non-existent column errors. Explicit `RETURNING c1,
   c2` projects those columns; `RETURNING expr`/aggregates are out
   (`SP-PG-SQL-RETURNING-EXPR`).

7. **Large match sets.** A WHERE matching very many rows builds one big
   `Op::Txn`. V1 has no chunking; an unbounded `DELETE FROM t` (no WHERE)
   or a predicate matching the whole table builds an N-op Txn. The
   `Op::Delete` cascade budget (200_000) still applies per inner op.
   Chunked/streaming large-DML is `SP-PG-SQL-DML-CHUNK`.

## 7. Implementation shape

### 7.1 kessel-sql (T3)

Two new `Stmt` variants carry the resolved-at-execute pieces:

```rust
Stmt::UpdateWhere { type_id, program: Vec<u8>, sets: Vec<(u16, Value)>, returning: Option<Vec<String>> }
Stmt::DeleteWhere { type_id, program: Vec<u8>, returning: Option<Vec<String>> }
```

- `program` is the kessel-expr predicate from `compile_where` (the same
  bytes SELECT/QueryExpr consume).
- `returning` is `None` (no clause), `Some(["*"])` (star sentinel — reuse
  the `insert_returning` convention), or `Some([col, ...])`.
- Parse: in `compile_stmt_from_tokens` UPDATE arm and the DELETE arm,
  attempt the by-PK fast path FIRST (`parse_where_id_eq`); on its
  "general WHERE" error, restore the cursor and take the general path:
  `compile_where` the predicate, then parse optional `RETURNING`.

### 7.2 server (T2)

In each `Stmt::Update{,Where}` dispatch site (lib.rs simple `0xFE`
path, lib.rs `PARAMETERIZED_SQL_TAG` path, cluster.rs VSR path):

1. `Op::QueryExpr { type_id, program }` → `OpResult::Got(ids_bytes)`
   (concatenated 16-byte sorted ids).
2. Build `Op::Txn { ops }` where each op is
   `Op::UpdateSet { type_id, id, sets }` (UPDATE) or
   `Op::Delete { type_id, id }` (DELETE).
3. If RETURNING: for UPDATE, apply the Txn then read back each id
   (post-mutation record); for DELETE, read each record BEFORE the Txn
   (pre-delete). Frame the affected rows.
4. Return the count + (optional) rows. Surfaced via `OpResult::Got` with
   a DML-result frame `[u32 affected][u32 nrows][u32 reclen][rec]...]`
   (nrows=0 when no RETURNING). The gateway's UPDATE/DELETE keyword
   routing decodes this frame (distinct from the SELECT `Got` path).

This keeps the replicated artifact a concrete `Op::Txn`; the framing is a
read-side concern on the leader, not part of the state transition.

### 7.3 gateway (T4)

- `apply_sql_with_count` override on `EngineHandle`: for an UPDATE/DELETE
  whose result is the DML-result frame, decode `affected` for the
  CommandComplete tag (`UPDATE N` / `DELETE N`).
- RETURNING: when the SQL has a RETURNING clause, decode the framed rows
  and emit RowDescription + N DataRows (reuse the `render_insert_returning`
  column-expansion + decode helpers).

## 8. Execution

5-8 commits: T1 design, T2 server scan→Txn + KATs, T3 kessel-sql general
WHERE + RETURNING parse + KATs, T4 gateway count + RETURNING render +
KATs, T5 vulcan smoke, T6 closure. All cargo on vulcan with
`CARGO_TARGET_DIR=/tmp/kdb-t-dml`. Direct commits to main; CI green is the
gate. Determinism: seed-7 + 3-replica byte-identity green or BLOCKED.
