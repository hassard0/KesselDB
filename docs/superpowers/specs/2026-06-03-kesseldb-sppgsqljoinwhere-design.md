# SP-PG-SQL-JOIN-WHERE — filtered inner joins (`JOIN … WHERE`) (design)

Date: 2026-06-03
Arc type: **FEATURE** (additive proto change + engine filter + SQL compile; gateway render reused).

## 1. Context

The relationship arc (SP-PG-ORM-RELATIONSHIPS, HEAD `b0da826`) shipped a
working **bare** inner equi-join: `SELECT a.name, b.title FROM a JOIN b ON
a.id = b.a_id` renders end-to-end through SQLAlchemy 2.0 (`query.join(Book)`).
But it is a BARE join — no `WHERE` filter on the combined rows.

Real ORM queries constantly filter joined results:

```python
session.execute(
    select(Author.name, Book.title)
    .join(Book, Author.id == Book.author_id)
    .where(Book.title == "lotr")
)
# → SELECT authors.name, books.title FROM authors JOIN books
#     ON authors.id = books.author_id WHERE books.title = $1
```

This is the single most common real-app join pattern beyond bare joins. The
`WHERE` predicate references columns from BOTH tables, qualified (`b.title`,
`a.name`), and filters the COMBINED join rows.

## 2. Path A (chosen) — filter in the engine

`Op::Join` gains an OPTIONAL filter program (`kessel-expr` bytecode) over the
**combined** join schema. The engine joins (unchanged), builds each combined
record `rec` against the combined `ObjectType` `cot` (left fields `a.<col>`
then right fields `b.<col>`, exactly as the existing `KTR1` self-describing
result already does), then runs `kessel_expr::eval(&filter, &cot, &rec)` and
keeps only matching combined rows.

`kessel-sql` compiles the qualified `WHERE` predicate against the combined
`(a-fields ++ b-fields)` field layout: `a.name` → the left field, `b.title` →
the right field. Same column name in both tables disambiguates by qualifier.

### Why Path A over Path B (post-join filter at the gateway)

Path B (engine returns ALL joined rows, gateway filters) pushes work to the
wrong layer, breaks the "engine returns the answer" model, and would need the
gateway to compile/run a predicate it doesn't own. Path A keeps one Op, one
deterministic answer, reuses the existing expr VM, and the gateway render is
**byte-untouched** (it just receives fewer combined rows).

## 3. Scope (V1)

- Inner equi-join + `WHERE` (the existing `ON a.x = b.y` equi-join).
- `WHERE` = AND/OR of comparisons over qualified columns from EITHER table
  (the existing `compile_where` → `or_expr`/`and_expr`/`cmp_expr` chain runs
  verbatim against the combined schema, so AND **and** OR/NOT/IN/BETWEEN/LIKE
  come free — see §5).
- Param in `WHERE` (`b.title = $1`): the gateway substitutes `$N` to a literal
  upstream (`extq/substitute.rs`) BEFORE kessel-sql sees the SQL, so the join
  WHERE compile only ever sees a literal — no new param plumbing.
- Both projection shapes (`SELECT a.c, b.c …` and `SELECT * …`) over the
  filtered join — render reused from the relationship arc.

## 4. V1 out of scope (named follow-ups)

- `SP-PG-SQL-JOIN-ORDERBY` — `JOIN … WHERE … ORDER BY / LIMIT` ordering of
  combined rows (LIMIT on a join already works pre-filter; post-filter ORDER
  BY over the combined schema is a separate render-side sort).
- `SP-PG-SQL-OUTER-JOIN` — LEFT/RIGHT/FULL OUTER (Op::Join is inner-only).
- `SP-PG-SQL-MULTI-JOIN` — 3+ table joins.
- `SP-PG-SQL-JOIN-AGG` — aggregates over a join.
- NOTE: OR/NOT/IN/BETWEEN/LIKE in the join-WHERE are NOT a follow-up — they
  fall out free because the combined schema is just an `ObjectType` and the
  full `compile_where` runs against it (validated by KAT).

## 5. Acceptance

`SELECT a.name, b.title FROM a JOIN b ON a.id=b.a_id WHERE b.title=$1`
returns ONLY the matching combined rows (e.g. only `(tolkien, lotr)` when
`a` has tolkien, `b` has (hobbit, lotr) both FK→tolkien, and `$1 = 'lotr'`).

## 6. Weak spots (anticipated + mitigations)

1. **Combined-schema field-offset resolution for the predicate.** The
   predicate must compile against the combined `(a ++ b)` field ids, NOT
   either single table's. Mitigation: kessel-sql builds the SAME synthetic
   combined `ObjectType` the engine builds (left fields `a.<col>` reassigned
   field_id `0..nL`, right fields `b.<col>` `nL..nL+nR`), then runs the
   existing `compile_where` against it. Engine and SQL build the combined
   schema by the identical recipe ⇒ field ids match by construction.

2. **Qualifier ambiguity — same column name in both tables.** Both tables
   may have `id`. A bare `WHERE id = 1` is ambiguous. Mitigation: the
   combined field name is `<table>.<col>`; the join-WHERE term resolver
   resolves `a.x` against `"a.x"` and `b.x` against `"b.x"`. A bare
   (unqualified) `col` in a join-WHERE matches by the unqualified suffix —
   if it is ambiguous (present in both), it is an error (`ambiguous column`).

3. **Param binding in join-WHERE.** `$1` is substituted to a literal by the
   gateway's extq substitute pass before kessel-sql parses; the join-WHERE
   sees `b.title = 'lotr'`. No new path. (psql direct uses literals already.)

4. **Predicate referencing only ONE table's cols.** `WHERE a.name = 'x'`
   (left only) or `WHERE b.title = 'y'` (right only) must both resolve. The
   combined schema carries both sides, so a single-side predicate is just a
   subset of the combined columns — resolves the same way.

5. **NULL handling.** A combined record may carry a NULL field (nullable
   column). `kessel_expr::eval` already defines NULL comparison semantics
   (NULL ≠ value ⇒ row excluded for `=`); the join filter inherits them
   verbatim — no join-specific NULL logic.

6. **Backward / determinism compatibility of the wire change.** `Op::Join`
   gains a trailing OPTIONAL filter (`Vec<u8>`, empty = no filter). Encoded
   ONLY when non-empty, so an older bare-join frame is a valid prefix that
   decodes to "no filter" — byte-identical behaviour for every existing join.
   The filter is a PURE function of the combined record + predicate bytes:
   same committed state + same Op ⇒ same kept rows in the same order
   (left-key order preserved; filter only drops, never reorders). seed-7 +
   3-replica determinism holds.

7. **Two apply sites.** `Op::Join` is applied in BOTH the main apply arm and
   the read-only `Op::Txn` bypass arm (identical bodies). The filter MUST be
   applied identically in both, or a join inside a txn would differ from a
   bare join. Mitigation: same filter call added to both arms; KAT exercises
   the main arm, determinism oracle exercises the replay path.

## 7. Determinism

The filter is applied per combined row, in the existing deterministic
left-key/right-scan order, dropping non-matches. It introduces no ordering,
no clock, no RNG. Same predicate + same state ⇒ same result. VSR seed-7
oracle + 3-replica convergence must stay green or the arc is BLOCKED.
