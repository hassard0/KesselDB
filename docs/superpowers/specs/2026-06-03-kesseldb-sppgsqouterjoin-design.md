# SP-PG-SQL-OUTER-JOIN — `LEFT [OUTER] JOIN` (design)

Date: 2026-06-03
Arc type: **FEATURE** (additive proto change + engine outer-join emit + SQL compile; gateway render REUSED unchanged).

## 1. Context

The relationship + filtered-join arcs (SP-PG-ORM-RELATIONSHIPS, SP-PG-SQL-JOIN-WHERE,
HEAD `769c99a`) shipped a working **INNER** equi-join with an optional combined-schema
`WHERE` filter. `Op::Join` is inner-only: a left row with no matching right row
produces NO output row.

Real ORMs emit `LEFT OUTER JOIN` for EVERY optional relationship. SQLAlchemy's
`User.posts` (a user may have zero posts) compiles to:

```python
select(User.name, Post.title).join(Post, User.id == Post.user_id, isouter=True)
# → SELECT users.name, posts.title FROM users LEFT OUTER JOIN posts
#     ON users.id = posts.user_id
```

A LEFT join returns EVERY left row; for left rows with no match the right
(`b.*`) columns are NULL. Without LEFT join, a user with no posts simply
vanishes from the result — wrong for any "list users and their (optional)
posts" query.

## 2. Semantics (V1)

`SELECT a.name, b.title FROM a LEFT [OUTER] JOIN b ON a.id = b.aid [WHERE …]`:

- Every `a` row is emitted.
- For each `a` row with ≥1 matching `b` row: one combined row per match
  (identical to INNER).
- For each `a` row with NO matching `b` row: exactly ONE combined row, with
  every `b.*` field NULL.
- `OUTER` is an optional noise word: `LEFT JOIN` ≡ `LEFT OUTER JOIN`.
- Composes with the existing combined-schema `WHERE` filter and projection.

## 3. Path (chosen) — outer-join in the engine

`Op::Join` gains a `join_type: JoinType { Inner, Left }` field (additive;
default `Inner` ⇒ existing behaviour, wire-byte-identical). The engine apply:

1. Builds the right-side hash map keyed by the join value (unchanged).
2. Builds the combined `ObjectType` `cot` (left fields `a.<col>`, right fields
   `b.<col>`) — UNCHANGED for `Inner`. For `Left`, every RIGHT field is marked
   `nullable` in `cot` so the codec accepts a NULL value (an unmatched left row
   encodes NULL `b.*`). Left fields and INNER mode are byte-identical.
3. Scans the left side in key order. For a matching left row: emit one combined
   record per right match (unchanged). For an unmatched left row IN LEFT MODE:
   emit one combined record = left values ++ `[Null; nR]`.
4. The optional `WHERE` filter runs on EVERY emitted combined record (matched
   and unmatched) exactly as before.

Applied IDENTICALLY in BOTH apply sites (the main apply arm and the read-only
`Op::Txn` bypass arm), so a LEFT join inside a read-only Txn matches a bare
LEFT join byte-for-byte.

### Why the gateway needs NO change

The combined `KTR1` record already carries a null bitmap. The gateway's
`render_join_result` decodes each combined record with `decode_record`, which
reads the bitmap and yields `None` for NULL cells; `encode_data_row` already
renders a `None` cell as the PG `i32 -1` NULL sentinel (covered by the existing
`t8_select_null_column_emits_negative_one_sentinel` KAT). So once the engine
sets the right-field NULL bits, NULL `b.*` columns render correctly with ZERO
gateway change. The only gateway-adjacent change is teaching `join_projection`
(the shape detector) to recognise the `LEFT [OUTER] JOIN` keyword so the result
is routed to `render_join_result`.

## 4. Wire change (additive, backward-compatible)

`Op::Join` already carries an OPTIONAL trailing length-prefixed `filter`
(SP-PG-SQL-JOIN-WHERE). We append a SECOND optional trailing field: a single
join-type tag byte, emitted ONLY when `join_type != Inner`. A LEFT join with no
filter writes an empty (len-0) filter ahead of the tag to keep the decode
unambiguous. Decode reads the filter (if bytes remain), then the tag (if bytes
remain) ⇒ `Inner` when absent. Net result:

- Every INNER join (filtered or not) is byte-IDENTICAL to the pre-arc frame
  (regression KAT `inner_join_no_filter_wire_byte_identical`: 17-byte frame).
- An older log frame decodes to `Inner`.
- An unknown tag is REJECTED at decode (forward-incompat op surfaced, not
  silently mis-applied) — KAT `unknown_join_type_tag_rejected`.

## 5. Scope (V1)

- `LEFT [OUTER] JOIN` (inner stays the default; bare `JOIN` ≡ INNER).
- Composes with the combined-schema `WHERE` filter + qualified/`*` projection.
- Both apply sites (main + RO-Txn bypass).

## 6. V1 out of scope (named follow-ups)

- `SP-PG-SQL-RIGHT-JOIN` — `RIGHT [OUTER] JOIN` (swap operands: a RIGHT join
  is a LEFT join with the tables exchanged; a thin compile-side rewrite, plus a
  `JoinType::Right` if we prefer to keep operand order).
- `SP-PG-SQL-FULL-JOIN` — `FULL [OUTER] JOIN` (needs BOTH-sides-unmatched: every
  unmatched left row with NULL right AND every unmatched right row with NULL
  left; the right side must track which keys were consumed).
- `SP-PG-SQL-MULTI-JOIN` — 3+ table / chained outer joins.
- `SP-PG-SQL-JOIN-ALIAS` — `FROM a AS x LEFT JOIN b AS y`.

## 7. Weak spots (anticipated + mitigations)

1. **NULL `b.*` field encoding.** The codec rejects a NULL in a non-nullable
   field (`NullInNonNullable`). A right column declared `NOT NULL` would make
   the unmatched-row encode fail. Mitigation: in LEFT mode the synthetic
   combined `cot` marks EVERY right field `nullable = true` (the join output is
   a derived view, not the base table — an unmatched right side is legitimately
   absent). INNER mode is unchanged.

2. **Combined-schema NULL marking must match the engine's emit.** The same
   recipe builds `cot` in BOTH apply arms; the nullability relaxation is part
   of that single recipe, so the emitted record's bitmap and the type the
   gateway decodes against agree by construction.

3. **LEFT JOIN + WHERE on a right (`b.*`) column.** PostgreSQL: a predicate on
   a NULL right column is UNKNOWN ⇒ the row is excluded, effectively turning the
   LEFT join back into an INNER join for that predicate. KesselDB inherits this
   for free: the filter runs on the unmatched (NULL-right) combined record, and
   `kessel_expr::eval` already drops a row whose compared field is NULL. KAT
   `left_join_emits_unmatched_left_with_null_right` asserts `… WHERE ord.amt =
   200` drops the orphan. (Documented: filter on the right side of a LEFT join
   removes unmatched rows — standard PG behaviour, surprising to some users.)

4. **`limit` accounting.** `limit` caps EMITTED rows. An unmatched left row
   counts as one emitted row, so it is subject to the same `n >= limit` guard
   before emit — no special case.

5. **Determinism of unmatched-row order.** Left rows are scanned in key
   (object-id) order; an unmatched left row is emitted at its position in that
   scan, deterministically interleaved with matched rows. No ordering, clock,
   or RNG. Same committed state ⇒ same combined-row sequence ⇒ seed-7 +
   3-replica convergence holds. The proto change is additive and INNER joins
   are wire-identical, so historical replay is unaffected.

6. **Shape detection across the front-end.** Three SQL front-end helpers key on
   the `JOIN` keyword to decide "single-table vs join shape": `join_projection`
   (gateway routing), and the two bare-projection detectors. All three are
   taught the `LEFT [OUTER]` prefix so a LEFT join is correctly classified as a
   join (not mis-read as a single-table select on table `a` with a stray
   `LEFT` token). KAT covers `join_projection` on both `LEFT JOIN` and
   `LEFT OUTER JOIN`.

7. **Two apply sites.** As with JOIN-WHERE, `Op::Join` is applied in the main
   arm and the RO-`Op::Txn` bypass arm. The LEFT-mode emit is added to BOTH,
   identically, so a LEFT join inside a read-only Txn equals a bare LEFT join.

## 8. Acceptance

`SELECT a.name, b.title FROM a LEFT JOIN b ON a.id = b.aid` over `a={1:tolkien,
2:orphan}`, `b={(aid 1, lotr)}` returns TWO rows: `(tolkien, lotr)` and
`(orphan, NULL)`. The orphan row carries the PG NULL sentinel for `b.title`.
Determinism oracle (VSR seed-7 + 3-replica) green.
