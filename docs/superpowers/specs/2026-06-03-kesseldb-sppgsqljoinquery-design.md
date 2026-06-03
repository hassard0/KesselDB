# SP-PG-SQL-JOIN-QUERY — `JOIN … [WHERE] ORDER BY / LIMIT / OFFSET` (design)

Date: 2026-06-03
Arc type: **FEATURE** (additive proto change + engine sort/paginate over combined
join rows reusing SP23 logic + SQL compile; gateway render REUSED unchanged).

## 1. Context

The relationship / filtered-join / outer-join arcs (SP-PG-ORM-RELATIONSHIPS,
SP-PG-SQL-JOIN-WHERE, SP-PG-SQL-OUTER-JOIN, HEAD `4a2d235`) shipped a working
inner/left equi-join with an optional combined-schema `WHERE` filter. The join
returns the combined `KTR1` rows in left-key/right-scan order, capped by the
pre-existing `limit` field.

But there is no `ORDER BY` / `LIMIT` / `OFFSET` *over join results*. Every
paginated list view in every real app is:

```sql
SELECT a.name, b.title FROM a JOIN b ON a.id=b.aid [WHERE …]
ORDER BY b.created LIMIT 20 OFFSET 40
```

SP23 (`Op::SelectSorted`) already does ORDER BY / LIMIT / OFFSET over a SINGLE
table (sort by a field with a deterministic id tiebreak, then offset/limit).
This arc COMPOSES that sort/page machinery with the combined-row schema the join
already produces.

## 2. Semantics (V1)

`SELECT a.name, b.title FROM a JOIN b ON a.id=b.aid [WHERE pred] ORDER BY b.title
[DESC] [LIMIT n] [OFFSET m]`:

- The join produces combined rows (a-fields ++ b-fields), filtered by the
  optional `WHERE` (UNCHANGED).
- `ORDER BY <qualified col>` sorts the *combined* rows by ONE column from either
  table (`a.x` or `b.y`), `ASC` (default) or `DESC`.
- `LIMIT n` / `OFFSET m` paginate the *sorted* rows: skip `m`, then emit ≤ `n`.
- Sort is a TOTAL deterministic order: stable sort keyed by the sort column with
  a deterministic tiebreak (combined-row scan position — itself the deterministic
  left-key/right-scan order). NULL sort values (a LEFT-join unmatched right
  column) order NULLS LAST for ASC / NULLS FIRST for DESC — PostgreSQL's default.

## 3. Path (chosen) — sort+paginate in the engine, reuse SP23

`Op::Join` gains three OPTIONAL additive fields:

- `order_by: Option<(u16, bool)>` — `(sort field id in the COMBINED schema,
  desc)`. The sort field id is a reference into the combined `(a ++ b)` field
  layout (`b.title` → the b-side field offset).
- `limit_n: Option<u64>` — post-sort row cap.
- `offset_n: Option<u64>` — post-sort skip.

Engine apply (both apply sites via ONE shared helper `apply_join`):

1. Build the right-side hash map + the combined `ObjectType` `cot` (UNCHANGED;
   LEFT mode still marks b-fields nullable).
2. Scan the left side, build each combined `row: Vec<Value>` (matched →
   `lv ++ rv`; unmatched-left in LEFT mode → `lv ++ [Null; nR]`), run the
   optional `WHERE` filter (UNCHANGED).
3. **No `order_by`** → emit each surviving combined record into the `KTR1`
   stream immediately, capped by the legacy `limit` field — BYTE-IDENTICAL to
   the pre-arc bare/filtered/left join (regression-critical).
4. **`order_by` present** → COLLECT the surviving `(row, rec)` pairs into a Vec
   (in scan order = the deterministic tiebreak), STABLE-sort by the sort
   column's `Value` (NULL-aware, kind-aware comparator mirroring SP23
   `cmp_field` + the CHAR-pad trim), reverse for `DESC`, then `skip(offset_n)`
   + `take(limit_n)` and emit. The legacy `limit` field is set to 0 (unlimited)
   by the SQL compile whenever it emits `order_by`, so pagination lives entirely
   in `offset_n`/`limit_n` (no double-cap).

The sort comparator compares the combined `Value` at the sort index (decoded
already — the join builds `Vec<Value>` rows before encoding), so a NULL b-column
on a LEFT join is a first-class `Value::Null` and orders deterministically. This
mirrors SP23's `cmp_field` (numeric by kind, CHAR-pad-trimmed for byte kinds)
but on `Value` instead of raw record bytes — the SAME total order, NULL-extended.

### Why the gateway needs NO change

The result is still the same `KTR1` combined-row stream `render_join_result`
already decodes; it is merely sorted + paginated upstream. Zero gateway change.

## 4. Wire change (additive, backward-compatible)

`Op::Join` already carries two OPTIONAL trailing fields: a length-prefixed
combined-schema `filter`, then a single join-type tag byte (emitted only when
non-`Inner`). We append a THIRD optional trailing region — the "page block" —
guarded by a single marker byte so the decode stays unambiguous:

- The page block is emitted ONLY when `order_by | limit_n | offset_n` is `Some`.
- When emitted, it FORCES the filter (empty if none) and the join-type tag to be
  written first as positional anchors, then:
  `[u8 marker=1][u8 has_order][u16 field][u8 desc][u8 has_limit][u64][u8 has_offset][u64]`
  (each `u64` present only when its `has_*` flag is 1).
- A bare / filtered / left join with NO pagination writes NOTHING extra ⇒
  byte-IDENTICAL to the pre-arc frame (regression KATs
  `inner_join_no_filter_wire_byte_identical`, `left_join_no_filter_carries_tag`
  stay green).
- Decode: read filter (if bytes remain), read join-type tag (if bytes remain),
  then if bytes STILL remain read the marker (must be 1 ⇒ else reject) + the
  page fields. An older frame has no trailing bytes ⇒ `order_by=None`,
  `limit_n=None`, `offset_n=None`.

So every existing INNER/LEFT join (filtered or not, paginated-or-not pre-arc) is
wire-identical, and an old log frame replays to `None` page fields.

## 5. Scope (V1)

- `ORDER BY` ONE qualified column (`a.x` or `b.y`), `ASC`/`DESC`.
- `LIMIT` + `OFFSET` over the sorted combined rows.
- Composes with the combined-schema `WHERE` and inner/left join.
- Both apply sites (main + RO-Txn bypass), via one shared helper.

## 6. V1 out of scope (named follow-ups)

- `SP-PG-SQL-JOIN-ORDERBY-MULTI` — `ORDER BY` multiple columns.
- `SP-PG-SQL-JOIN-ORDERBY-EXPR` — `ORDER BY` an expression / computed key.
- `SP-PG-SQL-JOIN-AGG` — `GROUP BY` / aggregates over a join.
- `SP-PG-SQL-JOIN-NULLS-ORDER` — explicit `NULLS FIRST`/`NULLS LAST` override
  (V1 uses PG's default: NULLS LAST for ASC, NULLS FIRST for DESC).

## 7. Weak spots (anticipated + mitigations)

1. **Combined-schema sort-field resolution.** `ORDER BY b.title` must resolve to
   the b-side field id IN THE COMBINED `(a ++ b)` schema, not the b table's own
   id. Mitigation: kessel-sql builds the SAME synthetic combined field list the
   engine builds (left fields `a.<col>` ids `0..nL`, right fields `b.<col>` ids
   `nL..nL+nR`), resolves the qualified `ORDER BY` column against it, emits that
   combined field id. Engine resolves the sort field by the SAME id against the
   SAME combined `cot` ⇒ agree by construction. Unqualified `ORDER BY col`
   resolves by unqualified suffix; ambiguous (both tables) ⇒ error.

2. **Tiebreak determinism.** Two combined rows with equal sort keys must order
   deterministically. Mitigation: a STABLE sort over rows collected in the
   deterministic left-key/right-scan order ⇒ the tiebreak is the scan position,
   which is itself a pure function of committed state. No RNG, no clock, no hash
   iteration in the sorted output (the right hash map is only a probe; emit order
   follows the left scan + per-key right scan order). seed-7 + 3-replica holds.

3. **LIMIT/OFFSET on combined rows vs the legacy `limit` cap.** The pre-arc
   `limit` field caps emitted rows PRE-sort. Mixing it with post-sort pagination
   would double-cap / truncate the wrong rows. Mitigation: when kessel-sql emits
   `order_by`/page fields it sets the legacy `limit` to 0 (unlimited pre-sort) so
   the full result is sorted, THEN `offset_n`/`limit_n` paginate. A bare
   `JOIN … LIMIT n` with NO `ORDER BY` keeps using the legacy `limit` (pre-sort
   = first-n in scan order) — unchanged, and the SQL compile routes a no-ORDER-BY
   `LIMIT` to the legacy field for byte-identity.

4. **DESC.** Reverse AFTER the stable ascending sort, exactly as SP23
   (`rows.reverse()`), so a DESC sort over equal keys reverses the tiebreak order
   too — matching SP23's documented behaviour. (PG leaves ties arbitrary; we are
   deterministic, a stronger guarantee.)

5. **Sort field from the left vs right table.** The sort index is just a position
   into the combined `Vec<Value>` row; `a.x` (index `< nL`) and `b.y` (index
   `>= nL`) are handled uniformly by the same Value comparator. No left/right
   special case.

6. **NULL ordering for LEFT-join NULL fields.** A LEFT-join unmatched right
   column is `Value::Null`. The comparator orders `Null` GREATEST in the
   ascending base order (NULLS LAST for ASC); after the DESC reverse it becomes
   NULLS FIRST — PostgreSQL's default for `ORDER BY … DESC`. Documented; explicit
   `NULLS FIRST/LAST` is the named follow-up `SP-PG-SQL-JOIN-NULLS-ORDER`.

7. **Two apply sites.** `Op::Join` is applied in the main arm and the RO-`Op::Txn`
   bypass arm. The sort/paginate body is added ONCE in a shared `apply_join`
   helper called by both, so a paginated join inside a read-only Txn equals a
   bare paginated join byte-for-byte.

## 8. Acceptance

`SELECT a.name, b.title FROM a JOIN b ON a.id=b.aid ORDER BY b.title LIMIT 2`
over `a={1:tolkien}`, `b={(aid1,lotr),(aid1,hobbit),(aid1,silmarillion)}` returns
TWO rows sorted: `(tolkien, hobbit)`, `(tolkien, lotr)`. Determinism oracle (VSR
seed-7 + 3-replica) green.
