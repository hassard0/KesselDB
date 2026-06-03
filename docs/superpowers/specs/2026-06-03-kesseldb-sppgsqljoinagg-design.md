# SP-PG-SQL-JOIN-AGG — `GROUP BY` + aggregate over a JOIN (design)

Date: 2026-06-03
Arc type: **FEATURE** (additive proto change + engine group-aggregate over combined
join rows reusing the SP22 / SP-Analytic-Plan-MULTI aggregator + SQL compile +
NEW gateway render for the join-group-aggregate result).

## 1. Context

The relationship / filtered / outer / paginated join arcs (SP-PG-ORM-RELATIONSHIPS,
SP-PG-SQL-JOIN-WHERE, SP-PG-SQL-OUTER-JOIN, SP-PG-SQL-JOIN-QUERY, HEAD `bedf22a`)
shipped a working inner/left equi-join with optional combined-schema `WHERE`,
`ORDER BY`, `LIMIT`, `OFFSET`. The join emits the combined `(a ++ b)` rows.

But there is no `GROUP BY` + aggregate *over join results* — the dashboard /
reporting query every real app runs:

```sql
SELECT a.name, COUNT(b.id) FROM a JOIN b ON a.id = b.aid GROUP BY a.name
```

"count (or sum / min / max / avg) the related rows per parent". SP22
(`Op::GroupAggregate`) + SP-Analytic-Plan-MULTI (`Op::GroupAggregateMulti`)
already group + aggregate over a SINGLE table. This arc COMPOSES that
group-aggregate machinery with the combined join rows the join already produces.

## 2. Semantics (V1)

`SELECT a.name, COUNT(b.id) [AS cnt] FROM a JOIN b ON a.id=b.aid [WHERE pred]
GROUP BY a.name [ORDER BY …] [LIMIT …]`:

- The join produces combined rows (a-fields ++ b-fields), filtered by the
  optional `WHERE` (UNCHANGED).
- `GROUP BY <qualified col>` groups the combined rows by ONE column (a-side or
  b-side, a reference into the combined schema).
- The projection is `[<group col>,] <agg>(…)+` — leading group column(s) (V1: the
  single GROUP BY column) followed by one or more aggregates over combined-row
  fields. `COUNT(*)` = group size; `COUNT(b.id)` = count of non-NULL b.id in the
  group; `SUM/MIN/MAX/AVG(col)` fold the non-NULL numeric values.
- One result row per group: `(group_key, agg_value+)`, groups in ascending
  group-key order (BTreeMap — deterministic).

### LEFT-join NULL semantics (PG-accurate, documented)

For a LEFT join, an unmatched parent emits ONE combined row with all `b.*` = NULL.
- `COUNT(b.id)` over that group counts **0** (the NULL b.id is not counted) —
  PostgreSQL `COUNT(<col>)` ignores NULLs.
- `COUNT(*)` counts **1** (the combined row exists) — PostgreSQL `COUNT(*)` counts
  rows. This is the classic LEFT-JOIN-COUNT gotcha; we match PG exactly.
- `SUM/MIN/MAX/AVG(b.col)` skip the NULL ⇒ an all-unmatched group yields SUM 0,
  MIN/MAX/AVG over the empty set 0 (mirrors the single-table `Op::GroupAggregate`
  empty-fold convention).

## 3. Path (chosen — Option A) — group-aggregate in the engine, reuse SP22 fold

`Op::Join` gains ONE more OPTIONAL additive field:

```
group_aggregate: Option<JoinGroupAgg>
JoinGroupAgg { group_field: u16, aggregates: Vec<(u8, u16)> }
```

both `group_field` and each aggregate's `field_id` are references into the
COMBINED `(a ++ b)` schema (left fields `a.<col>` ids `0..nL`, right fields
`b.<col>` ids `nL..nL+nR`) — the SAME ids kessel-sql resolves and the engine's
combined `cot` carries. `aggregates` is `Vec<(kind, field_id)>` with the canonical
kind codes (0 COUNT / 1 SUM / 2 MIN / 3 MAX / 4 AVG); for COUNT the field id 0 is
ignored only for `COUNT(*)`, but `COUNT(b.id)` carries the real field id and the
engine NULL-checks it (see §2).

Engine apply (both apply sites via the shared `apply_join` helper):

1. Build the right-side hash map + combined `cot` (UNCHANGED).
2. Scan the left side, build each combined `row: Vec<Value>`, run the optional
   `WHERE` filter (UNCHANGED).
3. **No `group_aggregate`** → the existing emit/sort/paginate path (BYTE-IDENTICAL
   to a pre-arc bare / filtered / left / paginated join — regression-critical).
4. **`group_aggregate` present** → instead of emitting join rows, fold the
   surviving combined `Vec<Value>` rows into a `BTreeMap<group-key-bytes, Acc>`:
   - group key = the group field's Value encoded to its fixed-width raw bytes
     (so the BTreeMap orders groups deterministically, ascending — matching
     `Op::GroupAggregate`'s contract). The group column is a non-NULL a-side (or
     a matched b-side) field in practice; a NULL group key sorts as its raw zero
     bytes (deterministic, documented edge).
   - per aggregate slot, fold the combined-row Value at the agg field id:
     COUNT(*) (field convention, "count every row") increments unconditionally;
     COUNT(col) increments only when the Value is non-Null; SUM/MIN/MAX/AVG fold
     the numeric value (Uint→i128 / Int→i128) only when non-Null.
   - emit `[u32 ngroups]` then per group `[u32 keylen][key][16B i128 LE × n_aggs]`
     — the SAME result encoding `Op::GroupAggregateMulti` uses, so it is a known,
     test-anchored shape.

The fold runs over the DECODED combined `Vec<Value>` rows (not raw bytes) so a
LEFT-join NULL b-column is a first-class `Value::Null` and the NULL semantics in
§2 fall out directly. Determinism: BTreeMap ascending key order + an associative
per-slot fold over rows visited in the deterministic left-key/right-scan order ⇒
a byte-identical result on every replica. No RNG, no clock, no hash-iteration in
the output order.

### Distinguishing COUNT(*) from COUNT(col)

`(kind=0, field_id=COUNT_STAR_SENTINEL=u16::MAX)` ⇒ COUNT(*) (count rows).
`(kind=0, field_id=<real>)` ⇒ COUNT(col) (count non-NULL col). The sentinel keeps
the wire shape a plain `(u8, u16)` pair (no new variant) and is resolved by the
engine + render identically.

## 4. Wire change (additive, backward-compatible)

`Op::Join` already carries optional trailing regions: length-prefixed `filter`,
the join-type tag, and (SP-PG-SQL-JOIN-QUERY) the marker-guarded page block. We
append a FOURTH optional trailing region — the "group-aggregate block" — guarded
by its own marker byte AFTER the page block:

- Emitted ONLY when `group_aggregate` is `Some`. When emitted, the filter (empty
  if none), join-type tag, and page block (all-absent marker if no pagination)
  are FORCED as positional anchors first, then:
  `[u8 ga_marker=1][u16 group_field][u16 n_aggs][ (u8 kind)(u16 field_id) ]×n_aggs`.
- A join with NO group-aggregate writes NOTHING extra here ⇒ byte-IDENTICAL to
  the pre-arc frame (every prior join / paginated-join KAT stays green).
- Decode: after the page block, if bytes remain read the ga marker (must be 1 ⇒
  else reject as a forward-incompat op) + the group/agg fields. An older frame has
  no trailing bytes ⇒ `group_aggregate = None`.

## 5. Gateway render (NEW — the join-agg result is NOT `KTR1`)

The join-group-aggregate result is the `[u32 ngroups]…` group-aggregate encoding,
NOT the self-describing `KTR1` join stream — so `render_join_result` does not
apply. (Note: single-table GROUP BY has NO PG-wire render today; this arc adds
the first group-aggregate render.) We add:

- kessel-sql text helper `join_group_aggregate(sql) -> Option<JoinGroupAggProj>`
  returning the group column's combined name + kind, plus each aggregate's kind +
  optional alias + output column name, recovered from the SQL text + the two
  tables' schemas (the engine result is value-only).
- `render_join_group_aggregate(row_bytes, proj)` in the gateway: RowDescription =
  [group col (its kind's OID), agg cols (int8 OID)]; for each group decode the
  key bytes → Value (by the group col kind) → `render_pg_text`, and each 16-byte
  i128 → decimal text. Emit one DataRow per group + CommandComplete("SELECT N").

`render_select_got` routes to this NEW render when the SQL parses as a join-group-
aggregate (checked alongside the existing `select_aggregate` / `join_projection`
shapes). The `KTR1`-magic gate for `render_join_result` is unchanged (the join-agg
result has no `KTR1` magic), so every existing render path is byte-untouched.

## 6. Scope (V1)

- INNER + LEFT join + `GROUP BY` ONE qualified combined column.
- Aggregates COUNT(*) / COUNT(col) / SUM / MIN / MAX / AVG over combined fields,
  ≥1 aggregate, optional `AS alias`.
- Composes with the combined-schema `WHERE`.
- Both apply sites (main + RO-Txn bypass) via the shared `apply_join` helper.

## 7. V1 out of scope (named follow-ups)

- `SP-PG-SQL-HAVING` — `HAVING <agg pred>` post-group filter.
- `SP-PG-SQL-JOIN-GROUP-MULTI` — `GROUP BY` multiple columns.
- `SP-PG-SQL-JOIN-AGG-3TABLE` — `GROUP BY` over a 3+-table join.
- `SP-PG-SQL-JOIN-AGG-ORDERBY-AGG` — `ORDER BY <agg>` (sort by the computed value).

## 8. Weak spots (anticipated + mitigations)

1. **Combined-schema group-key + agg-arg resolution.** `GROUP BY a.name` /
   `COUNT(b.id)` must resolve to combined `(a ++ b)` field ids, not the per-table
   ids. Mitigation: kessel-sql builds the SAME synthetic combined field list
   (`combined_join_type`) the engine builds and resolves the qualified group
   column + each aggregate arg against it, emitting combined field ids. The engine
   resolves by the SAME id against the SAME combined `cot` ⇒ agree by construction.

2. **COUNT(b.id) on a LEFT-join unmatched parent.** The b.id is `Value::Null`;
   PG counts it 0. Mitigation: the fold runs over DECODED `Vec<Value>` rows and
   COUNT(col) increments only on a non-Null Value; COUNT(*) (sentinel field id)
   increments unconditionally ⇒ exact PG LEFT-JOIN-COUNT semantics. KAT-covered.

3. **Aggregate render (no `KTR1`).** The result is value-only group-aggregate
   bytes, not the self-describing join stream. Mitigation: a NEW
   `render_join_group_aggregate` that recovers the group-col kind + agg kinds from
   the SQL text + schemas (helper `join_group_aggregate`) and decodes the
   `[u32 ngroups]…` stream — reusing `render_pg_text` for the group key and the
   scalar-aggregate i128→decimal render for the agg values.

4. **Determinism of group order.** Groups must emit in a deterministic order on
   every replica. Mitigation: a `BTreeMap<group-key-bytes, Acc>` keyed by the
   group field's raw fixed-width bytes ⇒ ascending key order, a pure function of
   committed state (mirrors `Op::GroupAggregate`'s ascending-key contract). The
   per-slot fold (COUNT/SUM associative; MIN/MAX associative+commutative) over the
   deterministic combined-row scan order is order-independent for the value, and
   the key order is fixed by the BTreeMap. seed-7 + 3-replica holds.

5. **Multi-aggregate render.** `SELECT a.name, COUNT(b.id), SUM(b.qty) GROUP BY
   a.name` emits 2 agg values per group. Mitigation: the engine encodes
   `n_aggs × 16B` per group (matching `Op::GroupAggregateMulti`); the render reads
   `n_aggs` from the parsed projection and emits one DataRow cell per aggregate.

6. **Two apply sites.** `Op::Join` is applied in the main arm and the RO-`Op::Txn`
   bypass arm. The group-aggregate fold is added ONCE in the shared `apply_join`
   helper called by both, so a grouped-aggregated join inside a read-only Txn
   equals a bare one byte-for-byte.

7. **Byte-identity of the non-grouped join.** Any join WITHOUT `GROUP BY` must be
   wire- and result-identical to the pre-arc join. Mitigation: the ga block is
   emitted only when `group_aggregate` is `Some`; the engine branch is gated on
   the same; every prior join / paginated-join KAT (proto round-trip + sm apply +
   sql parse) is a regression gate.

## 9. Acceptance

`SELECT author.name, COUNT(book.id) FROM author JOIN book ON author.id=book.aid
GROUP BY author.name` over `author={1:tolkien, 2:lewis}`,
`book={(aid1,lotr),(aid1,hobbit),(aid2,narnia)}` returns TWO rows:
`(tolkien, 2)`, `(lewis, 1)` (groups in ascending name order: lewis, tolkien).
Determinism oracle (VSR seed-7 + 3-replica) green.
