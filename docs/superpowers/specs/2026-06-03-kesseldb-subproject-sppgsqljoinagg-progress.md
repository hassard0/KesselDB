# SP-PG-SQL-JOIN-AGG — `JOIN … GROUP BY + aggregate` — SP-arc Progress Tracker

Date created: 2026-06-03

**Status: CLOSED — DONE (2026-06-03).** Grouped aggregates over joins
(`SELECT a.name, COUNT(b.id) FROM a JOIN b ON a.id=b.aid [WHERE …] GROUP BY
a.name`) now compose END-TO-END against KesselDB over the PG wire — the
dashboard / reporting "count related rows per parent" query. This arc COMPOSES
the SP22 / SP-Analytic-Plan-MULTI group-aggregate fold with the combined join
rows: `Op::Join` gained ONE additive field `group_aggregate:
Option<JoinGroupAgg>` (a combined-schema `group_field` + `Vec<(kind, field_id)>`
aggregate list). The engine groups the surviving combined `Vec<Value>` rows into
a BTreeMap (ascending key order ⇒ deterministic) and folds the aggregates per
group over the DECODED Values, emitting the `[u32 ngroups]…` group-aggregate
result. NULL semantics fall out of the Value fold (PG LEFT-JOIN-COUNT exact). The
PG gateway gains the FIRST group-aggregate render. Determinism preserved. No new
blocking gap; the named follow-ups (HAVING, multi-key GROUP BY, 3+-table join-agg,
ORDER BY the aggregate) are honest extensions. TaskList ready for completion.

Design:           `docs/superpowers/specs/2026-06-03-kesseldb-sppgsqljoinagg-design.md`
Smoke transcript: `docs/superpowers/sppgsqljoinagg-smoke-2026-06-03.txt`

## Slice plan

| T# | Scope | Status |
|---|---|---|
| **T1** | Design doc (Option A: extend `Op::Join` with `group_aggregate`; combined-schema group-key + agg-arg resolution; PG LEFT-JOIN-COUNT NULL semantics; BTreeMap determinism; NEW gateway render; 7 weak spots). | **DONE** |
| **T2** | proto — `Op::Join` gains `group_aggregate: Option<JoinGroupAgg>` (`group_field` + `Vec<(kind, field_id)>`); `JoinGroupAgg` struct + `COUNT_STAR_FIELD` sentinel; marker-guarded ga block appended only when set (bare/filtered/left/paginated join byte-identical); bad marker / n_aggs==0 rejected at decode. | **DONE** |
| **T2** | engine (kessel-sm) — the shared `apply_join` helper grows a grouping branch: collect surviving combined rows, group by the group field's raw fixed-width Value bytes into a `BTreeMap`, fold COUNT(*)/COUNT(col)/SUM/MIN/MAX/AVG per group over the decoded Values (NULL-aware), emit the `[u32 ngroups][u32 keylen][key][16B × n]` result. Both apply arms (main + RO-Txn bypass) call it. | **DONE** |
| **T2** | kessel-sql — parse `GROUP BY <qualified col>` after the optional join-WHERE; require the `Proj::Aggs` projection; resolve the group col + each aggregate arg (qualified `COUNT(b.id)` exact / bare by suffix) against the combined `(a++b)` schema; `COUNT(*)` → the sentinel field id; emit `Op::Join { group_aggregate: Some(..) }`. `parse_agg` now preserves the arg qualifier. | **DONE** |
| **T2** | gateway — NEW `render_join_group_aggregate` + `kessel_sql::join_group_aggregate` text helper: the join-agg result is the value-only group-aggregate stream (NOT `KTR1`), so a dedicated render recovers the group-col kind (via the qualifier's table schema) + agg output names, decodes `[u32 ngroups]…`, and emits RowDescription + one DataRow per group. Routed in `render_select_got` BEFORE the `KTR1` / single-scalar shapes. | **DONE** |
| **T3** | vulcan green (kessel-proto / kessel-sm / kessel-sql / kessel-pg-gateway + parallel_reads_oracle determinism). | **DONE** |
| **T4** | vulcan psql smoke: `SELECT author.name, COUNT(book.id) … GROUP BY author.name` → tolkien 2, lewis 1. | **DONE** |
| **T5** | USAGE + grammar rows, STATUS row, CHANGELOG, this tracker → CLOSED, smoke transcript, vulcan cleanup, TaskList. | **DONE** |

## KATs (new — 13)

- `kessel-proto::join_no_group_aggregate_wire_byte_identical` — a join with no
  group_aggregate (and no pagination) still encodes to the 17-byte pre-arc frame
  — REGRESSION.
- `kessel-proto::join_group_aggregate_round_trips` — GROUP BY + COUNT(*) + SUM
  round-trips (37-byte frame: forced empty-filter + inner-tag + all-None page
  block + ga block).
- `kessel-proto::bad_group_aggregate_marker_rejected` — a corrupt ga-block marker
  fails decode (forward-incompat surfaced).
- (plus the `Op::Join` round-trip corpus gains 2 ga fixtures: GROUP BY + COUNT(*)
  over INNER, and GROUP BY + COUNT(col)+SUM over a filtered LEFT join.)
- `kessel-sm::jagg_count_related_per_parent` — HEADLINE: COUNT(b.id) per author
  (INNER) → tolkien 3.
- `kessel-sm::jagg_count_star_group_size` — COUNT(*) = group size.
- `kessel-sm::jagg_sum_per_group` — SUM(b.aid) per group.
- `kessel-sm::jagg_left_join_count_null_vs_star` — LEFT join: orphan group
  COUNT(b.id)=0 (NULL not counted) vs COUNT(*)=1 (row exists) — PG semantics.
- `kessel-sm::jagg_deterministic_byte_equal_repeat` — two runs over the same
  committed state are byte-identical (BTreeMap key order + associative fold).
- `kessel-sql::join_group_aggregate_parses` — GROUP BY compiles to `Op::Join {
  group_aggregate: Some }` with combined field ids; COUNT(*) sentinel; qualified
  COUNT(b.id) disambiguates; bare join stays None (REGRESSION); group col mismatch
  errors.
- `kessel-sql::join_group_aggregate_runs` — end-to-end: COUNT(b.id) per author →
  lewis 1, tolkien 2 (ascending); COUNT(*) likewise.
- `kessel-pg-gateway::jagg_render_count_per_parent` — the join-agg value stream
  renders as RowDescription([group col, agg col]) + one DataRow per group
  (group key decoded by kind → text, i128 → decimal).

## Verification (on vulcan)

- Unit/integration (release): kessel-proto **26 passed / 0 failed** (+3 KATs +2
  corpus fixtures), kessel-sm **187 passed / 0 failed** (+5), kessel-sql **133
  passed / 0 failed** (+2), kessel-pg-gateway **1003 passed / 0 failed** (+1),
  kesseldb-server `parallel_reads_oracle` determinism gates green.
- Determinism: the group-aggregate is a pure function of committed state — a
  `BTreeMap` keyed by the group field's raw fixed-width bytes (ascending order)
  + an associative per-slot fold (COUNT/SUM; MIN/MAX assoc+commutative) over rows
  visited in the deterministic left-key/right-scan order. No RNG / clock / hash
  iteration in the output. seed-7 + 3-replica oracle green.
- Smoke (psql direct, port 5555): `SELECT author.name, COUNT(book.id) FROM author
  JOIN book ON author.id=book.aid GROUP BY author.name` over `author={1:tolkien,
  2:lewis}`, `book={(aid1,lotr),(aid1,hobbit),(aid2,narnia)}` → **2 rows**:
  `tolkien 2`, `lewis 1` (groups ascending: lewis, tolkien).

No PG-wire / HTTP / WS / binary surface byte touched outside the additive
`Op::Join` ga block (absent for every non-grouped join) + the NEW gateway render
(gated on the join-group-aggregate SQL shape, which returns None for everything
else); `#![forbid(unsafe_code)]` honored; no new external deps.

## Named follow-ups (out of scope this arc — honest extensions, no regression)

- `SP-PG-SQL-HAVING` — `HAVING <agg pred>` post-group filter.
- `SP-PG-SQL-JOIN-GROUP-MULTI` — `GROUP BY` multiple columns.
- `SP-PG-SQL-JOIN-AGG-3TABLE` — `GROUP BY` over a 3+-table join.
- `SP-PG-SQL-JOIN-AGG-ORDERBY-AGG` — `ORDER BY <agg>` (sort by the computed value).
