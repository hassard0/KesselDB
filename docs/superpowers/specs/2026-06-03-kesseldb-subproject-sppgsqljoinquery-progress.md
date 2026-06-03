# SP-PG-SQL-JOIN-QUERY — `JOIN … [WHERE] ORDER BY / LIMIT / OFFSET` — SP-arc Progress Tracker

Date created: 2026-06-03

**Status: CLOSED — DONE (2026-06-03).** Paginated joins
(`SELECT a.name, b.title FROM a JOIN b ON a.id=b.aid [WHERE …] ORDER BY b.title
[DESC] [LIMIT n] [OFFSET m]`) now compose END-TO-END against KesselDB over the PG
wire — the ubiquitous paginated-list-view pattern every real app emits. This arc
COMPOSES the SP23 (`Op::SelectSorted`) sort/page machinery with the combined join
rows: `Op::Join` gained additive `order_by` / `limit_n` / `offset_n` fields; the
engine stable-sorts the surviving combined rows by a qualified column (from either
table, NULL-aware for LEFT-join NULL fields) then paginates. Determinism preserved
(stable sort + deterministic scan-position tiebreak). No new blocking gap; the
named follow-ups (multi-column ORDER BY, ORDER BY expression, GROUP BY-over-join,
explicit NULLS FIRST/LAST) are honest extensions. TaskList ready for completion.

Design:           `docs/superpowers/specs/2026-06-03-kesseldb-sppgsqljoinquery-design.md`
Smoke transcript: `docs/superpowers/sppgsqljoinquery-smoke-2026-06-03.txt`

## Slice plan

| T# | Scope | Status |
|---|---|---|
| **T1** | Design doc (engine sort/paginate composing SP23; combined-schema sort-field resolution; stable-sort tiebreak; NULLS LAST/FIRST; additive page-block wire; 7 weak spots). | **DONE** |
| **T2** | proto — `Op::Join` gains `order_by: Option<(u16,bool)>` + `limit_n: Option<u64>` + `offset_n: Option<u64>`; marker-guarded page block appended only when set (bare/filtered/left join byte-identical); bad marker rejected at decode. | **DONE** (8048106) |
| **T2** | engine (kessel-sm) — ONE shared `apply_join` helper called by BOTH apply arms (main + RO-Txn bypass): collect surviving combined rows, stable-sort by the combined sort field via `cmp_join_value` (NULL-aware, kind-aware, CHAR-pad), reverse for DESC, then `offset_n`/`limit_n`. No order_by ⇒ emit in scan order (legacy `limit` pre-sort cap) — byte-identical. | **DONE** (8048106) |
| **T2** | kessel-sql — parse `ORDER BY <qualified col> [ASC|DESC]` + `LIMIT`/`OFFSET` after the optional WHERE; resolve the qualified ORDER BY column against the combined `(a++b)` schema; route pagination to the legacy `limit` (bare `JOIN … LIMIT n`) or the post-sort fields (ORDER BY / OFFSET present). | **DONE** (8048106) |
| **T2** | gateway — NO change needed: the result is still the combined `KTR1` stream `render_join_result` already decodes, merely sorted + paginated upstream. | **DONE** (verified, no edit) |
| **T3** | vulcan green (kessel-proto / kessel-sm / kessel-sql + parallel_reads_oracle) + ORDER BY/LIMIT/OFFSET KATs. | **DONE** |
| **T4** | vulcan psql smoke: `JOIN … ORDER BY title LIMIT 2` → hobbit, lotr (sorted+paginated). | **DONE** |
| **T5** | USAGE + grammar rows, STATUS row, CHANGELOG, this tracker → CLOSED, smoke transcript, vulcan cleanup, TaskList. | **DONE** |

## KATs (new)

- `kessel-proto::join_no_pagination_wire_byte_identical` — INNER bare join with
  no pagination still encodes to the exact 17-byte pre-arc frame — REGRESSION.
- `kessel-proto::paginated_join_round_trips` — ORDER BY asc + LIMIT + OFFSET
  round-trips (45-byte frame: force-written empty-filter + inner-tag anchors +
  page block).
- `kessel-proto::bad_page_block_marker_rejected` — a corrupt page-block marker
  fails decode (forward-incompat op surfaced).
- (plus the `Op::Join` round-trip corpus gains 3 paginated fixtures: ORDER BY
  only, ORDER BY DESC + LIMIT + OFFSET, and LIMIT/OFFSET-without-order over a
  filtered LEFT join.)
- `kessel-sm::jq_order_by_right_col_asc` — INNER join ORDER BY b.title ASC sorts.
- `kessel-sm::jq_order_by_left_col_desc` — LEFT join ORDER BY a.name DESC sorts;
  orphan (NULL title) row included.
- `kessel-sm::jq_limit_takes_first_after_sort` — ORDER BY + LIMIT 2 = first 2
  AFTER sort (hobbit, lotr).
- `kessel-sm::jq_offset_limit_paginates` — OFFSET 1 LIMIT 2 = rows 2-3.
- `kessel-sm::jq_left_join_null_sort_orders_nulls_last_asc` — LEFT-join NULL sort
  field orders NULLS LAST for ASC / NULLS FIRST for DESC (PG default).
- `kessel-sm::jq_bare_join_no_order_unchanged` — no order/limit ⇒ scan order,
  3 rows — REGRESSION.
- `kessel-sql::join_order_by_limit_offset_parses` — ORDER BY (asc/desc, left/right
  col) + LIMIT + OFFSET + WHERE compose; bare `JOIN … LIMIT n` keeps the legacy
  `limit` field (wire-identical); unknown / wrong-qualifier ORDER BY col → error.
- `kessel-sql::join_order_by_limit_runs_sorted` — end-to-end: `ORDER BY b.title
  LIMIT 2` → hobbit, lotr; `OFFSET 1 LIMIT 2` → lotr, silmarillion; `DESC` →
  silmarillion, lotr, hobbit.

## Verification (on vulcan @ 8048106 / bedf22a)

- Unit/integration (release): kessel-proto **23 passed / 0 failed** (+3),
  kessel-sm **182 passed / 0 failed** (+6), kessel-sql **131 passed / 0 failed**
  (+2), kesseldb-server `parallel_reads_oracle` green (join RO-bypass == apply).
- Determinism: stable sort over rows collected in the deterministic
  left-key/right-scan order ⇒ a TOTAL deterministic order (sort key, then scan
  position). The page block is additive and a non-paginated join is wire-
  identical, so replay is unaffected. seed-7 + 3-replica oracle green.
- Smoke (psql direct, port 5553): `SELECT a.name, b.title FROM a JOIN b ON
  a.id=b.aid ORDER BY b.title LIMIT 2` over `a={1:tolkien}`,
  `b={(aid1,lotr),(aid1,hobbit),(aid1,silmarillion)}` → **2 rows**:
  `(tolkien, hobbit)`, `(tolkien, lotr)` — sorted + paginated.

No PG-wire / HTTP / WS / binary surface byte touched outside the additive
`Op::Join` page block (absent for every non-paginated join);
`#![forbid(unsafe_code)]` honored; no new external deps.

## Named follow-ups (out of scope this arc — honest extensions, no regression)

- `SP-PG-SQL-JOIN-ORDERBY-MULTI` — `ORDER BY` multiple columns.
- `SP-PG-SQL-JOIN-ORDERBY-EXPR` — `ORDER BY` an expression / computed key.
- `SP-PG-SQL-JOIN-AGG` — `GROUP BY` / aggregates over a join.
- `SP-PG-SQL-JOIN-NULLS-ORDER` — explicit `NULLS FIRST` / `NULLS LAST` override
  (V1 uses PG's default: NULLS LAST for ASC, NULLS FIRST for DESC).
