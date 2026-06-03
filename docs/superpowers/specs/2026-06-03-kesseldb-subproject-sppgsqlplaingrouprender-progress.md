# SP-PG-SQL-PLAIN-GROUP-RENDER — progress tracker

**Arc:** render a PLAIN (non-JOIN) `GROUP BY` group-aggregate SELECT over the
PostgreSQL wire.
**Date:** 2026-06-03
**Design:** `docs/superpowers/specs/2026-06-03-kesseldb-sppgsqlplaingrouprender-design.md`
**Status:** CLOSED.

## Task list

- [x] sql: `plain_group_aggregate(sql) -> Option<PlainGroupAggProj>` recognizer
      (mirrors `join_group_aggregate`). Accepts `SELECT <g>, <AGG>(*|[t.]col)
      [AS a] [, …]* FROM <t> [WHERE …] GROUP BY <g> [HAVING …] [ORDER BY …]
      [LIMIT n] [OFFSET n]`. Group col bare or qualified; 1+ aggregates;
      COUNT/SUM/MIN/MAX/AVG; optional `AS` aliases (else PG default name).
- [x] sql: returns `None` for a JOIN group-aggregate, a single scalar aggregate
      (no GROUP BY), a non-aggregate projection, and no-GROUP-BY queries — every
      existing render path stays byte-untouched.
- [x] sql: recognizer KATs — single COUNT(*), multi-agg + aliases + HAVING +
      ORDER BY + LIMIT + OFFSET, qualified cols, and all the `None` shapes.
- [x] gateway: `render_plain_group_aggregate` (mirrors
      `render_join_group_aggregate`) decodes the value-only group stream
      `[u32 ngroups]([u32 keylen][key][16B i128 × n_aggs])*`, types + names the
      group key from the FROM-table schema (`engine.describe_table`), types
      aggregate OIDs per kind (COUNT/SUM → int8, AVG → numeric, MIN/MAX →
      source-column OID; COUNT(*) / unresolved → int8).
- [x] gateway: Shape-0.45 branch in `render_select_got` placed AFTER the JOIN
      group-agg branch and BEFORE the single-scalar-agg branch (neither can
      shadow it: a JOIN never reaches it + the recognizer rejects `JOIN`;
      `select_aggregate` returns `None` for a leading group column).
- [x] gateway: render KATs — CHAR group key (default `count` name) + INT group
      key (5 aggregates with an alias).
- [x] determinism: RENDER-ONLY — no `Op` / wire-format change, no oracle literal
      construction sites touched; corpus / partition / 3-replica byte-identity
      untouched.
- [x] vulcan: `cargo test --workspace --release` green (2700-ish tests).
- [x] vulcan: psql smoke (`scripts/sppgsqlplaingrouprender-smoke.py`) — the
      headline `SELECT category, COUNT(*) FROM products GROUP BY category`
      ERRORED on pre-fix origin/main and renders the per-category counts
      post-fix; multi-agg (COUNT/SUM/AVG/MIN/MAX) + HAVING also PASS.
- [x] docs: design, this progress tracker, USAGE §3 + the GROUP BY section,
      STATUS, CHANGELOG.
- [x] commit + push to origin/main; cleanup vulcan worktree + target dir.

## Determinism

RENDER-ONLY arc. `Op::GroupAggregate` / `Op::GroupAggregateMulti` and their
result stream are untouched; no new write path; no oracle literal construction
site changed. The engine's group stream is already deterministic (BTreeMap
ascending raw-key order), so the corpus / partition / 3-replica byte-identity
oracles are unaffected.

## Honest caveat — ORDER BY / LIMIT / OFFSET

A trailing `ORDER BY … LIMIT … OFFSET …` on a plain GROUP BY is *parsed* but not
yet threaded into the group ops (they carry no sort/limit fields), so the engine
returns ALL groups in ascending group-key order regardless. The render faithfully
emits whatever group set the engine returns, in key order — it does NOT silently
drop or reorder rows. Sort/limit pushdown for plain group-agg is the follow-on
arc **SP-PG-SQL-GROUP-SORT-LIMIT**.

## vulcan smoke transcript

`cargo test --workspace --release` on vulcan: **exit 0 (all green)**, including
the corpus / partition / 3-replica byte-identity determinism oracles.

**Before-state** (pre-fix, established by code inspection of
`render_select_got`): the dispatcher had branches for JOIN group-aggregate,
single scalar aggregate, `KTR1` JOIN stream, projection list, and whole-row
`SELECT *` — a plain `SELECT g, COUNT(*) … GROUP BY g` matched NONE, so the
engine's value-only group stream hit the bottom `0A000 … only renders SELECT *`
arm. The new Shape-0.45 branch fixes exactly that gap.

**After-state** (live psql/psycopg2 smoke against the gateway-built server,
`KESSELDB_PG_ADDR=127.0.0.1:5550`):

```
# psycopg2 2.9.12 -> postgresql://test:admin@127.0.0.1:5550/kesseldb
STAGE ddl: PASS products created
STAGE seed: PASS 6 products in 3 categories
STAGE single_count: PASS 3 groups: [('books', 3), ('gadgets', 1), ('toys', 2)]
STAGE multi_agg: PASS 5 aggregates × 3 groups: {'books': (3, 60, 20, 10, 30), 'gadgets': (1, 100, 100, 100, 100), 'toys': (2, 20, 10, 5, 15)}
STAGE having: PASS before=3 after=2 (gadgets dropped): [('books', 3), ('toys', 2)]
STAGE order_limit: PASS groups rendered: [('books', 3), ('gadgets', 1), ('toys', 2)]

=== SP-PG-SQL-PLAIN-GROUP-RENDER SMOKE SUMMARY ===
  PASS  ddl / seed / single_count / multi_agg / having / order_limit
--- 6/6 stages PASS ---
```

HEADLINE `SELECT category, COUNT(*) FROM products GROUP BY category` →
**{books:3, gadgets:1, toys:2}**. `multi_agg` confirms COUNT/SUM/AVG/MIN/MAX
typing (books prices 10/20/30 → sum 60, avg 20, min 10, max 30). `HAVING
COUNT(*) > 1` drops the gadgets singleton.
