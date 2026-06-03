# SP-PG-ORM-REALAPP — CAPSTONE realistic blog app — SP-arc Progress Tracker

Date created: 2026-06-03

**Status: CLOSED — DONE (2026-06-03).** A realistic THREE-model SQLAlchemy 2.0
BLOG application (`User` 1—N `Post` 1—N `Comment`, FKs + declarative
`relationship()` with `back_populates`, insertmanyvalues batching ON — the
default) exercising the FULL query range a real app uses, back-to-back, now
runs END-TO-END against KesselDB over the PG wire: **8/8 stages PASS** on
vulcan, every query returning REAL data. This is the headline real-world
readiness statement — the truest test of whether tonight's relational surface
(CRUD + FK + inner/filtered/sorted/paginated/grouped-aggregated joins +
relationships) COMPOSES for real use.

The first smoke run surfaced TWO precise gaps; both got a SURGICAL fix (the
arc's "small fix that unblocks a major path is OK" allowance), each with a
unit test, neither touching the engine apply path or the Op wire encoding:

1. **kessel-sql lexer — SQL-standard doubled-quote string escape.**
   `'bob''s post'` is the value `bob's post` (PG §4.1.2.1). The single-quote
   lexer stopped at the FIRST inner `'`, truncating to `bob` and then choking
   (`expected ',' or ')'`). This broke the seed the instant any value carried
   an apostrophe — a CORRECTNESS BUG that hits ANY app with apostrophes in
   names/titles/prose. Fix mirrors the existing `"` delimited-identifier
   escape: doubled `''` → one `'`; lone `'` closes. Unescaped strings are
   byte-identical (every prior literal KAT passes).

2. **kessel-pg-gateway — ORDER-BY-over-a-projection render.**
   A projection-list SELECT with `ORDER BY` (`SELECT title FROM posts ORDER
   BY title LIMIT n`) lowers to `Op::SelectSorted` (the `_ if sort.is_some()`
   match arm fires before `Proj::Cols`), which emits the FULL record stream —
   the projection is DROPPED at the engine layer. The gateway's narrow
   projected-row decoder then mismatched the width (`128 != 64`). Fix: detect
   the shape (`kessel_sql::select_projection_is_sorted`) and decode each FULL
   record against the table schema, re-projecting requested columns by index
   (with proper null-bitmap NULL fidelity — strictly better than the narrow
   path). Non-sorted projection keeps the byte-identical narrow path.

No NEW follow-ups required — the blog app is 8/8. TaskList ready.

Design:           `docs/superpowers/specs/2026-06-03-kesseldb-sppgormrealapp-design.md`
Smoke transcript: `docs/superpowers/sppgormrealapp-smoke-2026-06-03.txt`

## Slice plan

| T# | Scope | Status |
|---|---|---|
| **T1** | Design doc — capstone real-app validation across the full relational surface; blog data model; the 8-stage workload; triage policy (PASS/GAP per query; surgical-fix-or-name-follow-up). | **DONE** |
| **T2** | Smoke script `scripts/sppgormrealapp-smoke.py` (triage harness, isolated per-stage try/except); build + run on vulcan (isolated worktree, `CARGO_TARGET_DIR=/tmp/kdb-t-ra`, ports 5556/6556). | **DONE** |
| **T3** | Triage — first run: schema/Q1/Q2/Q3/Q5/Q6 PASS, `cascade_seed` GAP (apostrophe `''`). Surgical FIX 1 (lexer) → seed + reads pass; `Q4_paginate` GAP (sorted projection width 128≠64). Surgical FIX 2 (gateway re-project). Final: **8/8 PASS**. | **DONE** |
| **T4** | USAGE.md §9 "Real multi-model app (blog)" headline row (8/8); STATUS.md row; this tracker → CLOSED; smoke transcript; vulcan worktree+target cleanup; TaskList. | **DONE** |

## KATs (new — 3)

- `kessel-sql::doubled_quote_string_escape` — `'bob''s post'` → `Tok::Str("bob's post")`;
  `'a''b''c'` → `a'b'c`; `''''` → `'`; plain `'hello world'` byte-identical
  (REGRESSION); `''` → empty string.
- `kessel-sql::projection_sorted_detection` — `SELECT title FROM posts ORDER BY
  title LIMIT 2` → true; qualified + DESC + multi-col forms → true; no ORDER BY
  (narrow path) → false; `SELECT *` / aggregate → false.
- (gateway re-projection exercised end-to-end by the vulcan smoke + direct psql;
  the `emit_projected_from_full_records` path rides the existing `decode_record`
  + `encode_data_row` KATs.)

## Verification (on vulcan)

- Unit/integration (release): kessel-sql **135 passed / 0 failed** (+2 new KATs),
  kessel-pg-gateway **1003 passed / 0 failed**, kessel-sm
  `select_sorted_is_deterministic` + `select_sorted_orders_and_paginates` PASS.
- Determinism: neither fix touches the engine apply path or the Op wire encoding
  (FIX 1 is lexer-only — same token for unescaped strings; FIX 2 is pure gateway
  render of bytes the unchanged `Op::SelectSorted` already produced). VSR
  `large_seed_corpus_is_deterministic_and_converges` +
  `jepsen_3replica_partition_converges_byte_identical` PASS.
- Smoke (SQLAlchemy 2.0.45, insertmanyvalues ON, port 5556): the blog app
  **8/8** — schema (3 tables, 2 FKs) / cascade seed (2 users, 3 posts, 2
  comments) / Q1 JOIN (all 3 posts + authors) / Q2 filtered JOIN (alice's 2
  posts) / Q3 GROUP-BY-COUNT over JOIN (`('hello world', 2)`) / Q4 ORDER-BY+LIMIT
  (`["bob's first post", 'hello world']`) / Q5 lazy nav (alice.posts) / Q6
  UPDATE+DELETE (1 comment remaining). Gateway log clean.
- Direct psql: `SELECT posts.title FROM posts ORDER BY posts.title` renders 3
  sorted rows incl. `bob's first post`; `… WHERE title = 'bob''s first post'`
  round-trips the apostrophe on the filter side too.

No PG-wire / HTTP / WS / binary surface byte touched outside the lexer string
arm (byte-identical for unescaped strings) + the NEW gateway sorted-projection
render path (gated on `select_projection_is_sorted`, false for every other
shape); `#![forbid(unsafe_code)]` honored; no new external deps.

## Named follow-ups (out of scope this arc — honest extensions, no regression)

None NEW required — the realistic blog app is 8/8. The pre-existing named
follow-ups still stand and the blog workload does NOT exercise them:
`SP-PG-DDL-FK-ENFORCE` (FK is parse-and-skip), `SP-PG-SQL-MULTI-JOIN` (3+
tables in ONE query), `SP-PG-SQL-PROJ-NULL` (NULL fidelity on the NARROW
projection path — the sorted path already has it), `SP-PG-SQL-PROJ-ALIAS`
(alias-named RowDescription).
