# SP-PG-ORM-REALAPP — capstone realistic multi-model SQLAlchemy app

**Date:** 2026-06-03
**Arc:** SP-PG-ORM-REALAPP (capstone / validation)
**HEAD baseline:** `7bfaef6`

## Context

KesselDB now has a complete core relational surface accessible through the
PostgreSQL wire protocol:

- CRUD (CREATE TABLE / INSERT … RETURNING / SELECT / UPDATE / DELETE)
- FK DDL parse (accept-and-skip; `SP-PG-ORM-RELATIONSHIPS`)
- Inner equi-JOIN with qualified projection (`SP-PG-ORM-RELATIONSHIPS`)
- Filtered JOIN (`JOIN … WHERE`; `SP-PG-SQL-JOIN-WHERE`)
- Sorted / paginated JOIN (`ORDER BY` / `LIMIT` / `OFFSET`;
  `SP-PG-SQL-JOIN-QUERY`)
- GROUP BY + aggregate over a JOIN (`SP-PG-SQL-JOIN-AGG`)

Each landed and was smoked in isolation. SQLAlchemy 2.0 and Django 6.0 ORM
CRUD both work. What has NOT yet been validated is whether these surfaces
**compose** in a single realistic application that uses the full query range
back-to-back.

## Scope

Run a realistic **blog application** workload through a real SQLAlchemy 2.0
declarative-ORM program (NOT raw `cursor.execute`) and document, per query,
whether it PASSES end-to-end or surfaces a GAP. This is a **validation arc**:
measure + document the boundary + name gaps as precise follow-up arcs. A small
surgical fix that unblocks a major path is acceptable; large gaps are named,
not fixed inline.

### Data model

```
User (users)        1 — N   Post (posts)        1 — N   Comment (comments)
  id, name                    id, title, user_id          id, body, post_id
```

Two FK relationships (`posts.user_id → users.id`,
`comments.post_id → posts.id`), three declarative `relationship()` pairs with
`back_populates`.

### Workload (the queries a real blog uses)

| Stage | What it exercises |
|---|---|
| `schema` | 3 CREATE TABLE, two carrying a FK table-constraint |
| `cascade_seed` | multi-level relationship cascade insert (`user.posts = [...]`) + a second commit adding `post.comments = [...]` |
| `Q1_join` | list all posts with author name — inner JOIN, qualified projection |
| `Q2_filtered_join` | posts by a specific author — `JOIN … WHERE name = $1` |
| `Q3_group_agg` | comment count per post — `GROUP BY` + `COUNT()` over a JOIN |
| `Q4_paginate` | recent posts — `ORDER BY … LIMIT` |
| `Q5_nav` | relationship navigation — `alice.posts` (lazy SELECT WHERE FK) |
| `Q6_update_delete` | `UPDATE … WHERE` + `DELETE … WHERE`, re-count |

## Method

- Smoke script `scripts/sppgormrealapp-smoke.py` with a triage harness: every
  stage runs in its own `try/except` so one failure does not mask later
  stages. Each prints `STAGE <name>: PASS|GAP`.
- Build + run on vulcan (isolated worktree `/tmp/kdb-ra-wt`,
  `CARGO_TARGET_DIR=/tmp/kdb-t-ra`), PG port 5556 / native 6556.
- Capture the exact SQL emitted from the gateway log for any GAP.

## Triage policy

- Each query: PASS or GAP. For a GAP, capture the exact failing SQL + error
  from the gateway log.
- If a small surgical fix unblocks a major query, ship it (and document it —
  determinism + wire-compat preserved). Otherwise name a follow-up arc with
  the exact failing SQL.

## Closure

- USAGE.md §9 — a "Real multi-model app (blog)" row with the N/M headline
  score (the real-world-readiness statement).
- STATUS.md row, progress tracker → CLOSED or DONE_WITH_CONCERNS.
- Smoke transcript `docs/superpowers/sppgormrealapp-smoke-2026-06-03.txt`.
- Named follow-ups for any gaps.
