# SP-PG-SQL-JOIN-ALIAS — progress tracker

**Date:** 2026-06-03
**Status:** CLOSED
**Design:** `docs/superpowers/specs/2026-06-03-kesseldb-sppgsqljoinalias-design.md`

## Goal

Resolve TABLE ALIASES in JOIN queries:
`SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id` (and the
`FROM users AS u` form). The parser accepted the alias but column qualifiers only
resolved against the FULL table name, so the universal SQLAlchemy/Django/Rails
aliased-join form failed. Build an alias→table map from the FROM/JOIN clause and
resolve every qualifier (projection, ON, WHERE, ORDER BY, GROUP BY) through it,
for binary AND multi-table (3+) joins.

## Task list

- [x] **sql — alias map + resolver.** `JoinTableRef { table, alias }`,
      `resolve_join_qualifier` (full name → itself, alias → its table, else clean
      Err), `validate_join_refs` (reject duplicate/ambiguous alias, alias
      shadowing a table, self-join), `parse_optional_alias` / `next_optional_alias`
      (`[AS] <alias>`, clause-keyword aware).
- [x] **sql — engine compile path (`compile_select`).** Capture each table's
      optional `[AS] <alias>` (left speculatively, kept only if a JOIN follows;
      right; every chained table) into a running `refs`. Resolve every qualifier
      through `refs`: base ON, each chain-step ON, WHERE
      (`compile_join_where{,_multi}` gained a `refs` param), GROUP BY + aggregate
      args + HAVING (`resolve_combined`), ORDER BY.
- [x] **sql — gateway projection text-helpers.** `join_projection` re-walks the
      FROM/JOIN clause (skipping ON/WHERE tokens), builds `refs`, and rewrites
      each `JoinProjCol.qualifier` alias → full table name; `join_group_aggregate`
      resolves the GROUP BY qualifier the same way. Unresolvable ⇒ `None` ⇒ the
      gateway renders the standard 42703 error.
- [x] **gateway** — NO change to `crates/kessel-pg-gateway/src/dispatch.rs`: the
      `JoinProjCol.qualifier` reaching `render_join_result` is already the full
      table name (resolved in the SQL layer).
- [x] **Determinism** — resolution entirely in `kessel-sql`; the alias is
      rewritten to the full table name during parse, so an aliased join compiles
      to the BYTE-IDENTICAL wire `Op` as its full-name twin. No `Op`/proto change,
      no construction-site churn, no oracle literal changes.
- [x] **sql unit tests** — `alias_join_compiles_identically_to_full_names`
      (binary/3-way/WHERE/ORDER-BY aliased == full-name `Op`; error cases) +
      `join_projection_resolves_aliases` (alias/AS/3-way/full-name/star resolve;
      unknown qualifier + dup alias ⇒ None).
- [x] **psql smoke** `scripts/sppgsqljoinalias-smoke.py` (real psycopg2, 8 stages).
- [x] **Docs** — design doc, this progress tracker, USAGE §3 JOIN ref + grammar +
      examples, STATUS, CHANGELOG, README.

## Deferred (named follow-up, V1 out-of-scope)

- **Self-join under two aliases of the SAME table** (`FROM users a JOIN users b
  ON …`) — `SP-PG-SQL-SELF-JOIN`. The combined `KTR1` schema would have duplicate
  `<table>.<col>` names (both sides are `users.*`), so the alias→full-name rewrite
  would be ambiguous against that schema. Rejected with a clear error in
  `validate_join_refs` (not silently mis-resolved). It is the only deferred
  sub-case and it adds real risk (the combined schema needs per-instance names),
  so it is gated. Distinct-table joins with aliases (the common case) are fully
  in scope.

## Validation

### 1. Workspace test (vulcan, `cargo test --workspace --release`)

`cargo test --workspace --release` on vulcan (fresh worktree off origin/main):
**exit 0 — all green**, including the determinism oracles
(`jepsen_3replica_partition_converges_byte_identical`,
`large_seed_corpus_is_deterministic_and_converges`,
`sharded_engine t2_determinism_oracle_k1_k4_k8_byte_equal`,
`read_pool determinism_oracle_100_random_workloads`, and the scatter-scan /
MVCC byte-identity suite). Alias resolution lives entirely in the `kessel-sql`
layer — the `Op` wire is UNCHANGED, so no oracle literal construction site moved
and byte-identity is structurally preserved.

### 2. psql smoke transcript (vulcan, real psycopg2)

Real `scripts/sppgsqljoinalias-smoke.py` against a live `kesseldb` built
`--features pg-gateway` on vulcan (PG `127.0.0.1:5553`):

```
# psycopg2 2.9.12 -> postgresql://test:admin@127.0.0.1:5553/kesseldb
STAGE ddl: PASS users, posts, comments created
STAGE seed: PASS 2 users, 3 posts, 3 comments
STAGE alias_binary: PASS u.name/p.title (aliased) → [('alice', 'hello'), ('alice', 'world'), ('bob', 'solo')]
STAGE full_names: PASS full table names → [('alice', 'hello'), ('alice', 'world'), ('bob', 'solo')] (identical to aliased)
STAGE as_form: PASS FROM users AS u JOIN posts AS p → [('alice', 'hello'), ('alice', 'world'), ('bob', 'solo')]
STAGE alias_three_way: PASS 3-way aliased chain → [('alice', 'hello', 'nice'), ('alice', 'hello', 'ok'), ('alice', 'world', 'wow')]
STAGE alias_where: PASS WHERE u.id=1 → [('alice', 'hello'), ('alice', 'world')]; WHERE u.id=2 → [('bob', 'solo')]
STAGE alias_order_by: PASS ORDER BY p.title → ['hello', 'solo', 'world']

=== SP-PG-SQL-JOIN-ALIAS SMOKE SUMMARY ===
  PASS  ddl / seed / alias_binary / full_names / as_form / alias_three_way / alias_where / alias_order_by
--- 8/8 stages PASS ---
```

Every stage uses a HARD `assert` on exact expected rows, so 8/8 is a verified
result. `full_names` asserts the full-table-name form returns rows IDENTICAL to
the aliased form (back-compat lock).

HEADLINE proven: `SELECT u.name, p.title FROM users u JOIN posts p ON u.id =
p.user_id` returns the correct rows over the PG wire, AND the SAME query with
FULL table names (`SELECT users.name, posts.title FROM users JOIN posts ON
users.id = posts.user_id`) returns identical rows (back-compat). The `AS` form,
the aliased 3-way chain, aliased WHERE, and aliased ORDER BY all pass.

## CLOSED
