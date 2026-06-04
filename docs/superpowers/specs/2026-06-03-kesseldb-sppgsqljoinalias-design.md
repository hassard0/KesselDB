# SP-PG-SQL-JOIN-ALIAS — table aliases in JOIN queries

**Date:** 2026-06-03
**Status:** SHIPPED
**Arc:** SP-PG-SQL-JOIN-ALIAS

## The gap (deferred by SP-PG-SQL-MULTI-JOIN)

The parser accepted `FROM users u JOIN posts p ON …` but column qualifiers only
resolved against the FULL table name, so the universal ORM/SQL form failed:

```sql
SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id;
--     ^^      ^^                                   ^^   ^^
--     the u./p. alias qualifiers did NOT resolve
```

SQLAlchemy and Django emit aliased joins constantly (`FROM users AS u`, or the
implicit `FROM users u`), so this is high real-world value. This arc builds an
alias→table map from the FROM/JOIN clause and resolves EVERY qualifier (ON,
WHERE, projection, ORDER BY, GROUP BY) through it, for binary AND multi-table
(3+) INNER joins.

## Design

### Where resolution happens — entirely in `kessel-sql`

The engine's join result is a self-describing `KTR1` stream whose embedded
schema names columns by FULL table name (`users.name`, `posts.title`). So the
alias must be resolved to the full table name BEFORE matching against that
schema. We do this **entirely in the SQL layer**: aliases are rewritten to full
table names while parsing, so:

- the emitted wire `Op` is **byte-identical** to the spelled-out full-name form
  (an aliased join and its full-name twin compile to the SAME `Op`) — **no
  determinism risk, no oracle literal churn, no `Op`/proto change**;
- `crates/kessel-pg-gateway/src/dispatch.rs` (`render_join_result`) is
  **unchanged** — the `JoinProjCol.qualifier` reaching it is already the full
  table name.

### The alias map + resolver

```rust
struct JoinTableRef { table: String, alias: Option<String> }

fn resolve_join_qualifier(refs, qual) -> Result<String, SqlError>
//   full table name      → itself      (back-compat)
//   alias of one table   → its table
//   anything else        → clean Err   (never a silent mis-resolution)

fn validate_join_refs(refs) -> Result<(), SqlError>
//   duplicate alias                         → Err
//   alias shadows another table's real name → Err
//   same table joined twice (self-join)     → Err (named follow-up)
```

`[AS] <alias>` is parsed after each table name (`AS` optional, per SQL-92); a
clause-starting keyword (`ON`, `JOIN`, `WHERE`, `GROUP`, `ORDER`, …) is NOT an
implicit alias.

### Two integration points (both in `crates/kessel-sql/src/lib.rs`)

1. **Engine compile path (`compile_select`).** Capture the optional `[AS]
   <alias>` for the left table, the right table, and every chained table into a
   running `refs: Vec<JoinTableRef>` (validated incrementally). Resolve every
   qualifier through `refs` BEFORE the existing full-name resolution:
   - base ON (`a1`/`a2`) and each chain-step ON (`q1`/`q2`);
   - `WHERE` — `compile_join_where` / `compile_join_where_multi` gained a `refs`
     param and resolve a qualified token's alias before matching the combined
     schema;
   - `GROUP BY` + aggregate args + `HAVING` — the shared `resolve_combined`
     closure resolves the qualifier first;
   - `ORDER BY` — the qualifier is resolved before the combined-name lookup.

   The left alias is consumed **speculatively** and kept only if a JOIN keyword
   follows, so a single-table `SELECT * FROM users u` (a different shape handled
   by the paths below) stays byte-identical.

2. **Gateway projection text-helpers (`join_projection`, `join_group_aggregate`).**
   These recover the projection from SQL text (the engine discards it) and have
   NO catalog — but the aliases are declared right in the same FROM/JOIN clause,
   so they build the same `refs` by re-walking the clause and rewrite each
   `JoinProjCol.qualifier` (and the group-by qualifier) from alias → full table
   name. An unresolvable qualifier ⇒ `None` ⇒ the gateway renders the standard
   42703 column-does-not-exist error (never a mis-render).

## V1 scope + named follow-ups

- **In:** `FROM t1 [AS] a1 JOIN t2 [AS] a2 [ON …] [JOIN t3 [AS] a3 ON …]` —
  binary + multi-table (3+) INNER chains, now with aliases resolved in
  projection, ON, WHERE, ORDER BY, and GROUP-BY-over-join. `SELECT *` and
  FULL-table-name qualifiers keep working (back-compat). `AS` and implicit alias
  forms both work.
- **Clean errors (not mis-resolution):** unknown qualifier, duplicate/ambiguous
  alias, alias shadowing another table's name.
- **Deferred:** a **self-join with two aliases of the SAME table**
  (`FROM users a JOIN users b ON …`). The combined `KTR1` schema would have
  duplicate `<table>.<col>` names (both sides are `users.*`), so the
  alias→full-name rewrite would be ambiguous against that schema. Rejected with a
  clear error (`validate_join_refs`); it is a NAMED follow-up
  (`SP-PG-SQL-SELF-JOIN`) requiring the combined schema to carry per-instance
  names. This is the only deferred sub-case and it adds real risk, so it is gated
  rather than silently mis-resolved.

## Determinism

Pure SQL-layer parse/resolve: the alias is rewritten to the full table name, so
the emitted `Op` is byte-identical to the full-name form. No `Op`/proto touched,
no construction-site churn, no oracle literal changes. The 3-replica byte-
identity + seed-corpus + partition-corpus oracles are untouched by construction.
