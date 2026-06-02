# SP-PG-EXTQ-PARSED-FUNCTIONS — close the scalar-function text-fallback gap — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: IN PROGRESS — T1 design + diagnosis landed.**

## DIAGNOSIS headline: Reality A — the named follow-up is REDUNDANT

Scalar-function SELECTs (`SELECT version()`, `current_database()`,
`current_schema()`, `current_user`, `SELECT 1`, …) do **NOT** fall
through to the text-substitute path under the
SP-PG-EXTQ-PARSED-DEFAULT typed-default regime. They are intercepted by
`pg_catalog::catalog_query_hook` at the TOP of BOTH dispatch entry
points (`dispatch_query_with_params` at `dispatch.rs:111` AND
`dispatch_query` at `dispatch.rs:273`), **before** the typed/text branch
and **before** any `engine.apply_sql*` / `select_star_table` call.
`preprocess_typed_params` returns `Some(vec![])` for 0-param SQL, so the
typed branch is taken, and that branch hooks the catalog FIRST. No text
concatenation, no engine call, no correctness or security gap.

The arc therefore ships **regression-lock KATs only** + this honest
closure. No new function support, no routing change, no behavior delta.
SQL/PG-wire surface byte-untouched.

Design spec (full diagnosis with code-line evidence):
`docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparsedfunctions-design.md`

## What this SP-arc ships

End-to-end Parse → Bind → Execute regression-lock KATs proving
scalar-function SELECTs are answered by the catalog synthesizer through
the full Extended Query machinery and **never reach the engine's typed
or text path** (locked via a panic-on-engine-call test engine). Plus the
honest "redundant follow-up" closure.

## Slice plan (mirrors design spec §5)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + diagnosis + progress tracker. | **DONE** | (this slice) |
| **T2** | +5–7 end-to-end regression-lock KATs in `extq/mod.rs`. | TODO | — |
| **T3** | vulcan psycopg3 Extended-Query smoke. | TODO | — |
| **T4** | STATUS row + tracker → CLOSED + smoke transcript. | TODO | — |

## Out-of-scope (named follow-up)

- **SP-PG-EXTQ-PARSED-FUNCTIONS-PARAM** — gateway-evaluated minimal set
  of PARAMETERIZED scalar functions (`upper($1)`, `lower($1)`,
  `length($1)`, `coalesce($1,$2)`). Deferred as YAGNI: no ORM
  connect-probe issues parameterized scalar functions; today they hit
  honest rejection (kessel-sql rejects the unsupported projection), not
  a silent wrong answer.

## References

- Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparsedfunctions-design.md`
- Parent arc tracker (CLOSED): `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgextqparseddefault-progress.md`
- Gateway dispatcher: `crates/kessel-pg-gateway/src/extq/mod.rs::dispatch_execute`
- Typed-path dispatch: `crates/kessel-pg-gateway/src/dispatch.rs::dispatch_query_with_params`
- Catalog hook: `crates/kessel-pg-gateway/src/pg_catalog/mod.rs::catalog_query_hook`
- Scalar synthesizers: `crates/kessel-pg-gateway/src/pg_catalog/synthesize.rs::synthesize_helper_function`
