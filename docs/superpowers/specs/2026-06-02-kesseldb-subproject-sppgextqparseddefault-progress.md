# SP-PG-EXTQ-PARSED-DEFAULT — flip the gateway typed-param path to default — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T4 (2026-06-02).** Typed-param path
is now the gateway DEFAULT for every bound parameter the classifier
returns `Some` for; the text-substitution path remains as the
fallback for FLOAT/TIMESTAMPTZ/NUMERIC + BYTEA binary. HEADLINE
closure: the SP-PG-EXTQ V1 §11 weak-spot #1 attack surface is now
closed at the DISPATCH layer (V1 closed it at the kessel-sql +
classifier layer only). vulcan-verified with psycopg2 + asyncpg +
psycopg3 smoke regression-free AND the headline quote-injection
wire test (`"; DROP TABLE inj_smoke; --` payload stored verbatim;
table NOT dropped; post-injection INSERT succeeds → 2 rows visible).
**TaskList #376 ready for completion.**

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparseddefault-design.md`
Parent SP-arc: SP-PG-EXTQ-PARSED V1 (closed 2026-06-02 at T4).

## What this SP-arc ships

A new `EngineApply::apply_sql_with_params(sql, params: &[Option<Value>])
-> OpResult` trait method (default impl forwards to `apply_sql` for
backward compat). On `kesseldb-server::EngineHandle` the real impl
sends a new `PARAMETERIZED_SQL_TAG = 0xF3` admin frame whose decode
on the engine thread runs `kessel_sql::compile_stmt_with_params`
against the live catalog. The gateway `dispatch_execute` flips to
prefer the typed path; the text-substitution path stays as the
fallback for FLOAT/TIMESTAMPTZ/NUMERIC + BYTEA-binary.

## Slice plan (mirrors design spec §4)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1+T2** | Design spec + progress tracker + `EngineApply::apply_sql_with_params` trait method + `PARAMETERIZED_SQL_TAG` constant + wire encode/decode + render_params_into_sql + EngineHandle override + dispatch flip + +11 KATs (4 wire encoder + 2 render helper + 7 dispatch flip with the headline quote-injection KAT at the dispatch layer). | **DONE** | `9ccbe82` |
| **T3** | vulcan ORM smoke + injection test. psycopg2 + asyncpg + psycopg3 smoke regression-free; quote-injection wire test confirms table NOT dropped (payload stored verbatim; post-injection INSERT succeeds → 2 rows visible). | **DONE** | (no commit — verification-only) |
| **T4** | USAGE §9 note + STATUS row + progress tracker → CLOSED. | **DONE** | (this slice) |

## Out-of-scope (named V2+ follow-ups)

- **SP-PG-EXTQ-PARSED-INFER** — per-position OID-driven type inference
  at Parse time (deferred from V1).
- **SP-PG-EXTQ-PARSED-CACHE** — pre-compiled prepared-statement AST
  cache (deferred from V1).
- **SP-PG-EXTQ-PARSED-BYTEA-TYPED** — typed-path support for BYTEA
  binary. **CLOSED 2026-06-02** by the same-named arc; non-UTF8
  bytes now round-trip byte-equal through the typed path.

## References

- Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparseddefault-design.md`
- Parent V1 spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparsed-design.md`
- Parent V1 tracker (CLOSED): `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgextqparsed-progress.md`
- Gateway dispatcher: `crates/kessel-pg-gateway/src/extq/mod.rs::dispatch_execute`
- Classifier: `crates/kessel-pg-gateway/src/extq/substitute.rs::preprocess_typed_params`
- kessel-sql entry: `crates/kessel-sql/src/lib.rs::compile_with_params`
- Engine bridge: `crates/kesseldb-server/src/lib.rs` (EngineApply impl on EngineHandle)
