# SP-PG-EXTQ-PARSED-DEFAULT — flip the gateway typed-param path to default — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: IN PROGRESS.** V1 typed-param path was opt-in; this arc
flips `dispatch_execute` so the typed path becomes the default
runtime route, with the text-substitution path remaining as a narrow
fallback for parameter shapes the typed classifier returns `None`
for (FLOAT4/FLOAT8/TIMESTAMPTZ/NUMERIC + BYTEA binary). HEADLINE:
closes the SP-PG-EXTQ V1 §11 weak-spot #1 attack surface at the
dispatch layer (V1 closed it at the kessel-sql + classifier layer
only).

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
| **T1+T2** | Design spec + progress tracker + `EngineApply::apply_sql_with_params` trait method + `PARAMETERIZED_SQL_TAG` constant + wire encode/decode + render_params_into_sql + EngineHandle override + dispatch flip + +11 KATs. | **(pending commit)** | |
| **T3** | vulcan ORM smoke + injection test. | (pending) | |
| **T4** | USAGE §9 note + STATUS row + progress tracker → CLOSED. | (pending) | |

## Out-of-scope (named V2+ follow-ups)

- **SP-PG-EXTQ-PARSED-INFER** — per-position OID-driven type inference
  at Parse time (deferred from V1).
- **SP-PG-EXTQ-PARSED-CACHE** — pre-compiled prepared-statement AST
  cache (deferred from V1).
- **SP-PG-EXTQ-PARSED-BYTEA-TYPED** — typed-path support for BYTEA
  binary (currently falls back to text path because the rewrite uses
  `String::from_utf8_lossy` which corrupts non-UTF8 byte sequences).

## References

- Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparseddefault-design.md`
- Parent V1 spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparsed-design.md`
- Parent V1 tracker (CLOSED): `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgextqparsed-progress.md`
- Gateway dispatcher: `crates/kessel-pg-gateway/src/extq/mod.rs::dispatch_execute`
- Classifier: `crates/kessel-pg-gateway/src/extq/substitute.rs::preprocess_typed_params`
- kessel-sql entry: `crates/kessel-sql/src/lib.rs::compile_with_params`
- Engine bridge: `crates/kesseldb-server/src/lib.rs` (EngineApply impl on EngineHandle)
