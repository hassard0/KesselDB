# SP-PG-EXTQ-PARSED — kessel-sql `$N` parameter token + typed-param threading — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T4 (2026-06-02).** kessel-sql lexer
recognizes `$N` as `Tok::Param`; `compile_with_params(sql, cat,
params)` threads typed `Value`s through the parser WITHOUT SQL-text
concatenation; gateway classifier `preprocess_typed_params` routes
typed-path-eligible Binds. HEADLINE security KAT locked end-to-end:
a quote-injection payload survives as a `Value::Blob` operand at the
EQ comparison, never as injected SQL. V1 disposition: typed path is
opt-in (KAT-only); default `dispatch_execute` still uses the text-
substitution path. Follow-up `SP-PG-EXTQ-PARSED-DEFAULT` flips the
default after soak. **TaskList #374 ready for completion.**

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparsed-design.md`
Parent SP-arc: SP-PG-EXTQ V1 (closed 2026-05-29 at T8). SP-PG-EXTQ
V1 §11 weak-spot #1 named this follow-up as the structurally-correct
fix for the SQL-text-substitution attack surface.

## What this SP-arc ships

The kessel-sql lexer recognizes `$N` (1..99) as a new `Tok::Param(u16)`
variant. The compile pipeline gains a `compile_with_params(sql, cat,
params: &[Option<Value>]) → Op` entry point that rewrites each
`Tok::Param(n)` to the typed equivalent of `params[n-1]` AFTER lex +
BEFORE parse. The bound value enters as a `Value` (typed) and emerges
in the program as the same `Value` — no SQL text concatenation, no
quoting, no escape rules. Closes the V1 §11 weak-spot #1 attack
surface for every typed-path-eligible parameter.

## Slice plan (mirrors design spec §4)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + progress tracker + lexer extension (`Tok::Param(u16)` recognition for `$1..$99` + `$0` error + `$100+` error + bare-`$` error) + 7 lexer KATs locking `$N` token shape. | **DONE** | `d4d6366` |
| **T2** | `compile_with_params(sql, cat, params: &[Option<Value>])` + `compile_stmt_with_params(...)` + token-rewrite + +12 KATs covering INSERT VALUES, UPDATE SET, WHERE predicate, multi-position ordering, same-`$N`-twice, NULL injection, out-of-bounds rejection, bare-compile rejection, no-placeholders pass-through, mixed bare-literal, `Value::Uint` coercion, AND the headline quote-injection adversarial KAT. Internal refactor extracts `compile`/`compile_stmt` bodies into `compile_from_tokens` / `compile_stmt_from_tokens` so params + bare paths share one parser dispatch. | **DONE** | `fd7fdd1` |
| **T3** | Gateway classifier `extq::substitute::preprocess_typed_params(params, formats, oids) -> Option<Vec<Option<Value>>>`; per-OID routing (INT2/4/8 / BOOL / TEXT/VARCHAR/BYTEA → typed; FLOAT4/8 / TIMESTAMPTZ / NUMERIC → fallback). +12 KATs including the gateway-end-to-end HEADLINE security KAT (payload routes through gateway → kessel-sql → program, never as SQL text). | **DONE** | `de9dbea` |
| **T4** | USAGE §9 note + STATUS row + progress tracker → CLOSED. | **DONE** | (this slice) |

## Out-of-scope (named V2+ follow-ups)

- **SP-PG-EXTQ-PARSED-INFER** — per-position OID-driven type inference
  at Parse time.
- **SP-PG-EXTQ-PARSED-DEFAULT** — default-flip of the gateway substitute
  path from text-substitution → typed-param after a soak.
- **SP-PG-EXTQ-PARSED-CACHE** — pre-compiled prepared-statement AST cache
  (avoids re-lex/re-parse on every Execute).
- **Parameterized DDL** — `CREATE TABLE t (col $1)` doesn't work in PG
  itself; V1 returns `SqlError::ParamInDdl`.
- **Identifier substitution** — `SELECT * FROM $1` doesn't work; PG
  itself forbids it.

## References

- Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparsed-design.md`
- Parent V1 spec: `docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
- Parent V1 tracker (CLOSED): `docs/superpowers/specs/2026-05-28-kesseldb-subproject-sppgextq-progress.md`
- Lexer: `crates/kessel-sql/src/lib.rs::lex` (T1 lands `Tok::Param`)
- Gateway bridge: `crates/kessel-pg-gateway/src/extq/substitute.rs` (T3
  adds `preprocess_typed_params`)
- Gateway dispatcher: `crates/kessel-pg-gateway/src/extq/mod.rs::
  dispatch_execute` (T3 adds the opt-in branch)
