# SP-PG-EXTQ-PARSED — kessel-sql `$N` parameter token + typed-param threading — SP-arc Progress Tracker

Date created: 2026-06-02
**Status:** IN-PROGRESS (T1 design + lexer DONE; T2-T4 queued)

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
| **T1** | Design spec (this file's companion) + progress tracker + lexer extension (`Tok::Param(u16)` recognition for `$1..$99` + `$0` error + `$100+` error + bare-`$` error) + 7 lexer KATs locking `$N` token shape. Parser still rejects `Tok::Param` in any position — until T2 lands, a `Tok::Param` reaching the parser falls through to the existing `_ => Err(...)` arms. | **DONE** | (this slice) |
| **T2** | `Lit::Param(u16)` variant + value-position acceptance + `compile_with_params(sql, cat, params)` entry point + typed-param threading through INSERT/UPDATE/DELETE/SELECT/WHERE. KATs: bare lex/parse round-trip for `Tok::Param`; `compile_with_params` injection for INSERT VALUES, UPDATE SET, WHERE predicate, multi-row VALUES, JOIN ON; NULL injection via `Vec<Option<Value>>`; out-of-bounds `$N` rejection; quote-injection adversarial KAT (the headline security improvement). | **QUEUED** | — |
| **T3** | Gateway scaffold — opt-in route through `compile_with_params` for the int/text/bytea/bool subset; text-substitution path stays as default fallback. New helper `extq::substitute::preprocess_typed_params` returns `Vec<Option<Value>>` for typed-path-eligible params, `None` overall when any parameter falls outside the V1 typed subset (so the dispatcher routes that Bind through the existing text path). KATs: typed path covers `pgJDBC setInt(1, 42)` + `psycopg2 (b"hello",)`; text path unchanged for FLOAT/TIMESTAMPTZ. NO default flip (still text by default; the typed path is exercised only by the KAT harness). | **QUEUED** | — |
| **T4** | USAGE §9 note + STATUS row + progress tracker → CLOSED. | **QUEUED** | — |

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
