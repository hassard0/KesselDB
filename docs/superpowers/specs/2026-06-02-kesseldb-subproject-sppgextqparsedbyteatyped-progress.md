# SP-PG-EXTQ-PARSED-BYTEA-TYPED — typed-path BYTEA support that preserves arbitrary bytes — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: IN PROGRESS.**

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparsedbyteatyped-design.md`
Parent SP-arc: SP-PG-EXTQ-PARSED-DEFAULT V1 (closed 2026-06-02).

## What this SP-arc ships

A new `Tok::Bytes(Vec<u8>)` + `Lit::Bytes(Vec<u8>)` lexer-token +
parser-literal pair that threads raw bytes losslessly through the
SQL parser when a bound parameter's `Value::Blob` is materialized
into a value-position token. Drops the
`String::from_utf8_lossy(b).into_owned()` call in
`kessel_sql::rewrite_param_tokens` that previously corrupted non-
UTF8 byte sequences. Removes the BYTEA-binary carve-out in
`extq::substitute::preprocess_binary_value` so PG_TYPE_BYTEA flows
through the typed path uniformly with INT/BOOL/TEXT/VARCHAR.

## Slice plan (mirrors design spec §4)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1+T2** | Design spec + progress tracker + `Tok::Bytes` + `Lit::Bytes` + `rewrite_param_tokens` re-route + value-position parser arms + `lit_to_value` Lit::Bytes route + `preprocess_binary_value` BYTEA admission + KATs locking lossless non-UTF8 round-trip. | PENDING | — |
| **T3** | vulcan psycopg2 smoke: non-UTF8 BYTES bind → store → SELECT round-trip with byte-equal verification. | PENDING | — |
| **T4** | USAGE §9 note + STATUS row + progress tracker → CLOSED. | PENDING | — |

## Out-of-scope (named V2+ follow-ups)

- **SP-PG-EXTQ-PARSED-BYTEA-TEXT-DECODE** — text-format BYTEA
  decoding `'\xHEX'` → `Value::Blob` at the gateway. V1 keeps the
  pass-through-as-is shape (stores the escape literal text).
- **SP-PG-EXTQ-PARSED-INFER** — per-position OID-driven type
  inference at Parse time (deferred from V1).
- **SP-PG-EXTQ-PARSED-CACHE** — pre-compiled prepared-statement AST
  cache (deferred from V1).

## References

- Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparsedbyteatyped-design.md`
- Parent V1 spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparseddefault-design.md`
- Token rewriter: `crates/kessel-sql/src/lib.rs::rewrite_param_tokens`
- Classifier: `crates/kessel-pg-gateway/src/extq/substitute.rs::preprocess_binary_value`
- Codec values: `crates/kessel-codec/src/lib.rs::Value`
