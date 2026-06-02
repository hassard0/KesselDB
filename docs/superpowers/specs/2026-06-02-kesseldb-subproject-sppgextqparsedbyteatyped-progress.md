# SP-PG-EXTQ-PARSED-BYTEA-TYPED — typed-path BYTEA support that preserves arbitrary bytes — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T4 (2026-06-02).** The typed-param
path now uniformly carries BYTEA values (and every other
`Value::Blob`-bound parameter) without the prior
`String::from_utf8_lossy` UTF-8 round-trip that corrupted non-UTF8
byte sequences. kessel-sql's new `Tok::Bytes(Vec<u8>)` +
`Lit::Bytes(Vec<u8>)` variants thread raw bytes through the parser
into `lit_to_value` losslessly; the gateway's
`preprocess_binary_value(PG_TYPE_BYTEA, _)` now returns
`Some(Value::Blob(bytes.to_vec()))` (was `None`, forcing the text-
substitute fallback). **vulcan-verified**: psycopg3 binary-format
INSERT of payloads `fffefd8090a0b0c0`, `0000000000000000`,
`deadbeefcafebabe` all round-trip byte-equal. **TaskList #378 ready
for completion.**

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
| **T1+T2** | Design spec + progress tracker + `Tok::Bytes` + `Lit::Bytes` + `rewrite_param_tokens` re-route + value-position parser arms + `lit_to_value` Lit::Bytes route + `preprocess_binary_value` BYTEA admission + KATs locking lossless non-UTF8 round-trip. +10 KATs (6 kessel-sql + 4 kessel-pg-gateway). | **DONE** | `7ae8042` |
| **T3** | vulcan psycopg3 binary-format BYTEA smoke: non-UTF8 BYTES bind → store → SELECT round-trip with byte-equal verification (3 payloads). | **DONE** | (no commit — verification-only; transcript in `docs/superpowers/sppgextqparsedbyteatyped-t3-smoke-2026-06-02.txt`) |
| **T4** | USAGE §9 note + STATUS row + progress tracker → CLOSED. | **DONE** | (this slice) |

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
- vulcan smoke transcript: `docs/superpowers/sppgextqparsedbyteatyped-t3-smoke-2026-06-02.txt`
