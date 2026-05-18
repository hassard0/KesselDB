# KesselDB Sub-project 65 — `kessel-crypto` (pgcrypto-equivalent subset)

**Date:** 2026-05-17  **Status:** shipped, test-vector-verified. 165 green.

## Scope (stated honestly, by user decision)

The **safely hand-rollable** pgcrypto subset: **SHA-256** (FIPS 180-4) and
**HMAC-SHA256** (RFC 2104). These are well-specified, deterministic, pure
functions — a perfect fit for KesselDB's replicated state machine (a hash
is identical on every replica). We deliberately do **not** hand-roll
symmetric encryption or TLS here (that is the opt-in `tls` server
feature) — that would be irresponsible amateur crypto.

## Delivered

- New zero-dependency crate `kessel-crypto`: `sha256(&[u8]) -> [u8;32]`,
  `hmac_sha256(key, msg) -> [u8;32]`, `hex(&[u8]) -> String`. Verified
  against published **NIST/FIPS 180-4** SHA-256 vectors (`""`, `"abc"`,
  the 56-byte case) and **RFC 4231** HMAC-SHA256 vectors (cases 1, 2, and
  the long-key path).
- Deterministic expr-VM opcodes `SHA256 (21)` and `HMAC256 (22)` +
  `Program::sha256()/hmac256()`. Because they live in the gas-bounded
  deterministic VM, pgcrypto-style hashing now works **inside `CHECK`
  constraints and triggers** — e.g. a trigger can hash-on-write a column,
  or a `CHECK` can enforce an HMAC — bit-identically on every replica.

## Tests (4 new, 165 total)

`kessel-crypto`: 3 known-answer tests (NIST SHA-256, RFC 4231 HMAC incl.
>block-size key, determinism). `kessel-expr`:
`sha256_and_hmac_opcodes_are_correct_and_deterministic` (opcode result
equals the library digest via an `eq` predicate; wrong digest → false, no
panic; HMAC RFC-4231 case-2 through the VM). Full workspace regression
green (165) — the new opcodes don't perturb the determinism/VSR corpus.

## Honest scope boundary

Hashing + HMAC only (SHA-256 family). No symmetric encryption, no
password-hashing KDF (`crypt()`/`gen_salt`), no SQL `digest()` scalar
*function syntax* yet — the primitive is exposed via the library and the
expr VM (CHECK/trigger), which is where deterministic hashing belongs;
a SQL `digest(col,'sha256')` projection function is a named follow-up
(grammar work). Legacy MD5/SHA-1 deliberately omitted (user chose the
SHA-256-only safe set).
