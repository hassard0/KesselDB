# SP-PG-EXTQ-PARSED-BYTEA-TYPED — typed-path BYTEA support that preserves arbitrary bytes — DESIGN

**Status:** design — scopes the named V2 follow-up
`SP-PG-EXTQ-PARSED-BYTEA-TYPED` in
`docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparseddefault-design.md`
§2.2 V2 follow-ups. SP-PG-EXTQ-PARSED-DEFAULT V1 (closed 2026-06-02)
flipped the typed-param path to be the gateway default for every
parameter the classifier returns `Some` for; BYTEA binary still
falls back to the text-substitution path because the typed path's
`kessel_sql::rewrite_param_tokens` does
`String::from_utf8_lossy(b).into_owned()` when materializing a
`Value::Blob` into `Tok::Str` — the lossy UTF-8 conversion
corrupts non-UTF8 byte sequences BEFORE they reach the engine's
storage layer. This arc threads raw bytes losslessly through the
parser by adding a `Tok::Bytes(Vec<u8>)` + `Lit::Bytes(Vec<u8>)`
pair, routing `Value::Blob` to `Tok::Bytes`, and accepting that
shape at every value-position parse site.

Companion progress tracker:
`docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgextqparsedbyteatyped-progress.md`.

**Builds on:**
- **SP-PG-EXTQ-PARSED-DEFAULT V1** (closed 2026-06-02) — the
  gateway-default typed-param path. This arc removes the BYTEA-
  binary carve-out so it no longer needs the text-substitute
  fallback.
- **SP-PG-EXTQ-PARSED V1** (closed 2026-06-02) — the
  `compile_with_params` + `rewrite_param_tokens` token-rewrite
  pair. This arc widens the rewriter to emit `Tok::Bytes` for
  `Value::Blob` instead of `Tok::Str`.

---

## 1. Context — why the carve-out exists today

The V1 disposition in `extq::substitute::preprocess_binary_value`
explicitly carves out PG_TYPE_BYTEA from the typed path:

```rust
// SP-PG-EXTQ-PARSED-DEFAULT T2 — BYTEA BINARY route needs the
// `'\xHEX'::bytea` cast wrapper that only the text-substitution
// path emits (kessel-sql's `rewrite_param_tokens` does a
// utf8-lossy cast of the bytes into `Tok::Str` which is wrong
// for non-UTF8 byte sequences). Fall back so the V1 cast-
// wrapper path renders the literal correctly.
PG_TYPE_BYTEA => None,
```

The fall-back works for the round-trip case where the bytes ARE
valid UTF-8 (the text-substitute path's `\xHEX::bytea` cast
preserves arbitrary bytes through the cast). But the typed path
is the more secure shape (no SQL text concatenation, no escape
rules) AND every other supported type already routes through it.
Closing this carve-out is a strict improvement on the security
+ uniformity axes.

The actual fix-site is `kessel_sql::rewrite_param_tokens` — its
`Value::Blob(b)` arm produces `Tok::Str(String::from_utf8_lossy(b))`
which corrupts any byte that isn't a valid UTF-8 start sequence
(notably `0xFE`, `0xFF`, and most isolated continuation bytes
`0x80..=0xBF`). The bytes never enter SQL text BUT they do round-
trip through a String, and the String constructor mangles them.

## 2. Scope

### 2.1 V1 — what's in (this arc, T1..T4)

1. **`Tok::Bytes(Vec<u8>)` + `Lit::Bytes(Vec<u8>)`.** New variants
   on the lexer-token + parser-literal enums in
   `crates/kessel-sql/src/lib.rs`. NOT producible by the lexer (no
   surface syntax binds to it); reserved for the param-rewrite
   path.

2. **`rewrite_param_tokens` re-route.** `Value::Blob(b)` →
   `Tok::Bytes(b)` (no UTF-8 round-trip). Drops the
   `String::from_utf8_lossy` call entirely on the blob path.

3. **Value-position parsers accept `Tok::Bytes`.** The places that
   already accept `Tok::Str` for value positions (INSERT VALUES,
   UPDATE SET, WHERE-clause RHS for `=`/range comparisons) gain a
   `Tok::Bytes` arm that produces `Lit::Bytes` (in the parser) or
   the equivalent byte-vec for index hints. DDL-string contexts
   (CREATE EXTERNAL SOURCE, COPY format options) DO NOT need the
   new shape — they're not bound-parameter positions.

4. **`lit_to_value` routing for `Lit::Bytes`.**
   - `Lit::Bytes(b)` + `Char(_) | Bytes(_) | Ref | OverflowRef`
     → `Value::Blob(b)` (raw bytes preserved, NO UTF-8 round-trip).
   - `Lit::Bytes(b)` + numeric kinds: attempt
     `std::str::from_utf8(&b).and_then(parse::<i128>())` — if the
     bytes are valid UTF-8 + a clean decimal, coerce; else the
     existing `literal/column type mismatch` error fires (same
     shape as `Lit::Str` with a non-numeric string).

5. **Typed-path BYTEA admission.**
   `preprocess_binary_value(bytes, PG_TYPE_BYTEA)` → `Some(Value::
   Blob(bytes.to_vec()))` (drop the `None` carve-out).
   `preprocess_text_value(bytes, PG_TYPE_BYTEA)` already returns
   `Some(Value::Blob(bytes.to_vec()))` — verify, no change.

6. **KATs.**
   - `rewrite_param_tokens` preserves non-UTF8 bytes through the
     blob arm (no corruption).
   - INSERT VALUES with a `$1` bound to non-UTF8 `Value::Blob`
     stores the original bytes verbatim.
   - WHERE clause `data = $1` with non-UTF8 `Value::Blob` matches
     a row with those exact bytes.
   - `preprocess_binary_value(PG_TYPE_BYTEA, [0x00, 0xFF])` →
     `Some(Value::Blob(vec![0x00, 0xFF]))`.
   - End-to-end gateway: `compile_with_params` + the gateway's
     `preprocess_typed_params` round-trip non-UTF8 bytes byte-for-
     byte.

### 2.2 V1 — what's out (deferred)

- **BYTEA text-format `\xHEX` parse on the wire.** The typed
  path's `preprocess_text_value` for BYTEA passes raw wire bytes
  through as `Value::Blob`; the BYTES storage layer accepts them
  as-is. Decoding the `\xHEX` shape would be a behavior change
  (V1 stores the escape literal text into the bytes column,
  matching the V1 text-substitute path). Deferred to a future
  arc named `SP-PG-EXTQ-PARSED-BYTEA-TEXT-DECODE`.
- **NUMERIC / FLOAT / TIMESTAMPTZ typed-path support.** Still
  carved out in `preprocess_binary_value` because `Value` doesn't
  carry float/timestamp variants yet. Out of scope; named follow-
  ups already exist.

## 3. Acceptance criteria

V1 (T1..T4) ships when:

1. **`Tok::Bytes(Vec<u8>)` + `Lit::Bytes(Vec<u8>)` exist** with
   `rewrite_param_tokens` routing `Value::Blob` → `Tok::Bytes`.
2. **Value-position parsers accept `Tok::Bytes`** at INSERT VALUES,
   UPDATE SET, WHERE comparison RHS.
3. **`lit_to_value(Lit::Bytes, Char|Bytes|Ref|OverflowRef)`** →
   `Value::Blob(b)` losslessly (no UTF-8 round-trip).
4. **`preprocess_binary_value(PG_TYPE_BYTEA, _)`** returns
   `Some(Value::Blob(bytes.to_vec()))`.
5. **All prior KATs still pass** byte-equal. No regression in extq /
   substitute / kessel-sql.
6. **+5-10 new KATs** locking the lossless byte path.
7. **vulcan psycopg2 smoke**: insert non-UTF8 BYTES via parameter
   binding → SELECT round-trip returns the same bytes. HEADLINE.
8. **CI green.** `#![forbid(unsafe_code)]` honored. No new external
   deps.

## 4. Task decomposition (T1..T4)

| T# | Scope | KAT delta |
|---|---|---|
| **T1+T2** | Design spec + progress tracker + `Tok::Bytes` + `Lit::Bytes` + `rewrite_param_tokens` re-route + value-position parser arms + `lit_to_value` route + `preprocess_binary_value` BYTEA admission + KATs locking lossless non-UTF8 round-trip. | +6 |
| **T3** | vulcan psycopg2 smoke: non-UTF8 BYTES bind → store → SELECT round-trip with byte-equal verification. | 0 |
| **T4** | USAGE §9 note + STATUS row + progress tracker → CLOSED. | 0 |

Estimated V1 total: **+6 KATs across 3 commits**.

## 5. References

- Parent V1 spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparseddefault-design.md`
- Parent V1 tracker (CLOSED): `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgextqparseddefault-progress.md`
- Token rewriter: `crates/kessel-sql/src/lib.rs::rewrite_param_tokens`
- Classifier: `crates/kessel-pg-gateway/src/extq/substitute.rs::preprocess_binary_value`
- Codec values: `crates/kessel-codec/src/lib.rs::Value`
