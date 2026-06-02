# SP-PG-EXTQ-BIN-NUMERIC — PostgreSQL Extended Query binary-format NUMERIC — DESIGN

**Status:** design — scopes the V2 follow-up named in
`docs/superpowers/specs/2026-06-01-kesseldb-sppgextq-bin-design.md` §2.2
and `docs/superpowers/specs/2026-06-01-kesseldb-sppgextq-bin-results-design.md`
§2.2 (both V1 CLOSED at T3 on 2026-06-01). Mirrored progress trackers:
`2026-06-01-kesseldb-subproject-sppgextqbin-progress.md` +
`2026-06-01-kesseldb-subproject-sppgextqbinr-progress.md`.

SP-PG-EXTQ-BIN V1 + SP-PG-EXTQ-BIN-RESULTS V1 closed binary-format
parameter decode AND binary-format result encode for the common PG
scalar types — BOOL, INT2/4/8, FLOAT4/8, TEXT/VARCHAR, BYTEA,
TIMESTAMPTZ. NUMERIC was deferred to this follow-up arc because its
binary wire format is base-10000 variable-length-digit (sign + dscale
+ weight + N i16 digits), and the encoding is bug-prone enough to
warrant its own careful per-encoding KAT pass.

This arc lifts the NUMERIC-binary restriction at both the PARAM (Bind)
and RESULT (DataRow) sides, unlocking asyncpg `decimal.Decimal`,
pgJDBC `BigDecimal`, and sqlx `Decimal` column round-trips end-to-end.

**Builds on:**

- **SP-PG-EXTQ-BIN V1** (`docs/superpowers/specs/2026-06-01-kesseldb-sppgextq-bin-design.md`)
  — the `decode_binary_param(bytes, type_oid) -> Result<String,
  BinaryDecodeError>` helper in `extq/substitute.rs`. V1's NUMERIC arm
  returns `BinaryDecodeError::Unsupported { arc: "SP-PG-EXTQ-BIN-NUMERIC" }`;
  this arc replaces the early-return with a call to a new
  `decode_numeric_binary(bytes) -> Result<String, BinaryNumericError>`
  pure function. The decoded SQL-literal string flows through the same
  `PreparedParam::Text` rendering as every other numeric-shaped literal
  (single-quoted, gateway parser does the implicit cast).
- **SP-PG-EXTQ-BIN-RESULTS V1** (`docs/superpowers/specs/2026-06-01-kesseldb-sppgextq-bin-results-design.md`)
  — the `encode_binary_value(text, type_oid) -> Result<Vec<u8>,
  BinaryEncodeError>` helper in `extq/binary_results.rs`. V1's NUMERIC
  arm returns `BinaryEncodeError::Unsupported { arc: "SP-PG-EXTQ-BIN-NUMERIC" }`;
  this arc replaces the early-return with a call to a new
  `encode_numeric_binary(decimal_str) -> Result<Vec<u8>, BinaryNumericError>`
  pure function. The encoded bytes flow into the same per-column
  DataRow rewrite as every other binary-result column.
- **`crates/kessel-pg-gateway/src/proto.rs`** — the PG type OID
  catalog (`PG_TYPE_NUMERIC: u32 = 1700`).

---

## 1. Context — PG NUMERIC binary format

Per PG `src/backend/utils/adt/numeric.c::numeric_recv` +
`numeric_send`, every NUMERIC binary value carries a fixed 8-byte
header followed by a variable-length digit array. All integers are
network-byte-order (big-endian).

### 1.1 Wire layout

```
[ndigits:  i16]   // number of base-10000 digits in the digits array
[weight:   i16]   // base-10000 weight of digit[0] (see §1.2)
[sign:     u16]   // 0x0000 = positive, 0x4000 = negative, 0xC000 = NaN
[dscale:   i16]   // display scale: number of decimal digits after the point
[digit:    i16]   // digit[0] — base-10000 digit (0..9999)
[digit:    i16]   // digit[1] — base-10000 digit (0..9999)
...
[digit:    i16]   // digit[ndigits-1] — base-10000 digit (0..9999)
```

Total wire size = `8 + 2*ndigits` bytes.

### 1.2 Weight semantics

`digit[0]` carries the most-significant base-10000 digit. Its *positional
weight* is `10000^weight`. Subsequent digits' weights decrease by 1
each. The digits array has NO leading or trailing zero stripping
EXCEPT in the canonical PG encoding — but real producers may include
them. V1 tolerates them.

For example, `12345.6789` decomposes in base-10000 as:
- digit[0] = 1     (weight=1 → 10000^1 = 10000)
- digit[1] = 2345  (weight=0 → 10000^0 = 1)
- digit[2] = 6789  (weight=-1 → 10000^-1 = 1/10000)

`ndigits=3, weight=1, sign=0, dscale=4`.

### 1.3 Canonical examples

| value | ndigits | weight | sign | dscale | digits |
|---|---|---|---|---|---|
| `0` | 0 | 0 | 0x0000 | 0 | (none — header-only) |
| `42` | 1 | 0 | 0x0000 | 0 | [42] |
| `1.5` | 2 | 0 | 0x0000 | 1 | [1, 5000] |
| `12345.6789` | 3 | 1 | 0x0000 | 4 | [1, 2345, 6789] |
| `0.0001` | 1 | -1 | 0x0000 | 4 | [1] |
| `-3.14` | 2 | 0 | 0x4000 | 2 | [3, 1400] |
| `NaN` | 0 | 0 | 0xC000 | 0 | (none) |

### 1.4 Sign codes

- `0x0000` = positive number (`NUMERIC_POS` in `numeric.h`)
- `0x4000` = negative number (`NUMERIC_NEG`)
- `0xC000` = NaN (`NUMERIC_NAN`)
- (Two more PG 14+ codes exist for `+Infinity` / `-Infinity`; V1
  rejects them — they're documented in §2.2 out-of-scope.)

### 1.5 NaN special

When `sign == 0xC000`, ndigits MUST be 0 and the digits array is
empty. The value is the IEEE NaN concept (no numeric value); V1
rejects it with `BinaryNumericError::NaN` mapped to SQLSTATE `22023
invalid_parameter_value` (not `0A000` — V1 supports the encoding well
enough to recognize it; the rejection names the V2 follow-up
`SP-PG-EXTQ-BIN-NUMERIC-NAN` for operators looking to unlock it).

## 2. Scope

### 2.1 V1 — what's in (this arc, T1..T5)

1. **Pure-Rust NUMERIC binary codec** in
   `crates/kessel-pg-gateway/src/extq/binary_numeric.rs`:
   - `decode_numeric_binary(bytes: &[u8]) -> Result<String, BinaryNumericError>`
     — parse header + digit array; reconstruct the canonical decimal
     string `[sign][int_part].[frac_part]` (e.g. `"-3.14"`, `"0"`,
     `"12345.6789"`).
   - `encode_numeric_binary(decimal_str: &str) -> Result<Vec<u8>, BinaryNumericError>`
     — parse decimal string; produce the PG NUMERIC binary wire bytes.
2. **Decimal-string round-trip identity** locked: for every supported
   value, `decode(encode(s)) == s` and `encode(decode(bytes)) == bytes`
   (modulo canonical normalization — leading zeros in digit[0], trailing
   zeros after dscale).
3. **V1 supported value range**: `|value| < 10^18` with up to 18
   fractional digits. Wider values (multi-digit-array integers or
   long-tail fractions) are rejected with
   `BinaryNumericError::OutOfRange { arc: "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM" }`
   → SQLSTATE `22003 numeric_value_out_of_range`. This range
   covers Decimal/BigDecimal as ORM clients typically use them
   (i64-shaped amounts, currency, percentages, fractional rates) —
   the bignum support is a future arc.
4. **Wire into substitute decoder**: `decode_binary_param` NUMERIC
   arm calls `decode_numeric_binary`; the resulting decimal-string
   literal flows through the existing `PreparedParam::Text` path
   (single-quoted, gateway parser does the implicit cast). The
   `binary_format_supported_for_oid` predicate now answers `true` for
   `PG_TYPE_NUMERIC`.
5. **Wire into binary results encoder**: `encode_binary_value` NUMERIC
   arm calls `encode_numeric_binary`; the encoded bytes flow into
   the same per-column DataRow rewrite as every other supported type.
   The `binary_result_supported_for_oid` predicate now answers
   `true` for `PG_TYPE_NUMERIC`.
6. **COPY-BIN admission stays unchanged** — `copy/dispatch.rs` keeps
   its explicit NUMERIC rejection so the `SP-PG-COPY-BIN-NUMERIC`
   arc remains a clean follow-up. (The encoder + decoder now *could*
   handle NUMERIC for COPY too, but the per-row COPY framing has
   different recovery semantics, so COPY's deferral is kept
   independent.)
7. **psycopg2 / asyncpg / pgJDBC Decimal round-trip** flips from
   `0A000 SP-PG-EXTQ-BIN-NUMERIC` to PASS for the V1 supported range.

### 2.2 V1 — what's out (named V2+ follow-ups — each is its own arc)

- **`SP-PG-EXTQ-BIN-NUMERIC-BIGNUM`** — arbitrary-precision NUMERIC.
  Real PG NUMERIC is essentially unbounded (up to 131072 digits before
  the decimal point + 16383 after). V1 covers the common ORM range
  (`|value| < 10^18`, ≤18 fractional digits) which fits in `i128`
  accumulators. The bignum arc needs an arbitrary-precision integer
  type (or a bignum dep).
- **`SP-PG-EXTQ-BIN-NUMERIC-NAN`** — NaN binary support. V1 rejects
  `sign=0xC000` because the engine has no native NaN representation.
- **`SP-PG-EXTQ-BIN-NUMERIC-INF`** — `+Infinity` / `-Infinity` binary
  support (PG 14+). Same engine limitation.
- **`SP-PG-COPY-BIN-NUMERIC`** — NUMERIC binary inside COPY frames.
  Same encoding, different framing. Deliberately kept separate so the
  COPY arc can re-use the new codec when it lands.

## 3. Implementation sketch

### 3.1 `extq/binary_numeric.rs` (new module)

```rust
#![forbid(unsafe_code)]
#![allow(dead_code)]

const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;

/// Errors from the NUMERIC binary codec. The decoder + encoder share
/// the same enum so callers can pattern-match a single type.
#[derive(Debug, PartialEq, Eq)]
pub enum BinaryNumericError {
    /// Wire byte count smaller than the 8-byte header, or not aligned
    /// to a 2-byte digit boundary. Maps to SQLSTATE `08P01
    /// protocol_violation`.
    WrongLength { actual: usize },
    /// Wire bytes declared `ndigits=N` but the buffer only carried
    /// `M < 2*N` digit bytes. Maps to SQLSTATE `08P01`.
    Truncated { ndigits: usize, available: usize },
    /// `sign=0xC000` (NaN). Maps to SQLSTATE `22023` with a precise
    /// V2 arc name.
    NaN,
    /// Unknown sign code (not POS/NEG/NAN/INF/-INF). Maps to SQLSTATE
    /// `08P01`.
    BadSign { sign: u16 },
    /// Per-digit value out of [0, 9999]. Maps to SQLSTATE `08P01`.
    BadDigit { position: usize, value: i16 },
    /// V1 only supports `|value| < 10^18` + ≤18 fractional digits.
    /// Maps to SQLSTATE `22003 numeric_value_out_of_range`.
    OutOfRange { reason: String, arc: &'static str },
    /// Encoder: input string isn't a valid decimal literal.
    /// Maps to SQLSTATE `22P02 invalid_text_representation`.
    BadDecimalString { input: String, reason: String },
}

/// Decode a PG NUMERIC binary wire frame into the canonical decimal
/// string PG itself emits via `numeric_out` (e.g. `"42"`, `"-3.14"`,
/// `"0.0001"`).
pub fn decode_numeric_binary(bytes: &[u8]) -> Result<String, BinaryNumericError> {
    // 1. Header parse (8 bytes).
    // 2. Sign dispatch: NaN reject, INF reject (BadSign), POS/NEG continue.
    // 3. Build the integer accumulator via base-10000 digit shifts.
    // 4. Apply dscale to insert the decimal point.
    // 5. Prepend the sign.
}

/// Encode a canonical decimal string `[-]?\d+(\.\d+)?` into the PG
/// NUMERIC binary wire format.
pub fn encode_numeric_binary(decimal_str: &str) -> Result<Vec<u8>, BinaryNumericError> {
    // 1. Lex sign + integer + fraction.
    // 2. Reject ≥10^19 (out-of-range) + reject >18 fractional digits.
    // 3. Combine integer + fraction into a single i128 accumulator.
    // 4. Compute dscale = number of fractional digits.
    // 5. Compute weight = ceil(integer-part / 4) - 1.
    // 6. Pack base-10000 digits from least significant up; reverse.
    // 7. Emit header + digits.
}
```

### 3.2 Wire substitute decoder

`extq/substitute.rs::decode_binary_param`'s NUMERIC arm:

```rust
PG_TYPE_NUMERIC => crate::extq::binary_numeric::decode_numeric_binary(bytes)
    .map_err(|e| BinaryDecodeError::BadValue { type_oid, reason: render_numeric_error(&e) }),
```

The `render_numeric_error` helper maps the new error variants to
existing `BinaryDecodeError` shapes:
- `OutOfRange { arc }` → `BinaryDecodeError::Unsupported { arc, type_oid }`.
- `NaN` → `BinaryDecodeError::Unsupported { arc: "SP-PG-EXTQ-BIN-NUMERIC-NAN", type_oid }`.
- `WrongLength` / `Truncated` / `BadSign` / `BadDigit` →
  `BinaryDecodeError::BadValue { type_oid, reason }`.

The `binary_format_supported_for_oid` predicate adds `PG_TYPE_NUMERIC`.

### 3.3 Wire binary results encoder

`extq/binary_results.rs::encode_binary_value`'s NUMERIC arm:

```rust
PG_TYPE_NUMERIC => crate::extq::binary_numeric::encode_numeric_binary(
    std::str::from_utf8(text).map_err(|_| BinaryEncodeError::BadValue {
        type_oid,
        reason: "NUMERIC text must be valid UTF-8".into(),
    })?,
).map_err(|e| match e {
    BinaryNumericError::OutOfRange { arc, reason } =>
        BinaryEncodeError::Unsupported { type_oid, arc },
    other => BinaryEncodeError::BadValue { type_oid, reason: format!("{other:?}") },
}),
```

The `binary_result_supported_for_oid` predicate adds `PG_TYPE_NUMERIC`.

### 3.4 COPY-BIN admission stays restrictive

`copy/dispatch.rs` keeps the explicit `oid == PG_TYPE_NUMERIC` arm
that returns `SP-PG-COPY-BIN-NUMERIC` so the COPY follow-up arc owns
its own enablement decision.

### 3.5 KAT corpus (T2, ~15-20 KATs)

Per-encoder unit KATs in `binary_numeric.rs`:

- `t2_decode_zero_returns_zero_string` — 8-byte all-zero header → `"0"`.
- `t2_decode_one_digit_42` — header (ndigits=1, weight=0, dscale=0)
  + digit=42 → `"42"`.
- `t2_decode_one_and_a_half` — (ndigits=2, weight=0, dscale=1)
  + [1, 5000] → `"1.5"`.
- `t2_decode_pi_ish_12345_6789` — (ndigits=3, weight=1, dscale=4)
  + [1, 2345, 6789] → `"12345.6789"`.
- `t2_decode_negative_3_14` — (ndigits=2, weight=0, sign=0x4000,
  dscale=2) + [3, 1400] → `"-3.14"`.
- `t2_decode_small_fraction_0_0001` — (ndigits=1, weight=-1, dscale=4)
  + [1] → `"0.0001"`.
- `t2_decode_nan_rejected` — sign=0xC000 → `Err(NaN)`.
- `t2_decode_bad_sign_rejected` — sign=0x1234 → `Err(BadSign)`.
- `t2_decode_truncated_rejected` — ndigits=2 but only 1 digit in
  buffer → `Err(Truncated)`.
- `t2_decode_wrong_header_length_rejected` — <8 bytes → `Err(WrongLength)`.

- `t2_encode_zero` — `"0"` → 8-byte all-zero header.
- `t2_encode_42` — `"42"` → 1-digit header.
- `t2_encode_one_and_a_half` — `"1.5"` → 2-digit header.
- `t2_encode_pi_ish_12345_6789` — `"12345.6789"` → 3-digit header.
- `t2_encode_negative_3_14` — `"-3.14"` → sign=0x4000 + 2 digits.
- `t2_encode_small_fraction_0_0001` — `"0.0001"` → ndigits=1, weight=-1.
- `t2_encode_bad_decimal_string_rejected` — `"abc"` → `Err(BadDecimalString)`.
- `t2_encode_out_of_range_rejected` — `"1e19"` ≥10^19 → `Err(OutOfRange)`.

- `t2_round_trip_decode_encode_identity` — for every canonical example
  above: `encode(decode(bytes)) == bytes`.
- `t2_round_trip_encode_decode_identity` — for every canonical example
  above: `decode(encode(str)) == str`.

Substitute-side integration KAT (one):

- `t3_decode_binary_param_numeric_routes_through_codec` — bytes of `42`
  + `PG_TYPE_NUMERIC` → `Ok("42")`.

Binary-results-side integration KAT (one):

- `t3_encode_binary_value_numeric_routes_through_codec` — `"42"` +
  `PG_TYPE_NUMERIC` → `Ok(wire bytes)`.

Plus parity updates to `binary_format_supported_for_oid` +
`binary_result_supported_for_oid` matchers (existing tests assert
NUMERIC is *not* supported — those flip to assert it IS).

## 4. Acceptance criteria

V1 (T1..T5) ships when:

1. **psycopg2 round-trip with `decimal.Decimal`** succeeds on vulcan
   (the headline smoke).
2. **No regression on existing binary-format KATs** — every BIN +
   BIN-RESULTS V1 KAT continues to pass byte-for-byte.
3. **NUMERIC out-of-range / NaN** reject with messages naming the
   V2 follow-up arcs (`SP-PG-EXTQ-BIN-NUMERIC-BIGNUM` /
   `SP-PG-EXTQ-BIN-NUMERIC-NAN`).
4. **COPY-BIN NUMERIC rejection unchanged** — `SP-PG-COPY-BIN-NUMERIC`
   message still appears for COPY binary into NUMERIC columns; the
   COPY arc is independently enablable.
5. **seed-7 GREEN**, default tree-grep EMPTY, CI green at every
   commit on this arc.

## 5. Task decomposition (T1..T5)

| T# | Scope | KAT delta |
|---|---|---|
| **T1** | This design spec. | +0 |
| **T2** | `extq/binary_numeric.rs` module — `decode_numeric_binary` + `encode_numeric_binary` + `BinaryNumericError` + KAT corpus (~15-20 KATs). No dispatcher / substitute / binary_results changes yet. | +15-20 |
| **T3** | Wire the codec into `extq/substitute.rs::decode_binary_param` (NUMERIC arm) + `extq/binary_results.rs::encode_binary_value` (NUMERIC arm). Flip the supported-OID predicates to include NUMERIC. Update existing KATs that asserted NUMERIC reject → now assert NUMERIC accept. | +2-4 |
| **T4** | Real psycopg2 + asyncpg + pgJDBC smoke on vulcan; USAGE.md §9 row updates. Smoke script + transcript checked in. | +0-2 |
| **T5** | STATUS.md row + progress tracker → CLOSED + V2 follow-up names. TaskList #367 ready. | +0 |

Estimated V1 total: **~17-26 KATs across 5 slices**.

## 6. References

- SP-PG-EXTQ V1 spec: `docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
- SP-PG-EXTQ-BIN V1 spec: `docs/superpowers/specs/2026-06-01-kesseldb-sppgextq-bin-design.md`
- SP-PG-EXTQ-BIN-RESULTS V1 spec: `docs/superpowers/specs/2026-06-01-kesseldb-sppgextq-bin-results-design.md`
- SP-PG-EXTQ-BIN V1 progress (CLOSED): `docs/superpowers/specs/2026-06-01-kesseldb-subproject-sppgextqbin-progress.md`
- SP-PG-EXTQ-BIN-RESULTS V1 progress (CLOSED): `docs/superpowers/specs/2026-06-01-kesseldb-subproject-sppgextqbinr-progress.md`
- PostgreSQL Documentation §55.8 — Binary representations
- PostgreSQL source `src/backend/utils/adt/numeric.c::numeric_recv` /
  `numeric_send` — the canonical encoder/decoder this V1 mirrors
- `crates/kessel-pg-gateway/src/extq/substitute.rs::decode_binary_param`
  — V1 binary-param decoder this arc extends
- `crates/kessel-pg-gateway/src/extq/binary_results.rs::encode_binary_value`
  — V1 binary-result encoder this arc extends
