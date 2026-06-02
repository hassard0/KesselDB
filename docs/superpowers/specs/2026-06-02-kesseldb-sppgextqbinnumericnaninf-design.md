# SP-PG-EXTQ-BIN-NUMERIC-NAN-INF — PG NUMERIC special-value (NaN / ±Infinity) binary support — DESIGN

**Status:** design — closes the V2 follow-ups named in
`docs/superpowers/specs/2026-06-02-kesseldb-sppgextqbinnumeric-design.md`
§2.2 (`SP-PG-EXTQ-BIN-NUMERIC-NAN` + `SP-PG-EXTQ-BIN-NUMERIC-INF`) as a
single combined arc. Mirrored progress tracker:
`2026-06-02-kesseldb-subproject-sppgextqbinnumericnaninf-progress.md`.

SP-PG-EXTQ-BIN-NUMERIC V1 (closed 2026-06-02 at T4) covers the *finite*
PG NUMERIC binary wire shape (`|value| < 10^18` with ≤18 fractional
digits) at both the PARAM (Bind) and RESULT (DataRow) sides. PG NUMERIC
*also* carries 3 special values via reserved sign-field codes:

| Special    | Sign (BE u16) | Canonical PG string |
|---|---|---|
| NaN        | `0xC000`      | `"NaN"`             |
| +Infinity  | `0xD000`      | `"Infinity"`        |
| -Infinity  | `0xF000`      | `"-Infinity"`       |

Per PG `src/backend/utils/adt/numeric.c::numeric_send`, all three
specials carry `ndigits=0, weight=0, dscale=0` — the wire payload is
just the 8-byte header with the special sign code and an empty digit
array. The PG 14+ release notes (2021-09-30) introduced the
`+Infinity` / `-Infinity` codes; `NaN` has been the canonical reserved
sign code since PG 7.4.

This arc lifts the V1 specials rejection at BOTH the decoder (codec
reads the sign field and emits the canonical PG string representation)
AND the encoder (codec accepts the canonical PG string, case-insensitive,
and emits the canonical 8-byte all-zero-data wire frame).

**Builds on:**

- **SP-PG-EXTQ-BIN-NUMERIC V1** (`docs/superpowers/specs/2026-06-02-kesseldb-sppgextqbinnumeric-design.md`)
  — the `decode_numeric_binary` / `encode_numeric_binary` codec in
  `crates/kessel-pg-gateway/src/extq/binary_numeric.rs`. V1 rejects
  `sign=0xC000` with `BinaryNumericError::NaN` and `sign=0xD000` /
  `0xF000` with `BinaryNumericError::BadSign`. This arc replaces those
  rejections with the special-string mapping.
- **`extq/substitute.rs::decode_numeric`** — the dispatcher boundary
  that maps `BinaryNumericError` to `BinaryDecodeError`. V1's NaN arm
  emits `BinaryDecodeError::Unsupported { arc: "SP-PG-EXTQ-BIN-NUMERIC-NAN" }`;
  this arc replaces the arm with a `BinaryDecodeError::Ok` shape
  (special string flows through the existing `PreparedParam::Text`
  rendering, same as every other decoded numeric literal).
- **`extq/binary_results.rs::encode_numeric`** — the inverse boundary
  for the Execute RESULT side.

---

## 1. Context — PG NUMERIC special-value encoding

### 1.1 Wire layout (unchanged from V1)

```
[ndigits:  i16 BE = 0]
[weight:   i16 BE = 0]
[sign:     u16 BE]      // 0xC000 = NaN, 0xD000 = +Inf, 0xF000 = -Inf
[dscale:   i16 BE = 0]
```

Total wire size = 8 bytes (header only; digit array empty).

### 1.2 Canonical PG string forms

Per `numeric_out` in `src/backend/utils/adt/numeric.c`:

- NaN       → `"NaN"`        (mixed-case canonical)
- +Infinity → `"Infinity"`
- -Infinity → `"-Infinity"`

PG's `numeric_in` accepts case-insensitive variants on input (`"nan"`,
`"NAN"`, `"infinity"`, `"INFINITY"`, `"inf"`, `"-inf"`, etc.) and
normalizes to the canonical mixed-case form. This arc's encoder also
accepts case-insensitive variants for symmetry; the decoder always
emits the canonical mixed-case form.

### 1.3 Sign-code constants (already defined in V1)

```rust
pub(crate) const NUMERIC_POS: u16 = 0x0000;
pub(crate) const NUMERIC_NEG: u16 = 0x4000;
pub(crate) const NUMERIC_NAN: u16 = 0xC000;
// New in this arc:
pub(crate) const NUMERIC_PINF: u16 = 0xD000;
pub(crate) const NUMERIC_NINF: u16 = 0xF000;
```

## 2. Scope

### 2.1 In — what this arc ships

1. **Decoder**: `decode_numeric_binary` recognizes `sign=0xC000` /
   `0xD000` / `0xF000` and returns `"NaN"` / `"Infinity"` / `"-Infinity"`
   respectively. The V1 `BinaryNumericError::NaN` reject is removed; the
   V1 `BinaryNumericError::BadSign` reject narrows to *unknown* sign
   codes (anything other than POS / NEG / NAN / PINF / NINF).
2. **Encoder**: `encode_numeric_binary` recognizes case-insensitive
   `"NaN"` / `"Infinity"` / `"+Infinity"` / `"-Infinity"` / `"inf"` /
   `"+inf"` / `"-inf"` inputs and emits the canonical 8-byte wire frame
   with the matching sign code and `ndigits=weight=dscale=0`.
3. **Dispatcher**: the `extq/substitute.rs::decode_numeric` boundary
   no longer maps `BinaryNumericError::NaN` to `Unsupported` (the
   codec never emits that variant after this arc). The variant itself
   is preserved for source compatibility — V1 callers that pattern
   match on it still compile — but the codec no longer constructs it.
4. **KAT delta target +6-12**: per-encoding decode + encode + reject +
   round-trip KATs across the 3 specials.

### 2.2 Out — what's deferred

- **Engine-side NaN/Inf storage**. KesselDB's NUMERIC engine type
  (`FieldKind::I128`) cannot represent NaN/Inf. The codec-level support
  this arc adds makes the *wire* round-trip work for clients that send
  the special as a Bind parameter that flows into a context where the
  parser accepts the string form (e.g. an explicit CAST to TEXT or a
  comparison). Direct INSERT into an `I128` column still rejects at
  the SQL parse boundary; that's an engine arc (out of scope here).
- **Round-trip through engine storage**. A separate engine-level
  follow-up arc could add a `FieldKind::Numeric` variant that
  represents the special values natively; this arc deliberately
  doesn't touch the engine.

## 3. Implementation sketch

### 3.1 `extq/binary_numeric.rs` — decoder change

```rust
// At top of decode_numeric_binary, after parsing the 8-byte header:
match sign {
    NUMERIC_POS | NUMERIC_NEG => { /* fall through to V1 finite path */ }
    NUMERIC_NAN => return Ok("NaN".to_string()),
    NUMERIC_PINF => return Ok("Infinity".to_string()),
    NUMERIC_NINF => return Ok("-Infinity".to_string()),
    _ => return Err(BinaryNumericError::BadSign { sign }),
}
```

V1's "ndigits MUST be 0 when sign is special" is enforced as a
defensive check before the early return — if the wire frame carries
`sign=0xC000` but `ndigits != 0`, that's a protocol violation; reject
with `BadSign` (the malformed shape isn't the canonical PG special
encoding).

### 3.2 `extq/binary_numeric.rs` — encoder change

```rust
// At top of encode_numeric_binary, before the lex-sign-digit path:
let trimmed = decimal_str.trim();
let lower = trimmed.to_ascii_lowercase();
match lower.as_str() {
    "nan" => return Ok(special_wire(NUMERIC_NAN)),
    "infinity" | "+infinity" | "inf" | "+inf" => return Ok(special_wire(NUMERIC_PINF)),
    "-infinity" | "-inf" => return Ok(special_wire(NUMERIC_NINF)),
    _ => { /* fall through to V1 finite path */ }
}

fn special_wire(sign: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(8);
    out.extend_from_slice(&0i16.to_be_bytes()); // ndigits = 0
    out.extend_from_slice(&0i16.to_be_bytes()); // weight = 0
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&0i16.to_be_bytes()); // dscale = 0
    out
}
```

### 3.3 `extq/substitute.rs::decode_numeric` — boundary cleanup

The `BinaryNumericError::NaN` arm in the error-mapping closure becomes
unreachable after this arc (the codec never returns it). The arm is
kept as a defensive `unreachable!()` fallback so a future codec
regression that re-introduces the variant compiles cleanly. The
mapping for the other variants stays unchanged.

### 3.4 KAT corpus

Per-codec unit KATs in `binary_numeric.rs`:

- `t2sp_decode_nan_returns_nan_string` — header with `sign=0xC000`
  decodes to `"NaN"`.
- `t2sp_decode_pos_infinity_returns_infinity_string` — `sign=0xD000`
  decodes to `"Infinity"`.
- `t2sp_decode_neg_infinity_returns_minus_infinity_string` —
  `sign=0xF000` decodes to `"-Infinity"`.
- `t2sp_encode_nan_returns_canonical_bytes` — `"NaN"` encodes to the
  8-byte `0x00 0x00 0x00 0x00 0xC0 0x00 0x00 0x00`.
- `t2sp_encode_pos_infinity_returns_canonical_bytes` — `"Infinity"`.
- `t2sp_encode_neg_infinity_returns_canonical_bytes` — `"-Infinity"`.
- `t2sp_encode_special_case_insensitive` — `"nan"` / `"NAN"` /
  `"infinity"` / `"INF"` / `"+inf"` / `"-INF"` all encode to the
  canonical bytes for their respective signs.
- `t2sp_round_trip_specials` — decode(encode(s)) == s for `"NaN"` /
  `"Infinity"` / `"-Infinity"`.
- `t2sp_decode_unknown_sign_still_rejects` — `sign=0x1234` still
  rejects with `BadSign` (V1 invariant preserved).
- `t2sp_decode_nan_with_nonzero_ndigits_rejects` — malformed wire
  (sign=NaN but ndigits=1) rejects with `BadSign`.

V1 KAT update:

- `t2_decode_nan_rejected` — was: NaN → Err(NaN); now: NaN → Ok("NaN").
  Renamed to `t2_decode_nan_returns_nan_string` and merged with the
  new specials KATs (kept under the old name as an alias for blame
  continuity? No — single rename, the behavior changed).

Substitute-side integration KAT (one):

- `t3num_decode_numeric_nan_returns_nan_string_through_codec` — was:
  Err(Unsupported, arc=NAN); now: Ok("NaN").

Binary-results-side integration KAT (one):

- `t3num_encode_numeric_nan_through_codec` — `"NaN"` encodes to the
  canonical wire frame.

## 4. Acceptance criteria

V1 (T1..T4) ships when:

1. **Decoder accepts all 3 special sign codes** and returns the
   canonical PG string (`"NaN"` / `"Infinity"` / `"-Infinity"`).
2. **Encoder accepts the 3 canonical strings** (plus case-insensitive
   variants) and emits the canonical 8-byte wire frame.
3. **No regression on existing binary-format KATs** — every BIN +
   BIN-RESULTS V1 KAT continues to pass byte-for-byte.
4. **Unknown sign codes still reject** with `BinaryNumericError::BadSign`.
5. **psycopg2 + asyncpg smoke on vulcan** — Decimal NaN/Infinity
   either round-trip end-to-end OR reject with a precise engine-level
   error (the codec layer is no longer the failure point).
6. **CI green at every commit** on this arc.

## 5. Task decomposition (T1..T4)

| T# | Scope | KAT delta |
|---|---|---|
| **T1+T2** | This design spec + codec change (NUMERIC_PINF/NINF constants + sign dispatch in decode + special-string preamble in encode) + per-codec KATs (~8-10). | +8-10 |
| **T3** | Real psycopg2 + asyncpg smoke on vulcan + USAGE.md §9 update. Smoke script + transcript checked in. | +0-2 |
| **T4** | STATUS.md row + progress tracker → CLOSED + TaskList #380 ready. | +0 |

Estimated total: **~8-12 KATs across 2-4 commits**.

## 6. References

- SP-PG-EXTQ-BIN-NUMERIC V1 spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqbinnumeric-design.md`
- SP-PG-EXTQ-BIN-NUMERIC V1 progress (CLOSED): `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgextqbinnumeric-progress.md`
- PostgreSQL source `src/backend/utils/adt/numeric.c::numeric_send` /
  `numeric_recv` / `numeric_out` / `numeric_in` — the canonical
  encoder/decoder this arc mirrors for the 3 specials
- PostgreSQL 14 release notes (2021-09-30) — `+Infinity` / `-Infinity`
  introduced
- `crates/kessel-pg-gateway/src/extq/binary_numeric.rs` — the i128
  codec this arc extends
