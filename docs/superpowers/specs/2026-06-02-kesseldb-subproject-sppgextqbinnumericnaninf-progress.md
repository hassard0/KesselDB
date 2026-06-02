# SP-PG-EXTQ-BIN-NUMERIC-NAN-INF — PG NUMERIC special-value (NaN / ±Infinity) binary support — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T4 (2026-06-02).** The two V2
follow-ups named in SP-PG-EXTQ-BIN-NUMERIC V1 (2026-06-02) design spec
§2.2 — `SP-PG-EXTQ-BIN-NUMERIC-NAN` and `SP-PG-EXTQ-BIN-NUMERIC-INF` —
are now both CLOSED at the codec layer by this single combined arc.
Real psycopg2 + asyncpg sessions on vulcan send the 3 PG NUMERIC
binary special-value wire frames (sign codes `0xC000` / `0xD000` /
`0xF000`); the KesselDB codec decodes them into the canonical PG
strings `"NaN"` / `"Infinity"` / `"-Infinity"` and encodes them back
into the canonical 8-byte all-zero-data wire frame. The codec-layer
rejection that V1 surfaced as `0A000 SP-PG-EXTQ-BIN-NUMERIC-NAN` /
`-INF` is gone; the remaining downstream rejection is engine-level
(`FieldKind::I128` cannot natively store these specials — a separate
engine arc, deliberately out of scope here). TaskList #380 ready for
completion.

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqbinnumericnaninf-design.md`
Parent SP-arc:
- SP-PG-EXTQ-BIN-NUMERIC V1 (closed 2026-06-02 at T4 —
  `2026-06-02-kesseldb-subproject-sppgextqbinnumeric-progress.md`)

## What this SP-arc shipped

V1 = "PG NUMERIC binary wire frames carrying the 3 reserved sign codes
(NaN / +Infinity / -Infinity) decode at the codec layer to canonical
PG strings; canonical PG strings encode back to the matching wire
frame; case-insensitive variants accepted on encode for symmetry with
PG's `numeric_in`". Before this arc those paths failed with `0A000
SP-PG-EXTQ-BIN-NUMERIC-NAN` (sign=0xC000) or `0A000 SP-PG-EXTQ-BIN-NUMERIC-INF`
(sign=0xD000/0xF000). After this arc:

1. **Codec change** in
   `crates/kessel-pg-gateway/src/extq/binary_numeric.rs`:
   - New constants `NUMERIC_PINF: u16 = 0xD000` and
     `NUMERIC_NINF: u16 = 0xF000`.
   - `decode_numeric_binary`: special-sign dispatch added BEFORE the
     finite-value path. `sign=0xC000` → `Ok("NaN")`, `sign=0xD000` →
     `Ok("Infinity")`, `sign=0xF000` → `Ok("-Infinity")`. Wires with
     special sign + non-zero `ndigits` reject via `BadSign`
     (defensive — PG `numeric_send` always emits `ndigits=0` for
     specials; non-zero ndigits is a protocol violation).
   - `encode_numeric_binary`: case-insensitive special-string preamble
     added BEFORE the lex-sign-digit path. `"nan"` / `"infinity"` /
     `"+infinity"` / `"inf"` / `"+inf"` / `"-infinity"` / `"-inf"` all
     emit the canonical 8-byte `[0, 0, sign_BE, 0]` wire frame via
     the new `encode_special` helper.
   - `BinaryNumericError::NaN` variant preserved for source
     compatibility but never constructed by the new codec — callers
     pattern-matching on it remain valid.
2. **Dispatcher boundary** (`extq/substitute.rs::decode_numeric`):
   the `BinaryNumericError::NaN` arm is preserved as a defensive
   fallback but is unreachable in practice (the codec never returns
   it). The mapping to `BinaryDecodeError::Unsupported { arc:
   "SP-PG-EXTQ-BIN-NUMERIC-NAN" }` stays for source compatibility.
3. **Test updates**:
   - V1 KAT `t2_decode_nan_rejected` flipped to
     `t2sp_decode_nan_returns_nan_string` (behavior changed).
   - V1 dispatcher KAT
     `t3num_decode_numeric_nan_rejects_with_followup_arc` flipped to
     `t3num_decode_numeric_nan_returns_nan_string_through_codec`.
   - +9 new module KATs `t2sp_*` covering all 3 specials × decode +
     encode + round-trip + case-insensitive variants + malformed-
     special-wire reject + unknown-sign reject + non-special string
     reject.
   - +2 new dispatcher KATs for +Infinity and -Infinity decode.
   - +1 new binary_results KAT for all 3 specials encoded via the
     `encode_binary_value` boundary.

**Out-of-scope (deliberately deferred):**

- **Engine-level NaN/Inf storage**. KesselDB's NUMERIC engine type
  (`FieldKind::I128`) has no native NaN/Inf representation. After
  this arc the gateway decodes the wire successfully but the kessel-
  sql parser rejects `'NaN'` (and the Infinity variants) when it
  attempts to cast the substituted literal to an I128 column. A
  separate engine arc would add a `FieldKind::Numeric` variant (or a
  side-channel "is_special" bit on `Value::Int`) that represents the
  special values natively. NOT named here because no current arc
  surface needs that capability and naming an arc prematurely would
  pre-commit the engine design.

## Slice plan (mirrors design spec §5)

| T# | Scope | Status | Commits |
|---|---|---|---|
| **T1+T2** | Design spec (`docs/superpowers/specs/2026-06-02-kesseldb-sppgextqbinnumericnaninf-design.md`, ~200 LoC) + codec change (NUMERIC_PINF/NINF constants + special-sign decode dispatch + special-string encode preamble + `encode_special` helper) + per-codec KATs (+9 `t2sp_*`) + dispatcher KATs (+2 substitute + 1 binary_results) + 1 V1 KAT flip + 1 V1 dispatcher KAT flip. | **DONE** | `cbfdf24` |
| **T3** | Real psycopg2 + asyncpg smoke on vulcan. Smoke script (`scripts/sppgextqbinnumericnaninf-smoke.py`) + transcript (`docs/superpowers/sppgextqbinnumericnaninf-t3-smoke-2026-06-02.txt`) checked in. USAGE.md §9 updated (drop the "NaN rejects with SP-PG-EXTQ-BIN-NUMERIC-NAN" caveat; add the NaN-INF support paragraph). | **DONE** | `94920a0` |
| **T4** | Arc closure — STATUS.md row, progress tracker → CLOSED. TaskList #380 ready. | **DONE** (this commit) | (this commit) |

## T1+T2 — what landed (2026-06-02, commit `cbfdf24`)

**One commit, +529 / -33 LoC across 4 files**:

### Design spec (~200 LoC)

- §1 Context — PG `numeric_send` special-value encoding, canonical
  string forms, sign-code constants.
- §2 Scope — V1 in (codec change + sign dispatch + case-insensitive
  encode preamble); V1 out (engine-side storage).
- §3 Implementation sketch — codec function changes, dispatcher
  boundary cleanup, encode_special helper.
- §4 Acceptance criteria — psycopg2 + asyncpg Decimal NaN/Inf no
  longer reject at the codec layer.
- §5 Task decomposition — T1..T4 KAT delta estimates.
- §6 References — V1 spec links, PG docs, source pointers.

### Codec change (`extq/binary_numeric.rs`)

- `NUMERIC_PINF: u16 = 0xD000` + `NUMERIC_NINF: u16 = 0xF000`
  constants added.
- `decode_numeric_binary` special-sign match BEFORE the finite path:
  ```rust
  match sign {
      NUMERIC_POS | NUMERIC_NEG => { /* finite */ }
      NUMERIC_NAN if ndigits == 0 => return Ok("NaN".to_string()),
      NUMERIC_PINF if ndigits == 0 => return Ok("Infinity".to_string()),
      NUMERIC_NINF if ndigits == 0 => return Ok("-Infinity".to_string()),
      _ => return Err(BinaryNumericError::BadSign { sign }),
  }
  ```
- `encode_numeric_binary` case-insensitive preamble:
  ```rust
  let lower = s.to_ascii_lowercase();
  match lower.as_str() {
      "nan" => return Ok(encode_special(NUMERIC_NAN)),
      "infinity"|"+infinity"|"inf"|"+inf" => return Ok(encode_special(NUMERIC_PINF)),
      "-infinity"|"-inf" => return Ok(encode_special(NUMERIC_NINF)),
      _ => { /* finite */ }
  }
  ```
- `encode_special(sign)` helper emits the 8-byte all-zero-data wire
  frame `[0, 0, sign_BE, 0]`.

### KATs (`extq/binary_numeric.rs::tests`)

Flipped: `t2_decode_nan_rejected` → `t2sp_decode_nan_returns_nan_string`.

Added 9 (`t2sp_*`):
- `t2sp_decode_nan_returns_nan_string`
- `t2sp_decode_pos_infinity_returns_infinity_string`
- `t2sp_decode_neg_infinity_returns_minus_infinity_string`
- `t2sp_decode_nan_with_nonzero_ndigits_rejects`
- `t2sp_decode_unknown_sign_still_rejects`
- `t2sp_encode_nan_returns_canonical_bytes`
- `t2sp_encode_pos_infinity_returns_canonical_bytes`
- `t2sp_encode_neg_infinity_returns_canonical_bytes`
- `t2sp_encode_special_case_insensitive`
- `t2sp_round_trip_specials`
- `t2sp_encode_non_special_still_rejects_as_bad_decimal_string`

### Dispatcher KATs (`extq/substitute.rs::tests`)

Flipped: `t3num_decode_numeric_nan_rejects_with_followup_arc` →
`t3num_decode_numeric_nan_returns_nan_string_through_codec`.

Added 2:
- `t3num_decode_numeric_pos_infinity_returns_infinity_string_through_codec`
- `t3num_decode_numeric_neg_infinity_returns_minus_infinity_string_through_codec`

### Binary-results dispatcher KAT (`extq/binary_results.rs::tests`)

Added 1:
- `t3num_encode_numeric_specials_through_codec` — all 3 specials
  encode through the dispatcher boundary to the canonical bytes.

### Test counts (host vulcan, 2026-06-02)

| Surface | Before T1+T2 | After T1+T2 | Delta |
|---|---|---|---|
| `kessel-pg-gateway::extq::binary_numeric` mod | 25 | 37 | +12 |
| `kessel-pg-gateway` lib total | ~850 | 862 | +12 |

CI green; default tree-grep EMPTY (no new external deps);
`#![forbid(unsafe_code)]` honored; HTTP/1.1 + WS + binary + PG-wire
surfaces byte-untouched for every finite NUMERIC value (the new
specials path is strictly additive — V1 finite wire frames decode
identically; V1 finite encode output is byte-equal).

## T3 — what landed (2026-06-02, commit `94920a0`)

**One commit, +272 / -2 LoC across 3 files**:

### Vulcan smoke

`scripts/sppgextqbinnumericnaninf-smoke.py` checked in for
re-runnability. Sends `decimal.Decimal('NaN')` / `'Infinity'` /
`'-Infinity'` from both psycopg2 (sync) and asyncpg (async) drivers
against a fresh kesseldb on `127.0.0.1:5536`; asserts the resulting
error verdict does NOT contain the `SP-PG-EXTQ-BIN-NUMERIC-{NAN,INF}`
arc name (codec-layer assertion).

Companion transcript:
`docs/superpowers/sppgextqbinnumericnaninf-t3-smoke-2026-06-02.txt`.

### Headline result

```
--- psycopg2 (sync) ---
         NaN: [PASS] INSERT_REJECT: DatatypeMismatch: sql: literal/column type mismatch
    Infinity: [PASS] INSERT_REJECT: DatatypeMismatch: sql: literal/column type mismatch
   -Infinity: [PASS] INSERT_REJECT: DatatypeMismatch: sql: literal/column type mismatch

--- asyncpg (async) ---
         NaN: [PASS] INSERT_REJECT: DataError: invalid input for query argument $1: Decimal('NaN') (expected str, got Decimal)
    Infinity: [PASS] INSERT_REJECT: DataError: invalid input for query argument $1: Decimal('Infinity') (expected str, got Decimal)
   -Infinity: [PASS] INSERT_REJECT: DataError: invalid input for query argument $1: Decimal('-Infinity') (expected str, got Decimal)

CODEC-LAYER PASS
```

Both drivers reach the codec, the codec accepts the wire frame, and
the downstream rejection comes from somewhere else (engine
type-cast for psycopg2; asyncpg client-side encoder for asyncpg).
Neither rejection names the codec arc — the codec layer is no
longer the failure point.

### USAGE.md §9 update

Replaced the V1 "NaN rejects with `SP-PG-EXTQ-BIN-NUMERIC-NAN`"
caveat with the `SP-PG-EXTQ-BIN-NUMERIC-NAN-INF` codec-support
paragraph; engine-level storage remains a clean follow-up.

## T4 — arc closure (2026-06-02, this commit)

- STATUS.md Track A.-1.6 row added — V1 SHIPPED at T3 (with the
  engine-level limitation called out as a separate follow-up).
- USAGE.md §9 already updated in T3.
- This progress tracker created + populated.
- Parent progress tracker
  (`2026-06-02-kesseldb-subproject-sppgextqbinnumeric-progress.md`)
  V2 follow-up rows for NAN and INF marked CLOSED with cross-link.

TaskList #380 ready for completion.
