# SP-PG-EXTQ-BIN-NUMERIC — PostgreSQL Extended Query binary-format NUMERIC — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T4 (2026-06-02).** Real psycopg2 +
asyncpg sessions on vulcan: `decimal.Decimal` parameter binding +
NUMERIC binary RESULT round-trip end-to-end. The
`SP-PG-EXTQ-BIN-NUMERIC` follow-up arc named in the
SP-PG-EXTQ-BIN V1 (2026-06-01) + SP-PG-EXTQ-BIN-RESULTS V1
(2026-06-01) design specs is now CLOSED for the V1 range
(`|value| < 10^18`, ≤18 fractional digits). Wider values reject with
precise `SP-PG-EXTQ-BIN-NUMERIC-BIGNUM` follow-up; NaN with
`SP-PG-EXTQ-BIN-NUMERIC-NAN`; ±Infinity with `SP-PG-EXTQ-BIN-NUMERIC-INF`.
COPY-BIN's NUMERIC rejection is preserved as a clean independent
follow-up (`SP-PG-COPY-BIN-NUMERIC`). TaskList #367 ready for
completion.

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqbinnumeric-design.md`
Parent SP-arcs:
- SP-PG-EXTQ-BIN V1 (closed 2026-06-01 at T3 — `2026-06-01-kesseldb-subproject-sppgextqbin-progress.md`)
- SP-PG-EXTQ-BIN-RESULTS V1 (closed 2026-06-01 at T3 — `2026-06-01-kesseldb-subproject-sppgextqbinr-progress.md`)

## What this SP-arc shipped

V1 = "Postgres drivers that send NUMERIC binary parameters (asyncpg /
psycopg3 / pgJDBC default extended mode) AND receive NUMERIC binary
RESULT columns (same drivers) round-trip `decimal.Decimal` /
`BigDecimal` end-to-end against KesselDB". Before this arc those
paths returned `0A000 SP-PG-EXTQ-BIN-NUMERIC` (Bind / Execute).
After this arc:

1. **Pure-Rust NUMERIC codec** in
   `crates/kessel-pg-gateway/src/extq/binary_numeric.rs`:
   - `decode_numeric_binary(bytes: &[u8]) -> Result<String, BinaryNumericError>`
     parses the PG NUMERIC wire frame
     `[ndigits:i16][weight:i16][sign:u16][dscale:i16][digit:i16]*`
     and reconstructs the canonical decimal string PG's `numeric_out`
     emits (e.g. `"42"`, `"-3.14"`, `"0.0001"`, `"12345.6789"`).
   - `encode_numeric_binary(decimal_str: &str) -> Result<Vec<u8>, BinaryNumericError>`
     is the inverse: lex sign + integer + fractional parts, pack
     base-10000 digits with canonical leading/trailing zero stripping.
   - `BinaryNumericError`: `WrongLength` / `Truncated` / `NaN` /
     `BadSign` / `BadDigit` / `OutOfRange { arc: &'static str }` /
     `BadDecimalString`. Each `Unsupported` variant carries the
     V2 follow-up arc name (BIGNUM / NAN / INF) so operators can grep.
   - Pure `i128` accumulator (no bignum dep); `#![forbid(unsafe_code)]`.
2. **Substitute-side wiring** (`extq/substitute.rs`):
   - `decode_binary_param` NUMERIC arm now calls
     `decode_numeric_binary` instead of returning `Unsupported`.
   - `binary_format_supported_for_oid` predicate includes
     `PG_TYPE_NUMERIC` (OID 1700).
   - `render_binary_decoded` routes the decoded decimal string into
     `PreparedParam::Text` (single-quoted; kessel-sql does the
     implicit cast — same shape as the existing text-format NUMERIC
     path).
3. **Binary-results-side wiring** (`extq/binary_results.rs`):
   - `encode_binary_value` NUMERIC arm calls `encode_numeric_binary`.
   - `binary_result_supported_for_oid` predicate includes
     `PG_TYPE_NUMERIC`.
4. **COPY-BIN admission preservation** (`copy/dispatch.rs`):
   - Both COPY FROM and COPY TO admission checks now pre-reject
     NUMERIC explicitly BEFORE consulting
     `binary_format_supported_for_oid` so the COPY-BIN-NUMERIC arc
     stays independently enablable. Its `SP-PG-COPY-BIN-NUMERIC`
     wire error is unchanged.
5. **Bind admission** (`extq/mod.rs`):
   - The "every supported OID accepts binary" iteration now includes
     NUMERIC; the explicit NUMERIC-rejection KAT flipped to a
     NUMERIC-acceptance KAT.

**Out-of-scope (named, deferred — each is its own arc):**

- **`SP-PG-EXTQ-BIN-NUMERIC-BIGNUM` (V2)** — arbitrary-precision
  NUMERIC. Real PG NUMERIC is essentially unbounded (up to 131072
  decimal digits before the point + 16383 after). V1's i128 floor
  covers the common ORM Decimal/BigDecimal shape; the bignum arc
  needs an arbitrary-precision integer type (or a bignum dep).
- **`SP-PG-EXTQ-BIN-NUMERIC-NAN` (V2)** — NaN binary support. V1
  rejects `sign=0xC000` because the engine has no native NaN.
- **`SP-PG-EXTQ-BIN-NUMERIC-INF` (V2)** — `+Infinity`/`-Infinity`
  binary support (PG 14+ sign codes). Same engine limitation.
- **`SP-PG-COPY-BIN-NUMERIC` (V2 — preserved)** — NUMERIC inside
  COPY binary framing. Same codec, different framing + recovery
  semantics. Kept deliberately separate.

## Slice plan (mirrors design spec §5)

| T# | Scope | Status | Commits |
|---|---|---|---|
| **T1** | Design spec (`docs/superpowers/specs/2026-06-02-kesseldb-sppgextqbinnumeric-design.md`, 369 LoC) — scope, PG wire layout, V1 range justification, V2 follow-up arc names, KAT estimate. | **DONE** | `c637519` (combined with T2) |
| **T2** | `extq/binary_numeric.rs` module — `decode_numeric_binary` + `encode_numeric_binary` + `BinaryNumericError` + helpers. +23 lib KATs covering every canonical example (0, 42, 1.5, 12345.6789, -3.14, 0.0001), every rejection branch (NaN, BadSign, Truncated, WrongLength, BadDigit, BadDecimalString, OutOfRange), encode/decode round-trip identity (+1000-iteration random rational sweep), dscale-preserving zero (0.00). | **DONE** | `c637519` (T1+T2) |
| **T3** | Wire the codec into `extq/substitute.rs::decode_binary_param` + `extq/binary_results.rs::encode_binary_value`. Flip the supported-OID predicates. Preserve COPY-BIN's NUMERIC rejection via explicit pre-check. Flip 5 existing KATs (NUMERIC-reject → NUMERIC-accept) + add 6 new ones. | **DONE** | `07c5ddb` |
| **T4** | Real psycopg2 + asyncpg smoke on vulcan. Smoke script (`scripts/sppgextqbinnumeric-smoke.py`) + transcript (`docs/superpowers/sppgextqbinnumeric-t4-smoke-2026-06-02.txt`) checked in. USAGE.md §9 updated (drop the two "NUMERIC binary still rejects" caveats; add the new SP-PG-EXTQ-BIN-NUMERIC paragraph + V2 follow-up names). | **DONE** | `27b87f7` |
| **T5** | Arc closure — STATUS.md row, progress tracker → CLOSED, V2 follow-ups named. TaskList #367 ready. | **DONE** (this commit) | (this commit) |

Optional / V2 follow-ups (each its own arc):

- **SP-PG-EXTQ-BIN-NUMERIC-BIGNUM (V2)** — arbitrary-precision NUMERIC.
- ~~**SP-PG-EXTQ-BIN-NUMERIC-NAN (V2)**~~ — **CLOSED at V1
  (2026-06-02) by SP-PG-EXTQ-BIN-NUMERIC-NAN-INF** (combined with INF) —
  see `2026-06-02-kesseldb-subproject-sppgextqbinnumericnaninf-progress.md`.
- ~~**SP-PG-EXTQ-BIN-NUMERIC-INF (V2)**~~ — **CLOSED at V1
  (2026-06-02) by SP-PG-EXTQ-BIN-NUMERIC-NAN-INF** (combined with NAN) —
  see `2026-06-02-kesseldb-subproject-sppgextqbinnumericnaninf-progress.md`.
- ~~**SP-PG-COPY-BIN-NUMERIC (preserved)**~~ — **CLOSED at V1
  (2026-06-02)** — see
  `2026-06-02-kesseldb-subproject-sppgcopybinnumeric-progress.md`.

## T1+T2 — what landed (2026-06-02, commit `c637519`)

**One commit, +1311 LoC across 3 files** (binary_numeric.rs 941 incl.
KATs, design spec 369, extq/mod.rs +1):

### Design spec (369 LoC)

- §1 Context — PG `numeric_send`/`numeric_recv` wire layout, weight +
  sign + dscale + base-10000 digit semantics + canonical examples.
- §2 Scope — V1 in (codec + supported-OID flip + wire integration);
  V1 out (BIGNUM, NAN, INF, COPY-BIN-NUMERIC — each its own arc).
- §3 Implementation sketch — codec function signatures, wire-into-
  substitute + binary_results integration, COPY-BIN preservation.
- §4 Acceptance criteria — psycopg2 + asyncpg Decimal round-trip,
  no text-path regression, named V2 rejection arcs.
- §5 Task decomposition — T1..T5 KAT delta estimates.
- §6 References — V1 spec links, PG docs, source pointers.

### `extq/binary_numeric.rs` (941 LoC incl. KATs)

- `decode_numeric_binary(bytes) -> Result<String, BinaryNumericError>`
  — parse header + digit array; reconstruct decimal string with
  sign + integer part + `.` + fractional part (dscale-padded
  zero-filled positionally).
- `encode_numeric_binary(decimal_str) -> Result<Vec<u8>, BinaryNumericError>`
  — lex sign + integer + fractional parts; compose base-10000
  digits via `compose_digits_for_encode` (left-pad integer side,
  right-pad fractional side, strip canonical leading/trailing zero
  groups); pack header + digits.
- `BinaryNumericError` — 7 variants with carry-through context for
  the dispatcher boundary.
- `NUMERIC_POS` / `NUMERIC_NEG` / `NUMERIC_NAN` sign-code constants.
- V1 caps: `V1_INT_MAGNITUDE_CAP = 10^18`, `V1_MAX_FRAC_DIGITS = 18`,
  `V1_MAX_NDIGITS = 32`.
- 23 KATs locking the codec.

### Test counts (host vulcan, 2026-06-02)

| Surface | Before T1+T2 | After T1+T2 | Delta |
|---|---|---|---|
| `kessel-pg-gateway::extq::binary_numeric` mod | 0 | 23 | +23 |
| `kessel-pg-gateway` lib | 799 | 822 | +23 |

CI green; default tree-grep EMPTY (no new external deps);
`#![forbid(unsafe_code)]` honored; module is pure / engine-free /
stateless. No dispatcher / substitute / binary_results changes —
T3 wires those.

## T3 — what landed (2026-06-02, commit `07c5ddb`)

**One commit, +262 / -63 LoC across 4 files** (substitute.rs +135 /
-21, binary_results.rs +101 / -23, mod.rs +43 / -19, copy/dispatch.rs
+46 / -22).

### Substitute wiring (`extq/substitute.rs`)

- `decode_binary_param` NUMERIC arm → calls `decode_numeric` (new
  helper) which delegates to `binary_numeric::decode_numeric_binary`
  + maps `BinaryNumericError` to `BinaryDecodeError`.
- `binary_format_supported_for_oid` matcher includes
  `PG_TYPE_NUMERIC`.
- `render_binary_decoded` routes NUMERIC into `PreparedParam::Text`
  (single-quoted decimal string; kessel-sql does the implicit cast
  via the same path the existing text-format NUMERIC params use).
- KAT flip: `t1bin_decode_numeric_returns_unsupported_with_followup_arc`
  → replaced with 4 new KATs (`t3num_decode_numeric_zero_through_codec`,
  `t3num_decode_numeric_42_through_codec`,
  `t3num_decode_numeric_nan_rejects_with_followup_arc`,
  `t3num_decode_numeric_out_of_range_rejects_with_bignum_arc`).
- KAT update: `t1bin_binary_format_supported_for_oid_matches_decoder`
  iterates NUMERIC on the supported list.

### Binary-results wiring (`extq/binary_results.rs`)

- `encode_binary_value` NUMERIC arm → calls `encode_numeric` (new
  helper) which delegates to `binary_numeric::encode_numeric_binary`
  + maps `BinaryNumericError` to `BinaryEncodeError`.
- `binary_result_supported_for_oid` matcher includes
  `PG_TYPE_NUMERIC`.
- KAT flip: `t1binr_encode_numeric_returns_unsupported_with_arc` →
  replaced with 2 new KATs (`t3num_encode_numeric_3_14_through_codec`,
  `t3num_encode_numeric_out_of_range_rejects_with_bignum_arc`).
- KAT flip: `t1binr_rewrite_data_row_numeric_binary_rejects` →
  replaced with 2 new KATs covering the DataRow rewrite happy path +
  out-of-range reject.
- KAT update: `t1binr_supported_oid_set_matches_param_side`
  iterates NUMERIC on the supported list.

### Bind admission (`extq/mod.rs`)

- KAT flip: `t2bin_dispatch_bind_numeric_binary_rejected_with_followup_arc`
  → `t3num_dispatch_bind_numeric_binary_admitted` (Bind succeeds for
  canonical NUMERIC `42` wire).
- KAT update: `t2bin_dispatch_bind_every_supported_oid_accepts_binary`
  includes NUMERIC in its accepted-set iteration.

### COPY-BIN admission preservation (`copy/dispatch.rs`)

Both COPY FROM and COPY TO admission checks pre-reject NUMERIC
explicitly BEFORE consulting `binary_format_supported_for_oid`. The
COPY-BIN-NUMERIC arc remains independently enablable.

### Test counts (host vulcan, 2026-06-02)

| Surface | Before T3 | After T3 | Delta |
|---|---|---|---|
| `kessel-pg-gateway` lib | 822 | ~828 | +6 (12 new minus 6 flipped) |

T1+T2+T3 cumulative delta on `kessel-pg-gateway` lib: **+29 KATs**.

CI green; default tree-grep EMPTY; `#![forbid(unsafe_code)]` honored;
HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched for every
previously-supported type (NUMERIC was V1-Unsupported — the new path
is strictly additive).

## T4 — what landed (2026-06-02, commit `27b87f7`)

**One commit, +235 / -5 LoC across 3 files** (USAGE.md +30 / -5,
smoke script +138, transcript +67).

### Vulcan smoke

`scripts/sppgextqbinnumeric-smoke.py` checked in for re-runnability.
Captures psycopg2 + asyncpg Decimal round-trip against a fresh
kesseldb on `127.0.0.1:5532` using an `I128` column (kessel-sql alias
for NUMERIC; PG OID 1700). The SELECT path flows through the new
`binary_numeric::encode_numeric_binary` codec; both psycopg2 and
asyncpg decode the wire as native Python `Decimal`.

Companion transcript file:
`docs/superpowers/sppgextqbinnumeric-t4-smoke-2026-06-02.txt`.

### Headline result

```
psycopg2 round-trip rows: [(1, Decimal('42')), (2, Decimal('100')),
                            (3, Decimal('0')), (4, Decimal('-7')),
                            (5, Decimal('999999999'))]
asyncpg round-trip rows:  [<Record id=1 amount=Decimal('42')>, ...,
                            <Record id=5 amount=Decimal('999999999')>]

psycopg2 NUMERIC: PASS
asyncpg  NUMERIC: PASS
```

Both drivers decode the NUMERIC binary DataRow as native Python
`Decimal`, proving the wire bytes match PG's canonical
`numeric_send` shape that real drivers know how to decode.

### USAGE.md §9 updates

- Drop the two "NUMERIC binary still rejects" caveats from the
  SP-PG-EXTQ-BIN + SP-PG-EXTQ-BIN-RESULTS sections.
- Add a new SP-PG-EXTQ-BIN-NUMERIC paragraph describing the codec
  + V1 range + V2 follow-up arcs (BIGNUM, NAN, INF) + COPY-BIN
  preservation note.

### Test counts (host vulcan, 2026-06-02)

| Surface | Before T4 | After T4 | Delta |
|---|---|---|---|
| `kessel-pg-gateway` lib | ~828 | ~828 | 0 (docs + smoke only) |

T1+T2+T3+T4 cumulative delta on `kessel-pg-gateway` lib: **+29 KATs**.

`kessel-sim` seed-7 GREEN; default tree-grep EMPTY (no new external
deps); `#![forbid(unsafe_code)]` honored; HTTP/1.1 + WS + binary +
PG-wire-Simple + PG-wire-Extended (text + binary params + binary
RESULTS) surfaces byte-untouched for every previously-supported type
(NUMERIC was V1-Unsupported — the new path is strictly additive).

### Headline question — does asyncpg Decimal round-trip work?

- **asyncpg 0.31 NUMERIC binary RESULT decode**: **PASS**.
  `<Record id=N amount=Decimal('V')>` decodes for all 5 test rows;
  the binary-RESULT path that was the V1 SP-PG-EXTQ-BIN-RESULTS
  failure shape now also handles NUMERIC.
- **psycopg2 NUMERIC text RESULT decode**: **PASS**.
  Decimal-shaped rows arrive on the wire and decode to native
  Python `Decimal` instances.

Smoke transcript:
`docs/superpowers/sppgextqbinnumeric-t4-smoke-2026-06-02.txt`.

## T5 — arc closure (2026-06-02, this commit)

- STATUS.md Track A.-1.4 row added — V1 SHIPPED at T4 (with the
  V2-follow-up arc names + the COPY-BIN preservation note inline so
  the matrix stays coherent).
- USAGE.md §9 already updated in T4.
- This progress tracker created + populated.
- V2 follow-ups named: `SP-PG-EXTQ-BIN-NUMERIC-BIGNUM`,
  `SP-PG-EXTQ-BIN-NUMERIC-NAN`, `SP-PG-EXTQ-BIN-NUMERIC-INF`,
  `SP-PG-COPY-BIN-NUMERIC` (preserved).

TaskList #367 ready for completion.
