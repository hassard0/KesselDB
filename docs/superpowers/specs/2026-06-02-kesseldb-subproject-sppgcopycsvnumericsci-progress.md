# SP-PG-COPY-CSV-NUMERIC-SCI — scientific notation in text/CSV COPY NUMERIC — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: T1 SHIPPING — `parse_scientific_notation` helper + scientific
branch in `validate_numeric_text` + KATs (this commit).**

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgcopycsvnumericsci-design.md`

Parent SP-arc:
- SP-PG-COPY-CSV-NUMERIC (V1 closed 2026-06-02 — canonical
  decimals + NaN/±Infinity validator). V1 explicitly rejected
  scientific notation with a precise `ScientificNotation` variant
  naming this V2 arc. This arc lifts that rejection by parsing
  the exponent and expanding the value to canonical decimal form.

## What this SP-arc ships

V1 = "text + CSV COPY into a NUMERIC column accepts scientific
notation (`1e10`, `1.5E-3`, `6.022e+23`, `-3.14e2`) and expands the
exponent into the canonical PG decimal text representation BEFORE
the row reaches the engine. Out-of-range or malformed exponents
reject with a precise `22P02` naming the reason."

1. `parse_scientific_notation(s: &str) -> Result<Option<String>, CsvNumericError>`
   helper in `crates/kessel-pg-gateway/src/copy/csv.rs`.
2. Scientific branch in `validate_numeric_text` that runs the
   helper BEFORE the canonical-decimal grammar, so any input
   containing `e`/`E` is handled here. Inputs without `e`/`E` skip
   the helper at zero cost.
3. The expanded canonical decimal replaces the original field bytes
   on success (same behaviour as the V1 sign-normalisation /
   NaN-canonicalisation).

**Out-of-scope (named, deferred — each is its own arc):**

- **SP-PG-COPY-CSV-NUMERIC-SCI-TRAILDOT (V2)** — trailing-dot
  mantissa (`5.e2`). Real PG accepts; no ORM/spreadsheet emits.
- **SP-PG-COPY-NUMERIC-BIGNUM (V2)** — arbitrary-precision NUMERIC
  beyond the kessel-sql i128 cap. Validator accepts within
  `|exp|≤100`; engine-side cap still applies downstream.
- **SP-PG-COPY-NUMERIC-LOCALE (V2)** — comma-decimal mantissa
  (`1,5e3`).

## Slice plan (mirrors design spec §7)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + `parse_scientific_notation` + scientific branch + KATs. | **DONE** | (this commit) |
| **T2** | Real psql 16 vulcan smoke + USAGE §9 expansion. | pending | |
| **T3** | STATUS row + tracker → CLOSED + TaskList #385 ready. | pending | |

## T1 — what landed (2026-06-02)

- Design spec (~210 LoC) with §1-§10 sections.
- Progress tracker (this file).
- `csv.rs`: `parse_scientific_notation` helper (hand-rolled —
  splits on `e`/`E`, parses exponent as i32 with range cap,
  expands mantissa digit-string by decimal-point shift).
- `csv.rs`: scientific branch in `validate_numeric_text` runs
  BEFORE the canonical-decimal grammar; on `Ok(Some(canonical))`
  returns the expanded form; on `Ok(None)` falls through to the
  V1 finite-decimal path; on `Err(...)` returns the precise
  variant.
- KATs (~14): canonical scientific forms (positive + negative
  exponents + signed mantissas + case-insensitive E + leading-dot
  mantissa), out-of-range exponent, missing exponent, multiple
  exponent markers, malformed exponent sign, non-integer exponent,
  bare `e10` (no mantissa), trailing-dot mantissa rejection.
