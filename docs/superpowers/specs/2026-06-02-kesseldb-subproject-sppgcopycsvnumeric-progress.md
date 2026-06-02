# SP-PG-COPY-CSV-NUMERIC — text/CSV COPY NUMERIC validator — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: T1 IN-FLIGHT — validator + KATs landed; T2 vulcan smoke
pending; T3 STATUS + USAGE + closure pending.**

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgcopycsvnumeric-design.md`

Parent SP-arcs:
- SP-PG-COPY-CSV (V1 closed 2026-06-01 — CSV codec) — this arc adds
  the NUMERIC validation layer on top.
- SP-PG-COPY-BIN-NUMERIC (V1 closed 2026-06-02 — binary NUMERIC) —
  this arc is the text/CSV companion for the same canonical-value
  surface.
- SP-PG-EXTQ-BIN-NUMERIC-NAN-INF (V1 closed 2026-06-02) — same
  case-insensitive NaN/Inf acceptance, lifted to the binary codec.

## What this SP-arc ships

V1 = "text + CSV COPY into a NUMERIC column accepts the full PG
canonical-value grammar (finite decimals + NaN + ±Infinity,
case-insensitive) and rejects malformed inputs with precise
`22P02 invalid_text_representation` errors that name the failing
row + column + reason."

1. `validate_numeric_text(s: &str) -> Result<String, CsvNumericError>`
   in `crates/kessel-pg-gateway/src/copy/csv.rs`.
2. Dispatcher integration in `process_copy_data_csv` and
   `process_copy_data_text` — when the column's PG OID is
   `PG_TYPE_NUMERIC` (1700), validate the field BEFORE adding it to
   `pending_rows`. NULL fields pass through unchanged.
3. Canonical form replaces the original bytes so the synthesized
   INSERT VALUES carries the normalised representation (sign
   stripped from `+42`; `nan` → `NaN`; `inf` → `Infinity`).

**Out-of-scope (named, deferred — each is its own arc):**

- **SP-PG-COPY-CSV-NUMERIC-SCI (V2)** — scientific notation
  (`1e10`, `2E-3`). V1 rejects; arc name surfaced in the rejection
  message.
- **SP-PG-COPY-NUMERIC-BIGNUM (V2)** — arbitrary-precision NUMERIC
  beyond the i128 / 18-frac-digit cap. The validator accepts any
  well-formed decimal text; engine-side i128 cap still applies
  downstream.
- **SP-PG-COPY-NUMERIC-LOCALE (V2)** — locale-aware decimal
  separators (e.g. `,` instead of `.`).

## Slice plan (mirrors design spec §6)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + `validate_numeric_text` + `CsvNumericError` + dispatcher wiring + KATs. | **DONE** | (this commit) |
| **T2** | Real psql 16 vulcan smoke + USAGE §9 expansion + smoke transcript. | pending | — |
| **T3** | STATUS row + tracker → CLOSED + TaskList #381 ready. | pending | — |

## T1 — what landed (2026-06-02)

- Design spec (~230 LoC) with §1-§9 sections.
- Progress tracker (this file).
- `csv.rs`: `CsvNumericError` enum (4 variants) + `validate_numeric_text`
  function. Hand-rolled grammar, no external crate.
- `dispatch.rs`: text + CSV per-row hook that calls the validator on
  NUMERIC-OID columns and rewrites the field bytes to the canonical
  form OR fails the COPY with `22P02`.
- KATs (~13): canonical decimals, sign normalisation, leading-dot,
  case-insensitive NaN/Inf, scientific notation rejection, empty,
  garbage, multi-dot, multi-sign.

## T2 — pending

- Spin a fresh `kesseldb` on vulcan with
  `CARGO_TARGET_DIR=/tmp/kdb-target-csvnumeric`.
- `psql -h 127.0.0.1 -p 5532 -U test -d kesseldb` —
  `CREATE TABLE num_csv (id BIGINT, amount NUMERIC); COPY num_csv
  FROM STDIN WITH (FORMAT csv, HEADER)` with the 6 canonical shapes
  (42, 12345.6789, -3.14, NaN, Infinity, -Infinity).
- Round-trip via `COPY num_csv TO STDOUT WITH (FORMAT csv, HEADER)`
  and confirm byte-equal output.
- Capture transcript at
  `docs/superpowers/sppgcopycsvnumeric-t2-smoke-2026-06-02.txt`.
- USAGE §9 — add a CSV NUMERIC subsection naming the validator +
  accepted forms + rejected forms.

## T3 — pending

- STATUS row under "Current capabilities (2026-06-02)".
- Tracker → CLOSED.
- TaskList #381 ready.
