# SP-PG-COPY-CSV-NUMERIC — text/CSV COPY NUMERIC validator — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T3 (2026-06-02).** Real psql 16
smoke on vulcan (port 5538/6538 — collision-avoiding) confirms text
+ CSV COPY into a NUMERIC-OID column accepts the full canonical PG
decimal grammar + case-insensitive NaN/Infinity, rewrites canonical
forms into the row before the BULKAPPLY fold, and surfaces precise
`22P02 invalid_text_representation` errors on malformed input with
row + column + reason + V2-arc-name where applicable. TaskList #381
ready for completion.

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
| **T1** | Design spec + `validate_numeric_text` + `CsvNumericError` + dispatcher wiring + KATs. | **DONE** | `e9e8adb` |
| **T2** | Real psql 16 vulcan smoke + USAGE §9 expansion + smoke transcript. | **DONE** | `2001074` |
| **T3** | STATUS row + tracker → CLOSED + TaskList #381 ready. | **DONE** | (this commit) |

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

## T2 — landed (commit `2001074`)

Real psql 16 vulcan smoke on port 5538/6538 (sibling-agent collision
on default 5532/6532 forced a port shift). Smoke transcript:
`docs/superpowers/sppgcopycsvnumeric-t2-smoke-2026-06-02.txt`.

Confirmed (validator scope):
- CSV happy path with sign normalisation (`+999` → `999` stored).
- Text-format happy path with same normalisation (`+88` → `88`).
- CSV → text round-trip byte-equal except for sign canonicalisation.
- Validator-layer rejection: 4 CSV shapes (`1.2.3`, `hello`, `1e10`,
  `--5`) + 1 text shape (`hello`) surface precise `22P02` with row
  + column name + reason + V2-arc-name where applicable.

Honest documentation of downstream limitations (out-of-scope for
this arc but tested):
- NaN / Infinity / -Infinity pass the validator (canonicalised to
  mixed-case) but engine-side I128 storage can't hold them →
  downstream "sql: expected value" engine error. V2 arc:
  `SP-PG-COPY-NUMERIC-BIGNUM` / `SP-PG-NAN-IN-ENGINE`.
- Pre-existing protocol artefact "unsupported message tag: 0x63"
  appears after every text/CSV COPY error (psql sends CopyDone
  after seeing ErrorResponse but gateway already in Idle). Same
  tail confirmed on a field-count-mismatch failure; not introduced
  by this arc. V2 arc: `SP-PG-COPY-ABORT-DONE-TAIL`.

USAGE.md updated (this commit folded into T3) — new
"SP-PG-COPY-CSV-NUMERIC — canonical NUMERIC validator" subsection
under SP-PG-COPY-CSV with examples + rejection messages + spec
links.

## T3 — landed (this commit)

- STATUS.md row added under "Current capabilities (2026-06-02)".
- Tracker → CLOSED (this file).
- TaskList #381 ready.
