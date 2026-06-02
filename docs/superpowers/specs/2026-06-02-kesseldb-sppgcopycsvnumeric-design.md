# SP-PG-COPY-CSV-NUMERIC — text/CSV COPY NUMERIC canonical validator

> Status: T1 — design spec + `validate_numeric_text` validator + KATs
> (this commit). T2 wires into the text + CSV dispatch path + vulcan
> smoke. T3 STATUS + USAGE + tracker closure.
>
> SP-arc parent: SP-PG-COPY-CSV (V1 closed 2026-06-01 — CSV format
> codec). SP-PG-COPY-BIN-NUMERIC (V1 closed 2026-06-02 — binary
> NUMERIC) handles the wire-binary side. This arc closes the
> validate-on-text-input gap for NUMERIC columns when COPY runs in
> text or CSV format.
>
> Companion progress tracker:
> `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgcopycsvnumeric-progress.md`
> (created at T1; updated each slice).
>
> Date: 2026-06-02

## §1. Context — the text/CSV NUMERIC validation gap

SP-PG-COPY V1 (text format) and SP-PG-COPY-CSV V1 (CSV format) both
pass NUMERIC column values straight from the wire byte slice into
the synthesized INSERT VALUES tuple. The kessel-sql parser then
parses the bare token as a decimal literal. This works for the
99% case of finite decimal values that look like `42`, `12345.6789`,
or `-3.14`. Three shapes still hit a confusing failure mode:

1. **PG NaN / Infinity literals.** PG 14+ accepts `NaN`,
   `Infinity`, `-Infinity` (case-insensitive) as NUMERIC literals
   in COPY text + CSV input. Today these reach the kessel-sql
   parser as bare unquoted tokens; the parser rejects with a
   generic `parse_error` because `NaN` isn't a kessel-sql literal.
2. **Garbage that "looks decimal-ish."** `1.2.3`, `--5`, `4..2`,
   `1e10` (scientific notation), trailing-`.` (`42.`), leading-`.`
   without integer part (`.5`) — the kessel-sql parser may either
   accept these as something unintended OR reject with a confusing
   message naming kessel-sql tokens rather than the canonical PG
   NUMERIC grammar.
3. **Case-insensitive NaN/Inf.** PG accepts `nan`, `NAN`, `inf`,
   `INFINITY`, `+Inf`, `-INF` and normalises to the canonical mixed
   case `NaN` / `Infinity` / `-Infinity` for downstream rendering.
   Today every case-form except the canonical one rejects.

Adoption surfaces unlocked by SP-PG-COPY-CSV-NUMERIC V1:

| Workflow | Today | After V1 |
|---|---|---|
| `pg_dump --csv` of a NUMERIC column carrying `NaN` | parse failure | round-trips |
| `psql \copy t FROM 'analyst.csv' CSV HEADER` with mixed-case `infinity` | parse failure | accepted, canonicalised |
| Spreadsheet CSV with `#N/A` accidentally typed into a NUMERIC col | confusing engine-level error | clean `22P02` at the COPY layer |
| `1e10` scientific-notation in a CSV exported from R/Sheets | confusing parse_error | clean `22P02` naming `SP-PG-COPY-CSV-NUMERIC-SCI` follow-up arc |

## §2. Scope

### §2.1. V1 in-scope

1. **`validate_numeric_text(s: &str) -> Result<String, CsvNumericError>`**
   in `crates/kessel-pg-gateway/src/copy/csv.rs` (alongside the
   existing CSV codec).
   - Accepts canonical signed decimal: `[+-]?\d+(\.\d+)?` OR
     `[+-]?\.\d+` (leading-dot fractional). Returns the canonical
     form with sign normalised (`+42` → `42`).
   - Accepts case-insensitive NaN: `nan`, `NaN`, `NAN`, … →
     canonical `"NaN"`.
   - Accepts case-insensitive Infinity: `infinity`, `Infinity`,
     `+infinity`, `inf`, `+inf`, … → canonical `"Infinity"`.
   - Accepts case-insensitive minus-Infinity: `-infinity`,
     `-Infinity`, `-inf`, … → canonical `"-Infinity"`.
   - Rejects everything else with a precise `CsvNumericError`
     variant.
2. **Dispatcher integration.** `dispatch.rs::process_copy_data_csv`
   and `process_copy_data_text`: when the column kind maps to
   `PG_TYPE_NUMERIC`, run `validate_numeric_text` on the field bytes
   BEFORE pushing into `pending_rows`. On error, fail the COPY with
   `22P02 invalid_text_representation` (the canonical SQLSTATE for
   bad textual representations of typed values) + a precise message
   naming the failing row, column, and reason.
3. **NULL passes through unchanged.** Validation only applies to
   non-NULL field bytes; `None` fields are forwarded as-is so the
   kessel-sql column-omit auto-NULL-fill semantics keep working.
4. **`#![forbid(unsafe_code)]`** preserved; no new external
   dependencies (hand-rolled validator, no regex crate).

### §2.2. V1 out-of-scope (named follow-ups)

- **`SP-PG-COPY-NUMERIC-BIGNUM` (V2)** — arbitrary-precision NUMERIC
  beyond the i128 / 18-fractional-digit cap. The validator accepts
  any well-formed decimal string regardless of magnitude (PG's text
  representation is unbounded), but engine-side storage still hits
  the kessel-sql i128 cap. The validator surfaces only shape
  errors; magnitude is checked downstream. ~1 slice when the bignum
  engine layer arrives.
- **`SP-PG-COPY-CSV-NUMERIC-SCI` (V2)** — scientific notation
  (`1.5e10`, `2E-3`). PG's `numeric_in` accepts these and normalises
  by expanding the exponent. V1 rejects with a precise message
  naming the follow-up arc. ~1 slice (add a separate `e`/`E`
  exponent parsing branch to the validator).
- **`SP-PG-COPY-NUMERIC-LOCALE` (V2)** — locale-aware decimal
  separators (`,` instead of `.`). PG's `COPY ... WITH (DECIMAL ',')`
  doesn't exist; locales handled at the application layer. V1
  accepts only `.` as the decimal separator.

## §3. Module layout

```
crates/kessel-pg-gateway/src/copy/
├── csv.rs            — adds validate_numeric_text + CsvNumericError
└── dispatch.rs       — wires the validator into the text + CSV
                         per-row path before BULKAPPLY fold
```

### §3.1. `validate_numeric_text` surface

```rust
pub enum CsvNumericError {
    /// Empty string (or all-whitespace).
    Empty,
    /// Non-decimal byte at the given position.
    BadByte { position: usize, byte: u8 },
    /// Multiple decimal points / multiple signs.
    Malformed { reason: &'static str },
    /// Scientific notation rejected — V2 SP-PG-COPY-CSV-NUMERIC-SCI.
    ScientificNotation,
}

/// Validate a text/CSV NUMERIC field's contents.
///
/// Returns the CANONICAL form on success:
/// - finite decimals: sign-normalised (`+42` → `42`); leading-zero
///   integer + integer-only tolerated as-is (`007` stays `007` —
///   kessel-sql later parses the canonical form).
/// - NaN / Infinity / -Infinity: returned as the canonical mixed-
///   case PG form.
pub fn validate_numeric_text(s: &str) -> Result<String, CsvNumericError>;
```

### §3.2. Dispatcher integration

```rust
// process_copy_data_csv + process_copy_data_text:
//   for each field f in fields:
//     if f.is_some() && column_oid == PG_TYPE_NUMERIC {
//         match validate_numeric_text(text_of(f)) {
//             Ok(canonical) => f = Some(canonical.into_bytes()),
//             Err(e) => fail("22P02", "COPY row N column C: ..."),
//         }
//     }
```

The canonical form REPLACES the original field bytes so the
synthesized INSERT VALUES carries the normalised representation
(important for `+42` → `42` and `nan` → `NaN` so kessel-sql sees a
consistent token form).

## §4. Validator grammar

| Input | Canonical | Notes |
|---|---|---|
| `42` | `42` | bare integer |
| `12345.6789` | `12345.6789` | finite decimal |
| `-3.14` | `-3.14` | signed |
| `0.0001` | `0.0001` | small frac |
| `+42` | `42` | sign normalised |
| `.5` | `.5` | leading-dot tolerated (PG accepts) |
| `5.` | `5.` | trailing-dot tolerated (PG accepts) |
| `007` | `007` | leading zeros preserved (engine canonicalises) |
| `nan` / `NAN` / `NaN` | `NaN` | case-insensitive specials |
| `infinity` / `INFINITY` / `+Infinity` / `inf` / `+inf` | `Infinity` | |
| `-infinity` / `-Infinity` / `-inf` | `-Infinity` | |
| `hello` | Err(BadByte) | |
| `1.2.3` | Err(Malformed) | multiple decimal points |
| `--5` | Err(Malformed) | multiple signs |
| `1e10` | Err(ScientificNotation) | V2 SP-PG-COPY-CSV-NUMERIC-SCI |
| `` (empty) | Err(Empty) | |

## §5. Error semantics — additions

| Trigger | SQLSTATE | Message | State after |
|---|---|---|---|
| `validate_numeric_text` returns BadByte | `22P02` | `COPY row N column C NUMERIC: bad byte 0x.. at position P` | In→Idle |
| `validate_numeric_text` returns Malformed | `22P02` | `COPY row N column C NUMERIC: malformed (REASON)` | In→Idle |
| `validate_numeric_text` returns ScientificNotation | `22P02` | `COPY row N column C NUMERIC: scientific notation not supported in V1 (SP-PG-COPY-CSV-NUMERIC-SCI)` | In→Idle |
| `validate_numeric_text` returns Empty | `22P02` | `COPY row N column C NUMERIC: empty value (use \\N for NULL in text format, empty unquoted for CSV)` | In→Idle |

## §6. Task decomposition

| T# | Scope | KAT delta |
|---|---|---|
| **T1** | Design spec (this commit) + `validate_numeric_text` validator + `CsvNumericError` enum + dispatcher wiring + KATs. | +13 |
| **T2** | Real psql 16 vulcan smoke + USAGE §9 expansion + smoke transcript. | (smoke) |
| **T3** | STATUS row + progress tracker → CLOSED + TaskList #381 ready. | (docs) |

Target KAT delta: +8 to +15.

## §7. Acceptance criteria

1. `psql ... COPY t (id, amount) FROM STDIN WITH (FORMAT csv)` with
   NUMERIC column accepts canonical decimals + NaN/Infinity/-Infinity.
2. Mixed-case `nan` / `Infinity` / `-inf` accepted and canonicalised.
3. Malformed input (`1.2.3`, `hello`, `1e10`) rejects with `22P02` +
   precise message naming the failing row + column.
4. NULL passes through unchanged (text `\N`, CSV empty unquoted).
5. The validator imposes no new dependencies (`#![forbid(unsafe_code)]`
   honored).
6. PG-wire-Simple + Extended + HTTP/1.1 + WS surfaces byte-untouched.
7. CI green on the full workspace.

## §8. Weak spots / open questions

1. **Scientific notation rejected, not accepted.** A CSV exported
   from R or Sheets may carry `1e10`-style values. V1 rejects with a
   precise message; V2 SP-PG-COPY-CSV-NUMERIC-SCI lifts. *Mitigation:*
   documented; arc named in the rejection message.
2. **No arbitrary-precision storage check.** The validator accepts
   any well-formed decimal string regardless of magnitude. The
   engine-side i128 cap surfaces at INSERT time with a less precise
   message. V2 SP-PG-COPY-NUMERIC-BIGNUM lifts.
3. **No locale support.** Only `.` as decimal separator. Most CSVs
   use `.` regardless of locale; comma-decimal CSVs would fail. V2
   SP-PG-COPY-NUMERIC-LOCALE lifts.

## §9. References

- PostgreSQL §SQL-COPY "Text Format" + "CSV Format" — NUMERIC
  column values use the PG canonical decimal representation, same
  as the output of `SELECT numericcol::text`.
- PostgreSQL `src/backend/utils/adt/numeric.c::numeric_in` — the
  C-side parser that accepts NaN / Infinity / case-insensitive
  specials and rejects shape errors.
- SP-PG-EXTQ-BIN-NUMERIC-NAN-INF design spec (2026-06-02) — the
  binary-wire side of the same special-string acceptance.
