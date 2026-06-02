# SP-PG-COPY-CSV-NUMERIC-SCI — scientific notation in text/CSV COPY NUMERIC

> Status: T1 — design spec + scientific-notation parser added to
> `validate_numeric_text` + KATs (this commit). T2 wires the vulcan
> smoke + USAGE. T3 STATUS + tracker closure.
>
> SP-arc parent: SP-PG-COPY-CSV-NUMERIC (V1 closed 2026-06-02 —
> canonical decimals + NaN/±Infinity validator). V1 explicitly
> rejected scientific notation with a precise
> `ScientificNotation` variant naming this V2 arc. This arc lifts
> that rejection by parsing the exponent and expanding the value
> back to canonical decimal form.
>
> Companion progress tracker:
> `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgcopycsvnumericsci-progress.md`
> (created at T1; updated each slice).
>
> Date: 2026-06-02

## §1. Context — the scientific-notation gap

SP-PG-COPY-CSV-NUMERIC V1 closed the text/CSV NUMERIC validation
gap for the 99% case of canonical signed decimals + case-insensitive
NaN/±Infinity. The arc explicitly carved out scientific notation
as a follow-up: V1 rejects `1e10`, `2E-3`, `6.022e+23` with a
precise `22P02` naming this V2 arc.

Three real-world workflows surface scientific notation in CSV input:

1. **ORM exports.** SQLAlchemy / Hibernate / EF Core emit large
   or very small NUMERIC values in scientific form when the value's
   absolute magnitude exceeds the default decimal-string width.
   `6.022e23` is the canonical Avogadro-number export shape.
2. **Spreadsheet exports.** Excel / Google Sheets / LibreOffice
   Calc auto-format any number with more than 11 significant digits
   as scientific notation, and `Save As CSV` emits that scientific
   form verbatim. The same applies to very small fractions where
   the exponent reduces the visible digit count.
3. **Scientific / statistical packages.** R's `write.csv()`, NumPy's
   `np.savetxt`, Stata's `outsheet` all default to scientific
   notation for values outside a `[1e-4, 1e15]` window. The CSV the
   analyst hands you carries the scientific form.

Real PostgreSQL accepts scientific notation in COPY text + CSV
NUMERIC fields and the `numeric_in` C-side parser normalises by
expanding the exponent into the canonical decimal text the
`numeric_out` function would emit. After this arc lands, KesselDB
matches that behaviour for the V1 magnitude band.

Adoption surfaces unlocked by SP-PG-COPY-CSV-NUMERIC-SCI V1:

| Workflow | Today (V1 SP-PG-COPY-CSV-NUMERIC) | After V2 |
|---|---|---|
| ORM `COPY t (id, score) FROM STDIN CSV` with `6.022e23` in `score` | `22P02 scientific notation not supported in V1 (SP-PG-COPY-CSV-NUMERIC-SCI)` | accepted, expanded to `602200000000000000000000` |
| Sheets `Save As CSV` with auto-formatted `1.5E-3` in a NUMERIC col | same `22P02` rejection | accepted, expanded to `0.0015` |
| R `write.csv()` of a NUMERIC vector with `-3.14e2` | same `22P02` rejection | accepted, expanded to `-314` |

## §2. Scope

### §2.1. V1 in-scope

1. **Parse the scientific-notation grammar:**
   - `[+-]?\d+(\.\d+)?[eE][+-]?\d+` — mantissa with integer or
     integer+fractional part (e.g. `1e10`, `1.5E-3`, `6.022e+23`,
     `-3.14e2`).
   - `[+-]?\.\d+[eE][+-]?\d+` — mantissa with leading-dot fractional
     only (e.g. `.5e2`).
   - Mantissa with trailing dot (`5.e2`) is **out-of-scope** for V1
     (V2 SP-PG-COPY-CSV-NUMERIC-SCI-TRAILDOT) — it never appears in
     practice from any ORM/spreadsheet export.
2. **Expand the exponent into canonical decimal form** by shifting
   the decimal point through the mantissa's digit string. Examples:
   - `1e10` → `10000000000`
   - `1.5e2` → `150`
   - `1.5e-2` → `0.015`
   - `6.022e23` → `602200000000000000000000`
   - `-3.14e2` → `-314`
   - `+1.5e+3` → `1500`
   - `1E10` → `10000000000` (case-insensitive `e`/`E`)
   - `1e0` → `1`
   - `0e0` → `0`
3. **Range cap on the exponent.** Reject exponents with
   `|exp| > 100` as `Malformed { reason: "exponent out of range" }`.
   The hard cap matches the CSV-NUMERIC base codec's
   `|value| < 10^18` storage range with a 5× margin for fractional
   mantissas. Larger magnitudes still surface as a clean
   validator-level error rather than allocating a 1000-character
   digit string only for the engine to reject downstream.
4. **The expanded canonical form replaces the original field bytes**
   on success, so the synthesized INSERT VALUES carries the
   normalised decimal representation. This matches the V1
   sign-normalisation + NaN/Inf canonicalisation behaviour.
5. **`#![forbid(unsafe_code)]`** preserved; no new external
   dependencies (hand-rolled parser/expander, no regex / no
   bigint crate).

### §2.2. V1 out-of-scope (named follow-ups)

- **SP-PG-COPY-CSV-NUMERIC-SCI-TRAILDOT (V2)** — trailing-dot
  mantissa in scientific notation (`5.e2`, `1.E10`). Real PG accepts
  these. No ORM / spreadsheet export emits them in practice. ~1
  slice when the gap surfaces.
- **SP-PG-COPY-NUMERIC-BIGNUM (V2)** — arbitrary-precision NUMERIC
  beyond the kessel-sql i128 / 10^18 cap. The validator accepts any
  well-formed scientific value within the |exp|≤100 cap, but the
  engine-side i128 still surfaces at INSERT time for the magnitude
  band above. ~1 slice when the bignum engine layer arrives.
- **SP-PG-COPY-NUMERIC-LOCALE (V2)** — locale-aware decimal
  separators in the mantissa (`1,5e3` European-style). Same
  reasoning as the parent V1 spec — locales handled at the
  application layer.

## §3. Module layout

```
crates/kessel-pg-gateway/src/copy/
└── csv.rs            — adds parse_scientific_notation helper +
                         scientific-notation branch in the existing
                         validate_numeric_text grammar (replaces the
                         hard ScientificNotation reject).
```

No changes to `dispatch.rs` — the dispatcher already routes any
`Ok(canonical)` returned by `validate_numeric_text` into the field
bytes verbatim and any `Err(...)` into a precise `22P02`. After
this arc, `1e10` returns `Ok("10000000000")` instead of
`Err(ScientificNotation)`; the dispatcher path is unchanged.

### §3.1. Parser surface (in `csv.rs`)

```rust
/// Try to parse `s` as scientific-notation NUMERIC and return the
/// canonical decimal form. Returns:
/// - `Ok(Some(canonical))` if the input matches the scientific
///   grammar AND the exponent shift produced a valid decimal.
/// - `Ok(None)` if the input doesn't contain `e`/`E` at all
///   (caller falls through to the canonical-decimal grammar).
/// - `Err(CsvNumericError)` if the input contains `e`/`E` but the
///   mantissa or exponent is malformed.
fn parse_scientific_notation(s: &str) -> Result<Option<String>, CsvNumericError>;
```

The signature returns `Ok(None)` for the "no `e`" case so the
existing finite-decimal grammar runs only on inputs that don't
match the scientific shape. This keeps the V1 finite-decimal happy
path zero-cost (single byte-scan for `e`/`E`).

### §3.2. CsvNumericError — no new variants

The existing variants already cover every scientific-notation
failure mode:
- `Empty` — bare `e10` (no mantissa) or `1e` (no exponent).
- `BadByte` — `1e1x` (non-digit in exponent).
- `Malformed { reason }` — `1e+-3`, `1ee2`, `1e1.5`, exponent
  out-of-range (`1e1000`).

The `ScientificNotation` variant becomes **unused after this arc**
but is preserved in the public enum for back-compat with any
downstream pattern-match. The V1 dispatcher arm for the variant
stays in place as defensive-default (unreachable in practice).

## §4. Algorithm — decimal-point shift

The expansion algorithm is straightforward digit-string surgery.
Given a mantissa `M` with `mant_dot_pos` (the byte offset of the
decimal point in M, or `M.len()` if there's no dot) and an exponent
`E`:

1. **Strip the dot from the mantissa** — the resulting digit string
   `D` represents the integer value `M * 10^(M.len() - mant_dot_pos - 1)`
   (i.e. the dot was originally `(M.len() - mant_dot_pos - 1)` places
   from the right).
2. **Compute the effective shift** `K = E - (M.len() - mant_dot_pos - 1)`
   — this is "how many places to shift D's implicit decimal point
   to the right." Equivalently, the final value is `D * 10^K`.
3. **Render the canonical decimal:**
   - `K >= 0`: append `K` zeros to `D`.
   - `K < 0` and `|K| < D.len()`: insert a dot `|K|` places from the
     right of `D`. Strip a leading zero in front of the dot only if
     the integer part is empty (no — keep the integer part always
     present per PG canonical form).
   - `K < 0` and `|K| >= D.len()`: pad with leading zeros — the
     result is `0.{(|K|-D.len()) zeros}{D}`.
4. **Strip leading zeros** from the integer part (but preserve a
   single `0` if the integer part is otherwise empty).
5. **Apply the original mantissa sign** to the result, with the
   `-0` → `0` canonicalisation rule (consistent with V1).

Worked examples:
- `1e10`: M=`"1"`, mant_dot_pos=1, E=10. D=`"1"`, K=10. Append 10
  zeros → `"10000000000"`.
- `1.5e2`: M=`"1.5"`, mant_dot_pos=1, E=2. D=`"15"`, K=2-1=1. Append
  1 zero → `"150"`.
- `1.5e-2`: M=`"1.5"`, mant_dot_pos=1, E=-2. D=`"15"`, K=-2-1=-3.
  `|K|=3 > D.len()=2`, so pad: `0.0{15}` → `"0.015"`.
- `6.022e23`: M=`"6.022"`, mant_dot_pos=1, E=23. D=`"6022"`, K=23-3=20.
  Append 20 zeros → `"602200000000000000000000"`.
- `1e-3`: M=`"1"`, mant_dot_pos=1, E=-3. D=`"1"`, K=-3-0=-3. `|K|=3 ==
  D.len()=1` × 3, pad: `0.001`.
- `-3.14e2`: M=`"-3.14"`, mant_dot_pos=2, E=2. Sign=`-`, mantissa
  body=`"3.14"`, D=`"314"`, K=2-2=0. Result: `"314"`, sign prepended
  → `"-314"`.
- `0e0`: M=`"0"`, mant_dot_pos=1, E=0. D=`"0"`, K=0. Result `"0"`.

## §5. Grammar — additions to validate_numeric_text

| Input | V1 behaviour | V2 (this arc) behaviour |
|---|---|---|
| `1e10` | Err(ScientificNotation) | Ok(`"10000000000"`) |
| `1.5E-3` | Err(ScientificNotation) | Ok(`"0.0015"`) |
| `6.022e+23` | Err(ScientificNotation) | Ok(`"602200000000000000000000"`) |
| `-3.14e2` | Err(ScientificNotation) | Ok(`"-314"`) |
| `+1.5e+3` | Err(ScientificNotation) | Ok(`"1500"`) |
| `1E10` | Err(ScientificNotation) | Ok(`"10000000000"`) |
| `.5e2` | Err(ScientificNotation) | Ok(`"50"`) |
| `1e0` | Err(ScientificNotation) | Ok(`"1"`) |
| `0e0` | Err(ScientificNotation) | Ok(`"0"`) |
| `1e1000` | Err(ScientificNotation) | Err(Malformed: "exponent out of range") |
| `e10` (no mantissa) | Err(BadByte at 0) | Err(BadByte at 0) (unchanged — pre-scan rejects) |
| `1e` (no exponent) | Err(ScientificNotation) | Err(Malformed: "missing exponent") |
| `1ee2` | Err(ScientificNotation) | Err(Malformed: "multiple exponent markers") |
| `1e+-3` | Err(ScientificNotation) | Err(Malformed: "malformed exponent") |
| `1e1.5` | Err(ScientificNotation) | Err(Malformed: "non-integer exponent") |
| `5.e2` (trailing-dot mantissa) | Err(ScientificNotation) | Err(Malformed: "trailing-dot mantissa in scientific notation not supported in V1 (SP-PG-COPY-CSV-NUMERIC-SCI-TRAILDOT)") |

## §6. Error semantics

| Trigger | SQLSTATE | Message | State after |
|---|---|---|---|
| `validate_numeric_text` returns Malformed("exponent out of range") | `22P02` | `COPY {fmt} row N column "C" NUMERIC: malformed (exponent out of range)` | In→Idle |
| `validate_numeric_text` returns Malformed("missing exponent") | `22P02` | `COPY {fmt} row N column "C" NUMERIC: malformed (missing exponent)` | In→Idle |

The dispatcher's existing `CsvNumericError::Malformed { reason }`
arm already handles these — no new dispatch code.

## §7. Task decomposition

| T# | Scope | KAT delta |
|---|---|---|
| **T1** | Design spec (this commit) + `parse_scientific_notation` helper + scientific branch in `validate_numeric_text` + KATs. | +14 |
| **T2** | Real psql 16 vulcan smoke + USAGE §9 expansion. | (smoke) |
| **T3** | STATUS row + tracker → CLOSED + TaskList #385 ready. | (docs) |

Target KAT delta: +10 to +15.

## §8. Acceptance criteria

1. `psql ... COPY t FROM STDIN WITH (FORMAT csv, HEADER)` with a
   NUMERIC column accepts `1e10`, `1.5e-3`, `6.022e23`, `-3.14e2`.
2. The expanded canonical form is what reaches the engine (verified
   via `SELECT * FROM t` after the COPY).
3. Out-of-range exponent (`1e1000`) and malformed exponent (`1e+-3`,
   `1e1.5`, `1e`) reject with `22P02` + precise message naming the
   reason.
4. The validator imposes no new dependencies (`#![forbid(unsafe_code)]`
   honored).
5. PG-wire-Simple + Extended + HTTP/1.1 + WS surfaces byte-untouched.
6. CI green on the full workspace.

## §9. Weak spots / open questions

1. **Engine-side magnitude cap still surfaces downstream.** A value
   like `1e25` parses cleanly at the validator (`|exp|≤100`) and
   expands to a 26-digit string, but the kessel-sql i128 backing
   storage caps at ~`1.7e38` in absolute terms — so values within
   `1e18 < |v| < 1e38` succeed end-to-end, while `|v| > 1.7e38` fails
   at the engine boundary with a less precise message. V2
   SP-PG-COPY-NUMERIC-BIGNUM lifts.
2. **Trailing-dot mantissa rejected.** `5.e2` is real PG-valid but
   no ORM/spreadsheet emits it. V2 SP-PG-COPY-CSV-NUMERIC-SCI-TRAILDOT.
3. **No locale-aware mantissa.** `1,5e3` (comma decimal) rejects. V2
   SP-PG-COPY-NUMERIC-LOCALE.

## §10. References

- PostgreSQL `src/backend/utils/adt/numeric.c::numeric_in` — accepts
  scientific notation and expands the exponent into the canonical
  text representation.
- SP-PG-COPY-CSV-NUMERIC design spec (2026-06-02) — parent V1 arc
  that explicitly named `SP-PG-COPY-CSV-NUMERIC-SCI` as the
  follow-up.
- IEEE 754 / SQL:2016 §6.5 — scientific notation grammar (mantissa
  with optional fractional + `e`/`E` + signed integer exponent).
