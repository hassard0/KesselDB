# SP-PG-COPY-CSV — CSV format for `COPY FROM STDIN` / `COPY TO STDOUT`

> Status: T1 — design spec + parser/encoder + KATs (this commit). T2
> wires into dispatcher; T3 vulcan smoke; T4 arc closure.
>
> SP-arc parent: SP-PG-COPY (V1 closed 2026-05-30 — text format). Named
> as a deferred V2 follow-up in the SP-PG-COPY design spec §2.2 and the
> SP-PG-COPY progress tracker §V2 follow-ups list.
>
> Companion progress tracker:
> `docs/superpowers/specs/2026-06-01-kesseldb-subproject-sppgcopycsv-progress.md`
> (created at T1; updated each slice).
>
> Date: 2026-06-01

## §1. Context — why CSV is the next COPY lever

After SP-PG-COPY V1 (text format) closed, three workflows still hit
`0A000 SP-PG-COPY-CSV`:

1. **`pg_dump --csv`** — PG 14+ supports `--csv` to emit CSV-format
   dumps. The output is human-readable (no `\t`/`\N` escapes) and
   directly openable in Excel / Sheets / R / `pandas.read_csv` without
   any preprocessing.
2. **`psql \copy t FROM 'data.csv' CSV HEADER`** — the canonical
   analyst-on-ramp. A SaaS-style CSV-upload UI built on KesselDB
   should accept the exact same byte stream a user just exported from
   Sheets.
3. **`COPY (SELECT ...) TO STDOUT WITH (FORMAT csv, HEADER)`** — the
   shape every Postgres-aware reporting tool uses for one-off
   exports. (Query-form COPY is its own follow-up arc
   `SP-PG-COPY-QUERY` — V1 of this arc covers only the `<table>`
   form, but the CSV codec is shared so the query form lands cheaply.)

Adoption surfaces unlocked by SP-PG-COPY-CSV V1:

| Surface | Today (text only) | After SP-PG-COPY-CSV V1 |
|---|---|---|
| `pg_dump --csv` | FAIL — 0A000 `SP-PG-COPY-CSV` | works |
| `psql \copy ... FROM 'x.csv' CSV` | FAIL | works |
| `psql \copy ... TO 'x.csv' CSV HEADER` | FAIL | works |
| `pandas.read_sql + .to_csv` round-trip | works (text + post-process) | works (direct CSV) |
| Excel / Sheets bulk import | requires text→CSV converter | direct |

## §2. Scope

### §2.1. V1 in-scope

1. **`WITH (FORMAT csv, ...)`** parsed by the COPY-command recognizer.
   `format=csv` flips both directions (FROM + TO) to the CSV codec.
2. **CSV codec options**: `DELIMITER 'X'`, `QUOTE 'X'`, `ESCAPE 'X'`,
   `NULL 'string'`, `HEADER` (boolean — `HEADER` or `HEADER true` or
   `HEADER false`). Each option is parsed into a `CsvOptions` struct
   with PG-canonical defaults.
3. **CSV parse** per PG §55.2.7 / `src/backend/commands/copyfromparse.c`:
   - Default delimiter `,`, default quote `"`, default escape = quote.
   - Empty unquoted field = NULL (when not overridden by `NULL '...'`).
   - Empty quoted field = empty string (DISTINCT from NULL).
   - Embedded delimiter / quote / newline → quoted field. Inside a
     quoted field, the quote char is escaped by DOUBLING (`""`) —
     OR by the configured `ESCAPE` char if it differs from `QUOTE`.
   - Newlines inside a quoted field are part of the value (rows can
     span multiple lines).
   - Backslashes are NOT escape characters in CSV (unlike text). A
     literal backslash is just a backslash.
4. **CSV encode**:
   - Values containing delimiter, quote, or newline → quoted.
   - Embedded quotes → doubled (or escaped by `ESCAPE` char).
   - NULL → empty unquoted (or the configured `NULL '...'` marker).
   - Empty string → empty quoted (`""`).
5. **HEADER** on input: skip the first row (the column-name header).
   On output: emit a first row containing the column names per the
   resolved column list.
6. **Dispatcher wiring**: `process_copy_data` dispatches text vs CSV
   per `state.format`. `dispatch_copy_to` likewise.
7. **No-extra-deps invariant preserved.** Hand-rolled CSV codec in
   `kessel-pg-gateway::copy::csv`. Pure Rust + std. `#![forbid(unsafe_code)]`.
   The `csv` crate is NOT added.

### §2.2. V1 out-of-scope (named follow-ups)

- **`SP-PG-COPY-CSV-FORCEQUOTE` (V2)** — `FORCE_QUOTE (col1, col2)`,
  `FORCE_NOT_NULL (col1)`, `FORCE_NULL (col1)`. These are column-scoped
  modifiers PG uses when a column's empty string vs NULL semantics
  needs special handling. V1 parses the option clause but rejects with
  a precise `0A000` if any FORCE_* option is present. ~1 slice.
- **`SP-PG-COPY-CSV-ENCODING` (V2)** — `ENCODING 'utf16'` etc.
  KesselDB's catalog is UTF-8 everywhere; V1 accepts only the
  PG-default encoding (UTF-8). Non-UTF-8 inputs surface a clean
  `0A000` rejection. ~1 slice.
- **`SP-PG-COPY-QUERY` (V2)** — `COPY (SELECT ...) TO STDOUT WITH
  (FORMAT csv, ...)`. The CSV codec lands here cheaply when the
  query-form parser arrives. ~1 slice.

### §2.3. PG semantics — defaults table

| Option | Text default | CSV default | V1 honors |
|---|---|---|---|
| DELIMITER | `\t` | `,` | yes |
| QUOTE | n/a | `"` | yes |
| ESCAPE | n/a | same as QUOTE | yes |
| NULL marker | `\N` | empty unquoted | yes |
| HEADER | no | no | yes |
| FORCE_QUOTE | n/a | none | rejected (V2) |
| FORCE_NOT_NULL | n/a | none | rejected (V2) |
| FORCE_NULL | n/a | none | rejected (V2) |
| ENCODING | client_encoding | client_encoding | accept utf8 only (V2 lifts) |

## §3. Module layout

```
crates/kessel-pg-gateway/src/copy/
├── mod.rs           — CopyState, CopyInState (+ format field)
├── proto.rs         — wire encoders (unchanged)
├── text.rs          — text-format codec (unchanged)
├── csv.rs           — NEW: CSV-format codec
├── command.rs       — extended: parse WITH options into CsvOptions
└── dispatch.rs      — extended: branch on format for CSV vs text
```

### §3.1. `csv.rs` surface

```rust
pub struct CsvOptions {
    pub delimiter: u8,    // default b','
    pub quote: u8,        // default b'"'
    pub escape: u8,       // default = quote
    pub null_marker: String, // default ""
    pub header: bool,     // default false
}

impl Default for CsvOptions { /* PG defaults */ }

pub enum CsvParseError {
    UnterminatedQuote,
    InvalidEscape,
    FieldCountMismatch { expected: usize, actual: usize },
}

/// Parse one CSV record from `bytes` starting at `pos`. Returns the
/// parsed fields + the byte offset of the start of the NEXT record
/// (one past the trailing newline). Returns `Ok(None)` if `bytes`
/// contains a partial record (need more data).
pub fn parse_csv_record(
    bytes: &[u8],
    pos: usize,
    options: &CsvOptions,
) -> Result<Option<(Vec<Option<String>>, usize)>, CsvParseError>;

/// Encode one CSV record. Returns bytes INCLUDING the trailing `\n`.
pub fn encode_csv_record(
    values: &[Option<&str>],
    options: &CsvOptions,
) -> Vec<u8>;
```

A record-oriented parser is required (not line-oriented) because a
CSV field can contain literal newlines inside quotes. The carry-buffer
contract carries over from text format: when `parse_csv_record`
returns `Ok(None)`, the dispatcher saves the trailing bytes for the
next CopyData frame.

### §3.2. `CopyFormat` enum + `CopyInState` extension

```rust
pub enum CopyFormat {
    Text,
    Csv(CsvOptions),
}

pub struct CopyInState {
    // ... existing fields ...
    pub format: CopyFormat,
}
```

`dispatch_copy_in_start` resolves the format at COPY-start time and
seeds `CopyInState::format`. `process_copy_data` branches on format
inside its row-extraction loop.

### §3.3. `command.rs` extension

`parse_copy_command` returns a richer `ParsedCopy`:

```rust
pub enum ParsedCopy {
    From { table: String, columns: Option<Vec<String>>, format: CopyFormat },
    To   { table: String, columns: Option<Vec<String>>, format: CopyFormat },
    Rejected { reason: RejectReason },
}
```

The existing `extract_format_clause` helper widens to a full
`parse_with_options(s) -> Result<CopyFormat, RejectReason>` that walks
the parenthesized list and populates `CsvOptions` fields. Unknown
options are silently dropped (V1 stance). FORCE_QUOTE / FORCE_NOT_NULL
/ FORCE_NULL → `RejectReason::UnsupportedCsvOption` → `0A000` with
the precise option name.

## §4. CSV codec — edge cases

| Input | Parsed |
|---|---|
| `1,hello,world` | `[Some("1"), Some("hello"), Some("world")]` |
| `1,,world` | `[Some("1"), None, Some("world")]` (empty unquoted = NULL) |
| `1,"",world` | `[Some("1"), Some(""), Some("world")]` (empty quoted = empty string) |
| `1,"hello, world",3` | `[Some("1"), Some("hello, world"), Some("3")]` |
| `1,"embedded ""quote""",3` | `[Some("1"), Some(r#"embedded "quote""#), Some("3")]` |
| `1,"line1\nline2",3` | `[Some("1"), Some("line1\nline2"), Some("3")]` (3 fields, multi-line record) |
| `1;hello` with `DELIMITER ';'` | `[Some("1"), Some("hello")]` |
| `1,NULL,3` with `NULL 'NULL'` | `[Some("1"), None, Some("3")]` |

Encode is the symmetric inverse. A value needs quoting iff it contains:
delimiter, quote char, `\n`, `\r`, OR it equals the configured null
marker (when non-empty — so the marker doesn't round-trip as NULL when
it's actually a real value).

## §5. HEADER handling

**On input** (CSV → rows):
- The first CSV record is consumed as the header row.
- V1 does NOT validate column names against the table schema (PG
  itself doesn't — HEADER is informational; you can have `colA,colB`
  in the header even if the table columns are `colX,colY`).
- The COPY's `columns` clause (`COPY t (c1, c2) FROM STDIN`) takes
  precedence — HEADER is consumed but ignored for column-mapping.
- V2 `SP-PG-COPY-CSV-HEADER-MATCH` would add `HEADER MATCH` PG-15+
  semantics (validate header against schema).

**On output** (rows → CSV):
- A first record is emitted containing the resolved column names
  (from the COPY's `columns` clause OR the table schema).
- Each name is rendered with the same quoting rules as data
  (delimiter/quote in a column name forces quoting).

## §6. Error semantics — additions to SP-PG-COPY §6

| Trigger | SQLSTATE | Message | State after |
|---|---|---|---|
| `WITH (FORMAT csv, FORCE_QUOTE ...)` | `0A000` | `COPY csv option FORCE_QUOTE not supported in V1 (SP-PG-COPY-CSV-FORCEQUOTE)` | Idle |
| `WITH (FORMAT csv, FORCE_NOT_NULL ...)` | `0A000` | `COPY csv option FORCE_NOT_NULL not supported in V1 (SP-PG-COPY-CSV-FORCEQUOTE)` | Idle |
| `WITH (FORMAT csv, ENCODING 'utf16')` | `0A000` | `COPY csv non-UTF-8 encoding not supported in V1 (SP-PG-COPY-CSV-ENCODING)` | Idle |
| Unterminated quote in CSV | `22023` | `COPY row N: unterminated CSV quoted field` | In→Idle |
| Field count mismatch | `22023` | `COPY row N: expected E fields, got A` | In→Idle |
| `WITH (FORMAT csv, DELIMITER '<multichar>')` | `22023` | `COPY csv DELIMITER must be a single character` | Idle |

## §7. Task decomposition

| T# | Scope | KAT delta |
|---|---|---|
| **T1** | Design spec (this commit) + `csv.rs` codec + `CsvOptions` + `command.rs` `WITH` option parser + `dispatch.rs` format branch + format field on `CopyInState`. ~15-20 new KATs. | +18 |
| **T2** | Vulcan smoke + USAGE update. Real psql `\copy ... CSV HEADER` round-trip, including quoted+escaped fields. | (smoke) |
| **T3** | STATUS row + USAGE §9 expansion + progress tracker → CLOSED + TaskList #358 ready. | (docs) |

Estimated +15-20 KATs total.

## §8. Acceptance criteria

1. `psql -h vulcan -p 5532 -U test -c "COPY t FROM STDIN WITH (FORMAT csv, HEADER)" < x.csv` ingests rows + skips the header.
2. `psql ... -c "COPY t TO STDOUT WITH (FORMAT csv, HEADER)"` emits a CSV file with a header row + per-row CSV records.
3. Round-trip: `COPY t TO STDOUT WITH (FORMAT csv) > file.csv; COPY t FROM STDIN WITH (FORMAT csv) < file.csv` produces an identical row set.
4. Quoted+escaped fields preserve correctly: a value containing a comma OR an embedded quote round-trips byte-equal.
5. Custom `DELIMITER ';'` / `NULL 'NULL'` honored.
6. `pg_dump --csv` shape works end-to-end.
7. PG-wire-Simple + Extended + HTTP/1.1 + WS surfaces byte-untouched.
8. `#![forbid(unsafe_code)]` honored; zero new external deps.

## §9. Weak spots / open questions

1. **HEADER MATCH not implemented.** PG 15+ adds `HEADER MATCH` which
   validates the input header against the table schema. V1 silently
   consumes the header regardless. *Mitigation*: documented; V2 arc named.
2. **No streaming validation of `ESCAPE != QUOTE` semantics.** When
   `ESCAPE` differs from `QUOTE`, the escape sequence is `<ESCAPE><QUOTE>`
   instead of `<QUOTE><QUOTE>`. V1 honors this in the parser/encoder
   but real-world data using a distinct ESCAPE is rare; the path is
   covered by a KAT but not yet by a vulcan smoke.
3. **No `FORCE_QUOTE *`** — a common shape for "quote every field
   always." V1 rejects FORCE_QUOTE in any form. V2 `SP-PG-COPY-CSV-
   FORCEQUOTE` lifts.
4. **Multi-byte UTF-8 delimiter/quote.** PG requires single-byte
   delimiters; V1 enforces with `22023` if a multi-char option value
   is supplied. Documented.

## §10. References

- PostgreSQL §55.2.7 "COPY-related" message formats (text-format
  applies to CSV too at the wire — only the payload encoding differs).
- PostgreSQL §SQL-COPY "CSV Format" subsection — option semantics +
  defaults table.
- RFC 4180 — Common Format and MIME Type for CSV Files (the
  general-purpose CSV grammar; PG's CSV implementation is a
  superset/strict-subset of RFC 4180 depending on the option).
- libpq's `PQputCopyData` is format-agnostic — the CSV semantics
  are a server-side parsing/encoding concern; the wire framing is
  identical to text.
