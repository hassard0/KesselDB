# SP-PG-COPY-CSV — CSV format for COPY FROM/TO — SP-arc Progress Tracker

Date created: 2026-06-01
**Status: CLOSED — V1 SHIPPED at T2 (2026-06-01).** Real psql 16
smoke on vulcan: `COPY FROM STDIN WITH (FORMAT csv, HEADER)` with
3 rows (including embedded comma + doubled-quote escape) + `SELECT
* FROM csv_smoke` returns the right values + `COPY TO STDOUT WITH
(FORMAT csv, HEADER)` round-trips byte-equal. Custom `DELIMITER ';'`
+ `NULL '<NA>'` end-to-end verified. FORCE_QUOTE / non-UTF-8
ENCODING / invalid single-byte options all surface precise V2-pointing
`0A000` / `22023` messages without dropping the connection.
TaskList #358 ready for completion.

Design spec: `docs/superpowers/specs/2026-06-01-kesseldb-sppgcopycsv-design.md`
Smoke transcript: `docs/superpowers/sppgcopycsv-t2-smoke-2026-06-01.txt`

Parent SP-arc: SP-PG-COPY (closed 2026-05-30, text format). Named as
a V2 follow-up in the SP-PG-COPY design spec §2.2 and progress
tracker §V2 follow-ups list — this is the realisation.

## What this SP-arc ships

V1 = "`pg_dump --csv` + `psql \copy ... CSV HEADER` end-to-end work
against KesselDB."

1. `WITH (FORMAT csv [, DELIMITER 'X'] [, QUOTE 'X'] [, ESCAPE 'X']
   [, NULL 'string'] [, HEADER [true|false]])` parsed at the COPY
   command recognizer.
2. CSV-format `COPY ... FROM STDIN` decodes per-record + dispatches
   each row through the existing engine path (SP-PG-COPY-BULKAPPLY V1
   batching + NULL-fallback semantics apply unchanged — CSV is just
   a different payload codec at the dispatcher).
3. CSV-format `COPY ... TO STDOUT` emits a CSV record stream with the
   resolved delimiter/quote/null options, optionally prefixed with a
   HEADER record carrying the column names.
4. Quoted-field semantics per RFC 4180 + PG superset: embedded
   delimiter/quote/newline → quoted; doubled-quote escape (or custom
   ESCAPE char); HEADER consumed on input + emitted on output.
5. Record-oriented parser: a CSV record containing literal newlines
   inside quoted fields reassembles correctly across CopyData frame
   boundaries via the carry buffer.

**Out-of-scope (named, deferred — each is its own arc):**

- **SP-PG-COPY-CSV-FORCEQUOTE (V2)** — column-scoped `FORCE_QUOTE`,
  `FORCE_NOT_NULL`, `FORCE_NULL` options. Today rejected with a
  precise V2-pointing `0A000`.
- **SP-PG-COPY-CSV-ENCODING (V2)** — non-UTF-8 input/output
  encodings. Today rejected with a precise V2-pointing `0A000`.
- **SP-PG-COPY-CSV-HEADER-MATCH (V2)** — PG-15+ `HEADER MATCH`
  semantics (validate input header against table schema).

## Slice plan (mirrors design spec §7)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + `copy::csv` codec (`CsvOptions` + `parse_csv_record` + `encode_csv_record` + `validate_single_byte`) + `command::parse_with_options` extension + `dispatch.rs` format branch + `CopyInState::new_with_format` + KATs. +24 KATs. | **DONE** | `2d2b414` (T1 commit) |
| **T2** | Real psql 16 vulcan smoke + USAGE §9 expansion + smoke transcript. | **DONE** | (this commit) |
| **T3** | STATUS row + progress tracker → CLOSED + TaskList #358 ready. | **DONE** | (this commit — folded with T2) |

## T1 — what landed (2026-06-01, commit `2d2b414`)

**One commit, +1834 LoC across 6 files:**

### Design spec (`docs/superpowers/specs/2026-06-01-kesseldb-sppgcopycsv-design.md`, ~225 LoC):

- §1 Context — `pg_dump --csv` + `psql \copy ... CSV HEADER` +
  spreadsheet round-trip.
- §2 Scope — V1 in (5 items); V1 out (3 named V2 arcs).
- §2.3 Defaults table mapping PG SQL-COPY options to text vs CSV.
- §3 Module layout + `csv.rs` surface + `CopyFormat` enum + `command.rs`
  + dispatch extensions.
- §4 CSV codec edge case table (8 entries).
- §5 HEADER input/output semantics.
- §6 Error semantics extension (5 new rows on top of SP-PG-COPY §6).
- §7 T1..T3 task decomposition.
- §8 Acceptance criteria (8 items).
- §9 Self-review weak spots (4 items).

### Scaffold + wiring (`crates/kessel-pg-gateway/src/copy/`):

- **`csv.rs` (NEW)** — `CsvOptions` + `parse_csv_record` +
  `encode_csv_record` + `validate_single_byte`. Hand-rolled (no `csv`
  crate). **15 KATs** (parse: basic / empty-null / empty-quoted /
  delim-in-quoted / doubled-quote / newline-in-quoted / bare newline
  ends record / CRLF / partial-quoted returns None / custom delim /
  custom NULL marker / no trailing newline / encode: basic / comma /
  quote / newline / null default / empty string / custom null marker
  + round-trip property / field-count mismatch / header no count
  check / validate single byte).
- **`mod.rs`** — `CopyFormat` enum (`Text` / `Csv(CsvOptions)`); +
  `format` + `pending_header` fields on `CopyInState`; +
  `new_with_format` constructor.
- **`command.rs`** — `ParsedCopy::From/To` widened with `format`
  field; `parse_with_options` extension parses `FORMAT csv` +
  DELIMITER / QUOTE / ESCAPE / NULL / HEADER + FREEZE no-op +
  ENCODING UTF-8 only + FORCE_QUOTE/FORCE_NOT_NULL/FORCE_NULL rejection.
  +5 new KATs (csv default accepted / HEADER flag / custom options /
  invalid delimiter / FORCE_QUOTE rejected).
- **`dispatch.rs`** — `process_copy_data` splits into text vs CSV
  paths; CSV uses record-oriented parser via the carry buffer;
  `dispatch_copy_to` emits CSV payloads + HEADER row when CSV format
  requested; `reject_sqlstate` / `reject_message` extended for the
  new RejectReason variants. +5 dispatch KATs (CSV from with HEADER /
  doubled quote / empty unquoted = NULL / quoted newline carries /
  CSV to with embedded comma / CSV to HEADER).

### Test counts (release on vulcan + local debug):

| Surface | Before T1 | After T1 | Delta |
|---|---|---|---|
| `kessel-pg-gateway::copy::*` lib | 89 | 113 | +24 |
| `kessel-pg-gateway` lib total | 694 | 718 | +24 |

`#![forbid(unsafe_code)]` honored across all touched modules;
HTTP/1.1 + WS + binary + PG-wire-Simple + PG-wire-Extended + COPY
text + COPY CSV surfaces all green.

## T2 — real psql smoke on vulcan (2026-06-01)

**Setup**:
- Server: `kesseldb` built from commit `2d2b414` with
  `--features pg-gateway`.
- Listener: `127.0.0.1:5534` (PG-wire), `127.0.0.1:6534` (binary).
- Token: `admin`.
- Client: PostgreSQL 16 psql.

**Headline results** (full transcript:
`docs/superpowers/sppgcopycsv-t2-smoke-2026-06-01.txt`):

1. `CREATE TABLE csv_smoke (id BIGINT, name CHAR(64))` → `CREATE TABLE`.
2. `COPY csv_smoke FROM STDIN WITH (FORMAT csv, HEADER)` against
   3-row input with embedded comma (`"Alice, the brave"`) + doubled-
   quote escape (`"Bob ""the builder"""`) → `COPY 3`.
3. `SELECT * FROM csv_smoke` returns the three rows with embedded
   comma + literal `"` preserved.
4. `COPY csv_smoke TO STDOUT WITH (FORMAT csv, HEADER)` emits a CSV
   stream byte-equal to the input file (header row + 3 data rows).
5. Custom `DELIMITER ';' + NULL '<NA>'` round-trip: 2-row CSV with
   `<NA>` decoded as NULL on input + emitted as `<NA>` on output.

**Headline question — does `psql COPY CSV with quoted+escaped fields`
work end-to-end against the binary?** YES. Three independent KATs
verify quoted+escaped fields, custom options, NULL handling, and
round-trip byte-equality.

## Closure notes

- TaskList #358 ready for completion.
- HTTP/1.1 + WS + binary + PG-wire-Simple + PG-wire-Extended + COPY
  text + COPY CSV surfaces all green.
- `#![forbid(unsafe_code)]` honored across the new `copy::csv`
  submodule.
- Zero new external deps — CSV codec is hand-rolled.
- USAGE.md §9 expanded with a SP-PG-COPY-CSV subsection.
- STATUS.md Track A.2.1 row added.
