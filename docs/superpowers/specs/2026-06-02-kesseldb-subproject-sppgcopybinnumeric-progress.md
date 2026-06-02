# SP-PG-COPY-BIN-NUMERIC — PostgreSQL `COPY ... WITH (FORMAT binary)` NUMERIC — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T3 (2026-06-02).** Real psql 16.14
binary COPY NUMERIC round-trip on vulcan: `CREATE TABLE num_bin (id I64,
amount I128)` + INSERT 4 rows (42, 100, 999999999, 0) + `COPY num_bin
TO STDOUT WITH (FORMAT binary) > /tmp/num-bin-export.bin` emits 135
bytes (canonical PGCOPY signature + 4 binary rows with `numeric_send`-
shape NUMERIC payloads + EOD `ff ff`); `COPY num_bin2 FROM STDIN WITH
(FORMAT binary)` returns `COPY 4` + SELECT shows the same row set;
re-export `md5sum` match (`18e15ae0e38be860d4b10a45412ff8eb`)
byte-equal to original. Negative-value sub-smoke (INSERT (5, -7) +
COPY TO + COPY FROM into a third table) preserves the negative
(sign=0x4000). The SP-PG-COPY-BIN-NUMERIC follow-up named in
SP-PG-COPY-BIN V1 + preserved through SP-PG-EXTQ-BIN-NUMERIC V1 is
now CLOSED for the V1 NUMERIC range (`|value| < 10^18`,
≤18 fractional digits). TaskList #370 ready for completion.

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgcopybinnumeric-design.md`
Smoke transcript: `docs/superpowers/sppgcopybinnumeric-t3-smoke-2026-06-02.txt`

Parent SP-arcs:
- SP-PG-COPY-BIN V1 (closed 2026-06-02 at T4 — `2026-06-02-kesseldb-subproject-sppgcopybin-progress.md`)
- SP-PG-EXTQ-BIN-NUMERIC V1 (closed 2026-06-02 at T5 — `2026-06-02-kesseldb-subproject-sppgextqbinnumeric-progress.md`)

## What this SP-arc shipped

V1 = "`pg_dump --format=custom` restore of a table with a NUMERIC
column + JDBC `CopyManager.copyIn(PGCopyOutputStream)` with BigDecimal
columns + `pgloader` binary-COPY fast path against a NUMERIC-bearing
schema now work against KesselDB." Before this arc the COPY-BIN
admission pre-rejected NUMERIC at COPY-start with
`0A000 SP-PG-COPY-BIN-NUMERIC`. After this arc:

1. **Drop the explicit NUMERIC pre-reject** in
   `crates/kessel-pg-gateway/src/copy/dispatch.rs`:
   - `dispatch_copy_in_start` — the explicit `oid == PG_TYPE_NUMERIC`
     arm that returned `Failed { 0A000 SP-PG-COPY-BIN-NUMERIC }` is
     removed; admission falls through to the standard
     `binary_format_supported_for_oid` consultation.
   - `dispatch_copy_to` — same shape, same removal.
2. **`binary_format_supported_for_oid` predicate** (in
   `extq::substitute`) already returns `true` for `PG_TYPE_NUMERIC`
   after SP-PG-EXTQ-BIN-NUMERIC T3 (2026-06-02). The COPY-BIN admission
   now lights up NUMERIC by leaning on the existing predicate.
3. **Per-row codec paths are unchanged**:
   - FROM: `process_copy_data_binary` calls `decode_binary_param(bytes,
     PG_TYPE_NUMERIC)` per field — already wired into
     `extq::binary_numeric::decode_numeric_binary` by
     SP-PG-EXTQ-BIN-NUMERIC T3.
   - TO: `dispatch_copy_to`'s binary branch calls
     `encode_binary_value(text, PG_TYPE_NUMERIC)` per column — already
     wired into `extq::binary_numeric::encode_numeric_binary` by the
     same arc.
4. **CopyFormat::Binary doc** (in `copy/mod.rs`) updated to reflect
   the new 11-type supported set including NUMERIC.

**Out-of-scope (named, deferred — each inherits from EXTQ-BIN-NUMERIC):**

- **`SP-PG-EXTQ-BIN-NUMERIC-BIGNUM`** (inherited) — arbitrary-precision
  NUMERIC. Wider values reject at the per-row codec layer.
- **`SP-PG-EXTQ-BIN-NUMERIC-NAN`** (inherited) — NaN binary.
- **`SP-PG-EXTQ-BIN-NUMERIC-INF`** (inherited) — ±Infinity binary
  (PG 14+).
- **`SP-PG-COPY-BIN-EXTRA`** (unchanged) — UUID / JSONB / ARRAY inside
  COPY frames.
- **`SP-PG-COPY-BIN-DIRECT`** (unchanged) — typed parameter binding to
  bypass the per-value binary→text→SQL round trip.

## Slice plan (mirrors design spec §5)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1+T2** | Design spec (`docs/superpowers/specs/2026-06-02-kesseldb-sppgcopybinnumeric-design.md`, ~240 LoC) — scope, V2 follow-up arc names, KAT estimate, codec-reuse stance. Dispatch wire-up: drop explicit NUMERIC pre-reject arms in `dispatch_copy_in_start` + `dispatch_copy_to`; update `CopyFormat::Binary` doc. +7 `t1num_*` KATs in `copy::dispatch::tests`. | **DONE** | `0e52104` |
| **T3** | Real psql 16.14 binary COPY NUMERIC round-trip smoke on vulcan: CREATE TABLE + INSERT seed + COPY TO STDOUT binary > file + COPY FROM STDIN binary into a fresh table → `COPY 4` + SELECT preserves the rows + re-export byte-equal (md5sum match) + negative-value sub-smoke. USAGE.md §9 SP-PG-COPY-BIN subsection updated: drop the NUMERIC-pre-reject caveat; replace example error with UUID/JSONB example; strike SP-PG-COPY-BIN-NUMERIC in V2 follow-up list; update SP-PG-EXTQ-BIN-NUMERIC paragraph's COPY remark. Smoke transcript checked in. | **DONE** | `97a613c` |
| **T4** | Arc closure — STATUS.md row + progress tracker → CLOSED + TaskList #370 ready. | **DONE** (this commit) | (final docs commit) |

## T1+T2 — what landed (2026-06-02, commit `0e52104`)

**One commit, +515 / -33 LoC across 3 files** (design spec ~240,
dispatch.rs +245, copy/mod.rs ±~5).

### Design spec (`docs/superpowers/specs/2026-06-02-kesseldb-sppgcopybinnumeric-design.md`, ~240 LoC):

- §1 Context — three concrete workflows (pg_dump --format=custom with
  NUMERIC column, JDBC CopyManager.copyIn binary stream with BigDecimal,
  pgloader binary-COPY fast path with NUMERIC) + ecosystem table.
- §2 Scope — V1 in (drop NUMERIC pre-reject, lean on existing
  predicate + already-wired per-row codec, ~5-10 KATs at dispatch
  boundary); V1 out (4 named inherited V2 arcs).
- §3 Implementation sketch — exact code edits (FROM admission +
  TO admission), per-value path no-change explanation.
- §4 Acceptance criteria — 5 items including psql round-trip, no
  regression, NUMERIC-out-of-range still rejects, UUID still rejects,
  CI green.
- §5 Task decomposition — T1..T4 with KAT-delta estimates.
- §6 References — SP-PG-COPY-BIN V1 spec, SP-PG-EXTQ-BIN-NUMERIC V1
  spec, PG §55.2.7, `numeric_send`/`numeric_recv`, the admission +
  codec call sites.

### Code (`crates/kessel-pg-gateway/src/copy/dispatch.rs`):

- `dispatch_copy_in_start` — drop the `if oid == PG_TYPE_NUMERIC` arm
  that fired BEFORE `binary_format_supported_for_oid`. Update the
  surrounding comment to reflect closure.
- `dispatch_copy_to` — same shape, same removal.
- 7 new KATs at the end of `mod tests`:
  - `t1num_encode_binary_value_numeric_42_byte_equal_to_codec` — the
    per-column TO encoder's output for NUMERIC `"42"` byte-equals
    `extq::binary_numeric::encode_numeric_binary("42")`.
  - `t1num_decode_binary_param_numeric_42_round_trips_to_string` —
    the per-column FROM decoder's output for the canonical NUMERIC `42`
    wire is the literal `"42"`.
  - `t1num_dispatch_copy_in_start_binary_numeric_column_admitted` —
    `dispatch_copy_in_start` on a 2-column table with an `I128`
    column + FORMAT binary returns `Started { … }` (was
    `Failed { 0A000 SP-PG-COPY-BIN-NUMERIC }` pre-arc).
  - `t1num_dispatch_copy_to_binary_numeric_column_admitted` — same
    shape on the TO path emits `H` (CopyOutResponse) not `E`.
  - `t1num_copy_to_binary_numeric_column_emits_canonical_bytes` — a
    single-row table with NUMERIC=42 emits CopyData carrying the
    canonical `numeric_send` payload bytes verbatim.
  - `t1num_copy_from_binary_numeric_column_ingests_row` — a synthesized
    binary CopyData frame with one NUMERIC=42 row ingests through
    `process_copy_data_binary` with the synthesized INSERT carrying
    `VALUES (7, 42)` (bare decimal, no quotes).
  - `t1num_round_trip_encode_then_decode_through_dispatch_codecs` —
    6-value round-trip identity (`"0", "42", "1.5", "-3.14",
    "12345.6789", "0.0001"`) through `encode_binary_value` +
    `decode_binary_param`.

### `copy/mod.rs`:

- `CopyFormat::Binary` doc updated — drop the NUMERIC-V2 caveat;
  acknowledge the SP-PG-COPY-BIN-NUMERIC V1 closure.

### Test counts (host vulcan, 2026-06-02 after T1+T2):

| Surface | Before T1+T2 | After T1+T2 | Delta |
|---|---|---|---|
| `kessel-pg-gateway::copy::dispatch::tests` | 32 | 39 | +7 |
| `kessel-pg-gateway` lib | 822 | 829 | +7 |

`#![forbid(unsafe_code)]` honored across the edited modules. Zero new
external deps. CI green at this commit.

## T3 — real psql binary COPY NUMERIC smoke on vulcan (2026-06-02, commit `97a613c`)

**Setup**:
- Server: `kesseldb` built from commit `0e52104` with
  `--features pg-gateway` on vulcan
  (`CARGO_TARGET_DIR=/tmp/kdb-target-copybnum`, 24.85s compile).
- Listener: `127.0.0.1:5532` (PG-wire), `127.0.0.1:6532` (binary).
- Token: `admin`.
- Client: PostgreSQL 16.14 psql.

**Headline results** (full transcript:
`docs/superpowers/sppgcopybinnumeric-t3-smoke-2026-06-02.txt`):

1. **CREATE TABLE** `num_bin (id I64, amount I128)` →
   `CREATE TABLE`. (`I128` maps to `PG_TYPE_NUMERIC = 1700`.)
2. **INSERT (1, 42), (2, 100), (3, 999999999), (4, 0)** →
   `INSERT 0 4`. `SELECT *` shows the seeded rows.
3. **COPY num_bin TO STDOUT WITH (FORMAT binary) > /tmp/num-bin-export.bin**
   → 135 bytes with canonical `PGCOPY\n\xff\r\n\0` signature
   (verified via `hexdump -C`), 4 binary rows carrying the canonical
   `numeric_send`-shape NUMERIC payloads (e.g.
   `00 01 00 00 00 00 00 00 00 2a` for 42; `00 03 00 02 00 00 00 00
   00 09 27 0f 27 0f` for 999999999 = ndigits=3, weight=2,
   digits=[9, 9999, 9999]), + EOD marker `\xff\xff`.
4. **COPY num_bin2 FROM STDIN WITH (FORMAT binary) < /tmp/num-bin-export.bin**
   (into fresh table) → `COPY 4`. `SELECT * FROM num_bin2 ORDER BY id`
   returns the same row set.
5. **Re-export** `COPY num_bin2 TO STDOUT WITH (FORMAT binary)` →
   `diff -q` byte-equal vs original (`md5sum` match
   `18e15ae0e38be860d4b10a45412ff8eb`).
6. **Negative-value sub-smoke** — INSERT (5, -7) + COPY TO + COPY FROM
   into a third table preserves the negative (sign=0x4000) value.

**Headline question — does psql binary COPY NUMERIC round-trip work?**
YES. The full round-trip (export → import → re-export) is byte-equal
across the wire for the 4-row positive table, and the negative-value
sub-smoke succeeds end-to-end. Every layer is consistent (binary
admission → BinaryDecoder framing → per-row `decode_numeric_binary`
→ INSERT synthesis bare-decimal rendering → engine I128 storage →
SELECT path numeric text emission → per-row
`encode_numeric_binary` → binary CopyData framing).

### USAGE.md §9 updates

- SP-PG-COPY-BIN subsection: drop the NUMERIC-pre-reject caveat;
  expand "10 column types" to "11 column types" (BOOL, INT2/4/8,
  FLOAT4/8, TEXT/VARCHAR, BYTEA, TIMESTAMPTZ, **NUMERIC**); replace
  the example error block with the still-rejecting
  SP-PG-COPY-BIN-EXTRA UUID/JSONB example; strike
  SP-PG-COPY-BIN-NUMERIC in the V2 follow-up list (now CLOSED); add
  the smoke transcript link.
- SP-PG-EXTQ-BIN-NUMERIC subsection: replace "COPY binary's NUMERIC
  pre-reject remains independent (`SP-PG-COPY-BIN-NUMERIC` is its
  own follow-up)" with the closure pointer to this arc + its smoke
  transcript.

### Test counts (host vulcan, 2026-06-02 after T3)

| Surface | Before T3 | After T3 | Delta |
|---|---|---|---|
| `kessel-pg-gateway` lib | 829 | 829 | 0 (docs only) |

T1+T2+T3 cumulative delta on `kessel-pg-gateway` lib: **+7 KATs**.

## V2 follow-ups (each its own arc — listed here for the SP-PG-COPY
parent arc's running follow-up index)

- **`SP-PG-EXTQ-BIN-NUMERIC-BIGNUM`** (inherited) — arbitrary-precision
  NUMERIC. PG NUMERIC is essentially unbounded (up to 131072 digits
  before the decimal point + 16383 after). V1 covers the common ORM
  range (`|value| < 10^18`, ≤18 fractional digits) which fits in
  `i128` accumulators. The bignum arc needs an arbitrary-precision
  integer type (or a bignum dep).
- **`SP-PG-EXTQ-BIN-NUMERIC-NAN`** (inherited) — NaN binary support.
- **`SP-PG-EXTQ-BIN-NUMERIC-INF`** (inherited) — `+Infinity` /
  `-Infinity` binary support (PG 14+).
- **`SP-PG-COPY-BIN-EXTRA`** (unchanged) — binary UUID / JSONB /
  ARRAY inside COPY frames.
- **`SP-PG-COPY-BIN-DIRECT`** (unchanged) — typed parameter binding
  to bypass the per-value binary→text→SQL round trip.

## Closure notes

- TaskList #370 ready for completion.
- HTTP/1.1 + WS + binary + PG-wire-Simple + PG-wire-Extended +
  PG-wire-COPY-text + PG-wire-COPY-CSV + PG-wire-COPY-BIN surfaces
  byte-untouched (NUMERIC was V1-Unsupported on COPY-BIN, so the new
  path is strictly additive).
- `#![forbid(unsafe_code)]` honored across the edited modules.
- Zero new external deps — no new codec lands; this arc is dispatch
  wire-up only.
- USAGE.md §9 — SP-PG-COPY-BIN subsection updated to reflect the
  closure (NUMERIC dropped from the V2 follow-up list; smoke
  transcript link added).
- STATUS.md current-capabilities row added (Track A.-1.5).
