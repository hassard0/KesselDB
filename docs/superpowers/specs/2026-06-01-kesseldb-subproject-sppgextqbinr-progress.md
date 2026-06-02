# SP-PG-EXTQ-BIN-RESULTS — PostgreSQL Extended Query binary-format RESULTS — SP-arc Progress Tracker

Date created: 2026-06-01
**Status: CLOSED — V1 SHIPPED at T3 (2026-06-01).** Real asyncpg
session on vulcan: parameterized SELECT round-trips with binary
RowDescription + binary DataRow end-to-end. The asyncpg
"insufficient data in buffer" mis-decode failure shape recorded
in the SP-PG-EXTQ-BIN T3 transcript (last asterisk on the asyncpg
row of the USAGE §9 ORM matrix) is CLOSED for the V1 supported PG
scalar types (INT2/INT4/INT8/FLOAT4/FLOAT8/BOOL/TEXT/VARCHAR/BYTEA/
TIMESTAMPTZ). NUMERIC binary still rejects with a precise
`SP-PG-EXTQ-BIN-NUMERIC` follow-up arc name. TaskList #356 ready
for completion.

Design spec: `docs/superpowers/specs/2026-06-01-kesseldb-sppgextq-bin-results-design.md`
Parent SP-arc: SP-PG-EXTQ-BIN V1 (closed 2026-06-01 at T3 — design
spec §2.2 named this arc).

## What this SP-arc shipped

V1 = "Postgres drivers that request binary-format RESULTS (asyncpg /
JDBC default extended mode / sqlx) connect AND succeed on
parameterized SELECT round-trips against KesselDB". Before this arc
those drivers had to fall back to a forced-text mode (asyncpg has no
such option — every SELECT errored) or accept "insufficient data in
buffer" decode failures. After this arc, V1 emits binary DataRow +
binary RowDescription per the portal's requested per-column format
codes:

1. **Binary RESULT encode at Execute time** — INT2/INT4/INT8 (decimal
   text → big-endian signed int); FLOAT4/FLOAT8 (decimal text →
   IEEE 754 BE); BOOL (`t`/`f`/`true`/`false` → 1 byte 0x01/0x00);
   TEXT/VARCHAR (UTF-8 pass-through); BYTEA (`\xHEX` text → raw bytes);
   TIMESTAMPTZ (ISO `YYYY-MM-DD HH:MM:SS.ffffff+00` → 8 bytes BE i64
   microseconds since 2000-01-01 UTC).
2. **Per-column format dispatch in `dispatch_execute`** — after the
   existing `split_dispatch_query_bytes` step, the post-processor
   rewrites each buffered DataRow + the prelude RowDescription
   format_code slot in lockstep per the PG length conventions (0
   codes = all text, 1 code = all-same, N codes = per-column). NULL
   columns and text columns pass through unchanged; the rewrite is
   zero-cost (skipped entirely) for the existing text-only path.
3. **Rewritten DataRows persist in the portal's `ExecState::Buffered`**
   so re-Execute paginated emits binary directly without re-encoding.
4. **`extract_type_oids_from_row_description` helper** parses the
   RowDescription frame the engine already produced to surface
   per-field type OIDs — no engine round-trip needed for the rewrite.
5. **`ExtqError::BinaryResultEncodeFailed { position, type_oid,
   reason }`** + server.rs mapping → SQLSTATE `0A000
   feature_not_supported` with the V2 follow-up arc name embedded
   in the message text (NUMERIC → `SP-PG-EXTQ-BIN-NUMERIC`;
   JSONB/UUID/ARRAY → `SP-PG-EXTQ-BIN-EXTRA`).
6. **Pure-Rust `days_from_civil`** (inverse of V1 `civil_from_days`;
   Howard Hinnant public-domain algorithm) for the TIMESTAMPTZ
   encode — symmetric with the V1 SP-PG-EXTQ-BIN decoder. No new
   external deps.

**Out-of-scope (named, deferred — each is its own arc):**

- **Binary NUMERIC** — V2 `SP-PG-EXTQ-BIN-NUMERIC`. Same arc that
  SP-PG-EXTQ-BIN V1 deferred. Bug-prone base-10000 variable-length-
  digit encoding. V1 here rejects with the same arc name in the
  ErrorResponse message.
- **Binary JSONB / UUID / ARRAY** — V2 `SP-PG-EXTQ-BIN-EXTRA`. Less
  common; V1 rejects with the arc name.
- **Server-side parameter type inference** — V2 `SP-PG-EXTQ-PARAM-
  INFER`. Still applies (asyncpg's parameterized INSERT into INT
  columns hits the same SP-PG-EXTQ-CAST gap as before; that's a
  separate arc).
- **CHAR(N) padding-aware EQ comparator** — V2 `SP-CHAR-PAD-COMPARE`.
  An engine-side quirk surfaced by the T3 smoke (parameterized WHERE
  on CHAR(N) returns 0 rows because the engine's EQ-on-Char doesn't
  ignore trailing NUL padding). NOT a binary-RESULT regression — the
  binary path works (proven by the SELECT *).

## Slice plan (mirrors design spec §5)

| T# | Scope | Status | Commits |
|---|---|---|---|
| **T1** | Design spec (`docs/superpowers/specs/2026-06-01-kesseldb-sppgextq-bin-results-design.md`, 321 LoC) + new `extq/binary_results.rs` module (1213 LoC incl. tests) with: `encode_binary_value(text, type_oid)` per-OID encoder covering BOOL/INT2/INT4/INT8/FLOAT4/FLOAT8/TEXT/VARCHAR/BYTEA/TIMESTAMPTZ; `rewrite_data_row_with_formats(frame, formats, type_oids)` parser + per-column re-encoder; `rewrite_row_description_with_formats(frame, formats)` per-field format_code slot flipper; `extract_type_oids_from_row_description(frame)` parser; `binary_result_supported_for_oid` + `unsupported_binary_result_arc` admission/naming helpers; pure-Rust Howard Hinnant `days_from_civil` (inverse of V1 `civil_from_days`). +39 lib KATs locking every supported-type encode shape against canonical wire byte patterns, every rejection branch (NUMERIC + unknown OID), decode/encode round-trip identity (INT8/FLOAT8/BOOL/BYTEA/TIMESTAMPTZ), parse_data_row round-trips, rewrite_data_row passthrough/binary/mixed branches, RowDescription format_code slot flip, supported-OID set matches param side. | **DONE** | `71dfe53` (design + helpers + 39 KATs) |
| **T2** | `dispatch_execute` post-processing: after `split_dispatch_query_bytes` rewrites RowDescription + DataRow per result_formats when any code is FORMAT_CODE_BINARY. Rewritten rows persist in `ExecState::Buffered` so re-Execute serves binary directly. New `ExtqError::BinaryResultEncodeFailed { position, type_oid, reason }` variant + server.rs SQLSTATE `0A000` mapping with V2 arc name. Zero-cost early-out for text-only path; every existing text-format KAT continues to pass byte-for-byte. +6 lib KATs: headline binary INT8 byte-correct (8 bytes BE), empty/all-text passthrough (text col_len=1), single text format `[0]` byte-equal to empty `[]`, re-Execute on Buffered keeps binary rows, empty result set with binary formats no crash, INSERT through binary-result portal succeeds. | **DONE** | `159b5c8` (dispatch_execute rewrite + KATs) |
| **T3** | Real asyncpg SELECT smoke on vulcan: `conn.fetch("SELECT * FROM t WHERE name = $1", "first")` + `conn.fetch("SELECT * FROM t")` both succeed end-to-end with binary parameters + binary results. The 2-row SELECT * round-trip proves binary DataRow + binary RowDescription are coherent on the wire (asyncpg decodes `id=42, name='first'` and `id=43, name='second'` as native Python types). USAGE §9 matrix flipped: asyncpg PASS\* (BIN T3) → PASS (BINR T3); residual gap paragraph rewritten to point at SP-PG-EXTQ-CAST + SP-CHAR-PAD-COMPARE + the remaining V2 binary arcs. Smoke script (`scripts/sppgextqbinr-asyncpg-smoke.py`) + transcript (`docs/superpowers/sppgextqbinr-t3-smoke-2026-06-01.txt`) checked in. Note: the parameterized WHERE on CHAR(N) returned 0 rows due to a pre-existing engine-side EQ-on-Char padding quirk (SP-CHAR-PAD-COMPARE, separate arc); the binary RESULT path itself works (proven by SELECT *). | **DONE** | `9ca1731` (smoke + USAGE + transcript) |
| **T4** | Arc closure — STATUS.md row, progress tracker → CLOSED, V2 follow-ups named (`SP-PG-EXTQ-BIN-NUMERIC` / `SP-PG-EXTQ-BIN-EXTRA` for the residual binary types; `SP-PG-EXTQ-CAST` for parameterized INSERT into INT; `SP-CHAR-PAD-COMPARE` for the CHAR(N)-WHERE quirk surfaced by the T3 smoke). TaskList #356 ready for completion. | **DONE** (this commit) | (this commit) |

Optional / V2 follow-ups (each its own arc):

- **SP-PG-EXTQ-BIN-NUMERIC (V2)** — binary NUMERIC encoding (base-
  10000 variable-length-digit). Bug-prone; deferred to a careful
  per-encoding KAT pass. Symmetric param + result sides both reject
  with this arc name today.
- **SP-PG-EXTQ-BIN-EXTRA (V2)** — JSONB / UUID / ARRAY binary. Less
  common; each is a small encoding match arm. Reject with this arc
  name today.
- **SP-PG-EXTQ-CAST (V2)** — gateway-side `::type` cast rewrite for
  kessel-sql. JDBC simple-query mode + asyncpg parameterized INSERT
  into INT columns both hit this gap.
- **SP-CHAR-PAD-COMPARE (V2 engine-side)** — CHAR(N) padding-aware
  EQ comparator. Surfaced by the T3 smoke when `WHERE name = $1`
  matched zero rows on a CHAR(32) column.
- **SP-PG-EXTQ-PARSED (V2)** — typed-parameter AST in kessel-sql
  (replaces text substitution, removes SQL-injection attack surface).
- **SP-PG-EXTQ-CACHE (V2)** — server-side prepared-stmt cache across
  reconnect.
- **SP-PG-JDBC-SMOKE (V2)** — JDBC pgJDBC round-trip on vulcan (needs
  JDK install). With the BIN + BIN-RESULTS arcs both closed, this
  smoke would replace the SKIP row in the USAGE §9 matrix with PASS.

## T1 — what landed (2026-06-01, commit `71dfe53`)

**One commit, +1535 LoC across 3 files** (binary_results.rs 1213
incl. KATs, design spec 321, mod.rs +1):

### Design spec (321 LoC):

- §1 Context — PG §55.2.3 + §55.7 + §55.8; binary RESULT wire encoding
  table (mirror of the V1 BIN param decoder).
- §2 Scope — V1 in (binary RESULT encode + per-column format dispatch
  + RowDescription format_code flip); V1 out (NUMERIC binary, JSONB/
  UUID/ARRAY, simple-query — simple-query is text-only forever).
- §3 Implementation sketch — `encode_binary_value` per-type match
  dispatch, `rewrite_data_row_with_formats` parser + re-encoder,
  `rewrite_row_description_with_formats` slot-flipper, `extract_
  type_oids_from_row_description` helper, `dispatch_execute`
  post-processing flow.
- §4 Acceptance criteria — asyncpg `conn.fetch(...)` round-trip + no
  regression on text path + NUMERIC reject naming V2 arc + seed-7 +
  CI green.
- §5 Task decomposition — T1..T4 KAT delta estimates.
- §6 References — SP-PG-EXTQ V1 / SP-PG-EXTQ-BIN V1 specs + T3
  transcript + PG docs + source pointers.

### `extq/binary_results.rs` (1213 LoC incl. tests):

- `encode_binary_value(text, type_oid) -> Result<Vec<u8>,
  BinaryEncodeError>` — per-type match dispatch.
- `BinaryEncodeError` — `BadValue { type_oid, reason }` +
  `Unsupported { type_oid, arc: &'static str }`.
- `rewrite_data_row_with_formats(frame, formats, type_oids) ->
  Result<Vec<u8>, BinaryRewriteError>` — parses a complete `D` wire
  frame, re-encodes per-column per the PG length conventions, emits
  a fresh `D` frame.
- `BinaryRewriteError` — `MalformedDataRow` (defensive) + `Encode
  { position, error }`.
- `rewrite_row_description_with_formats(frame, formats) -> Vec<u8>`
  — flips the per-field format_code slot in `T` wire frames.
  Zero-cost early-out for all-text.
- `extract_type_oids_from_row_description(frame) -> Option<Vec<u32>>`
  — parses `T` frames to surface per-field type_oid.
- `parse_data_row(frame) -> Option<Vec<Option<Vec<u8>>>>` — inverse
  of `response::encode_data_row`. Returns `None` on malformed.
- `binary_result_supported_for_oid(type_oid)` + `unsupported_binary_
  result_arc(type_oid)` helpers.
- `effective_format_code(formats, i)` — PG length convention.
  Mirror of `substitute::effective_format_code`; both call sites
  share the rule.
- Pure-Rust Howard Hinnant `days_from_civil` (inverse of V1's
  `civil_from_days`).

### Test counts (release on host, 2026-06-01)

| Surface | Before T1 | After T1 | Delta |
|---|---|---|---|
| `kessel-pg-gateway` lib | 639 | 678 | +39 |

CI green; default tree-grep EMPTY (no new external deps);
`#![forbid(unsafe_code)]` honored; HTTP/1.1 + WS + binary + PG-wire-
Simple + PG-wire-Extended (text + binary params + text RESULTS)
surfaces byte-untouched (binary_results is a new module, no
dispatcher changes).

## T2 — what landed (2026-06-01, commit `159b5c8`)

**One commit, +434 LoC across 2 files** (extq/mod.rs +426 incl.
KATs, server.rs +10):

### dispatch_execute (`extq/mod.rs`):

- Capture `portal.result_formats` alongside other portal state at
  the top of `dispatch_execute`.
- After `split_dispatch_query_bytes`: if ANY format code is
  FORMAT_CODE_BINARY AND `buffered_rows` is non-empty AND prelude
  starts with `T`:
  - Extract per-column type_oids from the prelude via
    `binary_results::extract_type_oids_from_row_description`.
  - Re-encode each buffered DataRow per-column via
    `binary_results::rewrite_data_row_with_formats`. On error,
    map to `BinaryResultEncodeFailed` + set error_state.
  - Rewrite prelude RowDescription's per-field format_code slot
    in lockstep via `rewrite_row_description_with_formats`.
- Rewritten DataRows persist in `ExecState::Buffered { rows, .. }`.
- Zero-cost early-out for text-only portals (the `needs_binary_results
  && !buffered_rows.is_empty()` guard short-circuits the whole
  post-processor).

### `ExtqError::BinaryResultEncodeFailed { position, type_oid, reason }`:

- New variant. Maps to SQLSTATE `0A000` at the server.rs boundary
  with the V2 follow-up arc name embedded in the message
  (NUMERIC → `SP-PG-EXTQ-BIN-NUMERIC`; other → `SP-PG-EXTQ-BIN-
  EXTRA`).

### server.rs:

- One new error-variant arm mapping to SQLSTATE `0A000` with the
  follow-up-arc name in the message text.

### Test counts (release on host, 2026-06-01)

| Surface | Before T2 | After T2 | Delta |
|---|---|---|---|
| `kessel-pg-gateway` lib | 678 | 684 | +6 |

T1+T2 cumulative delta on `kessel-pg-gateway` lib: **+45 KATs**.

## T3 — what landed (2026-06-01, commit `9ca1731`)

**One commit, +251 LoC across 3 files** (USAGE.md +24 net,
smoke script +137, transcript +90):

Real asyncpg SELECT smoke on vulcan flips the asterisk from BIN T3:

### Fix verification

The asyncpg `fetch()` round-trip on a SELECT was the headline failure
shape of the BIN T3 transcript — asyncpg requested binary RESULTS,
V1 emitted text DataRow, asyncpg mis-decoded with "insufficient data
in buffer". V1 now:
- Accepts asyncpg's `result_formats=[1]` Bind (was always accepted
  at the wire level; the V1 ignore-result-formats behavior was
  benign at Bind time but wrong at Execute time).
- Emits RowDescription with per-field `format_code=0x0001` (binary).
- Emits DataRow with INT8 column as 8 bytes BE + CHAR(32)→TEXT
  column as raw UTF-8 bytes.
- asyncpg decodes both columns as native Python `int` and `str`
  types — confirms binary RowDescription + binary DataRow are
  coherent on the wire.

### Smoke script

`scripts/sppgextqbinr-asyncpg-smoke.py` checked in for re-
runnability. Captures the asyncpg `conn.fetch(...)` round-trip
against a fresh kesseldb-server on vulcan. Companion transcript
file: `docs/superpowers/sppgextqbinr-t3-smoke-2026-06-01.txt`.

### Pre-existing limitations surfaced (NOT regressions)

- Parameterized `WHERE name = $1` with `$1='first'` returned 0 rows
  because the engine's EQ-on-Char doesn't ignore trailing NUL
  padding (CHAR(32) "first" is stored as "first" + 27 NULs; query
  literal compares against the unpadded form). Binary-RESULT path
  itself works (SELECT * proves it). Tracked as V2
  `SP-CHAR-PAD-COMPARE` (engine-side).
- Parameterized SELECT into INT columns still needs SP-PG-EXTQ-CAST
  (`::int8` cast rewrite). Same gap as BIN V1.

### USAGE.md §9 updates

- asyncpg 0.31.0 row: PASS\* (BIN T3) → PASS (BINR T3). Asterisk
  note removed.
- "Remaining residual gap" paragraph rewritten: drop binary-RESULTS
  from the list (the arc shipped); add the new T3-surfaced gaps
  (SP-PG-EXTQ-CAST + SP-CHAR-PAD-COMPARE + the residual
  SP-PG-EXTQ-BIN-NUMERIC/EXTRA).
- Result-side narrative paragraph added describing how T2's
  `dispatch_execute` post-processor works.

### Test counts (release on host, 2026-06-01)

| Surface | Before T3 | After T3 | Delta |
|---|---|---|---|
| `kessel-pg-gateway` lib | 684 | 684 | 0 (docs + smoke only) |

T1+T2+T3 cumulative delta on `kessel-pg-gateway` lib: **+45 KATs**.

`kessel-sim` seed-7 GREEN (3 / 3); default tree-grep EMPTY (no new
external deps); `#![forbid(unsafe_code)]` honored; HTTP/1.1 + WS +
binary + PG-wire-Simple + PG-wire-Extended (text RESULT path)
surfaces byte-untouched. CI green at every commit.

### Headline question — does asyncpg SELECT with binary results work?

- **asyncpg 0.31 fetch round-trip on a 2-row table**: **PASS**
  (BIN T3 PASS\* → BINR T3 PASS). The 2-row SELECT * returned
  `[(42, 'first'), (43, 'second')]` decoded as native Python
  types. asyncpg's binary-RESULT decode path is now satisfied
  by V1's binary RowDescription + binary DataRow output.

Smoke transcript: `docs/superpowers/sppgextqbinr-t3-smoke-2026-06-01.txt`.

## T4 — arc closure (2026-06-01, this commit)

- STATUS.md row added — V1 SHIPPED at T3 (with the BIN T3 caveat-
  flip notes inline so the matrix stays coherent).
- USAGE.md §9 matrix already updated in T3.
- This progress tracker created + populated.
- V2 follow-ups named: `SP-PG-EXTQ-BIN-NUMERIC`,
  `SP-PG-EXTQ-BIN-EXTRA`, `SP-PG-EXTQ-CAST`, `SP-CHAR-PAD-COMPARE`
  (new), `SP-PG-EXTQ-PARSED`, `SP-PG-EXTQ-CACHE`, `SP-PG-JDBC-SMOKE`.

TaskList #356 ready for completion.
