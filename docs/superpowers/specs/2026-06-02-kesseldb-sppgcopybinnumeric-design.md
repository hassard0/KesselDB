# SP-PG-COPY-BIN-NUMERIC — PostgreSQL `COPY ... WITH (FORMAT binary)` NUMERIC — DESIGN

**Status:** design — scopes the V2 follow-up named in
`docs/superpowers/specs/2026-06-02-kesseldb-sppgcopybin-design.md` §2.2
(COPY-BIN V1 CLOSED at T3 on 2026-06-02) AND deliberately preserved as
the independent follow-up by `SP-PG-EXTQ-BIN-NUMERIC` V1
(`docs/superpowers/specs/2026-06-02-kesseldb-sppgextqbinnumeric-design.md`
— CLOSED at T5 on 2026-06-02). Mirrored progress tracker:
`docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgcopybinnumeric-progress.md`.

SP-PG-COPY-BIN V1 closed binary-format COPY for the 10 common PG scalar
types — BOOL, INT2/4/8, FLOAT4/8, TEXT/VARCHAR, BYTEA, TIMESTAMPTZ —
but explicitly pre-rejected NUMERIC at COPY-start with `0A000` +
`SP-PG-COPY-BIN-NUMERIC` because the per-row framing has independent
recovery semantics from extended-query Bind/Execute. SP-PG-EXTQ-BIN-
NUMERIC V1 shipped the pure-Rust NUMERIC codec
(`crates/kessel-pg-gateway/src/extq/binary_numeric.rs`) and wired it
into the Bind decode + Execute result encode paths, but deliberately
left COPY-BIN's pre-reject intact so this arc could land cleanly.

This arc removes the explicit COPY-BIN NUMERIC pre-reject and routes
COPY binary FROM/TO through the existing
`extq::binary_numeric::{encode_numeric_binary, decode_numeric_binary}`
codec — the SAME codec the SP-PG-EXTQ-BIN-NUMERIC arc shipped. Net
code delta is small: drop the explicit `oid == PG_TYPE_NUMERIC` arm
in `copy/dispatch.rs::dispatch_copy_in_start` + the matching arm in
`dispatch_copy_to`, then lean on the existing
`binary_format_supported_for_oid` / `binary_result_supported_for_oid`
predicates (which already include NUMERIC after SP-PG-EXTQ-BIN-NUMERIC
T3). The per-value encode/decode call sites in `process_copy_data_binary`
+ `dispatch_copy_to`'s binary branch already route through
`decode_binary_param` + `encode_binary_value` — both of which now
delegate to `binary_numeric::*`. No new codec lands in this arc.

**Builds on:**

- **SP-PG-COPY-BIN V1** (`docs/superpowers/specs/2026-06-02-kesseldb-sppgcopybin-design.md`)
  — the framing layer (`copy::binary` — signature, header, per-row
  length-prefixed fields, EOD marker) + per-row encode/decode pipeline
  through `extq::binary_results::encode_binary_value` (TO) and
  `extq::substitute::decode_binary_param` (FROM).
- **SP-PG-EXTQ-BIN-NUMERIC V1** (`docs/superpowers/specs/2026-06-02-kesseldb-sppgextqbinnumeric-design.md`)
  — the pure-Rust NUMERIC binary codec
  (`encode_numeric_binary` + `decode_numeric_binary` + `BinaryNumericError`
  in `extq/binary_numeric.rs`). The codec is engine-free / stateless /
  `#![forbid(unsafe_code)]` and uses an `i128` accumulator (no bignum
  dep). V1 range: `|value| < 10^18` with up to 18 fractional digits.
- **`crates/kessel-pg-gateway/src/proto.rs`** — the PG type OID
  catalog (`PG_TYPE_NUMERIC: u32 = 1700`).

---

## 1. Context — why COPY-BIN-NUMERIC closes a real gap

After SP-PG-COPY-BIN V1 + SP-PG-EXTQ-BIN-NUMERIC V1, the only remaining
NUMERIC gap in KesselDB's PG surface is `COPY t TO/FROM STDOUT/STDIN
WITH (FORMAT binary)` against a table whose schema contains a NUMERIC /
`I128` / `U128` / `Fixed` column. Three concrete workflows trip on it:

1. **`pg_dump --format=custom`** of a table containing a NUMERIC column
   — `pg_restore` uses COPY binary by default, and emits NUMERIC values
   inline with the rest of the row. SP-PG-COPY-BIN V1 today refuses
   the dump at COPY-start.
2. **JDBC `CopyManager.copyIn(stream, ...)` + binary-format stream**
   from a financial-data Spring Boot service — NUMERIC currency
   columns are the most common bulk-load shape in fintech adoption
   workloads.
3. **`pgloader`'s binary-COPY fast path** copying a Postgres source
   table with NUMERIC columns into KesselDB — pgloader probes the
   schema, chooses binary, and bails on the V1 pre-reject.

After this arc, all three workflows succeed end-to-end through the
SAME pure-Rust NUMERIC codec the Bind/Execute path uses.

| Workflow | Today (post-COPY-BIN V1) | After SP-PG-COPY-BIN-NUMERIC V1 |
|---|---|---|
| `pg_dump --format=custom` w/ NUMERIC column | FAIL — `0A000 SP-PG-COPY-BIN-NUMERIC` at COPY-start | works |
| JDBC `CopyManager.copyIn(binary stream)` w/ BigDecimal column | FAIL — same | works |
| `pgloader` binary-COPY fast path w/ NUMERIC | FAIL — same | works |
| `psql \copy t TO STDOUT WITH (FORMAT binary)` against `(id BIGINT, amount I128)` | FAIL — same | works |

The headline acceptance test for V1: a `psql` round-trip via
`COPY t TO STDOUT WITH (FORMAT binary) > out.bin` followed by
`COPY t2 FROM STDIN WITH (FORMAT binary) < out.bin` against a table
with a NUMERIC column preserves the NUMERIC values byte-equal (modulo
canonical PG `numeric_send` normalization — leading-zero base-10000
digit stripping).

## 2. Scope

### 2.1 V1 — what's in (this arc, T1..T4)

1. **Drop the explicit NUMERIC pre-reject** in `copy/dispatch.rs`:
   - `dispatch_copy_in_start` — remove the
     `if oid == crate::proto::PG_TYPE_NUMERIC { … return Failed }`
     arm that fires BEFORE `binary_format_supported_for_oid`.
   - `dispatch_copy_to` — remove the matching arm.
   - `binary_format_supported_for_oid` and `binary_result_supported_for_oid`
     already include `PG_TYPE_NUMERIC` after SP-PG-EXTQ-BIN-NUMERIC T3,
     so leaving the standard supported-OID consultation in place
     admits NUMERIC.
2. **Per-value codec wiring** — the FROM path already calls
   `decode_binary_param(bytes, oid)` per column, which dispatches on
   `PG_TYPE_NUMERIC` into `extq::binary_numeric::decode_numeric_binary`.
   The TO path already calls `encode_binary_value(text, oid)` per
   column, which dispatches into `extq::binary_numeric::encode_numeric_binary`.
   NO new call sites land in this arc.
3. **KAT corpus** in `copy::binary::tests` (~5-10 KATs):
   - Encode NUMERIC value `"42"` through `encode_binary_value` →
     byte-equal to `extq::binary_numeric::encode_numeric_binary("42")`.
   - Decode the same NUMERIC binary bytes through `decode_binary_param`
     → byte-equal `"42"` string.
   - COPY FROM binary with a NUMERIC column accepts rows (no pre-reject).
   - COPY TO binary with a NUMERIC column emits canonical PG binary.
   - Round-trip: COPY TO binary then COPY FROM binary preserves NUMERIC
     values for a 3-row table `(id BIGINT, amount I128)`.
4. **Real psql binary COPY NUMERIC round-trip smoke** on vulcan
   (T3) — `CREATE TABLE … (id BIGINT, amount I128)` + INSERT seed +
   COPY TO STDOUT binary → file + COPY FROM STDIN binary into fresh
   table + SELECT preserves the rows. Companion transcript checked
   in at `docs/superpowers/sppgcopybinnumeric-t3-smoke-2026-06-02.txt`.
5. **USAGE.md §9** drops the NUMERIC caveat from the SP-PG-COPY-BIN
   subsection (the `COPY binary: column "amount" type OID 1700 not
   supported in V1` block becomes `COPY binary now supports NUMERIC
   for the V1 SP-PG-EXTQ-BIN-NUMERIC range`).
6. **STATUS.md** gets a Track A.-1.5 row (V1 SHIPPED).
7. **Progress tracker** → CLOSED; TaskList #370 ready.

### 2.2 V1 — what's out (named V2+ follow-ups — each is its own arc)

- **`SP-PG-EXTQ-BIN-NUMERIC-BIGNUM`** (preserved) — arbitrary-precision
  NUMERIC. COPY-BIN inherits the V1 codec's `|value| < 10^18` +
  ≤18-fractional-digit cap. Values outside the cap reject at the
  per-row encoder/decoder layer with the existing arc name.
- **`SP-PG-EXTQ-BIN-NUMERIC-NAN`** (preserved) — NaN binary. COPY-BIN
  inherits the same rejection at the per-row codec layer.
- **`SP-PG-EXTQ-BIN-NUMERIC-INF`** (preserved) — ±Infinity binary
  (PG 14+). Same inheritance.
- **`SP-PG-COPY-BIN-EXTRA`** (unchanged) — binary UUID / JSONB / ARRAY
  inside COPY frames. Same independence as SP-PG-EXTQ-BIN-EXTRA
  vs SP-PG-EXTQ-BIN.
- **`SP-PG-COPY-BIN-DIRECT`** (unchanged) — typed parameter binding
  to bypass the per-value binary→text→SQL round trip.

## 3. Implementation sketch

### 3.1 `copy/dispatch.rs::dispatch_copy_in_start` (FROM admission)

Drop the explicit NUMERIC arm:

```rust
if format.is_binary() {
    for (name, kind) in chosen_columns.iter().zip(chosen_kinds.iter()) {
        let oid = field_kind_to_oid(*kind);
        // SP-PG-COPY-BIN-NUMERIC V1 — NUMERIC now supported via the
        // SP-PG-EXTQ-BIN-NUMERIC codec (binary_format_supported_for_oid
        // includes PG_TYPE_NUMERIC after T3 of that arc). Remove the
        // explicit pre-reject and lean on the standard supported-OID
        // consultation.
        if !binary_format_supported_for_oid(oid) {
            return CopyInStartOutcome::Failed {
                bytes: error_response_then_rfq(
                    "0A000",
                    &format!(
                        "COPY binary: column \"{name}\" type OID {oid} not supported in V1 (SP-PG-COPY-BIN-EXTRA)"
                    ),
                ),
            };
        }
    }
}
```

### 3.2 `copy/dispatch.rs::dispatch_copy_to` (TO admission)

Same shape — drop the explicit NUMERIC arm:

```rust
if format.is_binary() {
    for (i, &idx) in chosen_indices.iter().enumerate() {
        let kind = schema_cols[idx].kind;
        let oid = field_kind_to_oid(kind);
        // SP-PG-COPY-BIN-NUMERIC V1 — NUMERIC now supported via the
        // SP-PG-EXTQ-BIN-NUMERIC codec.
        if !binary_format_supported_for_oid(oid) {
            return error_response_then_rfq(
                "0A000",
                &format!(
                    "COPY binary: column \"{}\" type OID {oid} not supported in V1 (SP-PG-COPY-BIN-EXTRA)",
                    chosen_names[i]
                ),
            );
        }
    }
}
```

### 3.3 Per-value paths — no new code

- FROM path: `process_copy_data_binary` already calls
  `decode_binary_param(bytes, oid)` per field. For
  `oid == PG_TYPE_NUMERIC` this dispatches into
  `extq::binary_numeric::decode_numeric_binary(bytes)`, returning the
  decimal-string literal which flows through the existing INSERT
  synthesis path (the SQL synthesizer's "bare token" branch picks the
  bare decimal literal for the `I128` / `Fixed` column kinds).
- TO path: `dispatch_copy_to` binary branch already calls
  `encode_binary_value(text, oid)` per column. For
  `oid == PG_TYPE_NUMERIC` this dispatches into
  `extq::binary_numeric::encode_numeric_binary(text)`, returning the
  PG NUMERIC binary wire bytes which the existing
  `encode_binary_row` framing wraps into a CopyData payload.

### 3.4 KAT corpus (T2, ~5-10 KATs)

Per-call-site integration KATs in `copy::binary::tests` (or in
`copy::dispatch::tests` — closer to the call site):

- `t1num_encode_binary_row_numeric_42_byte_equal_to_extq_codec` — the
  per-row encoder's NUMERIC payload bytes are byte-equal to the
  output of `extq::binary_numeric::encode_numeric_binary("42")`.
- `t1num_decode_binary_field_numeric_42_round_trips_to_string` — the
  per-row decoder's NUMERIC bytes decode through `decode_binary_param`
  into the literal `"42"`.
- `t1num_dispatch_copy_in_start_binary_numeric_column_admitted` —
  `dispatch_copy_in_start` on a table whose schema includes an
  `I128` column with FORMAT binary returns `Started { … }` (was
  `Failed { … 0A000 SP-PG-COPY-BIN-NUMERIC }` pre-arc).
- `t1num_dispatch_copy_to_binary_numeric_column_admitted` —
  `dispatch_copy_to` on the same shape emits a binary CopyOutResponse
  (the binary `H` frame) instead of `ErrorResponse 0A000`.
- `t1num_copy_to_binary_numeric_column_emits_canonical_bytes` — the
  emitted CopyData for a row containing NUMERIC value `42` carries
  the exact `numeric_send` wire bytes the codec produces.
- `t1num_copy_round_trip_binary_numeric_three_rows` — synthesize a
  binary CopyData with 3 NUMERIC rows (42, -3.14, 12345.6789),
  process_copy_data_binary parses them, the INSERT synthesizer emits
  the right SQL, and the engine sees the right bare-decimal values.
- `t1num_dispatch_copy_in_start_binary_uuid_still_rejects_with_extra_arc`
  — invariant: removing the NUMERIC pre-reject doesn't accidentally
  admit UUID / JSONB / ARRAY (which are still pre-rejected via
  `binary_format_supported_for_oid` returning false).

## 4. Acceptance criteria

V1 (T1..T4) ships when:

1. **psql round-trip on vulcan with NUMERIC column** succeeds: COPY
   TO binary then COPY FROM binary into a fresh table preserves the
   rows.
2. **No regression on existing COPY-BIN KATs** — every SP-PG-COPY-BIN
   V1 KAT continues to pass byte-for-byte.
3. **NUMERIC out-of-range / NaN** reject at the per-row codec layer
   with the inherited `SP-PG-EXTQ-BIN-NUMERIC-{BIGNUM,NAN,INF}` arc
   names (codec error is rendered by the existing
   `binary_decode_message` / `BinaryEncodeError` mapper).
4. **UUID / JSONB / ARRAY columns still pre-reject** at COPY-start
   with the unchanged `SP-PG-COPY-BIN-EXTRA` arc name. (The NUMERIC
   removal doesn't accidentally widen the supported-OID set.)
5. **seed-7 GREEN**, default tree-grep EMPTY, CI green at every
   commit on this arc.

## 5. Task decomposition (T1..T4)

| T# | Scope | KAT delta |
|---|---|---|
| **T1** | This design spec. | +0 |
| **T2** | Drop the explicit NUMERIC pre-reject in `dispatch_copy_in_start` + `dispatch_copy_to`. Add the integration KAT corpus (~5-10 KATs) covering the per-call-site happy path + invariants (UUID still rejects). | +5-10 |
| **T3** | Real psql binary COPY NUMERIC round-trip smoke on vulcan; USAGE.md §9 caveat-drop. Smoke transcript checked in. | +0 |
| **T4** | STATUS.md row + progress tracker → CLOSED. TaskList #370 ready. | +0 |

Estimated V1 total: **~5-10 KATs across 4 slices**.

## 6. References

- SP-PG-COPY-BIN V1 spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgcopybin-design.md`
- SP-PG-COPY-BIN V1 progress (CLOSED): `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgcopybin-progress.md`
- SP-PG-EXTQ-BIN-NUMERIC V1 spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqbinnumeric-design.md`
- SP-PG-EXTQ-BIN-NUMERIC V1 progress (CLOSED): `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgextqbinnumeric-progress.md`
- PostgreSQL Documentation §55.2.7 — COPY binary format
- PostgreSQL source `src/backend/utils/adt/numeric.c::numeric_recv` /
  `numeric_send` — the canonical encoder/decoder this arc inherits
  via SP-PG-EXTQ-BIN-NUMERIC.
- `crates/kessel-pg-gateway/src/copy/dispatch.rs::dispatch_copy_in_start`
  + `::dispatch_copy_to` — the admission sites this arc edits.
- `crates/kessel-pg-gateway/src/extq/binary_numeric.rs` — the codec
  the per-value paths route through unchanged.
