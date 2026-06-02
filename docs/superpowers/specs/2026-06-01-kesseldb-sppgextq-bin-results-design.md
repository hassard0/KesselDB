# SP-PG-EXTQ-BIN-RESULTS — PostgreSQL Extended Query binary-format RESULTS — DESIGN

**Status:** design — scopes the V2 follow-up named in
`docs/superpowers/specs/2026-06-01-kesseldb-sppgextq-bin-design.md` §2.2
and the progress-tracker closure note in
`docs/superpowers/specs/2026-06-01-kesseldb-subproject-sppgextqbin-progress.md`
(SP-PG-EXTQ-BIN V1 CLOSED at T3, 2026-06-01).

SP-PG-EXTQ-BIN V1 lifted the binary-format PARAMETER rejection — asyncpg
+ psycopg3 default cursor + JDBC can now send binary-format Bind
parameters and Execute succeeds. The remaining gap is the SYMMETRIC
result-side encoding: clients that request binary-format RESULTS still
get text DataRows from V1 and mis-decode them ("insufficient data in
buffer" — exactly the asyncpg failure shape recorded in the T3 transcript
at `docs/superpowers/sppgextqbin-t3-smoke-2026-06-01.txt`).

This arc lifts the result-side restriction for the same V1 supported
scalar types: INT2/INT4/INT8, FLOAT4/FLOAT8, BOOL, TEXT/VARCHAR, BYTEA,
TIMESTAMPTZ. NUMERIC binary is the same base-10000 variable-length-digit
encoding deferred to `SP-PG-EXTQ-BIN-NUMERIC`.

**Builds on:**

- **SP-PG-EXTQ V1** (`docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`)
  — the Parse / Bind / Describe / Execute dispatch path, `extq::Portal.result_formats`
  (already populated per-position by T3 — V1 just ignored it).
- **SP-PG-EXTQ-BIN V1** (`docs/superpowers/specs/2026-06-01-kesseldb-sppgextq-bin-design.md`)
  — the binary parameter decoder + per-position format dispatch +
  type OID admission helper (`binary_format_supported_for_oid`). The
  RESULTS path mirrors the PARAMS path: per-position format codes
  from the portal, supported OIDs are encoded, unsupported reject
  with a precise V2 arc name in the message.
- **`crates/kessel-pg-gateway/src/response.rs`** — the existing
  text-DataRow encoder (`encode_data_row`) stays for simple-query +
  Execute-with-all-text-results. The new binary encoder shares the
  same wire-frame envelope (`D [length:4] [col_count:2] [col_length:4
  | col_bytes]*`) — only the per-column bytes differ.
- **`crates/kessel-pg-gateway/src/dispatch.rs`** — `dispatch_query`
  still produces text DataRows internally. The Extended Query path
  POST-PROCESSES the DataRow frames after splitting `dispatch_query`'s
  output, re-encoding per-column when the portal's `result_formats`
  requires binary. Simple-query (FE_QUERY 'Q' tag) is byte-untouched.

---

## 1. Context — what binary-format RESULTS look like

PG §55.2.3 (extended query) + §55.8 (binary representations):

`Bind` carries a `result_formats: Vec<u16>` array with the same PG
length conventions as `param_formats`:
- 0 codes  = all columns text
- 1 code   = every column the same (so `[1]` means ALL binary)
- N codes  = per-column

For each column in DataRow, the bytes are encoded per the chosen
format. asyncpg + JDBC default mode send `result_formats=[1]` — every
column binary. psycopg3 default cursor sends `result_formats=[]` (all
text) so V1 already worked for it (T3 PASS).

The binary wire encoding mirrors the V1 param decoder (`decode_binary_param`):

| PG type | OID | Bytes | Binary encoding |
|---|---|---|---|
| BOOL | 16 | 1 | `0x00` = false, `0x01` = true |
| BYTEA | 17 | var | raw bytes (column length comes from per-column length field) |
| INT2 | 21 | 2 | big-endian i16 |
| INT4 | 23 | 4 | big-endian i32 |
| INT8 | 20 | 8 | big-endian i64 |
| TEXT | 25 | var | UTF-8 bytes |
| VARCHAR | 1043 | var | UTF-8 bytes |
| FLOAT4 | 700 | 4 | IEEE 754 single, big-endian |
| FLOAT8 | 701 | 8 | IEEE 754 double, big-endian |
| TIMESTAMPTZ | 1184 | 8 | i64 microseconds since 2000-01-01 00:00:00 UTC |
| NUMERIC | 1700 | var | `ndigits:i16 weight:i16 sign:i16 dscale:i16 [digit:i16]*` (base-10000) — V2 follow-up |

NULL is format-agnostic: per-column length field = `-1` regardless of
format (matches PG §55.2.3 and parallels the V1 param NULL handling).

## 2. Scope

### 2.1 V1 — what's in (this arc, T1..T4)

1. **Binary RESULT encode** for the common scalars (BOOL/INT2/INT4/
   INT8/FLOAT4/FLOAT8/TEXT/VARCHAR/BYTEA/TIMESTAMPTZ). Each is one
   match arm in `encode_binary_value`. Input is the same TEXT-format
   wire bytes the existing `encode_data_row` consumes (the gateway's
   `render_pg_text` already produced them); output is the PG binary
   bytes per the table above.
2. **Per-column format dispatch in the Execute response path** — the
   existing `dispatch_query` text-only output is post-processed when
   the portal's `result_formats` says any column wants binary. For
   `result_formats=[1]` every column flips; for `[1, 0, 1]` only
   positions 0 + 2 flip; for `[]` or `[0]` nothing changes (zero-cost
   for the existing text-only path).
3. **RowDescription per-field format_code flip** — when a column is
   emitted binary in DataRow, the corresponding `format_code` field in
   RowDescription MUST be 1 (libpq inspects this to choose its read
   path). The post-processor rewrites RowDescription's per-field
   format_code in lockstep with the DataRow rewrite.
4. **asyncpg parameterized SELECT flips PARTIAL → PASS** on the T3
   vulcan smoke. JDBC default mode untested on vulcan (no javac);
   expected to PASS by symmetry.

### 2.2 V1 — what's out (named V2+ follow-ups)

- **Binary NUMERIC** — V2 `SP-PG-EXTQ-BIN-NUMERIC`. Same arc that
  SP-PG-EXTQ-BIN V1 deferred. V1 here rejects with the same arc name.
- **Binary JSONB / UUID / ARRAY** — V2 `SP-PG-EXTQ-BIN-EXTRA`. Same
  shape; V1 rejects with the arc name.
- **Binary RowDescription field-format default** — most clients
  inspect the per-field format_code to choose their decode path; a
  few (older JDBC) ignore it. V1 ships the correct shape (text
  default 0, binary 1 per the portal's request).
- **Simple-query binary results** — simple-query 'Q' has no Bind, so
  `result_formats` doesn't apply. Simple-query stays text-only
  forever (matches PG itself — PG's simple-query path is also text-
  only). The dispatch_query path is byte-untouched.

## 3. Implementation sketch

### 3.1 `extq::binary_results::encode_binary_value`

A new helper module `extq/binary_results.rs`. The encoder takes
already-rendered text bytes (the same bytes `encode_data_row` would
emit verbatim) + the column's PG type OID, and produces binary bytes:

```rust
pub fn encode_binary_value(
    text: &[u8],
    type_oid: u32,
) -> Result<Vec<u8>, BinaryEncodeError> {
    match type_oid {
        PG_TYPE_BOOL => match text {
            b"t" => Ok(vec![0x01]),
            b"f" => Ok(vec![0x00]),
            _ => Err(BinaryEncodeError::BadValue { type_oid, reason: "BOOL text expected 't'/'f'" }),
        },
        PG_TYPE_INT2 => {
            let n: i16 = std::str::from_utf8(text).ok()?.parse().ok()?;
            Ok(n.to_be_bytes().to_vec())
        }
        PG_TYPE_INT4 => /* i32 BE 4 bytes */,
        PG_TYPE_INT8 => /* i64 BE 8 bytes */,
        PG_TYPE_FLOAT4 => /* f32 IEEE 754 BE */,
        PG_TYPE_FLOAT8 => /* f64 IEEE 754 BE */,
        PG_TYPE_TEXT | PG_TYPE_VARCHAR => Ok(text.to_vec()),
        PG_TYPE_BYTEA => /* decode `\xHEX` text → raw bytes */,
        PG_TYPE_TIMESTAMPTZ => /* ISO string → i64 µs since 2000-01-01 BE 8 bytes */,
        PG_TYPE_NUMERIC => Err(BinaryEncodeError::Unsupported { type_oid, arc: "SP-PG-EXTQ-BIN-NUMERIC" }),
        _ => Err(BinaryEncodeError::Unsupported { type_oid, arc: "SP-PG-EXTQ-BIN-EXTRA" }),
    }
}
```

The TIMESTAMPTZ encoder is the inverse of `decode_timestamptz` — the
Howard Hinnant `days_from_civil` algorithm (companion to the V1
`civil_from_days`). Public-domain pure-Rust.

The BYTEA encoder parses the `\xHEX` text representation back to raw
bytes (text path of `render_pg_text` for `FieldKind::Bytes` already
produces this exact shape).

### 3.2 `extq::binary_results::rewrite_data_row_with_formats`

Takes a complete DataRow wire frame (the bytes `encode_data_row`
emitted), parses out per-column `(length, bytes)`, and re-emits with
per-column format conversion:

```rust
pub fn rewrite_data_row_with_formats(
    text_frame: &[u8],
    formats: &[u16],
    type_oids: &[u32],
) -> Result<Vec<u8>, BinaryEncodeError> {
    let cols = parse_data_row(text_frame);
    let mut new_cols: Vec<Option<Vec<u8>>> = Vec::with_capacity(cols.len());
    for (i, col) in cols.into_iter().enumerate() {
        let format = effective_format_code(formats, i);
        match col {
            None => new_cols.push(None),  // NULL is format-agnostic
            Some(text) if format == FORMAT_CODE_TEXT => new_cols.push(Some(text)),
            Some(text) => {
                let oid = type_oids.get(i).copied().unwrap_or(0);
                new_cols.push(Some(encode_binary_value(&text, oid)?));
            }
        }
    }
    let borrowed: Vec<Option<&[u8]>> = new_cols.iter().map(|c| c.as_deref()).collect();
    Ok(crate::response::encode_data_row(&borrowed))
}
```

### 3.3 `extq::binary_results::rewrite_row_description_with_formats`

Takes a complete RowDescription wire frame and rewrites the per-field
`format_code` slot to match the portal's per-column request. The slot
is the LAST 2 bytes of each field's 18 + name_len byte sub-frame
(after table_oid:4 + column_attr:2 + type_oid:4 + type_size:2 +
type_modifier:4). Walks fields by parsing the name cstring.

```rust
pub fn rewrite_row_description_with_formats(
    rd_frame: &[u8],
    formats: &[u16],
) -> Vec<u8> { ... }
```

If `formats` is empty (all text), returns `rd_frame.to_vec()`
unchanged (zero-cost for the text path).

### 3.4 `dispatch_execute` post-processing

After the existing `split_dispatch_query_bytes` step, if the portal's
`result_formats` requires binary for ANY column, run two extra steps:

```rust
let needs_binary = result_formats.iter().any(|&f| f == FORMAT_CODE_BINARY);
if needs_binary {
    // Look up column type OIDs by re-using the dispatch_describe
    // path's `row_description_or_no_data_for_sql` (only the OIDs
    // matter — we already have RowDescription bytes from the split).
    let type_oids = extract_type_oids_from_row_description(&prelude);
    // Rewrite RowDescription's format_code slot per column.
    prelude = rewrite_row_description_with_formats(&prelude, &result_formats);
    // Rewrite each buffered DataRow's columns per format.
    for row in &mut buffered_rows {
        *row = rewrite_data_row_with_formats(row, &result_formats, &type_oids)?;
    }
}
```

Errors map to `ExtqError::BinaryResultEncodeFailed { position, reason }`
→ SQLSTATE `0A000` with the V2 follow-up arc name.

The path is additive: if `result_formats` is `[]` or all-zero, the
existing text path runs unchanged byte-for-byte.

### 3.5 Error variant

```rust
pub enum ExtqError {
    // ... existing variants ...
    BinaryResultEncodeFailed {
        position: usize,
        type_oid: u32,
        reason: String,
    },
}
```

Maps to SQLSTATE `0A000 feature_not_supported`. Per spec §3 + §6 the
dispatcher sets `error_state = true`.

### 3.6 KAT corpus (T1 + T2 combined, ~12-18 KATs)

T1 lib KATs (binary-encode helper):
- `t1binr_encode_bool_true_byte_correct`
- `t1binr_encode_bool_false_byte_correct`
- `t1binr_encode_int4_be_byte_correct`
- `t1binr_encode_int8_be_byte_correct`
- `t1binr_encode_int2_be_byte_correct`
- `t1binr_encode_float8_pi_be_byte_correct`
- `t1binr_encode_float4_be_byte_correct`
- `t1binr_encode_text_utf8_pass_through`
- `t1binr_encode_bytea_hex_to_raw`
- `t1binr_encode_timestamptz_iso_to_pg_micros`
- `t1binr_encode_numeric_returns_unsupported_with_arc`
- `t1binr_encode_unknown_oid_returns_unsupported_with_arc`
- `t1binr_round_trip_decode_encode_int8` — decode then encode = identity
- `t1binr_round_trip_decode_encode_float8`
- `t1binr_round_trip_decode_encode_timestamptz`

T2 dispatcher KATs (rewrite + dispatch_execute integration):
- `t2binr_rewrite_data_row_all_binary_int8_byte_correct`
- `t2binr_rewrite_data_row_mixed_text_and_binary`
- `t2binr_rewrite_data_row_null_column_stays_null`
- `t2binr_rewrite_data_row_empty_formats_passthrough`
- `t2binr_rewrite_row_description_flips_format_codes`
- `t2binr_dispatch_execute_with_binary_result_formats_produces_binary_data_row`
- `t2binr_numeric_binary_result_rejects_with_followup_arc`

## 4. Acceptance criteria

V1 (T1-T4) ships when:

1. **asyncpg `conn.fetch("SELECT * FROM t WHERE id = $1", 42)`** returns
   rows end-to-end on vulcan (no `insufficient data in buffer` mis-decode).
2. **No regression on text-result path.** Every existing SP-PG-EXTQ
   KAT continues to pass byte-for-byte (the post-processing is
   additive and skipped when `result_formats` is empty or all-zero).
3. **NUMERIC binary result** rejects with a message naming
   `SP-PG-EXTQ-BIN-NUMERIC`.
4. **seed-7 GREEN**, default tree-grep EMPTY, CI green at every
   commit on this arc.

## 5. Task decomposition (T1-T4)

| T# | Scope | KAT delta |
|---|---|---|
| **T1** | This design spec + `binary_results` module with `encode_binary_value` + `rewrite_data_row_with_formats` + `rewrite_row_description_with_formats` + `extract_type_oids_from_row_description` helpers. Pure functions, no dispatcher changes. ~12-15 lib KATs locking every supported-type encode shape + the rewrite invariants + the round-trip identity. | +12-15 |
| **T2** | `dispatch_execute` post-processing + new `ExtqError::BinaryResultEncodeFailed` variant + server.rs SQLSTATE mapping. Existing text-format KATs continue to pass byte-for-byte. New KATs for the dispatcher integration. | +3-6 |
| **T3** | Real asyncpg + psycopg3 smoke on vulcan (asyncpg PARTIAL → PASS). USAGE §9 matrix update. | +0-3 |
| **T4** | STATUS.md row + bullet + progress tracker → CLOSED + V2 follow-up names. TaskList #356 ready. | +0 |

Estimated V1 total: **~15-24 KATs across 4 slices** (target +10-18 per
task brief).

## 6. References

- SP-PG-EXTQ V1 design spec: `docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
- SP-PG-EXTQ V1 progress: `docs/superpowers/specs/2026-05-28-kesseldb-subproject-sppgextq-progress.md`
- SP-PG-EXTQ-BIN V1 design spec: `docs/superpowers/specs/2026-06-01-kesseldb-sppgextq-bin-design.md`
- SP-PG-EXTQ-BIN V1 progress (CLOSED at T3): `docs/superpowers/specs/2026-06-01-kesseldb-subproject-sppgextqbin-progress.md`
- T3 transcript naming the result-side gap: `docs/superpowers/sppgextqbin-t3-smoke-2026-06-01.txt`
- PostgreSQL Documentation §55.2.3 — Extended Query
- PostgreSQL Documentation §55.7 — Message Formats (RowDescription, DataRow)
- PostgreSQL Documentation §55.8 — Binary representations
- `crates/kessel-pg-gateway/src/extq/substitute.rs` — V1 binary-param decoder (mirrors the V1 here)
- `crates/kessel-pg-gateway/src/response.rs` — text DataRow encoder V1 reuses
- `crates/kessel-pg-gateway/src/dispatch.rs` — text simple-query path (byte-untouched here)
