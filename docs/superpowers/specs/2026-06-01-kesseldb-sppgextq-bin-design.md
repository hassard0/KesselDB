# SP-PG-EXTQ-BIN — PostgreSQL Extended Query binary-format parameters — DESIGN

**Status:** design — scopes the V2 follow-up named in
`docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
§2.2 + §11 weak-spot #1, and the progress-tracker closure note in
`docs/superpowers/specs/2026-05-28-kesseldb-subproject-sppgextq-progress.md`
(SP-PG-EXTQ V1 CLOSED at T8, 2026-05-29).

SP-PG-EXTQ V1 ships text-format parameters only — the `Bind` message's
`param_formats` array is parsed + validated, but any code `1` (binary)
gets `0A000 feature_not_supported`. That covers ~95% of real-world
ORM traffic (psycopg2 / SQLAlchemy / node-postgres / Django default
to text), but blocks the four ORMs that default to binary:
asyncpg, JDBC pgJDBC (default extended mode), psycopg3 (default
ServerCursor), sqlx (PG mode). The T8 transcript
(`docs/superpowers/sppgextq-t8-orm-smoke-2026-05-29.txt`) recorded
asyncpg + JDBC as PARTIAL for exactly this reason.

This arc lifts the binary-format parameter restriction for the common
scalar types: INT2/INT4/INT8, FLOAT4/FLOAT8, BOOL, TEXT/VARCHAR,
BYTEA, TIMESTAMPTZ. NUMERIC binary is base-10000 variable-length-digit
and bug-prone; it's a follow-up `SP-PG-EXTQ-BIN-NUMERIC` arc.

**Builds on:**
- **SP-PG-EXTQ V1** (`docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`)
  — the Parse / Bind / Describe / Execute / Sync / Close / Flush
  dispatchers, `extq::SessionState`, `extq::Portal.param_formats`
  array (already populated per-position by T3), and the
  `extq/substitute.rs` text-format `$N` substitution helper (T5).
  This arc EXTENDS substitute with a per-parameter format dispatch:
  text params keep the existing path; binary params route through a
  new `decode_binary_param` helper that yields the SQL-literal
  representation, which then flows through the same single-quote
  escaping rules.
- **`crates/kessel-pg-gateway/src/types.rs`** — the PG type OID
  catalog (INT2=21, INT4=23, INT8=20, FLOAT4=700, FLOAT8=701,
  BOOL=16, TEXT=25, VARCHAR=1043, BYTEA=17, TIMESTAMPTZ=1184,
  NUMERIC=1700). The binary decoder switches on these OIDs.

---

## 1. Context — what binary-format parameters look like

PG §55.2.3 (extended query) + §55.8 (binary representations):

Each parameter in a Bind message carries a `format_code: u16`:
- `0` = text (the V1-supported path — the wire bytes are a UTF-8
  ASCII representation of the value, e.g. `"42"` for an int).
- `1` = binary (network-byte-order fixed/var encoding per PG type).

V1 already parses the `param_formats` array per spec §4 length
conventions:
- 0 codes = "all positions text" (no rejection in V1).
- 1 code = "every position the same" (V1 rejects iff that code is 1).
- N codes = "per-position" (V1 rejects the first position where code
  is 1).

This arc replaces the V1 "reject any binary" check with per-position
DISPATCH: text → existing path; binary → `decode_binary_param(bytes,
type_oid)` → SQL literal → same single-quote escape.

### 1.1 Wire decoding table

Per PG §55.8 binary representations. V1 supports the common scalars:

| PG type | OID | Bytes | Binary encoding |
|---|---|---|---|
| BOOL | 16 | 1 | `0x00` = false, `0x01` = true |
| BYTEA | 17 | var | raw bytes (no length prefix — comes from CopyData wrapper) |
| INT2 | 21 | 2 | big-endian i16 |
| INT4 | 23 | 4 | big-endian i32 |
| INT8 | 20 | 8 | big-endian i64 |
| TEXT | 25 | var | UTF-8 bytes (no length prefix) |
| VARCHAR | 1043 | var | UTF-8 bytes |
| FLOAT4 | 700 | 4 | IEEE 754 single, big-endian |
| FLOAT8 | 701 | 8 | IEEE 754 double, big-endian |
| TIMESTAMPTZ | 1184 | 8 | i64 microseconds since 2000-01-01 00:00:00 UTC |
| NUMERIC | 1700 | var | `ndigits:i16 weight:i16 sign:i16 dscale:i16 [digit:i16]*` (base-10000) — V2 follow-up `SP-PG-EXTQ-BIN-NUMERIC` |

### 1.2 What the SQL-literal output looks like

`decode_binary_param(bytes, type_oid)` returns a `Result<String,
ExtqError>` whose `Ok` is the bare SQL-literal text (NOT
single-quoted — the caller wraps the result in `'...'` and applies
single-quote doubling escape via the existing `render_param` shape).

| Bound binary | type OID | Decoded SQL literal |
|---|---|---|
| `[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64]` | INT8 (20) | `100` |
| `[0xFF, 0xFF, 0xFF, 0xFF]` | INT4 (23) | `-1` |
| `[0x01]` | BOOL (16) | `true` |
| `[0x00]` | BOOL (16) | `false` |
| `[0x40, 0x09, 0x21, 0xFB, 0x54, 0x44, 0x2D, 0x18]` | FLOAT8 (701) | `3.141592653589793` |
| `[0x68, 0x65, 0x6C, 0x6C, 0x6F]` | TEXT (25) | `hello` |
| `[0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x27, 0x77]` | TEXT (25) | `hello'w` (caller escapes `'`→`''`) |
| `[0xDE, 0xAD]` | BYTEA (17) | `\xdead` (the caller wraps via `'\\xdead'::bytea`) |
| `[microseconds BE]` | TIMESTAMPTZ (1184) | `2026-06-01 12:34:56.789012+00` |

The caller (`substitute_params`) then wraps text-shaped output in
single quotes per the existing `render_param`, applies `'`→`''`
doubling, and types BYTEA + TIMESTAMPTZ with the `::bytea` /
`::timestamptz` suffix so the SQL parser accepts the literal.

## 2. Scope

### 2.1 V1 — what's in (this arc, T1..T4)

1. **Binary-format parameter decode for the common scalars:**
   INT2/INT4/INT8, FLOAT4/FLOAT8, BOOL, TEXT/VARCHAR, BYTEA,
   TIMESTAMPTZ. Each is one match arm in `decode_binary_param`.
2. **Per-parameter format dispatch in substitute:** the existing
   `substitute_text_format_params(sql, params)` helper is renamed
   `substitute_params(sql, params, formats, oids)`. Text path
   unchanged; binary path calls `decode_binary_param` and renders
   the result through the same `render_param` shape.
3. **Bind no longer rejects binary format codes.** The T3
   `BinaryFormatNotSupported` rejection lifts iff the corresponding
   `param_oids[i]` is one of the supported types. Unknown / NUMERIC
   binary still rejects with `0A000` (but now with a precise
   `binary <typename>: SP-PG-EXTQ-BIN-NUMERIC` or `binary type
   <oid>: SP-PG-EXTQ-BIN-<TYPE>` message pointing at the V2
   follow-up).
4. **asyncpg + psycopg3 default mode + JDBC default mode flip
   PARTIAL → PASS** on the T3 vulcan smoke. No ClientCursor
   workaround needed.

### 2.2 V1 — what's out (named V2+ follow-ups — each is its own arc)

- **Binary NUMERIC** — V2 `SP-PG-EXTQ-BIN-NUMERIC`. The PG binary
  numeric encoding is base-10000 with variable-length digits +
  sign + dscale + weight; transcribing to KesselDB's I128/Fixed
  needs careful bug-tested arithmetic. The V1 rejection points
  directly at this arc.
- **Binary result format** (server emits DataRow as binary) — V2
  `SP-PG-EXTQ-BIN-RESULTS`. The PARAM side is V1; emitting binary
  DataRow on `result_formats==1` is V2.
- **JSONB / UUID / ARRAY binary** — V2 `SP-PG-EXTQ-BIN-EXTRA`. Less
  common, more bespoke.

## 3. Implementation sketch

### 3.1 `extq/substitute.rs`

New helper:

```rust
pub fn decode_binary_param(
    bytes: &[u8],
    type_oid: u32,
) -> Result<String, BinaryDecodeError> {
    match type_oid {
        PG_TYPE_BOOL => match bytes {
            [0x00] => Ok("false".to_string()),
            [0x01] => Ok("true".to_string()),
            _ => Err(BinaryDecodeError::BadValue { type_oid, reason: "BOOL binary expects 1 byte 0x00/0x01" }),
        },
        PG_TYPE_INT2 => read_int_be::<2>(bytes, type_oid).map(|n| n.to_string()),
        PG_TYPE_INT4 => read_int_be::<4>(bytes, type_oid).map(|n| n.to_string()),
        PG_TYPE_INT8 => read_int_be::<8>(bytes, type_oid).map(|n| n.to_string()),
        PG_TYPE_FLOAT4 => read_float_be::<4>(bytes, type_oid).map(|f| format!("{:?}", f as f32)),
        PG_TYPE_FLOAT8 => read_float_be::<8>(bytes, type_oid).map(|f| format!("{:?}", f)),
        PG_TYPE_TEXT | PG_TYPE_VARCHAR => std::str::from_utf8(bytes)
            .map(|s| s.to_string())
            .map_err(|_| BinaryDecodeError::BadValue { ... }),
        PG_TYPE_BYTEA => Ok(hex_encode(bytes)), // caller wraps in '\xHEX'::bytea
        PG_TYPE_TIMESTAMPTZ => {
            let micros = read_int_be::<8>(bytes, type_oid)?;
            Ok(timestamptz_to_iso(micros))
        }
        PG_TYPE_NUMERIC => Err(BinaryDecodeError::Unsupported {
            type_oid,
            arc: "SP-PG-EXTQ-BIN-NUMERIC",
        }),
        _ => Err(BinaryDecodeError::UnknownType { type_oid }),
    }
}
```

### 3.2 `substitute_params` per-format dispatch

The existing `substitute_text_format_params(sql, params)` becomes the
text-only inner path. A new `substitute_params(sql, params, formats,
oids)` is the unified entry the dispatcher calls. The text path
calls into the existing scanner; the binary path pre-decodes each
binary param to its SQL-literal string and passes a derived
`Vec<Option<Vec<u8>>>` through.

Or simpler: pre-process `params` BEFORE handing them to the existing
text-format substitute. Each binary param is decoded once, the result
is converted to the wire-text-shape bytes the existing helper
expects, and the unchanged text path takes over. This is the V1
implementation strategy — minimal churn, maximal regression-safety.

```rust
pub fn substitute_params(
    sql: &str,
    params: &[Option<&[u8]>],
    formats: &[u16],          // per spec §4 length conventions
    type_oids: &[u32],        // from PreparedStmt.param_oids
) -> Result<String, SubstituteError> {
    let pre = preprocess_binary_params(params, formats, type_oids)?;
    substitute_with_renderer(sql, &pre)
}
```

Where `pre: Vec<RenderedParam>` is a discriminated union:
- `Text(bytes)` — emits `'bytes-with-singlequote-doubled'`.
- `BinaryDecoded(literal, suffix)` — emits `'literal'` then
  optionally `::bytea` / `::timestamptz`.

The bytea + timestamptz cases need the `::` suffix because the
literal text doesn't parse as bytea/timestamptz by default; the
suffix tells the SQL parser the column type.

### 3.3 Bind dispatcher accepts binary iff supported OID

`dispatch_bind` in `extq/mod.rs` currently rejects ANY binary code
at position i. The new behavior:

- If `formats[i]` is 0 (text), accept (unchanged).
- If `formats[i]` is 1 (binary) AND `param_oids[i]` is one of the
  V1-supported binary OIDs (INT2/INT4/INT8/FLOAT4/FLOAT8/BOOL/TEXT/
  VARCHAR/BYTEA/TIMESTAMPTZ), accept and remember the format.
- If `formats[i]` is 1 AND `param_oids[i]` is NUMERIC → reject with
  precise `0A000 binary NUMERIC: SP-PG-EXTQ-BIN-NUMERIC` pointing
  at the V2 follow-up.
- If `formats[i]` is 1 AND `param_oids[i]` is an OID V1 doesn't
  recognize → reject with `0A000 binary type <oid>:
  SP-PG-EXTQ-BIN-EXTRA`.
- If `formats[i]` is 1 AND Parse omitted the OID hint
  (`param_oids.len() == 0`) — Bind still has to validate. V1 picks
  the conservative shape: reject with `0A000 binary format requires
  Parse-time type OID hint (asyncpg/JDBC supply OIDs by default —
  if you see this, your driver omitted them)`. Empirically asyncpg
  always sends OID hints when it uses binary format; JDBC too. So
  this rejection is defensive.

The Bind dispatcher's binary rejection happens BEFORE storage. The
Execute dispatcher then trusts that any binary format code in the
portal is one of the supported OIDs.

### 3.4 KAT corpus (T1 + T2 combined, ~15-20 KATs)

T1 lib KATs:
- `t1bin_decode_int8_binary_positive` — 8-byte BE → `100`.
- `t1bin_decode_int8_binary_negative` — 8-byte BE `0xFFFFFFFFFFFFFFFF` → `-1`.
- `t1bin_decode_int4_binary` → `-1`.
- `t1bin_decode_int2_binary` → `42`.
- `t1bin_decode_bool_true` / `_false` / `_invalid`.
- `t1bin_decode_float8_pi` — IEEE 754 binary of π → `3.141592653589793`.
- `t1bin_decode_float4` — IEEE 754 binary of 1.5 → `1.5`.
- `t1bin_decode_text_utf8` → bare UTF-8 string.
- `t1bin_decode_text_with_quote` — caller-escaped later, decoder
  returns the raw `'`-containing string.
- `t1bin_decode_bytea_hex` → `\x<hex>`.
- `t1bin_decode_timestamptz_iso` — microseconds → ISO timestamp.
- `t1bin_decode_numeric_returns_unsupported`.
- `t1bin_decode_unknown_oid_returns_unsupported`.
- `t1bin_decode_int8_wrong_length_rejects`.

T2 substitute + dispatcher KATs:
- `t2bin_substitute_text_path_unchanged` — backward-compat lock.
- `t2bin_substitute_mixed_text_and_binary` — INT8 binary + TEXT text.
- `t2bin_substitute_binary_bytea_wraps_with_cast` — `'\\xdead'::bytea`.
- `t2bin_substitute_binary_timestamptz_wraps_with_cast`.
- `t2bin_dispatch_bind_binary_int8_accepted` — replaces T3's
  `t3_dispatch_bind_binary_format_per_position_rejected` for
  the supported-OID case.
- `t2bin_dispatch_bind_binary_numeric_rejected_with_followup_arc`.
- `t2bin_dispatch_bind_binary_unknown_oid_rejected`.

T3 server KATs:
- `t3bin_real_asyncpg_smoke` — flips PARTIAL → PASS.
- `t3bin_real_psycopg3_default_cursor_smoke` — flips PARTIAL → PASS.

## 4. Acceptance criteria

V1 (T1-T4) ships when:

1. **asyncpg `conn.execute("INSERT ... VALUES ($1, $2)", 42, "hello")`**
   succeeds end-to-end on vulcan (no `0A000`).
2. **psycopg3 with DEFAULT cursor (not ClientCursor)** can
   `cur.execute("SELECT * FROM t WHERE id = %s", (42,))` and get
   the row back.
3. **No regression on text-format-only path.** Every existing
   T5/T6/T7/T8 SP-PG-EXTQ KAT continues to pass byte-for-byte.
4. **NUMERIC binary** rejects with a message naming
   `SP-PG-EXTQ-BIN-NUMERIC` so operators can grep for the gap.
5. **seed-7 GREEN**, default tree-grep EMPTY, CI green at every
   commit on this arc.

## 5. Task decomposition (T1-T4)

| T# | Scope | KAT delta |
|---|---|---|
| **T1** | This design spec + `decode_binary_param` helper in `extq/substitute.rs` + ~10 lib KATs locking every supported-type binary decode + the unsupported-type rejection. No dispatcher / Bind / Execute changes. | +10-14 |
| **T2** | `substitute_params` per-format dispatch + Bind dispatcher accepts binary iff supported OID + `dispatch_execute` passes formats + oids to substitute. Existing text-only KATs continue to pass. New KATs for mixed text/binary substitution + Bind binary-accept. | +6-10 |
| **T3** | Real asyncpg + psycopg3-default + JDBC-default smoke on vulcan (the T8 PARTIAL drivers flip to PASS). USAGE §9 matrix update. | +0-3 |
| **T4** | STATUS.md row + bullet + progress tracker → CLOSED + V2 follow-up names. TaskList #355 ready. | +0 |

Estimated V1 total: **~16-27 KATs across 4 slices** (target +10-20 per
task brief).

## 6. References

- SP-PG-EXTQ V1 design spec: `docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
- SP-PG-EXTQ V1 progress (CLOSED at T8): `docs/superpowers/specs/2026-05-28-kesseldb-subproject-sppgextq-progress.md`
- T8 ORM compat transcript: `docs/superpowers/sppgextq-t8-orm-smoke-2026-05-29.txt`
- PostgreSQL Documentation §55.2.3 — Extended Query
- PostgreSQL Documentation §55.8 — Binary representations
- libpq source `src/interfaces/libpq/fe-protocol3.c` — binary encoder side V1 mirrors
- `crates/kessel-pg-gateway/src/types.rs` — PG type OID catalog the binary decoder switches on
- `crates/kessel-pg-gateway/src/extq/substitute.rs` — text-format substitution V1 extends
- `crates/kessel-pg-gateway/src/extq/mod.rs` — `dispatch_bind` binary rejection V1 lifts
