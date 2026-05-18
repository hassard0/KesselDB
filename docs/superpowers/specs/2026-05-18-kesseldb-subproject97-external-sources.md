# KesselDB Sub-project 97 — External Sources (JSON/CSV over HTTP), slice 1

**Date:** 2026-05-18  **Status:** shipped. Full workspace regression
222 green (feature OFF — the default); feature-ON oracle green.

EXT slice 1: registered external JSON/CSV-over-HTTP sources,
materialized into normal KesselDB types via an explicit, replicated
`REFRESH`. The deterministic kernel is untouched; the feature is OFF
by default; the seed-7 corpus and the determinism digest are unaffected
when off.

---

## What shipped, per crate

### `kessel-fetch` (new optional crate, feature `external-sources`)

Pure-std HTTP/1.1 GET + JSON + CSV + `FieldKind` coercion. Four
source files: `http` (connect/GET/chunked-decode, 64 MiB body cap
configurable), `json` (array-of-flat/nested-scalar-objects, dotted
path), `csv` (RFC 4180 by header name), `coerce` (raw cell → typed
`FieldKind` bytes, every KesselDB primitive). Public surface:

```rust
pub fn fetch_rows(url, auth, format, cols, max_body)
    -> Result<Vec<Vec<Vec<u8>>>, FetchError>
```

`FetchError` variants: `Http`, `Parse`, `Type`, `Auth`, `TooLarge`.

`Auth` enum — resolved by the caller (router) from its own env;
never persisted:

```rust
pub enum Auth { None, Bearer(String), Header { name, value } }
```

This crate is **not compiled when the feature is off**. The default
workspace build (`cargo build --workspace`, `cargo test --workspace`)
does not depend on it.

### `kessel-catalog` — `ExternalRecipe` trailer

Additive backward-compatible trailer appended to the
length-delimited catalog blob (same mechanism SP86 used for column
defaults). Fields:

| Field | Type | Notes |
|---|---|---|
| `type_id` | `u32` | backing type |
| `url` | `String` | HTTP URL |
| `format` | `u8` | 0 = JSON, 1 = CSV |
| `key_field_id` | `u32` | column whose value determines `ObjectId` |
| `auth` | `ExternalAuth` | `None` / `BearerEnv(env_var_name)` / `HeaderEnv { header, env_var_name }` — only the var NAME is persisted |
| `mapping` | `Vec<(field_id, source_path)>` | explicit per-column mapping |

Decode for all callers with an existing catalog blob is untouched
(the trailer is length-framed and absent in old blobs → empty
`external` vec, no change to the digest for existing types).

### Three new ops in `kessel-proto`

| Op | Kind | Direction |
|---|---|---|
| `Op::CreateExternalSource { name, type_def, url, format, key_field_id, auth_kind, auth_a, auth_b, mapping }` | replicated DDL | all nodes |
| `Op::DropExternalSource { name }` | replicated DDL | all nodes |
| `Op::RefreshExternalSource { name }` | router-only signal | never enters SM |

`auth_kind`: 0 = none, 1 = bearer-env, 2 = header-env.
`auth_a` / `auth_b`: for bearer = env var name + ""; for header = header name + env var name.
Wire round-trip test: `external_source_ops_wire_round_trip` (kessel-proto, 1 test).

### `kessel-sm` — apply Create/Drop + reject Refresh

`Op::CreateExternalSource`: validates auth_kind as a pure pre-check
(no side effects if invalid); creates the backing `ObjectType` via the
proven `CreateType` path; appends the `ExternalRecipe` to the catalog.
Atomic: if `CreateType` fails, no recipe is added (C1 regression fix:
bad auth_kind creates nothing; no orphaned type).

`Op::DropExternalSource`: resolves the type by name → `DropType` (FK
guard included: if another type has a FK referencing the backing type,
`DropType` returns `Constraint` and the recipe is NOT removed; I1
regression fix — a failed `DropType` leaves the recipe in place).

`Op::RefreshExternalSource { .. }` at the SM level →
`OpResult::SchemaError("RefreshExternalSource must be handled by the router …")`.
The SM is stateless with respect to HTTP; refresh is router-only.

### `kessel-sql` — SQL grammar for all three statements

```sql
CREATE EXTERNAL SOURCE <name> (
    <col> <TYPE> [NOT NULL] FROM '<json-dotted-path-or-csv-header>',
    ...
) FROM '<url>' FORMAT JSON|CSV KEY <col>
  [AUTH BEARER ENV '<VARNAME>' | AUTH HEADER '<H>' ENV '<VARNAME>']

REFRESH <name>

DROP EXTERNAL SOURCE <name>
```

No trailing comma after the final column (the parser expects either `,` to continue or `)` to end the column list).

Parser: recursive-descent extension of the existing `compile()` path.
Validates: column list non-empty (clear error); `KEY` column must be a
declared column (error names the missing column). Lowers to the three
ops above. Backward-compatible: the grammar extension is new keywords
not reachable from any prior SQL.

### `kesseldb-server` (feature `external-sources`) — `do_refresh` oracle

The router gains a `REFRESH` dispatch path:

1. Load the `ExternalRecipe` from the catalog (`Describe`-class read —
   catalog is global, byte-identical on all shards).
2. Resolve the auth secret from the **router's own environment** via
   `std::env::var(env_var_name)`. The secret VALUE is never placed in
   any op, log, or digest.
3. Call `kessel_fetch::fetch_rows(url, auth, format, cols, max_body)`.
   If the body exceeds 64 MiB or any row fails type coercion →
   `SchemaError`; nothing is mutated.
4. For each parsed row, derive `ObjectId` = first 16 bytes of a
   domain-separated `kessel-crypto` SHA-256 hash of the canonicalized
   KEY value (domain tag `b"extkey:"`, big-endian key bytes). Same
   upstream key → same `ObjectId` across refreshes (stable upsert key).
5. For each row, check existence via `GetById`; emit `Create` (absent)
   or `Update` (present). Batch into a single `Op::Txn`.
6. Submit the `Op::Txn` through the existing replicated path (`Route::One`
   on the owning shard). The fetch happens exactly once, on the router;
   only the captured rows enter the replicated log. A failed fetch
   submits nothing (all-or-nothing per refresh).

`CREATE EXTERNAL SOURCE` and `DROP EXTERNAL SOURCE` are routed as
`Route::All` (catalog DDL, broadcast to every shard).

---

## Deterministic ObjectId scheme

```
ObjectId = SHA-256( b"extkey:" || be_bytes(key_value) )[0..16]
```

`kessel-crypto` SHA-256 (zero-dep, NIST-vector-verified, SP65).
Deterministic and stable: the same upstream primary-key value always
maps to the same row. Idempotent: same rows → same ids → same
Create/Update verdict → same resulting state.

---

## Exactly-once / atomic-upsert design

- One `Op::Txn` per `REFRESH` → one consensus decision → one atomic
  commit or abort.
- The router resolves Create-vs-Update via a point `GetById` check per
  row before building the Txn. This is the same approach cross-shard
  txns use for their per-shard RMW steps.
- The whole batch is deterministic once the fetched bytes are fixed: the
  same body → the same `Op::Txn` → the same result on every replica.
- Slice 1 = **upsert only**: rows deleted upstream are NOT auto-pruned.
  `REFRESH … MODE REPLACE` (prune/replace-all) is an explicit follow-on.

---

## Auth env-ref security model

Only the env-var NAME (`ENV 'MY_TOKEN'`) is persisted in the
`ExternalRecipe` and replicated through the catalog. The actual secret
value is resolved at `REFRESH` time from the router's process
environment (`std::env::var`) and is never placed in any op, WAL
entry, digest, or log line. A replica that cannot run `do_refresh`
(only the router runs it) never sees the secret.

---

## Honest boundary / Determinism & consistency boundary

A source reflects only its **last successful `REFRESH`**. Queries read
the materialized snapshot, never live upstream. `REFRESH` is the sole
point external data enters — captured once on the router, then
replicated identically to every replica. This is the same kind of
explicit, documented boundary as the cross-shard "no consistent
snapshot" and cross-shard-txn consistency boundaries.

HTTP-only in slice 1: `kessel-fetch` speaks plain HTTP/1.1. HTTPS is
not supported; use a TLS-terminating sidecar or reverse proxy.

---

## Verified test evidence

### `kessel-fetch` — 15 tests

**Internal (12):**

| Test | What it proves |
|---|---|
| `coerce::tests::integers_little_endian_by_width` | U8/U16/U32/U64/I8…I64 encode as LE bytes |
| `coerce::tests::out_of_range_integers_are_rejected` | overflow → `FetchError::Type` |
| `coerce::tests::bool_and_char_and_null_and_bad` | Bool, Char(n), null cell, unknown kind |
| `csv::tests::header_selected_by_name_with_quotes_and_newlines` | RFC 4180 quoted fields + multiline |
| `csv::tests::missing_header_column_is_parse_error` | absent header → `FetchError::Parse` |
| `csv::tests::final_record_without_trailing_newline` | CRLF / bare-LF / no-final-NL |
| `csv::tests::empty_field_is_null_and_escaped_quote` | empty cell = null; `""` = literal `"` |
| `json::tests::extracts_flat_and_nested_scalars` | dotted path traversal |
| `json::tests::null_and_bool_and_missing_path` | null/bool JSON literals; missing path = error |
| `json::tests::handles_strings_with_escapes_and_numbers` | JSON string escapes + number types |
| `json::tests::rejects_non_array_top_level_and_bad_json` | non-array body → error |
| `json::tests::preserves_multibyte_utf8_in_strings` | UTF-8 passthrough |

**Integration stub-server (3):**

| Test | What it proves |
|---|---|
| `json_over_http_with_bearer_round_trips` | in-process TCP server, Bearer header sent correctly, response parsed |
| `body_too_large_is_typed_error` | body exceeds cap → `FetchError::TooLarge`, no panic |
| `truncated_chunked_body_is_typed_error_not_panic` | chunked decode tear → `FetchError::Http`, no panic |

### `kessel-catalog` — 1 test (in the 5-test catalog suite)

`catalog_external_recipe_round_trips_and_is_backward_compatible`:
round-trips all three auth variants (None / BearerEnv / HeaderEnv);
a catalog blob without the trailer decodes with an empty `external`
vec (backward compatibility).

### `kessel-proto` — wire round-trip (in the 7-test proto suite)

`external_source_ops_wire_round_trip`: all three ops encode/decode to
identical values.

### `kessel-sm` — 1 test (in the 69-test SM suite)

`create_and_drop_external_source_manages_type_and_recipe`:
- `CreateExternalSource` → `OpResult::Ok`; catalog has the backing type + recipe.
- `RefreshExternalSource` at SM level → `OpResult::SchemaError` (router-only rejected).
- `DropExternalSource` → `OpResult::Ok`; type AND recipe removed.
- `DropExternalSource` for non-existent name → `OpResult::NotFound`.
- C1 regression: bad `auth_kind` (99) → `SchemaError`; no orphaned type in catalog.
- I1 regression: `DropExternalSource` when backing type is FK-referenced → `Constraint`; recipe intact.

### `kessel-sql` — 2 tests (in the 27-test SQL suite)

`parse_create_external_source`:
- JSON + BEARER ENV: fields, url, format=0, auth_kind=1, auth_a=env-var, mapping with dotted path.

`parse_refresh_and_drop_external_source`:
- `REFRESH feed` → `Op::RefreshExternalSource { name: "feed" }`.
- `DROP EXTERNAL SOURCE feed` → `Op::DropExternalSource { name: "feed" }`.
- CSV FORMAT: format=1, auth_kind=0 (no auth).
- HEADER auth variant: auth_kind=2, auth_a=header-name, auth_b=env-var-name.
- Bad KEY (not a declared column): error message names the missing column.
- Empty column list: error contains "at least one column".

### `kesseldb-server` (feature `external-sources`) — 25 lib + 1 oracle

Lib: the existing 25 server/cluster/router tests all pass unchanged
with the feature enabled — zero regression.

`refresh_oracle_materializes_idempotent_upserts_and_atomic_abort`
(4 assertions, real TCP cluster + real stub HTTP server):

1. `REFRESH` materializes **exactly** the served rows (independent
   model comparison).
2. Identical re-`REFRESH` is **idempotent**: row set and raw blob
   fingerprint unchanged.
3. A changed upstream row is **updated in place** (same ObjectId, no
   duplicate, count still 2).
4. A schema-violating row → `SchemaError`/`Constraint` and prior data
   is **byte-for-byte unchanged** (atomic abort).

### Feature-OFF full workspace gate

```
cargo test --workspace --release
```

FAILED = 0 · TOTAL = 222 · `large_seed_corpus_is_deterministic_and_converges` = 1 (ok)

`kessel-fetch` is compiled in (it has no `#[cfg(feature)]` guard on the
crate itself — the guard is in `kesseldb-server`'s dependency
declaration). Its 15 unit/integration tests are included in the 222
count and pass without any network activity (stub HTTP server, no real
net). When the feature flag is off, the router's `do_refresh` path is
simply absent; the oracle test file has `#![cfg(feature = "external-sources")]`
and is excluded.

The deterministic kernel digest is unchanged: `ExternalRecipe` fields
are not part of any op's determinism domain; existing type-defs are
unaffected; the seed-7 corpus is green.

---

## Non-goals and follow-ons

| Item | Status |
|---|---|
| TLS in `kessel-fetch` | follow-on slice; use a TLS-terminating sidecar for now |
| `REFRESH … MODE REPLACE` (prune rows deleted upstream) | follow-on slice |
| NDJSON / pagination / nested-object-as-column-value | non-goals for slice 1 |
| Scheduled / automatic refresh (cron-style) | follow-on |
| Schema inference | non-goal (breaks strong typing / determinism) |
| Cross-source joins as a special case | non-goal (plain SQL JOINs over materialized types already work) |
| OBJ object-store substrate | separate sub-project |
| WASM guest UDF integration | separate sub-project |
| Secrets in the catalog | hard non-goal; only env-var names are persisted |
| In-engine external I/O | hard non-goal; router-only by design |
| Live per-query federation | hard non-goal (breaks determinism) |
