# KesselDB Sub-project 86 — column DEFAULT + ON DELETE SET DEFAULT

**Date:** 2026-05-18  **Status:** shipped. Closes the genuine part of
the "SET DEFAULT & ON UPDATE" deferral. (`ON UPDATE` was separately
reclassified as model-inapplicable — FKs reference an immutable object
id.)

## Backward-compatible storage (no on-disk catalog hazard)

A per-column default needed a persisted home. The catalog's per-type
serialization has no length prefix (adding a new list there would
corrupt an upgraded on-disk catalog — the SP77 hazard), and adding a
field to `Field` would touch all 129 literal sites. Instead:

- `ObjectType.defaults: Vec<(u16, Vec<u8>)>` (~6 construction sites +
  `from_def`).
- `encode_type_def_with_defaults(name, fields, defaults)` appends a
  trailer `[u16 ndef] ndef×([u16 fid][u16 len][bytes])` **only when
  non-empty**, *inside* the already length-delimited type-def blob.
- `decode_type_def`/`encode_type_def` (77 callers) are **unchanged** —
  `decode_type_def` already ignores trailing bytes, so an old blob and
  a new blob both decode name+fields identically. A separate
  `decode_type_defaults` parses the trailer (empty for old blobs).
- `Catalog::encode` uses the `_with_defaults` form; `Catalog::decode`
  reconstructs `ObjectType.defaults` from the same length-prefixed
  slice. Fully backward-compatible; no format version, no per-type
  framing change.

## Behaviour

- **SQL** `CREATE TABLE t (… <type> [NOT NULL] [DEFAULT <lit>] …)` —
  parsed, lowered to the trailer keyed by the positional (1-based) id
  the engine assigns; the `CreateType` handler loads
  `ObjectType.defaults` via `decode_type_defaults`.
- **INSERT** — an omitted column takes its `DEFAULT` if declared
  (including a `NOT NULL` column that has a default); else `NULL`
  (nullable) or a clean "missing NOT NULL column (no default)" error.
  An explicit value always overrides.
- **`ON DELETE SET DEFAULT`** — FK action `4` (validation widened to
  `0..=4`). On parent delete the child FK column is set to its
  declared column default (a *present* value — no null bit); with no
  declared default it deterministically **degrades to SET NULL**
  (documented). Reuses the existing `collect_set_null` machinery
  (extended with an `Option<default bytes>`); FKs remain op-level (no
  SQL `ON DELETE` syntax), so SET DEFAULT is reached via
  `Op::AddForeignKey { on_delete: 4 }`.

## Verified

- `kessel-sql::column_default_is_applied_and_persists`: omitted columns
  take defaults (incl. NOT-NULL-with-default); explicit values
  override; NOT NULL without default still errors; the default
  survives a full `Catalog::encode`/`decode` round-trip; deterministic.
- `kessel-sm::on_delete_set_default_writes_column_default`: parent
  delete sets the child FK to the declared default (value, not NULL)
  and re-indexes under it; a no-default child with `on_delete=4`
  degrades to SET NULL.
- `kessel-catalog` roundtrip test now carries a non-empty `defaults`
  and proves the trailer survives `Catalog` encode/decode.

Full workspace regression green; determinism / VSR partition corpus
(incl. seed 7) unchanged (additive trailer; existing decoders
untouched).

## Honest boundary

`SET DEFAULT` with no declared default degrades to `SET NULL` (stated,
deterministic). FK syntax remains op-level; there is no SQL
`FOREIGN KEY … ON DELETE` grammar (separate, pre-existing scope).
`ON UPDATE` is not here because it is inapplicable by model, not
deferred (documented elsewhere).
