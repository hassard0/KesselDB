# SP-PG-DDL-FK-ENFORCE — progress tracker

**Status: CLOSED**

Make a `FOREIGN KEY` declared in `CREATE TABLE` DDL actually ENFORCE referential
integrity by WIRING the DDL parser to the pre-existing engine FK machinery.

## Tasks

| # | Task | Status |
|---|------|--------|
| T1 | `kessel-catalog`: `FkSpec` + `encode_type_def_full_fk` / `decode_type_fks` (marker-guarded additive FK trailer; empty ⇒ byte-identical) | DONE |
| T2 | `kessel-sql`: `parse_referential_actions` returns the `on_delete` code; capture table-level + inline FK descriptors into `fk_specs`; route `CreateType` through `encode_type_def_full_fk` | DONE |
| T3 | `kessel-sm`: factor `add_foreign_key` helper; `Op::CreateType` pre-validates FK names → ids (atomic, no half-created type on forward ref) then registers via the shared path | DONE |
| T4 | `kessel-pg-gateway`: widen `constraint_to_sqlstate` so the RESTRICT / dangling-ref messages also map to `23503` (INSERT-enforce path already mapped) | DONE |
| T5 | Unit tests: catalog FK trailer round-trip + byte-identity; SQL FK-capture + ON DELETE keyword mapping; SM DDL-FK registered+enforced + forward-ref clean error; gateway 23503 mapping | DONE |
| T6 | New psql smoke `scripts/sppgddlfkenforce-smoke.py` (good/bad/NULL insert + RESTRICT) | DONE |
| T7 | Regression: ORM relationships + realapp smokes still green under enforcement | DONE |
| T8 | Closure docs (this file, design, USAGE/STATUS/CHANGELOG/README) + push | DONE |

## Determinism

- The FK trailer is additive + marker-guarded (`0xFE`): a no-FK CREATE TABLE emits
  a BYTE-IDENTICAL `def` to before this arc. The `Op` enum is UNCHANGED, so every
  `Op::CreateType { def }` construction site (proto/sm/sql/read_pool/sharded_engine/
  oracles/benches) is unaffected — no "missing field in oracle literal".
- FK registration runs on the single deterministic apply thread; name→id
  resolution is a pure function of catalog state. Oracles stay green (see
  workspace transcript).

## REAL transcripts

### Workspace test (vulcan, `cargo test --workspace --release`)

```
<WORKSPACE_TEST_TAIL>
```

### Regression — `scripts/sppgormrelationships-smoke.py`

```
<RELATIONSHIPS_SMOKE_SUMMARY>
```

### Regression — `scripts/sppgormrealapp-smoke.py`

```
<REALAPP_SMOKE_SUMMARY>
```

### New smoke — `scripts/sppgddlfkenforce-smoke.py`

```
<NEW_SMOKE_TRANSCRIPT>
```

## Deferred

- `SP-PG-DDL-COMPOSITE-FK` — composite FKs (V1 captures the FIRST column only).
- `SP-PG-DDL-FK-ON-UPDATE` — `ON UPDATE` actions parsed but not enforced.
- True inline circular FKs require the parent to exist first (`ALTER TABLE ADD
  CONSTRAINT` via `Op::AddForeignKey` is the cycle escape hatch).
