# KesselDB Sub-project 4 — Built-in constraints (UNIQUE + NOT NULL)

**Date:** 2026-05-17  **Status:** spec + build (autonomous continuation)
**Builds on:** SP1–SP3. North Star constraint set is NOT NULL / UNIQUE /
FK-ref / CHECK / balance-guard; this slice delivers **NOT NULL + UNIQUE**
(FK / CHECK / balance-guard remain a later spec, kept out so this slice is
fully correct and tested).

## Goal

Deterministic, replication-correct enforcement of `NOT NULL` and `UNIQUE`
inside the state machine, returning a dedicated `OpResult::Constraint`.

## Design

- **`OpResult::Constraint(String)`** — a deterministic op result (like
  `Exists`/`NotFound`); every replica produces the same accept/reject.
- **NOT NULL** — from `Field.nullable == false`. Enforced only for
  well-formed codec records: `len == record_size` **and** header
  `field_count == fields.len()`. Any other shape is an opaque/raw write and
  opts out (documented kernel scoping — constraints ride the codec contract;
  raw byte writers, e.g. benchmarks, opt out by construction). Null detection
  reads only the codec header constants (no full codec dependency in the SM).
- **UNIQUE** — `ObjectType.unique: Vec<u16>` (always a subset of `indexes`;
  UNIQUE implies the SP3 equality index). On Create/Update the field value's
  index bucket is consulted; a conflicting *other* object id ⇒
  `Constraint`. Self is excluded so idempotent updates pass.
- **`Op::AddUnique { type_id, field_id }`** — ensures the backing index
  exists (builds + backfills it if absent), then validates current data has
  no duplicate (rejects with `Constraint` if it does — does NOT half-apply),
  then records the constraint in the replicated catalog. Idempotent.
- **Determinism:** all checks read committed state via the deterministic
  storage/index; nothing uses clock/RNG; catalog mutations go through the
  replicated log. Convergence is digest-covered and explicitly tested
  through a 3-node VSR cluster.

## Scope / non-goals (honest)

- Only NOT NULL + UNIQUE. FK-ref, CHECK expressions, balance-guard, and the
  deterministic WASM trigger sandbox remain later specs.
- NOT NULL is enforced for codec records only (documented above).
- UNIQUE uses the SP3 read-modify-write index path (correct; not yet
  throughput-optimized — same documented limitation as SP3).

## Tests

`not_null_enforced_for_codec_records` (valid passes; hand-set null bit on a
NOT NULL field rejected), `unique_rejects_duplicate_on_create_and_update`
(incl. self-exclusion + idempotent AddUnique), `add_unique_validates_existing_data`
(refuses on pre-existing dup, succeeds once fixed, then enforces),
`unique_constraint_replicates_and_converges` (3-node VSR). 60 tests total green.
