# KesselDB Sub-project 8 — Deterministic mutating triggers

**Date:** 2026-05-17  **Status:** spec + build (autonomous continuation)
**Builds on:** SP7 (same `kessel-expr` VM). Completes the programmable
layer: Postgres-style BEFORE triggers / generated columns, but deterministic
and replicated.

## Goal

`Op::AddTrigger { type_id, program }` — a kessel-expr program run on every
Create/Update before constraints. It may **mutate** the record (derived/
generated fields) or **reject** the write.

## Design

- VM extended with two trigger-only opcodes: `SET_FIELD(field_id)` (pop a
  value, write it into a working copy at the field's offset — numeric→LE
  width, bytes→fixed width, Null→zeroed + null bit if codec-shaped) and
  `REJECT` (abort). `eval_trigger` returns `Some(rec')` or `None` (rejected).
- **Order-independent:** `LoadField`/`IsNull` always read the *original*
  input record; `SET_FIELD` mutates the working copy. So a trigger's output
  doesn't depend on intra-program field-write ordering — keeps determinism
  trivial and identical across replicas.
- `ObjectType.triggers: Vec<Vec<u8>>` in the replicated catalog; triggers run
  in order (each sees the previous trigger's output), then NOT NULL / UNIQUE
  / FK / CHECK run on the final record, then it is stored/indexed.
- `Op::AddTrigger` structurally validates the program (BadProgram/
  StackUnderflow ⇒ `SchemaError`); it does not retroactively rewrite existing
  rows (triggers transform *future* writes).

## Why this is the payoff

CHECK (SP7) proved deterministic *predicates* inside the state machine.
Triggers prove deterministic *mutation* — derived columns, normalization,
policy enforcement — the PL/pgSQL-shaped capability TigerBeetle forbids,
delivered with byte-identical replica convergence (VSR test included).

## Scope / non-goals (honest)

- BEFORE-write only; no AFTER triggers, no statement-level triggers, no
  cross-row or cascading writes (single-row purity preserved on purpose).
- No loops/branches in the ISA (branch-free ⇒ trivially terminating);
  conditional logic is expressed arithmetically or via a following CHECK.
- Trigger errors at `AddTrigger` time on the zero record are tolerated
  (a trigger may legitimately need real data); only hard structural faults
  are rejected at add time.

## Tests

`kessel-expr`: `trigger_sets_derived_field`, `trigger_can_reject` (8 total).
`kessel-sm`: `trigger_derives_field_on_write` (derive on create + re-derive
on update), `trigger_can_reject_write` (+ malformed ⇒ SchemaError),
`trigger_then_check_compose_deterministically`.
`kessel-vsr`: `trigger_replicates_and_converges`. 83 tests total green.
