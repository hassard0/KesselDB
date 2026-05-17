# KesselDB Sub-project 7 — Deterministic expression VM + CHECK

**Date:** 2026-05-17  **Status:** spec + build (autonomous continuation)
**Builds on:** SP1–SP6. This is the **revolutionary differentiator**: user
logic that runs *inside* the replicated deterministic state machine — the
thing Postgres has (CHECK/PL) that TigerBeetle deliberately forbids.

## Why this matters

The whole thesis is "Postgres flexibility at TigerBeetle speed." Programmable
constraints are the sharpest example of Postgres flexibility that a
deterministic, replicated, zero-allocation core supposedly cannot have.
KesselDB gets it by making the program a **pure, gas-bounded, total
function** that is part of replicated state — so it is byte-identical on
every replica by construction.

## `kessel-expr` VM

- Stack bytecode, zero dependencies. Values: `Int(i128)` / `Bytes` / `Null`.
- Opcodes: PushInt/PushBytes, LoadField, IsNull, Eq/Ne/Lt/Le/Gt/Ge,
  Add/Sub/Mul/Div/Mod (wrapping; div/mod-by-zero ⇒ error ⇒ reject),
  And/Or/Not. **No backward jumps** ⇒ trivially terminating; a `GAS_LIMIT`
  instruction cap defends against malformed huge programs.
- Field decode mirrors the engine: sign-extended for signed/Fixed, LE for
  unsigned/bool/timestamp, raw bytes for Char/Bytes/Ref. Null via the codec
  header (codec-shaped records); opaque records expose fields as present.
- Pure: no I/O, clock, RNG. Same (program, schema, record) ⇒ same verdict on
  every machine.

## CHECK wiring (state machine)

- `ObjectType.checks: Vec<Vec<u8>>` (compiled programs) in the replicated
  catalog.
- `Op::AddCheck { type_id, program }`: structurally validates the program
  (eval vs a zero record — rejects BadProgram/StackUnderflow/EmptyResult as
  `SchemaError`), then validates **every existing row** (rejects with
  `Constraint`, without enabling, if any row fails), then records it.
- Enforced on Create/Update after FK; `false` or any VM error ⇒
  `OpResult::Constraint`. Read-only over the candidate record ⇒ deterministic;
  3-node VSR convergence test included.

## Scope / non-goals (honest)

- Expression-only (predicate over a single row). It is *not yet* a trigger
  (cannot mutate / cascade) — that is SP8, which reuses this exact VM.
- No string ops beyond equality/ordering; no aggregates; no cross-row access
  (single-row purity is deliberate — keeps determinism trivial).
- `u128` whose high bit is set folds into the `i128` domain (documented
  edge); u8..u64 are exact.

## Tests

`kessel-expr`: comparison/logic, arithmetic + div-zero, signed decode,
determinism, malformed-program-not-panic, IS_NULL (6).
`kessel-sm`: `check_constraint_enforced_via_vm`,
`add_check_validates_existing_and_rejects_bad_program`, `check_is_deterministic`.
`kessel-vsr`: `check_constraint_replicates_and_converges`. 77 tests total green.
