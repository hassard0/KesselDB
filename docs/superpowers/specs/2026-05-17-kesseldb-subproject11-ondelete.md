# KesselDB Sub-project 11 — ON DELETE RESTRICT / CASCADE

**Date:** 2026-05-17  **Status:** spec + build (autonomous continuation)
**Builds on:** SP6 (FK), SP3 (index), SP9 (txn). Completes referential
integrity: deleting a parent now has correct, deterministic semantics.

## Goal

`Op::AddForeignKey { .., on_delete }` where `on_delete` ∈
{0 NoAction (SP6 behaviour), 1 RESTRICT, 2 CASCADE}. On deleting a parent:
RESTRICT aborts (with zero effect) if any child still references it; CASCADE
recursively deletes the whole referencing closure.

## Design

- FK tuple is now `(field_id, ref_type_id, on_delete)` in the replicated
  catalog. `AddForeignKey` with action ≠ NoAction **auto-ensures an index**
  on the FK field (build + backfill) so the reverse "who references X?"
  lookup is just `idx_lookup`.
- `cascade_collect` DFS computes the closure: root + all CASCADE-reachable
  descendants, with a `visited` set (handles diamonds/cycles) and a budget
  (deterministic termination). A RESTRICT edge with any child ⇒ `Err` ⇒ the
  whole `Delete` returns `Constraint` with **zero** effect.
- The multi-object delete is **atomic**: if not already inside an `Op::Txn`,
  `Delete` wraps the closure in a storage transaction (begin/commit; abort on
  any error) — so a partially-cascaded state can never be observed or
  persisted. Index maintenance runs per deleted object.
- Pure reads over committed state ⇒ deterministic ⇒ replica-identical
  (3-node VSR cascade test included).

## Scope / non-goals (honest)

- Actions: NoAction / RESTRICT / CASCADE. `SET NULL` / `SET DEFAULT` and
  `ON UPDATE` actions are future work.
- Reverse lookup requires the FK field be indexable (not OverflowRef) —
  `AddForeignKey` enforces and auto-indexes it.
- Cascade closure is bounded by a budget (200k objects) — exceeding it
  aborts deterministically rather than running unbounded.

## Tests

`kessel-sm`: `on_delete_restrict_blocks_parent_delete`,
`on_delete_cascade_removes_children_recursively` (3-level parent→child→
grandchild), `on_delete_is_deterministic` (random delete workload).
`kessel-vsr`: `on_delete_cascade_replicates_and_converges`. 95 tests green.
