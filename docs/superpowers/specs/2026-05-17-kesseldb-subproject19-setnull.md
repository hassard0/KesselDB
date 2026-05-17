# KesselDB Sub-project 19 — ON DELETE SET NULL

**Date:** 2026-05-17  **Status:** spec + build. Completes the referential-
action set: NoAction(0) / RESTRICT(1) / CASCADE(2) / **SET NULL(3)**.

## Goal

When a parent is deleted, children whose FK has `on_delete = 3` keep
existing but have that FK field set to NULL (instead of being deleted or
blocking the delete).

## Design

- `cascade_collect` now treats action 3 as "not a delete" (skips it; only
  CASCADE recurses, RESTRICT aborts).
- `collect_set_null(closure)` finds, for every object being deleted, the
  children (not themselves in the delete closure) whose `on_delete=3` FK
  references it — via the existing reverse `idx_lookup`, deduped.
- In `Delete`, inside the same atomic transaction as the cascade deletes:
  each such child's FK field bytes are zeroed and, when the record is
  codec-shaped, the codec null bit is set (true NULL); index maintenance
  (`idx_maintain old→new`) updates equality/ordered/reverse-FK indexes so
  the child no longer references the gone parent. All-or-nothing with the
  rest of the delete.
- Deterministic (reverse lookup + content mutation over committed state);
  replicated convergence tested on a 3-node cluster.

## Scope / non-goals (honest)

- True NULL only for codec-shaped records; raw/opaque records get the field
  zeroed (no null bit) — documented, consistent with NOT NULL/FK scoping.
- Single-level (SET NULL doesn't recurse — it nulls the reference, by
  definition). No `ON UPDATE` actions or SET DEFAULT (future).

## Tests

`kessel-sm`: `on_delete_set_null_nulls_referencing_fk` (child survives, FK
decodes NULL, no longer indexed under the parent),
`on_delete_set_null_is_deterministic`. `kessel-vsr`:
`on_delete_set_null_replicates_and_converges`. 107 tests total green.
