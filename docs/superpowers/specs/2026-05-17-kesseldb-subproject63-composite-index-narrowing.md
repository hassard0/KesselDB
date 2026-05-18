# KesselDB Sub-project 63 — composite-index candidate narrowing

**Date:** 2026-05-17  **Status:** shipped, oracle-tested. 160 green.
Production-feature-gap pass, slice 10. Safe by design — **no protocol /
replicated-op change.**

## The gap

SP62 made single-column equality on an indexed column accelerate mixed
WHEREs. But a multi-column equality (`WHERE a = 1 AND b = 2`) where the
columns are covered only by a **composite** index still fell back to a
full scan / per-field intersection — and if neither column was singly
indexed, no acceleration at all.

## Why a *safe* slice (deliberately bounded)

A full range-index narrowing would require adding a range-hint to the
replicated `Op::QueryRows` shape — a protocol/determinism-sensitive
change unfit for a momentum slice. SP63 instead does composite narrowing
**inside the existing `Op::QueryRows`**, no new op, no wire change:

- Planner (`kessel-sql`): a `col = literal` mandatory-AND conjunct now
  also yields an equality hint when the column is a member of a
  composite index (not only when single-indexed). Hints still only come
  from a WHERE span with no `OR`/`NOT`/`(` (SP62 safety rule).
- Engine (`kessel-sm`, `Op::QueryRows`): after the per-field equality
  intersection, if the equality hints' field **set** exactly matches a
  composite index's field set, it builds the concatenated key in that
  index's declared order and does one `idx_lookup` on the composite —
  intersecting that id set into the candidates.

Correctness is unchanged and total: the composite lookup returns the
**exact** id set for the full equality tuple (a superset of true
matches); the full `WHERE` program still verifies **every** candidate, so
the result is byte-identical to a scan. `FindByComposite`'s correctness
is already established (SP27). Candidate emit order is unchanged
(`BTreeSet`-sorted by id), so determinism is untouched — confirmed by the
whole VSR/determinism corpus staying green.

## The oracle (strengthened, not just re-run)

`planner_equivalence_oracle` now also creates a `(k, v)` composite index
and adds two query shapes — exact composite equality `k = K AND v = M`
and composite-eq + extra range conjunct — to its randomized matrix
(**~480 randomized queries**, each asserted equal to an independent
brute-force filter). Passed before *and* after the change.

## Result

`SELECT * FROM t WHERE a = 1 AND b = 2` with a composite index on
`(a, b)` — neither column singly indexed — is now narrowed via the
composite index instead of a full scan, byte-identical result (verified
live + by the oracle). 160 green.

## Honest scope boundary

Composite narrowing requires the equality hints to cover the composite
index's field set **exactly** (KesselDB's composite index is one
concatenated key, not a prefix-scannable B-tree). Partial-prefix use and
range-index (`FindRange`) narrowing remain the named next perf
follow-up — and would need the careful, separately-designed replicated-op
change SP63 deliberately avoided. Correctness is already total for all
shapes (the program is the source of truth); this is purely about which
queries are *also* sub-linear.
