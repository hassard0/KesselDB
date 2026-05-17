# KesselDB Sub-project 22 — GROUP BY aggregation

**Date:** 2026-05-17  **Status:** spec + build. Core Postgres analytics.

`Op::GroupAggregate { type_id, program, group_field, kind, agg_field }` —
over rows matching the VM `program`, group by `group_field`'s value and
compute `kind` (0 COUNT / 1 SUM / 2 MIN / 3 MAX) of `agg_field` per group.
Result: `[u32 ngroups]` then per group `[u32 keylen][key][16B i128 LE]`,
groups in ascending key order.

## Design

Filtered `scan_range` into a `BTreeMap<group-key-bytes, (count,sum,min,max)>`
— the BTreeMap gives deterministic ascending-key output for free. agg field
decode reuses the SP20 numeric ≤8B path. Read-only, deterministic,
txn-allowed, no catalog change.

## Non-goals (honest)

Single group field; numeric ≤8B agg field; no HAVING / multi-key grouping /
AVG (AVG = SUM/COUNT client-side). Full scan O(n).

## Tests

`kessel-sm`: `group_aggregate_sum_and_count_per_group` (SUM/COUNT/MAX per
group, ascending order), `group_aggregate_is_readonly_and_deterministic`.
`kessel-proto` round-trips. 113 tests total green.
