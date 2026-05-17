# KesselDB — Status

Honest milestone tracker. Updated every milestone. "Done" means code + tests committed and passing.

| Milestone | State | Notes |
|---|---|---|
| M0 — workspace + determinism seam | not started | |
| M1 — storage engine (LSM+WAL+recovery) | not started | |
| M2 — catalog + codec + single-node SM | not started | early go/no-go benchmark gate |
| M3 — VSR replication | not started | hardest milestone (consensus from scratch) |
| M4 — cache + sharding + perf | not started | cloud-scaling speculation |

## What this is NOT (yet)

Out of scope for Sub-project 1 (each a later spec): variable-length overflow store, secondary
indexes, filtered scans, multi-index planner, built-in constraints, WASM triggers, destructive
ALTER/DROP, cluster membership reconfiguration, client SDKs.

## Performance log

No numbers recorded yet. Benchmarks land at M2 (single-node) and M4 (replicated). All numbers
will be localhost measurements with explicit, reasoned cloud-scaling speculation — not cloud
measurements.
