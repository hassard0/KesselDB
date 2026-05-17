# KesselDB — Status

Honest milestone tracker. Updated every milestone. "Done" means code + tests committed and passing.

| Milestone | State | Notes |
|---|---|---|
| M0 — workspace + determinism seam | **done** | proto/io/sim crates; 13 tests green; determinism gate = 100 seeds × 2 runs identical |
| M1 — storage engine (LSM+WAL+recovery) | **done** | WAL+memtable+SSTable+compaction+manifest+crash recovery; 5 tests incl. property-vs-oracle & crash-recovery; Vfs seam added |
| M2 — catalog + codec + single-node SM | in progress | early go/no-go benchmark gate |
| M3 — VSR replication | not started | hardest milestone (consensus from scratch) |
| M4 — cache + sharding + perf | not started | cloud-scaling speculation |

## What this is NOT (yet)

Out of scope for Sub-project 1 (each a later spec): variable-length overflow store, secondary
indexes, filtered scans, multi-index planner, built-in constraints, WASM triggers, destructive
ALTER/DROP, cluster membership reconfiguration, client SDKs.

## Performance log

### M1 standalone storage (localhost, single-thread, MemVfs in-memory, no real fsync, unoptimized)

- PUT: ~254,000 ops/s (128B records)
- GET: ~137,000 ops/s (128B records)

**Honest reading:** modest and far below TigerBeetle-class numbers — expected at M1
(unoptimized, single-thread, value-cloning hot path). The notable finding is GET < PUT:
`get()` is O(#sstables) with a binary search + full value clone per table and no bloom
filter. This is a known architectural debt earmarked for M4 perf work (bloom filters,
level compaction, zero-copy reads), recorded here rather than hidden. The first
*thesis-relevant* number is the M2 single-node state-machine benchmark.

Benchmarks continue at M2 (single-node SM) and M4 (replicated, cache on/off) with explicit
reasoned cloud-scaling speculation — all localhost, never cloud-measured.
