# KesselDB — Status

Honest milestone tracker. Updated every milestone. "Done" means code + tests committed and passing.

| Milestone | State | Notes |
|---|---|---|
| M0 — workspace + determinism seam | **done** | proto/io/sim crates; 13 tests green; determinism gate = 100 seeds × 2 runs identical |
| M1 — storage engine (LSM+WAL+recovery) | **done** | WAL+memtable+SSTable+compaction+manifest+crash recovery; 5 tests incl. property-vs-oracle & crash-recovery; Vfs seam added |
| M2 — catalog + codec + single-node SM | **done — CONDITIONAL GO** | thesis not refuted; group-commit added (37× win); see verdict below |
| M3 — VSR replication | **done (core) — hardening backlog listed** | crash-stop VSR: normal op, client table, view change w/ log recovery, state transfer, loss tolerance; 4 sim invariants green |
| M4 — cache + sharding + perf | in progress | cloud-scaling speculation |

## M3 VSR — done vs. hardening backlog (honest)

**Working & sim-tested (4 deterministic invariants green):** normal-case
replication, group-commit-compatible apply, exactly-once client table, primary
failover via view change with best-log selection, gap state transfer, retransmit
recovery. Tests: linearizable-vs-reference (single-client total order),
same-seed determinism, primary-crash → view-change → progress + survivor
convergence, convergence under 25% message loss.

**Explicit hardening backlog (NOT yet done — listed, not hidden):**
asymmetric network-partition matrix, disk corruption *during* a view change,
large randomized seed-corpus sweep (CI), real socket transport (currently
in-process deterministic bus only), cluster membership reconfiguration. These
are tracked for M3-hardening / later specs; the protocol is transport-agnostic
so the socket swap is mechanical.

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

### M2 single-node state machine (localhost, single-thread, 128B TB-equivalent record)

| Path | CREATE | GET |
|---|---|---|
| MemVfs, per-op (in-mem upper bound) | ~245K ops/s | ~589K ops/s |
| MemVfs, generalized (codec) | ~205K ops/s | — |
| DirVfs real fsync, **per-op** | **2,339 ops/s** | ~2.0M ops/s |
| DirVfs real fsync, **batch=1000 (group commit)** | **87,338 ops/s** | ~1.05M ops/s |

GET fast on DirVfs because post-flush data sits in OS-cached SSTables; the slower
MemVfs GET reflects the known O(#sstables) read path (no bloom filter yet, M4 work).

### M2 go/no-go verdict: CONDITIONAL GO

The spec's M2 gate asks: is the generalization cost fatal before we invest in VSR?

- **Generalization cost is NOT fatal.** Schema-driven codec records cost ~20% vs a
  raw fixed type (205K vs 245K create) — comfortably within the spec's ≥70%-of-kernel
  intent. The flexibility layer is cheap.
- **The real gap vs TigerBeetle (~1M+/s) was batching, not flexibility.** Naive
  per-op fsync = 2,339/s (purely fsync-bound: p50 395µs ≈ one Windows fsync).
  Adding TB-style **group commit** (one fsync per batch) took the durable path to
  **87,338/s — a 37× win** — with a single, well-understood change. With larger
  batches / parallel fsync / faster storage this scales further; the thesis that
  "schema flexibility at TB-class speed" is achievable is **supported, not refuted**,
  conditional on batched group commit (now implemented) and the remaining M4 perf
  work (bloom filters, zero-copy reads, level compaction).

Confirming evidence: with MemVfs (no real fsync) batch=1000 gives ~242K/s ≈ the
~245K/s per-op number — batching changes nothing in-memory. It only helps on real
disk (2,339 → 87,338). That isolates fsync as the *sole* bottleneck of the naive
path, exactly as the thesis analysis predicted.

**Decision:** proceed to M3 (VSR). The VSR primary will hand committed *batches* to
`StateMachine::apply_batch`, so replication and group commit compose naturally.

Benchmarks continue at M3/M4 (replicated, cache on/off) with explicit reasoned
cloud-scaling speculation — all localhost, never cloud-measured.
