# KesselDB — Design Spec

**Date:** 2026-05-17
**Status:** Approved (design); pending spec review
**Scope of this document:** Sub-project 1 only. The full product vision is recorded as the North Star appendix for context, not as buildable scope.

---

## 1. Premise & the core tension

KesselDB ("made the Kessel Run in 12 parsecs") aims for **PostgreSQL-grade functional flexibility at TigerBeetle-grade speed**.

The honest constraint driving every decision: **TigerBeetle is fast *because* it is inflexible.** Its throughput comes from a hardcoded schema, a single-domain deterministic state machine, immutable records, static allocation, single-threaded core, io_uring, and large request batches. "Flexibility + that speed" is not a point — it is a tradeoff curve. This spec fixes KesselDB's chosen point on that curve and decomposes the work so the speed thesis is validated *before* the most expensive complexity is built.

## 2. Locked product decisions (the North Star target)

| Dimension | Decision |
|---|---|
| What it is | A schema-flexible OLTP kernel that generalizes TB's fixed record. Constrained by design: no SQL surface and no relational join engine, even though it gains a multi-index query planner |
| Build approach | **Fresh Rust codebase** that ports TigerBeetle's *designs* (VSR, LSM, deterministic state machine, static allocation, io_uring/batching, VOPR-style simulation) — not its code |
| Data model | Fixed-width core record **+ a separate variable-length overflow store** for arbitrary TEXT/BLOB |
| Schema lifecycle | **Online DDL as replicated operations** — schema is replicated consensus state |
| Access paths | ID lookup + user secondary indexes + arbitrary filtered scans + **multi-index intersection planner** (no SQL joins) |
| Write model | **Mutable: in-place UPDATE and DELETE** |
| Constraints | Built-in set (NOT NULL / UNIQUE / FK-ref / CHECK / balance-guard) **+ deterministic gas-metered WASM trigger hooks** |

**Scope reality:** the full North Star is a multi-year, from-scratch general-purpose deterministic database that reuses TB's hardest components in spirit. It must be decomposed. Approach C (fresh reimplementation) was chosen with explicit acknowledgement that it re-creates TB's most-tested moat (VSR, simulation, LSM) and is the slowest path to a trustworthy "we match TB" number.

## 3. Decomposition (build order)

Each sub-project gets its own spec → plan → build cycle:

1. **Schema-driven object kernel — with VSR** ← *this spec*
2. Variable-length overflow store
3. User secondary indexes (equality + range)
4. Query layer: filtered scans → multi-index intersection planner
5. Built-in constraint engine
6. Online DDL stress: destructive ALTER/DROP, on-disk schema evolution, migrations
7. Deterministic WASM trigger sandbox
8. Client/wire SDKs, ops tooling, simulation hardening at scale

## 4. Sub-project 1 — scope

**In scope:** schema catalog + generalized fixed-width record codec + `CreateType`/`AlterTypeAddField`/`Create`/`GetById`/`Update`/`Delete` + deterministic state machine + LSM storage + WAL + crash recovery + **Viewstamped Replication from scratch** + VOPR-style deterministic simulator + head-to-head benchmark vs vanilla TigerBeetle.

**Explicitly out of scope** (each its own later spec; on-disk format reserves space so they land without a format break): variable-length overflow store, secondary indexes, filtered scans, multi-index planner, built-in constraints, WASM triggers, destructive ALTER/DROP, cluster membership reconfiguration, client SDKs.

## 5. Architecture

### 5.1 The determinism seam (foundational)

Everything above storage is a **pure function over an injected clock, disk, and network**. Production injects real io_uring; the simulator injects a seeded, fault-injecting fake. The entire database runs deterministically from a single seed. This is the mechanism that makes a from-scratch VSR reimplementation *verifiable* rather than hopeful.

### 5.2 Crates

| Crate | Purpose | Depends on |
|---|---|---|
| `kessel-proto` | Wire format, op/request/response types, batched message framing | — |
| `kessel-io` | Trait abstracting clock + disk + network; real (io_uring) + simulated impls | proto |
| `kessel-storage` | LSM (memtable/SSTable/compaction) + WAL + crash recovery, single-writer, static-alloc | io |
| `kessel-catalog` | Replicated schema registry: object types, field layouts, versions | proto |
| `kessel-codec` | Encode/decode generalized fixed-width records from a catalog schema | catalog |
| `kessel-sm` | Deterministic state machine: applies committed ops | storage, codec, catalog |
| `kessel-vsr` | Viewstamped Replication: log, quorum, view change, state transfer, client table | io, proto, sm |
| `kessel-sim` | VOPR-style simulator: seeded time/network/disk fault injection over full stack | all |
| `kessel-bench` | Head-to-head throughput/latency harness vs vanilla TigerBeetle | proto |
| `kesseldb` | Binary: wires real io_uring + VSR + SM into a running node | all |

**Hard invariant:** `kessel-sm`, `kessel-catalog`, `kessel-codec` contain zero I/O, clock, or RNG. Only `kessel-io`, `kessel-storage`, `kessel-vsr` touch the outside world, always through the `kessel-io` trait.

### 5.3 Data flow

Write: client batch → `proto` decode → `vsr` (replicate, quorum, order, assign op-number + primary timestamp) → `sm.apply` → `codec` encode → `storage` (WAL append → memtable) → batched reply via client table.
Read (`GetById`): served by the state machine's consistent read path without a log entry.

## 6. On-disk format

### 6.1 Schema catalog

```
ObjectType { type_id: u32, name: char[32], schema_ver: u32, fields: [Field; N≤64], record_size: u16 }
Field { field_id: u16, name: char[32], kind: FieldKind, offset: u16, width: u16, nullable: bool }
FieldKind = U8|U16|U32|U64|U128 | I8..I128 | Fixed(scale) | Bytes(len) | Char(len)
          | Timestamp | Ref(type_id) | OverflowRef   // OverflowRef reserved, unused in Sub-project 1
```

The catalog is **stored as object type 0** with a hardcoded bootstrap layout. `CreateType`/`AlterTypeAddField` are VSR-log ops that mutate type 0 and bump `schema_ver`. Schema is therefore replicated, deterministic, and crash-consistent for free.

### 6.2 Record codec

Given `(type_id, schema_ver)` the codec computes a fixed offset table once and encodes/decodes a flat byte record — no per-row allocation, no reflection. A null bitmap (1 bit/nullable field) sits in a fixed header. `record_size`, offsets, padding are a **pure function of the field list** (identical on every replica). All integers little-endian explicitly (no host-endianness leakage). Records pad to a power-of-two size.

### 6.3 Storage layout

LSM key = `type_id (4B) ‖ primary_id (16B)`; value = encoded record. A type is a contiguous key range (sets up future range scans). SSTable blocks fixed-size. WAL frame = `(vsr_op_number, op_kind, type_id, payload, crc32c)`. Each record header stores its `schema_ver (2B)`; reads decode under that version then up-project to current (added nullable fields → null). Destructive ALTER/DROP deferred to Sub-project 6.

## 7. State machine & VSR

### 7.1 Op set (`kessel-sm`, pure `apply(state, op) → (state', reply)`)

| Op | Effect | Determinism note |
|---|---|---|
| `CreateType` / `AlterTypeAddField` | Mutate catalog, bump `schema_ver` | Offsets recomputed purely from field list |
| `Create(type_id, id, record)` | Insert if absent; else `Exists` | Caller supplies 128-bit `id`; engine never generates ids |
| `Update(type_id, id, record)` | Whole-record replace; else `NotFound` | Field-level patch deferred |
| `Delete(type_id, id)` | Tombstone; else `NotFound` | LSM tombstone, latest-wins |
| `GetById(type_id, id)` | Read-only consistent read | Served without a log entry |

Timestamps: the VSR primary stamps a deterministic monotonic op-timestamp at sequencing time and replicates it in the op; the state machine never reads a clock.

### 7.2 VSR (`kessel-vsr`, ported design)

Replica roles, op log, prepare/prepare-ok quorum, commit number, **view change**, **state transfer**, **client table** (exactly-once for retried client batches). Cluster size fixed at config (3 or 5). Membership reconfiguration out of scope. Recovery: replay WAL into LSM, then state-transfer reconcile against peers before serving.

### 7.3 Failure semantics

Application errors (`Exists`, `NotFound`) are **deterministic op results** returned to clients, consuming an op number so all replicas agree. Infrastructure faults (disk/network/crash) are handled by VSR/recovery and never reach the state machine as nondeterminism.

## 8. Testing strategy

The **simulator is the primary correctness argument**, not a supplementary test suite.

**`kessel-sim`** drives the entire stack (N replicas + clients + fake `kessel-io`) from one u64 seed, controlling simulated time and a fault model that drops/duplicates/reorders/delays messages, partitions the cluster, crashes/restarts replicas, and corrupts/tears/stalls disk I/O. One seed = one fully reproducible history, bit-for-bit on any machine. CI runs a fixed seed corpus + randomized seeds; every failing seed becomes a permanent committed regression.

**Continuously-checked oracle invariants:** linearizability vs an in-memory reference model; replica state convergence (byte-identical LSM per `(type_id, id)`); determinism (same seed → identical log, commit order, final state hash; nightly hash-diff job); durability (acked op survives crash+recovery); catalog safety (every record's `schema_ver` decodes).

**Test layers (boundary payoff — each crate testable alone):** `codec`/`catalog` property tests (round-trip, layout purity); `storage` standalone torn-write/recovery; `vsr` protocol tests with storage stubbed; `sm` pure unit/property tests; full-stack `kessel-sim` integrated runs.

## 9. Success criteria

**`kessel-bench`** drives identical workloads against KesselDB and a vanilla TigerBeetle node: (a) TB-equivalent shape (single fixed object type ≈ TB transfer size) and (b) a generalized-schema workload. Reports throughput + p50/p99/p99.99 latency, batched/unbatched, single-node and replicated.

**Gate:** replicated KesselDB sustains **≥ 70% of vanilla TigerBeetle throughput at comparable tail latency** on the TB-equivalent workload. Missing this is a recorded thesis finding (the honest cost of generalization), not a defect to paper over.

## 10. Milestones (internally gated)

| # | Milestone | Exit gate |
|---|---|---|
| **M0** | Determinism seam + scaffolding: all crates stubbed; `kessel-io` real+fake; `kessel-sim` runs a no-op cluster from a seed | Same seed → identical empty-run trace hash across 100 seeds |
| **M1** | Storage engine: LSM + WAL + crash recovery, single-writer, static-alloc | Torn-write/crash-recovery sim passes; standalone throughput recorded |
| **M2** | Catalog + codec + single-node SM (no VSR) | Linearizable vs reference model; **single-node KesselDB vs single-node TB benchmark recorded — first thesis read / go-no-go** |
| **M3** | VSR replication: log, quorum, view change, state transfer, client table; 3-node | Full fault model in `kessel-sim`: convergence + linearizability + durability hold across large seed corpus |
| **M4** | Replicated benchmark + hardening; regression seed corpus frozen | **≥70% of vanilla TB throughput at comparable tail latency**, OR documented thesis finding explaining the gap |

**Sequencing rule:** each exit gate green before the next milestone; any failing seed committed as a permanent regression first. **M2 is the early go/no-go** — if generalization cost already looks fatal there, stop and reassess before investing in the VSR reimplementation.

---

## Appendix A — North Star (context only, NOT Sub-project 1 scope)

The eventual product layers, atop the Sub-project 1 kernel: variable-length overflow store; user secondary indexes; arbitrary filtered scans; multi-index intersection query planner; mutable in-place writes with index maintenance; built-in constraint engine (NOT NULL / UNIQUE / FK-ref / CHECK / balance-guard); deterministic gas-metered WASM trigger sandbox (no syscalls/clock/rand); destructive online DDL with on-disk schema evolution; client/wire SDKs and ops tooling. All preserve the determinism seam and VSR consensus established here.
