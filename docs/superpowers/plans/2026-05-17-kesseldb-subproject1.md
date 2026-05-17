# KesselDB Sub-project 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline, batch with checkpoints — chosen because the operator is offline and pre-authorized autonomous execution). Steps use checkbox (`- [ ]`) syntax.

**Goal:** Build a fresh-Rust, schema-driven, deterministic OLTP object kernel that ports TigerBeetle's designs (determinism seam, LSM, WAL, VSR) and is benchmarked vs. baseline expectations.

**Architecture:** A Cargo workspace of focused crates. Everything above storage is a pure function over an injected clock/disk/network (`kessel-io`); production uses real I/O, the simulator uses a seeded fake. Schema is replicated state (object type 0). Single-node first (M0–M2), then VSR replication (M3), then benchmark/hardening + read cache + sharding groundwork (M4).

**Tech Stack:** Rust 1.95 stable (MSVC), Cargo workspace, `criterion` for benchmarks, property tests via `proptest`, no async runtime in the deterministic core (sync + injected I/O), GitHub private repo.

---

## File Structure

```
KesselDB/
  Cargo.toml                      # workspace
  README.md                       # status-honest overview
  docs/STATUS.md                  # milestone tracker (kept current)
  docs/ARCHITECTURE.md            # architecture + replication/sharding/caching notes
  crates/
    kessel-proto/   src/lib.rs    # op/request/response types, varint framing, ids
    kessel-io/      src/lib.rs    # Clock+Disk+Net traits; real + simulated impls
    kessel-storage/ src/lib.rs    # memtable, SSTable, WAL, manifest, recovery, LSM
    kessel-catalog/ src/lib.rs    # ObjectType/Field, schema registry, layout calc
    kessel-codec/   src/lib.rs    # record encode/decode from a schema (pure)
    kessel-sm/      src/lib.rs    # deterministic apply(state,op)->(state',reply)
    kessel-vsr/     src/lib.rs    # replica log, quorum, view change, client table
    kessel-cache/   src/lib.rs    # bounded read cache (M4)
    kessel-sim/     src/lib.rs    # seeded fault simulator + invariant oracle
    kessel-bench/   src/main.rs   # throughput/latency harness
    kesseldb/       src/main.rs   # node binary wiring real io + sm (+ vsr)
```

Each crate has one responsibility and is independently testable. `kessel-sm`, `kessel-catalog`, `kessel-codec` MUST contain zero I/O/clock/RNG.

---

## Milestone M0 — Workspace, determinism seam, scaffolding

### Task 0.1: Workspace + crates skeleton
- [ ] Create workspace `Cargo.toml` listing all crates as members.
- [ ] `cargo new --lib` each crate; wire path deps per the dependency table in the spec.
- [ ] `cargo build` succeeds; commit.

### Task 0.2: `kessel-proto` core types
- [ ] Define `ObjectId([u8;16])`, `TypeId(u32)`, `OpNumber(u64)`, `ClientId(u128)`.
- [ ] Define `Op` enum: `CreateType`, `AlterTypeAddField`, `Create`, `Update`, `Delete`, `GetById`.
- [ ] Define `OpResult` enum incl. deterministic app errors `Exists`, `NotFound`, `SchemaError`.
- [ ] Little-endian explicit (de)serialization helpers + round-trip unit test. Commit.

### Task 0.3: `kessel-io` trait seam
- [ ] Traits: `Clock` (`now_nanos`), `Disk` (`read_block`/`write_block`/`sync`/`len`), `Net` (`send`/`recv`).
- [ ] Real impls: `SystemClock`, `FileDisk` (std::fs, fixed block size), `TcpNet`.
- [ ] Simulated impls: `SimClock`, `MemDisk` (with torn-write + corruption injection), `SimNet` (drop/dup/reorder/delay) — all seeded by `u64`.
- [ ] Unit test: same seed → identical `SimClock`/`SimNet` sequences. Commit.

### Task 0.4: `kessel-sim` skeleton + determinism gate
- [ ] Harness that boots N in-process nodes over `SimNet`/`MemDisk`/`SimClock` from a seed, runs no ops, hashes a trace.
- [ ] Test: 100 seeds, each run twice → identical trace hash (M0 exit gate). Commit.

---

## Milestone M1 — Storage engine (LSM + WAL + recovery)

### Task 1.1: WAL
- [ ] Frame `(op_number u64, kind u8, type_id u32, payload, crc32c u32)`; append + iterate.
- [ ] Test: write N frames, reopen, replay yields identical sequence; truncated/corrupt tail detected & stopped at last good frame (over `MemDisk`). Commit.

### Task 1.2: Memtable + SSTable
- [ ] Memtable: sorted map keyed by `type_id‖primary_id` → record bytes / tombstone.
- [ ] SSTable: fixed-size blocks, sorted, bloom filter, footer index; flush memtable→SSTable; point-get.
- [ ] Property test: random insert/get/delete vs. `BTreeMap` reference oracle. Commit.

### Task 1.3: LSM + compaction + recovery
- [ ] Levels with size-tiered compaction; latest-version-wins; tombstone reclamation.
- [ ] Manifest file tracks live SSTables; recovery = load manifest + replay WAL tail.
- [ ] Sim test: crash at random points (MemDisk torn writes) → recovery converges to last durable acked state. Commit.
- [ ] **M1 exit gate:** crash-recovery sim passes; record standalone write/read throughput in `docs/STATUS.md`.

---

## Milestone M2 — Catalog + codec + single-node state machine

### Task 2.1: `kessel-catalog`
- [ ] `Field{field_id,name,kind,offset,width,nullable}`, `FieldKind` enum (ints, Fixed(scale), Char(len), Bytes(len), Timestamp, Ref, OverflowRef-reserved).
- [ ] `ObjectType` with pure `compute_layout()` → offsets/record_size (power-of-two pad, null bitmap header). Catalog = object type 0, bootstrap layout.
- [ ] Property test: layout is a pure function of field list; stable across runs/platforms. Commit.

### Task 2.2: `kessel-codec`
- [ ] `encode(schema, fields)->Vec<u8>` / `decode(schema, bytes)->fields`; null bitmap; LE ints; per-record `schema_ver` header; up-projection of added nullable fields.
- [ ] Property test: round-trip across random schemas + versions. Commit.

### Task 2.3: `kessel-sm` single-node
- [ ] `State` = catalog + storage handle. `apply(&mut State, Op)->OpResult`, pure over injected storage; no clock/RNG (timestamps arrive in the op).
- [ ] Implement `CreateType`, `AlterTypeAddField`, `Create` (Exists guard), `Update` (NotFound guard, whole-record), `Delete` (tombstone), `GetById`.
- [ ] Sim test: op stream linearizable vs. in-memory reference model; convergence of state hash on replay. Commit.

### Task 2.4: `kesseldb` single-node binary + M2 benchmark
- [ ] Binary: real `FileDisk`/`SystemClock`, request loop applying batched ops.
- [ ] `kessel-bench`: workload generator (TB-equivalent single fixed type ≈128B; + generalized multi-field schema); reports throughput + p50/p99/p99.99, batched/unbatched.
- [ ] **M2 exit gate (early go/no-go):** record single-node numbers in `docs/STATUS.md` with an honest scaling speculation for cloud. Commit.

---

## Milestone M3 — VSR replication

### Task 3.1: Replica log + roles + quorum
- [ ] `Replica{role, view, op_log, commit_number, client_table}`; primary assigns op_number + deterministic timestamp.
- [ ] Prepare / PrepareOk / Commit messages over `Net`; f+1 quorum; backups apply in op-number order.
- [ ] Sim test (no faults): 3 nodes converge; client sees linearizable results. Commit.

### Task 3.2: View change + state transfer + client table
- [ ] View-change protocol on primary timeout; new primary recovers log; lagging replica state transfer.
- [ ] Client table dedups retried client batches (exactly-once apply).
- [ ] Sim test with full fault model (partition/crash/restart/reorder/corrupt) across a large seed corpus → convergence + linearizability + durability. Commit.
- [ ] **M3 exit gate:** fault-injection corpus green; failing seeds frozen as regressions.

---

## Milestone M4 — Benchmark, read cache, sharding groundwork, hardening

### Task 4.1: `kessel-cache`
- [ ] Bounded LRU read cache keyed by `type_id‖id`; invalidated on Update/Delete via state machine hook; behind a feature flag (off path stays deterministic).
- [ ] Test: cache never returns stale after Update/Delete; hit/miss metrics. Commit.

### Task 4.2: Sharding groundwork
- [ ] `ShardMap`: deterministic key→shard (rendezvous/hash on `type_id‖id`); single-shard today, interface ready for multi-shard routing. Document multi-shard cross-shard-txn limitation in ARCHITECTURE.md.
- [ ] Test: shard assignment stable & balanced. Commit.

### Task 4.3: Replicated benchmark + scaling speculation
- [ ] `kessel-bench` replicated mode (3-node, in-process SimNet + a real-socket localhost mode).
- [ ] Record throughput/latency single-node vs 3-node, cache on/off; write a reasoned cloud-scaling speculation (network RTT, fsync, batch size sensitivity) in `docs/STATUS.md`.
- [ ] **M4 exit gate:** numbers + honest gap analysis vs. TigerBeetle-class expectations recorded.

---

## Cross-cutting

- **Docs kept current:** every milestone updates `docs/STATUS.md` (done/in-progress/not-started, real numbers) and `README.md`. `docs/ARCHITECTURE.md` covers replication, sharding, caching design.
- **GitHub:** private repo `hassard0/KesselDB`, push after each milestone.
- **Honesty rule:** STATUS.md and README state exactly what is implemented and tested vs. roadmap. No claimed completeness beyond what tests prove.

## Self-Review notes

- Spec coverage: M0 seam ✓, M1 storage ✓, M2 catalog/codec/SM + benchmark gate ✓, M3 VSR ✓, M4 cache+sharding+scaling ✓. Out-of-scope items (var-length, indexes, planner, constraints, WASM, destructive DDL) intentionally excluded; `OverflowRef` reserved in codec.
- Type consistency: `Op`/`OpResult` defined in `kessel-proto` (Task 0.2) and consumed unchanged by `kessel-sm` (2.3) and `kessel-vsr` (3.1); key encoding `type_id‖primary_id` consistent across storage (1.2), SM (2.3), cache (4.1), sharding (4.2).
- Placeholder scan: no TBD/TODO; tasks specify concrete types, tests, and gates.
