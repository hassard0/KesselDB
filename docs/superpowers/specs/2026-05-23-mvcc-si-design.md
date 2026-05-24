# S2 — Serializable MVCC / Snapshot Isolation over the deterministic VSR log: Parent Design

**Date:** 2026-05-23
**Status:** Approved (autonomous mandate — see Process Note)
**Strategic-tier item:** S2 of the THESIS.md S1–S4 backlog. **Parent** design
covering the full MVCC-SI scope and its decomposition into sub-slices
S2.1 through S2.6.
**Builds on:** THESIS.md (commit `457e1ce`); S1/SP109 (the
TLA+/TLC verifiable-behavior artifact whose discipline this slice
extends to MVCC); the existing deterministic kernel
(`kessel-sm::StateMachine::apply` + `kessel-storage::Storage::put/get`
as the implementation seams the new MVCC layer sits on top of).

---

## Process Note (autonomy + thesis sequencing)

Produced under the standing overnight autonomous-build mandate
(`feedback_kesseldb_autonomous_build` + the strategic-tier mandate
`feedback_kesseldb_strategic_tier`): "build the backlog autonomously,
BOLD choices, don't wait for approval, keep the two-stage review gate
+ full tests + pentest passes." **The brainstorming user-review gate
is satisfied by this documented decision record.** All other rigor
retained: two-stage subagent review per task (spec then artifact-
quality), a final whole-implementation review per sub-slice, and the
existing Rust gates (`cargo test --workspace --release`, seed-7) which
remain hard gates for every code-touching task.

**Strategic-tier sequencing.** THESIS.md names S1 (TLA+), S2 (MVCC),
S3 (Jepsen), S4 (WASM UDF). S1 is shipping as SP109 in parallel. S2
is the second-largest strategic-tier item. This document is **the
parent design for S2** — it covers the full scope and decomposes into
six sub-slices (S2.1–S2.6). The first sub-slice's implementation plan
(`docs/superpowers/plans/2026-05-23-mvcc-si-s2-1.md`) is committed in
the same docs slice as this design. Each subsequent sub-slice
(S2.2–S2.6) will get its own plan when the prior sub-slice lands.

---

## Problem

The thesis names **deterministic** + **replayable** as two of KesselDB's
three core properties. Today the kernel has a single-version key-value
store: `Storage::put(op_number, key, value)` overwrites the prior value
in the memtable, and `Storage::get(&key)` returns the latest. Two real
costs of this:

1. **Long-running reads conflict with writes.** A read that needs a
   consistent point-in-time view of multiple keys today must either (a)
   batch under a short-lived snapshot, (b) block writes, or (c) accept
   torn reads. The current single-version path forces option (c) for
   non-trivial queries: there is no way to express "read all rows of
   table T as of opnum N".

2. **No deterministic concurrent-write conflict resolution.** Two
   transactions modifying overlapping keys today either (a) serialize
   via outer coordination, or (b) the second write silently overwrites
   the first. Postgres-style "first committer wins" SI semantics —
   the second transaction aborts deterministically on every replica —
   are not expressible.

These costs are not blocking the existing SP/M slices, but they DO
block the thesis-defining application: **deterministic replicated SQL
with serializable semantics**. Without MVCC, `SELECT ... FOR UPDATE`,
`REPEATABLE READ`, and concurrent-`UPDATE` workloads cannot be
serialized correctly. With MVCC over the deterministic log, every
snapshot is a function of an agreed log prefix, every conflict
decision is a deterministic function of the log, and every replica
agrees by construction — no extra coordination protocol.

This S2 parent design lays out the full MVCC-SI architecture; sub-
slice S2.1 ships the **foundation primitive** (versioned key-value
layer + opnum-as-timestamp read path), and S2.2–S2.6 build the
transaction layer, conflict detection, SSI promotion, GC, and SQL
integration on top.

---

## Decisions (bold choices, documented)

### Decision 1 — Isolation level target: **SSI (Serializable Snapshot Isolation)**

The thesis sentence in THESIS.md S2 is "Serializable MVCC / Snapshot
Isolation". SSI (Cahill et al., 2008; PostgreSQL's serializable mode
implementation) **promotes plain SI to true serializability** by
tracking read-write dependencies and aborting transactions whose
dependency graphs would form a dangerous cycle. The path:

- **S2.1–S2.3** ship **plain SI**: snapshot reads + write-write
  conflict detection + first-committer-wins. This is enough to ship
  a useful product slice and is the substrate SSI extends.
- **S2.4** promotes plain SI to **SSI** by adding read-set tracking
  and the SSI dangerous-cycle detector. After S2.4 ships, the default
  isolation level becomes SSI (serializable).

**Why both, in this order.** Plain SI is the well-trodden first step
(every MVCC database — PostgreSQL, MySQL/InnoDB, CockroachDB —
implemented plain SI before adding SSI). Plain SI is independently
useful (it permits write-skew anomalies but covers most workloads).
SSI is the smaller delta on top of SI than from scratch. Shipping
both in one slice would balloon the slice; staging the promotion is
the discipline.

**Rationale (bold over safe).** Plain SI alone would be a thesis
under-claim: "Serializable" is in the thesis sentence. Shipping
through SSI is the bold target that matches the thesis verbatim.

**Thesis fit:** `deterministic` (SI/SSI conflict detection runs inside
the deterministic state machine `apply` path → every replica decides
identically). `replayable` (every snapshot is `max(commit_opnum) ≤
snapshot_opnum` over the log — replay the log, replay the snapshot).

### Decision 2 — Timestamp source: **log opnum (no wall-clock, no HLC)**

Three options weighed:

- **(a) Wall-clock timestamps.** Rejected: violates the determinism
  pillar (the `kessel-io` seam exists precisely to keep wall-clock
  reads out of the deterministic core). Wall-clock SI also has the
  clock-skew misordering problem (Spanner solves it with TrueTime;
  KesselDB has no TrueTime).

- **(b) Hybrid logical clock (HLC).** Rejected: HLCs combine a logical
  counter with a wall-clock component for cross-replica ordering. Use
  cases are distributed databases without a single ordered log
  (Cassandra, CockroachDB). KesselDB **has a single ordered log** —
  the VSR commit sequence — so the wall-clock component is
  unnecessary overhead.

- **(c, taken) Log opnum.** Every operation that reaches the state
  machine has a unique, monotonically-increasing `op_number` assigned
  by VSR. **Use the op_number as the commit timestamp directly.** A
  read snapshot is an opnum; "the version at snapshot S" is "the
  newest version whose `commit_opnum ≤ S`". A write's commit
  timestamp is the opnum the SM assigns at apply time.

**Why bold over safe.** The "snapshot is just an opnum" insight is
the thesis-fit angle: **deterministic ≠ slow**. Every commit gets a
unique timestamp for free (it's the same op_number the storage layer
already uses), every snapshot is replayable from the log prefix
≤ snapshot_opnum, and there is **no clock-skew failure mode** at all
because there is no clock.

**Architectural implication.** The "snapshot opnum" picked at read
time is the **latest committed opnum the replica has seen**. Reads
on different replicas at the same wall-clock instant may pick
slightly different snapshots — that's the SI permission to lag,
not a divergence — because every snapshot is internally consistent
(reads max(commit_opnum) ≤ snapshot is deterministic given the log
prefix). The thesis-fit phrasing: **MVCC reads are bounded-staleness
linearizable; writes remain strictly linearizable via VSR commit
order.**

**Thesis fit:** `deterministic` (timestamps from the log, not from
wall-clocks); `replayable` (every snapshot = log prefix function).

### Decision 3 — Version storage: **append-only versions in the same kessel-storage LSM, keyed by `(key, commit_opnum)`**

The current `kessel-storage::Storage` is an LSM (memtable + immutable
SSTables + compaction) over the WAL/VFS. Three storage options for
MVCC:

- **(a) In-place updates with undo log (Postgres-style).** The current
  value is updated in place; an undo entry records the prior value.
  Rejected: requires a separate undo-log substrate; doesn't fit the
  append-only LSM shape; vacuum/undo-truncation is a separate concern.

- **(b) Append-only versions with version chains (InnoDB-style).** Each
  row has a linked list of versions. Rejected: the linked-list shape
  doesn't compose with LSM range scans, and the chain length becomes a
  read-amplification cost.

- **(c, taken) Append-only versions keyed by `(key, commit_opnum)` in
  the LSM, descending-opnum-first ordering.** Versions live in the
  existing kessel-storage LSM. The key encoding extends the current
  `make_key(type_id, object_id)` shape to `make_versioned_key(type_id,
  object_id, commit_opnum_inverted)`. Inverting the opnum (e.g., as
  `u64::MAX - commit_opnum`) makes the most-recent version sort
  **first** in the lexicographic LSM key order, so a snapshot read at
  opnum S is a **single seek + scan** down to the first version with
  `commit_opnum ≤ S`. Time-travel queries (read as of past opnum) are
  free — the version is already in the LSM, possibly compacted into
  an SSTable.

**Why bold over safe.** The "extend the key with inverted opnum, sort
versions by recency" is the FoundationDB-style trick and composes
naturally with the existing LSM substrate. **No new physical store,
no new file format, no new compaction logic.** The existing
crash-recovery + WAL + SSTable code keeps working unchanged. GC of
old versions is a separate, deterministic op (Decision 6) that simply
range-deletes obsolete `(key, opnum)` entries — also natural for LSM
tombstones.

**The detailed key encoding (S2.1):**
- Current: `type_id (4 LE) ++ object_id (16)` = 20-byte data key.
- MVCC-shape: `type_id (4 LE) ++ object_id (16) ++ inverted_opnum (8 BE)`
  = 28-byte versioned key. `inverted_opnum = u64::MAX - commit_opnum`
  (BE-encoded so lex-order matches numeric order). Reads for a given
  (type_id, object_id) prefix yield versions newest-first; the first
  version with `commit_opnum ≤ snapshot_opnum` is the snapshot read.

**Backward compatibility migration.** Existing single-version paths
(SP1–SP108) use 20-byte keys. The MVCC layer sits **as a new module
inside kessel-storage** with its own API (`put_versioned`,
`get_at_snapshot`) and its own keyspace shape (28-byte keys). Old
20-byte-key paths remain byte-identical. The SQL/SM cutover to MVCC
is **S2.6**, which is the slice that flips the SM's data writes from
20-byte to 28-byte keys. Pre-S2.6, MVCC is dormant code; the legacy
gate is byte-net-0 (no Rust path changes), only new tests of the new
module run.

**Thesis fit:** `deterministic` (same log prefix → same version chain
on every replica); `zero-dep` (reuses existing LSM; no new external
crate).

### Decision 4 — Conflict detection: **at SM-apply time (deterministic by construction)**

The headline thesis-fit insight of S2:

> **In a deterministic state machine fed by a totally-ordered log,
> conflict detection is a function of the log prefix.** Every replica
> sees the same log in the same order; every replica runs the same
> `apply(op_number, Op::Commit { write_set, snapshot_opnum })` and
> reaches the same conflict verdict. **No distributed conflict-
> resolution coordination is required.** Compare to non-deterministic
> systems (Spanner: TrueTime + Paxos per shard; CockroachDB: HLC + the
> "txn record" coordination protocol) — KesselDB sidesteps this entire
> class of complexity because the log already orders the conflict
> checks.

The mechanics:

- **Transaction lifecycle.** Begin: client picks `snapshot_opnum`
  (the latest committed opnum the replica has seen at request time).
  Reads: go through `get_at_snapshot(key, snapshot_opnum)` — local,
  no consensus. Buffer writes client-side. Commit: client submits
  `Op::CommitTxn { snapshot_opnum, read_set, write_set }` to VSR.

- **Conflict check (plain SI; S2.3).** When SM applies the commit op:
  for every key in `write_set`, scan versions of that key in
  `(snapshot_opnum, current_opnum)`; if any version exists in that
  range, **the snapshot has been invalidated by an intervening
  commit** → abort the transaction (first-committer-wins). Otherwise:
  install the new versions at `commit_opnum = current_opnum`.

- **Conflict check (SSI; S2.4).** Extends the above with read-set
  tracking. SSI's "dangerous structures" (rw-antidependencies forming
  cycles) are detected via the Cahill SSI algorithm: each in-flight
  transaction tracks its incoming/outgoing rw-antidependency edges,
  and a transaction commits only if its edges don't form a dangerous
  pattern. The SSI bookkeeping is itself part of the SM state and
  thus deterministic across replicas.

**Why bold over safe.** The "deterministic apply IS the conflict
resolver" framing is the headline thesis-fit insight of S2. It is
the property that lets KesselDB claim "consensus + SQL can be simpler
than MVCC-centric systems" (the THESIS.md S2 framing verbatim).
Documenting it prominently is required.

**Honest disclosure.** This works only because **the snapshot_opnum
the client used IS itself committed by the time the commit op
applies** (the client sends the snapshot_opnum as part of the commit
payload; every replica sees that value as part of the log entry; the
range-scan check `(snapshot_opnum, current_opnum)` is a deterministic
function of the log prefix). If a replica receives the commit op
before its locally-applied opnum reaches `snapshot_opnum`, the apply
**stalls until the replica's apply cursor reaches `snapshot_opnum`**.
This is the natural "wait for the prefix you depend on" pattern, and
it terminates because VSR delivers entries in commit order. Document
this in S2.3.

**Thesis fit:** `deterministic` (conflict resolution is a function of
the log prefix); `verifiable` (the SSI invariant is mechanically
checkable via TLA+ as an extension of `Replication.tla` — see
Decision 7).

### Decision 5 — Read path: **local snapshot read, no consensus, no blocking**

Snapshot reads are **read-only**: they do not need to replicate, do
not need a quorum, do not modify any state. The natural seam:

- **Local API (kessel-storage::Storage):** `get_at_snapshot(key,
  snapshot_opnum) -> Option<Vec<u8>>` (S2.1's headline API). Reads
  the LSM at the (key, inverted_opnum) prefix, returns the first
  version with `commit_opnum ≤ snapshot_opnum`.

- **SM API (kessel-sm::StateMachine):** `read_at_snapshot(snapshot,
  Op::ReadXxx)` mirrors the existing `apply(op_number, Op)` shape
  but is **non-mutating** and takes a snapshot opnum instead of a
  commit opnum. Wraps `Storage::get_at_snapshot` plus any catalog
  decoding.

- **Client API (SQL, S2.6):** `SELECT` queries pick `snapshot_opnum =
  max_committed_opnum` at request start, hold that snapshot through
  the query's lifetime, never re-pick. `SELECT FOR UPDATE` and
  read-modify-write workloads use the transaction lifecycle from
  Decision 4.

**Why bold over safe.** Reads never block writes; writes never block
reads. The classic SI promise. The implementation is **trivially
correct given Decision 3's storage shape** (the version chain is
append-only, and reads only look at the prefix `commit_opnum ≤
snapshot`, which is monotonically stable — concurrent writes only
append later versions).

**Latency note.** Snapshot reads are **strictly faster** than reads
through VSR consensus: no quorum round-trip, no log append, no apply
gating. This is the read-amplification offset for the small write-
amplification cost of multi-version storage. The product of these two
effects — fast reads, modest write overhead — is the standard SI
tradeoff and is what the thesis bets on.

**Thesis fit:** `deterministic` (the snapshot opnum + log prefix
uniquely determine the read result); `replayable` (re-running the log
to a given opnum reproduces every snapshot read).

### Decision 6 — Garbage collection: **`Op::AdvanceWatermark(low_water_mark_opnum)` — a deterministic SM op**

Old versions accumulate; without GC, the version chain grows
unboundedly. Three options:

- **(a) Background non-deterministic GC.** Rejected: violates
  determinism. A GC thread reclaiming versions on its own schedule
  produces replica divergence.

- **(b) Per-read in-line GC.** Rejected: reads must be pure (no side
  effects); in-line GC would couple the read path to side effects and
  break the "reads never block writes" promise.

- **(c, taken) `Op::AdvanceWatermark(low_water_mark_opnum)` — a
  deterministic SM op submitted through VSR.** The replica with the
  oldest active read snapshot reports its snapshot opnum (or the
  primary tracks it via a heartbeat); the primary periodically
  submits an `Op::AdvanceWatermark(min_active_snapshot)` op. When SM
  applies this op, it range-deletes every version of every key whose
  `commit_opnum < low_water_mark AND a newer version exists with
  commit_opnum ≤ low_water_mark`. (The "newest version ≤ watermark"
  is preserved so future reads at the watermark still find a version
  for every key that existed then.)

**Why bold over safe.** GC is **deterministic by construction**: the
`AdvanceWatermark(N)` op produces the same set of deletions on every
replica that applies it. The watermark itself is computed
deterministically by the primary (e.g., min across known active
snapshots), passed in the op, and applied identically.

**Subtlety.** The min-active-snapshot computation needs the active
snapshot opnums from all replicas. The primary collects these via
heartbeat messages (an extension of the existing `Ping`/`Pong`
infrastructure). The collected value goes INTO the op, so every
replica sees the same watermark — divergence-proof.

**S2.5 ships this:** the watermark protocol + the `AdvanceWatermark`
op + the SM apply path + tests that confirm version count drops after
GC + a pentest that hostile watermark values (i64::MAX, behind-
current-snapshots) can't break the invariant.

**Thesis fit:** `deterministic` (GC is a log op); `replayable` (GC
decisions are functions of the log prefix).

### Decision 7 — TLA+ verification: **extend `Replication.tla` per sub-slice (S1 discipline applied to MVCC)**

S1/SP109 ships a TLA+ spec of the VSR replication protocol. S2's
thesis-fit alignment is that **every MVCC sub-slice ships its own
TLA+ extension** to model and check the slice's safety property:

- **S2.1** — TLA+ extension `MVCCStorage.tla` modeling the versioned
  key-value layer; invariant: **SnapshotReadConsistency** ("for every
  key K and snapshot S, `get_at_snapshot(K, S)` returns the version
  with `max(commit_opnum) ≤ S` among versions of K").
- **S2.2** — TLA+ extension `MVCCTxn.tla` modeling Tx
  start/read/commit-buffer; invariant: **ReadSetCoherence** ("every
  read in a Tx returns the snapshot's version").
- **S2.3** — TLA+ extension `MVCCSi.tla` modeling SI commit; invariant:
  **FirstCommitterWins** ("if two Txs' write_sets overlap, exactly one
  commits"). **DeterministicAbort** ("every replica reaches the same
  commit/abort verdict for every Tx").
- **S2.4** — TLA+ extension `MVCCSsi.tla` modeling SSI dangerous-
  cycle detection; invariant: **Serializable** ("committed schedule
  has a serial equivalent").
- **S2.5** — TLA+ extension `MVCCGc.tla` modeling watermark
  advancement; invariant: **GcSafety** ("no version with commit_opnum
  ≥ any active snapshot is reclaimed").
- **S2.6** — SQL integration; the TLA+ work is the
  **SQL→MVCC-op mapping table** (named correspondence, not mechanized
  refinement, per S1/SP109 Decision 5).

This is the S1 discipline applied to MVCC: every sub-slice ships a
mechanically-checkable artifact alongside the Rust code. The
TLC rigor-checkpoint cadence rule (from SP109) extends to MVCC: any
change to the MVCC apply path requires the MVCC TLA+ specs to re-pass
TLC before merge.

**Why bold over safe.** Doing TLA+ for "the entire MVCC stack" up
front would be a 1000-line spec that took months. Per-sub-slice
extensions are the manageable cadence and align with the rigor-
checkpoint pattern S1 established.

**Thesis fit:** `verifiable` (every MVCC sub-slice ships a TLA+
artifact); `honest-docs` (the model-vs-implementation gap is the
same gap S1 disclosed; same disclosure language carries over).

### Decision 8 — Backward compatibility: **MVCC as a parallel module; SM cutover in S2.6**

The existing `kessel-sm::StateMachine` writes single-version data via
`Storage::put(op_number, key, value)`. The cutover to MVCC is non-
trivial: every SP1–SP108 path writes through this API, and every
deterministic-byte-identity gate (V1 KAT, the per-binary tests, the
seed-7 simulation) bytes-equals across replicas only because the
storage is single-version.

The bold-but-safe migration path:

- **S2.1–S2.4: MVCC ships as a NEW MODULE inside `kessel-storage`**
  with its own API (`put_versioned`, `get_at_snapshot`,
  `get_versions_in_range`). The 20-byte single-version key path is
  untouched. The cargo gate stays net-0 for SP1–SP108 paths; only the
  NEW MVCC tests + the NEW TLA+ artifacts add to the gate.

- **S2.5: GC ships as a NEW Op variant** (`Op::AdvanceWatermark`).
  SM rejects this op for the existing data store; only the MVCC
  store accepts it.

- **S2.6: SM cutover.** The SM switches its data writes from
  `Storage::put` to `Storage::put_versioned`. Catalog writes (object
  type 0) stay single-version (they don't benefit from MVCC, and the
  cutover would force a catalog migration). Index writes stay single-
  version for the first SQL-MVCC cut; S2.6.X may extend MVCC to
  indexes if needed. **The SM cutover is the slice where the
  byte-identity gate explicitly changes** — a "single-version → MVCC"
  bytes-on-disk migration is honest-disclosed in S2.6's record.

**Why bold over safe.** Cutover-in-one-slice (the bolder option) was
considered and rejected: the migration risk is too high (a single
slice would touch the SM apply path AND every catalog/index path AND
the recovery path simultaneously). The parallel-module-then-cutover
path keeps every intermediate slice small, byte-net-0 for legacy
paths, and individually-reviewable.

**Migration spell-out (S2.6 will execute this).** The S2.6 slice will:
(a) write a one-shot migration that re-keys all existing single-
version data into 28-byte MVCC keys at `commit_opnum = high_op_at_
migration`; (b) cut the SM's `Storage::put` calls to
`Storage::put_versioned`; (c) update every test that asserts on
single-version keys to assert on MVCC keys; (d) honest-disclose the
byte-identity-gate change; (e) ship a "migrate on first boot if
manifest indicates legacy" path.

**Thesis fit:** `honest-docs` (the cutover-is-a-disclosed-byte-change
discipline); `deterministic` (migration is itself a deterministic op
keyed on `high_op`).

---

## Architecture

### High-level layering

```
                  +---------------------------+
                  |  kessel-sql (SQL surface) |   <-- S2.6
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-sm (StateMachine) |   <-- S2.2 / S2.3 / S2.4 / S2.6
                  |  + Tx context + commit op |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage MVCC      |   <-- S2.1
                  |  (put_versioned,          |
                  |   get_at_snapshot,        |
                  |   gc_below_watermark)     |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage LSM       |   (existing, unchanged)
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-io (VFS seam)     |   (existing, unchanged)
                  +---------------------------+
```

### Key encoding (the foundation S2.1 ships)

- **Legacy single-version key (20 bytes):**
  `type_id (4 LE) ++ object_id (16)`
- **MVCC versioned key (28 bytes):**
  `type_id (4 LE) ++ object_id (16) ++ inverted_commit_opnum (8 BE)`
  where `inverted_commit_opnum = u64::MAX - commit_opnum`. BE encoding
  + inversion makes newest-version-first the natural lex order.

A lookup at `(type_id, object_id, snapshot_opnum)`:
1. Seek to `prefix = type_id ++ object_id ++ 00..00` (the smallest
   28-byte key with that 20-byte prefix; equivalent to the newest-
   possible version).
2. Scan forward (i.e., descending commit_opnum) until the first key
   whose `decode_commit_opnum(k[20..28]) ≤ snapshot_opnum`.
3. Return that key's value. None if no such key exists.

A tombstone is encoded by storing `Option<Vec<u8>>::None` at the
versioned key — the existing storage layer already supports `None`
values (see `kessel-storage::Storage::commit` which preserves them).

### New module: `kessel-storage::mvcc`

A new sub-module inside the kessel-storage crate (NOT a new crate)
keeps the dep graph unchanged and the cargo-tree net-0 for legacy
paths. Public API:

```rust
pub mod mvcc {
    use crate::{Key, Storage};
    use kessel_io::Vfs;

    /// Build a 28-byte MVCC versioned key.
    pub fn make_versioned_key(
        type_id: u32,
        object_id: &[u8; 16],
        commit_opnum: u64,
    ) -> Vec<u8>;

    /// Decode the commit_opnum out of a 28-byte versioned key (errors
    /// on a 20-byte legacy key).
    pub fn decode_commit_opnum(key: &[u8]) -> Result<u64, MvccKeyError>;

    /// Write a new version at the SM-supplied commit_opnum.
    /// Append-only: prior versions of the same (type_id, object_id)
    /// remain in the store until GC.
    pub fn put_versioned<V: Vfs>(
        store: &mut Storage<V>,
        type_id: u32,
        object_id: &[u8; 16],
        commit_opnum: u64,
        value: Option<Vec<u8>>,    // None == tombstone
    ) -> std::io::Result<()>;

    /// Snapshot read: returns the newest version of (type_id, object_id)
    /// with commit_opnum <= snapshot_opnum. Returns Ok(None) for "no
    /// version visible at snapshot" (either the key was never written
    /// before snapshot, OR the latest pre-snapshot version is a
    /// tombstone — the two cases are distinguished by an inner enum).
    pub fn get_at_snapshot<V: Vfs>(
        store: &Storage<V>,
        type_id: u32,
        object_id: &[u8; 16],
        snapshot_opnum: u64,
    ) -> SnapshotRead;
}

pub enum SnapshotRead {
    Found(Vec<u8>),         // newest visible version with content
    Tombstoned,             // newest visible version is a deletion
    NotYetWritten,          // no version with commit_opnum <= snapshot
}

pub enum MvccKeyError {
    Length(usize),          // not 28 bytes
    InversionMalformed,
}
```

S2.1 ships precisely this module + tests + the TLA+ extension.
S2.2–S2.6 extend it.

### State-machine seams

Today's `StateMachine::apply(op_number, op)` is the single SM entry
point. S2 introduces two new entry points (introduced incrementally
across sub-slices, not in S2.1):

- `StateMachine::read_at_snapshot(snapshot_opnum, read_op) -> ReadResult`
  (S2.2) — non-mutating; reads through `mvcc::get_at_snapshot`.

- `StateMachine::apply(commit_opnum, Op::CommitTxn { snapshot_opnum,
  read_set, write_set })` (S2.3 for plain SI; extended in S2.4 for SSI)
  — the conflict-detection seam. Returns
  `OpResult::TxnCommitted | OpResult::TxnAbortedConflict`.

The legacy `apply` paths (Op::Create, Op::Update, etc., from SP1–
SP108) continue to call `Storage::put` directly through S2.1–S2.5;
the SM cutover in S2.6 reroutes them through `mvcc::put_versioned`.

### Conflict-detection seam (S2.3 + S2.4)

The `Op::CommitTxn` apply path:

```rust
fn apply_commit_txn(
    &mut self,
    commit_opnum: u64,
    snapshot_opnum: u64,
    read_set: &[ReadIntent],     // SSI uses this; SI ignores
    write_set: &[WriteIntent],
) -> OpResult {
    // Plain SI (S2.3) — write-write conflict:
    for w in write_set {
        // Look for any version of w.key in the half-open range
        // (snapshot_opnum, commit_opnum). Existence ⇒ abort.
        if mvcc::has_version_in_range(
            &self.storage, w.type_id, &w.object_id,
            snapshot_opnum + 1, commit_opnum,
        ) {
            return OpResult::TxnAbortedConflict;
        }
    }
    // SSI (S2.4) — read-write antidependency cycle:
    if self.ssi_state.would_form_dangerous_cycle(read_set, write_set) {
        return OpResult::TxnAbortedConflict;
    }
    // Install versions at commit_opnum.
    for w in write_set {
        mvcc::put_versioned(
            &mut self.storage, w.type_id, &w.object_id,
            commit_opnum, w.value.clone(),
        )?;
    }
    self.ssi_state.record_committed(commit_opnum, read_set, write_set);
    OpResult::TxnCommitted
}
```

The deterministic-conflict-resolution property: every replica running
this code over the same log prefix reaches the same `OpResult`. The
`ssi_state` is part of the SM state and is itself part of the
replicated state (its mutations are deterministic functions of the
applied ops).

### GC seam (S2.5)

```rust
fn apply_advance_watermark(
    &mut self,
    commit_opnum: u64,
    low_water_mark: u64,
) -> OpResult {
    // For every (type_id, object_id) with at least one version with
    // commit_opnum <= low_water_mark, retain the NEWEST such version
    // and tombstone (i.e., LSM-delete the key) any older versions.
    self.mvcc_gc.reclaim_below(&mut self.storage, low_water_mark)?;
    self.mvcc_gc.last_watermark = low_water_mark;
    OpResult::WatermarkAdvanced
}
```

The reclaim is implemented as a range scan over each (type_id,
object_id) prefix in the LSM, finding versions with commit_opnum <
low_water_mark, keeping the most recent, and issuing
`Storage::commit(Entry { value: None, ... })` for the rest. This
reuses the LSM's tombstone mechanism — compaction eventually drops
the tombstones.

### TLA+ integration point (S1 discipline extended)

The S1 `Replication.tla` models the log itself. The MVCC TLA+ specs
sit ABOVE this model:

```
kesseldb-tla/
├── Replication.tla            (S1/SP109; the log itself)
├── Replication.cfg
├── MVCCStorage.tla            (S2.1; versioned storage)
├── MVCCStorage.cfg
├── MVCCTxn.tla                (S2.2; transaction lifecycle)
├── MVCCTxn.cfg
├── MVCCSi.tla                 (S2.3; plain SI commit)
├── MVCCSi.cfg
├── MVCCSsi.tla                (S2.4; SSI dangerous cycles)
├── MVCCSsi.cfg
├── MVCCGc.tla                 (S2.5; watermark GC)
├── MVCCGc.cfg
└── results/
    ├── 2026-05-19-baseline.txt    (S1/SP109)
    ├── 2026-05-23-mvcc-storage-baseline.txt   (S2.1)
    └── ...
```

Each MVCC TLA+ spec EXTENDS the relevant prior spec (per Lamport's
TLA+ module composition) so the MVCC invariants are checked over the
same VSR log model that S1 verified, not over a redundant log-protocol
model. The cumulative TLC runtime stays tractable because each
extension adds bounded MVCC variables and bounded actions.

---

## The MVCC contract (formal)

The contracts each sub-slice of S2 establishes. Together they
constitute the S2 deliverable.

### S2.1 contract — versioned storage layer

**Snapshot read consistency.** For every key `K` and every snapshot
opnum `S`:
```
get_at_snapshot(K, S) = the version v of K with commit_opnum(v)
                       = max { commit_opnum(v') : v' is a version of K
                               AND commit_opnum(v') <= S }
                       OR NotYetWritten if no such version exists
                       OR Tombstoned if max-version is None.
```

**Append-only invariant.** `put_versioned(K, c, v)` does NOT modify
or overwrite any prior version of `K`; it appends a new entry at
`(K, c)`. The version chain only grows (until GC, S2.5, prunes it).

**Determinism invariant.** Two replicas with the same applied log
prefix have byte-identical version chains for every key.

**Replication invariant.** Two replicas that have both applied opnum
N produce byte-identical `get_at_snapshot(K, S)` results for every
`K` and every `S ≤ N`.

### S2.2 contract — transaction lifecycle (read-set tracking)

**Snapshot pinning.** A transaction `T` starts with `snapshot_opnum(T)
= s_T`. All reads in `T` execute as `get_at_snapshot(K, s_T)`. The
snapshot does not advance during `T`'s lifetime.

**Read-set recording.** Every read in `T` appends `(K, observed_
commit_opnum)` to `T.read_set`. Read-your-own-writes is layered on
top (buffer + read-from-buffer-first).

### S2.3 contract — plain SI commit (first-committer-wins)

**SI commit invariant.** A transaction `T` with `snapshot_opnum =
s_T`, `write_set = W_T`, and proposed `commit_opnum = c_T > s_T`
commits iff:
```
for every key K in W_T:
    NO version v of K exists with s_T < commit_opnum(v) < c_T.
```
Otherwise: aborts. (No other transaction overlapping `T`'s write keys
committed in the interval (s_T, c_T).)

**Deterministic abort.** Every replica reaches the same commit/abort
verdict for every `T`, because the version chain is a deterministic
function of the log prefix and the verdict is a function of the
version chain + `T`'s parameters (which arrive in the commit op).

### S2.4 contract — SSI promotion (true serializability)

**SSI commit invariant.** In addition to the plain-SI check, a
transaction `T` commits iff committing `T` would NOT close a
**dangerous structure** in the rw-antidependency graph (Cahill SSI:
two consecutive rw-edges into and out of `T`). If it would,
**one of the implicated transactions** (per the Cahill SSI algorithm)
is aborted.

**Serializable schedule.** Any committed schedule of transactions
under SSI has a serial equivalent (Cahill et al. 2008, Theorem 1).

### S2.5 contract — GC

**GC safety.** No version `v` with `commit_opnum(v) ≥ any active
snapshot opnum` is reclaimed.

**Determinism.** `apply(Op::AdvanceWatermark(N))` produces the same
set of LSM tombstones on every replica.

### S2.6 contract — SQL surface

Every `SELECT` uses snapshot reads at the request-start snapshot.
Every `UPDATE`/`INSERT`/`DELETE` participates in the Tx
lifecycle. The SM's data writes are versioned (28-byte keys);
catalog and index writes stay single-version (the byte-identity gate
change is honest-disclosed in S2.6's record).

---

## Sub-slice decomposition (S2.1–S2.6)

The S2 work decomposes into six sub-slices. Each ships its own plan
(forthcoming for S2.2–S2.6); S2.1's plan accompanies this design.

| Sub-slice | Headline scope | Headline artifact | Honest cargo-gate impact |
|-----------|----------------|-------------------|--------------------------|
| **S2.1** | **Versioned key-value layer + opnum-as-timestamp snapshot read.** Append-only versions in kessel-storage keyed by (key, inverted_opnum). NO SQL change. NO conflict detection. NO transactions. Pure storage primitive + its TLA+ proof. | `kessel-storage::mvcc` module + `kesseldb-tla/MVCCStorage.tla` | **+5 to +10** (new MVCC tests). Legacy paths byte-net-0. |
| **S2.2** | **Snapshot-IDed transactions + read-set tracking.** New `Tx { snapshot_opnum, read_set, write_buffer }` context in kessel-sm; pinned-snapshot reads; client-side write buffering. NO commit-time conflict check yet (writes simply buffer). | `StateMachine::begin_tx`, `tx_read`, `tx_buffer_write` + `kesseldb-tla/MVCCTxn.tla` | **+5 to +10**. Legacy paths byte-net-0. |
| **S2.3** | **Write-write conflict detection (plain SI).** `Op::CommitTxn` SM apply path that runs the SI write-set check. First-committer-wins. Deterministic abort. | `apply_commit_txn` + `kesseldb-tla/MVCCSi.tla` | **+10 to +15**. Legacy paths byte-net-0. |
| **S2.4** | **SSI promotion (read-set tracking + dangerous-cycle detection).** Cahill SSI algorithm. The slice that promotes plain SI to true serializability. | SSI bookkeeping in SM + `kesseldb-tla/MVCCSsi.tla` (the **Serializable** invariant) | **+10 to +15**. Legacy paths byte-net-0. |
| **S2.5** | **GC + watermark.** `Op::AdvanceWatermark(N)` SM op; primary-driven watermark heartbeat protocol; LSM-tombstone-based version reclaim. | `apply_advance_watermark` + `kesseldb-tla/MVCCGc.tla` (GC safety invariant) | **+5 to +10**. Legacy paths byte-net-0. |
| **S2.6** | **SQL integration + SM cutover + Jepsen-style integration tests.** SQL `SELECT`/`UPDATE` routed through MVCC. SM data writes cutover from `Storage::put` to `mvcc::put_versioned`. One-shot migration. **Byte-identity gate change honest-disclosed.** | SQL→MVCC mapping table + cutover migration + full E2E tests | **+20 to +50** (SQL tests + the migration test + the cutover smoke tests). Legacy single-version path is retired post-S2.6 except for catalog/indexes. |

**Estimated cumulative gate growth across S2:** +55 to +110 tests
across six sub-slices. Each sub-slice's plan will refine the estimate.

**Sub-slice ordering rationale.** S2.1 must precede S2.2 (Tx
context needs the snapshot-read primitive). S2.2 must precede S2.3
(SI commit needs the write_set). S2.3 must precede S2.4 (SSI
extends plain SI). S2.5 (GC) is technically orderable anywhere
after S2.1 but ships after S2.4 because the SSI state needs to
interact with GC (the SSI in-flight bookkeeping must respect the
watermark). S2.6 ships last because the SM cutover requires every
prior piece to be in place.

---

## Honest deferred set

These items are explicitly out of scope for S2 and named here so the
S2 record can't drift into over-claim territory:

- **Multi-key snapshot read atomicity beyond what SI requires.** SI's
  snapshot is implicitly atomic (every read in the snapshot sees the
  same version-state). MVCC does NOT add cross-key cross-tablet
  atomicity beyond that. Distributed scatter-scan atomicity is
  open issue #75 SP-A (an existing item, not S2).

- **Time-travel SQL syntax** (`SELECT ... AS OF SYSTEM TIME N`).
  The storage primitive supports it (S2.1's `get_at_snapshot` takes an
  arbitrary opnum), but the SQL surface for it is deferred. Easy
  follow-up post-S2.6 (a one-day SQL parser extension + planner pass).

- **Cross-database transactions** (MVCC across multiple kesseldb-server
  instances). Out of scope; KesselDB is single-cluster.

- **Read-only-replica read isolation lag.** Read-only replicas may
  lag the primary's apply cursor; reads at a snapshot beyond the
  replica's applied prefix block until the apply catches up. This is
  the standard SI follower-read behavior; no extra protocol needed.

- **Per-row locking, gap locks, predicate locks.** SSI subsumes the
  use cases for most of these. Explicit `SELECT FOR UPDATE` is
  expressible as a tx with an empty write_set + a forced rw-edge;
  designed but not shipped in S2.6's initial cut.

- **Per-tablet MVCC sharding.** The current design is single-tablet
  MVCC. Per-tablet MVCC composes with #75 SP-A scatter-scan
  (issue lineage from M3) but is a separate slice.

- **Index MVCC.** Secondary index entries stay single-version in
  S2.6's initial cutover. Index MVCC is an S2.7 follow-up if SQL
  query patterns demand it.

- **Pluggable isolation levels.** PostgreSQL ships `READ COMMITTED`,
  `REPEATABLE READ`, and `SERIALIZABLE`. KesselDB's S2 ships SSI
  (the strongest). A pluggable downgrade to READ COMMITTED is a
  one-day follow-up (skip the write-set check for READ COMMITTED).
  Not in S2 scope.

- **TLA+-mechanized refinement TLA+ ↔ Rust.** Same gap S1/SP109
  disclosed. Per-sub-slice named-action correspondence; not a
  refinement proof.

---

## Thesis-fit note

**Thesis fit:** `deterministic` (the headline S2 contribution — every
MVCC operation, conflict check, and GC decision is a deterministic
function of the log prefix; the thesis-fit-pattern phrase **"consensus
+ deterministic apply = no need for distributed conflict-resolution
coordination"** is the S2 thesis claim);
`replayable` (every snapshot, every conflict verdict, every GC
reclaim is replayable from the log prefix preceding it; debugging
remains `(seed, log)`); `verifiable` (every sub-slice ships a TLA+
extension to `Replication.tla`, mechanically-checked by TLC against
named MVCC invariants — extends S1/SP109 discipline to the MVCC
stack); `honest-docs` (the parallel-module-then-cutover migration
discipline; the byte-identity-gate change in S2.6 named explicitly;
the per-sub-slice gate growth named explicitly; the named-action
TLA+-↔-Rust correspondence honest-disclosed).

---

## S2.1 plan summary (the implementation plan for the first sub-slice is the companion document)

The companion document `docs/superpowers/plans/2026-05-23-mvcc-si-s2-1.md`
ships the **S2.1 implementation plan**: the versioned key-value layer +
opnum-as-timestamp snapshot read, NO SQL integration, NO conflict
detection, NO transactions. Pure storage primitive + the TLA+ artifact
`kesseldb-tla/MVCCStorage.tla`. Tasks T0–T6:

- **T0** baseline (current cargo gate + seed-7 green + MVCC module
  absent).
- **T1** design the versioned-storage Rust types (`SnapshotRead`,
  `MvccKeyError`, the module skeleton).
- **T2** implement `put_versioned` + `get_at_snapshot` + the key
  encoding + tombstones; unit tests.
- **T3** integration tests: replication-byte-identity across a
  3-replica `kessel-sim` configuration; determinism gate.
- **T4** hand-built coverage tests: monotonic writes, snapshot read
  returns historical version, no-snapshot-into-future, tombstone
  visibility.
- **T5** pentest: hostile inputs (`i64::MAX` and `u64::MAX` opnums,
  malformed inverted-opnum keys, snapshot-after-future-write
  attempts, version-chain truncation, 20-byte-vs-28-byte cross-key
  confusion).
- **T6** docs + STATUS row + memory + the `MVCCStorage.tla` TLA+
  artifact + the TLC baseline run + the thesis-fit note.

**Model-selection guidance per S2.1 task:**
- T2 (the Rust storage module — non-trivial LSM-shape design) and
  T3 (integration tests; non-trivial replication-byte-identity
  proof) require a capable model (Opus-class).
- T6's TLA+ artifact (MVCCStorage.tla + .cfg + baseline TLC run)
  requires TLA+ familiarity (Opus-class), per the S1/SP109 model-
  selection discipline.
- T0/T1/T4/T5 are standard Rust/Markdown work — any capable model.

The full plan is in the companion document.

---

## Internal record

This parent design is `docs/superpowers/specs/2026-05-23-mvcc-si-design.md`.

The S2.1 plan is `docs/superpowers/plans/2026-05-23-mvcc-si-s2-1.md`.

When S2.1 ships, its slice-record file will be
`docs/superpowers/specs/2026-05-23-kesseldb-subproject<N>-mvcc-si-s2-1.md`
(subproject number assigned at ship time; mirrors the SP108/SP109
slice-record convention).

S2.2–S2.6 plans will be added under
`docs/superpowers/plans/` as each prior sub-slice's record lands.
Each sub-slice cross-references this parent design.
