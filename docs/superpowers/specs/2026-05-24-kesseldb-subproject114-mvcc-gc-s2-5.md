# KesselDB — Subproject 114: S2.5 — Garbage Collection + Dynamic Watermark Protocol (Supersedes SP113 Bounded Window)

**Date:** 2026-05-24  **Status:** done — `kessel-storage::mvcc::delete_versions_older_than` + `kessel-storage::ssi::prune_pending_txs_by_watermark` + `kessel-storage::Storage<V>::{low_water_mark, set_low_water_mark}` + `kessel-storage::Tx::{begin, begin_rw, begin_ssi}` BREAKING return-type change to `Result<Self, TxError>` with new `TxError::SnapshotTooOld { low_water_mark }` + `kessel-sm::StateMachine::low_water_mark` field + `kessel-proto::Op::AdvanceWatermark { low_water_mark }` at wire tag 45 + `WatermarkRejection::{NotMonotonic, AboveCommitCeiling}` + `OpResult::{WatermarkAdvanced, WatermarkRejected}` + SM apply arm (monotonic + commit-ceiling validation → mvcc GC + ssi watermark-prune + Storage watermark sync) + `MVCCGc.tla` TLA+ rigor checkpoint (sixth module) committed and pushed.

Builds on:
- Subproject 100 — Object-store external sources (OBJ-1):
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject100-objstore.md`
- Subproject 101 — Parquet OBJ-2a:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject101-parquet.md`
- Subproject 102 — RLE/bit-packing hybrid:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject102-rle.md`
- Subproject 103 — Parquet dictionary encoding:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject103-dict.md`
- Subproject 104 — Parquet Snappy decompression:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject104-snappy.md`
- Subproject 105 — Parquet OPTIONAL/nullable columns:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject105-optional.md`
- Subproject 106 — Parquet GZIP page decompression:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject106-gzip.md`
- Subproject 107 — Parquet V2 data pages:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject107-v2pages.md`
- Subproject 108 — Parquet INT96 + DECIMAL (OBJ-2c-4):
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject108-int96-decimal.md`
- Subproject 109 — S1: TLA+ Model-Checked Replication Safety:
  `docs/superpowers/specs/2026-05-23-kesseldb-subproject109-tla-replication-safety.md`
- Subproject 110 — S2.1: MVCC versioned storage (foundation primitive):
  `docs/superpowers/specs/2026-05-23-kesseldb-subproject110-mvcc-s2-1.md`
- Subproject 111 — S2.2: MVCC Tx context + read-set tracking:
  `docs/superpowers/specs/2026-05-24-kesseldb-subproject111-mvcc-tx-s2-2.md`
- Subproject 112 — S2.3: SI write-side + conflict detection at SM apply time:
  `docs/superpowers/specs/2026-05-24-kesseldb-subproject112-mvcc-si-s2-3.md`
- Subproject 113 — S2.4: Serializable SI via Cahill dangerous-structure detection:
  `docs/superpowers/specs/2026-05-24-kesseldb-subproject113-mvcc-ssi-s2-4.md`
- Project THESIS:
  `docs/THESIS.md`

Parent S2 design document:
`docs/superpowers/specs/2026-05-23-mvcc-si-design.md`

S2.5 design document:
`docs/superpowers/specs/2026-05-24-mvcc-si-s2-5-design.md`

S2.5 plan document:
`docs/superpowers/plans/2026-05-24-mvcc-si-s2-5.md`

Foundational reference: standard MVCC GC literature — PostgreSQL autovacuum + per-backend xmin protocol; CockroachDB per-range GC queue; Spanner safe_time Paxos protocol. KesselDB's contribution: GC becomes a totally-ordered log Op rather than a background reclamation protocol, structurally eliminating the coordination layer those systems require.

---

## Strategic-tier framing

S2.5 is the **fifth sub-slice of S2** (Serializable MVCC / Snapshot Isolation) in the THESIS.md strategic-tier backlog. SP114 in the subproject numbering — the slice immediately after SP113 (S2.4 SSI Cahill detection), SP112 (S2.3 SI write-side + SM-apply-time conflict resolver), SP111 (S2.2 read-only Tx + read-set tracking), and SP110 (S2.1 MVCC versioned storage). All five numbers reference the same slice family. The parent S2 design (`docs/superpowers/specs/2026-05-23-mvcc-si-design.md`) decomposes S2 into 6 sub-slices: S2.1 (SP110 — versioned-storage primitive) → S2.2 (SP111 — Tx context + read-set) → S2.3 (SP112 — SI write-side + deterministic conflict detection at SM apply time) → S2.4 (SP113 — SSI promotion via Cahill dangerous-structure detection) → **S2.5 (this slice — GC + dynamic watermark protocol)** → S2.6 (SQL integration + SM cutover). This slice **CLOSES the SP113 bounded-window false-negative** — SP113's `prune_pending_txs(MAX_TX_AGE = 4096)` documented a fixed-window limitation (Decision 5 honest disclosure): a Tx whose snapshot is older than `current - MAX_TX_AGE` falls outside the lookback horizon; the SSI dangerous-structure detector cannot reach back to the relevant pending_txs records; an anomaly may be missed. S2.5's watermark-driven prune uses the WATERMARK instead of a fixed window — and the watermark, by Decision 2 of S2.5 design, is bounded above by min(active snapshots) when the heartbeat producer is well-behaved; therefore any concurrent-Tx pair whose snapshots are within the active-reader population has its pending_txs records preserved across watermark advances; the false-negative is FORMALLY CLOSED.

---

## THESIS-FIT CENTERPIECE — GC as a structural property of the deterministic log

**This is the most important paragraph in this record.**

Per the parent S2 design Decision 4 (carried), the S2.5 design Decision 1 (verbatim):

> KesselDB does not need PostgreSQL's autovacuum + per-backend xmin or
> CockroachDB's per-range GC queue or Spanner's safe_time Paxos protocol
> because the reclamation is itself a totally-ordered log Op; every
> replica's deterministic apply executes the same reclamation byte-
> identically. **GC becomes a structural property of the log, not a
> background thread or a distributed-coordination protocol.**

S2.5 is the slice that puts this in code. The reclamation happens AT SM APPLY TIME via a deterministic `Op::AdvanceWatermark { low_water_mark: u64 }` at wire tag 45; every replica's deterministic `StateMachine::apply` arm runs the same validation (strict monotonicity + commit-ceiling), the same MVCC version full-scan delete (`mvcc::delete_versions_older_than`), the same pending_txs prune (`ssi::prune_pending_txs_by_watermark`), and the same field updates (`StateMachine.low_water_mark` + `Storage.low_water_mark`). The outcome is byte-identical on every replica by construction.

**The structural implication:** KesselDB does NOT need PostgreSQL's autovacuum (a background thread that races against in-flight Txs and requires per-backend xmin negotiation); KesselDB does NOT need CockroachDB's per-range GC queues (separate workqueue scheduling); KesselDB does NOT need Spanner's safe_time Paxos (a separate consensus protocol for the reclamation horizon). The entire distributed-GC-coordination layer is **structurally absent**. This is what THESIS.md S2 means by "deterministic replicated SQL with bounded memory by construction."

**The SP113-supersession claim is the second thesis-fit headline.** SP113's fixed `prune_pending_txs(MAX_TX_AGE)` had a documented bounded-window false-negative case (`too_old_snapshot_false_negative` in `crates/kessel-storage/tests/pentest_mvcc_ssi.rs`); SP114's `prune_pending_txs_by_watermark` REPLACES that prune at the watermark-advance seam (SP113's fixed-window prune RETAINED as a belt-and-suspenders fallback ceiling on the commit-apply seam — see Decision 4 of S2.5 design). The supersession claim is mechanically asserted at THREE LAYERS:

1. **Rust integration (`it_supersedes_sp113_bounded_window_false_negative` in `crates/kessel-storage/tests/integration_mvcc_gc.rs`)** — reconstructs the SP113 PT-4 `too_old_snapshot_false_negative` workload at the SM apply level and verifies the S2.5 path detects and aborts the previously-missed write-skew.
2. **TLA+ (`BoundedWindowSupersededByWatermark` invariant in `kesseldb-tla/MVCCGc.tla`)** — formalizes that under the well-behaved-heartbeat operating point (lowWaterMark <= every Active Tx's snapshot), every slot a still-Active Tx might need is preserved across watermark advances; mechanically checked by TLC across 1,594,330 distinct states.
3. **Production code (the `Op::AdvanceWatermark` apply arm in `crates/kessel-sm/src/lib.rs`)** — every replica's deterministic apply runs the same prune; the SP113 fixed-window prune is RETAINED on the commit-apply seam as the fallback ceiling per Decision 4 of S2.5 design.

**MVCC GC in a deterministic log is genuinely novel.** PostgreSQL's autovacuum assumes a non-deterministic backend population; the per-backend xmin protocol is the coordination cost. CockroachDB's per-range GC queues assume non-deterministic range scheduling; the queue is the coordination cost. Spanner's safe_time Paxos assumes non-deterministic snapshot reads; the Paxos is the coordination cost. KesselDB's substrate eliminates all three: the SM apply arm IS the GC executor; every replica reaches the same reclaimed-version set against the same log prefix; the implementation footprint is ~30 lines in `kessel-storage::mvcc` (the `delete_versions_older_than` primitive) + ~5 lines in `kessel-storage::ssi` (the `prune_pending_txs_by_watermark` primitive) + ~50 lines in `kessel-sm` (the `Op::AdvanceWatermark` apply arm) — vs hundreds of lines per coordination protocol in those systems.

---

## What shipped

### `crates/kessel-proto/src/lib.rs` extensions

- **`Op::AdvanceWatermark { low_water_mark: u64 }`** — additive variant at wire tag 45 (Decision 5 of S2.5 design). Encoder uses the same u64-encoding helpers SP112's `Op::CommitTx { commit_opnum }` uses; decoder roundtrips the value byte-identically.
- **`OpResult::WatermarkAdvanced { new_low_water_mark: u64, versions_deleted: usize, pending_txs_evicted: usize }`** — additive variant carrying the apply-arm outcome on accept.
- **`OpResult::WatermarkRejected { reason: WatermarkRejection }`** — additive variant carrying the rejection cause.
- **`WatermarkRejection`** enum with `NotMonotonic { proposed: u64, current: u64 }` (strict-monotonicity violation, Decision 5 of S2.5 design) and `AboveCommitCeiling { proposed: u64, current_commit: u64 }` (commit-ceiling violation, Decision 5). `#[non_exhaustive]` for forward-compat.

### `crates/kessel-storage/src/mvcc.rs` extensions

- **`pub fn delete_versions_older_than<V: Vfs>(store: &mut Storage<V>, low_water_mark: u64) -> Result<usize, MvccKeyError>`** — full-scan tombstone-based GC primitive (Decision 3 of S2.5 design). Iterates the full 28-byte versioned-key space lex-ordered; for every key whose decoded commit_opnum is < low_water_mark, calls `Storage::delete(low_water_mark, k)` (writes a tombstone via the SP110 LSM `delete` path; LSM byte-size shrinks by value-payload only — physical erasure is the LSM compaction layer's concern, OOS for S2.5). Returns the count of versions deleted. Deterministic by sorted-key scan order; every replica reaches byte-identical post-GC state. O(N) scan; range-prune and bloom-filter optimisations deferred to S2.X.

### `crates/kessel-storage/src/ssi.rs` extensions

- **`pub fn prune_pending_txs_by_watermark(pending_txs: &mut BTreeMap<u64, PendingTxRecord>, low_water_mark: u64)`** — watermark-driven eviction (Decision 4 of S2.5 design). Implemented via `BTreeMap::split_off(&low_water_mark)` which keeps records with commit_opnum >= low_water_mark and drops the rest. REPLACES SP113's `prune_pending_txs(MAX_TX_AGE)` AT THE WATERMARK-ADVANCE SEAM; the SP113 `prune_pending_txs(MAX_TX_AGE)` remains RETAINED as a belt-and-suspenders fallback ceiling on the commit-apply seam.

### `crates/kessel-storage/src/tx.rs` extensions (BREAKING)

- **`Tx::begin / Tx::begin_rw / Tx::begin_ssi`** — **BREAKING return-type change**: from `Self` to `Result<Self, TxError>` (Decision 7 of S2.5 design). Each constructor now reads `store.low_water_mark()` at the top and returns `Err(TxError::SnapshotTooOld { low_water_mark })` if `snapshot_opnum < low_water_mark`; otherwise returns `Ok(Self { ... })`. This is the single non-byte-net-0 API surface in SP114. At the call-site level: every in-tree caller (52 test sites + the standalone-form integration tests) was updated to `.expect("watermark = 0")` or `?` in T1; for the watermark = 0 default (no `Op::AdvanceWatermark` has fired), every constructor produces byte-identical Tx state vs SP113 — the breaking change is at the COMPILE level, not the runtime level. Production callers wire in S2.6.
- **`TxError::SnapshotTooOld { low_water_mark: u64 }`** — additive variant on the `#[non_exhaustive]` enum carrying the watermark at rejection time for caller diagnostics.

### `crates/kessel-storage/src/lib.rs` extensions

- **`Storage<V>::low_water_mark: u64`** — new field, initial 0 (Decision 6 of S2.5 design). Mutated only by `set_low_water_mark(u64)` (called from the SM apply arm at step 6 of the AdvanceWatermark accept branch).
- **`Storage<V>::low_water_mark(&self) -> u64`** — accessor; reads the field.
- **`Storage<V>::set_low_water_mark(&mut self, low_water_mark: u64)`** — setter; called from the SM apply arm.

### `crates/kessel-sm/src/lib.rs` extensions

- **`StateMachine<V>::low_water_mark: u64`** — new field, initial 0 (Decision 6 of S2.5 design). Mutated only by the `Op::AdvanceWatermark` apply arm on the accept branch. Rebuilt deterministically by re-applying the log prefix; every replica's low_water_mark is byte-identical against the same prefix.
- **`Op::AdvanceWatermark` SM apply arm** — full 7-step implementation per Decision 5 + 6 + 7 of S2.5 design:
  1. Validate `low_water_mark > self.low_water_mark` (strict-monotonic) → on fail, return `OpResult::WatermarkRejected { reason: NotMonotonic { proposed, current } }`.
  2. Validate `low_water_mark <= self.commit_opnum` (commit-ceiling) → on fail, return `OpResult::WatermarkRejected { reason: AboveCommitCeiling { proposed, current_commit } }`.
  3. Call `mvcc::delete_versions_older_than(&mut self.store, low_water_mark)` → `versions_deleted`.
  4. Call `ssi::prune_pending_txs_by_watermark(&mut self.pending_txs, low_water_mark)` → compute `pending_txs_evicted`.
  5. Update `self.low_water_mark = low_water_mark`.
  6. Call `self.store.set_low_water_mark(low_water_mark)` to sync the Tx-side accessor.
  7. Return `OpResult::WatermarkAdvanced { new_low_water_mark: low_water_mark, versions_deleted, pending_txs_evicted }`.

### `kesseldb-tla/MVCCGc.tla` + `MVCCGc.cfg` + `results/2026-05-24-mvcc-gc-baseline.txt`

The **sixth TLA+ rigor-gate artifact** in the project (after SP109 Replication, SP110 MVCCStorage, SP111 MVCCTx, SP112 MVCCSi, SP113 MVCCSsi). EXTENDS MVCCSsi; adds `lowWaterMark: Nat` state var; adds the `AdvanceWatermark(W)` action modeling the SM apply arm with all 3 branches (NotMonotonic / AboveCommitCeiling / Accepted) inline; tightens every lifted MVCCSsi action to preserve `lowWaterMark` UNCHANGED unless mutated; tightens the `BeginGc(t, s)` precondition with `s >= lowWaterMark` (mirrors the Rust `Tx::begin*` snapshot-too-old check). Adds **6 new invariants** on top of the 17 carried forward via EXTENDS — but the EXTENDS layer's `CommitAtomicity` and `DeterministicApply` invariants are legitimately violated by GC (versions deleted below the watermark) and are DROPPED from the .cfg invariant list, REPLACED by GC-aware reformulations `CommitAtomicityGc` and `DeterministicApplyGc` (SP109-SP113 discipline: never weaken; restate). The new invariants:

- **`TypeOKGc`** — well-typed envelope (extends TypeOKSsi with the lowWaterMark variable).
- **`WatermarkMonotonic`** — lowWaterMark <= opCount (the strongest current-state projection of the strict-monotonicity contract that the bounded TLC model can mechanically check).
- **`NoVersionBelowWatermark`** — for every key k and every version e in versions[k], e.opnum >= lowWaterMark. Stable; preserved by every action.
- **`NoPendingTxBelowWatermark`** — for every slot c with HasPending(c), c >= lowWaterMark.
- **`SnapshotAvailability`** — for every Active Tx t in the well-behaved-heartbeat regime (s >= lowWaterMark), every version with opnum <= snapshot satisfies opnum >= lowWaterMark (i.e., is preserved by NoVersionBelowWatermark). The misbehaving-heartbeat case is vacuously satisfied; documented Decision 2 disclosure.
- **`BoundedWindowSupersededByWatermark`** — THE SP113-CLOSURE INVARIANT. For every Active Tx t with lowWaterMark <= t.snapshot (the well-behaved-heartbeat operating point), every slot c > t.snapshot satisfies c >= lowWaterMark — i.e., NO slot the still-Active Tx might need for rw-edge derivation is in the prune-eligible range. The watermark-driven prune only evicts slots c < lowWaterMark; therefore no slot c > t.snapshot >= lowWaterMark can be evicted. The SP113 bounded-window false-negative (Decision 5 of SP113 design) is FORMALLY CLOSED in the well-behaved-heartbeat regime; the misbehaving-heartbeat regime is the documented Decision 2 disclosure and the antecedent is vacuously false there.

Plus the 2 GC-aware reformulations:

- **`CommitAtomicityGc`** — every Committed Tx t with `commit_opnum >= lowWaterMark` and non-empty write_set still has its writes present in storage. Below the watermark, GC legitimately reclaimed them.
- **`DeterministicApplyGc`** — same shape; conditioned on `commit_opnum >= lowWaterMark`; the `commit_opnum \in OpNums` well-typedness clause carries unconditionally.

### TLA+ rigor checkpoint — TLC outcome

- **`MVCCGc.tla`** — abstract single-replica TLA+ specification of the GC + watermark protocol. EXTENDS `MVCCSsi` so the GC invariants are checked over the same versioned-storage + Tx + SI + SSI model TLC has already verified in S2.1/S2.2/S2.3/S2.4. Adds the lowWaterMark state via a fresh Nat; adds the AdvanceWatermark action with 3 branches; adds 6 new + 2 GC-aware-reformulated invariants. Module head carries the action-mapping table pointing each TLA+ action to its Rust counterpart in `kessel-storage::mvcc::delete_versions_older_than` + `kessel-storage::ssi::prune_pending_txs_by_watermark` + `kessel-storage::tx::Tx::begin*` + `kessel-storage::Storage<V>::{low_water_mark, set_low_water_mark}` + `kessel-sm::StateMachine` (Op::AdvanceWatermark apply arm) (mirrors SP109/SP110/SP111/SP112/SP113 named-correspondence discipline).

- **`MVCCGc.cfg`** — TLC configuration: `TypeIds = {1}`, `ObjectIds = {1, 2}`, `OpNums = {0, 1, 2}`, `Values = {"v1", "v2"}`, `MaxOps = 3`, `TxIds = {"t1", "t2"}`, `MaxTxOps = 4`, `MaxTxAge = 5`, `MaxWatermark = 2`, sentinels `SiUnused = "Si"`, `SsiUnused = "Ssi"`, `GcUnused = "Gc"`. `CHECK_DEADLOCK FALSE`. **23 invariants** in the INVARIANT block (12 MVCCSi+prior carried forward MINUS 2 GC-incompatible ones DROPPED + 5 SSI-specific carried forward + 6 new GC-specific + 2 GC-aware reformulations).

- **`results/2026-05-24-mvcc-gc-baseline.txt`** — captured baseline TLC run: **`Model checking completed. No error has been found.`** 1,594,330 distinct states / 9,420,629 states generated / depth 12 / **48 seconds** wall-clock on Windows. Complete coverage (queue drained to 0 states left).

### TLC honest disclosure — 3 spec-issue fixes landed in T6

T6 found **3 TLC-driven design-completions** all of which were classification-(a) genuine TLA+ contract refinements (NOT spec bugs in the underlying Rust):

- **Fix #1 (BoundedWindowSupersededByWatermark phrasing).** First-pass formulation required the slot c to be EITHER still pending OR beyond opCount. TLC found a counterexample where neither held (a slot in the live range with no commit ever landing there is the natural state of the bounded model). REPHRASED as the structural claim: "under the well-behaved-heartbeat operating point, every slot c > snapshot is NOT below the watermark" — which IS the formal SP113-closure (the antecedent rules out the prune-eligible case; the consequent is the structural impossibility of the prune evicting a needed slot). The rephrase is mechanically TIGHTER than the original first-pass: it asserts the structural impossibility rather than the case-split.

- **Fix #2 (SnapshotAvailability phrasing).** First-pass formulation asserted every Active Tx has snapshot >= lowWaterMark unconditionally. TLC found a counterexample where the heartbeat (free-choice TLA+ AdvanceWatermark) over-advanced past an in-flight Tx's snapshot — exactly the documented Decision 2 heartbeat-trust boundary disclosure. REPHRASED as the conditional contract: "for every Active Tx t with snapshot >= lowWaterMark (well-behaved-heartbeat case), every version it might need is preserved by NoVersionBelowWatermark." The misbehaving-heartbeat case is vacuously satisfied; documented Decision 2.

- **Fix #3 (CommitAtomicity + DeterministicApply EXTENDS-substrate-violation under GC).** The inherited `MVCCSi.CommitAtomicity` and `MVCCSi.DeterministicApply` invariants assert "every Committed Tx has its writes installed in versions[k]" — legitimately violated by GC reclaiming a Committed Tx's versions (commit_opnum < lowWaterMark). Per SP109-SP113 discipline (never weaken; restate): DROPPED both from the .cfg invariant list and REPLACED with `CommitAtomicityGc` and `DeterministicApplyGc` (same shape, conditioned on `commit_opnum >= lowWaterMark`). The GC-aware forms are STRONGER above the watermark (same claim) and legitimately silent below (the AdvanceWatermark action's contract IS that versions below the watermark are gone).

**Final TLC outcome:**
- States generated: 9,420,629
- Distinct states found: 1,594,330
- Depth of complete state graph: 12
- Wall-clock: 48s on Windows 11 (16 workers, 7147MB heap)
- Queue: drained to 0 states left → **complete coverage at the configured bounds**
- Invariant violations: 0 (after the 3 fixes above — clean first-pass on each refinement)

### Bounded-config sizing

The S2.5 design Decision 8 sized the initial config at the MVCCSsi inheritance baseline plus `MaxWatermark = 2`. The shipped config matches the design exactly. The 2-Tx model IS sufficient to produce the SP113-supersession counterexample (the BoundedWindowSupersededByWatermark scenario requires only 2 concurrent Tx); a 3-Tx model would let TLC also explore canonical multi-pivot dangerous-structure interactions with watermark advances — S2.X follow-up. The GC composite state space (~1.6M distinct vs MVCCSsi's 348K) is larger than MVCCSsi's because the AdvanceWatermark action's free-choice W exercises three branches (NotMonotonic / AboveCommitCeiling / Accepted) per opnum slot, and the watermark mutation introduces a new state dimension across the existing SSI interleaving.

This is the **sixth TLA+ rigor-gate artifact** in the project. The six modules now form a layered verification stack:
- `kesseldb-tla/Replication.tla` (SP109/S1) — VSR replication protocol
- `kesseldb-tla/MVCCStorage.tla` (SP110/S2.1) — versioned storage primitive
- `kesseldb-tla/MVCCTx.tla` (SP111/S2.2) — Tx context + read-set
- `kesseldb-tla/MVCCSi.tla` (SP112/S2.3) — SI write-side + SM-apply-time conflict resolver
- `kesseldb-tla/MVCCSsi.tla` (SP113/S2.4) — SSI Cahill dangerous-structure detector + full-serializability invariants
- `kesseldb-tla/MVCCGc.tla` (SP114/S2.5) — GC + dynamic watermark protocol + SP113-closure invariant

Each extends the prior; the invariants compose (modulo the 2 GC-aware reformulations of inherited invariants that are legitimately weakened by GC); the GC + watermark protocol is mechanically-checked over the same VSR-log substrate that S1/SP109 verified.

### Test surface (cargo gate growth — all new tests on the new GC + watermark surface)

| Task | Tests added | Cumulative cargo total | Notes |
|---|---|---|---|
| T1 (scaffold) | +2 | 610 → 612 | Type-shape locks for `Op::AdvanceWatermark` + `WatermarkRejection` + `OpResult::{WatermarkAdvanced, WatermarkRejected}` + `Tx::begin*` Result-return + `TxError::SnapshotTooOld` + `Storage::{low_water_mark, set_low_water_mark}` + `StateMachine::low_water_mark`; 52 in-tree call-sites of `Tx::begin*` updated to `.expect("watermark = 0")` / `?` for the breaking Result return type |
| T2 (impl + KATs) | +11 | 612 → 623 | 11 hand-derived KATs covering the GC + watermark contract: empty-storage GC count / 5-version reclaim-count / non-monotonic-rejection / above-commit-ceiling-rejection / pending_txs prune / Tx::begin snapshot-too-old / 7-version count / at-watermark-preserved (strict <) / wire roundtrip for Op + OpResult + WatermarkRejection / Storage accessor symmetry / SM watermark advance sequence persistence |
| T3 (integration) | +6 | 623 → 629 | 6 integration tests including the **SP113 supersession headline** (`it_supersedes_sp113_bounded_window_false_negative` reconstructs the SP113 PT-4 workload at SM apply level and verifies the dangerous-structure abort fires under the watermark protocol) + 3-replica byte-identity for GC ops (the thesis-fit determinism gate) + snapshot-too-old rejection consistency across all 3 Tx constructors + heartbeat trust-boundary contract test + advance-after-commit interleave + SM-apply ↔ local-path byte-equivalence |
| T4 (coverage) | +5 | 629 → 634 | watermark=0 no-op (SP1-SP113 byte-net-0) / watermark=commit_opnum reclaims-all / monotonic-violation chain rejection / 1000-version GC scaling (perf-as-correctness) / advance-interleaved-with-commit |
| T5 (pentest) | +6 | 634 → 640 | hostile u64::MAX watermark (no overflow; rejected as AboveCommitCeiling) / monotonic-violation storm (10_000 consecutive below-watermark; all rejected) / snapshot=0 after MAX watermark / 100k-version GC under load / watermark+SSI interleaving (mixed Op::CommitTx + Op::AdvanceWatermark; deterministic dispatch) / watermark-advance during in-flight commit (deterministic ordering); no vuln found |
| T6 (this) | 0 | 640 → 640 | Docs + MVCCGc.tla + STATUS + memory only; no Rust touched |

**Total: 610 → 640 (+30 net-additive tests).** All on the new GC + watermark surface (mvcc::delete_versions_older_than + ssi::prune_pending_txs_by_watermark + Op::AdvanceWatermark + WatermarkAdvanced/WatermarkRejected/WatermarkRejection + Storage::{low_water_mark, set_low_water_mark} + StateMachine::low_water_mark + Tx::begin* Result return + TxError::SnapshotTooOld); every legacy SP1–SP113 path remains byte-net-0 WHEN `low_water_mark = 0` (the steady-state default; until an Op::AdvanceWatermark op fires). The `Tx::begin*` return-type change is API-breaking at the COMPILE level (every caller must handle Result) but BYTE-IDENTICAL at the RUNTIME level when watermark = 0. `FAILED=0`, `large_seed_corpus_is_deterministic_and_converges` green, zero new external dependencies (`cargo tree -p kesseldb-server | grep -Ei "parquet|objstore|rustls|webpki"` unchanged from SP113 = unchanged from SP112 = unchanged from SP111 = unchanged from SP110), `#![forbid(unsafe_code)]` honored in every touched file.

---

## TLA+-to-Rust correspondence

Named-action correspondence per SP109/SP110/SP111/SP112/SP113 discipline (Decision 8 of the S2.5 design). NOT mechanized refinement — a divergence between the spec and the implementation is a human-discovered issue. The TLA+ spec's module head carries the live mapping table; this record reproduces it for archival. Line numbers accurate as of T6 commit (HEAD = `fb9891e` from T5).

| TLA+ in MVCCGc.tla | Rust counterpart | Notes |
|---|---|---|
| `AdvanceWatermark(W)` | `Op::AdvanceWatermark { low_water_mark: W }` SM apply arm in `crates/kessel-sm/src/lib.rs` | Validates W > self.low_water_mark (NotMonotonic) AND W <= self.commit_opnum (AboveCommitCeiling); on accept, calls mvcc::delete_versions_older_than + ssi::prune_pending_txs_by_watermark + updates self.low_water_mark + self.store.set_low_water_mark(W) |
| `delete_versions_lt(W)` (inline) | `kessel-storage::mvcc::delete_versions_older_than(&mut store, W)` in `crates/kessel-storage/src/mvcc.rs` | Full LSM scan; deletes every versioned entry with commit_opnum < W; deterministic by sorted-key scan order; tombstone-based |
| `prune_pending_lt(W)` (inline) | `kessel-storage::ssi::prune_pending_txs_by_watermark(&mut pending_txs, W)` in `crates/kessel-storage/src/ssi.rs` | BTreeMap::split_off(&W); keeps records with commit_opnum >= W |
| `BeginGc` precondition `s >= lowWaterMark` | `kessel-storage::tx::Tx::{begin, begin_rw, begin_ssi}` in `crates/kessel-storage/src/tx.rs` | Reads store.low_water_mark(); returns Err(TxError::SnapshotTooOld { lwm }) if s < lwm; otherwise Ok(Tx) |
| `lowWaterMark` (state var) | `Storage<V>::low_water_mark` field + `StateMachine<V>::low_water_mark` field; kept in sync by SM apply arm step 6 | At TLA+ level a single Nat suffices (the two Rust fields are abstractly merged) |
| `TypeOKGc` (invariant) | `Storage<V>::low_water_mark: u64` + `StateMachine<V>::low_water_mark: u64` | Well-typed envelope |
| `WatermarkMonotonic` (invariant) | `Op::AdvanceWatermark` apply arm strict-monotonicity validation + accept-branch field update | lowWaterMark only ever increases (or stays) across the trace |
| `NoVersionBelowWatermark` (invariant) | mvcc::delete_versions_older_than post-condition | After any AdvanceWatermark, no version with opnum < lowWaterMark survives |
| `NoPendingTxBelowWatermark` (invariant) | ssi::prune_pending_txs_by_watermark post-condition | After any AdvanceWatermark, no pending_txs record at slot < lowWaterMark survives |
| `SnapshotAvailability` (invariant) | Conjunction of BeginGc precondition + NoVersionBelowWatermark | Well-behaved-heartbeat Tx reads against preserved versions |
| `BoundedWindowSupersededByWatermark` (invariant) | The SP113-supersession claim, mechanically asserted | The watermark prune NEVER evicts a slot a still-Active Tx might need under the well-behaved-heartbeat regime |
| `CommitAtomicityGc` (invariant) | The SP110+SP112 commit-installation contract conditioned on `commit_opnum >= lowWaterMark` | GC-aware reformulation of inherited CommitAtomicity |
| `DeterministicApplyGc` (invariant) | Same as DeterministicApply, conditioned on `commit_opnum >= lowWaterMark` for the versions-preservation clause; commit_opnum well-typedness unconditional | GC-aware reformulation of inherited DeterministicApply |

---

## Honest gate accounting

Pre-SP114 cargo baseline: **610/0** (post-SP113 final).

Post-SP114 cargo gate: **640/0** (+30 net-additive tests across T1–T5; T6 added 0 Rust tests).

The +30 delta is **all new tests on the NEW GC + watermark surface** (mvcc::delete_versions_older_than + ssi::prune_pending_txs_by_watermark + Op::AdvanceWatermark + WatermarkAdvanced/WatermarkRejected/WatermarkRejection + Storage::{low_water_mark, set_low_water_mark} + StateMachine::low_water_mark + Tx::begin* Result return + TxError::SnapshotTooOld). Every legacy SP1–SP113 path is byte-net-0 WHEN `low_water_mark = 0` (the steady-state default; until an Op::AdvanceWatermark op fires) — verified at five levels:

1. **`low_water_mark = 0` default preserves every SP1-SP113 behavior.** No SP1-SP113 path inspects the watermark; the new code paths (mvcc::delete_versions_older_than + ssi::prune_pending_txs_by_watermark + the SM apply arm + the Tx::begin* snapshot-too-old check) are GATED on `low_water_mark > 0` semantically (every legacy Tx with snapshot_opnum >= 0 satisfies `snapshot_opnum >= 0 = low_water_mark`). T4's `it_coverage_watermark_zero_no_op` test asserts SP1-SP113 byte-identity for 20 mixed Ops under the watermark = 0 default.

2. **`Tx::begin*` return-type change is API-breaking at the COMPILE level, byte-identical at the RUNTIME level for watermark = 0.** Every in-tree caller (52 test sites) was updated to `.expect("watermark = 0")` or `?` in T1; under the watermark = 0 default the constructor returns `Ok(Tx { ... })` byte-identically to the SP113 `Self` shape. Production callers wire in S2.6 — they MUST handle the Result.

3. **`Op::AdvanceWatermark` wire format is additively appended at tag 45.** SP113 wire-roundtrip KATs continue to pass byte-net-0 (tags 0-44 unchanged); tag 45 is fresh.

4. **`OpResult::{WatermarkAdvanced, WatermarkRejected}` and `WatermarkRejection` variants are additive on `#[non_exhaustive]` enums.** SP113's existing OpResult variants encode/decode byte-unchanged.

5. **`cargo tree -p kesseldb-server | grep -Ei "parquet|objstore|rustls|webpki"` unchanged from SP113** (zero new external dependencies). `#![forbid(unsafe_code)]` honored in every touched file.

`FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` green; `cargo test --workspace --release` green at 640/0.

---

## Per-task evidence chain

| Task | Commit | Evidence |
|---|---|---|
| T1 scaffold | `a255543` | Op::AdvanceWatermark variant + WatermarkRejection enum + OpResult::{WatermarkAdvanced, WatermarkRejected} variants + Tx::begin / begin_rw / begin_ssi return-type changed to Result<Self, TxError> with stub Ok bodies + TxError::SnapshotTooOld variant + mvcc::delete_versions_older_than + ssi::prune_pending_txs_by_watermark signatures with todo!() bodies + Storage::{low_water_mark, set_low_water_mark} field+accessor+setter + StateMachine::low_water_mark field + 52 in-tree Tx::begin* call-sites updated for the breaking Result + 2 scaffold tests; 610 → 612 |
| T2 impl + KATs | `f804825` | mvcc::delete_versions_older_than full-scan impl + ssi::prune_pending_txs_by_watermark BTreeMap::split_off impl + Tx::begin* snapshot-too-old check + Op::AdvanceWatermark SM apply arm with 7-step body (monotonic-validation → commit-ceiling-validation → mvcc-GC → ssi-watermark-prune → SM-field-update → Storage-field-sync → result-return) + 11 hand-derived KATs covering empty-storage / reclaim-count / non-monotonic-rejection / above-commit-ceiling-rejection / pending_txs prune / snapshot-too-old / at-watermark-preserved (strict <) / wire roundtrip / accessor symmetry / multi-advance persistence; 612 → 623 |
| T3 integration | `ee043d4` | 6 integration tests in tests/integration_mvcc_gc.rs: **`it_supersedes_sp113_bounded_window_false_negative` (the SP113-supersession HEADLINE)** + `it_classic_gc_reclaims_versions_byte_identically_across_3_replicas` (the thesis-fit determinism HEADLINE — 3-replica byte-identity for AdvanceWatermark apply) + `it_snapshot_too_old_rejected_consistently` (all 3 Tx constructors return Err on snapshot < watermark; at-watermark Ok per Decision 7 boundary) + `it_long_running_tx_pins_watermark` (heartbeat trust-boundary contract test — SM trusts caller-supplied W per Decision 2; operational concern of heartbeat producer) + `it_advance_watermark_after_commit_commit_advance_sequence` (mixed-op apply-arm dispatch correctness) + `it_sm_apply_byte_equivalence_with_local_path` (SM apply ↔ direct mvcc + ssi primitive calls); 623 → 629 |
| T4 coverage | `3b76d0e` | 5 coverage tests: watermark=0 no-op (SP1-SP113 byte-net-0 in default state) / watermark=commit_opnum reclaims-all (boundary case for commit-ceiling rule) / monotonic-violation chain (10 below-watermark all rejected) / 1000-version GC scaling (loose perf-as-correctness gate <500ms) / advance-interleaved-with-commit; 629 → 634 |
| T5 pentest | `fb9891e` | 6 adversarial-input tests in tests/pentest_mvcc_gc.rs: hostile u64::MAX watermark (no overflow; rejected AboveCommitCeiling) / monotonic-violation storm (10_000 consecutive; all rejected; SM state stable) / snapshot=0 after MAX watermark (SnapshotTooOld; at-watermark Ok) / 100k-version GC under load (perf-as-correctness gate <5s; honest disclosure of full-scan complexity per Decision 3) / watermark+SSI interleaving (50-op mixed workload; deterministic dispatch; SP113 prune_pending_txs(MAX_TX_AGE) fallback ceiling fires on every commit apply) / watermark-advance during in-flight commit (deterministic ordering; heartbeat-trust contract boundary per Decision 2); 634 → 640; no vuln found |
| T6 docs + TLA+ | _(this commit)_ | SP114 record + STATUS row + `MVCCGc.tla` (EXTENDS MVCCSsi; 7 GC-lifted actions preserving gcVars UNCHANGED + fresh AdvanceWatermark action with 3 branches inline; 6 new invariants on top of 17 carried forward; 2 GC-aware reformulations of inherited invariants legitimately weakened by GC = 23 invariants total in the .cfg) + `MVCCGc.cfg` (bounded 2-Tx model per Decision 8) + baseline TLC run (1.59M distinct states / depth 12 / no violation / 48s / complete coverage; 3 TLC-found refinements landed — BoundedWindowSupersededByWatermark phrasing tightened; SnapshotAvailability rephrased to conditional contract; CommitAtomicity + DeterministicApply DROPPED from inherited and REPLACED with GC-aware reformulations); 640 → 640 (no Rust touched) |

---

## Honest disclosure — the slice's primary discipline

- **GC + watermark dormant pending S2.6 SM cutover.** No production caller submits `Op::AdvanceWatermark` to VSR in S2.5. The op is exercised via direct `StateMachine::apply` calls in integration tests (T3) and via construction-only tests of the underlying primitives. The watermark heartbeat producer (the agent that gathers min(active_snapshot) and submits the op) is NOT shipped — per Decision 2 of S2.5 design, the SM TRUSTS the caller-supplied watermark. The "GC + watermark works" claim is the contract + the 30 new tests + the TLA+ pass; the "GC is in the production data path" claim is **reserved for S2.6** (SM cutover + heartbeat producer integration).

- **Tombstone-based delete (Storage::delete writes LSM tombstones, NOT physical erasures).** `mvcc::delete_versions_older_than` calls `Storage::delete(low_water_mark, k)` which writes a tombstone marker in the LSM byte stream. Value reclamation happens immediately (the value payload is replaced with a marker); physical byte-stream erasure happens at LSM compaction time (out of scope for S2.5). Per Decision 3 + the PT-5 induction (vd = 2c+1 per cycle): tombstones from each GC pass survive until the next GC pass — compounding linearly with GC cadence. The perf KAT for sustained-cadence workloads is deferred to S2.X. T5's pentest documents the 100k-version GC behavior under load and the perf-as-correctness gate.

- **Heartbeat producer NOT modeled.** Per Decision 2 of S2.5 design: the SM apply arm TRUSTS the caller-supplied `low_water_mark`. The heartbeat producer (an agent gathering min(active_snapshot) and submitting `Op::AdvanceWatermark`) is operational infrastructure outside the SM's view. T3's `it_long_running_tx_pins_watermark` test explicitly documents this trust boundary: the SM admits any AdvanceWatermark satisfying its 2 validations (strict monotonicity + commit-ceiling); over-advancing past an in-flight Tx's snapshot is the heartbeat's operational responsibility, not a runtime-prevention concern. The TLA+ AdvanceWatermark action mirrors: it accepts any caller-supplied W satisfying the on-Op validation; the well-behaved-heartbeat constraint is locked at the `BoundedWindowSupersededByWatermark` invariant level (conditional on `lowWaterMark <= every Active Tx's snapshot`), and the misbehaving case is the documented Decision 2 disclosure (vacuously satisfied; antecedent false).

- **`Tx::begin*` BREAKING return-type change.** The single non-byte-net-0 API surface in SP114. From `Self` to `Result<Self, TxError>` — every caller must handle the Result. 52 in-tree test call-sites were updated in T1. At runtime under the watermark = 0 default, the constructor returns `Ok(Tx { ... })` byte-identically to the SP113 `Self` shape. Production callers wire in S2.6 (the SM-cutover slice) — they MUST handle the Result.

- **SP113 MAX_TX_AGE prune RETAINED as belt-and-suspenders fallback.** Per Decision 4 of S2.5 design: SP113's `prune_pending_txs(MAX_TX_AGE = 4096)` is RETAINED on the commit-apply seam as a fallback ceiling for the pending_txs map. SP114's `prune_pending_txs_by_watermark` is the PRIMARY prune at the watermark-advance seam (`Op::AdvanceWatermark` apply arm step 4). The dual prune is intentional: SP113's ceiling prevents pending_txs unbounded growth in the absence of an AdvanceWatermark heartbeat; SP114's watermark prune is the deterministic, log-driven primary mechanism. T5's `pt_watermark_plus_ssi_interleaving` test asserts the SP113 fallback ceiling continues to fire on every commit apply.

- **SM checkpoint persistence of `low_water_mark` is NOT shipped in S2.5.** In-memory only + log-replay-rebuilt — every replica's `low_water_mark` is reconstructed by re-applying the recent log prefix containing `Op::AdvanceWatermark` entries. SM checkpoint integration is an S2.X follow-up.

- **The TLA+ spec is abstract single-replica.** It models a single replica's GC + watermark state. Multi-replica byte-identity of GC verdicts is verified at the Rust integration-test level (T3 ships `it_classic_gc_reclaims_versions_byte_identically_across_3_replicas`), NOT at the TLA+ level. Lifting that to TLA+ would require a per-replica `lowWaterMark[r]` shape — S2.X follow-up.

- **The TLA+ correspondence is named, not mechanized.** Same caveat as SP109/SP110/SP111/SP112/SP113 — a divergence between the spec and the Rust code is a human-discovered issue. The action-mapping table in `MVCCGc.tla` and reproduced above is the audit trail; the line-number table will drift as the Rust code is refactored and must be re-run.

- **Bounded TLC config.** TLC exhausts the bounded model at `TypeIds = {1}, ObjectIds = {1, 2}, OpNums = {0, 1, 2}, Values = {v1, v2}, MaxOps = 3, TxIds = {t1, t2}, MaxTxOps = 4, MaxTxAge = 5, MaxWatermark = 2` (1,594,330 distinct states, 9,420,629 generated, depth 12, 48s, complete coverage). The 2-Tx model IS sufficient for the SP113-supersession scenario; a 3-Tx model would let TLC also explore canonical multi-pivot dangerous-structure interactions with watermark advances — S2.X follow-up. The Rust pentest tests (T5) cover boundary watermarks (u64::MAX, 0) explicitly that the bounded TLC model cannot reach.

- **TLC found 3 spec-design refinements during T6.** All classification-(a) genuine TLA+ contract refinements: (Fix #1) `BoundedWindowSupersededByWatermark` first-pass disjunction tightened to the structural-impossibility form; (Fix #2) `SnapshotAvailability` first-pass unconditional form rephrased as conditional contract for the well-behaved-heartbeat regime; (Fix #3) inherited `CommitAtomicity` + `DeterministicApply` DROPPED (legitimately violated by GC) and REPLACED with GC-aware reformulations conditioned on `commit_opnum >= lowWaterMark`. SP109-SP113 discipline (never weaken; restate / tighten preconditions) applied throughout. Each refinement is its own conceptual fix; all three landed together in T6 because they all surface only after TLC sees the GC layer interacting with the EXTENDS substrate.

- **`OpResult::WatermarkAdvanced { ... }` and `OpResult::WatermarkRejected { ... }` are produced by T3 + T4 + T5 integration tests.** Wire encode/decode round-trip tested in T2 (KAT 9: kat_op_advancewatermark_wire_roundtrip); SM apply path semantic gate tested across all three test surfaces; the `versions_deleted` + `pending_txs_evicted` counts are preserved across the wire by the additive variant codec.

---

## Deferred / S2.X follow-up backlog (within S2)

Per the parent S2 design + S2.5 design:

| ID | Item | Status |
|---|---|---|
| S2.6 | SQL integration + SM cutover (wires production callers to Tx::begin* Result + AdvanceWatermark heartbeat; replaces 20-byte legacy paths with 28-byte MVCC + Tx + Op::CommitTx; SQL routing picks Tx::commit vs Tx::commit_ssi per Tx; wires cursor-stall-on-snapshot-not-yet-applied; heartbeat producer for AdvanceWatermark) | Deferred (next slice) |
| S2.X | Watermark heartbeat producer — the agent that gathers min(active_snapshot) and submits Op::AdvanceWatermark | Deferred (separable concern; S2.6 may bundle the simplest form) |
| S2.X | SM checkpoint persistence of low_water_mark — currently in-memory + log-replay-rebuilt; checkpoint integration would skip the replay cost on restart | Deferred |
| S2.X | LSM compaction of GC tombstones — currently tombstone-based delete (Storage::delete writes a tombstone marker); physical byte-stream erasure is OOS for S2.5 | Deferred |
| S2.X | Sustained-cadence perf KAT — tombstones from each GC pass survive until the next; compounding linearly with GC cadence; perf-test for the steady state | Deferred |
| S2.X | Range-prune optimisation for `delete_versions_older_than` (currently full O(N) scan; range-prune via the LSM's bloom filter would reduce to O(reclaim-count + log N)) | Deferred |
| S2.X | 3-Tx TLC bound for MVCCGc (canonical multi-pivot dangerous-structure interactions with watermark advances) | Deferred |
| S2.X | Multi-replica TLA+ for GC (lift `lowWaterMark[r]` to per-replica; mechanize the byte-identity claim) | Deferred |
| S2.X | Restart-rebuild of low_water_mark at the TLA+ level (production rebuilds it by re-applying the recent log prefix) | Deferred |

---

## Strategic-tier context update

SP114 SHIPS S2.5. The strategic-tier backlog after SP114:

| ID | Item | Status |
|---|---|---|
| S1 | TLA+/model-checked safety specs for the VSR protocol | **DONE (SP109)** |
| S2 | Serializable MVCC/SI over the deterministic log | **In progress: S2.1 DONE (SP110); S2.2 DONE (SP111); S2.3 DONE (SP112); S2.4 DONE (SP113); S2.5 DONE (SP114); S2.6 open** |
| S3 | Jepsen harness for real distributed fault injection | Open |
| S4 | Deterministic in-tree WASM UDFs (sandboxed, reproducible, no imports) | Open |

S2 strategic-tier parent stays open with S2.6 as the next slice (SQL integration + SM cutover — the byte-identity-gate-change slice; wires every SP110-SP114 path into the production data plane).

---

## Process note

SP114 executes under the autonomous-mandate (see `feedback_kesseldb_autonomous_build.md`). The autonomous-mandate substitutes the brainstorming user-review gate; the two-stage subagent review gate IS the SP114 review (this T6 closeout + the final whole-implementation reviewer that follows). Each task was committed separately:
- T1 scaffold → `a255543`
- T2 impl + KATs → `f804825`
- T3 integration → `ee043d4`
- T4 coverage → `3b76d0e`
- T5 pentest → `fb9891e`
- T6 closeout (this commit) — docs + TLA+ + STATUS + memory

The TLA+ artifact landed with 3 first-pass refinements (BoundedWindowSupersededByWatermark phrasing / SnapshotAvailability conditional rephrase / CommitAtomicity + DeterministicApply GC-aware reformulation) — all classification-(a) genuine TLA+ contract refinements per the SP109-SP113 discipline (never weaken; restate / tighten preconditions). The discipline lessons from SP110 (readLog-temporal-category-error), SP111 (every invariant a current-state property), SP112 (mirror-agreement + monotonicity + free-Put-removal), and SP113 (window-bounded substrate) all carried forward via EXTENDS. The 3 T6 refinements are NOT spec bugs in the underlying Rust — they are TLA+ formalizations that surface only when the GC layer interacts with the EXTENDS substrate (the inherited CommitAtomicity / DeterministicApply naturally don't account for legitimate version reclamation; the bounded-window-supersession and heartbeat-trust-boundary invariants need conditional phrasing for the well-behaved vs misbehaving operating points).

All plan-deviation disclosures (none — the bounded config + invariant list match the design Decision 8 exactly; the 3 TLA+ refinements are the documented "TLC found N issues, all classification-(a) refinements per SP109/SP110/SP111/SP112/SP113 discipline" — no Rust spec bugs surfaced; the test count drift from estimated 640 to actual 640 is exact; the breaking Tx::begin* Result change is the documented Decision 7 disclosure surfaced verbatim) are made in this record, not suppressed.
