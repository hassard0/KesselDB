# S2.5 — Garbage Collection + Dynamic Watermark (Reclaim Obsolete MVCC Versions; Supersede SP113's MAX_TX_AGE Bounded Window): Design

**Date:** 2026-05-24
**Status:** Approved (autonomous mandate — see Process Note)
**Strategic-tier item:** S2.5 sub-slice of S2 (Serializable MVCC / Snapshot
Isolation) in the THESIS.md S1–S4 backlog. The **fifth** built sub-slice of
S2 after S2.1/SP110 (MVCC versioned-storage primitive), S2.2/SP111 (Tx
context + read-set), S2.3/SP112 (SI write-side + conflict detection at SM
apply time), and S2.4/SP113 (Cahill SSI dangerous-structure detector +
pending_txs window with a FIXED MAX_TX_AGE bounded-window honest false-
negative). **SP114** in the subproject numbering.
**Builds on:**
- Project THESIS — `docs/THESIS.md`.
- S2 parent design — `docs/superpowers/specs/2026-05-23-mvcc-si-design.md`.
- S2.1 record (SP110) — `docs/superpowers/specs/2026-05-23-kesseldb-subproject110-mvcc-s2-1.md`.
- S2.2 record (SP111) — `docs/superpowers/specs/2026-05-24-kesseldb-subproject111-mvcc-tx-s2-2.md`.
- S2.3 record (SP112) — `docs/superpowers/specs/2026-05-24-kesseldb-subproject112-mvcc-si-s2-3.md`.
- S2.4 record (SP113) — `docs/superpowers/specs/2026-05-24-kesseldb-subproject113-mvcc-ssi-s2-4.md`.
- S2.4 design — `docs/superpowers/specs/2026-05-24-mvcc-si-s2-4-design.md`.
- S2.4 TLA+ artifact — `kesseldb-tla/MVCCSsi.tla` + `.cfg` + baseline TLC
  run (`kesseldb-tla/results/2026-05-24-mvcc-ssi-baseline.txt`).
- The MVCC module surface shipped in SP110: `crates/kessel-storage/src/mvcc.rs`
  (`make_versioned_key`, `decode_commit_opnum`, `put_versioned`,
  `get_at_snapshot`, `has_version_in_range`, `SnapshotRead`,
  `MvccKeyError`, `VERSIONED_KEY_LEN`, `PREFIX_LEN`).
- The SSI module surface shipped in SP113: `crates/kessel-storage/src/ssi.rs`
  (`PendingTxRecord`, `sorted_vec_intersects`, `detect_dangerous_structure`,
  `prune_pending_txs`, `MAX_TX_AGE = 4096`).
- The Tx module shipped through SP113: `crates/kessel-storage/src/tx.rs`
  (`Tx<'a, V>` with `begin`, `begin_rw`, `begin_ssi`, `read`, `write`,
  `read_set`, `write_set`, `snapshot_opnum`, `commit`, `commit_ssi`,
  `commit_read_only`, `abort`; `TxCommitOutcome`; `TxError`).
- The SM apply path shipped through SP113: `crates/kessel-sm/src/lib.rs`
  (`StateMachine` with `pending_txs`, `Op::CommitTx` arm running both the
  SP112 deterministic write-write conflict check and the SP113 Cahill SSI
  dangerous-structure detector gated on non-empty read_set).
- The proto Op + result shape shipped through SP113: `crates/kessel-proto/src/lib.rs`
  (`Op::CommitTx { snapshot_opnum, write_set, commit_opnum, read_set }` at
  wire tag 44; `OpResult::TxCommitted`/`TxAborted` at wire tags 9/10;
  `AbortReason` with four variants at sub-tags 0/1/2/3).
- **External background:** standard MVCC garbage-collection literature —
  PostgreSQL's `VACUUM` + `OldestXmin` horizon; CockroachDB's `GCThreshold`
  per-range watermark; Spanner's `safe_time` advancement. KesselDB's
  variant: a SINGLE GLOBAL watermark advanced by a deterministic SM op so
  the apply path performs the same reclamation byte-identically on every
  replica.

---

## Process note (autonomy + brainstorming gate)

Produced under the standing overnight autonomous-build mandate
(`feedback_kesseldb_autonomous_build` + the strategic-tier mandate
`feedback_kesseldb_strategic_tier`): "build the backlog autonomously,
BOLD choices, don't wait for approval, keep the two-stage review gate
+ full tests + pentest passes." **The brainstorming user-review gate
is substituted by this documented decision record** — the 10 brainstorm
decisions below are resolved boldly in this document; the user does
not re-review them before the plan executes. **The two-stage subagent
review gate is preserved** for every substantive task (T2/T3/T5/T6),
with the final whole-implementation reviewer dispatched at the end of
T6, exactly as SP110/SP111/SP112/SP113 did.

---

## Strategic-tier framing

S2.5 is the **fifth sub-slice of S2** in the THESIS.md backlog. SP114
in the subproject numbering. The parent S2 design
(`docs/superpowers/specs/2026-05-23-mvcc-si-design.md`) decomposes S2
into 6 sub-slices: S2.1 (SP110 — versioned-storage primitive) →
S2.2 (SP111 — Tx context + read-set) → S2.3 (SP112 — SI write-side +
deterministic conflict at SM apply) → S2.4 (SP113 — SSI promotion via
Cahill dangerous-structure detection) → **S2.5 (this slice — GC of
obsolete MVCC versions + dynamic watermark protocol that supersedes
SP113's fixed MAX_TX_AGE bounded window)** → S2.6 (SQL integration +
SM cutover). This slice ships TWO coupled deliverables: (1) **bounded
storage** — reclaim obsolete MVCC versions older than the oldest
active read snapshot; (2) **bounded-window supersession** — replace
SP113's fixed `MAX_TX_AGE = 4096` pruning horizon with a dynamic
watermark derived from the actual concurrency level, closing the
honest-disclosed false-negative documented in the SP113 record's
Decision 5.

**S2.1 → S2.2 → S2.3 → S2.4 → S2.5 dependency chain.** S2.1 shipped the
append-only versioned-storage primitive AND named "S2.5 GC reclaims
pre-watermark tombstones" as a forward-link in `SnapshotRead`'s
doc-comment. S2.2 shipped the Tx context whose `snapshot_opnum` field
is the primary input to the watermark heartbeat (S2.5 reads it).
S2.3 shipped the SM apply seam at which deterministic ops produce
verdicts. S2.4 shipped the `pending_txs` map with the
honest-disclosed `MAX_TX_AGE = 4096` fixed bound AND named S2.5 as the
slice that would supersede it (see SP113 record's Decision 5; see
`crates/kessel-storage/src/ssi.rs:31` MAX_TX_AGE doc-comment; see
`kesseldb-tla/MVCCSsi.tla` line ~70 "GC / watermark / version
reclamation is NOT modeled (carried forward from MVCCStorage + MVCCTx
+ MVCCSi). S2.5 follow-up."). **S2.5 cashes BOTH forward-links** by
adding the `Op::AdvanceWatermark(low_water_mark)` deterministic op,
the `mvcc::delete_versions_older_than` primitive, the watermark-
driven `prune_pending_txs` replacement, and the snapshot-too-old read
rejection.

**The thesis-fit headline of S2.5.** Per the parent S2 design Decision
6 (verbatim from `docs/superpowers/specs/2026-05-23-mvcc-si-design.md`):
"low_water_mark advancement is itself a deterministic op (apply replays
it identically across replicas), so GC is deterministic." Every
replica's `Op::AdvanceWatermark(W)` apply runs the SAME version-
deletion logic against its log-derived MVCC state AND the SAME
`pending_txs` truncation against its log-derived SSI state — the
resulting bytes-on-disk + in-SM state remain byte-identical across
replicas after every advance. **In contrast, every non-deterministic
MVCC GC implementation (PostgreSQL's per-backend xmin computation +
autovacuum daemon; CockroachDB's per-range GC queue driven by wall-
clock TTL; Spanner's `safe_time` advanced by Paxos-coordinated
TrueTime ticks) is either non-deterministic across replicas (per-
node GC progress diverges; the storage state diverges accordingly) or
requires an additional distributed-coordination protocol to keep
replicas in sync.** KesselDB's GC pays approximately zero coordination
cost because the watermark advance is itself a totally-ordered log op
that flows through the same VSR pipe every other op uses; every
replica's deterministic apply executes the same reclamation
deterministically. **This is the second direct expression of the
"deterministic replicated SQL" pillar in the S2 backlog**: GC becomes
a structural property of the log, not a background-thread or a
distributed-coordination protocol. **S2.5 also operationalises the
"replayability" pillar at the storage tier**: the storage state after
any prefix of the log is reproducible byte-identically by replaying
the same log against an empty store — and GC ops are a first-class
participant in that replay, so the post-GC steady-state size is itself
log-deterministic.

---

## Problem

After S2.4/SP113 ships, KesselDB has:
- An append-only MVCC versioned-storage primitive (`kessel-storage::mvcc`)
  with no version reclamation. Storage grows monotonically with every
  Tx commit.
- A pending_txs window with a FIXED `MAX_TX_AGE = 4096` bounded-window
  pruning horizon. Per SP113 Decision 5 (honest disclosure): a Tx whose
  snapshot is older than `current_apply_opnum - MAX_TX_AGE` cannot be
  evaluated for SSI dangerous-structure participation because the
  relevant pending_txs records have been evicted. **This is a documented
  false-negative**: SSI can in principle MISS a write-skew anomaly
  whose participating Tx have aged out of the window.
- An MVCCSsi.tla TLA+ artifact that explicitly disclaims modeling GC /
  watermark / version reclamation (see `kesseldb-tla/MVCCSsi.tla`
  OUT-OF-SCOPE disclosures #3 and #4).

What's still missing — and what S2.5 ships:

1. **Bounded storage.** MVCC version chains grow without bound. A
   logical key that is repeatedly updated keeps every old version
   forever. After enough Tx, storage size becomes unbounded relative
   to the live data size. **S2.5 reclaims versions older than the
   global low_water_mark** — defined as the oldest snapshot_opnum
   pinned by any currently-active reader.
2. **Bounded-window supersession.** SP113's fixed `MAX_TX_AGE = 4096`
   horizon does not adapt to the actual concurrency level. Under a
   workload with long-running readers (snapshot_opnum way behind
   current_apply_opnum) the horizon is too short — SSI misses
   anomalies. Under a workload with short readers (snapshot_opnum
   close to current_apply_opnum) the horizon is too long — pending_txs
   carries records that will never participate in any verdict. **S2.5
   replaces the fixed horizon with the dynamic watermark**: the
   pruning threshold becomes `low_water_mark` itself; pending_txs only
   evicts records strictly older than every live reader's snapshot.
3. **A deterministic GC op.** The reclamation MUST be a state-machine
   op (Decision 1 below) so apply re-runs the same reclamation on every
   replica byte-identically. A background-thread GC would diverge
   storage state across replicas even with the same log prefix —
   thesis violation.
4. **A snapshot-too-old read rejection.** Once GC has reclaimed
   versions older than `low_water_mark`, a Tx that attempts to read
   at `snapshot_opnum < low_water_mark` CANNOT be served (the visible
   versions have been deleted). **S2.5 adds a typed read-time
   rejection** — `Tx::begin` / `Tx::begin_rw` / `Tx::begin_ssi`
   return `Err(TxError::SnapshotTooOld { low_water_mark })` when the
   requested snapshot is below the watermark.
5. **A `MVCCGc.tla` TLA+ specification.** Per the parent S2 design
   Decision 7, every MVCC sub-slice ships its own TLA+ extension.
   S2.5's spec must extend `MVCCSsi` with `low_water_mark` state + the
   `AdvanceWatermark(W)` action + invariants gating the version-
   reclamation safety property, the watermark monotonicity, and —
   critically — the **BoundedWindowSupersededByWatermark** claim that
   formally encodes "with watermark-driven pruning, no detection
   false-negative can occur if all watermark advances are ≤ min(active
   snapshots)."

S2.5 is the slice that solves all five.

---

## Decisions (bold choices, documented)

### Decision 1 — GC trigger mechanism: **deterministic SM Op (`Op::AdvanceWatermark`), NOT a background thread**

Two structural options for triggering GC:

- **(a) Deterministic SM op.** `Op::AdvanceWatermark { low_water_mark }`
  flows through VSR like every other op. Every replica's deterministic
  apply runs the SAME reclamation logic against the SAME log-derived
  MVCC state + pending_txs state. The storage AND pending_txs state
  after any prefix of the log is byte-identical across replicas.
- **(b) Background thread.** A replica-local GC daemon scans the
  storage and deletes obsolete versions on its own clock. Replicas
  may diverge in physical storage even given the same log prefix
  (different GC schedules); SM state stays equivalent (snapshot
  reads return the same logical values) but bytes-on-disk diverge.
  Thesis violation — the determinism contract is "bytes-on-disk
  identical across replicas," not "same logical reads."

**Taken: (a) — `Op::AdvanceWatermark` deterministic SM op.**

**Why bold over safe.** The parent S2 design Decision 6 explicitly
calls this out as the bold-deterministic angle ("low_water_mark
advancement is itself a deterministic op, so GC is deterministic").
Option (b) is what every non-deterministic MVCC GC ships, including
PostgreSQL's autovacuum + CockroachDB's per-range GC queue, because
those systems lack a totally-ordered log per replica. KesselDB has
that log; the cost of routing GC through it is one new Op variant +
one new SM apply arm. The benefit is the deterministic-bytes-on-disk
invariant carries to the post-GC state. Per the SP110 design Decision
3 doc-comment "Append-only: prior versions of the same `(type_id,
object_id)` remain in the store until S2.5 GC reclaims them," S2.5
inherits the determinism contract via the same SM apply seam.

The watermark VALUE the op carries is computed by a SEPARATE heartbeat
mechanism — see Decision 2. The heartbeat lives outside the
deterministic apply path; the apply path TRUSTS the op's
`low_water_mark` field (subject to the monotonicity validation in
Decision 5) and runs the reclamation deterministically. **This
separation is intentional**: the watermark computation is inherently
non-deterministic (each replica reports its local min(active
snapshot)); the watermark RESULT is deterministic by virtue of being
serialized as an op.

**Thesis fit:** `deterministic` (the GC reclamation is a function of
the log prefix, identical on every replica); `verifiable` (the
`MVCCGc.tla` extension mechanically proves the
**NoVersionBelowWatermark** invariant — after any watermark advance,
no MVCC version with commit_opnum < low_water_mark remains);
`honest-docs` (the rejected background-thread option is documented
as a thesis violation, not silently discarded; the watermark-
computation-vs-watermark-application split is named).

### Decision 2 — Watermark source: **caller-supplied via the Op; SM validates monotonicity + commit_opnum ceiling; heartbeat that produces the value is a SEPARATE non-deterministic concern**

Where does the new `low_water_mark` value come from? Three options:

- **(a) Precise SM-side computation.** Compute `min(snapshot_opnum)`
  over all currently-active `Tx` whose state the SM tracks. Most
  precise (the watermark always equals the actual oldest live read).
  **Problem**: the SM does NOT currently track per-Tx state — Tx is a
  client-side construct in S2.1–S2.4. Adding SM-side Tx tracking is
  out of scope for S2.5 (it would be a new SM responsibility on the
  scale of pending_txs); deferred to a hypothetical S2.X.
- **(b) Loose commit-opnum-lag.** Compute `current_commit_opnum -
  LAG_CONSTANT`. Simple but loose — a long-running reader pinned at
  an old snapshot would be incorrectly cut off; correctness violation.
- **(c) Caller-supplied.** The `Op::AdvanceWatermark` op carries the
  proposed `low_water_mark` value as a payload field. The SM
  validates (monotonicity + commit_opnum ceiling per Decision 5) and
  trusts the value. The actual computation lives in a separate
  heartbeat mechanism (per-replica reports its local `min(active
  snapshot_opnum)`; the leader gathers + computes the global min;
  the leader periodically emits an `AdvanceWatermark` op).

**Taken: (c) — caller-supplied watermark; SM validates + trusts; heartbeat is a separate concern.**

**Why bold over safe.** Option (c) preserves the **SM apply path's
deterministic-pure-function discipline**: the SM has no view of "live
Tx" outside what it can derive from the log (Tx is a client-side
construct, deliberately). Option (a) would require the SM to track
per-Tx state across replicas, which would itself require a
distributed Tx-registration protocol — the very kind of coordination
this slice eliminates. Option (b) is fundamentally incorrect.

**The heartbeat mechanism is OUT OF SCOPE for S2.5.** S2.5 ships the
Op shape, the apply arm, the validation, the reclamation, the
pending_txs replacement, and the read-time rejection. The heartbeat
(which produces the `low_water_mark` value the op carries) is named
as a separate concern, scoped for S2.X (or for S2.6 if it shows up in
SQL integration). **The SM apply arm DOES NOT CARE where the
watermark value came from** — it validates it (Decision 5) and runs
the reclamation. Production deployments will hook a heartbeat
producer to the leader's op-submission path; tests in S2.5 submit
`Op::AdvanceWatermark` ops directly to exercise the apply path.

**The wire shape:**

```rust
// kessel-proto::Op (wire tag 45 — first free tag after SP112+SP113's 44):
AdvanceWatermark {
    /// The proposed new low_water_mark in opnum space. The SM validates
    /// monotonicity (must be > current low_water_mark per Decision 5)
    /// and commit_opnum ceiling (must be <= current SM commit_opnum
    /// per Decision 5). On validation failure: the apply returns
    /// OpResult::WatermarkRejected { reason } (Decision 8) and storage
    /// is UNCHANGED. On validation success: storage is reclaimed +
    /// pending_txs is pruned per Decisions 3+4; SM persists the new
    /// watermark per Decision 6.
    low_water_mark: u64,
}
```

**Thesis fit:** `deterministic` (the apply path is a pure function of
the op + the prior SM state — no source of non-determinism from where
the value came from); `honest-docs` (the heartbeat-vs-apply split is
documented; the rejected options (a) SM-side Tx tracking and (b) loose
lag are named).

### Decision 3 — Version-deletion primitive on `mvcc.rs`: **`pub fn delete_versions_older_than(store, low_water_mark) -> Result<usize, MvccKeyError>` — scan-and-delete; full LSM scan complexity disclosed**

Add a new primitive to `crates/kessel-storage/src/mvcc.rs`:

```rust
/// SP114 / S2.5: Garbage-collect MVCC versions whose commit_opnum is
/// strictly less than `low_water_mark`. Returns the count of versions
/// deleted (for observability + test gating).
///
/// Algorithm: scan the full versioned-storage range, decode every
/// 28-byte key's commit_opnum, delete every entry whose commit_opnum
/// < low_water_mark. Tombstones are also reclaimed (they are versions
/// with `value = None` — the SP110 SnapshotRead::Tombstoned distinction
/// is only meaningful at-or-above the watermark).
///
/// Complexity: O(N) where N is the total number of versioned entries
/// in storage. For S2.5 a full LSM scan is acceptable — the SP110
/// versioned-storage primitive is the only producer of 28-byte keys.
/// A range-pruning optimisation (skip prefixes that have no versions
/// below watermark) is an S2.X follow-up.
///
/// Determinism: the scan order is BTreeMap-deterministic (sorted by
/// 28-byte key); the deletion order is therefore deterministic; the
/// resulting LSM state is byte-identical across replicas given the
/// same pre-GC state + the same low_water_mark.
///
/// IMPORTANT: this function does NOT update the SM's
/// `low_water_mark` field — that is the SM apply arm's responsibility
/// (Decision 6).
pub fn delete_versions_older_than<V: Vfs>(
    store: &mut Storage<V>,
    low_water_mark: u64,
) -> Result<usize, MvccKeyError>
```

**Why bold over safe.** Three options:

- **(a) Full scan.** Walk every versioned key; delete those with
  `commit_opnum < low_water_mark`. O(N) per advance.
- **(b) Per-key scan.** Track every distinct logical key written
  since the last GC; on advance, scan each key's version chain and
  delete pre-watermark entries. O(K log V) where K is the live-key
  count and V is the average versions-per-key. Requires a per-key
  tracking structure.
- **(c) Bloom-filter / RangePrune optimisation.** Skip prefix ranges
  that demonstrably have no pre-watermark versions. Complicated.

**Taken: (a) — full scan.** For S2.5's shipping target the full scan
is acceptable: the GC ops will fire at a low rate (every N apply ops
in production; every test-fired op in S2.5 tests); the LSM scan is
linear in the total version count which is bounded by the workload.
**Honest disclosure**: under a heavy-write workload with frequent GC
ops, the O(N) per-advance cost may dominate. **An S2.X follow-up may
adopt (b) or (c) when benchmarks show the cost.** The S2.5 shipping
target is the deterministic-correctness contract, not the
performance-optimisation surface.

**Counts deleted** are returned in the `OpResult::WatermarkAdvanced`
shape (Decision 8) for observability and test gating. T2's KAT
`kat_advance_watermark_reclaims_pre_watermark_versions_count` asserts
the count is exactly the expected delete count for a constructed
workload.

**Thesis fit:** `deterministic` (full-scan order is sorted-key
deterministic; the delete sequence is byte-identical across replicas);
`zero-dep` (uses only the existing `Storage::scan_range_versions` +
`Storage::delete` surface from SP110 + the kessel-storage core);
`honest-docs` (the O(N) per-advance cost is named; the
optimisation follow-ups (b)/(c) are named for S2.X).

### Decision 4 — Pending_txs pruning replacement: **watermark-driven `prune_pending_txs(pending_txs, low_water_mark)` REPLACES SP113's fixed-MAX_TX_AGE pruning; SP113 MAX_TX_AGE retained as FALLBACK ceiling only**

S2.4/SP113 ships `ssi::prune_pending_txs(pending_txs, current_commit_opnum,
max_tx_age)` which evicts records older than
`current_commit_opnum - max_tx_age` on every commit. The SP113 record
honest-discloses this as a false-negative source.

S2.5 introduces a NEW pruning function:

```rust
/// SP114 / S2.5: Prune pending_txs records whose commit_opnum is
/// strictly less than `low_water_mark`. Replaces SP113's fixed
/// MAX_TX_AGE-driven prune for the SSI dangerous-structure detector.
///
/// Correctness: a Tx evicted at low_water_mark cannot participate in
/// any dangerous structure with a still-live reader, because by
/// definition low_water_mark = min(active_snapshot_opnum) — every
/// live reader pins a snapshot >= low_water_mark; an evicted Tx's
/// commit_opnum < low_water_mark, so no live reader's snapshot is
/// older than the evicted Tx's commit. The Cahill rw-edge
/// concurrent-Tx condition (concurrent ⇔ snapshot < commit_opnum)
/// requires snapshot < (some pending Tx's commit_opnum); for an
/// evicted record this is provably FALSE. **This is the formal
/// closure of the SP113 bounded-window false-negative.**
///
/// Determinism: BTreeMap::split_off — deterministic across replicas.
pub fn prune_pending_txs_by_watermark(
    pending_txs: &mut BTreeMap<u64, PendingTxRecord>,
    low_water_mark: u64,
)
```

**The SP113 MAX_TX_AGE-driven prune is RETAINED as a fallback ceiling
only** — the SM `Op::CommitTx` apply arm still calls
`ssi::prune_pending_txs(pending_txs, current, MAX_TX_AGE)` defensively
(in case the watermark heartbeat stalls and `low_water_mark` does not
advance). With both pruners in effect: the SP113 ceiling is the SAFETY
NET (bounded memory if no watermark advances ever happen); the S2.5
watermark-driven prune is the PRECISE bound (closes the false-
negative when watermark advances normally).

**Behaviour ordering at apply.** The SM `Op::AdvanceWatermark` apply
arm calls `prune_pending_txs_by_watermark(low_water_mark)` AFTER
calling `delete_versions_older_than(low_water_mark)` — version
reclamation first, then SSI bookkeeping reclamation. The SM
`Op::CommitTx` apply arm continues to call the SP113
`prune_pending_txs(MAX_TX_AGE)` defensively at the top of its SSI
inner branch as before; the watermark-driven prune is NOT called on
every commit (only on every `AdvanceWatermark` op apply).

**Why bold over safe.** Two options:

- **(a) Replace SP113 prune entirely.** Drop the MAX_TX_AGE constant +
  the SP113 prune call. **Risk**: if no `AdvanceWatermark` op ever
  arrives (heartbeat down), pending_txs grows unboundedly.
- **(b) Add watermark-driven prune AS A NEW PATH; keep SP113 prune
  as the fallback ceiling.** Watermark-driven prune runs on every
  `AdvanceWatermark` op apply; SP113 prune runs on every commit apply
  as a defensive ceiling. **Belt-and-suspenders**: even if the
  watermark protocol stalls, the SP113 ceiling bounds memory.

**Taken: (b) — add as new path; keep SP113 as fallback.**

The SP113 prune is structurally cheap (`BTreeMap::split_off` against
a far-back horizon usually evicts zero records when the watermark is
advancing normally); keeping it costs nothing in the steady state and
provides a safety net for the degenerate "watermark heartbeat
crashed" case. The SP113 false-negative is closed for any workload
where watermark advances ≥ pending_txs prune frequency (i.e., the
normal case); when the watermark stalls the SP113 ceiling re-engages
with its original false-negative — **but at that point the SP114
honest-disclosure is "watermark heartbeat is down, please restart
the watermark producer."**

**Thesis fit:** `deterministic` (both pruners are BTreeMap-
deterministic); `honest-docs` (the SP113 fallback ceiling + the
heartbeat-stall behaviour are named; the SP113 false-negative is
formally closed for the watermark-active case).

### Decision 5 — Op::AdvanceWatermark wire shape + validation: **tag 45; `{ low_water_mark: u64 }` payload; SM validates monotonicity + commit_opnum ceiling; rejection returns `OpResult::WatermarkRejected { reason }`**

**Wire tag.** `Op::AdvanceWatermark` is assigned wire tag **45**
(immediately after SP112's `Op::CommitTx` at tag 44). No SP1–SP113
tag changes.

**Payload.** `{ low_water_mark: u64 }` — a single u64 field. Encoded
as a single `u64` BE/LE per the kessel-proto convention (whichever
the existing codec uses for `commit_opnum`; matched on the encode
side for consistency).

**Validation at apply.** The SM apply arm validates:

1. **Monotonicity.** `low_water_mark > self.low_water_mark` (strict).
   A `low_water_mark <= self.low_water_mark` op is REJECTED. This
   prevents replay-based-attacks AND ensures every advance is a
   real advance. **Equality is rejected**: an advance to the same
   value would be a no-op AND would create a non-deterministic state
   transition source (does the apply count the advance?). Strict
   monotonicity is simpler.
2. **Commit_opnum ceiling.** `low_water_mark <= self.commit_opnum`
   (or whatever the SM-internal "highest applied op" tracker is).
   An advance past the current commit_opnum would mark
   not-yet-committed versions for reclamation — incoherent.
   **Equality IS allowed**: an advance to the current commit_opnum
   says "no version is still needed by any live reader"; the next
   commit will produce a version >= low_water_mark which survives
   trivially.

**Rejection shape.** `OpResult::WatermarkRejected { reason: WatermarkRejection }`
— append-only addition to the `OpResult` enum at a new tag (per
kessel-proto convention). The rejection variants:

```rust
#[non_exhaustive]
pub enum WatermarkRejection {
    /// Proposed watermark is <= current watermark (monotonicity).
    NotMonotonic { proposed: u64, current: u64 },
    /// Proposed watermark exceeds the SM's current commit_opnum.
    AboveCommitCeiling { proposed: u64, current_commit: u64 },
}
```

**Acceptance shape.** `OpResult::WatermarkAdvanced { new_low_water_mark: u64, versions_deleted: usize, pending_txs_evicted: usize }`
— the counts are SP114-only fields surfaced for observability + test
gating.

**Why bold over safe.** Two options:

- **(a) Strict monotonicity + commit_opnum ceiling.** Crisp validation;
  bounded state-machine behaviour; deterministic outcome.
- **(b) Loose validation — accept any value; clamp internally.**
  Avoids the rejection branch; but the SM no longer rejects bad input
  — every heartbeat error becomes an internal-state silent change.

**Taken: (a) — strict validation.**

The SM apply path becomes a pure function with crisp pre/postconditions;
test cases can exercise both the success and failure branches; pentest
T5 can submit adversarial values and assert deterministic rejection
behaviour.

**Thesis fit:** `deterministic` (apply outcome is a pure function of
`(op, prior_state)` — succeed or reject deterministically);
`honest-docs` (the two rejection variants are named; the WatermarkAdvanced
counts are documented as observability surface).

### Decision 6 — SM persistent watermark state: **`StateMachine.low_water_mark: u64` — new persistent field, restored on replica restart, initial 0**

The SM gains ONE new field:

```rust
pub struct StateMachine<V: Vfs> {
    // ... SP1-SP113 fields unchanged ...

    /// SP114 / S2.5: The global low_water_mark in opnum space. Any MVCC
    /// version with commit_opnum < low_water_mark has been reclaimed
    /// by a prior Op::AdvanceWatermark apply. Any Tx with
    /// commit_opnum < low_water_mark has been evicted from pending_txs.
    /// Any Tx::begin_* request with snapshot_opnum < low_water_mark
    /// is rejected with SnapshotTooOld.
    ///
    /// Persisted via the existing SM checkpoint mechanism (Decision 6).
    /// Restored on replica restart. Initial value 0 (no GC has happened;
    /// every snapshot >= 0 is serveable, which is every snapshot).
    /// Strictly monotonic-increasing across the lifetime of the SM (per
    /// Decision 5 validation).
    low_water_mark: u64,
}
```

**Persistence.** Per existing SM checkpoint discipline (whatever
kessel-sm currently uses for `commit_opnum`, `pending_txs` —
note pending_txs is rebuild-from-log per SP113 Decision 5, not
checkpointed; `commit_opnum` is checkpointed if any SM state is). The
`low_water_mark` joins the checkpointed-on-disk state — restoration
on restart is critical because pre-watermark versions have been
deleted; a replica that restarted with `low_water_mark = 0` would
incorrectly accept reads at obsolete snapshots that storage cannot
serve.

**Restart-rebuild fallback.** If the SM has no checkpoint mechanism
yet (S2.X follow-up), the `low_water_mark` is reconstructed by
re-applying every `Op::AdvanceWatermark` in the log on startup —
exactly the same deterministic apply path that built it
pre-crash. **This works because the apply is monotonic**: replaying
all advance ops in order yields the same final low_water_mark.
**Honest disclosure**: this fallback is O(M) where M is the number
of AdvanceWatermark ops in the log; for a deployment with frequent
GC this could become a startup cost. The SM checkpoint path is the
proper long-term answer.

**Why bold over safe.** Two options:

- **(a) Persisted field, restore on restart.** The state is durable;
  no log-replay fallback needed beyond the existing SM checkpoint
  flow.
- **(b) In-memory only, rebuild from log on startup.** Simpler now;
  works because of monotonicity; slower startup.

**Taken: (a) — persisted, with (b) as the safety fallback if the
checkpoint flow is not yet built out.** S2.5 ships the field as
in-memory + log-replay-rebuild (mirrors SP113's pending_txs
in-memory design); a future S2.X SM-checkpoint slice can promote
it to checkpointed state.

**Thesis fit:** `deterministic` (the field's value is a pure function
of the log prefix — `max(low_water_mark across all AdvanceWatermark
ops in prefix)`); `replayable` (restart re-derives via log replay).

### Decision 7 — Read path watermark check: **`Tx::begin` / `Tx::begin_rw` / `Tx::begin_ssi` reject `snapshot_opnum < low_water_mark` with `Err(TxError::SnapshotTooOld { low_water_mark })`**

Once GC has reclaimed versions older than `low_water_mark`, a Tx that
attempts to read at `snapshot_opnum < low_water_mark` CANNOT be
served correctly — the visible versions have been deleted; reads
would return wrong-version data (or `NotYetWritten` for keys that
DID have a pre-watermark version).

**The fix.** Add a typed read-time rejection.

```rust
#[non_exhaustive]
pub enum TxError {
    // SP112 variant:
    SnapshotOutOfRange,

    // SP113 carried forward (no SSI-specific variant here; the SSI abort
    // surfaces via TxCommitOutcome::AbortedDangerousStructure).

    // SP114 / S2.5 — NEW:
    /// Requested snapshot_opnum is below the SM's low_water_mark; the
    /// versions that would be visible have been reclaimed by a prior
    /// Op::AdvanceWatermark apply. Replay with a fresh snapshot
    /// >= low_water_mark.
    SnapshotTooOld { low_water_mark: u64 },
}
```

**The `Tx::begin_*` API surface** gains an `Err` path that previously
did not exist:

```rust
impl<'a, V: Vfs> Tx<'a, V> {
    // SP111 — UNCHANGED on success; NEW Err path:
    pub fn begin(store: &'a Storage<V>, snapshot_opnum: u64) -> Result<Self, TxError>;
    pub fn begin_rw(store: &'a mut Storage<V>, snapshot_opnum: u64) -> Result<Self, TxError>;
    // SP113 — UNCHANGED on success; NEW Err path:
    pub fn begin_ssi(store: &'a mut Storage<V>, snapshot_opnum: u64) -> Result<Self, TxError>;
}
```

**Wire: how does the Tx know `low_water_mark`?** The standalone Tx
form (used in S2.1–S2.5 tests) does not have direct access to the
SM's watermark — Tx lives in `kessel-storage`, the SM lives in
`kessel-sm`. **Resolution**: the watermark is a `Storage`-level
state (mirrors SP113's `pending_txs` resolution — the Tx form passes
a local empty pending_txs). For S2.5: add a `Storage::low_water_mark()
-> u64` accessor that returns the storage's known watermark; `Tx::begin_*`
reads it and validates. The storage's watermark is set via a new
`Storage::set_low_water_mark(u64)` method that the SM apply arm calls
on `Op::AdvanceWatermark` apply. This keeps `Storage` as the
single-process source of truth for the watermark visible to the Tx
layer.

**Why bold over safe.** Three options:

- **(a) Hard error (`SnapshotTooOld`).** Caller must replay with a
  fresh snapshot.
- **(b) Silent re-pin to `low_water_mark`.** The Tx is created at the
  watermark; the caller gets data, but data potentially newer than
  it expected.
- **(c) Best-effort serve.** Try to serve; return wrong data on
  reclaimed keys.

**Taken: (a) — hard error.**

Option (b) silently changes semantics; option (c) is correctness-
violating. Option (a) is what PostgreSQL ships ("ERROR: snapshot too
old") and what every correct MVCC GC implementation does.

**API breakage.** `Tx::begin*` returning `Result` instead of `Self`
breaks SP111+SP112+SP113 callers. **Honest disclosure**: this is a
real breaking change. All in-tree callers (tests, KATs) are updated
in T1 + T2; no production callers exist yet (S2.6 is the production-
caller slice). **This is acceptable** under the autonomous mandate
because S2.5 is the natural last opportunity to land the breaking
change before S2.6 wires production callers.

**Thesis fit:** `honest-docs` (the API breakage is named; the
rejected silent-re-pin and best-effort-serve options are
documented as semantically wrong); `verifiable` (the
**SnapshotAvailability** invariant in `MVCCGc.tla` mechanically
encodes "snapshot_opnum >= low_water_mark ⇒ Tx can be served;
snapshot_opnum < low_water_mark ⇒ Tx is rejected").

### Decision 8 — TLA+ verification: **`MVCCGc.tla` extends `MVCCSsi` with `low_water_mark` state + `AdvanceWatermark(W)` action + 5 invariants including the formal SP113 supersession claim**

Per the parent design Decision 7 + SP110/SP111/SP112/SP113 discipline,
S2.5 ships a TLA+ extension. The spec EXTENDS `MVCCSsi` (the SP113
spec) so the GC layer is checked over the same versioned-storage + Tx
+ SI + SSI model TLC has already verified.

**File:** `kesseldb-tla/MVCCGc.tla` — `EXTENDS MVCCSsi`.

**State variable additions:**
- `lowWaterMark` — a TLA+ Nat (natural number) initialised to 0.
  Strictly monotonic-increasing per the `AdvanceWatermark` action's
  postcondition.

**Actions (additions over SP113's BeginSsi / CommitSsi etc.):**
- `AdvanceWatermark(W)` — the GC + pending_txs prune + watermark
  update action at proposed value W. Precondition:
  `W > lowWaterMark` AND `W <= opCount`. Semantics:
  1. **Version reclamation.** Remove from `versions` every record
     whose `commit_opnum < W`. (Mirrors `delete_versions_older_than`.)
  2. **Pending_txs pruning.** Remove from `pendingTxs` every record
     whose `commit_opnum < W`. (Mirrors `prune_pending_txs_by_watermark`.)
  3. **Watermark update.** `lowWaterMark' = W`.
  4. **Storage delta on Tx state.** No change to `txs` — Tx state is
     a separate variable; watermark only affects future `Begin*`
     actions via the SnapshotAvailability precondition.
- `BeginSi(t, s)` / `BeginRw(t, s)` / `BeginSsi(t, s)` — extend each
  Begin action's precondition with `s >= lowWaterMark`. A Begin
  action with `s < lowWaterMark` is BLOCKED in the TLA+ model
  (the action is not enabled); the Rust counterpart returns
  `Err(TxError::SnapshotTooOld)` which is the runtime mirror of the
  TLA+ disabled-action.

**Invariants (the verifiable claims):**
- All 16 MVCCSsi invariants preserved.
- **TypeOKGc** — well-typed GC state-space (extends `TypeOKSsi` with
  `lowWaterMark: Nat`).
- **WatermarkMonotonic** — `lowWaterMark` never decreases. Stated as
  a TLA+ STABILITY property over the next-state relation.
- **NoVersionBelowWatermark** — after any state, for every version
  record in `versions`, `commit_opnum >= lowWaterMark`. (Equivalently:
  GC actually reclaims what it claims to reclaim.)
- **NoPendingTxBelowWatermark** — after any state, for every record
  in `pendingTxs`, `commit_opnum >= lowWaterMark`. (Equivalently:
  the watermark-driven prune actually evicts what it claims to evict.)
- **SnapshotAvailability** — for every Tx t with status="Active" or
  status="Committed", `txs[t].snapshot >= lowWaterMark`. (Equivalently:
  the read-time rejection actually prevents Tx from being created at
  an obsolete snapshot.)
- **BoundedWindowSupersededByWatermark** — for any committed schedule
  in which every `AdvanceWatermark(W)` satisfies `W <=
  min({txs[t].snapshot : t \in TxIds /\ txs[t].status \in {"Active",
  "Committed"}})` (i.e., the watermark advances only up to the actual
  minimum active snapshot), **no SSI dangerous-structure false-
  negative can occur**: every dangerous structure that would form
  among any two concurrent Tx is detectable because the relevant
  pending_txs records are still present (they have not been evicted
  by the watermark-driven prune). **This is the formal SP113-
  supersession claim.** Stated as an action-temporal invariant —
  for every `CommitSsi(t, c)` action enabled at a state in which a
  dangerous structure exists per the global rw-edge graph (over the
  full committed history, ignoring the bounded-window), the
  pending_txs records needed to detect it are still in the map.

**Bounded model (initial `.cfg`):**

```
SPECIFICATION Spec

CONSTANTS
    Keys      = {k1, k2}      \* (type_id, object_id) pairs
    Values    = {v1, v2}
    MaxOpnum  = 4
    MaxOps    = 5
    TxIds     = {t1, t2}      \* 2 concurrent Tx — sufficient
    MaxTxOps  = 4             \* Begin + Write + Read + Commit/Abort
    MaxTxAge  = 5             \* SP113 fallback; SP114 watermark
    MaxWatermark = 4          \* watermark <= MaxOpnum

INVARIANT
    TypeOK
    SnapshotImmutability
    ReadSetMonotonic
    ReadSetCoversAllReads
    ReadAtSnapshot
    TxStatusMonotonic
    WriteSetMonotonic
    WriteWriteConflictDetected
    CommitAtomicity
    FirstCommitterWins
    DeterministicApply
    TypeOKSsi
    PendingTxsWindowBounded
    DangerousStructureAborts
    NoWriteSkew
    SerializableEquivalence
    TypeOKGc
    WatermarkMonotonic
    NoVersionBelowWatermark
    NoPendingTxBelowWatermark
    SnapshotAvailability
    BoundedWindowSupersededByWatermark

CHECK_DEADLOCK FALSE
```

**Coverage target.** Per the SP110/SP111/SP112/SP113 precedent, target
complete coverage of the bounded model with ZERO invariant violations.
The state space is larger than MVCCSsi's (the additional watermark
state + the AdvanceWatermark action multiplies the action-space). The
bounded constants are tightened relative to MVCCSsi (Keys = 2, TxIds =
2, MaxOps = 5) to keep wall-clock runtime tractable. **A 2-Tx model IS
sufficient to produce the SP113 supersession counterexample** (the
2-Tx write-skew is the simplest possible workload that the bounded
window could miss; under SP114 the watermark protocol catches it
provided the watermark advances correctly).

**Honest disclosure.** The bounded model verifies the GC invariants
on the abstract MVCCGc spec — not the Rust code itself. The
named-action correspondence (action-mapping table in the spec head)
is the manually-maintained bridge between the spec and the Rust code,
exactly as SP110/SP111/SP112/SP113 disclosed. The Rust integration
tests (T3) gate the byte-identity claim across 3 replicas for GC ops
AND prove the SP113 PT-4 (`too_old_snapshot_false_negative`)
scenario now ABORTS correctly under SP114 — the watermark-driven
prune keeps the pending_txs record live, the dangerous-structure
detector fires, the Tx aborts.

**Thesis fit:** `verifiable` (extends SP113's TLA+ rigor to the GC
layer; NoVersionBelowWatermark + SnapshotAvailability +
BoundedWindowSupersededByWatermark are mechanically-checked GC-
correctness claims; the **sixth** rigor-gate TLA+ module in the
project; the **first** that formally encodes a per-slice supersession
claim relating SP114 to SP113); `honest-docs` (the 2-Tx bound +
multi-replica TLA+ S2.X follow-up + spec-vs-Rust correspondence
caveat are all disclosed; the BoundedWindowSupersededByWatermark
invariant is precisely the SP113 Decision 5 honest-disclosure formally
closed).

### Decision 9 — Backward compatibility: **purely additive at the Op + StateMachine level; BREAKING at the Tx::begin* level (Result-returning); zero changes to SP112 SI behaviour; zero changes to SP113 SSI behaviour**

Per the parent design Decision 8 + SP110/SP111/SP112/SP113 discipline:

- **NEW Op variant `Op::AdvanceWatermark` at wire tag 45** — append-
  only addition to the Op enum; SP1–SP113 Op variants byte-unchanged.
  The SM apply path adds ONE new dispatch arm; SP112's `Op::CommitTx`
  arm + SP113's SSI inner branch UNCHANGED.
- **NEW SM field `low_water_mark: u64`** — initial value 0; SP1–SP113
  SM behaviour byte-unchanged when `low_water_mark = 0` (no version
  has commit_opnum < 0; no Tx has snapshot_opnum < 0; the watermark
  is structurally a no-op until an AdvanceWatermark op fires).
- **NEW mvcc primitive `delete_versions_older_than`** — additive
  function on `crates/kessel-storage/src/mvcc.rs`; SP110 surface
  unchanged.
- **NEW ssi primitive `prune_pending_txs_by_watermark`** — additive
  function on `crates/kessel-storage/src/ssi.rs`; SP113
  `prune_pending_txs(MAX_TX_AGE)` UNCHANGED (retained as fallback
  ceiling per Decision 4).
- **BREAKING change at `Tx::begin*`** — return type changes from
  `Self` to `Result<Self, TxError>`. **All in-tree callers** (tests,
  KATs, examples) **are updated in T1+T2**. Per Decision 7 honest
  disclosure: no production callers exist (S2.6 is the production-
  caller slice); the breaking change is shipped now to avoid landing
  it after S2.6 wires production. **The S2 backlog's roll-up
  byte-identity claim re-baselines to post-S2.5**: every SP1–SP113
  CALL-SITE that uses `Tx::begin*` is updated; every SP1–SP113 KAT
  + integration test is updated to unwrap the Ok variant; the
  storage-layer behaviour for `low_water_mark = 0` (the default) is
  byte-net-0 to SP113.
- **NEW TxError variant `SnapshotTooOld`** — additive on the
  `#[non_exhaustive]` enum (SP112 discipline); every SP112 caller's
  `match` arm with a `_` clause continues to compile.
- **NEW OpResult variant `WatermarkAdvanced` + `WatermarkRejected`** —
  additive at new tags (per kessel-proto convention).
- **NEW WatermarkRejection enum** — at the OpResult tag.
- **NEW Storage methods `low_water_mark()` + `set_low_water_mark(u64)`** —
  additive on the Storage public surface.
- **Zero changes to `kessel-sql`** in S2.5. SQL routing through GC is
  the S2.6 responsibility (S2.6 wires the watermark heartbeat into
  the SQL layer).
- **Zero changes to `kessel-vsr`** in S2.5. The Op enum extension is
  wire-compatible (additive variant at a new tag); replication
  serializes the new op via the existing kessel-proto codec.
- **Zero new external dependencies** in S2.5. The GC primitive uses
  `std::collections::BTreeMap` only (SP110+SP111+SP112+SP113 already
  use it). `cargo tree -p kesseldb-server | grep -Ei
  "parquet|objstore|rustls|webpki"` stays byte-identical to the
  SP113 baseline.
- **Every SP1–SP113 path bytes-on-disk-identical when `low_water_mark = 0`**
  (the default; until an AdvanceWatermark op fires). The S2.5
  changes touch only NEW code paths (the Op variant, the SM field,
  the GC primitive, the watermark-driven prune, the snapshot-too-old
  rejection) which are dormant by default.

**The Tx::begin breaking change is the single non-byte-net-0 surface
in S2.5.** It is honest-disclosed in every relevant test commit
message; the SP114 record's gate accounting names it explicitly; and
it is accepted because the alternative (defer to S2.6) would land the
breakage on top of the SQL-integration cutover, which already carries
its own byte-identity-gate change per the parent S2 design.

**Thesis fit:** `honest-docs` (the parallel-module + additive-Op +
breaking-Tx-begin discipline holds; the breakage is named and
justified; the watermark = 0 default makes SP1–SP113 paths byte-
net-0 in the steady state); `zero-dep` (no new external crate).

### Decision 10 — Slice numbering: **SP114** (the slice immediately after SP113)

SP114 in the subproject numbering. The S2.5 plan/spec filenames use
the `2026-05-24` date prefix. The internal record (T6 will create it)
is:

- Spec/design: this file —
  `docs/superpowers/specs/2026-05-24-mvcc-si-s2-5-design.md`.
- Plan: companion file —
  `docs/superpowers/plans/2026-05-24-mvcc-si-s2-5.md`.
- Slice closeout record (T6 will create it):
  `docs/superpowers/specs/2026-05-24-kesseldb-subproject114-mvcc-gc-s2-5.md`.

Subproject-number / S2-sub-slice cross-reference table after S2.5:

| Subproject | S2 sub-slice | Status | Headline |
|---|---|---|---|
| SP110 | S2.1 | done | MVCC versioned-storage primitive |
| SP111 | S2.2 | done | Tx context + read-set tracking |
| SP112 | S2.3 | done | SI write-side + conflict detection at SM apply time |
| SP113 | S2.4 | done | SSI promotion via Cahill dangerous-cycle detection |
| **SP114** | **S2.5** | **this slice** | **GC + dynamic watermark (supersedes SP113's fixed MAX_TX_AGE bounded window)** |
| SP115+ | S2.6 | pending | SQL + SM cutover |

**Thesis fit:** `honest-docs` (the slice numbering + cross-reference
makes the strategic-tier trajectory inspectable from any single record;
the SP115/S2.6 forward-link is named here; the SP113 supersession
relationship is formally encoded in the cross-reference + in the
TLA+ invariant **BoundedWindowSupersededByWatermark**).

---

## Architecture

### High-level layering after S2.5

```
                  +---------------------------+
                  |  kessel-sql (unchanged)   |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-sm (NEW field +   |   <-- S2.5 (this slice)
                  |  NEW Op::AdvanceWatermark |       GC apply arm runs HERE
                  |  + low_water_mark state   |       version reclamation +
                  |  + monotonic validation   |       pending_txs prune +
                  |  + reclamation orchestr.  |       watermark update
                  |  SP112 + SP113 paths      |
                  |  UNCHANGED                |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage::tx       |   <-- S2.5 (this slice)
                  |  + begin* RETURNS Result  |       snapshot_too_old check
                  |  + SnapshotTooOld variant |
                  |  SP112+SP113 commit       |
                  |  paths UNCHANGED          |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage::mvcc     |   <-- S2.5 (this slice)
                  |  + delete_versions_       |       new GC primitive
                  |    older_than()           |
                  |  SP110 surface UNCHANGED  |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage::ssi      |   <-- S2.5 (this slice)
                  |  + prune_pending_txs_     |       watermark-driven prune
                  |    by_watermark()         |       (SP113 MAX_TX_AGE
                  |  SP113 surface RETAINED   |       prune retained as
                  |  as FALLBACK ceiling      |       safety net)
                  +---------------------------+

                  +---------------------------+
                  |  kessel-proto (EXTENDED   |   <-- S2.5 (this slice)
                  |  Op::AdvanceWatermark at  |       new wire tag 45;
                  |  tag 45)                  |       OpResult extended
                  |  + WatermarkAdvanced      |       with two new variants
                  |  + WatermarkRejected      |
                  |  + WatermarkRejection enum|
                  +---------------------------+
```

The GC verdict-seam is `kessel-sm::StateMachine::apply`'s NEW
`Op::AdvanceWatermark` arm — a sibling seam to SP112's `Op::CommitTx`.
The two seams compose: every commit produces a version; every advance
reclaims versions; the storage state at any point is `versions in
[low_water_mark, commit_opnum]`.

### Module changes (S2.5 deltas only)

- `crates/kessel-proto/src/lib.rs` — add `Op::AdvanceWatermark
  { low_water_mark: u64 }` at wire tag 45; add `OpResult::WatermarkAdvanced
  { new_low_water_mark: u64, versions_deleted: usize, pending_txs_evicted:
  usize }` and `OpResult::WatermarkRejected { reason: WatermarkRejection }`
  at new tags; add `WatermarkRejection` enum with two variants
  (`NotMonotonic`, `AboveCommitCeiling`); update encode/decode for
  each. **SP112+SP113 wire-format byte-unchanged**.
- `crates/kessel-storage/src/mvcc.rs` — add `pub fn
  delete_versions_older_than<V: Vfs>(store: &mut Storage<V>,
  low_water_mark: u64) -> Result<usize, MvccKeyError>`; SP110 surface
  unchanged.
- `crates/kessel-storage/src/ssi.rs` — add `pub fn
  prune_pending_txs_by_watermark(pending_txs: &mut BTreeMap<u64,
  PendingTxRecord>, low_water_mark: u64)`; SP113
  `prune_pending_txs(current, MAX_TX_AGE)` UNCHANGED (retained per
  Decision 4); SP113 MAX_TX_AGE constant UNCHANGED.
- `crates/kessel-storage/src/tx.rs` — change `begin`, `begin_rw`,
  `begin_ssi` return types to `Result<Self, TxError>`; add a
  `low_water_mark` check at the top of each; extend `TxError` with
  `SnapshotTooOld { low_water_mark: u64 }` variant.
- `crates/kessel-storage/src/lib.rs` — add `pub fn
  low_water_mark(&self) -> u64` + `pub fn set_low_water_mark(&mut
  self, w: u64)` on `Storage<V>`. The watermark is in-memory
  Storage-level state (mirrors SP113's pending_txs Storage-level
  resolution at the test seam).
- `crates/kessel-sm/src/lib.rs` — add `low_water_mark: u64` field to
  `StateMachine<V>` (initial 0); add new `Op::AdvanceWatermark` arm
  in apply (validate monotonicity + commit_opnum ceiling → reject
  or accept; on accept: call `mvcc::delete_versions_older_than` +
  `ssi::prune_pending_txs_by_watermark` + update SM watermark + call
  `Storage::set_low_water_mark`). `StateMachine::new` extended to
  initialise `low_water_mark: 0`. SP112+SP113 SM behaviour unchanged.
- `kesseldb-tla/MVCCGc.tla` + `.cfg` + baseline TLC result.
- New test files: `crates/kessel-storage/tests/integration_mvcc_gc.rs`,
  `crates/kessel-storage/tests/pentest_mvcc_gc.rs`.
- New slice record (T6): `docs/superpowers/specs/2026-05-24-kesseldb-subproject114-mvcc-gc-s2-5.md`.

**No new crates. No new files outside the seven listed.**

### Internal data shape

**`Tx<'a, V: Vfs>` (S2.5 surface — UNCHANGED struct shape).** The Tx
struct keeps its four fields (`store`, `snapshot_opnum`, `read_set`,
`write_set`); the only S2.5 change is the `Tx::begin*` constructors
now return `Result` instead of `Self`.

**`StateMachine<V: Vfs>` (S2.5 expansion).** Gains ONE new field:
`low_water_mark: u64` (initial 0). All SP1–SP113 fields unchanged.

**`Storage<V: Vfs>` (S2.5 expansion).** Gains ONE new field:
`low_water_mark: u64` (initial 0). All SP1–SP113 fields unchanged.
Read via `storage.low_water_mark()`; set via
`storage.set_low_water_mark(w)`.

**Restart-rebuild semantics.** Per Decision 6: `low_water_mark` is
not yet persisted (until the SM checkpoint flow ships). On SM
restart it is rebuilt by replaying every `Op::AdvanceWatermark` in
the log — exactly the same deterministic apply path that built it
pre-crash. Storage's `low_water_mark` follows the SM's via the
`set_low_water_mark` call on every apply.

### Call graph (S2.5 additions)

```
caller (test in S2.5 / watermark heartbeat producer in S2.X)
   |
   | sm.apply(op_number, Op::AdvanceWatermark { low_water_mark: W })
   |
   v
StateMachine::apply(op_number, Op::AdvanceWatermark { low_water_mark: W })
   |
   |--+ if W <= self.low_water_mark:
   |  |   → OpResult::WatermarkRejected { reason: NotMonotonic { proposed: W, current: self.low_water_mark } }
   |
   |--+ if W > self.commit_opnum (or whatever the SM's current commit_opnum tracker is):
   |  |   → OpResult::WatermarkRejected { reason: AboveCommitCeiling { proposed: W, current_commit: self.commit_opnum } }
   |
   |--+ (validation passed)
   |  | let versions_deleted = mvcc::delete_versions_older_than(&mut self.store, W)?;
   |  | let pending_txs_evicted = {
   |  |     let before = self.pending_txs.len();
   |  |     ssi::prune_pending_txs_by_watermark(&mut self.pending_txs, W);
   |  |     before - self.pending_txs.len()
   |  | };
   |  | self.low_water_mark = W;
   |  | self.store.set_low_water_mark(W);
   |
   v
OpResult::WatermarkAdvanced { new_low_water_mark: W, versions_deleted, pending_txs_evicted }


// Separately, on the read side:
caller (test or SQL planner)
   |
   | Tx::begin_ssi(&mut store, snapshot_opnum: S)
   |
   v
Tx::begin_ssi(store, S) -> Result<Self, TxError>
   |
   |--+ if S < store.low_water_mark():
   |  |   → Err(TxError::SnapshotTooOld { low_water_mark: store.low_water_mark() })
   |
   |--+ otherwise:
   |     → Ok(Tx { store, snapshot_opnum: S, read_set: BTreeSet::new(), write_set: BTreeMap::new() })
```

**Per-step determinism.** Every step is a pure function of the apply
inputs + the deterministic SM state. The
`delete_versions_older_than` scan is sorted-key deterministic; the
`prune_pending_txs_by_watermark` is BTreeMap::split_off deterministic;
the storage's `set_low_water_mark` is a pure assignment. **No wall
clock, no thread scheduling, no allocator state.** The thesis-fit
`deterministic` invariant carries.

### MVCCGc.tla extension (the verifiable artifact)

Per Decision 8. See `kesseldb-tla/MVCCGc.tla` after T6 lands. The
spec mechanically checks (over the bounded 2-Tx model):

- All 16 MVCCSsi invariants carried forward.
- **6 new GC invariants**: `TypeOKGc`, `WatermarkMonotonic`,
  `NoVersionBelowWatermark`, `NoPendingTxBelowWatermark`,
  `SnapshotAvailability`, **`BoundedWindowSupersededByWatermark`**.

The highlighted invariant is the **SP113-supersession claim**: with
watermark-driven pruning (provided watermark advances only up to
min(active snapshots)), no SSI dangerous-structure false-negative
can occur. **This is the formal closure of the SP113 Decision 5
honest-disclosure**, mechanically encoded as an invariant in the
sixth rigor-gate TLA+ module in the project.

---

## The GC + watermark correctness contract (formal)

This section states, in code-grounded prose, what S2.5 ships as
behaviour. Every clause is gated by a KAT in T2, an integration test
in T3, a coverage test in T4, or a pentest in T5.

### Version reclamation invariant (the headline GC claim)

After any `Op::AdvanceWatermark { low_water_mark: W }` apply that
returns `OpResult::WatermarkAdvanced { ... }`, the storage contains
ZERO MVCC versions with `commit_opnum < W`. Gated by T2's KAT
`kat_advance_watermark_reclaims_pre_watermark_versions_count` and
T3's `it_3_replica_byte_identity_for_gc_op`.

### Bounded-window supersession invariant (the SP113-closure claim)

For any committed schedule in which every `Op::AdvanceWatermark` op
advances `low_water_mark` to a value <= `min(snapshot_opnum across
all currently-active Tx)`, **no SSI dangerous-structure false-
negative can occur**: every concurrent-Tx pair whose dangerous
structure SP113's fixed-MAX_TX_AGE bound would have missed is now
correctly detected because the relevant pending_txs records remain
in the map (they are above the watermark; the watermark-driven prune
has not evicted them). **The SP113 record's Decision 5 honest-
disclosure is formally closed** for any production that respects
the heartbeat protocol. Gated by T3's
`it_supersedes_sp113_bounded_window_false_negative` (which constructs
the SP113 PT-4 `too_old_snapshot_false_negative` scenario and verifies
S2.5's behaviour: the previously-undetectable write-skew is now
detected and aborted).

### Snapshot-too-old rejection invariant

For every `Tx::begin*` call with `snapshot_opnum < low_water_mark`,
the result is `Err(TxError::SnapshotTooOld { low_water_mark })` — no
Tx is constructed; no storage state is touched. Gated by T2's KAT
`kat_tx_begin_rejects_below_watermark`.

### Monotonicity invariant

For any apply sequence, `low_water_mark` is non-decreasing. An
`Op::AdvanceWatermark { W }` with `W <= self.low_water_mark` returns
`OpResult::WatermarkRejected { reason: NotMonotonic }`; the SM's
`low_water_mark` is unchanged. Gated by T2's KAT
`kat_advance_watermark_rejects_non_monotonic` and T4's coverage test
on monotonicity-violation rejection.

### Commit_opnum ceiling invariant

For any apply sequence, `low_water_mark <= commit_opnum`. An
`Op::AdvanceWatermark { W }` with `W > self.commit_opnum` returns
`OpResult::WatermarkRejected { reason: AboveCommitCeiling }`; the
SM's `low_water_mark` is unchanged. Gated by T5's pentest
`pt_advance_watermark_above_commit_ceiling_rejected`.

### Determinism invariant (carried forward + extended)

The GC verdict — `WatermarkAdvanced { new_low_water_mark,
versions_deleted, pending_txs_evicted }` or `WatermarkRejected { reason }`
— is a deterministic function of `(log prefix, prior SM state,
proposed low_water_mark)`. Every replica running the same log prefix
reaches the same verdict and writes byte-identical storage +
pending_txs + low_water_mark state. Gated by T3's
`it_3_replica_byte_identity_for_gc_op`.

### SP1-SP113-byte-net-0-when-watermark-is-zero invariant

For every apply sequence that contains zero `Op::AdvanceWatermark`
ops, the SM state + storage state + pending_txs state are
byte-identical to the equivalent SP113 SM apply on the same op
sequence. **The watermark = 0 default is fully backward-compatible.**
Gated by T4's `it_coverage_watermark_zero_byte_identical_to_sp113`.

### Long-running Tx pins watermark invariant

For a workload with a long-running Tx pinned at `snapshot_opnum = S`,
the watermark CAN NOT advance past S without rejecting reads from that
Tx. **This is the operational mechanism by which the watermark
protocol respects active readers**: the heartbeat producer computes
`min(active_snapshot)` which is bounded above by S until the
long-running Tx terminates; therefore the watermark never crosses S
during the long Tx's lifetime; therefore the long Tx's reads remain
serveable. Gated by T3's `it_long_running_tx_pins_watermark`.

---

## Sub-slice gate accounting (estimated)

Total cargo gate growth in S2.5: estimated **+25 to +35 tests** on
the new GC + watermark surface. Breakdown:

| Task | Expected tests | Cumulative | Notes |
|---|---|---|---|
| T0 baseline | 0 | 610 | SP113 final, expect FAILED=0 + seed-7 green |
| T1 scaffold | +2 | 612 | Type-shape locks: `Op::AdvanceWatermark` constructible; `OpResult::WatermarkAdvanced` constructible; `OpResult::WatermarkRejected` constructible; `WatermarkRejection` variants constructible; `TxError::SnapshotTooOld` constructible; `Storage::low_water_mark()` returns 0 by default; `StateMachine::low_water_mark` field present; `mvcc::delete_versions_older_than` signature present; `ssi::prune_pending_txs_by_watermark` signature present; `Tx::begin*` return-type-Result |
| T2 impl + KATs | +11 | 623 | empty-storage / kat_advance_watermark_reclaims_pre_watermark_versions_count / kat_advance_watermark_rejects_non_monotonic / kat_advance_watermark_rejects_above_commit_ceiling / kat_prune_pending_txs_by_watermark / kat_tx_begin_rejects_below_watermark / kat_delete_versions_older_than_count / kat_delete_versions_older_than_preserves_at_watermark / kat_op_advancewatermark_wire_roundtrip / kat_storage_low_water_mark_accessor / kat_sm_low_water_mark_field_persists_through_advance_op |
| T3 integration | +6 | 629 | it_classic_gc_reclaims_versions_byte_identically_across_3_replicas (HEADLINE) / **it_supersedes_sp113_bounded_window_false_negative** (the SP113-closure claim) / it_snapshot_too_old_rejected_consistently / it_long_running_tx_pins_watermark / it_advance_watermark_after_commit_commit_advance_sequence / it_sm_apply_byte_equivalence_with_local_path |
| T4 coverage | +5 | 634 | it_coverage_watermark_zero_no_op / it_coverage_watermark_u64_max_reclaims_everything / it_coverage_monotonic_violation_chain_rejected / it_coverage_1000_version_gc_scaling / it_coverage_advancewatermark_interleaved_with_committx |
| T5 pentest | +6 | 640 | pt_hostile_watermark_u64_max / pt_hostile_monotonic_violation_storm / pt_hostile_snapshot_zero_after_max_watermark / pt_100k_version_gc_under_load / pt_watermark_plus_ssi_interleaving / pt_watermark_advance_during_in_flight_commit |
| T6 docs + TLA+ | 0 Rust | 640 | MVCCGc.tla + .cfg + TLC baseline + SP114 record + STATUS + memory |

**Estimated final cargo gate after S2.5:** **~640 tests** (`FAILED=0`,
seed-7 green). The actual number lands in T6.

The TLA+ artifact's gate is the TLC baseline run (zero invariant
violations on the bounded config) + the artifact files committed to
`kesseldb-tla/`.

---

## Sub-slice decomposition reminder (S2.6 still pending)

S2.5 ships ONLY the GC + dynamic watermark protocol: `Op::AdvanceWatermark`,
the SM `low_water_mark` field + apply arm, the
`mvcc::delete_versions_older_than` primitive, the
`ssi::prune_pending_txs_by_watermark` primitive, the
`Storage::low_water_mark/set_low_water_mark` accessors, the
`Tx::begin*` snapshot-too-old check, the `TxError::SnapshotTooOld`
variant, and the `MVCCGc.tla` artifact. The following are explicitly
**OUT of scope for S2.5** and tracked in the parent S2 design's
sub-slice decomposition:

- **S2.6** — SQL surface integration + SM cutover (the byte-identity-
  gate-change slice, honest-disclosed there). Also wires the
  watermark heartbeat producer into the SQL planner, the
  `sm.next_op_number()` source of `commit_opnum`, the cursor-stall-on-
  snapshot-not-yet-applied semantics, and the per-Tx SI-vs-SSI choice
  at the SQL planner level.

Each subsequent sub-slice will get its own plan when the prior one
lands. S2.6 plan is the next docs slice expected after this S2.5
slice ships.

---

## Honest deferred set

Items explicitly out of scope for S2.5, named here so the S2.5 record
can't drift into over-claim territory:

- **Watermark heartbeat producer.** S2.5 ships the SM apply path that
  accepts an `Op::AdvanceWatermark`; the heartbeat producer that
  computes `min(active_snapshot)` across the cluster and emits the
  op periodically is OUT of scope — Decision 2 honest disclosure;
  scoped for S2.X (or S2.6 if it shows up in SQL integration).
- **SM checkpoint integration.** Per Decision 6: `low_water_mark` is
  log-replay-rebuilt rather than checkpointed. Promoting to
  checkpointed state is an S2.X follow-up.
- **Range-pruning optimisation of `delete_versions_older_than`.** Per
  Decision 3: S2.5 uses a full LSM scan; bloom-filter / range-prune
  optimisations are S2.X follow-ups when benchmarks justify.
- **SQL integration.** Deferred to S2.6.
- **`SM-side `next_op_number()` helper supplying `commit_opnum`.**
  Deferred to S2.6 (carried forward from SP112's deferred set).
- **Cursor-stall on snapshot-not-yet-applied.** Deferred to S2.6
  (carried forward from SP112's deferred set).
- **Cross-thread Tx, Tx-pool, Tx ID allocation.** Not on the S2
  roadmap (carried forward from S2.2). Tx is single-thread /
  stack-frame-bound by construction.
- **3-Tx TLA+ model.** S2.5 ships a 2-Tx bounded model (sufficient
  for the SP113 supersession counterexample). A 3-Tx model would
  let TLC also find the canonical T0→T1→T2 cases; deferred to an
  S2.X follow-up.
- **Multi-replica TLA+ model.** Same scope decision as
  SP110/SP111/SP112/SP113 carried forward.
- **Larger TLC bounds for MVCCGc.** Same disclosure as
  SP110/SP111/SP112/SP113: the bounded config in T6 may be tightened
  to keep TLC tractable.
- **TLA+-mechanized-refinement TLA+ ↔ Rust.** Same gap S1/SP109 +
  SP110/SP111/SP112/SP113 disclosed. Per-sub-slice named-action
  correspondence carries forward; not a refinement proof.
- **Persistent `pending_txs`.** S2.4 deferred this; S2.5 also defers.
- **Adaptive MAX_TX_AGE.** SP113's MAX_TX_AGE = 4096 stays as the
  fallback ceiling (Decision 4). Tuning it dynamically based on
  workload would be an S2.X follow-up.

---

## Thesis-fit note

**Thesis fit:** `deterministic` (the GC reclamation + watermark-driven
prune + watermark advance is computed by SM apply over the log-derived
SM state; structurally cannot diverge across replicas — the same
property SP110/SP111/SP112/SP113 established for storage, Tx, SI, and
SSI, now extended to the GC layer; **the thesis-fit headline phrase
"GC becomes a structural property of the log, not a background-thread
or a distributed-coordination protocol" is operationalised in this
slice's Op::AdvanceWatermark apply arm**; this is the second direct
deterministic-replicated payoff in the S2 backlog after SP113's
SSI verdict — **storage bound becomes a structural property of the
log**);
`replayable` (the post-GC storage state is a deterministic function of
the log prefix — `(initial state) + apply(commit ops) - apply(advance
ops' reclamation)` — debugging IS replay; a production bug-report on
a SnapshotTooOld error reduces to a `(seed, log, snapshot_opnum)`
tuple);
`verifiable` (`MVCCGc.tla` extends SP113's MVCCSsi.tla with the
low_water_mark + AdvanceWatermark action + six new invariants —
TypeOKGc, WatermarkMonotonic, NoVersionBelowWatermark,
NoPendingTxBelowWatermark, SnapshotAvailability,
**BoundedWindowSupersededByWatermark** — all mechanically-checked by
TLC against the same VSR-log substrate S1/SP109 verified; the **sixth**
rigor-gate TLA+ module in the project; the **first** that formally
encodes a per-slice supersession claim — the SP113 false-negative is
mechanically proven absent under the SP114 watermark protocol);
`honest-docs` (the rejected GC mechanisms (background-thread,
loose-lag) are explicitly documented as rejected, not silently
revised; the deterministic-Op-vs-background-thread tradeoff is named;
the Tx::begin breaking change is explicitly named and justified; the
heartbeat-producer-vs-apply-path split is named; the SP113 MAX_TX_AGE
retention as fallback ceiling is documented; the SP113 supersession
relationship is mechanically encoded as a TLA+ invariant AND
operationally proven in T3's integration test
`it_supersedes_sp113_bounded_window_false_negative`).

The thesis-fit headline of this slice: **GC is just another
deterministic function of the log prefix.** KesselDB does not need
PostgreSQL's autovacuum + per-backend xmin or CockroachDB's per-range
GC queue or Spanner's safe_time Paxos protocol because the reclamation
is itself a totally-ordered log op; every replica's deterministic apply
executes the same reclamation byte-identically. **This is the most
direct expression yet of the "deterministic replicated SQL with
verifiable behavior and replayability" thesis at the storage tier**
— the storage steady-state size is itself log-deterministic, and the
SP113 bounded-window false-negative is formally closed.

---

## Internal record

This design document is
`docs/superpowers/specs/2026-05-24-mvcc-si-s2-5-design.md`.

The S2.5 implementation plan is
`docs/superpowers/plans/2026-05-24-mvcc-si-s2-5.md`.

When S2.5 ships, its slice-record file will be
`docs/superpowers/specs/2026-05-24-kesseldb-subproject114-mvcc-gc-s2-5.md`
(SP114 in the subproject numbering; mirrors the SP110/SP111/SP112/SP113
filename pattern). The record will carry the honest gate accounting
(610 → final), the per-task evidence chain, the TLA+-to-Rust
correspondence table, the deferred backlog (S2.6), and the
strategic-tier context update.

The S2.5 TLA+ artifact will be `kesseldb-tla/MVCCGc.tla` +
`.cfg` + `kesseldb-tla/results/2026-05-24-mvcc-gc-baseline.txt` (the
captured TLC baseline run).
