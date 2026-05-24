# S2.4 — SSI Promotion (Serializable SI via Dangerous-Cycle Detection): Design

**Date:** 2026-05-24
**Status:** Approved (autonomous mandate — see Process Note)
**Strategic-tier item:** S2.4 sub-slice of S2 (Serializable MVCC / Snapshot
Isolation) in the THESIS.md S1–S4 backlog. The **fourth** built sub-slice of
S2 after S2.1/SP110 (MVCC versioned-storage primitive), S2.2/SP111
(Tx context + read-set), and S2.3/SP112 (SI write-side + conflict detection
at SM apply time). **SP113** in the subproject numbering.
**Builds on:**
- Project THESIS — `docs/THESIS.md`.
- S2 parent design — `docs/superpowers/specs/2026-05-23-mvcc-si-design.md`.
- S2.1 record (SP110) — `docs/superpowers/specs/2026-05-23-kesseldb-subproject110-mvcc-s2-1.md`.
- S2.2 record (SP111) — `docs/superpowers/specs/2026-05-24-kesseldb-subproject111-mvcc-tx-s2-2.md`.
- S2.3 record (SP112) — `docs/superpowers/specs/2026-05-24-kesseldb-subproject112-mvcc-si-s2-3.md`.
- S2.3 design — `docs/superpowers/specs/2026-05-24-mvcc-si-s2-3-design.md`.
- S2.3 TLA+ artifact — `kesseldb-tla/MVCCSi.tla` + `.cfg` + baseline TLC
  run (`kesseldb-tla/results/2026-05-24-mvcc-si-baseline.txt`).
- The MVCC module surface shipped in SP110: `crates/kessel-storage/src/mvcc.rs`.
- The Tx module shipped through SP112: `crates/kessel-storage/src/tx.rs`
  (`Tx<'a, V>` with `read_set`, `write_set`, `commit`, `commit_read_only`,
  `abort`; `TxCommitOutcome`; `TxError`).
- The SM apply path shipped in SP112: `crates/kessel-sm/src/lib.rs`
  (`Op::CommitTx` arm running the deterministic write-write conflict check).
- The proto Op + result shape shipped in SP112: `crates/kessel-proto/src/lib.rs`
  (`Op::CommitTx { snapshot_opnum, write_set, commit_opnum }` at wire tag 44;
  `OpResult::TxCommitted`/`TxAborted` at wire tags 9/10; `AbortReason` with
  three variants at sub-tags 0/1/2).
- **External background:** Cahill, Röhm, Fekete, "Serializable Isolation for
  Snapshot Databases" (SIGMOD 2008). The SSI algorithm KesselDB adopts in
  this slice (and that PostgreSQL ≥ 9.1 also ships).

---

## Process note (autonomy + brainstorming gate)

Produced under the standing overnight autonomous-build mandate
(`feedback_kesseldb_autonomous_build` + the strategic-tier mandate
`feedback_kesseldb_strategic_tier`): "build the backlog autonomously,
BOLD choices, don't wait for approval, keep the two-stage review gate
+ full tests + pentest passes." **The brainstorming user-review gate
is substituted by this documented decision record** — the 9 brainstorm
decisions below are resolved boldly in this document; the user does
not re-review them before the plan executes. **The two-stage subagent
review gate is preserved** for every substantive task (T2/T3/T5/T6),
with the final whole-implementation reviewer dispatched at the end of
T6, exactly as SP110/SP111/SP112 did.

---

## Strategic-tier framing

S2.4 is the **fourth sub-slice of S2** in the THESIS.md backlog. SP113
in the subproject numbering. The parent S2 design
(`docs/superpowers/specs/2026-05-23-mvcc-si-design.md`) decomposes S2
into 6 sub-slices: S2.1 (SP110 — versioned-storage primitive) →
S2.2 (SP111 — Tx context + read-set) → S2.3 (SP112 — SI write-side +
deterministic conflict at SM apply) → **S2.4 (this slice — SSI
promotion via rw-antidependency dangerous-cycle detection)** →
S2.5 (GC + watermark) → S2.6 (SQL integration + SM cutover). This slice
**closes the write-skew hole** that plain SI necessarily leaves open
and upgrades the isolation level from Snapshot Isolation to true
**Serializable Snapshot Isolation** — the same level PostgreSQL ships
under the name SSI.

**S2.1 → S2.2 → S2.3 → S2.4 dependency chain.** S2.1 shipped the
versioned-storage primitive. S2.2 shipped the Tx context with the
`read_set` *specifically because S2.4 was going to consume it* (see
SP111 design Decision 3 forward-link). S2.3 shipped the write-side +
the SM-apply-time write-write conflict check, with the explicit
SP112 honest-deferred entry "SSI dangerous-cycle detection — deferred
to S2.4." **S2.4 cashes both forward-links** by adding the
rw-antidependency edge tracking and the dangerous-structure detector
to the existing SM `Op::CommitTx` apply path.

**The thesis-fit headline of S2.4.** Per the parent S2 design Decision
4 and the SP112 thesis-fit headline ("deterministic apply IS the
conflict resolver"), KesselDB's SM apply path is the seam at which
every replica converges on the same conflict verdict. S2.4 extends
this property to the **harder** verdict — instead of "is there a
conflicting write?" it is "does this commit close a dangerous
rw-antidependency cycle?" — and shows that the deterministic-log
substrate makes the answer **just as cheap to derive deterministically**
as plain SI. **In contrast, every non-deterministic SSI implementation
(PostgreSQL ships SIReadLock predicate locks plus a per-Tx SI flag
inside its non-deterministic MVCC + 2PC coordination layer; CockroachDB
emulates SSI via timestamp-based read-refresh in HLC space) pays a
significant coordination cost.** KesselDB pays approximately zero
coordination cost because the rw-edge graph is itself a deterministic
function of the log prefix — the same `(snapshot_opnum, read_set,
write_set, commit_opnum)` triple every replica already sees. **This is
the most direct expression yet of the "deterministic replicated SQL"
pillar in the S2 backlog**: serializability becomes a structural
property of the log, not a coordination protocol.

---

## Problem

After S2.3/SP112 ships, KesselDB has:
- A snapshot-pinned read primitive (`mvcc::get_at_snapshot`).
- A Tx context (`kessel_storage::tx::Tx`) that pins a snapshot opnum,
  accumulates a read-set, buffers writes, and runs a deterministic
  write-write conflict check at SM apply time.
- An `MVCCSi.tla` TLA+ artifact mechanically checking the SI
  invariants — WriteWriteConflictDetected, CommitAtomicity,
  FirstCommitterWins, DeterministicApply — over the bounded model.

What's still missing for true **serializability** (and what S2.4 ships):
1. **Detection of rw-antidependency cycles.** Plain SI allows the
   classic write-skew anomaly: two concurrent Tx (Tx_A and Tx_B) each
   read a key the other will write, then both commit (because neither
   wrote to a key the other already wrote). Under plain SI both
   commit; under serializability at most one should. This is the
   "dangerous structure" Cahill characterizes as two consecutive
   rw-antidependency edges (Tx_X →rw Tx_A →rw Tx_B).
2. **An SM-apply-time SSI verdict.** Cahill's SSI requires tracking
   per-Tx rw-edges as concurrent Tx interact. In a deterministic-log
   system the natural place to derive these edges is the SM `Op::CommitTx`
   apply arm, where the committing Tx's `(snapshot_opnum, write_set,
   read_set)` is intersected against a deterministic in-SM **pending-tx
   window** of recently-committed concurrent Tx metadata. The verdict
   is a function of the log prefix, just like SP112's plain-SI
   verdict.
3. **An `MVCCSsi.tla` TLA+ specification.** Per the parent S2 design
   Decision 7, every MVCC sub-slice ships its own TLA+ extension. S2.4's
   spec must extend `MVCCSi` with the rw-edge graph + the
   dangerous-structure detector + the **NoWriteSkew** /
   **DangerousStructureAborts** / **SerializableEquivalence** invariants
   so TLC can mechanically prove the absence of write-skew counterexamples
   at the bounded model.

S2.4 is the slice that solves all three.

---

## Decisions (bold choices, documented)

### Decision 1 — SSI algorithm: **Cahill SSI (dangerous-structure detection on the rw-antidependency graph)**

Three structural options for closing the SI write-skew hole:

- **(a) Cahill SSI (dangerous-structure detection).** Track per-Tx
  rw-antidependency edges. At commit time, check whether the
  committing Tx is the pivot of two consecutive rw-edges
  (Tx_X →rw Tx_A →rw Tx_B); if so, abort one Tx in the structure.
  Matches PostgreSQL's SSI implementation. Most precise (does not
  generate spurious aborts for non-dangerous rw structures).
- **(b) Predicate locks.** Per-Tx record predicates over the read
  set; on a concurrent commit, check whether the commit's write set
  invalidates any predicate. The classical literature approach.
  Less common in MVCC because the predicates' expressiveness scales
  poorly with read-set size, and the key-level approximation
  collapses into a per-key map that is essentially (a)'s rw-edge
  tracking under a different name.
- **(c) RW-conflict graph cycle detection.** Maintain a directed
  graph of rw-edges among all concurrent Tx; at commit time, check
  whether the graph contains a cycle; abort if so. Strictly more
  conservative than (a) — every cycle does contain a dangerous
  structure but not every cycle is forced to abort by Cahill's
  rule; conversely, Cahill's rule sometimes aborts before a full
  cycle materialises. Easier to implement (any cycle detection)
  but loses precision.

**Taken: (a) — Cahill SSI dangerous-structure detection.**

**Why bold over safe.** Option (a) is the gold standard. PostgreSQL
adopted it, the underlying paper has been refined over two decades of
production deployment, and crucially — the deterministic-log substrate
of KesselDB makes (a) **easier** to implement than in PostgreSQL,
where the per-process SIReadLock predicate-locking infrastructure is
the bulk of the complexity. In KesselDB the rw-edge graph is computed
at one place (the SM `Op::CommitTx` arm) over deterministic state (a
bounded in-SM `pending_txs` map) — no shared-memory lock manager, no
predicate-encoding ambiguity, no SIReadLock GC. The Cahill paper's
"dangerous structure" abort rule maps to ~20 lines of Rust inside the
existing SP112 `Op::CommitTx` arm.

Option (b) predicate locks would be the textbook adoption, but the
key-level approximation we would need (we have no general predicate
language; reads are by `(type_id, object_id)`) collapses to (a)
exactly. Option (c) cycle detection is strictly more conservative —
spuriously aborts more Tx — and the precision loss is observable in
benchmarks.

**Thesis fit:** `deterministic` (the dangerous-structure verdict is a
function of the SM-side `pending_txs` map + the committing Tx's
read/write set, all of which are functions of the log prefix);
`verifiable` (the `MVCCSsi.tla` extension mechanically proves the
**NoWriteSkew** invariant — every committed Tx sequence is equivalent
to some serial schedule); `honest-docs` (the rejected options (b)
predicate locks and (c) cycle detection are documented here, not
silently revised).

### Decision 2 — RW-edge tracking: **in-SM `pending_txs` window keyed by commit_opnum + `(read_set, write_set, snapshot_opnum)` payload per Tx**

The SM `StateMachine` gains an in-memory field:

```rust
/// SP113 / S2.4: SSI pending-tx window. Stores per-Tx metadata for
/// every Tx that has committed at-or-after the current SSI lookback
/// horizon. Used by `Op::CommitTx` apply to derive rw-antidependency
/// edges deterministically: when Tx_B commits, the SM walks
/// `pending_txs` for every concurrent Tx_A (defined as A.commit_opnum
/// > B.snapshot_opnum, i.e. Tx_A's commit was invisible to Tx_B's
/// snapshot), and for each (k) in (A.write_set ∩ B.read_set) records
/// the rw-edge Tx_B →rw Tx_A. Then for each (k') in (B.write_set ∩
/// A.read_set) records Tx_A →rw Tx_B. Then runs the dangerous-
/// structure check across the resulting per-Tx edge-tag map.
///
/// IMPORTANT: this is in-memory + recoverable. On SM restart it
/// rebuilds from the tail of the log (replay every Op::CommitTx in
/// `(current_apply_opnum - WINDOW, current_apply_opnum]`) — the
/// window contents are a deterministic function of the log prefix,
/// preserving the thesis-fit deterministic-apply property.
pending_txs: BTreeMap<u64 /* commit_opnum */, PendingTxRecord>
```

with

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingTxRecord {
    pub snapshot_opnum: u64,
    pub read_set:  Vec<(u32, [u8; 16])>,            // sorted at construction
    pub write_set: Vec<(u32, [u8; 16])>,            // keys only — values discarded
    /// SSI rw-edge tags: TRUE iff this Tx has an *outgoing* rw-edge to
    /// some later committer; the dangerous-structure detector reads
    /// this on every new commit. Updated in-place as later Tx commit.
    pub has_outgoing_rw: bool,
    /// SSI rw-edge tags: TRUE iff this Tx has an *incoming* rw-edge
    /// from some earlier committer.
    pub has_incoming_rw: bool,
}
```

**Why this exact shape.**
- The `BTreeMap<u64, _>` keyed by `commit_opnum` is deterministic-
  iteration (essential for byte-identical SM state) and supports
  cheap range-truncation when the window slides (Decision 5).
- The two `bool` fields are the Cahill SSI per-Tx tags: a Tx with
  BOTH `has_incoming_rw` AND `has_outgoing_rw` is the pivot of a
  dangerous structure (T1 in Cahill's notation has T0→T1→T2 →
  abort one of them).
- The `read_set` / `write_set` are stored as **`Vec<(u32, [u8; 16])>`**
  (sorted, keys only — the values are unnecessary for SSI). Storing
  keys-only halves the memory footprint vs the wire `write_set` (which
  carries values). This is consistent with the SSI literature:
  rw-edges are over **keys**, not values.

**Read-only Tx are NOT tracked in `pending_txs`.** A Tx that calls
`Tx::commit_read_only` is not part of any rw-edge graph: it has no
write_set, so no other Tx can have an rw-edge into it; and its own
reads cannot invalidate a later committer's snapshot (the snapshot
is pinned at the later Tx's begin-time). Read-only Tx therefore
generate zero pending_txs bookkeeping. This is the **fast path**
optimisation that Cahill specifically calls out for SI workloads
dominated by SELECTs.

**Thesis fit:** `deterministic` (BTreeMap iteration + key-only edge
tracking ⇒ byte-identical pending_txs state across replicas);
`replayable` (a pending_txs snapshot is a function of the log tail —
restart-rebuilds via log replay); `honest-docs` (the read-only-Tx
fast-path is explicitly documented).

### Decision 3 — Dangerous-structure abort policy: **abort the LATEST committer of the structure (the committing Tx)**

Cahill's paper leaves the abort choice within a dangerous structure
as an implementation decision; the algorithm guarantees serializability
regardless of which Tx in `{T0, T1, T2}` is aborted. KesselDB picks:

**The committing Tx (the one whose commit is currently being applied)
is the Tx that aborts** when a dangerous structure is detected.

**Why bold over safe.** Three options:
- **(a) Abort the pivot.** Picks the Tx in the middle of the
  rw-edge chain. Maximises preserved-commit work (the two ends'
  commits are already done; only the pivot rolls back). **But**
  this requires the SM to *undo* an already-applied commit — which
  in KesselDB's append-only versioned-storage model would either
  require a *compensating-write* op (write the pre-commit version at
  the new commit_opnum; net-out the abort) or a tombstone-based
  recall. Either way: a new SM op shape AND a per-Tx revertibility
  property that S2.5/S2.6 do not have.
- **(b) Abort the latest committer.** Picks the Tx whose commit op is
  currently being applied. No undo required; the SM apply path simply
  returns `OpResult::TxAborted` and the storage state never changes.
  **Pro: composes byte-identically with SP112's existing
  Op::CommitTx-Aborted shape**. Con: may abort more Tx than (a) in
  pathological workloads (the literature reports ~5–10% extra aborts
  on TPC-C-shape workloads).
- **(c) Abort the earliest in the structure.** Symmetric mirror of
  (a); same SM-undo cost.

**Taken: (b) — abort the latest (the committing) Tx.**

This is the **only option that does not violate the append-only
versioned-storage discipline** SP110 shipped. It is also the option
PostgreSQL uses by default (Postgres aborts the Tx whose commit
encounters the dangerous structure). The 5–10% extra-abort cost is an
acceptable workload-rare price for the structural simplicity, and it
keeps S2.4 a pure additive slice on the SP112 apply arm.

**Encoding.** The SP112 `OpResult::TxAborted { reason: AbortReason }`
shape extends with one new variant:

```rust
#[non_exhaustive]
pub enum AbortReason {
    // SP112 variants:
    SnapshotOutOfRange,
    WriteWriteConflict { type_id: u32, object_id: [u8; 16] },
    StorageIo { kind: i32 },
    // SP113 / S2.4 — NEW:
    /// The committing Tx was the pivot or outer node of an SSI
    /// dangerous structure (two consecutive rw-antidependency edges
    /// in the rw-edge graph). Aborted per Cahill SSI to preserve
    /// serializability. Replay with a fresh snapshot.
    DangerousStructure {
        /// The other Tx in the rw-edge chain whose commit_opnum
        /// reveals the dangerous structure. Surfaced for debugging
        /// and observability; does NOT affect the verdict.
        other_commit_opnum: u64,
    },
}
```

Append-only variant addition; wire-compatible with SP112's sub-tag
encoding (the new variant gets sub-tag 3).

**Thesis fit:** `deterministic` (the abort verdict is computed by SM
apply over the log-derived pending_txs + the committing Tx's
read/write set; structurally cannot diverge across replicas);
`honest-docs` (the abort-the-pivot vs abort-the-latest tradeoff is
documented; the +5–10% over-abort cost is named explicitly).

### Decision 4 — Op::CommitTx wire extension: **additive `read_set` field; SP112 SI-only behaviour preserved when read_set is empty**

Two structural options for shipping the read_set over the wire:

- **(a) Extend the existing `Op::CommitTx` variant** with a `read_set`
  field. Wire-compatible because the kessel-proto codec uses tagged
  encoding (the new field is appended within the existing tag-44
  payload at a new sub-tag; readers that don't know the sub-tag skip
  it).
- **(b) Add a new `Op::CommitSsiTx { ... }` variant** at a new wire
  tag (45). Cleanest separation between SI and SSI commits. Doubles
  the SM apply arm count; SQL/Tx callers in S2.6 would have to pick
  one variant per Tx.

**Taken: (a) — extend `Op::CommitTx` with a `read_set: Vec<(u32, [u8;
16])>` field.**

**Why bold over safe.** Option (a) preserves the **single canonical
commit path** through the SM. The SM apply arm gains one inner branch:
*if* `read_set` is non-empty, run the SSI dangerous-structure check
(the new SP113 logic); *else* skip directly to the SP112 plain-SI
behaviour. This means:
- **Existing SP112 callers ship `read_set = vec![]`** and get byte-
  identical SI semantics. **Zero behavioural change to any SP1–SP112
  path.**
- **S2.4 callers (those using `Tx::commit_ssi` per Decision 6)** ship
  the read_set and get SSI promotion semantics on the SAME commit op.
- **S2.6 (SQL integration) picks per-Tx whether to set the read_set**
  via the Tx's SSI-mode flag — without a wire-format split.

**Final shape on the wire (post-S2.4):**

```rust
// kessel-proto::Op (wire tag 44; SP112-shipped, S2.4-extended):
CommitTx {
    snapshot_opnum: u64,
    write_set: Vec<(u32, [u8; 16], Option<Vec<u8>>)>,
    commit_opnum: u64,
    /// SP113 / S2.4: SSI read-set tracking. Empty vec preserves
    /// SP112 plain-SI behaviour; non-empty vec activates the
    /// dangerous-structure detector. Sorted by (type_id, object_id)
    /// at construction for deterministic SM-side iteration.
    read_set: Vec<(u32, [u8; 16])>,
}
```

**Wire-compat encoding.** The SP112 encode/decode sequence (per
`crates/kessel-proto/src/lib.rs`'s tag-44 arm) currently writes:
`u64 snapshot, write_set_len, [write_set entries...], u64 commit`.
S2.4 **appends** `read_set_len, [read_set entries...]` after `commit`.
Old SP112 frames decode under S2.4 by treating absent read_set bytes
as `vec![]` (the natural "SI-only" fall-through). New S2.4 frames
decode under SP112 binaries by truncating the unknown trailing bytes —
**which would be a wire-incompatibility if shipped in production**.
**Honest disclosure**: in S2.4 we ship the wire extension AND the SP112
decode path is updated to **require** the read_set length prefix
(absent ⇒ `vec![]`). This is acceptable because:
1. No production cluster has yet exchanged Op::CommitTx frames (S2.6
   is the slice that wires production callers; S2.3 + S2.4 only
   exercise the op via direct `StateMachine::apply` calls in tests).
2. The decode change is single-source-of-truth (one file, one match
   arm); regression-tested in T2.
3. The wire-extension cost is explicitly named in T2's
   wire-roundtrip KAT.

**Empty read_set ⇒ degenerate to SI.** A Tx that opts into SSI mode
but performs no reads (or only writes) has an empty read_set; the
SSI dangerous-structure check is structurally a no-op on an empty
read_set (no rw-edges can form into or out of an empty read_set);
the verdict reduces to SP112's plain-SI write-write check. **This is
the formal equivalence between SP112 and SP113 on read-free workloads.**

**Thesis fit:** `deterministic` (single canonical commit op; SI vs
SSI is a property of the data shipped, not of the op variant);
`honest-docs` (the wire extension is single-source-updated; the
empty-read_set degeneration is documented; the SP112 wire-compat
trade-off is explicitly named).

### Decision 5 — Pending-tx window: **horizon = (current_apply_opnum − MAX_TX_AGE), MAX_TX_AGE = 4096; window-truncation on every commit**

How long must a committed Tx stay in `pending_txs`? Cahill's
algorithm requires it remain visible to **every concurrent committing
Tx whose snapshot_opnum is older than its commit_opnum**. The
operational bound is therefore:

> A committed Tx's record must remain in `pending_txs` until **no
> active Tx could possibly have a snapshot_opnum that predates it**.

In a deterministic-log system, the natural realisation of "no active
Tx could predate it" is the **read watermark** that S2.5 will ship.
S2.4 cannot rely on S2.5 (sub-slice ordering: S2.5 is the next slice
after S2.4) — so S2.4 picks a **conservative fixed-MAX_TX_AGE bound**:

```rust
/// SP113 / S2.4: The fixed lookback horizon in opnums. Any Tx whose
/// commit_opnum is older than (current_apply_opnum - MAX_TX_AGE)
/// is evicted from pending_txs. Chosen conservatively: long enough
/// that any reasonable Tx (a few seconds at 1000 ops/sec) is still
/// in the window; short enough that pending_txs memory is bounded
/// (~4096 records × ~1 KiB per record = ~4 MiB worst-case under
/// SSI-heavy workloads). S2.5 (GC + watermark) supersedes this
/// fixed bound with a watermark-derived dynamic horizon.
const MAX_TX_AGE: u64 = 4096;
```

**Why bold over safe.** Three options:
- **(a) Unbounded `pending_txs`.** Correct but unbounded memory.
  Tx records pile up forever. Rejected as a footgun.
- **(b) Fixed MAX_TX_AGE.** Correct iff every Tx commits within
  MAX_TX_AGE ops of its snapshot. **Bold choice.** The 4096 bound
  is generous: at 1000 commits/sec, a Tx has 4 seconds to commit;
  far longer than any OLTP-shape Tx. **Honest disclosure**: a Tx
  whose snapshot_opnum is older than (current_apply_opnum -
  MAX_TX_AGE) at commit time **is unconditionally aborted with
  `SnapshotOutOfRange`** — the SM cannot determine whether a
  dangerous structure exists because it has discarded the records.
  This is conservative-correct (no false-negative aborts; only
  possibly-spurious aborts on very-long-running Tx) and matches
  PostgreSQL's `idle_in_transaction_session_timeout` behavior in
  spirit.
- **(c) Watermark-derived horizon.** The right long-term answer.
  Deferred to S2.5 per the parent-S2 sub-slice decomposition.

**Taken: (b) — fixed `MAX_TX_AGE = 4096` opnum bound.** S2.5 swaps
to (c) when the watermark protocol ships.

**Pending-tx truncation runs on every commit apply.** The SM
`Op::CommitTx` arm, before adding the new committer to `pending_txs`,
**evicts** every entry whose `commit_opnum < (commit_opnum_of_this_op
- MAX_TX_AGE)`. This is a `BTreeMap::split_off` operation —
amortised O(log n + k) where k is the number of evicted records.
Deterministic across replicas (every replica's `pending_txs` state
after the apply is byte-identical).

**Pending-tx restart-rebuild.** On SM startup (after a crash or fresh
boot), `pending_txs` is empty. **It is rebuilt by replaying the last
MAX_TX_AGE Op::CommitTx entries from the log** — exactly the same
deterministic apply path that built it pre-crash. This is the
**determinism-by-construction** discipline: pending_txs is not
persistent state; it is a **memoised function** of the log tail.

**Thesis fit:** `deterministic` (the eviction is BTreeMap range-
truncation — deterministic across replicas); `replayable` (the
pending_txs state is a deterministic function of the last MAX_TX_AGE
log entries; restart-rebuilds via log replay); `honest-docs` (the
fixed-bound vs watermark-derived choice is explicitly named; the
conservative-abort behavior on too-old Tx is named).

### Decision 6 — Tx API surface: **`Tx::begin_ssi(&mut store, snapshot_opnum)` + `Tx::commit_ssi(self, commit_opnum)` (new constructors / new commit; SP112 surface untouched)**

Two structural options:

- **(a) Add a per-Tx `ssi_mode: bool` flag.** Every Tx is constructed
  with `Tx::begin_rw(&mut store, snapshot, ssi_mode: bool)`; the
  `commit` arm consults the flag and emits an Op::CommitTx with
  either an empty or non-empty `read_set`. **Pros**: one Tx
  constructor, one commit arm. **Cons**: callers must pass the
  ssi_mode at begin-time, threading it through every code path —
  the bool spread.
- **(b) Add explicit `Tx::begin_ssi` + `Tx::commit_ssi` methods.**
  The SP112 `Tx::begin_rw` + `Tx::commit` stay as the SI-only path;
  SSI callers use the new methods. **Pros**: zero churn to SP112
  callers; the SSI vs SI distinction is type-level-explicit in the
  caller's code (any code that calls `commit_ssi` is documentably
  SSI). **Cons**: surface area grows by two methods.

**Taken: (b) — `Tx::begin_ssi` + `Tx::commit_ssi` (NEW); SP112 `Tx::begin_rw` + `Tx::commit` (UNCHANGED).**

**Why bold over safe.** Option (b) preserves the **explicit-call-site
discipline** the project uses everywhere (the SP104 `decode_dict_v1`
vs `decode_dict_v2` split; the SP107 V1 vs V2 page paths). The SSI
opt-in becomes visible at every call site; future maintainers reading
the Tx flow know without grep-checking a struct field whether a Tx is
SSI or SI. The two-method surface growth is small relative to the
documentation cost a `ssi_mode: bool` flag would impose.

**API shape (post-S2.4):**

```rust
impl<'a, V: Vfs> Tx<'a, V> {
    // SP111 / SP112 — UNCHANGED:
    pub fn begin(store: &'a Storage<V>, snapshot_opnum: u64) -> Self;
    pub fn begin_rw(store: &'a mut Storage<V>, snapshot_opnum: u64) -> Self;
    pub fn read(&mut self, type_id: u32, object_id: &[u8; 16]) -> SnapshotRead;
    pub fn write(&mut self, type_id: u32, object_id: &[u8; 16], value: Option<Vec<u8>>);
    pub fn read_set(&self) -> &BTreeSet<(u32, [u8; 16])>;
    pub fn write_set(&self) -> &BTreeMap<(u32, [u8; 16]), Option<Vec<u8>>>;
    pub fn snapshot_opnum(&self) -> u64;
    pub fn commit(self, commit_opnum: u64) -> Result<TxCommitOutcome, TxError>;       // SI only (SP112)
    pub fn commit_read_only(self);
    pub fn abort(self);

    // SP113 / S2.4 — NEW:
    /// Begin an SSI-mode write-capable Tx. Identical to `begin_rw` at
    /// the storage-borrow level; differs only in the eventual commit
    /// path: `commit_ssi` ships the read_set over the wire so the SM
    /// can run Cahill's dangerous-structure check.
    pub fn begin_ssi(store: &'a mut Storage<V>, snapshot_opnum: u64) -> Self;

    /// Conflict-checked SSI commit. Ships the Tx's read_set + write_set +
    /// snapshot_opnum in `Op::CommitTx`; the SM's apply arm derives
    /// rw-antidependency edges against its pending_txs window and
    /// aborts on a dangerous structure (Cahill SSI Decision 1).
    /// Otherwise behaviour is identical to `commit` (SI mode).
    ///
    /// Outcome shape extends `TxCommitOutcome::Aborted` to surface the
    /// `DangerousStructure` reason (new in S2.4); SP112 SI callers
    /// using `commit` still see only `WriteWriteConflict` etc.
    pub fn commit_ssi(self, commit_opnum: u64) -> Result<TxCommitOutcome, TxError>;
}
```

`Tx::begin_ssi` and `Tx::begin_rw` are **structurally identical** at
the Tx level (both store an exclusive borrow + initialise empty
read_set/write_set). The difference is **what `commit` vs `commit_ssi`
serializes into `Op::CommitTx`**: `commit` sends `read_set = vec![]`
(SI semantics on the apply side); `commit_ssi` sends
`read_set = self.read_set.iter().copied().collect()` (SSI semantics).
Internally there is **no SSI-mode flag on the Tx struct** — the SI/SSI
distinction is purely a property of *which commit method was called*.

**`TxCommitOutcome::Aborted { conflicting_key, reason }` extension.**
The SP112 shape was `Aborted { conflicting_key: (u32, [u8; 16]) }`;
S2.4 extends to:

```rust
#[non_exhaustive]
pub enum TxCommitOutcome {
    Committed { commit_opnum: u64 },
    /// SP112 SI variant — wraps a single conflicting key.
    Aborted { conflicting_key: (u32, [u8; 16]) },
    /// SP113 / S2.4 — SSI-specific dangerous-structure abort. The
    /// pivot Tx had two consecutive rw-antidependency edges in the
    /// SM's pending_txs window; Cahill SSI aborts the committing Tx
    /// to preserve serializability. `other_commit_opnum` surfaces
    /// the other Tx in the chain for debugging.
    AbortedDangerousStructure { other_commit_opnum: u64 },
}
```

**Backward compat.** `TxCommitOutcome` is `#[non_exhaustive]` (SP112
discipline); a new variant is non-breaking. Every SP112 caller's
`match` arm with a `_` clause continues to compile.

**Thesis fit:** `deterministic` (the SI/SSI distinction is structural,
not flag-based); `honest-docs` (the `commit_ssi`-vs-`commit` split is
documented at every call site; the read_set field carries the SI/SSI
distinction over the wire).

### Decision 7 — TLA+ verification: **`MVCCSsi.tla` extends `MVCCSi` with rw-edge graph + dangerous-structure detector + NoWriteSkew + SerializableEquivalence invariants**

Per the parent design Decision 7 + SP110/SP111/SP112 discipline, S2.4
ships a TLA+ extension. The spec EXTENDS `MVCCSi` (the SP112 spec) so
the SSI layer is checked over the same versioned-storage + Tx + SI
model TLC has already verified.

**File:** `kesseldb-tla/MVCCSsi.tla` — `EXTENDS MVCCSi`.

**State variable additions:**
- `pendingTxs` — TLA+ function from `OpNums` (a Tx's `commit_opnum`)
  to a record `[snapshot, read_set, write_set, has_incoming_rw,
  has_outgoing_rw]` for every committed Tx still within MAX_TX_AGE
  of the latest commit. Domain restricted by the truncation rule.
- `rwEdges` — TLA+ set of records `[from_commit_opnum,
  to_commit_opnum, key]` recording every rw-antidependency edge
  derived during apply. Strictly auxiliary (used by the
  `NoWriteSkew` invariant; not consulted by the abort decision —
  the per-Tx `has_incoming_rw`/`has_outgoing_rw` flags carry the
  abort signal).

**Actions (additions over SP112's TxBegin / TxRead / TxWrite /
TxAbort / CommitTx):**
- `BeginSsi(t, s)` — identical to `TxBegin(t, s)` modulo an internal
  flag distinguishing the SSI mode. (The flag is unnecessary in the
  TLA+ model because `CommitSsi` is the only action that consults
  the read_set for rw-edge derivation; `BeginSsi` is therefore a
  TLA+ alias for `TxBegin`.)
- `CommitSsi(t, c)` — the SSI conflict-checked + dangerous-structure-
  checked commit at commit_opnum c. Precondition:
  `txs[t].status = "Active"` AND `c \in OpNums` AND `txs[t].snapshot
  <= c`. Semantics:
  1. **Window truncation.** Evict from `pendingTxs` every record
     whose `commit_opnum < c - MAX_TX_AGE`.
  2. **Plain-SI write-write conflict check** (carried forward from
     `MVCCSi.CommitTx`):
     ```
     ww_conflict(t, c) ==
         \E k \in DOMAIN txs[t].write_set :
             HasVersionInRange(k, txs[t].snapshot, c - 1)
     ```
     If TRUE: flip status to Aborted; storage UNCHANGED.
  3. **SSI rw-edge derivation.** For every Tx_A in `pendingTxs`
     concurrent with t (`pendingTxs[a].commit_opnum > txs[t].snapshot`):
     - If `DOMAIN pendingTxs[a].write_set \cap txs[t].read_set != {}`:
       record an rw-edge **t →rw a** (Tx_A's write invalidated a
       read Tx_t made of the same key). Mark `pendingTxs[a].has_incoming_rw := TRUE`.
       *Also* mark a synthetic outgoing flag on the committing Tx t.
     - If `DOMAIN txs[t].write_set \cap pendingTxs[a].read_set != {}`:
       record an rw-edge **a →rw t** (Tx_t's write would invalidate
       a read Tx_A made of the same key). Mark `pendingTxs[a].has_outgoing_rw := TRUE`.
       *Also* mark a synthetic incoming flag on the committing Tx t.
  4. **Dangerous-structure check.** If the committing Tx t has BOTH
     an outgoing AND an incoming rw-edge (per step 3's synthetic
     flags) → abort t with `DangerousStructure` (Decision 3).
     Storage UNCHANGED. Pending_txs UNCHANGED (the would-be record
     for t is NOT added).
  5. **Otherwise** install every (k, v) in `txs[t].write_set` via
     lifted `Put` / `Tombstone` actions at `commit_opnum = c`; flip
     status to Committed; **add a fresh pendingTxs record for t**
     `[snapshot |-> txs[t].snapshot, read_set |-> txs[t].read_set,
       write_set |-> DOMAIN txs[t].write_set, has_incoming_rw |-> FALSE,
       has_outgoing_rw |-> FALSE]`; bump `opCount` past c.

**Invariants (the verifiable claims):**
- All 11 MVCCSi invariants preserved.
- **TypeOKSsi** — well-typed SSI state-space (extends `TypeOKSi`
  with `pendingTxs` and `rwEdges` fields).
- **PendingTxsWindowBounded** — every record in `pendingTxs` has
  `commit_opnum >= current_apply_opnum - MAX_TX_AGE`.
- **DangerousStructureAborts** — for every two committed Tx t_pivot,
  t_outer with `t_pivot →rw t_outer` in `rwEdges` AND
  `t_pivot ←rw t_inner` in `rwEdges` for some t_inner, **at most
  one of {t_pivot, t_outer, t_inner} is in status="Committed"**.
  (The dangerous structure forced at least one abort.)
- **NoWriteSkew** — for every pair of committed concurrent Tx t1, t2
  with non-trivial read-write skew
  (`txs[t1].read_set \cap DOMAIN txs[t2].write_set != {}`
  AND `DOMAIN txs[t1].write_set \cap txs[t2].read_set != {}`),
  **at most one is in status="Committed"**. (The classic write-skew
  anomaly is impossible.)
- **SerializableEquivalence** — there exists a permutation of the
  committed Tx such that running them in that order against an
  initially-empty MVCC store produces the same final `versions`
  state. (The strong form of the serializability claim; TLC checks
  it via existential quantification over the small `TxIds` universe.)

**Bounded model (initial `.cfg`):**
```
SPECIFICATION Spec

CONSTANTS
    Keys      = {k1, k2}      \* (type_id, object_id) pairs
    Values    = {v1, v2}
    MaxOpnum  = 3
    MaxOps    = 4
    TxIds     = {t1, t2}      \* 2 concurrent Tx — sufficient for write-skew counterexample
    MaxTxOps  = 4             \* Begin + Write + Read + Commit/Abort
    MaxTxAge  = 5             \* truncation bound; MAX_TX_AGE in the Rust code

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

CHECK_DEADLOCK FALSE
```

**Coverage target.** Per the SP110 + SP111 + SP112 precedent, target
complete coverage of the bounded model with ZERO invariant violations.
The state space is meaningfully larger than MVCCSi's (the per-Tx
rw-edge tags plus the pendingTxs map; the CommitSsi action multiplies
the action-space by the rw-edge derivation branches). The bounded
constants are tightened relative to MVCCSi (Keys = 2, Values = 2,
MaxOpnum = 3, MaxOps = 4, TxIds = 2, MaxTxOps = 4, MaxTxAge = 5) to
keep wall-clock runtime tractable. **A 2-Tx model IS sufficient to
produce the classic write-skew counterexample** (Cahill's original
TPC-C banking example is 2 Tx); a 3-Tx model would let TLC also find
the canonical T0→T1→T2 dangerous-structure triple, which is an S2.X
follow-up if the 2-Tx model leaves the `DangerousStructureAborts`
invariant non-trivially constrained.

**Honest disclosure.** The bounded model verifies the SSI invariants
on the abstract MVCCSsi spec — not the Rust code itself. The
named-action correspondence (action-mapping table in the spec head)
is the manually-maintained bridge between the spec and the Rust code,
exactly as SP110/SP111/SP112 disclosed. The Rust integration tests
(T3) gate the byte-identity claim across 3 replicas for SSI commits
AND prove `Tx::commit_ssi` (standalone path) ↔ `Op::CommitTx` with
non-empty read_set (SM apply path) byte-equivalence.

**Thesis fit:** `verifiable` (extends SP112's TLA+ rigor to the SSI
layer; NoWriteSkew + SerializableEquivalence are mechanically-checked
serializability claims; the fifth rigor-gate TLA+ module in the project);
`honest-docs` (the 2-Tx bound + 3-Tx S2.X follow-up + spec-vs-Rust
correspondence caveat are all disclosed).

### Decision 8 — Backward compatibility: **purely additive; zero legacy-path bytes change; SP112 plain SI is the empty-read_set special case**

Per the parent design Decision 8 + SP110/SP111/SP112 discipline:

- **Zero changes to existing `kessel-sm` apply paths** in S2.4 except
  the **inner extension** of the SP112 `Op::CommitTx => ...` arm with
  the SSI dangerous-structure check **gated on `read_set.is_empty()
  == false`**. The plain-SI path (empty read_set) is byte-identical
  to SP112 — every existing SP112 KAT, integration test, and pentest
  passes byte-net-0.
- **Zero changes to `kessel-sql`** in S2.4. SQL routing through SSI is
  the S2.6 responsibility (S2.6 picks per-Tx whether to use
  `Tx::commit` or `Tx::commit_ssi`).
- **Zero changes to `kessel-vsr`** in S2.4. The Op enum extension is
  wire-compatible (additive field within tag 44); replication
  serializes the new field via the existing kessel-proto codec.
- **Zero new external dependencies** in S2.4. The SSI pending-tx map
  uses `std::collections::BTreeMap` (already in std; SP111 + SP112
  use the same module). `cargo tree -p kesseldb-server | grep -Ei
  "parquet|objstore|rustls|webpki"` stays byte-identical to the SP112
  baseline.
- **Zero new public methods on `Storage<V>`** in S2.4. The Tx layer
  calls `mvcc::has_version_in_range` (SP110) and `mvcc::put_versioned`
  (SP110) only. The new SSI rw-edge derivation is an SM-internal pure
  function operating on `pending_txs` + the committing Tx's
  `(snapshot_opnum, read_set, write_set)`.
- **Every SP1–SP112 path bytes-on-disk-identical.** The S2.4 changes
  touch only NEW code paths (the SM `pending_txs` field, the inner
  branch of `Op::CommitTx` apply gated on non-empty read_set, the new
  `Tx::begin_ssi`/`commit_ssi` methods). The legacy 20-byte key path
  AND the S2.1 MVCC 28-byte versioned key path BOTH stay byte-net-0.
- **SP112 SI behaviour is the empty-read_set special case** of S2.4's
  SSI semantics. **This is a formal equivalence**: a Tx whose
  read_set is empty produces zero rw-edges, hence zero dangerous
  structures, hence the SSI verdict reduces to the SP112 SI verdict.
  Documented as a T2 KAT (`kat_ssi_empty_read_set_degenerates_to_si`)
  and a T4 coverage test.

**Gate growth is purely new SSI tests** on the dangerous-structure
detector + the rw-edge derivation + the `Tx::begin_ssi`/`commit_ssi`
methods. The existing 570-test cargo gate (SP112 final) plus the new
SSI tests becomes the S2.4 final; T6 records the actual delta.

**Thesis fit:** `honest-docs` (the parallel-module + additive-Op
discipline holds; the SI-as-SSI-special-case equivalence is documented
as a formal claim and gated by a KAT); `zero-dep` (no new external crate).

### Decision 9 — Slice numbering: **SP113** (the slice immediately after SP112)

SP113 in the subproject numbering. The S2.4 plan/spec filenames use
the `2026-05-24` date prefix. The internal record (T6 will create it)
is:

- Spec/design: this file —
  `docs/superpowers/specs/2026-05-24-mvcc-si-s2-4-design.md`.
- Plan: companion file —
  `docs/superpowers/plans/2026-05-24-mvcc-si-s2-4.md`.
- Slice closeout record (T6 will create it):
  `docs/superpowers/specs/2026-05-24-kesseldb-subproject113-mvcc-ssi-s2-4.md`.

Subproject-number / S2-sub-slice cross-reference table after S2.4:

| Subproject | S2 sub-slice | Status | Headline |
|---|---|---|---|
| SP110 | S2.1 | done | MVCC versioned-storage primitive |
| SP111 | S2.2 | done | Tx context + read-set tracking |
| SP112 | S2.3 | done | SI write-side + conflict detection at SM apply time |
| **SP113** | **S2.4** | **this slice** | **SSI promotion via Cahill dangerous-cycle detection** |
| SP114+ | S2.5 | pending | GC + watermark (supersedes Decision 5's fixed MAX_TX_AGE) |
| ... | S2.6 | pending | SQL + SM cutover |

**Thesis fit:** `honest-docs` (the slice numbering + cross-reference
makes the strategic-tier trajectory inspectable from any single record;
the SP114/S2.5 forward-link to the MAX_TX_AGE-supersession is named here).

---

## Architecture

### High-level layering after S2.4

```
                  +---------------------------+
                  |  kessel-sql (unchanged)   |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-sm (NEW field +   |   <-- S2.4 (this slice)
                  |  EXTENDED Op::CommitTx)   |       SSI dangerous-structure
                  |  + pending_txs map        |       detector runs HERE,
                  |  + Cahill SSI verdict     |       inner-gated on non-
                  |  + window truncation      |       empty read_set
                  |  SP112 SI path UNCHANGED  |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage::tx       |   <-- S2.2 + S2.3 + S2.4
                  |  + begin_ssi / commit_ssi |       (SSI commit added)
                  |  + read-your-writes       |       (SP112 commit untouched)
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage::mvcc     |   <-- S2.1 (UNCHANGED)
                  +---------------------------+

                  +---------------------------+
                  |  kessel-proto (EXTENDED   |   <-- S2.4 (this slice)
                  |  Op::CommitTx with        |       wire-compat via
                  |  read_set field)          |       additive field at
                  |  + AbortReason::          |       tag 44; AbortReason
                  |    DangerousStructure     |       sub-tag 3
                  +---------------------------+
```

The SSI verdict-seam is `kessel-sm::StateMachine::apply`'s extended
`Op::CommitTx` arm — same seam as SP112's plain-SI verdict. The two
verdicts compose: plain SI runs first (write-write check); on no
ww-conflict, SSI runs (rw-edge derivation + dangerous-structure
check) iff the read_set is non-empty.

### Module changes (S2.4 deltas only)

- `crates/kessel-proto/src/lib.rs` — extend the `Op::CommitTx`
  variant with `read_set: Vec<(u32, [u8; 16])>`; extend `enum
  AbortReason` with `DangerousStructure { other_commit_opnum: u64 }`
  at sub-tag 3; update encode/decode for both extensions.
- `crates/kessel-storage/src/tx.rs` — add `pub fn begin_ssi(store:
  &'a mut Storage<V>, snapshot_opnum: u64) -> Self`; add `pub fn
  commit_ssi(self, commit_opnum: u64) -> Result<TxCommitOutcome,
  TxError>`; extend `enum TxCommitOutcome` with
  `AbortedDangerousStructure { other_commit_opnum: u64 }`; SP112 surface
  unchanged.
- `crates/kessel-sm/src/lib.rs` — add `pending_txs: BTreeMap<u64,
  PendingTxRecord>` field to `StateMachine<V>`; define
  `PendingTxRecord` struct (`pub(crate)`); extend the
  `Op::CommitTx => { ... }` arm with the SSI inner branch (window
  truncation + rw-edge derivation + dangerous-structure check + new
  pending_txs insertion on commit); the SP112 plain-SI behaviour is
  the structural fall-through for empty-read_set commits.
- `crates/kessel-sm/src/lib.rs` — extend `StateMachine::new` (or the
  equivalent constructor) to initialise `pending_txs:
  BTreeMap::new()`. Restart-rebuild is automatic via the SP112 SM
  apply path being driven by the log replay (every replayed
  `Op::CommitTx` reconstructs its pending_txs slot; eviction
  re-runs as it did originally).
- `kesseldb-tla/MVCCSsi.tla` + `.cfg` + baseline TLC result.
- New test files: `crates/kessel-storage/tests/integration_mvcc_ssi.rs`,
  `crates/kessel-storage/tests/pentest_mvcc_ssi.rs`.
- New slice record (T6): `docs/superpowers/specs/2026-05-24-kesseldb-subproject113-mvcc-ssi-s2-4.md`.

**No new crates. No new files outside the seven listed.**

### Internal data shape

**`Tx<'a, V: Vfs>` (S2.4 surface — UNCHANGED from S2.3).** The Tx
struct keeps its four fields (`store`, `snapshot_opnum`, `read_set`,
`write_set`); no per-Tx SSI-mode flag is added (Decision 6).

**`StateMachine<V: Vfs>` (S2.4 expansion).** Gains ONE new field:
`pending_txs: BTreeMap<u64, PendingTxRecord>`. The
`PendingTxRecord` struct is `pub(crate)` (SM-internal; not exposed
across the crate boundary).

**`PendingTxRecord`** (per Decision 2):
- `snapshot_opnum: u64` — pinned at the committed Tx's begin-time.
- `read_set: Vec<(u32, [u8; 16])>` — sorted, deterministic-iteration.
- `write_set: Vec<(u32, [u8; 16])>` — keys only, sorted, deterministic.
  Values discarded (rw-edges are over keys, not values).
- `has_outgoing_rw: bool` — set TRUE when a later commit reveals an
  rw-edge from this Tx to itself.
- `has_incoming_rw: bool` — set TRUE when a later commit reveals an
  rw-edge to this Tx from itself.

**Restart-rebuild semantics.** `pending_txs` is **NOT persisted**.
On SM startup, it is empty. The SP112 SM apply path is the log replay
driver; every replayed `Op::CommitTx` reconstructs its pending_txs slot
exactly as it did originally (eviction included). After replaying the
last MAX_TX_AGE log entries, `pending_txs` is byte-identical to its
pre-crash state. **This is the determinism-by-construction property
SP112 used for the SM state; S2.4 inherits it.**

### Call graph (S2.4 additions)

```
caller (test / SQL planner in S2.6)
   |
   | tx = Tx::begin_ssi(&mut store, snapshot_opnum);
   | tx.read(...); tx.write(...);           // SP112 surface, unchanged
   | tx.commit_ssi(commit_opnum)
   |
   v
Tx::commit_ssi(self, commit_opnum)
   |
   | -- construct Op::CommitTx with read_set = Vec::from(self.read_set)
   | -- in S2.4 STANDALONE form (no SM-caller integration; matches SP112):
   |    drive the SM apply path locally for testability.
   |
   v
StateMachine::apply(op_number, Op::CommitTx { snapshot, write_set, commit, read_set })
   |
   |--+ if snapshot > commit → OpResult::TxAborted { SnapshotOutOfRange }
   |
   |--+ (SP112 SI write-write check; UNCHANGED)
   |  | for (t, o, _) in write_set:
   |  |   if has_version_in_range(store, t, &o, snapshot, commit - 1):
   |  |     → OpResult::TxAborted { WriteWriteConflict { t, o } }
   |
   |--+ (S2.4 SSI inner branch; gated on read_set.is_empty() == false)
   |  | // Step A: window truncation
   |  | let lo = commit.saturating_sub(MAX_TX_AGE);
   |  | let _ = pending_txs.split_off(&lo);   // evict older
   |  |
   |  | // Step B: rw-edge derivation
   |  | let mut has_outgoing = false;
   |  | let mut has_incoming = false;
   |  | let mut other_commit_opnum = 0u64;
   |  | for (a_commit, a_rec) in pending_txs.range(snapshot+1 .. commit) {
   |  |   // (a) does Tx_A's write-set invalidate any of Tx_t's reads?
   |  |   if !disjoint(&a_rec.write_set, &this_read_set_keys) {
   |  |     has_outgoing = true; other_commit_opnum = *a_commit;
   |  |     pending_txs.get_mut(a_commit).has_incoming_rw = true;
   |  |   }
   |  |   // (b) does Tx_t's write-set invalidate any of Tx_A's reads?
   |  |   if !disjoint(&this_write_set_keys, &a_rec.read_set) {
   |  |     has_incoming = true; other_commit_opnum = *a_commit;
   |  |     pending_txs.get_mut(a_commit).has_outgoing_rw = true;
   |  |   }
   |  | }
   |  |
   |  | // Step C: dangerous-structure check (Cahill)
   |  | // The pivot has BOTH an incoming AND an outgoing rw-edge. The
   |  | // committing Tx t becomes a dangerous pivot iff both flags
   |  | // were set above, OR iff any pending Tx_A now has both flags
   |  | // (i.e. became a pivot via this commit).
   |  | if has_outgoing && has_incoming {
   |  |   return OpResult::TxAborted { DangerousStructure { other_commit_opnum } };
   |  | }
   |  | for (_, a_rec) in pending_txs.range(snapshot+1 .. commit) {
   |  |   if a_rec.has_incoming_rw && a_rec.has_outgoing_rw {
   |  |     // The pre-existing committed Tx_A is now a dangerous pivot
   |  |     // BECAUSE OF this commit. Cahill's abort-the-latest rule
   |  |     // (Decision 3) aborts THIS commit, not Tx_A.
   |  |     return OpResult::TxAborted { DangerousStructure { other_commit_opnum: a_rec_commit } };
   |  |   }
   |  | }
   |
   |--+ (SP112 SI install path; UNCHANGED)
   |  | for (t, o, v) in write_set:
   |  |   put_versioned(&mut store, t, &o, commit, v)?;
   |
   |--+ (S2.4 register THIS Tx in pending_txs)
   |  | if !read_set.is_empty() || !write_set.is_empty():
   |  |   pending_txs.insert(commit, PendingTxRecord { ... });
   |
   v
OpResult::TxCommitted { commit_opnum: commit }
```

**Per-step determinism.** Every step is a pure function of the apply
inputs + the deterministic `pending_txs` state. The
`BTreeMap::split_off` + `BTreeMap::range` operations are
deterministic-iteration (sorted by `commit_opnum`); the rw-edge
checks use sorted-vec intersection (a tight O(n + m) loop over two
sorted slices; no hashing); the abort verdict is a function of the
two boolean flags. **No wall clock, no thread scheduling, no
allocator state.** The thesis-fit `deterministic` invariant carries.

### MVCCSsi.tla extension (the verifiable artifact)

Per Decision 7. See `kesseldb-tla/MVCCSsi.tla` after T6 lands. The
spec mechanically checks (over the bounded 2-Tx model):

- All 11 MVCCSi invariants carried forward (TypeOKSi,
  WriteSetMonotonic, WriteWriteConflictDetected, CommitAtomicity,
  FirstCommitterWins, DeterministicApply, + 6 MVCCTx invariants).
- **5 new SSI invariants**: `TypeOKSsi`, `PendingTxsWindowBounded`,
  `DangerousStructureAborts`, **`NoWriteSkew`**, **`SerializableEquivalence`**.

The two highlighted invariants are the **serializability-level
claims**: write-skew is impossible (NoWriteSkew) and every committed
sequence is equivalent to a serial schedule (SerializableEquivalence).
These are the verifiable artifacts that distinguish S2.4 from S2.3 at
the formal-spec level.

---

## The SSI conflict-detection contract (formal)

This section states, in code-grounded prose, what S2.4 ships as
behaviour. Every clause is gated by a KAT in T2, an integration test
in T3, a coverage test in T4, or a pentest in T5.

### Write-skew prevention invariant (the headline SSI claim)

For any two concurrent Tx Tx_A, Tx_B (both with non-empty read_sets;
both calling `commit_ssi`) whose commits would constitute a write-skew
anomaly under plain SI — that is, `read_set(A) ∩ write_set(B) != ∅`
AND `write_set(A) ∩ read_set(B) != ∅` — **at most one of {Tx_A, Tx_B}
commits successfully**; the other aborts with
`TxCommitOutcome::AbortedDangerousStructure`. Gated by T3's
integration test
`integration_classic_write_skew_aborted_under_ssi_committed_under_si`,
which runs the same workload twice (once via `commit`, once via
`commit_ssi`) and asserts opposite outcomes.

### Dangerous-structure detection invariant

Per Cahill SSI. For any committed Tx Tx_pivot with an rw-edge
Tx_pivot →rw Tx_outer AND an rw-edge Tx_inner →rw Tx_pivot in the SM's
`pending_txs` window at any apply point, **at least one of {Tx_pivot,
Tx_outer, Tx_inner} was aborted**. The abort-the-latest rule
(Decision 3) means the aborted Tx is whichever was the committing Tx
when the dangerous structure was completed. Gated by T2's KAT
`kat_dangerous_structure_aborts_committing_tx`.

### Determinism invariant (carried forward + extended)

The SSI verdict — Committed / AbortedDangerousStructure / Aborted{WriteWriteConflict}
/ Aborted{SnapshotOutOfRange} — is a deterministic function of
`(log prefix, pending_txs state at apply, snapshot_opnum, read_set,
write_set, commit_opnum)`. Every replica running the same log prefix
reaches the same verdict and writes byte-identical `pending_txs` +
storage state. Gated by T3's 3-replica byte-identity test for SSI
commits.

### SI-equivalence on empty-read_set invariant

For every `Op::CommitTx` apply where `read_set.is_empty()`, the verdict
is **byte-identical to SP112's plain-SI verdict** (no rw-edge
derivation runs; no pending_txs insertion gated by the read_set check
— though the write_set-only insertion may still happen for ww-edge
tracking in later slices, but in S2.4 we DO NOT insert pending_txs
for empty-read_set commits per the Decision 2 read-only-fast-path).
Gated by T2's KAT `kat_ssi_empty_read_set_degenerates_to_si` and the
SP112 regression — every SP112 KAT runs unchanged in T2's regression
sweep.

### Pending-tx window bounded invariant

For every committed Tx in `pending_txs` after any `Op::CommitTx`
apply, `commit_opnum >= current_apply_opnum - MAX_TX_AGE`.
**Equivalently:** the size of `pending_txs` is bounded by the number
of distinct `commit_opnum` values in the last MAX_TX_AGE log entries.
Gated by T5's pentest `pt_pending_txs_window_truncation_bounded`.

### Apply atomicity invariant (carried forward from SP112)

For every Tx with status="Committed" at `commit_opnum c`, EVERY key
in `write_set` has a version at exactly `commit_opnum c` in the
versioned storage, and a `pending_txs` entry at key `c` (gated on
non-empty read_set). For every Tx with status="Aborted" (any reason),
ZERO keys in `write_set` have a version at `commit_opnum c`, and NO
pending_txs entry exists at key `c`. Gated by T2's KAT
`kat_aborted_ssi_commit_leaves_no_pending_txs_record`.

### Read-set communication invariant (forward-compat for S2.5/S2.6)

`Tx::commit_ssi` serializes the entire `read_set` into
`Op::CommitTx.read_set`. The wire cost is `read_set.len() * 20 bytes
+ length-prefix overhead`. For a 100k-entry read_set this is ~2 MiB.
**Honest disclosure**: S2.4 accepts this cost; S2.X may revisit with
a bloom-filter approximation or a per-Tx read-set hash. The empty-
read_set fast-path (Decision 4) bounds the cost for SI-mode commits
to zero. Gated by T5's pentest `pt_hostile_giant_read_set_under_ssi`.

---

## Sub-slice gate accounting (estimated)

Total cargo gate growth in S2.4: estimated **+25 to +35 tests** on
the new SSI surface + the dangerous-structure detector. Breakdown:

| Task | Expected tests | Cumulative | Notes |
|---|---|---|---|
| T0 baseline | 0 | 570 | SP112 final, expect FAILED=0 + seed-7 green |
| T1 scaffold | +2 | 572 | Type-shape locks: AbortReason::DangerousStructure constructible; TxCommitOutcome::AbortedDangerousStructure constructible; Tx::begin_ssi + commit_ssi signatures present |
| T2 impl + KATs | +11 | 583 | classic-write-skew-aborted / ssi-empty-read-set-degenerates-to-si / dangerous-structure-aborts-committing-tx / aborted-ssi-leaves-no-pending-txs / rw-edge-derivation-incoming / rw-edge-derivation-outgoing / window-truncation-evicts-old / SM-apply-matches-Tx-commit_ssi-byte-equiv / Op::CommitTx wire roundtrip with non-empty read_set / read-only-tx-no-pending-tx-insert / si-and-ssi-tx-coexist-in-same-stream |
| T3 integration | +6 | 589 | classic-write-skew SI-vs-SSI distinction (the headline) / two-non-conflicting-SSI-Tx-both-commit / read-only-SSI-Tx-never-aborts / 3-replica byte-identity for SSI commits / SM-apply-byte-equivalence with Tx::commit_ssi / write-skew with intermediate non-SSI Tx |
| T4 coverage | +4 | 593 | empty-read_set commit via commit_ssi (degenerates) / large-read_set (1000 entries) commit_ssi success / dangerous structure detected on every replica identically / SI-via-commit and SSI-via-commit_ssi interleaved |
| T5 pentest | +6 | 599 | hostile 100k read_set under SSI / pathological RW-edge graph (max concurrency) / pending-tx window boundary (commit_opnum exactly at MAX_TX_AGE) / snapshot-just-past-MAX_TX_AGE-rejected / overflow-safety (commit_opnum=u64::MAX) / compile-time lock on commit_ssi-after-abort |
| T6 docs + TLA+ | 0 Rust | 599 | MVCCSsi.tla + .cfg + TLC baseline + SP113 record + STATUS + memory |

**Estimated final cargo gate after S2.4:** **~599 tests** (`FAILED=0`,
seed-7 green). The actual number lands in T6.

The TLA+ artifact's gate is the TLC baseline run (zero invariant
violations on the bounded config) + the artifact files committed to
`kesseldb-tla/`.

---

## Sub-slice decomposition reminder (S2.5–S2.6 still pending)

S2.4 ships ONLY the SSI promotion: `Tx::begin_ssi`/`commit_ssi`, the
extended `Op::CommitTx` with read_set, the SM `pending_txs` map +
dangerous-structure detector, and the `MVCCSsi.tla` artifact. The
following are explicitly **OUT of scope for S2.4** and tracked in the
parent S2 design's sub-slice decomposition:

- **S2.5** — GC + watermark (`Op::AdvanceWatermark`). **Supersedes
  Decision 5's fixed MAX_TX_AGE** with a watermark-derived dynamic
  horizon. The natural follow-up to S2.4.
- **S2.6** — SQL surface integration + SM cutover (the byte-identity-
  gate-change slice, honest-disclosed there). Also wires the
  `sm.next_op_number()` source of `commit_opnum`, the cursor-stall-on-
  snapshot-not-yet-applied semantics, and the per-Tx SI-vs-SSI choice
  at the SQL planner level.

Each subsequent sub-slice will get its own plan when the prior one
lands. S2.5 plan is the next docs slice expected after this S2.4
slice ships.

---

## Honest deferred set

Items explicitly out of scope for S2.4, named here so the S2.4 record
can't drift into over-claim territory:

- **GC + watermark.** Deferred to S2.5; supersedes Decision 5's fixed
  MAX_TX_AGE = 4096 bound.
- **SM-side `next_op_number()` helper supplying `commit_opnum`.**
  Deferred to S2.6 (carried forward from SP112's deferred set).
- **Cursor-stall on snapshot-not-yet-applied.** Deferred to S2.6
  (carried forward from SP112's deferred set).
- **SQL integration.** Deferred to S2.6.
- **Bloom-filter / hash-based read-set approximation.** S2.4 sends the
  full read_set over the wire (Decision 4 honest disclosure). An
  approximation could be considered in an S2.X follow-up if the wire
  cost shows up in benchmarks.
- **Persistent `pending_txs`.** S2.4 keeps `pending_txs` in-memory +
  log-replay-rebuilt. A persistent shadow (e.g. in the RocksDB CF)
  is not on the S2 roadmap; if SSI restart-rebuild latency becomes a
  problem an S2.X slice could add it.
- **Cross-thread Tx, Tx-pool, Tx ID allocation.** Not on the S2
  roadmap (carried forward from S2.2). Tx is single-thread /
  stack-frame-bound by construction.
- **3-Tx TLA+ model.** S2.4 ships a 2-Tx bounded model (sufficient
  for the 2-Tx write-skew counterexample). A 3-Tx model would let
  TLC also find the canonical T0→T1→T2 dangerous-structure triple;
  deferred to an S2.X follow-up if the 2-Tx model leaves the
  `DangerousStructureAborts` invariant non-trivially under-checked.
- **Multi-replica TLA+ model.** Same scope decision as
  SP110/SP111/SP112 carried forward.
- **Larger TLC bounds for MVCCSsi.** Same disclosure as
  SP110/SP111/SP112: the bounded config in T6 may be tightened to
  keep TLC tractable.
- **TLA+-mechanized-refinement TLA+ ↔ Rust.** Same gap S1/SP109 +
  SP110/SP111/SP112 disclosed. Per-sub-slice named-action
  correspondence carries forward; not a refinement proof.
- **Wire compatibility for Op::CommitTx pre/post-S2.4.** S2.4 is the
  slice that breaks SP112's wire format (the read_set length prefix
  becomes mandatory). Acceptable because no production cluster has
  exchanged Op::CommitTx frames yet (S2.6 is the production-caller
  slice). Documented in Decision 4.

---

## Thesis-fit note

**Thesis fit:** `deterministic` (the SSI verdict is computed by SM
apply over the log-derived `pending_txs` + the committing Tx's
read/write set; structurally cannot diverge across replicas — the same
property SP112 established for plain SI, now extended to the harder
dangerous-structure verdict; **the thesis-fit headline phrase "the
deterministic-log substrate makes serializability cheaper to verify
than in HA-coordination systems" is operationalised in this slice's
Op::CommitTx SSI inner branch**; this is the most direct
deterministic-replicated payoff in the S2 backlog **so far —
serializability becomes a structural property of the log, not a
coordination protocol**);
`replayable` (every SSI Tx outcome is a function of `(snapshot_opnum,
read_set, write_set, commit_opnum, pending_txs window state)` — and
`pending_txs` is itself a function of the log tail, so the full
verdict reduces to the log prefix; debugging IS replay; a production
bug-report on a dangerous-structure abort reduces to a `(seed, log,
opnum, snapshot)` tuple);
`verifiable` (`MVCCSsi.tla` extends SP112's MVCCSi.tla with the
rw-edge graph + the dangerous-structure detector + five new
invariants — TypeOKSsi, PendingTxsWindowBounded,
DangerousStructureAborts, **NoWriteSkew**, **SerializableEquivalence**
— all mechanically-checked by TLC against the same VSR-log substrate
S1/SP109 verified; the **fifth** rigor-gate TLA+ module in the
project; the **first** that proves an isolation-level claim at the
serializability tier);
`honest-docs` (the rejected SSI algorithms (predicate locks, cycle
detection) are explicitly documented as rejected, not silently
revised; the abort-the-latest-vs-abort-the-pivot tradeoff is named
with the +5–10% over-abort cost; the wire-format extension of
Op::CommitTx is single-source-updated and tested; the
fixed-MAX_TX_AGE-vs-watermark trade-off is explicitly named with the
S2.5 supersession forward-link; the SI-as-SSI-empty-read_set special
case is documented as a formal equivalence and gated by a KAT).

The thesis-fit headline of this slice: **serializability is just
another deterministic function of the log prefix.** KesselDB does
not need PostgreSQL's SIReadLock predicate-locking machinery or
CockroachDB's HLC + read-refresh because the rw-edge graph itself is
a function of the deterministic-log substrate; the dangerous-structure
verdict is reached identically on every replica by construction. This
is the most direct expression yet of the "deterministic replicated
SQL with verifiable behavior" pillar in the strategic-tier backlog —
and the slice that closes the SI hole the parent S2 design explicitly
flagged as the reason SSI was on the sub-slice roadmap at all.

---

## Internal record

This design document is
`docs/superpowers/specs/2026-05-24-mvcc-si-s2-4-design.md`.

The S2.4 implementation plan is
`docs/superpowers/plans/2026-05-24-mvcc-si-s2-4.md`.

When S2.4 ships, its slice-record file will be
`docs/superpowers/specs/2026-05-24-kesseldb-subproject113-mvcc-ssi-s2-4.md`
(SP113 in the subproject numbering; mirrors the SP110/SP111/SP112
filename pattern). The record will carry the honest gate accounting
(570 → final), the per-task evidence chain, the TLA+-to-Rust
correspondence table, the deferred backlog (S2.5–S2.6), and the
strategic-tier context update.
