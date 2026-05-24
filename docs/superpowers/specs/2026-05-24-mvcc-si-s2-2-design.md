# S2.2 — Snapshot-IDed Transactions + Read-Set Tracking: Design

**Date:** 2026-05-24
**Status:** Approved (autonomous mandate — see Process Note)
**Strategic-tier item:** S2.2 sub-slice of S2 (Serializable MVCC / Snapshot
Isolation) in the THESIS.md S1–S4 backlog. The **second** built sub-slice of
S2 after S2.1/SP110 (MVCC versioned-storage primitive). SP111 in the
subproject numbering — the slice immediately after SP110.
**Builds on:**
- Project THESIS — `docs/THESIS.md`.
- S2 parent design — `docs/superpowers/specs/2026-05-23-mvcc-si-design.md`.
- S2.1 slice record (SP110) — `docs/superpowers/specs/2026-05-23-kesseldb-subproject110-mvcc-s2-1.md`.
- S2.1 plan — `docs/superpowers/plans/2026-05-23-mvcc-si-s2-1.md`.
- S2.1 TLA+ artifact — `kesseldb-tla/MVCCStorage.tla` + `.cfg` + baseline
  TLC run (`kesseldb-tla/results/2026-05-24-mvcc-storage-baseline.txt`).
- The MVCC module surface that S2.1 shipped: `crates/kessel-storage/src/mvcc.rs`
  (`make_versioned_key`, `decode_commit_opnum`, `put_versioned`,
  `get_at_snapshot`, `has_version_in_range`, `SnapshotRead { Found |
  Tombstoned | NotYetWritten }`, `MvccKeyError`).

---

## Process note (autonomy + brainstorming gate)

Produced under the standing overnight autonomous-build mandate
(`feedback_kesseldb_autonomous_build` + the strategic-tier mandate
`feedback_kesseldb_strategic_tier`): "build the backlog autonomously,
BOLD choices, don't wait for approval, keep the two-stage review gate +
full tests + pentest passes." **The brainstorming user-review gate is
substituted by this documented decision record** — the 9 brainstorm
decisions below are resolved boldly in this document; the user does not
re-review them before the plan executes. **The two-stage subagent review
gate is preserved** for every substantive task (T2/T3/T5/T6), with the
final whole-implementation reviewer dispatched at the end of T6, exactly
as SP110 did.

---

## Strategic-tier framing

S2.2 is the **second sub-slice of S2** in the THESIS.md backlog. SP111
in the subproject numbering. The parent S2 design
(`docs/superpowers/specs/2026-05-23-mvcc-si-design.md`) decomposes S2
into 6 sub-slices: S2.1 (SP110 — versioned-storage primitive) →
**S2.2 (this slice — Tx context + read-set tracking)** → S2.3 (SI
commit + write-set conflict detection) → S2.4 (SSI promotion) → S2.5
(GC + watermark) → S2.6 (SQL integration + SM cutover). This slice
ships the **transaction context + the snapshot-pinned read path + the
read-set bookkeeping** that S2.3's SI conflict detection and S2.4's
SSI dangerous-cycle detection will both consume.

**S2.1 → S2.2 dependency chain.** S2.1 shipped the storage primitive
that answers "what version of K was committed at-or-before opnum S?"
S2.2 introduces a **Tx context** that captures S at begin-time, routes
every read through `get_at_snapshot(K, S)`, and records the set of keys
read so subsequent slices have the bookkeeping they need:

- S2.3 (plain SI) needs `Tx::snapshot_opnum()` to compute the
  `(snapshot_opnum, commit_opnum]` conflict window. It does NOT need
  `read_set` (plain SI ignores reads).
- S2.4 (SSI) needs `Tx::read_set()` to detect dangerous rw-antidependency
  cycles. SSI's promotion of plain SI to true serializability **is the
  reason `read_set` exists in S2.2**.

Shipping `read_set` in S2.2 (not in S2.4) is deliberate: it lets S2.3
land without touching the Tx API again, and it lets S2.4 ship as a
pure SM-state-machine extension without changing the kessel-storage
surface a second time.

---

## Problem

The S2.1 storage primitive answers "give me the version of K at
snapshot S" but provides no concept of a **transaction**. Today, a
SQL `SELECT ... FROM T1 JOIN T2` that wants a consistent point-in-time
view must thread the snapshot opnum manually through every storage
call, and there is nowhere to record which keys were read. Two real
problems:

1. **No snapshot pinning.** A multi-key query reading via the S2.1
   primitive risks **snapshot drift** if any caller updates the
   snapshot opnum mid-query. The primitive does not block this — it
   accepts any `snapshot_opnum: u64` per call. A Tx context binds
   snapshot at begin-time and serves every subsequent read at the
   same opnum, eliminating the drift class of bug by construction.

2. **No read-set substrate for SSI (S2.4).** SSI requires per-Tx
   tracking of which keys the Tx read at which snapshot. Without a
   Tx context, every SSI implementation would either (a) re-thread the
   read-set through every storage call manually (a fragile shape) or
   (b) push read-set tracking down into `kessel-storage::mvcc`
   (a layering violation — storage should not know about transactions).
   A first-class Tx struct that owns its `read_set` is the clean
   substrate.

Neither problem is acute today (no SQL caller integrates with MVCC
until S2.6), but they MUST be solved before S2.3 (which assumes the Tx
context exists and reads come from `Tx::read`) and S2.4 (which
consumes the read-set). S2.2 is the slice that solves them.

---

## Decisions (bold choices, documented)

### Decision 1 — Tx scope: **read-only Tx in S2.2; writes deferred to S2.3**

Three options:

- **(a) Read-only Tx in S2.2.** Tx exposes `begin(snapshot_opnum)`,
  `read(type_id, object_id) -> SnapshotRead`, `snapshot_opnum()`,
  `read_set()`, `commit_read_only() -> Result<(), TxError>`. No write
  API. S2.3 introduces `write(type_id, object_id, value)` and
  `commit() -> Result<CommitOutcome, TxError>` (the conflict-checking
  variant).
- **(b) Read+write Tx in S2.2 with conflict-check deferred to S2.3.**
  Tx accepts writes into a buffer; `commit_read_only` is replaced with
  a no-op `commit` that always succeeds (no conflict check yet); S2.3
  adds the conflict check inside the existing `commit` shape.
- **(c) Full read+write Tx with conflict check in S2.2.** Pulls S2.3
  forward. Bigger slice; harder to two-stage-review.

**Taken: (a) — read-only Tx in S2.2.**

**Why bold over safe.** Option (b) was the parent-design's strawman
(see the S2 sub-slice decomposition table's S2.2 row: "client-side
write buffering. NO commit-time conflict check yet (writes simply
buffer)"). Two costs of (b) ruled it out: (i) shipping a `commit()`
that "looks like a commit but defers the conflict check" is a
half-implemented Tx — a footgun for any caller who reads the type
signature and assumes commit-means-conflict-checked; (ii) the write
buffer's representation matters to S2.3 (Cahill SSI's write-set is
distinct from a naive buffered-writes list — it carries per-write
metadata), and committing to a buffer shape in S2.2 only to refactor
it in S2.3 is wasted work. **Read-only Tx in S2.2** is the cleaner
separation: S2.2 ships exactly the substrate S2.3 + S2.4 need (snapshot
pin + read-set), and S2.3 introduces the write side together with the
conflict check in one coherent slice. The parent-design strawman is
hereby **revised** — this is the autonomous-mandate's "bold choice
documented" path.

**S2.3 impact.** S2.3 will introduce `Tx::write(type_id, object_id,
value: Option<Vec<u8>>)` (the buffered-write API) and `Tx::commit() ->
Result<CommitOutcome, TxError>` (the conflict-checked commit) in the
same slice. The S2.2 `commit_read_only` shape stays — it remains the
correct API for `SELECT`-only transactions in S2.6 and beyond.

**Thesis fit:** `deterministic` (Tx state is pure, snapshot pin is
log-derived); `replayable` (Tx behavior is a function of the snapshot
+ the keys read — both inputs to `(seed, log)` debugging); `honest-docs`
(the parent-design strawman revision is documented here, not silently
deviated from).

### Decision 2 — Snapshot opnum source: **caller-supplied (decoupled API); SM wiring in S2.6**

Two options:

- **(a) Tx::begin reads the snapshot opnum from the SM's committed-
  opnum counter.** The Tx struct holds a reference to both `Storage`
  AND `StateMachine` (or just `StateMachine` if Storage is reachable
  through it). Pros: callers can't forget to pin. Cons: couples
  kessel-storage to kessel-sm (currently they are independent —
  kessel-storage knows nothing about state machines); forces the Tx
  type to live in kessel-sm rather than kessel-storage; testing
  requires an SM stub.

- **(b) Tx::begin takes the snapshot opnum as an explicit parameter.**
  Pros: testable in pure kessel-storage tests (no SM dependency);
  preserves the kessel-storage/kessel-sm boundary; lets S2.6 wire the
  SM as the snapshot-opnum source without re-shaping the API. Cons:
  callers can pass a stale or out-of-bounds opnum (mitigated by
  pentest tests — see T5).

**Taken: (b) — caller-supplied snapshot opnum.**

**Why bold over safe.** Keeping Tx in `kessel-storage::tx` decouples
it from kessel-sm and lets S2.2 ship without a single SM-side change.
The "SM provides snapshot opnum" wiring is the S2.6 responsibility (in
the same slice that cuts SQL over to MVCC) — S2.2 ships a Tx API that
S2.6's SM-integration code calls with `sm.last_committed_opnum()`. This
preserves the parent-design's "MVCC as a parallel module" discipline
(Decision 8) at the Tx layer: Tx is parallel infrastructure that the
SM does not touch until S2.6. The pentest tests in T5 cover hostile
snapshot opnums (0, u64::MAX, snapshot > high_op) so option (b)'s "callers
might lie" failure mode is gated.

**Honest disclosure.** Callers in S2.2 are tests + future S2.3 code;
no production code path reaches Tx until S2.6. The "tests pass a literal
opnum" pattern is the S2.2 + S2.3 normal — production code in S2.6 will
wrap `Tx::begin` in an SM helper that reads `sm.last_committed_opnum()`.

**Thesis fit:** `deterministic` (caller-supplied opnum is itself a
function of the log, since callers read it from `sm.last_committed_opnum()`
which is log-derived); `zero-dep` (kessel-storage stays standalone — no
kessel-sm dep added).

### Decision 3 — Read-set representation: **`BTreeSet<(u32, [u8; 16])>` (set semantics, deterministic iteration)**

Three options:

- **(a) `HashSet<(u32, [u8; 16])>`.** Set semantics; O(1) insert; O(1)
  contains. Cons: hash iteration order is non-deterministic across
  runs (Rust's default hasher randomizes per-process). This would NOT
  be a determinism violation as long as the read-set is only consumed
  set-wise (S2.4 SSI cycle detection is set semantics), but ANY debug
  formatting or test snapshot of `read_set` would have nondeterministic
  ordering — a thesis-fit violation on the `replayable` pillar's
  "debugging IS replay" discipline.

- **(b) `BTreeSet<(u32, [u8; 16])>`.** Set semantics; O(log n) insert
  and contains; **deterministic iteration in sorted order**. The
  ordering is `(type_id, object_id)` lex order — the same order
  `get_at_snapshot` would yield if you scanned all-types-all-objects.
  Composes naturally with the LSM's lex-key shape.

- **(c) `Vec<(u32, [u8; 16])>`.** Insertion-ordered; allows duplicates;
  preserves the temporal read sequence. Cons: SSI (S2.4) needs the SET,
  not the order; the temporal sequence is debug-useful but adds a
  duplicate-de-dup pass when consumed.

**Taken: (b) — `BTreeSet<(u32, [u8; 16])>`.**

**Why bold over safe.** Option (a) `HashSet` is the textbook "set of
keys" data structure and would compile fine, but it violates the
thesis's `replayable` pillar at the debugging layer: a printed `Tx`
state would have non-reproducible ordering, breaking the
`(seed, log) → exact same observable behavior` rule. Option (c) `Vec`
preserves the read sequence but pays a de-dup pass at every SSI cycle
check (S2.4 calls `read_set` repeatedly). Option (b) `BTreeSet` gives
deterministic iteration AND set semantics for free — the
log-derived/log-replayable ordering is automatic. The slight cost of
O(log n) vs O(1) insert is irrelevant at expected read-set sizes
(SQL queries read tens-to-hundreds of keys per Tx, not millions; T4
covers a 1000-read scaling test).

**S2.4 impact.** S2.4's SSI implementation iterates `read_set` to look
up rw-antidependency edges. Sorted iteration order means the dangerous-
cycle detection traverses keys in the same order on every replica —
trivially deterministic. With `HashSet`, S2.4 would need an explicit
sort step before iteration.

**Thesis fit:** `deterministic` (BTreeSet iteration is sorted, hence
replica-byte-identical); `replayable` (debug-formatted Tx state is
deterministic; `(seed, log)` debugging works).

### Decision 4 — Tombstone observability in read-set: **YES — every observed key enters the read-set regardless of variant**

When `Tx::read(t, o)` returns `SnapshotRead::Tombstoned` or
`SnapshotRead::NotYetWritten`, should `(t, o)` enter the read-set?

**Taken: YES, every read enters the read-set regardless of which
SnapshotRead variant is returned.**

**Why.** Conceptually the Tx **observed the absence** of a live
version at the snapshot. If a concurrent transaction installs a new
version of `(t, o)` between the Tx's snapshot_opnum and its commit_opnum,
the observation is invalidated — exactly the situation SSI's rw-
antidependency edge models. The read-set MUST contain the key regardless
of whether the snapshot view was Found, Tombstoned, or NotYetWritten;
otherwise SSI cannot detect cycles involving absence-observations
(the canonical "phantom write" anomaly).

**Honest disclosure.** SQL `SELECT WHERE key = ?` semantics treat
`Tombstoned` and `NotYetWritten` identically as "row not found"; the
client-visible result is the same. But the SSI bookkeeping must
distinguish "the Tx looked at K and saw nothing" (read-set member,
observable to SSI) from "the Tx never asked about K" (not in read-set).
S2.2 ships the conservative discipline: **every `Tx::read` call adds
the key to the read-set**, no exceptions, even if the read result is
`NotYetWritten`.

**Thesis fit:** `verifiable` (the SSI invariant in S2.4's MVCCSsi.tla
relies on this property; the S2.2 MVCCTx.tla invariant
`ReadSetCoversAllReads` enforces it); `honest-docs` (the
"absence-observation IS a read" discipline is documented here, not
silently embedded in the implementation).

### Decision 5 — Tx lifecycle: **struct-with-borrow; commit_read_only consumes self**

Two structural choices:

- **(a) Tx is an opaque handle (Tx ID, u64) and the Storage owns the
  Tx state.** Pros: Tx can outlive a stack frame. Cons: requires
  Tx GC; ID allocation is a new concern; lifetime-correctness checking
  is at runtime not compile-time.

- **(b) Tx is a struct holding `&'a Storage<V>` (or `&'a mut Storage<V>`
  for the S2.3 write variant). The read-set lives inside the Tx struct;
  Tx is dropped (or commit_read_only is called) before the borrow ends.**
  Pros: compile-time-checked Tx lifetime; no Tx GC needed; "read after
  commit" is a borrow-checker error not a runtime error. Cons: Tx is
  tied to a single thread / stack frame (acceptable for S2.2 — no
  cross-thread Tx use case is on the roadmap).

**Taken: (b) — struct holding `&'a Storage<V>`; `commit_read_only`
consumes self.**

**Mechanics.** The S2.2 read-only Tx holds `&'a Storage<V>` (shared
borrow — reads only). `commit_read_only(self) -> Result<(), TxError>`
takes `self` by value, dropping the borrow. `abort(self)` does the
same. Re-reading after commit/abort is a borrow-checker error at
compile time. The read-set is moved out (returnable) on commit_read_only
if S2.4 SSI later needs to persist it (the S2.2 cut returns just
`Result<(), TxError>` — S2.3+ can extend the return type without
breaking S2.2 callers since `Result<(), TxError>` is the conservative
shape).

**S2.3 forward-compat.** When S2.3 introduces the write-Tx, the write
variant will hold `&'a mut Storage<V>` (exclusive borrow). Read-only
Tx and write Tx may end up as two struct types (`TxRead<'a, V>`,
`TxWrite<'a, V>`) or one struct with a phantom-marker. S2.3 will
pick; S2.2 ships only `Tx` (the read-only variant) and the naming is
chosen so the S2.3 split is non-breaking: the public S2.2 type is
`Tx<'a, V>` (the implementation is a read-only Tx; in S2.3 this stays
the read-only path).

**Thesis fit:** `zero-dep` (no Tx-ID allocator added; pure Rust
borrow-checker handles lifecycle); `honest-docs` (the read-only-vs-
write naming convention is named here so S2.3 doesn't accidentally
break compat).

### Decision 6 — API surface (the public Rust signatures)

The S2.2 public surface in `crates/kessel-storage/src/tx.rs`:

```rust
pub struct Tx<'a, V: Vfs> {
    store: &'a Storage<V>,
    snapshot_opnum: u64,
    read_set: BTreeSet<(u32, [u8; 16])>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxError {
    // S2.2 ships zero error variants for commit_read_only because a
    // read-only Tx with no conflict check cannot fail at commit time.
    // S2.3 will introduce ConflictAborted, SnapshotInvalid, etc.
    // The enum is shipped (not Result<(), Infallible>) so S2.3 can
    // extend it without breaking S2.2 callers.
    // T1 ships this with one placeholder variant + #[non_exhaustive]
    // so the type is usable but pattern-matches must use `_`.
}

impl<'a, V: Vfs> Tx<'a, V> {
    /// Begin a Tx pinned at `snapshot_opnum`. Caller supplies the
    /// snapshot — in production (S2.6) the SM will wrap this with
    /// `Tx::begin(&store, sm.last_committed_opnum())`.
    pub fn begin(store: &'a Storage<V>, snapshot_opnum: u64) -> Self;

    /// Snapshot read at the Tx's pinned snapshot_opnum. Records
    /// `(type_id, *object_id)` in the read_set regardless of which
    /// SnapshotRead variant is returned (Decision 4).
    pub fn read(
        &mut self,
        type_id: u32,
        object_id: &[u8; 16],
    ) -> SnapshotRead;

    /// The snapshot_opnum the Tx was pinned at. Never changes after begin.
    pub fn snapshot_opnum(&self) -> u64;

    /// Immutable view of the read_set so far. S2.4 SSI will consume this.
    pub fn read_set(&self) -> &BTreeSet<(u32, [u8; 16])>;

    /// Commit a read-only Tx. Drops the Tx, releasing the borrow on
    /// Storage. Returns Result<(), TxError> for forward-compat with
    /// S2.3 (which adds error variants). In S2.2 this is Ok(())
    /// unconditionally.
    pub fn commit_read_only(self) -> Result<(), TxError>;

    /// Explicit abort. For S2.2 (read-only Tx with no buffered state)
    /// this is identical to dropping the Tx; shipped for symmetry with
    /// the S2.3 write variant which will need explicit abort semantics
    /// to discard the buffered writes.
    pub fn abort(self);
}
```

**Differences vs the brainstorm-prompt API.**
- Read-set uses `BTreeSet` instead of `HashSet` (Decision 3).
- An explicit `abort(self)` method is added for S2.3 forward-compat.
- `TxError` is shipped as an enum (with a `#[non_exhaustive]` marker
  + one placeholder variant) rather than left undefined; the enum exists
  so S2.3 can extend it without breaking S2.2 call sites.

**Module layout.** New file `crates/kessel-storage/src/tx.rs`. Re-exported
from `crates/kessel-storage/src/lib.rs` as `pub mod tx;`. The MVCC
storage primitive (`mvcc::get_at_snapshot`) is called from inside `Tx::read`;
the Tx layer does not duplicate the storage logic.

**Thesis fit:** `deterministic` (every method's behavior is a function
of `(snapshot_opnum, storage state, calls made)` — no hidden state);
`honest-docs` (the `commit_read_only` vs future-`commit` naming is
documented; the `TxError` placeholder is forward-compat not aspirational).

### Decision 7 — TLA+ verification: **`MVCCTx.tla` extends `MVCCStorage.tla` with Tx state**

Per the parent design Decision 7 + S2.1 (SP110) discipline, S2.2 ships
a TLA+ extension modeling the Tx layer.

**File:** `kesseldb-tla/MVCCTx.tla` — EXTENDS `MVCCStorage` so the Tx
invariants are checked over the same versioned-storage model TLC
already verified in S2.1.

**State variables (additions):**
- `txs` : a function from `TxIds` to records `[snapshot |-> Nat,
   read_set |-> SUBSET Keys, status |-> {"Active", "Committed",
   "Aborted"}]`.
- `txCount` : bounded counter to keep the model finite.

**Actions:**
- `TxBegin(tx, s)` — adds a fresh active tx with snapshot=s, empty
  read_set, status=Active.
- `TxRead(tx, k)` — records `k \in read_set` (set union); does NOT
  mutate any other tx state; observes `SnapshotReadOf(k, txs[tx].snapshot)`
  (the read return value is not stored in state in this abstract model;
  the read invariant asserts the value is well-defined).
- `TxCommitReadOnly(tx)` — flips status from Active to Committed.
  In S2.2 (read-only), this action is unconditionally available for
  any Active tx.
- `TxAbort(tx)` — flips status from Active to Aborted.

**Invariants:**
- `TypeOK` — well-typed state.
- `SnapshotImmutability` — for every tx, the snapshot field never
  changes after TxBegin. Captured as an action invariant: every
  enabled action that touches `txs[tx]` preserves `txs[tx].snapshot`.
- `ReadSetMonotonic` — for every tx in status Active or Committed,
  the read_set only grows during the tx's lifetime. (Reset to empty
  is only legal on TxBegin's transition from undefined.)
- `ReadSetCoversAllReads` — every TxRead action commits its key to
  read_set in the next state (the read-set-IS-the-record-of-reads
  invariant, per Decision 4).
- `ReadAtSnapshot` — every TxRead action returns
  `SnapshotReadOf(k, txs[tx].snapshot)` (the snapshot pin is honored
  by every read; no read picks a different snapshot).
- `TxStatusMonotonic` — status transitions are Active → Committed
  or Active → Aborted; no reverse transitions; Committed and
  Aborted are absorbing.

**Bounded model (initial `.cfg`):**
```
SPECIFICATION Spec

CONSTANTS
    Keys      = {k1, k2}        \* (type_id, object_id) pairs in MVCCStorage
    Values    = {v1, v2}
    MaxOpnum  = 3
    MaxOps    = 5
    TxIds     = {t1, t2}        \* 2 concurrent Tx
    MaxTxOps  = 6               \* TxBegin+Read+Read+Commit etc.

INVARIANT
    TypeOK
    SnapshotImmutability
    ReadSetMonotonic
    ReadSetCoversAllReads
    ReadAtSnapshot
    TxStatusMonotonic

CHECK_DEADLOCK FALSE
```

**Coverage target.** Per the SP110 precedent (MVCCStorage TLC ran
1.225M distinct states in 46s to **complete coverage** of its bounded
model), MVCCTx targets the same: complete coverage of the bounded
model, ZERO invariant violations. The state space is bigger than
MVCCStorage's by a factor of ~10–100 (Tx state per Tx × 2 Tx × multiple
actions), so MVCCTx may run 10M–100M distinct states; T6 records the
actual count + runtime. If TLC's coverage runtime exceeds 10 minutes
on the developer's machine, the bounded constants are tightened (e.g.,
MaxTxOps=5) and the smaller-model coverage is the gate; the larger
configuration is run on a beefier machine (per SP109's vulcan
precedent: SP109 ran 528M distinct states on vulcan but a smaller
config locally).

**Thesis fit:** `verifiable` (extends S2.1's TLA+ rigor to the Tx
layer); `honest-docs` (the bounded-model coverage cadence + the
named-correspondence caveat carry forward from SP109/SP110).

### Decision 8 — Backward compatibility: **purely additive; zero legacy-path bytes change**

Per the parent design Decision 8: MVCC ships as a parallel module
through S2.5; SM cutover is S2.6. S2.2 extends this discipline:

- **Zero changes to `kessel-sm`** in S2.2. Tx lives in `kessel-storage::tx`;
  it does not touch the SM apply path.
- **Zero changes to `kessel-sql`** in S2.2. SQL routing through Tx is
  the S2.6 responsibility.
- **Zero new external dependencies** in S2.2. The Tx module uses
  `std::collections::BTreeSet` (already in std). `cargo tree
  -p kesseldb-server | grep -Ei "parquet|objstore|rustls|webpki"` stays
  byte-identical to the SP110 baseline.
- **Zero new public methods on `Storage<V>`** in S2.2. The Tx struct
  holds `&Storage<V>` and calls existing public methods
  (`mvcc::get_at_snapshot`, indirectly via `Storage`-public `scan_range_versions`).
  S2.1 already shipped the substrate Tx needs; no new Storage seam.
- **Every SP1–SP110 path bytes-on-disk-identical.** The Tx layer is
  pure metadata + an in-Tx `BTreeSet` — it writes nothing to disk. The
  legacy single-version 20-byte key path and the S2.1 MVCC 28-byte
  versioned key path both stay byte-net-0 in S2.2.

**The S2.2 gate growth is purely new Tx tests** on the new module. The
existing 513-test cargo gate (SP110 final) plus the new Tx tests becomes
the S2.2 final; T6 records the actual delta. Estimated +20 to +30 tests
(see Sub-slice gate accounting below).

**Thesis fit:** `honest-docs` (the parallel-module discipline holds);
`zero-dep` (no new external crate).

### Decision 9 — Slice numbering: **SP111** (the slice immediately after SP110)

SP111 in the subproject numbering. The S2.2 plan/spec filenames use the
`2026-05-24` date prefix. The internal record (T6 will create it) is:

- Spec/design: this file —
  `docs/superpowers/specs/2026-05-24-mvcc-si-s2-2-design.md`.
- Plan: companion file —
  `docs/superpowers/plans/2026-05-24-mvcc-si-s2-2.md`.
- Slice closeout record (T6 will create it):
  `docs/superpowers/specs/2026-05-24-kesseldb-subproject111-mvcc-tx-s2-2.md`.

Subproject-number / S2-sub-slice cross-reference table after S2.2:

| Subproject | S2 sub-slice | Status | Headline |
|---|---|---|---|
| SP110 | S2.1 | done | MVCC versioned-storage primitive |
| **SP111** | **S2.2** | **this slice** | **Tx context + read-set tracking** |
| SP112+ | S2.3 | pending | Plain SI commit + write-set conflict check |
| ... | S2.4 | pending | SSI promotion |
| ... | S2.5 | pending | GC + watermark |
| ... | S2.6 | pending | SQL + SM cutover |

**Thesis fit:** `honest-docs` (the slice numbering + cross-reference
makes the strategic-tier trajectory inspectable from any single record).

---

## Architecture

### High-level layering after S2.2

```
                  +---------------------------+
                  |  kessel-sql (unchanged)   |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-sm (unchanged)    |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage::tx       |   <-- S2.2 (this slice)
                  |  Tx { snapshot, read_set} |
                  |  - begin / read / commit  |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage::mvcc     |   <-- S2.1 (SP110)
                  |  get_at_snapshot, ...     |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage LSM       |   (existing, unchanged)
                  +---------------------------+
```

The Tx layer sits between any future caller (S2.6 SM-integration code)
and the MVCC storage primitive. The Tx layer is **stateless on disk** —
its entire state (snapshot_opnum, read_set) lives in the `Tx` struct
in memory.

### Module layout

New file: `crates/kessel-storage/src/tx.rs`.

Updated file: `crates/kessel-storage/src/lib.rs` — adds one line:
`pub mod tx;` (between `pub mod mvcc;` and the first `pub fn`).

No other crate changes in S2.2.

### Internal data shape

`Tx<'a, V: Vfs>` holds three fields:
- `store: &'a Storage<V>` — shared borrow of the storage layer; reads only.
- `snapshot_opnum: u64` — pinned at `begin`, never mutated.
- `read_set: BTreeSet<(u32, [u8; 16])>` — accumulates `(type_id, *object_id)`
   on every `read` call; iteration order is deterministic (sorted lex).

The `Tx` struct is `!Send + !Sync` by default (it holds an `&Storage`,
and `Storage<V: Vfs>` is `!Send` when `V` is `!Send`). This is
intentional — Tx is single-thread by construction in S2.2.

### Call graph

```
Tx::begin(&store, s)
    -> constructs Tx { store: &store, snapshot_opnum: s, read_set: BTreeSet::new() }

Tx::read(t, o)
    -> calls mvcc::get_at_snapshot(self.store, t, o, self.snapshot_opnum)
    -> calls self.read_set.insert((t, *o))     // regardless of returned variant
    -> returns the SnapshotRead

Tx::commit_read_only(self)
    -> drops self (releasing the borrow on Storage)
    -> returns Ok(())                          // S2.2 has no failure mode

Tx::abort(self)
    -> drops self (releasing the borrow on Storage)
    -> returns ()                              // identical to commit_read_only for S2.2
```

### MVCCTx.tla extension (the verifiable artifact)

`kesseldb-tla/MVCCTx.tla` EXTENDS `MVCCStorage` to model the Tx layer
over the verified versioned-storage primitive. See Decision 7 above for
state vars, actions, and invariants. The `.cfg` ships at the bounded
config in Decision 7. The baseline TLC run lands in
`kesseldb-tla/results/2026-05-24-mvcc-tx-baseline.txt` with the same
discipline as SP110's `2026-05-24-mvcc-storage-baseline.txt`: complete
coverage of the bounded config, zero invariant violations, honest
disclosure of state-count + runtime + bounded-config caveat.

---

## The Tx contract (formal)

The contract S2.2 establishes — referenced by S2.3 + S2.4 + S2.6.

### Snapshot pinning

For every `Tx` `T`:
```
T.snapshot_opnum is fixed at T = Tx::begin(_, s);
                  it does NOT change for the lifetime of T.
```

### Read-at-snapshot

For every `Tx::read(t, o)` call on a Tx `T`:
```
T.read(t, o) = mvcc::get_at_snapshot(T.store, t, o, T.snapshot_opnum)
```
(byte-identical to the storage primitive's read result).

### Read-set coverage

For every `Tx::read(t, o)` call on a Tx `T`:
```
After the call: (t, *o) \in T.read_set
                regardless of which SnapshotRead variant was returned.
```

### Read-set monotonicity

For every Tx `T` and any two points in T's lifetime `p1 < p2`:
```
T.read_set@p1  \subseteq  T.read_set@p2
```
The read_set never shrinks; it only grows on `read` calls.

### Commit no-op (S2.2-only)

For every Tx `T` in S2.2:
```
T.commit_read_only() = Ok(())  unconditionally.
T.abort() = ()                  unconditionally.
```
The semantics in S2.3+ will extend `commit` (the write-Tx variant)
with conflict-check failure modes; the read-only `commit_read_only`
shape stays trivial.

### Determinism

For two Tx invocations `T1`, `T2` on byte-identical `Storage` states
with byte-identical `snapshot_opnum` and the same sequence of `read`
calls:
```
T1's read results == T2's read results
T1.read_set     == T2.read_set
```
This is trivially true by composition of the S2.1 MVCC determinism
contract + the BTreeSet's deterministic-iteration property.

---

## Sub-slice gate accounting (estimated)

Total cargo gate growth in S2.2: estimated **+20 to +30 tests** on
the new `tx` module. Breakdown:

| Task | Expected tests | Cumulative | Notes |
|---|---|---|---|
| T0 baseline | 0 | 513 | SP110 final, expect FAILED=0 + seed-7 green |
| T1 scaffold | +2 | 515 | Type-shape locks for Tx + TxError |
| T2 implementation + KATs | +7 to +9 | 522 to 524 | begin / snapshot_opnum_pin / read+read_set / read_duplicate / read_tombstone_in_read_set / read_never_written_in_read_set / read_set_sorted_iteration / commit_read_only_ok / abort_ok |
| T3 integration tests | +3 to +4 | 525 to 528 | snapshot-pin survives concurrent puts / multi-tx-same-snapshot byte-identity / read-set ≥ size after N distinct reads / tombstone-read in-read-set |
| T4 coverage tests | +4 to +5 | 529 to 533 | re-read same key (no dup in read_set) / read-after-commit-is-compile-error doc-test / large-read-set scaling (1000 reads) / tx-with-zero-reads / read-set-clone-equivalence |
| T5 pentest | +5 to +7 | 534 to 540 | snapshot_opnum=0 / u64::MAX / snapshot > high_op / giant read-set (no OOM at 100k reads) / 2-tx-same-snapshot byte-identical results / read after Storage drop (compile-blocked) / read_set field private (compile-blocked) |
| T6 docs + TLA+ | 0 Rust | 540 | MVCCTx.tla + .cfg + TLC baseline + SP111 record + STATUS + memory |

**Estimated final cargo gate after S2.2:** 533 to 540 tests
(`FAILED=0`, seed-7 green). The actual number lands in T6.

The TLA+ artifact's gate is the TLC baseline run (zero invariant
violations on the bounded config) + the artifact files committed to
`kesseldb-tla/`.

---

## Sub-slice decomposition reminder (S2.3–S2.6 still pending)

S2.2 ships ONLY the Tx context + read-set tracking. The following are
explicitly **OUT of scope for S2.2** and tracked in the parent S2
design's sub-slice decomposition:

- **S2.3** — Write side of the Tx (`Tx::write`, the buffered-writes API)
  + `Op::CommitTxn` SM apply path + plain SI write-write conflict
  check (`mvcc::has_version_in_range` is already shipped from S2.1).
- **S2.4** — SSI promotion: the dangerous-cycle detector that consumes
  S2.2's `read_set`. The slice that promotes plain SI to true
  serializability.
- **S2.5** — GC + watermark (`Op::AdvanceWatermark`).
- **S2.6** — SQL surface integration + SM cutover (the byte-identity-
  gate-change slice, honest-disclosed there).

Each subsequent sub-slice will get its own plan when the prior one
lands. S2.3 plan is the next docs slice expected after this S2.2
slice ships.

---

## Honest deferred set

Items explicitly out of scope for S2.2, named here so the S2.2 record
can't drift into over-claim territory:

- **Write side of Tx.** Deferred to S2.3 per Decision 1.
- **Commit-time conflict detection.** Deferred to S2.3 (plain SI) and
  S2.4 (SSI promotion).
- **SSI dangerous-cycle detection.** Deferred to S2.4. S2.2 ships the
  `read_set` substrate ONLY.
- **GC + watermark.** Deferred to S2.5.
- **SM-side `last_committed_opnum()` helper.** Deferred to S2.6 per
  Decision 2.
- **SQL integration.** Deferred to S2.6.
- **Cross-thread Tx, Tx-pool, Tx ID allocation.** Not on the S2 roadmap.
  Tx is single-thread / stack-frame-bound by construction per Decision 5.
- **Multi-replica Tx TLA+ model.** Same scope decision as SP110's
  MVCCStorage.tla: the S2.2 MVCCTx.tla models a single replica's Tx
  state; multi-replica byte-identity is verified at the Rust level
  (T3). Multi-replica TLA+ is an S2.X follow-up (would need per-replica
  `txs[r][tx]` shape).
- **Bounded-TLC-config caveats.** Same disclosure as SP110: the TLA+
  artifact proves the absence of counterexamples at the configured
  bounds only; larger bounds are an S2.X follow-up. The Rust pentest
  tests cover boundary opnums (0, u64::MAX) explicitly.
- **TLA+-mechanized-refinement TLA+ ↔ Rust.** Same gap S1/SP109 +
  SP110 disclosed. Per-sub-slice named-action correspondence; not a
  refinement proof.

---

## Thesis-fit note

**Thesis fit:** `deterministic` (Tx snapshot pin + BTreeSet-deterministic-
iteration mean two Tx with the same inputs produce byte-identical
read-sets and read results; the thesis-fit-pattern phrase **"a Tx is a
deterministic function of (snapshot_opnum, storage_state, sequence of
reads)"** is the S2.2 thesis claim);
`replayable` (every Tx is replayable from `(seed, log, snapshot_opnum,
read sequence)` — debugging IS replay; the BTreeSet's deterministic
iteration is what makes Tx-state-formatting reproducible);
`verifiable` (MVCCTx.tla extends SP110's MVCCStorage.tla with the Tx
state vars + actions + 6 invariants — mechanically-checked by TLC
against the same VSR-log substrate S1/SP109 verified);
`honest-docs` (the parent-design-strawman revision in Decision 1
documented here, not silently deviated; the SSI-bookkeeping rationale
for `read_set` in S2.2 documented here so S2.4 can consume it without
re-justifying; the bounded-TLC-config + named-correspondence + dormant-
module caveats carried forward from SP110 verbatim).

---

## Internal record

This design document is
`docs/superpowers/specs/2026-05-24-mvcc-si-s2-2-design.md`.

The S2.2 implementation plan is
`docs/superpowers/plans/2026-05-24-mvcc-si-s2-2.md`.

When S2.2 ships, its slice-record file will be
`docs/superpowers/specs/2026-05-24-kesseldb-subproject111-mvcc-tx-s2-2.md`
(SP111 in the subproject numbering; mirrors the SP108/SP109/SP110
slice-record convention).

S2.3–S2.6 plans will be added under `docs/superpowers/plans/` as each
prior sub-slice's record lands. Each sub-slice cross-references the
parent S2 design and this S2.2 design.
