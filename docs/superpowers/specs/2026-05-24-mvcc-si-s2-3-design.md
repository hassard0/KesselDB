# S2.3 — SI Write-Side + Conflict Detection at SM Apply Time: Design

**Date:** 2026-05-24
**Status:** Approved (autonomous mandate — see Process Note)
**Strategic-tier item:** S2.3 sub-slice of S2 (Serializable MVCC / Snapshot
Isolation) in the THESIS.md S1–S4 backlog. The **third** built sub-slice of
S2 after S2.1/SP110 (MVCC versioned-storage primitive) and S2.2/SP111
(Tx context + read-set). **SP112** in the subproject numbering.
**Builds on:**
- Project THESIS — `docs/THESIS.md`.
- S2 parent design — `docs/superpowers/specs/2026-05-23-mvcc-si-design.md`.
- S2.1 record (SP110) — `docs/superpowers/specs/2026-05-23-kesseldb-subproject110-mvcc-s2-1.md`.
- S2.2 record (SP111) — `docs/superpowers/specs/2026-05-24-kesseldb-subproject111-mvcc-tx-s2-2.md`.
- S2.2 design — `docs/superpowers/specs/2026-05-24-mvcc-si-s2-2-design.md`.
- S2.2 TLA+ artifact — `kesseldb-tla/MVCCTx.tla` + `.cfg` + baseline TLC
  run (`kesseldb-tla/results/2026-05-24-mvcc-tx-baseline.txt`).
- The MVCC module surface shipped in SP110: `crates/kessel-storage/src/mvcc.rs`
  (`make_versioned_key`, `decode_commit_opnum`, `put_versioned`,
  `get_at_snapshot`, `has_version_in_range`, `SnapshotRead { Found |
  Tombstoned | NotYetWritten }`, `MvccKeyError`). **`has_version_in_range`
  was shipped in SP110 specifically for this slice's conflict check —
  see SP110 design Decision 7 for that forward-link.**
- The Tx module shipped in SP111: `crates/kessel-storage/src/tx.rs`
  (`Tx<'a, V>`, `TxError` with `#[non_exhaustive]`, six public methods).

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
T6, exactly as SP110 + SP111 did.

---

## Strategic-tier framing

S2.3 is the **third sub-slice of S2** in the THESIS.md backlog. SP112
in the subproject numbering. The parent S2 design
(`docs/superpowers/specs/2026-05-23-mvcc-si-design.md`) decomposes S2
into 6 sub-slices: S2.1 (SP110 — versioned-storage primitive) →
S2.2 (SP111 — Tx context + read-set) → **S2.3 (this slice — SI
write-side + conflict detection at SM apply time)** → S2.4 (SSI
promotion) → S2.5 (GC + watermark) → S2.6 (SQL integration + SM
cutover). This slice ships the **thesis-fit centerpiece of S2**: the
write-side Tx API (`Tx::write`, the buffered-writes overlay on
`Tx::read`, `Tx::commit`) AND the deterministic conflict-check at SM
apply time. Per the parent S2 design's Decision 4 — "deterministic
apply IS the conflict resolver, no distributed coordination needed."

**S2.1 → S2.2 → S2.3 dependency chain.** S2.1 shipped the storage
primitive (`get_at_snapshot`, `has_version_in_range`) that answers
"what version of K was committed at-or-before opnum S?" and "did any
version of K commit in the half-open opnum window (lo_excl, hi_incl]?"
S2.2 shipped the Tx context that pins the snapshot opnum at begin-time
and accumulates a read-set. **S2.3 closes the loop**: it adds the
write-side to the Tx, defines the SM-level commit op shape, and uses
`mvcc::has_version_in_range` — the primitive SP110 shipped *specifically*
for this slice — to implement the deterministic write-write conflict
check. After S2.3 ships, the next slice (S2.4) layers SSI's dangerous-
cycle detection on top of S2.2's `read_set`; the slice after that
(S2.5) adds GC; the final slice (S2.6) wires SQL and SM-cutover.

**The thesis-fit headline.** Per the parent S2 design's Decision 4:

> In a deterministic state machine fed by a totally-ordered log,
> conflict detection is a function of the log prefix. Every replica
> sees the same log in the same order; every replica runs the same
> `apply(op_number, Op::CommitTx { snapshot_opnum, write_set })` and
> reaches the same conflict verdict. **No distributed conflict-
> resolution coordination is required.** Compare to non-deterministic
> systems (Spanner: TrueTime + Paxos per shard; CockroachDB: HLC + the
> "txn record" coordination protocol) — KesselDB sidesteps this entire
> class of complexity because the log already orders the conflict
> checks.

S2.3 is the slice that **operationalizes** that claim: the
`Op::CommitTx` variant + its SM apply path are the
log-derived-deterministic-conflict-resolver in code. The TLA+ artifact
`MVCCSi.tla` mechanically checks that the conflict verdict is a
function of the log prefix only (no time, no per-replica randomness,
no scheduler-dependent ordering).

---

## Problem

After S2.2 ships, KesselDB has:
- A snapshot-pinned read primitive (`mvcc::get_at_snapshot`) that
  serves point-in-time reads deterministically.
- A Tx context (`kessel_storage::tx::Tx`) that pins the snapshot opnum
  at begin-time and accumulates a read-set on every `read` call.
- A conflict-check primitive (`mvcc::has_version_in_range`) — shipped
  in SP110 specifically for this slice; not yet called by any consumer.

What's still missing for plain SI:
1. **A write API on Tx.** Today, `Tx` is read-only. There is no way to
   stage a buffered write that becomes visible to subsequent
   `Tx::read` calls (read-your-writes) and that commits atomically with
   conflict-check at commit time.
2. **A state-machine-level commit op.** Today, there is no `Op` variant
   that carries `(snapshot_opnum, write_set)` through the VSR log so
   the SM's deterministic apply can run the conflict check. The conflict
   check MUST happen at SM apply time (not at Tx-side commit time),
   because that's the only point in the system at which every replica
   sees the same input (the totally-ordered log prefix). Running the
   check at Tx-side would be per-replica and could diverge — the very
   thing the deterministic-replicated thesis disallows.
3. **An MVCCSi TLA+ specification.** Per the parent S2 design Decision
   7, every MVCC sub-slice ships its own TLA+ extension. S2.3's spec
   must formalize the FirstCommitterWins + DeterministicApply
   invariants and let TLC mechanically prove their absence-of-
   counterexamples at the bounded model.

S2.3 is the slice that solves all three.

---

## Decisions (bold choices, documented)

### Decision 1 — Write API surface: **single `Tx` with `write(type_id, object_id, value: Option<Vec<u8>>)`; commit may now Err**

Two structural options:

- **(a) Extend the existing `Tx` to support writes** — add
  `write(type_id, object_id, value: Option<Vec<u8>>)` that buffers
  into a `write_set` field; `commit(commit_opnum: u64) ->
  Result<TxCommitOutcome, TxError>` that runs the conflict check and
  either installs the writes or aborts. The S2.2 `commit_read_only`
  shape stays for SELECT-only Tx (forward-compatible).
- **(b) Two distinct types** — keep the existing `Tx` as read-only;
  introduce a separate `RwTx` with `write` + `commit`. Pros: type-level
  distinction between read-only and read-write; harder to misuse. Cons:
  forces the SQL caller in S2.6 to pick the right type at SQL-statement
  start time (which may not yet know whether the statement is RO); two
  parallel implementations of `read`; doubles the API surface to test.

**Taken: (a) — single `Tx` with `write` added; commit may now Err.**

**Why bold over safe.** Option (b) cleanly separates the two modes, but
it forces callers to pre-classify every statement and doubles the
public surface — and it would force S2.6 (SQL integration) to thread
type-level read-only-ness through the SQL planner, which is gratuitous
complexity for the same observable behavior. Option (a) keeps a single
`Tx` type whose `commit_read_only` is the no-conflict-check SELECT
path AND whose new `commit(commit_opnum)` is the conflict-checked
read-write path. A read-only Tx whose `write_set` is empty COULD
commit through `commit(_)` without conflict (the conflict check is a
no-op on an empty write_set) — but `commit_read_only` is kept as the
ergonomic, never-aborts shape for SELECT-only callers.

**The S2.2 contract is preserved verbatim.** Every S2.2 method
(`begin`, `read`, `snapshot_opnum`, `read_set`, `commit_read_only`,
`abort`) keeps its signature. S2.3 ADDS:
- `write(&mut self, type_id: u32, object_id: &[u8; 16], value:
  Option<Vec<u8>>)` — the buffered-write API (Decision 2 covers the
  buffer shape).
- `write_set(&self) -> &BTreeMap<(u32, [u8; 16]), Option<Vec<u8>>>`
  — immutable view of the buffered writes (mirrors the existing
  `read_set()` accessor).
- `commit(self, commit_opnum: u64) -> Result<TxCommitOutcome, TxError>`
  — the conflict-checked commit (Decision 6 covers the outcome shape).

**Thesis fit:** `deterministic` (single Tx type means the commit-path
semantics are uniform and log-derived); `replayable` (the write_set is
deterministic-iteration BTreeMap; same `(seed, log, Tx ops)` tuple
produces byte-identical Tx state); `honest-docs` (the parent-S2-strawman
"separate read+write types" path is explicitly rejected here, not
silently revised).

### Decision 2 — Buffer shape: **`BTreeMap<(u32, [u8; 16]), Option<Vec<u8>>>` (overlay; tombstones explicit)**

Three options:

- **(a) `Vec<(u32, [u8; 16], Option<Vec<u8>>)>`** — insertion-ordered
  list of writes; same-key writes coalesce ONLY at commit time. Cons:
  read-your-writes overlay (Decision 3) requires a linear scan to find
  the most-recent buffered write for a key; same-key duplicates inflate
  the commit op payload.
- **(b) `HashMap<(u32, [u8; 16]), Option<Vec<u8>>>`** — map semantics;
  O(1) overlay lookup. Cons: non-deterministic iteration order ⇒
  the SM-side serialized commit op's `write_set` field would have
  different byte representations across replicas (broken determinism)
  unless an explicit sort step is added at commit time.
- **(c) `BTreeMap<(u32, [u8; 16]), Option<Vec<u8>>>`** — map semantics
  with deterministic iteration (sorted lex by `(type_id, object_id)`).
  Same-key writes coalesce immediately on `Tx::write` (last-write-wins
  *within* a single Tx). The serialized commit op's `write_set` is
  byte-identical across replicas by construction.

**Taken: (c) — `BTreeMap<(u32, [u8; 16]), Option<Vec<u8>>>`.**

**Why bold over safe.** Option (b) HashMap is the textbook "key → value"
shape but reintroduces non-determinism at the serialization layer.
Option (a) Vec preserves intent-ordering but pays a linear scan on
every read overlay. Option (c) BTreeMap matches the SP111 read-set's
`BTreeSet` discipline: deterministic iteration is the determinism-by-
construction shape KesselDB uses everywhere. The `Option<Vec<u8>>`
value carries tombstones (`None`) explicitly — a tombstone is a write
of "this key is deleted at commit_opnum", consistent with the S2.1
MVCC primitive's `put_versioned(..., None)` shape.

**S2.4 forward-compat.** SSI in S2.4 reads `write_set` to construct
rw-antidependency edges. Sorted iteration order means the dangerous-
cycle detection traverses the write_set in the same order on every
replica — trivially deterministic, no explicit sort step needed.

**Thesis fit:** `deterministic` (sorted iteration ⇒ byte-identical
serialization across replicas); `replayable` (debug-formatted Tx
state, including write_set, is replica-byte-identical).

### Decision 3 — Read-your-writes: **YES — `Tx::read` checks write_set first; buffered tombstones are honored**

If `Tx::write(t, o, Some(v))` was called earlier in the same Tx, a
subsequent `Tx::read(t, o)` MUST return that buffered value, not the
snapshot value. Mainstream DB API contract.

**Mechanics.** Modify `Tx::read` to consult the write_set BEFORE
falling through to `mvcc::get_at_snapshot`:

```rust
pub fn read(&mut self, type_id: u32, object_id: &[u8; 16]) -> SnapshotRead {
    // Read-your-writes overlay (Decision 3, S2.3).
    if let Some(buffered) = self.write_set.get(&(type_id, *object_id)) {
        // Record in read_set per the S2.2 Decision 4 discipline (the
        // read DID observe the key; SSI tracks it).
        self.read_set.insert((type_id, *object_id));
        return match buffered {
            Some(v) => SnapshotRead::Found(v.clone()),
            None    => SnapshotRead::Tombstoned,    // buffered tombstone
        };
    }
    // S2.2 path unchanged.
    self.read_set.insert((type_id, *object_id));
    crate::mvcc::get_at_snapshot(self.store, type_id, object_id, self.snapshot_opnum)
}
```

**Honest disclosure.** Read-your-writes is per-Tx semantic only — a
SECOND Tx pinned at the same snapshot does NOT see the first Tx's
buffered writes until that first Tx commits (and even then, only after
the second Tx's snapshot opnum advances past the commit_opnum, which
by snapshot pinning will never happen for the second Tx — the second
Tx sees a frozen pre-commit world). This matches SI semantics exactly.

**Read-set coverage.** The buffered-read path STILL inserts
`(type_id, *object_id)` into the read_set — the S2.2 Decision 4
discipline is preserved (every key the Tx observed, by any path,
enters the read_set; SSI consumes both buffered-read observations and
snapshot-read observations).

**Thesis fit:** `deterministic` (read-your-writes is a pure function
of write_set state, which is a deterministic BTreeMap); `honest-docs`
(the per-Tx-only semantic + the read-set-still-records discipline are
documented here, not silently embedded).

### Decision 4 — Conflict detection happens AT SM APPLY TIME, not at Tx-side commit time (the thesis-fit headline)

The parent S2 design Decision 4 is the foundational claim of S2; S2.3
operationalizes it. Two structural options:

- **(a) Tx-side commit-time conflict check.** `Tx::commit` runs the
  `has_version_in_range` check directly against the local Storage,
  returns `Committed`/`Aborted`, and (on Committed) installs the
  writes via `put_versioned` BEFORE returning. The SM then logs an
  `Op::CommitTx` (or a sequence of `Op::Put`s) for replication.
  **Rejected as a thesis violation** — the conflict verdict is now a
  function of the LOCAL Storage state, which may differ across replicas
  if the SM apply cursor is at different positions. Two replicas could
  reach different verdicts for the same Tx, breaking the
  deterministic-replicated thesis.

- **(b) SM-apply-time conflict check.** Tx::commit DOES NOT run the
  conflict check. It constructs an `Op::CommitTx { snapshot_opnum,
  write_set, commit_opnum }` payload (Decision 5) and returns it to
  the caller. The caller submits the op to VSR; VSR appends it to the
  totally-ordered log; the SM's deterministic `apply(op_number, op)`
  runs the conflict check on the **log-derived** versioned storage
  state and either installs the writes or aborts the Tx. The verdict
  is byte-identical on every replica by construction (deterministic
  apply over the same log prefix).

**Taken: (b) — SM-apply-time conflict check. The thesis-fit headline.**

**Why bold over safe.** Option (a) "Tx-side conflict check then log"
is the textbook implementation in non-replicated databases (PostgreSQL
single-node; SQLite). In a replicated context it is **wrong** — the
conflict verdict must be derivable from the log prefix alone, never
from the local Storage state, or the system fails determinism.

The headline implication: **KesselDB does not need distributed
conflict-resolution coordination protocols.** Spanner's TrueTime +
Paxos-per-shard, CockroachDB's HLC + txn-record-coordination — both
exist to give non-deterministic replicated systems a way to agree on
commit ordering. KesselDB's VSR log already orders every commit op;
the SM's deterministic apply already agrees on the verdict; the
distributed-coordination layer is **structurally absent**. This is
what THESIS.md S2 means by "consensus + SQL can be simpler than MVCC-
centric systems."

**Honest disclosure (carried forward from parent design Decision 4).**
This works **only because the snapshot_opnum the client used IS itself
committed by the time the commit op applies** (the client sends the
snapshot_opnum as part of the commit payload; every replica sees that
value as part of the log entry; the range-scan check
`(snapshot_opnum, commit_opnum-1]` is a deterministic function of the
log prefix). If a replica receives the commit op before its locally-
applied opnum reaches `snapshot_opnum`, the apply **stalls until the
replica's apply cursor reaches `snapshot_opnum`**. This is the natural
"wait for the prefix you depend on" pattern, and it terminates because
VSR delivers entries in commit order.

In S2.3, the SM apply path does NOT model the cursor-stall behavior
(no production SM caller integrates with Tx until S2.6). The S2.3 SM
apply path assumes the snapshot_opnum is <= current_opnum (the natural
pre-condition); a malformed commit op with snapshot_opnum >
current_opnum is treated as a conflict (conservative: abort). The
cursor-stall semantics ship in S2.6 when the SM caller integration
lands.

**Thesis fit:** `deterministic` (the conflict verdict is a function
of the log prefix only; structurally cannot diverge across replicas);
`verifiable` (the `MVCCSi.tla` extension models this directly via
the `CommitTx(t)` action being enabled only by the SM-side conflict
check); `replayable` (a Tx's outcome is replayable from its
`(snapshot_opnum, write_set, commit_opnum)` tuple + the log prefix);
`honest-docs` (the cursor-stall deferral to S2.6 is explicitly
documented).

### Decision 5 — Commit op shape: **`Op::CommitTx { snapshot_opnum, write_set, commit_opnum }`**

The state-machine-level commit op. Wire shape:

```rust
// New variant on the existing kessel-proto Op enum.
CommitTx {
    snapshot_opnum: u64,
    /// Sorted by (type_id, object_id) at construction time (Decision 2's
    /// BTreeMap iteration order). Each entry is one buffered write or
    /// tombstone. Empty write_set => trivial commit (no-op apply).
    write_set: Vec<(u32, [u8; 16], Option<Vec<u8>>)>,
    /// Caller-supplied. In S2.3, tests pass literal opnums. In S2.6 the
    /// SM caller wraps Tx::commit with sm.next_op_number() so the SM
    /// assigns this from the VSR log position. Parent design Decision 4
    /// honest-disclosure carried forward.
    commit_opnum: u64,
}
```

**SM apply pseudocode** (the headline thesis-fit implementation):

```rust
Op::CommitTx { snapshot_opnum, write_set, commit_opnum } => {
    // Pre-check: snapshot_opnum sanity.
    // Conservative bound: snapshot_opnum must be <= commit_opnum (a
    // snapshot from the future is a hostile / malformed op; abort).
    if snapshot_opnum > commit_opnum {
        return OpResult::TxAborted { reason: AbortReason::SnapshotOutOfRange };
    }
    // Conflict check: for every key in write_set, scan versions in the
    // half-open window (snapshot_opnum, commit_opnum - 1]. If ANY
    // version exists for ANY key in that window, the snapshot has been
    // invalidated by an intervening commit => first-committer-wins =>
    // abort this Tx.
    //
    // The hi bound is commit_opnum - 1 (not commit_opnum) because the
    // Tx's own writes will install at commit_opnum; they must not
    // conflict with themselves. The lo bound is snapshot_opnum
    // EXCLUSIVE (the snapshot version itself was visible to the Tx
    // and is not a conflict; only LATER versions invalidate the
    // snapshot).
    //
    // Edge case: commit_opnum == 0. Then commit_opnum - 1 wraps to
    // u64::MAX. We handle this explicitly: a Tx at commit_opnum=0
    // cannot conflict with anything (no prior versions could exist).
    // Skip the check.
    if commit_opnum > 0 {
        let hi = commit_opnum - 1;
        for (type_id, object_id, _new_value) in &write_set {
            if mvcc::has_version_in_range(&self.storage, *type_id, object_id, snapshot_opnum, hi) {
                return OpResult::TxAborted {
                    reason: AbortReason::WriteWriteConflict {
                        type_id: *type_id, object_id: *object_id,
                    },
                };
            }
        }
    }
    // No conflict: install every write at commit_opnum.
    for (type_id, object_id, value) in write_set {
        mvcc::put_versioned(&mut self.storage, type_id, &object_id, commit_opnum, value)?;
    }
    OpResult::TxCommitted { commit_opnum }
}
```

**Why this exact shape.** The `(snapshot_opnum, write_set, commit_opnum)`
triple is the minimum sufficient state for the SM-apply-time conflict
check. The check itself is two `mvcc::has_version_in_range` semantics
(half-open interval `(lo_excl, hi_incl]`) applied per write_set key —
exactly the primitive shipped in SP110 for this purpose. The write_set
is `Vec<...>` on the wire (not BTreeMap — BTreeMap doesn't have a
stable serialization order in `kessel-proto`'s codec) but is
constructed from the BTreeMap's sorted iteration so the wire bytes are
deterministic.

**Op variant placement.** Append `CommitTx` as the next variant in
`kessel-proto::Op`. Append-only Op variant addition matches the
existing project discipline (the `Txn { ops }` variant is the only
existing transaction-shaped op; `CommitTx` is the MVCC-aware sibling).
S2.3 wires the apply path in `kessel-sm::StateMachine::apply`; the
existing legacy `Op::Txn` path remains untouched and continues to
serve the SP9 atomic-batch semantics.

**No SM-caller integration in S2.3.** No `kessel-sm` or `kessel-sql`
caller submits `Op::CommitTx` to VSR in S2.3. The op is exercised via
direct `StateMachine::apply` calls in integration tests (T3) and via
construction-only tests of `Tx::commit`. S2.6 will wire the production
caller path.

**Thesis fit:** `deterministic` (the apply path is byte-identical
across replicas by construction); `verifiable` (the `MVCCSi.tla`
extension models the `CommitTx(t)` action with the same precondition);
`zero-dep` (no new external crate; the op uses existing kessel-proto
primitives only).

### Decision 6 — Commit API surface: **`Tx::commit(self, commit_opnum: u64) -> Result<TxCommitOutcome, TxError>`**

The Rust API S2.3 ships on `Tx`:

```rust
/// Outcome of a conflict-checked commit. `Committed` carries the
/// commit_opnum (echoed back from the caller's supplied value for
/// audit). `Aborted` carries the (type_id, object_id) of the FIRST
/// conflicting key encountered — for debugging + observability.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TxCommitOutcome {
    Committed { commit_opnum: u64 },
    Aborted { conflicting_key: (u32, [u8; 16]) },
}

/// Conflict-checked commit (S2.3). Caller supplies the commit_opnum
/// (matches the S2.1/S2.2 decoupling decision; S2.6 wires SM to
/// provide it from sm.next_op_number()).
///
/// IMPORTANT: the conflict check this function performs is the
/// SAME deterministic check that runs at SM apply time. In production
/// (S2.6), the function will NOT run the conflict check locally —
/// it will construct an `Op::CommitTx` payload and submit it to VSR,
/// and the verdict will arrive back via the SM apply callback. The
/// S2.3 standalone form runs the check locally for testability +
/// the dormant-module discipline (no SM caller integration yet).
///
/// Empty write_set => Ok(Committed { commit_opnum }) (no-op commit).
/// Hostile snapshot_opnum > commit_opnum => Err(TxError::SnapshotOutOfRange).
pub fn commit(self, commit_opnum: u64) -> Result<TxCommitOutcome, TxError>;
```

**`TxError` extension.** S2.2 shipped `TxError` as `#[non_exhaustive]`
with a `_Reserved` placeholder. S2.3 adds:
```rust
#[non_exhaustive]
pub enum TxError {
    #[doc(hidden)]
    _Reserved,                                  // S2.2 placeholder
    SnapshotOutOfRange { snapshot: u64, commit: u64 },  // S2.3
    StorageIo(std::io::Error),                  // S2.3 — put_versioned errors
}
```
The `WriteWriteConflict` failure mode is NOT a `TxError` — it surfaces
as `TxCommitOutcome::Aborted { conflicting_key }` because an SI conflict
is a normal/expected outcome (the Tx must retry with a fresher
snapshot), not an error. `TxError` is reserved for malformed input
and infrastructure failures.

**Why bold over safe.** Distinguishing "conflict-aborted-retry-me"
(`Ok(Aborted)`) from "malformed-input-fix-your-code" (`Err(TxError)`)
makes the caller's retry-loop trivial: `match outcome { Ok(Committed)
=> done; Ok(Aborted) => retry-with-fresh-snapshot; Err(_) => bubble
up }`. Mixing the two into a single `Err` shape would force every
caller to discriminate via string-matching or variant-typing — a
footgun.

**Backward compatibility.** Every S2.2 method signature is unchanged.
The S2.2 `commit_read_only` shape stays. The S2.2 `TxError` enum
shape stays (with `#[non_exhaustive]`, adding variants is non-breaking).
S2.3 is purely additive at the kessel-storage::tx surface — every
SP1–SP111 caller continues to compile + run with byte-identical
behavior. The `Op::CommitTx` variant is new on `kessel-proto::Op`;
the existing op-apply paths are untouched; no legacy op's semantics
change.

**Thesis fit:** `deterministic` (the outcome is a function of the
inputs alone; no hidden state); `honest-docs` (the
`TxError`-vs-`TxCommitOutcome::Aborted` distinction is documented here,
not embedded silently); `replayable` (the
`(commit_opnum, write_set, snapshot_opnum)` triple is the replay key).

### Decision 7 — TLA+ verification: **`MVCCSi.tla` extends `MVCCTx` with write_set + CommitTx**

Per the parent design Decision 7 + SP110/SP111 discipline, S2.3 ships
a TLA+ extension. The spec EXTENDS `MVCCTx` (the SP111 spec) so the SI
layer is checked over the same versioned-storage + Tx model TLC has
already verified.

**File:** `kesseldb-tla/MVCCSi.tla` — EXTENDS `MVCCTx`.

**State variable additions:**
- `txs[t].write_set` — extend the SP111 `TxRecord` shape:
  `[snapshot, read_set, write_set, status]`. `write_set` is a
  TLA+ function from `Keys` to `Values \cup {Tombstone}`; domain restricted
  to the keys this Tx wrote.

**Actions (additions over SP111's TxBegin / TxRead / TxCommitReadOnly /
TxAbort):**
- `TxWrite(t, k, v)` — record a buffered write/tombstone of key k with
  value v in `txs[t].write_set` (function-update; same-key overwrites).
- `CommitTx(t, c)` — the SI conflict-checked commit at commit_opnum c.
  Precondition: `txs[t].status = "Active"` AND `c \in OpNums` AND
  `txs[t].snapshot <= c`. Conflict-check semantics:
  ```
  conflict(t, c) ==
      \E k \in DOMAIN txs[t].write_set :
          HasVersionInRange(k, txs[t].snapshot, c - 1)
  ```
  If `conflict(t, c)` is FALSE: install every (k, v) in
  `txs[t].write_set` via lifted `Put` / `Tombstone` actions at
  `commit_opnum = c`; flip status to Committed; bump `opCount` past c.
  If TRUE: flip status to Aborted; storage state UNCHANGED.

**Invariants (the verifiable claims):**
- All 6 SP111 MVCCTx invariants preserved.
- **WriteSetMonotonic** — `txs[t].write_set` only grows during Active
  (no key deletions; same-key updates are last-write-wins, not
  removals).
- **WriteWriteConflictDetected** — for every Tx t with
  status="Committed" and commit_opnum c, NO key in `txs[t].write_set`
  has a version in `(txs[t].snapshot, c - 1]`. This is the SI safety
  invariant. (Equivalently: every committed Tx was conflict-free
  at apply time.)
- **CommitAtomicity** — for every Tx t with status="Committed" at
  commit_opnum c, EVERY key in `txs[t].write_set` has a version at
  exactly commit_opnum c in `versions`. No partial apply.
- **FirstCommitterWins** — for any two Txs t1 != t2 with overlapping
  write_sets (`DOMAIN txs[t1].write_set \cap DOMAIN txs[t2].write_set
  != {}`), at MOST one can be in status="Committed" with the other
  conflicting via the shared key. (Cleaner formulation: there is a
  total commit ordering — the first to commit wins; the second to
  attempt aborts.)
- **DeterministicApply** — re-state the parent S2 Decision 4 at the SI
  level: given the same prefix of CommitTx actions, the resulting
  `versions` state is byte-identical across all possible interleavings
  of the storage actions. (Strictly: this is implied by the
  per-CommitTx atomicity + the per-CommitTx commit_opnum being
  apply-order-derived; TLC checks it by exhausting all interleavings.)

**Bounded model (initial `.cfg`):**
```
SPECIFICATION Spec

CONSTANTS
    Keys      = {k1, k2}      \* (type_id, object_id) pairs from MVCCStorage
    Values    = {v1, v2}
    MaxOpnum  = 4
    MaxOps    = 6
    TxIds     = {t1, t2}      \* 2 concurrent Tx
    MaxTxOps  = 8             \* Begin + Write + Read + Commit/Abort

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

CHECK_DEADLOCK FALSE
```

**Coverage target.** Per the SP110 + SP111 precedent, target complete
coverage of the bounded model with ZERO invariant violations. The
state space is bigger than MVCCTx's (additional `write_set` per Tx,
the CommitTx action multiplies the action-space by ~|Keys| ×
|OpNums|). The bounded constants may need to be tightened in T6 to
keep wall-clock runtime tractable; the smaller-model coverage is the
gate; the larger configuration is an S2.X follow-up.

**Thesis fit:** `verifiable` (extends SP111's TLA+ rigor to the SI
layer; WriteWriteConflictDetected + FirstCommitterWins + CommitAtomicity
+ DeterministicApply are mechanically-checked); `honest-docs` (the
bounded-model coverage cadence + the named-correspondence caveat
carry forward from SP109/SP110/SP111).

### Decision 8 — Backward compatibility: **purely additive; zero legacy-path bytes change**

Per the parent design Decision 8 + SP110/SP111 discipline:

- **Zero changes to existing `kessel-sm` apply paths** in S2.3 except
  the addition of an `Op::CommitTx => ...` arm at the end of the
  `match op` block in `StateMachine::apply`. Every existing op variant's
  apply semantics are byte-unchanged.
- **Zero changes to `kessel-sql`** in S2.3. SQL routing through Tx is
  the S2.6 responsibility.
- **Zero changes to `kessel-vsr`** in S2.3. The Op enum addition is
  wire-compatible (append-only variant); replication serializes the new
  variant via the existing kessel-proto codec.
- **Zero new external dependencies** in S2.3. The Tx write-side uses
  `std::collections::BTreeMap` (already in std). `cargo tree -p
  kesseldb-server | grep -Ei "parquet|objstore|rustls|webpki"` stays
  byte-identical to the SP111 baseline (= the SP110 baseline).
- **Zero new public methods on `Storage<V>`** in S2.3. The Tx layer
  calls `mvcc::has_version_in_range` (shipped in SP110) and
  `mvcc::put_versioned` (shipped in SP110) only — both already public.
- **Every SP1–SP111 path bytes-on-disk-identical.** The S2.3 changes
  touch only NEW code paths (the Tx write_set + the `Op::CommitTx`
  apply arm). The legacy single-version 20-byte key path and the
  S2.1 MVCC 28-byte versioned key path BOTH stay byte-net-0 in S2.3.

**The S2.3 gate growth is purely new SI tests** on the new write-side
+ commit + Op::CommitTx apply paths. The existing 540-test cargo gate
(SP111 final) plus the new SI tests becomes the S2.3 final; T6 records
the actual delta.

**Thesis fit:** `honest-docs` (the parallel-module + additive-Op
discipline holds); `zero-dep` (no new external crate).

### Decision 9 — Slice numbering: **SP112** (the slice immediately after SP111)

SP112 in the subproject numbering. The S2.3 plan/spec filenames use
the `2026-05-24` date prefix. The internal record (T6 will create it)
is:

- Spec/design: this file —
  `docs/superpowers/specs/2026-05-24-mvcc-si-s2-3-design.md`.
- Plan: companion file —
  `docs/superpowers/plans/2026-05-24-mvcc-si-s2-3.md`.
- Slice closeout record (T6 will create it):
  `docs/superpowers/specs/2026-05-24-kesseldb-subproject112-mvcc-si-s2-3.md`.

Subproject-number / S2-sub-slice cross-reference table after S2.3:

| Subproject | S2 sub-slice | Status | Headline |
|---|---|---|---|
| SP110 | S2.1 | done | MVCC versioned-storage primitive |
| SP111 | S2.2 | done | Tx context + read-set tracking |
| **SP112** | **S2.3** | **this slice** | **SI write-side + conflict detection at SM apply time** |
| SP113+ | S2.4 | pending | SSI promotion (rw-antidependency cycle detection) |
| ... | S2.5 | pending | GC + watermark |
| ... | S2.6 | pending | SQL + SM cutover |

**Thesis fit:** `honest-docs` (the slice numbering + cross-reference
makes the strategic-tier trajectory inspectable from any single
record).

---

## Architecture

### High-level layering after S2.3

```
                  +---------------------------+
                  |  kessel-sql (unchanged)   |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-sm (NEW arm)      |   <-- S2.3 (this slice)
                  |  + Op::CommitTx apply     |       SI conflict check
                  |  legacy ops UNCHANGED     |       runs HERE
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage::tx       |   <-- S2.2 + S2.3
                  |  + write/commit/write_set |       (write side added)
                  |  + read-your-writes       |       (overlay added)
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage::mvcc     |   <-- S2.1 (UNCHANGED)
                  |  has_version_in_range     |       (the primitive S2.3
                  |  + put_versioned          |        was waiting for)
                  |  + get_at_snapshot        |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage LSM       |   (existing, unchanged)
                  +---------------------------+

                  +---------------------------+
                  |  kessel-proto (NEW Op)    |   <-- S2.3 (this slice)
                  |  + Op::CommitTx variant   |       wire-compatible
                  |  legacy variants UNCHANGED|       (append-only)
                  +---------------------------+
```

The conflict-check seam is `kessel-sm::StateMachine::apply`'s new
`Op::CommitTx` arm — that's where every replica converges on the
same verdict.

### Module changes (S2.3 deltas only)

- `crates/kessel-proto/src/lib.rs` — append `CommitTx { ... }` variant
  to `enum Op` (append-only; no reordering of existing variants).
- `crates/kessel-storage/src/tx.rs` — add `write_set: BTreeMap<...>`
  field to `Tx`; extend `Tx::read` with the read-your-writes overlay
  (Decision 3); add `Tx::write`, `Tx::write_set`, `Tx::commit` methods;
  extend `TxError` with `SnapshotOutOfRange` + `StorageIo` variants
  + `TxCommitOutcome` enum. The existing `commit_read_only` and
  `abort` methods stay.
- `crates/kessel-sm/src/lib.rs` — add the `Op::CommitTx { ... } => {
  ... }` arm at the end of `StateMachine::apply`'s `match op` block;
  zero changes to existing arms.
- `crates/kessel-sm/src/lib.rs` (or `kessel-proto`) — define
  `OpResult::TxCommitted` + `OpResult::TxAborted` (or extend the
  existing `OpResult` enum). T2 will pick the exact location; the
  design constraint is that the result variants are exposed through
  the existing OpResult shape so the SM apply API stays uniform.
- `kesseldb-tla/MVCCSi.tla` + `.cfg` + baseline TLC result.

**No new crates. No new files outside the four listed.**

### Internal data shape

`Tx<'a, V: Vfs>` (S2.3 expansion) holds FOUR fields:
- `store: &'a Storage<V>` — unchanged from S2.2 (shared borrow).
- `snapshot_opnum: u64` — unchanged from S2.2.
- `read_set: BTreeSet<(u32, [u8; 16])>` — unchanged from S2.2.
- `write_set: BTreeMap<(u32, [u8; 16]), Option<Vec<u8>>>` — NEW in
  S2.3. BTreeMap for deterministic iteration order (Decision 2).
  `None` value = buffered tombstone.

The struct stays `!Send + !Sync` (holds `&Storage`); single-thread by
construction; the borrow lifetime gives compile-time-enforced lifecycle.

### Call graph (S2.3 additions)

```
Tx::write(t, o, value)
    -> self.write_set.insert((t, *o), value)   // overwrites same-key writes (BTreeMap)
    -> returns ()                              // no failure mode (buffered only)

Tx::read(t, o)  [read-your-writes overlay]
    -> if let Some(buffered) = self.write_set.get(&(t, *o)):
           insert (t, *o) into self.read_set
           return SnapshotRead::Found(v) or SnapshotRead::Tombstoned
    -> else:
           insert (t, *o) into self.read_set
           return mvcc::get_at_snapshot(self.store, t, o, self.snapshot_opnum)

Tx::write_set()
    -> &self.write_set                         // immutable view

Tx::commit(commit_opnum)
    -> if self.snapshot_opnum > commit_opnum:
           return Err(TxError::SnapshotOutOfRange { snapshot, commit })
    -> if commit_opnum > 0:
           let hi = commit_opnum - 1
           for (t, o, _) in self.write_set:
               if mvcc::has_version_in_range(self.store, t, &o, self.snapshot_opnum, hi):
                   return Ok(TxCommitOutcome::Aborted { conflicting_key: (t, o) })
    -> for (t, o, v) in self.write_set:
           mvcc::put_versioned(self.store_mut(), t, &o, commit_opnum, v)
              .map_err(TxError::StorageIo)?
    -> return Ok(TxCommitOutcome::Committed { commit_opnum })

SM::apply(opnum, Op::CommitTx { snapshot_opnum, write_set, commit_opnum })
    -> [same logic as Tx::commit, operating on the SM's owned Storage]
    -> returns OpResult::TxCommitted { commit_opnum }
       OR        OpResult::TxAborted { reason }
```

**Note on the Tx::commit Storage mutability.** The S2.2 Tx holds
`&'a Storage<V>` (shared borrow — read-only). S2.3's `Tx::commit`
needs to call `put_versioned` which takes `&mut Storage<V>`. Two
implementation strategies are open at the kessel-storage::tx layer:
1. Keep `&'a Storage<V>` and use interior mutability (e.g.,
   `Storage`'s LSM is already RefCell/Mutex-wrapped internally) to
   call put_versioned through the shared borrow.
2. Change the borrow to `&'a mut Storage<V>` on Tx construction
   (a breaking change to S2.2's `Tx::begin` signature).

T2 will pick the path. The design constraint is that the S2.2 read-
only Tx use case continues to work (a SELECT-only Tx must NOT need
to upgrade the borrow to mut). The recommended approach is (1) —
preserve the S2.2 borrow shape; rely on Storage's existing interior
mutability for the commit-time put_versioned calls. If Storage's
public API doesn't expose that, T2 introduces a `commit_writes`
helper on Storage that takes `&self` and goes through interior
mutability. **Implementation choice deferred to T2 with a documented
rollback path** — if the interior-mutability shape doesn't work
ergonomically, T2 may pivot to (2) and add a `Tx::begin_rw` constructor
that takes the `&mut Storage<V>` borrow, leaving `Tx::begin` (the
S2.2 signature) as the read-only constructor. Either path preserves
the S2.2 contract.

### MVCCSi.tla extension (the verifiable artifact)

`kesseldb-tla/MVCCSi.tla` EXTENDS `MVCCTx` to model the SI layer over
the verified versioned-storage + Tx primitives. See Decision 7 above
for state vars, actions, and invariants. The `.cfg` ships at the
bounded config in Decision 7. The baseline TLC run lands in
`kesseldb-tla/results/2026-05-24-mvcc-si-baseline.txt` with the same
discipline as SP110's and SP111's baselines: complete coverage of the
bounded config, zero invariant violations, honest disclosure of
state-count + runtime + bounded-config caveat.

---

## The SI conflict-detection contract (formal)

The contract S2.3 establishes — referenced by S2.4 + S2.6.

### Conflict-detection invariant

For every Tx `T` that COMMITS at `commit_opnum = c` with snapshot
opnum `s` and write_set `W`:
```
For every (type_id, object_id) in W:
    has_version_in_range(store_state, type_id, &object_id, s, c - 1) == false
```
(Where the half-open interval `(s, c - 1]` is the SI conflict window.
Equivalently: NO version of any key in W was committed in the window
between the Tx's snapshot and the moment just before this Tx's commit.)

### Apply atomicity invariant

For every Tx `T` that COMMITS at `commit_opnum = c` with write_set `W`:
```
After SM apply: EVERY (type_id, object_id, value) in W has a version
                in storage at exactly commit_opnum = c, with the
                Tx-supplied value (or tombstone if value == None).

If T ABORTS: NO version is installed (storage state unchanged).
```

### Deterministic-apply invariant (the thesis-fit headline)

For every two replicas R1, R2 that have applied the same log prefix
ending at an Op::CommitTx:
```
R1's apply result for that Op::CommitTx == R2's apply result for that
                                           Op::CommitTx.
R1's versions state after apply == R2's versions state after apply
                                   (byte-identical).
```
(Equivalently: the verdict commit-or-abort is a deterministic function
of the log prefix; no replica can reach a different verdict than any
other replica.)

### First-committer-wins invariant

For any two Txs `T1`, `T2` with overlapping write_sets and overlapping
snapshot windows (both snapshots <= both commit opnums; both write_sets
share at least one key):
```
At most one of {T1, T2} can be in COMMITTED status.
The other MUST be in ABORTED status (per the conflict-detection
invariant above).
```

### Read-your-writes invariant

For every Tx `T` and key `k`:
```
If T called Tx::write(k, Some(v)) at any point:
    EVERY subsequent Tx::read(k) returns SnapshotRead::Found(v).

If T called Tx::write(k, None) at any point:
    EVERY subsequent Tx::read(k) returns SnapshotRead::Tombstoned.

(Both regardless of the snapshot-storage state — the buffered write
shadows the snapshot value within T.)
```

### Read-set coverage invariant (carried forward from S2.2 Decision 4)

For every `Tx::read(t, o)` call on a Tx `T`:
```
After the call: (t, *o) \in T.read_set
                regardless of which SnapshotRead variant was returned
                AND regardless of whether the read was served from the
                snapshot path or the buffered-write overlay.
```

---

## Sub-slice gate accounting (estimated)

Total cargo gate growth in S2.3: estimated **+25 to +35 tests** on
the new write-side + commit + Op::CommitTx apply paths. Breakdown:

| Task | Expected tests | Cumulative | Notes |
|---|---|---|---|
| T0 baseline | 0 | 540 | SP111 final, expect FAILED=0 + seed-7 green |
| T1 scaffold | +2 | 542 | Type-shape locks for new Tx fields + Op::CommitTx + TxCommitOutcome |
| T2 impl + KATs | +9 to +11 | 551 to 553 | write+read-overlay / read-yourself-Found / read-yourself-Tombstoned / write_set_sorted_iteration / commit_empty_write_set / commit_non_conflicting_apply / commit_conflict_aborts / SM apply byte-equivalence with Tx::commit / Op::CommitTx wire roundtrip |
| T3 integration | +5 to +6 | 556 to 559 | write-then-read sees own writes / two-Tx-no-overlap-both-commit / two-Tx-overlap-first-wins-second-aborts / 3-replica byte-identity for SI commits / SM apply path matches Tx::commit on identical storage |
| T4 coverage | +5 | 561 to 564 | empty-write_set commit / write+abort no apply / write-same-key-twice-overlay-coalesces / large-write_set (1000 writes commit) / mixed-write-then-tombstone-then-write in same Tx |
| T5 pentest | +6 to +7 | 567 to 571 | hostile giant write_set (100k) / conflict at exact snapshot boundary (snapshot+1 conflict) / snapshot=0, commit_opnum=u64::MAX (no overflow) / write-then-tombstone-then-write same key / commit_opnum=0 edge (cannot subtract 1) / snapshot > commit_opnum (rejected as SnapshotOutOfRange) / compile-time lock on commit-after-commit |
| T6 docs + TLA+ | 0 Rust | 567 to 571 | MVCCSi.tla + .cfg + TLC baseline + SP112 record + STATUS + memory |

**Estimated final cargo gate after S2.3:** **567 to 571 tests** (`FAILED=0`,
seed-7 green). The actual number lands in T6.

The TLA+ artifact's gate is the TLC baseline run (zero invariant
violations on the bounded config) + the artifact files committed to
`kesseldb-tla/`.

---

## Sub-slice decomposition reminder (S2.4–S2.6 still pending)

S2.3 ships ONLY the write-side Tx API + the Op::CommitTx SM apply
arm + the conflict-check + the MVCCSi.tla artifact. The following
are explicitly **OUT of scope for S2.3** and tracked in the parent
S2 design's sub-slice decomposition:

- **S2.4** — SSI promotion: the dangerous-cycle detector that consumes
  S2.2's `read_set` AND S2.3's `write_set`. The slice that promotes
  plain SI to true serializability.
- **S2.5** — GC + watermark (`Op::AdvanceWatermark`).
- **S2.6** — SQL surface integration + SM cutover (the byte-identity-
  gate-change slice, honest-disclosed there). Also wires the
  `sm.next_op_number()` source of `commit_opnum` and the
  cursor-stall-on-snapshot-not-yet-applied semantics carried forward
  from Decision 4's honest disclosure.

Each subsequent sub-slice will get its own plan when the prior one
lands. S2.4 plan is the next docs slice expected after this S2.3
slice ships.

---

## Honest deferred set

Items explicitly out of scope for S2.3, named here so the S2.3 record
can't drift into over-claim territory:

- **SSI dangerous-cycle detection.** Deferred to S2.4. S2.3 ships
  plain SI only — write-write conflicts are detected; read-write
  anti-dependencies (the SSI promotion) are not.
- **GC + watermark.** Deferred to S2.5.
- **SM-side `next_op_number()` helper supplying `commit_opnum`.**
  Deferred to S2.6 per Decision 6. S2.3 tests pass literal opnums.
- **Cursor-stall on snapshot-not-yet-applied.** Per Decision 4 honest
  disclosure: deferred to S2.6 when the SM caller integration lands.
  In S2.3 the SM apply path treats `snapshot_opnum > current_opnum` as
  a malformed op (conservatively aborts with `SnapshotOutOfRange`).
- **SQL integration.** Deferred to S2.6.
- **Cross-thread Tx, Tx-pool, Tx ID allocation.** Not on the S2 roadmap
  (carried forward from S2.2). Tx is single-thread / stack-frame-bound
  by construction.
- **Multi-replica Tx TLA+ model.** Same scope decision as SP110/SP111
  carried forward: the S2.3 `MVCCSi.tla` models a single replica's
  Tx + storage + commit-apply; multi-replica byte-identity is verified
  at the Rust integration-test level (T3 ships a 3-replica byte-
  identity test). Multi-replica TLA+ is an S2.X follow-up.
- **Larger TLC bounds for MVCCSi.** Same disclosure as SP110/SP111:
  the bounded config in T6 may be tightened to keep TLC tractable.
- **TLA+-mechanized-refinement TLA+ ↔ Rust.** Same gap S1/SP109 +
  SP110/SP111 disclosed. Per-sub-slice named-action correspondence
  carries forward; not a refinement proof.
- **Wire compatibility tests for Op::CommitTx across protocol
  versions.** S2.3 ships the variant; the existing kessel-proto codec
  serializes it via the append-only enum-variant discipline. A formal
  forward/backward wire-compat test is an S2.X follow-up if a wire
  version bump becomes relevant.

---

## Thesis-fit note

**Thesis fit:** `deterministic` (the conflict verdict is computed by
SM apply over the log prefix — by construction, every replica reaches
the same verdict; the thesis-fit headline phrase **"deterministic
apply IS the conflict resolver, no distributed coordination needed"**
is operationalized in this slice's Op::CommitTx apply path; this is
the most direct deterministic-replicated payoff in the S2 backlog);
`replayable` (every Tx outcome is a function of `(snapshot_opnum,
write_set, commit_opnum, log prefix)` — debugging IS replay; a
production-bug-report on a SI conflict reduces to a `(seed, log,
opnum)` tuple);
`verifiable` (`MVCCSi.tla` extends SP111's MVCCTx.tla with the
write_set + the CommitTx action + five invariants —
WriteSetMonotonic, WriteWriteConflictDetected, CommitAtomicity,
FirstCommitterWins, DeterministicApply — all mechanically-checked
by TLC against the same VSR-log substrate S1/SP109 verified; the
fourth rigor-gate TLA+ module in the project);
`honest-docs` (the parent-design Decision 4 thesis-fit headline is
operationalized HERE for the first time, not silently embedded; the
Tx-side-vs-SM-side conflict-check choice is explicitly documented as
"only (b) preserves the determinism thesis"; the commit_opnum-source
deferral to S2.6 + the cursor-stall deferral to S2.6 are both
explicitly named; the `TxError`-vs-`TxCommitOutcome::Aborted`
distinction is documented to prevent caller misuse).

The thesis-fit headline of this slice: **the deterministic state
machine IS the conflict resolver — KesselDB does not need TrueTime,
HLCs, or txn-record coordination because the VSR log already orders
every commit op, and the SM's deterministic apply already agrees on
the verdict.** This is the most direct expression of the
"deterministic replicated SQL" pillar in the strategic-tier backlog
so far — and the slice that makes the S2 thesis claim "consensus +
SQL can be simpler than MVCC-centric systems" land in code.

---

## Internal record

This design document is
`docs/superpowers/specs/2026-05-24-mvcc-si-s2-3-design.md`.

The S2.3 implementation plan is
`docs/superpowers/plans/2026-05-24-mvcc-si-s2-3.md`.

When S2.3 ships, its slice-record file will be
`docs/superpowers/specs/2026-05-24-kesseldb-subproject112-mvcc-si-s2-3.md`
(SP112 in the subproject numbering; mirrors the SP110/SP111
filename pattern). The record will carry the honest gate accounting
(540 → final), the per-task evidence chain, the TLA+-to-Rust
correspondence table, the deferred backlog (S2.4–S2.6), and the
strategic-tier context update.
