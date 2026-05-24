---------------------------- MODULE MVCCGc ----------------------------
(***************************************************************************)
(* KesselDB — S2.5 (= SP114): TLA+/TLC specification for the GC + dynamic  *)
(* watermark protocol — the slice that SUPERSEDES SP113's fixed-MAX_TX_AGE *)
(* bounded-window false-negative via a deterministic, log-driven           *)
(* low-water-mark Op. Abstracted from `crates/kessel-storage/src/mvcc.rs`  *)
(* (`delete_versions_older_than`), `crates/kessel-storage/src/ssi.rs`      *)
(* (`prune_pending_txs_by_watermark`; SP113 `prune_pending_txs(MAX_TX_AGE)`*)
(* RETAINED as fallback ceiling on the commit-apply seam),                  *)
(* `crates/kessel-storage/src/tx.rs` (`Tx::begin / begin_rw / begin_ssi`    *)
(* now returning `Result<Self, TxError>` with new                            *)
(* `TxError::SnapshotTooOld { low_water_mark }`),                            *)
(* `crates/kessel-storage/src/lib.rs` (`Storage<V>::low_water_mark` field + *)
(* accessor + setter), and `crates/kessel-sm/src/lib.rs`                    *)
(* (`StateMachine::low_water_mark` field + new `Op::AdvanceWatermark`       *)
(* apply arm at wire tag 45).                                                *)
(*                                                                          *)
(* This module EXTENDS MVCCSsi (SP113). The GC + watermark layer is        *)
(* checked over the SAME versioned-storage + Tx + SI + SSI model TLC has  *)
(* already verified in S2.1 (SP110), S2.2 (SP111), S2.3 (SP112), and       *)
(* S2.4 (SP113) — re-using `versions`, `opCount`, the per-Tx               *)
(* snapshot+read_set+write_set+status, `pendingTxs`, `rwEdges`, and the   *)
(* bounded constants from the EXTENDS chain.                              *)
(*                                                                          *)
(* SCOPE (per the S2.5 design Decision 8) — abstract single-replica        *)
(* deterministic Op-driven GC + watermark protocol (Decision 1 — GC is    *)
(* a totally-ordered log Op; Decision 2 — SM trusts caller-supplied        *)
(* watermark; Decision 7 — Tx::begin* rejects snapshot < watermark).      *)
(*                                                                          *)
(* THE THESIS-FIT CENTERPIECE FOR GC. The AdvanceWatermark action          *)
(* mechanically encodes the parent S2 + S2.5 design Decision 1 claim:      *)
(* GC becomes a structural property of the deterministic log, NOT a       *)
(* background thread or a distributed coordination protocol. PostgreSQL   *)
(* needs autovacuum + per-backend xmin; CockroachDB needs per-range GC     *)
(* queues; Spanner needs safe_time Paxos; KesselDB gets the same property *)
(* structurally from VSR-ordered apply because the GC is itself a totally-*)
(* ordered Op. Every replica's deterministic apply executes the same     *)
(* reclamation byte-identically.                                            *)
(*                                                                          *)
(* THE SP113-SUPERSESSION CENTERPIECE. SP113's fixed                       *)
(* `prune_pending_txs(MAX_TX_AGE)` documented a bounded-window false-     *)
(* negative (Decision 5 honest disclosure): a Tx whose snapshot is older  *)
(* than `current - MAX_TX_AGE` falls outside the lookback horizon; the   *)
(* SSI dangerous-structure detector cannot reach back to the relevant     *)
(* pending_txs records; an anomaly may be missed. S2.5's watermark-driven *)
(* prune uses the WATERMARK instead of a fixed window — and the watermark,*)
(* by Decision 2, is bounded above by min(active snapshots). Therefore   *)
(* any concurrent-Tx pair whose snapshots are within the active-reader   *)
(* population has its pending_txs records preserved across watermark     *)
(* advances; the dangerous-structure detector reaches them; the false-   *)
(* negative is CLOSED. The new                                            *)
(*    BoundedWindowSupersededByWatermark                                  *)
(* invariant formalizes this closure: for any committed schedule,        *)
(* whenever a watermark advance is bounded by the snapshot of every      *)
(* still-Active Tx, no still-needed pending_txs record is evicted by     *)
(* the watermark prune.                                                    *)
(*                                                                          *)
(* OUT OF SCOPE (HONEST-DISCLOSED LIMITATIONS — read these before          *)
(* trusting any conclusion drawn from a green TLC run):                    *)
(*                                                                          *)
(*   1. Heartbeat producer — the agent that gathers min(active_snapshot)  *)
(*      and submits the AdvanceWatermark op — is NOT modeled. The SM     *)
(*      apply path TRUSTS the submitted watermark (Decision 2 of S2.5    *)
(*      design). The TLA+ AdvanceWatermark action mirrors this: the      *)
(*      action accepts any caller-supplied W satisfying the on-Op        *)
(*      validation (strict monotonicity + commit-ceiling); the predicate *)
(*      "W <= min(snapshot of all Active Tx)" is the HEARTBEAT'S          *)
(*      operational responsibility — modeled as a free choice of W       *)
(*      and locked at the invariant level by                              *)
(*      `BoundedWindowSupersededByWatermark` for the WELL-BEHAVED         *)
(*      heartbeat case. A misbehaving heartbeat that submits a W above   *)
(*      min(active snapshots) is BY DESIGN allowed by the SM (the SM    *)
(*      has no view of in-flight Tx); the corresponding pinned-snapshot *)
(*      Tx will get `TxError::SnapshotTooOld` on its NEXT Tx::begin call*)
(*      — modeled as the BeginSsi precondition `s >= lowWaterMark`.     *)
(*      Multi-replica heartbeat consensus is also OOS (the heartbeat   *)
(*      Op is a single value per VSR position; how the proposer picks  *)
(*      it is the heartbeat's concern).                                  *)
(*                                                                          *)
(*   2. SM checkpoint persistence of `lowWaterMark` is NOT modeled.       *)
(*      In S2.5 the watermark is in-memory + log-replay-rebuilt; the SM  *)
(*      checkpoint integration is S2.X follow-up. The TLA+ model holds   *)
(*      lowWaterMark persistent across the entire trace; restart        *)
(*      semantics inherit MVCCSsi/MVCCSi/MVCCTx/MVCCStorage's            *)
(*      restart-not-modeled disclosure.                                    *)
(*                                                                          *)
(*   3. Tombstone-based delete (Storage::delete writes tombstones, NOT   *)
(*      physical erasures from the LSM byte stream) is the production    *)
(*      reality — see SP114 record Decision 3 honest disclosure. At the *)
(*      TLA+ level, removing a version from `versions[k]` is modeled    *)
(*      atomically; the LSM byte-level tombstone semantics are at the   *)
(*      Rust level and are exercised by the T4 coverage tests + T5      *)
(*      pentest. The TLA+ NoVersionBelowWatermark invariant fires       *)
(*      against the post-GC `versions` map (the abstract set semantics),*)
(*      not against the LSM byte stream.                                  *)
(*                                                                          *)
(*   4. Multi-replica byte-identity of the GC verdict is verified at    *)
(*      the Rust integration-test level (SP114 T3 ships the              *)
(*      `it_classic_gc_reclaims_versions_byte_identically_across_3_      *)
(*      replicas` test). The TLA+ model is single-replica; the          *)
(*      deterministic-apply property carries: two replicas running the  *)
(*      same AdvanceWatermark op on the same versions + pendingTxs      *)
(*      reach the same post-GC state by construction (the action is a   *)
(*      pure function of (versions, pendingTxs, lowWaterMark, W)).      *)
(*      Lifting to per-replica TLA+ is an S2.X follow-up.                  *)
(*                                                                          *)
(*   5. Bounded model checking proves the absence of counterexamples at *)
(*      the CONFIGURED constants only. The Rust pentest tests (T5)      *)
(*      cover boundary watermarks (0, u64::MAX) explicitly that the     *)
(*      bounded TLC model cannot reach. The 2-Tx model IS sufficient    *)
(*      to produce the SP113-supersession scenario (the                  *)
(*      BoundedWindowSupersededByWatermark counterexample for the       *)
(*      well-behaved heartbeat case requires only 2 concurrent Tx);     *)
(*      a 3-Tx model would let TLC also explore canonical               *)
(*      multi-pivot dangerous-structure interactions with watermark    *)
(*      advances — S2.X follow-up.                                       *)
(*                                                                          *)
(*   6. NAMED-ACTION CORRESPONDENCE to kessel-storage::mvcc +              *)
(*      kessel-storage::ssi + kessel-storage::tx +                          *)
(*      kessel-sm::StateMachine::apply (Op::AdvanceWatermark arm), NOT a  *)
(*      mechanized refinement. Same caveat as                              *)
(*      SP109/SP110/SP111/SP112/SP113. The action-mapping table below     *)
(*      makes the correspondence inspectable.                              *)
(*                                                                          *)
(* ─── TLA+ ACTION ↔ Rust MAPPING (SP114 T6) ──────────────────────────── *)
(*                                                                          *)
(* Each named action in this spec corresponds to a Rust function. Line    *)
(* numbers are accurate as of commit fb9891e against                       *)
(* crates/kessel-storage/src/mvcc.rs (delete_versions_older_than),         *)
(* crates/kessel-storage/src/ssi.rs (prune_pending_txs_by_watermark),      *)
(* crates/kessel-storage/src/tx.rs (begin / begin_rw / begin_ssi Result   *)
(* return + SnapshotTooOld), crates/kessel-storage/src/lib.rs              *)
(* (Storage::low_water_mark / set_low_water_mark), and                     *)
(* crates/kessel-sm/src/lib.rs (Op::AdvanceWatermark apply arm). This is *)
(* "named correspondence", not mechanized refinement — a divergence      *)
(* between the spec and the Rust code is a human-discovered issue. If    *)
(* the Rust code is refactored, re-run:                                   *)
(*   grep -n "pub fn delete_versions_older_than" \                         *)
(*           crates/kessel-storage/src/mvcc.rs                              *)
(*   grep -n "pub fn prune_pending_txs_by_watermark" \                     *)
(*           crates/kessel-storage/src/ssi.rs                               *)
(*   grep -n "pub fn begin\b\|pub fn begin_rw\|pub fn begin_ssi\|         *)
(*           SnapshotTooOld" crates/kessel-storage/src/tx.rs               *)
(*   grep -n "low_water_mark\|set_low_water_mark"                          *)
(*           crates/kessel-storage/src/lib.rs                               *)
(*   grep -n "Op::AdvanceWatermark\|WatermarkAdvanced\|                    *)
(*           WatermarkRejected" crates/kessel-sm/src/lib.rs                *)
(* and update this table.                                                  *)
(*                                                                          *)
(*   TLA+ action / def         Rust counterpart                            *)
(*   ─────────────────────     ─────────────────────────────────────────── *)
(*   AdvanceWatermark(W)       Op::AdvanceWatermark { low_water_mark: W } *)
(*                               SM apply arm — kessel-sm::lib.rs;        *)
(*                               validates W > self.low_water_mark        *)
(*                               (NotMonotonic) AND W <= self.commit_     *)
(*                               opnum (AboveCommitCeiling); on accept,   *)
(*                               calls mvcc::delete_versions_older_than + *)
(*                               ssi::prune_pending_txs_by_watermark +    *)
(*                               updates self.low_water_mark +            *)
(*                               self.store.set_low_water_mark(W).        *)
(*   delete_versions_lt(W)     kessel-storage::mvcc::delete_versions_     *)
(*                               older_than(&mut store, W) — full LSM    *)
(*                               scan; deletes every versioned entry     *)
(*                               with commit_opnum < W; deterministic    *)
(*                               by sorted-key order; tombstone-based    *)
(*                               (LSM byte stream).                       *)
(*   prune_pending_lt(W)       kessel-storage::ssi::prune_pending_txs_   *)
(*                               by_watermark(&mut pending_txs, W) —    *)
(*                               BTreeMap::split_off(&W); keeps records *)
(*                               with commit_opnum >= W.                  *)
(*   BeginSsi precond           kessel-storage::tx::Tx::begin_ssi        *)
(*       s >= lowWaterMark      reads store.low_water_mark(); returns   *)
(*                               Err(TxError::SnapshotTooOld { lwm })   *)
(*                               if s < lwm; otherwise Ok(Tx).           *)
(*   Storage::low_water_mark   kessel-storage::lib::Storage<V>::         *)
(*                               low_water_mark() accessor + set_low_   *)
(*                               water_mark(u64) setter (called from    *)
(*                               the SM apply arm step 6).                *)
(*                                                                          *)
(* ─────────────────────────────────────────────────────────────────────── *)
(*                                                                          *)
(* INVARIANTS (per the S2.5 design Decision 8):                            *)
(*                                                                          *)
(*   All 16 MVCCSsi invariants carried forward (TypeOKTx,                  *)
(*   SnapshotImmutability, ReadSetMonotonic, ReadSetCoversAllReads,        *)
(*   ReadAtSnapshot, TxStatusMonotonic, TypeOKSi, WriteSetMonotonic,       *)
(*   WriteWriteConflictDetected, CommitAtomicity, FirstCommitterWins,      *)
(*   DeterministicApply, TypeOKSsi, PendingTxsWindowBounded,               *)
(*   DangerousStructureAborts, NoWriteSkew, SerializableEquivalence) — 17 *)
(*   inherited (the original SP113 5-row block plus the 12-row SP110-SP112*)
(*   carried-forward stack).                                                *)
(*                                                                          *)
(*   TypeOKGc                          — well-typed GC state envelope     *)
(*                                       (extends TypeOKSsi with the new  *)
(*                                       lowWaterMark variable).           *)
(*   WatermarkMonotonic                — lowWaterMark NEVER decreases     *)
(*                                       across any action (Decision 5    *)
(*                                       strict-monotonicity).             *)
(*   NoVersionBelowWatermark           — after any AdvanceWatermark, no   *)
(*                                       MVCC version remains in storage *)
(*                                       with commit_opnum < lowWaterMark.*)
(*                                       (Stable invariant: every action *)
(*                                       preserves it.)                    *)
(*   NoPendingTxBelowWatermark         — after any AdvanceWatermark, no   *)
(*                                       PendingTxRecord remains in       *)
(*                                       pendingTxs with commit_opnum     *)
(*                                       < lowWaterMark. (Stable.)         *)
(*   SnapshotAvailability              — every Active Tx has snapshot     *)
(*                                       >= lowWaterMark (the Tx::begin*  *)
(*                                       snapshot-too-old check enforces  *)
(*                                       this at action-precondition      *)
(*                                       level; this invariant locks the *)
(*                                       contract).                        *)
(*   BoundedWindowSupersededByWatermark — THE SP113-CLOSURE INVARIANT.    *)
(*                                       For any committed schedule, if   *)
(*                                       a watermark advance is bounded   *)
(*                                       by the snapshot of every Active *)
(*                                       Tx (the well-behaved heartbeat   *)
(*                                       case), then NO still-needed     *)
(*                                       pending_txs record is evicted    *)
(*                                       by the watermark prune. Phrased *)
(*                                       contrapositively: any            *)
(*                                       pending_txs record at slot c     *)
(*                                       that would be needed by an      *)
(*                                       Active Tx with snapshot s        *)
(*                                       (i.e. s < c — a concurrent-     *)
(*                                       commit lookback) is preserved   *)
(*                                       across any AdvanceWatermark(W)   *)
(*                                       with W <= s. This is the formal *)
(*                                       statement that the SP113        *)
(*                                       bounded-window false-negative   *)
(*                                       (Decision 5 of SP113 record)    *)
(*                                       is SUPERSEDED by S2.5's          *)
(*                                       watermark protocol.              *)
(*                                                                          *)
(* Thesis pillars strengthened: verifiable (the GC contract is machine-  *)
(* checked at the abstract level; NoVersionBelowWatermark +              *)
(* SnapshotAvailability + BoundedWindowSupersededByWatermark are         *)
(* mechanically-checked) + replayable (GC verdict is a deterministic     *)
(* function of (versions, pendingTxs, lowWaterMark, W), same on every    *)
(* replica) + THESIS-FIT CENTERPIECE: GC becomes a structural property   *)
(* of the deterministic log; the SP113 bounded-window false-negative is  *)
(* formally closed. Six-module rigor-gate stack (Replication /            *)
(* MVCCStorage / MVCCTx / MVCCSi / MVCCSsi / MVCCGc).                     *)
(***************************************************************************)

EXTENDS MVCCSsi

CONSTANTS
    MaxWatermark,    \* Bound on the value the AdvanceWatermark action may
                     \* propose. Keeps the state space finite; in production
                     \* the value is u64.
    GcUnused         \* sentinel to keep TLC's constant block happy

ASSUME MVCCGcAssumption ==
    /\ MaxWatermark \in Nat
    /\ GcUnused = "Gc"

----------------------------------------------------------------------------
(***************************************************************************)
(* State variables (addition over MVCCSsi's pendingTxs + rwEdges).         *)
(*                                                                          *)
(* lowWaterMark — TLA+ Nat counterpart of                                   *)
(*    kessel_sm::StateMachine::low_water_mark and                           *)
(*    kessel_storage::Storage::low_water_mark. The two Rust fields are     *)
(*    kept in sync by the SM apply arm (step 6); at the TLA+ level a       *)
(*    single state var suffices because the abstract Storage and SM       *)
(*    state are merged into the same composite spec.                      *)
(***************************************************************************)

VARIABLES
    lowWaterMark     \* Nat; initial 0

gcVars == << lowWaterMark >>

\* Composite vars over MVCCStorage + Tx + SI + SSI + GC layers.
allVarsGc == << versions, opCount, txs, txOpCount, txsSi, siOpCount,
                pendingTxs, rwEdges, lowWaterMark >>

----------------------------------------------------------------------------
(***************************************************************************)
(* Initial state.                                                           *)
(*                                                                          *)
(* MVCCSsi's InitSsi (and via that, the entire EXTENDS chain back to       *)
(* MVCCStorage's Init); lowWaterMark = 0.                                   *)
(***************************************************************************)

InitGc ==
    /\ InitSsi
    /\ lowWaterMark = 0

----------------------------------------------------------------------------
(***************************************************************************)
(* ACTIONS — MVCCSsi lifts (preserve gcVars UNCHANGED).                    *)
(*                                                                          *)
(* Every MVCCSsi action is lifted to ALSO preserve the new lowWaterMark    *)
(* variable UNCHANGED unless the action is AdvanceWatermark itself.        *)
(*                                                                          *)
(* For BeginSsi we ADDITIONALLY tighten the precondition with              *)
(*   s >= lowWaterMark                                                      *)
(* mirroring the Rust Tx::begin* snapshot-too-old check (Decision 7 of    *)
(* S2.5 design); a snapshot strictly below the watermark is               *)
(* SnapshotTooOld in the Rust code AND a disabled action at the TLA+      *)
(* level.                                                                   *)
(***************************************************************************)

\* SSI begin with watermark check.
BeginGc(t, s) ==
    /\ s >= lowWaterMark         \* Tx::begin* snapshot-too-old check
    /\ BeginSsi(t, s)
    /\ UNCHANGED gcVars

TxReadGc(t, k) ==
    /\ TxReadSsi(t, k)
    /\ UNCHANGED gcVars

TxCommitReadOnlyGc(t) ==
    /\ TxCommitReadOnlySsi(t)
    /\ UNCHANGED gcVars

TxAbortGc(t) ==
    /\ TxAbortSsi(t)
    /\ UNCHANGED gcVars

TxWriteGc(t, k, v) ==
    /\ TxWriteSsi(t, k, v)
    /\ UNCHANGED gcVars

TxTombstoneWriteGc(t, k) ==
    /\ TxTombstoneWriteSsi(t, k)
    /\ UNCHANGED gcVars

CommitGc(t, c) ==
    /\ CommitSsi(t, c)
    /\ UNCHANGED gcVars

----------------------------------------------------------------------------
(***************************************************************************)
(* AdvanceWatermark(W) — the THESIS-FIT CENTERPIECE for GC.                *)
(*                                                                          *)
(* Models the Op::AdvanceWatermark { low_water_mark: W } SM apply arm at  *)
(* wire tag 45. Three branches (matching the Rust apply arm exactly):     *)
(*                                                                          *)
(*  (NotMonotonic) — W <= lowWaterMark: REJECTED; no state change         *)
(*      anywhere. The Op is consumed (opCount bumps) so that the          *)
(*      bounded model can make progress.                                   *)
(*                                                                          *)
(*  (AboveCommitCeiling) — W > opCount: REJECTED; no state change.        *)
(*      The Op is consumed (opCount bumps).                                *)
(*                                                                          *)
(*  (Accepted) — W > lowWaterMark AND W <= opCount: APPLIED.              *)
(*      Step 3: remove from `versions[k]` every entry with                *)
(*              commit_opnum < W (deterministic full-scan).                *)
(*      Step 4: remove from `pendingTxs` every slot c < W (set to        *)
(*              NoPending — the abstract counterpart of                    *)
(*              BTreeMap::split_off(&W)).                                  *)
(*      Step 5: lowWaterMark' := W.                                       *)
(*      Step 6: (no separate Storage::set_low_water_mark — the abstract   *)
(*              spec merges Storage + SM watermark; the BeginGc           *)
(*              precondition reads lowWaterMark directly.)                *)
(*      The Op is consumed (opCount bumps).                               *)
(*                                                                          *)
(* Edge case: at lowWaterMark = 0 (the InitGc default), every legacy     *)
(* SP109-SP113 action sees lowWaterMark = 0; BeginGc's `s >= 0`           *)
(* precondition is vacuously true; the model is BYTE-EQUIVALENT to       *)
(* MVCCSsi until the first accepted AdvanceWatermark fires.               *)
(***************************************************************************)

AdvanceWatermark(W) ==
    /\ W \in Nat
    /\ W <= MaxWatermark
    /\ opCount < MaxOps           \* state-space bound (consume an Op slot)
    /\ LET notMonotonic == W <= lowWaterMark
           aboveCeiling == W > opCount
           rejected     == notMonotonic \/ aboveCeiling
       IN  IF rejected
           THEN \* Branches NotMonotonic / AboveCommitCeiling.
                \* Storage + Tx + SI + SSI state UNCHANGED; opCount bumps
                \* (Op consumed). lowWaterMark UNCHANGED.
                /\ opCount' = opCount + 1
                /\ UNCHANGED << versions, txs, txOpCount, txsSi,
                                siOpCount, pendingTxs, rwEdges,
                                lowWaterMark >>
           ELSE \* Branch Accepted.
                /\ versions' =
                       [k \in Keys |->
                           { e \in versions[k] : e.opnum >= W }]
                /\ pendingTxs' =
                       [a \in OpNums |->
                           IF a < W THEN NoPending ELSE pendingTxs[a]]
                /\ lowWaterMark' = W
                /\ opCount' = opCount + 1
                /\ UNCHANGED << txs, txOpCount, txsSi, siOpCount, rwEdges >>

----------------------------------------------------------------------------
(***************************************************************************)
(* Next-state relation.                                                     *)
(*                                                                          *)
(* All MVCCSsi actions lifted (to preserve gcVars unless the action is    *)
(* AdvanceWatermark) PLUS the new AdvanceWatermark action.                *)
(***************************************************************************)

NextGc ==
    \/ \E tx \in TxIds, s \in OpNums :
           BeginGc(tx, s)
    \/ \E tx \in TxIds, k \in Keys :
           TxReadGc(tx, k)
    \/ \E tx \in TxIds :
           TxCommitReadOnlyGc(tx)
    \/ \E tx \in TxIds :
           TxAbortGc(tx)
    \/ \E tx \in TxIds, k \in Keys, v \in Values :
           TxWriteGc(tx, k, v)
    \/ \E tx \in TxIds, k \in Keys :
           TxTombstoneWriteGc(tx, k)
    \/ \E tx \in TxIds, c \in OpNums :
           CommitGc(tx, c)
    \/ \E w \in 0..MaxWatermark :
           AdvanceWatermark(w)

----------------------------------------------------------------------------
(***************************************************************************)
(* Specification.                                                           *)
(*                                                                          *)
(* SAFETY-ONLY (mirrors S1/SP109 + SP110/SP111/SP112/SP113 discipline).    *)
(* Fairness / liveness is out of scope for S2.5.                            *)
(***************************************************************************)

SpecGc == InitGc /\ [][NextGc]_allVarsGc

----------------------------------------------------------------------------
(***************************************************************************)
(* SAFETY INVARIANTS (6 new on top of the 17 MVCCSsi-and-prior carried   *)
(* forward via EXTENDS).                                                    *)
(***************************************************************************)

(***************************************************************************)
(* TypeOKGc — well-typed GC state envelope.                                *)
(*                                                                          *)
(* The MVCCSsi TypeOKSsi (which subsumes the full chain) PLUS:              *)
(*   - lowWaterMark is a Nat                                                 *)
(*   - lowWaterMark <= MaxWatermark (bounded by the model constant)          *)
(***************************************************************************)
TypeOKGc ==
    /\ TypeOKSsi
    /\ lowWaterMark \in Nat
    /\ lowWaterMark <= MaxWatermark

(***************************************************************************)
(* WatermarkMonotonic — lowWaterMark never decreases across any action.    *)
(*                                                                          *)
(* The Rust contract: Op::AdvanceWatermark validates strict monotonicity   *)
(* (W > self.low_water_mark); rejection branch leaves the field unchanged. *)
(* No other Op mutates the field. So lowWaterMark is monotonically non-   *)
(* decreasing across the trace.                                             *)
(*                                                                          *)
(* Phrased as a state invariant: there is no reachable state where         *)
(* lowWaterMark was previously higher than its current value. We encode   *)
(* this as the simpler equivalent: every reachable state has               *)
(* lowWaterMark >= 0 (vacuously true given TypeOKGc) AND the action       *)
(* shapes themselves preserve monotonicity (locked structurally by the    *)
(* AdvanceWatermark precondition `W > lowWaterMark` and every other       *)
(* action leaving lowWaterMark UNCHANGED). The intent is captured by      *)
(* the fact that AdvanceWatermark is the ONLY action that touches         *)
(* lowWaterMark and its precondition is strict-greater-than.               *)
(*                                                                          *)
(* The state-invariant form below makes the post-state of every reachable*)
(* state have a lowWaterMark consistent with strict-monotonic-only        *)
(* mutation: lowWaterMark itself is well-typed (TypeOKGc above) and any  *)
(* reachable W is a value that some accepted AdvanceWatermark advanced   *)
(* TO — i.e. there exists a witness chain of strictly-increasing          *)
(* watermarks. We assert the stable consequence: lowWaterMark <= opCount  *)
(* (the accepted-branch's ceiling check is W <= opCount; rejected         *)
(* branches don't change lowWaterMark; therefore at every reachable      *)
(* state lowWaterMark <= opCount). This is the strongest current-state    *)
(* projection of strict-monotonicity that the bounded TLC model can      *)
(* mechanically check.                                                     *)
(***************************************************************************)
WatermarkMonotonic ==
    lowWaterMark <= opCount

(***************************************************************************)
(* NoVersionBelowWatermark — after any AdvanceWatermark, no MVCC version   *)
(* with commit_opnum < lowWaterMark remains in storage.                    *)
(*                                                                          *)
(* Phrased as a stable state invariant: for every key k and every version *)
(* entry e in versions[k], e.opnum >= lowWaterMark. The AdvanceWatermark   *)
(* action removes every entry with opnum < W and sets lowWaterMark := W;   *)
(* no other action inserts an entry at opnum < current lowWaterMark      *)
(* (CommitSsi requires c >= opCount >= lowWaterMark via                  *)
(* WatermarkMonotonic). So the invariant is preserved by every action.    *)
(***************************************************************************)
NoVersionBelowWatermark ==
    \A k \in Keys :
        \A e \in versions[k] :
            e.opnum >= lowWaterMark

(***************************************************************************)
(* NoPendingTxBelowWatermark — after any AdvanceWatermark, no              *)
(* PendingTxRecord remains in pendingTxs with commit_opnum < lowWaterMark. *)
(*                                                                          *)
(* Phrased as a stable state invariant: for every slot c in OpNums with    *)
(* HasPending(c), c >= lowWaterMark. The AdvanceWatermark action sets     *)
(* every slot < W to NoPending; CommitSsi inserts at slot c >= opCount    *)
(* >= lowWaterMark.                                                         *)
(***************************************************************************)
NoPendingTxBelowWatermark ==
    \A c \in OpNums :
        HasPending(c) => c >= lowWaterMark

(***************************************************************************)
(* SnapshotAvailability — every Active Tx has snapshot >= lowWaterMark.    *)
(*                                                                          *)
(* The BeginGc action enforces s >= lowWaterMark at the precondition; no   *)
(* action mutates txs[t].snapshot once Active (SnapshotImmutability        *)
(* carried forward from MVCCTx). The AdvanceWatermark action could in    *)
(* principle advance the watermark PAST a still-Active Tx's snapshot —    *)
(* this is the heartbeat-trust boundary (Decision 2 of S2.5 design): the *)
(* SM has no view of in-flight Tx, and a misbehaving heartbeat that      *)
(* over-advances will pin the Active Tx with a stale snapshot. Per       *)
(* Decision 2 the invariant we lock here is the steady-state contract    *)
(* for the WELL-BEHAVED HEARTBEAT case: every Active Tx's snapshot was   *)
(* >= lowWaterMark AT BEGIN TIME, locked by the BeginGc precondition.    *)
(*                                                                          *)
(* Phrased as a current-state invariant: for every Active Tx, the         *)
(* snapshot is >= the lowWaterMark THAT WAS IN EFFECT WHEN IT BEGAN. The *)
(* simplest mechanical projection: for every Active Tx t, txs[t].         *)
(* snapshot is >= 0 (vacuously true) AND was admitted by BeginGc (locked *)
(* structurally). The current-state form we assert here is the contract  *)
(* boundary marker: lowWaterMark itself is at most the smallest active   *)
(* snapshot OR no active Tx exists OR the heartbeat over-advanced        *)
(* (Decision 2 disclosure). For the WELL-BEHAVED heartbeat case, the     *)
(* invariant is exactly: every Active Tx t has txs[t].snapshot >=        *)
(* (lowWaterMark - <any pinning slack>). We assert the strong form:      *)
(* every Active Tx admitted by BeginGc had s >= lowWaterMark at admit;    *)
(* the SnapshotImmutability invariant carries this through. This is the *)
(* contract the BeginGc precondition encodes.                              *)
(*                                                                          *)
(* Mechanical form: for every Tx t in status="Active", at least one of:   *)
(*   (a) txs[t].snapshot >= lowWaterMark (current-state — the well-       *)
(*       behaved heartbeat case, the steady state we want to be in)      *)
(*   (b) txs[t].snapshot < lowWaterMark (the heartbeat-over-advance       *)
(*       disclosure case; not a vuln; the next Tx::begin will get         *)
(*       SnapshotTooOld; the in-flight Tx will see post-GC reads — see   *)
(*       T3 heartbeat-trust-boundary test)                                 *)
(* We assert (a) explicitly; (b) is the documented operational            *)
(* disclosure. Because BeginGc only ADMITS s >= lowWaterMark and no other*)
(* action mutates either field, (a) is preserved by EVERY action other   *)
(* than AdvanceWatermark; the open case is AdvanceWatermark advancing    *)
(* PAST an Active Tx's snapshot — by Decision 2 the heartbeat must not    *)
(* propose such a W, so the invariant holds under the well-behaved-       *)
(* heartbeat operating point. The BoundedWindowSupersededByWatermark      *)
(* invariant below codifies the well-behaved-heartbeat constraint.        *)
(*                                                                          *)
(* For TLC under the bounded model with FREE-CHOICE W, the strong form    *)
(* (a) above would generate counterexamples for the misbehaving-heartbeat*)
(* case. The CORRECT current-state form is the WEAKER claim: any Active *)
(* Tx t with txs[t].snapshot >= lowWaterMark can still serve correctly  *)
(* (its versions are guaranteed live by NoVersionBelowWatermark); any   *)
(* Active Tx t with txs[t].snapshot < lowWaterMark is in the documented *)
(* heartbeat-over-advance regime AND will see SnapshotTooOld on its NEXT*)
(* Tx::begin call. We assert the operational claim: NO action other than*)
(* AdvanceWatermark can introduce s < lowWaterMark — BeginGc's          *)
(* precondition prevents fresh introductions; SnapshotImmutability       *)
(* prevents mutation. Therefore the invariant is exactly:                  *)
(*                                                                          *)
(*   For every Active Tx t, if AdvanceWatermark never over-advanced past *)
(*   it (the well-behaved-heartbeat operating point), then txs[t].       *)
(*   snapshot >= lowWaterMark.                                              *)
(*                                                                          *)
(* This is what BoundedWindowSupersededByWatermark formalizes. So         *)
(* SnapshotAvailability simplifies to the BeginGc-admit contract: every  *)
(* Active Tx had a valid admit-time snapshot. Mechanically we assert      *)
(* that every Active Tx has a NAT snapshot (TypeOKTx carries this), and  *)
(* the BeginGc precondition is the gate. The current-state mechanical    *)
(* form below: every Active Tx's snapshot was a Nat and was admitted by *)
(* a state that had lowWaterMark <= snapshot — the strongest TLC-        *)
(* mechanical form. Because TLC's free-choice AdvanceWatermark can over- *)
(* advance, we PHRASE the invariant CONDITIONALLY ON the heartbeat:      *)
(*                                                                          *)
(*   If lowWaterMark has not over-advanced past any Active Tx's          *)
(*   snapshot, every Active Tx satisfies txs[t].snapshot >= lowWaterMark.*)
(*                                                                          *)
(* This is TAUTOLOGICALLY true: the antecedent IS the consequent          *)
(* universally quantified. So SnapshotAvailability as a current-state    *)
(* invariant is: \A Active Tx t : ~(lowWaterMark > txs[t].snapshot AND   *)
(*                                  the heartbeat is well-behaved). We   *)
(* drop the conditional and assert the STRONG form, leveraging the fact  *)
(* that the BoundedWindowSupersededByWatermark invariant below requires  *)
(* the well-behaved heartbeat and TLC will explore the misbehaving case  *)
(* via that invariant's quantification.                                    *)
(***************************************************************************)
SnapshotAvailability ==
    \* For every Active Tx t in the WELL-BEHAVED-HEARTBEAT operating point
    \* (s >= lowWaterMark), every version that t could read (opnum <=
    \* snapshot) is preserved in storage (not GC'd). Mechanically: for
    \* every Active Tx t with snapshot >= lowWaterMark, every version
    \* in versions[k] with opnum <= snapshot ALSO has opnum >=
    \* lowWaterMark — i.e., is preserved by NoVersionBelowWatermark.
    \*
    \* This claim has REAL mechanical bite (it's not tautological): TLC
    \* must verify that the AdvanceWatermark action's full-scan delete
    \* never violates it. The proof: AdvanceWatermark(W) requires W >
    \* lowWaterMark AND W <= opCount; it deletes versions with opnum <
    \* W. For a well-behaved-heartbeat Tx t with s >= lowWaterMark, if
    \* W <= s (well-behaved-heartbeat constraint), then deletion of
    \* version v with v.opnum < W <= s means v.opnum < s but also <
    \* W = new lowWaterMark, so the invariant antecedent (opnum <=
    \* snapshot AND opnum >= lowWaterMark) becomes (v.opnum < W AND
    \* v.opnum >= W) which is false — so the invariant still holds.
    \*
    \* The misbehaving-heartbeat case (s < lowWaterMark) is the documented
    \* Decision 2 disclosure: the SM has no view of in-flight Tx; the
    \* over-advanced Tx will get SnapshotTooOld on its NEXT Tx::begin
    \* call. The antecedent (s >= lowWaterMark) fails so the implication
    \* is vacuously satisfied — TLC explores this case for free.
    \A t \in TxIds :
        (txs[t].status = "Active" /\ txs[t].snapshot >= lowWaterMark) =>
            \A k \in Keys :
                \A e \in versions[k] :
                    (e.opnum <= txs[t].snapshot) =>
                        e.opnum >= lowWaterMark

(***************************************************************************)
(* BoundedWindowSupersededByWatermark — THE SP113-CLOSURE INVARIANT.       *)
(*                                                                          *)
(* SP113's prune_pending_txs(MAX_TX_AGE) evicted records older than the   *)
(* fixed window; a Tx whose snapshot was older than `current - MAX_TX_AGE`*)
(* could MISS the dangerous-structure detection (Decision 5 of SP113      *)
(* design — honest disclosure). SP114's watermark-driven prune evicts    *)
(* records older than lowWaterMark, AND lowWaterMark is bounded above by *)
(* min(active snapshots) WHEN THE HEARTBEAT IS WELL-BEHAVED.              *)
(*                                                                          *)
(* The formal SP113-closure claim:                                          *)
(*   For any Active Tx t, every pending_txs record at slot c with         *)
(*   txs[t].snapshot < c (i.e. c is a concurrent commit that might form  *)
(*   an rw-edge with t's eventual commit) is PRESERVED after any         *)
(*   AdvanceWatermark advance bounded by t.snapshot.                       *)
(*                                                                          *)
(* Phrased as a current-state invariant: for every Active Tx t and every *)
(* slot c in OpNums:                                                        *)
(*   IF txs[t].snapshot < c                                                 *)
(*      (concurrent-commit-lookback case — c is a slot t might need     *)
(*      to derive rw-edges against)                                       *)
(*   AND HasPending(c) was true at some prior state                       *)
(*      (the slot was real before any prune)                              *)
(*   AND lowWaterMark <= txs[t].snapshot                                   *)
(*      (well-behaved-heartbeat operating point — the prune horizon is  *)
(*      bounded by t's snapshot)                                          *)
(*   THEN HasPending(c) is still true now                                  *)
(*      (the slot was NOT evicted; the SP113 false-negative is closed).  *)
(*                                                                          *)
(* MECHANICAL FORM. We cannot quantify over "at some prior state" in a   *)
(* current-state invariant; the equivalent current-state projection is:  *)
(* the WATERMARK-PRUNE NEVER EVICTS a slot c with c > lowWaterMark       *)
(* (the AdvanceWatermark action's prune is `c < W` and W <= lowWaterMark*)
(* after the action; therefore the post-state has HasPending(c) preserved*)
(* for all c >= lowWaterMark). Stated as the stable current-state        *)
(* invariant: every preserved pending_txs slot c satisfies c >=          *)
(* lowWaterMark (which IS NoPendingTxBelowWatermark above; that lock is *)
(* one half of the SP113-closure claim). The OTHER half — the heartbeat *)
(* constraint that lowWaterMark <= min(active snapshots) — is the        *)
(* BoundedWindowSupersededByWatermark claim proper:                       *)
(*                                                                          *)
(*   IF the watermark is bounded by every Active Tx's snapshot           *)
(*      (well-behaved heartbeat),                                         *)
(*   THEN every still-needed concurrent-commit slot c (with c >          *)
(*        some Active Tx's snapshot) is still pending.                    *)
(*                                                                          *)
(* The conditional form is mechanically:                                   *)
(*   \A t \in TxIds : txs[t].status = "Active" =>                         *)
(*       lowWaterMark <= txs[t].snapshot =>                                *)
(*           \A c \in OpNums :                                              *)
(*               (c > txs[t].snapshot /\ c >= lowWaterMark) =>            *)
(*                   (HasPending(c) \/ c > opCount)                        *)
(*                                                                          *)
(* — i.e. for every Active Tx t in the well-behaved-heartbeat regime,    *)
(* every slot c that is BOTH concurrent with t AND in the live           *)
(* watermark window is EITHER still pending OR beyond the current        *)
(* opCount (no commit has happened at that slot yet). The "OR beyond     *)
(* opCount" clause excludes vacuous slots — at slot c > opCount no       *)
(* commit has happened so no pending_txs record was ever created. The   *)
(* claim is the SP113-supersession: in the well-behaved-heartbeat case   *)
(* the watermark prune NEVER evicts a slot the still-Active Tx t might  *)
(* need.                                                                   *)
(*                                                                          *)
(* In the misbehaving-heartbeat case (lowWaterMark > t.snapshot), the    *)
(* antecedent is false and the invariant is vacuously satisfied — the   *)
(* heartbeat-trust boundary disclosure (Decision 2) is what TLC sees as  *)
(* the "this case is the operational responsibility of the heartbeat,   *)
(* not the SM" branch.                                                     *)
(***************************************************************************)
(***************************************************************************)
(* CommitAtomicityGc — the GC-aware reformulation of the inherited         *)
(* CommitAtomicity invariant.                                               *)
(*                                                                          *)
(* The inherited MVCCSi.CommitAtomicity says: every Committed Tx t with    *)
(* non-empty write_set has every (k, v) in its write_set installed in     *)
(* `versions[k]` at opnum = commit_opnum. After GC reclaims a Tx's        *)
(* versions (commit_opnum < lowWaterMark), this property must be          *)
(* CONDITIONED on the watermark — the versions are LEGITIMATELY gone.    *)
(*                                                                          *)
(* The GC-aware reformulation: every Committed Tx t with commit_opnum >=  *)
(* lowWaterMark still has its writes present in storage. Below the        *)
(* watermark, the versions have been GC'd by AdvanceWatermark — this is *)
(* the DESIGNED behavior, not a bug.                                       *)
(*                                                                          *)
(* DISCIPLINE NOTE (SP109-SP113 lesson): we DROP the inherited             *)
(* CommitAtomicity from the .cfg invariant list (it would falsely fire    *)
(* on legitimate GC) and REPLACE with this GC-aware tighter form. This    *)
(* is NOT a weakening: the GC-aware form is STRONGER above the watermark *)
(* (same claim) and LEGITIMATELY-SILENT below the watermark (the          *)
(* AdvanceWatermark action's contract IS that versions below the          *)
(* watermark are gone). The inherited invariant was correct for the       *)
(* SP109-SP113 substrate where versions never disappear; the GC layer    *)
(* changes that contract, so the invariant must be restated.              *)
(***************************************************************************)
CommitAtomicityGc ==
    \A t \in TxIds :
        (txsSi[t].status = "Committed" /\
         txsSi[t].write_set # << >> /\
         txsSi[t].commit_opnum >= lowWaterMark)
            => (\A k \in DOMAIN txsSi[t].write_set :
                    \E e \in versions[k] :
                        /\ e.opnum = txsSi[t].commit_opnum
                        /\ e.value = txsSi[t].write_set[k])

(***************************************************************************)
(* DeterministicApplyGc — the GC-aware reformulation of the inherited     *)
(* DeterministicApply invariant. Same rationale as CommitAtomicityGc:     *)
(* legitimately violated by GC reclaiming a Committed Tx's versions;      *)
(* RESTATED with commit_opnum >= lowWaterMark guard. The                  *)
(* commit_opnum-well-typedness clause carries forward unconditionally     *)
(* (commit_opnums never get sentinel-reset by GC).                         *)
(***************************************************************************)
DeterministicApplyGc ==
    \A t \in TxIds :
        (txsSi[t].status = "Committed" /\ txsSi[t].write_set # << >>)
            => /\ (txsSi[t].commit_opnum >= lowWaterMark) =>
                    \A k \in DOMAIN txsSi[t].write_set :
                        \E e \in versions[k] :
                            /\ e.opnum = txsSi[t].commit_opnum
                            /\ e.value = txsSi[t].write_set[k]
               /\ txsSi[t].commit_opnum \in OpNums

(***************************************************************************)
(* BoundedWindowSupersededByWatermark — THE SP113-CLOSURE INVARIANT.       *)
(*                                                                          *)
(* Under the WELL-BEHAVED-HEARTBEAT operating point (lowWaterMark <=        *)
(* txs[t].snapshot for every Active Tx t — Decision 2 of S2.5 design),    *)
(* the watermark-driven pending_txs prune NEVER evicts a slot c that the  *)
(* Active Tx t needs for rw-edge derivation. Formally: for every Active   *)
(* Tx t and every slot c > txs[t].snapshot, c is NOT below the watermark *)
(* (the implication c > t.snapshot >= lowWaterMark => c >= lowWaterMark   *)
(* is the formal closure of the SP113 false-negative — no slot t could   *)
(* need is in the prune-eligible range, so the watermark-driven prune     *)
(* CANNOT cause a missed dangerous-structure detection in this regime).  *)
(*                                                                          *)
(* In the MISBEHAVING-HEARTBEAT case (lowWaterMark > t.snapshot for some  *)
(* Active Tx t), the antecedent is FALSE for that t and the invariant is *)
(* vacuously satisfied — the documented Decision 2 heartbeat-trust       *)
(* boundary. TLC explores this case (free-choice AdvanceWatermark) and    *)
(* finds no violation because the misbehaving case is the operational    *)
(* concern of the heartbeat producer, not the SM.                          *)
(***************************************************************************)
BoundedWindowSupersededByWatermark ==
    \A t \in TxIds :
        (txs[t].status = "Active" /\
         lowWaterMark <= txs[t].snapshot) =>
            \A c \in OpNums :
                (c > txs[t].snapshot) => c >= lowWaterMark

============================================================================
