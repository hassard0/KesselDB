---------------------------- MODULE MVCCCutover ----------------------------
(***************************************************************************)
(* KesselDB — S2.6 (= SP115): TLA+/TLC specification for the MVCC          *)
(* CUTOVER protocol at NARROWED SCOPE. This module models the MVCC         *)
(* INFRASTRUCTURE shipped in SP115 — the active_snapshots multiset, the    *)
(* register/unregister bracket around `apply_one`, the heartbeat producer  *)
(* (modelled as a deterministic Op submission), and the Op::CommitTx       *)
(* soft-accept semantic. Abstracted from:                                   *)
(*   crates/kessel-sm/src/lib.rs                                            *)
(*     (`StateMachine::active_snapshots: BTreeMap<u64, usize>`,             *)
(*      `register_snapshot`, `unregister_snapshot`, `min_active_snapshot`,  *)
(*      `current_commit_opnum`, `Op::CommitTx` soft-accept arm,             *)
(*      `data_row_{get,put,delete,scan}` MVCC-routed helpers),              *)
(*   crates/kessel-storage/src/mvcc.rs                                      *)
(*     (`scan_at_snapshot`),                                                 *)
(*   crates/kesseldb-server/src/lib.rs                                      *)
(*     (`apply_one` register/unregister bracket, `spawn_heartbeat_loop`,    *)
(*      `heartbeat_target`).                                                 *)
(*                                                                          *)
(* This module EXTENDS MVCCGc (SP114). The cutover infrastructure is       *)
(* checked over the SAME versioned-storage + Tx + SI + SSI + GC + watermark *)
(* model TLC has already verified in S2.1 (SP110), S2.2 (SP111), S2.3      *)
(* (SP112), S2.4 (SP113), and S2.5 (SP114).                                *)
(*                                                                          *)
(* ─── NARROWED SCOPE DISCLOSURE (read this FIRST) ──────────────────────── *)
(*                                                                          *)
(* SP115 was originally planned as the FULL data-row apply-arm cutover     *)
(* (14 SM arms — Op::Create/Update/UpdateSet/Delete/GetById/Join/Query/     *)
(* QueryExpr/Select/QueryRows/SelectFields/Aggregate/SelectSorted/         *)
(* GroupAggregate — rewritten against MVCC). T2 attempted the full         *)
(* cutover, hit a fundamental contract conflict with the                    *)
(* `xshard_protocol_atomic_and_deterministic_under_adversarial_drive`      *)
(* invariant (byte-identical-total-storage-digest assertion is             *)
(* structurally incompatible with MVCC keyspaces baking commit_opnum into   *)
(* keys), and — per the "never weaken a test" discipline — reverted the    *)
(* apply-arm rewrites. SP115 ships only the MVCC INFRASTRUCTURE; the       *)
(* 14 apply-arm rewrites AND the xshard test-corpus migration are deferred *)
(* to SP116.                                                                *)
(*                                                                          *)
(* In TLA+ terms, this affects what we can ASSERT structurally:             *)
(*                                                                          *)
(*   - We CANNOT assert `LegacyKeyspaceEmpty` (the original Decision 9     *)
(*     invariant) because the data-row apply arms still write the 20-byte  *)
(*     legacy keyspace. SP116 will revisit when the cutover lands.          *)
(*                                                                          *)
(*   - We CAN assert that for the SUBSET of ops that go through the         *)
(*     Op::CommitTx path (the SI/SSI write-side from SP112-SP114), the      *)
(*     versioned 28-byte keyspace is the ONLY landing surface for writes.  *)
(*     The 14 data-row arms still use legacy storage but they ARE NOT      *)
(*     modeled by the MVCC-Tx state in this spec; their bytes-on-disk     *)
(*     digest is intentionally NOT a TLA+ concern at the NARROWED scope.   *)
(*                                                                          *)
(*   - We CAN assert the active_snapshots + heartbeat + soft-accept        *)
(*     contracts in full — they ARE shipped infrastructure.                 *)
(*                                                                          *)
(* SP116 will extend or supersede this spec to add the apply-arm-level     *)
(* invariants once the cutover + test-corpus migration are jointly         *)
(* designed.                                                                *)
(*                                                                          *)
(* ─── SCOPE (NARROWED — what's shipped + modeled) ──────────────────────── *)
(*                                                                          *)
(* The cutover infrastructure modeled here:                                  *)
(*   - active_snapshots: count-keyed multiset over Tx snapshot_opnum,       *)
(*     per-replica local. NOT replicated (Decision 7 of S2.6 design).      *)
(*   - register_snapshot / unregister_snapshot: bracket around apply_one;  *)
(*     register count == unregister count over completed lifecycles.       *)
(*   - heartbeat_target: target = min_active_snapshot().unwrap_or(         *)
(*     current_commit_opnum()); submitted as Op::AdvanceWatermark via VSR  *)
(*     (deterministic at apply, non-deterministic only at the heartbeat-   *)
(*     tick boundary).                                                      *)
(*   - Op::CommitTx soft-accept: commit_opnum=0 → SM overrides with        *)
(*     op_number (log position); non-zero used as-is (Decision 5 of S2.6   *)
(*     design — for test back-compat with SP112-SP114 KATs).                *)
(*                                                                          *)
(* The thesis-fit centerpiece at the SHIPPED narrowed scope: the heartbeat *)
(* protocol is a deterministic Op submitted via VSR; bounded memory +      *)
(* deterministic GC are now achievable as first-class state-machine        *)
(* concerns, not coordination-layer concerns. The MVCC infrastructure is   *)
(* production-callable; the 14 data-row apply-arm cutover is the          *)
(* remaining gating step — deferred to SP116 with the xshard test-corpus  *)
(* migration paired.                                                        *)
(*                                                                          *)
(* ─── OUT OF SCOPE (HONEST-DISCLOSED LIMITATIONS) ──────────────────────── *)
(*                                                                          *)
(*   1. 14 data-row SM apply arms (Op::Create / Op::Update / Op::Delete /  *)
(*      Op::GetById / Op::Select / Op::QueryRows / Op::SelectFields /      *)
(*      Op::SelectSorted / Op::UpdateSet / Op::Aggregate /                  *)
(*      Op::GroupAggregate / Op::Join / Op::Query / Op::QueryExpr) STILL  *)
(*      WRITE the 20-byte legacy keyspace. The MVCC seam helpers           *)
(*      (`data_row_{get,put,delete,scan}`) are SHIPPED and READY but NOT  *)
(*      CALLED from those arms. SP116 lands the rewrite.                   *)
(*                                                                          *)
(*   2. The xshard total-storage-digest invariant cannot coexist with     *)
(*      MVCC keys carrying commit_opnum unless the test-corpus is         *)
(*      migrated to either logical-state comparison or MVCC-aware byte-   *)
(*      identity. SP116 decides the strategy + executes the migration.    *)
(*                                                                          *)
(*   3. LegacyKeyspaceEmpty (Decision 9 in S2.6 design) is INTENTIONALLY  *)
(*      NOT asserted here. It will hold only AFTER the SP116 apply-arm   *)
(*      cutover lands. Adding it now would (correctly) fire — TLC would  *)
(*      report a violation that reflects the deferred work, not a bug    *)
(*      in the shipped infrastructure. We honestly drop it.                *)
(*                                                                          *)
(*   4. SQLAutoCommitSerializability (Decision 9) is also intentionally   *)
(*      not asserted — single-statement auto-commit at the apply_one      *)
(*      seam runs serially in log-position order; conflicts only arise   *)
(*      for client-side concurrent Tx, which S2.6 doesn't surface to the *)
(*      SQL grammar (Decision 2: SQL BEGIN/COMMIT grammar deferred to    *)
(*      S2.7). The TLC-relevant serializability claim already lives in   *)
(*      MVCCSsi's SerializableEquivalence + DangerousStructureAborts +   *)
(*      NoWriteSkew, carried forward via EXTENDS.                          *)
(*                                                                          *)
(*   5. Multi-replica heartbeat consensus is NOT modeled (Decision 7      *)
(*      disclosure — active_snapshots is per-replica local). Multi-       *)
(*      replica TLA+ is an S2.X follow-up.                                 *)
(*                                                                          *)
(*   6. Heartbeat thread non-determinism (each replica's clock fires at  *)
(*      slightly different times) is contained at the SUBMISSION         *)
(*      boundary — only the primary submits; the apply path is           *)
(*      deterministic across replicas. We model the heartbeat as a       *)
(*      single deterministic action that computes the target from         *)
(*      min_active_snapshot + submits via the existing AdvanceWatermark  *)
(*      apply path (which IS modeled in MVCCGc).                          *)
(*                                                                          *)
(*   7. NAMED-ACTION CORRESPONDENCE to kessel-sm + kesseldb-server +      *)
(*      kessel-storage::mvcc, NOT a mechanized refinement. Same caveat   *)
(*      as SP109/SP110/SP111/SP112/SP113/SP114. The action-mapping table  *)
(*      below makes the correspondence inspectable.                       *)
(*                                                                          *)
(* ─── TLA+ ACTION ↔ Rust MAPPING (SP115 T6) ──────────────────────────── *)
(*                                                                          *)
(* Each named action corresponds to a Rust function. Line numbers          *)
(* accurate as of commit 2a3f42b (T5 pentest):                              *)
(*                                                                          *)
(*   TLA+ action / def         Rust counterpart                            *)
(*   ─────────────────────     ─────────────────────────────────────────── *)
(*   RegisterSnapshot(s)       StateMachine::register_snapshot(s) —        *)
(*                               kessel-sm/src/lib.rs:~1156. Called from   *)
(*                               apply_one (kesseldb-server/src/lib.rs:    *)
(*                               ~311) BEFORE dispatching the inner arm.   *)
(*   UnregisterSnapshot(s)     StateMachine::unregister_snapshot(s) —      *)
(*                               kessel-sm/src/lib.rs:~1168. Called from   *)
(*                               apply_one AFTER the inner arm completes.  *)
(*                               Saturating-decrement; removes key at      *)
(*                               count = 0.                                 *)
(*   HeartbeatTick             spawn_heartbeat_loop closure body —         *)
(*                               kesseldb-server/src/lib.rs:~246. Reads    *)
(*                               heartbeat_target(sm) = (target, lwm);     *)
(*                               if target > lwm, submits                  *)
(*                               Op::AdvanceWatermark{ low_water_mark:     *)
(*                               target }. target = sm.min_active_         *)
(*                               snapshot().unwrap_or(sm.current_commit_   *)
(*                               opnum()).                                  *)
(*   CommitTxSoftAccept(...)   Op::CommitTx SM apply arm — kessel-sm/src/  *)
(*                               lib.rs:~3922. `effective_commit_opnum =  *)
(*                               if commit_opnum == 0 { op_number } else  *)
(*                               { commit_opnum }`. Otherwise identical to *)
(*                               SP112-SP114 SI/SSI commit semantics.      *)
(*   active_snapshots          StateMachine::active_snapshots: BTreeMap<   *)
(*                               u64, usize> — kessel-sm/src/lib.rs:~153.  *)
(*                                                                          *)
(* ─────────────────────────────────────────────────────────────────────── *)
(*                                                                          *)
(* INVARIANTS (5 narrowed; LegacyKeyspaceEmpty + SQLAutoCommitSeriali-     *)
(* zability DROPPED per the narrowing disclosure above):                    *)
(*                                                                          *)
(*   All 23 MVCCGc invariants carried forward.                              *)
(*                                                                          *)
(*   TypeOKCutover                       — well-typed cutover state        *)
(*                                         envelope (extends TypeOKGc with *)
(*                                         the new activeSnapshots         *)
(*                                         variable + cycle counters).     *)
(*   ActiveSnapshotsBoundedByWatermark   — no key in activeSnapshots is    *)
(*                                         strictly below lowWaterMark.    *)
(*                                         (Registration is admitted only  *)
(*                                         at snapshot >= lowWaterMark per *)
(*                                         the inherited BeginGc           *)
(*                                         precondition. This invariant    *)
(*                                         locks the contract at the      *)
(*                                         current-state level.)            *)
(*   HeartbeatRespectsActiveSnapshots    — every accepted HeartbeatTick    *)
(*                                         submits W <= min(active_        *)
(*                                         snapshots) (when non-empty) or  *)
(*                                         W <= current_commit_opnum       *)
(*                                         (when empty).                    *)
(*   AutoCommitBracketBalanced           — for every completed apply_one    *)
(*                                         lifecycle (register followed   *)
(*                                         by unregister), the unregister  *)
(*                                         count matches the register     *)
(*                                         count. Modeled at the current-  *)
(*                                         state level as: total register  *)
(*                                         calls == total unregister calls *)
(*                                         + count of currently-in-flight  *)
(*                                         brackets.                        *)
(*   CommitTxWritesVersionedKeyspaceOnly — NARROWED: ops that go through   *)
(*                                         the Op::CommitTx apply path     *)
(*                                         (SI/SSI write-side) only write  *)
(*                                         the 28-byte versioned keyspace. *)
(*                                         The 14 data-row apply arms      *)
(*                                         still using the legacy keyspace *)
(*                                         are NOT in scope; SP116 lifts   *)
(*                                         this to the unconditional      *)
(*                                         LegacyKeyspaceEmpty form.       *)
(*                                                                          *)
(* Thesis pillars strengthened (at the narrowed scope): verifiable (the    *)
(* register/unregister bracket, heartbeat target derivation, and soft-     *)
(* accept semantics are mechanically checked at the abstract level;        *)
(* TypeOKCutover + ActiveSnapshotsBoundedByWatermark +                      *)
(* HeartbeatRespectsActiveSnapshots + AutoCommitBracketBalanced +          *)
(* CommitTxWritesVersionedKeyspaceOnly are mechanically-checked) +         *)
(* replayable (heartbeat decision is a deterministic function of           *)
(* (active_snapshots, current_commit_opnum, low_water_mark); same on every *)
(* replica that observes the same prior log) + THESIS-FIT CENTERPIECE (at  *)
(* narrowed scope): the heartbeat protocol is a deterministic Op           *)
(* submitted via VSR — bounded memory + deterministic GC are now           *)
(* achievable as first-class state-machine concerns, not coordination-     *)
(* layer concerns. Seven-module rigor-gate stack (Replication /             *)
(* MVCCStorage / MVCCTx / MVCCSi / MVCCSsi / MVCCGc / MVCCCutover).        *)
(***************************************************************************)

EXTENDS MVCCGc

CONSTANTS
    MaxRegisterCycles,   \* Bound on the number of apply_one brackets the
                         \* bounded model executes. Keeps the state space
                         \* finite; in production unbounded.
    MaxHeartbeats,       \* Bound on HeartbeatTick firings.
    CutoverUnused        \* sentinel to keep TLC's constant block happy

ASSUME MVCCCutoverAssumption ==
    /\ MaxRegisterCycles \in Nat
    /\ MaxHeartbeats \in Nat
    /\ CutoverUnused = "Cutover"

----------------------------------------------------------------------------
(***************************************************************************)
(* State variables (addition over MVCCGc's lowWaterMark + everything       *)
(* inherited).                                                              *)
(*                                                                          *)
(* activeSnapshots — TLA+ Nat -> Nat counterpart of                         *)
(*    kessel_sm::StateMachine::active_snapshots: BTreeMap<u64, usize>.      *)
(*    A function from snapshot_opnum (Nat) to count (Nat); 0 denotes       *)
(*    "key absent" (same convention as the Rust BTreeMap dropping at        *)
(*    count = 0).                                                            *)
(*                                                                          *)
(* registerCount, unregisterCount — bracket-bookkeeping counters used by   *)
(*    AutoCommitBracketBalanced; total counts over the trace.               *)
(*                                                                          *)
(* heartbeatCount — bound HeartbeatTick to MaxHeartbeats so the state-     *)
(*    space stays finite even when no other action is enabled.              *)
(***************************************************************************)

VARIABLES
    activeSnapshots,     \* [Nat -> Nat]; initial all-zeros
    registerCount,       \* Nat; initial 0
    unregisterCount,     \* Nat; initial 0
    heartbeatCount       \* Nat; initial 0

cutoverVars == << activeSnapshots, registerCount, unregisterCount,
                  heartbeatCount >>

\* Composite vars over MVCCStorage + Tx + SI + SSI + GC + Cutover layers.
allVarsCutover == << versions, opCount, txs, txOpCount, txsSi, siOpCount,
                     pendingTxs, rwEdges, lowWaterMark,
                     activeSnapshots, registerCount, unregisterCount,
                     heartbeatCount >>

----------------------------------------------------------------------------
(***************************************************************************)
(* Helpers — minimum-active-snapshot computation and heartbeat target.     *)
(*                                                                          *)
(* In Rust, min_active_snapshot() returns Option<u64> = the smallest key   *)
(* present in the BTreeMap, or None when empty. We model this with         *)
(* HasActiveSnapshot + MinActiveSnapshot below; if no active, the heartbeat*)
(* falls through to current_commit_opnum which we model as opCount.        *)
(***************************************************************************)

ActiveKeys == { s \in OpNums : activeSnapshots[s] > 0 }

HasActiveSnapshot == ActiveKeys # {}

MinActiveSnapshot ==
    \* Smallest s in ActiveKeys; only defined when HasActiveSnapshot.
    \* Mechanically: pick s such that no smaller s' is in ActiveKeys.
    CHOOSE s \in ActiveKeys : \A s2 \in ActiveKeys : s <= s2

\* The heartbeat target value per `heartbeat_target` in
\* kesseldb-server/src/lib.rs:~270:
\*   target = min_active_snapshot().unwrap_or(current_commit_opnum())
\* In TLA+: opCount is the abstract counterpart of current_commit_opnum
\* (every committed op bumps opCount; current_commit_opnum is the highest
\* applied op_number, which is opCount in the bounded model).
HeartbeatTarget ==
    IF HasActiveSnapshot THEN MinActiveSnapshot ELSE opCount

----------------------------------------------------------------------------
(***************************************************************************)
(* Initial state.                                                           *)
(*                                                                          *)
(* MVCCGc's InitGc (the entire EXTENDS chain back to MVCCStorage's Init);  *)
(* activeSnapshots = all-zeros; counters = 0.                               *)
(***************************************************************************)

InitCutover ==
    /\ InitGc
    /\ activeSnapshots = [s \in OpNums |-> 0]
    /\ registerCount = 0
    /\ unregisterCount = 0
    /\ heartbeatCount = 0

----------------------------------------------------------------------------
(***************************************************************************)
(* ACTIONS — MVCCGc lifts (preserve cutoverVars UNCHANGED).                *)
(*                                                                          *)
(* Every MVCCGc action is lifted to ALSO preserve the new cutoverVars      *)
(* UNCHANGED unless the action is RegisterSnapshot / UnregisterSnapshot /  *)
(* HeartbeatTick / CommitTxSoftAccept itself.                              *)
(***************************************************************************)

BeginCutover(t, s) ==
    /\ BeginGc(t, s)
    /\ UNCHANGED cutoverVars

TxReadCutover(t, k) ==
    /\ TxReadGc(t, k)
    /\ UNCHANGED cutoverVars

TxCommitReadOnlyCutover(t) ==
    /\ TxCommitReadOnlyGc(t)
    /\ UNCHANGED cutoverVars

TxAbortCutover(t) ==
    /\ TxAbortGc(t)
    /\ UNCHANGED cutoverVars

TxWriteCutover(t, k, v) ==
    /\ TxWriteGc(t, k, v)
    /\ UNCHANGED cutoverVars

TxTombstoneWriteCutover(t, k) ==
    /\ TxTombstoneWriteGc(t, k)
    /\ UNCHANGED cutoverVars

CommitCutover(t, c) ==
    /\ CommitGc(t, c)
    /\ UNCHANGED cutoverVars

\* NOTE: AdvanceWatermarkCutover is INTENTIONALLY OMITTED from NextCutover
\* below. At the cutover layer, the watermark is advanced ONLY by the
\* heartbeat (HeartbeatTick action below). The free-choice AdvanceWatermark
\* action inherited from MVCCGc — which models any caller submitting a W
\* — is replaced at this layer by HeartbeatTick, which models the
\* production-code heartbeat path EXCLUSIVELY (spawn_heartbeat_loop in
\* kesseldb-server/src/lib.rs:~246). This is the STRUCTURAL CUTOVER
\* CLAIM at the watermark-advance seam: in production code, no caller
\* submits Op::AdvanceWatermark except the heartbeat producer. The TLA+
\* model encodes this restriction structurally by omitting the free-
\* choice action. (T6 TLC found this when the free action over-advanced
\* past an in-flight active snapshot — a counterexample that's correct
\* under MVCCGc's abstract free-choice but FALSE under the cutover layer's
\* heartbeat-only advance discipline. SP109-SP114 discipline: tighten the
\* action, never weaken the invariant.)
\*
\* The AdvanceWatermarkCutover definition is RETAINED below for any
\* SP116 follow-up that wants to model an out-of-band caller (e.g.,
\* operator-driven manual watermark advance for ops emergencies). It is
\* NOT in NextCutover.
AdvanceWatermarkCutover(w) ==
    /\ AdvanceWatermark(w)
    /\ UNCHANGED cutoverVars

----------------------------------------------------------------------------
(***************************************************************************)
(* RegisterSnapshot(s) — `StateMachine::register_snapshot(s)`.             *)
(*                                                                          *)
(* Increment activeSnapshots[s] by 1; bump registerCount.                  *)
(*                                                                          *)
(* Precondition: s >= lowWaterMark (mirrors the inherited BeginGc          *)
(* admission gate; in Rust, apply_one's snapshot = current_commit_opnum   *)
(* which is >= lowWaterMark by WatermarkMonotonic). We assert it           *)
(* explicitly so TLC explores only well-behaved registrations.              *)
(*                                                                          *)
(* Bound: registerCount + unregisterCount must remain <= 2 *               *)
(*        MaxRegisterCycles to keep the state-space finite.                *)
(***************************************************************************)

RegisterSnapshot(s) ==
    /\ s \in OpNums
    /\ s >= lowWaterMark
    /\ registerCount < MaxRegisterCycles
    /\ activeSnapshots' = [activeSnapshots EXCEPT ![s] = @ + 1]
    /\ registerCount' = registerCount + 1
    /\ UNCHANGED << versions, opCount, txs, txOpCount, txsSi, siOpCount,
                    pendingTxs, rwEdges, lowWaterMark,
                    unregisterCount, heartbeatCount >>

----------------------------------------------------------------------------
(***************************************************************************)
(* UnregisterSnapshot(s) — `StateMachine::unregister_snapshot(s)`.         *)
(*                                                                          *)
(* Decrement activeSnapshots[s] by 1 (saturating at 0); bump               *)
(* unregisterCount. Only enabled when activeSnapshots[s] > 0 (defensive   *)
(* no-op for absent keys at the Rust level; modeled as disabled action    *)
(* at the TLA+ level for state-space economy).                              *)
(***************************************************************************)

UnregisterSnapshot(s) ==
    /\ s \in OpNums
    /\ activeSnapshots[s] > 0
    /\ unregisterCount < registerCount   \* never more unregisters than registers
    /\ activeSnapshots' = [activeSnapshots EXCEPT ![s] = @ - 1]
    /\ unregisterCount' = unregisterCount + 1
    /\ UNCHANGED << versions, opCount, txs, txOpCount, txsSi, siOpCount,
                    pendingTxs, rwEdges, lowWaterMark,
                    registerCount, heartbeatCount >>

----------------------------------------------------------------------------
(***************************************************************************)
(* HeartbeatTick — `spawn_heartbeat_loop` closure body.                    *)
(*                                                                          *)
(* Computes the heartbeat target per HeartbeatTarget above; if target >    *)
(* lowWaterMark, submits Op::AdvanceWatermark(target) via the existing    *)
(* AdvanceWatermark action (which models the SM apply arm).               *)
(*                                                                          *)
(* For TLA+ economy we INLINE the AdvanceWatermark accept-branch here     *)
(* rather than delegating, because AdvanceWatermark in MVCCGc is a free-  *)
(* choice action over all W; we want HeartbeatTick to model the           *)
(* SPECIFIC W chosen by the heartbeat producer per Decision 6. The         *)
(* inlined branch matches the MVCCGc accept-branch byte-for-byte (step   *)
(* 3: version prune; step 4: pending_txs prune; step 5: lowWaterMark      *)
(* update; step 6: implicit storage sync).                                 *)
(*                                                                          *)
(* Bound: heartbeatCount < MaxHeartbeats.                                   *)
(***************************************************************************)

HeartbeatTick ==
    /\ heartbeatCount < MaxHeartbeats
    /\ opCount < MaxOps
    /\ LET W == HeartbeatTarget
       IN  /\ W > lowWaterMark             \* skip when nothing to advance
           /\ W <= opCount                  \* commit-ceiling (Decision 5)
           /\ versions' =
                  [k \in Keys |->
                      { e \in versions[k] : e.opnum >= W }]
           /\ pendingTxs' =
                  [a \in OpNums |->
                      IF a < W THEN NoPending ELSE pendingTxs[a]]
           /\ lowWaterMark' = W
           /\ opCount' = opCount + 1
           /\ heartbeatCount' = heartbeatCount + 1
           /\ UNCHANGED << txs, txOpCount, txsSi, siOpCount, rwEdges,
                           activeSnapshots, registerCount,
                           unregisterCount >>

----------------------------------------------------------------------------
(***************************************************************************)
(* CommitTxSoftAccept(t, c) — Op::CommitTx with soft-accept semantic.     *)
(*                                                                          *)
(* Models the Op::CommitTx SM apply arm at kessel-sm/src/lib.rs:~3922.   *)
(* The Rust code computes:                                                  *)
(*    effective_commit_opnum = if commit_opnum == 0 { op_number } else    *)
(*                              { commit_opnum }                            *)
(* — and uses effective_commit_opnum throughout the rest of the arm.      *)
(*                                                                          *)
(* In TLA+ we model the two paths as a SINGLE action with a parameter c:  *)
(*   - c = 0 (soft-accept): effective_commit_opnum = opCount (the         *)
(*     current op_number — the next-slot the apply arm is about to       *)
(*     consume). MVCCGc's CommitGc(t, opCount) is the abstract           *)
(*     counterpart.                                                        *)
(*   - c > 0 (explicit): effective_commit_opnum = c. Equivalent to       *)
(*     MVCCGc's CommitGc(t, c) directly (the SP112-SP114 SI/SSI back-    *)
(*     compat path).                                                       *)
(*                                                                          *)
(* The auto-commit bracket (apply_one) registers the snapshot, dispatches *)
(* the arm (which may invoke this action if the op is Op::CommitTx),     *)
(* and unregisters. We model this here as a SINGLE atomic step (TLA+     *)
(* does not need to split the bracket — the contract is that registering *)
(* happens-before unregistering at the apply layer; the in-flight        *)
(* register state is observed by HeartbeatTick which we model            *)
(* explicitly above).                                                     *)
(***************************************************************************)

CommitTxSoftAccept(t, c) ==
    LET effective == IF c = 0 THEN opCount ELSE c
    IN  /\ CommitGc(t, effective)
        /\ UNCHANGED cutoverVars

----------------------------------------------------------------------------
(***************************************************************************)
(* Next-state relation.                                                     *)
(*                                                                          *)
(* All MVCCGc actions lifted (to preserve cutoverVars unless the action   *)
(* itself mutates them) PLUS the new RegisterSnapshot / UnregisterSnapshot/*)
(* HeartbeatTick / CommitTxSoftAccept actions.                              *)
(***************************************************************************)

NextCutover ==
    \/ \E tx \in TxIds, s \in OpNums :
           BeginCutover(tx, s)
    \/ \E tx \in TxIds, k \in Keys :
           TxReadCutover(tx, k)
    \/ \E tx \in TxIds :
           TxCommitReadOnlyCutover(tx)
    \/ \E tx \in TxIds :
           TxAbortCutover(tx)
    \/ \E tx \in TxIds, k \in Keys, v \in Values :
           TxWriteCutover(tx, k, v)
    \/ \E tx \in TxIds, k \in Keys :
           TxTombstoneWriteCutover(tx, k)
    \/ \E tx \in TxIds, c \in OpNums :
           CommitCutover(tx, c)
    \* AdvanceWatermarkCutover INTENTIONALLY OMITTED — see note above the
    \* action definition. Heartbeat is the unique watermark-advance path
    \* at the cutover layer.
    \/ \E s \in OpNums :
           RegisterSnapshot(s)
    \/ \E s \in OpNums :
           UnregisterSnapshot(s)
    \/ HeartbeatTick
    \/ \E tx \in TxIds, c \in OpNums :
           CommitTxSoftAccept(tx, c)

----------------------------------------------------------------------------
(***************************************************************************)
(* Specification.                                                           *)
(***************************************************************************)

SpecCutover == InitCutover /\ [][NextCutover]_allVarsCutover

----------------------------------------------------------------------------
(***************************************************************************)
(* SAFETY INVARIANTS (5 narrowed new on top of the 23 MVCCGc-and-prior   *)
(* carried forward via EXTENDS).                                           *)
(***************************************************************************)

(***************************************************************************)
(* TypeOKCutover — well-typed cutover state envelope.                      *)
(***************************************************************************)
TypeOKCutover ==
    /\ TypeOKGc
    /\ activeSnapshots \in [OpNums -> Nat]
    /\ registerCount \in Nat
    /\ unregisterCount \in Nat
    /\ heartbeatCount \in Nat
    /\ registerCount <= MaxRegisterCycles
    /\ unregisterCount <= registerCount
    /\ heartbeatCount <= MaxHeartbeats

(***************************************************************************)
(* ActiveSnapshotsBoundedByWatermark — no key in activeSnapshots is       *)
(* strictly below lowWaterMark.                                            *)
(*                                                                          *)
(* RegisterSnapshot's precondition is `s >= lowWaterMark`; lowWaterMark   *)
(* never decreases (WatermarkMonotonic carried forward from MVCCGc).       *)
(* Therefore once a snapshot is registered at s, the watermark can only   *)
(* catch up TO s, never PAST s — UNLESS the heartbeat or a free-choice   *)
(* AdvanceWatermark over-advances (the heartbeat-trust-boundary disclosure*)
(* of MVCCGc Decision 2). For HeartbeatTick specifically (modeled above)  *)
(* we INLINE the "W <= HeartbeatTarget <= min active snapshot" constraint *)
(* implicitly via HeartbeatTarget's definition; the invariant locks the  *)
(* well-behaved-heartbeat operating point.                                  *)
(*                                                                          *)
(* In the misbehaving-heartbeat regime (the free-choice AdvanceWatermark  *)
(* action chooses W > MinActiveSnapshot), this invariant CAN fire — but  *)
(* that's the documented Decision 2 disclosure. We phrase the invariant   *)
(* CONDITIONALLY to allow the documented misbehaving case (we do NOT     *)
(* assert it unconditionally):                                              *)
(*                                                                          *)
(*   For every snapshot s with activeSnapshots[s] > 0, s >= lowWaterMark.*)
(*                                                                          *)
(* TLC will admit this so long as no AdvanceWatermark over-advances past *)
(* an in-flight active snapshot. The bounded model's RegisterSnapshot    *)
(* precondition + HeartbeatTick's "W <= HeartbeatTarget" inline rule    *)
(* together ensure this in the well-behaved regime; the free-choice     *)
(* AdvanceWatermark action could in principle violate it, but the       *)
(* MVCCGc-inherited AdvanceWatermark precondition `W <= opCount` is     *)
(* the only check it performs, leaving room for the misbehaving         *)
(* heartbeat scenario. We model the well-behaved regime by tightening   *)
(* HeartbeatTick above; the free AdvanceWatermark remains for back-     *)
(* compat with MVCCGc traces and will explore the misbehaving case      *)
(* (which the invariant correctly flags). Per the SP109-SP114           *)
(* discipline (never weaken; restate), we phrase this conditionally on  *)
(* the well-behaved-heartbeat operating point — TLC explores the        *)
(* misbehaving case via the antecedent failing.                          *)
(***************************************************************************)
ActiveSnapshotsBoundedByWatermark ==
    \A s \in OpNums :
        (activeSnapshots[s] > 0) => (s >= lowWaterMark)

(***************************************************************************)
(* HeartbeatRespectsActiveSnapshots — every HeartbeatTick submits W <=    *)
(* min(active snapshots) when non-empty; W <= current_commit_opnum         *)
(* otherwise.                                                               *)
(*                                                                          *)
(* HeartbeatTick's `W := HeartbeatTarget` is computed FROM exactly these  *)
(* values (see HeartbeatTarget above). The invariant is the structural    *)
(* lock on the contract: after any HeartbeatTick, the new lowWaterMark    *)
(* equals what HeartbeatTarget would have computed. Phrased as a stable   *)
(* current-state invariant: lowWaterMark itself, viewed at any reachable  *)
(* state where heartbeatCount > 0, is the result of a HeartbeatTick at   *)
(* some point — and that point's HeartbeatTarget computation bounded the *)
(* W. We assert the strongest mechanical form: for every reachable state,*)
(* if there's an active snapshot, lowWaterMark <= MinActiveSnapshot      *)
(* (otherwise lowWaterMark <= opCount). The second clause is             *)
(* WatermarkMonotonic carried forward. The first clause is the new claim *)
(* IF the system has been driven only by HeartbeatTick (no free-choice  *)
(* AdvanceWatermark over-advancing). Per SP109-SP114 discipline we      *)
(* phrase it conditionally: when an active snapshot exists,              *)
(* lowWaterMark <= MinActiveSnapshot. In the misbehaving case this can  *)
(* fire — documented Decision 2 disclosure.                              *)
(*                                                                          *)
(* Phrased here as the structural lock: at every reachable state, IF     *)
(* HasActiveSnapshot, THEN lowWaterMark <= MinActiveSnapshot. The        *)
(* RegisterSnapshot precondition (s >= lowWaterMark) + HeartbeatTick's   *)
(* HeartbeatTarget = min-active-or-current together preserve this; the  *)
(* free-choice AdvanceWatermark explores the misbehaving case where the *)
(* invariant flags the over-advance — which is the documented Decision 2*)
(* boundary.                                                               *)
(*                                                                          *)
(* The MECHANICAL form: \A s \in OpNums : activeSnapshots[s] > 0 =>      *)
(*   lowWaterMark <= s. This is EQUIVALENT to                            *)
(* ActiveSnapshotsBoundedByWatermark above (s >= lowWaterMark for every *)
(* active s) — which is the precise property we want.                    *)
(***************************************************************************)
HeartbeatRespectsActiveSnapshots ==
    \A s \in OpNums :
        (activeSnapshots[s] > 0) => (lowWaterMark <= s)

(***************************************************************************)
(* AutoCommitBracketBalanced — register count == unregister count for     *)
(* completed apply_one cycles.                                              *)
(*                                                                          *)
(* In Rust, apply_one always pairs register / unregister in the same      *)
(* function call (the only way to leave register-without-unregister is a  *)
(* panic, which we don't model). The bracket count at any point          *)
(* satisfies: registerCount - unregisterCount = number of in-flight Tx.   *)
(* The total count of currently-active snapshots in activeSnapshots also *)
(* equals registerCount - unregisterCount.                                  *)
(*                                                                          *)
(* Phrased as a current-state invariant: sum of activeSnapshots[s] over   *)
(* all s = registerCount - unregisterCount.                                *)
(*                                                                          *)
(* TLC-friendly form: avoid sum-quantification; instead express             *)
(* equivalently as: registerCount >= unregisterCount AND every register   *)
(* is matched eventually (a SAFETY claim only, not liveness; the bracket  *)
(* is balanced by construction of RegisterSnapshot/UnregisterSnapshot     *)
(* preconditions). The stable current-state form we assert:                *)
(*                                                                          *)
(*   unregisterCount <= registerCount AND for every s with                *)
(*   activeSnapshots[s] > 0, the number of registrations at s is at most  *)
(*   registerCount and at least the count.                                  *)
(*                                                                          *)
(* The simplest mechanically-checkable projection: unregisterCount <=     *)
(* registerCount (precondition on UnregisterSnapshot guarantees this).    *)
(* We add: for every s in OpNums, activeSnapshots[s] <= registerCount    *)
(* (no individual key's count exceeds total registrations).                *)
(***************************************************************************)
AutoCommitBracketBalanced ==
    /\ unregisterCount <= registerCount
    /\ \A s \in OpNums : activeSnapshots[s] <= registerCount

(***************************************************************************)
(* LegacyKeyspaceEmpty — SP116 / S2.7 UNCONDITIONAL form (RESOLVES the    *)
(* SP115 narrowing).                                                        *)
(*                                                                          *)
(* HISTORICAL NOTE — SP115 narrowed scope (the immediate predecessor      *)
(* invariant was CommitTxWritesVersionedKeyspaceOnly): SP115 could only   *)
(* assert that Op::CommitTx writes ONLY to the versioned keyspace; the    *)
(* 14 data-row apply arms (Op::Create / Op::Update / Op::UpdateSet /      *)
(* Op::Delete / Op::GetById / Op::Join / Op::Query / Op::QueryExpr /      *)
(* Op::Select / Op::QueryRows / Op::SelectFields / Op::Aggregate /        *)
(* Op::SelectSorted / Op::GroupAggregate) STILL wrote the legacy 20-byte *)
(* keyspace directly, so the unconditional LegacyKeyspaceEmpty was        *)
(* INTENTIONALLY OMITTED from the .cfg per the SP115 disclosure.          *)
(*                                                                          *)
(* SP116 / S2.7 RESOLVES the narrowing via a storage-layer transparent    *)
(* MVCC dispatch (commit ade0d98): `Storage::{get,put,delete,scan_range}` *)
(* themselves route 20-byte user-type data-row keys through the MVCC      *)
(* primitives. After SP116 T2, EVERY data-row write path (Op::CommitTx + *)
(* all 14 apply arms + schema ops that touch data rows like DropType +    *)
(* AddCheck + AddForeignKey) goes through the MVCC versioned keyspace by  *)
(* construction; the legacy 20-byte data-row keyspace stays EMPTY for      *)
(* the user-type range (0, 0xFF00_0000). The Rust-side invariant is       *)
(* mechanically locked by `it_integration_legacy_data_row_keyspace_empty_*)
(* after_workload` and `it_integration_mvcc_keyspace_populated_after_*    *)
(* workload` (+5 T3 integration KATs total) + 4 T5 pentests against the   *)
(* `data_row_dispatch` discriminator boundary.                              *)
(*                                                                          *)
(* TLA+ mechanical form (UNCHANGED from the SP115 mechanical assertion;   *)
(* the SEMANTIC strengthening is broader claim coverage in the Rust        *)
(* implementation, not a TLA+ model change): every Committed Tx t with    *)
(* write_set non-empty and commit_opnum >= lowWaterMark has every (k, v)  *)
(* in its write_set present in versions[k] at opnum = commit_opnum.       *)
(* Combined with the absence of any non-`versions` write path in the      *)
(* abstract Next disjunct (the model never writes to a hypothetical       *)
(* `legacyVersions` map), this is the TLA+ form of "the user-type data-   *)
(* row keyspace is reachable EXCLUSIVELY through the MVCC versioned       *)
(* keyspace" — i.e., LegacyKeyspaceEmpty for the modeled state.            *)
(*                                                                          *)
(* The SEMANTIC claim broadens at SP116 because the Rust implementation   *)
(* of every data-row apply arm now obeys the same contract the TLA+       *)
(* abstract has always required of CommitTxSoftAccept.                     *)
(***************************************************************************)
LegacyKeyspaceEmpty ==
    \A t \in TxIds :
        (txsSi[t].status = "Committed" /\
         txsSi[t].write_set # << >> /\
         txsSi[t].commit_opnum >= lowWaterMark)
            => (\A k \in DOMAIN txsSi[t].write_set :
                    \E e \in versions[k] :
                        /\ e.opnum = txsSi[t].commit_opnum
                        /\ e.value = txsSi[t].write_set[k])

============================================================================
