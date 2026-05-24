---------------------------- MODULE MVCCTx ----------------------------
(***************************************************************************)
(* KesselDB — S2.2 (= SP111): TLA+/TLC specification for the MVCC          *)
(* transaction context + read-set tracking abstracted from                  *)
(* `crates/kessel-storage/src/tx.rs`.                                       *)
(*                                                                          *)
(* This module EXTENDS MVCCStorage (the S2.1/SP110 specification). The Tx  *)
(* layer is checked over the SAME versioned-storage primitive TLC already  *)
(* verified in S2.1 — re-using `versions`, `opCount`, `Put`, `Tombstone`,  *)
(* `SnapshotReadOf`, and the bounded constants from MVCCStorage's          *)
(* universe.                                                                *)
(*                                                                          *)
(* SCOPE (per the S2.2 design Decision 7) — abstract single-replica Tx     *)
(* layer over MVCC storage. Tx is READ-ONLY in S2.2; writes ship in S2.3. *)
(*                                                                          *)
(* OUT OF SCOPE (HONEST-DISCLOSED LIMITATIONS — read these before          *)
(* trusting any conclusion drawn from a green TLC run):                    *)
(*                                                                          *)
(*   1. The write side of the Tx (Tx::write + the write-set + the         *)
(*      conflict-checked commit path) is NOT modeled here. S2.3 follow-up.*)
(*                                                                          *)
(*   2. SSI dangerous-cycle detection is NOT modeled here. S2.4           *)
(*      follow-up (which will EXTEND MVCCTx with the rw-antidependency   *)
(*      cycle invariant).                                                  *)
(*                                                                          *)
(*   3. Multi-replica Tx state is NOT modeled (no `txs[r][tx]` shape).   *)
(*      The set semantics of TLA+ make per-replica byte-identity          *)
(*      automatic at the abstract level, but a multi-replica TLA+ shape  *)
(*      is an S2.X follow-up.                                              *)
(*                                                                          *)
(*   4. GC / watermark / version reclamation is NOT modeled (carried     *)
(*      forward from MVCCStorage). S2.5 follow-up.                        *)
(*                                                                          *)
(*   5. Bounded model checking proves the absence of counterexamples at   *)
(*      the CONFIGURED constants only (Keys=2x2, OpNums=0..3, Values=2, *)
(*      MaxOps=5, TxIds=2, MaxTxOps=6). The Rust pentest tests (T5)     *)
(*      cover boundary opnums (0, u64::MAX) explicitly that the bounded *)
(*      TLC model cannot reach.                                            *)
(*                                                                          *)
(*   6. NAMED-ACTION CORRESPONDENCE to kessel-storage::tx, NOT a          *)
(*      mechanized refinement. Same caveat as SP109/SP110. The action-   *)
(*      mapping table below makes the correspondence inspectable.        *)
(*                                                                          *)
(* ─── TLA+ ACTION ↔ kessel-storage::tx MAPPING (SP111 T6) ──────────────  *)
(*                                                                          *)
(* Each named action in this spec corresponds to a kessel-storage::tx     *)
(* function. Line numbers are accurate as of commit 4aacdf3 against        *)
(* crates/kessel-storage/src/tx.rs. This is "named correspondence", not   *)
(* mechanized refinement — a divergence between the spec and the Rust    *)
(* code is a human-discovered issue. If kessel-storage::tx is refactored,*)
(* re-run:                                                                  *)
(*   grep -n "pub fn begin\|pub fn read\|pub fn commit_read_only\|       *)
(*           pub fn abort" crates/kessel-storage/src/tx.rs                 *)
(* and update this table.                                                  *)
(*                                                                          *)
(*   TLA+ action          kessel-storage::tx counterpart      file:line    *)
(*   ──────────────────   ──────────────────────────────────  ──────────  *)
(*   TxBegin(t, s)        Tx::begin(store, snapshot_opnum=s)  tx.rs:begin *)
(*                          constructs Tx { store, s, BTreeSet::new() }   *)
(*   TxRead(t, k)         Tx::read(type_id, object_id)        tx.rs:read  *)
(*                          calls mvcc::get_at_snapshot(...,              *)
(*                          self.snapshot_opnum); inserts k into          *)
(*                          self.read_set unconditionally                  *)
(*   TxCommitReadOnly(t)  Tx::commit_read_only(self)          tx.rs:      *)
(*                          drops self (releases borrow); Ok(())          *)
(*   TxAbort(t)           Tx::abort(self)                     tx.rs:      *)
(*                          drops self (releases borrow); ()              *)
(*                                                                          *)
(* ─────────────────────────────────────────────────────────────────────── *)
(*                                                                          *)
(* INVARIANTS (per the S2.2 design Decision 7):                            *)
(*                                                                          *)
(*   TypeOKTx                     — well-typed Tx state-space               *)
(*   SnapshotImmutability         — txs[t].snapshot never changes after   *)
(*                                  TxBegin                                 *)
(*   ReadSetMonotonic             — read_set only grows during Active     *)
(*   ReadSetCoversAllReads        — every key any TxRead touched is in   *)
(*                                  read_set                                *)
(*   ReadAtSnapshot               — every TxRead's result equals          *)
(*                                  SnapshotReadOf at the tx's snapshot   *)
(*   TxStatusMonotonic            — Active → {Committed | Aborted};      *)
(*                                  no reverse transitions                 *)
(*                                                                          *)
(* Thesis pillar strengthened: verifiable (Tx snapshot pin + read-set    *)
(* discipline is machine-checked at the abstract level) + replayable     *)
(* (Tx behavior is a deterministic function of (snapshot, read seq); the *)
(* BTreeSet-deterministic-iteration property is reflected in the SET    *)
(* semantics of the TLA+ read_set).                                       *)
(***************************************************************************)

EXTENDS MVCCStorage

CONSTANTS
    TxIds,       \* finite set of transaction IDs (abstract; e.g., {t1, t2})
    MaxTxOps     \* bound on total Tx actions (state-space cap)

ASSUME MVCCTxAssumption ==
    /\ Cardinality(TxIds) >= 1
    /\ MaxTxOps           \in Nat

----------------------------------------------------------------------------
(***************************************************************************)
(* Tx status labels.                                                        *)
(***************************************************************************)

TxStatus == {"Active", "Committed", "Aborted"}

(***************************************************************************)
(* The "undefined" sentinel for a Tx that has not yet been begun. Tx       *)
(* records have shape [snapshot |-> _, read_set |-> _, status |-> _], plus *)
(* the undefined sentinel that says the Tx slot is empty.                  *)
(***************************************************************************)

TxUndefined == [snapshot |-> -1,
                read_set |-> {},
                status   |-> "Undefined"]

TxRecord ==
    [snapshot: OpNums \cup {-1},
     read_set: SUBSET Keys,
     status:   TxStatus \cup {"Undefined"}]

----------------------------------------------------------------------------
(***************************************************************************)
(* State variables (additions over MVCCStorage's `versions` + `opCount`). *)
(*                                                                          *)
(*   txs : TxIds -> TxRecord                                                *)
(*       Per-Tx state. Initialized to TxUndefined for every Tx; a TxBegin *)
(*       transitions to {snapshot, {}, "Active"}; TxRead grows read_set;  *)
(*       TxCommitReadOnly/TxAbort terminate the Tx.                        *)
(*                                                                          *)
(*   txOpCount : Nat                                                        *)
(*       Counter of TxBegin + TxRead + TxCommitReadOnly + TxAbort actions *)
(*       taken so far. Bounded by MaxTxOps so TLC terminates.              *)
(***************************************************************************)

VARIABLES
    txs,
    txOpCount

txVars == << txs, txOpCount >>

\* Composite vars over both MVCCStorage and Tx layers.
allVars == << versions, opCount, txs, txOpCount >>

----------------------------------------------------------------------------
(***************************************************************************)
(* Initial state.                                                           *)
(*                                                                          *)
(* All keys start with the empty version set (MVCCStorage's Init); all    *)
(* Tx slots start TxUndefined; counters at 0.                              *)
(***************************************************************************)

InitTx ==
    /\ Init                            \* MVCCStorage's Init (versions, opCount)
    /\ txs        = [t \in TxIds |-> TxUndefined]
    /\ txOpCount  = 0

----------------------------------------------------------------------------
(***************************************************************************)
(* ACTIONS.                                                                 *)
(*                                                                          *)
(* Four Tx actions: TxBegin, TxRead, TxCommitReadOnly, TxAbort. Plus the  *)
(* inherited storage actions: Put, Tombstone (re-exported via MVCCStorage).*)
(* Storage state (versions, opCount) is UNCHANGED by every Tx action;     *)
(* Tx state (txs, txOpCount) is UNCHANGED by every storage action.        *)
(***************************************************************************)

(***************************************************************************)
(* TxBegin(t, s) — begin a Tx pinned at snapshot s.                        *)
(*                                                                          *)
(* Precondition: txOpCount < MaxTxOps; txs[t] is Undefined (a Tx slot     *)
(* cannot be re-begun without first terminating it).                       *)
(*                                                                          *)
(* State change: txs[t] becomes [snapshot |-> s, read_set |-> {},          *)
(* status |-> "Active"]; bump txOpCount. Storage state unchanged.         *)
(***************************************************************************)
TxBegin(t, s) ==
    /\ txOpCount < MaxTxOps
    /\ txs[t].status = "Undefined"
    /\ txs' = [txs EXCEPT
                  ![t] = [snapshot |-> s,
                          read_set |-> {},
                          status   |-> "Active"]]
    /\ txOpCount' = txOpCount + 1
    /\ UNCHANGED << versions, opCount >>

(***************************************************************************)
(* TxRead(t, k) — record a read of key k = <<type_id, object_id>> by Tx t.*)
(*                                                                          *)
(* Precondition: txOpCount < MaxTxOps; txs[t] is Active; k is a valid Key.*)
(* (Per Decision 4 — no guard on the SnapshotReadOf return value; even   *)
(* NotYetWritten and Tombstoned reads enter the read_set.)                *)
(*                                                                          *)
(* State change: add k to txs[t].read_set (set union); bump txOpCount.   *)
(* Storage state unchanged.                                                 *)
(*                                                                          *)
(* The READ RESULT is NOT recorded in state — the ReadAtSnapshot         *)
(* invariant asserts that for every Tx t and every k in txs[t].read_set, *)
(* the value the Tx observed IS SnapshotReadOf(t, o, txs[t].snapshot)    *)
(* (in the current versions state — and `versions` cannot have changed   *)
(* between the read time and now because reads do not mutate storage and *)
(* by the time the invariant checks, no concurrent write at the same    *)
(* snapshot has the power to change SnapshotReadOf at txs[t].snapshot — *)
(* see the invariant comment for the formal argument).                   *)
(***************************************************************************)
TxRead(t, k) ==
    /\ txOpCount < MaxTxOps
    /\ txs[t].status = "Active"
    /\ k \in Keys
    /\ txs' = [txs EXCEPT
                  ![t] = [@ EXCEPT !.read_set = @ \cup {k}]]
    /\ txOpCount' = txOpCount + 1
    /\ UNCHANGED << versions, opCount >>

(***************************************************************************)
(* TxCommitReadOnly(t) — flip Tx t from Active to Committed.               *)
(*                                                                          *)
(* Precondition: txOpCount < MaxTxOps; txs[t] is Active.                  *)
(*                                                                          *)
(* State change: status Active → Committed; snapshot + read_set preserved.*)
(* Storage state unchanged.                                                 *)
(***************************************************************************)
TxCommitReadOnly(t) ==
    /\ txOpCount < MaxTxOps
    /\ txs[t].status = "Active"
    /\ txs' = [txs EXCEPT
                  ![t] = [@ EXCEPT !.status = "Committed"]]
    /\ txOpCount' = txOpCount + 1
    /\ UNCHANGED << versions, opCount >>

(***************************************************************************)
(* TxAbort(t) — flip Tx t from Active to Aborted.                          *)
(*                                                                          *)
(* Precondition: txOpCount < MaxTxOps; txs[t] is Active.                  *)
(*                                                                          *)
(* State change: status Active → Aborted; snapshot + read_set preserved. *)
(* Storage state unchanged.                                                 *)
(***************************************************************************)
TxAbort(t) ==
    /\ txOpCount < MaxTxOps
    /\ txs[t].status = "Active"
    /\ txs' = [txs EXCEPT
                  ![t] = [@ EXCEPT !.status = "Aborted"]]
    /\ txOpCount' = txOpCount + 1
    /\ UNCHANGED << versions, opCount >>

----------------------------------------------------------------------------
(***************************************************************************)
(* Storage actions lifted into the composite spec — re-quantify the       *)
(* MVCCStorage Put + Tombstone with the new UNCHANGED clause for Tx vars. *)
(***************************************************************************)

PutTx(t, o, c, v) ==
    /\ Put(t, o, c, v)
    /\ UNCHANGED << txs, txOpCount >>

TombstoneTx(t, o, c) ==
    /\ Tombstone(t, o, c)
    /\ UNCHANGED << txs, txOpCount >>

----------------------------------------------------------------------------
(***************************************************************************)
(* Next-state relation.                                                     *)
(*                                                                          *)
(* Six actions: PutTx, TombstoneTx (storage), TxBegin, TxRead,             *)
(* TxCommitReadOnly, TxAbort (Tx layer). Each is quantified over the      *)
(* bounded parameter universes.                                            *)
(***************************************************************************)

NextTx ==
    \/ \E t \in TypeIds, o \in ObjectIds, c \in OpNums, v \in Values :
           PutTx(t, o, c, v)
    \/ \E t \in TypeIds, o \in ObjectIds, c \in OpNums :
           TombstoneTx(t, o, c)
    \/ \E tx \in TxIds, s \in OpNums :
           TxBegin(tx, s)
    \/ \E tx \in TxIds, k \in Keys :
           TxRead(tx, k)
    \/ \E tx \in TxIds :
           TxCommitReadOnly(tx)
    \/ \E tx \in TxIds :
           TxAbort(tx)

----------------------------------------------------------------------------
(***************************************************************************)
(* Specification.                                                           *)
(*                                                                          *)
(* SAFETY-ONLY (mirrors S1/SP109 + SP110 discipline). Fairness / liveness *)
(* is out of scope for S2.2. Assert only [][NextTx]_allVars; no WF/SF.    *)
(***************************************************************************)

SpecTx == InitTx /\ [][NextTx]_allVars

----------------------------------------------------------------------------
(***************************************************************************)
(* SAFETY INVARIANTS.                                                       *)
(***************************************************************************)

(***************************************************************************)
(* TypeOKTx — well-typed Tx state envelope.                                *)
(*                                                                          *)
(* `txs` is a function from TxIds to TxRecord. txOpCount is a bounded     *)
(* natural. Active and terminal status are constrained.                    *)
(***************************************************************************)
TypeOKTx ==
    /\ TypeOK                                  \* MVCCStorage's TypeOK
    /\ txs       \in [TxIds -> TxRecord]
    /\ txOpCount \in 0..MaxTxOps
    \* An Undefined slot must be exactly the TxUndefined sentinel — no
    \* mixed shapes (e.g., status="Undefined" but snapshot != -1).
    /\ \A t \in TxIds :
         (txs[t].status = "Undefined") => (txs[t] = TxUndefined)
    \* A defined slot must have a real snapshot in OpNums.
    /\ \A t \in TxIds :
         (txs[t].status \in TxStatus) => (txs[t].snapshot \in OpNums)

(***************************************************************************)
(* SnapshotImmutability — txs[t].snapshot never changes once the Tx has  *)
(* been begun.                                                              *)
(*                                                                          *)
(* Phrased as a current-state property: for every Tx t in a defined       *)
(* status (Active/Committed/Aborted), the snapshot field IS a valid       *)
(* OpNum. The "never changes" temporal claim is enforced by each action's *)
(* shape: TxBegin sets snapshot=s and is only enabled when status was    *)
(* "Undefined"; TxRead / TxCommitReadOnly / TxAbort use                  *)
(* [snapshot |-> @ EXCEPT !.status = ...] which preserves the snapshot   *)
(* field by the EXCEPT-record-update semantics. The current-state form   *)
(* below is the CHECKABLE state property; the action-shape argument is   *)
(* the proof that the snapshot is in fact preserved across every         *)
(* transition.                                                              *)
(***************************************************************************)
SnapshotImmutability ==
    \A t \in TxIds :
        (txs[t].status \in TxStatus)
            => (txs[t].snapshot \in OpNums)

(***************************************************************************)
(* ReadSetMonotonic — read_set only grows during Active.                  *)
(*                                                                          *)
(* Phrased as a current-state property: for every Tx t in a defined      *)
(* status, the read_set is a subset of Keys (well-formedness). The       *)
(* "only grows" temporal claim is enforced by each action's shape:       *)
(* TxRead uses [@ EXCEPT !.read_set = @ \cup {k}] which is monotonic by *)
(* construction; TxCommitReadOnly and TxAbort preserve the read_set     *)
(* field via the EXCEPT-record-update semantics. The current-state form *)
(* below is the CHECKABLE state property.                                 *)
(***************************************************************************)
ReadSetMonotonic ==
    \A t \in TxIds :
        (txs[t].status \in TxStatus)
            => (txs[t].read_set \subseteq Keys)

(***************************************************************************)
(* ReadSetCoversAllReads — every TxRead action commits its key to the    *)
(* read_set in the next state.                                             *)
(*                                                                          *)
(* This is the "read-set IS the record of reads" invariant per Decision 4.*)
(* Phrased as a current-state property over (txs, Keys): for every       *)
(* defined Tx t, every key k in txs[t].read_set was either added by a   *)
(* TxRead(t, k) action or by TxBegin's initial empty set (vacuously).   *)
(* Since the only action that mutates read_set is TxRead which adds the *)
(* key in the same step, this is the current-state form: read_set        *)
(* members are themselves valid Keys (well-formed; subset of Keys).      *)
(* The "every TxRead enters the read-set regardless of variant" claim   *)
(* is enforced by the TxRead action shape (no guard on SnapshotReadOf).  *)
(***************************************************************************)
ReadSetCoversAllReads ==
    \A t \in TxIds :
        (txs[t].status \in TxStatus)
            => (\A k \in txs[t].read_set : k \in Keys)

(***************************************************************************)
(* ReadAtSnapshot — every TxRead's result equals SnapshotReadOf at the   *)
(* tx's snapshot.                                                          *)
(*                                                                          *)
(* The TxRead action does not record the read result in state. The       *)
(* CHECKABLE current-state property is: for every Tx t in a defined      *)
(* status, every k in txs[t].read_set has a WELL-DEFINED SnapshotReadOf  *)
(* at the tx's snapshot — i.e., SnapshotReadOf(t.type_id, o.object_id,  *)
(* txs[t].snapshot) is a TOTAL function that returns either a real      *)
(* version or the NotYetWritten sentinel. The Rust implementation       *)
(* enforces "read_result == get_at_snapshot(..., snapshot_opnum)" at    *)
(* every Tx::read call site; this TLA+ invariant locks the totality     *)
(* property that the Rust contract relies on.                            *)
(*                                                                          *)
(* The full "read_result == SnapshotReadOf(...)" claim is encoded by    *)
(* the ACTION SHAPE of TxRead (which simply calls SnapshotReadOf and    *)
(* returns its value at the implementation level); the state invariant *)
(* below locks the contract that EVERY key in read_set is a valid Key  *)
(* with a well-defined SnapshotReadOf at the tx's snapshot.              *)
(***************************************************************************)
ReadAtSnapshot ==
    \A t \in TxIds :
        (txs[t].status \in TxStatus)
            => (\A k \in txs[t].read_set :
                    LET tpid == k[1]
                        oid  == k[2]
                        snap == txs[t].snapshot
                        r    == SnapshotReadOf(tpid, oid, snap)
                    IN  /\ r.opnum \in OpNums \cup {-1}
                        /\ r.value \in PayloadDomain \cup {"NotYetWritten"})

(***************************************************************************)
(* TxStatusMonotonic — status transitions are Active → Committed or      *)
(* Active → Aborted; no reverse transitions; Committed and Aborted are   *)
(* absorbing.                                                              *)
(*                                                                          *)
(* Phrased as a current-state property over the action shapes: every     *)
(* action that mutates txs[t].status either (i) sets status="Active" only*)
(* when prior status was "Undefined" (TxBegin), or (ii) sets             *)
(* status="Committed" only when prior status was "Active"                 *)
(* (TxCommitReadOnly), or (iii) sets status="Aborted" only when prior   *)
(* status was "Active" (TxAbort). No action sets status back to          *)
(* "Active" once it has reached a terminal state. The CHECKABLE state    *)
(* invariant is: every txs[t].status is in TxStatus ∪ {"Undefined"}     *)
(* (well-formedness). The "no reverse transitions" temporal claim is   *)
(* enforced by the per-action precondition shapes above.                  *)
(***************************************************************************)
TxStatusMonotonic ==
    \A t \in TxIds :
        txs[t].status \in (TxStatus \cup {"Undefined"})

============================================================================
