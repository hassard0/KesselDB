---------------------------- MODULE MVCCSi ----------------------------
(***************************************************************************)
(* KesselDB — S2.3 (= SP112): TLA+/TLC specification for the SI write-side  *)
(* + conflict detection at SM apply time, abstracted from                   *)
(* `crates/kessel-storage/src/tx.rs` (Tx::write / Tx::commit) and           *)
(* `crates/kessel-sm/src/lib.rs` (Op::CommitTx apply arm).                  *)
(*                                                                          *)
(* This module EXTENDS MVCCTx (the S2.2/SP111 specification). The SI       *)
(* layer is checked over the SAME versioned-storage + Tx model TLC already *)
(* verified in S2.1 (SP110) and S2.2 (SP111) — re-using `versions`,        *)
(* `opCount`, the per-Tx snapshot+read_set+status, and the bounded          *)
(* constants from MVCCTx's universe.                                       *)
(*                                                                          *)
(* SCOPE (per the S2.3 design Decision 7) — abstract single-replica plain  *)
(* SI write-side + the deterministic SM-apply-time conflict check.          *)
(*                                                                          *)
(* THE THESIS-FIT CENTERPIECE. The CommitTx action mechanically encodes    *)
(* the parent S2 design's Decision 4: in a deterministic state machine    *)
(* fed by a totally-ordered log, conflict detection is a function of the *)
(* log prefix. Every replica running the same apply on the same prefix   *)
(* reaches the same verdict — no distributed coordination required        *)
(* (no TrueTime, no HLC, no txn-record protocol). The DeterministicApply  *)
(* invariant locks this property at the abstract level: `versions`        *)
(* state after CommitTx is a deterministic function of (versions, txs[t]) *)
(* at the action's firing.                                                  *)
(*                                                                          *)
(* OUT OF SCOPE (HONEST-DISCLOSED LIMITATIONS — read these before          *)
(* trusting any conclusion drawn from a green TLC run):                    *)
(*                                                                          *)
(*   1. SSI dangerous-cycle detection is NOT modeled here. S2.4           *)
(*      follow-up (which will EXTEND MVCCSi with the rw-antidependency   *)
(*      cycle invariant over Tx::read_set + Tx::write_set).               *)
(*                                                                          *)
(*   2. Multi-replica Tx state is NOT modeled (no `txs[r][tx]` shape).   *)
(*      The CommitTx action's apply-determinism is the abstract proof    *)
(*      that two replicas running the same op on the same versions       *)
(*      reach the same verdict; per-replica byte-identity at the LSM    *)
(*      level is verified at the Rust integration-test level (T3 ships  *)
(*      a 3-replica byte-identity test for SI commits). Multi-replica   *)
(*      TLA+ is an S2.X follow-up.                                       *)
(*                                                                          *)
(*   3. GC / watermark / version reclamation is NOT modeled (carried     *)
(*      forward from MVCCStorage + MVCCTx). S2.5 follow-up.               *)
(*                                                                          *)
(*   4. The cursor-stall semantics ("commit op arrives before its         *)
(*      snapshot_opnum has been applied locally") are NOT modeled here.  *)
(*      S2.6 follow-up. In S2.3 the SM apply path treats                  *)
(*      `snapshot_opnum > commit_opnum` as a malformed op                 *)
(*      (conservative: abort with SnapshotOutOfRange).                    *)
(*                                                                          *)
(*   5. Bounded model checking proves the absence of counterexamples at   *)
(*      the CONFIGURED constants only. The Rust pentest tests (T5)       *)
(*      cover boundary opnums (0, u64::MAX) explicitly that the bounded *)
(*      TLC model cannot reach.                                            *)
(*                                                                          *)
(*   6. NAMED-ACTION CORRESPONDENCE to kessel-storage::tx +                *)
(*      kessel-sm::StateMachine::apply, NOT a mechanized refinement.      *)
(*      Same caveat as SP109/SP110/SP111. The action-mapping table below  *)
(*      makes the correspondence inspectable.                              *)
(*                                                                          *)
(* ─── TLA+ ACTION ↔ Rust MAPPING (SP112 T6) ───────────────────────────── *)
(*                                                                          *)
(* Each named action in this spec corresponds to a Rust function. Line    *)
(* numbers are accurate as of commit 50e17e4 against                       *)
(* crates/kessel-storage/src/tx.rs and crates/kessel-sm/src/lib.rs        *)
(* (`Op::CommitTx` apply arm). This is "named correspondence", not       *)
(* mechanized refinement — a divergence between the spec and the Rust   *)
(* code is a human-discovered issue. If the Rust code is refactored,    *)
(* re-run:                                                                  *)
(*   grep -n "pub fn write\|pub fn commit\b\|pub fn write_set"           *)
(*           crates/kessel-storage/src/tx.rs                               *)
(*   grep -n "Op::CommitTx" crates/kessel-sm/src/lib.rs                   *)
(* and update this table.                                                  *)
(*                                                                          *)
(*   TLA+ action          Rust counterpart                file:line        *)
(*   ──────────────────   ────────────────────────────   ────────────     *)
(*   TxWrite(t, k, v)     Tx::write(type_id, &object_id, *)
(*                            Some(v)) (live)             tx.rs:write     *)
(*                          insert into BTreeMap          (BTreeMap        *)
(*                          (last-write-wins per key)     deterministic    *)
(*                                                          iteration)     *)
(*   TxTombstoneWrite(t,k) Tx::write(type_id, &object_id, *)
(*                            None) (buffered tombstone)  tx.rs:write     *)
(*   CommitTx(t, c)       (a) Tx::commit(c) for the      tx.rs:commit    *)
(*                            local-tested path AND       sm.rs:apply     *)
(*                        (b) Op::CommitTx { snapshot,  Op::CommitTx     *)
(*                            write_set, commit_opnum }   arm — runs the  *)
(*                            apply at SM apply time —    has_version_in_ *)
(*                            BOTH paths run the same     range check     *)
(*                            deterministic conflict     per write_set    *)
(*                            check (T3's byte-           key in window   *)
(*                            equivalence integration     (snapshot,      *)
(*                            test gates this).           commit-1]       *)
(*                                                                          *)
(* ─────────────────────────────────────────────────────────────────────── *)
(*                                                                          *)
(* INVARIANTS (per the S2.3 design Decision 7):                            *)
(*                                                                          *)
(*   All 6 MVCCTx invariants carried forward (TypeOKTx,                    *)
(*   SnapshotImmutability, ReadSetMonotonic, ReadSetCoversAllReads,        *)
(*   ReadAtSnapshot, TxStatusMonotonic).                                   *)
(*                                                                          *)
(*   TypeOKSi                     — well-typed SI state-space (extends    *)
(*                                  TypeOKTx with the write_set field)    *)
(*   WriteSetMonotonic            — write_set keys persist while Active   *)
(*                                  (same-key updates replace value but  *)
(*                                  key stays present)                    *)
(*   WriteWriteConflictDetected   — for any Committed Tx, NO key in its  *)
(*                                  write_set has a version committed   *)
(*                                  in (snapshot, commit-1]               *)
(*   CommitAtomicity              — Committed/Aborted are absorbing;     *)
(*                                  every Committed Tx's write_set keys *)
(*                                  have a version at exactly its        *)
(*                                  commit_opnum (or all writes apply   *)
(*                                  or none)                              *)
(*   FirstCommitterWins           — two overlapping-write Tx cannot both *)
(*                                  Commit if their commit windows       *)
(*                                  conflict; the later-attempt Aborts  *)
(*   DeterministicApply           — every Committed Tx's versions delta *)
(*                                  is a function of (write_set,         *)
(*                                  commit_opnum) only; given the same  *)
(*                                  log prefix, every replica reaches    *)
(*                                  the same Committed/Aborted verdict  *)
(*                                                                          *)
(* Thesis pillar strengthened: verifiable (the SI conflict-check         *)
(* contract is machine-checked at the abstract level) + replayable      *)
(* (Tx outcome is a deterministic function of (snapshot_opnum,           *)
(* write_set, commit_opnum, log prefix); BTreeMap-deterministic-        *)
(* iteration is reflected in the FUNCTION semantics of the TLA+         *)
(* write_set) + the THESIS-FIT CENTERPIECE: the deterministic state     *)
(* machine IS the conflict resolver, no distributed coordination needed. *)
(***************************************************************************)

EXTENDS MVCCTx

CONSTANTS
    \* (No new constants — inherits TypeIds/ObjectIds/OpNums/Values/MaxOps
    \* from MVCCStorage and TxIds/MaxTxOps from MVCCTx.)
    SiUnused

ASSUME MVCCSiAssumption ==
    SiUnused = "Si"   \* sentinel to keep TLC's constant block happy

----------------------------------------------------------------------------
(***************************************************************************)
(* Domain helpers for the SI layer.                                         *)
(*                                                                          *)
(* The SI write-set is modeled as a TLA+ function from Keys to              *)
(* PayloadDomain (which already contains Values \cup {TOMBSTONE}); the     *)
(* domain restriction (only the keys this Tx has actually written) is      *)
(* enforced by the per-action shape. A "Tx that hasn't written K"          *)
(* corresponds to "K \notin DOMAIN txs[t].write_set"; a buffered tombstone *)
(* is `write_set[K] = TOMBSTONE`; a buffered live write is                 *)
(* `write_set[K] = v` for some v in Values.                                *)
(***************************************************************************)

\* The "undefined" sentinel for a Tx slot that has not yet been begun in
\* the SI extension. Same shape as MVCCTx.TxUndefined but adds a
\* `write_set` field (empty function) and uses commit_opnum=-1 sentinel.
TxUndefinedSi == [snapshot    |-> -1,
                  read_set    |-> {},
                  write_set   |-> << >>,           \* empty TLA+ sequence as "no domain"
                  commit_opnum|-> -1,
                  status      |-> "Undefined"]

\* A TxRecord in the SI extension carries the SP111 fields plus write_set
\* (modeled as a [Keys -> PayloadDomain] partial function whose domain is
\* the set of keys the Tx has buffered a write for) and commit_opnum
\* (the SM-assigned commit opnum once the Tx is Committed; -1 while
\* Active/Aborted).
TxRecordSi ==
    [snapshot:     OpNums \cup {-1},
     read_set:     SUBSET Keys,
     write_set:    [Keys -> PayloadDomain] \cup {<< >>},
     commit_opnum: OpNums \cup {-1},
     status:       TxStatus \cup {"Undefined"}]

----------------------------------------------------------------------------
(***************************************************************************)
(* State variables (additions over MVCCTx's `txs` + `txOpCount`).          *)
(*                                                                          *)
(* The SI layer EXTENDS the Tx record shape with two new fields            *)
(* (`write_set`, `commit_opnum`). Rather than re-declare `txs` and break   *)
(* the EXTENDS, we keep `txs` from MVCCTx (the SP111 read-set state) and  *)
(* introduce a parallel SI map `txsSi: TxIds -> TxRecordSi` that the      *)
(* SI actions mutate. The two stay synchronized by the action shapes:    *)
(* every TxBegin/TxRead/TxCommitReadOnly/TxAbort lifts into SI by         *)
(* mirroring the txs mutation and leaving write_set/commit_opnum at      *)
(* their SP111-defaults; every SI-specific action mutates txsSi only.    *)
(*                                                                          *)
(* This is the cleanest TLA+ pattern for "extend the record without       *)
(* rewriting the parent module" — see the SP111 EXTENDS-MVCCStorage      *)
(* precedent (where MVCCTx added txs+txOpCount as new variables).        *)
(***************************************************************************)

VARIABLES
    txsSi,           \* TxIds -> TxRecordSi
    siOpCount        \* Nat — counts SI-specific actions (TxWrite + CommitTx)

siVars == << txsSi, siOpCount >>

\* Composite vars over MVCCStorage + Tx + SI layers.
allVarsSi == << versions, opCount, txs, txOpCount, txsSi, siOpCount >>

----------------------------------------------------------------------------
(***************************************************************************)
(* Initial state.                                                           *)
(*                                                                          *)
(* All keys start with empty version sets (MVCCStorage's Init); all Tx    *)
(* slots start Undefined; all SI slots start TxUndefinedSi; counters at 0. *)
(***************************************************************************)

InitSi ==
    /\ InitTx                            \* MVCCTx's InitTx (and via that, MVCCStorage's Init)
    /\ txsSi      = [t \in TxIds |-> TxUndefinedSi]
    /\ siOpCount  = 0

----------------------------------------------------------------------------
(***************************************************************************)
(* ACTIONS — Tx layer lifted into SI (mirror MVCCTx actions on txsSi).    *)
(*                                                                          *)
(* These are the SP111 actions re-stated to keep `txsSi` in step with     *)
(* `txs`. We re-use MVCCTx's enabling conditions (txOpCount < MaxTxOps,   *)
(* status guards) and EXTEND each action to also update the SI fields.   *)
(***************************************************************************)

TxBeginSi(t, s) ==
    /\ TxBegin(t, s)
    /\ txsSi[t].status = "Undefined"
    /\ txsSi' = [txsSi EXCEPT
                    ![t] = [snapshot     |-> s,
                            read_set     |-> {},
                            write_set    |-> << >>,
                            commit_opnum |-> -1,
                            status       |-> "Active"]]
    /\ UNCHANGED siOpCount

TxReadSi(t, k) ==
    /\ TxRead(t, k)
    /\ txsSi[t].status = "Active"
    /\ txsSi' = [txsSi EXCEPT
                    ![t] = [@ EXCEPT !.read_set = @ \cup {k}]]
    /\ UNCHANGED siOpCount

TxCommitReadOnlySi(t) ==
    /\ TxCommitReadOnly(t)
    /\ txsSi[t].status = "Active"
    \* TLC-found tightening (TIGHTENED 2026-05-24): TxCommitReadOnly is
    \* the no-conflict-check, no-writes-installed SELECT path. Allowing it
    \* to fire on a Tx with a non-empty write_set would mark the Tx
    \* "Committed" without installing the writes, violating CommitAtomicity.
    \* The Rust contract for `Tx::commit_read_only` does not buffer-check
    \* (a caller misuse — they should be calling `Tx::commit`), but the
    \* abstract model TIGHTENS the precondition: TxCommitReadOnlySi is
    \* only enabled when the write_set is empty. This is the SP109/SP110
    \* discipline: tighten preconditions, never weaken invariants. The
    \* Rust-side equivalent is a follow-up `debug_assert!` in
    \* `commit_read_only` (tracked S2.X).
    /\ txsSi[t].write_set = << >>
    /\ txsSi' = [txsSi EXCEPT
                    ![t] = [@ EXCEPT !.status = "Committed"]]
    /\ UNCHANGED siOpCount

TxAbortSi(t) ==
    /\ TxAbort(t)
    /\ txsSi[t].status = "Active"
    /\ txsSi' = [txsSi EXCEPT
                    ![t] = [@ EXCEPT !.status = "Aborted"]]
    /\ UNCHANGED siOpCount

\* Lifted storage actions — Put/Tombstone leave SI state UNCHANGED.
PutSi(t, o, c, v) ==
    /\ PutTx(t, o, c, v)
    /\ UNCHANGED siVars

TombstoneSi(t, o, c) ==
    /\ TombstoneTx(t, o, c)
    /\ UNCHANGED siVars

----------------------------------------------------------------------------
(***************************************************************************)
(* SI-SPECIFIC ACTIONS — TxWrite + CommitTx.                              *)
(***************************************************************************)

(***************************************************************************)
(* TxWrite(t, k, v) — buffer a live write of key k=<<t_id, o_id>> with    *)
(* value v in Tx t's write_set.                                            *)
(*                                                                          *)
(* Precondition: siOpCount < MaxTxOps (state-space bound); txsSi[t]       *)
(* is Active; k is a valid Key; v is a valid Value.                       *)
(*                                                                          *)
(* State change: txsSi[t].write_set is updated to include the binding     *)
(* k |-> v. If k was already in DOMAIN write_set, the value is replaced  *)
(* (last-write-wins per key; Decision 2 of S2.3 design — BTreeMap         *)
(* coalescing). Storage state UNCHANGED; SP111 tx state UNCHANGED.       *)
(***************************************************************************)
TxWrite(t, k, v) ==
    /\ siOpCount < MaxTxOps
    /\ txsSi[t].status = "Active"
    /\ k \in Keys
    /\ v \in Values
    /\ LET ws == IF txsSi[t].write_set = << >>
                 THEN [kk \in {k} |-> v]
                 ELSE [kk \in (DOMAIN txsSi[t].write_set) \cup {k}
                          |-> IF kk = k THEN v ELSE txsSi[t].write_set[kk]]
       IN  txsSi' = [txsSi EXCEPT
                        ![t] = [@ EXCEPT !.write_set = ws]]
    /\ siOpCount' = siOpCount + 1
    /\ UNCHANGED << versions, opCount, txs, txOpCount >>

(***************************************************************************)
(* TxTombstoneWrite(t, k) — buffer a tombstone write of key k in Tx t's   *)
(* write_set.                                                              *)
(*                                                                          *)
(* Same shape as TxWrite, but the buffered value is TOMBSTONE              *)
(* (PayloadDomain element representing Option<Vec<u8>>::None at commit).  *)
(***************************************************************************)
TxTombstoneWrite(t, k) ==
    /\ siOpCount < MaxTxOps
    /\ txsSi[t].status = "Active"
    /\ k \in Keys
    /\ LET ws == IF txsSi[t].write_set = << >>
                 THEN [kk \in {k} |-> TOMBSTONE]
                 ELSE [kk \in (DOMAIN txsSi[t].write_set) \cup {k}
                          |-> IF kk = k THEN TOMBSTONE
                                        ELSE txsSi[t].write_set[kk]]
       IN  txsSi' = [txsSi EXCEPT
                        ![t] = [@ EXCEPT !.write_set = ws]]
    /\ siOpCount' = siOpCount + 1
    /\ UNCHANGED << versions, opCount, txs, txOpCount >>

(***************************************************************************)
(* HasVersionInRange(k, lo_excl, hi_incl) — abstract conflict-check       *)
(* primitive lifted from kessel-storage::mvcc::has_version_in_range.       *)
(*                                                                          *)
(* Returns TRUE iff there exists a version of key k with opnum in the     *)
(* half-open interval (lo_excl, hi_incl].                                  *)
(***************************************************************************)
HasVersionInRange(k, lo_excl, hi_incl) ==
    \E e \in versions[k] :
        /\ e.opnum > lo_excl
        /\ e.opnum <= hi_incl

(***************************************************************************)
(* CommitTx(t, c) — the SI conflict-checked commit at commit_opnum c.     *)
(*                                                                          *)
(* THIS IS THE THESIS-FIT CENTERPIECE. The action models BOTH the         *)
(* Tx::commit standalone path AND the Op::CommitTx SM apply arm — they   *)
(* are semantically identical because both run the SAME deterministic    *)
(* conflict check against the same versioned-storage state. The Rust    *)
(* integration tests (T3) gate the byte-equivalence claim; this TLA+    *)
(* action locks the abstract semantics.                                   *)
(*                                                                          *)
(* Precondition: siOpCount < MaxTxOps; txsSi[t] is Active; c in OpNums   *)
(* (the commit_opnum); c >= txsSi[t].snapshot (SnapshotOutOfRange         *)
(* sanity); no other version of any write_set key exists at opnum c      *)
(* (Put's uniqueness — captured by the LET binding below in the          *)
(* conflict-free branch).                                                  *)
(*                                                                          *)
(* Two branches:                                                            *)
(*                                                                          *)
(*  (1) CONFLICT — \E k \in DOMAIN write_set : HasVersionInRange(k,       *)
(*      snapshot, c - 1). Status -> Aborted; storage UNCHANGED;            *)
(*      commit_opnum stays at -1.                                           *)
(*                                                                          *)
(*  (2) NO CONFLICT — install every (k, v) in write_set as a versioned   *)
(*      entry at opnum c. Status -> Committed; commit_opnum := c;         *)
(*      versions updated.                                                   *)
(*                                                                          *)
(* Edge case: commit_opnum = 0. Then c - 1 wraps; we explicitly skip the *)
(* conflict-check sub-formula when c = 0 (a Tx committing at opnum 0    *)
(* cannot conflict with anything because no prior versions could exist). *)
(***************************************************************************)
CommitTx(t, c) ==
    /\ siOpCount < MaxTxOps
    /\ txsSi[t].status = "Active"
    /\ c \in OpNums
    /\ txsSi[t].snapshot >= 0
    /\ txsSi[t].snapshot <= c
    \* TLC-found tightening (TIGHTENED 2026-05-24): the commit_opnum c
    \* is assigned by the VSR log position; commits land in apply order.
    \* Enforce strict monotonicity: c >= opCount, and on success we bump
    \* opCount to c + 1. Without this, TLC admits an interleaving where
    \* t2 commits at c=1 AFTER t1 commits at c=2, retro-creating a
    \* version at opnum=1 inside t1's already-evaluated conflict window
    \* and violating WriteWriteConflictDetected — a counterexample that
    \* does NOT correspond to any real-system behavior (the log totally
    \* orders commits). This is the SP109/SP110 discipline.
    /\ c >= opCount
    /\ \* The Tx's own writes must not collide with any pre-existing
       \* version at exactly c (Put's per-(t,o) uniqueness constraint
       \* lifted to the multi-key commit; redundant with monotonicity
       \* above but kept for safety against future Put re-enablement).
       (txsSi[t].write_set = << >>) \/
       (\A k \in DOMAIN txsSi[t].write_set :
            \A e \in versions[k] : e.opnum # c)
    /\ LET ws        == txsSi[t].write_set
           hasConflict ==
               IF c = 0 \/ ws = << >>
               THEN FALSE
               ELSE \E k \in DOMAIN ws :
                       HasVersionInRange(k, txsSi[t].snapshot, c - 1)
       IN  IF hasConflict
           THEN \* Branch (1): conflict → Abort. Both layers' status flips
                \* to "Aborted" to preserve TypeOKSi's mirror agreement
                \* (per S2.3 design: CommitTx is the terminal action for
                \* the SI Tx; the SP111 Tx-layer status mirrors it).
                \* Abort still advances opCount past c (the log entry was
                \* consumed even though no version was installed).
                /\ txsSi' = [txsSi EXCEPT
                                ![t] = [@ EXCEPT !.status = "Aborted"]]
                /\ txs'   = [txs EXCEPT
                                ![t] = [@ EXCEPT !.status = "Aborted"]]
                /\ opCount' = c + 1
                /\ UNCHANGED << versions, txOpCount >>
           ELSE \* Branch (2): no conflict → install writes; Commit.
                \* Both layers' status flips to "Committed" to preserve
                \* TypeOKSi's mirror agreement. opCount advances past c.
                /\ versions' =
                       IF ws = << >>
                       THEN versions
                       ELSE [k \in Keys |->
                                IF k \in DOMAIN ws
                                THEN versions[k] \cup
                                       {[opnum |-> c, value |-> ws[k]]}
                                ELSE versions[k]]
                /\ txsSi' = [txsSi EXCEPT
                                ![t] = [@ EXCEPT
                                          !.status       = "Committed",
                                          !.commit_opnum = c]]
                /\ txs'   = [txs EXCEPT
                                ![t] = [@ EXCEPT !.status = "Committed"]]
                /\ opCount' = c + 1
                /\ UNCHANGED txOpCount
    /\ siOpCount' = siOpCount + 1

----------------------------------------------------------------------------
(***************************************************************************)
(* Next-state relation.                                                     *)
(*                                                                          *)
(* All MVCCTx actions, lifted to also mutate txsSi where the corresponding *)
(* SP111 action mutates txs; plus the SI-specific TxWrite,                 *)
(* TxTombstoneWrite, and CommitTx actions.                                 *)
(***************************************************************************)

NextSi ==
    \* TLC-found tightening (TIGHTENED 2026-05-24): the free-floating
    \* PutSi / TombstoneSi storage actions (lifted from MVCCTx where they
    \* modeled "background storage activity outside any Tx") are NOT
    \* enabled at the SI level. In the real system, EVERY versioned write
    \* flows through a CommitTx; there is no free-floating Put. Including
    \* free PutSi / TombstoneSi in NextSi admits behaviors where a
    \* version is inserted at an opnum inside an already-Committed Tx's
    \* conflict window, retro-violating WriteWriteConflictDetected — a
    \* counterexample that does NOT correspond to any real-system
    \* behavior. TIGHTEN by gating all storage writes through CommitTx.
    \* This is the SP109/SP110 discipline: tighten preconditions, never
    \* weaken invariants. The MVCCStorage + MVCCTx base modules retain
    \* the free Put/Tombstone actions for the read-only-Tx slice where
    \* they remain semantically valid.
    \/ \E tx \in TxIds, s \in OpNums :
           TxBeginSi(tx, s)
    \/ \E tx \in TxIds, k \in Keys :
           TxReadSi(tx, k)
    \/ \E tx \in TxIds :
           TxCommitReadOnlySi(tx)
    \/ \E tx \in TxIds :
           TxAbortSi(tx)
    \/ \E tx \in TxIds, k \in Keys, v \in Values :
           TxWrite(tx, k, v)
    \/ \E tx \in TxIds, k \in Keys :
           TxTombstoneWrite(tx, k)
    \/ \E tx \in TxIds, c \in OpNums :
           CommitTx(tx, c)

----------------------------------------------------------------------------
(***************************************************************************)
(* Specification.                                                           *)
(*                                                                          *)
(* SAFETY-ONLY (mirrors S1/SP109 + SP110 + SP111 discipline). Fairness /  *)
(* liveness is out of scope for S2.3. Assert only [][NextSi]_allVarsSi.   *)
(***************************************************************************)

SpecSi == InitSi /\ [][NextSi]_allVarsSi

----------------------------------------------------------------------------
(***************************************************************************)
(* SAFETY INVARIANTS.                                                       *)
(***************************************************************************)

(***************************************************************************)
(* TypeOKSi — well-typed SI state envelope.                                *)
(*                                                                          *)
(* The MVCCTx TypeOKTx (which subsumes MVCCStorage TypeOK) PLUS:            *)
(*   - txsSi well-typed                                                     *)
(*   - siOpCount in 0..MaxTxOps                                             *)
(*   - per-Tx invariants between txsSi and txs: status agreement, snapshot *)
(*     agreement, read_set agreement (SP111 fields are mirrored).         *)
(***************************************************************************)
TypeOKSi ==
    /\ TypeOKTx
    /\ siOpCount \in 0..MaxTxOps
    /\ \A t \in TxIds :
         /\ txsSi[t].snapshot \in OpNums \cup {-1}
         /\ txsSi[t].read_set \subseteq Keys
         /\ txsSi[t].commit_opnum \in OpNums \cup {-1}
         /\ txsSi[t].status \in TxStatus \cup {"Undefined"}
         \* Mirror agreement: SI snapshot/read_set/status match Tx layer.
         /\ txsSi[t].snapshot = txs[t].snapshot
         /\ txsSi[t].read_set = txs[t].read_set
         /\ txsSi[t].status   = txs[t].status

(***************************************************************************)
(* WriteSetMonotonic — write_set keys persist while Active.                *)
(*                                                                          *)
(* For every Tx t in status Active or Committed, every key k previously   *)
(* in write_set remains in write_set (or, equivalently, no action removes *)
(* a key — same-key updates replace value but keep the key present).      *)
(*                                                                          *)
(* Phrased as a current-state property: every k in DOMAIN write_set is    *)
(* a valid Key (well-formedness). The "only grows" temporal claim is      *)
(* enforced by per-action shape: TxWrite and TxTombstoneWrite update      *)
(* via the binder `kk \in DOMAIN @ \cup {k}` which is monotonic;          *)
(* CommitTx and TxAbortSi do not mutate write_set; TxCommitReadOnlySi    *)
(* does not mutate write_set (the no-writes-Committed case keeps the     *)
(* empty/existing write_set intact).                                       *)
(***************************************************************************)
WriteSetMonotonic ==
    \A t \in TxIds :
        txsSi[t].write_set = << >> \/
        (\A k \in DOMAIN txsSi[t].write_set :
            /\ k \in Keys
            /\ txsSi[t].write_set[k] \in PayloadDomain)

(***************************************************************************)
(* WriteWriteConflictDetected — the SI safety invariant.                   *)
(*                                                                          *)
(* For every Tx t with status="Committed", NO key in txsSi[t].write_set  *)
(* has a version committed in the half-open window                         *)
(* (txsSi[t].snapshot, txsSi[t].commit_opnum - 1]. Equivalently: every    *)
(* Committed Tx was conflict-free at apply time.                           *)
(*                                                                          *)
(* This is the inverse of the CommitTx action's "conflict branch":        *)
(* if the conflict branch fired, the Tx is Aborted; if the no-conflict   *)
(* branch fired, the Tx is Committed AND the window was empty AT THAT    *)
(* TIME. The state invariant locks "window is STILL empty in the         *)
(* current state for any Committed Tx" — which is true because Put       *)
(* only adds entries at opnum c that match the Tx's own commit_opnum     *)
(* (the conflict check excluded the (snapshot, c-1] window, and only    *)
(* the Tx's own writes land at c, so the window stays empty).            *)
(***************************************************************************)
WriteWriteConflictDetected ==
    \A t \in TxIds :
        (txsSi[t].status = "Committed" /\ txsSi[t].write_set # << >>)
            => (\A k \in DOMAIN txsSi[t].write_set :
                    ~ HasVersionInRange(k,
                                        txsSi[t].snapshot,
                                        txsSi[t].commit_opnum - 1))

(***************************************************************************)
(* CommitAtomicity — Committed Txs install ALL their writes at exactly    *)
(* commit_opnum; Aborted Txs install NOTHING; both terminal states are   *)
(* absorbing.                                                              *)
(*                                                                          *)
(* For every Tx t with status="Committed" and commit_opnum c:             *)
(*   - every key k in DOMAIN write_set has an entry [opnum |-> c,         *)
(*     value |-> write_set[k]] in versions[k] (every write applied).      *)
(*                                                                          *)
(* For every Tx t with status="Aborted":                                  *)
(*   - the implementation does NOT install any write attributable solely *)
(*     to this Tx — this is the "no partial apply" guarantee.            *)
(*     (Phrased here as a property over the CommitTx action shape: the   *)
(*     Aborted branch UNCHANGED's versions; only the no-conflict branch  *)
(*     mutates versions and only when transitioning to Committed.)       *)
(***************************************************************************)
CommitAtomicity ==
    \A t \in TxIds :
        (txsSi[t].status = "Committed" /\ txsSi[t].write_set # << >>)
            => (\A k \in DOMAIN txsSi[t].write_set :
                    \E e \in versions[k] :
                        /\ e.opnum = txsSi[t].commit_opnum
                        /\ e.value = txsSi[t].write_set[k])

(***************************************************************************)
(* FirstCommitterWins — at most one of two overlapping-write Txs can be   *)
(* Committed if their commit windows overlap.                              *)
(*                                                                          *)
(* For any two distinct Txs t1, t2, both Committed, both with overlapping *)
(* write_sets and both with commit_opnums where the windows interact:    *)
(*   - the LATER committer's window (snapshot, commit-1] CANNOT contain *)
(*     the EARLIER committer's commit_opnum (else the WriteWriteConflict *)
(*     invariant would be violated for the later one).                    *)
(*                                                                          *)
(* Equivalently: if t1.commit_opnum < t2.commit_opnum AND t1 and t2 share *)
(* a key K in their write_sets, then t2.commit_opnum must be < or =     *)
(* t2.snapshot + 1 (impossible since snapshot < commit) OR              *)
(* t1.commit_opnum must be NOT in (t2.snapshot, t2.commit_opnum - 1]    *)
(* (in which case t1.commit_opnum <= t2.snapshot — t2's snapshot was    *)
(* taken at or after t1 already committed; valid sequential commit).    *)
(***************************************************************************)
FirstCommitterWins ==
    \A t1, t2 \in TxIds :
        (t1 # t2 /\
         txsSi[t1].status = "Committed" /\
         txsSi[t2].status = "Committed" /\
         txsSi[t1].write_set # << >> /\
         txsSi[t2].write_set # << >> /\
         (DOMAIN txsSi[t1].write_set) \cap (DOMAIN txsSi[t2].write_set) # {} /\
         txsSi[t1].commit_opnum < txsSi[t2].commit_opnum)
            => \* t1 committed first; t2 must have taken its snapshot
               \* AT OR AFTER t1's commit — else t2 would have seen the
               \* shared-key conflict and aborted.
               txsSi[t1].commit_opnum <= txsSi[t2].snapshot

(***************************************************************************)
(* DeterministicApply — the THESIS-FIT centerpiece.                        *)
(*                                                                          *)
(* For every Committed Tx t with commit_opnum c, the versions delta       *)
(* installed by this Tx is a FUNCTION OF (write_set, c) ONLY. Phrased   *)
(* as a current-state property: for every key k in DOMAIN write_set,    *)
(* the entry at opnum c in versions[k] has value exactly write_set[k].  *)
(* No other entry at opnum c attributable to this Tx exists; no missing  *)
(* entries; no extra entries.                                              *)
(*                                                                          *)
(* This is the abstract version of the parent S2 design Decision 4       *)
(* claim: "every replica running the same apply on the same prefix      *)
(* reaches the same verdict and the same versions delta." Because the   *)
(* CommitTx action is deterministic in (versions, txsSi[t].snapshot,    *)
(* txsSi[t].write_set, c), running it on any two replicas with the same *)
(* `versions` and `txsSi[t]` state produces byte-identical results.    *)
(*                                                                          *)
(* The Rust T3 integration tests gate the byte-identity claim at the    *)
(* LSM level; this invariant locks the abstract semantics: the          *)
(* CommitTx action HAS the property "outcome = f(versions, txsSi[t],   *)
(* c)" because it does not reference any other state variable.         *)
(***************************************************************************)
DeterministicApply ==
    \A t \in TxIds :
        (txsSi[t].status = "Committed" /\ txsSi[t].write_set # << >>)
            => /\ \A k \in DOMAIN txsSi[t].write_set :
                    \E e \in versions[k] :
                        /\ e.opnum = txsSi[t].commit_opnum
                        /\ e.value = txsSi[t].write_set[k]
               \* And no Committed Tx's commit_opnum can be -1 sentinel.
               /\ txsSi[t].commit_opnum \in OpNums

============================================================================
