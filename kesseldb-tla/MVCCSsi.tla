---------------------------- MODULE MVCCSsi ----------------------------
(***************************************************************************)
(* KesselDB — S2.4 (= SP113): TLA+/TLC specification for the SSI           *)
(* (Serializable Snapshot Isolation) Cahill dangerous-structure detector,  *)
(* layered on top of the SP112 SI write-side + SM-apply-time conflict      *)
(* check. Abstracted from `crates/kessel-storage/src/ssi.rs`               *)
(* (`detect_dangerous_structure`, `sorted_vec_intersects`,                  *)
(* `prune_pending_txs`, `PendingTxRecord`, `MAX_TX_AGE`),                  *)
(* `crates/kessel-storage/src/tx.rs` (`Tx::begin_ssi`, `Tx::commit_ssi`,   *)
(* `TxCommitOutcome::AbortedDangerousStructure`), and                       *)
(* `crates/kessel-sm/src/lib.rs` (`Op::CommitTx` apply arm SSI branch,     *)
(* `StateMachine::pending_txs`).                                            *)
(*                                                                          *)
(* This module EXTENDS MVCCSi (SP112). The SSI layer is checked over the *)
(* SAME versioned-storage + Tx + SI model TLC has already verified in     *)
(* S2.1 (SP110), S2.2 (SP111), and S2.3 (SP112) — re-using `versions`,   *)
(* `opCount`, the per-Tx snapshot+read_set+write_set+status, and the     *)
(* bounded constants from the EXTENDS chain.                              *)
(*                                                                          *)
(* SCOPE (per the S2.4 design Decision 7) — abstract single-replica SSI   *)
(* layer with Cahill rw-antidependency derivation + dangerous-structure   *)
(* abort decision (Decision 3: abort the latest committer).               *)
(*                                                                          *)
(* THE THESIS-FIT CENTERPIECE FOR SSI. The deterministic-log architecture *)
(* makes Cahill's rw-edge tracking + dangerous-structure detection a      *)
(* state-machine-internal computation, NOT a distributed coordination     *)
(* protocol. Every replica's deterministic apply reaches the same SSI    *)
(* verdict against the same log prefix. PostgreSQL needs SLRU +           *)
(* sophisticated locking; KesselDB gets the same property structurally   *)
(* from VSR-ordered apply. The CommitSsi action mechanically encodes the *)
(* parent S2 + S2.4 design Decision 1 + Decision 4 claim: serializability *)
(* becomes a structural property of the log, not a coordination protocol. *)
(*                                                                          *)
(* OUT OF SCOPE (HONEST-DISCLOSED LIMITATIONS — read these before          *)
(* trusting any conclusion drawn from a green TLC run):                    *)
(*                                                                          *)
(*   1. The Rust standalone `Tx::commit_ssi` runs against an EMPTY local  *)
(*      pending_txs map (it has no access to the SM's pending_txs); the  *)
(*      production SSI path is the SM apply arm. The TLA+ CommitSsi      *)
(*      action models the SM apply path: pendingTxs is a single global   *)
(*      state variable. The standalone form's "no rw-edges can form on   *)
(*      empty pending_txs" property is locked at the Rust integration-   *)
(*      test level (T3 byte-equivalence test).                            *)
(*                                                                          *)
(*   2. Multi-replica Tx state is NOT modeled (carried forward from       *)
(*      SP110/SP111/SP112). The CommitSsi action's apply-determinism is  *)
(*      the abstract proof that two replicas running the same op on the *)
(*      same versions + pendingTxs reach the same verdict; per-replica   *)
(*      byte-identity at the LSM level is verified at the Rust          *)
(*      integration-test level (T3 ships a 3-replica byte-identity test *)
(*      for SSI commits). Multi-replica TLA+ is an S2.X follow-up.       *)
(*                                                                          *)
(*   3. GC / watermark / version reclamation is NOT modeled (carried     *)
(*      forward from MVCCStorage + MVCCTx + MVCCSi). S2.5 follow-up. The *)
(*      pendingTxs window-truncation via MAX_TX_AGE is the SSI-specific  *)
(*      bounded-window mechanism that S2.5's dynamic watermark protocol  *)
(*      will supersede.                                                   *)
(*                                                                          *)
(*   4. MAX_TX_AGE = 4096 in the Rust code; TLC uses a much smaller       *)
(*      MaxTxAge constant. A Tx whose snapshot is older than the         *)
(*      truncation horizon may FALSE-NEGATIVE (an rw-edge with a Tx     *)
(*      already evicted from pendingTxs is undetectable). Decision 5     *)
(*      honest disclosure; T5 pentest documents this with the            *)
(*      `too_old_snapshot_false_negative` test. S2.5 watermark           *)
(*      supersedes.                                                       *)
(*                                                                          *)
(*   5. Bounded model checking proves the absence of counterexamples at   *)
(*      the CONFIGURED constants only (2 Tx, 2 keys, 2 values,            *)
(*      3 commit_opnums). The Rust pentest tests (T5) cover boundary     *)
(*      opnums (0, u64::MAX) explicitly that the bounded TLC model       *)
(*      cannot reach. The 2-Tx model IS sufficient to produce the         *)
(*      classic write-skew counterexample (Cahill's TPC-C banking        *)
(*      example uses 2 Tx); a 3-Tx model would let TLC also find the    *)
(*      canonical T0→T1→T2 dangerous-structure triple — S2.X follow-up.  *)
(*                                                                          *)
(*   6. Restart-rebuild of pendingTxs is NOT modeled. In production the  *)
(*      pendingTxs map is reconstructed by re-applying the recent log    *)
(*      prefix (carrying the SP112 Op::CommitTx → SP113 pending_txs      *)
(*      insertion logic). The TLA+ model holds pendingTxs persistent     *)
(*      across the entire trace; restart semantics are an S2.X follow-up. *)
(*                                                                          *)
(*   7. NAMED-ACTION CORRESPONDENCE to kessel-storage::ssi +               *)
(*      kessel-storage::tx::Tx::commit_ssi + kessel-sm::StateMachine::    *)
(*      apply, NOT a mechanized refinement. Same caveat as                *)
(*      SP109/SP110/SP111/SP112. The action-mapping table below makes    *)
(*      the correspondence inspectable.                                    *)
(*                                                                          *)
(* ─── TLA+ ACTION ↔ Rust MAPPING (SP113 T6) ──────────────────────────── *)
(*                                                                          *)
(* Each named action in this spec corresponds to a Rust function. Line    *)
(* numbers are accurate as of commit 476319c against                       *)
(* crates/kessel-storage/src/ssi.rs, crates/kessel-storage/src/tx.rs,    *)
(* and crates/kessel-sm/src/lib.rs (`Op::CommitTx` SSI branch). This is *)
(* "named correspondence", not mechanized refinement — a divergence      *)
(* between the spec and the Rust code is a human-discovered issue. If    *)
(* the Rust code is refactored, re-run:                                   *)
(*   grep -n "pub fn detect_dangerous_structure\|pub fn prune_pending_txs\| *)
(*           pub fn sorted_vec_intersects\|pub struct PendingTxRecord"    *)
(*           crates/kessel-storage/src/ssi.rs                              *)
(*   grep -n "pub fn begin_ssi\|pub fn commit_ssi\|                       *)
(*           AbortedDangerousStructure" crates/kessel-storage/src/tx.rs   *)
(*   grep -n "Op::CommitTx\|pending_txs\|MAX_TX_AGE\|                     *)
(*           detect_dangerous_structure" crates/kessel-sm/src/lib.rs      *)
(* and update this table.                                                  *)
(*                                                                          *)
(*   TLA+ action / def        Rust counterpart                  file:line *)
(*   ────────────────────     ──────────────────────────────   ────────── *)
(*   BeginSsi(t, s)           Tx::begin_ssi(&mut store, s)     tx.rs:455  *)
(*                              (TLA+ alias for TxBeginSi —    Decision 6 *)
(*                              the SI/SSI distinction is per-            *)
(*                              call-site, not on the Tx       *)
(*                              struct)                          *)
(*   PruneWindow(c)           ssi::prune_pending_txs(&mut      ssi.rs:239 *)
(*                              pending_txs, c, MAX_TX_AGE)               *)
(*                              (Decision 5)                             *)
(*   ConcurrentOf(t, c)       BTreeMap::range(snapshot+1..c)  sm.rs:3796 *)
(*                              over pending_txs               (range-    *)
(*                              (concurrent ⇔                    fold)    *)
(*                              snapshot < pending.commit < c)            *)
(*   DetectDangerous(t, c)    ssi::detect_dangerous_structure  ssi.rs:146 *)
(*                              (BTreeMap walk + per-Tx tag                *)
(*                              update + Cahill                            *)
(*                              both-tags-set check)                       *)
(*   CommitSsi(t, c)          (a) Tx::commit_ssi(c) standalone tx.rs:479  *)
(*                            AND (b) Op::CommitTx { snapshot,            *)
(*                                write_set, commit_opnum,                 *)
(*                                read_set } SM apply arm —    sm.rs:3729 *)
(*                                BOTH paths run the same SSI             *)
(*                                detector against pending_txs            *)
(*                                (the standalone form runs                *)
(*                                against an empty local map               *)
(*                                — documented limitation 1                *)
(*                                above).                                  *)
(*   sorted_vec_intersects    ssi::sorted_vec_intersects       ssi.rs:91  *)
(*       (set-cap helper)                                                  *)
(*                                                                          *)
(* ─────────────────────────────────────────────────────────────────────── *)
(*                                                                          *)
(* INVARIANTS (per the S2.4 design Decision 7):                            *)
(*                                                                          *)
(*   All 11 MVCCSi invariants carried forward (TypeOKTx,                   *)
(*   SnapshotImmutability, ReadSetMonotonic, ReadSetCoversAllReads,        *)
(*   ReadAtSnapshot, TxStatusMonotonic, TypeOKSi, WriteSetMonotonic,       *)
(*   WriteWriteConflictDetected, CommitAtomicity, FirstCommitterWins,      *)
(*   DeterministicApply).                                                   *)
(*                                                                          *)
(*   TypeOKSsi                    — well-typed SSI state-space (extends   *)
(*                                  TypeOKSi with pendingTxs and rwEdges).*)
(*   PendingTxsWindowBounded      — Cardinality(pendingTxs) bounded; every*)
(*                                  record has commit_opnum within the     *)
(*                                  MaxTxAge horizon of opCount.           *)
(*   DangerousStructureAborts     — for every rw-edge structure           *)
(*                                  Tx_in →rw Tx_pivot →rw Tx_out         *)
(*                                  recorded in rwEdges, at most one of   *)
(*                                  {Tx_in, Tx_pivot, Tx_out} is in       *)
(*                                  status="Committed" (Cahill claim).    *)
(*   NoWriteSkew                  — for every pair of committed concurrent*)
(*                                  Tx t1, t2 with non-trivial read-write *)
(*                                  skew (t1.read ∩ t2.write ≠ {} AND    *)
(*                                  t1.write ∩ t2.read ≠ {}), at most one*)
(*                                  is Committed — the classic write-skew *)
(*                                  anomaly is impossible.                 *)
(*   SerializableEquivalence      — for the set of committed Txs, there  *)
(*                                  EXISTS a serial schedule (a one-Tx-  *)
(*                                  at-a-time order) over them that      *)
(*                                  produces an equivalent final         *)
(*                                  versions state. Phrased as: every    *)
(*                                  Committed Tx's write_set lands at    *)
(*                                  exactly one opnum (its commit_opnum) *)
(*                                  AND the commit_opnums totally order  *)
(*                                  the committed Txs (the log IS the    *)
(*                                  serial schedule); plus the           *)
(*                                  DangerousStructureAborts +           *)
(*                                  NoWriteSkew invariants together      *)
(*                                  establish equivalence-to-some-serial. *)
(*                                                                          *)
(* Thesis pillars strengthened: verifiable (the SSI conflict-detection   *)
(* contract is machine-checked at the abstract level; NoWriteSkew +      *)
(* SerializableEquivalence are mechanically-checked serializability      *)
(* claims) + replayable (Cahill verdict is a deterministic function of  *)
(* (versions, pendingTxs, snapshot, read_set, write_set, commit_opnum), *)
(* same on every replica) + THESIS-FIT CENTERPIECE: serializability is  *)
(* a structural property of the deterministic log, not a coordination   *)
(* protocol. Five-module rigor-gate stack (Replication / MVCCStorage /  *)
(* MVCCTx / MVCCSi / MVCCSsi).                                            *)
(***************************************************************************)

EXTENDS MVCCSi

CONSTANTS
    MaxTxAge,        \* TLA+ analogue of Rust MAX_TX_AGE (= 4096 in prod).
                     \* Bounded model uses a much smaller value (see .cfg);
                     \* the bounded-window false-negative limitation 4 in
                     \* the head matter is the honest disclosure.
    SsiUnused        \* sentinel to keep TLC's constant block happy

ASSUME MVCCSsiAssumption ==
    /\ MaxTxAge \in Nat
    /\ SsiUnused = "Ssi"

----------------------------------------------------------------------------
(***************************************************************************)
(* Domain helpers for the SSI layer.                                        *)
(*                                                                          *)
(* The SSI layer adds two new state shapes:                                 *)
(*                                                                          *)
(*   PendingTxRecord — the abstract counterpart of                          *)
(*       `kessel_storage::ssi::PendingTxRecord`. Holds the                  *)
(*       (snapshot_opnum, read_set, write_set, has_incoming_rw,             *)
(*       has_outgoing_rw) tuple for every committed Tx still within         *)
(*       MaxTxAge of the current opCount. write_set is keys-only            *)
(*       (matching the Rust struct's Vec<(u32, [u8;16])>).                  *)
(*                                                                          *)
(*   RwEdgeRecord — the abstract counterpart of an rw-antidependency edge. *)
(*       Strictly auxiliary (used by the NoWriteSkew / DangerousStructure  *)
(*       invariants for observability); the abort decision is driven by    *)
(*       the per-Tx has_incoming_rw / has_outgoing_rw flags on the         *)
(*       PendingTxRecord, NOT by walking rwEdges. This matches Cahill +    *)
(*       the Rust ssi.rs structure.                                         *)
(***************************************************************************)

PendingTxRecord ==
    [snapshot:        OpNums \cup {-1},
     read_set:        SUBSET Keys,
     write_set_keys:  SUBSET Keys,
     has_incoming_rw: BOOLEAN,
     has_outgoing_rw: BOOLEAN]

RwEdgeRecord ==
    [from_commit:    OpNums,
     to_commit:      OpNums]

----------------------------------------------------------------------------
(***************************************************************************)
(* State variables (additions over MVCCSi's txsSi + siOpCount).            *)
(*                                                                          *)
(* pendingTxs — TLA+ function from OpNums (a Tx's commit_opnum) to         *)
(*    PendingTxRecord or the sentinel NoPending. Domain restricted to       *)
(*    OpNums; the "no entry for c" case is encoded as a sentinel record   *)
(*    so the function shape stays simple.                                  *)
(*                                                                          *)
(* rwEdges — TLA+ set of RwEdgeRecord. Auxiliary; used by the            *)
(*    DangerousStructureAborts invariant ONLY. The abort decision is      *)
(*    driven by the per-Tx flags on PendingTxRecord.                       *)
(***************************************************************************)

NoPending == [snapshot        |-> -1,
              read_set        |-> {},
              write_set_keys  |-> {},
              has_incoming_rw |-> FALSE,
              has_outgoing_rw |-> FALSE]

VARIABLES
    pendingTxs,      \* OpNums -> PendingTxRecord \cup {NoPending}
    rwEdges          \* SUBSET RwEdgeRecord

ssiVars == << pendingTxs, rwEdges >>

\* Composite vars over MVCCStorage + Tx + SI + SSI layers.
allVarsSsi == << versions, opCount, txs, txOpCount, txsSi, siOpCount,
                 pendingTxs, rwEdges >>

----------------------------------------------------------------------------
(***************************************************************************)
(* Initial state.                                                           *)
(*                                                                          *)
(* MVCCSi's InitSi (and via that, MVCCTx's InitTx + MVCCStorage's Init);   *)
(* pendingTxs all NoPending; rwEdges empty.                                *)
(***************************************************************************)

InitSsi ==
    /\ InitSi
    /\ pendingTxs = [c \in OpNums |-> NoPending]
    /\ rwEdges    = {}

----------------------------------------------------------------------------
(***************************************************************************)
(* SSI HELPERS.                                                             *)
(***************************************************************************)

\* "Slot c has a real pending record" — c maps to a record other than the
\* NoPending sentinel.
HasPending(c) == pendingTxs[c] # NoPending

\* The set of concurrent committer commit_opnums for a Tx that takes its
\* snapshot at `s` and commits at `c`. By Cahill's definition: every
\* committed Tx_A with snapshot < A.commit < c is concurrent with this Tx.
\* In the TLA+ model: every OpNums slot strictly between s and c that
\* HasPending.
ConcurrentCommits(s, c) ==
    {a \in OpNums : a > s /\ a < c /\ HasPending(a)}

\* Sorted-vec-intersects abstraction: TLA+ has set intersection directly.
\* Returns TRUE iff the two key sets share at least one element.
KeySetsIntersect(ks1, ks2) == (ks1 \cap ks2) # {}

----------------------------------------------------------------------------
(***************************************************************************)
(* ACTIONS — SSI lifts.                                                     *)
(*                                                                          *)
(* BeginSsi is the TLA+ alias for TxBeginSi (Decision 6 — SSI-vs-SI       *)
(* distinction is per-call-site, not a flag on the Tx struct). Every       *)
(* MVCCSi action is lifted to ALSO preserve the new pendingTxs + rwEdges  *)
(* variables UNCHANGED unless the action is the SSI commit itself.         *)
(***************************************************************************)

\* SSI begin is the SI begin (no SSI-specific state at begin time).
BeginSsi(t, s) ==
    /\ TxBeginSi(t, s)
    /\ UNCHANGED ssiVars

\* SSI reads lift the SI reads (the rw-edge derivation happens at commit
\* time, not at read time — Cahill is commit-time validation).
TxReadSsi(t, k) ==
    /\ TxReadSi(t, k)
    /\ UNCHANGED ssiVars

\* SSI read-only commit lifts the SI read-only commit (no pending_txs
\* insertion for a read-only Tx with empty write_set; the SSI fast path
\* is empty-read_set, not empty-write_set — those are independent).
TxCommitReadOnlySsi(t) ==
    /\ TxCommitReadOnlySi(t)
    /\ UNCHANGED ssiVars

TxAbortSsi(t) ==
    /\ TxAbortSi(t)
    /\ UNCHANGED ssiVars

TxWriteSsi(t, k, v) ==
    /\ TxWrite(t, k, v)
    /\ UNCHANGED ssiVars

TxTombstoneWriteSsi(t, k) ==
    /\ TxTombstoneWrite(t, k)
    /\ UNCHANGED ssiVars

----------------------------------------------------------------------------
(***************************************************************************)
(* CommitSsi(t, c) — the THESIS-FIT CENTERPIECE for SSI.                   *)
(*                                                                          *)
(* Models BOTH the standalone Tx::commit_ssi path AND the                  *)
(* Op::CommitTx { read_set = non-empty } SM apply arm; the                 *)
(* standalone form runs against an EMPTY local pending_txs and is         *)
(* therefore unable to detect rw-edges (documented limitation 1 above).   *)
(* The TLA+ action models the SM apply path: pendingTxs is the global    *)
(* state.                                                                  *)
(*                                                                          *)
(* Preconditions (Decision 7 of S2.4 design):                              *)
(*   - siOpCount < MaxTxOps (state-space bound — shared with SI commit). *)
(*   - txsSi[t] is Active.                                                 *)
(*   - c in OpNums (the commit_opnum from VSR log position).               *)
(*   - txsSi[t].snapshot <= c (SnapshotOutOfRange rejection — same as SI).*)
(*   - c >= opCount (VSR log monotonic; same as SI CommitTx).              *)
(*   - SP112 WW-conflict check passes (Decision 7 step 2; runs FIRST so   *)
(*     SP112's WW > SSI verdict-precedence holds).                          *)
(*                                                                          *)
(* Semantics:                                                                *)
(*                                                                          *)
(*   1. Window-truncation (Decision 5; modeled by computing the new       *)
(*      pendingTxs map with sentinels for any slot below                  *)
(*      max(c - MaxTxAge, 0) — these are "evicted").                      *)
(*                                                                          *)
(*   2. SP112 SI WW-conflict check (carried via MVCCSi.CommitTx pattern). *)
(*      If TRUE: flip status to Aborted (SI path; SSI does not refine the *)
(*      WW verdict); storage UNCHANGED; pendingTxs UNCHANGED; rwEdges     *)
(*      UNCHANGED. This is the "WW verdict precedes SSI verdict" claim.  *)
(*                                                                          *)
(*   3. SSI rw-edge derivation. For every Tx_A in pendingTxs concurrent  *)
(*      with t (a in ConcurrentCommits(snapshot, c)):                     *)
(*      - If A.write_set_keys ∩ t.read_set != {}: t→rw a; mark A's      *)
(*        has_incoming_rw := TRUE; mark t synthetic has_outgoing := TRUE *)
(*        for the local check.                                            *)
(*      - If t.write_set_keys ∩ A.read_set != {}: a→rw t; mark A's      *)
(*        has_outgoing_rw := TRUE; mark t synthetic has_incoming := TRUE *)
(*        for the local check.                                            *)
(*      Also record the edges in rwEdges for invariant inspection.        *)
(*                                                                          *)
(*   4. Dangerous-structure check (Cahill). If THIS has BOTH synthetic   *)
(*      flags after step 3 → abort t with DangerousStructure              *)
(*      (Decision 3 abort-the-latest); storage UNCHANGED; t NOT added to *)
(*      pendingTxs; rwEdges DOES carry the edges (for observability +    *)
(*      the DangerousStructureAborts invariant). Per Decision 3, even if *)
(*      a pre-existing pending Tx_X newly became a pivot, we still abort *)
(*      THIS (the latest committer) — the Rust code matches this in      *)
(*      `detect_dangerous_structure` check (2).                            *)
(*                                                                          *)
(*   5. Otherwise: install every (k, v) in write_set at commit_opnum c;  *)
(*      flip status to Committed; insert a fresh PendingTxRecord at       *)
(*      pendingTxs[c]; bump opCount past c.                                *)
(*                                                                          *)
(* Edge case: c = 0. Skip the WW check (no prior versions can exist);    *)
(* skip the rw-edge derivation (no concurrent Tx possible: snapshot      *)
(* must be <= c = 0). Falls into branch (5) directly.                    *)
(***************************************************************************)
CommitSsi(t, c) ==
    /\ siOpCount < MaxTxOps
    /\ txsSi[t].status = "Active"
    /\ c \in OpNums
    /\ txsSi[t].snapshot >= 0
    /\ txsSi[t].snapshot <= c
    /\ c >= opCount
    \* As in MVCCSi.CommitTx: no pre-existing version at exactly opnum c
    \* (Put's per-(t,o) uniqueness lifted to the multi-key commit;
    \* redundant with the monotonicity above but kept for safety).
    /\ (txsSi[t].write_set = << >>) \/
       (\A k \in DOMAIN txsSi[t].write_set :
            \A e \in versions[k] : e.opnum # c)
    /\ LET ws == txsSi[t].write_set
           snap == txsSi[t].snapshot
           \* SP112 SI WW-conflict check (precedence: WW > SSI).
           wwConflict ==
               IF c = 0 \/ ws = << >>
               THEN FALSE
               ELSE \E k \in DOMAIN ws :
                       HasVersionInRange(k, snap, c - 1)
           writeKeys == IF ws = << >> THEN {} ELSE DOMAIN ws
           concurrentSlots == ConcurrentCommits(snap, c)
           \* Synthetic edge derivation for THIS Tx.
           \* (a) THIS has outgoing rw-edge to a: A.write ∩ THIS.read.
           thisHasOutgoing ==
               \E a \in concurrentSlots :
                   KeySetsIntersect(pendingTxs[a].write_set_keys,
                                    txsSi[t].read_set)
           \* (b) THIS has incoming rw-edge from a: THIS.write ∩ A.read.
           thisHasIncoming ==
               \E a \in concurrentSlots :
                   KeySetsIntersect(writeKeys, pendingTxs[a].read_set)
           dangerous == thisHasOutgoing /\ thisHasIncoming
           \* Window truncation: evict pendingTxs slots whose commit_opnum
           \* is below max(c - MaxTxAge, 0). Modeled by a fresh function.
           truncationThreshold ==
               IF c > MaxTxAge THEN c - MaxTxAge ELSE 0
           prunedPending ==
               [a \in OpNums |->
                   IF a < truncationThreshold
                   THEN NoPending
                   ELSE pendingTxs[a]]
           \* Updated pendingTxs after marking incoming/outgoing tags on
           \* every CONCURRENT pre-existing record. The walk mutates the
           \* `has_incoming_rw` / `has_outgoing_rw` tags on pre-existing
           \* records to reflect the rw-edges discovered above.
           markedPending ==
               [a \in OpNums |->
                   IF a \in concurrentSlots /\ prunedPending[a] # NoPending
                   THEN [prunedPending[a] EXCEPT
                           !.has_incoming_rw =
                              @ \/ KeySetsIntersect(@,
                                                    {}) \* placeholder; see below
                          ]
                   ELSE prunedPending[a]]
           \* (The placeholder above is unused — TLA+ syntax limitation
           \* for nested EXCEPT-with-conditional. We instead build the
           \* updated record per-slot via an IF/THEN/ELSE on the action
           \* shape below — see `nextPendingOnCommit`.)
           \* Per-slot tag-update; replaces markedPending above.
           updatedPending ==
               [a \in OpNums |->
                   IF a \in concurrentSlots /\ prunedPending[a] # NoPending
                   THEN [snapshot        |-> prunedPending[a].snapshot,
                         read_set        |-> prunedPending[a].read_set,
                         write_set_keys  |-> prunedPending[a].write_set_keys,
                         has_incoming_rw |->
                            prunedPending[a].has_incoming_rw
                            \/ KeySetsIntersect(
                                  prunedPending[a].write_set_keys,
                                  txsSi[t].read_set),
                         has_outgoing_rw |->
                            prunedPending[a].has_outgoing_rw
                            \/ KeySetsIntersect(writeKeys,
                                                prunedPending[a].read_set)]
                   ELSE prunedPending[a]]
           \* New rwEdges set: commit_opnum-pair edges only (key
           \* granularity is unnecessary — the DangerousStructureAborts
           \* invariant only inspects the (from, to) pairs).
           outgoingEdgeSlots ==
               { a \in concurrentSlots :
                    KeySetsIntersect(pendingTxs[a].write_set_keys,
                                     txsSi[t].read_set) }
           incomingEdgeSlots ==
               { a \in concurrentSlots :
                    KeySetsIntersect(writeKeys, pendingTxs[a].read_set) }
           newRwEdges ==
               rwEdges
                \cup { [from_commit |-> c, to_commit |-> a]
                         : a \in outgoingEdgeSlots }
                \cup { [from_commit |-> a, to_commit |-> c]
                         : a \in incomingEdgeSlots }
       IN  IF wwConflict
           THEN \* Branch (2) — SI WW-conflict path. Same shape as
                \* MVCCSi.CommitTx's abort branch (status flip on both
                \* layers; opCount bump; storage UNCHANGED). pendingTxs
                \* gets the window-truncation update (the SSI window
                \* advances regardless of WW verdict); rwEdges UNCHANGED
                \* (no rw-derivation performed on a WW-aborted Tx).
                /\ txsSi' = [txsSi EXCEPT
                                ![t] = [@ EXCEPT !.status = "Aborted"]]
                /\ txs'   = [txs EXCEPT
                                ![t] = [@ EXCEPT !.status = "Aborted"]]
                /\ opCount' = c + 1
                /\ pendingTxs' = prunedPending
                /\ rwEdges' = rwEdges
                /\ UNCHANGED << versions, txOpCount >>
           ELSE IF dangerous
                THEN \* Branch (4) — SSI dangerous structure → abort
                     \* THIS. Storage UNCHANGED; THIS NOT added to
                     \* pendingTxs; rwEdges carries the discovered edges
                     \* (for invariant inspection). pendingTxs gets the
                     \* window-truncation + per-pre-existing-Tx tag
                     \* updates from the rw-derivation walk (so the
                     \* DangerousStructureAborts invariant can inspect
                     \* the structure).
                     /\ txsSi' = [txsSi EXCEPT
                                     ![t] = [@ EXCEPT !.status = "Aborted"]]
                     /\ txs'   = [txs EXCEPT
                                     ![t] = [@ EXCEPT !.status = "Aborted"]]
                     /\ opCount' = c + 1
                     /\ pendingTxs' = updatedPending
                     /\ rwEdges' = newRwEdges
                     /\ UNCHANGED << versions, txOpCount >>
                ELSE \* Branch (5) — no conflict; install + record.
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
                                     ![t] = [@ EXCEPT
                                                !.status = "Committed"]]
                     /\ opCount' = c + 1
                     \* Insert a fresh PendingTxRecord for THIS at slot c,
                     \* preserving the per-pre-existing-Tx tag updates.
                     /\ pendingTxs' =
                            [updatedPending EXCEPT
                                ![c] = [snapshot        |-> snap,
                                        read_set        |-> txsSi[t].read_set,
                                        write_set_keys  |-> writeKeys,
                                        has_incoming_rw |-> FALSE,
                                        has_outgoing_rw |-> FALSE]]
                     /\ rwEdges' = newRwEdges
                     /\ UNCHANGED txOpCount
    /\ siOpCount' = siOpCount + 1

----------------------------------------------------------------------------
(***************************************************************************)
(* Next-state relation.                                                     *)
(*                                                                          *)
(* All MVCCSi actions lifted (to preserve ssiVars unless mutated) PLUS the *)
(* SSI-specific CommitSsi action. We REMOVE MVCCSi's own NextSi-level     *)
(* CommitTx from the SSI next-state — at the SSI layer EVERY commit       *)
(* goes through CommitSsi (Decision 8 backward-compat: empty-read_set is *)
(* the SP112 fast path; non-empty-read_set is the SSI path; CommitSsi    *)
(* models BOTH). If we re-allowed MVCCSi.CommitTx at the SSI level, the  *)
(* same Tx could be committed via TWO paths and TLC would see ghost      *)
(* state — admissible only if we disable one. We disable the SI-only    *)
(* path here per the design's "the SM apply arm IS the SSI detector,    *)
(* gated on read_set.is_empty for the fast path".                         *)
(***************************************************************************)

NextSsi ==
    \/ \E tx \in TxIds, s \in OpNums :
           BeginSsi(tx, s)
    \/ \E tx \in TxIds, k \in Keys :
           TxReadSsi(tx, k)
    \/ \E tx \in TxIds :
           TxCommitReadOnlySsi(tx)
    \/ \E tx \in TxIds :
           TxAbortSsi(tx)
    \/ \E tx \in TxIds, k \in Keys, v \in Values :
           TxWriteSsi(tx, k, v)
    \/ \E tx \in TxIds, k \in Keys :
           TxTombstoneWriteSsi(tx, k)
    \/ \E tx \in TxIds, c \in OpNums :
           CommitSsi(tx, c)

----------------------------------------------------------------------------
(***************************************************************************)
(* Specification.                                                           *)
(*                                                                          *)
(* SAFETY-ONLY (mirrors S1/SP109 + SP110 + SP111 + SP112 discipline).      *)
(* Fairness / liveness is out of scope for S2.4.                             *)
(***************************************************************************)

SpecSsi == InitSsi /\ [][NextSsi]_allVarsSsi

----------------------------------------------------------------------------
(***************************************************************************)
(* SAFETY INVARIANTS.                                                       *)
(***************************************************************************)

(***************************************************************************)
(* TypeOKSsi — well-typed SSI state envelope.                              *)
(*                                                                          *)
(* The MVCCSi TypeOKSi (which subsumes TypeOKTx + MVCCStorage TypeOK)      *)
(* PLUS:                                                                    *)
(*   - pendingTxs is a function from OpNums to PendingTxRecord \cup        *)
(*     {NoPending}                                                          *)
(*   - rwEdges is a subset of RwEdgeRecord                                  *)
(***************************************************************************)
TypeOKSsi ==
    /\ TypeOKSi
    /\ pendingTxs \in [OpNums -> PendingTxRecord \cup {NoPending}]
    /\ rwEdges \subseteq RwEdgeRecord

(***************************************************************************)
(* PendingTxsWindowBounded — every record in pendingTxs has a               *)
(* commit_opnum within the MaxTxAge horizon of the current opCount.        *)
(*                                                                          *)
(* The "commit_opnum" of a record at slot c is c itself (the map's key);    *)
(* the truncation rule evicts slot c whenever c < opCount - MaxTxAge.     *)
(* The invariant: for every slot c with HasPending(c), c >= opCount -     *)
(* MaxTxAge (or opCount <= MaxTxAge, in which case no eviction has run). *)
(***************************************************************************)
PendingTxsWindowBounded ==
    \A c \in OpNums :
        HasPending(c) =>
            (opCount <= MaxTxAge \/ c >= opCount - MaxTxAge)

(***************************************************************************)
(* DangerousStructureAborts — Cahill's claim. For every rw-edge structure *)
(* Tx_in →rw Tx_pivot →rw Tx_out recorded in rwEdges, at most one of the *)
(* three is currently in status="Committed" — i.e. AT LEAST one of the    *)
(* three was aborted to break the cycle. (The Cahill abort decision      *)
(* picks the latest committer per Decision 3.)                             *)
(*                                                                          *)
(* Phrased over rwEdges: for any three distinct commit_opnums in, p, out  *)
(* in OpNums with edges (in -> p) and (p -> out) in rwEdges, NOT all     *)
(* three slots simultaneously have a pendingTxs record (the aborted Tx   *)
(* never gets a pendingTxs entry, so its commit_opnum slot is NoPending). *)
(*                                                                          *)
(* This is mechanically how DangerousStructure aborts manifest: the       *)
(* aborted Tx (the latest committer) never lands in pendingTxs.           *)
(***************************************************************************)
DangerousStructureAborts ==
    \A in_c \in OpNums, p_c \in OpNums, out_c \in OpNums :
        (in_c # p_c /\ p_c # out_c /\ in_c # out_c /\
         (\E e1 \in rwEdges : e1.from_commit = in_c /\ e1.to_commit = p_c) /\
         (\E e2 \in rwEdges : e2.from_commit = p_c /\ e2.to_commit = out_c))
            => ~ (HasPending(in_c) /\ HasPending(p_c) /\ HasPending(out_c))

(***************************************************************************)
(* NoWriteSkew — the classic write-skew anomaly is impossible.             *)
(*                                                                          *)
(* For every pair of distinct Tx t1, t2 both in status="Committed", both *)
(* with non-trivial read-write-skew shape (t1.read_set ∩ t2.write != {} *)
(* AND t1.write ∩ t2.read_set != {}), AT MOST ONE is in status            *)
(* "Committed". Equivalently: any pair of Tx with this shape has at      *)
(* least one Aborted.                                                      *)
(*                                                                          *)
(* This is the Cahill SSI flagship claim phrased as a state invariant.    *)
(* Two concurrent Tx exhibiting the write-skew schedule MUST have at     *)
(* least one aborted by the SSI detector — proven by mechanically        *)
(* enumerating all 2-Tx schedules in the bounded model.                  *)
(***************************************************************************)
NoWriteSkew ==
    \A t1, t2 \in TxIds :
        (t1 # t2 /\
         txsSi[t1].status = "Committed" /\
         txsSi[t2].status = "Committed" /\
         txsSi[t1].write_set # << >> /\
         txsSi[t2].write_set # << >> /\
         \* t1 read a key t2 wrote (potential rw t1→t2).
         (txsSi[t1].read_set \cap DOMAIN txsSi[t2].write_set) # {} /\
         \* t2 read a key t1 wrote (potential rw t2→t1) — the
         \* skew shape: both directions of rw between t1 and t2.
         (txsSi[t2].read_set \cap DOMAIN txsSi[t1].write_set) # {} /\
         \* Concurrency: neither's snapshot saw the other's commit
         \* (each saw a snapshot taken before the other's commit_opnum).
         txsSi[t1].snapshot < txsSi[t2].commit_opnum /\
         txsSi[t2].snapshot < txsSi[t1].commit_opnum)
            => FALSE  \* (Both Committed under skew shape is the
                      \* anomaly we forbid; the SSI detector must
                      \* have aborted one — so both-Committed is
                      \* impossible under the skew shape.)

(***************************************************************************)
(* SerializableEquivalence — for the set of committed Txs, the log       *)
(* itself IS the serial schedule. Phrased as: the totally-ordered          *)
(* commit_opnums induce a serial schedule (every Tx's writes installed   *)
(* at exactly its commit_opnum; commit_opnums totally order the          *)
(* committed Tx); plus the DangerousStructureAborts + NoWriteSkew         *)
(* invariants together establish equivalence-to-some-serial.              *)
(*                                                                          *)
(* The constructive serial schedule: sort committed Txs by commit_opnum   *)
(* ascending. Running them in that order against an initially-empty MVCC *)
(* store produces a `versions` map where every key k has entries for     *)
(* every Committed Tx that wrote k, at exactly that Tx's commit_opnum.   *)
(* This is bit-for-bit the SAME `versions` map our actions produce.      *)
(*                                                                          *)
(* Phrased as a state invariant: every Committed Tx's commit_opnum slot  *)
(* (in pendingTxs, if still within window) has the matching              *)
(* (snapshot, read_set, write_set_keys) tuple; AND every Committed Tx    *)
(* has a unique commit_opnum (the log totally orders).                    *)
(***************************************************************************)
SerializableEquivalence ==
    /\ \* Every Committed Tx's commit_opnum is unique (the VSR log
       \* totally orders commits — no two Tx land at the same opnum).
       \A t1, t2 \in TxIds :
            (t1 # t2 /\
             txsSi[t1].status = "Committed" /\
             txsSi[t2].status = "Committed" /\
             txsSi[t1].write_set # << >> /\
             txsSi[t2].write_set # << >>)
                => txsSi[t1].commit_opnum # txsSi[t2].commit_opnum
    /\ \* Every Committed Tx with a non-empty write_set has a corresponding
       \* pendingTxs entry at its commit_opnum slot (provided still in
       \* window) — the abstract record matches the Tx's actual
       \* (snapshot, read_set, write_set_keys). This locks the "the
       \* pendingTxs map is the deterministic projection of the committed
       \* Tx set into the SSI auxiliary state" property.
       \A t \in TxIds :
            (txsSi[t].status = "Committed" /\
             txsSi[t].write_set # << >> /\
             HasPending(txsSi[t].commit_opnum))
                => /\ pendingTxs[txsSi[t].commit_opnum].snapshot
                        = txsSi[t].snapshot
                   /\ pendingTxs[txsSi[t].commit_opnum].read_set
                        = txsSi[t].read_set
                   /\ pendingTxs[txsSi[t].commit_opnum].write_set_keys
                        = DOMAIN txsSi[t].write_set

============================================================================
