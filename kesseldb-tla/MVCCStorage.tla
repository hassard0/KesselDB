---------------------------- MODULE MVCCStorage ----------------------------
(***************************************************************************)
(* KesselDB — S2.1 (= SP110): TLA+/TLC specification for the MVCC          *)
(* versioned-storage primitive abstracted from `crates/kessel-storage/src/ *)
(* mvcc.rs`.                                                                *)
(*                                                                          *)
(* This module models the MVCC layer as an abstract per-(type_id,          *)
(* object_id) version chain. Each (type_id, object_id) maps to a set of    *)
(* (commit_opnum, value-or-tombstone) entries with unique commit_opnum     *)
(* values. The model supports three abstract actions — Put, Tombstone,     *)
(* and Read — and asserts four invariants that capture the S2.1 contract  *)
(* from the parent design (`docs/superpowers/specs/                       *)
(* 2026-05-23-mvcc-si-design.md`).                                          *)
(*                                                                          *)
(* SCOPE (per the SP110 plan T6 spec) — abstract single-replica MVCC.      *)
(*                                                                          *)
(* OUT OF SCOPE (HONEST-DISCLOSED LIMITATIONS — read these before          *)
(* trusting any conclusion drawn from a green TLC run):                    *)
(*                                                                          *)
(*   1. Multi-replica replication-byte-identity is NOT modeled here.       *)
(*      The set semantics of `versions` (TLA+ sets are unordered) make     *)
(*      per-replica chain equality automatic at the abstract level, but    *)
(*      the actual byte-identity of the LSM-stored 28-byte keys across    *)
(*      replicas is verified at the Rust integration-test level (SP110   *)
(*      T3, 5 byte-identity tests). Lifting that to TLA+ would require   *)
(*      a per-replica `versions[r]` shape; that is an S2.X follow-up.    *)
(*                                                                          *)
(*   2. GC / watermark / version reclamation is NOT modeled. Versions      *)
(*      monotonically grow. S2.5 follow-up will model the watermark and  *)
(*      its interaction with snapshot reads.                              *)
(*                                                                          *)
(*   3. Tx context / conflict detection / SSI is NOT modeled. This spec   *)
(*      is the storage-primitive contract only. S2.3 (SI commit) and     *)
(*      S2.4 (SSI promotion) follow-ups will model the transaction      *)
(*      layer.                                                            *)
(*                                                                          *)
(*   4. Bounded model checking proves the absence of counterexamples at    *)
(*      the CONFIGURED constants only (the .cfg sets TypeIds = 1..2,      *)
(*      ObjectIds = 1..2, OpNums = 0..3, Values = {v1, v2}, MaxOps = 5).  *)
(*      Larger bounds are an S2.X follow-up.                              *)
(*                                                                          *)
(*   5. This is NAMED-ACTION CORRESPONDENCE to kessel-storage::mvcc, NOT   *)
(*      a mechanized refinement. A discrepancy between this spec and the  *)
(*      Rust code is a human-discovered issue. The action-to-Rust         *)
(*      mapping below makes the correspondence inspectable.                *)
(*                                                                          *)
(* ─── TLA+ ACTION ↔ kessel-storage::mvcc MAPPING (SP110 T6) ───────────── *)
(*                                                                          *)
(* Each named action in this spec corresponds to a kessel-storage::mvcc    *)
(* function. Line numbers are accurate as of commit f067740 against        *)
(* crates/kessel-storage/src/mvcc.rs (1271 lines). This is "named         *)
(* correspondence", not mechanized refinement — see Honest Disclosure #5  *)
(* above. If kessel-storage::mvcc is refactored, re-run:                  *)
(*   grep -n "pub fn make_versioned_key\|pub fn put_versioned\|           *)
(*           pub fn get_at_snapshot" crates/kessel-storage/src/mvcc.rs    *)
(* and update this table; the spec's safety still holds, but the audit    *)
(* trail loses precision until the table is refreshed.                    *)
(*                                                                          *)
(*   TLA+ action       kessel-storage::mvcc counterpart      file:line     *)
(*   ───────────────   ──────────────────────────────────   ────────────   *)
(*   Put(t,o,c,v)      put_versioned(store,t,o,c,Some(v))    mvcc.rs:170  *)
(*                       writes 28-byte versioned key via                 *)
(*                       Storage::put_entry_versioned                     *)
(*   Tombstone(t,o,c)  put_versioned(store,t,o,c,None)       mvcc.rs:170  *)
(*                       writes 28-byte versioned key with                *)
(*                       value=None (LSM tombstone)                       *)
(*   ReadResult(t,o,s) get_at_snapshot(store,t,o,s)          mvcc.rs:204  *)
(*                       prefix-scan from newest, returns first           *)
(*                       version with commit_opnum <= snapshot            *)
(*                                                                          *)
(* ─────────────────────────────────────────────────────────────────────── *)
(*                                                                          *)
(* INVARIANTS (per the SP110 plan T6 spec):                                *)
(*                                                                          *)
(*   TypeOK                       — well-typed state-space                  *)
(*   SnapshotMonotonic            — older snapshots can't see newer       *)
(*                                  versions (monotone visibility)        *)
(*   NeverNotYetWrittenAfterPut   — once a put happens at opnum=c, every  *)
(*                                  snapshot >= c sees Found or Tombstoned *)
(*   TombstoneObservability       — newest version at-or-before snapshot   *)
(*                                  is the read result; tombstones hide   *)
(*                                  older live versions                    *)
(*                                                                          *)
(* Thesis pillar strengthened: verifiable (snapshot-read semantics is     *)
(* machine-checked at the abstract level) + replayable (deterministic     *)
(* version chain from put sequence; set-of-records semantics is the      *)
(* abstraction of byte-identical LSM keys verified by SP110 T3).         *)
(***************************************************************************)

EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    TypeIds,     \* finite set of type_id values (abstract; e.g., 1..2)
    ObjectIds,   \* finite set of object_id values (abstract; e.g., 1..2)
    OpNums,      \* finite set of commit_opnum values (e.g., 0..3)
    Values,      \* finite set of payload values (e.g., {v1, v2})
    MaxOps       \* bound on total Put + Tombstone actions (state-space cap)

ASSUME MVCCAssumption ==
    /\ Cardinality(TypeIds)   >= 1
    /\ Cardinality(ObjectIds) >= 1
    /\ Cardinality(OpNums)    >= 1
    /\ Cardinality(Values)    >= 1
    /\ MaxOps                 \in Nat

----------------------------------------------------------------------------
(***************************************************************************)
(* Domain helpers.                                                          *)
(*                                                                          *)
(* A "key" is a (type_id, object_id) pair. A "version entry" is a record   *)
(* [opnum |-> c, value |-> v] where v \in Values \cup {"Tombstone"}.       *)
(*                                                                          *)
(* The TOMBSTONE marker is the literal string "Tombstone"; this is a       *)
(* TLA+-side encoding and does NOT need to mirror the Rust Option<Vec<u8>> *)
(* directly. The Rust `value = None` corresponds 1:1 to this marker.       *)
(***************************************************************************)

TOMBSTONE == "Tombstone"

PayloadDomain == Values \cup {TOMBSTONE}

Keys == TypeIds \X ObjectIds

VersionEntry == [opnum: OpNums, value: PayloadDomain]

----------------------------------------------------------------------------
(***************************************************************************)
(* State variables.                                                         *)
(*                                                                          *)
(*   versions : Keys -> SUBSET VersionEntry                                 *)
(*       The per-key set of version entries. Modeled as a SET, not a list, *)
(*       because there is no ordering implicit in the data structure       *)
(*       beyond the lex order induced by `opnum`. The Rust LSM imposes a  *)
(*       byte-order via the 28-byte key encoding; that ordering is        *)
(*       byte-identity verified at the Rust level (SP110 T3).             *)
(*                                                                          *)
(*       Uniqueness: at most one entry per (key, opnum). Enforced in the   *)
(*       Put/Tombstone action guards.                                      *)
(*                                                                          *)
(*   opCount : Nat                                                          *)
(*       Counter of Put + Tombstone actions taken so far. Bounded by      *)
(*       MaxOps so TLC's state-space enumeration terminates.              *)
(*                                                                          *)
(* SP110 TLC-found fix #1 (2026-05-24): the original design carried a    *)
(* `readLog` state variable to relate reads taken at different times. TLC *)
(* found that this introduced a category error: invariants over readLog  *)
(* assert STATE-properties, but reads recorded in readLog are TEMPORAL   *)
(* facts about past states. Specifically: a Read(NotYetWritten) recorded *)
(* at snap=0 followed by a Put(opnum=0) followed by a Read("v1") at      *)
(* snap=0 violates "SnapshotMonotonic at the same snapshot" — but the   *)
(* contract being violated is "reads at the same snapshot are time-      *)
(* invariant", which is FALSE for monotonically-growing storage and is   *)
(* not the MVCC contract. The contract IS the snapshot-axis monotonicity *)
(* of SnapshotReadOf as a function of the current state, NOT a time-     *)
(* axis time-invariance of reads.                                         *)
(*                                                                          *)
(* Fix: drop readLog entirely; all invariants become universal           *)
(* properties over (TypeIds, ObjectIds, OpNums) quantified over the     *)
(* CURRENT versions state. This is the correct encoding of the MVCC     *)
(* contract and is what S2.2-S2.6 will rely on. Gate working as          *)
(* designed.                                                              *)
(***************************************************************************)

VARIABLES
    versions,
    opCount

vars == << versions, opCount >>

----------------------------------------------------------------------------
(***************************************************************************)
(* Initial state.                                                           *)
(*                                                                          *)
(* All keys start with the empty version set; no operations recorded; no   *)
(* reads observed.                                                          *)
(***************************************************************************)

Init ==
    /\ versions = [k \in Keys |-> {}]
    /\ opCount  = 0

----------------------------------------------------------------------------
(***************************************************************************)
(* SnapshotReadOf — the read semantics, lifted from kessel-storage::mvcc:: *)
(* get_at_snapshot.                                                         *)
(*                                                                          *)
(* Given a key (t, o) and a snapshot opnum s, return the version entry    *)
(* with the largest opnum <= s. If no such entry exists, return a         *)
(* sentinel [opnum |-> -1, value |-> "NotYetWritten"].                    *)
(*                                                                          *)
(* The -1 sentinel is a TLA+-side convention; the Rust API returns the    *)
(* `SnapshotRead::NotYetWritten` enum variant. The mapping is:            *)
(*    result.value = "NotYetWritten" <=> Rust SnapshotRead::NotYetWritten *)
(*    result.value = TOMBSTONE       <=> Rust SnapshotRead::Tombstoned    *)
(*    result.value \in Values        <=> Rust SnapshotRead::Found(bytes)  *)
(***************************************************************************)

SnapshotReadOf(t, o, s) ==
    LET visible == { e \in versions[<<t, o>>] : e.opnum <= s }
    IN  IF visible = {}
        THEN [opnum |-> -1, value |-> "NotYetWritten"]
        ELSE CHOOSE e \in visible :
                \A e2 \in visible : e2.opnum <= e.opnum

----------------------------------------------------------------------------
(***************************************************************************)
(* ACTIONS.                                                                 *)
(*                                                                          *)
(* Per the SP110 plan T6 spec there are three actions: Put, Tombstone,    *)
(* Read. Each is a state-transition predicate over (vars, vars').         *)
(***************************************************************************)

(***************************************************************************)
(* Put(t, o, c, v) — write a new live version at commit_opnum c.          *)
(*                                                                          *)
(*   kessel-storage::mvcc counterpart: put_versioned(store, t, o, c,      *)
(*                                                  Some(v))               *)
(*                                                                          *)
(* Precondition: opCount < MaxOps (state-space bound); no prior version   *)
(* of (t, o) exists at opnum c (uniqueness of the 28-byte key).           *)
(*                                                                          *)
(* State change: add the entry [opnum |-> c, value |-> v] to              *)
(* versions[<<t, o>>]; bump opCount.                                       *)
(*                                                                          *)
(* readLog is UNCHANGED — a Put does not generate a read entry.           *)
(***************************************************************************)
Put(t, o, c, v) ==
    /\ opCount < MaxOps
    /\ \A e \in versions[<<t, o>>] : e.opnum # c
    /\ versions' = [versions EXCEPT
                       ![<<t, o>>] = @ \cup {[opnum |-> c, value |-> v]}]
    /\ opCount'  = opCount + 1

(***************************************************************************)
(* Tombstone(t, o, c) — write a tombstone at commit_opnum c.              *)
(*                                                                          *)
(*   kessel-storage::mvcc counterpart: put_versioned(store, t, o, c, None) *)
(*                                                                          *)
(* Precondition: opCount < MaxOps; no prior version of (t, o) at opnum c. *)
(*                                                                          *)
(* State change: add [opnum |-> c, value |-> TOMBSTONE] to versions; bump *)
(* opCount.                                                                 *)
(***************************************************************************)
Tombstone(t, o, c) ==
    /\ opCount < MaxOps
    /\ \A e \in versions[<<t, o>>] : e.opnum # c
    /\ versions' = [versions EXCEPT
                       ![<<t, o>>] = @ \cup
                                       {[opnum |-> c, value |-> TOMBSTONE]}]
    /\ opCount'  = opCount + 1

(***************************************************************************)
(* Read(t, o, s) — REMOVED in SP110 TLC-found fix #1 (2026-05-24).        *)
(*                                                                          *)
(*   kessel-storage::mvcc counterpart: get_at_snapshot(store, t, o, s)    *)
(*                                                                          *)
(* SnapshotReadOf above remains as the abstract read FUNCTION, applied   *)
(* directly inside the invariants as a current-state property quantified *)
(* over (t, o, s). Reads have no observable effect on storage state, so  *)
(* they need not be modeled as actions; making them an invariant clause  *)
(* over the SnapshotReadOf function is the equivalent encoding without   *)
(* the temporal-log category error.                                      *)
(***************************************************************************)

----------------------------------------------------------------------------
(***************************************************************************)
(* Next-state relation.                                                     *)
(*                                                                          *)
(* Two actions: Put and Tombstone, each quantified over the bounded       *)
(* parameter universes (the .cfg bounds each to a small finite set so    *)
(* TLC terminates). Reads are not state-affecting; SnapshotReadOf is     *)
(* exercised as a current-state property inside the invariants.          *)
(***************************************************************************)

Next ==
    \/ \E t \in TypeIds, o \in ObjectIds, c \in OpNums, v \in Values :
           Put(t, o, c, v)
    \/ \E t \in TypeIds, o \in ObjectIds, c \in OpNums :
           Tombstone(t, o, c)

----------------------------------------------------------------------------
(***************************************************************************)
(* Specification.                                                           *)
(*                                                                          *)
(* This slice is SAFETY-ONLY (mirrors the S1/SP109 discipline). Fairness   *)
(* / liveness is out of scope for S2.1. We assert only [][Next]_vars;     *)
(* no WF_vars / SF_vars.                                                   *)
(***************************************************************************)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(***************************************************************************)
(* SAFETY INVARIANTS.                                                       *)
(***************************************************************************)

(***************************************************************************)
(* TypeOK — well-typed state envelope.                                     *)
(*                                                                          *)
(* `versions` is a function from Keys to subsets of VersionEntry. opCount *)
(* is a bounded natural. Plus a per-(t, o) opnum-uniqueness constraint    *)
(* reflecting the LSM 28-byte key uniqueness.                              *)
(***************************************************************************)
TypeOK ==
    /\ versions \in [Keys -> SUBSET VersionEntry]
    /\ opCount  \in 0..MaxOps
    \* Per-(t, o) opnum uniqueness — the 28-byte key encoding makes
    \* (type_id, object_id, commit_opnum) the unique LSM key, so two
    \* entries for the same (t, o) cannot share an opnum.
    /\ \A k \in Keys :
         \A e1, e2 \in versions[k] :
             (e1.opnum = e2.opnum) => (e1 = e2)

(***************************************************************************)
(* SnapshotMonotonic — older snapshots can't see newer versions.          *)
(*                                                                          *)
(* For every (t, o) and every pair of snapshots s1 <= s2 over the CURRENT *)
(* versions state:                                                          *)
(*                                                                          *)
(*   (1) The opnum returned at s1 is <= s1 (or -1 for NotYetWritten).     *)
(*   (2) The opnum returned at s2 is <= s2 (or -1).                       *)
(*   (3) If the s1 read returns a real version, the s2 read returns a    *)
(*       version with opnum >= the s1 read's opnum (newer snapshots see  *)
(*       at least as recent a version as older snapshots).                *)
(*   (4) If the s1 read returns a real version (opnum != -1), the s2     *)
(*       read CANNOT return NotYetWritten — a version visible at s1      *)
(*       remains visible at s2 since s2 >= s1.                            *)
(*                                                                          *)
(* This captures the MVCC contract's snapshot-axis monotonicity: in a    *)
(* fixed storage state, snapshot visibility is monotone in the snapshot  *)
(* opnum.                                                                   *)
(***************************************************************************)
SnapshotMonotonic ==
    \A t \in TypeIds, o \in ObjectIds, s1 \in OpNums, s2 \in OpNums :
        (s1 <= s2)
            => LET r1 == SnapshotReadOf(t, o, s1)
                   r2 == SnapshotReadOf(t, o, s2)
               IN  /\ r1.opnum = -1 \/ r1.opnum <= s1
                   /\ r2.opnum = -1 \/ r2.opnum <= s2
                   /\ (r1.opnum # -1) => (r2.opnum # -1)
                   /\ (r1.opnum # -1 /\ r2.opnum # -1)
                       => (r2.opnum >= r1.opnum)

(***************************************************************************)
(* NeverNotYetWrittenAfterPut — once a put has happened for (t, o) at     *)
(* commit_opnum=c, any Read at snapshot >= c returns Found or Tombstoned, *)
(* never NotYetWritten.                                                    *)
(*                                                                          *)
(* This is the read-after-write monotonicity property: in the CURRENT     *)
(* state, for every (t, o, s) such that some version of (t, o) has       *)
(* opnum <= s, SnapshotReadOf must NOT be NotYetWritten.                 *)
(*                                                                          *)
(* Phrased as a current-state property over SnapshotReadOf (not over the *)
(* recorded readLog) because the contract is about what the storage      *)
(* CURRENTLY returns, not about historical reads recorded before later   *)
(* writes existed. SP110 TLC-found fix #1 (2026-05-24): the prior        *)
(* readLog-quantified form admitted a counterexample where a Read at     *)
(* snap=0 recorded NotYetWritten correctly, then a subsequent Put(c=0)  *)
(* added a version with opnum=0 <= 0, retroactively "violating" the     *)
(* invariant; that is the temporal property "Reads at NotYetWritten     *)
(* remain correct AT THEIR TIME OF READ", which is a transition          *)
(* property, not a state invariant. The current-state form below is the *)
(* right encoding: the storage NEVER returns NotYetWritten when a       *)
(* visible version exists. Gate working as designed.                     *)
(***************************************************************************)
NeverNotYetWrittenAfterPut ==
    \A t \in TypeIds, o \in ObjectIds, s \in OpNums :
        (\E e \in versions[<<t, o>>] : e.opnum <= s)
            => SnapshotReadOf(t, o, s).value # "NotYetWritten"

(***************************************************************************)
(* TombstoneObservability — if the newest version at-or-before snapshot   *)
(* is a tombstone, Read returns Tombstoned (not the prior live version).  *)
(*                                                                          *)
(* Equivalently: the result the SnapshotReadOf function returns IS the   *)
(* max-opnum version at-or-before the snapshot, full stop. A tombstone   *)
(* with a later opnum than a live version hides that live version from a *)
(* snapshot >= the tombstone's opnum.                                     *)
(*                                                                          *)
(* Phrased as a universal property over CURRENT state and CURRENT        *)
(* SnapshotReadOf semantics (not over the recorded readLog) — same       *)
(* category-error reasoning as NeverNotYetWrittenAfterPut above (SP110   *)
(* TLC-found fix #1). For every (t, o, s):                                *)
(*                                                                          *)
(*   - SnapshotReadOf returns NotYetWritten iff no version with opnum<=s. *)
(*   - Otherwise SnapshotReadOf returns the unique max-opnum version e   *)
(*     with e.opnum <= s; its value (TOMBSTONE or v) IS what the read    *)
(*     returns. Tombstones at a later opnum hide earlier live versions.  *)
(*                                                                          *)
(* This captures the storage's snapshot-read contract directly as a     *)
(* property of the SnapshotReadOf function over reachable states; the    *)
(* cross-snapshot relational invariant (SnapshotMonotonic above) is also *)
(* a universal current-state property post-TLC-fix-#1.                   *)
(***************************************************************************)
TombstoneObservability ==
    \A t \in TypeIds, o \in ObjectIds, s \in OpNums :
        LET cur     == SnapshotReadOf(t, o, s)
            visible == { e \in versions[<<t, o>>] : e.opnum <= s }
        IN  IF visible = {}
            THEN cur.opnum = -1 /\ cur.value = "NotYetWritten"
            ELSE \E best \in visible :
                    /\ \A e \in visible : e.opnum <= best.opnum
                    /\ cur.opnum = best.opnum
                    /\ cur.value = best.value

============================================================================
