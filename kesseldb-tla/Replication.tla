---------------------------- MODULE Replication ----------------------------
(***************************************************************************)
(* KesselDB — S1 (= SP109): TLA+/TLC safety specification for the          *)
(* VSR replication protocol abstracted from `crates/kessel-vsr`.           *)
(*                                                                          *)
(* This module models Viewstamped Replication (Oki & Liskov 1988) as       *)
(* implemented in kessel-vsr: a primary-backup quorum protocol with a      *)
(* view-change recovery path that picks the most up-to-date log from the   *)
(* quorum of DoViewChange messages. The model is parametric over the       *)
(* replica set (CONSTANT Replicas), bounded message loss (MaxDrops),       *)
(* bounded view-change escalations (MaxViewChanges), and bounded client    *)
(* requests (MaxRequests), so TLC's state-space exploration terminates.    *)
(*                                                                          *)
(* SCOPE (per the SP109 design spec, Decision 2) — full normal-mode +      *)
(* minimal view-change.                                                     *)
(*                                                                          *)
(* OUT OF SCOPE (HONEST-DISCLOSED LIMITATIONS — read these before          *)
(* trusting any conclusion drawn from a green TLC run):                    *)
(*                                                                          *)
(*   1. State transfer (Msg::GetState / Msg::NewState) is NOT modeled.     *)
(*      The model assumes view-change reconciles divergent logs via the    *)
(*      DoViewChange-quorum "pick highest (normal_view, log-length)"       *)
(*      rule. A real lagging replica catches up via GetState/NewState in   *)
(*      kessel-vsr; that path is an S1.5 follow-up.                        *)
(*                                                                          *)
(*   2. Client-table idempotence / exactly-once reply replay is NOT        *)
(*      modeled. The kessel-vsr `client_table` lets a retransmitted        *)
(*      request return the cached committed reply without re-execution;   *)
(*      that lives above the log and is a state-machine-level concern. An *)
(*      S1.3 linearizability follow-up will need it.                       *)
(*                                                                          *)
(*   3. Persistence across crash-stop restart is NOT modeled. The model    *)
(*      has no Crash(r)/Restart(r) action pair; in particular `applied[r]` *)
(*      is never reset to a smaller value. S1.6 follow-up.                 *)
(*                                                                          *)
(*   4. The five invariants below are SAFETY-only. Liveness (eventual      *)
(*      commit; eventual view-change completion) requires temporal/        *)
(*      fairness formulas and is deferred to S1.2.                         *)
(*                                                                          *)
(*   5. Bounded model checking proves the absence of counterexamples at    *)
(*      the CONFIGURED constants only (the .cfg in T3 will set Replicas = *)
(*      {r1, r2, r3}, MaxDrops = 3, MaxViewChanges = 2, MaxRequests =     *)
(*      3). Bigger configurations (N=5, more drops, more requests) are    *)
(*      S1.4 follow-up.                                                    *)
(*                                                                          *)
(*   6. This is NAMED-ACTION CORRESPONDENCE to kessel-vsr, NOT a           *)
(*      mechanized refinement. A discrepancy between this spec and the    *)
(*      Rust code is a human-discovered issue; closing that gap is S1.7   *)
(*      follow-up. The action-to-Rust mapping table below makes the       *)
(*      correspondence inspectable.                                        *)
(*                                                                          *)
(* ACTION-TO-RUST MAPPING (per Decision 5 of the design spec):             *)
(*                                                                          *)
(*   TLA+ action          kessel-vsr counterpart                            *)
(*   -----------------    -------------------------------------             *)
(*   ClientRequest        Msg::Request   / Replica::on_request              *)
(*   Prepare              Msg::Prepare   / Replica::on_prepare              *)
(*   PrepareOk            Msg::PrepareOk / Replica::on_prepare_ok           *)
(*   Commit               Msg::Commit    / Replica::on_commit_msg           *)
(*   Apply                Replica::apply_through (body)                     *)
(*   TimeoutPrimary       Replica::tick (idle-tick branch)                  *)
(*                        -> Replica::start_view_change                     *)
(*   StartViewChange      Msg::StartViewChange / Replica::on_svc            *)
(*   DoViewChange         Msg::DoViewChange    / Replica::maybe_finish_svc  *)
(*   BecomePrimary        Replica::maybe_finish_view_change                 *)
(*   StartView            Msg::StartView / Replica::on_start_view           *)
(*   DropMessage          (no Rust fn; sim drop_pct injector counterpart)  *)
(*                                                                          *)
(* INVARIANTS (per Decision 4 of the design spec):                         *)
(*                                                                          *)
(*   TypeOK                — well-typed state-space                         *)
(*   LogPrefixSafety       — committed prefixes mutually consistent        *)
(*   NoDivergence          — same-length committed prefixes byte-identical  *)
(*   ExactlyOnceApply      — each committed op applied exactly once         *)
(*   MonotonicCommitPoint  — commit point non-decreasing across actions    *)
(*                                                                          *)
(* T4 (the kessel-vsr-line-number mapping comment) will be a follow-up to *)
(* this T2; this file is the structural spec.                              *)
(***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Replicas,           \* set of replica identifiers, e.g. {r1, r2, r3}
    MaxDrops,           \* max total message drops (bounds state space)
    MaxViewChanges,     \* max total view-change escalations
    MaxRequests         \* max total client requests injected

ASSUME ReplicaAssumption ==
    /\ Cardinality(Replicas) >= 1
    /\ MaxDrops      \in Nat
    /\ MaxViewChanges \in Nat
    /\ MaxRequests   \in Nat

----------------------------------------------------------------------------
(***************************************************************************)
(* Derived helpers.                                                         *)
(***************************************************************************)

\* Replica count and quorum size (majority).
N      == Cardinality(Replicas)
Quorum == (N \div 2) + 1

\* A fixed total ordering of the replicas. CHOOSE is deterministic for a
\* given set — TLC evaluates this once per behavior so it acts as a fixed
\* (canonical) ordering of Replicas into 1..N. PrimaryOf(v) picks the
\* (v mod N) + 1-th element, mirroring kessel-vsr's `primary_of` which is
\* the (view % n)-th replica in idx order.
ReplicaSeq ==
    CHOOSE seq \in [1..N -> Replicas] :
        \A i, j \in 1..N : (i # j) => (seq[i] # seq[j])

PrimaryOf(v) == ReplicaSeq[((v % N) + 1)]

\* `min` and `max` on Nat — useful for the invariants and action guards.
Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b

----------------------------------------------------------------------------
(***************************************************************************)
(* Domain types.                                                            *)
(*                                                                          *)
(* An Entry is the abstract log-entry record: opnum is the monotonically   *)
(* increasing operation number (mirrors LogEntry::op_number in Rust);      *)
(* client is the client identifier that issued the request; req is the    *)
(* per-client request sequence number. Concrete operation payloads are    *)
(* intentionally abstracted (the safety invariants are about log-prefix    *)
(* identity, not about op semantics).                                       *)
(***************************************************************************)

\* Opaque "operations" — bounded sub-universes for TLC enumeration. The
\* bounds mirror the TypeOK envelope below so they do NOT change protocol
\* semantics; they merely make the Messages record-set finite (TLC bails
\* on Nat-valued record fields during initial-state enumeration). Clients
\* is bounded to 1..MaxRequests because the ClientRequest action assigns
\* `client |-> requested + 1`, which grows up to MaxRequests; this is the
\* tightest bound that doesn't reject any reachable state. SP109 T3-TLC-
\* found fix #2 (2026-05-23 — first attempt used Clients=1..1, TypeOK-
\* violated at trace state 3 client=2; honest disclosure of the gate
\* working as designed twice on the way to the rigor-checkpoint baseline).
OpNums       == 1..MaxRequests
CommitPoints == 0..MaxRequests
Views        == 0..(MaxViewChanges + 1)
Clients      == 1..MaxRequests
Reqs         == 1..MaxRequests

Entries == [opnum: OpNums, client: Clients, req: Reqs]

\* Message-kind universes. Each is a record set; the spec uses tagged
\* records with a kind field for clarity. Bounded sub-universes are used
\* so TypeOK can give TLC a finite envelope.
\* (Sequences over Entries are bounded by MaxRequests in length.)
BoundedLogs == UNION { [1..k -> Entries] : k \in 0..MaxRequests }
\* Note: a TLA+ sequence of length 0 is << >>; [1..0 -> Entries] yields << >>.

Messages ==
    [kind: {"Prepare"},
     view: Views, opnum: OpNums, entry: Entries, commit: CommitPoints,
     from: Replicas, to: Replicas]
  \cup
    [kind: {"PrepareOk"},
     view: Views, opnum: OpNums,
     from: Replicas, to: Replicas]
  \cup
    [kind: {"Commit"},
     view: Views, commit: CommitPoints,
     from: Replicas, to: Replicas]
  \cup
    [kind: {"StartViewChange"},
     view: Views, from: Replicas, to: Replicas]
  \cup
    [kind: {"DoViewChange"},
     view: Views, log: BoundedLogs, commit: CommitPoints, normalView: Views,
     from: Replicas, to: Replicas]
  \cup
    [kind: {"StartView"},
     view: Views, log: BoundedLogs, commit: CommitPoints,
     from: Replicas, to: Replicas]

----------------------------------------------------------------------------
(***************************************************************************)
(* State variables.                                                         *)
(*                                                                          *)
(*   log[r]            sequence of Entries known to replica r               *)
(*   commit[r]         length of r's committed prefix (Nat; 0 = none)       *)
(*   view[r]           r's current view number                              *)
(*   normalView[r]     last view in which r was Status::Normal              *)
(*                     (kessel-vsr's `normal_view`; used by DoViewChange's  *)
(*                     "pick most-up-to-date log" rule)                     *)
(*   status[r]         "Normal" or "ViewChange"                             *)
(*   applied[r]        sequence of opnums that have been applied at r       *)
(*                     (mirrors kessel-vsr's apply_through side-effect on  *)
(*                     the state machine; used by ExactlyOnceApply)        *)
(*   msgs              SET of in-flight messages (a network model that     *)
(*                     supports drop + reorder; bounded by the action      *)
(*                     guards rather than by an explicit queue length)     *)
(*   dropped           count of DropMessage actions taken so far            *)
(*   viewChanges       count of view-change escalations so far              *)
(*   requested         count of ClientRequest actions taken so far          *)
(***************************************************************************)

VARIABLES
    log,
    commit,
    view,
    normalView,
    status,
    applied,
    msgs,
    dropped,
    viewChanges,
    requested

vars == << log, commit, view, normalView, status, applied,
           msgs, dropped, viewChanges, requested >>

----------------------------------------------------------------------------
(***************************************************************************)
(* Initial state.                                                           *)
(*                                                                          *)
(* Every replica starts in view 0, status Normal, with an empty log and    *)
(* no applied operations; no messages in flight; no drops/view-changes/    *)
(* requests yet. View 0's primary is PrimaryOf(0).                          *)
(***************************************************************************)

Init ==
    /\ log         = [r \in Replicas |-> << >>]
    /\ commit      = [r \in Replicas |-> 0]
    /\ view        = [r \in Replicas |-> 0]
    /\ normalView  = [r \in Replicas |-> 0]
    /\ status      = [r \in Replicas |-> "Normal"]
    /\ applied     = [r \in Replicas |-> << >>]
    /\ msgs        = {}
    /\ dropped     = 0
    /\ viewChanges = 0
    /\ requested   = 0

----------------------------------------------------------------------------
(***************************************************************************)
(* Type invariant (TypeOK).                                                 *)
(*                                                                          *)
(* TLA+ has no static type system; TypeOK is the convention. It captures   *)
(* the well-formed shape of every reachable state — primarily a sanity     *)
(* check that the action bodies preserve the intended structure (e.g., a  *)
(* commit point never exceeds the log length; a status is one of the two   *)
(* legal values). TLC checks TypeOK on every state.                        *)
(*                                                                          *)
(* Thesis pillar strengthened: verifiable.                                  *)
(***************************************************************************)

TypeOK ==
    /\ log         \in [Replicas -> BoundedLogs]
    /\ commit      \in [Replicas -> 0..MaxRequests]
    /\ view        \in [Replicas -> 0..(MaxViewChanges + 1)]
    /\ normalView  \in [Replicas -> 0..(MaxViewChanges + 1)]
    /\ status      \in [Replicas -> {"Normal", "ViewChange"}]
    /\ applied     \in [Replicas -> Seq(OpNums)]
    /\ msgs        \subseteq Messages
    /\ dropped     \in 0..MaxDrops
    /\ viewChanges \in 0..MaxViewChanges
    /\ requested   \in 0..MaxRequests
    \* Structural consistency: commit cannot exceed log length.
    /\ \A r \in Replicas : commit[r] <= Len(log[r])
    \* Applied prefix and log prefix agree on opnums up to commit[r].
    /\ \A r \in Replicas : Len(applied[r]) <= commit[r]

----------------------------------------------------------------------------
(***************************************************************************)
(* Helper predicates and queries over the in-flight message set.           *)
(***************************************************************************)

\* All PrepareOk messages received by replica r for (view, opnum).
\* (kessel-vsr's primary aggregates these in `self.prepare_ok`.)
PrepareOkAcks(v, n, p) ==
    { m \in msgs :
        /\ m.kind  = "PrepareOk"
        /\ m.view  = v
        /\ m.opnum = n
        /\ m.to    = p }

\* Set of (distinct) replicas that have sent PrepareOk for (v, n) to p.
PrepareOkSenders(v, n, p) ==
    { m.from : m \in PrepareOkAcks(v, n, p) }

\* StartViewChange votes seen by replica r for view v.
SvcVotes(v, r) ==
    { m.from : m \in
        { mm \in msgs :
            /\ mm.kind = "StartViewChange"
            /\ mm.view = v
            /\ mm.to   = r } }

\* DoViewChange messages received by primary-elect p for view v.
DvcAt(v, p) ==
    { m \in msgs :
        /\ m.kind = "DoViewChange"
        /\ m.view = v
        /\ m.to   = p }

\* Set of replicas that sent a DoViewChange for view v to primary-elect p.
DvcSenders(v, p) == { m.from : m \in DvcAt(v, p) }

----------------------------------------------------------------------------
(***************************************************************************)
(* ACTIONS.                                                                 *)
(*                                                                          *)
(* Per the SP109 design (Decision 5), there are 11 named actions. Each is *)
(* a state-transition predicate over (vars, vars'). The Next-state         *)
(* relation is the disjunction at the bottom of this section.              *)
(***************************************************************************)

(***************************************************************************)
(* ClientRequest                                                            *)
(*                                                                          *)
(*   kessel-vsr counterpart: Msg::Request / Replica::on_request             *)
(*                                                                          *)
(* Precondition: we have not exhausted MaxRequests; let p be the primary   *)
(* in view[p]; p is Status::Normal.                                         *)
(*                                                                          *)
(* State change: append a fresh entry (with opnum = Len(log[p]) + 1) to    *)
(* p's log; emit one Prepare(view, opnum, entry, commit) message per       *)
(* backup; bump `requested`.                                                *)
(*                                                                          *)
(* Abstraction note: this action assumes the client always reaches the    *)
(* CURRENT primary; the real kessel-vsr lets backups relay Request to the *)
(* primary (Replica::on_request, lines 476–485). Modeling the relay is    *)
(* extra state-space cost for no safety leverage.                          *)
(***************************************************************************)
\* All replicas at view 0 agree on the primary at Init; once views diverge,
\* ClientRequest fires only on the replica that currently believes itself
\* primary in its OWN view (mirrors `Replica::is_primary()` in the real code).
ClientRequest ==
    /\ requested < MaxRequests
    /\ \E pr \in Replicas :
        /\ pr = PrimaryOf(view[pr])
        /\ status[pr] = "Normal"
        /\ LET opnum == Len(log[pr]) + 1
               entry == [opnum  |-> opnum,
                         client |-> requested + 1,
                         req    |-> 1]
           IN  /\ log' = [log EXCEPT ![pr] = Append(@, entry)]
               /\ msgs' = msgs \cup
                      { [kind   |-> "Prepare",
                         view   |-> view[pr],
                         opnum  |-> opnum,
                         entry  |-> entry,
                         commit |-> commit[pr],
                         from   |-> pr,
                         to     |-> bk] :
                        bk \in Replicas \ {pr} }
               /\ requested' = requested + 1
               /\ UNCHANGED << commit, view, normalView, status,
                               applied, dropped, viewChanges >>

(***************************************************************************)
(* Prepare (a backup receives a Prepare from the primary)                  *)
(*                                                                          *)
(*   kessel-vsr counterpart: Msg::Prepare / Replica::on_prepare             *)
(*                                                                          *)
(* Precondition: the backup is at the message's view (the spec ignores    *)
(* the higher-view branch that triggers state-transfer — that's S1.5);    *)
(* the backup is Status::Normal; opnum is exactly the next slot (the      *)
(* spec ignores the "ahead-of-log → solicit GetState" branch — also S1.5);*)
(* the recipient is not the primary itself.                                *)
(*                                                                          *)
(* State change: append entry to the backup's log; emit one PrepareOk to  *)
(* the primary; consume the Prepare message.                                *)
(***************************************************************************)
Prepare(m) ==
    /\ m.kind  = "Prepare"
    /\ m.view  = view[m.to]
    /\ status[m.to] = "Normal"
    /\ m.to # PrimaryOf(m.view)
    /\ m.opnum = Len(log[m.to]) + 1
    /\ log' = [log EXCEPT ![m.to] = Append(@, m.entry)]
    /\ msgs' = (msgs \ {m}) \cup
            { [kind  |-> "PrepareOk",
               view  |-> m.view,
               opnum |-> m.opnum,
               from  |-> m.to,
               to    |-> PrimaryOf(m.view)] }
    /\ UNCHANGED << commit, view, normalView, status, applied,
                    dropped, viewChanges, requested >>

(***************************************************************************)
(* PrepareOk (the primary records a backup's acknowledgement)              *)
(*                                                                          *)
(*   kessel-vsr counterpart: Msg::PrepareOk / Replica::on_prepare_ok        *)
(*                                                                          *)
(* In the real code, the primary's `prepare_ok` map accumulates senders   *)
(* and the side-effect of crossing the quorum threshold is the Commit/    *)
(* Apply chain. In TLA+, the "accumulator" is the set of in-flight        *)
(* PrepareOk messages; the side-effect lives in the Commit action (which  *)
(* queries the message set). So PrepareOk has NO local side-effect at the *)
(* primary beyond consuming the message (and is in fact a no-op for the   *)
(* spec — modeled here only for narrative completeness; the Commit action *)
(* checks PrepareOkSenders to decide quorum).                              *)
(*                                                                          *)
(* For TLC efficiency, PrepareOk does NOT delete the message — Commit     *)
(* needs to count quorum, so the PrepareOk messages persist until         *)
(* dropped. This is safe: the spec does not depend on PrepareOk delivery  *)
(* order, only on the quorum predicate.                                    *)
(***************************************************************************)
PrepareOk(m) ==
    /\ m.kind = "PrepareOk"
    /\ m.to   = PrimaryOf(m.view)
    /\ status[m.to] = "Normal"
    /\ m.view = view[m.to]
    \* No-op consumption — the message stays in `msgs` until Commit fires or
    \* DropMessage drops it. This keeps the spec's quorum check stateless
    \* against PrepareOk-delivery ordering.
    /\ UNCHANGED vars

(***************************************************************************)
(* Commit (the primary detects quorum and advances its commit point,      *)
(*         then broadcasts Commit so backups can catch up)                *)
(*                                                                          *)
(*   kessel-vsr counterpart: Msg::Commit / Replica::on_commit_msg          *)
(*                          + Replica::apply_through                       *)
(*                                                                          *)
(* Precondition: replica p is the current primary in its view, Status::   *)
(* Normal, and there exists an opnum > commit[p] for which PrepareOk      *)
(* quorum has been reached (the primary counts itself).                    *)
(*                                                                          *)
(* State change: commit[p] advances to the next opnum (one-step           *)
(* per-action — TLC sees the "contiguous catch-up" of kessel-vsr's        *)
(* `on_prepare_ok` loop as a sequence of single-step Commit actions; this *)
(* is sound because each step is independently a legal commit advance);   *)
(* p broadcasts a Commit message to every backup.                          *)
(*                                                                          *)
(* Inductive monotonicity (MonotonicCommitPoint): the new commit is       *)
(* exactly old + 1, so commit[p]' > commit[p] by construction.            *)
(***************************************************************************)
Commit ==
    \E p \in Replicas :
        /\ p = PrimaryOf(view[p])
        /\ status[p] = "Normal"
        /\ \E n \in 1..Len(log[p]) :
            /\ n = commit[p] + 1
            \* Quorum INCLUDES the primary's implicit self-vote, so we
            \* need at least Quorum-1 distinct PrepareOk senders other
            \* than p, OR Quorum if the senders set may include p (it
            \* does not — PrepareOk is sent by backups only).
            /\ Cardinality(PrepareOkSenders(view[p], n, p)) >= Quorum - 1
            /\ commit' = [commit EXCEPT ![p] = n]
            /\ msgs' = msgs \cup
                   { [kind   |-> "Commit",
                      view   |-> view[p],
                      commit |-> n,
                      from   |-> p,
                      to     |-> bk] :
                     bk \in Replicas \ {p} }
            /\ UNCHANGED << log, view, normalView, status, applied,
                            dropped, viewChanges, requested >>

(***************************************************************************)
(* Apply (a replica applies the next committed log entry to its state    *)
(*        machine, appending the opnum to `applied`)                       *)
(*                                                                          *)
(*   kessel-vsr counterpart: Replica::apply_through (body)                  *)
(*                                                                          *)
(* Precondition: the replica has a not-yet-applied committed entry —      *)
(* Len(applied[r]) < commit[r] AND commit[r] <= Len(log[r]).               *)
(*                                                                          *)
(* State change: append the (Len(applied[r]) + 1)-th log entry's opnum    *)
(* to applied[r]. Strictly local; emits no messages. Crucial for          *)
(* ExactlyOnceApply.                                                       *)
(***************************************************************************)
Apply ==
    \E r \in Replicas :
        /\ Len(applied[r]) < commit[r]
        /\ commit[r] <= Len(log[r])
        /\ LET nextIdx == Len(applied[r]) + 1
           IN  applied' = [applied EXCEPT ![r] =
                            Append(@, log[r][nextIdx].opnum)]
        /\ UNCHANGED << log, commit, view, normalView, status, msgs,
                        dropped, viewChanges, requested >>

(***************************************************************************)
(* HandleCommit (a backup receives a Commit message and advances its      *)
(*               commit pointer to match what the primary has committed)  *)
(*                                                                          *)
(*   kessel-vsr counterpart: (part of Msg::Commit / Replica::on_commit_msg) *)
(*                                                                          *)
(* Precondition: the backup is at the message's view, Status::Normal, and *)
(* the message's commit value is <= Len(log[backup]) (so the backup      *)
(* actually has those entries — the higher-than-log case in the real     *)
(* code triggers GetState, which is S1.5).                                 *)
(*                                                                          *)
(* State change: commit[backup] advances to m.commit (monotonic by guard:  *)
(* we require m.commit > commit[backup]).                                  *)
(***************************************************************************)
HandleCommit(m) ==
    /\ m.kind = "Commit"
    /\ m.view = view[m.to]
    /\ status[m.to] = "Normal"
    /\ m.to # PrimaryOf(m.view)
    /\ m.commit > commit[m.to]
    /\ m.commit <= Len(log[m.to])
    /\ commit' = [commit EXCEPT ![m.to] = m.commit]
    /\ msgs' = msgs \ {m}
    /\ UNCHANGED << log, view, normalView, status, applied,
                    dropped, viewChanges, requested >>

(***************************************************************************)
(* TimeoutPrimary (a backup detects primary loss and starts a view change)*)
(*                                                                          *)
(*   kessel-vsr counterpart: Replica::tick (idle-tick branch)               *)
(*                          -> Replica::start_view_change                   *)
(*                                                                          *)
(* Precondition: viewChanges < MaxViewChanges (bound for TLC); replica r  *)
(* is NOT the current primary; r is Status::Normal (so we don't re-issue  *)
(* a StartViewChange while already in one).                                *)
(*                                                                          *)
(* State change: r advances its view to view[r] + 1 (the real code uses   *)
(* max(view, max_view_seen) + 1, which can skip several views at once;    *)
(* the spec's one-step-at-a-time bound is conservative — it explores a    *)
(* strict subset of what the implementation can do, and the bound matches *)
(* the MaxViewChanges accounting). r transitions to Status::ViewChange and *)
(* broadcasts a StartViewChange to all other replicas.                     *)
(***************************************************************************)
TimeoutPrimary ==
    \E r \in Replicas :
        /\ viewChanges < MaxViewChanges
        /\ r # PrimaryOf(view[r])
        /\ status[r] = "Normal"
        /\ view' = [view EXCEPT ![r] = view[r] + 1]
        /\ status' = [status EXCEPT ![r] = "ViewChange"]
        /\ msgs' = msgs \cup
              { [kind |-> "StartViewChange",
                 view |-> view[r] + 1,
                 from |-> r,
                 to   |-> other] :
                other \in Replicas \ {r} }
        /\ viewChanges' = viewChanges + 1
        /\ UNCHANGED << log, commit, normalView, applied,
                        dropped, requested >>

(***************************************************************************)
(* StartViewChange (a replica receives a StartViewChange and votes in)    *)
(*                                                                          *)
(*   kessel-vsr counterpart: Msg::StartViewChange / Replica::on_svc        *)
(*                                                                          *)
(* Precondition: the message's view >= the recipient's view; the recipient*)
(* may be in either status.                                                *)
(*                                                                          *)
(* State change: if the message's view is HIGHER than the recipient's,    *)
(* the recipient adopts the higher view and transitions to ViewChange. In *)
(* either case the recipient broadcasts its own StartViewChange for the   *)
(* new view to gather votes (this is `on_svc`'s implicit behavior — by   *)
(* recording the sender as a vote AND adding itself, the recipient        *)
(* signals participation).                                                 *)
(*                                                                          *)
(* In the spec, the message is consumed and a fresh broadcast is emitted  *)
(* from this replica unless it has already sent one (a self-StartViewChange*)
(* would already exist in msgs). This abstraction makes the quorum check *)
(* in DoViewChange below correspond to "Quorum distinct senders observed".*)
(***************************************************************************)
StartViewChange(m) ==
    /\ m.kind = "StartViewChange"
    /\ m.view >= view[m.to]
    \* Real-VSR tightening (T3-TLC-found #3, SP109): a replica that has
    \* ALREADY completed view-change for some view >= m.view (status =
    \* "Normal" /\ normalView[m.to] >= m.view) does not regress to
    \* ViewChange when a stale StartViewChange for that view trickles in
    \* — kessel-vsr's on_svc ignores StartViewChange whose view does not
    \* exceed the replica's current normal view. Without this guard, a
    \* stale StartViewChange could push the newly-elected primary (or any
    \* backup that has already adopted the new view via StartView) back
    \* to ViewChange status, which then re-enables a duplicate
    \* BecomePrimary that overwrites a freshly-committed log entry while
    \* leaving `applied` stranded (TypeOK violation, baseline trace
    \* 2026-05-23 states 10 -> 13).
    /\ ~ (status[m.to] = "Normal" /\ normalView[m.to] >= m.view)
    /\ LET adopted == Max(view[m.to], m.view)
       IN  /\ view' = [view EXCEPT ![m.to] = adopted]
           /\ status' = [status EXCEPT ![m.to] = "ViewChange"]
           \* Consume the incoming vote; rebroadcast THIS replica's vote
           \* for `adopted` if it has not already done so. (Idempotence:
           \* msgs is a set, so duplicate broadcasts are absorbed.)
           /\ msgs' = (msgs \ {m}) \cup
                   { [kind |-> "StartViewChange",
                      view |-> adopted,
                      from |-> m.to,
                      to   |-> other] :
                     other \in Replicas \ {m.to} }
    /\ UNCHANGED << log, commit, normalView, applied,
                    dropped, viewChanges, requested >>

(***************************************************************************)
(* DoViewChange (once a replica has seen a quorum of StartViewChange      *)
(*               votes for the same view, it sends its log+commit to the *)
(*               primary-elect)                                            *)
(*                                                                          *)
(*   kessel-vsr counterpart: Msg::DoViewChange / Replica::maybe_finish_svc *)
(*                                                                          *)
(* Precondition: r is in Status::ViewChange in some view v; r has         *)
(* observed >= Quorum distinct senders (including itself) of              *)
(* StartViewChange(v) addressed to it; r has not already sent a            *)
(* DoViewChange for v.                                                    *)
(*                                                                          *)
(* State change: r emits a DoViewChange to PrimaryOf(v) carrying its full *)
(* log, its commit, and its normalView (the "last view r was Normal in").  *)
(* This is the critical message: BecomePrimary will pick the most         *)
(* up-to-date log among the quorum of DoViewChange senders via the       *)
(* (normalView, len log) lexicographic key.                                *)
(***************************************************************************)
DoViewChange ==
    \E r \in Replicas, v \in 1..(MaxViewChanges + 1) :
        /\ status[r] = "ViewChange"
        /\ view[r]   = v
        /\ Cardinality(SvcVotes(v, r) \cup {r}) >= Quorum
        \* Guard: we have not already sent a DoViewChange(v) from r.
        /\ ~ \E m \in msgs :
              /\ m.kind = "DoViewChange"
              /\ m.view = v
              /\ m.from = r
        /\ msgs' = msgs \cup
              { [kind       |-> "DoViewChange",
                 view       |-> v,
                 log        |-> log[r],
                 commit     |-> commit[r],
                 normalView |-> normalView[r],
                 from       |-> r,
                 to         |-> PrimaryOf(v)] }
        /\ UNCHANGED << log, commit, view, normalView, status, applied,
                        dropped, viewChanges, requested >>

(***************************************************************************)
(* BecomePrimary (the new primary, on collecting a quorum of              *)
(*                DoViewChange, picks the most up-to-date log, adopts it, *)
(*                and broadcasts StartView)                               *)
(*                                                                          *)
(*   kessel-vsr counterpart: Replica::maybe_finish_view_change             *)
(*                                                                          *)
(* Precondition: p == PrimaryOf(v); p has received >= Quorum               *)
(* DoViewChange(v) messages; p has not already become primary for v       *)
(* (status[p] # "Normal" OR normalView[p] # v).                            *)
(*                                                                          *)
(* State change: p picks the log with the lexicographically-largest       *)
(* (normalView, log-length) among the received DoViewChange messages —   *)
(* this is the SP37-fixed rule that prevents a stale-log replica from     *)
(* dropping committed entries. p adopts that log, takes the max commit   *)
(* across the quorum, sets status to Normal and normalView to v, then    *)
(* broadcasts a StartView(v, log, commit) to all other replicas.          *)
(*                                                                          *)
(* Honest note: this action OVERWRITES log[p] with the chosen log. Any   *)
(* entries that were on p's prior log but not on the chosen log are lost  *)
(* — but the (normalView, length) pick rule guarantees those entries     *)
(* were uncommitted at p (otherwise some other replica in the quorum     *)
(* would have had at least p's prior committed prefix). This is the      *)
(* exact reasoning the SP37 bug fix codified.                             *)
(***************************************************************************)
BecomePrimary ==
    \E v \in 1..(MaxViewChanges + 1) :
        LET p == PrimaryOf(v)
        IN  /\ Cardinality(DvcSenders(v, p)) >= Quorum
            /\ ~ (status[p] = "Normal" /\ normalView[p] = v)
            /\ LET dvcs == DvcAt(v, p)
                   \* Lexicographic max on (normalView, log length).
                   bestMsg == CHOOSE m \in dvcs :
                                  \A m2 \in dvcs :
                                      \/ m.normalView > m2.normalView
                                      \/ /\ m.normalView = m2.normalView
                                         /\ Len(m.log) >= Len(m2.log)
                   maxCommit == CHOOSE c \in { mm.commit : mm \in dvcs } :
                                  \A mm \in dvcs : c >= mm.commit
               IN  /\ log' = [log EXCEPT ![p] = bestMsg.log]
                   /\ commit' = [commit EXCEPT ![p] = maxCommit]
                   /\ view' = [view EXCEPT ![p] = v]
                   /\ normalView' = [normalView EXCEPT ![p] = v]
                   /\ status' = [status EXCEPT ![p] = "Normal"]
                   /\ msgs' = msgs \cup
                          { [kind   |-> "StartView",
                             view   |-> v,
                             log    |-> bestMsg.log,
                             commit |-> maxCommit,
                             from   |-> p,
                             to     |-> other] :
                            other \in Replicas \ {p} }
                   /\ UNCHANGED << applied, dropped, viewChanges, requested >>

(***************************************************************************)
(* StartView (a non-primary replica receives a StartView, adopts the new *)
(*            log + commit, returns to Status::Normal)                    *)
(*                                                                          *)
(*   kessel-vsr counterpart: Msg::StartView / Replica::on_start_view       *)
(*                                                                          *)
(* Precondition: m.view >= view[r]; r is not the primary for m.view (the  *)
(* primary has already done BecomePrimary).                                *)
(*                                                                          *)
(* State change: r adopts the new view, normalView, log, commit; status   *)
(* returns to Normal. The PrepareOk-relay that the real on_start_view     *)
(* emits is modeled as the normal Prepare / PrepareOk cycle re-engaging   *)
(* in subsequent steps — the spec does not need it for the safety        *)
(* invariants.                                                              *)
(***************************************************************************)
StartView(m) ==
    /\ m.kind = "StartView"
    /\ m.view >= view[m.to]
    /\ m.to # PrimaryOf(m.view)
    /\ m.commit <= Len(m.log)
    \* Real-VSR tightening (T3-TLC-found #3, SP109): a replica that has
    \* already completed view-change for some view >= m.view does not
    \* re-overwrite its log + commit from a stale StartView. kessel-vsr's
    \* on_start_view returns early when the incoming view does not
    \* advance the local normal-view. Without this guard, a stale
    \* StartView could overwrite a backup's locally-extended log (post-
    \* Prepare) with a shorter stale log, breaking TypeOK's commit <=
    \* Len(log) and ExactlyOnceApply's applied[i] = log[i].opnum
    \* alignment.
    /\ ~ (status[m.to] = "Normal" /\ normalView[m.to] >= m.view)
    /\ log' = [log EXCEPT ![m.to] = m.log]
    /\ commit' = [commit EXCEPT ![m.to] = Max(commit[m.to], m.commit)]
    /\ view' = [view EXCEPT ![m.to] = m.view]
    /\ normalView' = [normalView EXCEPT ![m.to] = m.view]
    /\ status' = [status EXCEPT ![m.to] = "Normal"]
    /\ msgs' = msgs \ {m}
    /\ UNCHANGED << applied, dropped, viewChanges, requested >>

(***************************************************************************)
(* DropMessage (the network drops a message)                              *)
(*                                                                          *)
(*   kessel-vsr counterpart: (none — modeled abstractly; corresponds to   *)
(*   the kessel-sim drop_pct injector)                                    *)
(*                                                                          *)
(* Precondition: dropped < MaxDrops; some message is in flight.            *)
(*                                                                          *)
(* State change: remove one arbitrary message from msgs; bump `dropped`.  *)
(*                                                                          *)
(* This is the network-fault-injection model. The action is what gives   *)
(* TLC the freedom to explore "what if THIS message never arrives" —     *)
(* including the worst case for view-change correctness.                   *)
(***************************************************************************)
DropMessage ==
    /\ dropped < MaxDrops
    /\ \E m \in msgs :
        /\ msgs' = msgs \ {m}
        /\ dropped' = dropped + 1
        /\ UNCHANGED << log, commit, view, normalView, status, applied,
                        viewChanges, requested >>

----------------------------------------------------------------------------
(***************************************************************************)
(* Next-state relation.                                                     *)
(*                                                                          *)
(* The disjunction of every action. Each message-driven action quantifies *)
(* over the in-flight message set; the rest quantify over replicas or are *)
(* nullary.                                                                 *)
(***************************************************************************)

Next ==
    \/ ClientRequest
    \/ \E m \in msgs : Prepare(m)
    \/ \E m \in msgs : PrepareOk(m)
    \/ Commit
    \/ Apply
    \/ \E m \in msgs : HandleCommit(m)
    \/ TimeoutPrimary
    \/ \E m \in msgs : StartViewChange(m)
    \/ DoViewChange
    \/ BecomePrimary
    \/ \E m \in msgs : StartView(m)
    \/ DropMessage

----------------------------------------------------------------------------
(***************************************************************************)
(* Specification.                                                           *)
(*                                                                          *)
(* This slice is SAFETY-ONLY (per Decision 4 of the design spec).          *)
(* Fairness / liveness is deferred to S1.2. We therefore only assert the  *)
(* always-Next-step formula `[][Next]_vars`; no WF_vars / SF_vars.         *)
(***************************************************************************)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(***************************************************************************)
(* SAFETY INVARIANTS.                                                       *)
(*                                                                          *)
(* These are the five invariants from Decision 4 of the SP109 design       *)
(* spec. TLC checks each at every reachable state. A counterexample to any *)
(* of them is either a real protocol bug or a spec bug; the design spec's *)
(* Honest-Disclosure section describes how to triage.                      *)
(***************************************************************************)

(***************************************************************************)
(* LogPrefixSafety — THE HEADLINE CONTRACT.                                *)
(*                                                                          *)
(* For any two replicas r1, r2: if r1's commit point is no greater than   *)
(* r2's, then the first commit[r1] entries of r1's log equal the first    *)
(* commit[r1] entries of r2's log. Equivalently: there is a single global *)
(* committed log, and every replica's committed portion is a prefix of it.*)
(*                                                                          *)
(* This is the SP37 bug-class invariant: "a higher-view replica with a    *)
(* stale log winning DoViewChange and dropping committed ops" violates    *)
(* exactly this property. Catching this class of bug at N=3 is the slice's*)
(* primary safety claim.                                                   *)
(*                                                                          *)
(* Thesis pillar strengthened: verifiable (the headline pillar).           *)
(***************************************************************************)
LogPrefixSafety ==
    \A r1, r2 \in Replicas :
        commit[r1] <= commit[r2]
            => \A i \in 1..commit[r1] :
                  /\ i <= Len(log[r1])
                  /\ i <= Len(log[r2])
                  /\ log[r1][i] = log[r2][i]

(***************************************************************************)
(* NoDivergence.                                                            *)
(*                                                                          *)
(* Replicas at the same commit-point have byte-identical committed log    *)
(* prefixes. Stated as a per-position equality for all positions up to    *)
(* Min(commit[r1], commit[r2]). Follows from LogPrefixSafety + reflexivity *)
(* (apply LogPrefixSafety with the roles of r1, r2 swapped); we state it  *)
(* separately because TLC's per-invariant counterexample reporting makes  *)
(* it useful to have the SAME-LENGTH violation distinguished from the    *)
(* PREFIX violation if one arises.                                         *)
(*                                                                          *)
(* Thesis pillar strengthened: verifiable.                                  *)
(***************************************************************************)
NoDivergence ==
    \A r1, r2 \in Replicas :
        \A i \in 1..Min(commit[r1], commit[r2]) :
            /\ i <= Len(log[r1])
            /\ i <= Len(log[r2])
            /\ log[r1][i] = log[r2][i]

(***************************************************************************)
(* ExactlyOnceApply.                                                        *)
(*                                                                          *)
(* For every replica r:                                                    *)
(*   (1) applied[r] has no duplicate opnums (no double-apply);             *)
(*   (2) Len(applied[r]) = commit[r] is NOT asserted as an inductive      *)
(*       invariant because the spec models Apply as a SEPARATE action     *)
(*       from Commit — between a Commit step and the corresponding Apply *)
(*       step, Len(applied[r]) is strictly less than commit[r]. The       *)
(*       weaker invariant we DO assert is Len(applied[r]) <= commit[r]    *)
(*       (no over-apply), AND that the applied sequence matches the log   *)
(*       prefix's opnums position-by-position;                             *)
(*   (3) applied[r][i] = log[r][i].opnum for i in 1..Len(applied[r]).      *)
(*                                                                          *)
(* Together (1) + (3) + Len(applied[r]) <= commit[r] (from TypeOK)         *)
(* encode "each committed entry is applied AT MOST ONCE and in order."    *)
(* Eventual completeness (applied catches up to commit) is a Liveness     *)
(* property and is S1.2.                                                   *)
(*                                                                          *)
(* Thesis pillar strengthened: verifiable.                                  *)
(***************************************************************************)
ExactlyOnceApply ==
    /\ \A r \in Replicas :
          \A i, j \in 1..Len(applied[r]) :
              (i # j) => (applied[r][i] # applied[r][j])
    /\ \A r \in Replicas :
          Len(applied[r]) <= commit[r]
    /\ \A r \in Replicas :
          \A i \in 1..Len(applied[r]) :
              /\ i <= Len(log[r])
              /\ applied[r][i] = log[r][i].opnum

(***************************************************************************)
(* MonotonicCommitPoint — a TRANSITION (action) property.                 *)
(*                                                                          *)
(* The commit point at each replica is non-decreasing across every       *)
(* transition. Formally:                                                  *)
(*                                                                          *)
(*   [][\A r \in Replicas : commit'[r] >= commit[r]]_vars                  *)
(*                                                                          *)
(* This is stated as an action invariant (a TLA+ step property) rather    *)
(* than a state invariant because monotonicity is a relation BETWEEN      *)
(* states, not a property OF a single state. TLC accepts it via the      *)
(* PROPERTY clause in the .cfg (T3) OR can be checked by encoding it as  *)
(* an inductive invariant via the action shapes themselves (every action *)
(* either leaves commit unchanged OR advances it). The .cfg in T3 will   *)
(* declare it under PROPERTY.                                              *)
(*                                                                          *)
(* For TLC's INVARIANT clause, the inductive flavor is captured by the   *)
(* per-action structure: every action's `commit'` formula is either       *)
(*   - UNCHANGED commit (most actions), or                                *)
(*   - [commit EXCEPT ![r] = old + 1]                  (Commit)            *)
(*   - [commit EXCEPT ![r] = m.commit] with guard m.commit > old          *)
(*                                                       (HandleCommit)   *)
(*   - [commit EXCEPT ![p] = maxCommit] with maxCommit derived from a    *)
(*       quorum of DoViewChange whose commit values are >= old (by       *)
(*       LogPrefixSafety induction)               (BecomePrimary)        *)
(*   - [commit EXCEPT ![r] = Max(old, m.commit)]      (StartView)         *)
(* All four advance OR preserve commit; the BecomePrimary case is the    *)
(* subtle one — its monotonicity follows from LogPrefixSafety holding   *)
(* over the predecessor states. So MonotonicCommitPoint and              *)
(* LogPrefixSafety are mutually reinforcing.                              *)
(***************************************************************************)
MonotonicCommitPoint ==
    [][\A r \in Replicas : commit'[r] >= commit[r]]_vars

============================================================================
