# S1 — TLA+/model-checked safety specs for the replication log: Design

**Date:** 2026-05-19
**Status:** Approved (autonomous mandate — see Process Note)
**Subproject:** 109 (first built sub-slice of the strategic tier; S1 in the
THESIS.md S1–S4 backlog)
**Builds on:** THESIS.md (the verifiable-behavior pillar); SP12/SP13
(VSR partition + view-change hardening); SP37 VSR view-change safety
hardening (the real-safety-bug-fixed lineage that motivates mechanical
verification).

## Process Note (autonomy + thesis sequencing)

Produced under the standing overnight autonomous-build mandate
(`feedback_kesseldb_autonomous_build` + the strategic-tier mandate
`feedback_kesseldb_strategic_tier`): "build the backlog autonomously,
BOLD choices, don't wait for approval, keep the two-stage review gate
+ full tests + pentest passes." The brainstorming user-review gate is
satisfied by this documented decision record. All other rigor retained:
two-stage subagent review per task (spec then artifact-quality), a
final whole-implementation review, and the existing Rust gates
(`cargo test --workspace --release`, seed-7) which remain unchanged
because this slice ships no Rust code change.

**Strategic-tier sequencing.** THESIS.md (commit `457e1ce`) names the
S1–S4 strategic backlog. S1 is "TLA+/model-checked safety specs for
the replication log" and is the immediate next slice after THESIS.md.
This document is that S1's design record.

## Problem

The thesis names **verifiable behavior** as one of KesselDB's three
core properties. Today the verification story is:

- Rust unit and integration tests (484 passing post-SP108).
- The seeded VSR partition simulator (`kessel-sim`), with the
  historically difficult seed 7 as a hard gate.
- The SP37 review that found and fixed a real safety bug
  ("a higher-view replica with a stale log could win DoViewChange and
  drop committed ops") — the cleanest evidence that the VSR
  implementation is non-trivially subtle.

What is missing is the **mechanical, model-checked proof** that the
abstract VSR protocol upholds the safety invariants the implementation
is built to provide. Specifically, the foundational invariant — that
**all committed state is a function of an agreed log prefix** — is
defended today only by tests and by inspection. A formal model
checker can exhaust the state space of small finite configurations
(N=3 replicas, bounded messages, bounded view changes) and either
confirm the safety invariant holds OR produce a concrete
counterexample trace.

TigerBeetle ships a TLA+ specification of its replication protocol
for this same reason. AWS uses TLA+ for DynamoDB, S3, and other
services. The pattern is well-established and the tooling is mature
(TLA+ + TLC, Leslie Lamport's reference implementation).

This slice ships a TLA+ specification of the VSR replication protocol
abstracted from `kessel-vsr`, plus a TLC model configuration that
checks the 5 safety invariants below, plus the README and helper
scripts to reproduce the TLC run, plus a captured baseline run output
as evidence.

## Decisions (bold choices, documented)

### Decision 1 — Tool: **TLA+ + TLC model checker**

Pre-resolved by the controller per the autonomous-mandate
"pick-the-obvious-and-proceed" rule. Lamport's conventional
combination; mature; widely used by TigerBeetle and AWS for exactly
this kind of work. Not Apalache (which is symbolic and would change
the workflow), not Ivy (which is a different paradigm). Start with
TLC and the standard `.tla` + `.cfg` file form.

**Rationale.** TLC is the de-facto industry standard for this kind
of safety check. Its state-space-enumeration model is well-understood
and its counterexample traces are concrete (a sequence of named
actions over named replicas), which is what the slice needs. The
counterexample-replay seam (Decision 5) becomes useful only because
TLC traces are concrete.

**Thesis fit: verifiable.** Replaces "we tested it" with "the TLC
state-space exhaustion at N=3 found no counterexample to the five
invariants under bounded message loss + bounded view changes."

### Decision 2 — Scope of the TLA+ model: **full normal-mode + minimal view-change**

Three options weighed; the strongest one-of-a-kind path is taken.

- **(A) Normal-mode replication ONLY (no view-change).** The simplest
  model; the safety invariant becomes nearly trivial because no
  primary failure ever occurs. Rejected: the SP37 bug was a
  view-change-induced safety violation; checking the safety invariant
  in the absence of view-change is exactly what tests already do and
  proves nothing new.

- **(B) Full VSR protocol including normal mode + view change + state
  transfer + client retransmission + crash-stop restart.** The most
  thorough model; would map 1:1 to the `kessel-vsr` implementation.
  Rejected for this first slice: the state-space explodes (TLC takes
  days, not hours), the spec balloons to 500+ lines, the result is
  unreadable, and TLC's bounded check loses leverage because the
  parameters are too coarse to be informative.

- **(C, taken) Full normal-mode replication + minimal view-change.**
  Models Request → Prepare → PrepareOk → Commit → Apply over a
  quorum of replicas with bounded message loss and reordering, AND a
  minimal view-change abstraction (a backup detects primary timeout,
  issues a StartViewChange, the new primary collects a quorum of
  DoViewChange messages with their logs, picks the log with the
  highest (normal_view, log_length), broadcasts a StartView, all
  replicas adopt that log). State transfer (GetState/NewState) is
  out of scope (the model assumes view-change recovers everything
  it needs from DoViewChange messages); crash-stop restart is out
  of scope (no recovery from disk in the model). Client
  retransmission deferred (idempotency lives in the client-table
  layer, which is a state-machine-level concern; the safety
  invariants this slice checks are below the client-table).

  **Rationale (why bold over safe).** This is the regime where the
  safety invariant is non-trivial. SP37 proved that the dangerous
  bugs live at the boundary between normal-mode replication and
  view-change. Modeling only normal-mode would shadow-prove the
  safety invariant; modeling the FULL VSR protocol would never
  complete. The "minimal view-change" formulation is the
  Goldilocks point: it captures the bug class that SP37 fixed (and
  the bug classes that might still exist) while keeping TLC's
  state-space exploration tractable.

  **Out-of-scope, deferred (S1.X follow-ups):**
  - State transfer (GetState/NewState messages).
  - Crash-stop restart from disk.
  - Client retransmission and the client-table dedup.
  - Bounded delays (Liveness; this slice is safety-only).
  - The catalog and SQL layers (above the log; not what this slice
    proves).

### Decision 3 — Replica count + message-loss model: **N=3, bounded queue, drop+reorder, ≤2 view changes, ≤3 client requests**

TLC explores finite state spaces. Standard models: 3 replicas (the
minimal interesting quorum), bounded message queue, bounded client
requests, optional message reordering/loss/duplication.

- **Replicas:** N=3 (quorum=2). The minimal interesting size.
  Increasing to N=5 (quorum=3) is a follow-up; the bugs that exist
  at N=3 generally exist at N=5 in this protocol, and N=5 explodes
  the state-space ~10×.

- **Message queue:** at most one in-flight message per
  (sender,receiver) pair at a time (so the global queue is bounded
  by ~9 in-flight at N=3). Messages are delivered in an order
  chosen non-deterministically (so reordering is modeled).

- **Message loss:** modeled by allowing the spec to choose
  "Drop(m)" as an alternative to "Deliver(m)" for any in-flight
  message. Bounded so TLC terminates: at most MaxDrops drops total
  (default 3).

- **View changes:** at most MaxViewChanges (default 2), so the
  protocol may transition through a sequence of up to 3 views
  (0 → 1 → 2). Two view changes is enough to exhibit the SP37 bug
  class (which is about the second view-change inheriting a stale
  log).

- **Client requests:** at most MaxRequests (default 3) operations
  injected by clients. Three requests is enough to exhibit
  out-of-order commit, partial replication, and prefix divergence.

**Tractability target:** TLC runs to completion in **< 4 hours on
a single core** on a development workstation (the rigor-checkpoint
cadence). If TLC runs longer than 8 hours at these bounds, the
bounds are reduced (e.g., MaxRequests → 2 or MaxDrops → 2) and the
reduction is documented in the result file. If TLC finds a
counterexample, that IS the slice's first defect — it is documented
and resolved (either the model is refined or the protocol bug is
confirmed and tracked).

**Why these bounds, not larger.** Larger bounds (N=5, MaxRequests=10,
MaxDrops=10) explode the state space exponentially. The TigerBeetle
TLA+ spec uses similar bounds for similar reasons. The bounded
exploration does NOT prove the protocol correct for arbitrary
configurations; it proves there is no counterexample within the
bounded configuration. This is the well-understood limitation of
bounded model checking and is disclosed in the Honest Disclosure
section.

### Decision 4 — Invariants to check: **5 invariants (TypeOK + 4 safety)**

- **TypeOK** — the standard TLA+ type invariant. Every variable
  matches its declared type; every record has its declared fields;
  the protocol is well-formed at every reachable state.

- **LogPrefixSafety** — for any two replicas r1, r2 with committed
  log lengths c1 ≤ c2, the first c1 entries of r2's committed log
  equal the first c1 entries of r1's committed log. Equivalently:
  there is a single global committed log; every replica's committed
  log is a prefix of it. **This is the slice's headline contract.**

- **NoDivergence** — any two replicas with the same committed log
  length have byte-identical committed logs. (Determinism over
  consensus.) This is a consequence of LogPrefixSafety + the
  state-machine determinism property (which the kessel-sm crate
  enforces at the implementation level), but stating it as a
  separate invariant catches a useful subset of bugs explicitly.

- **ExactlyOnceApply** — a committed log entry is applied to the
  state machine **exactly once per replica**. Formally: for every
  replica r and every operation op, the number of times op appears
  in r's `applied` sequence equals 1 if op is in r's committed log
  and 0 otherwise. (No duplicate-apply, no missing-apply.)

- **MonotonicCommitPoint** — the commit point at each replica is
  non-decreasing across all transitions. (Safety-adjacent: committed
  entries never "un-commit.") This is the per-replica analogue of
  LogPrefixSafety.

**Out of scope (deferred):**

- **Liveness** (eventual progress / "every client request eventually
  commits"). Liveness requires TLA+ temporal formulas (`WF`/`SF`
  fairness) and is a natural S1.5 follow-up. The current slice is
  safety-only.

- **Linearizability**. A linearizability check requires modeling
  client request/response pairs and ordering them; the present
  model has Request injection but no client-side history. A
  linearizability invariant can be added as an S1.X follow-up
  (it would require ~30 lines of extra spec).

### Decision 5 — Spec-to-Rust correspondence: **middle (named-action correspondence)**

Three options weighed:

- **(a) Loose abstraction** (capture the protocol's essence, not its
  API). Rejected: no traceability, future readers struggle to map
  TLA+ ↔ Rust.

- **(b) Tight per-function correspondence** (every TLA+ action maps
  to a Rust function name; refinement-style). Rejected: would force
  the TLA+ spec to re-encode Rust-API artifacts (e.g., the
  `Out::msgs`/`Out::replies` separation, the `Replica::handle`
  dispatch). Adds noise without adding safety leverage.

- **(c, taken) Middle: named-action correspondence.** TLA+ action
  names match Rust event names: `Request`, `Prepare`, `PrepareOk`,
  `Commit`, `StartViewChange`, `DoViewChange`, `StartView`, `Apply`.
  A short table in the spec maps each TLA+ action to the `Replica`
  method (or `kessel-vsr::Msg` variant) that implements it. A
  future reader can trace TLA+ ↔ Rust without needing mechanized
  refinement.

**The mapping table:**

| TLA+ action | kessel-vsr counterpart | Notes |
|---|---|---|
| `ClientRequest(c, op)` | `Msg::Request` → `Replica::on_request` | Client → primary |
| `Prepare(p, e)` | `Msg::Prepare` → `Replica::on_prepare` | Primary → backups |
| `PrepareOk(r, e)` | `Msg::PrepareOk` → `Replica::on_prepare_ok` | Backup → primary |
| `Commit(e)` | `Msg::Commit` → `Replica::on_commit_msg` + `apply_through` | Primary advances commit point + broadcasts |
| `Apply(r, e)` | `Replica::apply_through` body | State-machine `apply()` per entry |
| `TimeoutPrimary(r)` | `Replica::tick` (idle-tick branch) → `start_view_change` | Backup detects primary loss |
| `StartViewChange(r, v)` | `Msg::StartViewChange` → `Replica::on_svc` | Vote for view change |
| `DoViewChange(r, v)` | `Msg::DoViewChange` → `Replica::maybe_finish_svc` | Carries log + commit to new primary |
| `BecomePrimary(r, v)` | `Replica::maybe_finish_view_change` | Picks best log; sets normal_view |
| `StartView(v)` | `Msg::StartView` → `Replica::on_start_view` | New primary broadcasts the chosen log |
| `DropMessage(m)` | (modeled in spec; corresponds to the sim's `drop_pct` injector) | Message loss |

**Honest note on the mapping.** This is **named correspondence, not
mechanized refinement**. The TLA+ spec does not generate the Rust
code; the Rust code does not check against the TLA+ spec. A
discrepancy between them is a human-discovered issue, not a
mechanically-flagged one. Closing that gap (mechanized refinement)
is a deep follow-up (months of work; the literature has
TLA+-to-code workflows but none are turnkey for Rust). For now,
the named-correspondence table + the honest disclosure is the
discipline.

### Decision 6 — Counterexample-replay seam: **document for this slice, harness as S1.5 follow-up**

When TLC finds a counterexample, it emits a trace: a sequence of
named actions with their parameter bindings, producing a sequence
of states. Two options:

- **(a) Auto-replay harness in Rust.** A Rust harness that reads the
  TLC counterexample trace and drives `kessel-vsr` through the
  exactly-equivalent sequence of messages and ticks, comparing
  the model state to the implementation state at each step. Rejected
  for this first slice: the harness is itself a major piece of work
  (~500 lines of Rust + a TLC-trace parser). The trace format is
  TLA+-specific (a `_TLCError` or `_TLCSuccess` JSON / textual
  output); a robust parser is a one-week task.

- **(b, taken) Manual translation + documentation.** TLC
  counterexamples are inspected manually; the spec's "Mapping"
  section + the README explain how to translate a TLC trace step
  into a sequence of `Replica::handle`/`Replica::tick` calls.
  Documented as the workflow for THIS slice.

**S1.5 follow-up (documented but not in scope):** auto-replay
harness in Rust that consumes the TLC `_TLCError`/`_TLCSuccess`
text/JSON output and drives `kessel-vsr` through the same sequence.

**Honest disclosure.** Without a replay harness, the discipline is
weaker than it could be: a counterexample in the model is only as
useful as the human reader's diligence in translating it to Rust.
The mitigation is the action-name correspondence table (Decision 5),
which makes the translation mechanical.

### Decision 7 — CI integration: **rigor-checkpoint, not per-commit gate**

TLC runs are slow (minutes-to-hours depending on bounds). Running
TLC on every Rust commit would block normal CI. Two options:

- **(a) Full CI gate on every commit.** Every PR / push runs TLC.
  Rejected: too slow; TLC takes hours; would block routine refactors.

- **(b, taken) Rigor-checkpoint.** TLC is run as a deliberate
  checkpoint (manually, or as a scheduled CI job): before any merge
  that changes `kessel-vsr`, OR weekly, whichever comes first. The
  latest pass/fail is recorded in `kesseldb-tla/results/`. Normal
  CI does NOT block on TLC. A failing TLC run blocks the next
  `kessel-vsr` change.

**Cadence rule (recorded in the spec + README):**

- TLC MUST pass before any `kessel-vsr` change is merged. (Implemented
  as a discipline rule, not as a hook — the next agent / contributor
  reads the rule from the README before touching `kessel-vsr`.)

- The baseline TLC pass is recorded in
  `kesseldb-tla/results/2026-05-19-baseline.txt` (or similar dated
  file) as the slice's evidence.

- Re-runs are required at least quarterly, and any re-run failure
  is itself a defect investigation.

**S1.X follow-up (documented but not in scope):** scheduled CI
integration via GitHub Actions (a weekly job that runs TLC + uploads
the result file). Pending S1 spec stabilization.

### Decision 8 — Slice numbering: **SP109**

This is the slice immediately after SP108 (Parquet INT96/DECIMAL).
Numbered SP109 by extension of the existing pattern. The internal
record path is
`docs/superpowers/specs/2026-05-19-kesseldb-subproject109-tla-replication-safety.md`
(produced by T6 of the implementation plan; mirrors the SP108 record
convention exactly).

This is also **S1 in the THESIS.md strategic-tier backlog**. The
record cross-references both numbering schemes.

### Decision 9 — Test artifacts: **8-item bundle**

The slice ships:

1. The `.tla` spec file: `kesseldb-tla/Replication.tla` (the protocol
   model).
2. The `.cfg` model file: `kesseldb-tla/Replication.cfg` (bounds +
   invariants).
3. A README: `kesseldb-tla/README.md` (how to install TLC, how to
   run it, how to interpret output, where evidence lives).
4. A helper script: `kesseldb-tla/verify.ps1` AND
   `kesseldb-tla/verify.sh` (Windows + POSIX) that runs TLC with
   the standard bounds.
5. A captured baseline run: `kesseldb-tla/results/2026-05-19-baseline.txt`
   (full TLC stdout + the summary line proving 0 errors found).
6. The SP109 record:
   `docs/superpowers/specs/2026-05-19-kesseldb-subproject109-tla-replication-safety.md`.
7. STATUS.md row addition (SP109 row after SP108, numeric order).
8. The per-slice thesis-fit note in the spec naming `verifiable` as
   the strengthened pillar.

**Out-of-scope artifacts (deferred to S1.X):**

- Auto-replay Rust harness.
- Refinement proof linking TLA+ ↔ Rust.
- GitHub Actions scheduled CI job.
- Liveness specification.
- Linearizability check.

## Honest Disclosure: model-vs-implementation gap

**This slice does NOT prove `kessel-vsr` is bug-free.** It proves
that the ABSTRACT MODEL of the protocol, with the specified bounds
(N=3, MaxDrops=3, MaxViewChanges=2, MaxRequests=3), upholds the
five named invariants.

The gap between the model and the implementation is the well-known
**abstraction-vs-implementation gap** in formal methods:

- **The model is bounded.** Bigger configurations (N=5, more requests,
  more drops, more view changes) are not checked. Bugs that only
  manifest at scale (e.g., 100 replicas, 1 million requests) are
  outside the model's reach.

- **The model is abstract.** It does NOT model state transfer
  (GetState/NewState), crash-stop restart, the client table, the
  catalog, or SQL. Bugs in those layers are outside the model's
  reach.

- **The model is not mechanically refined to the implementation.**
  A bug that exists in the Rust code but NOT in the TLA+ model
  (e.g., an arithmetic overflow in `apply_through`) is not caught
  by TLC. The TLC pass is a necessary, not sufficient, condition.

- **The model is bounded by the spec author's correctness.** A bug
  in the TLA+ spec itself (e.g., the spec omitting a possible
  message reordering) can hide an implementation bug. Defense: the
  spec is reviewed by the two-stage subagent gate; the
  named-action correspondence table makes the spec's structure
  inspectable; the slice's "Verification" section in the record
  lists every modeling choice the spec made.

**What the slice DOES achieve (claim with bounded scope):**

- It rules out a class of safety bugs (the four listed invariants)
  in the abstract VSR protocol at the bounded configuration. That
  is a non-trivial property — SP37 fixed exactly this class of bug
  in the implementation; TLC would have caught a model with that
  bug.

- It establishes the artifact that future S1.X follow-ups (Liveness,
  state-transfer, mechanized refinement) extend.

- It provides a mechanized check that a future developer modifying
  `kessel-vsr` can run to detect a protocol-level regression.

- It produces a permanent record (the result file in
  `kesseldb-tla/results/`) that an outsider can verify by re-running
  TLC themselves.

**This honest disclosure is the slice's primary discipline.** A
record that overclaims (e.g., "kessel-vsr is now formally verified")
would be a thesis violation under the honest-engineering pillar.

## Architecture: `kesseldb-tla/` directory layout

`kesseldb-tla/` is **independent of the Cargo workspace**. It contains
no Rust code. `cargo build` and `cargo test --workspace --release`
ignore it (TLA+ files have no extension Cargo cares about). The
directory ships as part of the repository so the artifact is shareable.

```
kesseldb-tla/
├── README.md                            # how to install TLC, run, interpret
├── Replication.tla                      # the protocol model (the meat)
├── Replication.cfg                      # TLC bounds + invariants + init/next
├── verify.ps1                           # Windows helper: runs TLC
├── verify.sh                            # POSIX helper: runs TLC
└── results/
    └── 2026-05-19-baseline.txt          # captured TLC run output (evidence)
```

### File contents (high-level)

- **`Replication.tla`** declares the constants (`N`, `Replicas`,
  `MaxDrops`, `MaxViewChanges`, `MaxRequests`), the variables
  (`log[r]`, `commit[r]`, `view[r]`, `normalView[r]`, `status[r]`,
  `applied[r]`, `messages` (a bag), `dropCount`, `viewChangeCount`,
  `requestCount`), the `Init` predicate, the `Next` state-transition
  relation (a disjunction of every action — `Request`, `Prepare`,
  `PrepareOk`, `Commit`, `Apply`, `TimeoutPrimary`, `StartViewChange`,
  `DoViewChange`, `BecomePrimary`, `StartView`, `DropMessage`),
  and the invariants (`TypeOK`, `LogPrefixSafety`, `NoDivergence`,
  `ExactlyOnceApply`, `MonotonicCommitPoint`).

- **`Replication.cfg`** sets the constants to their bounded values
  (`N <- 3`, `MaxDrops <- 3`, `MaxViewChanges <- 2`,
  `MaxRequests <- 3`), declares `Init` and `Next` as the
  initial-state and next-state predicates, and lists the five
  invariants in `INVARIANT`.

- **`verify.ps1`** / **`verify.sh`** discover TLC's jar location
  (either via a `TLC_JAR` environment variable or by checking a
  conventional install path), invoke
  `java -XX:+UseParallelGC -cp $TLC_JAR tlc2.TLC Replication`,
  and stream stdout to both the console and
  `results/<timestamp>-baseline.txt`. Exits 0 on TLC success
  (zero errors); exits nonzero on counterexample (so a CI hook
  could chain on it). Both scripts emit a final summary line
  "TLC: 0 errors found, N states explored, M distinct states,
  runtime: H:MM:SS".

- **`README.md`** documents (i) the slice's purpose ("S1 of the
  THESIS.md strategic backlog"), (ii) the install instructions for
  TLC (download the jar from the lamport/tlaplus release, set
  `TLC_JAR`), (iii) the run instructions (`./verify.sh` or
  `.\verify.ps1`), (iv) the rigor-checkpoint cadence rule
  ("TLC MUST pass before any kessel-vsr change is merged"),
  (v) the counterexample translation workflow (per Decision 6),
  (vi) the honest disclosure (model-vs-implementation gap),
  (vii) the deferred S1.X follow-ups list.

### TLA+ spec structure (Replication.tla outline)

```
---- MODULE Replication ----
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Replicas, MaxDrops, MaxViewChanges, MaxRequests

VARIABLES
    log,               \* log[r] : Seq of entries
    commit,            \* commit[r] : Nat
    view,              \* view[r] : Nat
    normalView,        \* normalView[r] : Nat
    status,            \* status[r] : {"Normal", "ViewChange"}
    applied,           \* applied[r] : Seq of op_numbers
    messages,          \* set of in-flight messages
    dropCount,         \* Nat
    viewChangeCount,   \* Nat
    requestCount       \* Nat

\* Helpers
Quorum == (Cardinality(Replicas) \div 2) + 1
PrimaryOf(v) == \* (v % N)-th replica
Entry(opNum, client, req) == [ op |-> opNum, client |-> client, req |-> req ]

\* Type invariant
TypeOK == ...

\* Safety invariants
LogPrefixSafety ==
    \A r1, r2 \in Replicas :
        commit[r1] <= commit[r2]
        => SubSeq(log[r2], 1, commit[r1]) = SubSeq(log[r1], 1, commit[r1])

NoDivergence ==
    \A r1, r2 \in Replicas :
        commit[r1] = commit[r2]
        => SubSeq(log[r1], 1, commit[r1]) = SubSeq(log[r2], 1, commit[r2])

ExactlyOnceApply ==
    \A r \in Replicas :
        \A i \in 1..Len(applied[r]) :
            \A j \in 1..Len(applied[r]) :
                i # j => applied[r][i] # applied[r][j]
    /\ \A r \in Replicas :
        \A i \in 1..commit[r] :
            \E j \in 1..Len(applied[r]) : applied[r][j] = log[r][i].op

MonotonicCommitPoint == \* expressed via a TLA+ history variable in the next step
    ... (uses a prev_commit shadow var or a TLA+ temporal-history pattern)

\* Initial state
Init ==
    /\ log = [r \in Replicas |-> << >>]
    /\ commit = [r \in Replicas |-> 0]
    /\ view = [r \in Replicas |-> 0]
    /\ normalView = [r \in Replicas |-> 0]
    /\ status = [r \in Replicas |-> "Normal"]
    /\ applied = [r \in Replicas |-> << >>]
    /\ messages = {}
    /\ dropCount = 0
    /\ viewChangeCount = 0
    /\ requestCount = 0

\* Actions (each is a state-transition disjunct)
ClientRequest(c, op) == ...
Prepare(p, e) == ...
PrepareOk(r, e) == ...
Commit(e) == ...
Apply(r, e) == ...
TimeoutPrimary(r) == ...
StartViewChange(r, v) == ...
DoViewChange(r, v) == ...
BecomePrimary(r, v) == ...
StartView(v) == ...
DropMessage(m) == ...

\* Next-state relation
Next == \E ... : ClientRequest(...) \/ Prepare(...) \/ ... \/ DropMessage(...)

\* Spec
Spec == Init /\ [][Next]_<<log, commit, view, normalView, status, applied, messages, dropCount, viewChangeCount, requestCount>>

====
```

### TLC config (Replication.cfg)

```
CONSTANT
    Replicas = {r1, r2, r3}
    MaxDrops = 3
    MaxViewChanges = 2
    MaxRequests = 3

INIT Init
NEXT Next

INVARIANT
    TypeOK
    LogPrefixSafety
    NoDivergence
    ExactlyOnceApply
    MonotonicCommitPoint
```

(Exact form to be finalized in T3; this outline matches Lamport's
"Specifying Systems" conventions and the parquet.thrift discipline
of the SP108 spec — every form is grounded in an external
authority.)

## Workflow

**To run TLC for the first time:**

1. Install Java 11+ (TLC requires it).
2. Download the TLC jar from
   `https://github.com/tlaplus/tlaplus/releases/latest`. Place at
   any path; export `TLC_JAR` to that path.
3. From `kesseldb-tla/`, run `./verify.sh` (POSIX) or
   `.\verify.ps1` (Windows). The script streams TLC output to
   stdout and `results/<timestamp>.txt`.
4. On success, TLC prints
   `Model checking completed. No error has been found.` and a
   summary of states explored.

**Expected baseline outcome** (post-T3): zero errors, ~10^5–10^6
states explored, ~minutes-to-hours runtime depending on hardware.
If runtime exceeds 8 hours, lower the bounds and document the
reduction in the result file.

**To interpret a counterexample (if TLC finds one):**

1. TLC's stdout contains the counterexample trace: a sequence of
   states, each tagged with the action that produced it (e.g.,
   `Action: Prepare(r1, entry-3)`).
2. Each action maps to a `kessel-vsr` call per the
   Decision 5 mapping table.
3. The variables (`log`, `commit`, `view`, etc.) map to the
   `Replica` fields of the same name.
4. The implementer reproduces the trace by driving a manual test
   sequence (or eventually, via the S1.5 auto-replay harness).
5. If the trace corresponds to a real protocol bug:
   - Fix the bug in `kessel-vsr`.
   - Re-run TLC; confirm zero errors.
   - Add a Rust regression test that drives the trace.
6. If the trace corresponds to a model bug (a spec that allows
   something the protocol does not):
   - Refine the spec to disallow the over-permissive transition.
   - Re-run TLC; confirm zero errors.
   - Document the refinement in the spec's commentary.

**Rigor-checkpoint cadence:**

- Before any merge that modifies `crates/kessel-vsr/`, re-run TLC.
  If it fails, the merge is blocked until the cause is resolved.
- Quarterly re-runs (at minimum) on the baseline configuration.
- All TLC outputs are captured in `kesseldb-tla/results/` (dated
  files; never overwrite the baseline).

## Determinism / invariants gate (every task)

- `cargo test --workspace --release` `FAILED=0`; seed-7
  (`large_seed_corpus_is_deterministic_and_converges`) green.
- **Honest gate accounting:** baseline = measured post-SP108 484
  (recorded in Task 0). This slice ships NO Rust code change, so
  the gate is **expected net-0** (484 stays 484). The TLA+ work is
  a separate rigor artifact, captured in
  `kesseldb-tla/results/2026-05-19-baseline.txt`.
- Kernel pulls no new external dependency; `kessel-vsr/Cargo.toml`
  unchanged; `kesseldb-tla/` is outside the Cargo workspace and
  has zero impact on `cargo build` / `cargo tree`.
- `#![forbid(unsafe_code)]` invariants unchanged (no Rust files
  modified).
- Existing oracles green: `external_source_oracle` (2),
  `external_source_tls_oracle` (1), `external_source_objstore_oracle`
  (1); all SP100–108 paths byte-unchanged.
- **TLC rigor gate (the new gate this slice introduces):** the
  baseline TLC run completes with zero errors. The run output is
  committed to `kesseldb-tla/results/2026-05-19-baseline.txt` as
  evidence.

## Thesis-fit note

**Thesis fit:** `verifiable` (the headline pillar this slice
strengthens — converts "the VSR protocol is tested" into "the
abstract VSR protocol with bounded N=3, MaxDrops=3, MaxViewChanges=2,
MaxRequests=3 is model-checked against 5 safety invariants and zero
counterexamples are found") + `honest-docs` (the honest disclosure
of the model-vs-implementation gap; the named-correspondence-not-
mechanized-refinement disclosure; the bounded-not-arbitrary
disclosure).

## Honest deferred set (S1.X follow-ups)

- **S1.1 — Auto-replay harness in Rust** (Decision 6).
  Consumes a TLC `_TLCError`/`_TLCSuccess` text/JSON trace and
  drives `kessel-vsr` through the same sequence of
  `Replica::handle` / `Replica::tick` calls, comparing the model
  state to the Rust state at each step. ~500 lines of Rust + a
  TLC-trace parser. Removes the manual-translation discipline gap.

- **S1.2 — Liveness invariants.** The slice is safety-only. A
  follow-up adds temporal formulas (`WF`/`SF` fairness) to express
  "every client request eventually commits under sufficient
  fairness." ~30-50 lines of extra spec + a TLC run with the
  Property: clause.

- **S1.3 — Linearizability check.** Models client request/response
  pairs and the per-client history; states an external observer
  could be sorted into a linear sequence consistent with each
  client's local order. ~50-80 lines of spec.

- **S1.4 — N=5 configuration.** The current bounds are N=3 (the
  minimal interesting quorum). N=5 (the next industry-standard
  size) explodes the state space ~10×. Run as a separate result
  file (overnight or weekend job); document.

- **S1.5 — State-transfer modeling.** GetState/NewState messages
  are out of scope today; they would let the model check the
  recovery-from-lag invariants. ~80-120 lines of spec extension.

- **S1.6 — Crash-stop restart modeling.** Today the model assumes
  no restart from disk. A follow-up adds a `Crash(r)`/`Restart(r)`
  action pair that resets `applied[r]` to the on-disk prefix.

- **S1.7 — Mechanized refinement TLA+ ↔ Rust.** The deep
  follow-up. Bridges the abstraction-vs-implementation gap
  mechanically. Months of work; the literature has approaches
  (e.g., the FoundationDB Flow → TLA+ refinement, the Dafny
  pattern) but none are turnkey for Rust.

- **S1.8 — GitHub Actions scheduled CI job.** Once the spec is
  stable, a weekly job runs TLC and uploads the result file as
  a build artifact. Requires the spec to be stable enough that a
  CI failure means "a regression," not "an evolving spec."

## Task decomposition (one coherent slice)

- **T0** determinism baseline (record measured post-SP108 484).
- **T1** scaffold `kesseldb-tla/` directory (README, verify.ps1,
  verify.sh, results/ dir with `.gitkeep`). No spec content yet —
  just the structural scaffolding.
- **T2** write `Replication.tla` modeling the protocol (3 replicas,
  bounded queues, normal-mode + minimal view-change). TLA+
  expertise required — the controller selects a capable model
  (Opus-class) for this task.
- **T3** write `Replication.cfg` with the bounded constants + the
  five invariants; run TLC to produce the baseline result. Capture
  TLC stdout in `kesseldb-tla/results/2026-05-19-baseline.txt`.
  TLA+ expertise required (Opus-class). **If TLC finds a
  counterexample, that's the slice's first DEFECT** — pause +
  document the trace + decide whether it's a model bug or a real
  protocol bug. If a real protocol bug: this slice's scope grows
  to include the kessel-vsr fix + a Rust regression test, OR the
  bug is filed as an open item and the slice's gate accounting
  honestly reflects "TLC found a counterexample — investigating."
  This is the gate working as designed; honest disclosure trumps
  green-checkmark theater.
- **T4** the TLA+-to-Rust mapping table (in the spec — table form;
  the mapping is already drafted in Decision 5 of this document
  but is finalized as the actual spec lands). Standard agent
  model suffices for this Markdown task.
- **T5** the README + workflow docs in `kesseldb-tla/` (how to
  install TLC, how to run, how to interpret output, where evidence
  lives, the cadence rule). Standard agent model suffices.
- **T6** docs + gate reconciliation + memory (SP109 record + STATUS
  row + per-slice thesis-fit note + memory update). Honest gate
  accounting: `cargo test --workspace --release` unchanged at 484
  (since no Rust changed); the TLC pass is the rigor gate,
  documented in the SP109 record.

**Model selection for subagent-driven-development:** T2 and T3
require TLA+ familiarity (Opus-class). T0/T1/T4/T5/T6 are standard
Rust/Markdown work (any capable model). The controller honors
this division when dispatching subagents.

## Internal record

`docs/superpowers/specs/2026-05-19-kesseldb-subproject109-tla-replication-safety.md`
at docs time, mirroring the SP108 record convention exactly
(KesselDB H1, `**Status:**` line, bare-backtick-path Builds-on,
`---` separators, honest gate reconciliation, the
abstraction-vs-implementation honest disclosure + the S1.1–S1.8
deferred follow-ups + the rigor-checkpoint cadence rule + the
named-correspondence-not-mechanized-refinement disclosure +
strategic-tier cross-reference (S1 in THESIS.md backlog) +
slice-numbering cross-reference (SP109 in subproject numbering)).
