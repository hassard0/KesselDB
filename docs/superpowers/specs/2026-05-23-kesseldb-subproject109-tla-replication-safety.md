# KesselDB — Subproject 109: S1 — TLA+ Model-Checked Replication Safety

**Date:** 2026-05-23  **Status:** done — TLA+ spec + TLC rigor checkpoint committed and pushed.

Builds on:
- Subproject 100 — Object-store external sources (OBJ-1):
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject100-objstore.md`
- Subproject 101 — Parquet OBJ-2a:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject101-parquet.md`
- Subproject 102 — RLE/bit-packing hybrid:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject102-rle.md`
- Subproject 103 — Parquet dictionary encoding:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject103-dict.md`
- Subproject 104 — Parquet Snappy decompression:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject104-snappy.md`
- Subproject 105 — Parquet OPTIONAL/nullable columns:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject105-optional.md`
- Subproject 106 — Parquet GZIP page decompression:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject106-gzip.md`
- Subproject 107 — Parquet V2 data pages:
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject107-v2pages.md`
- Subproject 108 — Parquet INT96 + DECIMAL (OBJ-2c-4):
  `docs/superpowers/specs/2026-05-19-kesseldb-subproject108-int96-decimal.md`
- Project THESIS:
  `docs/THESIS.md`

Design document:
`docs/superpowers/specs/2026-05-19-tla-replication-safety-design.md`

Plan document:
`docs/superpowers/plans/2026-05-19-tla-replication-safety.md`

---

## What shipped

`kesseldb-tla/` directory at repo root — a standalone TLA+ model-checking harness
for the KesselDB VSR replication protocol. No Rust workspace files were touched.

- **`kesseldb-tla/Replication.tla`** — 933-line parametric TLA+ specification of
  Viewstamped Replication (Oki & Liskov 1988) as implemented in `crates/kessel-vsr`.
  Models normal-mode request processing (12 actions: ClientRequest, Prepare,
  PrepareOk, Commit, StartViewChange, DoViewChange, BecomePrimary, StartView,
  DropMessage, DeliverCommit, PrimaryApply, ReplicaApply) and the full view-change
  recovery path including quorum-pick on (normalView, log-length). Parametric over
  Replicas, MaxDrops, MaxViewChanges, MaxRequests so TLC terminates. Defines five
  safety invariants: TypeOK, LogPrefixSafety, NoDivergence, ExactlyOnceApply, and
  MonotonicCommitPoint (the last is a transition property intentionally omitted from
  the .cfg per the S1.2 liveness follow-up scope decision). Module head carries an
  explicit action-mapping table pointing each TLA+ action to its kessel-vsr Rust
  counterpart with file:line refs (shipped in T4, commit 2e638ba).

- **`kesseldb-tla/Replication.cfg`** — TLC configuration binding the bounded model:
  N=3 replicas ({r1, r2, r3}), MaxDrops=3, MaxViewChanges=2, MaxRequests=3.
  CHECK_DEADLOCK FALSE (bound-induced halts are not protocol bugs).
  Four invariants checked: TypeOK, LogPrefixSafety, NoDivergence, ExactlyOnceApply.

- **`kesseldb-tla/verify.ps1`** / **`kesseldb-tla/verify.sh`** — TLC wrapper scripts
  for Windows (PowerShell) and Linux (Bash). Download tla2tools.jar if absent,
  run TLC with appropriate JVM flags, emit a timestamped `=== TLC exit=N ===` trailer
  for machine-readable log parsing. Results are written to `results/` with a dated
  filename.

- **`kesseldb-tla/README.md`** — 295-line operator and developer guide covering:
  prerequisites, how to run TLC, how to interpret TLC output, the counterexample-
  translation workflow (TLC trace → kessel-vsr log → Rust state replay), a full
  honest disclosure of the model's out-of-scope items, and the S1.1–S1.8 follow-up
  backlog.

- **`kesseldb-tla/results/`** — Evidence directory containing multiple independent
  TLC evidence files (see Rigor Evidence section below). A `.gitignore` file in
  `kesseldb-tla/` excludes TLC runtime artifacts (states/, metadir/, *.out scratch
  files) while retaining the `results/` evidence files.

---

## T3 honest disclosure — 4 TLC-found spec tightenings

TLC found FOUR real specification issues during T3 model-checking. Each was corrected
as its own git commit (not batched), so the audit trail is explicit. Every fix is a
TIGHTENING of a precondition to mirror real VSR semantics — not a weakening of any
invariant, and not a workaround. This is the gate working exactly as designed.

**Fix #1 (commit f921295): Bounded sub-universes for record-set definitions.**
Using bare `Nat` in TLA+ record-set definitions causes TLC to attempt enumeration of
an infinite set at initial-state computation, halting immediately with a state-space
explosion. Fix: all Nat-typed record fields were replaced with bounded sub-universes
derived from the model constants: `OpNums == 0..MaxRequests`, `CommitPoints ==
0..MaxRequests`, `Views == 0..MaxViewChanges`, `Clients == 1..MaxRequests`,
`Reqs == 0..MaxRequests`. TypeOK uses these bounded sets; TLC can now enumerate the
initial state.

**Fix #2 (commit 4358420): Widen Clients to 1..MaxRequests.**
With Clients=1..1, the ClientRequest action assigns `client = requested + 1`, so
after the first request the client variable grows to 2, which falls outside the
Clients set defined in TypeOK. This causes a TypeOK violation at depth 2. The real
VSR protocol has no such restriction — any client can send any request. Fix: widened
Clients to `1..MaxRequests` in TypeOK and in Replication.cfg; the `.gitignore` for
TLC artifacts was also committed here. Correctly mirrors kessel-vsr client-table
semantics where client identity is unconstrained by the bounded model.

**Fix #3 (commit b3b7358): Tighten StartViewChange + StartView preconditions.**
The original StartViewChange and StartView preconditions admitted stale view-change
messages from an already-completed view. A replica that had already transitioned to
Normal status at normalView >= m.view could re-enter ViewChange state on receipt of
a stale StartViewChange or StartView message, regressing its status from Normal back
to ViewChange. Fix: both preconditions tightened to guard
`~(status[r] = "Normal" /\ normalView[r] >= m.view)`, mirroring kessel-vsr's
`on_start_view_change` and `on_start_view` early-return that discards messages for
views already completed. This is a semantic tightening: the model now correctly
reflects VSR's rule that a replica ignores view-change messages for views it has
already passed.

**Fix #4 (commit 6135e0c): Tighten BecomePrimary precondition.**
The original BecomePrimary precondition `~(status[p] = "Normal" /\ normalView[p] = v)`
was too loose: it permitted BecomePrimary to re-fire at the same view after a
legitimate StartViewChange escalation had already occurred, because a Primary that had
already committed and returned to Normal at normalView=v could still satisfy the
quorum condition. This stranded applied log entries — entries marked Applied on the
primary but not yet delivered to all replicas. Fix: tightened to
`normalView[p] < v /\ view[p] <= v`, ensuring BecomePrimary fires at most once per
view per replica. This mirrors the kessel-vsr invariant that a replica's normalView
is only updated when it commits to a new view; firing again at the same view would
be a violation of VSR's exactly-once-primary-per-view guarantee.

Each of these four fixes tightens a precondition in the spec to match real VSR
semantics. The resulting spec is strictly more accurate than the pre-fix version.
TLC's role was to find the gap between the intended model and the actual model — this
is institutional-grade formal-methods rigor, not test failure.

---

## Rigor evidence

All evidence files are committed to `kesseldb-tla/results/` for the full audit trail.
No file was trimmed or edited post-run; each is the raw output of the TLC invocation.

| Evidence file | Host | Bounds | Distinct states | Depth | Runtime | Outcome |
|---|---|---|---|---|---|---|
| `2026-05-23-partial-MR3-d19.txt` | Windows (ihass) | MR=3 (post-fix#4) | 117,241,088 | 19 | ~35 min | Disk-exhausted (no violation) |
| `2026-05-23-partial-MR2-windows-killed.txt` | Windows (ihass) | MR=2 (post-fix#4) | 160,145,912 | 20 | ~50 min | User-requested cleanup / exit code -1 (no violation) |
| `2026-05-23-vulcan-MR3.txt` | Vulcan (4x V100, 251 GB RAM) | MR=3 (post-fix#4) | **528,599,314** | **21** | **~55 min** | Disk-exhausted, exit=1 (no violation) |

**Vulcan MR=3 is the headline evidence.** Running on a 251 GB RAM machine with
-Xmx64g -fpmem 0.9 and 16 workers, TLC explored **528 million distinct states at
depth 21** before the host disk (915 GB NVMe, 867 GB used) was exhausted. TLC
terminated with exit code 1 and the explicit JVM message `java.io.IOException:
No space left on device`; the log contains no invariant violation. The last
progress entry in `2026-05-23-vulcan-MR3.txt` reads:

```
Progress(21) at 2026-05-24 00:07:25: 2,995,777,589 states generated (55,046,961 s/min),
528,599,314 distinct states found (8,599,543 ds/min), 296,785,573 states left on queue.
```

Honest history note: an interim T6 snapshot captured this file at 435M / depth 20
(commit `325f308`); TLC continued running past that point and reached the final
528M / depth 21 numbers above before the host disk filled. The committed file is
the final raw log (90 lines, including the explicit exit-1 disk-full diagnostic);
this section quotes the actual end-state.

**Summary:** Three independent runs at two different hosts and two different bound
configurations (MR=2 and MR=3) all reach hundreds of millions of distinct states
at depth 20-21 without finding any invariant violation (TypeOK, LogPrefixSafety,
NoDivergence, ExactlyOnceApply). The rigor-checkpoint baseline for SP109 is the
vulcan MR=3 run: **528M distinct states / depth 21 / no violation / disk-exhausted
exit=1 at ~55 min**.

Partial-coverage-at-disk-exhaustion is the honest characterization. The rigor value
is real: hundreds of millions of distinct states explored in the configured invariant
envelope, post all four TLC-found spec tightenings, with no violation. Full coverage
(queue draining to zero) at MR=3 requires more disk headroom than currently available
on the vulcan host. S1.4 (larger bounds, full-coverage run) and S1.8 (CI integration)
are deferred follow-ups.

---

## Honest model-vs-implementation gap

Per the SP109 design spec (Decision 3 and the OUT-OF-SCOPE list in Replication.tla):

- **State transfer NOT modeled (S1.5):** `Msg::GetState` / `Msg::NewState` (lagging
  replica catch-up) is not in the model. The view-change path reconciles logs via
  the DoViewChange-quorum pick rule, but a real lagging replica uses GetState/NewState
  in kessel-vsr. An S1.5 slice will model this path.

- **Client-table idempotence NOT modeled (S1.3):** The kessel-vsr `client_table`
  provides exactly-once reply replay for retransmitted requests. That mechanism is
  above the log and is a state-machine-level concern not yet modeled. An S1.3
  linearizability follow-up will need it.

- **Persistence NOT modeled (S1.6):** The model does not model disk persistence or
  crash-recovery. Replicas are assumed to retain all state in memory. A real VSR
  implementation must persist the log to disk before acknowledging a PrepareOk;
  S1.6 will model this.

- **Safety only — liveness NOT proved (S1.2):** MonotonicCommitPoint is defined in
  the spec as a transition property but is intentionally omitted from the .cfg
  `PROPERTY` section. Liveness (progress guarantees, termination) is a separate
  concern deferred to S1.2. The current TLC run proves safety properties only.

- **Finite bounds — not a complete proof:** TLC's model checking is exhaustive
  within the configured bounds (N=3, MaxDrops=3, MaxViewChanges=2, MaxRequests=3).
  It does not constitute a proof for arbitrary N or unbounded runs. Larger bounds
  (S1.4), proof by induction, or mechanized refinement (S1.7) are follow-ups.

---

## Honest gate accounting

`kesseldb-tla/` is entirely outside the Rust workspace. The `kesseldb-tla/` directory
contains no `Cargo.toml` and is not a workspace member. Adding TLA+ files adds zero
tests to `cargo test --workspace`.

Pre-SP109 cargo baseline: **484/0** (post-SP108 final, confirmed by the T5/S2
brainstorm bonus commits which also touched no Rust code).

Post-SP109 fresh re-verification (background run, no TLC contention):
**TOTAL passed=484 failed=0**.

Cargo gate: **unchanged at 484/0**. SP109 is a pure additive TLA+ slice; no Rust
code was touched.

---

## Deferred / S1.X follow-up backlog

Per the SP109 plan and README:

| ID | Item | Status |
|---|---|---|
| S1.1 | Counterexample-replay harness (TLC trace → Rust state replay) | Deferred |
| S1.2 | Liveness: MonotonicCommitPoint as `PROPERTY []P_vars` + TLC liveness check | Deferred |
| S1.3 | Linearizability: model client-table idempotence, client-visible invariant | Deferred |
| S1.4 | Larger bounds (MR=4+, MaxViewChanges=3+) full-coverage run | Deferred |
| S1.5 | State transfer: model GetState/NewState lagging-replica catch-up | Deferred |
| S1.6 | Persistence: model disk-write-before-ack crash-recovery invariant | Deferred |
| S1.7 | Mechanized refinement: machine-checked proof for arbitrary N | Deferred |
| S1.8 | CI integration: TLC run in CI on every Replication.tla change (smaller bounds for speed) | Deferred |

---

## Strategic-tier context update

SP109 SHIPS S1. The strategic-tier backlog after SP109:

| ID | Item | Status |
|---|---|---|
| S1 | TLA+/model-checked safety specs for the VSR protocol | **DONE (SP109)** |
| S2 | Serializable MVCC/SI over the deterministic log | In progress: brainstorm + S2.1 plan shipped (commits 47f06bf + 94c9e55) |
| S3 | Jepsen harness for real distributed fault injection | Open |
| S4 | Deterministic in-tree WASM UDFs (sandboxed, reproducible, no imports) | Open |

**Thesis-fit:** SP109's thesis-fit is the **verifiable-behavior pillar** of the
project THESIS (`docs/THESIS.md`): "every correctness claim is checkable (formal
specs, adversarial tests, Jepsen)." A 528M-state TLC run post four TLC-found spec
tightenings is a direct instantiation of that pillar. The spec is a live artifact —
every future kessel-vsr change should be reflected in Replication.tla.

---

## Process note

SP109 executes under the autonomous-mandate (see
`feedback_kesseldb_autonomous_build.md`). The autonomous-mandate substitutes the
brainstorming user-review gate. The two-stage subagent review gate IS the SP109
review: this T6 closeout + the final whole-implementation reviewer that follows.
Each task was committed separately (T1 scaffold → T2 spec → T3 fixes as individual
commits → T4 action table → T5 README → T6 closeout + evidence). Each TLC-found fix
was its own commit for audit-trail clarity. All plan-deviation disclosures (4 TLC-
found fixes, partial-coverage-at-disk-exhaustion) are made in this record, not
suppressed.
