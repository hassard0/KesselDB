# KesselDB TLA+ specifications

Mechanically-checked safety invariants for the kessel-vsr replication protocol,
strengthening the THESIS.md verifiable-behavior pillar.

**Status:** S1 of the THESIS.md strategic-tier backlog (SP109). Date adopted: 2026-05-19.

---

## What's in here

- **`Replication.tla`** — TLA+ specification of the VSR replication protocol abstracted
  from `crates/kessel-vsr`. Models full normal-mode replication (ClientRequest, Prepare,
  PrepareOk, Commit) and a minimal view-change (StartViewChange, DoViewChange, StartView).
  Safety-only; no liveness/temporal formulas in this slice.

- **`Replication.cfg`** — TLC model configuration. Binds the bounded constants
  (N=3, MaxDrops=3, MaxViewChanges=2, MaxRequests=2) and lists the four per-state safety
  invariants TLC verifies in every reachable state: TypeOK, LogPrefixSafety, NoDivergence,
  ExactlyOnceApply. The fifth invariant (MonotonicCommitPoint) is a transition property
  `[][P]_vars` deferred to the S1.2 liveness follow-up per the T3 plan note in the cfg's
  leading comment.

- **`verify.ps1`** — Windows PowerShell 5.1 TLC runner. Accepts `$env:TLA2TOOLS_JAR` or
  `$env:TLC_JAR`; changes into the script directory; invokes TLC with `-workers auto`;
  tees stdout to a timestamped file in `results/`.

- **`verify.sh`** — POSIX TLC runner. Same logic; accepts `$TLA2TOOLS_JAR` or `$TLC_JAR`.

- **`results/`** — captured TLC stdout, one dated file per run. Never overwritten.
  Current evidence: `results/2026-05-23-baseline.txt` (MR=2, completed) and
  `results/2026-05-23-partial-MR3-d19.txt` (MR=3, partial — see Honest Disclosure).

- **`.gitignore`** — excludes TLC scratch directories (`states/`, `*.class`, generated
  TLA+ files) so only the source spec and captured result files reach version control.

---

## How to run TLC

**Prerequisites.**

1. Java 17+ on PATH. Verify: `java -version`.
2. `tla2tools.jar` from `https://github.com/tlaplus/tlaplus/releases`. Place it anywhere;
   point the env var at it:
   - PowerShell: `$env:TLA2TOOLS_JAR = 'C:\path\to\tla2tools.jar'`
   - POSIX bash: `export TLA2TOOLS_JAR=/path/to/tla2tools.jar`
   - Either script also accepts `TLC_JAR` as an alias.

**Run.**

```powershell
# Windows
.\verify.ps1
```

```bash
# POSIX
./verify.sh
```

The script changes into `kesseldb-tla/`, invokes TLC against `Replication.tla` /
`Replication.cfg`, and tees stdout to a timestamped file in `results/`.

**Expected output (success).**

TLC prints progress lines approximately every minute:

```
Progress(N) at HH:MM:SS: X states generated, Y distinct states found, ...
```

The final lines are:

```
Model checking completed. No error has been found.
  The number of states generated: X
  The number of distinct states found: Y
  ...
```

This is the green-gate condition. The exit code is 0.

**Expected output (invariant violation).**

TLC prints:

```
Error: Invariant <NAME> is violated.
```

followed by a counterexample trace. See How to read counterexamples below.

**Resource expectations at the current MR=2 baseline.**

The MR=2 configuration completes in minutes on a modern laptop. The MR=3
configuration (the intended rigor bound) ran 35 min / 117M distinct states /
depth 19 with no invariant violation before exhausting disk headroom on the
build host; see Honest Disclosure.

---

## How to read counterexamples

When TLC finds an invariant violation it prints an error trace: a numbered sequence
of states, each preceded by an action header.

**Step 1 — identify the violated invariant.**

The error line names it:

```
Error: Invariant TypeOK is violated.
```

The four invariants and what they assert:

| Invariant | Asserts |
|-----------|---------|
| `TypeOK` | All variables have the declared types (no malformed state). |
| `LogPrefixSafety` | No two replicas have conflicting entries at the same log index. |
| `NoDivergence` | All replicas with identical commit points have identical log prefixes. |
| `ExactlyOnceApply` | No log entry is applied to the state machine more than once. |

**Step 2 — read the trace headers.**

Each state is preceded by a header of the form:

```
State N: <ActionName line L, col C of module Replication>
```

The action name is one of: `ClientRequest`, `Prepare`, `PrepareOk`, `Commit`,
`StartViewChange`, `DoViewChange`, `StartView`, `DropMessage`. Each action has a
per-block comment in `Replication.tla` naming its `kessel-vsr counterpart` and the
relevant file/line in `crates/kessel-vsr/src/`. The consolidated action-mapping
table near the top of `Replication.tla` lists all mappings in one place.

**Step 3 — read the variable bindings.**

Each state shows the full variable assignment. Focus on:

- `log[r]` — per-replica log contents.
- `commitPoint[r]` — per-replica commit index.
- `viewNumber[r]` — per-replica current view.
- `messages` — the multiset of in-flight messages (bag).
- `dropCount` — cumulative drops consumed.

**Step 4 — map the trace back to kessel-vsr.**

1. Walk the action sequence from State 1 forward. Identify which action produced
   the violating state and what the relevant variable bindings were just before it.
2. For each action, the mapping table in `Replication.tla` names the corresponding
   `Replica::handle` / `Replica::tick` call or message type in kessel-vsr.
3. Any `DropMessage` step corresponds to a message that never reaches its
   destination replica — equivalent to a network drop in the Rust sim harness.

**Step 5 — classify the finding.**

Three cases:

- **(a) TLA+ spec bug.** The action body is more permissive than the real protocol
  (missing precondition, wrong guard). Fix: tighten the spec; re-run TLC.
- **(b) Over-strict invariant.** The assertion is stronger than what VSR actually
  guarantees in this configuration. Fix: weaken the invariant to match the real
  guarantee; document the deviation; re-run TLC.
- **(c) Real protocol issue.** The abstract VSR model as written violates the
  invariant — the protocol has a safety hole at these bounds. This is a major find.
  Fix: determine whether kessel-vsr's Rust code has the same hole; if so, fix
  kessel-vsr (honest commit, regression test), fix the spec, re-run TLC; only land
  the merge once TLC re-passes.

**Step 6 — write the Rust regression test.**

For case (c): write an integration test in `crates/kessel-vsr/tests/` (or extend
`kessel-sim`) that drives the action sequence from the TLC trace — same message
sequence, same drops — and asserts the invariant that TLC found violated. The test
should fail before the fix and pass after. The auto-replay harness that makes this
mechanical is S1.1.

---

## Honest disclosure (model-vs-implementation gap)

This slice does NOT prove `kessel-vsr` is bug-free. It proves that the abstract
VSR model, at the configured bounded constants, upholds the four named per-state
invariants.

**What this spec proves.**

At N=3 / MaxDrops=3 / MaxViewChanges=2 / MaxRequests=2, the abstract VSR protocol
satisfies TypeOK, LogPrefixSafety, NoDivergence, and ExactlyOnceApply across the
full reachable state space at those bounds. TLC exhausted the bounded model with
no violation. The evidence is `results/2026-05-23-baseline.txt`.

**What this spec does NOT prove.**

- **Bounded N.** The spec checks N=3. Bugs that only manifest at N=5 or N=7 are
  outside the model's reach. Promoting to N=5 is S1.4.
- **Bounded requests.** MaxRequests=2 is the completed baseline. MaxRequests=3 is the
  intended rigor bound; at MR=3 TLC explored 117M distinct states to depth 19 with
  no violation before exhausting host disk headroom (6.7 GB free). The partial evidence
  is `results/2026-05-23-partial-MR3-d19.txt`. No spec change is needed to re-run;
  the bound is in `Replication.cfg`. Promoting back to MR=3 on a host with >20 GB
  free is S1.4.
- **Unmodeled actions.** State transfer (GetState / NewState), crash-stop restart,
  client retransmission, the client table, the catalog, and SQL are not modeled.
  Bugs in those layers are outside the model's reach. See S1.5 and S1.6.
- **Not mechanically refined.** A bug in the Rust code that falls outside the
  abstract action set is not caught by TLC. The TLC pass is necessary, not
  sufficient, for kessel-vsr correctness. Mechanized refinement is S1.7.
- **Spec author correctness.** A bug in the TLA+ spec itself can hide an
  implementation bug. Mitigations: the two-stage subagent review gate and the
  named-action correspondence table.
- **Safety only.** This slice does not prove liveness — that every client request
  eventually commits under fairness, or that view-change terminates. Liveness
  invariants with weak fairness are S1.2.
- **MonotonicCommitPoint.** The fifth invariant from the design is a transition
  property (`[][CommitPoint does not decrease]_vars`); it is not in the cfg's
  INVARIANT block. It is deferred to the S1.2 liveness slice.

**What this slice does achieve.**

- Rules out a class of safety bugs (the four checked invariants) in the abstract
  VSR protocol at the current bounded configuration. This is non-trivial: SP37
  fixed exactly this class of bug in the implementation, and TLC at these
  parameters would have caught a model with that bug.
- Establishes the artifact that S1.X follow-ups extend.
- Provides a mechanized regression check for future kessel-vsr changes.
- Produces a permanent, externally-checkable record (the dated result files in
  `results/`).

---

## S1.X follow-ups

Follow-ups deferred from this slice per the SP109 design decisions and plan:

- **S1.1** Auto-replay harness in Rust: parse a TLC error trace and drive kessel-vsr
  through the equivalent action sequence, comparing state at each step.
- **S1.2** Liveness invariants: add temporal formulas + weak fairness to the spec
  (eventual commit, view-change termination). Promote MonotonicCommitPoint to a
  PROPERTY.
- **S1.3** Client-table modeling: exactly-once reply replay (Msg::Reply deduplication).
- **S1.4** Larger bounds: MaxRequests=3 (re-run on host with >20 GB free); N=5
  configuration as a separate result file.
- **S1.5** State-transfer action: model Msg::GetState / Msg::NewState.
- **S1.6** Crash-stop restart action: model a replica crashing and re-joining.
- **S1.7** Mechanized refinement: establish a formal correspondence between the TLA+
  spec and the Rust implementation (TLA+ spec -> Rust correspondence proof).
- **S1.8** CI integration: GitHub Actions scheduled job that runs TLC on every
  kessel-vsr change (or at minimum weekly), blocking the merge on failure.

---

## Rigor-checkpoint cadence (governing rule)

TLC MUST pass before any merge that modifies `crates/kessel-vsr/`. This is a
discipline rule for the next agent or contributor; it is enforced by reading this
README, not by a hook (the bounded model-check takes minutes to hours — per-commit
blocking would be too slow).

The checkpoint procedure:

1. Run `.\verify.ps1` (Windows) or `./verify.sh` (POSIX) from `kesseldb-tla/`.
2. If TLC completes with `Model checking completed. No error has been found.`,
   the merge may proceed. Capture the result file from `results/` in the commit.
3. If TLC finds a counterexample: halt the merge; classify the finding per the
   How to read counterexamples section; commit the fix as its own honest commit
   with a clear message; re-run TLC; only land the merge once TLC re-passes.

Every TLC run's stdout is captured in `results/` as a dated file. Never overwrite
an existing result file. The baseline `results/2026-05-23-baseline.txt` is the
canonical evidence for the MR=2 completed run.

Quarterly re-runs (at minimum) on the baseline configuration are expected even
when kessel-vsr is not actively changing, to confirm the baseline remains
reproducible as the host environment evolves.

---

## References

- **TLA+ home:** `https://lamport.azurewebsites.net/tla/tla.html` — language
  reference, Lamport's *Specifying Systems* (free PDF), video course.
- **TLA+ tools release:** `https://github.com/tlaplus/tlaplus/releases` — download
  `tla2tools.jar` here.
- **TigerBeetle VSR spec:** `https://github.com/tigerbeetle/tigerbeetle/tree/main/src/vsr` —
  the closest prior art for TLA+ on a Rust VSR database; mirrors the same
  bounded-model-check workflow on the same protocol family.
- **Oki & Liskov, "Viewstamped Replication" (1988)** — the VSR origin paper.
- **SP109 design doc:** `docs/superpowers/specs/2026-05-19-tla-replication-safety-design.md`
- **SP109 plan:** `docs/superpowers/plans/2026-05-19-tla-replication-safety.md`
- **KesselDB THESIS.md:** `docs/THESIS.md` — the strategic-tier context (S1–S4
  backlog) that motivates this slice.
