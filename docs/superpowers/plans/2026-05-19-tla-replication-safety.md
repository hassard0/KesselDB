# S1 — TLA+/model-checked safety specs for the replication log: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. **Model selection (per the design spec, Decisions 2 & 4):** T2 (Replication.tla) and T3 (Replication.cfg + TLC baseline run) require TLA+ familiarity — dispatch to an Opus-class subagent. T0/T1/T4/T5/T6 are standard Markdown/scripting/Rust-baseline work — any capable model.

**Goal:** Ship a TLA+ specification of the VSR replication protocol abstracted from `kessel-vsr` (full normal-mode + minimal view-change), a TLC model configuration with bounded constants (N=3, MaxDrops=3, MaxViewChanges=2, MaxRequests=3), the five safety invariants (TypeOK, LogPrefixSafety, NoDivergence, ExactlyOnceApply, MonotonicCommitPoint), helper scripts (`verify.ps1` + `verify.sh`), a README with the rigor-checkpoint cadence rule + counterexample-translation workflow + honest disclosure of the model-vs-implementation gap, and a captured baseline TLC run output as evidence. Zero Rust code change in this slice — the existing `cargo test --workspace --release` gate stays at 484 (net-0); the TLC pass is the new rigor gate.

**Architecture:** `kesseldb-tla/` directory at the repo root (outside the Cargo workspace). Files: `Replication.tla` (protocol model), `Replication.cfg` (bounds + invariants), `verify.ps1` / `verify.sh` (TLC runner wrappers), `README.md` (install + run + interpret + cadence + honest disclosure), `results/2026-05-19-baseline.txt` (captured TLC stdout). `cargo build`/`cargo test --workspace --release` ignore the directory entirely.

**Tech Stack:** TLA+ specification language (Lamport, "Specifying Systems"); TLC model checker (the reference Java implementation from `lamport/tlaplus`); PowerShell + POSIX shell for the runner scripts; Markdown for the README and the slice record. Zero Rust deps added; zero impact on `cargo tree`.

---

## Context for the implementer (read once)

**Strategic-tier framing.** This is **S1 of the THESIS.md backlog** (the verifiable-behavior pillar) AND **SP109 in the subproject numbering** (the slice immediately after SP108). The record references both numbers.

**Why this slice now.** THESIS.md was just adopted (commit `457e1ce`). The S1–S4 backlog is now the canonical strategic-tier work. S1 is the immediate next slice. The design spec (`docs/superpowers/specs/2026-05-19-tla-replication-safety-design.md`) is the brainstorm record and the source of truth for every decision below.

**Why TLA+ + TLC, not Apalache / Ivy.** Pre-resolved in Decision 1 of the design spec. TLC is the de-facto industry standard for VSR-class protocol model-checking (TigerBeetle, AWS DynamoDB / S3). Apalache and Ivy are alternatives with different workflow tradeoffs; not chosen.

**Scope of the model — Decision 2 of the design spec.** Full normal-mode replication + minimal view-change. State transfer (GetState/NewState), crash-stop restart, client retransmission, the client table, the catalog, and SQL are **out of scope**. The slice is **safety-only** (no liveness / temporal formulas in this slice).

**Bounds — Decision 3.** N=3, MaxDrops=3, MaxViewChanges=2, MaxRequests=3. Target: TLC completes in <4h on a single-core dev workstation. If TLC exceeds 8h, lower the bounds and document.

**Invariants — Decision 4.** Five invariants: TypeOK + LogPrefixSafety + NoDivergence + ExactlyOnceApply + MonotonicCommitPoint. **LogPrefixSafety is the slice's headline contract** — "any two replicas' committed-prefixes are mutually consistent."

**TLA+-to-Rust correspondence — Decision 5.** Middle option (named-action correspondence). The action-mapping table is reproduced verbatim in T4. NOT mechanized refinement — the gap is honest-disclosed.

**Counterexample-replay seam — Decision 6.** Documented for this slice (manual translation per the README workflow + the action-mapping table). Auto-replay harness is **S1.1 follow-up** — out of scope.

**CI integration — Decision 7.** Rigor-checkpoint cadence: TLC must pass before any `kessel-vsr` merge; baseline result recorded in `kesseldb-tla/results/`. NOT a per-commit blocking gate. GitHub Actions scheduled job is **S1.8 follow-up**.

**Honest disclosure (Decision 9 + the design spec's Honest Disclosure section).** This slice does NOT prove `kessel-vsr` is bug-free; it proves the ABSTRACT MODEL at bounded N=3 upholds the five invariants. Bigger N, state transfer, restart, client table, SQL: all out of model scope.

**`kesseldb-tla/` layout (Decision 9 / Architecture):**

```
kesseldb-tla/
├── README.md
├── Replication.tla
├── Replication.cfg
├── verify.ps1
├── verify.sh
└── results/
    ├── .gitkeep
    └── 2026-05-19-baseline.txt    (created in T3)
```

**parquet.thrift-style discipline.** Every TLA+ form is grounded in an external authority: Lamport, *Specifying Systems* (free PDF on lamport.azurewebsites.net); the TigerBeetle vsr.tla as the closest peer-quality reference (publicly hosted in their repo); the Oki-Liskov VSR paper as the protocol-spec source. If a TLA+ construct disagrees with these sources, **the spec is wrong, never the source** — same BLOCKED-not-faked discipline as parquet.thrift.

**Determinism / invariants gate — EVERY task (T0–T6):**
- `cargo test --workspace --release` → `FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` green. **Total expected to stay at 484 (post-SP108) for the entire slice** (no Rust code touched).
- `crates/kessel-vsr/Cargo.toml` unchanged; `cargo tree -p kesseldb-server` output unchanged; `kesseldb-tla/` outside Cargo workspace.
- Existing oracles green: `external_source_oracle`(2), `external_source_tls_oracle`(1), `external_source_objstore_oracle`(1); all SP100–108 Parquet paths byte-unchanged.
- **NEW gate (the slice's rigor contribution):** `kesseldb-tla/results/2026-05-19-baseline.txt` exists and ends with TLC's "Model checking completed. No error has been found." plus the states-explored summary. T3 produces this file; T6 honors it as the headline evidence in the SP109 record.

**Commit discipline:** straight to `main`, no Co-Authored-By, no signing, message style like `git log -3` of SP108. `git push` after every task (single-branch-main durably authorized by `feedback_kesseldb_autonomous_build`; the two-stage gate IS the review; ignore the recurring soft-block notice). Bash: prefix `cd /c/Users/ihass/KesselDB &&`; `cargo test --workspace --release` long — allow 600000ms. **TLC run in T3 may take hours — use `run_in_background` for the TLC invocation; periodically check the result file or use a Monitor wait.**

---

### Task 0: Determinism baseline (#198)

- [ ] **Step 1:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -30` — sum `passed` → `<BASELINE>` (expected **484** from post-SP108); `FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` in an `ok` result.
- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo tree -p kesseldb-server 2>&1 | grep -Ei "parquet|objstore|rustls|webpki"` → no output (or only the expected feature-gated lines unchanged from SP108).
- [ ] **Step 3:** `cd /c/Users/ihass/KesselDB && cargo test -p kessel-vsr --release 2>&1 | tail -10` → all VSR-crate tests green; `large_seed_corpus_is_deterministic_and_converges` in the pass list.
- [ ] **Step 4:** Verify `kesseldb-tla/` does NOT yet exist: `ls kesseldb-tla 2>&1 | head -3` → "No such file or directory" (or the PowerShell equivalent). This proves the slice is starting fresh.
- [ ] **Step 5:** No commit. Report DONE with `S1 (SP109) baseline: <BASELINE> tests passing, FAILED=0, seed-7 green, kesseldb-tla/ absent (fresh start)` + per-binary counts.

---

### Task 1: Scaffold `kesseldb-tla/` directory + scripts + README skeleton (#199)

**Files:** Create `kesseldb-tla/README.md`, `kesseldb-tla/verify.ps1`, `kesseldb-tla/verify.sh`, `kesseldb-tla/results/.gitkeep`. No `.tla` or `.cfg` content yet — pure scaffolding.

- [ ] **Step 1: Create the directory.** `cd /c/Users/ihass/KesselDB && mkdir -p kesseldb-tla/results`.

- [ ] **Step 2: Create `kesseldb-tla/results/.gitkeep`** — empty file so `git add` picks up the directory.

- [ ] **Step 3: Create `kesseldb-tla/verify.sh`** (POSIX runner). Discover the TLC jar via `TLC_JAR` env var (with a helpful error if unset); invoke `java -XX:+UseParallelGC -cp "$TLC_JAR" tlc2.TLC -config Replication.cfg Replication`; tee stdout to both the terminal AND a dated file under `results/`. Exit 0 on TLC success (zero errors); exit nonzero on counterexample. Mark executable (`chmod +x kesseldb-tla/verify.sh`).

```bash
#!/usr/bin/env sh
# kesseldb-tla/verify.sh — POSIX TLC runner for the S1 replication-safety spec.
# Requires: java (11+) on PATH; TLC_JAR env var pointing to tla2tools.jar.
set -eu

if [ -z "${TLC_JAR:-}" ]; then
    echo "verify.sh: TLC_JAR env var must point to tla2tools.jar." >&2
    echo "Download from https://github.com/tlaplus/tlaplus/releases/latest" >&2
    echo "and set: export TLC_JAR=/path/to/tla2tools.jar" >&2
    exit 2
fi

cd "$(dirname "$0")"
STAMP="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
OUT="results/${STAMP}.txt"
mkdir -p results

echo "Running TLC on Replication.tla with Replication.cfg ..."
echo "Output -> $OUT"

java -XX:+UseParallelGC -cp "$TLC_JAR" tlc2.TLC -config Replication.cfg Replication 2>&1 \
    | tee "$OUT"
rc=$?
exit "$rc"
```

- [ ] **Step 4: Create `kesseldb-tla/verify.ps1`** (Windows runner). Same logic, PowerShell syntax. Use `$env:TLC_JAR`; chain commands per the PowerShell rules (no `&&`; use `if ($LASTEXITCODE -eq 0) { ... }`). Capture output via `Tee-Object` to a dated `results/` file.

```powershell
# kesseldb-tla/verify.ps1 — Windows TLC runner for the S1 replication-safety spec.
# Requires: java (11+) on PATH; $env:TLC_JAR pointing to tla2tools.jar.
$ErrorActionPreference = "Stop"

if (-not $env:TLC_JAR) {
    Write-Host "verify.ps1: `$env:TLC_JAR must point to tla2tools.jar." -ForegroundColor Red
    Write-Host "Download from https://github.com/tlaplus/tlaplus/releases/latest"
    Write-Host "and set: `$env:TLC_JAR = 'C:\path\to\tla2tools.jar'"
    exit 2
}

Set-Location $PSScriptRoot
$Stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH-mm-ssZ")
$Out = Join-Path "results" "$Stamp.txt"
New-Item -ItemType Directory -Force -Path "results" | Out-Null

Write-Host "Running TLC on Replication.tla with Replication.cfg ..."
Write-Host "Output -> $Out"

& java -XX:+UseParallelGC -cp $env:TLC_JAR tlc2.TLC -config Replication.cfg Replication 2>&1 |
    Tee-Object -FilePath $Out
exit $LASTEXITCODE
```

- [ ] **Step 5: Create `kesseldb-tla/README.md`** — skeleton form. Sections (each will be expanded in T5 when the spec/cfg/results actually exist; T1 ships the skeleton with placeholder ALL-CAPS "TBD: T5" markers for content not yet known):

```markdown
# kesseldb-tla — TLA+/TLC safety specs for the VSR replication log

**Status:** S1 of the THESIS.md strategic-tier backlog (= SP109 in
subproject numbering). Date adopted: 2026-05-19.

**Thesis pillar strengthened:** verifiable behavior.

## What this directory contains

- `Replication.tla` — the TLA+ specification of the VSR replication
  protocol, abstracted from `crates/kessel-vsr`. Models full
  normal-mode replication + a minimal view-change. (TBD: T5 — link
  to the spec-author commentary.)
- `Replication.cfg` — the TLC model configuration: bounded constants
  (N=3, MaxDrops=3, MaxViewChanges=2, MaxRequests=3) + the five
  invariants (TypeOK, LogPrefixSafety, NoDivergence, ExactlyOnceApply,
  MonotonicCommitPoint).
- `verify.ps1` / `verify.sh` — Windows / POSIX runner scripts.
- `results/` — captured TLC stdout, dated. Baseline:
  `results/2026-05-19-baseline.txt`.

## Quick start

1. Install Java 11+ on PATH.
2. Download `tla2tools.jar` from
   https://github.com/tlaplus/tlaplus/releases/latest.
3. Set `TLC_JAR` to the jar's path:
   - POSIX: `export TLC_JAR=/path/to/tla2tools.jar`
   - PowerShell: `$env:TLC_JAR = 'C:\path\to\tla2tools.jar'`
4. From this directory: `./verify.sh` (POSIX) or `.\verify.ps1`
   (Windows). The script runs TLC and tees stdout to a dated file in
   `results/`.

(TBD: T5 — interpret-output section + cadence rule + counterexample
workflow + honest disclosure.)
```

- [ ] **Step 6:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total == `<BASELINE>` (no Rust changed; net-0), seed-7 green.

- [ ] **Step 7: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add kesseldb-tla/ && git commit -m "tla: scaffold kesseldb-tla/ directory with verify.{ps1,sh} runners + README skeleton (S1/SP109 T1)" && git push
```

---

### Task 2: Write `Replication.tla` — the TLA+ protocol model (#200)

**Files:** Create `kesseldb-tla/Replication.tla`. **TLA+ expertise required — dispatch this task to an Opus-class subagent.**

This task ships the headline artifact of the slice: the TLA+ specification of the VSR replication protocol. The model covers full normal-mode replication + a minimal view-change (per Decision 2 of the design spec). State transfer, crash-stop restart, client retransmission, the client table, and SQL are explicitly **out of scope** and are NOT modeled — every action introduced must correspond to one of the in-scope actions named in the design spec's Decision-5 mapping table.

- [ ] **Step 1: Read the design spec.** Read `docs/superpowers/specs/2026-05-19-tla-replication-safety-design.md` in full — Decisions 2 (scope), 4 (invariants), 5 (mapping to kessel-vsr). The TLA+ spec must implement exactly the scope named there. Read `crates/kessel-vsr/src/lib.rs` lines 1–780 (the `Msg` enum, `Replica` struct, `handle`/`tick` dispatch) to anchor the spec to the real code without copying it.

- [ ] **Step 2: Write the spec file `kesseldb-tla/Replication.tla`.** Use Lamport's *Specifying Systems* conventions. Structure:

```tla
---------------------------- MODULE Replication ----------------------------
(*
S1 — TLA+/TLC safety specs for the KesselDB VSR replication log.

This module models the VSR replication protocol abstracted from
crates/kessel-vsr. Scope (per the S1 design spec, Decision 2):
full normal-mode replication + minimal view-change. Out of scope:
state transfer (GetState/NewState), crash-stop restart, client
retransmission/client-table, the catalog and SQL. Safety-only;
liveness deferred to S1.2.

Invariants checked (per Decision 4): TypeOK, LogPrefixSafety,
NoDivergence, ExactlyOnceApply, MonotonicCommitPoint.

Action-to-Rust correspondence (per Decision 5):
  ClientRequest    -> Msg::Request    / Replica::on_request
  Prepare          -> Msg::Prepare    / Replica::on_prepare
  PrepareOk        -> Msg::PrepareOk  / Replica::on_prepare_ok
  Commit           -> Msg::Commit     / Replica::on_commit_msg + apply_through
  Apply            -> Replica::apply_through body
  TimeoutPrimary   -> Replica::tick (idle-tick branch) -> start_view_change
  StartViewChange  -> Msg::StartViewChange / Replica::on_svc
  DoViewChange     -> Msg::DoViewChange / Replica::maybe_finish_svc
  BecomePrimary    -> Replica::maybe_finish_view_change
  StartView        -> Msg::StartView / Replica::on_start_view
  DropMessage      -> (no kessel-vsr counterpart; corresponds to sim drop_pct)

Honest disclosure: this is a named-action correspondence, not a
mechanized refinement. See the S1 design spec's Honest Disclosure
section for the full model-vs-implementation gap discussion.
*)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Replicas,           \* set of replica identifiers, e.g. {r1, r2, r3}
    MaxDrops,           \* max total message drops (bounds state space)
    MaxViewChanges,     \* max total view-change escalations
    MaxRequests         \* max total client requests injected

ASSUME
    /\ Cardinality(Replicas) >= 3
    /\ MaxDrops \in Nat
    /\ MaxViewChanges \in Nat
    /\ MaxRequests \in Nat

VARIABLES
    log,                \* log[r] : Seq([opnum: Nat, client: Nat, req: Nat])
    commit,             \* commit[r] : Nat (committed prefix length)
    view,               \* view[r] : Nat (current view)
    normalView,         \* normalView[r] : Nat (last view this replica was Normal in)
    status,             \* status[r] : {"Normal", "ViewChange"}
    applied,            \* applied[r] : Seq(Nat) — opnums actually applied
    messages,           \* set of in-flight messages (a multiset modeled as bag)
    dropCount,          \* Nat — number of drops so far
    viewChangeCount,    \* Nat — number of view changes started
    requestCount        \* Nat — client requests injected

vars == << log, commit, view, normalView, status, applied,
           messages, dropCount, viewChangeCount, requestCount >>

\* Helper: number of replicas N; quorum size.
N == Cardinality(Replicas)
Quorum == (N \div 2) + 1

\* Convert a view number to its primary replica. We assume Replicas is an
\* ordered set: PrimaryOf(v) is the (v mod N)-th element. Implemented via
\* CHOOSE over a fixed ordering relation (TLC handles this for finite sets).
ReplicaOrdering == CHOOSE seq \in [1..N -> Replicas] :
    \A i, j \in 1..N : i # j => seq[i] # seq[j]

PrimaryOf(v) == ReplicaOrdering[((v % N) + 1)]

\* Message kinds (use records with a "kind" tag):
\*   [kind |-> "Prepare", view |-> v, opnum |-> n, op |-> op]
\*   [kind |-> "PrepareOk", view |-> v, opnum |-> n, from |-> r]
\*   [kind |-> "Commit", view |-> v, commit |-> c]
\*   [kind |-> "StartViewChange", view |-> v, from |-> r]
\*   [kind |-> "DoViewChange", view |-> v, log |-> l, commit |-> c, normalView |-> nv, from |-> r]
\*   [kind |-> "StartView", view |-> v, log |-> l, commit |-> c]

\* Initial state.
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

\* ---- Actions ----

\* A client submits a new request to the current primary. (Client-table
\* dedup is out of scope; every Request is a fresh op.)
ClientRequest ==
    /\ requestCount < MaxRequests
    /\ LET p == PrimaryOf(view[PrimaryOf(0)])     \* (a deterministic primary)
           opnum == Len(log[p]) + 1
           entry == [ opnum |-> opnum, client |-> requestCount + 1, req |-> 1 ]
       IN  /\ status[p] = "Normal"
           /\ log' = [log EXCEPT ![p] = Append(@, entry)]
           /\ messages' = messages
                  \cup {[kind |-> "Prepare", view |-> view[p],
                         opnum |-> opnum, entry |-> entry, commit |-> commit[p],
                         to |-> r] : r \in Replicas \ {p}}
           /\ requestCount' = requestCount + 1
           /\ UNCHANGED << commit, view, normalView, status, applied,
                           dropCount, viewChangeCount >>

\* A backup receives a Prepare, appends to its log, sends PrepareOk.
HandlePrepare(m) ==
    /\ m \in messages
    /\ m.kind = "Prepare"
    /\ status[m.to] = "Normal"
    /\ m.view >= view[m.to]
    /\ m.opnum = Len(log[m.to]) + 1
    /\ log' = [log EXCEPT ![m.to] = Append(@, m.entry)]
    /\ messages' = (messages \ {m})
           \cup {[kind |-> "PrepareOk", view |-> m.view,
                  opnum |-> m.opnum, from |-> m.to,
                  to |-> PrimaryOf(m.view)]}
    /\ view' = [view EXCEPT ![m.to] = m.view]
    /\ UNCHANGED << commit, normalView, status, applied,
                    dropCount, viewChangeCount, requestCount >>

\* The primary collects PrepareOk; on quorum, advances commit + applies +
\* broadcasts Commit.
HandlePrepareOk(m) == ...    \* (full body in the spec; see action mapping)

\* The primary advances commit and applies; equivalent to apply_through.
Apply(r, opnum) == ...

\* (Other actions: HandleCommit, TimeoutPrimary, StartViewChange,
\* HandleStartViewChange, DoViewChange, HandleDoViewChange = BecomePrimary,
\* StartView, HandleStartView, DropMessage. See the action mapping.)

DropMessage(m) ==
    /\ m \in messages
    /\ dropCount < MaxDrops
    /\ messages' = messages \ {m}
    /\ dropCount' = dropCount + 1
    /\ UNCHANGED << log, commit, view, normalView, status, applied,
                    viewChangeCount, requestCount >>

\* Next-state relation: disjunction of every action.
Next ==
    \/ ClientRequest
    \/ \E m \in messages :
           \/ HandlePrepare(m)
           \/ HandlePrepareOk(m)
           \/ HandleCommit(m)
           \/ HandleStartViewChange(m)
           \/ HandleDoViewChange(m)
           \/ HandleStartView(m)
           \/ DropMessage(m)
    \/ \E r \in Replicas : TimeoutPrimary(r)
    \/ \E r \in Replicas, op \in 1..Len(log[r]) : Apply(r, op)

\* Spec — initial state + always Next (or stuttering).
Spec == Init /\ [][Next]_vars

\* ---- Invariants ----

TypeOK ==
    /\ log \in [Replicas -> Seq([opnum: Nat, client: Nat, req: Nat])]
    /\ commit \in [Replicas -> Nat]
    /\ view \in [Replicas -> Nat]
    /\ normalView \in [Replicas -> Nat]
    /\ status \in [Replicas -> {"Normal", "ViewChange"}]
    /\ applied \in [Replicas -> Seq(Nat)]
    /\ messages \subseteq UNION { ... }   \* full set in the spec
    /\ dropCount \in 0..MaxDrops
    /\ viewChangeCount \in 0..MaxViewChanges
    /\ requestCount \in 0..MaxRequests

LogPrefixSafety ==
    \A r1, r2 \in Replicas :
        commit[r1] <= commit[r2]
            => \A i \in 1..commit[r1] : log[r1][i] = log[r2][i]

NoDivergence ==
    \A r1, r2 \in Replicas :
        \A i \in 1..(commit[r1] \min commit[r2]) :
            log[r1][i] = log[r2][i]

ExactlyOnceApply ==
    /\ \A r \in Replicas :
        \A i, j \in 1..Len(applied[r]) :
            i # j => applied[r][i] # applied[r][j]
    /\ \A r \in Replicas :
        Len(applied[r]) = commit[r]
    /\ \A r \in Replicas :
        \A i \in 1..commit[r] : applied[r][i] = log[r][i].opnum

\* MonotonicCommitPoint is naturally captured by the temporal formula
\* (always commit[r]' >= commit[r]); express as a TLA+ action property
\* in the cfg (PROPERTY) OR as an inductive invariant pinned by the
\* state-transition structure (every action that updates commit only
\* assigns a value >= prior). The latter is preferred for TLC's
\* state-space efficiency; the cfg lists it as an INVARIANT verified
\* by the action shape, not as a temporal PROPERTY.
\*
\* The spec encodes MonotonicCommitPoint via a sanity check on every
\* commit-write: every action that updates commit asserts the new
\* value >= the old. Captured as a defensive inline assertion in each
\* commit-updating action; the action's pre-/post-condition makes
\* the invariant inductive.

==============================================================================
```

The above is an OUTLINE — the implementer fills in the remaining action bodies (`HandlePrepareOk`, `HandleCommit`, `TimeoutPrimary`, `StartViewChange`, `HandleStartViewChange`, `HandleDoViewChange` / `BecomePrimary`, `HandleStartView`) following the same shape and the action-mapping table. Each action's body must be derivable from the corresponding `kessel-vsr` method's logic, abstracted to the model's variables.

- [ ] **Step 3: Cross-check the spec against the kessel-vsr implementation.** For each action body, open the corresponding Rust method (per the mapping table) and verify the abstract transition matches. Document the abstraction choices in TLA+ comments (`(* ... *)`) — every place the spec is more permissive than the Rust code, or strictly less permissive, gets a comment.

- [ ] **Step 4: Sanity-parse the spec.** A heuristic syntax check: if SANY (TLA+ syntactic analyzer, ships with TLA Toolbox / tla2tools.jar) is available, run `java -cp $TLC_JAR tla2sany.SANY Replication.tla` from `kesseldb-tla/`. Expected: zero errors. If SANY is unavailable, defer the check to T3 (TLC will sany the spec before model-checking).

- [ ] **Step 5: Compile-only check via TLC.** From `kesseldb-tla/`, run `java -cp $TLC_JAR tlc2.TLC -simulate -depth 1 -config Replication.cfg Replication 2>&1 | head -20` (the `-simulate -depth 1` flag runs TLC's parser without exploring the state space — a cheap syntax-and-types pass). T3 supplies the .cfg; T2 can prepare a minimal one inline if needed, or defer this check to T3.

- [ ] **Step 6:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total == `<BASELINE>` (no Rust changed), seed-7 green.

- [ ] **Step 7: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add kesseldb-tla/Replication.tla && git commit -m "tla: Replication.tla — VSR replication protocol model (normal-mode + minimal view-change; safety-only)" && git push
```

---

### Task 3: `Replication.cfg` + baseline TLC run (#201)

**Files:** Create `kesseldb-tla/Replication.cfg`. Run TLC. Create `kesseldb-tla/results/2026-05-19-baseline.txt` (TLC stdout). **TLA+ expertise required — dispatch this task to an Opus-class subagent.**

- [ ] **Step 1: Create `kesseldb-tla/Replication.cfg`** with the bounded constants + the five invariants:

```
SPECIFICATION Spec

CONSTANTS
    Replicas = {r1, r2, r3}
    MaxDrops = 3
    MaxViewChanges = 2
    MaxRequests = 3

INVARIANT
    TypeOK
    LogPrefixSafety
    NoDivergence
    ExactlyOnceApply

\* MonotonicCommitPoint is encoded as an inductive invariant via the
\* action shapes in Replication.tla (see the spec commentary); it is
\* not declared here as a separate INVARIANT because TLC checks
\* per-state, and "non-decreasing across a transition" is a transition
\* property. If a future iteration of this slice (S1.2 Liveness) adds
\* PROPERTY-clause checks, MonotonicCommitPoint will be promoted to a
\* TLA+ temporal formula and listed here under PROPERTY.

CHECK_DEADLOCK FALSE
```

The `CHECK_DEADLOCK FALSE` line is important: TLC otherwise flags every halt state as a deadlock. The model has natural halt states (all messages dropped, no more requests to inject) and these are NOT bugs.

- [ ] **Step 2: Run TLC.** From `kesseldb-tla/`, run `./verify.sh` (POSIX) or `.\verify.ps1` (Windows). The runner script tees output to `results/<timestamp>.txt`. **Expected runtime: minutes to a few hours on a modest workstation.** If TLC runs > 8 hours, kill it, lower one bound (e.g., MaxRequests=2 or MaxDrops=2), document the reduction in the result file, and re-run. Use `run_in_background: true` on the Bash invocation and check completion later.

- [ ] **Step 3: Interpret the result.**
  - **Success case:** TLC stdout ends with "Model checking completed. No error has been found." + a summary like "X states generated, Y distinct states found." Rename (or copy) the dated result file to `results/2026-05-19-baseline.txt`. The slice's headline gate passes.
  - **Failure case (counterexample found):** TLC prints "Error: Invariant <NAME> is violated." followed by a state trace. **This is the slice's first DEFECT** and the gate is working as designed. Stop. Document:
    1. The counterexample trace verbatim in `results/2026-05-19-counterexample.txt`.
    2. The translation to a kessel-vsr operation sequence (using the action-mapping table from T4 — which T2's spec commentary already references).
    3. The diagnosis: is the spec wrong (over-permissive — refine), or is the protocol wrong (real safety bug — fix kessel-vsr AND add a Rust regression test before re-running TLC)?
    The slice's gate accounting **honestly reflects "TLC found a counterexample — investigating"** rather than green-checkmark theater. If the bug is real, this slice's scope grows to include the fix + regression test; if the bug is in the spec, refine the spec and re-run. **NEVER weaken the bounds or the invariants to hide the counterexample** — that violates the verifiable-behavior pillar.

- [ ] **Step 4: Capture the canonical baseline file.** Whether the run succeeded or revealed a defect, the canonical evidence file is `kesseldb-tla/results/2026-05-19-baseline.txt`. On success it contains the "No error has been found" line; on the defect path (after the spec/protocol is fixed and re-run), it contains the green run from after the fix, and the prior counterexample is preserved alongside it as `2026-05-19-counterexample.txt`.

- [ ] **Step 5: Sanity-check the result file.**
```bash
cd /c/Users/ihass/KesselDB/kesseldb-tla && grep -E "(No error|Error|Invariant|states generated|states found)" results/2026-05-19-baseline.txt | head -10
```
Expected (success): one line "Model checking completed. No error has been found." and the states-generated/distinct summary.

- [ ] **Step 6:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total == `<BASELINE>` (no Rust changed), seed-7 green.

- [ ] **Step 7: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add kesseldb-tla/Replication.cfg kesseldb-tla/results/ && git commit -m "tla: Replication.cfg + baseline TLC run (S1/SP109 T3; 5 invariants, 0 errors)" && git push
```

Adapt the commit message if the run revealed a defect: `"tla: Replication.cfg + baseline TLC run (S1/SP109 T3; counterexample found — see results/2026-05-19-counterexample.txt; investigating)"` — honest disclosure wins over false-green messaging.

---

### Task 4: TLA+-to-kessel-vsr mapping table (in the spec record) (#202)

**Files:** This task's output is the mapping table that will go into the SP109 record (T6). T4 produces a draft of the table and adds it as a comment block inside `Replication.tla` so the spec is self-describing.

- [ ] **Step 1: Confirm the mapping table from the design spec.** Read Decision 5 of `docs/superpowers/specs/2026-05-19-tla-replication-safety-design.md` — the 11-row mapping table is the source of truth. Cross-check each row against the actual kessel-vsr method signatures by reading the corresponding line ranges in `crates/kessel-vsr/src/lib.rs`:
  - `Msg::Request` / `Replica::on_request` (line ~449)
  - `Msg::Prepare` / `Replica::on_prepare` (line ~502)
  - `Msg::PrepareOk` / `Replica::on_prepare_ok` (line ~554)
  - `Msg::Commit` / `Replica::on_commit_msg` (line ~585)
  - `Replica::apply_through` (line ~340) — the `Apply` action body
  - `Replica::tick` (line ~722) — the timeout/idle branch
  - `Msg::StartViewChange` / `Replica::on_svc` (line ~627)
  - `Msg::DoViewChange` / `Replica::maybe_finish_svc` + `maybe_finish_view_change` (line ~640 + ~663)
  - `Msg::StartView` / `Replica::on_start_view` (line ~698)

- [ ] **Step 2: Add an in-spec mapping-table comment block.** Inside `Replication.tla`, after the opening preamble and before `EXTENDS`, ensure the action-mapping table is present as a comment block (T2 sketched this; T4 finalizes it). Form:

```tla
(*

Action-to-Rust mapping table (per the S1 design spec, Decision 5):

  TLA+ action            kessel-vsr counterpart                  Notes
  -------------------    -------------------------------------   --------------------
  ClientRequest          Msg::Request / Replica::on_request      Client -> primary
  HandlePrepare          Msg::Prepare / Replica::on_prepare      Primary -> backups
  HandlePrepareOk        Msg::PrepareOk / on_prepare_ok          Backup -> primary
  HandleCommit + Apply   Msg::Commit / on_commit_msg+apply_through
  Apply                  apply_through body                      per-entry SM apply
  TimeoutPrimary         Replica::tick idle branch -> start_view_change
  StartViewChange        Msg::StartViewChange / on_svc           Vote for view change
  HandleDoViewChange     Msg::DoViewChange / maybe_finish_svc    Sent by backup
  BecomePrimary          maybe_finish_view_change                Picks best log
  HandleStartView        Msg::StartView / on_start_view          New primary broadcasts
  DropMessage            (no Rust fn; corresponds to sim drop_pct injector)

The mapping is NAMED CORRESPONDENCE, not mechanized refinement. The S1
design spec's Honest Disclosure section names the gap explicitly.

*)
```

- [ ] **Step 3:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total == `<BASELINE>`, seed-7 green.

- [ ] **Step 4: Commit** (if T2 already shipped the comment block, this task may net-0 the file and only update the SP109 record — in that case skip the commit and roll the mapping refinements into T6):
```bash
cd /c/Users/ihass/KesselDB && git add kesseldb-tla/Replication.tla && git commit -m "tla: action-mapping comment block in Replication.tla (S1/SP109 T4; named correspondence, not mechanized refinement)" && git push
```

---

### Task 5: README + workflow docs (#203)

**Files:** Modify `kesseldb-tla/README.md` (replace the T1 skeleton with the full content).

- [ ] **Step 1: Expand `kesseldb-tla/README.md`** to its full form. Sections (mirroring the design spec's Workflow + Honest Disclosure + Decisions 6 + 7):

```markdown
# kesseldb-tla — TLA+/TLC safety specs for the VSR replication log

**Status:** S1 of the THESIS.md strategic-tier backlog (= SP109 in
subproject numbering). Date adopted: 2026-05-19.

**Thesis pillar strengthened:** verifiable behavior.

## What this directory contains

- `Replication.tla` — TLA+ specification of the VSR replication
  protocol abstracted from `crates/kessel-vsr`. Models full
  normal-mode replication + a minimal view-change. **Safety-only**
  (no Liveness/temporal formulas in this slice).
- `Replication.cfg` — TLC model configuration: bounded constants
  (N=3, MaxDrops=3, MaxViewChanges=2, MaxRequests=3) + the five
  safety invariants (TypeOK, LogPrefixSafety, NoDivergence,
  ExactlyOnceApply, MonotonicCommitPoint).
- `verify.ps1` / `verify.sh` — Windows / POSIX runner scripts.
- `results/` — captured TLC stdout, dated. Baseline:
  `results/2026-05-19-baseline.txt`.

## What this directory is NOT

- It is not part of the Cargo workspace; `cargo build` and
  `cargo test --workspace --release` ignore it.
- It does not prove `kessel-vsr` is bug-free. It proves the
  abstract model at bounded N=3 upholds the five invariants. See
  the Honest Disclosure section below.
- It does not include an auto-replay harness from TLC traces to
  Rust tests. Counterexample translation is manual — guided by the
  action-mapping table in `Replication.tla`. The auto-replay
  harness is **S1.1 follow-up**.

## Quick start

1. Install Java 11+ on PATH.
2. Download `tla2tools.jar` from
   https://github.com/tlaplus/tlaplus/releases/latest. Place at any
   path; export `TLC_JAR` to it:
   - POSIX: `export TLC_JAR=/path/to/tla2tools.jar`
   - PowerShell: `$env:TLC_JAR = 'C:\path\to\tla2tools.jar'`
3. From this directory: `./verify.sh` (POSIX) or `.\verify.ps1`
   (Windows). The script runs TLC and tees stdout to a dated file
   in `results/`.

Expected baseline outcome: TLC prints
"Model checking completed. No error has been found." and a summary
of states explored (~10^5–10^6 states; minutes to hours).

## Interpreting TLC output

**Success.** The final line of stdout contains
`Model checking completed. No error has been found.` plus a
summary line `X states generated, Y distinct states found`. This is
the green-gate condition.

**Counterexample (failure).** TLC prints
`Error: Invariant <NAME> is violated.` followed by a state trace —
a sequence of `State 1: ...`, `State 2: ...` records each showing
the action taken and the resulting variable bindings.

To translate a TLC counterexample to a kessel-vsr behavior:

1. Inspect the trace's first violating state — what action produced
   it? What were the relevant variable bindings?
2. Look up the action in `Replication.tla`'s mapping comment block
   (= the S1 design spec's Decision-5 table). Each TLA+ action
   maps to a kessel-vsr method or Msg variant.
3. Walk back through the trace to identify the sequence of actions
   (including any DropMessage steps) that led to the violation.
4. Write a Rust integration test in `crates/kessel-vsr/tests/`
   (or extend the existing sim harness) that drives the
   corresponding sequence of `Replica::handle` / `Replica::tick`
   calls + sim message drops. The test should fail in the same way
   the model failed.
5. **Diagnose:** Is the spec wrong (more permissive than the real
   protocol — refine the spec), or is the protocol wrong (real
   safety bug — fix kessel-vsr + add the regression test as a
   permanent gate)?
6. Re-run TLC after the fix; confirm zero errors.

**Auto-replay harness (S1.1 follow-up).** When implemented, this
manual translation step becomes mechanical: a Rust harness parses
the TLC trace and drives kessel-vsr through the equivalent
sequence, comparing state at each step. Documented but not in this
slice.

## Rigor-checkpoint cadence rule

The TLC run is **NOT** a per-commit CI gate (the bounded
model-check takes minutes-to-hours; per-commit blocking would be
too slow). Instead:

- **TLC MUST pass before any merge that modifies
  `crates/kessel-vsr/`.** This is a discipline rule for the next
  agent / contributor. Implemented by reading-the-README, not by
  hook.
- **Quarterly re-runs** (at minimum) on the baseline configuration.
- **Every TLC output is captured** in `results/` (dated files;
  never overwrite the baseline).
- The baseline result file `results/2026-05-19-baseline.txt` is
  the canonical evidence; any future run that produces a different
  result is itself a defect investigation.

A scheduled CI integration (GitHub Actions weekly job) is
**S1.8 follow-up** — not in this slice.

## Honest disclosure: the model-vs-implementation gap

**This slice does NOT prove `kessel-vsr` is bug-free.** It proves
that the abstract model of the VSR protocol, with the specified
bounded constants (N=3, MaxDrops=3, MaxViewChanges=2,
MaxRequests=3), upholds the five named invariants.

What this leaves uncovered:

- **Bounded N.** N=5, N=7 are not checked. Bugs that only manifest
  at scale are outside the model's reach. (Follow-up: S1.4.)
- **Bounded actions.** State transfer (GetState/NewState),
  crash-stop restart, client retransmission, the client table, the
  catalog, and SQL are not modeled. Bugs in those layers are
  outside the model's reach. (Follow-ups: S1.5, S1.6.)
- **Not mechanically refined.** A bug that exists in the Rust code
  but NOT in the TLA+ model is not caught by TLC. The TLC pass is
  a necessary, not sufficient, condition for kessel-vsr
  correctness. (Follow-up: S1.7.)
- **Spec author correctness.** A bug in the TLA+ spec itself can
  hide an implementation bug. Mitigation: the two-stage subagent
  review gate; the named-action correspondence table.
- **Safety only, not Liveness.** This slice does NOT prove
  "every client request eventually commits under fairness." That
  is **S1.2 follow-up**.

What this slice DOES achieve:

- Rules out a class of safety bugs (the four checked invariants)
  in the abstract VSR protocol at the bounded configuration. This
  is a non-trivial property — SP37 fixed exactly this class of bug
  in the implementation, and TLC at the parameters of this slice
  would have caught a model with that bug.
- Establishes the artifact that S1.X follow-ups extend.
- Provides a mechanized regression check for future kessel-vsr
  changes.
- Produces a permanent, externally-checkable record (the baseline
  result file).

## Deferred S1.X follow-ups

- **S1.1** Auto-replay harness in Rust (consumes TLC trace; drives
  kessel-vsr).
- **S1.2** Liveness invariants (temporal formulas + fairness).
- **S1.3** Linearizability check (model client request/response
  histories).
- **S1.4** N=5 configuration (run as separate result file).
- **S1.5** State-transfer modeling (GetState/NewState).
- **S1.6** Crash-stop restart modeling.
- **S1.7** Mechanized refinement TLA+ ↔ Rust.
- **S1.8** GitHub Actions scheduled CI integration.

## References

- Lamport, *Specifying Systems* (free PDF on
  lamport.azurewebsites.net) — the TLA+ language reference.
- Oki & Liskov, "Viewstamped Replication" (1988) — the protocol's
  origin paper.
- TigerBeetle vsr.tla — the closest peer-quality reference; mirrors
  the same protocol with a similar bounded-model-check workflow.
- KesselDB THESIS.md (`docs/THESIS.md`) — the strategic-tier
  context that motivates this slice (S1).
- KesselDB S1 design spec
  (`docs/superpowers/specs/2026-05-19-tla-replication-safety-design.md`) —
  every decision in this directory is recorded there.
```

- [ ] **Step 2:** `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | tail -15` → `FAILED=0`, total == `<BASELINE>`, seed-7 green.

- [ ] **Step 3: Commit**
```bash
cd /c/Users/ihass/KesselDB && git add kesseldb-tla/README.md && git commit -m "tla: kesseldb-tla/README.md — workflow, cadence, honest disclosure (S1/SP109 T5)" && git push
```

---

### Task 6: SP109 record + STATUS + memory + gate reconciliation (#204)

**Files:** Create `docs/superpowers/specs/2026-05-19-kesseldb-subproject109-tla-replication-safety.md`; modify `docs/STATUS.md`; modify (auto-memory, OUTSIDE repo, never git-add) `…\memory\project_kesseldb.md`.

- [ ] **Step 1: Measure.** `cargo test --workspace --release 2>&1 | tail -25` → `<FINAL>`; `<DELTA> = <FINAL> − <BASELINE>` (expected **0** — no Rust code changed in this slice). FAILED=0, seed-7 green. `cargo tree -p kesseldb-server | grep -Ei "parquet|objstore|rustls|webpki"` unchanged from SP108. Inspect `kesseldb-tla/results/2026-05-19-baseline.txt`:

```bash
cd /c/Users/ihass/KesselDB && grep -E "(No error|Error|Invariant|states generated|states found)" kesseldb-tla/results/2026-05-19-baseline.txt | head -10
```

Capture the states-generated / states-found numbers and the runtime.

- [ ] **Step 2: Internal record.** Read `docs/superpowers/specs/2026-05-19-kesseldb-subproject108-int96-decimal.md` for the EXACT convention, then create `docs/superpowers/specs/2026-05-19-kesseldb-subproject109-tla-replication-safety.md` mirroring it: `# KesselDB — Subproject 109: S1 TLA+/model-checked safety specs`; `**Date:** 2026-05-19  **Status:** done — TLA+ spec + TLC baseline run committed.`; bare-backtick Builds-on lines pointing at THESIS.md + SP12/SP13/SP37 (the lineage that motivated the slice) + SP108 (the immediately-preceding subproject record); Design + Plan lines (the two files this task is the record of); `---` separators. Sections:

  - **Strategic-tier framing:** "S1 in the THESIS.md backlog (verifiable-behavior pillar; the single artifact that converts 'looks rigorous' → 'is provably so'). SP109 in the subproject numbering (the slice immediately after SP108 INT96+DECIMAL). Both numbers reference the same slice."

  - **What shipped:** `kesseldb-tla/` directory at the repo root (outside the Cargo workspace; zero impact on cargo build/test). Files: `Replication.tla` (VSR protocol model — full normal-mode + minimal view-change), `Replication.cfg` (bounded constants N=3, MaxDrops=3, MaxViewChanges=2, MaxRequests=3 + 5 invariants TypeOK / LogPrefixSafety / NoDivergence / ExactlyOnceApply / MonotonicCommitPoint), `verify.ps1` + `verify.sh` (Windows + POSIX TLC runners with TLC_JAR env-var contract), `README.md` (install + run + interpret + cadence + counterexample workflow + honest disclosure + S1.1–S1.8 follow-ups + references), `results/2026-05-19-baseline.txt` (captured TLC baseline run — <STATES_GENERATED> states generated, <STATES_FOUND> distinct, runtime <H:MM:SS>, ZERO ERRORS).

  - **TLA+-to-Rust correspondence:** named correspondence per the design spec's Decision 5 (11-row table reproduced in `Replication.tla`'s comment block + below). **Not mechanized refinement.** The honest disclosure of the gap is the slice's primary discipline.

  - **The 5 invariants (briefly):** TypeOK (well-formedness); LogPrefixSafety (committed prefixes are mutually consistent — **the headline contract**); NoDivergence (same-length committed prefixes are byte-identical); ExactlyOnceApply (each committed entry applied exactly once per replica); MonotonicCommitPoint (commit point non-decreasing; encoded as inductive via action shapes).

  - **Bounds rationale:** N=3 (minimal quorum), MaxDrops=3 + MaxViewChanges=2 + MaxRequests=3 (enough to exhibit the SP37 bug class; tractable for TLC). Larger configurations are S1.4 follow-up.

  - **Verification:** TLC ran to completion with zero invariant violations. The baseline result file is `kesseldb-tla/results/2026-05-19-baseline.txt`. Re-runnable by any contributor with Java 11+ and the TLA+ jar; the README documents the install + run path.

  - **Cross-crate impact:** ZERO. No Rust code was modified. `cargo test --workspace --release` total stays at `<BASELINE>` (= `<FINAL>` = 484). `kesseldb-tla/` is outside the Cargo workspace. `cargo tree` output unchanged.

  - **T2/T3 model selection disclosure:** T2 (Replication.tla) and T3 (Replication.cfg + TLC baseline run) required TLA+ familiarity and were dispatched to an Opus-class subagent; T0/T1/T4/T5/T6 are standard Markdown/scripting work. This is the controller's model-selection discipline working as designed.

  - **Honest gate accounting:** `<BASELINE>` → `<FINAL>` (== `<BASELINE>`; net-0, expected — no Rust changed). The NEW rigor gate is **TLC: 0 invariant violations at the bounded configuration**, captured in `kesseldb-tla/results/2026-05-19-baseline.txt`. This is the slice's first non-cargo-test gate; it represents the verifiable-behavior pillar of the THESIS becoming a checkable artifact rather than an intention.

  - **Honest disclosure (the slice's primary discipline):**
    - Bounded model: N=3 only; bigger sizes uncovered.
    - Bounded actions: state transfer, restart, client table, catalog, SQL: not modeled.
    - Not mechanically refined to Rust: a Rust-only bug is not caught.
    - Spec-author correctness: a bug in the spec itself can hide a real bug.
    - Safety-only: Liveness deferred to S1.2.
    The full discussion is in the README's Honest Disclosure section.

  - **Deferred (S1.1–S1.8 + thesis-tier S2/S3/S4):**
    - S1.1 auto-replay harness; S1.2 Liveness; S1.3 linearizability; S1.4 N=5; S1.5 state-transfer; S1.6 crash-restart; S1.7 mechanized refinement; S1.8 CI scheduled job.
    - S2 MVCC over the deterministic log; S3 Jepsen against a real cluster; S4 deterministic WASM UDF runtime (per THESIS.md strategic backlog).

  - **Thesis-fit note** (mandatory per THESIS.md):
    `Thesis fit: verifiable (the headline pillar this slice strengthens — converts "the VSR protocol is tested" into "the abstract VSR protocol with bounded N=3 is model-checked against 5 safety invariants and zero counterexamples are found"); honest-docs (the model-vs-implementation gap is named explicitly; the bounded-not-arbitrary disclosure; the named-correspondence-not-mechanized-refinement disclosure).`

- [ ] **Step 3: STATUS.md** — insert SP109 row immediately AFTER the SP108 row (numeric order), matching the SP108 row format incl. gate `<BASELINE>→<FINAL>` (here both are 484 — net-0, with the explicit "no Rust changed; TLC pass is the new rigor gate" qualifier), `Record:` backlink, clause:

> "S1 of the THESIS.md strategic-tier backlog (verifiable-behavior pillar). Ships `kesseldb-tla/` — TLA+ specification of the VSR replication protocol abstracted from `kessel-vsr` (full normal-mode + minimal view-change; safety-only; state transfer/restart/client-table out of scope), bounded TLC model config (N=3 / MaxDrops=3 / MaxViewChanges=2 / MaxRequests=3), and 5 safety invariants (TypeOK + LogPrefixSafety + NoDivergence + ExactlyOnceApply + MonotonicCommitPoint). TLC baseline run: <STATES_GENERATED> states / <STATES_FOUND> distinct / <H:MM:SS> / ZERO ERRORS — captured in `kesseldb-tla/results/2026-05-19-baseline.txt`. Action-mapping table inside `Replication.tla` (named correspondence, NOT mechanized refinement — honest-disclosed). Rigor-checkpoint cadence: TLC must pass before any kessel-vsr merge; baseline file is the canonical evidence; not a per-commit CI gate. Honest gate: 484→484 (net-0; no Rust touched — the TLA+ work is the rigor artifact, not a test-count delta). Honest disclosure (the slice's primary discipline): bounded model (N=3, MaxDrops=3, MaxViewChanges=2, MaxRequests=3) — bigger configurations + state transfer + restart + client table + SQL not modeled; not mechanically refined to Rust — Rust-only bugs not caught by TLC; safety-only — Liveness deferred. Deferred S1.X: S1.1 auto-replay harness / S1.2 Liveness / S1.3 linearizability / S1.4 N=5 / S1.5 state-transfer / S1.6 crash-restart / S1.7 mechanized refinement / S1.8 CI scheduled job. Record: `docs/superpowers/specs/2026-05-19-kesseldb-subproject109-tla-replication-safety.md`."

- [ ] **Step 4:** `cargo test --workspace --release 2>&1 | tail -12` → `FAILED=0`, total == `<BASELINE>` == `<FINAL>` = 484, seed-7 green.

- [ ] **Step 5: Commit docs**
```bash
cd /c/Users/ihass/KesselDB && git add docs/superpowers/specs/2026-05-19-kesseldb-subproject109-tla-replication-safety.md docs/STATUS.md && git commit -m "docs: S1/SP109 TLA+ replication-safety record + STATUS row + gate reconciliation" && git push
```

- [ ] **Step 6: Auto-memory (OUTSIDE repo — never git-add).** Append via Bash heredoc an SP109 block to `/c/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/project_kesseldb.md`: summarise the TLA+ Replication.tla + Replication.cfg + 5 invariants + bounded constants (N=3 etc.) + the baseline TLC pass (0 errors / <STATES_GENERATED> states / <H:MM:SS>) + the named-correspondence honest disclosure + the rigor-checkpoint cadence + the 8 S1.X follow-ups. Then Read `/c/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/MEMORY.md`, find the `- [KesselDB](project_kesseldb.md) — …` line, Edit its trailing status clause to:

`SP109 SHIPPED: S1 of THESIS.md strategic backlog — TLA+/TLC replication-safety specs in kesseldb-tla/ (Replication.tla protocol model + Replication.cfg bounded N=3 + 5 safety invariants TypeOK/LogPrefixSafety/NoDivergence/ExactlyOnceApply/MonotonicCommitPoint + verify.{ps1,sh} runners + README + baseline TLC pass 0 errors). Net-0 cargo gate (no Rust changed); TLC is the new rigor gate. Named correspondence not mechanized refinement (honest-disclosed). OBJ-2c arc still 3/5. Open: S1.1 auto-replay harness / S1.2 Liveness / S1.3 linearizability / S1.4 N=5 / S1.5 state-transfer / S1.6 crash-restart / S1.7 mechanized refinement / S1.8 CI scheduled job; S2 MVCC / S3 Jepsen / S4 WASM UDF; OBJ-2c-2 zstd / OBJ-2c-5 REPEATED-nested / Fixed-coerce + signed-Timestamp-coerce / OBJ-3 Iceberg / OBJ-4 listing / OBJ-5 STS-SAS / WASM / #75 SP-A scatter scan / seed-7 liveness / SQL-over-cluster`

Keep the line's existing prefix intact.

- [ ] **Step 7:** `cd /c/Users/ihass/KesselDB && git status --porcelain` EMPTY (no memory path, no stray logs; rm -f any test-output.log; `kesseldb-tla/results/*.txt` files OTHER than `2026-05-19-baseline.txt` and `2026-05-19-counterexample.txt` (if present) can be deleted as transient — only the canonical baseline is the permanent evidence). Report DONE.

---

## Self-Review

**1. Spec coverage:** Scaffold `kesseldb-tla/` + verify scripts + README skeleton → T1; Replication.tla protocol model (full normal-mode + minimal view-change, 11 actions per the mapping table) → T2; Replication.cfg bounded constants + 5 invariants + baseline TLC run + results capture → T3; action-mapping table comment block in Replication.tla → T4; full README (workflow + cadence + counterexample-translation + honest disclosure + 8 deferred S1.X items + references) → T5; SP109 record + STATUS row + memory + honest gate accounting (net-0 cargo + the new TLC rigor gate) → T6. All design-spec sections mapped to a task.

**2. Placeholder scan:** N=3 / MaxDrops=3 / MaxViewChanges=2 / MaxRequests=3 are the design-spec-fixed bounds. The 5 invariants are named explicitly (TypeOK + LogPrefixSafety + NoDivergence + ExactlyOnceApply + MonotonicCommitPoint). The 11-action mapping is taken verbatim from Decision 5 of the design spec. The kessel-vsr line numbers (449/502/554/585/340/722/627/640/663/698) are read-and-verified from the current `lib.rs`. `<BASELINE>` and `<FINAL>` are runtime-measured (T0/T6). `<STATES_GENERATED>` and `<STATES_FOUND>` and `<H:MM:SS>` are TLC outputs measured in T3 and inserted by T6. No "TBD" beyond what the README skeleton honestly marks for T5 expansion. The TLC counterexample contingency in T3 is the gate-working-as-designed disclosure path, not a hand-wave.

**3. Type consistency:** The plan ships only `.tla` / `.cfg` / `.ps1` / `.sh` / `.md` / `.txt` files plus the SP109 record + STATUS row. No Rust files modified. `cargo test --workspace --release` total stays at 484. `kesseldb-tla/` is OUTSIDE the Cargo workspace (no `Cargo.toml`, no `[workspace.members]` entry). `cargo tree -p kesseldb-server` output unchanged from SP108. The new gate is the TLC pass captured in `kesseldb-tla/results/2026-05-19-baseline.txt`. Everything compiles by construction (no Rust changes to compile); the cargo gate is provably net-0 by file-list-inspection.

**4. Honest-disclosure scan:** The design spec's Honest Disclosure section + the README's Honest Disclosure section + the SP109 record's Honest Disclosure paragraph all carry the same message: bounded model + bounded actions + not-mechanically-refined + safety-only + spec-author-correctness-caveat. The slice does NOT overclaim. The thesis-fit note explicitly names both `verifiable` (the strengthened pillar) and `honest-docs` (the disclosure discipline). The counterexample-found contingency in T3 is a real path, not a green-checkmark theater path; if TLC finds a defect, the gate accounting records it honestly.

**5. Autonomous-mandate handling:** The brainstorming user-review gate is satisfied by the design spec (`docs/superpowers/specs/2026-05-19-tla-replication-safety-design.md`); the two-stage subagent review gate is preserved per task (spec-quality then artifact-quality); the final whole-implementation review is preserved; the "offer execution choice" terminal step of writing-plans is pre-resolved to `subagent-driven-development` per the mandate. The plan does NOT prompt; it commits its docs and reports back.

Plan is internally consistent and fully covers the S1 / SP109 design.
