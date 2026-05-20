# KesselDB — Project Thesis

**Date adopted:** 2026-05-19  
**Status:** adopted  
**Source:** 2026-05-19 strategic review; recorded in `memory/project_kesseldb_strategic_tier.md`
and the strategic-tier context section of
`docs/superpowers/specs/2026-05-19-kesseldb-subproject108-int96-decimal.md`.

---

## The thesis

> **Deterministic replicated SQL with verifiable behavior and replayability.**

Each term has a concrete meaning in this codebase:

- **Deterministic replicated SQL.** The state machine (`kessel-sm`) and the
  Viewstamped Replication layer (`kessel-vsr`) together guarantee that, given the
  same log prefix, every replica produces byte-identical committed state.  No
  wall-clock reads, no thread-scheduling-dependent ordering, and no implicit
  allocator-dependent behavior appear inside the deterministic path.  The SQL
  surface (`kessel-sql`, `kessel-expr`) is served from that same deterministic
  core; a SQL query is a function of committed log state, nothing else.

- **Verifiable behavior.** "Looks rigorous" is insufficient.  The project ships
  mechanically-checked artifacts (S1: TLA+/model-checked safety specs for the
  replication protocol and MVCC) and externally-attested rigor (S3: Jepsen
  results against a real 3-node cluster under partition + clock skew + process
  kill).  Internal tests are necessary but not sufficient; the thesis demands
  artifacts an outsider can check independently.

- **Replayability.** Every committed behavior is a function of a seed corpus
  and an ordered log.  The `kessel-sim` fault simulator has run the historically
  difficult seed 7 since M3; the seeded adversarial-replay pattern is the
  debugging discipline: any bug report reduces to a `(seed, log)` tuple.
  Debugging IS replay.  The strategic-tier WASM UDF work (S4) extends this
  property to user-defined extensions: a WASM UDF is sandboxed, gas-accounted,
  and deterministic, so a UDF's behavior is also replayable.

The strategic-tier backlog items S1–S4 (listed below) turn these three
properties from design intentions into mechanically-provable and
externally-attested artifacts.

---

## Comparison to the great database theses

Each great database system has a deep core idea that makes it distinctive.
KesselDB's thesis is a peer of these:

| System        | Core thesis                                                   |
|---------------|---------------------------------------------------------------|
| PostgreSQL    | Extensible relational engine                                  |
| FoundationDB  | Ordered transactional key-value core                          |
| DuckDB        | Embedded vectorized OLAP                                      |
| TigerBeetle   | Deterministic financial ledger                                |
| Datomic       | Immutable temporal database                                   |
| **KesselDB**  | **Deterministic replicated SQL with verifiable behavior and replayability** |

This framing is the user's verbatim rationale from the 2026-05-19 strategic
review: "The great database systems usually have a deep core idea.  KesselDB's
path to incredible is probably: deterministic replicated SQL with verifiable
behavior and replayability — that's the most differentiated part of the design."

---

## What this thesis commits to

These are design rules that flow directly from the thesis.  They are not
aspirational; they are constraints that every slice must satisfy.

### Deterministic kernel

- No wall-clock reads, no thread-nondeterminism, no implicit
  allocator-dependent ordering inside `kessel-sm`, `kessel-catalog`,
  `kessel-codec`, `kessel-vsr`, or `kessel-expr`.
- Replication produces bytewise-identical committed state across all replicas
  given the same log prefix.  The seeded VSR simulation corpus (100 seeds × 2
  runs identical; seed 7 green at every merge) is the ongoing gate.
- The `kessel-io` seam is the single injection point for clock, disk, and
  network.  Production injects real I/O; `kessel-sim` injects a seeded,
  fault-injecting fake.  Nothing above `kessel-io` may cross this boundary
  without going through the seam.

### Verifiable behavior

- The project ships mechanically-checked safety invariants (S1: TLA+/TLC or
  Apalache model-checked specs for linearizability, exactly-once apply, and
  log-prefix safety under partition + restart).
- The project ships externally-attested rigor (S3: Jepsen linearizability
  checker — Knossos/Elle — against a real 3-node cluster).
- Gate figures in slice records are real measured numbers, not estimates.
  Every plan deviation is disclosed in the slice's permanent record (the SP107
  V1-ordering-defect disclosure, the SP108 plan-arithmetic correction, and the
  T4 cross-physical-type-pin gate-caught correction are the discipline, not the
  exception).

### Replayability

- Every commit's behavior is a function of its seed corpus and log state.
  This is not a goal; it is enforced by the determinism seam.
- Bug reports reduce to `(seed, log)` tuples.  Debugging IS replay: reproduce
  the simulation run, replay the log, observe the defect.
- The `kessel-sim` seed-7 liveness test (the historically-difficult seed) runs
  on every CI merge and is a hard gate.

### Zero-dependency kernel

- External dependencies are kept out of the deterministic path.  The
  `kessel-parquet` crate (pure-Rust, zero external deps, `#![forbid(unsafe_code)]`)
  and the hand-rolled zero-dep HMAC-SHA256 / RFC-1952 / Snappy / Thrift
  implementations are existing examples of this discipline.
- New external dependencies in the default build require an explicit
  thesis-fit justification.  Feature-gated dependencies (rustls, objstore) are
  acceptable because they do not enter the deterministic core path.

### Honest-engineering documentation

- Every slice's permanent record in `docs/superpowers/specs/` names its
  plan deviations, gate disclosures, and any retroactive corrections.
- The honest-gate accounting pattern (e.g., "Honest gate: 425→484 (+59; not
  zero-delta") is mandatory, not optional.  Suppressing gate disclosures is a
  thesis violation.
- Going forward, every spec gets a one-line **thesis-fit note** (see the
  per-slice rule below).

---

## What this thesis explicitly does NOT commit to

The thesis is only useful if it has boundaries.  The following are
explicitly out of scope:

**Not optimizing for:**

- Vendor-locked feature parity with PostgreSQL or MySQL.  Coverage breadth
  is not a thesis-defining goal; coverage of features that prove deterministic
  replicated correctness is.
- Cost-based optimizer feature parity with DuckDB or Snowflake.  A CBO is
  deferred until SQL workload demand justifies it and until the thesis core is
  complete.
- Storage-format breadth beyond what proves the thesis.  Parquet is supported
  because the Iceberg/lakehouse path (OBJ-3) is on the thesis trajectory.
  Arbitrary new formats are deferred until the thesis-fit justifies them.
- SaaS-style operational features that bloat the binary (e.g., cloud-native
  autoscaling APIs, multi-cloud storage tiering, managed backup).

**Not a replacement for:**

- PostgreSQL.  KesselDB is not Postgres-with-replication.  Users who need
  PostgreSQL's extension ecosystem, maturity, or operational tooling should
  use PostgreSQL.
- Streaming databases (Materialize, RisingWave).  Their thesis is continuous
  incremental view maintenance over event streams; KesselDB's thesis is
  deterministic replicated correctness over a committed log.
- Graph databases.  No graph traversal primitives are on the roadmap.
- Feature stores.  Online/offline feature store semantics are not a goal.

---

## Strategic-tier backlog (S1–S4)

These four items were added during the 2026-05-19 strategic review.  They are
ordered by thesis-leverage: each converts an existing design property from
"intended" to "provable" or "externally attested."  S1 is the immediate next
slice after THESIS.md.

Sources: `memory/project_kesseldb_strategic_tier.md` and the strategic-tier
context section of
`docs/superpowers/specs/2026-05-19-kesseldb-subproject108-int96-decimal.md`.

### S1 — TLA+/model-checked safety specs

**Thesis lever:** verifiable behavior (the single artifact that converts
"looks rigorous" → "is provably so").

Safety invariants for the replication log, checked mechanically via TLC or
Apalache: linearizability, exactly-once apply, log-prefix safety under
partition + restart.  The `kesseldb-tla/` directory.  Starts immediately
after this THESIS.md commit.  This is the TigerBeetle rigor lever:
TigerBeetle's formal protocol verification is a primary reason the system
is trusted in financial contexts.  KesselDB's deterministic log is the
right substrate for the same discipline.

### S2 — Serializable MVCC / Snapshot Isolation over the deterministic log

**Thesis lever:** deterministic + replayable (proves "consensus + SQL can be
simpler than MVCC-centric systems" concretely).

Snapshot reads without blocking writes; deterministic conflict resolution;
long-running reads without stalling compaction; replicated MVCC correctness
proofs.  Multi-slice (estimated 4–6 slices).  MVCC state is part of the
log; every snapshot is replayable from the log prefix that precedes it.

### S3 — Jepsen harness against a real 3-node cluster

**Thesis lever:** verifiable behavior (externally-attested; the gold-standard
adoption signal for serious distributed databases).

Linearizability checker (Knossos/Elle) under partition + clock-skew +
process-kill against a live cluster.  Pairs with S1: together they complete
the full rigor story — S1 proves the protocol correct by construction, S3
demonstrates correctness under real fault conditions.  Published Jepsen
results have been the canonical trigger for institutional adoption of
distributed databases (CockroachDB, TigerBeetle, etc.).

### S4 — Deterministic in-tree WASM UDF runtime

**Thesis lever:** deterministic + replayable + zero-dep (the distinctive
extension story: most databases cannot safely combine extensibility with
deterministic replication).

Sandboxed, gas-accounted, zero-import WASM UDF runtime inside the
deterministic core.  A WASM UDF is part of the replicated catalog; every
replica runs byte-identical logic; UDF behavior is replayable from the log.
This subsumes the existing open "WASM trigger sandbox" item from SP4/SP8.
The gas-accounting constraint prevents non-termination without breaking the
determinism property.

---

## Per-slice thesis-fit note (rule, going forward)

Every future spec in `docs/superpowers/specs/` must include a one-line
**thesis-fit note** in its decisions section, naming which thesis pillar(s)
the slice strengthens:

| Pillar            | Label             |
|-------------------|-------------------|
| Deterministic kernel | `deterministic` |
| Verifiable behavior  | `verifiable`    |
| Replayability        | `replayable`    |
| Zero-dep kernel      | `zero-dep`      |
| Honest-engineering docs | `honest-docs` |

Example note (in a decisions section):

```
Thesis fit: verifiable (source-independence pin proves format-agnostic
decode correctness), honest-docs (7th e2e fail-closed + T4 plan-arithmetic
correction disclosed).
```

**Retroactive mapping for recent slices** (so future maintainers reading
those records can locate the thesis contribution):

| Slice | Thesis-fit mapping |
|---|---|
| SP107 V1-ordering-defect regression KAT | `verifiable` + `honest-docs` |
| SP107 source-independence pin (V2 format) | `verifiable` + `deterministic` |
| SP108 source-format-independence pin (INT96/DECIMAL cross-physical-type) | `verifiable` + `deterministic` |
| SP108 7th e2e via FailClosedCase struct | `honest-docs` |
| SP108 T4 plan-arithmetic correction disclosed | `honest-docs` |
| SP106 zero-dep RFC-1952/RFC-1951 GZIP inflate | `zero-dep` + `deterministic` |
| SP100/SP101 zero-dep SigV4/Parquet (no external crate) | `zero-dep` |
| M3 seeded VSR partition simulation (seed-7 gate) | `replayable` + `verifiable` |
| M0 determinism seam (`kessel-io` injection) | `deterministic` |

---

## Process note: how this thesis was adopted

The 2026-05-19 strategic review was user-led (via a ChatGPT strategic-tier
analysis session).  The thesis sentence — "deterministic replicated SQL with
verifiable behavior and replayability" — was identified by the user as the
most differentiated path for KesselDB, by analogy to the core ideas of the
great database systems (PostgreSQL, FoundationDB, DuckDB, TigerBeetle,
Datomic).

The user then resolved a sequencing question: finish SP108 (OBJ-2c-4
INT96/DECIMAL) first, then write `docs/THESIS.md`, then start S1 (TLA+
specs).  That decision is recorded in `memory/project_kesseldb_strategic_tier.md`
and mirrored in the strategic-tier context section of the SP108 record.

This document is the output of that decision.  It is a permanent record, not
a living document; if the thesis is refined in a future review, a new dated
entry supersedes this one rather than editing it in place.
