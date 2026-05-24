# S2.6 — SQL Integration + SM Cutover (Move MVCC From Dormant to Production Data Path; Ship AdvanceWatermark Heartbeat): Design

**Date:** 2026-05-24
**Status:** Approved (autonomous mandate — see Process Note)
**Strategic-tier item:** S2.6 sub-slice of S2 (Serializable MVCC / Snapshot
Isolation) in the THESIS.md S1–S4 backlog. The **sixth and FINAL** built
sub-slice of S2 after S2.1/SP110 (MVCC versioned-storage primitive),
S2.2/SP111 (Tx context + read-set), S2.3/SP112 (SI write-side +
conflict detection at SM apply time), S2.4/SP113 (Cahill SSI
dangerous-structure detector + pending_txs window), and S2.5/SP114
(GC + dynamic watermark protocol; superseded SP113 bounded-window
false-negative). **SP115** in the subproject numbering. **CLOSES S2.**
**Builds on:**
- Project THESIS — `docs/THESIS.md`.
- S2 parent design — `docs/superpowers/specs/2026-05-23-mvcc-si-design.md`.
- S2.1 record (SP110) — `docs/superpowers/specs/2026-05-23-kesseldb-subproject110-mvcc-s2-1.md`.
- S2.2 record (SP111) — `docs/superpowers/specs/2026-05-24-kesseldb-subproject111-mvcc-tx-s2-2.md`.
- S2.3 record (SP112) — `docs/superpowers/specs/2026-05-24-kesseldb-subproject112-mvcc-si-s2-3.md`.
- S2.4 record (SP113) — `docs/superpowers/specs/2026-05-24-kesseldb-subproject113-mvcc-ssi-s2-4.md`.
- S2.5 record (SP114) — `docs/superpowers/specs/2026-05-24-kesseldb-subproject114-mvcc-gc-s2-5.md`.
- S2.5 design — `docs/superpowers/specs/2026-05-24-mvcc-si-s2-5-design.md`.
- S2.5 TLA+ artifact — `kesseldb-tla/MVCCGc.tla` + `.cfg` + baseline TLC
  run (`kesseldb-tla/results/2026-05-24-mvcc-gc-baseline.txt`, 1,594,330
  distinct states, depth 12, complete coverage, zero violations).
- The MVCC module surface shipped through SP114: `crates/kessel-storage/src/mvcc.rs`
  (`make_versioned_key`, `decode_commit_opnum`, `put_versioned`,
  `get_at_snapshot`, `has_version_in_range`, `delete_versions_older_than`,
  `SnapshotRead`, `MvccKeyError`, `VERSIONED_KEY_LEN`, `PREFIX_LEN`).
- The SSI module surface shipped through SP114: `crates/kessel-storage/src/ssi.rs`
  (`PendingTxRecord`, `sorted_vec_intersects`, `detect_dangerous_structure`,
  `prune_pending_txs`, `prune_pending_txs_by_watermark`, `MAX_TX_AGE = 4096`).
- The Tx module shipped through SP114: `crates/kessel-storage/src/tx.rs`
  (`Tx<'a, V>` with `begin`, `begin_rw`, `begin_ssi` returning
  `Result<Self, TxError>`; `read`, `write`, `read_set`, `write_set`,
  `snapshot_opnum`, `commit`, `commit_ssi`, `commit_read_only`, `abort`;
  `TxCommitOutcome`; `TxError::{ReadOnlyCannotCommit, SnapshotTooOld, ...}`).
- The SM apply path shipped through SP114: `crates/kessel-sm/src/lib.rs`
  (`StateMachine<V>` with `pending_txs`, `low_water_mark`,
  `Op::CommitTx` arm, `Op::AdvanceWatermark` arm with full 7-step body).
- The proto Op + result shape shipped through SP114:
  `crates/kessel-proto/src/lib.rs` (`Op::CommitTx { snapshot_opnum,
  write_set, commit_opnum, read_set }` at wire tag 44;
  `Op::AdvanceWatermark { low_water_mark }` at wire tag 45;
  `OpResult::TxCommitted/TxAborted/WatermarkAdvanced/WatermarkRejected`;
  `AbortReason`; `WatermarkRejection`).
- The SQL surface shipped through SP30+: `crates/kessel-sql/src/lib.rs`
  (`compile`, `compile_stmt`, `Stmt::{Op, Update, Explain}` —
  `compile_stmt` is the source-of-truth wrapper that the server's
  `apply_one` dispatches over; `compile` returns a bare `Op`).
- The legacy 20-byte data path shipped through SP1–SP113:
  `crates/kessel-sm/src/lib.rs` apply arms for `Op::Create`,
  `Op::Update`, `Op::Delete`, `Op::GetById`, `Op::Select`,
  `Op::QueryRows`, `Op::SelectFields`, `Op::SelectSorted`,
  `Op::UpdateSet`, `Op::Aggregate`, `Op::GroupAggregate`, `Op::Join`,
  `Op::Query`, `Op::QueryExpr` — each calls `self.storage.put(op_number,
  Key, Vec<u8>)` / `self.storage.get(&Key) -> Option<Vec<u8>>` /
  `self.storage.scan_range(&Key, &Key) -> Vec<(Key, Vec<u8>)>` against
  20-byte object keys (per `Storage::put` at `crates/kessel-storage/src/lib.rs:550`).
- The server SQL dispatcher: `crates/kesseldb-server/src/lib.rs::apply_one`
  (single source of truth for "what one request does" — receives a
  `[0xFE] ++ SQL` frame, compiles via the `CompileCache`, dispatches
  through `StateMachine::apply`; the per-request entry point for both
  the normal path and pipelined batches; this is the SEAM at which
  S2.6 wraps every SQL request in an auto-commit Tx).
- **External background:** PostgreSQL's auto-commit (every statement
  outside a BEGIN/COMMIT block runs in its own implicit Tx);
  CockroachDB's "session implicit txn" (same model); SQL:1999
  `BEGIN`/`COMMIT`/`ROLLBACK` grammar (deferred to S2.7). KesselDB's
  variant: every auto-commit Tx submits an `Op::CommitTx` op with
  `commit_opnum = 0` placeholder; the SM apply path overwrites with
  `op_number` (the log position), producing a totally-ordered commit
  sequence by construction.

---

## Process note (autonomy + brainstorming gate)

Produced under the standing overnight autonomous-build mandate
(`feedback_kesseldb_autonomous_build` + the strategic-tier mandate
`feedback_kesseldb_strategic_tier`): "build the backlog autonomously,
BOLD choices, don't wait for approval, keep the two-stage review gate
+ full tests + pentest passes." **The brainstorming user-review gate
is substituted by this documented decision record** — the 10 brainstorm
decisions below are resolved boldly in this document; the user does
not re-review them before the plan executes. **The two-stage subagent
review gate is preserved** for every substantive task (T2/T3/T5/T6),
with the final whole-implementation reviewer dispatched at the end of
T6, exactly as SP110/SP111/SP112/SP113/SP114 did. **S2.6 is the LARGEST
sub-slice of S2** (touches production callers across SM + SQL +
introduces the heartbeat that S2.5 deferred + removes the SP1-SP113
20-byte legacy data path); the autonomous-mandate disclosure is
operationally critical here because the cargo-gate delta is the
widest range of any S2 slice (see Decision 8 honest range).

---

## Strategic-tier framing

S2.6 is the **sixth and final sub-slice of S2** in the THESIS.md
backlog. SP115 in the subproject numbering. The parent S2 design
(`docs/superpowers/specs/2026-05-23-mvcc-si-design.md`) decomposes S2
into 6 sub-slices: S2.1 (SP110 — versioned-storage primitive) →
S2.2 (SP111 — Tx context + read-set) → S2.3 (SP112 — SI write-side +
deterministic conflict at SM apply) → S2.4 (SP113 — SSI promotion via
Cahill dangerous-structure detection) → S2.5 (SP114 — GC + dynamic
watermark + bounded-window supersession) → **S2.6 (this slice — SQL
integration + SM cutover; moves MVCC from dormant to production data
path; ships the AdvanceWatermark heartbeat S2.5 deferred)**. This
slice CLOSES S2. After S2.6 ships, every production SELECT / INSERT /
UPDATE / DELETE flows through the MVCC layer; the legacy SP1-SP113
20-byte object-keyspace is REMOVED; the watermark heartbeat producer
runs in the VSR-adjacent layer and periodically submits
`Op::AdvanceWatermark` ops to keep MVCC storage bounded; the
`Tx::begin*` Result-returning API SP114 made breaking is finally
consumed by production callers as designed.

**The thesis-fit headline of S2.6.** This is the slice that
**operationalizes** the thesis "deterministic replicated SQL with
verifiable behavior and replayability" at the SQL surface. After S2.6,
a `SELECT` is byte-deterministically a snapshot read at
`current_commit_opnum` against the MVCC store; an `INSERT`/`UPDATE`/`DELETE`
is byte-deterministically an auto-commit Tx that submits an
`Op::CommitTx` through VSR; replicas converge on byte-identical MVCC
storage state after every committed SQL statement. **The thesis is no
longer aspirational at the SQL surface — it is the structural
guarantee of every executed SQL statement.** This is the second
strategic-tier headline of S2 after SP114's "GC becomes a structural
property of the log": SP115's "every SQL statement is a deterministic
MVCC Tx" makes the SQL→VSR→storage data flow a closed deterministic
loop, with no remaining legacy escape hatch.

**The breaking-cutover headline.** S2.6 deletes the SP1-SP113 20-byte
object-keyspace from the production data path. Every Op::Create /
Op::Update / Op::Delete / Op::GetById / Op::Select* / Op::Query* /
Op::Aggregate* / Op::GroupAggregate / Op::Join / Op::UpdateSet /
Op::QueryExpr apply arm — 14 SM apply arms touched — REWRITES against
the MVCC layer. Auxiliary structures (indexes, constraints, catalog
entries) continue to use the legacy 20-byte keypath in S2.6 (Decision
1b refinement); only the row-data keyspace converts. This is honest-
disclosed in Decision 1 below. The cargo gate impact range per
Decision 8: HIGHLY ASYMMETRIC — the existing legacy-keypath data-row
SM apply tests (~80 tests across SP3-SP100 era) either migrate to
MVCC-keyspace assertions or get DELETED; SQL+MVCC integration tests
land NEW (~30-40 tests); pentest hardens the cutover (~15 tests);
NET cargo gate delta is +20 to +50 tests with HIGH UNCERTAINTY (the
honest range — see Decision 8).

---

## Problem

After S2.5/SP114 ships, KesselDB has:
- A complete MVCC primitive stack: versioned storage (SP110), Tx context
  + read-set (SP111), SI write-side conflict detection (SP112), SSI
  Cahill dangerous-structure detection (SP113), GC + dynamic watermark
  (SP114). **All five sub-slices DORMANT in production**: no SQL
  statement routes through Tx; no Op::CommitTx is submitted by the
  production SQL dispatcher; no Op::AdvanceWatermark is ever issued by
  any production caller.
- A `Tx::begin*` API that returns `Result<Self, TxError>` (SP114 Decision
  7 BREAKING) — **52 in-tree test call-sites use it; ZERO production
  callers**.
- An `Op::AdvanceWatermark` SM apply arm that is fully implemented and
  TLA+-proven against 1.59M states, but **never fires in production**
  (S2.5 Decision 2 deferred the heartbeat producer; the dormant SM arm
  is exercised only via test harnesses).
- A legacy SP1-SP113 20-byte object-keyspace data path that is
  STILL THE ONLY PRODUCTION DATA PATH for every SELECT / INSERT /
  UPDATE / DELETE — Op::Create's apply arm calls
  `self.storage.put(op_number, Key, record_bytes)` with a 20-byte
  primary key; Op::GetById reads with `self.storage.get(&Key)`; the
  visible-version semantics are "last write wins" without MVCC
  isolation. **The MVCC layer is a parallel universe, fully working
  but not consumed.**

What's still missing — and what S2.6 ships:

1. **SQL routing through MVCC.** Every `SELECT` becomes a Tx::begin
   (read-only) at `snapshot_opnum = current_commit_opnum`. Every
   `INSERT` / `UPDATE` / `DELETE` becomes an auto-commit Tx::begin_rw
   + Tx::write (or Tx::delete) + Tx::commit submitting an Op::CommitTx
   through VSR. **The SQL→MVCC integration is the slice's first
   headline.**

2. **SM cutover from legacy to MVCC.** Every Op::Create / Op::Update /
   Op::Delete / Op::GetById / Op::Select* / Op::Query* / Op::Aggregate*
   / Op::GroupAggregate / Op::Join / Op::UpdateSet / Op::QueryExpr apply
   arm REWRITES against the MVCC layer. The 20-byte legacy data-row
   keyspace is REMOVED from the SM apply path. Auxiliary keyspaces
   (indexes, constraints, catalog, sequencer counters, blobs) RETAIN
   the legacy 20-byte path per Decision 1's scope. **The legacy data
   path removal is the slice's second headline.**

3. **AdvanceWatermark heartbeat producer.** Per S2.5 Decision 2 honest
   disclosure: the heartbeat producer was deferred. S2.6 ships it in
   the VSR-adjacent layer (Decision 6 below): a background task on
   the VSR primary periodically gathers `min(active_snapshot_opnum)`
   from the SM's new `active_snapshots: BTreeSet<u64>` field, computes
   the new watermark target, and submits an `Op::AdvanceWatermark` op
   through the VSR pipeline. **The heartbeat is the slice's third
   headline — closes the S2.5 dormant-watermark gap.**

4. **`SQLAutoCommitSerializability` invariant.** A new TLA+
   artifact `MVCCCutover.tla` extends `MVCCGc` with `active_snapshots`
   state + `RegisterSnapshot(s)` / `UnregisterSnapshot(s)` /
   `HeartbeatTick` actions + new invariants:
   `ActiveSnapshotsBoundedByWatermark` (no active snapshot < watermark),
   `HeartbeatRespectsActiveSnapshots` (the heartbeat target ≤ min(active)),
   `SQLAutoCommitSerializability` (the auto-commit Tx sequence is
   serializable per SI/SSI), `LegacyKeyspaceEmpty` (after cutover, no
   20-byte data-row keys remain). **This is the seventh rigor-gate
   TLA+ module in the project** and the FIRST that mechanically encodes
   the dormant-to-production cutover correctness contract.

5. **A migration disclosure.** Pre-existing on-disk data using SP1-SP113
   20-byte object keys is UNREADABLE after S2.6 ships. Per the
   autonomous-mandate's BOLD-choice discipline, the repo has no
   production users so back-compat is not enforced; an offline
   conversion tool is documented as a hypothetical S2.X follow-up.
   **This is the slice's primary honest disclosure** (Decision 1 +
   Migration section below).

S2.6 is the slice that solves all five. **And it closes S2.**

---

## Decisions (bold choices, documented)

### Decision 1 — Cutover strategy: **full replace; SP1-SP113 20-byte data-row legacy keyspace REMOVED; auxiliary keyspaces (indexes/constraints/catalog/blobs/sequencer) RETAIN 20-byte legacy path**

Three structural options for the dormant-to-production cutover:

- **(a) Full replace.** Remove the SP1-SP113 20-byte legacy data-row
  keypath entirely. Every SM apply arm that touches data-rows REWRITES
  against MVCC. The cleanest end-state: 28-byte versioned keys are the
  only data-row keyspace; the determinism + replayability + verifiability
  pillars apply to every SQL statement; the thesis is uniformly
  enforced at the SQL surface.
- **(b) Feature-gate.** Legacy + MVCC data paths coexist behind a
  runtime config flag; default to MVCC for new installs, legacy for
  existing. Adds operational config-surface complexity; doubles the
  test matrix (every SM apply path tested both ways); the thesis is
  no longer uniformly enforced (legacy installs do not get
  serializability + GC + bounded storage).
- **(c) Compatibility layer.** Legacy 20-byte keys silently upgraded
  to 28-byte MVCC at first write; legacy reads succeed via fallback
  scan. The slowest cutover path; complicates the SM apply arms with
  two-path read fallback; preserves no-data-loss invariant for
  pre-existing installs.

**Taken: (a) — full replace.**

**Why bold over safe.** The repo has NO PRODUCTION USERS — the
autonomous-mandate's BOLD-choice discipline weights end-state purity
over backward-compatibility for an empty user base. Option (b)'s
feature-gate is the conventional cautious choice for a database with
real users; KesselDB has zero. Option (c)'s compatibility layer is
the gentlest but lowest-payoff path — every SM apply arm carries
two-path read logic forever, slowing every subsequent slice. Option
(a) gives the cleanest end-state, the highest test-quality, and the
most honest THESIS-alignment claim. **Migration path for any future
user: an offline conversion tool documented as S2.X (not built);
documented honestly in the Migration section below.**

**Scope refinement (the "1b").** The full-replace covers the
DATA-ROW keyspace only. Auxiliary 20-byte keyspaces — secondary
indexes (SP3 / SP15 / SP25-27 per-entry indexes), constraints
(SP4 UNIQUE / SP6 FK / SP7 CHECK metadata), catalog
(`catalog_key()`), sequencer counters (SP79 `seq_counter_key` /
`seq_entry_key`), blobs (SP2 overflow store) — RETAIN the legacy
20-byte keypath in S2.6. **The MVCC layer is for primary-key data
rows ONLY** (matches Decision 1 of S2.1 / SP110: "MVCC versioned
storage indexes by `(type_id, object_id, inverted_commit_opnum)`").
Indexes are derived structures, not transactional data; constraints
are catalog metadata; the catalog is structurally outside MVCC.
Promoting indexes / constraints / catalog to MVCC is a hypothetical
S3.X follow-up well outside S2's scope. **Honest disclosure:** an
index-as-MVCC slice is a real future enhancement (would yield
deterministic-bytes-on-disk for secondary indexes too); S2.6 does
not ship it; not on any roadmap.

**Thesis fit:** `deterministic` (every data-row apply is now an MVCC
op; replicas converge byte-identically); `verifiable` (the
`LegacyKeyspaceEmpty` TLA+ invariant mechanically encodes "no 20-byte
data-row keys remain after cutover"); `honest-docs` (the rejected
options (b)/(c) are documented as rejected, not silently revised; the
auxiliary-keyspace exception is named explicitly; the no-back-compat
migration disclosure is the dedicated Migration section).

### Decision 2 — SQL execution path: **per-statement auto-commit Tx for S2.6; SQL `BEGIN`/`COMMIT`/`ROLLBACK` grammar for S2.7 (NOT in this slice)**

Two structural options for SQL execution:

- **(a) Per-statement auto-commit.** Every SQL statement (SELECT /
  INSERT / UPDATE / DELETE) wraps itself in `Tx::begin*` / Tx::write* /
  `Tx::commit*` automatically. The auto-commit Tx exists for the
  duration of the single statement; no SQL-level transaction is
  exposed to the user. Simplest; matches every existing SP1-SP113
  SQL test's "one statement = one server-applied state change"
  invariant; ZERO SQL grammar churn.
- **(b) Per-transaction.** SQL `BEGIN` / `COMMIT` / `ROLLBACK` grammar
  opens an explicit Tx; statements within it use the same Tx. Requires
  SQL grammar work (new keywords, parser updates); requires
  server-side session state (the active Tx ID per connection);
  introduces multi-statement Tx semantics that interact with
  pipelining + connection-pool reuse. **Larger scope than S2.6 can
  honestly ship.**

**Taken: (a) per-statement auto-commit for S2.6 + (b) SQL BEGIN/COMMIT
grammar deferred to S2.7 (an out-of-S2 follow-up slice).**

**Why bold over safe.** Auto-commit is the natural starting point —
every PostgreSQL/CockroachDB/MySQL session defaults to auto-commit
mode for a reason. Adding explicit transactions on top later is
strictly additive (a server-side session-state map + new SQL
keywords); not adding them now keeps S2.6's scope sharp (cutover +
heartbeat). **The S2.7 forward-link is named.** A hypothetical
SQL transaction surface ships as `BEGIN [ISOLATION LEVEL {SI |
SSI}]` / `COMMIT` / `ROLLBACK` with the per-Tx isolation level
selecting between `Tx::commit` (SI) and `Tx::commit_ssi` (SSI). For
S2.6, every auto-commit Tx is SSI by default (Decision 7 picks
between).

**SQL surface contract for S2.6:**
```
SELECT ... → Tx::begin (read-only Shared borrow); Tx::read*; Tx::commit_read_only.
INSERT ... → Tx::begin_rw (Exclusive borrow); Tx::write*; Tx::commit_ssi → Op::CommitTx.
UPDATE ... → server-side GetById via Tx::read; mutate; Tx::write; Tx::commit_ssi → Op::CommitTx.
DELETE ... → Tx::begin_rw; Tx::write (tombstone, value = None); Tx::commit_ssi → Op::CommitTx.
```

The auto-commit Tx wraps every statement at the
`crates/kesseldb-server/src/lib.rs::apply_one` seam (Decision 6 +
architecture below) — the single source of truth for "what one request
does" that the server uses uniformly across the normal path and
pipelined batches. **No SQL syntax changes in S2.6.**

**Thesis fit:** `deterministic` (every auto-commit Tx routes through
VSR's totally-ordered log; replicas reach byte-identical post-statement
state); `honest-docs` (the deferred SQL transaction grammar is named
as S2.7; the per-statement-auto-commit-vs-multi-statement-Tx tradeoff
is documented).

### Decision 3 — Snapshot opnum source for auto-commit Tx: **`current_commit_opnum` from SM (READ COMMITTED semantics)**

Three options:

- **(a) `current_commit_opnum`.** Snapshot at the latest committed point.
  Matches PostgreSQL READ COMMITTED isolation — every statement sees
  every prior committed write. The simplest correct semantics.
- **(b) `current_commit_opnum - 1`.** Snapshot from immediately before
  any in-flight commit. Avoids a class of "I just inserted but my
  SELECT doesn't see it" anomalies for single-statement auto-commit
  (where the INSERT's commit_opnum is N and a follow-up SELECT at
  snapshot N would see it under (a) but not (b)). Semantically wrong
  for auto-commit (a commit's effects MUST be visible to the next
  statement's snapshot).
- **(c) `low_water_mark`.** Oldest serveable snapshot. Trivially loses
  every committed write since the last watermark advance.
  Correctness-violating.

**Taken: (a) — `current_commit_opnum` from SM.**

**Why bold over safe.** PostgreSQL READ COMMITTED is the
industry-standard default; matching it is the right choice. **For
auto-commit Tx specifically, the snapshot must be ≥ the most recently
committed prior statement's commit_opnum** — otherwise a `INSERT;
SELECT` sequence in two auto-commit Tx would fail to read the just-
inserted row. Option (a) gives this for free.

**Implementation seam.** The SM gains a new accessor:
```rust
impl<V: Vfs> StateMachine<V> {
    /// SP115 / S2.6: The latest committed op_number. Auto-commit Tx
    /// reads this at Tx::begin to pin its snapshot at READ COMMITTED.
    /// Deterministic: equal across replicas at the same log prefix.
    pub fn current_commit_opnum(&self) -> u64;
}
```

The accessor returns whatever the SM's authoritative tracker is
(verify at impl-time — likely `self.commit_opnum` or
`self.storage.high_op()`). The server's `apply_one` reads it before
constructing each auto-commit Tx and passes it as `snapshot_opnum`.

**Thesis fit:** `deterministic` (the snapshot is a pure function of
the log prefix — every replica's `current_commit_opnum` is byte-
identical at the same log position); `honest-docs` (the rejected
options (b) / (c) are documented; the PostgreSQL READ COMMITTED
analogy is named).

### Decision 4 — commit_opnum source for auto-commit Tx::commit: **VSR-log-position-assigned (SM apply overwrites)**

When an auto-commit Tx commits, it submits `Op::CommitTx { commit_opnum,
... }` through VSR. What value goes in `commit_opnum`?

Two options:

- **(a) VSR-log-position-assigned.** The SQL layer submits
  `Op::CommitTx { commit_opnum: 0, ... }` as a PLACEHOLDER; SM apply
  overwrites with `op_number` (the log position) at apply time. The
  commit_opnum is the log's totally-ordered sequence number by
  construction; no pre-allocation needed.
- **(b) Pre-allocated.** The SQL layer queries the SM for the next
  op_number and submits `Op::CommitTx { commit_opnum: N, ... }`.
  Requires a non-deterministic SQL-layer ↔ SM round-trip BEFORE the
  Op enters VSR; introduces a race window where the pre-allocated N
  could be different from the actual log position if another op
  interleaves.

**Taken: (a) — VSR-log-position-assigned.**

**Why bold over safe.** Option (a) lets VSR be the single source of
truth for op ordering. The SM apply path overwrites
`commit_opnum = op_number` deterministically; every replica reaches
byte-identical state because they all observe the same `op_number` for
each entry. **This is a SEMANTIC change to the SP112 Op::CommitTx
contract** — see Decision 5 for the back-compat behavior. The bold
choice is to make the SQL path the production-correct path and
preserve test back-compat via soft acceptance.

**Implementation:** SM apply arm for `Op::CommitTx` reads
`commit_opnum` from the op payload; if `0`, overrides with `op_number`
(the log position); if non-zero, uses as-is (back-compat with SP112
test code that passes explicit values — Decision 5). The conflict
check then uses the (possibly overridden) commit_opnum.

**Thesis fit:** `deterministic` (the commit_opnum is a pure function
of the log position; no SQL-layer source of non-determinism); `honest-
docs` (the semantic change is named; the back-compat path is
documented in Decision 5).

### Decision 5 — Op::CommitTx semantic evolution: **soft acceptance — `commit_opnum=0` means "let SM assign from log position"; non-zero used as-is**

Per Decision 4: SM apply now uses the log-position `op_number` as
`commit_opnum` when the Op payload's `commit_opnum` is `0`. Two
options for the back-compat behavior:

- **(a) Hard cutover.** SM apply asserts `commit_opnum == 0` in every
  Op::CommitTx; errors if non-zero. All callers must use 0. **Breaks
  every SP112 / SP113 / SP114 KAT that passes explicit commit_opnum
  values** — including the SP112 conflict-check correctness suite
  (`apply_commit_tx_*` tests in `crates/kessel-sm/src/lib.rs`).
- **(b) Soft acceptance.** SM apply checks `commit_opnum`: if `0`,
  overrides with `op_number`; if non-zero, uses as-is. Production
  (SQL) passes `0`; test code can still pass explicit values for
  surgical SM-apply behavior assertions. **No SP112-SP114 KAT churn.**

**Taken: (b) — soft acceptance.**

**Why bold over safe.** Option (a) maximizes purity but breaks ~30
SP112-SP114 KATs that exercise the conflict-check path with explicit
commit_opnum values. **Those KATs are still valuable** — they test
the conflict-check logic, not the log-position-assignment logic; the
two concerns are orthogonal and should remain testable separately.
Option (b) preserves the testability of the conflict-check primitive
while enabling the production (SQL) caller to pass `0` and let the SM
assign. **The soft-acceptance is documented explicitly** so future
slice authors don't accidentally remove the override behavior.

**Pin: 0 is reserved as the "auto-assign" sentinel.** Every existing
SP112-SP114 KAT that uses explicit `commit_opnum` values uses values
≥ 1 (verified during T0 baseline). The reservation does not collide
with any in-tree caller. **Production callers MUST pass `0`** — the
SQL layer's auto-commit always does (Decision 4); explicit production
callers (none in S2.6; hypothetical future) MUST follow the same
contract.

**Thesis fit:** `deterministic` (whether explicit or auto-assigned,
the resulting commit_opnum is a deterministic function of the op or
the log prefix); `honest-docs` (the reserved-zero contract is named;
the rejected hard-cutover is documented).

### Decision 6 — AdvanceWatermark heartbeat producer: **background task in VSR-adjacent layer; configurable interval (default 1s); per-replica local `active_snapshots: BTreeSet<u64>` tracking; primary aggregates + submits AdvanceWatermark**

Three structural options for the heartbeat producer:

- **(a) Background task in VSR (or SM-adjacent) layer.** A timer-driven
  background task on the VSR primary periodically computes the new
  watermark target and submits an `Op::AdvanceWatermark` op via VSR.
  The op flows through VSR like any other; SM apply runs the
  deterministic GC. Configurable interval (default 1s); production-
  correctness path; bounded memory by construction.
- **(b) Per-commit piggyback.** Every N-th `Op::CommitTx` also
  implicitly advances watermark to `current_commit - LAG`. Eliminates
  the timer; couples commit cadence to GC cadence; rough on
  long-running readers (LAG must be configured loosely enough to not
  preempt them).
- **(c) Explicit operator-triggered.** `AdvanceWatermark` is an
  operator-submitted CLI/API call. Pushes the operational burden onto
  the deployer; storage grows unboundedly between manual triggers.
  Not production-acceptable.

**Taken: (a) — background task in VSR-adjacent layer.**

**Why bold over safe.** Option (a) is the only choice that gives
bounded memory by construction WITHOUT coupling GC cadence to commit
cadence (which would degrade in low-write workloads). Option (b)'s
piggyback is a clever optimization that has the right shape for
high-write workloads but the wrong shape for low-write (GC stops
entirely; storage from rare commits sticks around). Option (c) is the
operator-burden path — unacceptable for a "deterministic kernel"
thesis claim.

**Active-snapshots tracking.** Per Decision 7: each replica maintains
a local `active_snapshots: BTreeSet<u64>` (NEW field on SM) — `Tx::begin*`
registers `snapshot_opnum`; `Tx::commit*` / `Tx::abort` /
`Tx::commit_read_only` unregisters. The heartbeat producer reads
`active_snapshots.iter().next().copied().unwrap_or(self.commit_opnum)`
to get the local min (or `commit_opnum` as a high-water sentinel when
no Tx is active — letting the watermark advance to "everything is
free"). The primary submits the local min as the proposed watermark;
SM apply validates strictly (per S2.5 Decision 5) and accepts or
rejects.

**The heartbeat interval is configurable.** Default 1 second. Tunable
per deployment via a config knob on the VSR-adjacent layer (likely
`kesseldb-server` config — verify at impl-time). The heartbeat's
non-determinism (each replica's clock fires at slightly different
times) is contained at the SUBMISSION boundary: only the primary
submits; the apply path is deterministic.

**Multi-replica honest disclosure.** Per Decision 7's "active_snapshots
is per-replica local" treatment: each replica tracks ONLY its own
clients' active Tx. The primary's local view of `min(active_snapshots)`
is the LOCAL primary's view; it does NOT account for other replicas'
local active Tx. **This is acceptable for S2.6 single-replica-scope of
active-snapshots** because: (1) the SP1-SP113 production model is a
3-replica VSR cluster where every client routes through the primary
(per `kesseldb-server`'s connection-routing); (2) replicas other than
the primary don't serve client reads in the SP1-SP113 model. **A
multi-replica heartbeat with consensus on global min(active) is
deferred to S2.X** — would require a new VSR op or piggyback on
existing ops to aggregate per-replica active_snapshots. This honest-
disclosure is documented in the Deferred Set below.

**Thesis fit:** `deterministic` (the SM apply remains deterministic;
the heartbeat's non-determinism is contained at the submission
boundary); `honest-docs` (the multi-replica deferral is named; the
configurable-interval is named; the local-primary assumption is named).

### Decision 7 — active-snapshots tracking: **NEW SM field `active_snapshots: BTreeSet<u64>`; per-replica local (NOT replicated); registered at Tx::begin / unregistered at Tx::commit / abort**

Two design questions:

**(Q1) Where does the active_snapshots set live?** Two options:

- **(a) Replicated SM state.** Every replica's SM has the same
  `active_snapshots` — every Tx-begin op flows through VSR; every
  Tx-end op flows through VSR. **Expensive**: every Tx-begin / Tx-end
  is now a replicated op; the VSR pipeline carries Tx-lifecycle metadata.
- **(b) Per-replica local.** Each replica tracks ONLY its own clients'
  active Tx. The heartbeat producer on the primary reads its local
  view. **Cheap**: Tx-lifecycle is local; only the AdvanceWatermark
  payload flows through VSR.

**Taken: (b) per-replica local for S2.6 performance.**

**Why bold over safe.** Option (a)'s replicated tracking would
double the VSR traffic for every Tx (now Tx-begin and Tx-end are ops,
not just Tx-commit); the perf cost is structural. Option (b)
accepts that the primary's local view of active_snapshots is
approximate (only-its-own-clients) and accepts the multi-replica
gap as documented in Decision 6's deferral. **For the SP1-SP113
production model (clients route through primary; replicas don't
serve reads) the gap is empty**; the moment KesselDB adds read
replicas (post-S2), the gap surfaces and S2.X must close it.

**(Q2) When does registration happen?** Auto-commit Tx::begin
registers `snapshot_opnum` into `active_snapshots`; Tx::commit /
abort / commit_read_only unregisters. The registration is in the
Tx constructor; the unregistration is in the Tx destructor (or
explicit terminal method). **The set is a multiset semantically** —
multiple concurrent Tx may hold the same snapshot_opnum; the set
must accept duplicates OR use a `BTreeMap<u64, u32>` (count) to
avoid losing snapshots when one Tx commits and another at the same
snapshot remains live.

**Taken: `BTreeMap<u64, usize /* count */>` — count-keyed multiset.**

**Why bold over safe.** A `BTreeSet<u64>` cannot distinguish "one Tx
at snapshot 5 committed; another Tx at snapshot 5 still active" from
"the only Tx at snapshot 5 committed". The count-keyed multiset is
the correct shape; insertion is `*entry.or_insert(0) += 1`; removal
is `if entry decremented to 0, remove(key)`. The heartbeat reads
`map.keys().next().copied()`. **Implementation seam:** new field
`active_snapshots: BTreeMap<u64, usize>` on SM; new methods
`register_snapshot(u64)`, `unregister_snapshot(u64)`,
`min_active_snapshot() -> Option<u64>`. Mirrors SP113's `pending_txs`
BTreeMap shape for deterministic iteration.

**Tx::begin → SM:** the Tx layer can't directly access SM (kessel-
storage cannot depend on kessel-sm). The wiring is done at the
server-side `apply_one` seam: it constructs the Tx, registers the
snapshot via the SM accessor, then later unregisters on Tx::commit /
abort. **The standalone Tx form** (test-only; no SM context) doesn't
register — same way SP113's standalone Tx doesn't populate
`pending_txs`. Per-replica local Mexican-standoff is acceptable
(documented as the Decision 7 gap).

**Thesis fit:** `deterministic` (the active_snapshots is local
per-replica but the AdvanceWatermark op that flows through VSR is
deterministic at apply); `honest-docs` (the per-replica gap +
multi-replica deferral are named; the count-keyed multiset is named
as the correct shape vs the naïve BTreeSet).

### Decision 8 — Cargo gate impact + dormant-code removal: **HONEST RANGE +20 to +50; legacy keypath test deletes are large; SQL+MVCC integration tests add; pentest hardens**

Two-dimensional accounting:

- **Legacy keypath SM-apply test deletes/migrations.** Per Decision 1:
  the 20-byte data-row keypath is REMOVED. Existing SM tests that
  assert on legacy 20-byte storage state (~50-80 tests across
  SP3-SP100 era — covers `apply_*` tests touching `Op::Create / Op::Update
  / Op::Delete / Op::GetById / Op::Select / Op::QueryRows`) either:
  - **Migrate** to MVCC-keyspace assertions (most: the test's INTENT
    is "Create produces a readable row" — still true through MVCC,
    just with 28-byte versioned keys instead).
  - **Delete** as obsolete (tests asserting bytes-on-disk against
    20-byte key formats — those assertions no longer apply).
- **NEW SQL+MVCC integration tests.** T3: 6 integration tests
  (3-replica byte-identity for SQL workloads; auto-commit-statement
  end-to-end; heartbeat-advances-watermark; legacy-keyspace-empty
  assertion; SQL+SSI integration; mixed-read-write SQL). T4: 6
  coverage tests (per-statement Tx lifecycle; auto-commit rollback;
  heartbeat-respects-active-snapshots; large SQL batches; SQL+SSI
  integration). T5: 6 pentest tests (malformed SQL hostile input;
  watermark advancement under load; race-shaped active-snapshots
  churn; SQL injection against MVCC; heartbeat-during-in-flight-commit;
  legacy-keypath-resurrection-attempt). T2: 11 hand-derived KATs
  (active-snapshots / heartbeat / SQL→MVCC routing / cutover
  byte-identity / Decision-5 soft acceptance).
- **Net cargo gate delta: +20 to +50 tests, HONEST RANGE.** The
  uncertainty range is the WIDEST of any S2 slice because the legacy-
  keypath test count is not known precisely until T2 audit; some tests
  migrate (no delete), some delete entirely (no replacement), some
  spawn replacements (1→N). **T0 baseline records the actual SP114
  count (expected 640); T2 records the post-migration count; T6
  records the final.**

**The honest range is the slice's primary disclosure.** Per the
strategic-tier discipline (SP107 V1-defect / SP108 plan-arithmetic /
SP114 +30-actual): every gate accounting is reported as a real
measured number after the fact. The +20 to +50 estimate is the design
band; the actual is recorded in T6.

**Cargo dependency snapshot.** `cargo tree -p kesseldb-server | grep
-Ei "parquet|objstore|rustls|webpki"` MUST remain byte-identical to
the SP114 baseline. **Zero new external dependencies in S2.6** (uses
only `std::collections::BTreeMap` + existing crates).

**Thesis fit:** `honest-docs` (the wide gate range is named, not
hidden; the migration-vs-delete tradeoff per test is explicit; the
T6-records-actual discipline is named).

### Decision 9 — TLA+ verification: **`MVCCCutover.tla` extends `MVCCGc` with `active_snapshots` state + heartbeat actions + 4 new invariants including `LegacyKeyspaceEmpty` + `SQLAutoCommitSerializability`**

Per the parent design Decision 7 + SP110-SP114 discipline, S2.6 ships
a TLA+ extension. The spec EXTENDS `MVCCGc` (the SP114 spec) so the
cutover layer is checked over the same versioned-storage + Tx + SI +
SSI + GC model TLC has already verified.

**File:** `kesseldb-tla/MVCCCutover.tla` — `EXTENDS MVCCGc`.

**State variable additions:**
- `activeSnapshots` — a TLA+ multiset (modeled as a function `TxIds ->
  Nat \cup {NONE}`) mapping each Tx to its active snapshot (or NONE if
  not started / already committed/aborted).
- `legacyKeyspaceSize` — a TLA+ Nat that tracks the size of the
  hypothetical 20-byte legacy keyspace; initialized to 0; never
  incremented by any S2.6 action (the cutover removed legacy writes).
  The invariant LegacyKeyspaceEmpty asserts it stays 0.

**Actions (additions over MVCCGc's BeginSi/CommitSi/AdvanceWatermark):**
- `RegisterSnapshot(t, s)` — at Tx-begin, registers `t |-> s` in
  activeSnapshots.
- `UnregisterSnapshot(t)` — at Tx-end (commit/abort), removes `t |-> _`
  from activeSnapshots.
- `HeartbeatTick(W)` — submits an `AdvanceWatermark(W)` op where
  `W <= min({activeSnapshots[t] : t \in DOMAIN activeSnapshots} \cup
  {opCount})`. Acceptance is delegated to the existing AdvanceWatermark
  action.
- `SqlAutoCommitTx(t, s, ops)` — composite action: RegisterSnapshot(t, s)
  → some sequence of CommitTx ops → UnregisterSnapshot(t). Models the
  end-to-end auto-commit Tx lifecycle at the SQL surface.

**Invariants (the verifiable claims):**
- All 22 MVCCGc invariants preserved.
- **TypeOKCutover** — well-typed cutover state-space (extends `TypeOKGc`
  with activeSnapshots typing + legacyKeyspaceSize: Nat).
- **ActiveSnapshotsBoundedByWatermark** — every active snapshot is
  >= lowWaterMark. `\A t \in DOMAIN activeSnapshots: activeSnapshots[t]
  >= lowWaterMark`. This is the operational mechanism that makes
  long-running Tx safe under GC.
- **HeartbeatRespectsActiveSnapshots** — every HeartbeatTick(W) action
  has W <= min(activeSnapshots) (or W <= opCount if empty). Stated as
  an action precondition.
- **SQLAutoCommitSerializability** — every sequence of SqlAutoCommitTx
  actions produces a schedule that is serializable per the SI/SSI
  contract (delegated to MVCCSsi's SerializableEquivalence). Mechanically
  this is "the auto-commit lifecycle action is structurally equivalent
  to BeginSi → CommitSi at the per-Tx level".
- **LegacyKeyspaceEmpty** — `legacyKeyspaceSize = 0` always. Encodes
  "after S2.6 cutover, no 20-byte data-row keys are produced by any
  action".

**Bounded model (initial `.cfg`):**

```
SPECIFICATION Spec

CONSTANTS
    Keys      = {k1, k2}
    Values    = {v1, v2}
    MaxOpnum  = 4
    MaxOps    = 5
    TxIds     = {t1, t2}
    MaxTxOps  = 4
    MaxTxAge  = 5
    MaxWatermark = 4
    HeartbeatInterval = 2     \* fire every 2 ops

INVARIANT
    TypeOK
    SnapshotImmutability
    ReadSetMonotonic
    ReadSetCoversAllReads
    ReadAtSnapshot
    TxStatusMonotonic
    WriteSetMonotonic
    WriteWriteConflictDetected
    CommitAtomicity
    FirstCommitterWins
    DeterministicApply
    TypeOKSsi
    PendingTxsWindowBounded
    DangerousStructureAborts
    NoWriteSkew
    SerializableEquivalence
    TypeOKGc
    WatermarkMonotonic
    NoVersionBelowWatermark
    NoPendingTxBelowWatermark
    SnapshotAvailability
    BoundedWindowSupersededByWatermark
    TypeOKCutover
    ActiveSnapshotsBoundedByWatermark
    HeartbeatRespectsActiveSnapshots
    SQLAutoCommitSerializability
    LegacyKeyspaceEmpty

CHECK_DEADLOCK FALSE
```

**Coverage target.** Per the SP110-SP114 precedent, target complete
coverage of the bounded model with ZERO invariant violations. The
state space is larger than MVCCGc's (the additional activeSnapshots
function + the HeartbeatTick action multiplies the action-space).
The bounded constants are tightened to keep TLC tractable. **A 2-Tx
model IS sufficient for the SQLAutoCommitSerializability + LegacyKeyspace
Empty + heartbeat-respect invariants** (the auto-commit lifecycle is
single-statement per Tx by Decision 2; multi-Tx coverage is for the
SI/SSI substrate which MVCCSsi already verified).

**Honest disclosure.** The bounded model verifies the cutover
invariants on the abstract MVCCCutover spec — not the Rust code
itself. The named-action correspondence (action-mapping table in the
spec head) is the manually-maintained bridge. The Rust integration
tests (T3) gate the byte-identity claim across 3 replicas for SQL
workloads AND prove the LegacyKeyspaceEmpty contract at the Rust
level (an assertion that walks the storage's full keyspace and
verifies no 20-byte data-row keys remain). The
SQLAutoCommitSerializability is gated at both the TLA+ level
(MVCCCutover invariant) and the Rust level (T3 / T4 SQL+SSI tests
delegating to SP113's SSI contracts).

**Thesis fit:** `verifiable` (extends SP114's TLA+ rigor to the cutover
layer; ActiveSnapshotsBoundedByWatermark + HeartbeatRespectsActiveSnapshots
+ SQLAutoCommitSerializability + LegacyKeyspaceEmpty are mechanically-
checked cutover-correctness claims; the **seventh** rigor-gate TLA+
module in the project; the **FIRST** that formally encodes the
dormant-to-production cutover correctness contract; the **FIRST** that
introduces a per-slice "structural absence" invariant
(LegacyKeyspaceEmpty)); `honest-docs` (the 2-Tx bound + per-replica-
local heartbeat + spec-vs-Rust correspondence caveats are all
disclosed).

### Decision 10 — Slice numbering: **SP115** (the slice immediately after SP114)

SP115 in the subproject numbering. The S2.6 plan/spec filenames use
the `2026-05-24` date prefix. The internal record (T6 will create it)
is:

- Spec/design: this file —
  `docs/superpowers/specs/2026-05-24-mvcc-si-s2-6-design.md`.
- Plan: companion file —
  `docs/superpowers/plans/2026-05-24-mvcc-si-s2-6.md`.
- Slice closeout record (T6 will create it):
  `docs/superpowers/specs/2026-05-24-kesseldb-subproject115-mvcc-cutover-s2-6.md`.

Subproject-number / S2-sub-slice cross-reference table after S2.6:

| Subproject | S2 sub-slice | Status | Headline |
|---|---|---|---|
| SP110 | S2.1 | done | MVCC versioned-storage primitive |
| SP111 | S2.2 | done | Tx context + read-set tracking |
| SP112 | S2.3 | done | SI write-side + conflict detection at SM apply time |
| SP113 | S2.4 | done | SSI promotion via Cahill dangerous-cycle detection |
| SP114 | S2.5 | done | GC + dynamic watermark (supersedes SP113 bounded window) |
| **SP115** | **S2.6** | **this slice** | **SQL integration + SM cutover + AdvanceWatermark heartbeat (CLOSES S2)** |

**Thesis fit:** `honest-docs` (the slice numbering + cross-reference
makes the strategic-tier trajectory inspectable from any single record;
SP115 is the S2-CLOSING slice; the S3 forward-link (Jepsen) is named
in Strategic-Tier Context Update; the SP114-relationship is the GC
substrate the heartbeat finally activates).

---

## Architecture

### High-level layering after S2.6

```
                  +---------------------------+
                  |  kessel-sql (UNCHANGED    |
                  |  in S2.6 — no grammar     |
                  |  churn; auto-commit Tx    |
                  |  is server-side, not      |
                  |  parser-side)             |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kesseldb-server          |   <-- S2.6 (this slice)
                  |  ::apply_one              |       auto-commit Tx WRAPPER
                  |  (the production seam)    |       SELECT → Tx::begin (read-only)
                  |                           |       INSERT/UPDATE/DELETE →
                  |  + heartbeat producer     |         Tx::begin_rw +
                  |  (background task; per-   |         Op::CommitTx(commit_opnum=0)
                  |  primary; ~1s interval)   |       heartbeat ticks here
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-sm                |   <-- S2.6 (this slice)
                  |  + active_snapshots:      |       per-replica local
                  |    BTreeMap<u64, usize>   |       count-keyed multiset
                  |  + register_snapshot /    |
                  |    unregister_snapshot /  |
                  |    min_active_snapshot    |
                  |  + Op::CommitTx soft      |       commit_opnum=0 →
                  |    accept (Decision 5)    |       op_number; else as-is
                  |  + current_commit_opnum   |       accessor for snapshot
                  |    accessor               |
                  |  + DATA-ROW APPLY ARMS    |       Op::Create/Update/Delete/
                  |    REWRITTEN against MVCC |       GetById/Select*/Query* via
                  |    layer (was 20-byte     |       mvcc::put_versioned /
                  |    Storage::put; now      |       get_at_snapshot
                  |    mvcc::put_versioned)   |
                  |  SP1-SP114 paths for      |
                  |  catalog/indexes/blobs    |
                  |  UNCHANGED                |
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage::tx       |   (UNCHANGED in S2.6;
                  |  SP114 surface used by    |    SP114 already shipped
                  |  every auto-commit Tx)    |    the full Tx API)
                  +---------------------------+
                              |
                              v
                  +---------------------------+
                  |  kessel-storage::mvcc     |   (UNCHANGED in S2.6;
                  |  SP110 surface + SP114    |    SP110 + SP114 already
                  |  GC primitive; the slice  |    shipped everything; S2.6
                  |  CONSUMES not extends     |    just consumes)
                  +---------------------------+

                  +---------------------------+
                  |  kessel-vsr (UNCHANGED;   |
                  |  the heartbeat producer   |
                  |  lives ADJACENT to VSR    |
                  |  in kesseldb-server, not  |
                  |  inside VSR)              |
                  +---------------------------+
```

The cutover seam is THREE seams:

1. **`kesseldb-server::apply_one`** — wraps every SQL statement in
   auto-commit Tx; submits `Op::CommitTx` via VSR; manages session-
   visible commit_opnum return.
2. **`kessel-sm::StateMachine::apply`** — rewrites every data-row Op
   apply arm against MVCC; adds `active_snapshots` tracking via
   `register_snapshot` / `unregister_snapshot`; preserves SP114
   `Op::AdvanceWatermark` arm.
3. **`kesseldb-server` heartbeat task** — background task on the VSR
   primary; periodically gathers `min_active_snapshot()` from SM and
   submits `Op::AdvanceWatermark` via VSR.

### Module changes (S2.6 deltas only)

- `crates/kessel-sm/src/lib.rs` — ADD `active_snapshots: BTreeMap<u64,
  usize>` field to `StateMachine<V>`; ADD `register_snapshot(u64)`,
  `unregister_snapshot(u64)`, `min_active_snapshot() -> Option<u64>`,
  `current_commit_opnum() -> u64` methods; CHANGE `Op::CommitTx` apply
  arm — at top: `let effective_commit_opnum = if commit_opnum == 0 {
  op_number } else { commit_opnum };` and use `effective_commit_opnum`
  throughout; REWRITE `Op::Create` / `Op::Update` / `Op::Delete` /
  `Op::GetById` / `Op::Select*` / `Op::Query*` / `Op::Aggregate*` /
  `Op::GroupAggregate` / `Op::Join` / `Op::UpdateSet` / `Op::QueryExpr`
  apply arms to use `mvcc::put_versioned` / `mvcc::get_at_snapshot` /
  `mvcc::has_version_in_range` / `mvcc::scan_at_snapshot` (the LAST
  is a new helper in mvcc.rs — see below) instead of `Storage::put` /
  `Storage::get` / `Storage::scan_range`; PRESERVE catalog / index /
  blob / sequencer apply arms (Decision 1 scope).
- `crates/kessel-storage/src/mvcc.rs` — ADD `pub fn scan_at_snapshot<V:
  Vfs>(store: &Storage<V>, type_id: u32, snapshot_opnum: u64) ->
  Vec<(ObjectId, Vec<u8>)>` — full-type scan returning the latest
  version <= snapshot_opnum per object_id. Used by Op::Select / Op::Query*
  rewrite. SP110-SP114 surface UNCHANGED.
- `crates/kesseldb-server/src/lib.rs` — REWRITE `apply_one` to wrap
  every SQL statement in an auto-commit Tx (per Decision 2 contract).
  ADD heartbeat producer background task — spawned at server startup;
  ticks at configurable interval (default 1s); reads
  `sm.min_active_snapshot()` and submits `Op::AdvanceWatermark` via
  VSR.
- `crates/kessel-sm/src/lib.rs` test module — MIGRATE / DELETE the
  ~50-80 SP3-SP100 era SM-apply tests that assert on 20-byte legacy
  keypath state. Per Decision 8 disclosure: some migrate (intent-
  preserving), some delete (byte-level legacy asserts no longer apply).
- New file (TLA+): `kesseldb-tla/MVCCCutover.tla`,
  `kesseldb-tla/MVCCCutover.cfg`,
  `kesseldb-tla/results/2026-05-24-mvcc-cutover-baseline.txt`.
- New file (slice record): `docs/superpowers/specs/2026-05-24-kesseldb-subproject115-mvcc-cutover-s2-6.md` (T6).
- New test files: `crates/kessel-sm/tests/integration_mvcc_cutover.rs`,
  `crates/kessel-sm/tests/pentest_mvcc_cutover.rs`,
  `crates/kesseldb-server/tests/integration_sql_mvcc.rs`,
  `crates/kesseldb-server/tests/integration_heartbeat.rs`.

**No new crates.** All changes are in 3 existing crates + the TLA+
substrate.

### Internal data shape

**`StateMachine<V: Vfs>` (S2.6 expansion).** Gains ONE new field
`active_snapshots: BTreeMap<u64, usize>` (initial empty). All
SP1-SP114 fields unchanged.

**`Op::CommitTx` semantic** (S2.6 evolution). The `commit_opnum: u64`
field's semantics change from "must be the committer's pre-allocated
opnum" to "0 means auto-assign from log position; non-zero used
as-is". Wire format UNCHANGED. SP112-SP114 KATs continue to pass
unchanged (they pass non-zero values which are used as-is per
Decision 5 soft acceptance).

**Auto-commit Tx lifecycle (the production data flow):**

```
client                kesseldb-server                kessel-sm                VSR
  |                          |                          |                       |
  | SQL frame "INSERT ..."   |                          |                       |
  |------------------------->|                          |                       |
  |                          | sm.current_commit_opnum()|                       |
  |                          |------------------------->|                       |
  |                          |<--- S (e.g., 42) --------|                       |
  |                          | sm.register_snapshot(42) |                       |
  |                          |------------------------->|                       |
  |                          | Tx::begin_rw(&mut store, 42)                     |
  |                          | Tx::write(...)           |                       |
  |                          | Tx::commit_ssi() → Op::CommitTx{                 |
  |                          |   snapshot_opnum: 42,    |                       |
  |                          |   write_set: [...],      |                       |
  |                          |   commit_opnum: 0,  <-- placeholder              |
  |                          |   read_set: [...]        |                       |
  |                          | }                        |                       |
  |                          | vsr.submit(Op::CommitTx) |---------------------->|
  |                          |                          |                       | VSR consensus
  |                          |                          |<----------------------|
  |                          |                          | apply(N, Op::CommitTx)|
  |                          |                          |   effective_commit_opnum = N
  |                          |                          |   conflict check vs N |
  |                          |                          |   put_versioned at N  |
  |                          |                          |   pending_txs.insert(N, ...)
  |                          |                          | → OpResult::TxCommitted{commit_opnum: N}
  |                          |<---  N --                |                       |
  |                          | sm.unregister_snapshot(42)                       |
  |                          |------------------------->|                       |
  |<--- OK "inserted, commit_opnum=N" ----              |                       |
  
  
Heartbeat (every ~1s, primary only):
  
heartbeat task          kesseldb-server                kessel-sm                VSR
  |                          |                          |                       |
  |        tick              |                          |                       |
  |------------------------->|                          |                       |
  |                          | sm.min_active_snapshot() |                       |
  |                          |------------------------->|                       |
  |                          |<--- Some(W) or None -----|                       |
  |                          | target = W.unwrap_or(sm.current_commit_opnum())  |
  |                          | vsr.submit(Op::AdvanceWatermark{                 |
  |                          |   low_water_mark: target |---------------------->|
  |                          | })                       |                       | VSR consensus
  |                          |                          |<----------------------|
  |                          |                          | apply(N, Op::AdvanceWatermark)
  |                          |                          |   validate strict     |
  |                          |                          |   delete_versions_older_than
  |                          |                          |   prune_pending_txs_by_watermark
  |                          |                          | → OpResult::WatermarkAdvanced
```

### Call graph (S2.6 additions)

```
caller (SQL client over TCP, sending an INSERT)
   |
   | server::apply_one(frame: [0xFE] ++ "INSERT ...")
   |
   v
apply_one
   |
   |--+ Compile to Stmt via CompileCache
   |
   |--+ snapshot = sm.current_commit_opnum()
   |--+ sm.register_snapshot(snapshot)
   |--+ Tx::begin_rw(&mut store, snapshot)?
   |--+ Tx::write(type_id, object_id, payload)?
   |--+ commit_tx = Tx::commit_ssi() → Op::CommitTx { commit_opnum: 0, ... }
   |--+ result = vsr.submit(commit_tx).await
   |     → SM::apply(N, Op::CommitTx {commit_opnum: 0, ...}) at log position N
   |        → effective_commit_opnum = N (Decision 5 soft accept)
   |        → SI conflict check against versions in (snapshot, N-1]
   |        → mvcc::put_versioned(store, N, key, value)
   |        → ssi::detect_dangerous_structure(...) if read_set non-empty
   |        → pending_txs.insert(N, PendingTxRecord{...})
   |        → ssi::prune_pending_txs(MAX_TX_AGE) (SP113 fallback retained)
   |        → OpResult::TxCommitted { commit_opnum: N } or TxAborted
   |--+ sm.unregister_snapshot(snapshot)
   |--+ return result to client


// Heartbeat task (separate background task on primary):
heartbeat_loop()
   |
   | every interval (default 1s):
   |
   v
   |--+ target = sm.min_active_snapshot().unwrap_or(sm.current_commit_opnum())
   |--+ if target > sm.low_water_mark():
   |     | vsr.submit(Op::AdvanceWatermark { low_water_mark: target })
   |--+ sleep(interval)
```

**Per-step determinism.** Every step above is deterministic at apply.
The auto-commit lifecycle has two NON-deterministic surfaces — both
contained at the SUBMISSION boundary:

1. **`current_commit_opnum`** read by the server before Tx::begin is
   non-deterministic at the SUBMISSION layer (each replica's view at
   wall-clock-T may differ by a few ops); but every replica reaches
   the same `op_number` for the same VSR-totally-ordered op. Apply
   determinism is preserved.
2. **`heartbeat target`** is computed from the local `active_snapshots`
   which is per-replica; the primary's view is the authoritative one
   for submission. Apply of the resulting `Op::AdvanceWatermark` is
   deterministic.

**The thesis-fit determinism contract holds at the apply layer**, even
though the submission layer has wall-clock and per-replica-local
non-determinism. This separation is intentional per S2.5 Decision 2.

### MVCCCutover.tla extension (the verifiable artifact)

Per Decision 9. See `kesseldb-tla/MVCCCutover.tla` after T6 lands. The
spec mechanically checks (over the bounded 2-Tx model):

- All 22 MVCCGc invariants carried forward.
- **5 new cutover invariants**: `TypeOKCutover`,
  `ActiveSnapshotsBoundedByWatermark`,
  `HeartbeatRespectsActiveSnapshots`,
  `SQLAutoCommitSerializability`, `LegacyKeyspaceEmpty`.

The first highlighted invariant is the **heartbeat-correctness
claim**: the heartbeat producer never proposes a watermark that
exceeds the minimum active snapshot. The second highlighted
invariant is the **cutover-completeness claim**: after S2.6 cutover,
the 20-byte data-row keyspace is structurally absent — no action ever
produces a legacy key. **These are the formal closures of the cutover
correctness contract.** The seventh rigor-gate TLA+ module in the
project; the first to encode a structural-absence invariant
(LegacyKeyspaceEmpty).

---

## The cutover correctness contract (formal)

This section states, in code-grounded prose, what S2.6 ships as
behaviour. Every clause is gated by a KAT in T2, an integration test
in T3, a coverage test in T4, or a pentest in T5.

### Auto-commit Tx serializability invariant (the SQL→MVCC headline)

Every SQL statement executed through `kesseldb-server::apply_one`
flows through an auto-commit Tx that submits an `Op::CommitTx` (for
writes) or runs as a read-only Tx (for SELECT). The auto-commit Tx
sequence forms a schedule that is serializable per SI/SSI per the
SP113 contract. Gated by T3's `it_sql_auto_commit_ssi_isolation` and
T4's `it_coverage_sql_ssi_dangerous_structure_aborts`.

### Legacy keyspace empty invariant (the cutover-completeness headline)

After S2.6 cutover, no SM apply produces a 20-byte data-row key.
Specifically: for every `Op::Create / Op::Update / Op::Delete /
Op::GetById / Op::Select* / Op::Query* / Op::Aggregate* /
Op::GroupAggregate / Op::Join / Op::UpdateSet / Op::QueryExpr` apply
arm, the storage modifications are exclusively to 28-byte MVCC
versioned keys (via `mvcc::put_versioned` or related primitives).
**Auxiliary keyspaces (indexes, constraints, catalog, blobs,
sequencer) are unchanged per Decision 1 scope refinement** — those
20-byte keys remain. Gated by T3's `it_legacy_data_row_keyspace_empty`
(walks the full storage after a workload, asserts every key whose
prefix matches the data-row prefix is 28-byte).

### AdvanceWatermark heartbeat invariant (the dormant-to-production headline)

A running `kesseldb-server` on the VSR primary fires
`Op::AdvanceWatermark` ops periodically (default 1s interval). Every
heartbeat target is ≤ min(active_snapshots) (or
current_commit_opnum if empty). Gated by T3's
`it_heartbeat_advances_watermark_periodically` and T5's
`pt_heartbeat_respects_active_snapshots_under_load`.

### `commit_opnum = 0` soft acceptance invariant

For every `Op::CommitTx { commit_opnum: 0, ... }` op, SM apply
overrides with `op_number` (the log position) and uses that as the
effective commit_opnum. For every `Op::CommitTx { commit_opnum: N,
... }` with N > 0, SM apply uses N as-is. Gated by T2's KAT
`kat_op_committx_zero_means_auto_assign` and
`kat_op_committx_non_zero_used_as_is`.

### Snapshot READ COMMITTED invariant

Every auto-commit Tx begins at `snapshot_opnum =
sm.current_commit_opnum()` — the latest committed op_number visible
to the SM at Tx construction time. This matches PostgreSQL READ
COMMITTED isolation. Gated by T2's KAT
`kat_auto_commit_tx_snapshot_is_current_commit`.

### Active-snapshots-bounded-by-watermark invariant

For every active Tx with snapshot_opnum S, S >= sm.low_water_mark().
Operationally: the heartbeat producer's bounded-target property
(HeartbeatRespectsActiveSnapshots) keeps the watermark below the
oldest active snapshot, so the watermark never advances past a
live reader. Gated by T3's `it_long_running_sql_pins_watermark`
(a workload with a long-lived auto-commit Tx — actually a
hypothetical multi-statement-spanning future S2.7 case; for S2.6,
the auto-commit Tx is single-statement so the test exercises the
race where heartbeat fires DURING the auto-commit Tx's lifetime).

### 3-replica byte-identity invariant (carried forward + extended)

For every SQL workload, every replica reaches byte-identical storage
state after the workload completes. Gated by T3's
`it_3_replica_byte_identity_for_sql_workloads` (extends SP114's
`it_3_replica_byte_identity_for_gc_op` to cover the SQL→MVCC →
Op::CommitTx pipeline + interleaved heartbeats).

### SP1-SP114 catalog/index/blob byte-identity invariant

For every apply sequence that does NOT touch data rows (catalog
DDL, index creation, blob writes, sequencer ops), the storage state
is byte-identical to SP114. **Auxiliary keyspaces preserved per
Decision 1.** Gated by T4's `it_coverage_catalog_ddl_byte_identical_to_sp114`.

---

## Sub-slice gate accounting (estimated; HONEST RANGE)

Total cargo gate growth in S2.6: estimated **+20 to +50 tests** on
the new SQL→MVCC + heartbeat + cutover surface with HIGH
UNCERTAINTY because the legacy-keypath SM-test count is not known
precisely until T2 audit; some tests migrate, some delete, some
spawn replacements. Breakdown:

| Task | Expected tests | Cumulative | Notes |
|---|---|---|---|
| T0 baseline | 0 | 640 | SP114 final, expect FAILED=0 + seed-7 green |
| T1 scaffold | +2 | 642 | Type-shape locks: `StateMachine::active_snapshots` field present + register/unregister/min accessor signatures + `StateMachine::current_commit_opnum()` accessor + Op::CommitTx soft-accept comment + heartbeat task scaffolding (no-op tick body) + apply_one Tx-wrapper signatures with todo!() bodies + cutover plan tracker |
| T2 impl + KATs | +11 | 653 | active_snapshots register/unregister/min + Op::CommitTx soft-accept (commit_opnum=0 → op_number) + auto-commit Tx wrapper end-to-end at apply_one + REMOVE legacy 20-byte path from data-row SM apply arms + 11 hand-derived KATs (active-snapshots register/unregister/min, count-keyed multiset, soft-accept zero, soft-accept non-zero, auto-commit snapshot, auto-commit lifecycle, legacy-keypath-absent for Create/Update/Delete/GetById/Select, scan_at_snapshot deterministic order) |
| T2 LEGACY DELETES | -30 to -80 | depends | SP3-SP100 era SM-apply tests asserting 20-byte legacy keypath state: migrate (intent-preserving; ~50%) or delete (byte-level legacy; ~50%). T0 audits the exact count; T2 records the migration result. |
| T3 integration | +6 | depends | it_3_replica_byte_identity_for_sql_workloads (HEADLINE) + **it_sql_auto_commit_ssi_isolation** + it_legacy_data_row_keyspace_empty (HEADLINE) + it_heartbeat_advances_watermark_periodically (HEADLINE) + it_long_running_sql_pins_watermark + it_advance_watermark_during_in_flight_commit (heartbeat ↔ commit race) |
| T4 coverage | +6 | depends | per-statement Tx lifecycle / auto-commit rollback on error / heartbeat-respects-active-snapshots / large SQL batches / mixed read/write SQL / SQL+SSI integration / it_coverage_catalog_ddl_byte_identical_to_sp114 |
| T5 pentest | +6 | depends | malformed SQL hostile input / watermark advancement under load / race-shaped active-snapshots churn / SQL injection against MVCC layer / heartbeat-during-in-flight-commit / legacy-keypath-resurrection-attempt |
| T6 docs + TLA+ | 0 Rust | final | MVCCCutover.tla + .cfg + TLC baseline + SP115 record + STATUS + memory |

**Estimated final cargo gate after S2.6:** **640 + 31 (T1+T2+T3+T4+T5
adds) − 30 to −80 (T2 legacy deletes) = ~590 to ~640 tests** with
HONEST RANGE +20 to +50 NET from the SP114 baseline. The actual
number lands in T6 — recorded honestly with the per-task adds and
deletes named separately. **The wide range is the slice's primary
gate disclosure** — no other S2 slice has a legacy-delete dimension.

**Cargo dependency snapshot.** `cargo tree -p kesseldb-server | grep
-Ei "parquet|objstore|rustls|webpki"` MUST remain byte-identical to
the SP114 baseline. ZERO new external dependencies in S2.6.

The TLA+ artifact's gate is the TLC baseline run (zero invariant
violations on the bounded config) + the artifact files committed to
`kesseldb-tla/`.

---

## The BREAKING migration (legacy → MVCC cutover)

**This is the slice's primary breaking-change disclosure.** Required
for any future user, internal or external, who has data on disk in
the SP1-SP113 20-byte legacy format.

### What breaks

- **Pre-existing on-disk data using 20-byte data-row legacy keys is
  UNREADABLE after S2.6 ships.** The SM apply arms for
  `Op::GetById` / `Op::Select` / `Op::QueryRows` (and family) call
  `mvcc::get_at_snapshot` against 28-byte versioned keys; the legacy
  20-byte key would not match. **No data loss in the literal sense
  (the LSM bytes are still there); the data is just structurally
  unaddressable through the new code paths.**

- **No production caller exists today** (verified at T0 audit). The
  KesselDB repo has no shipping installations; no user is migrating
  off SP114. The breakage is theoretical for any hypothetical future
  user with on-disk state from a SP1-SP113 build.

### What tests need to migrate

- **~50-80 SP3-SP100 era SM-apply tests** that exercise the
  `Op::Create / Op::Update / Op::Delete / Op::GetById / Op::Select*
  / Op::Query*` apply arms and assert on legacy 20-byte storage
  state. Per Decision 8: each test is classified at T2:
  - **MIGRATE** (estimated ~50%): intent-preserving rewrite. The
    test's INTENT was "Op::Create produces a readable row"; the
    MVCC-rewritten apply still produces a readable row, the
    bytes-on-disk are just 28-byte versioned now. Update the test's
    assertion from `Storage::get(20-byte-key)` to a snapshot-read
    via `mvcc::get_at_snapshot(28-byte-key, snapshot)`.
  - **DELETE** (estimated ~50%): byte-level legacy-keypath assertion
    that no longer applies. E.g., a test asserting "the SM writes a
    20-byte key starting with type_id 0x01" — that's a contract
    about a removed code path; delete the test.

### What production code changes

Per Architecture above:
- **`kesseldb-server::apply_one`**: every SQL-derived Op routes
  through an auto-commit Tx.
- **`kessel-sm::StateMachine::apply`**: 14 data-row Op apply arms
  rewrite against MVCC.
- **`kesseldb-server` heartbeat task**: NEW background producer for
  `Op::AdvanceWatermark`.

### Rollback path (documented, NOT built)

If a hypothetical user has data in the SP1-SP113 20-byte format and
needs to migrate to S2.6:

- **Documented offline conversion tool (S2.X follow-up; NOT built in
  S2.6).** A standalone Rust binary that opens the pre-S2.6 storage,
  walks the 20-byte data-row keyspace, and writes each row as an MVCC
  versioned entry at `commit_opnum = 0` (or a configured "import"
  opnum). Auxiliary keyspaces (catalog, indexes, blobs, sequencer)
  pass through unchanged. After conversion, the resulting LSM is
  consumed by S2.6 SM apply paths correctly. **This tool is
  scoped for an S2.X follow-up if anyone ever needs it; per the
  autonomous-mandate BOLD-choice discipline, S2.6 does not build it
  speculatively.**

- **Honest disclosure of the no-build choice.** Building the
  conversion tool would cost ~200 LOC + a comprehensive integration
  test suite that's only useful for an empty user base. Per the
  autonomous-mandate "BOLD choices, don't gold-plate" framing,
  shipping the tool in S2.6 is speculative scope. The tool exists in
  the design (this section) as a future-build trigger; the trigger
  fires when someone actually has SP1-SP113 on-disk state to
  migrate.

### What does NOT break

- **Auxiliary keyspaces** (catalog, indexes, constraints, blobs,
  sequencer) — per Decision 1 scope refinement: the 20-byte keys for
  these structures are PRESERVED. SP1-SP114 DDL test bodies that
  assert on catalog state CONTINUE to pass byte-net-0.
- **Wire protocol** — `Op::CommitTx` wire format UNCHANGED (the
  semantic change is in SM apply, not the wire codec). `Op::Create /
  Op::Update / Op::Delete` wire formats UNCHANGED. Pipelined batches
  and client wire frames continue to work.
- **The `Op::AdvanceWatermark` SM apply arm** (shipped in SP114)
  continues to be exercised by direct SM-apply tests (T3 / T5
  integration tests). The heartbeat producer just ADDS production
  callers; the apply arm itself is unchanged.

---

## Sub-slice decomposition reminder (S2 CLOSES)

S2.6 is the SIXTH AND FINAL sub-slice of S2. **After S2.6 ships, S2 is
COMPLETE.** The S2 strategic-tier line item in `docs/STATUS.md` flips
to **done** after S2.6 closeout.

The strategic-tier backlog after S2.6:

| ID | Item | Status |
|---|---|---|
| S1 | TLA+/model-checked safety specs for the VSR protocol | **DONE (SP109)** |
| S2 | Serializable MVCC/SI over the deterministic log | **DONE (after this slice) — S2.1 (SP110), S2.2 (SP111), S2.3 (SP112), S2.4 (SP113), S2.5 (SP114), S2.6 (SP115)** |
| S3 | Jepsen harness for real distributed fault injection | Open |
| S4 | Deterministic in-tree WASM UDFs (sandboxed, reproducible, no imports) | Open |

The next strategic-tier slice after S2 closes is S3 (Jepsen) — the
externally-attested rigor pillar. Hypothetical S2.7 (SQL `BEGIN`/`COMMIT`/
`ROLLBACK` grammar; multi-statement Tx; per-Tx isolation level selection;
operator-driven snapshot read SQL syntax) is named as a future
enhancement but NOT on the strategic-tier roadmap.

---

## Honest deferred set

Items explicitly out of scope for S2.6, named here so the S2.6 record
can't drift into over-claim territory:

- **SQL `BEGIN` / `COMMIT` / `ROLLBACK` grammar.** Per Decision 2:
  S2.6 ships auto-commit per-statement only. Multi-statement Tx via
  SQL grammar is the natural S2.7 follow-up (parser changes,
  session-state map, per-Tx isolation level select between
  Tx::commit and Tx::commit_ssi). NOT on strategic-tier roadmap.
- **Multi-replica heartbeat with global min(active) consensus.** Per
  Decision 6 + 7: the heartbeat reads per-primary-local
  active_snapshots; replicas other than the primary don't contribute.
  For the SP1-SP113 production model (clients route through primary)
  the gap is empty; when read-replicas land (post-S2) the gap
  surfaces.
- **Offline 20-byte → 28-byte conversion tool.** Per Migration
  section: documented, NOT built. Speculative for an empty user base.
- **Auxiliary keyspaces (indexes / constraints / catalog / blobs /
  sequencer) promoted to MVCC.** Per Decision 1 scope: not in S2.6;
  not on any roadmap. Would yield deterministic-bytes-on-disk for
  secondary indexes too.
- **SM checkpoint persistence of `active_snapshots` (separate from
  SP114's `low_water_mark` deferral).** active_snapshots is per-
  replica local + in-memory only; restart-rebuild is fine (no
  snapshot to lose on crash because the surviving Tx's clients are
  also gone). Documented as not-required.
- **3-Tx TLC bound for MVCCCutover.** S2.6 ships a 2-Tx bounded model
  (sufficient for cutover correctness invariants since the auto-
  commit Tx is single-statement). A 3-Tx model would exercise
  multi-statement-Tx interactions which are S2.7 anyway.
- **Multi-replica TLA+ for cutover.** Same scope decision as
  SP110-SP114 carried forward.
- **TLA+-mechanized-refinement TLA+ ↔ Rust.** Same gap S1/SP109 +
  SP110-SP114 disclosed. Per-sub-slice named-action correspondence
  carries forward.
- **Heartbeat interval auto-tuning.** Default 1s; configurable via
  config knob. Adaptive scheduling based on commit rate is a
  hypothetical operational enhancement.
- **Tx-pool, cross-thread Tx, Tx ID allocation.** Not on S2 roadmap.
  Tx is single-thread / per-statement by construction.
- **SP1-SP113 20-byte data-row data on disk migration tool.**
  Per Migration section: documented + deferred.
- **Per-statement SI vs SSI isolation level selection.** Every
  auto-commit Tx in S2.6 uses SSI (Tx::commit_ssi). Choosing SI
  (Tx::commit) per-statement would require SQL syntax (probably
  S2.7).

---

## Thesis-fit note

**Thesis fit:** `deterministic` (the data flow at every SQL statement
is SQL → server → Tx → Op::CommitTx → VSR → SM::apply → mvcc::put_versioned;
every step at the apply layer is a pure function of the log prefix +
the Op; replicas converge byte-identically; the non-determinism is
contained at the SQL-frame-submission + heartbeat-tick boundaries,
which are explicitly outside the apply path; **the thesis is now
operational at the SQL surface, not just the SM-apply primitive
layer**; this is the second strategic-tier headline of S2 after
SP114's "GC becomes a structural property of the log" — SP115's "every
SQL statement is a deterministic MVCC Tx" makes the SQL→VSR→storage
data flow a closed deterministic loop with no remaining legacy escape
hatch);
`replayable` (every SQL statement is reducible to a `(seed, log,
SQL-frame-sequence)` tuple — debugging IS replay; a production bug
report on a SnapshotTooOld error or SI-conflict abort reduces to that
tuple);
`verifiable` (`MVCCCutover.tla` extends SP114's MVCCGc.tla with the
active_snapshots + HeartbeatTick action + four new invariants —
TypeOKCutover, ActiveSnapshotsBoundedByWatermark,
HeartbeatRespectsActiveSnapshots, SQLAutoCommitSerializability,
LegacyKeyspaceEmpty — all mechanically-checked by TLC against the
same VSR-log substrate; the **seventh** rigor-gate TLA+ module in
the project; the **first** that encodes a structural-absence
invariant (LegacyKeyspaceEmpty) AND the **first** that encodes
the dormant-to-production cutover correctness contract);
`honest-docs` (the breaking-migration disclosure is a dedicated
section; the auxiliary-keyspace scope refinement is named in
Decision 1; the per-replica heartbeat gap is named in Decision 6;
the offline conversion tool is documented but not built; the wide
HONEST-RANGE cargo gate disclosure is named in Decision 8; the
deferred S2.7 grammar is named in Decision 2; the rejected
alternatives in every decision are named, not silently revised; the
SP114 dormant-to-production cutover semantic shift is the slice's
primary disclosure throughout).

The thesis-fit headline of this slice: **Every SQL statement is now
a deterministic MVCC Tx.** KesselDB's production SQL surface is no
longer a legacy parallel universe to its MVCC primitive — the two
are unified at the apply layer; the auto-commit Tx wraps every
statement; the auto-commit lifecycle composes with the SP110-SP114
substrate to yield deterministic replicated SQL with verifiable
behavior and replayability. **This is the most direct expression yet
of the THESIS at the production data path** — S2 closes with the
substrate proven AND consumed by the SQL surface.

S2 is DONE.

---

## Internal record

This design document is
`docs/superpowers/specs/2026-05-24-mvcc-si-s2-6-design.md`.

The S2.6 implementation plan is
`docs/superpowers/plans/2026-05-24-mvcc-si-s2-6.md`.

When S2.6 ships, its slice-record file will be
`docs/superpowers/specs/2026-05-24-kesseldb-subproject115-mvcc-cutover-s2-6.md`
(SP115 in the subproject numbering; mirrors the SP110-SP114 filename
pattern). The record will carry the honest gate accounting (640 →
final; with the legacy-test-delete count and the SQL+MVCC-add count
named separately), the per-task evidence chain, the TLA+-to-Rust
correspondence table, the Migration section verbatim with the
S2.X-conversion-tool trigger condition, the deferred backlog, and
the **S2 closes** strategic-tier context update.

The S2.6 TLA+ artifact will be `kesseldb-tla/MVCCCutover.tla` +
`.cfg` + `kesseldb-tla/results/2026-05-24-mvcc-cutover-baseline.txt`
(the captured TLC baseline run).
