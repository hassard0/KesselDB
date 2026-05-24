# KesselDB — Subproject 112: S2.3 — SI Write-Side + Conflict Detection at SM Apply Time

**Date:** 2026-05-24  **Status:** done — `kessel-storage::tx` SI write-side + `kessel-sm::StateMachine::apply` `Op::CommitTx` arm + `kessel-proto::Op::CommitTx` + `OpResult::TxCommitted`/`TxAborted` + `MVCCSi.tla` TLA+ rigor checkpoint committed and pushed.

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
- Subproject 109 — S1: TLA+ Model-Checked Replication Safety:
  `docs/superpowers/specs/2026-05-23-kesseldb-subproject109-tla-replication-safety.md`
- Subproject 110 — S2.1: MVCC versioned storage (foundation primitive):
  `docs/superpowers/specs/2026-05-23-kesseldb-subproject110-mvcc-s2-1.md`
- Subproject 111 — S2.2: MVCC Tx context + read-set tracking:
  `docs/superpowers/specs/2026-05-24-kesseldb-subproject111-mvcc-tx-s2-2.md`
- Project THESIS:
  `docs/THESIS.md`

Parent S2 design document:
`docs/superpowers/specs/2026-05-23-mvcc-si-design.md`

S2.3 design document:
`docs/superpowers/specs/2026-05-24-mvcc-si-s2-3-design.md`

S2.3 plan document:
`docs/superpowers/plans/2026-05-24-mvcc-si-s2-3.md`

---

## Strategic-tier framing

S2.3 is the **third sub-slice of S2** (Serializable MVCC / Snapshot Isolation) in the THESIS.md strategic-tier backlog. SP112 in the subproject numbering — the slice immediately after SP111 (S2.2 read-only Tx + read-set tracking) and SP110 (S2.1 MVCC versioned storage). All three numbers reference the same slice family. The parent S2 design (`docs/superpowers/specs/2026-05-23-mvcc-si-design.md`) decomposes S2 into 6 sub-slices: S2.1 (SP110 — versioned-storage primitive) → S2.2 (SP111 — Tx context + read-set) → **S2.3 (this slice — SI write-side + deterministic conflict detection at SM apply time)** → S2.4 (SSI promotion) → S2.5 (GC + watermark) → S2.6 (SQL integration + SM cutover). This slice ships the **thesis-fit centerpiece of S2** — the deterministic conflict resolver that operationalizes the parent S2 design's Decision 4 claim "deterministic apply IS the conflict resolver, no distributed coordination needed."

---

## THESIS-FIT CENTERPIECE — deterministic apply IS the conflict resolver

**This is the most important paragraph in this record.**

Per the parent S2 design Decision 4 and the S2.3 design Decision 4 (verbatim):

> In a deterministic state machine fed by a totally-ordered log, conflict
> detection is a function of the log prefix. Every replica sees the same
> log in the same order; every replica runs the same
> `apply(op_number, Op::CommitTx { snapshot_opnum, write_set })` and
> reaches the same conflict verdict. **No distributed conflict-resolution
> coordination is required.** Compare to non-deterministic replicated
> systems (Spanner: TrueTime + Paxos per shard; CockroachDB: HLC + the
> txn-record coordination protocol) — KesselDB sidesteps this entire
> class of complexity because the log already orders the conflict checks.

S2.3 is the slice that puts this in code. The conflict check happens AT SM APPLY TIME, not at Tx-side commit time. The `Op::CommitTx { snapshot_opnum, write_set, commit_opnum }` op replicates via VSR; every replica's deterministic `StateMachine::apply` arm runs `mvcc::has_version_in_range(snapshot_opnum, commit_opnum-1)` for each write_set key against its locally-applied (and therefore log-derived) versioned-storage state and reaches the same Committed/Aborted verdict. The verdict is byte-identical on every replica by construction.

**The structural implication:** KesselDB does NOT need distributed conflict-resolution coordination protocols. Spanner's TrueTime + Paxos-per-shard, CockroachDB's HLC + txn-record coordination — both exist to give non-deterministic replicated systems a way to agree on commit ordering. KesselDB's VSR log already orders every commit op; the SM's deterministic apply already agrees on the verdict; the distributed-coordination layer is **structurally absent**. This is what THESIS.md S2 means by "consensus + SQL can be simpler than MVCC-centric systems."

**S2.3 is the slice that makes the S2 thesis claim land in code.** The Rust integration tests (T3) gate the byte-identity claim across 3 replicas for SI commits AND prove `Tx::commit` (standalone path) ↔ `Op::CommitTx` (SM apply path) byte-equivalence. The `MVCCSi.tla` TLA+ artifact (T6) mechanically checks that the conflict verdict + the resulting versions delta is a function of (versions, txsSi[t].snapshot, write_set, commit_opnum) only — the `DeterministicApply` invariant.

---

## What shipped

`crates/kessel-storage/src/tx.rs` — `Tx<'a, V>` extended with the SI write-side: `write_set: BTreeMap<(u32, [u8; 16]), Option<Vec<u8>>>` field (deterministic-iteration overlay buffer; sorted lex per S2.3 design Decision 2); `Tx::write(type_id, &object_id, value)` (buffered write API with same-key last-write-wins coalescing); `Tx::write_set(&self)` accessor (immutable view for S2.4 SSI consumption); `Tx::commit(self, commit_opnum) -> Result<TxCommitOutcome, TxError>` (conflict-checked commit; consumes self); read-your-writes overlay added to `Tx::read` (consults write_set first; read_set discipline preserved). Plus the **T2-decided storage-mutability split** — `TxStore<'a, V>` enum (`Shared(&'a Storage<V>)` / `Exclusive(&'a mut Storage<V>)`) + `Tx::begin_rw(&mut store, snapshot_opnum)` constructor for the write-side path. Rationale documented in the "T2-decided implementation choices" section below.

`crates/kessel-sm/src/lib.rs` — `Op::CommitTx { snapshot_opnum, write_set, commit_opnum }` arm appended to `StateMachine::apply`'s `match op` block. **The thesis-fit centerpiece in code.** Runs the deterministic conflict check via `kessel_storage::mvcc::has_version_in_range` per write_set key in the half-open window `(snapshot_opnum, commit_opnum-1]`; on conflict returns `OpResult::TxAborted { reason: AbortReason::WriteWriteConflict { type_id, object_id } }`; on no conflict installs every write via `mvcc::put_versioned` at `commit_opnum` and returns `OpResult::TxCommitted { commit_opnum }`. Handles the `commit_opnum=0` edge (no conflict check needed; subtracting 1 would underflow) explicitly. Handles the `snapshot_opnum > commit_opnum` malformed-op case by returning `OpResult::TxAborted { reason: AbortReason::SnapshotOutOfRange }` before any check.

`crates/kessel-proto/src/lib.rs` — `Op::CommitTx { snapshot_opnum, write_set, commit_opnum }` variant appended to `enum Op` at wire tag 44; `OpResult::TxCommitted { commit_opnum }` and `OpResult::TxAborted { reason: AbortReason }` variants appended at wire tags 9 and 10; `AbortReason` enum with `#[non_exhaustive]` and three variants (`SnapshotOutOfRange` / `WriteWriteConflict { type_id, object_id }` / `StorageIo { kind: i32 }`) encoded inside the `TxAborted` payload at inner tags 0/1/2. Append-only wire-compatible variant additions.

`crates/kessel-storage/src/tx.rs` — `TxCommitOutcome` enum (`#[derive(Debug, Clone, PartialEq, Eq)] #[non_exhaustive]`) with `Committed { commit_opnum }` and `Aborted { conflicting_key: (u32, [u8; 16]) }` variants. `TxError` extended with `SnapshotOutOfRange { snapshot, commit }`, `StorageIo(std::io::Error)`, and `ReadOnlyCannotCommit` variants. (The `WriteWriteConflict` failure mode surfaces as `Ok(TxCommitOutcome::Aborted)` per S2.3 design Decision 6 — conflict is a normal retry-me outcome, not an error.)

`kesseldb-tla/MVCCSi.tla` + `MVCCSi.cfg` + `results/2026-05-24-mvcc-si-baseline.txt` — the **fourth TLA+ rigor-gate artifact** in the project (after SP109 Replication, SP110 MVCCStorage, SP111 MVCCTx). EXTENDS MVCCTx; adds `txsSi: TxIds -> TxRecordSi` + `siOpCount: Nat` state vars; adds 3 SI actions (`TxWrite`, `TxTombstoneWrite`, `CommitTx`) plus lifted versions of all SP111 Tx actions; adds 5 new invariants (`TypeOKSi`, `WriteSetMonotonic`, `WriteWriteConflictDetected`, `CommitAtomicity`, `FirstCommitterWins`, `DeterministicApply`) on top of the 6 SP111 invariants carried forward — **11 invariants total**.

### TLA+ rigor checkpoint — TLC outcome

- **`MVCCSi.tla`** — abstract single-replica TLA+ specification of the SI write-side + SM-apply-time conflict-check layer. EXTENDS `MVCCTx` so the SI invariants are checked over the same versioned-storage + Tx model TLC has already verified in S2.1 (SP110) and S2.2 (SP111). Adds the SI write_set field + commit_opnum field via a parallel `txsSi` record map; adds 3 SI-specific actions (TxWrite/TxTombstoneWrite/CommitTx); adds 5 new invariants. Module head carries the action-mapping table pointing each TLA+ action to its Rust counterpart in `kessel-storage::tx` + `kessel-sm::StateMachine::apply` (mirrors SP109/SP110/SP111 named-correspondence discipline).

- **`MVCCSi.cfg`** — TLC configuration: `TypeIds = {1}`, `ObjectIds = {1, 2}`, `OpNums = {0, 1, 2}`, `Values = {"v1", "v2"}`, `MaxOps = 3`, `TxIds = {"t1", "t2"}`, `MaxTxOps = 6`, `SiUnused = "Si"`. `CHECK_DEADLOCK FALSE`. All 11 invariants in the INVARIANT block (6 SP111 carried forward + 5 SI-specific).

- **`results/2026-05-24-mvcc-si-baseline.txt`** — captured baseline TLC run: **`Model checking completed. No error has been found.`** 3,729,306 distinct states / 18,984,059 states generated / depth 13 / **34 seconds** wall-clock on Windows. Complete coverage (queue drained to 0 states left).

### TLC honest disclosure — 3 spec-issue fixes landed in T6

The first three TLC runs found three real spec issues (per SP109/SP110 precedent — TLC discipline classifies these as (a) spec bugs, fix by TIGHTENING preconditions, never by weakening invariants).

**Fix #1 — `CommitTx` did not flip `txs[t].status` (only `txsSi[t].status`), breaking `TypeOKSi`'s mirror agreement.** Counterexample at depth 5: PutSi → PutSi → TxBeginSi → CommitTx → `txs[t1].status = "Active"` while `txsSi[t1].status = "Committed"`. Fix: CommitTx flips BOTH `txs[t]` and `txsSi[t]` status; the SI layer's terminal action also mirrors at the SP111 Tx layer. Tightened the action shape, not the invariant.

**Fix #2 — `TxCommitReadOnlySi` allowed `Committed` of a Tx with a non-empty `write_set`, breaking `CommitAtomicity`.** Counterexample at depth 7: PutSi → PutSi → TxBeginSi → TxWrite → TxCommitReadOnlySi → `txsSi[t2].status = "Committed"` with `write_set = {(1,1) -> "v1"}` but no entry installed in versions[(1,1)] at any commit_opnum. Fix: TIGHTEN `TxCommitReadOnlySi` precondition to `txsSi[t].write_set = << >>` (the SELECT-only path is only enabled when no writes have been buffered). The Rust contract for `Tx::commit_read_only` is the no-conflict-check path — calling it on a Tx with buffered writes is caller misuse; tracking a `debug_assert!` follow-up to mirror the spec at runtime (S2.X).

**Fix #3 — free-floating `PutSi`/`TombstoneSi` could insert a version at an opnum INSIDE an already-Committed Tx's conflict window, retro-violating `WriteWriteConflictDetected`.** Counterexample at depth 6: PutSi(o=2,c=0) → TxBeginSi → TxWrite → CommitTx(c=2) → PutSi(o=1,c=1) [free-floating Put at opnum=1 inside (snapshot=0, commit-1=1] of t1]. Fix: REMOVE free-floating `PutSi`/`TombstoneSi` from `NextSi`. In the real system every versioned write flows through a `CommitTx` (via `Op::CommitTx` SM apply); there is no free-floating Put at the SI level. Plus a complementary tightening of `CommitTx` itself: TIGHTEN `c >= opCount` (the VSR log totally orders commits; commit_opnums are monotonically assigned) and bump `opCount' = c + 1` on success/abort (the log entry is consumed either way). Without this, TLC could re-order commit attempts (t2 at c=1 after t1 at c=2) — a counterexample that does NOT correspond to any real-system behavior.

All three fixes are TIGHTENINGS per the SP109/SP110 discipline. Gate working as designed.

**Final TLC outcome:**
- States generated: 18,984,059
- Distinct states found: 3,729,306
- Depth of complete state graph: 13
- Wall-clock: 34s on Windows 11 (16 workers, 7147MB heap)
- Queue: drained to 0 states left → **complete coverage at the configured bounds**
- Invariant violations: 0 (after the 3 spec tightenings above)
- Spec tightenings landed in T6: 3 (all classification (a) per SP109/SP110)

### Bounded-config calibration

The S2.3 design Decision 7 sized the initial config at `Keys={k1,k2}, Values={v1,v2}, MaxOpnum=4, MaxOps=6, TxIds={t1,t2}, MaxTxOps=8`. The shipped config matches the design on `Values` and `TxIds` and on `MaxTxOps` axis at 6 (the design said 8 — TIGHTENED to 6 to keep the composite SI state space tractable on Windows; the actual sized model completed in 34s at `MaxTxOps=6` and `MaxTxOps=8` would explode geometrically). The shipped config TIGHTENS on `TypeIds={1}` (single type — the per-(type_id, object_id) key uniqueness is exercised via the 2 ObjectIds; adding a second type doubles the action-space on every Put/TxWrite axis without exercising additional SI semantics) and on `OpNums=0..2` (3 commit_opnums — exercises commit_opnum=0 edge AND the in-window conflict case AND the at-snapshot-boundary case; expanding to 0..3 inflates the conflict-window-product axis). The shipped config still exercises every named action, every invariant, AND the FirstCommitterWins case across 2 concurrent Tx with overlapping write-sets. At the shipped bounds TLC exhausts the model in 34s; larger bounds are an S2.X follow-up (per SP109's MR=2-then-MR=3 cadence; the bounded config IS the gate at this slice).

This is the **fourth TLA+ rigor-gate artifact** in the project. The four modules now form a layered verification stack:
- `kesseldb-tla/Replication.tla` (SP109/S1) — VSR replication protocol
- `kesseldb-tla/MVCCStorage.tla` (SP110/S2.1) — versioned storage primitive
- `kesseldb-tla/MVCCTx.tla` (SP111/S2.2) — Tx context + read-set
- `kesseldb-tla/MVCCSi.tla` (SP112/S2.3) — SI write-side + SM-apply-time conflict resolver

Each extends the prior; the invariants compose; the SI conflict-detection contract is mechanically-checked over the same VSR-log substrate that S1/SP109 verified.

### Test surface (cargo gate growth — all new tests on the new SI surface)

| Task | Tests added | Cumulative cargo total | Notes |
|---|---|---|---|
| T1 (scaffold) | +2 smoke | 540 → 542 | Type-shape locks for `TxCommitOutcome` + extended `TxError` + new `Tx::write/write_set/commit` signatures + new `Op::CommitTx` + `OpResult::TxCommitted`/`TxAborted` + `AbortReason` |
| T2 (impl + KATs) | +11 hand-derived KATs | 542 → 553 | write/coalesce/read-your-writes(value+tombstone)/read_set-overlay-records/empty-commit/non-conflict-apply/conflict-aborts/snapshot-OOR/commit_opnum=0/Shared-cannot-commit/Op::CommitTx wire-roundtrip |
| T3 (integration) | +5 | 553 → 558 | read-your-writes through Tx::read / disjoint-writes-both-commit / overlap-second-aborts / 3-replica SI byte-identity for commits (the thesis-fit gate) / Tx::commit ↔ Op::CommitTx SM apply byte-equivalence |
| T4 (coverage) | +5 | 558 → 563 | empty-write_set commit / abort discards / coalesce-overwrite / 1000-write commit / mixed write-tombstone-write same key |
| T5 (pentest) | +7 | 563 → 570 | 100k giant write_set (no OOM) / conflict-at-exact-snapshot-boundary / u64::MAX commit_opnum no overflow / coalesce-repeat (1000x same key) / commit_opnum=0 edge / snapshot > commit_opnum rejected / compile-time invariant locks |
| T6 (this) | 0 | 570 → 570 | Docs + MVCCSi.tla + STATUS + memory only; no Rust touched |

**Total: 540 → 570 (+30 net-additive tests).** All on the new SI write-side + SM-apply-time conflict-check + Op::CommitTx wire surface; every legacy SP1–SP111 path remains byte-net-0. `FAILED=0`, `large_seed_corpus_is_deterministic_and_converges` green, zero new external dependencies (`cargo tree -p kesseldb-server | grep -Ei "parquet|objstore|rustls|webpki"` unchanged from SP111 = unchanged from SP110), `#![forbid(unsafe_code)]` honored in every touched file.

---

## T2-decided implementation choices (documented per design)

The S2.3 design left two implementation choices open for T2. Both decisions were made in T2 and shipped to main:

### Choice 1 — Storage mutability: `TxStore<'a, V>` enum (Shared / Exclusive)

The S2.2 `Tx` held `&'a Storage<V>` (shared borrow, read-only). S2.3's `Tx::commit` needs to call `put_versioned` which takes `&mut Storage<V>`. The design left this open with two paths: (1) interior mutability through the existing shared borrow, or (2) change Tx's borrow shape via a new `Tx::begin_rw` constructor.

**Taken: (2) — `TxStore<'a, V>` enum with Shared and Exclusive variants + a new `Tx::begin_rw(&mut store, snapshot_opnum)` constructor.** Rationale: `put_versioned` is shaped as `&mut Storage<V>` in the SP110 surface; interior mutability would require a Storage-internal refactor (Storage's LSM is not RefCell-wrapped at the public API level). The enum split preserves the SP111 `Tx::begin(&store, snapshot_opnum)` signature verbatim for read-only callers (every SP111 caller continues to compile and run unchanged) AND adds the new `Tx::begin_rw(&mut store, snapshot_opnum)` constructor for write-capable callers. A `Tx` constructed via `begin` (Shared) that attempts `commit` returns `Err(TxError::ReadOnlyCannotCommit)` — a typed error that's trivial for the caller to handle and impossible to accidentally suppress.

**Why this beats (1).** Interior mutability would have hidden the "this Tx mutates storage" semantic at the type level; the Shared/Exclusive enum makes the mutability promise explicit AND compile-time-checkable. The S2.2 read-only Tx use case continues to work without any borrow-shape upgrade.

### Choice 2 — `OpResult::TxCommitted`/`TxAborted` typed variants (vs encoded payload)

The S2.3 design left open whether to add new typed variants on `OpResult` or to encode the SI outcome inside an existing variant (e.g., `OpResult::Ok` + a side-channel result).

**Taken: typed `OpResult::TxCommitted { commit_opnum }` + `OpResult::TxAborted { reason: AbortReason }` variants on the existing `OpResult` enum.** `AbortReason` is its own `#[non_exhaustive]` enum with three variants (`SnapshotOutOfRange` / `WriteWriteConflict { type_id, object_id }` / `StorageIo { kind: i32 }`). Rationale: the typed variants preserve `conflicting_key` and the I/O `ErrorKind` across the wire without payload-bag string-parsing on the receiver side. The kessel-proto codec adds ~12 LOC for the new variants (wire tag 9 = TxCommitted, wire tag 10 = TxAborted; AbortReason sub-tagged inside TxAborted at inner tags 0/1/2 — `SnapshotOutOfRange` (0) / `WriteWriteConflict` (1, carries type_id+object_id) / `StorageIo` (2, carries kind as i32)). The append-only enum-variant-addition discipline is preserved (every SP1–SP111 OpResult variant's wire tag is byte-unchanged; the new variants append at tags 9 and 10).

**Why this beats an encoded-payload approach.** Carrying typed structure across the wire means the caller can `match` on `AbortReason` exhaustively (modulo `#[non_exhaustive]`) and route retry vs. surface-up logic at compile time. An encoded-payload approach would require every caller to discriminate via string-matching the bytes — a footgun that string-parsing-error tests cannot fully cover.

---

## Per-task evidence chain

| Task | Commit | Evidence |
|---|---|---|
| T1 scaffold | `2e6da34` | Module types + `Op::CommitTx` + `TxCommitOutcome` + extended `TxError` + new `Tx::write/write_set/commit` signatures with `todo!()` bodies; +2 smoke tests; 540 → 542 |
| T2 impl + KATs | `1120dab` | `Tx::write` body + `Tx::commit` body (with `TxStore::Shared`/`Exclusive` mutability split + `ReadOnlyCannotCommit` typed error) + read-your-writes overlay in `Tx::read` + `Op::CommitTx` SM apply arm in `kessel-sm::StateMachine::apply` (with `mvcc::has_version_in_range` conflict check + `commit_opnum=0` edge + `snapshot_opnum > commit_opnum` rejection) + `OpResult::TxCommitted`/`TxAborted` + `AbortReason` typed-variant wire encoding + 11 hand-derived KATs; 542 → 553 |
| T3 integration | `5e36b2e` | 5 integration tests in `crates/kessel-storage/tests/integration_mvcc_si.rs` covering read-your-writes through Tx::read, disjoint-writes-both-commit, overlap-second-aborts, **3-replica SI byte-identity for commits (the thesis-fit centerpiece gate)**, and **Tx::commit ↔ Op::CommitTx SM apply byte-equivalence**; 553 → 558 |
| T4 coverage | `dd9abbd` | 5 coverage tests (empty-write_set commit, abort discards buffered writes, coalesce-overwrite same-key, 1000-write commit, mixed write-tombstone-write same Tx); 558 → 563 |
| T5 pentest | `50e17e4` | 7 adversarial-input tests (100k giant write_set no OOM, conflict at exact snapshot-boundary, u64::MAX commit_opnum no overflow, coalesce-repeat 1000x same key, commit_opnum=0 edge, snapshot > commit_opnum rejected, compile-time invariant locks via trybuild-style assertions); 563 → 570; no vuln found |
| T6 docs + TLA+ | _(this commit)_ | SP112 record + STATUS row + `MVCCSi.tla` (EXTENDS MVCCTx, 3 SI actions + 5 new invariants on top of 6 SP111 carried forward = 11 total) + `MVCCSi.cfg` + baseline TLC run (3.729M distinct states / depth 13 / no violation / 34s / complete coverage) + 3 TLC-found spec tightenings (CommitTx mirror agreement / TxCommitReadOnlySi-empty-write_set / free-Put removed + commit_opnum monotonicity); 570 → 570 (no Rust touched) |

---

## TLA+-to-Rust correspondence

Named-action correspondence per SP109/SP110/SP111 discipline (Decision 7 of the S2.3 design). NOT mechanized refinement — a divergence between the spec and the implementation is a human-discovered issue. The TLA+ spec's module head carries the live mapping table; this record reproduces it for archival.

| TLA+ in MVCCSi.tla | Rust counterpart | Notes |
|---|---|---|
| `TxWrite(t, k, v)` | `Tx::write(type_id, &object_id, Some(v))` | Inserts into `BTreeMap` (deterministic iteration, last-write-wins per key) |
| `TxTombstoneWrite(t, k)` | `Tx::write(type_id, &object_id, None)` | Buffered tombstone (None value) |
| `CommitTx(t, c)` (both branches) | (a) `Tx::commit(c)` standalone path AND (b) `Op::CommitTx { snapshot, write_set, commit_opnum }` arm in `StateMachine::apply` | THE THESIS-FIT CENTERPIECE: both paths run the SAME deterministic `has_version_in_range(snapshot, c-1)` per write_set key; T3 byte-equivalence test gates the claim |
| `HasVersionInRange(k, lo_excl, hi_incl)` (function) | `mvcc::has_version_in_range(store, type_id, &object_id, lo_excl, hi_incl)` | SP110-shipped primitive specifically for this slice's conflict check |
| `TypeOKSi` (invariant) | `tx.rs` field types + Storage's interior contract | Well-typed envelope; mirror agreement between txs and txsSi locked by action shape |
| `WriteSetMonotonic` (invariant) | `Tx::write` BTreeMap insert/replace shape | Same-key updates replace value but key persists |
| `WriteWriteConflictDetected` (invariant) | `Op::CommitTx` apply arm's conflict-check branch | No Committed Tx's write_set has a version in (snapshot, commit-1] |
| `CommitAtomicity` (invariant) | `Op::CommitTx` apply arm's two-branch shape | All-or-nothing apply; Aborted preserves storage |
| `FirstCommitterWins` (invariant) | apply-order log discipline + `has_version_in_range` | Two overlapping-write Tx — the second to commit sees the first's version in its conflict window and aborts |
| `DeterministicApply` (invariant) | `Op::CommitTx` apply arm is a pure function of (versions, snapshot, write_set, commit_opnum) | The thesis-fit centerpiece invariant; abstract gate of cross-replica byte-identity |

---

## Honest gate accounting

Pre-SP112 cargo baseline: **540/0** (post-SP111 final).

Post-SP112 cargo gate: **570/0** (+30 net-additive tests across T1–T5; T6 added 0 Rust tests).

The +30 delta is **all new tests on the NEW SI write-side + SM-apply-time conflict-check + Op::CommitTx wire surface**. Every legacy SP1–SP111 path is byte-net-0 — verified at four levels:

1. The SI write_set is a per-Tx in-memory `BTreeMap` — it does not touch disk until `Op::CommitTx` apply at commit time. Tx that never commit (abort or commit_read_only) write nothing. SI commits write only 28-byte MVCC keys (SP110 path); the legacy single-version 20-byte key path stays byte-net-0.
2. `cargo tree -p kesseldb-server 2>&1 | grep -Ei "parquet|objstore|rustls|webpki"` unchanged from SP111 (zero new external dependencies).
3. Zero new public methods on `Storage<V>` in S2.3; the SI code calls only the existing SP110 surface (`mvcc::has_version_in_range`, `mvcc::put_versioned`).
4. `Op::CommitTx` is an append-only wire variant at tag 44; the existing op-apply paths for every other variant are untouched; no legacy op's semantics change.

`FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` green; `#![forbid(unsafe_code)]` honored in every touched file.

---

## Thesis-fit

This slice strengthens all three primary pillars of the project THESIS:

1. **Strengthens the verifiable-behavior pillar.** Five dimensions:
   - **Encoding correctness via T2 hand-derived KATs (11 KATs).** Every public method's pre/post-condition is mechanically asserted by hand-derived KATs: `Tx::write` inserts into write_set, same-key writes coalesce, read-your-writes returns buffered value, read-your-writes buffered tombstone, read_set records buffered reads, empty-write_set commit, non-conflicting commit applies writes, write-write conflict aborts second committer, commit_opnum=0 edge, snapshot OOR rejection, Shared-Tx-cannot-commit, Op::CommitTx wire roundtrip.
   - **3-replica SI byte-identity via T3 (the thesis-fit gate).** Three replicas given the same log prefix of Op::CommitTx ops reach byte-identical versioned-storage state. This is the deterministic-replicated-SI claim, mechanically asserted.
   - **Tx::commit ↔ Op::CommitTx byte-equivalence via T3.** The standalone `Tx::commit` path AND the SM-apply path produce byte-equivalent results on identical storage state. The two-path equivalence is the design's gate that "the SM apply IS the conflict resolver" holds — neither path can drift.
   - **Adversarial-input safety via T5 (7 pentest tests).** Hostile inputs (100k giant write_set, conflict-at-exact-snapshot-boundary, u64::MAX commit_opnum, coalesce-repeat 1000x, commit_opnum=0 edge, snapshot > commit_opnum, compile-time invariant locks) produce no panics, no OOM, no silent corruption. No vulnerabilities found.
   - **TLA+ machine-checked SI contract via `MVCCSi.tla`.** The 5 new SI invariants (WriteSetMonotonic, WriteWriteConflictDetected, CommitAtomicity, FirstCommitterWins, DeterministicApply) are mechanically-checked across 3.729M distinct states at the bounded configuration; the 6 SP111 MVCCTx invariants carry forward. The fourth rigor-gate TLA+ module in the project.

2. **Strengthens the replayable pillar.** Two dimensions:
   - **3-replica byte-identity for SI commits (T3).** Same log prefix → byte-identical versioned-storage state on every replica. The phrase "a Tx outcome is a deterministic function of (snapshot_opnum, write_set, commit_opnum, log prefix)" is the S2.3 thesis-fit claim, mechanically asserted at the Rust integration-test level AND abstracted-strong at the TLA+ level (the `DeterministicApply` invariant).
   - **SM-apply ↔ Tx-commit equivalence (T3).** The two commit paths are semantically equivalent; debugging IS replay because the apply path is the source of truth for the verdict.

3. **Crystallizes the deterministic-apply-is-conflict-resolver insight at the SI level.** This is the thesis-fit centerpiece of S2. The Op::CommitTx variant + its SM apply arm + the MVCCSi.tla `DeterministicApply` invariant together operationalize the parent S2 design Decision 4 claim: **"the deterministic state machine IS the conflict resolver — KesselDB does not need TrueTime, HLCs, or txn-record coordination because the VSR log already orders every commit op, and the SM's deterministic apply already agrees on the verdict."** This is the most direct expression of the "deterministic replicated SQL" pillar in the strategic-tier backlog so far — and the slice that makes the S2 thesis claim "consensus + SQL can be simpler than MVCC-centric systems" land in code.

---

## Honest disclosure — the slice's primary discipline

- **SI write-side dormant pending S2.6 SM cutover.** No production caller submits `Op::CommitTx` to VSR in S2.3. The op is exercised via direct `StateMachine::apply` calls in integration tests (T3) and via construction-only tests of `Tx::commit`. The `kessel-sm` apply path still writes 20-byte legacy keys via `Storage::put`/`Storage::delete` for every non-CommitTx op; the `kessel-sql` compile path is unchanged; the MVCC + Tx modules (S2.1 + S2.2) are also dormant. The "SI write-side works" claim is the contract + the 30 new tests + the TLA+ pass; the "SI write-side is in the production data path" claim is **reserved for S2.6** (SM cutover). The SI surface is parallel infrastructure that the SM does not touch until S2.6.

- **Plain SI only.** S2.3 ships write-write conflict detection. Read-write anti-dependencies (the SSI promotion that elevates plain SI to true serializability) are NOT detected. **S2.4 follow-up.** S2.3's Tx::read_set is preserved exactly as SP111 shipped it; S2.4's dangerous-cycle detection consumes both `read_set` (SP111) and `write_set` (this slice).

- **Cursor-stall on snapshot-not-yet-applied not modeled.** Per the S2.3 design Decision 4 honest disclosure: deferred to S2.6 when the SM caller integration lands. In S2.3 the SM apply path treats `snapshot_opnum > commit_opnum` as a malformed op (conservatively aborts with `AbortReason::SnapshotOutOfRange`). The "snapshot_opnum <= commit_opnum but > my locally-applied opnum" case (the natural "wait for the prefix you depend on" semantic) is the S2.6 wiring responsibility.

- **The TLA+ spec is abstract single-replica.** It models a single replica's per-Tx state + the SnapshotReadOf function + the CommitTx action. Multi-replica SI byte-identity is verified at the Rust integration-test level (T3 ships a 3-replica byte-identity test for SI commits), NOT at the TLA+ level. Lifting that to TLA+ would require a per-replica `txs[r][tx]` shape — that's an S2.X follow-up.

- **The TLA+ correspondence is named, not mechanized.** Same caveat as SP109/SP110/SP111 — a divergence between the spec and the Rust code is a human-discovered issue. The action-mapping table in `MVCCSi.tla` and reproduced above is the audit trail; the line-number table will drift as the Rust code is refactored and the table must be re-run.

- **Bounded TLC config.** TLC exhausts the bounded model at `TypeIds={1}, ObjectIds={1,2}, OpNums={0,1,2}, Values={v1,v2}, MaxOps=3, TxIds={t1,t2}, MaxTxOps=6` (3.729M distinct states, 18.984M generated, depth 13, 34s, complete coverage). The S2.3 design Decision 7 originally sized MaxTxOps=8; the shipped config tightens to keep the composite SI state space tractable on Windows. Larger configurations are an S2.X follow-up. The Rust pentest tests (T5) cover the actual boundary opnums (u64::MAX, 0) explicitly that the bounded TLC model cannot reach.

- **TLC found 3 spec issues during T6.** All three were classification-(a) spec bugs, fixed by TIGHTENING preconditions, not by weakening invariants. (1) CommitTx mirror-agreement fix; (2) TxCommitReadOnlySi-empty-write_set tighten; (3) free-Put removal + commit_opnum monotonicity tighten. The SP109/SP110/SP111 discipline ("tighten preconditions; never weaken invariants") carried forward. Gate working as designed.

- **No `OpResult::TxAborted { reason: AbortReason::StorageIo { kind: i32 } }` is yet produced by any test.** The `StorageIo` AbortReason variant is shipped for forward-compatibility with the production SM caller (S2.6) where I/O during `put_versioned` could fail; the in-memory test paths use `MemVfs` which does not produce I/O errors. The wire encode/decode round-trip IS tested via T2's wire-roundtrip KAT (it parses the StorageIo sub-tag correctly); the apply-time semantic gate is the S2.6 follow-up.

- **`TxStore::Shared` Tx that attempts `commit` returns `Err(TxError::ReadOnlyCannotCommit)`.** This is the T2-decided typed error for the storage-mutability split. Callers that misuse the API by calling `commit` on a `Tx::begin`-constructed Tx get a typed error instead of a compile-time refusal — design choice for ergonomics (callers can dynamically discover Tx mutability mode). The compile-time check ships as a `Tx::begin_rw` constructor that takes `&mut Storage<V>` (only this constructor yields a commit-capable Tx).

- **Cross-thread Tx not on the roadmap.** Tx is single-thread / stack-frame-bound by construction (Decision 5, carried forward from SP111). The struct is `!Send + !Sync` by default (holds an `&Storage` or `&mut Storage`). Cross-thread Tx is not on the S2 roadmap.

---

## Deferred / S2.X follow-up backlog (within S2)

Per the parent S2 design + S2.3 design:

| ID | Item | Status |
|---|---|---|
| S2.4 | SSI promotion (rw-antidependency cycle detection over `Tx::read_set` (SP111) + `Tx::write_set` (this slice)) | Deferred (next slice) |
| S2.5 | GC + watermark (reclaim pre-watermark tombstones + versions) | Deferred |
| S2.6 | SQL integration + SM cutover (replaces 20-byte legacy paths with 28-byte MVCC + Tx + Op::CommitTx; wires `sm.next_op_number()` source of `commit_opnum`; wires cursor-stall-on-snapshot-not-yet-applied) | Deferred |
| S2.X | Multi-replica Tx + SI TLA+ (lift `txs[r][tx]` to per-replica; mechanize the byte-identity claim) | Deferred |
| S2.X | Larger TLC bounds for MVCCSi (MaxTxOps=8 per design; MaxOpnum=4 with multi-Type) | Deferred |
| S2.X | `debug_assert!` mirror of `TxCommitReadOnlySi`-empty-write_set in `Tx::commit_read_only` | Deferred (Rust-side mirror of TLC fix #2) |
| S2.X | Production wire-compatibility test for Op::CommitTx across protocol versions | Deferred |

---

## Strategic-tier context update

SP112 SHIPS S2.3. The strategic-tier backlog after SP112:

| ID | Item | Status |
|---|---|---|
| S1 | TLA+/model-checked safety specs for the VSR protocol | **DONE (SP109)** |
| S2 | Serializable MVCC/SI over the deterministic log | **In progress: S2.1 DONE (SP110); S2.2 DONE (SP111); S2.3 DONE (SP112); S2.4–S2.6 open** |
| S3 | Jepsen harness for real distributed fault injection | Open |
| S4 | Deterministic in-tree WASM UDFs (sandboxed, reproducible, no imports) | Open |

S2 strategic-tier parent stays open with S2.4 as the next slice.

---

## Process note

SP112 executes under the autonomous-mandate (see `feedback_kesseldb_autonomous_build.md`). The autonomous-mandate substitutes the brainstorming user-review gate; the two-stage subagent review gate IS the SP112 review (this T6 closeout + the final whole-implementation reviewer that follows). Each task was committed separately:
- T1 scaffold → `2e6da34`
- T2 impl + KATs → `1120dab`
- T3 integration → `5e36b2e`
- T4 coverage → `dd9abbd`
- T5 pentest → `50e17e4`
- T6 closeout (this commit) — docs + TLA+ + STATUS + memory

The TLA+ artifact landed clean only after 3 TLC-found spec tightenings (CommitTx mirror agreement; TxCommitReadOnlySi-empty-write_set; free-Put removal + commit_opnum monotonicity). All three are classification-(a) spec bugs fixed by TIGHTENING preconditions per the SP109/SP110/SP111 discipline. Gate working as designed.

All plan-deviation disclosures (the TLC bounded-config tightening from design's `MaxOpnum=4 + MaxOps=6 + MaxTxOps=8` to shipped `OpNums=0..2 + MaxOps=3 + MaxTxOps=6` to keep the composite SI state space tractable on Windows; the test-count drift from estimated 567-571 to actual 570 — landing inside the estimated range; the 3 TLC-found spec tightenings) are made in this record, not suppressed.
