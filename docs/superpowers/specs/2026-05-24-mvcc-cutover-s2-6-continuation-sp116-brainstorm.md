# SP116 Brainstorm — Data-Row Apply-Arm Cutover + xshard Test-Corpus Migration (closes S2)

**Date:** 2026-05-24  **Status:** brainstorm (10 decisions to resolve before design + plan)

**Builds on:**
- SP115 record (`docs/superpowers/specs/2026-05-24-kesseldb-subproject115-mvcc-cutover-s2-6.md`) — the narrowed-scope predecessor
- S2.6 design (`docs/superpowers/specs/2026-05-24-mvcc-si-s2-6-design.md`)
- S2.6 plan (`docs/superpowers/plans/2026-05-24-mvcc-si-s2-6.md`)
- SP114 record (`docs/superpowers/specs/2026-05-24-kesseldb-subproject114-mvcc-gc-s2-5.md`)
- KesselDB autonomous-build mandate (`feedback_kesseldb_autonomous_build.md`)

> **Process note:** This brainstorm dispatch is authored in-band by the SP115 T6 closeout agent because the environment lacks a dispatchable Task/agent tool and lacks a `claude` CLI for background spawn. The standard "subagent-driven brainstorm" cadence does not apply; the brainstorm doc itself is the dispatch artifact. The next session that picks up SP116 should resolve the 10 decisions below first, then author the SP116 design + plan + execute.

---

## Goal

Close S2 by landing the **14 data-row apply-arm cutover** (Op::Create / Op::Update / Op::UpdateSet / Op::Delete / Op::GetById / Op::Join / Op::Query / Op::QueryExpr / Op::Select / Op::QueryRows / Op::SelectFields / Op::Aggregate / Op::SelectSorted / Op::GroupAggregate) onto the SP115-shipped `data_row_{get,put,delete,scan}` MVCC seam helpers, AND migrating the xshard test corpus so the `xshard_protocol_atomic_and_deterministic_under_adversarial_drive` invariant's byte-identical total-storage-digest assertion can coexist with MVCC keys baking `commit_opnum` into the 28-byte key shape.

After SP116 lands, the **S2 strategic-tier item closes** and the SP115 T6 narrowing disclosure is fully resolved (LegacyKeyspaceEmpty TLA+ assertion lifts from the narrowed `CommitTxWritesVersionedKeyspaceOnly` form to the unconditional form; "every SQL statement is a deterministic MVCC Tx" becomes a shipped claim, not a deferred one).

---

## The 10 decisions to resolve

### Decision 1 — xshard test migration strategy (THE GATING DECISION)

The `xshard_protocol_atomic_and_deterministic_under_adversarial_drive` test asserts byte-identical total-storage-digest across replicas after the xshard protocol completes. Two replicas applying the same logical writes via MVCC at the same log positions produce byte-identical 28-byte keys (commit_opnum is the log position, which agrees across replicas); the *total* storage digest still shifts because the MVCC keyspace contributes 28-byte keys + version chains where the legacy keyspace contributes a single 20-byte key per object.

Three candidate strategies:

**(a) Exclude MVCC keys from total-storage digest.** The simplest migration: the digest function is updated to skip the 28-byte MVCC key range (per-type-id `0x..` prefix discriminator). The xshard test's contract becomes "byte-identical NON-MVCC storage digest across replicas"; the MVCC byte-identity is gated separately by the SP115 T3 3-replica byte-identity test. **Tradeoff:** loses the strict "every byte the same on every replica" property; gains a clean separation between the xshard concern (legacy keyspace) and the MVCC concern (versioned keyspace). **Estimated effort:** ~30 LOC change in the digest function + ~5 LOC change in the xshard test.

**(b) Compare logical-state instead of byte-state.** The xshard test's contract becomes "every replica's logical row-set (latest committed version per (type_id, object_id)) matches byte-identically." This requires a logical-state digest function that scans MVCC + legacy, computes the latest visible (type_id, object_id) → value, and hashes the sorted result. **Tradeoff:** loses the byte-identity assertion at the LSM level entirely (could mask non-deterministic LSM byte differences — though the 3-replica byte-identity tests cover that); gains a contract that's MVCC-agnostic and survives future encoding changes. **Estimated effort:** ~100 LOC new logical-state digest function + xshard test rewrite.

**(c) Rewrite as MVCC-aware byte-identity.** Keep the byte-identical total-storage-digest assertion, but the digest is now computed over (legacy-keyspace bytes) + (MVCC-keyspace bytes with commit_opnum-in-key); the assertion holds because every replica observes the same VSR log prefix at the digest point. **Tradeoff:** strictest claim preserved; risk is that the digest fires on non-determinism that's actually expected (e.g., compaction reordering versions within a key's version chain). **Estimated effort:** ~10 LOC change in the digest function + likely ~50 LOC in compact-determinism shoring.

**Recommended:** (a) for the cleanest separation of concerns; (c) if the autonomous mandate's "BOLD choice" stance prefers the strictest gate. Avoid (b) unless logical-state-comparison is what the test was *really* asserting (review the original test author's intent before deciding).

### Decision 2 — which test corpus migration is acceptable per the test-corpus discipline

The KesselDB autonomous-build mandate has the "never weaken a test" rule. T2 reverted the apply-arm rewrites rather than touch the test. SP116 must touch the test — the question is HOW.

Options:
- **MIGRATE the test** (intent-preserving rewrite of the digest function per Decision 1): acceptable; the test's intent (replicas agree byte-for-byte on the xshard-touched state) is preserved; the digest function changes.
- **DELETE the test and add a stricter MVCC-aware replacement**: acceptable if the replacement is strictly stronger; the original test's coverage must remain provable.
- **DELETE the test without replacement**: NOT acceptable per the discipline.

**Recommended:** MIGRATE with a comment in the test marking the SP116 migration + the SP115 T2 narrowing as the historical context.

### Decision 3 — how to phase the 14 apply-arm rewrites

Two paths:

**(a) All-at-once in one T2 commit.** The cleanest history: one commit that flips all 14 arms + the xshard migration + the LegacyKeyspaceEmpty TLA+ lift in lockstep. **Tradeoff:** large single commit; if a test regression surfaces it's harder to bisect to the specific arm.

**(b) Subset-per-task across multiple SP116 sub-tasks.** Group the 14 arms by complexity / shared MVCC seam usage:
- T2a — read-only arms (Op::GetById, Op::Select, Op::QueryRows, Op::SelectFields, Op::SelectSorted, Op::Aggregate, Op::GroupAggregate, Op::Join, Op::Query, Op::QueryExpr) — all use `data_row_get` + `data_row_scan` + index keyspace unchanged.
- T2b — write arms (Op::Create, Op::Update, Op::UpdateSet, Op::Delete) — all use `data_row_put` + `data_row_delete`.
- T2c — xshard test migration.
- T2d — LegacyKeyspaceEmpty TLA+ lift + MVCCCutover.tla refinement (or new MVCCCutoverFull.tla).

**Recommended:** (b) — easier review, easier bisect, smaller per-commit blast radius.

### Decision 4 — data_row_* helpers SP115 shipped — confirm they're the right seam

SP115 T2 shipped `data_row_get / data_row_put / data_row_delete / data_row_scan` as the SOLE entry points the data-row apply arms will use after the cutover. The signatures:

```rust
pub(crate) fn data_row_get(&self, type_id: u32, oid: &[u8; 16]) -> Option<Vec<u8>>;
pub(crate) fn data_row_put(&mut self, op_number: u64, type_id: u32, oid: &[u8; 16], value: Option<Vec<u8>>) -> std::io::Result<()>;
pub(crate) fn data_row_delete(&mut self, op_number: u64, type_id: u32, oid: &[u8; 16]) -> std::io::Result<()>;
pub(crate) fn data_row_scan(&self, type_id: u32) -> Vec<(Key, Vec<u8>)>;
```

**Open questions for SP116:**
- Is `op_number` the right snapshot parameter for `data_row_get` / `data_row_scan`? SP115 hard-codes `u64::MAX` (latest). For the SP116 cutover the auto-commit Tx's snapshot is `current_commit_opnum`; should `data_row_get` accept a snapshot parameter? **Decision needed:** add a `snapshot_opnum` parameter to `data_row_get` / `data_row_scan` to support multi-statement Tx (S2.7); for SP116's per-statement auto-commit, `u64::MAX` continues to work because the apply arm runs serially in log-position order.
- `data_row_scan` returns `Vec<(Key, Vec<u8>)>` (reconstructed 20-byte legacy keys). Should the cutover preserve this shape (no caller churn) or migrate callers to operate over `(ObjectId, Vec<u8>)` directly? **Recommended:** preserve the legacy shape for SP116 (minimize call-site churn); migrate to `(ObjectId, Vec<u8>)` in a separate S2.X cleanup.

**Recommended seam:** the helpers ARE the right seam. Decision needed only on the snapshot-parameter optional-addition.

### Decision 5 — MVCC vs legacy key dual-write window if any

Two options:

**(a) Hard cutover: SP116 T2 flips all 14 arms in one commit; the legacy 20-byte data-row keyspace stops accumulating new writes.** The previously-written legacy data is left in place (it's effectively dead data; offline conversion tool is S2.X). **Tradeoff:** simplest; assumes empty user base (Decision 1 of S2.6 design — the bold-choice path).

**(b) Soft cutover: dual-write to BOTH legacy AND MVCC for one slice; gate reads on a config flag; later remove the legacy writes.** **Tradeoff:** safer for installed-base; adds dual-write overhead; requires the config flag plumbing.

**Recommended:** (a) — KesselDB has no installed base; the bold-choice path is the autonomous mandate's preferred stance; the offline conversion tool is documented S2.X follow-up for the eventual installed-base case.

### Decision 6 — auto-commit Tx wrapper for SQL — finalize the API

SP115 ships the `apply_one` register/unregister bracket but does NOT wrap each statement in a full `Tx::begin → Tx::write → Tx::commit_ssi → Op::CommitTx` lifecycle. The narrowed cutover at the apply_one seam is the register/unregister bracket only.

For SP116: the 14 data-row apply arms route through `data_row_{get,put,delete,scan}` directly. Do they ALSO need to be wrapped in a full Tx lifecycle?

**(a) Yes — every apply arm becomes a full auto-commit Tx (Tx::begin → reads via Tx::read → writes via Tx::write → Tx::commit_ssi → Op::CommitTx).** This is the strongest claim ("every SQL statement is a deterministic MVCC Tx" verbatim). **Tradeoff:** the Op::CommitTx SI/SSI conflict check fires per statement; for per-statement auto-commit (no client-side concurrent Tx), the WW-conflict and SSI dangerous-structure checks are trivially clean (the Tx is the entire log slot); the check is structural overhead.

**(b) No — the 14 apply arms route data ops directly through `data_row_*` (no Tx wrapper); the SI/SSI write-side activates only for explicit multi-statement Tx (S2.7).** **Tradeoff:** weaker claim; the deterministic-MVCC-Tx thesis-fit headline is still partially deferred to S2.7; the cutover ships only the keyspace migration, not the Tx wrapping.

**Recommended:** (a) — the full-Tx wrap is what makes the thesis-fit claim land. The structural overhead is negligible (the conflict checks are O(1) for per-statement Tx since the active-Tx population is at most 1 at the apply_one seam). The auto-commit bracket SP115 shipped IS the outer wrapper of the full-Tx lifecycle; SP116 just needs to add the inner Tx::begin / Tx::commit_ssi calls.

### Decision 7 — catalog + auxiliary keyspaces stay legacy per Decision 1 refinement (no further action)

Decision 1 of the S2.6 design scoped the cutover to data-row keyspaces ONLY; catalog (DDL ops: Op::CreateType, Op::AddIndex, etc.), index maintenance, blob storage (SP2), sequencer (SP79), constraint metadata (SP4 UNIQUE, SP6 FK, SP7 CHECK) all stay at the 20-byte legacy keyspace.

**Question for SP116:** any change to this scope refinement?

**Recommended:** NO. The decision is correct; the SP115 T4 `catalog DDL byte-net-0` coverage test continues to gate it. Lifting catalog/etc. to MVCC would explode the cutover scope without proportional thesis-fit gain.

### Decision 8 — TLA+: extend MVCCCutover with the rewritten arms' contract or new MVCCCutoverFull.tla

The SP115 MVCCCutover.tla asserts the NARROWED `CommitTxWritesVersionedKeyspaceOnly` invariant. For SP116, this should be LIFTED to the unconditional `LegacyKeyspaceEmpty` form (after the apply-arm cutover, no apply-arm-produced write lands in the legacy keyspace).

Two structural options:

**(a) Edit MVCCCutover.tla in place** — change `CommitTxWritesVersionedKeyspaceOnly` to `LegacyKeyspaceEmpty`; update the module head's narrowing-disclosure to "narrowing resolved at SP116"; re-run TLC. **Tradeoff:** preserves the 7-module rigor-gate stack at exactly 7; preserves the spec's history in git.

**(b) Add a new `MVCCCutoverFull.tla`** that EXTENDS MVCCCutover and adds the LegacyKeyspaceEmpty assertion + any new SP116-specific actions modeling the 14 apply arms' MVCC routing. **Tradeoff:** preserves MVCCCutover.tla as the narrowed-scope artifact for historical reference; grows the rigor-gate stack to 8 modules.

**Recommended:** (a) for the layered-stack discipline (each TLA+ module corresponds to one strategic-tier sub-slice; MVCCCutover.tla IS the S2.6 spec, just at the narrowed state pending SP116). Edit in place; capture the SP116 TLC baseline as `kesseldb-tla/results/2026-05-24-mvcc-cutover-sp116-baseline.txt` so the SP115 narrowed baseline + the SP116 unconditional baseline are both archived.

### Decision 9 — honest cargo delta range

SP115's cargo delta was +31. SP116's range depends on:
- 14 apply-arm rewrites: ~0 net (apply-arm bodies change shape but the test bodies asserting on OpResult variants continue to pass byte-net-0 because OpResult is unchanged; the byte-net-0 contract holds at the OpResult layer, which is what SP1-SP114 tests assert on).
- xshard test migration: ~0 net (one test changes shape; the test count is preserved).
- ~95 MIGRATE tests stay green if migration succeeds (per the SP115 T0 audit's classification).
- New SP116 integration tests: +8 to +15 (SQL+MVCC integration coverage, 3-replica byte-identity for the SQL surface, LegacyKeyspaceEmpty operational assertion, heartbeat-advances-watermark live test).

**Honest range:** +5 to +20 net-additive, with a STRONG expectation toward the small end if the migrate-vs-delete classification holds (the 95 MIGRATE tests stay green; the new tests add). The T0 audit for SP116 records the actual baseline; T6 records the actual final.

### Decision 10 — slice numbering

SP116. (S2 strategic-tier item closes at SP116.)

---

## Suggested SP116 task structure (sketch — finalize at the design stage)

- **T0** — Determinism baseline + S2.6-continuation surface audit + xshard-digest baseline recording.
- **T1** — Scaffold: add the `snapshot_opnum: u64` parameter to `data_row_get` / `data_row_scan` (Decision 4 add-on); confirm the SP115 helpers are still the seam; add Tx::auto_commit signature + comment markers in apply arm bodies.
- **T2a** — Read-only apply-arm rewrites (Decision 3 (b) subset 1) + KATs locking each arm's MVCC routing.
- **T2b** — Write apply-arm rewrites (Decision 3 (b) subset 2) + KATs + full-Tx wrap (Decision 6 (a)).
- **T2c** — xshard test migration (Decision 1 + Decision 2) + companion KATs.
- **T2d** — MVCCCutover.tla LegacyKeyspaceEmpty lift (Decision 8 (a)) + TLC baseline re-run + capture.
- **T3** — Integration tests: 3-replica SQL byte-identity headline + LegacyKeyspaceEmpty operational gate + heartbeat-advances-watermark live test + long-running-SQL/heartbeat race + SQL+MVCC interaction matrix.
- **T4** — Coverage tests: catalog DDL byte-net-0 carried forward + per-arm-coverage matrix + watermark/Tx interaction edges.
- **T5** — Pentest: malformed SQL hostile input + SQL injection against MVCC + heartbeat-during-in-flight-commit + legacy-keypath-resurrection-attempt (now actively gated) + per-arm pentest matrix.
- **T6** — Docs + STATUS + memory + final whole-impl reviewer + **S2 CLOSES**.

---

## Risk register

| Risk | Likelihood | Severity | Mitigation |
|---|---|---|---|
| xshard digest migration breaks a test other than the named one | Medium | Medium | T2c includes a workspace-wide test sweep before commit; T3 cross-asserts the digest function's coverage |
| Full-Tx wrap (Decision 6 (a)) introduces per-statement SI/SSI conflict-check overhead in the apply path | Low | Low | Per-statement auto-commit at the apply_one seam runs serially in log-position order; conflict checks are O(1); T5 pentest gates perf-as-correctness |
| Some of the 14 apply arms have hidden 20-byte-key dependencies (e.g., assume key layout for index correlation) | Medium | High | T2a/T2b proceed arm-by-arm with KAT gating; bisect-friendly subset commits per Decision 3 (b) |
| Hard cutover (Decision 5 (a)) corrupts existing on-disk legacy data for any installed-base user | Low | High | The bold-choice empty-user-base path; offline conversion tool documented S2.X; SP116 record explicitly disclaims any installed-base support |
| LegacyKeyspaceEmpty TLC assertion exposes a 14th arm that wasn't migrated correctly | Medium | Low | T2d runs LAST; TLC counterexample drives any missed-arm correction |
| The MVCCCutover.tla in-place edit (Decision 8 (a)) loses the SP115 narrowed-baseline archival | Low | Low | Capture SP115 baseline at `kesseldb-tla/results/2026-05-24-mvcc-cutover-baseline.txt` (already shipped); SP116 baseline at `kesseldb-tla/results/2026-05-24-mvcc-cutover-sp116-baseline.txt`; both preserved |

---

## When to invoke this brainstorm

The next agent picking up KesselDB work should:
1. Read this brainstorm doc first.
2. Resolve the 10 decisions (use the recommended answers as defaults unless the user overrides).
3. Author `docs/superpowers/specs/2026-05-24-mvcc-si-s2-6-continuation-sp116-design.md` with the resolved decisions.
4. Author `docs/superpowers/plans/2026-05-24-mvcc-si-s2-6-continuation-sp116.md` with the T0-T6 task structure.
5. Execute T0-T6 per the autonomous-mandate cadence (two-stage subagent review gate at T2, T3, T5, T6; final whole-impl reviewer at T6).
6. Update the SP115 record's "What did NOT ship" + "Deferred backlog" sections with backlinks to the SP116 record + flip the S2 strategic-tier item to DONE.

S2 closes when SP116 ships.
