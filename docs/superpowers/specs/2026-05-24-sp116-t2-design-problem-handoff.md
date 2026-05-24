# SP116 T2 — Design problem surfaced; T2 not yet shipped (handoff)

**Status:** T2 BLOCKED on design pivot — surface the choice before further work.

**Author context.** SP116 T0 + T1 shipped successfully (vulcan 673/0). T2 was attempted
in three configurations within this session, each surfaced a new dimension of the
real cutover surface that the SP116 design + plan undercounted. This document captures
the empirical findings so the next session can pick an approach with full visibility.

Builds-on:
- [SP116 design](2026-05-24-mvcc-si-s2-7-design.md) — the plan it derives from
- [SP116 plan](../plans/2026-05-24-mvcc-si-s2-7.md) — T2.A/B/C phasing the empirical
  test run showed is structurally infeasible
- [SP116 T0 baseline](2026-05-24-kesseldb-subproject116-baseline.md) — counts
  ~25 `Storage::digest` callers (vs sketch's 1) + 14 apply arms
- Commits `fe1e021` (T0), `695c751` (T1)

---

## What we found

**The plan says T2 = "rewrite 14 data-row apply arms" + "1-line digest filter".**

**The empirical reality** (measured on vulcan after partial migration of 6/14 arms):

1. **14 apply arms is an undercount.** Schema ops (Op::AddCheck, Op::AddForeignKey,
   Op::AddUnique, Op::DropType, Op::OnDelete*) ALSO scan the data-row keyspace via
   `self.storage.scan_range(&type_lo, &type_hi)` to validate / cascade. With writes
   going to MVCC and reads going to legacy, the validation scans return empty and
   tests break.

2. **The T2.A → T2.B → T2.C phasing is structurally infeasible.** Apply arms have
   intertwined read+write logic across arms (Op::Create writes; Op::GetById reads).
   Migrating only writes (T2.A) breaks any test that sequences `Create → GetById`.
   With **511** `sm.apply(*)` calls and **34** `Op::GetById` references in the test
   suite, partial cutover broke **25 tests** in the 6-arm partial.

3. **The real migration surface is "every data-row I/O site in `kessel-sm/src/lib.rs`"**
   — apply arms PLUS schema ops PLUS internal helpers. Probably 25-35 sites, not 14.

## Empirical 6-arm partial migration result

| | passed | failed | delta vs baseline |
|---|---|---|---|
| Post-T1 baseline | 673 | 0 | (reference) |
| 6-arm partial (Create/Update/UpdateSet/Delete + GetById/Join + digest filter) | 649 | 25 | **−24 passed / +25 failed** |

Failure breakdown:

| Category | Count | Root cause |
|----------|-------|------------|
| Schema-op tests (AddCheck/AddForeignKey/AddUnique/DropType/OnDelete*) | 7 | Schema ops scan data-row keyspace; not migrated |
| Unmigrated read arms (Query/QueryExpr/Select/QueryRows/SelectFields/Aggregate/SelectSorted/GroupAggregate) | 8 | Reads via legacy 20-byte keys find nothing (writes went to MVCC) |
| SQL layer (end_to_end_sql, planner_*, range_index_*) | 7 | SQL compiles to the apply arms; same root cause |
| Pentest (`pt_legacy_keypath_resurrection_via_committx`) | 1 | SP115 pentest asserts MVCC doesn't appear in legacy keyspace; needs update once cutover lands |
| Server cluster (sql_over_tcp, sql_over_cluster_full_crud_and_rmw) | 2 | E2E; downstream of SQL |

## Two design options for T2

### Option A — Per-site rewrite of all 25-35 data-row I/O sites

Faithful expansion of the plan. Touch each of the 14 apply arms PLUS the schema ops
PLUS any internal helpers that read/write data-row keyspace. Replace
`self.storage.get(&key)` with `self.data_row_get(type_id, &oid, u64::MAX)` and similar.
Add the 1-line `Storage::digest` filter. Add KATs locking each migrated site.

**Pros**: faithful to plan; surgical per-site control; each site individually reviewable.

**Cons**: large diff (~25-35 sites × ~5-10 lines each = 150-300 lines of mechanical
changes); risk of missing sites (only caught by test failures); needs careful
attention to make sure no NON-data-row 20-byte usage is collateral.

### Option B — Storage-layer transparent MVCC dispatch (RECOMMENDED then RETRACTED)

Make `Storage::get/put/delete/scan_range` themselves MVCC-aware: when key satisfies
the data-row discriminator, dispatch to `mvcc::*_at_snapshot(u64::MAX)`. NO changes
to apply arms or schema ops or helpers needed.

**The discriminator complication.** A naïve `key.len() == 20` discriminator is unsafe
because **index keys also use `make_key(0xFFFD_0000 | type_id, &id)` which produces
20-byte keys** (see `crates/kessel-sm/src/lib.rs:543`, `:830`). Routing those through
MVCC would version the indexes (wrong; indexes are point-overwrite single-value).

The **safer discriminator** is `key.len() == 20 AND type_id_high_byte != 0xFF`
(reserved 0xFFFx-prefix range covers all aux keyspaces: OVERFLOW / SEQ / XSHARD /
SEQ_DEDUP / XVOTE / IDX_EQ / IDX_NUM_ORD / IDX_STR_ORD).

**Pros**: ONE place to change; all 25-35 callers fixed transparently; no risk of
missing sites; cleanest end state; LegacyKeyspaceEmpty invariant naturally satisfied
(no data-row 20-byte writes ever land in legacy keyspace after the dispatch).

**Cons**: every `Storage::{get,put,delete,scan_range}` call now does a length+type
check; user-type-id range must stay below `0xFFFF_FFF0` (currently enforced by
catalog allocator but not statically guaranteed — would need a documented constraint).

The auto-classifier blocked an attempted commit of Option B mid-session because the
discriminator was `key.len() == 20` (the unsafe form). With the corrected discriminator
(`key.len() == 20 AND type_id_high_byte != 0xFF`), Option B is the architecturally
sound choice. The classifier flag was protective and informative — its concern was
genuine and pointed at the exact spot the discriminator needed tightening.

### What I'd do next session

1. **Adopt Option B with the corrected discriminator.** Single-file change in
   `crates/kessel-storage/src/lib.rs` to `Storage::{get,put,delete,scan_range}`.
   The digest filter (already in the uncommitted working tree) lands alongside.
2. **Verify on vulcan**: expected 673/0 → still 673/0 (no test changes, no apply-arm
   changes, no schema-op changes — the dispatch is transparent and data-row I/O
   silently moves to MVCC).
3. **Add 5+ KATs**: discriminator correctness (20-byte + user-type → MVCC;
   20-byte + 0xFFFx → legacy; 28-byte → legacy passthrough; etc.).
4. **Migrate the SP115 pentest** `pt_legacy_keypath_resurrection_via_committx` —
   its assertion needs updating once data-row writes land in MVCC.
5. **T3-T6 then proceed as planned** — integration tests, coverage, pentest, TLA+
   edit-in-place + S2 closure.

Honest cargo delta estimate for the consolidated Option-B T2: **+5 to +10**
(5+ discriminator KATs + maybe 1-2 dispatch KATs). Within the plan's +5 to +20 band.

## Where this session ended

Shipped (on `origin/main`):
- `fe1e021` — SP116 T0 baseline + audit
- `695c751` — SP116 T1 scaffold (snapshot_opnum + 14 markers + 2 KATs)

Uncommitted in Windows working tree at session end:
- `crates/kessel-storage/src/lib.rs` — Storage::digest 1-line MVCC-key skip filter
  + 1 new unit test `digest_excludes_mvcc_versioned_keyspace`. **This piece is safe
  to commit on its own**: it only excludes 28-byte keys from the digest, and before
  T2 lands, no 28-byte data-row keys exist anyway — so the digest is byte-identical
  for the existing test suite. The new unit test proves the filter works for synthetic
  28-byte keys. Vulcan test run pending (in flight at handoff).

Not shipped:
- T2 apply-arm migration (3 attempted approaches — all surfaced new scope)
- T3 / T4 / T5 / T6

**S2 strategic-tier item REMAINS OPEN.** SP115's narrowed-scope SHIPPED status is
preserved (MVCC infrastructure ready; production data path still on legacy 20-byte
keys until T2 lands).
