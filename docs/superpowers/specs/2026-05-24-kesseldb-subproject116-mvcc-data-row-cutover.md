# KesselDB — Subproject 116: MVCC data-row cutover (S2.7; CLOSES S2)

**Status:** done — code + tests + TLA+ + docs committed and passing on vulcan.

Builds-on:
- [SP115 narrowed-scope record](2026-05-24-kesseldb-subproject115-mvcc-cutover-s2-6.md)
- [SP116 design](2026-05-24-mvcc-si-s2-7-design.md)
- [SP116 plan](../plans/2026-05-24-mvcc-si-s2-7.md)
- [SP116 brainstorm](2026-05-24-mvcc-cutover-s2-6-continuation-sp116-brainstorm.md)
- [SP116 T0 baseline](2026-05-24-kesseldb-subproject116-baseline.md)
- [SP116 T2 design-problem handoff (pivot rationale)](2026-05-24-sp116-t2-design-problem-handoff.md)

---

## What SP116 ships

**The thesis-fit centerpiece of S2 lands.** After SP116 every data-row read/write
goes through the MVCC versioned keyspace (28-byte keys) by construction. The
20-byte legacy user-type data-row keyspace stays EMPTY for the user-type range
`(0, 0xFF00_0000)`. Reserved keyspaces (catalog type_id=0; aux 0xFFFF_FFFx;
index 0xFFFC/D/E_xxxx) remain on the legacy single-overwrite path per Decision 7.

### The architectural pivot — Option B

The SP116 plan called for per-arm rewrites of the 14 data-row apply arms in
T2.A/B/C subset phases. The empirical 6-arm partial migration broke 25 tests
because:
1. **Apply-arm read+write logic is inseparable across arms.** `Op::Create`
   writes; `Op::GetById` reads. Partial cutover (writes-only via MVCC, reads-
   still-legacy) breaks any test that sequences `Create → GetById`. The
   T2.A/B/C phasing was structurally infeasible.
2. **The "14 apply arms" count was an undercount.** Schema ops
   (`Op::AddCheck`, `Op::AddForeignKey`, `Op::AddUnique`, `Op::DropType`,
   `Op::OnDelete*`) ALSO scan the data-row keyspace via
   `self.storage.scan_range(&type_lo, &type_hi)` to validate / cascade.
   Real migration surface is ~25-35 data-row I/O sites in
   `kessel-sm/src/lib.rs`, not 14.

SP116 T2 pivoted to **storage-layer transparent MVCC dispatch** (Option B
per the [T2 design-problem handoff](2026-05-24-sp116-t2-design-problem-handoff.md)):
make `Storage::{get,put,delete,scan_range}` themselves MVCC-aware via a
`data_row_dispatch(key)` discriminator. Now NO apply-arm or schema-op changes
are needed; the 25-35 call sites transparently route through MVCC.

### The discriminator

```rust
fn data_row_dispatch(key: &[u8]) -> Option<u32> {
    if key.len() == mvcc::PREFIX_LEN /* 20 */ && key[3] != 0xFF {
        let type_id = u32::from_le_bytes([key[0], key[1], key[2], key[3]]);
        if type_id != 0 {
            return Some(type_id);
        }
    }
    None
}
```

Reserved-range exclusions (each enforced by an SP116 T2 KAT or T5 pentest):

| Range            | Used for                       | Stays legacy because |
|------------------|--------------------------------|----------------------|
| **type_id = 0**  | catalog self-storage blob      | catalog must NOT version |
| **0xFFFC_xxxx**  | IDX_STR ordered index          | indexes are point-overwrite |
| **0xFFFD_xxxx**  | IDX_NUM ordered index          | indexes are point-overwrite |
| **0xFFFE_xxxx**  | IDX_EQ + composite indexes     | indexes are point-overwrite |
| **0xFFFF_FFF0**  | SEQ                            | sequencer counter is single-value |
| **0xFFFF_FFF1**  | XSHARD                         | xshard coordinator metadata |
| **0xFFFF_FFF2**  | SEQ_DEDUP                      | dedup table is overwrite |
| **0xFFFF_FFF3**  | XVOTE                          | xshard vote single-value |
| **0xFFFF_FFFF**  | OVERFLOW                       | blob storage is content-addressed |

User-type IDs are catalog-allocated monotonically from 1 and stay safely in
the open interval (0, 0xFF00_0000).

---

## Sub-task ledger

| Task | Commit | Cargo delta | What |
|------|--------|-------------|------|
| T0  | `fe1e021` | baseline 671/0 | audit: digest callers (~25), 14 apply arms map, data_row_* signatures |
| T1  | `695c751` | +2 → 673/0    | `snapshot_opnum` param on data_row_get/scan + 14 markers + 2 scaffold KATs |
| T2-prep | `79abac6` | +1 → 674/0    | `Storage::digest` skips 28-byte MVCC keyspace + KAT |
| T2  | `ade0d98` | +5 → 679/0    | **Storage-layer transparent MVCC dispatch** + pentest migration (Decision 2) + 5 discriminator KATs |
| T3  | `092e9d3` | +5 → 684/0    | 5 integration tests (LegacyKeyspaceEmpty + MVCC populated + 3-replica + Create/GetById + mixed) |
| T4  | `532c265` | +3 → 687/0    | 3 coverage tests (50 ops + Aggregate + catalog DDL no-MVCC) |
| T5  | `a21bebc` | +4 → 691/0    | 4 adversarial pentests (boundary sweep + crafted 28-byte + off-by-one + extreme opnum) |
| T6  | (this)   | +0 (docs)     | MVCCCutover.tla LegacyKeyspaceEmpty rename + TLC re-baseline + this record + STATUS + memory + **S2 CLOSES** |

**Final cargo gate on vulcan: 691 passed / 0 failed (+20 net since T0; within
plan's +5 to +20 honest delta band — upper edge).**

---

## TLA+ MVCCCutover.tla edit-in-place (Decision 8)

The SP115 narrowed-scope invariant `CommitTxWritesVersionedKeyspaceOnly` was
RENAMED to **`LegacyKeyspaceEmpty`** with documentation explaining the SP116
semantic strengthening. The mechanical assertion is unchanged (the TLA+ model
only models commits to `versions`; it has no separate `legacyVersions` state
variable). The SEMANTIC strengthening: in the Rust implementation, where
SP115 only Op::CommitTx obeyed the contract, SP116 has all data-row paths
(CommitTx + 14 apply arms + schema ops touching data rows) obeying it via
the storage-layer dispatch.

TLC baseline (vulcan, MVCCCutover.cfg unchanged bounds: 1 type / 2 oids /
3 opnums / 2 values / 3 maxOps / 2 TxIds): captured at
`kesseldb-tla/results/2026-05-24-mvcc-cutover-sp116-baseline.txt`. The 26
invariants from MVCCCutover.cfg pass over the full reachable state space
(LegacyKeyspaceEmpty replaces CommitTxWritesVersionedKeyspaceOnly in the
invariant list; SnapshotImmutability, ReadAtSnapshot, DangerousStructure-
Aborts, NoWriteSkew, SerializableEquivalence, BoundedWindowSupersededByWater-
mark, etc., carry forward via the EXTENDS chain).

---

## Files touched

| File | Change |
|------|--------|
| `crates/kessel-storage/src/lib.rs` | `data_row_dispatch` function + dispatch in `Storage::{get,put,delete,scan_range}` + 5 discriminator KATs + `Storage::digest` 28-byte skip + digest KAT |
| `crates/kessel-sm/src/lib.rs` | T1 `snapshot_opnum` param on `data_row_get/scan` + 14 apply-arm markers + 2 scaffold KATs + 5 T3 integration KATs + 3 T4 coverage KATs (NO apply-arm body rewrites — the storage-layer dispatch made them unnecessary) |
| `crates/kessel-sm/tests/pentest_mvcc_cutover.rs` | `pt_legacy_keypath_resurrection_via_committx` migrated per Decision 2 (NotFound → Got post-cutover) + 4 new SP116 T5 pentests |
| `kesseldb-tla/MVCCCutover.tla` | `CommitTxWritesVersionedKeyspaceOnly` → `LegacyKeyspaceEmpty` (rename + doc-update) |
| `kesseldb-tla/MVCCCutover.cfg` | invariant list updated to use `LegacyKeyspaceEmpty`; .cfg header updated to "SP116 / S2.7 RESOLVED" |
| `kesseldb-tla/results/2026-05-24-mvcc-cutover-sp116-baseline.txt` | new TLC run record |
| `docs/superpowers/specs/2026-05-24-kesseldb-subproject116-mvcc-data-row-cutover.md` | this record |
| `docs/STATUS.md` | SP116 row + S2 strategic-tier item CLOSED |

---

## S2 strategic-tier item CLOSES

The S2 strategic-tier item (#199) — "Serializable MVCC / SI over the
deterministic log" — closes at SP116 T6. The S2 arc shipped over 7 sub-slices:

| Slice | Sub-slice | Module |
|-------|-----------|--------|
| SP110 | S2.1 versioned storage | `kessel-storage::mvcc` + `MVCCStorage.tla` |
| SP111 | S2.2 read-only Tx       | `kessel-storage::tx` + `MVCCTx.tla` |
| SP112 | S2.3 SI write-side       | `kessel-sm Op::CommitTx` SI arm + `MVCCSi.tla` |
| SP113 | S2.4 Cahill SSI          | `kessel-storage::ssi` + `MVCCSsi.tla` |
| SP114 | S2.5 GC + watermark      | `Op::AdvanceWatermark` + `MVCCGc.tla` |
| SP115 | S2.6 cutover infrastructure (narrowed) | `data_row_*` helpers + `apply_one` bracket + heartbeat + `MVCCCutover.tla` |
| SP116 | **S2.7 cutover RESOLVED** | `Storage::*` MVCC dispatch (no apply-arm rewrite needed) + `MVCCCutover.tla` LegacyKeyspaceEmpty |

The thesis claim — **"deterministic replicated SQL with verifiable behavior
and replayability"** — now lands at the data-row layer too: every SQL
statement that touches a user-type row is, by construction, a deterministic
MVCC transaction, replayable across replicas with byte-identical results.

Next strategic-tier items: S3 (Jepsen) and S4 (deterministic WASM UDFs)
remain open.
