# KesselDB — Subproject 116 baseline (T0)

**Status:** done — baseline measured + audit committed; T1 unblocked.

Builds-on:
- [SP115 narrowed-scope record](2026-05-24-kesseldb-subproject115-mvcc-cutover-s2-6.md)
- [SP116 design](2026-05-24-mvcc-si-s2-7-design.md)
- [SP116 plan](../plans/2026-05-24-mvcc-si-s2-7.md)
- [SP116 brainstorm](2026-05-24-mvcc-cutover-s2-6-continuation-sp116-brainstorm.md)

---

## Process note — vulcan substrate

Tests run on **vulcan** (`admin@192.168.4.178`, Rust 1.95.0 / OpenJDK 21 / tla2tools.jar 2.27 MB) per
the standing rule *dev-on-Windows / test-on-vulcan*. Source is sync'd via
`tar.exe` (Windows) → `pscp` → `tar -xz` (vulcan). Vulcan-side build artifacts live at
`/home/admin/KesselDB/target`. The Windows host stays free of `target/` after this slice opens.

---

## §1 Baseline cargo test (on vulcan)

```
plink -ssh -batch -pw admin admin@192.168.4.178 \
  "cd ~/KesselDB && . ~/.cargo/env && cargo test --workspace --release --no-fail-fast 2>&1 \
   | grep -E '^test result:|FAILED' > /tmp/kdb-test-results.txt"
```

**TOTAL passed=671 failed=0** (sum of 36 non-empty `test result:` lines; 23 doc-test crates
return 0/0 as expected). Zero `FAILED` lines in the dump. **Matches Windows baseline exactly.**

Largest single crate: `kessel-sm` — 26 tests in 15.21s (this is the only release-mode test
binary that takes appreciable wall time; vulcan finishes the full release rebuild + run in
under 4 minutes from a warm `target/`).

## §2 Dep snapshot

Zero-dep posture preserved at the kernel boundary — `cargo tree -p kesseldb-server --depth 1`
links no parquet / objstore / rustls / webpki when default features are used (the SP107-SP108
gate carried forward).

## §3 `Storage::digest` callers audit

The brainstorm sketch called out **one** test
(`xshard_protocol_atomic_and_deterministic_under_adversarial_drive`) needing migration.
The audit found **~25 determinism callers** across the workspace, all routed through
`Storage::digest` → `StateMachine::digest`:

| Surface | File:line | Count | What it asserts |
|---------|-----------|-------|-----------------|
| VSR replica byte-identity | `crates/kessel-vsr/src/lib.rs:302,895,1021,1752` | 4 | Cross-replica digest equality + against an oracle |
| SQL determinism | `crates/kessel-sql/src/lib.rs:2396,2679,2793,2897` | 4 | DDL/destructive ALTER / balance-guard byte-identity |
| Server snapshot / recovery | `crates/kesseldb-server/src/{cluster.rs:521,lib.rs:512,1241}` | 3 | Snapshot digest + recovery byte-identity |
| Storage internal | `crates/kessel-storage/src/lib.rs:1276` | 1 | Inside the `digest()` impl itself |
| SM determinism KATs | `crates/kessel-sm/src/lib.rs` (lines 4184, 4509, 4515, 4523, 4531, 4756, 4830, 4903, 4987, 5083, 5166, 5357, 5398, 5547, 5654, 5783, 6008, 6013, 6097, 6192, 6284, 6433, 6534, 6598, 6605, 6718, 6781) | ~25 | Apply byte-identity / sequencer / xshard / two-phase / UpdateSet / GC / range index / various |

**Crucial finding — the Decision 1 fix scales:** the plan's *1-line MVCC-key-exclusion filter
in `Storage::digest` itself* protects every caller transparently. The xshard test isn't
unique — it's just the brainstorm's representative example. So **T2.A migrates ONE function
(`Storage::digest`)** and all ~25 determinism tests stay green by construction.

No additional T2.A migration scope identified.

## §4 14 apply-arm line-number audit

| Op | file:line | Phase | Current path |
|----|-----------|-------|--------------|
| Op::Create | `crates/kessel-sm/src/lib.rs:1695` | **T2.A** | raw `storage.put` (legacy 20-byte key) |
| Op::Update | `crates/kessel-sm/src/lib.rs:1750` | **T2.A** | raw `storage.put` (legacy 20-byte key) |
| Op::UpdateSet | `crates/kessel-sm/src/lib.rs:1825` | **T2.A** | raw `storage.get` + `storage.put` |
| Op::Delete | `crates/kessel-sm/src/lib.rs:1879` | **T2.A** | raw `storage.delete` |
| Op::GetById | `crates/kessel-sm/src/lib.rs:1975` | **T2.B** | raw `storage.get` (point read) |
| Op::Query | `crates/kessel-sm/src/lib.rs:2657` | **T2.B** | raw `storage.scan_range` (type-prefix) |
| Op::QueryExpr | `crates/kessel-sm/src/lib.rs:2886` | **T2.B** | raw `storage.scan_range` + expr eval |
| Op::Select | `crates/kessel-sm/src/lib.rs:2917` | **T2.B** | raw `storage.scan_range` + projection |
| Op::QueryRows | `crates/kessel-sm/src/lib.rs:2948` | **T2.B** | raw `storage.scan_range` + filter |
| Op::SelectFields | `crates/kessel-sm/src/lib.rs:3190` | **T2.B** | raw `storage.scan_range` + field-projection |
| Op::Aggregate | `crates/kessel-sm/src/lib.rs:3233` | **T2.C** | raw `storage.scan_range` + reduce |
| Op::Join | `crates/kessel-sm/src/lib.rs:2328` | **T2.C** | raw `storage.scan_range` × 2 + hash-join |
| Op::SelectSorted | `crates/kessel-sm/src/lib.rs:3408` | **T2.C** | raw `storage.scan_range` + sort |
| Op::GroupAggregate | `crates/kessel-sm/src/lib.rs:3459` | **T2.C** | raw `storage.scan_range` + group + reduce |

Phase-by-phase line spans: T2.A = 1695-1879 (~184 lines, 4 arms), T2.B = 1975-3190 (~1215 lines
spanning, 6 arms), T2.C = 2328 + 3233-3459 (4 arms, mixed). Per-arm rewrite delta estimate:
each write arm gains the inner-Tx::begin/commit_ssi bracket (~6 lines); each read arm replaces a
3-5 line scan_range setup with a 1-2 line `data_row_scan(type_id)` call (net **-3 to +6** per arm).

## §5 `data_row_*` helpers snapshot

All four shipped at `crates/kessel-sm/src/lib.rs:1239-1305` (NOT `kessel-storage` — the helpers
live at the SM layer because they bridge `kessel_storage::mvcc` primitives to the apply-arm
contract). SP115 ships them but does **not** call them (the `dead_code` warning on
`data_row_delete` and `data_row_scan` confirms).

Current signatures:

```rust
fn data_row_get(&self, type_id: u32, oid: &[u8; 16]) -> Option<Vec<u8>>
fn data_row_put(&mut self, op_number: u64, type_id: u32, oid: &[u8; 16], value: Option<Vec<u8>>) -> io::Result<()>
fn data_row_delete(&mut self, op_number: u64, type_id: u32, oid: &[u8; 16]) -> io::Result<()>
fn data_row_scan(&self, type_id: u32) -> Vec<(Key, Vec<u8>)>
```

`data_row_get` and `data_row_scan` currently **hardcode `u64::MAX`** as the snapshot. T1 will
add an optional `snapshot_opnum: u64` param to both (callers in SP116's apply arms pass
`u64::MAX`; the S2.X multi-statement Tx future will pass a captured snapshot). Adding the param
is a non-breaking widening (existing two call-sites — internal — pass `u64::MAX` literally).
`data_row_put` and `data_row_delete` already take `op_number` and need no T1 change.

## §6 Deviations + concerns

- **None for cargo baseline** — 671/0 byte-identical between Windows and vulcan.
- **Storage::digest caller-count surprise** — sketch said 1, real is ~25. Does NOT widen
  SP116 scope because Decision 1's 1-line filter at the digest function protects every caller
  transparently. Updated this baseline doc + the SP116 design `Decision 1` to call this out
  for any reviewer who relies on the sketch wording.
- **Pyarrow fixture e2e tests** — the parquet test crate at `crates/kessel-parquet/tests/`
  contains fixtures generated by pyarrow; those rebuilt fixtures committed to git so vulcan
  doesn't need python3+pyarrow. Confirmed via byte-identical baseline (would have differed
  otherwise).

## §7 T0 status

**DONE.** All audit deliverables met. T1 unblocked.

---

## Sub-slice gate ledger

| Task | Cargo passed | Δ | Notes |
|------|--------------|----|-------|
| T0 (this doc) | 671 | +0 | docs-only commit; no code change |
| T1 (planned) | 671 | +2 | scaffold tests for `snapshot_opnum` param |
| T2.A (planned) | ~676-680 | +5 to +9 | write-arm KATs + xshard digest-filter test |
| T2.B (planned) | ~679-683 | +3 | simple-read-arm KATs |
| T2.C (planned) | ~682-686 | +3 | composite-read-arm KATs |
| T3 (planned) | ~686-691 | +4 to +5 | integration tests |
| T4 (planned) | ~689-696 | +3 to +5 | coverage tests |
| T5 (planned) | ~692-702 | +3 to +6 | pentest |
| T6 (planned) | 692-702 | +0 | docs-only; S2 strategic-tier item CLOSES |

Honest delta range per plan Decision 9: **+5 to +20 net-additive** over the SP115 baseline
(671 → 676-691 final). Outer bound (~+31) is the soft ceiling if all KAT/coverage/pentest
budgets land at their high estimates.
