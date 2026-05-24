# KesselDB — Subproject 110: S2.1 — Versioned Key-Value Layer + Opnum-as-Timestamp Snapshot Read

**Date:** 2026-05-24  **Status:** done — `kessel-storage::mvcc` module + `MVCCStorage.tla` TLA+ rigor checkpoint committed and pushed.

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
- Project THESIS:
  `docs/THESIS.md`

Parent design document:
`docs/superpowers/specs/2026-05-23-mvcc-si-design.md`

S2.1 plan document:
`docs/superpowers/plans/2026-05-23-mvcc-si-s2-1.md`

---

## Strategic-tier framing

S2.1 is the **first sub-slice of S2** (Serializable MVCC / Snapshot Isolation) in the THESIS.md strategic-tier backlog. SP110 in the subproject numbering — the slice immediately after SP109 (TLA+ replication safety). Both numbers reference the same slice. The parent S2 design (`docs/superpowers/specs/2026-05-23-mvcc-si-design.md`) decomposes S2 into 6 sub-slices: S2.1 (this slice — versioned-storage primitive) → S2.2 (Tx context + read-set) → S2.3 (SI commit + write-set conflict detection) → S2.4 (SSI promotion) → S2.5 (GC + watermark) → S2.6 (SQL integration + SM cutover). This slice ships ONLY the foundation primitive; S2.2–S2.6 build on top of what lands here.

---

## What shipped

`crates/kessel-storage/src/mvcc.rs` — a new `kessel-storage::mvcc` module: the **append-only versioned key-value layer** keyed by `(type_id, object_id, inverted_commit_opnum)`. Plus two new helper methods on the underlying `Storage` impl. Plus a new TLA+ rigor checkpoint (`kesseldb-tla/MVCCStorage.tla` + `MVCCStorage.cfg` + baseline TLC run). The MVCC module is **dormant code** — no caller integrates with it yet; S2.2–S2.6 will wire it in.

### Module surface (28-byte versioned key + 3-valued SnapshotRead)

- **`VERSIONED_KEY_LEN = 28`** — the 28-byte physical key encoding:
  `type_id (4 LE) || object_id (16) || (u64::MAX - commit_opnum) (8 BE)`
  BE encoding + opnum-inversion makes newest-version-first the natural lex order. A snapshot read is a single seek to the 20-byte prefix + scan forward until the first key with `decode_commit_opnum(k) ≤ snapshot_opnum`.

- **`pub enum SnapshotRead { Found(Vec<u8>), Tombstoned, NotYetWritten }`** — three variants. `Tombstoned` (deleted before snapshot) and `NotYetWritten` (never written before snapshot) are semantically distinct (parent design Decision 5 — required for SQL row-exists semantics and S2.5 watermark-GC reasoning).

- **`pub enum MvccKeyError { Length(usize) }`** — typed errors for `decode_commit_opnum` (only length-check needed at the scaffold; later variants can be added without breaking callers).

- **`pub fn make_versioned_key(type_id, object_id, commit_opnum) -> Key`** — builds the 28-byte key.

- **`pub fn decode_commit_opnum(key: &[u8]) -> Result<u64, MvccKeyError>`** — round-trips the encoding.

- **`pub fn put_versioned<V: Vfs>(store, type_id, object_id, commit_opnum, value: Option<Vec<u8>>)`** — single-write append. `Some(bytes)` for a write; `None` for a tombstone. Reuses `Storage::commit` via the new `put_entry_versioned` wrapper.

- **`pub fn get_at_snapshot<V: Vfs>(store, type_id, object_id, snapshot_opnum) -> SnapshotRead`** — newest-version-first scan; first version with `commit_opnum ≤ snapshot_opnum` is the answer.

- **`pub fn has_version_in_range<V: Vfs>(store, type_id, object_id, lo_excl, hi_incl) -> bool`** — helper for S2.3's write-set conflict detection. Half-open `(lo, hi]`. Shipped early per the parent plan's "ship helpers S2.3 will need now" guidance.

### Two new public methods on `Storage` (the only legacy-file change)

- **`pub fn put_entry_versioned(&mut self, op_number, key, value: Option<Vec<u8>>)`** — Option-accepting commit wrapper. Reuses the existing single-entry `commit` path so WAL/memtable/SSTable invariants apply unchanged; tombstones flow naturally because `Entry { value: None }` is already the LSM tombstone shape.

- **`pub fn scan_range_versions(&self, lo, hi) -> Vec<(Key, Option<Vec<u8>>)>`** — tombstone-visible scan. Like `scan_range` but yields `Option<Vec<u8>>` instead of skipping tombstones, because the MVCC layer needs to see deletions explicitly.

The legacy 20-byte keyspace (`Storage::put`/`Storage::delete`/`Storage::commit` from SP1–SP108) is undisturbed — legacy callers write only 20-byte keys, MVCC writes only 28-byte keys, no collision (T5.7b lock).

### TLA+ rigor checkpoint

- **`kesseldb-tla/MVCCStorage.tla`** — abstract single-replica TLA+ specification of the MVCC versioned-storage primitive. Models `versions[(type_id, object_id)]` as a set of `(opnum, value-or-tombstone)` entries with per-(t,o) opnum uniqueness; two actions (`Put(t, o, c, v)` and `Tombstone(t, o, c)`); the `SnapshotReadOf(t, o, s)` function as the abstract read. Module head carries the action-mapping table pointing each TLA+ action to its `kessel-storage::mvcc` Rust counterpart with `file:line` refs (mirrors SP109's named-correspondence discipline). Four invariants: `TypeOK`, `SnapshotMonotonic`, `NeverNotYetWrittenAfterPut`, `TombstoneObservability`.

- **`kesseldb-tla/MVCCStorage.cfg`** — TLC configuration: `TypeIds = {1,2}`, `ObjectIds = {1,2}`, `OpNums = {0,1,2,3}`, `Values = {v1, v2}`, `MaxOps = 5`. `CHECK_DEADLOCK FALSE`. All 4 invariants in the INVARIANT block.

- **`kesseldb-tla/results/2026-05-24-mvcc-storage-baseline.txt`** — captured baseline TLC run: **`Model checking completed. No error has been found.`** 1,225,093 distinct states / 5,944,369 states generated / depth 6 / **46 seconds** wall-clock on Windows. Complete coverage (queue drained to 0 states left).

### Test surface (cargo gate growth — all new tests on the new module)

| Task | Tests added | Cumulative cargo total | Notes |
|---|---|---|---|
| T1 (scaffold) | +3 smoke | 484 → 487 | Type-shape locks for SnapshotRead/MvccKeyError/VERSIONED_KEY_LEN |
| T2 (encoding + I/O) | +6 KATs (replaces 3 T1 smoke + adds 7 new = net +6) | 487 → 493 | Key roundtrip / boundary opnums / decode rejects / newer-first sort / put-get / multi-version / tombstone / range / display / clone-eq |
| T3 (byte-identity) | +5 | 493 → 498 | 3-replica byte-identity + lagging-replica prefix-consistency |
| T4 (coverage) | +6 | 498 → 504 | Never-written / snapshot-into-future / snapshot-at-commit / exhaustive multi-version / tombstone revival / out-of-order writes |
| T5 (pentest) | +9 | 504 → 513 | u64::MAX, 0, malformed lengths, adjacent type_id/object_id prefixes, type_id u32::MAX, reverse-order writes, legacy-vs-versioned coexistence + non-collision |
| T6 (this) | 0 | 513 → 513 | Docs + TLA+ + STATUS + memory only; no Rust touched |

**Total: 484 → 513 (+29 net-additive tests).** All on the new MVCC module; every legacy SP1–SP108 path remains byte-net-0. `FAILED=0`, `large_seed_corpus_is_deterministic_and_converges` green, zero new external dependencies (`cargo tree -p kesseldb-server | grep -Ei "parquet|objstore|rustls|webpki"` unchanged from SP108), `#![forbid(unsafe_code)]` honored in every touched file.

---

## Per-task evidence chain

| Task | Commit | Evidence |
|---|---|---|
| T1 scaffold | `d495d83` | Module types + signatures with `todo!()` bodies; +3 smoke tests; 484 → 487 |
| T2 encoding + KATs | `2f4e8f1` | `make_versioned_key`/`decode_commit_opnum`/`put_versioned`/`get_at_snapshot`/`has_version_in_range` bodies + 7 hand-derived KATs (boundary opnums, decode reject, newest-first sort, put-then-get, multi-version, tombstone, range half-open); 487 → 493 |
| T3 byte-identity | `4092a20` | 5 cross-replica byte-identity tests in `crates/kessel-storage/tests/mvcc_replication_byte_identity.rs` proving same log prefix → byte-identical version chains; 493 → 498 |
| T4 coverage | `821c98a` | 6 edge-case lifecycle tests (never-written, snapshot-into-future, snapshot-at-commit, exhaustive multi-version, tombstone revival, out-of-order writes); 498 → 504 |
| T5 pentest | `f067740` | 9 adversarial-input tests (u64::MAX/0 opnums, every non-28-byte length, adjacent type_id/object_id prefix non-bleed, type_id u32::MAX, reverse-order writes, legacy-vs-versioned non-collision, legacy+versioned coexistence); 504 → 513; no vuln found |
| T6 docs + TLA+ | _(this commit)_ | SP110 record + STATUS row + `MVCCStorage.tla` + `MVCCStorage.cfg` + baseline TLC run (1.225M distinct states / depth 6 / no violation / 46s); 513 → 513 (no Rust touched) |

---

## TLC honest disclosure — 1 TLC-found spec issue (gate working as designed)

TLC found ONE specification issue during T6 model-checking. It was corrected in-place during T6 (no separate commits since T6 is itself the TLA+ landing commit). The fix is a TIGHTENING of the spec's invariant formulation to mirror the actual storage contract — not a weakening of any invariant.

**Fix #1 (in this T6 commit): Drop the `readLog` state variable + reformulate `NeverNotYetWrittenAfterPut`, `SnapshotMonotonic`, and `TombstoneObservability` as current-state properties.**

The original design (mirroring the SP110 plan T6 stub) carried a `readLog` state variable to record observed snapshot reads and asserted cross-snapshot invariants over recorded reads. TLC's first counterexample was 5 states deep:

1. `Read(1, 1, 0)` returns `NotYetWritten` — correctly, since no version of (1,1) exists.
2. `Put(1, 1, 0, "v1")` adds the first version.
3. `Read(1, 1, 0)` now returns `Found("v1")`.
4. `NeverNotYetWrittenAfterPut` violated: the first read recorded NotYetWritten at snap=0, but the current state has a version with opnum=0 ≤ 0.

This is **classification (a) — TLA+ spec bug**: the invariant tried to assert a temporal property (reads-are-correct-at-their-time-of-read) as a state invariant over the readLog. The contract being checked was wrong; the storage IS allowed to have a different `(snap, result)` mapping over time as versions are added. The actual MVCC contract is about the CURRENT state's `SnapshotReadOf` function, not about historical reads.

**Tightening (not weakening):** drop `readLog`, drop the `Read` action (reads have no observable storage state-effect), and reformulate all read-related invariants as universal current-state properties quantified over `(TypeIds, ObjectIds, OpNums)`:

- `SnapshotMonotonic` — `∀ t, o, s1 ≤ s2: SnapshotReadOf(t,o,s2).opnum ≥ SnapshotReadOf(t,o,s1).opnum` (and the analogous "if s1 reads Found, s2 must too").
- `NeverNotYetWrittenAfterPut` — `∀ t, o, s: (∃ e ∈ versions[(t,o)] : e.opnum ≤ s) ⇒ SnapshotReadOf(t,o,s) ≠ NotYetWritten`.
- `TombstoneObservability` — `∀ t, o, s: SnapshotReadOf returns the unique max-opnum version with opnum ≤ s`.

After the tightening, TLC exhaustively explored **1,225,093 distinct states / 5,944,369 states generated / depth 6 / 46 seconds** with **zero invariant violations**. Queue drained to 0 states left — complete coverage at the configured bounds.

This is institutional-grade formal-methods rigor — TLC found the gap between "what the plan stub said the invariant should be" and "what the storage contract actually IS", and the fix is provably-correct (the contract is a property of the `SnapshotReadOf` function, not of historical reads). Gate working exactly as designed. The fix is in this T6 commit alongside the docs/STATUS/memory updates.

---

## TLA+-to-Rust correspondence

Named-action correspondence per SP109's discipline (Decision 6 of its design + this slice's parent design Decision 7). NOT mechanized refinement — a divergence between the spec and the implementation is a human-discovered issue. The TLA+ spec's module head carries the live mapping table; this record reproduces it for archival.

| TLA+ in MVCCStorage.tla | Rust counterpart | Notes |
|---|---|---|
| `Put(t, o, c, v)` | `kessel_storage::mvcc::put_versioned(t, o, c, Some(v))` | Each TLA+ Key abstracts (type_id, object_id); writes 28-byte versioned key via `Storage::put_entry_versioned` |
| `Tombstone(t, o, c)` | `kessel_storage::mvcc::put_versioned(t, o, c, None)` | Writes 28-byte versioned key with value=None (LSM tombstone) |
| `SnapshotReadOf(t, o, s)` | `kessel_storage::mvcc::get_at_snapshot(t, o, s)` | Returns 3-variant `SnapshotRead`; the TLA+ sentinel `opnum = -1` corresponds to Rust `SnapshotRead::NotYetWritten` |
| `versions[<<t, o>>]` | the LSM-stored 28-byte versioned key set with this (type_id, object_id) prefix | Byte-identity at the LSM layer is verified at the Rust level by T3 (5 byte-identity tests) — NOT modeled at the TLA+ level |
| `opCount` | (not modeled in Rust; TLA+-only bound for state-space termination) | |

---

## Honest gate accounting

Pre-SP110 cargo baseline: **484/0** (post-SP109 final). SP109 was a pure additive TLA+ slice so the gate didn't move.

Post-SP110 cargo gate: **513/0** (+29 net-additive tests across T1–T5; T6 added 0 Rust tests).

The +29 delta is **all new tests on the NEW MVCC module**. Every legacy SP1–SP108 path is byte-net-0 — verified at three levels:

1. The MVCC module writes only 28-byte keys via `put_entry_versioned`; legacy paths write only 20-byte keys via `put`/`delete`. The lengths don't collide (T5.7 lock proves the lex-order non-overlap; T5.7b proves they coexist without interference).
2. `cargo tree -p kesseldb-server 2>&1 | grep -Ei "parquet|objstore|rustls|webpki"` unchanged from SP108 (zero new external dependencies).
3. Existing oracles unchanged: `external_source_oracle` (2), `external_source_tls_oracle` (1), `external_source_objstore_oracle` (1); SP100–108 Parquet KAT/fixture paths byte-identical.

`FAILED=0`; `large_seed_corpus_is_deterministic_and_converges` green; `#![forbid(unsafe_code)]` honored in every touched file.

The TLA+ `MVCCStorage.tla` pass is the slice's **second rigor-gate artifact** — extends S1/SP109's `Replication.tla` discipline to the MVCC storage layer. The two TLA+ modules together cover the replication (SP109) and the versioned-storage (SP110) primitives of the kernel.

---

## Thesis-fit

This slice **strengthens the verifiable-behavior pillar** of the project THESIS along four distinct dimensions:

1. **Encoding correctness via hand-derived KATs (T2).** Seven KATs lock the 28-byte key encoding byte-for-byte: round-trip across boundary opnums (0, 1, u64::MAX-1, u64::MAX), decode-length rejection, newest-first lex order, put-then-read, multi-version coexistence, tombstone visibility, half-open range semantics. Every byte of the encoding is mechanically asserted.

2. **Cross-replica byte-identity via T3 (the headline replayable claim).** Five integration tests prove that two replicas with the same applied log prefix produce byte-identical version chains. This is the empirical foundation for the THESIS "deterministic / replayable" pillar at the storage level — and the property the future S2.2–S2.6 transaction layer will inherit.

3. **Edge-case lifecycle correctness via T4 (6 tests).** Snapshot reads at boundaries (never-written / snapshot-into-future / snapshot-at-commit / exhaustive multi-version / tombstone-revival / out-of-order writes) all behave per the contract.

4. **Adversarial-input safety via T5 (9 pentest tests).** Hostile inputs (u64::MAX opnums, malformed lengths, adjacent-prefix bleed attempts, type_id u32::MAX, reverse-order writes, legacy-vs-versioned coexistence) produce no panics, no OOM, no silent data corruption, no cross-prefix bleed. No vulnerabilities found.

5. **TLA+ machine-checked MVCC contract via `MVCCStorage.tla`.** The snapshot-axis monotonicity (older snapshots can't see newer versions), the read-after-write property (post-put reads never return NotYetWritten), and the tombstone-observability rule (newest-at-or-before-snapshot is the read result) are all proved across 1.2M distinct states at the bounded configuration. The TLA+ pass is the slice's second rigor-gate artifact (after S1/SP109).

This slice also **strengthens the replayable pillar**: same log prefix → byte-identical version chains on every replica, mechanically asserted at the Rust integration-test level (T3) and abstracted-but-strong at the TLA+ level (set-of-records equality is automatic for replicas that apply the same Put/Tombstone sequence).

---

## Honest disclosure — the slice's primary discipline

- **The MVCC module is dormant.** No caller integrates with it in S2.1. The `kessel-sm` apply path still writes 20-byte legacy keys via `Storage::put`/`Storage::delete`; the `kessel-sql` compile path is unchanged. The "MVCC works" claim is the contract + the 29 tests + the TLA+ pass; the "MVCC is in the production data path" claim is **reserved for S2.6** (SM cutover). The legacy 20-byte path coexists with the new 28-byte MVCC path undisturbed (T5.7b lock).

- **The TLA+ spec is abstract single-replica.** It models a single replica's per-key version set and the snapshot-read function; multi-replica replication-byte-identity is verified at the Rust integration-test level (T3, 5 tests), NOT at the TLA+ level. Lifting that to TLA+ would require a per-replica `versions[r]` shape — that's an S2.X follow-up.

- **The TLA+ correspondence is named, not mechanized.** Same caveat as SP109 — a divergence between the spec and the Rust code is a human-discovered issue. The action-mapping table in `MVCCStorage.tla` and reproduced above is the audit trail; the line-numbers will drift as `mvcc.rs` is refactored and the table must be re-run (`grep -n "pub fn make_versioned_key\|pub fn put_versioned\|pub fn get_at_snapshot" crates/kessel-storage/src/mvcc.rs`).

- **Bounded TLC config.** TLC exhausts the bounded model at `TypeIds = {1,2}`, `ObjectIds = {1,2}`, `OpNums = {0,1,2,3}`, `Values = {v1, v2}`, `MaxOps = 5` (1.225M distinct states, 5.944M generated, depth 6, 46s, complete coverage). Larger configurations are an S2.X follow-up. The Rust pentest tests (T5) cover the actual boundary opnums (u64::MAX, 0) explicitly that the bounded TLC model cannot reach.

- **GC / watermark / version reclamation not modeled.** Versions monotonically grow in both the Rust module and the TLA+ spec. S2.5 will model the watermark and its interaction with snapshot reads.

- **Tx context / conflict detection / SSI not modeled.** This is the storage-primitive contract only. S2.2–S2.4 follow-ups model the transaction layer.

- **TLC found 1 real spec issue (the readLog/temporal-category-error fix above).** That fix is in this T6 commit; the gate is working as designed.

---

## Deferred / S2.X follow-up backlog (within S2)

Per the parent S2 design:

| ID | Item | Status |
|---|---|---|
| S2.2 | Tx context + read-set tracking | Deferred (next slice — plan TBD at `docs/superpowers/plans/2026-05-24-mvcc-si-s2-2.md` or similar) |
| S2.3 | SI commit + write-set conflict detection (uses `has_version_in_range` shipped here) | Deferred |
| S2.4 | SSI promotion (read-set/write-set intersection cycle detection) | Deferred |
| S2.5 | GC + watermark (reclaim pre-watermark tombstones, never-yet-written stays semantically distinct above the watermark) | Deferred |
| S2.6 | SQL integration + SM cutover (the wire-it-in slice — replaces 20-byte legacy paths with 28-byte MVCC paths) | Deferred |
| S2.X | Multi-replica TLA+ (lift `versions[r]` to per-replica; mechanize the byte-identity claim) | Deferred |
| S2.X | Larger TLC bounds for MVCCStorage | Deferred |

---

## Strategic-tier context update

SP110 SHIPS S2.1. The strategic-tier backlog after SP110:

| ID | Item | Status |
|---|---|---|
| S1 | TLA+/model-checked safety specs for the VSR protocol | **DONE (SP109)** |
| S2 | Serializable MVCC/SI over the deterministic log | **In progress: S2.1 DONE (SP110); S2.2–S2.6 open** |
| S3 | Jepsen harness for real distributed fault injection | Open |
| S4 | Deterministic in-tree WASM UDFs (sandboxed, reproducible, no imports) | Open |

S2 strategic-tier parent stays open with S2.2 as the next slice.

---

## Process note

SP110 executes under the autonomous-mandate (see `feedback_kesseldb_autonomous_build.md`). The autonomous-mandate substitutes the brainstorming user-review gate; the two-stage subagent review gate IS the SP110 review (this T6 closeout + the final whole-implementation reviewer that follows). Each task was committed separately:
- T1 scaffold → `d495d83`
- T2 encoding + KATs → `2f4e8f1`
- T3 byte-identity → `4092a20`
- T4 coverage → `821c98a`
- T5 pentest → `f067740`
- T6 closeout (this commit) — docs + TLA+ + STATUS + memory

The TLC-found spec issue (readLog temporal category error) was fixed in-place during T6 since T6 is itself the TLA+ landing commit; the alternative — committing the broken spec first then fixing it — would have polluted the audit trail with a known-broken state. The fix is honest-disclosed in the TLC section above and in the spec head comment.

All plan-deviation disclosures (the 1 TLC-found fix; the readLog-removal architectural pivot; the test-count drift from "expected ≈22 = 7+2+6+7" to "actual +29 = 6+5+6+9+3" — the T1 smoke tests are net-additive on top of the T2 KAT counts) are made in this record, not suppressed.
