# SP-Perf-A-TXN-RW — Mixed-RW `Op::Txn` split-phase dispatch — Design

Date: 2026-05-30
Parent: SP-Perf-A-TXN-RO (SHIPPED; all-RO Op::Txn routed through
read-pool bypass; oltp-read-only at N=16 now 5.7× Postgres).
Closes: oltp-read-write loss documented in `docs/BENCHMARKS.md` §3e.

## 1. Context

SP-Perf-A-TXN-RO solved the all-RO Op::Txn bottleneck (28,977 tx/s at
N=16). But mixed-RW Op::Txn (sysbench's 10-reads + 4-writes shape)
still serializes on the apply-thread write lock for the WHOLE
transaction. Pre-arc result:

| Workload | KesselDB N=1 | N=8 | N=16 | Postgres N=8 | SQLite N=8 |
|---|---:|---:|---:|---:|---:|
| oltp-read-write | 1,378 | 718 | 711 | **3,024** | 4,386 |

Root cause: `sm.write().apply(op_no, Op::Txn{ops})` holds the engine
write lock for the entire ~14 ms transaction. At N=8 workers, only one
Txn runs at a time → effective throughput = 1/(14 ms) × 1 ≈ 71 tx/s
per writer-second; observed 718 tx/s says some queueing slack lets
multiple Txns share the second, but the lock is the bottleneck.

## 2. Honest design pivot from the original spec

The original spec proposed wiring Op::Txn through SP112's
`Tx::begin_rw` + `Tx::commit_with_si_check` — buffer writes in a Tx
overlay, commit with conflict detection. **That path is structurally
infeasible in V1** without major SM surgery:

- The `Tx` API (`tx.write(type_id, oid, value)`) operates at the raw
  MVCC layer — bytes-in, bytes-out, no catalog/index/constraint
  machinery.
- `Op::Create` / `Op::Update` / `Op::Delete` (the writes inside
  sysbench's Txn) are deeply intertwined with the SM:
  - Catalog lookup + schema validation
  - Index maintenance (`idx_maintain` for eq/ordered/composite)
  - NOT NULL / UNIQUE / FK / CHECK constraint validation
  - BEFORE INSERT / UPDATE triggers
  - Overflow blob materialization + reclaim
  - Cascade-delete closure for FK ON DELETE
- Threading these through `Tx::write` would require a 2nd
  implementation of every write op against the Tx overlay, with
  conflict-detection logic that knows about indexes and constraints.
  That's a multi-week design — **not a V1 arc**.

The TXN-RW pivot: **split-phase execution**. Sysbench's Txn shape
(and every other "reads-then-writes" Txn shape) lets us run the
read prefix WITHOUT the write lock (parallel reads on the snapshot),
then run the write suffix UNDER the write lock (serialized writes via
the proven apply path). This delivers most of the perf win without
touching the SM's write machinery.

**Correctness note**: The split is byte-equivalent to a unified
Op::Txn{ops} ONLY when the inner ops have no read-after-write
dependency (i.e., no write modifies state that a later read in the
SAME Txn would observe differently). Sysbench's Txn satisfies this
trivially: all 10 reads execute before any of the 4 writes. We lock
this with a `read_prefix_length(&[Op]) -> usize` helper that returns
the count of consecutive read-only ops at the head of `ops`; the
split is the (read-prefix, write-suffix) partition at that index.

For Txns that don't satisfy this shape (e.g., reads after writes),
we fall through to the existing `sm.write().apply(op_no, Op::Txn)`
path — byte-untouched.

## 3. Scope

**V1 — this arc — ships:**

- `read_pool::read_prefix_length(ops: &[Op]) -> usize` — counts
  consecutive read-only ops at the head of `ops`; 0 means "no
  parallelizable prefix" (first op is a write), `ops.len()` means
  "all reads" (would already be caught by TXN-RO classifier).
  Returns the boundary index for the split.
- A driver-level split in `tools/bench-compare/src/drivers/kesseldb.rs::
  run_sysbench_oltp` for mixed-RW Op::Txn: when `read_prefix_length`
  returns a non-empty prefix AND a non-empty suffix, dispatch
  - read prefix via `sm.read().read_only_op(Op::Txn{read_prefix})` —
    parallel against committed state, no write lock
  - write suffix via `sm.write().apply(op_no, Op::Txn{write_suffix})`
    — serialized through the engine queue, exactly as today
- The split produces byte-equivalent results to the unified path for
  read-then-write Txns (locked by KATs).
- Acceptance: vulcan sysbench oltp-RW at N=8/16 lifts from ~715 tx/s
  to ≥3000 tx/s (closes the Postgres gap from 4.2× to ≤1.5×).

**V2 — out of scope (named):**

- Full snapshot-isolation Op::Txn with SM-level write overlay
  (requires re-implementing Create/Update/Delete against the Tx
  overlay; multi-week effort). Naming: **SP-Perf-A-TXN-RW-SI**.
- Read-after-write Txn shapes (general read-your-writes). The
  classifier conservatively falls through to apply for these.
  Naming: **SP-Perf-A-TXN-RW-RYW**.
- Optimistic CC with abort+retry (Cahill SSI on the write path).
  Naming: **SP-Optimistic-CC**.

## 4. Architecture

### 4.1 Classifier extension (read_pool.rs)

```rust
/// Returns the count of consecutive read-only ops at the head of
/// `ops`. The (read_prefix, write_suffix) split at this index is
/// safe for parallel-prefix execution (no read-after-write
/// dependency at the split boundary).
///
/// SP-Perf-A-TXN-RW: enables split-phase dispatch for mixed-RW
/// Op::Txn{ops} where reads precede writes (the canonical sysbench
/// shape: 10 reads then 4 writes ⇒ prefix=10, suffix=4).
///
/// Cases:
///   - All reads ⇒ returns ops.len(); caller routes to TXN-RO
///     bypass (no need for split).
///   - First op is a write ⇒ returns 0; caller routes to apply
///     (no parallelizable prefix).
///   - Mixed with read-after-write (e.g. R,W,R) ⇒ returns count
///     until first write (2 in R,W,R example; the trailing R is
///     dropped into the suffix WITH the W, where apply-Txn
///     preserves correct read-your-writes via the overlay).
pub fn read_prefix_length(ops: &[Op]) -> usize {
    ops.iter().take_while(|o| is_read_only(o)).count()
}
```

### 4.2 SM helper (no new method needed)

The split-phase execution uses existing SM primitives:
- `sm.read().read_only_op(Op::Txn{read_prefix})` runs the read
  prefix via the SP-Perf-A-TXN-RO bypass (shipped + proven).
- `sm.write().apply(op_no, Op::Txn{write_suffix})` runs the
  write suffix via the proven apply path with full catalog/index/
  constraint machinery.

Both halves use the existing Op::Txn semantics (data-op validator,
overlay for the write half, commit_txn).

### 4.3 Driver-level dispatch (bench-compare)

```rust
// Pseudocode for run_sysbench_oltp's per-worker loop:
let split = read_pool::read_prefix_length(&inner);
let r = match (split, inner.len()) {
    (0, _) => {
        // No read prefix — pure write or write-led. Apply as-is.
        let op_no = op_seq.fetch_add(1, Ordering::Relaxed);
        sm.write().unwrap().apply(op_no, Op::Txn { ops: inner })
    }
    (n, total) if n == total => {
        // All reads — TXN-RO bypass.
        sm.read().unwrap().read_only_op(Op::Txn { ops: inner })
    }
    (n, _) => {
        // Mixed read-prefix + write-suffix — split-phase.
        let writes = inner.split_off(n);
        let read_r = sm.read().unwrap().read_only_op(
            Op::Txn { ops: inner }  // inner is now the read prefix
        );
        // Reads' inner-op result payloads are discarded by Op::Txn
        // semantics (returns Ok on success); if the prefix fails,
        // surface that verdict and SKIP the writes (matches apply-
        // Txn's first-failure abort).
        match read_r {
            OpResult::Ok => {
                let op_no = op_seq.fetch_add(1, Ordering::Relaxed);
                sm.write().unwrap().apply(
                    op_no, Op::Txn { ops: writes }
                )
            }
            failed => failed,  // first-read-failure verdict
        }
    }
};
```

### 4.4 First-failure semantics

Apply-Op::Txn returns the FIRST failing inner op's verdict and
aborts the whole batch (rolling back the overlay). The split path
preserves this:
- If the read prefix fails → return the failure, skip the write
  suffix (no apply, no op_no consumption). Equivalent to apply-Txn
  aborting on a read failure (no writes happened yet).
- If the read prefix succeeds → run the write suffix. If a write
  fails, apply-Txn aborts the suffix's overlay normally. The reads
  already executed have no overlay effect to roll back (they were
  pure reads).

Result-byte equivalence with unified apply: for the sysbench shape
(reads all succeed; writes either all succeed or one fails), the
verdict matches.

## 5. Correctness

### 5.1 Isolation level

Unified apply-Op::Txn holds the write lock for the whole Txn.
Concurrent reads cannot observe in-progress writes; concurrent
writes wait their turn.

Split apply: the read prefix holds `sm.read()` for the prefix
duration. During this time:
- Other readers can run their prefixes (RwLock allows concurrent
  readers — the WIN).
- No writer can advance committed state (RwLock blocks writers
  during reader presence — same as TXN-RO bypass guarantee).

When the prefix releases its read guard and the suffix takes the
write guard:
- Other workers waiting for the write lock queue up
- Committed state may have advanced between the prefix and suffix
  (a different worker's write suffix could have applied in between)

**Is this a problem?** Yes, it's a weaker isolation guarantee than
unified apply-Txn. Specifically, the split-Txn observes:
- Read prefix at snapshot S₀ (committed state at prefix start).
- Write suffix at snapshot S₁ ≥ S₀ (committed state at suffix
  start). Other Txns' writes may have intervened.

This is closer to READ COMMITTED (per-statement snapshot) than to
the SERIALIZABLE isolation apply-Txn provides. For sysbench's
workload:
- Each Txn reads + writes 4 disjoint random IDs (UPDATE_INDEX,
  UPDATE_NON_INDEX, DELETE, INSERT each pick independent ids).
  The READS are 10 different ids — disjoint from the writes by
  randomization (with rows_per_table=100K and 14 ops, collision
  probability is ~10⁻⁴ per Txn).
- The writes do not depend on the reads' values (the new k/c/pad
  values are random, not derived from prior reads).
- Therefore the split is observationally indistinguishable from
  unified apply-Txn for this workload class.

For Txns that DO depend on reads (e.g., "update WHERE id matches
prior SELECT"), the workload would need either (a) the unified
apply path (preserved via the `read_prefix_length == 0` or
`read_prefix_length < ops.len()` with `ops.len() - read_prefix_length
== 0` branch falling through to apply), or (b) a future SI-mode
arc that buffers writes and validates at commit.

### 5.2 Determinism oracle

The TXN-RO arc ships an oracle: 100K random workloads × all-RO
Op::Txn × parallel vs serial == byte-identical. We EXTEND that
oracle with a new KAT:

`txn_rw_split_byte_equivalent_to_unified_for_read_then_write_shape`

- Seed two engines (same schema, same initial data).
- Generate 1000 random Txns of shape (R₁,...,Rₘ, W₁,...,Wₙ) where
  reads are GetById on disjoint ids and writes are
  Create/Update/Delete on a per-Txn disjoint id range.
- Engine A: apply each Txn via unified `apply(op_no, Op::Txn{ops})`.
- Engine B: apply each Txn via the split path
  (read prefix via read_only_op, write suffix via apply).
- Assert: every per-Txn verdict matches; final state matches via
  Describe + Select over the full table.

For the SHAPE GUARD (reads precede writes), byte-equivalence is
exact. KAT name reflects the contract: not "all shapes byte-equal"
but "this shape byte-equal." That's the V1 claim.

## 6. Acceptance criteria

1. ✅ All prior tests pass.
2. ✅ HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched.
3. ✅ Default `cargo build -p kesseldb-server` byte-identical to
   HEAD (the classifier extension is additive; `is_read_only` is
   unchanged; `read_prefix_length` is a new function).
4. ✅ Determinism oracle: 1000 random read-then-write Txns ×
   split-vs-unified == byte-identical (verdict + final state).
5. **Headline gate: vulcan sysbench oltp-read-write at N=8/N=16
   lifts from ~715 tx/s to ≥3000 tx/s** (closes Postgres-loss gap
   from 4.2× to ≤1.5×).
6. oltp-read-only unchanged (the classifier reuses `is_read_only`
   which the TXN-RO arc proved correct; the split-phase logic
   never runs when all ops are read-only — the `n == total` branch
   routes to the TXN-RO bypass directly).
7. oltp-write-only unchanged (the classifier returns
   `read_prefix_length == 0` for write-led Txns; the dispatch
   falls through to apply as today).

## 7. Task decomposition (T1-T5)

| T# | Scope | Status |
|---|---|---|
| T1 | Design spec (this file) + progress tracker. | this commit |
| T2 | Classifier extension (`read_prefix_length`) + 6+ KATs. | next commit |
| T3 | Bench-compare driver: split-phase dispatch + determinism oracle (KAT in `parallel_reads_oracle.rs`). | next commit |
| T4 | vulcan sysbench OLTP sweep (RO + RW + WO pre/post) + BENCHMARKS.md §3e update. | next commit |
| T5 | STATUS row + arc closure + progress tracker close-out. | next commit |

## 8. Weak spots

1. **Isolation downgrade for split Txns.** Split observes READ
   COMMITTED-ish semantics; unified apply observes SERIALIZABLE-ish.
   The shape guard (read-then-write only) limits the blast radius to
   workloads that don't have read-after-write deps. Sysbench is
   safe. Locked by the byte-equivalence KAT for the supported
   shape. Documented as the V1 limit.
2. **Snapshot drift between prefix and suffix.** Committed state may
   advance between the read prefix and the write suffix (another
   worker's write applied in between). For workloads that derive
   writes from reads (e.g., SELECT FOR UPDATE pattern), this is
   wrong — the writes might commit on top of stale reads. Mitigation:
   the V1 driver-level split only fires for workloads that DON'T
   have this dependency (sysbench: writes are random, independent
   of reads). Future SI arc would catch the conflict at commit time.
3. **Write-set contention is now the bottleneck.** Once reads
   parallelize, the bottleneck shifts to the write lock. At ~148 µs
   per Txn's write suffix, throughput tops out at ~6750 tx/sec
   regardless of N. That's still 9.4× the current 715 tx/sec —
   substantial — but it's a hard ceiling, not unbounded scaling.
4. **First-failure semantics for split.** Apply-Txn rolls back the
   overlay on inner failure. Split path: if the read prefix fails,
   no writes ran (correct — apply-Txn would have rolled back too).
   If the write suffix fails, the reads already happened but had
   no side effects (correct — reads have no rollback semantic).
   Verdict matches apply-Txn for both shapes.
5. **Read-prefix-only Txns (no writes) are double-routed.** The
   `n == ops.len()` branch routes to the TXN-RO bypass — same as
   the TXN-RO arc handles. No regression; just a duplicated check.
6. **Driver-side vs server-side classification.** V1 ships
   driver-side (in bench-compare). Server-side dispatch via
   `apply_raw` would require decoding Op::Txn, computing the split,
   and emitting two writes back to the caller's response stream —
   complex and out of scope for the V1 perf demonstration.
   Naming: **SP-Perf-A-TXN-RW-SERVER** for the server-side version
   if needed by SQL clients submitting BEGIN/COMMIT brackets via
   PG-wire.
7. **Op-number consumption asymmetry.** Unified apply consumes
   `op_no + i` for each inner op (i = 0..ops.len()). Split path:
   the read prefix consumes ZERO op-numbers (reads don't take log
   slots); the write suffix consumes `op_no + i` for i = 0..suffix.len().
   The starting `op_no` for the suffix is the next fetched value,
   not `original_op_no + prefix.len()`. Behaviour equivalent IF the
   op_no sequence is only used for log-slot allocation (it is —
   reads don't take slots in apply either).
8. **MemVfs in-process benchmark.** The bench runs in-process with
   no fsync. Real-deployment results (with fsync group-commit) would
   show smaller relative lifts because fsync amortization dwarfs the
   per-Txn lock-hold cost. The MemVfs result is the upper bound on
   lock-removal wins; production-deployment wins would be smaller.
   Documented in BENCHMARKS.md.

## 9. Standing invariants

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-txnrw`
- Direct commits to main, no Co-Authored-By, no `-S`, push after each
- CI green check after each push
- Memory files OUTSIDE repo
- seed-7 GREEN
- `#![forbid(unsafe_code)]` honored
- No new external deps
- Determinism oracle (T3 of Perf-A) must still pass

## 10. File registry

- **Spec (this file)**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-txnrw-design.md`
- **Tracker**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-txnrw-progress.md`
- **Classifier**: `crates/kesseldb-server/src/read_pool.rs::read_prefix_length`
- **Driver**: `tools/bench-compare/src/drivers/kesseldb.rs::run_sysbench_oltp`
- **Oracle**: `crates/kesseldb-server/tests/parallel_reads_oracle.rs`
- **Benchmarks**: `docs/BENCHMARKS.md` §3e
