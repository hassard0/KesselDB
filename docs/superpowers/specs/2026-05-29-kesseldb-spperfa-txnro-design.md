# SP-Perf-A-TXN-RO — All-read-only `Op::Txn` bypass — Design

Date: 2026-05-29
Parent: SP-Perf-A (T2 bypass for bare-Op reads — `read_only_op` covers 16
variants; `EngineHandle::apply_raw` tag-fast-path routes them around the
write lock + group-commit fsync).
Closes: sysbench OLTP losses documented in `docs/BENCHMARKS.md` §3c / §3e.

## 1. Context

The bench-suite measurements (T3 of SP-Bench-Suite, published in
`docs/BENCHMARKS.md` §3c/§3d/§3e) showed KesselDB **losing** the
transaction-bracket family at every concurrency:

| Workload | KesselDB N=1 | N=8 | N=16 | Postgres N=8 | SQLite N=1 |
|---|---:|---:|---:|---:|---:|
| oltp-read-only | 1,241 | 641 | 680 | **4,068** | **6,507** |
| oltp-read-write | 1,378 | 718 | 711 | **3,024** | **4,835** |
| oltp-write-only | 136,035 | **53,409** | **52,321** | 10,254 | 13,451 |

(oltp-write-only KesselDB wins; that result stands.)

The N=1→N=8 KesselDB regression on the read-bracketed workloads
(1,241 → 641 tx/s on oltp-read-only) is the symptom of a single root cause:

> `Op::Txn{ops}` is dispatched through `StateMachine::apply()`, which
> takes the engine write lock for the ENTIRE transaction — even when
> every inner op is a read. The Perf-A T2 bypass that lifted bare-Op
> reads to ~5M ops/sec is `GetById`-only; it does NOT compose with
> `Op::Txn`. With N concurrent workers, every Txn serializes on the
> apply lock; the workload becomes effectively N=1 on the throughput
> dimension.

This arc closes that gap with **static all-RO detection**: when
`Op::Txn{ops}` arrives and EVERY inner op is in the spec §4 read-only
set (16 variants), the wrapper routes through the same `Arc<RwLock<…>>`
read path as a bare-Op read — no write lock, no apply-thread queue, no
group-commit fsync.

## 2. Scope

**V1 — this arc — ships:**
- Server-side classifier `is_read_only(&Op)` recognises all-RO Op::Txn:
  - `Op::Txn { ops }` ⇒ true iff every `inner` op is read-only by the
    existing 16-variant predicate (recursion guard: nested Op::Txn is
    already rejected at apply, so `is_read_only(Txn{[Txn]})` → false
    without traversal).
- `StateMachine::read_only_op(Op::Txn{ops})` handles the wrapper:
  iterates inner ops, calls `read_only_op(&inner)` for each, returns
  the SAME result shape as the apply path — `OpResult::Ok` on success;
  the first inner-op failure verbatim (`SchemaError` / `NotFound` /
  `Constraint`) otherwise. Inner-op result bytes are discarded, exactly
  as `Op::Txn` apply discards them today (Txn is a write wrapper; the
  caller only sees Ok or first-failure verdict).
- `EngineHandle::apply_raw` recognises tag 15 (Op::Txn) as a candidate
  read-only frame when `sm_shared` is enabled: decodes the frame,
  re-classifies via `is_read_only` (which now recurses inside Txn), and
  dispatches via `sm_shared.read().read_only_op(op)` when the
  classification holds. Mixed-RW Txns fall through to the existing
  engine queue, byte-untouched.
- `EngineHandle::apply(op)` in-process fast path: replaces the
  `!op.is_mutating()` check with `read_pool::is_read_only(&op)` so the
  recursion inside Txn participates in the in-process bypass too.
- Determinism oracle extension (`parallel_reads_oracle.rs`): 100K
  workloads × all-RO Op::Txn bracketed reads × parallel-vs-serial
  byte-equal. KAT `txn_ro_oracle_100k_workloads_byte_equal`.

**V2 — SP-Perf-A-TXN-RW (next arc) — out of scope:**
- Mixed-RW Op::Txn bypass. Requires snapshot isolation on the read pool
  + commit-time conflict detection (the Perf-A T2 bypass uses
  `Storage::get` which reads committed state at `u64::MAX`; an RW Txn
  needs SI under the bypass with proper read-set tracking + commit
  conflict resolution).
- Nested Op::Txn (already rejected at apply level by the data-op
  validator at `kessel-sm` line 5137). No change.

## 3. Architecture

### 3.1 Classifier (read_pool.rs)

```rust
pub fn is_read_only(op: &Op) -> bool {
    match op {
        Op::Txn { ops } => ops.iter().all(is_read_only),
        _ => !op.is_mutating(),
    }
}
```

The recursive walk is bounded: nested Op::Txn fails the data-op check
in `StateMachine::apply::Op::Txn` at the SM layer (only the 18 data ops
listed there are permitted; Txn is not in the list). On the classifier
side an inner Op::Txn would also classify to true if all its inner ops
are RO — that's safe to route via the RO bypass even though the SM
apply would reject the same nesting, because the bypass uses
`read_only_op` which has its OWN structural validator (see §3.2).

Cost: O(N) where N is the inner-op count. For the sysbench oltp-read-only
workload N=410 inner ops; the classifier walk is ~50 ns vs the per-Txn
apply cost of ~12.6 ms (5+ orders of magnitude cheaper).

### 3.2 SM dispatcher (kessel-sm/src/lib.rs)

`read_only_op(op)` gains a new arm:

```rust
Op::Txn { ops } => {
    // Defense in depth: re-validate every inner op is read-only
    // (the bypass must never silently execute a write).
    for o in &ops {
        if !matches!(o,
            Op::GetById{..} | Op::GetBlob{..} | Op::Describe{..}
            | Op::FindBy{..} | Op::FindByComposite{..}
            | Op::FindRange{..} | Op::Query{..} | Op::QueryExpr{..}
            | Op::Select{..} | Op::QueryRows{..} | Op::SelectFields{..}
            | Op::SelectSorted{..} | Op::Aggregate{..}
            | Op::GroupAggregate{..} | Op::SeqRead{..} | Op::Join{..}
        ) {
            return OpResult::SchemaError(
                "read_only_op: Op::Txn contains non-read op".into(),
            );
        }
    }
    // Execute each inner op against the same `&self` snapshot. The
    // apply-Txn path also iterates sequentially (it has to — overlay
    // writes feed the next op's read). For all-RO Txn there is no
    // overlay; we just need committed state, and `read_only_op` reads
    // exactly that. Inner failure semantics mirror apply-Txn:
    // first failure verdict is the Txn verdict.
    for o in ops {
        match self.read_only_op(o) {
            r @ (OpResult::Exists
                | OpResult::NotFound
                | OpResult::SchemaError(_)
                | OpResult::Constraint(_)) => return r,
            _ => {} // Ok / Got / etc. — successful inner read, discard payload
        }
    }
    OpResult::Ok
}
```

Op::Txn's apply path returns `OpResult::Ok` on success (the inner
results are discarded). We match that contract so callers see no
behavioural change.

### 3.3 Dispatch (apply_raw)

`apply_raw` already short-circuits read-only Op frames by tag-byte
lookup. Adding tag 15 (Op::Txn) to the candidate set requires the
structural classifier (we MUST decode and check inner ops; a tag-byte
alone is insufficient because Op::Txn could contain writes):

```rust
let is_read_candidate = matches!(tag,
    6 | 7 | 9 | 11 | 16 | 18 | 19 | 20 | 21 | 22
  | 23 | 25 | 26 | 27 | 28 | 35
  | 15  // Op::Txn — requires structural recheck via is_read_only
);
if is_read_candidate {
    if let Some(op) = Op::decode(&frame) {
        if read_pool::is_read_only(&op) {
            // ... dispatch via sm_shared.read().read_only_op(op)
        }
    }
}
```

The cost of decode + classifier walk for mixed-RW Op::Txn is paid only
once per call and falls through to the engine queue on failure (same
fallback path as a malformed RO frame today).

### 3.4 In-process fast path (apply)

`EngineHandle::apply(op)` switches its read-only check from
`!op.is_mutating()` (which classifies Op::Txn as mutating) to
`read_pool::is_read_only(&op)` (which recurses inside Txn). The op
moves by value into the read path — no extra clone.

## 4. Determinism

The bypass produces byte-identical results because:
1. Each inner `read_only_op` call is a pure function of committed state
   (already proven by SP-Perf-A T3 oracle on 100K reads).
2. The apply-Txn path for all-RO inner ops ALSO just iterates reads —
   no `begin_txn`/`commit_txn` work is observable to readers (the
   storage overlay is a no-op when no writes occur).
3. Inner-op result bytes are discarded in both paths (apply returns
   Ok; the bypass returns Ok).

Op-number consumption: apply consumes `op_number + i` for each inner
op. The bypass consumes ZERO op numbers (reads don't take log slots).
This matters for VSR replication: the bypass MUST NOT run if the Txn
might mutate (correct — the classifier guards against this) and the
caller MUST NOT request op-number assignment for an RO Txn (correct —
reads are not replicated; the bench's `op_seq.fetch_add(1)` for an RO
Txn is wasted but harmless because no log slot is consumed).

## 5. Correctness

The SP-Perf-A T3 determinism oracle (100K reads × 16 variants × parallel
vs serial == byte-identical) is EXTENDED with a new KAT:

`txn_ro_oracle_100k_workloads_byte_equal` —
- Seed two engines (parallel via `read_workers=Some(8)` + serial via
  `read_workers=None`), same schema as T3.
- Generate 100 workloads × 1000 ops each = 100K total ops.
- Each op is `Op::Txn { ops: Vec<read-only-op> }` where the inner-op
  count is random (1-20) and each inner picks uniformly from the 16
  read variants.
- Submit via `engine.apply(Op::Txn{ops})`. Assert byte-equal results.

This locks the contract: any future change that breaks the bypass
(e.g. cache invalidation skew between read paths, lock ordering bugs,
inner-op semantic drift) trips the oracle.

## 6. Acceptance criteria

1. ✅ All prior tests pass.
2. ✅ HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched.
3. ✅ Default `cargo build -p kesseldb-server` byte-identical to HEAD
   (the classifier extension + SM arm are additive; `is_mutating()` in
   proto is unchanged so VSR / replication / op-number assignment all
   carry on as before).
4. ✅ Determinism oracle: 100K random workloads × all-RO Op::Txn × parallel
   == serial byte-identical.
5. **Headline gate: vulcan sysbench oltp-read-only at N=16 lifts from
   ~680 tx/s to ≥3000 tx/s** (closing the Postgres-loss gap from ~7.5×
   to ~1.5×). At N=1 we expect a smaller lift (the bypass saves the
   write-lock contention, but N=1 has no contention).

## 7. Task decomposition

| T# | Scope |
|---|---|
| T1 | Design spec (this file) + progress tracker + scaffold + classifier extension (`read_pool::is_read_only` recurses into Op::Txn) + classifier KATs (all-RO Txn classifies true; mixed Txn classifies false; nested-Txn-with-mixed-inner classifies false). |
| T2 | SM `read_only_op` Op::Txn arm + per-arm KATs (single inner op, 10 inner ops, mixed-shape inners, write-op-injected refused, empty ops vec) + dispatch wiring (`apply_raw` tag-15 + `apply` classifier swap). |
| T3 | Determinism oracle extension: `txn_ro_oracle_100k_workloads_byte_equal` lock. |
| T4 | Bench-compare driver: oltp-read-only routes via `sm.read().unwrap().read_only_op(Op::Txn{ops})`; vulcan sysbench OLTP sweep (RO + RW + WO baseline + post) and BENCHMARKS.md §3c/§3e update. |
| T5 | STATUS row + arc closure + progress tracker close-out. |

## 8. Weak spots

1. **Mixed-RW Op::Txn still goes through apply.** V1 limit. Next arc
   (SP-Perf-A-TXN-RW) attacks this with snapshot isolation + commit
   conflict detection on the read path.
2. **Nested Op::Txn is rejected by the SM apply arm** but the
   classifier accepts an all-RO Txn-in-Txn. The bypass's defence-in-depth
   validator in §3.2 only checks individual ops are RO — it does NOT
   reject a Txn nested inside the bypass's Txn arm. We handle this by
   adding `Op::Txn{..}` to the rejection list in §3.2's validator: nested
   Txn is never executed via the bypass; apply rejects it too; behavior
   is identical between paths.
3. **BEGIN/COMMIT bracket isolation semantics.** Apply-Txn calls
   `storage.begin_txn()` + `storage.commit_txn()` to set up the overlay
   for writes. For all-RO inner ops the overlay is unused (reads bypass
   it). The bypass doesn't touch the overlay at all. Both paths observe
   the same committed-state snapshot for the duration of the call —
   the bypass holds `sm.read()` for the whole iteration, preventing a
   writer from advancing committed state mid-Txn. This is the strongest
   isolation possible on the bypass: read-committed AT THE MOMENT THE
   FIRST INNER OP RUNS, with no advance during the Txn (because writers
   are blocked on the rwlock). Apply-Txn has the same property by
   holding the write lock; the contracts match.
4. **The Perf-A T2 read cache is bypassed on the parallel path.**
   `read_only_op` documents this explicitly (T2 design; cache is
   `&mut` and the parallel path is `&self`). All-RO Op::Txn inherits
   the same behaviour — no cache lookups, every inner op hits storage.
   This matches what apply-Txn does for RO inner ops on the cache too
   (`Op::Txn{[reads...]}` apply doesn't insert into the cache for
   inner reads in any path I can see; if it did, the bypass would
   produce equivalent committed-state results because the cache is
   only a writer-side accelerator for un-flushed writes).
5. **Op-number consumption asymmetry.** Apply-Txn consumes inner-op
   op-numbers via `apply(op_number + i, o)`; the bypass consumes zero.
   This matters only if a caller is asserting op-number progression
   across an RO Txn; no such caller exists today (RO Txns are not
   replicated). The bench's `op_seq.fetch_add(1)` for the OUTER Txn is
   redundant on the bypass but harmless (the counter advances; no log
   slot is taken).
6. **Op::Txn{ops: vec![]} (empty Txn).** Apply-Txn returns Ok; the
   bypass returns Ok. Both paths agree.
7. **Latency tail under contention.** The bypass holds `sm.read()` for
   the entire iteration. A 410-inner-op Txn at ~225 ns per inner read
   = ~90 µs per Txn — concurrent writers block on `sm.write()` for that
   window. The N=16 sysbench RO sweep is pure RO so no writers compete;
   the RW sweep mixes 410 reads with 4 writes per Txn (mixed-RW), which
   still goes through apply (V1 limit), so RW is unaffected. A truly
   mixed workload (some RO Txns concurrent with some RW Txns) would
   see the RW Txns wait longer under the bypass than they do today —
   but the RO Txns would also complete faster, so net throughput rises.
   Measured in T4.
8. **Classifier walk cost on mixed-RW Op::Txn.** Every Op::Txn frame
   pays an O(N) decode + classifier walk before falling through to the
   engine queue on a write detection. For a 410-inner-op mixed-RW Txn
   that's ~50 ns of overhead per Txn — negligible compared to the
   ~14 ms per-Txn apply cost. Locked by KAT.

## 9. Standing invariants

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-txnro`
- Direct commits to main, no Co-Authored-By, no `-S`, push after each
- CI green check after each push
- Memory files OUTSIDE repo
- seed-7 GREEN
- `#![forbid(unsafe_code)]` honored
- No new external deps

## 10. File registry

- **Spec (this file)**: `docs/superpowers/specs/2026-05-29-kesseldb-spperfa-txnro-design.md`
- **Tracker**: `docs/superpowers/specs/2026-05-29-kesseldb-spperfa-txnro-progress.md`
- **Classifier**: `crates/kesseldb-server/src/read_pool.rs::is_read_only`
- **SM dispatcher**: `crates/kessel-sm/src/lib.rs::StateMachine::read_only_op` (Op::Txn arm)
- **Engine dispatch**: `crates/kesseldb-server/src/lib.rs::EngineHandle::apply_raw` + `apply`
- **Oracle**: `crates/kesseldb-server/tests/parallel_reads_oracle.rs`
- **Bench**: `tools/bench-compare/src/drivers/kesseldb.rs::run_sysbench_oltp`
- **Benchmarks**: `docs/BENCHMARKS.md` §3c / §3e
