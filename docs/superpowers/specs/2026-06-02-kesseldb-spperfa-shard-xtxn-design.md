## SP-Perf-A-SHARD-XTXN — cross-shard transaction routing — design spec

Date: 2026-06-02
Author: Track B (SP-Perf-A-SHARD-* family)
HEAD on main when this slice opened: `f8b9bd1` (post-SP-PG-EXTQ-BIN-NUMERIC arc closure)
Parent arc: SP-Perf-A-SHARD-APPLY (V1 SHIPPED — K=N apply path delivers 3.19× lift at K=8 on vulcan, 14.93M ops/sec for `get-by-id`).

The parent arc's progress tracker closed SHARD-APPLY DONE while
explicitly naming this follow-up:

> **SP-Perf-A-SHARD-XTXN** — Cross-shard atomic txns via XSHARD
> keyspace 2PC. V1 routes Op::Txn to shard 0 only.

That V1 routing is INCORRECT when the inner ops touch keys hashing to
different shards. `route_op` in `sharded_engine.rs` currently maps
every `Op::Txn{ops}` to `ShardRoute::ShardZero`:

```rust
Op::Txn { .. }
| Op::CommitTx { .. }
| Op::XshardApply { .. }
| Op::XshardDecide { .. }
| Op::XshardCommit { .. }
| Op::AdvanceWatermark { .. }
| Op::ReportActiveSnapshot { .. } => ShardRoute::ShardZero,
```

Shard 0's `StateMachine::apply_op(Op::Txn{ops})` runs each inner op
against shard 0's storage. If an inner op (Create / Update / Delete /
GetById / GetBlob / UpdateSet) targets a key whose `shard_of_key(key,
K)` is some other shard `s ≠ 0`, that op:

- on Create: silently writes to shard 0's storage. The write
  (`type_id`, `id`) lives on the WRONG shard. A subsequent
  `Op::GetById` (correctly routed via `route_op` to shard `s`) hits
  shard `s` — which has no record of the row — and returns
  `NotFound`. **Silent data loss.**
- on Update / Delete: shard 0 returns `NotFound` (since the row
  isn't there); the whole txn rolls back. Visible failure, but a
  confusing one — the caller's row exists "somewhere" in the
  cluster, just not where this txn looked.
- on GetById / GetBlob: returns `NotFound` from shard 0; same false-
  miss as above. Read-your-writes semantics broken.

This arc closes the correctness gap. The honest V1 deliverable: a
**classifier** that walks each inner op's primary key, computes the
set of distinct shards touched, and routes:

- **Empty txn** (`ops.len() == 0`): single-shard fast-path to shard
  0 (no-op semantics; apply-Txn arm returns `Ok` after a no-op
  commit).
- **Single-shard txn** (every keyed inner op lands on the same
  shard `s`; no scan-shape inner ops): route to shard `s` only.
  Full atomic semantics via shard `s`'s state-machine apply
  thread (its `StateMachine::apply` Op::Txn arm does begin_txn /
  per-op apply / commit_txn or abort_txn).
- **Multi-shard txn** (two or more distinct shards touched, OR
  any inner op has no extractable primary key — scan-shape ops
  like FindBy / Select / Aggregate / Describe / Query / etc.):
  reject with `OpResult::SchemaError(...)` carrying the typed
  cross-shard message + named V2 follow-up `SP-Perf-A-SHARD-XTXN-2PC`.

K=1 deployments are unchanged: at K=1 every `Op::Txn` is single-
shard by definition (every key folds to shard 0), so the classifier
returns `Single(0)` and the dispatch path is byte-identical to pre-
arc behavior.

---

### 1. Context — the V1 routing bug

#### 1.1 What SHARD-APPLY shipped

`crates/kesseldb-server/src/sharded_engine.rs::route_op` classifies
each `Op` into a `ShardRoute`:

```rust
pub enum ShardRoute {
    Single(usize),
    Broadcast,
    ShardZero,
    Scatter(ScatterKind),
}
```

- `Single(s)` — point-data ops route to their key's owning shard.
- `Broadcast` — DDL applies to every shard.
- `ShardZero` — admin / Txn / XSHARD / Join → shard 0.
- `Scatter(kind)` — scan-shape ops fan out across all K shards.

The SHARD-APPLY arc (commit `76d5a50`) labeled `Op::Txn` as a
"V1 limitation, named SHARD-SCAN follow-up" but the actual follow-
up arc that owns Op::Txn is THIS one — SHARD-XTXN. SHARD-SCAN
covered scan-shape ops (which now Scatter correctly via that arc).

#### 1.2 Concrete reproducer (K=4)

```rust
// Two rows whose keys hash to different shards at K=4:
//   key_a = make_key(1, ObjectId::from_u128(1))   → shard 1
//   key_b = make_key(1, ObjectId::from_u128(2))   → shard 3
//
// Currently (pre-XTXN):
//   engine.apply(Op::Txn { ops: vec![
//     Op::Create { type_id: 1, id: ObjectId::from_u128(1), record: r1 },
//     Op::Create { type_id: 1, id: ObjectId::from_u128(2), record: r2 },
//   ]})
// → route_op returns ShardZero
// → shard 0 applies the Op::Txn arm, writes BOTH rows to shard 0's storage
// → engine.apply(Op::GetById { type_id: 1, id: ObjectId::from_u128(1) })
//   → route_op returns Single(1)
//   → shard 1 reads its (empty) storage → NotFound
// → Silent data loss visible at read time.
```

The shard_of_key values above are illustrative; the actual mapping
depends on the FxHash fold. The point: a K-shard deployment that
accepts an Op::Txn whose keys spread across shards silently writes
to the wrong shard and breaks read-your-writes.

#### 1.3 Why a SchemaError reject is the honest V1

Option A (single-shard fast path + multi-shard 2PC) is the
production-quality fix but requires:

- A prepare phase: each touched shard validates its slice of the
  txn (decode, schema-check, lock-acquire) without committing.
- A decide phase: the coordinator collects every shard's prepare
  outcome; if all OK, broadcast commit; if any fail, broadcast
  abort.
- A failure model: what happens if a shard crashes between
  prepare and commit? Recovery via WAL replay + idempotent
  commit/abort tokens.

That's a multi-arc project on its own (named here as
`SP-Perf-A-SHARD-XTXN-2PC`). Shipping a broken V1 is worse than
rejecting cleanly: silent data loss is the worst kind of bug a
database can have. A typed error with a clear message lets
application code detect the limitation, partition transactions
client-side, and migrate when the 2PC arc lands.

Single-shard transactions ARE the common case for sharded
deployments: the canonical pattern (uuid or sequential-id
primary key + every dependent row carries that PK as FK) means
every related row hashes to the same shard. The fast-path
single-shard route is the ≥90%-of-workloads correct answer.

---

### 2. Scope

#### 2.1 In-scope (V1)

1. **`ShardRoute::CrossShardReject` variant** — new arm of the
   `ShardRoute` enum carrying the typed reject reason.
2. **`extract_txn_inner_pkey_shard(op: &Op, k: usize) ->
   Option<usize>`** helper — for a single inner op, returns
   `Some(shard)` if the op has a primary key, `None` if scan-
   shaped (no single owning shard).
3. **`route_op` Op::Txn arm**:
   - Walk every inner op.
   - If `ops.is_empty()` → `ShardRoute::Single(0)`.
   - If any inner op has no primary key (scan-shape) → reject.
   - If the set of touched shards has cardinality 1 → route to
     that shard.
   - If the set has cardinality ≥ 2 → reject.
4. **Dispatcher arm** — `apply_raw` matches the new
   `CrossShardReject` route and returns `OpResult::SchemaError`
   with the cross-shard message.
5. **KATs**:
   - Single-shard Op::Txn at K=4 routes to the correct shard
     (deterministic, type_id+id-derived).
   - Multi-shard Op::Txn at K=4 returns SchemaError.
   - Empty Op::Txn routes to Single(0).
   - K=1 Op::Txn always classified Single(0) (regardless of
     inner op count or shape).
   - Op::Txn with a scan-shape inner op (e.g. Describe, FindBy,
     Select) → CrossShardReject (V1 limit).
   - K=4 end-to-end: single-shard Op::Txn write-then-read round-
     trips correctly through the dispatcher.
   - K=4 end-to-end: multi-shard Op::Txn returns SchemaError
     WITHOUT modifying any shard's storage (atomicity preserved
     via reject-before-apply).
6. **Determinism oracle extension** — `sharded_engine.rs` test
   `t2_determinism_oracle_k1_k4_k8_byte_equal` extended with an
   Op::Txn block: single-shard Op::Txn at K=1/K=4/K=8 produces
   byte-identical OpResult; cross-shard Op::Txn at K=4 returns
   SchemaError (and at K=1 the SAME Op::Txn succeeds).

#### 2.2 Out-of-scope (V2 — `SP-Perf-A-SHARD-XTXN-2PC`)

1. **Two-phase commit across shards** — prepare/decide/commit
   phases coordinated via the existing XSHARD reserved keyspace
   (0xFFFF_FFF1) the cluster router already uses for cross-
   node txns. The in-process equivalent would lock per-shard
   apply threads in lockstep.
2. **Snapshot consistency for cross-shard reads inside a txn** —
   even with 2PC writes, a cross-shard READ snapshot needs MVCC
   coordination so every shard reads at the same `seq`. Named
   `SP-Perf-A-SHARD-SNAPSHOT`.
3. **Cross-shard `XshardApply` / `XshardDecide` / `XshardCommit`
   routing** — these XSHARD ops are part of the cluster-router
   2PC protocol; they're parked in ShardZero in the current
   in-process dispatch path because in-process sharding has no
   external coordinator. The V2 arc decides whether to repurpose
   them for in-process 2PC or introduce new internal ops.

---

### 3. Acceptance criteria

**V1 (this arc):**

1. **Single-shard Op::Txn correctness** — at any K ≥ 1, an
   Op::Txn whose inner ops' keys all map to a single shard `s`
   applies atomically against shard `s`'s state machine. Reads
   for any of those keys after commit see the new values; on
   failure, no row is modified. Byte-identical to K=1 result.
2. **Multi-shard Op::Txn rejected without data loss** — at
   K ≥ 2, an Op::Txn whose keys span ≥ 2 shards returns
   `OpResult::SchemaError("cross-shard transaction not
   supported in V1 — see SP-Perf-A-SHARD-XTXN-2PC")` WITHOUT
   writing to any shard. Per-shard `applied_ops_snapshot()`
   counter MUST be unchanged from pre-Op::Txn snapshot.
3. **K=1 byte-identical** — at `shard_count = None` or
   `Some(1)`, the classifier always returns `Single(0)`. The
   dispatcher's behavior is byte-identical to pre-XTXN.
4. **Determinism oracle (T3 SP-Perf-A) extended** — the K=1/K=4/
   K=8 oracle in `sharded_engine.rs::tests` covers Op::Txn for
   single-shard cases (byte-equal across K) and cross-shard
   cases (SchemaError at K≥2; success at K=1).
5. **Workspace tests green** — `cargo test --workspace` passes;
   no regression in the 2442-test baseline.
6. `#![forbid(unsafe_code)]` honored; zero new external deps.

**V2 (`SP-Perf-A-SHARD-XTXN-2PC`):**

1. Multi-shard Op::Txn applies atomically across N shards via
   2PC prepare/commit.
2. Cross-shard reads inside a txn see a consistent snapshot.
3. Recovery: shard crash mid-2PC replays cleanly via WAL.

---

### 4. The classifier — `extract_txn_inner_pkey_shard`

For each inner op variant, extract the primary key and compute
its owning shard. Variants without a single owning key (scan-
shape, DDL, admin) return `None` — the outer classifier treats
this as "must reject the whole txn".

| Inner op             | Primary key                          | Notes                                  |
|----------------------|--------------------------------------|----------------------------------------|
| `Op::Create`         | `(type_id, id)` → `make_key`         | Single shard                           |
| `Op::Update`         | `(type_id, id)` → `make_key`         | Single shard                           |
| `Op::UpdateSet`      | `(type_id, id)` → `make_key`         | Single shard                           |
| `Op::Delete`         | `(type_id, id)` → `make_key`         | Single shard                           |
| `Op::GetById`        | `(type_id, id)` → `make_key`         | Single shard                           |
| `Op::GetBlob`        | `(OVERFLOW, handle)` → `make_key`    | Single shard (overflow keyspace)       |
| `Op::SeqRead`        | None (scan)                          | Reject — scan-shape                    |
| `Op::SeqAppend`      | Fixed seq keyspace                   | Could classify, but V1 conservative: None |
| `Op::SeqAppendOnce`  | Fixed seq keyspace                   | Same as above                          |
| `Op::FindBy`         | None (secondary scan)                | Reject — scan-shape                    |
| `Op::FindByComposite`| None                                 | Reject                                 |
| `Op::FindRange`      | None                                 | Reject                                 |
| `Op::Describe`       | type-pinned shard (per route_op)     | V1 conservative: None (not a data op)  |
| `Op::Query`          | None                                 | Reject                                 |
| `Op::QueryExpr`      | None                                 | Reject                                 |
| `Op::Select*`        | None                                 | Reject                                 |
| `Op::Aggregate*`     | None                                 | Reject                                 |
| `Op::Join`           | None                                 | Reject                                 |
| anything else        | None                                 | Reject — defensive                     |

The conservative-None policy for SeqRead/SeqAppend/Describe is
intentional: these ops route to specific shards in standalone
mode, but their interaction with the atomic txn boundary is
complex enough that V1 punts. The apply-Txn arm in kessel-sm
already rejects SeqRead inside a txn (smoke test 7 in the oracle),
so this is consistent. Describe is allowed by apply-Txn but
practically isn't useful inside a write-txn — clients can issue
it standalone.

If a future workload needs SeqAppend / Describe inside a txn,
the V2 arc can relax the classifier without changing the wire
shape.

---

### 5. Determinism

Within a single shard, the apply-Txn arm preserves the existing
deterministic-replay contract: each shard's WAL records the
Op::Txn (encoded as one log entry), recovery replays it via
`StateMachine::apply(Op::Txn{ops})` → begin_txn / per-op apply
/ commit_txn (or abort_txn on failure). Cross-shard ordering
between Op::Txn dispatches that land on different shards is
not defined (each shard's apply thread serializes its own
input independently) — same as every other write at K≥2.

The cross-shard REJECT path is purely a dispatch-layer error:
no inner op is decoded, no storage is touched, no log entry is
written. The rejection is deterministic (same Op::Txn → same
classification → same SchemaError reply), so replica-replay
sees the same reject decision on every replica.

---

### 6. 5 weak-spots (the things this design can break)

1. **Workload-shape sensitivity** — applications that mix
   unrelated rows in a single txn (e.g. "credit user A AND
   debit user B" where A and B are independent uuids) will hit
   the reject path. The V1 caveat MUST be documented in
   USAGE.md / a per-arc note so clients know to either (a)
   partition keys client-side (hash-collide A and B onto one
   shard via a tenant_id prefix or similar) or (b) drop the
   atomicity guarantee (apply A and B as separate txns;
   compensate on failure). The 2PC V2 closes this gap.
2. **Hot-shard skew under reject** — if a workload has many
   cross-shard txn attempts, every reject is a no-op cost but
   still saturates the dispatcher. Per-shard utilization
   becomes meaningless if 50% of attempts reject. Bench
   coverage is needed (named below as a SHARD-XTXN-BENCH
   follow-up).
3. **SQL-layer translation may produce cross-shard txns
   accidentally** — `BEGIN ... INSERT INTO orders ... INSERT
   INTO line_items ... COMMIT` issued through the PG-wire
   gateway compiles to one Op::Txn. If orders.id and
   line_items.order_id hash to different shards (the natural
   case unless we composite-hash by order_id everywhere), the
   txn rejects. SQL layer needs a SchemaError → friendly
   PG-wire ERROR row translation; named here as a follow-up
   for the SQL layer arc.
4. **Empty-txn semantics** — `Op::Txn { ops: vec![] }` at K=1
   returns `Ok` (apply-Txn's loop is a no-op + commit_txn). At
   K≥2 the classifier returns `Single(0)` for empty, so
   apply-Txn on shard 0 also returns Ok. Byte-equal across K.
   KAT-locked.
5. **Inner-op decode cost on reject path** — every rejected
   Op::Txn pays the cost of decoding the Op (already paid by
   `apply_raw` before route_op is called) plus walking
   `ops.iter()` once. For txns with hundreds of inner ops, this
   is a non-trivial scan — but it's bounded by the txn size the
   client sent, so no DoS risk. The reject path is faster than
   the shard-0 silent-write path was, so this is a strict
   improvement.

---

### 7. Locked invariants

1. **K=1 byte-identical** — `shard_count = None` ⇒ no route
   change; `Some(1)` ⇒ classifier always returns `Single(0)`.
2. **No data loss on cross-shard reject** — the dispatcher
   MUST NOT invoke `shards[i].apply_raw` for any shard `i`
   when returning CrossShardReject. KAT verifies per-shard
   `applied_ops_snapshot` unchanged.
3. **Typed reject reason** — `OpResult::SchemaError` carries
   a stable message: `"cross-shard transaction not supported
   in V1 (see SP-Perf-A-SHARD-XTXN-2PC): N shards touched"`.
   SQL layer / client libraries can match on the
   `cross-shard transaction not supported` prefix.
4. **`#![forbid(unsafe_code)]` honored.**
5. **HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched**
   — XTXN is below the wire layer; the only client-observable
   change is the typed SchemaError on previously-silent-corrupt
   cases.
6. **No new external deps** — pure routing logic; reuses the
   existing `make_key_inline` + `shard_of_key` helpers.
7. **Determinism preserved** — the classifier is pure (no
   time / no RNG / no global state); same Op::Txn always
   routes the same way.

---

### 8. Slice plan

| T# | Scope | Status |
|---|---|---|
| **T1** | Design spec (this file) | THIS COMMIT |
| **T2** | `ShardRoute::CrossShardReject` variant + `extract_txn_inner_pkey_shard` helper + `route_op` Op::Txn arm + dispatcher `apply_raw` arm + classifier-level KATs | Next commit |
| **T3** | End-to-end KATs (K=4 single-shard write/read round-trip; K=4 multi-shard reject + no-data-loss assertion) + determinism oracle extension for Op::Txn at K=1/K=4/K=8 | Next commit |
| **T4** | vulcan verification (build + workspace tests + parallel_reads_oracle still green) | After T2/T3 push |
| **T5** | STATUS.md row + progress tracker arc closure | Final commit |

---

### 9. File registry

- **Spec (this file)**: `docs/superpowers/specs/2026-06-02-kesseldb-spperfa-shard-xtxn-design.md`
- **Parent tracker**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-progress.md` (SHARD-XTXN row → DONE on close)
- **Routing classifier**: `crates/kesseldb-server/src/sharded_engine.rs::route_op` (Op::Txn arm rewritten)
- **Helper**: `crates/kesseldb-server/src/sharded_engine.rs::extract_txn_inner_pkey_shard`
- **New variant**: `crates/kesseldb-server/src/sharded_engine.rs::ShardRoute::CrossShardReject`
- **KATs**: `crates/kesseldb-server/src/sharded_engine.rs::tests` (classifier-level + end-to-end)

---

### 10. Standing rules

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-shardxtxn`
- Direct commits to main, no Co-Authored-By, no `-S`, push after each
- CI green check after each push
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched
- `#![forbid(unsafe_code)]` honored
- No new external deps
- All prior tests pass (every slice additive)
- Determinism: oracle still passes at K=4
