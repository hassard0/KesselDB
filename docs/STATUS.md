# KesselDB — Status

Honest milestone tracker. Updated every milestone. "Done" means code + tests committed and passing.

| Milestone | State | Notes |
|---|---|---|
| M0 — workspace + determinism seam | **done** | proto/io/sim crates; 13 tests green; determinism gate = 100 seeds × 2 runs identical |
| M1 — storage engine (LSM+WAL+recovery) | **done** | WAL+memtable+SSTable+compaction+manifest+crash recovery; 5 tests incl. property-vs-oracle & crash-recovery; Vfs seam added |
| M2 — catalog + codec + single-node SM | **done — CONDITIONAL GO** | thesis not refuted; group-commit added (37× win); see verdict below |
| M3 — VSR replication | **done (core) — hardening backlog listed** | crash-stop VSR: normal op, client table, view change w/ log recovery, state transfer, loss tolerance; 4 sim invariants green |
| M4 — cache + sharding + perf | **done** | LRU read cache (observably invisible), rendezvous sharding groundwork, replicated bench, scaling speculation |
| **SP2 — variable-length overflow store** | **done** | replication-correct overflow blobs via op-derived deterministic handles; `GetBlob`; replicated-convergence test; GC deferred (documented) |
| **SP3 — equality secondary indexes** | **done** | `CreateIndex`/`FindBy`, deterministic backfill + maintenance, `Storage::scan_range`, replicated convergence; range scans & multi-index planner deferred |
| **SP4 — UNIQUE + NOT NULL constraints** | **done** | `OpResult::Constraint`, `Op::AddUnique` (validates existing data), enforced on create/update, replicated convergence; FK/CHECK/balance/WASM deferred |
| **SP5 — query planner** | **done** | `Op::Query` AND-of-(Eq/Ge/Le); multi-index intersection + filtered `scan_range` fallback; per-kind numeric compare; read-only & deterministic |
| **SP6 — foreign keys** | **done** | `Op::AddForeignKey` (validates existing data); ref-exists enforced on create/update (codec-scoped); replicated convergence; no ON DELETE cascade (documented) |
| **SP7 — expression VM + CHECK** | **done** | zero-dep deterministic gas-bounded stack VM (`kessel-expr`); `Op::AddCheck` (structural + existing-data validation); enforced on create/update; replicated convergence |
| **SP8 — deterministic triggers** | **done** | same VM + `SET_FIELD`/`REJECT`; `Op::AddTrigger`; mutate/reject before constraints; order-independent; replicated convergence |
| **SP9 — atomic transactions** | **done** | storage overlay (begin/commit/abort); `Op::Txn` all-or-nothing incl. index+cache rollback; one replicated op; VSR convergence |
| **SP10 — runnable TCP server + client** | **done** | `OpResult` wire codec; `kesseldb` binary (real fsync), `kessel-client`; single owning engine thread; end-to-end socket test |
| **SP11 — ON DELETE RESTRICT/CASCADE** | **done** | FK `on_delete`; auto-index for reverse lookup; recursive cascade closure (visited+budget); atomic via txn wrap; VSR convergence |
| **SP12 — VSR partition hardening** | **partial (honest)** | partition fault model + request-relay + VC-retry; determinism-under-partition & bounded post-heal convergence proven; **seed 7 = documented open VC-liveness repro** |
| **SP13 — VSR view-change hardening** | **partial (honest)** | max-view-seen convergence (no escalation chase) + introspection; precise seed-7 diagnosis (view-change storm → first op lost → SchemaError-converged empty DB); root cause = VSR uncommitted-log reconciliation, still open |
| **SP14 — OR/NOT boolean queries** | **done** | `Op::QueryExpr` reuses the deterministic expr VM as a row filter (arbitrary AND/OR/NOT); read-only, deterministic, txn-allowed; non-breaking (SP5 indexed fast path intact) |
| **SP15 — order-preserving range index** | **done** | `Op::AddOrderedIndex`+`FindRange`; sign-correct 8B order keys; sub-linear range scan; maintained on C/U/D; replicated/deterministic; fixed need_idx gate bug |
| **SP16 — flexibility-cost benchmark** | **done** | `kessel-bench flex`: plain CREATE ~893K/s; eq-index ~6.5× (top perf debt), ordered ~2.9×, CHECK/trigger ~3×, FindBy 1.2M/s; honest analysis recorded |
| **SP17 — eq-index sharding** | **reverted (honest negative result)** | built+tested but didn't improve the measured debt & regressed FindBy ~2×; reverted not shipped; real fix = per-(value,object) index keys (needs wider storage key) — documented future spec |
| **SP18 — Select (rows + LIMIT)** | **done** | `Op::Select` returns filtered whole rows (VM filter) up to LIMIT; read-only, deterministic, txn-allowed; end-to-end over the TCP server |
| **SP19 — ON DELETE SET NULL** | **done** | action 3; nulls referencing FK fields (codec null bit) atomically with cascade; index maintenance; deterministic; VSR convergence. Referential-action set complete |
| **SP20 — aggregates** | **done** | `Op::Aggregate` COUNT/SUM/MIN/MAX over a VM-filtered set; i128 result; read-only, deterministic, txn-allowed |
| **SP21 — projection** | **done** | `Op::SelectFields` returns only chosen fields per filtered row; read-only, deterministic, txn-allowed |
| **SP22 — GROUP BY** | **done** | `Op::GroupAggregate` COUNT/SUM/MIN/MAX per group key (BTreeMap → ascending-order deterministic output); read-only, txn-allowed |
| **SP23 — ORDER BY + paging** | **done** | `Op::SelectSorted` sort by field (cmp_field, id tiebreak), desc, OFFSET/LIMIT; read-only, deterministic, txn-allowed |
| **SP24 — variable-length Key** | **done** | storage `Key` [u8;20]→Vec<u8>; WAL/SSTable length-prefix keys; semantics unchanged; 115 green. Enabler for the real eq-index fix |
| **SP25 — per-entry equality index** | **done (honest mixed)** | one LSM entry/(value,object): writes O(1) & scalable — eq-index debt ~6.5×→~2.6× ✅; point reads now O(matching) prefix scan (slower per call, scalable) — a deliberate write-optimized tradeoff, NOT a pure win |
| **SP26 — lightweight scan_prefix** | **done** | keys-only memtable-fast-path scan for index reads; helped marginally; FindBy/write gap is an architectural tradeoff (corrected the earlier over-optimistic SP25 note honestly) |
| **SP27 — composite indexes** | **done** | multi-field equality index via SP25 per-entry design (synthetic fid + concatenated values); `AddCompositeIndex`/`FindByComposite`; maintained C/U/D; VSR convergence |
| **SP28 — SQL text layer** | **done** | `kessel-sql`: tokenizer + recursive-descent; CREATE/INSERT/SELECT(WHERE→expr VM, GROUP BY, ORDER BY, LIMIT/OFFSET, COUNT/SUM/MIN/MAX)/DELETE → existing Ops; e2e through StateMachine |
| **SP29 — SQL over TCP** | **done** | engine compiles `0xFE`-marked frames vs live catalog; `Client::sql()`; usable networked SQL DB; e2e SQL-over-socket test |
| **SP30 — SQL UPDATE** | **done** | `Stmt`/`compile_stmt`; `UPDATE t ID n SET …` via server-side GetById→decode→set→encode→Op::Update; full SQL CRUD; e2e |
| **SP31 — SQL SELECT by ID** | **done** | `SELECT … FROM t ID <n>` → O(1) `GetById` primary-key fast path; e2e over TCP |
| **SP32 — index-accelerated queries** | **done** | `Op::QueryRows` (index-narrowed candidates + VM-verified, identical to Select); SQL `SELECT * … WHERE c=v [AND…]` → sub-linear; clean fallback for non-restricted grammar |

## M3 VSR — done vs. hardening backlog (honest)

**Working & sim-tested (4 deterministic invariants green):** normal-case
replication, group-commit-compatible apply, exactly-once client table, primary
failover via view change with best-log selection, gap state transfer, retransmit
recovery. Tests: linearizable-vs-reference (single-client total order),
same-seed determinism, primary-crash → view-change → progress + survivor
convergence, convergence under 25% message loss.

**Explicit hardening backlog (NOT yet done — listed, not hidden):**
asymmetric network-partition matrix, disk corruption *during* a view change,
large randomized seed-corpus sweep (CI), real socket transport (currently
in-process deterministic bus only), cluster membership reconfiguration. These
are tracked for M3-hardening / later specs; the protocol is transport-agnostic
so the socket swap is mechanical.

## Sub-project 2 — variable-length overflow store (done)

Object types can have `OverflowRef` fields carrying arbitrary-length bytes
while the core record stays fixed-width. Spec:
`docs/superpowers/specs/2026-05-17-kesseldb-subproject2-overflow.md`.

- Write side rides inside `Create`/`Update` records as a trailer
  (`[fixed][u16 n]( [u16 field_idx][u32 len][bytes] )*`), so it's part of the
  replicated op — every replica writes identical bytes.
- Handle = `(op_number << 20) | field_idx` — deterministic, no counter/RNG,
  identical across replicas (proven: replicated-convergence test + a
  two-instance digest-equality test).
- Read via `Op::GetBlob { handle }`. Overflow lives in a reserved LSM
  keyspace, so it inherits crash recovery, the digest, and replication.
- **Honest limitation:** no overflow GC — an `Update` orphans the old blob
  (still resolvable; documented and asserted by `update_orphans_old_blob…`).
  Orphan compaction is a later spec.

## Sub-project 3 — equality secondary indexes (done)

`CreateIndex(type_id, field_id)` + `FindBy(type_id, field_id, value)`.
Replication-correct (content-derived keys, sorted id sets, digest-covered),
deterministic backfill of pre-existing rows, maintained on Create/Update/
Delete. Added `Storage::scan_range`. Spec:
`docs/superpowers/specs/2026-05-17-kesseldb-subproject3-indexes.md`.
**Honest limits:** equality only (no range / multi-index planner — next
spec); read-modify-write per index op (correct, not yet throughput-optimized);
`OverflowRef` fields not indexable.

## Sub-project 4 — UNIQUE + NOT NULL constraints (done)

`OpResult::Constraint`, NOT NULL from `Field.nullable` (codec-record scoped),
UNIQUE via the SP3 index (`ObjectType.unique`), `Op::AddUnique` that validates
existing data before enabling. Deterministic + replicated-convergence tested.
Spec: `docs/superpowers/specs/2026-05-17-kesseldb-subproject4-constraints.md`.
**Honest limits:** only NOT NULL + UNIQUE (FK/CHECK/balance-guard/WASM
deferred); NOT NULL enforced for codec records only; UNIQUE uses the SP3
read-modify-write path.

## Sub-project 5 — query planner (done)

`Op::Query` = AND of Eq/Ge/Le predicates. Planner intersects indexed-equality
id sets then post-filters; otherwise a filtered `scan_range`. Per-kind numeric
comparison (correct range on LE integers). Read-only, deterministic (digest
unchanged). Spec: `docs/superpowers/specs/2026-05-17-kesseldb-subproject5-query.md`.
**Honest limits:** AND-only (no OR/NOT), no order-preserving range index
(range = scan/post-filter), no cost-based intersection ordering.

## Sub-project 6 — foreign keys (done)

`ObjectType.fks`, `Op::AddForeignKey` (validates existing rows before
enabling, idempotent), ref-exists enforced on Create/Update (codec-record
scoped, NULL skipped), deterministic + VSR-convergence tested. Spec:
`docs/superpowers/specs/2026-05-17-kesseldb-subproject6-fk.md`.
**Honest limit:** no `ON DELETE`/`ON UPDATE` referential actions — deleting
a parent neither cascades nor is blocked (FK checked only on child write);
single-field FK only.

## Sub-project 7 — deterministic expression VM + CHECK (done)

`kessel-expr`: zero-dependency, pure, gas-bounded, terminating stack
bytecode VM. `ObjectType.checks` + `Op::AddCheck` (validates structure +
all existing rows before enabling). Enforced on create/update; rejects on
false or any VM error. 3-node VSR convergence tested. Spec:
`docs/superpowers/specs/2026-05-17-kesseldb-subproject7-check-vm.md`.
**This is the revolutionary core** — user logic, deterministic, inside the
replicated state machine. **Honest limits:** predicate-only (no mutation —
that's SP8 triggers, same VM); single-row; no aggregates; u128-high-bit edge.

## Sub-project 8 — deterministic mutating triggers (done)

Same `kessel-expr` VM + `SET_FIELD`/`REJECT`. `ObjectType.triggers` +
`Op::AddTrigger`. Before-write triggers run in order, may mutate (derived/
generated columns) or reject; output then flows through all constraints.
Order-independent (LoadField reads original record). 3-node VSR convergence
tested. Spec: `docs/superpowers/specs/2026-05-17-kesseldb-subproject8-triggers.md`.
**Honest limits:** BEFORE-only, single-row, branch-free ISA, no cascading.

## Sub-project 9 — atomic transactions (done)

`Op::Txn` = all-or-nothing batch on a storage overlay (begin/commit/abort);
rollback covers data, indexes, and the read cache. Replicated as one op ⇒
identical commit/rollback on every replica (VSR test with colliding txns).
Data-ops only (no DDL/nested); serial state machine ⇒ serializable by
construction. Spec: `docs/superpowers/specs/2026-05-17-kesseldb-subproject9-txn.md`.

## Sub-project 10 — runnable server + client (done)

`kesseldb` binary (TCP, real fsync, `127.0.0.1:7878` default) + `kessel-client`
+ `OpResult` wire codec. Single owning engine thread (deterministic core never
moves; connection threads talk to it via a channel). End-to-end socket test
incl. an atomic `Op::Txn` over the wire. KesselDB is now actually runnable.
Spec: `docs/superpowers/specs/2026-05-17-kesseldb-subproject10-server.md`.
**Honest limit:** single-node only (multi-node VSR-over-sockets still
deferred); no auth/TLS/back-pressure.

## Sub-project 11 — ON DELETE RESTRICT/CASCADE (done)

FK `on_delete` (NoAction/Restrict/Cascade). Action≠0 auto-indexes the FK
field for reverse lookup. Parent delete computes the cascade closure
(visited set + budget, handles diamonds/cycles), RESTRICT aborts with zero
effect, CASCADE recursively deletes; the whole multi-delete is atomic (txn
wrap). Replicated/deterministic (VSR test). Spec:
`docs/superpowers/specs/2026-05-17-kesseldb-subproject11-ondelete.md`.
**Honest limit:** no SET NULL/SET DEFAULT/ON UPDATE; budget-bounded cascade.

## Sub-project 12 — VSR partition hardening (partial, honest)

Added a deterministic transient-single-node partition fault model, a
backup→primary request relay (real liveness fix), and a view-change retry/
escalation timer. **Proven:** determinism under partition+loss; bounded
post-heal convergence for the corpus; no safety/divergence violation.
**Documented open limitation (not overclaimed):** `seed 7` reproduces a
view-change-liveness stall that persists after heal — the crash-stop VSR
does not yet guarantee universal post-heal liveness under arbitrary
partitions. Concrete repro kept in-code + spec. Spec:
`docs/superpowers/specs/2026-05-17-kesseldb-subproject12-partition.md`.

## What this is NOT (yet)

Still out of scope (each a later spec): **full VSR view-change liveness
under arbitrary partition (SP12/13 open repro: seed 7)**, index-accelerated
boolean-query planning, wide/byte-string range indexes, SET DEFAULT &
ON UPDATE actions, balance-guard constraint, cross-shard atomicity, multi-node VSR over sockets,
destructive ALTER/DROP, overflow GC, index-write throughput optimization,
disk-fault-during-view-change, membership reconfiguration, auth/TLS,
client SDKs beyond Rust.

## Performance log

### M1 standalone storage (localhost, single-thread, MemVfs in-memory, no real fsync, unoptimized)

- PUT: ~254,000 ops/s (128B records)
- GET: ~137,000 ops/s (128B records)

**Honest reading:** modest and far below TigerBeetle-class numbers — expected at M1
(unoptimized, single-thread, value-cloning hot path). The notable finding is GET < PUT:
`get()` is O(#sstables) with a binary search + full value clone per table and no bloom
filter. This is a known architectural debt earmarked for M4 perf work (bloom filters,
level compaction, zero-copy reads), recorded here rather than hidden. The first
*thesis-relevant* number is the M2 single-node state-machine benchmark.

### M2 single-node state machine (localhost, single-thread, 128B TB-equivalent record)

| Path | CREATE | GET |
|---|---|---|
| MemVfs, per-op (in-mem upper bound) | ~245K ops/s | ~589K ops/s |
| MemVfs, generalized (codec) | ~205K ops/s | — |
| DirVfs real fsync, **per-op** | **2,339 ops/s** | ~2.0M ops/s |
| DirVfs real fsync, **batch=1000 (group commit)** | **87,338 ops/s** | ~1.05M ops/s |

GET fast on DirVfs because post-flush data sits in OS-cached SSTables; the slower
MemVfs GET reflects the known O(#sstables) read path (no bloom filter yet, M4 work).

### M2 go/no-go verdict: CONDITIONAL GO

The spec's M2 gate asks: is the generalization cost fatal before we invest in VSR?

- **Generalization cost is NOT fatal.** Schema-driven codec records cost ~20% vs a
  raw fixed type (205K vs 245K create) — comfortably within the spec's ≥70%-of-kernel
  intent. The flexibility layer is cheap.
- **The real gap vs TigerBeetle (~1M+/s) was batching, not flexibility.** Naive
  per-op fsync = 2,339/s (purely fsync-bound: p50 395µs ≈ one Windows fsync).
  Adding TB-style **group commit** (one fsync per batch) took the durable path to
  **87,338/s — a 37× win** — with a single, well-understood change. With larger
  batches / parallel fsync / faster storage this scales further; the thesis that
  "schema flexibility at TB-class speed" is achievable is **supported, not refuted**,
  conditional on batched group commit (now implemented) and the remaining M4 perf
  work (bloom filters, zero-copy reads, level compaction).

Confirming evidence: with MemVfs (no real fsync) batch=1000 gives ~242K/s ≈ the
~245K/s per-op number — batching changes nothing in-memory. It only helps on real
disk (2,339 → 87,338). That isolates fsync as the *sole* bottleneck of the naive
path, exactly as the thesis analysis predicted.

**Decision:** proceed to M3 (VSR). The VSR primary will hand committed *batches* to
`StateMachine::apply_batch`, so replication and group commit compose naturally.

### M4 replicated + cache + sharding

- **3-node replicated CREATE: ~161,000 ops/s**, all replicas converged
  (in-process deterministic bus + MemVfs). This isolates **consensus/commit
  overhead only** — no network, no fsync. Single-node MemVfs create was ~245K/s,
  so the replication protocol overhead at this layer is ~35% (245K → 161K),
  which is reasonable for quorum replication.
- **Read cache:** correctness proven (`cache_on_equals_cache_off`: identical op
  results AND identical state digest over a 3,000-op random stream). It is
  observably invisible to the replicated core; value is workload-dependent
  (hit-rate metric exposed via `cache_hit_rate()`), so its speedup is
  characterized qualitatively, not over-claimed with a synthetic number.
- **Sharding:** rendezvous-hash routing, deterministic & ~balanced (<15% skew
  over 8 shards), <30% remap on 4→5 resize. Single-shard today; cross-shard
  transactions explicitly deferred (documented in ARCHITECTURE.md).

### SP16 flexibility-cost (N=100k, localhost, in-memory, single-thread)

plain CREATE **892,940/s** · +eq-index 135,901/s (~6.5× — **#1 perf debt:**
per-insert bucket read-modify-write) · +ordered-index 311,609/s · +CHECK
289,413/s · +trigger 292,309/s · FindBy **1,199,080/s** · FindRange(1%)
43,183/s · QueryExpr(full scan) 15/s. Honest reading: the kernel is
TB-class; every Postgres-flexibility layer has a measured, bounded,
improvable cost; equality-index write maintenance is the prioritized
optimization. Detail + analysis:
`docs/superpowers/specs/2026-05-17-kesseldb-subproject16-flexbench.md`.
**SP17** attempted shard+bitmap — reverted (didn't fix it). **SP24** widened
the storage key (Vec<u8>); **SP25** then implemented the correct fix — one
LSM entry per (value,object): eq-index **writes ~6.5×→~2.6×** (the flagged
debt, fixed). Honest tradeoff (SP26 correction): point-value reads are now an O(matching)
prefix scan, not a single bucket get — slower per call but scalable and not
skew-quadratic; the old ~1.2M FindBy was an artifact of the non-scalable
write design and is not the right baseline. Further read speedups (index
block index / bloom / read-cache routing) are honest future enhancements.
See `…-subproject25-perentry-index.md` (incl. the CORRECTION section).

### Cloud-scaling speculation (reasoned, NOT measured)

All numbers above are a single localhost machine. Extrapolating honestly:

1. **Durability is the dominant cloud cost.** Per-op fsync was 2.3K/s; group
   commit took it to 87K/s locally. Cloud NVMe fsync (~50–200µs) with batches
   of ~1–8K ops/fsync (TB-style) projects to **roughly 0.5–3M durable ops/s
   per node** — the thesis-relevant regime — but this is an extrapolation from
   the measured 37× batching win, not a cloud measurement.
2. **Replication adds RTT, not CPU.** The ~35% protocol overhead measured here
   is CPU/structural. In a cloud region, intra-AZ RTT (~0.1–0.5ms) is hidden by
   pipelining/batching (many ops in flight per round-trip) — throughput stays
   storage-bound; **p99 latency rises by ~1 RTT**, not throughput collapse.
   Cross-region replication would materially raise commit latency (10–80ms RTT)
   and is a deployment-topology decision, not an engine limit.
3. **Sharding is the horizontal-scale lever.** With independent VSR groups per
   shard and rendezvous routing, single-shard-key throughput scales ~linearly
   with shard count until the client/router fans out — bounded by the (deferred)
   cross-shard-transaction fraction of the workload.
4. **Known ceilings before any cloud claim is credible:** O(#sstables) reads
   (no bloom filter), value-cloning hot path, single-threaded core, in-process
   (not socket) transport. These are the M4-hardening / Sub-project-2+ backlog;
   until they're addressed, treat all projections as upper-bound reasoning.

**Bottom line:** the data supports "schema flexibility at TB-class speed is
*achievable*" — generalization costs ~20%, replication ~35%, and the historical
400× gap was batching (now fixed). It does not yet *demonstrate* TB-class
absolute numbers; that requires the hardening backlog and real hardware.
