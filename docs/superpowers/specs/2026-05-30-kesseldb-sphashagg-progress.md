# SP-Hash-Agg — progress tracker

Closes the SP-Analytic-Plan-MULTI residual TPC-H Q1 (4.5×) + Q6 (16×)
gaps vs Postgres by parallelising the per-row aggregate-fold across
N=4 worker threads within a single query (Postgres-style parallel
hash aggregate, KesselDB-style: `std::thread::scope` + per-worker
HashMap partials + sorted-BTreeMap merge).

Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-sphashagg-design.md`.

**Arc status: OPEN.**

---

## T1 — design + scaffold [TODO]

Commits:
- (pending)

Proof: spec + progress tracker + `MIN_PARALLEL_ROWS` /
`NUM_HASH_AGG_WORKERS` consts in `kessel-sm` + workspace builds clean.

## T2 — parallel `group_aggregate_multi` + parallel `Op::Aggregate` [TODO]

Commits:
- (pending)

Proof: `group_aggregate_multi()` rewritten with row-count gate;
`Op::Aggregate` apply arms (both `apply` + `read_only_op`) gain the
same parallel scan + per-worker scalar partials + merge path. All
existing aggregate KATs stay green.

## T3 — equivalence KATs [TODO]

Commits:
- (pending)

Proof: 2 new SM-level KATs lock parallel == serial byte-for-byte:
1. `sp_hash_agg_group_aggregate_multi_parallel_eq_serial` — 50K rows
   × Q1-shape (5 aggregates × 3 groups) — parallel result byte-equal
   to serial-path baseline.
2. `sp_hash_agg_aggregate_parallel_eq_serial` — 50K rows × Q6-shape
   (single SUM with range narrowing) — parallel result byte-equal to
   serial-path baseline.

## T4 — vulcan TPC-H Q1+Q6 sweep + BENCHMARKS.md update [TODO]

Commits:
- (pending)

Vulcan sweep (3 trials × 30s × SF=0.01 × N=1,4 × KesselDB only;
Postgres+SQLite from prior §3f/§3g sweeps unchanged):

| DB | Q1 N=1 q/s | Q1 N=4 q/s | Q6 N=1 q/s | Q6 N=4 q/s |
|---|---:|---:|---:|---:|
| KesselDB | TBD | TBD | TBD | TBD |
| Postgres | 46.53 | 186.02 | 355.88 | 1,686.01 |
| SQLite | 22.74 | 23.75 | 252.94 | 87.94 |

Headline: TBD.

## T5 — arc closure [TODO]

- STATUS.md row (next Track letter) — TBD
- BENCHMARKS.md §3f + §3g POST-HASH columns — TBD
- README perf section refresh — TBD
- This tracker → CLOSED or DONE_WITH_CONCERNS
- TaskList #345 ready for completion — TBD
