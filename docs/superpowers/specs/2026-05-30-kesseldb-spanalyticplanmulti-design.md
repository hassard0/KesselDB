# SP-Analytic-Plan-MULTI — multi-aggregate single-scan design

**Arc:** SP-Analytic-Plan-MULTI (V1)
**Track:** Analytics planner — closes the SP-Analytic-Plan T4 residual
TPC-H Q1 gap (1.15× lift → still 18× behind Postgres).
**Status:** T1 design (this doc).
**Date:** 2026-05-30.
**Parent:** SP-Analytic-Plan (V1 SHIPPED 2026-05-29) — named this arc
in BENCHMARKS.md §3f + progress tracker as the second prong for Q1.

---

## 1. Context — what V1 left on the table

`docs/BENCHMARKS.md` §3f, post-SP-Analytic-Plan vulcan numbers (3-trial
median, SF=0.01 ≈ 60K lineitem rows):

| Workload | KesselDB N=4 | Postgres N=4 | Gap |
|---|---:|---:|---:|
| TPC-H Q1 (multi-aggregate GROUP BY) | **10.14 q/s** | 185.99 q/s | Postgres 18× ahead |
| TPC-H Q6 (single SUM w/ shipdate range) | 103.38 q/s | 1,686 q/s | Postgres 16× ahead |

SP-Analytic-Plan V1 lifted Q6 by **7.5×** (123× → 16× gap) via order-
index scan-narrowing on `l_shipdate`. The same machinery only lifted
Q1 by **1.15×** because Q1's WHERE (`l_shipdate <= 19980901`) covers
~all 60K rows — the narrowing window finds nothing to exclude.

The remaining Q1 bottleneck is structural, not WHERE-narrowing:

> "Q1's 8-aggregate SQL becomes **4× separate `Op::GroupAggregate`
> scans** on KesselDB (COUNT + SUM(l_quantity) + SUM(l_extendedprice)
> + SUM(l_discount)) — each one a full-narrowed-set scan with its own
> kessel-expr WHERE evaluation. Postgres folds all 8 aggregates in a
> single parallel hash-aggregate pass; KesselDB pays 4× the per-row
> cost."

V1 closed the WHERE-narrowing prong; V2 closes the **multi-aggregate
fold** prong by introducing `Op::GroupAggregateMulti` — a single op
that accumulates N aggregates per row in ONE scan.

## 2. Scope

### V1 IN-SCOPE

1. **Proto** — additive new variant `Op::GroupAggregateMulti {
   type_id, program, group_field, aggregates: Vec<(u8, u16)>,
   range_preds: Vec<(u16, u8, Vec<u8>)> }` where each `(kind,
   field_id)` pair is one aggregate. Wire tag 47 (next free).
   Result encoding: `[u32 ngroups]` then per group `[u32 keylen][key]
   [u32 n_aggs] [16B i128 LE × n_aggs]`, groups in ascending key
   order (deterministic, mirrors `Op::GroupAggregate`). Existing
   `Op::Aggregate` (20) + `Op::GroupAggregate` (22) variants
   **unchanged** (back-compat).
2. **kessel-sm apply path** — both `read_only_op` and `apply` arms (and
   the SP116 MVCC arm if separate) gain a `Op::GroupAggregateMulti`
   branch. One scan over the narrowed candidate set (or full type
   keyspace if `range_preds.is_empty()`); per row, evaluate the WHERE
   program ONCE, then fold each aggregate's column value into its
   own accumulator. BTreeMap keyed by group bytes ⇒ deterministic
   iteration.
3. **kessel-sql planner** — `compile_select` detects multi-aggregate
   `SELECT` (≥2 of `COUNT|SUM|MIN|MAX|AVG`, with optional `GROUP BY`)
   and emits `Op::GroupAggregateMulti` (or `Op::Aggregate` for single
   aggregate, byte-identical to today). The existing single-aggregate
   path stays for back-compat with callers that build Ops directly.
4. **Result shape** — `Vec<(group_key_bytes, Vec<aggregate_value_i128>)>`
   in the same order as the `aggregates` field. AVG is computed by the
   SM (sum / count, integer division — matches existing
   `Op::Aggregate`/`Op::GroupAggregate` AVG semantics).
5. **bench-compare TPC-H Q1 driver** — replace the 4× separate
   `Op::GroupAggregate` calls with one `Op::GroupAggregateMulti` call
   carrying `aggregates: [(COUNT, 0), (SUM, L_QUANTITY), (SUM,
   L_EXTENDEDPRICE), (SUM, L_DISCOUNT)]` + the same `range_preds` on
   `l_shipdate`.

### V1 OUT-OF-SCOPE

- **`COUNT DISTINCT`** — not in `AggKind` (which mirrors existing
  `Op::Aggregate` kinds: 0=COUNT, 1=SUM, 2=MIN, 3=MAX, 4=AVG). V2
  could add 5=COUNT_DISTINCT.
- **`HAVING`** — V1 returns every group; client filters. Postgres'
  HAVING is a post-aggregate predicate; out of scope.
- **Per-aggregate WHERE** (`FILTER (WHERE …)` clause) — every
  aggregate in a single `Op::GroupAggregateMulti` shares the same
  WHERE program. V2 could add `Vec<Option<Vec<u8>>>` per-aggregate
  filter programs.
- **Aggregates over expressions** (`SUM(a * b)`) — V1 only accepts
  raw field references. The TPC-H Q6 driver already precomputes
  `l_q6_revenue` at load time for this reason; Q1 only needs raw
  fields. V2 could compile a per-aggregate value-expression program.
- **GROUP BY rollup / cube / grouping sets** — V1 single-column group
  key only (matches existing `Op::GroupAggregate`).
- **Parallel hash aggregate** — V1 is single-threaded fold per query.
  Closing the remaining ~5× gap to Postgres requires partitioned-by-
  hash parallelism (SP-Hash-Agg, future arc).

### What V1 will NOT change (back-compat guards)

- **Wire format** — additive new variant; existing `Op::Aggregate` /
  `Op::GroupAggregate` byte-encoding unchanged. Older WAL replays
  decode exactly as before (tag 20 / 22 / 47 dispatch).
- **Encode/decode KAT** — existing `wire_round_trip` vectors stay
  green; V1 appends two new vectors (empty range_preds, non-empty
  range_preds).
- **HTTP/1.1 + WebSocket + binary + PG-wire surfaces** byte-untouched.
  SQL planner emits the new op when SELECT has ≥2 aggregates; the
  single-aggregate path keeps emitting the older variants for back-
  compat with hand-rolled callers.
- **Replication (VSR)** — aggregate ops are reads (never replicated),
  so WAL footprint stays empty.

## 3. SQL planner integration

Today `kessel-sql::compile_select`'s projection parser handles ONE
aggregate via `Proj::Agg(kind, Option<column>)`. The grammar already
accepts `SELECT g, SUM(x), SUM(y) FROM t GROUP BY g` but the parser
returns "bad SELECT projection" because the comma-separated mix
(`g, SUM(x), ...`) is treated as columns and the SUM is rejected.

**Plan**: refactor `Proj` to:
```rust
enum Proj {
    Star,
    Cols(Vec<String>),
    Aggs(Vec<AggSpec>),                  // NEW: ≥1 aggregate
    GroupedAggs(Vec<String>, Vec<AggSpec>), // NEW: leading group cols + aggregates
}
struct AggSpec { kind: u8, field: Option<String> }
```

Parsing flow:
1. Tokenize SELECT projection list (comma-separated).
2. For each item: detect `COUNT(...)` / `SUM(...)` / `MIN(...)` / `MAX(...)`
   / `AVG(...)` via the existing single-aggregate keyword sniff. Plain
   identifiers go into a "leading group cols" bucket. Once we hit the
   first aggregate, subsequent plain identifiers are an error (would
   imply a non-aggregated, non-GROUP-BY column).
3. After parsing the list:
   - `0 aggs + ≥1 col` → existing `Proj::Cols` path.
   - `0 aggs + STAR` → existing `Proj::Star` path.
   - `1 agg + 0 leading cols` AND no `GROUP BY` → existing `Op::Aggregate`
     (single agg) for back-compat byte-identical.
   - `1 agg + 0 leading cols` AND `GROUP BY g` → existing
     `Op::GroupAggregate` (single agg) for back-compat byte-identical.
   - `≥2 aggs` OR `≥1 leading col + ≥1 agg` → emit new
     `Op::GroupAggregateMulti`. The leading group cols become the
     `GROUP BY` columns (V1: must match what GROUP BY clause says, or
     replace GROUP BY entirely — for V1 simplicity, require explicit
     `GROUP BY` whenever there are leading group cols).

Q1 maps directly:
```sql
SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice),
       AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
FROM lineitem WHERE l_shipdate <= 19980901
GROUP BY l_returnflag, l_linestatus
```
With V1's single-column GROUP BY restriction, the bench driver uses
the synthetic `l_groupkey` already in place (V2 could add multi-
column GROUP BY).

The shared `extract_range_preds(ot, span)` helper from SP-Analytic-Plan
T3 is reused unchanged.

## 4. Storage layer reuse

`narrow_by_range_preds` (introduced in SP-Analytic-Plan T2) is reused
verbatim. The new apply arm reuses:

- `make_key(type_id, &[0u8; 16])` / `&[0xFFu8; 16]` for the type-
  keyspace scan bounds (same as `Op::GroupAggregate`).
- The shared `narrow_by_range_preds` returns `Option<BTreeSet<[u8;16]>>`
  — `Some(set)` if any range narrowed; `None` for full-scan path.
- Per-aggregate column extraction uses the existing
  `Self::ord_field_pos` helper (numeric ≤8B columns) — same surface as
  `Op::Aggregate`.
- Result encoding mirrors `Op::GroupAggregate` for the per-group prefix
  but appends N×16B aggregate values instead of 1.

Apply skeleton:
```rust
Op::GroupAggregateMulti { type_id, program, group_field, aggregates, range_preds } => {
    let ot = self.catalog.get(type_id) ...;
    let cand = self.narrow_by_range_preds(type_id, &ot, &range_preds);
    let (gpos_off, gpos_w) = ...; // existing group_field offset extraction
    // Per-aggregate: resolve (off, w, signed) once.
    let apos: Vec<Option<(usize, usize, bool)>> = aggregates.iter().map(|(k, f)| {
        if *k == 0 { None } // COUNT — no field needed
        else { Self::ord_field_pos(&ot, *f).map(|(o, w, fk)| (o, w, fk_is_signed(fk))) }
    }).collect();
    let mut groups: BTreeMap<Vec<u8>, AggState> = BTreeMap::new();
    // AggState = Vec<(count, sum, min, max)> sized to aggregates.len()
    for (or full scan) row in candidates {
        if !uncond && !eval(program, row) { continue; }
        let gkey = row[gpos_off..gpos_off+gpos_w];
        let state = groups.entry(gkey).or_insert_with(|| init_state(&aggregates));
        for (i, (kind, _field)) in aggregates.iter().enumerate() {
            state[i].count += 1;
            if let Some((off, w, signed)) = apos[i] {
                let v = decode_i128(&row[off..off+w], w, signed);
                state[i].sum = state[i].sum.wrapping_add(v);
                state[i].min = Some(state[i].min.map_or(v, |m| m.min(v)));
                state[i].max = Some(state[i].max.map_or(v, |m| m.max(v)));
            }
        }
    }
    // Encode: [u32 ngroups] per group [u32 keylen][key][16B × n_aggs]
    ...
}
```

**Equivalence oracle**: byte-equal vs the same data scanned by N
sequential `Op::GroupAggregate` calls (one per aggregate). The
per-group, per-aggregate fold is mathematically identical; only the
scan count differs.

## 5. Acceptance criteria

- **TPC-H Q1 N=4 on vulcan** lifts from 10.14 q/s (post-V1) to
  **≥ 30 q/s** (≥ 3× lift from collapsing 4 scans → 1 scan; Q1's
  WHERE covers ~all rows so the per-row WHERE-eval is the dominant
  per-scan cost; collapsing 4 scans into 1 should give ~3-4×).
- **TPC-H Q1 N=1 on vulcan** lifts from 2.80 q/s (post-V1) to **≥ 8
  q/s** (same math, single-threaded).
- **Equivalence** — byte-equal vs N sequential `Op::GroupAggregate`
  calls (KAT covers this for COUNT, SUM, MIN, MAX, AVG, mixed).
- **All pre-arc tests pass** — existing `Op::Aggregate` /
  `Op::GroupAggregate` paths byte-identical; the new variant is
  additive.
- **seed-7 GREEN** (reads only; no VSR replication path touched).
- **HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched** for any
  caller that doesn't issue the new op.
- **CI green** on every push.

The remaining ~5× gap to Postgres (post-MULTI ~30 q/s vs 186 q/s) is
the parallel hash aggregate + C-level accumulator efficiency — the
SP-Hash-Agg arc target, not V1.

## 6. Task decomposition

| Task | Description | Acceptance |
|---|---|---|
| **T1** | Design + scaffold | This doc + `Op::GroupAggregateMulti` variant added to Op enum + encode/decode wire-round-trip KAT (empty + non-empty range_preds, 2+ aggregates) + workspace builds clean |
| **T2** | kessel-sm apply path | Both `read_only_op` and `apply` arms (and SP116 MVCC arm) gain the multi-aggregate fold + equivalence KAT (N×Op::GroupAggregate vs 1×Op::GroupAggregateMulti same answer) |
| **T3** | kessel-sql planner emits Multi for multi-aggregate SELECTs | `compile_select`'s projection parser handles `≥2 aggregates` or `leading_cols + ≥1 agg` → `Op::GroupAggregateMulti` (single-agg path unchanged) + integration KAT (`SELECT g, SUM(a), SUM(b) FROM t GROUP BY g` compiles to one Multi op) |
| **T4** | bench-compare TPC-H Q1 driver uses Multi + vulcan sweep | Driver replaces 4×GroupAggregate with 1×GroupAggregateMulti + 3-trial 30s sweep on vulcan + BENCHMARKS.md §3f updated with PRE-MULTI/POST-MULTI columns |
| **T5** | arc closure | STATUS row (Track F or next letter) + progress tracker + TaskList #342 ready for completion; SP-Hash-Agg named as the next prong |

## 7. Six-plus weak-spots self-review

1. **AggKind coverage.** V1 supports the 5 kinds already in
   `Op::Aggregate` (COUNT, SUM, MIN, MAX, AVG). `COUNT(DISTINCT col)`
   is not supported. **Mitigation**: TPC-H Q1 doesn't need DISTINCT;
   document as V2 candidate.
2. **No HAVING clause.** V1 returns every group; if the SQL has
   `HAVING SUM(x) > N` the client must filter. Postgres' planner pushes
   HAVING into the aggregate; V1 doesn't. **Mitigation**: V2; out of
   scope for closing the Q1 gap.
3. **Per-aggregate FILTER clause.** `SUM(x) FILTER (WHERE y > 0)` is
   SQL standard and Postgres supports it. V1 every aggregate shares
   the same WHERE program. **Mitigation**: V2; rare in practice.
4. **V1 only raw field references.** `SUM(a * b)` requires a value-
   expression program per aggregate; V1 only resolves field offsets.
   **Mitigation**: TPC-H Q6 already uses a precomputed column; Q1
   doesn't need this. V2 candidate.
5. **GROUP BY rollup / grouping sets.** Single-column group key
   only (inherits `Op::GroupAggregate`'s constraint). **Mitigation**:
   bench driver uses synthetic `l_groupkey` (Char(2)) to fold the two
   TPC-H Q1 group cols into one byte sequence. V2 could lift this.
6. **Source-level back-compat churn.** Adding a new Op variant doesn't
   require touching existing callsites (no enum struct-field change).
   The SQL planner refactor (Proj enum shape) is internal to
   `compile_select`. **Mitigation**: scope is contained.
7. **Wire-format growth.** The new variant adds ~5 bytes overhead per
   aggregate in the encoded op (vs not encoding it at all). For the
   typical Q1-shape op with 4 aggregates that's ~20 bytes extra per
   query — negligible.
8. **Aggregates with COUNT(*) need field_id=0 placeholder.** Mirrors
   the existing `Op::Aggregate { kind: 0, field_id: 0 }` convention;
   no change needed.
9. **Determinism**: BTreeMap iteration is ordered. Per-aggregate fold
   is associative+commutative for COUNT/SUM/MIN/MAX (and AVG = SUM/
   COUNT with integer division — matches existing semantics). The
   apply arm is deterministic.
10. **N=0 aggregates edge case**: reject at decode time (Vec must be
    non-empty), else the result encoding has nothing per group.

## 8. Files

- `docs/superpowers/specs/2026-05-30-kesseldb-spanalyticplanmulti-design.md` — this spec
- `docs/superpowers/specs/2026-05-30-kesseldb-spanalyticplanmulti-progress.md` — progress tracker (T1-T5)
- `crates/kessel-proto/src/lib.rs` — `Op::GroupAggregateMulti` variant + wire encode/decode + tag 47
- `crates/kessel-sm/src/lib.rs` — apply paths (apply, read_only_op, MVCC) gain multi-aggregate fold
- `crates/kessel-sql/src/lib.rs` — `compile_select` projection refactor + emits Multi for ≥2 aggregates
- `tools/bench-compare/src/drivers/kesseldb_tpch.rs` — Q1 driver uses one Multi op
- `docs/BENCHMARKS.md` — §3f updated with PRE-MULTI/POST-MULTI columns
- `docs/STATUS.md` — Track F (or next letter) row

## 9. Standing rules acknowledgement

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-multi`.
- Direct commits to main, no Co-Authored-By, no `-S`, push after each.
- CI green check after push.
- Memory files OUTSIDE repo.
- `#![forbid(unsafe_code)]` honored.
- No new external deps.
