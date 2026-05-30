# SP-Analytic-Plan — aggregate index-narrowing design

**Arc:** SP-Analytic-Plan (V1)
**Track:** Analytics planner — closes the SP-Bench-Suite T4 TPC-H Q1 + Q6 losses.
**Status:** T1 design (this doc).
**Date:** 2026-05-29.
**Parent:** SP-Bench-Suite T4 + T5 named this arc in BENCHMARKS.md §3f /
§3g + STATUS.md as the roadmap for the TPC-H losses.

---

## 1. Context — the actual numbers

`docs/BENCHMARKS.md` §3f + §3g, measured on vulcan (3-trial median,
SF=0.01 ≈ 60K lineitem rows):

| Workload | KesselDB N=4 | Postgres N=4 | Gap |
|---|---:|---:|---:|
| TPC-H Q1 (GROUP BY pricing summary) | **8.84 q/s** | 185.95 q/s | Postgres 7.8× ahead |
| TPC-H Q6 (single SUM w/ shipdate range) | **13.74 q/s** | 1,685 q/s | Postgres 123× ahead |

Single, identical root cause (from the BENCHMARKS.md "Why KesselDB
loses" sections):

> "`Op::Aggregate` is structurally fine for Q6 — single column sum,
> single result. The cost is the predicate evaluation: four AND'd
> comparisons over `l_shipdate` / `l_discount` / `l_quantity` get
> executed by the kessel-expr stack VM for every one of the 60K rows.
> Postgres + SQLite both use a btree on `l_shipdate` to narrow the
> candidate set first; the engine never reads the 52K rows outside the
> shipdate window."

`Op::QueryRows` (SP70) already accepts `range_preds: Vec<(u16, u8,
Vec<u8>)>` — `(field_id, op, value)` half-range hints (`op` 0=`>`,
1=`>=`, 2=`<`, 3=`<=`) — that the SM intersects against the existing
`AddOrderedIndex` / `FindRange` machinery before running the
verifying program. The aggregate ops do NOT consume this interface
today, so they pay the full-scan cost even when the SQL planner
*could* emit the narrowing hint.

## 2. Scope

### V1 IN-SCOPE

1. **Proto** — `Op::Aggregate` + `Op::GroupAggregate` each gain
   `range_preds: Vec<(u16, u8, Vec<u8>)>`. Encoded after every
   existing field (so a SP-Analytic-Plan-PRE frame is still a valid
   prefix that decodes to empty `range_preds` — back-compat). Wire
   tag stays the same (20 / 22).
2. **kessel-sm apply path** (both `read_only_op` and `apply` arms,
   plus the SP116 MVCC path if it has its own arm) — when
   `range_preds` is non-empty, narrow the scan via the existing
   ordered-index `scan_range` API to the candidate set, then apply
   the verifying program + accumulate aggregates. Empty `range_preds`
   ⇒ existing full-scan path unchanged (byte-identical back-compat).
3. **kessel-sql planner** — `compile_select`'s aggregate branch
   (`Proj::Agg`) extracts the same range hints `try_query_rows` does,
   for the same conjunct-safety reasons (no top-level OR/NOT/parens),
   and emits them in the new field on `Op::Aggregate` /
   `Op::GroupAggregate`.
4. **bench-compare TPC-H driver** — add `Op::AddOrderedIndex` on
   `l_shipdate` at load time + populate `range_preds` on the Q6
   `Op::Aggregate` and the Q1 `Op::GroupAggregate` calls (the driver
   builds Ops directly, not through SQL, so the planner path doesn't
   help here).
5. **Determinism** — every prior test must still pass; the
   range-narrowed result must be byte-identical to the full-scan
   result (the program still verifies every candidate, so the
   candidate set being a superset of the true match set is safe).

### V1 OUT-OF-SCOPE

- **`Op::GroupAggregateMulti`** (a multi-aggregate-per-call shape that
  collapses Q1's 4 separate scans into 1). This is the second prong
  the BENCHMARKS.md §3f roadmap names. It's orthogonal to range_preds
  (it folds 4× COUNT/SUM scans into 1×, doesn't change per-scan cost)
  and is deferred to **SP-Analytic-Plan-MULTI**. If T2-T4 lands
  early and the Q1 headline is still too low after range_preds
  narrowing, we'll evaluate folding it in here; otherwise it's the
  follow-up arc.
- **Cost-based planner / index selection** — V1 takes whatever
  `range_preds` the SQL planner emits or the bench driver passes in,
  in field-id order. There's no "which index is cheapest" decision
  yet. The conjunct-safety gate is the only safety check.
- **Disjunctive predicates** — `OR` at the top level of WHERE still
  bails out (same shape as `try_query_rows`).
- **Aggregate-on-non-leading group column** — if the GROUP BY key
  isn't on an order-indexed field, that part still scans the
  narrowed candidate set; we don't promote `group_field` into the
  range hint itself. V1 only accelerates the WHERE narrowing.

### What V1 will NOT change (back-compat guards)

- **Wire format** — additive: a `range_preds: vec![]` op encodes to
  bytes that match an OLDER SP-Analytic-Plan-PRE encoding only when
  the trailing length-zero `u32` is absent. Concretely, when
  `range_preds.is_empty()` we *omit the trailing u32* entirely (just
  like `Op::QueryRows`), so the pre-arc on-disk WAL is byte-
  identical for any aggregate op that doesn't use range hints.
  Decode tolerates the absence (length-prefixed conditional read).
- **`Op::Aggregate.encode()` byte-stability test** in
  `kessel-proto::tests::wire_round_trip` — V1 KATs append two new
  vectors (with non-empty range_preds) and leave the existing
  `range_preds: vec![]` vector encoding-stable.
- **HTTP/1.1 + WebSocket + binary + PG-wire surfaces** byte-untouched
  (no aggregate-op SQL surface change for `range_preds: vec![]`
  callers; only the SQL compiler now ALSO emits range hints when
  safe).
- **Replication (VSR)** — aggregate ops are reads (never replicated),
  so the WAL footprint stays empty.

## 3. SQL planner integration

`kessel-sql::compile_stmt` calls `compile_select` for general
`SELECT`. The aggregate branch in `compile_select` is `Proj::Agg`
(line ~1490). Today it compiles WHERE → kessel-expr program and emits
`Op::Aggregate` (no group) or `Op::GroupAggregate` (group present)
WITHOUT consulting the order-index catalog for range narrowing.

`try_query_rows` (line ~1186) does the same WHERE extraction PLUS a
post-parse walk over the parsed token span to pull conjunct-safe
range hints into the `range_preds: Vec<(u16, u8, Vec<u8>)>` field.
That walk is the proven safe pattern.

**Plan**: refactor `try_query_rows`'s range-extraction body into a
shared helper `fn extract_range_preds(ot: &ObjectType, span: &[Tok])
-> Vec<(u16, u8, Vec<u8>)>` and call it from BOTH `try_query_rows`
AND `compile_select`'s aggregate branch. The conjunct-safety gate
(no top-level OR/NOT/parens) is part of the helper too — same
verified shape as today.

The result is that an `Op::Aggregate` / `Op::GroupAggregate` whose
WHERE has a range predicate on an order-indexed column gains the
SAME narrowing the equivalent `Op::QueryRows` would have.

## 4. Storage layer reuse

The existing `kessel-sm` apply paths for `Op::QueryRows` already
implement the narrowing pattern (line ~1697 in `read_only_op`, line
~4173 in `apply`):

1. For each unique `field_id` in `range_preds`:
   - Find the field's order-index keyspace (numeric: `0xFFFD_xxxx`;
     vendored variable-length CHAR/BYTES: `Self::voidx_key`).
   - Fold all (op, value) pairs on that field into a single `[lo,
     hi]` byte range.
   - `self.storage.scan_range(&klo, &khi)` over the index entries,
     extracting the 16-byte row-ids.
   - Intersect against the running candidate set (`BTreeSet<[u8;
     16]>`).
2. After all range narrowings, iterate the candidates (or the full
   type-keyspace if `cand.is_none()`), apply the verifying program,
   fold into the aggregate accumulator.

Concretely the aggregate apply arms become:

```rust
Op::Aggregate { type_id, program, kind, field_id, range_preds } => {
    let ot = …;
    let cand = if !range_preds.is_empty() {
        Some(intersect_range_preds(&ot, type_id, &range_preds))
    } else {
        None
    };
    let lo = make_key(type_id, &[0u8; 16]);
    let hi = make_key(type_id, &[0xFFu8; 16]);
    // existing fold loop, but driven by `cand` if present, else full scan.
    …
}
```

The full-scan path (existing) is the oracle: when `range_preds` is
present, every candidate row's data is still re-verified by the
program before being accumulated, so the result is mathematically
identical to the full scan. The narrowing only skips rows that
*cannot* match a conjunct (because the order index proves their
column value is outside the range).

## 5. Acceptance criteria

- **TPC-H Q6 at N=1 on vulcan** lifts from 3.53 q/s (pre-arc) to
  **≥ 50 q/s** (≥ 14× lift). The 60K-row scan collapses to a
  ~8K-row scan (one year of seven), plus the per-row VM cost on the
  narrowed set. Postgres still wins (435 q/s) but the gap closes
  from 123× to ≤ ~9×.
- **TPC-H Q1 at N=1 on vulcan** lifts from 2.38 q/s (pre-arc) to
  **≥ 6 q/s** (≥ 2.5× lift). Q1's WHERE is `l_shipdate <=
  19980901`, which is a wide window (most data passes), so the
  narrowing is smaller than Q6 — but it's still better than the full
  scan because the order index lets us iterate just rows with
  `l_shipdate <= 19980901` instead of scanning everything and
  filtering. Postgres still wins (46-186 q/s) — Op::GroupAggregateMulti
  is the second prong for Q1 (named SP-Analytic-Plan-MULTI).
- **All pre-arc tests pass** — the empty-range_preds path must be
  byte-identical (workspace test count delta = +new range_preds
  KATs only).
- **seed-7 GREEN** (aggregate ops are reads against committed state;
  no VSR replication path touched).
- **HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched** for
  any caller that doesn't pass range hints.
- **CI green** on push.

## 6. Task decomposition

| Task | Description | Acceptance |
|---|---|---|
| **T1** | Design + scaffold | This doc + `range_preds` field added to Op enum + encode/decode wire-round-trip KAT for the new field + every callsite updated to pass `range_preds: vec![]` (back-compat default) + workspace builds clean |
| **T2** | kessel-sm aggregate apply uses range_preds | Both `read_only_op` and `apply` arms (and the SP116 MVCC arm if separate) gain the narrowing path + equivalence KAT (full-scan vs range_preds same answer on same data) |
| **T3** | kessel-sql planner emits range_preds for aggregate SELECT | `compile_select`'s `Proj::Agg` branch calls the shared `extract_range_preds` helper + integration KAT (`SELECT SUM(x) FROM t WHERE date >= a AND date < b` compiles to `Op::Aggregate { range_preds: [...] }`) |
| **T4** | vulcan TPC-H sweep + BENCHMARKS.md update | bench-compare TPC-H driver adds `AddOrderedIndex` on `l_shipdate` + populates `range_preds` + Q6 lift ≥ 50 q/s @ N=1 + BENCHMARKS.md §3f/§3g updated with PRE / POST numbers |
| **T5** | arc closure + V2 follow-up named | STATUS row + progress tracker + SP-Analytic-Plan-MULTI named in BENCHMARKS.md as the next prong |

## 7. Six-plus weak-spots self-review

1. **No cost model.** V1 always narrows when `range_preds` is non-
   empty. If the planner emits a range that matches most of the
   table (e.g. `l_shipdate <= 99991231`), the candidate-set
   construction is pure overhead vs the scan. **Mitigation**: the
   program-verifies-every-candidate property guarantees correctness;
   the cost regression is at worst the BTreeSet construction (O(N
   log N) range entries) vs a linear scan (O(N) records). For wide
   ranges, the order index entries are roughly N anyway so we're
   ~2× a clean scan in the worst case. V2 candidate.
2. **No index-only.** Even when the only column needed is the range
   column itself (e.g. `COUNT(*) WHERE date >= a`), V1 still fetches
   the full record because the program needs it (or might). V2
   could detect uncond + COUNT(*) + range_preds and return the
   index-entry count directly.
3. **Conjunctive-only.** V1 inherits `try_query_rows`'s top-level-
   conjunct gate. Disjunctive WHEREs (`OR`) skip the hint. V2 could
   per-disjunct intersect.
4. **GROUP BY column not range-indexed.** If the grouping key
   doesn't have an order index, V1 still does per-row group fold
   over the narrowed candidate set. Postgres' parallel hash
   aggregate beats this on Q1 by another factor; V2 candidate
   (SP-Analytic-Plan-MULTI or SP-Analytic-Plan-HASH).
5. **Numeric-only V1 range columns?** No: the SP70 helper supports
   both numeric ≤8B (0xFFFD keyspace) and variable-length CHAR/BYTES
   (0xFFFC, SP87). V1 reuses the same `Op::QueryRows` shape so both
   work.
6. **Wire-back-compat fragility.** Adding fields to enum variants
   is a Rust source-break for every callsite. We accept that one-
   time churn (every site adds `range_preds: vec![]`) because the
   alternative (a parallel `AggregateRanged` variant) doubles the
   surface area and the SM apply duplication. The wire format
   stays back-compat (empty range_preds ⇒ no trailing bytes).
7. **`Op::Aggregate` MIN/MAX fast path.** The current SM has an
   index-extreme fast path when `uncond && ordered.contains(field)`.
   That path is for MIN/MAX of the *aggregate* column, not the
   range-narrowing column. V1 disables the fast path when
   `range_preds` is non-empty (just to keep the change small); the
   slow path is still narrowed by range_preds so it's not a
   regression. A V2 enhancement could intersect the index-extreme
   with the candidate set.
8. **No test for narrowing yielding empty.** A range that selects
   zero candidate rows should produce an aggregate result that
   matches the full-scan oracle on the same data (COUNT=0,
   SUM=0, MIN/MAX=defaults). KAT covers this.

## 8. Files

- `docs/superpowers/specs/2026-05-29-kesseldb-spanalyticplan-aggregate-index-narrowing-design.md` — this spec
- `docs/superpowers/specs/2026-05-29-kesseldb-spanalyticplan-progress.md` — progress tracker (T1-T5)
- `crates/kessel-proto/src/lib.rs` — `Op::Aggregate` + `Op::GroupAggregate` gain `range_preds` field
- `crates/kessel-sm/src/lib.rs` — apply paths gain narrowing
- `crates/kessel-sql/src/lib.rs` — planner emits range hints for aggregate SELECT
- `tools/bench-compare/src/drivers/kesseldb_tpch.rs` — adds AddOrderedIndex + range_preds
- `docs/BENCHMARKS.md` — §3f + §3g updated with PRE/POST numbers

## 9. Standing rules acknowledgement

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-analytic`.
- Direct commits to main, no Co-Authored-By, no `-S`, push after each.
- CI green check after push.
- Memory files OUTSIDE repo.
- `#![forbid(unsafe_code)]` honored.
- No new external deps.
