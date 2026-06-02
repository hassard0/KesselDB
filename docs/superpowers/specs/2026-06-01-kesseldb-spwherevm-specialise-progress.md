# SP-WHERE-VM-Specialise — progress tracker

Drives down the per-row `kessel_expr::eval` stack-VM interpreter cost
that SP-Hash-Agg-Tune diagnosed as the dominant TPC-H Q1/Q6 wall-time
ceiling (V1-Tune sweep at N=4 lifted only 1.06× Q1 / 1.07× Q6 vs the
≥2× modelled prediction, naming the per-row WHERE-eval cost as the
actual floor).

`compile_filter` walks the WHERE bytecode ONCE per query and returns a
`Box<dyn Fn(&[u8]) -> bool + Send + Sync>` closure that captures
pre-resolved field offsets + comparison ops + AND/OR short-circuit
tree directly. Per-row dispatch loop + layout recompute + field-id
linear-scan are eliminated; byte-equivalent `kessel_expr::eval`
fallback covers unsupported opcode shapes.

Design spec: `docs/superpowers/specs/2026-06-01-kesseldb-spwherevm-specialise-design.md`.

**Arc status: V1 SHIPPED 2026-06-02.**

The shippable artifact is honest end-to-end: the closure path is
byte-equivalent to the interpreter (KAT-locked at both `kessel-expr`
and `kessel-sm` levels), Q6 spec floor (≥400 q/s at N=4) EXCEEDED + spec
stretch (≥500 q/s) ALSO EXCEEDED, Q1 lifts +1.35× at N=4. The
SP-Hash-Agg-Tune diagnosis is validated end-to-end — per-row WHERE-eval
WAS the dominant cost, the closure-built-once-per-query approach cut
it as modelled.

---

## T1 — design + compile_filter primitive + FilterNode + materialise + KATs [DONE]

Commits:
- `95b68cb` docs(spec) + feat(expr): SP-WHERE-VM-Specialise T1 —
  design spec + compile_filter closure-built-once + 15 KATs
- `1c38e31` fix(expr): SP-WHERE-VM-Specialise T1 — KAT panic format
  (FilterFn not Debug)

Proof:
- Design spec at `docs/superpowers/specs/2026-06-01-kesseldb-spwherevm-specialise-design.md`
- `kessel-expr::compile_filter(ot, program_bytes) -> Result<FilterFn,
  CompileError>` walks the bytecode ONCE per query:
  1. Decodes bytecode into a Vec<Opcode> with i128 / bytes operands
     inlined.
  2. Treats opcodes as RPN; builds a FilterNode AST:
     - PUSH_INT/PUSH_BYTES → Const leaf
     - LOAD_FIELD(fid) → Load leaf (resolves fid → offset/width/kind
       ONCE here via `ot.compute_layout()` + `ot.fields.iter().position`)
     - IS_NULL(fid) → IsNull leaf (resolves fid here)
     - EQ/NE/LT/LE/GT/GE → CmpNode
     - AND/OR → LogicNode
     - NOT → NotNode
     - any other opcode → `Err(Unsupported{op_name})`
     - stack underflow / stray operand → `Err(Malformed)`
  3. Recursively materialises the FilterNode into
     `Box<dyn Fn(&[u8]) -> bool + Send + Sync>`.
- Hot path `Load(off, width, signed) vs ConstInt(v) op CmpOp`
  specialises into direct memory reads + branch-folded i128 comparisons
  (Q6's 4-deep AND chain becomes ~4 field reads + 6 comparisons + 3
  `&&` short-circuits per row).
- Determinism oracle preserved: closure result is byte-equal to
  `kessel_expr::eval(program, &ot, rec).unwrap_or(false)` per row;
  KAT-locked at the expr level over random rows.
- 15 new `kessel-expr` lib KATs covering each opcode shape +
  Compile-fallback + equivalence-on-random-rows.

## T2 — SM hot-path wiring + SM-level equivalence KATs [DONE]

Commits:
- `40b4bef` feat(sm): SP-WHERE-VM-Specialise T2 —
  aggregate_numeric_scan uses compile_filter closure with eval()
  fallback
- `89b7d8c` test(sm): SP-WHERE-VM-Specialise T2 — SM-level
  equivalence KATs (compile_filter == model, Unsupported ->
  interpreter fallback)
- `e0ba6c4` feat(sm): SP-WHERE-VM-Specialise T2 —
  group_aggregate_multi also uses compile_filter (TPC-H Q1 hot path)

Proof:
- `aggregate_numeric_scan` (Q6 hot path) compiles the WHERE program
  ONCE before the parallel-fold spawn; per-row callsite invokes the
  closure instead of dispatching through the stack-VM interpreter.
  On compile failure (Unsupported, Malformed) the per-row callsite
  falls back to `kessel_expr::eval` — byte-identical observable
  behavior.
- `group_aggregate_multi` (Q1 hot path) mirrors the same wire-up.
  Sanity-bench at Q1 N=1 after only the `aggregate_numeric_scan`
  wire-up showed ~15.5 q/s (par with pre-arc 16.14 q/s) — Q1 maps to
  `Op::GroupAggregateMulti`, not `Op::Aggregate`; the
  `group_aggregate_multi` wire-up was required to lift Q1.
- Uncond sentinel (`program == PUSH_INT(1)`) skips the compile step
  entirely — same fast-path as pre-arc.
- Closure captured by reference into `fold_one`; `std::thread::scope`
  workers borrow `&fold_one` (SP-Hash-Agg-Tune V1 pattern), so the
  Send+Sync bound on FilterFn satisfies the cross-worker share
  without per-row Arc allocation.
- 2 new SM-level KATs (`sp_where_vm_specialise_*`):
  1. `sp_where_vm_specialise_aggregate_with_filter_eq_model` —
     Aggregate with 4-deep TPC-H Q6-shape WHERE at 10K rows
     (crosses MIN_PARALLEL_ROWS=8192); closure-driven parallel-fold
     result byte-equal to hand-computed model for all 5 aggregate
     kinds (COUNT/SUM/MIN/MAX/AVG); 5 reruns per kind lock
     determinism across closure-share + worker scheduling.
  2. `sp_where_vm_specialise_aggregate_falls_back_to_interpreter_on_unsupported`
     — Aggregate with WHERE `(v + 1) > 5000` (uses ADD, V1
     compile_filter declines as Unsupported{op_name:"ADD"}); per-row
     callsite falls back to `kessel_expr::eval` and returns the same
     COUNT as the hand-computed model. Locks the 'interpreter remains
     the oracle on fallback' contract.
- Full kessel-sm suite: 162 passed (was 160 pre-T2 + 2 new); 0
  failed. All 6 SP-Hash-Agg + SP-Hash-Agg-Tune KATs stay green
  (parallel == serial fold math unchanged; closure result == eval
  result per row by construction).

## T3 — sanity bench [DONE — folded into T2 commits + T4 sweep]

Sanity numbers (single trial per cell, recorded in commit messages
as the diagnosis aid):
- Q6 N=1 after T2 aggregate_numeric_scan wire-up: ~147 q/s vs
  pre-arc 33.95 q/s = **4.3× lift at N=1** (full sweep in T4).
- Q1 N=1 after T2 aggregate_numeric_scan wire-up: ~15.5 q/s (par
  with pre-arc 16.14 q/s) — diagnosed Q1 maps to
  `Op::GroupAggregateMulti`, prompting commit `e0ba6c4` to mirror
  the wire-up there.
- Q1 N=1 after `e0ba6c4` group_aggregate_multi wire-up: lift
  recovered (full sweep in T4 below).

## T4 — vulcan TPC-H Q1+Q6 sweep + BENCHMARKS.md update [DONE]

Commits:
- (this commit) docs(benchmarks): SP-WHERE-VM-Specialise T4 — vulcan
  TPC-H Q1+Q6 post-WHERE-VM sweep

Vulcan sweep (3 outer trials × bench-compare's 3 internal trials × 30s
× SF=0.01 × N=1,4 × KesselDB only; Postgres+SQLite from prior §3f/§3g
sweeps unchanged):
`/tmp/bench-tpch-q{1,6}-postvm-t{1..3}-w{1,4}.json`:

| Workload | N=1 q/s (median) | N=4 q/s (median) | vs pre-WHERE-VM | vs Postgres |
|---|---:|---:|---:|---:|
| Q1 | **25.50** | **85.82** | **+1.47× / +1.35×** | 2.17× behind |
| Q6 | **149.85** | **548.87** | **+4.41× / +2.78×** | 3.07× behind |

**Headline lifts vs pre-WHERE-VM (SP-Hash-Agg-Tune)**: Q1 N=1 +1.47×,
Q1 N=4 +1.35×; Q6 N=1 +4.41×, Q6 N=4 +2.78×.
**Cumulative 5-arc lift vs pre-arc baseline (SP-Bench-Suite T4)**:
Q1 N=4 **+9.71×** (8.84 → 85.82 q/s); Q6 N=4 **+39.95×** (13.74 →
548.87 q/s).
**Gap vs Postgres**: Q1 N=4 2.92× → **2.17×** (was 18× pre-arc);
Q6 N=4 8.53× → **3.07×** (was 123× pre-arc).

**Spec floor delivery**:
- Q6 N=4 acceptance target (≥400 q/s, named in design §4) EXCEEDED
  by 37% (548.87 q/s).
- Q6 N=4 stretch target (≥500 q/s, named in design §4) ALSO EXCEEDED
  by 10%.
- Q6 user-spec floor (≥350 q/s, inherited from SP-Hash-Agg-Tune):
  EXCEEDED by 57%.
- Q1 N=4 acceptance target (≥75 q/s, named in design §4) EXCEEDED
  by 14% (85.82 q/s).
- Q1 user-spec floor (≥120 q/s, inherited from SP-Hash-Agg-Tune):
  still MISSED (71% achieved); the remaining cost is the per-row
  aggregate-fold inner loop (4 measures × ~60K rows full-scan), not
  WHERE evaluation. SP-JIT-Aggregate targets it.

The SP-Hash-Agg-Tune diagnosis is now validated end-to-end: per-row
WHERE-eval WAS the dominant cost on TPC-H Q1/Q6 shapes; the
closure-built-once-per-query approach cut it as modelled (Q6 sits at
the high end of the spec's 1.5-2.5× modelled band).

## T5 — arc closure [DONE]

Commits:
- (this commit) docs(status + tracker): SP-WHERE-VM-Specialise T5 —
  arc closure (V1 SHIPPED)

- STATUS.md row (next Track letter after Track A.-1.1) — added with
  full headline + 5-arc cumulative + named follow-up arc
  (SP-JIT-Aggregate)
- BENCHMARKS.md §3f POST-WHERE-VM column — added with full lift +
  cumulative + gap-closing math; Q1 honest read rewritten to name
  the new bottleneck (per-row aggregate-fold inner loop)
- BENCHMARKS.md §3g POST-WHERE-VM column — added with same shape;
  Q6 honest read flipped from DONE_WITH_CONCERNS-style to SHIPPED-
  style (spec floors EXCEEDED)
- BENCHMARKS.md §1 summary table TPC-H rows — updated with new
  numbers + cumulative lifts + 2.17× / 3.07× gaps
- BENCHMARKS.md §4 raw-results file list — adds the postvm JSON
  family
- This tracker → V1 SHIPPED
- SP-JIT-Aggregate named as the next arc (closes the residual 2.17×
  Q1 / 3.07× Q6 gaps via LLVM/cranelift codegen for the per-row
  aggregate-update inner loop, what Postgres uses)
- TaskList #357 ready for completion

**Final standing-rules check (T5):**
- vulcan SSH used for bench data only (no rebuild needed in this
  closure pass)
- Direct commits to main, no Co-Authored-By, no `-S`, push after each
- CI green check after each push
- `#![forbid(unsafe_code)]` honored (just std + Box<dyn Fn>)
- No new external deps
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched (docs-only
  in T4/T5; T1-T2 only touched `kessel-expr` + `kessel-sm` internals,
  no wire format changes)
- Determinism oracle: closure == interpreter byte-for-byte on the
  same row (KAT-locked at both expr + SM level); SP-Hash-Agg + Tune
  KATs still green
- Replication (VSR): aggregates are reads; never replicated
- Target KAT delta met: +17 (15 expr-level T1 + 2 SM-level T2)
