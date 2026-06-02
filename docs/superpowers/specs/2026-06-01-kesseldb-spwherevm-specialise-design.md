# SP-WHERE-VM-Specialise — closure-built-once-per-query WHERE evaluator

**Arc:** SP-WHERE-VM-Specialise (V1)
**Track:** Analytics planner — drives down the per-row stack-VM dispatch
cost diagnosed by SP-Hash-Agg-Tune as the dominant TPC-H Q1 / Q6 wall-time
ceiling at N=4.
**Status:** T1 design (this doc).
**Date:** 2026-06-01.
**Parent:** SP-Hash-Agg-Tune (V1 SHIPPED 2026-05-30 DONE_WITH_CONCERNS) —
both §3f and §3g BENCHMARKS entries named **SP-WHERE-VM-Specialise** as the
follow-up arc.

---

## 1. Context — what SP-Hash-Agg-Tune diagnosed

`docs/BENCHMARKS.md` §3f + §3g, vulcan 3-trial median, SF=0.01 ≈ 60K
lineitem rows, post-Tune sweep:

| Workload | Pre-Tune | Post-Tune | Postgres | Tune lift | 4-arc cumulative |
|---|---:|---:|---:|---:|---:|
| TPC-H Q1 N=4 | 60.18 q/s | **63.77 q/s** | 186 q/s | **+1.06×** | **+7.21×** |
| TPC-H Q6 N=4 | 185.03 q/s | **197.55 q/s** | 1,686 q/s | **+1.07×** | **+14.38×** |

The streaming producer-channel-workers shape DID overlap producer iteration
with worker fold work — but the modest-vs-modelled lift (1.06× vs ≥2×
predicted) revealed that the V1 serial `Vec<Arc<[u8]>>` pre-collect was
NOT the wall-time floor. The actual dominant cost: **the per-row
`kessel_expr::eval` stack VM interpreter** evaluating the WHERE program ×
~60K rows per Q1 query (× 4 workers per query at N=4 = ~240K VM invocations
per second sustained).

Concretely, each `kessel_expr::eval` invocation today:
1. Allocates a fresh `Vec<Value>` stack (`run` calls `Vec::new()`).
2. Walks the bytecode opcode-by-opcode through a `match op { ... }`
   dispatch.
3. For each opcode that touches a field (LOAD_FIELD / IS_NULL), recomputes
   `ot.compute_layout()` AND linear-scans `ot.fields.iter().position(...)`
   to resolve the field-id → offset mapping.
4. Pops + pushes `Value` enums (Int(i128) / Bytes(Vec<u8>) / Null) onto
   the stack for every comparison + logical op.

For Q6's 4-predicate WHERE (`l_shipdate >= 19940101 AND l_shipdate <
19950101 AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24`), the
program is ~25 opcodes. Per row: 4× LOAD_FIELD (each = 1× layout-compute
+ 1× linear field-id scan + 1× field-kind dispatch + 1× decode into
i128), 6× PUSH_INT (each pushes i128 onto Vec stack), 6× cmp (each pops
+ pushes), 3× AND (each pops + pushes). Total ≈ 60-80 Vec push/pop ops
+ 4× redundant layout compute. At 60K rows × 4 workers × N=4 = 960K rows
per second, this becomes ~60M Vec push/pop ops per second per query
dispatcher — exactly the kind of interpretation cost LLVM-codegen
databases (Postgres + DuckDB) sidestep.

## 2. Scope

### V1 IN-SCOPE

1. **`Program::compile_filter(ot, &[range_pred_fields])`** — walks the
   bytecode ONCE per query and returns a `FilterFn = Box<dyn Fn(&[u8])
   -> bool + Send + Sync>` closure that captures pre-resolved field
   offsets + widths + signedness + the comparison/logic tree directly.
   Per-row cost drops to direct field reads + i128/bytes comparisons +
   `&&`/`||` short-circuits — no stack, no dispatch, no field-id
   lookup, no layout recompute.
2. **All existing kessel-expr opcodes specialised** — PUSH_INT,
   PUSH_BYTES, LOAD_FIELD, IS_NULL, EQ, NE, LT, LE, GT, GE, AND, OR,
   NOT. The codegen builds a closure tree (`FilterNode`) that mirrors
   the bytecode shape, then materialises a `Box<dyn Fn(&[u8]) -> bool>`
   whose body is composed-by-construction.
3. **`kessel-sm::aggregate_numeric_scan` uses the closure** — when the
   WHERE program is non-trivial (i.e. not the bare `PUSH_INT 1` uncond
   sentinel), compile_filter is called ONCE BEFORE the parallel-fold
   spawn; per-row `kessel_expr::eval(program, &ot, rec)` becomes
   `filter(rec)`. The closure is `Arc<FilterFn>`-shared across the 4
   workers (Send + Sync constraints honored).
4. **Equivalence oracle** — closure result must be byte-equal to
   `kessel_expr::eval(program, &ot, rec).map(|b| b).unwrap_or(false)`
   on every row, for every supported opcode pattern. Locked by KATs
   that run BOTH paths on 1000 random rows and assert byte-equal.
5. **Compile-time fallback to interpreter for unsupported patterns** —
   if the bytecode contains an opcode we can't yet specialise (e.g.
   ADD/SUB/MUL/DIV — arithmetic in WHERE is rare for analytics), or
   structurally weird shapes (the bytecode wasn't built by the
   standard kessel-sql compiler), `compile_filter` returns
   `Err(CompileError::Unsupported { reason })` and the caller falls
   back to `kessel_expr::eval` per-row. The caller path is:
   ```rust
   let filter: Box<dyn Fn(&[u8]) -> bool + Send + Sync> = match
       program.compile_filter(&ot) {
       Ok(f) => f,
       Err(_) => Box::new(move |rec| {
           kessel_expr::eval(&program_bytes, &ot, rec).unwrap_or(false)
       }),
   };
   ```
   Same fallback shape used for the `aggregate_numeric_scan` hot path;
   other call sites stay on the interpreter (V1 scope-cap — see V2
   below).

### V1 OUT-OF-SCOPE

- **Migrating ALL `kessel_expr::eval` call sites** to compile_filter.
  V1 only migrates `aggregate_numeric_scan` (the proven TPC-H Q1/Q6
  hot path). `Op::QueryRows` + CHECK + trigger paths stay on the
  interpreter; named follow-up arc **SP-WHERE-VM-Specialise-Broad**
  migrates them.
- **JIT codegen (cranelift / LLVM)**. The closure approach is a
  Rust-level specialisation — the dispatch loop disappears but the
  closure body still uses generic `Fn` indirection (1 virtual call per
  predicate at the AND/OR boundaries). True JIT (named arc
  **SP-JIT-Aggregate**) would fold the predicates into a single x86
  basic block. Out of scope here; the closure approach is the lower-
  cost incremental win.
- **Arithmetic opcodes (ADD/SUB/MUL/DIV/MOD)** — these appear in CHECK
  constraints + triggers but rarely in TPC-H WHERE shapes. V1 declines
  to specialise them (returns `Unsupported`); follow-up arc
  **SP-WHERE-VM-Specialise-Arith** adds them.
- **SHA256 / HMAC256 / LIKE** — none appear in TPC-H WHERE; V1 declines.
- **SET_FIELD / REJECT** — trigger-only opcodes; not predicates. V1
  declines.
- **Compile-result caching across queries.** Each query rebuilds the
  closure. For a TPC-H workload running the same query 30s repeatedly
  at SF=0.01, the per-query compile cost is ~30µs which is dwarfed
  by the ~15ms per-query scan-fold; an LRU cache keyed on `(program
  bytes, type_id)` is named follow-up arc **SP-WHERE-VM-Cache**.
- **Vectorisation** — process N rows at once with SIMD over the
  comparison kernel. Named follow-up arc **SP-WHERE-VM-Vectorise**.

### What V1 will NOT change (back-compat guards)

- **Wire format** — zero proto changes; `Op::Aggregate` /
  `Op::GroupAggregateMulti` retain the same program-bytes field.
- **Determinism oracle** — closure result is byte-equal to
  `kessel_expr::eval` per row (KAT-locked).
- **HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched.**
- **Replication (VSR)** — aggregates are reads; never replicated.
- **`#![forbid(unsafe_code)]`** honored (just std + Box<dyn Fn>).
- **No new external deps** — pure Rust closure composition.
- **Determinism contract** — total ordering on Int/Bytes comparisons
  matches the interpreter (Int×Int, Bytes×Bytes, mixed → false; same
  as `run::ord!`).

## 3. Architecture

### 3a. The compile pipeline

```
Program::compile_filter(ot: &ObjectType) -> Result<FilterFn, CompileError>:
    1. Decode bytecode into a Vec<Opcode> with i128 / bytes operands inlined.
    2. Treat opcodes as RPN; build a FilterNode AST.
       - PUSH_INT/PUSH_BYTES → Const leaf
       - LOAD_FIELD(fid) → Load leaf (resolve fid → offset/width/kind ONCE
         here via ot.compute_layout() + ot.fields.iter().position)
       - IS_NULL(fid) → IsNull leaf (resolve fid here)
       - EQ/NE/LT/LE/GT/GE → CmpNode { lhs, rhs, op }
       - AND/OR → LogicNode { lhs, rhs, op }
       - NOT → NotNode { inner }
       - any other opcode → Err(Unsupported)
       - stack underflow / stray operand → Err(Malformed)
    3. The root node is the surviving stack top after the walk. There
       must be exactly one (predicate programs always leave one bool).
    4. Recursively materialise the FilterNode into a Box<dyn Fn(&[u8])
       -> bool + Send + Sync>. Each Cmp/Logic node composes its
       children's closures.
```

### 3b. FilterNode shape (internal, not exported)

```rust
enum Operand {
    ConstInt(i128),
    ConstBytes(Vec<u8>),
    Load { off: usize, width: usize, kind: FieldKind, fid_idx: usize },
}

enum FilterNode {
    Bool(BoolNode),     // returns bool
}

enum BoolNode {
    True,                                       // PUSH_INT(1) — uncond
    False,                                      // PUSH_INT(0) — never matches
    IsNull { fid_idx: usize },                  // IS_NULL(fid)
    Cmp { lhs: Operand, rhs: Operand, op: CmpOp },
    And(Box<BoolNode>, Box<BoolNode>),
    Or(Box<BoolNode>, Box<BoolNode>),
    Not(Box<BoolNode>),
    // Edge case: a comparison applied to a non-bool predicate node,
    // e.g. `(a < b) == 1` (compiler emits this — see kessel-sql).
    // Modelled by wrapping a BoolNode as an Operand-bool via 0/1 cast.
    BoolAsInt(Box<BoolNode>),
}

enum CmpOp { Eq, Ne, Lt, Le, Gt, Ge }
```

### 3c. Materialisation — how the closure is built

The recursive builder turns `BoolNode` into `Box<dyn Fn(&[u8]) -> bool +
Send + Sync>`:

```rust
fn materialise_bool(node: BoolNode) -> Box<dyn Fn(&[u8]) -> bool + Send + Sync> {
    match node {
        BoolNode::True => Box::new(|_| true),
        BoolNode::False => Box::new(|_| false),
        BoolNode::IsNull { fid_idx } => Box::new(move |rec| is_null_at(rec, fid_idx)),
        BoolNode::Cmp { lhs, rhs, op } => materialise_cmp(lhs, rhs, op),
        BoolNode::And(a, b) => {
            let fa = materialise_bool(*a);
            let fb = materialise_bool(*b);
            Box::new(move |rec| fa(rec) && fb(rec))
        }
        BoolNode::Or(a, b) => {
            let fa = materialise_bool(*a);
            let fb = materialise_bool(*b);
            Box::new(move |rec| fa(rec) || fb(rec))
        }
        BoolNode::Not(a) => {
            let fa = materialise_bool(*a);
            Box::new(move |rec| !fa(rec))
        }
        BoolNode::BoolAsInt(a) => materialise_bool(*a), // identity for bool path
    }
}

fn materialise_cmp(lhs: Operand, rhs: Operand, op: CmpOp) -> ... {
    match (lhs, rhs, op) {
        (Operand::Load { off, width, kind, .. }, Operand::ConstInt(v), op)
            if numeric_kind(kind) =>
            specialise_load_vs_const_int(off, width, signed_for(kind), v, op),
        // ... symmetric: const vs load
        // Load vs Load
        // Bytes vs Bytes (Char comparison)
        // Fallback: per-row read both sides into Value + use interpreter cmp
    }
}
```

The hottest path — `Load(off, width, signed) vs ConstInt(v) op CmpOp`
— specialises into:

```rust
fn specialise_load_vs_const_int(
    off: usize, width: usize, signed: bool, v: i128, op: CmpOp,
) -> Box<dyn Fn(&[u8]) -> bool + Send + Sync> {
    match (width, signed, op) {
        (4, false, CmpOp::Ge) => Box::new(move |rec| {
            // bounds check folded; field offset captured
            if rec.len() < off + 4 { return false; }
            let raw = u32::from_le_bytes([rec[off], rec[off+1], rec[off+2], rec[off+3]]);
            (raw as i128) >= v
        }),
        (4, false, CmpOp::Lt) => /* similar */,
        // ... all (width, signed, op) combos
        _ => /* fallback to generic i128 read */,
    }
}
```

For the Q6 WHERE program, the materialised closure is effectively:

```rust
let f = move |rec: &[u8]| -> bool {
    let shipdate = u32::from_le_bytes(rec[80..84].try_into().unwrap()) as i128;
    let discount = u32::from_le_bytes(rec[96..100].try_into().unwrap()) as i128;
    let quantity = u32::from_le_bytes(rec[88..92].try_into().unwrap()) as i128;
    shipdate >= 19940101
        && shipdate < 19950101
        && discount >= 5      // BETWEEN 0.05 (scale-2) AND 0.07
        && discount <= 7
        && quantity < 24
};
```

Compared to the interpreter's ~60-80 Vec push/pop + 4× field-id lookup
+ 4× layout-compute, this is direct memory reads + branch-folded
comparisons — Rust's optimiser unrolls the AND chain into short-circuit
branches.

### 3d. Worker hand-off — closure shipped to workers

`std::thread::scope` workers all take `&filter` (the closure lives in
the outer scope). The closure's captured environment is shared via
borrow — no Arc needed. The `Send + Sync` bound on the closure type
satisfies the `scope.spawn` trait requirement.

For the producer-channel-workers path in `aggregate_numeric_scan`, the
`fold_one` closure captures `&filter` directly (replaces the existing
`if !uncond { kessel_expr::eval(program, &ot, rec) }` branch). All 4
workers reference the same compiled closure — zero per-worker compile
cost.

### 3e. Determinism contract — UNCHANGED from interpreter

- **Truthiness** — `Value::Int(n) if n != 0` is true (same as
  interpreter's `truthy`). Closure short-circuits on `&&`/`||` like
  Rust does; interpreter doesn't short-circuit (it evaluates BOTH
  sides then AND), but the result is byte-equal because both sides are
  pure of the row.
  - **Edge case**: an interpreter program that would compute a
    side-effecting RHS (e.g. DIV by zero on the RHS of an OR whose LHS
    is true) would error in the interpreter. The closure short-circuits
    so the RHS doesn't run. Resolution: V1 declines to specialise
    programs containing DIV/MOD (returns `Unsupported`), so no
    divergence is possible. AND/OR over pure cmp/logic/load is
    side-effect-free; short-circuit is safe.
- **Type-mismatch cmp** — interpreter returns false for Int×Bytes etc.
  Closure does the same explicit check.
- **Null operand** — interpreter returns false for any cmp involving
  Null. Closure does the same explicit check (after reading the field,
  consult the null bitmap; if null, return false).
- **Bytes cmp** — interpreter uses `Vec<u8>::cmp` (lex order). Closure
  uses `&[u8]::cmp` (same lex order — `Vec<u8>` derefs to `&[u8]`).
- **Empty stack at end** — interpreter returns `EmptyResult` error.
  compile_filter checks at compile time that the stack is exactly 1
  bool at end; returns `Malformed` error if not. Caller falls back to
  interpreter, which then errors → translates to `OpResult::SchemaError`
  via the existing path. Same observable behavior.

### 3f. Modelled speedup

Q6 worker per-row WHERE eval today (~25-opcode program):
- Interpreter: ~80 Vec push/pop + 4× HashMap-like lookup + 4× layout
  recompute. Microbench (single-threaded i7): ~1.5 µs / row.
- Closure: ~4 field reads + 6 i128 comparisons + 3 `&&`. Microbench
  estimate (same hardware): ~150-300 ns / row.

Per-query at 60K rows × 4 workers (Q1 N=4 has 4 dispatcher threads × 4
workers each = 16 workers in flight):
- Interpreter: 60K × 1.5µs / 4 workers = 22.5 ms WHERE cost per query.
- Closure: 60K × 250ns / 4 workers = 3.75 ms WHERE cost per query.

Q6 N=4 today: 197 q/s = 5.1 ms / query.
Q6 N=4 with WHERE-VM-Specialise (if WHERE was the only floor): 5.1 -
22.5 + 3.75 = -13.6 ms... which is impossible, indicating the modelled
WHERE share is wrong. The actual share is bounded by total query time
(5.1 ms) so WHERE must be ≤5.1 ms today; cutting it 5× would save ≤4 ms
→ ≥10 q/s → 1.05-1.2× lift floor; the upper bound depends on whether
WHERE truly dominates fold (where SP-Hash-Agg-Tune's diagnosis lands).

Q1 N=4 today: 63.77 q/s = 15.7 ms / query. WHERE cost should be ≥5 ms
here (Q1 scans all 60K rows uncond, NOT narrowed by shipdate range —
so cand=None and the entire 60K runs WHERE eval). Cutting that 5× ≈
saves ~3-4 ms → ~6-8 q/s lift → 1.10-1.13× lift floor.

**Realistic acceptance target**: Q6 N=4 lifts ≥1.5× (197 → ≥300 q/s)
AND Q1 N=4 lifts ≥1.3× (64 → ≥85 q/s). The user spec asks Q6 ≥2× lift
which is at the modelled-stretch ceiling — if the WHERE cost is really
the floor SP-Hash-Agg-Tune diagnosed, we'll hit it; if there's a
secondary floor (e.g. the per-row 8-byte field decode kernel itself
inside the interpreter), we'll land short and the next arc names that
floor.

## 4. Acceptance criteria

- **TPC-H Q6 N=4 on vulcan** lifts from 197.55 q/s → **≥ 400 q/s**
  (user-spec floor — would close the gap vs Postgres from 8.53× to
  4.22×). Stretch: ≥500 q/s (≥2.5× lift).
- **TPC-H Q1 N=4 on vulcan** lifts from 63.77 q/s — best-effort,
  ≥75 q/s acceptable. Q1's WHERE is `l_shipdate <= 19980901` which
  matches nearly every row (the full-scan dominates over the
  comparison itself); lift bounded by the share of wall-time WHERE
  actually consumes.
- **Equivalence** — closure result byte-equal to interpreter result
  on the same row, for every supported opcode pattern. Locked by
  10-15 lib KATs at `kessel-expr` level + 1-2 SM-level KATs at
  `kessel-sm::aggregate_numeric_scan` (specialised vs interpreter on
  10K random rows).
- **All pre-arc tests pass** — `kessel-expr` interpreter is
  untouched; all 8 existing KATs stay green. `kessel-sm`
  `sp_hash_agg_*` KATs stay green (the parallel fold contract is
  untouched; only the WHERE eval branch is rewritten).
- **CI green** on every push.
- **No new external deps**.
- **`#![forbid(unsafe_code)]`** honored.

## 5. Task decomposition

| Task | Description | Acceptance |
|---|---|---|
| **T1** | Design + `compile_filter` + FilterNode + materialise + KATs | This doc + new `kessel-expr::compile_filter` API; 10-12 new lib KATs covering each opcode shape + Compile-fallback + equivalence-on-random-rows |
| **T2** | Closure dispatch for remaining edge cases + equivalence stress KAT (1000 random rows ranging over every supported opcode pattern, compared byte-equal to interpreter) | All edge cases byte-equal; 1-3 more KATs |
| **T3** | `kessel-sm::aggregate_numeric_scan` wiring — compile once, share across workers; equivalence KAT at SM level | 1-2 SM-level KATs lock specialised path == interpreter path; existing `sp_hash_agg_*` KATs stay green |
| **T4** | vulcan TPC-H Q1+Q6 sweep + BENCHMARKS update | §3f + §3g get POST-WHERE-VM-Specialise columns; 3 trials × 30s × SF=0.01 × N=1,4 × KesselDB only; median-of-3 |
| **T5** | arc closure | STATUS row (next Track letter) + progress tracker → CLOSED + TaskList #357 ready |

Total expected KAT delta: +10 to +20.

## 6. Six-plus weak-spot self-review

1. **Box<dyn Fn> indirection is itself a vcall.** Each predicate level
   in the AND/OR tree adds 1 virtual call. For Q6's 4-deep AND chain,
   that's 4 vcalls per row vs the interpreter's 80 push/pop ops —
   still a big win. True elimination requires monomorphisation (closure
   types known at compile time) or a JIT; both are future arcs.
2. **Cmp specialisation matrix is wide.** Width ∈ {1,2,4,8,16},
   signed ∈ {true,false}, op ∈ {Eq,Ne,Lt,Le,Gt,Ge} = 60 combinations
   per (Load vs Const) shape. We use a generic i128-decode fallback
   for combos that don't appear in TPC-H, keeping the file size sane;
   the hot path for Q6 (width=4 unsigned) is fully specialised.
3. **Compile-time cost per query.** Walking ~25 opcodes + building a
   small AST + materialising 4-5 closures: estimated ~10-30 µs per
   query. At Q6's 5.1 ms/query budget, that's <1% overhead. Acceptable
   for V1; SP-WHERE-VM-Cache (LRU keyed on program bytes) would
   eliminate it for repeated queries.
4. **Fallback to interpreter on compile failure.** If `compile_filter`
   returns `Err`, the caller wraps the interpreter in a Box<dyn Fn>
   so the per-row call site is uniform. The fallback closure incurs
   the Box vcall + the interpreter cost — same as today, with one
   extra vcall. Acceptable.
5. **Send + Sync bound on the closure.** The closure captures owned
   values (i128 consts, Vec<u8> consts, raw offsets/widths). All are
   Send+Sync; the Box<dyn Fn(...)+Send+Sync> bound compiles cleanly.
   Workers borrow the closure from the outer scope via `&filter`.
6. **Bytes operand width.** PUSH_BYTES emits a 2-byte length + raw
   bytes. The closure captures the bytes by Vec ownership. For TPC-H
   WHERE these don't appear; spec'd anyway.
7. **Null handling.** Interpreter `load_field` returns `Value::Null`
   when the null bitmap bit is set; downstream cmp returns false.
   Closure does the explicit null-bitmap check inline — for the V1
   TPC-H workloads, the WHERE-touched columns are all NOT NULL by
   schema, so the null check is a constant `false` and the optimiser
   folds it. For Nullable schemas the cost is one bit-test per field
   read.
8. **Stack-underflow vs malformed program.** compile_filter must reject
   programs whose RPN doesn't reduce to exactly one BoolNode at end.
   Returns `Err(CompileError::Malformed)`; caller falls back to
   interpreter which then returns the proper error variant.
9. **Field-id resolution at compile time.** `ot.fields.iter().position(|f|
   f.field_id == fid)` runs ONCE per LOAD_FIELD at compile time, not
   per row. Saves N×60K iter-position calls per query.
10. **Closure as Arc vs scope-borrow.** `std::thread::scope` is used in
    `aggregate_numeric_scan` — workers borrow the closure from the
    scope. No Arc bump per row, no refcount cost. The closure lives
    for the duration of the parallel-fold; outlives the workers via
    scope semantics.
11. **OperatorType evolution.** If the schema's field offsets change
    between when compile_filter runs and when the closure is invoked
    (e.g. ALTER TABLE mid-scan), the cached offsets would be wrong.
    Mitigated by the fact that aggregate scans hold a clone of the
    catalog `ot` for the duration of the scan; ALTER TABLE goes
    through `apply` and respects the same `RwLock` discipline. No
    race in V1.
12. **Compile errors as opaque vs verbose.** `CompileError::Unsupported
    { reason: &'static str }` carries a static name for grep-ability
    (mirrors the SP-PG-EXTQ-BIN arc-naming convention). Operators
    can grep the error to find the missing specialisation.

## 7. Files

- `docs/superpowers/specs/2026-06-01-kesseldb-spwherevm-specialise-design.md` — this spec
- `crates/kessel-expr/src/lib.rs` — new `compile_filter` API + FilterNode
  internals + materialise builder + 10-15 new lib KATs
- `crates/kessel-sm/src/lib.rs` — `aggregate_numeric_scan` calls
  `compile_filter` once + per-row closure invocation replaces
  `kessel_expr::eval`; 1-2 new equivalence KATs
- `docs/BENCHMARKS.md` — §3f + §3g get POST-WHERE-VM-Specialise columns
- `docs/STATUS.md` — Track row added

## 8. Standing rules acknowledgement

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-wherevm`.
- Direct commits to main, no Co-Authored-By, no `-S`, push after each.
- CI green check after push.
- `#![forbid(unsafe_code)]` honored (just std + Box<dyn Fn>).
- No new external deps.
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched.
- Determinism oracle still passes (closure == interpreter byte-for-byte
  on the same row).
