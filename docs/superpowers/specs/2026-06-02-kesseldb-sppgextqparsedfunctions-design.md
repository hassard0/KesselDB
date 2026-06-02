# SP-PG-EXTQ-PARSED-FUNCTIONS — close the scalar-function text-fallback gap — DESIGN

**Status:** design + diagnosis — investigates the
`SP-PG-EXTQ-PARSED-FUNCTIONS` follow-up named in
`docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgextqparseddefault-progress.md`.

**Verdict (DIAGNOSIS, see §2):** the named follow-up is **REDUNDANT**.
Scalar-function SELECTs (`SELECT version()`, `SELECT current_database()`,
`SELECT current_schema()`, `SELECT current_user`, `SELECT 1`, …) do
**NOT** fall through to the text-substitute path under the
SP-PG-EXTQ-PARSED-DEFAULT typed-default regime. They are intercepted by
`pg_catalog::catalog_query_hook` at the TOP of BOTH dispatch entry
points (`dispatch_query_with_params` AND `dispatch_query`), **before**
the typed/text branch is ever consulted and **before** any
`engine.apply_sql*` / `select_star_table` call. This is **Reality A**
from the arc brief.

This arc therefore ships **regression-lock KATs only** — end-to-end
Parse → Bind → Execute coverage proving scalar-function SELECTs are
answered by the catalog synthesizer through the full Extended Query
machinery and never reach the engine's typed or text path — plus this
honest closure documenting the redundancy. No new function support, no
routing change, no behavior delta. The SQL surface is byte-untouched.

Companion progress tracker:
`docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgextqparsedfunctions-progress.md`.

**Builds on:**
- **SP-PG-EXTQ-PARSED-DEFAULT** (closed 2026-06-02) — flipped
  `dispatch_execute` to prefer the typed path
  (`dispatch_query_with_params`) when `preprocess_typed_params` returns
  `Some`, falling back to text-substitute (`dispatch_query`) otherwise.
- **SP-PG-EXTQ-DESCRIBE-VERSION** (closed 2026-06-02) — added
  `extq::scalar_row_descriptions::row_description_for_scalar_select`
  so Describe('S'/'P') on a scalar SELECT emits the matching
  RowDescription, not NoData.
- **SP-PG-CAT** (closed) — `pg_catalog::synthesize::synthesize_helper_function`
  recognizes `version()` / `current_database()` / `current_schema()` /
  `current_user` / `SELECT 1` / `SHOW <guc>` / multi-function probes and
  emits a complete `RowDescription + DataRow + CommandComplete + RFQ`
  frame.

---

## 1. Context — the named follow-up

The arc brief posited: *"SQL containing function calls (e.g.
`SELECT version()`) still falls back to the text-substitute path because
`compile_with_params`/`select_star_table` don't recognize function-call
shapes."* The brief asked to diagnose whether this is a real gap
(**Reality B**) or whether scalar functions already route cleanly
(**Reality A**) before scoping V1.

## 2. DIAGNOSIS — Reality A holds (scalar functions already work cleanly)

### 2.1 The dispatch flow, traced from code

`dispatch_execute` (`crates/kessel-pg-gateway/src/extq/mod.rs:1145`),
on the first (`Pending`) Execute of a portal:

1. Reads the bound params + formats from the portal.
2. Calls `substitute::preprocess_typed_params(&param_refs, &formats,
   &param_oids)` (`extq/mod.rs:1242`).
3. If `Some(typed)` → `dispatch::dispatch_query_with_params(&sql,
   &typed, engine)` (the typed path).
   If `None` → text-substitute, then `dispatch::dispatch_query(&rewritten,
   engine)` (the fallback path).

For a scalar function like `SELECT version()`, there are **0
placeholders**, so `preprocess_typed_params` iterates an empty param
slice and returns **`Some(vec![])`** (`extq/substitute.rs:720-745` —
the loop body never runs; the function unconditionally returns
`Some(out)`). The dispatcher therefore takes the **typed branch** →
`dispatch_query_with_params(sql, &[], engine)`.

Inside `dispatch_query_with_params`
(`crates/kessel-pg-gateway/src/dispatch.rs:80`):

```rust
// pg_catalog interceptor doesn't take params; route through hook.
if let Some(bytes) = crate::pg_catalog::catalog_query_hook(sql, engine) {
    return bytes;                       // dispatch.rs:111 — RETURNS HERE
}
let sql_trimmed = sql.trim().trim_end_matches(';').trim();
let select_table = kessel_sql::select_star_table(sql_trimmed);   // never reached for version()
let result = engine.apply_sql_with_params(sql_trimmed, params);  // never reached for version()
```

The hook (`pg_catalog::catalog_query_hook`, `pg_catalog/mod.rs:113`)
normalizes the SQL and, **before** any table-pattern matcher, runs the
single-call helper recognizer:

```rust
if let Some(bytes) = synthesize::synthesize_helper_function(&normalized) {
    return Some(bytes);                 // mod.rs:133 — version() matches here
}
```

`synthesize_helper_function` (`pg_catalog/synthesize.rs:2288`) matches
`select version()` → `single_text_row("version", KESSELDB_VERSION_STRING)`
(`synthesize.rs:2336`), which builds a complete
`RowDescription + DataRow + CommandComplete + ReadyForQuery('I')` frame
(`synthesize.rs:2183-2194`).

`dispatch_query_with_params` returns those bytes verbatim.
`dispatch_execute` then runs `split_dispatch_query_bytes` over them,
slicing into prelude (RowDescription) / data_rows (the DataRow) /
command_complete, buffers the row, and serves it through the normal
`max_rows` pagination path — exactly as it would for a real table
SELECT.

**Conclusion:** the bound-param classifier, `compile_with_params`,
`apply_sql_with_params`, and `select_star_table` are **all bypassed**
for scalar functions. The catalog hook is an unconditional pre-empt at
the top of dispatch. The fallback-to-text claim in the brief does not
hold; there is no text concatenation, no `apply_sql` call, no
correctness or security gap.

### 2.2 Why the fallback path is also safe (defense in depth)

Even in the hypothetical where `preprocess_typed_params` returned `None`
(it does not, for 0 params), the dispatcher would take the text-fallback
branch → `dispatch_query(&rewritten, engine)`, which **also** calls
`catalog_query_hook(sql, engine)` first (`dispatch.rs:273`). Both dispatch
entry points pre-empt scalar functions identically. There is no code path
under which `SELECT version()` reaches the engine.

### 2.3 Existing coverage vs. the gap this arc fills

The catalog hook's scalar recognition is densely KAT-covered at the
**unit** level (`pg_catalog::synthesize` + `catalog_query_hook` tests:
`version()`, `current_database()`, `current_schema()`, case-folding,
trailing-semicolon, `AS` alias, multi-function probes). The
DESCRIBE-VERSION arc KAT-covers the **Describe** step
(`row_description_or_no_data_for_sql` emits RowDescription not NoData).

What is **not** yet locked is the **end-to-end Extended Query Execute
path** for a scalar function: a full Parse → Bind → Execute through
`dispatch_execute` proving (a) the synthesized DataRow is buffered +
emitted, and (b) the engine's `apply_sql` / `apply_sql_with_params` is
**never invoked**. That invariant — "scalar functions never reach the
engine under the typed-default regime" — is the regression this arc
locks, so a future refactor of the typed/text branch ordering cannot
silently route a scalar function into the engine.

## 3. Scope

### 3.1 V1 — what's in

Regression-lock KATs in `crates/kessel-pg-gateway/src/extq/mod.rs`
(the existing extq test module) using a **panic-on-engine-call** test
engine, so any KAT that touched `apply_sql` / `apply_sql_with_params`
would panic — proving the catalog hook pre-empts:

1. **`SELECT version()` full Parse → Bind → Execute** emits the
   canned `version` RowDescription + DataRow + CommandComplete; engine
   never called.
2. **`SELECT current_database()`** same end-to-end, distinct column +
   value.
3. **`SELECT current_schema()`** same end-to-end.
4. **`SELECT 1`** (scalar-int) end-to-end — `?column?` INT4 DataRow;
   engine never called.
5. **Re-Execute exhaustion** — a second Execute on the drained scalar
   portal emits a bare CommandComplete (no duplicate DataRow), still
   without touching the engine.
6. **Engine-never-called invariant** locked explicitly via the
   panic-on-call engine across the above.

### 3.2 V1 — what's OUT (honestly named)

- **Parameterized scalar functions** (`SELECT upper($1)`,
  `SELECT length($1)`): KesselDB has **no gateway-side scalar-function
  evaluator**, and these are NOT part of any ORM connect-probe corpus
  (drivers issue `version()` / `current_*()` with zero params). Today
  such SQL would miss the catalog hook (it has a `$1`), take the typed
  path, and reach `apply_sql_with_params`, where kessel-sql would reject
  the unsupported `upper(...)` projection. That is **honest rejection**,
  not a silent wrong answer. If a real driver ever requires a
  parameterized scalar function, the follow-up
  **SP-PG-EXTQ-PARSED-FUNCTIONS-PARAM** would add a minimal
  gateway-evaluated set (`upper`/`lower`/`length`/`coalesce`) — deferred
  as YAGNI until a driver demands it.

## 4. Acceptance criteria

V1 (T1..T4) ships when:

1. **DIAGNOSIS documented** (this §2) with code-line evidence that the
   catalog hook pre-empts scalar functions before the typed/text branch.
2. **+5–7 end-to-end regression-lock KATs** in the extq module proving
   scalar-function SELECTs flow Parse → Bind → Execute via the
   synthesizer AND the engine's apply methods are never invoked.
3. **All existing KATs still pass** byte-equal. No regression in extq /
   substitute / pg_catalog / dispatch.
4. **No behavior delta.** SQL/PG-wire surface byte-untouched; this arc
   adds tests + docs only (no `src` logic change).
5. **vulcan smoke** confirms `SELECT version()` + `SELECT
   current_database()` answer correctly over psycopg3 Extended Query.
6. **CI green.** `#![forbid(unsafe_code)]` honored. No new external deps.

## 5. Task decomposition (T1..T4)

| T# | Scope | KAT delta |
|---|---|---|
| **T1** | This design spec + progress tracker. | 0 |
| **T2** | +5–7 end-to-end regression-lock KATs (scalar version/current_database/current_schema/SELECT 1 via Parse→Bind→Execute + engine-never-called invariant + re-Execute exhaustion) in `extq/mod.rs`. | +5–7 |
| **T3** | vulcan psycopg3 Extended-Query smoke (`SELECT version()` + `SELECT current_database()`). No new KATs. | 0 |
| **T4** | STATUS row + progress tracker → CLOSED + smoke transcript. | 0 |

Estimated V1 total: **+5–7 KATs across 4 slices**, regression-lock-only.

## 6. References

- Parent arc tracker (CLOSED): `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgextqparseddefault-progress.md`
- DESCRIBE-VERSION design: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqdescribeversion-design.md` (if present)
- Gateway dispatcher: `crates/kessel-pg-gateway/src/extq/mod.rs::dispatch_execute`
- Typed-path dispatch: `crates/kessel-pg-gateway/src/dispatch.rs::dispatch_query_with_params`
- Catalog hook: `crates/kessel-pg-gateway/src/pg_catalog/mod.rs::catalog_query_hook`
- Scalar synthesizers: `crates/kessel-pg-gateway/src/pg_catalog/synthesize.rs::synthesize_helper_function`
- Scalar RowDescription: `crates/kessel-pg-gateway/src/extq/scalar_row_descriptions.rs`
