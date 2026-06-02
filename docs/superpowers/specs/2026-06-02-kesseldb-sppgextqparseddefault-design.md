# SP-PG-EXTQ-PARSED-DEFAULT — flip the gateway typed-param path to default — DESIGN

**Status:** design — scopes the SP-PG-EXTQ-PARSED V1 follow-up named in
`docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparsed-design.md`
§2.2 V2 follow-ups (`SP-PG-EXTQ-PARSED-DEFAULT`). SP-PG-EXTQ-PARSED V1
closed 2026-06-02 with the typed-param entry point shipped OPT-IN
(`compile_with_params` + `preprocess_typed_params` classifier wired
end-to-end at the KAT layer); this arc flips the default in the
gateway's `dispatch_execute` so the typed path becomes the standard
runtime route, with the text-substitution path remaining as a narrow
fallback for FLOAT/TIMESTAMPTZ/NUMERIC/BYTEA-binary parameters the
typed path cannot represent cleanly.

Companion progress tracker:
`docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgextqparseddefault-progress.md`.

**Builds on:**
- **SP-PG-EXTQ-PARSED V1** (closed 2026-06-02) — the
  `compile_with_params(sql, cat, params: &[Option<Value>])` entry
  point + the `preprocess_typed_params(params, formats, oids) ->
  Option<Vec<Option<Value>>>` gateway classifier. V1 shipped both
  but kept `dispatch_execute` on the text-substitution path; this
  arc flips that.
- **SP-PG-EXTQ V1** (closed 2026-05-29) — the
  `extq::substitute::preprocess_params` text path + `dispatch_execute`
  glue. Stays in tree as the FALLBACK path for parameters the typed
  classifier returns `None` for.

---

## 1. Context — why flip the default

SP-PG-EXTQ-PARSED V1's progress tracker calls out the disposition
verbatim:

> V1 disposition: typed path is opt-in (KAT-only); default
> `dispatch_execute` still uses the text-substitution path. Follow-up
> `SP-PG-EXTQ-PARSED-DEFAULT` flips the default after soak.

The reason for the cautious V1 default was compat-regression risk: the
typed-param path threads `kessel_codec::Value`s through
`compile_with_params` instead of building a SQL text string with the
substituted literal. While V1's KATs lock the byte-equal property for
every typed-eligible parameter shape (INT2/4/8, BOOL, TEXT/VARCHAR
— and the same in binary format), real ORM traffic has long-tail
shapes (mixed-OID, weird coercions, edge-case quoting) the KAT corpus
can't exhaustively cover.

The flip-to-default is the security headline because:

1. **The text-substitution path is the V1 §11 weak-spot #1 attack
   surface.** Every parameter value goes through the `'`->`''` escape
   path before being concatenated into SQL text. The escape is correct
   today; the attack surface is real (any missed quoting rule lets the
   value escape the quoted region).
2. **The typed path bypasses string concatenation entirely.** The
   bound value enters as a `Value` (typed) and emerges in the program
   as the same `Value` — no quoting, no escape rules, no
   concatenation. Closes the SP-PG-EXTQ V1 §11 weak-spot #1 attack
   surface at the dispatch layer (V1 closed it at the kessel-sql +
   classifier layer; this arc extends the close to dispatch).

## 2. Scope

### 2.1 V1 — what's in (this arc, T1..T4)

1. **New trait method `EngineApply::apply_sql_with_params(sql: &str,
   params: &[Option<Value>]) -> OpResult`.** Default impl exists for
   backward compat: forwards to `apply_sql` after rendering the params
   inline via the V1 text-substitution path (so existing
   `EngineApply` impls compile without change). The real impl on
   `kesseldb-server::EngineHandle` (gated behind the `pg-gateway`
   feature) sends a new admin frame tag `PARAMETERIZED_SQL_TAG = 0xF3`
   that carries `(sql, params)` over the wire to the engine thread,
   where the apply path decodes + runs `compile_stmt_with_params`
   against the live `Catalog`.

2. **Wire frame format for `PARAMETERIZED_SQL_TAG = 0xF3`.**
   ```
   [0xF3]
   [u32 LE sql_len][sql bytes]
   [u32 LE param_count]
   param_count × ParamSlot
   ```
   `ParamSlot` is a tagged union:
   - `0x00` = `None` (SQL NULL).
   - `0x01 [i128 LE]` = `Value::Int`.
   - `0x02 [u128 LE]` = `Value::Uint`.
   - `0x03 [u32 LE len][bytes]` = `Value::Blob`.
   - `0x04` = `Value::Null` (explicit; kept distinct on the wire
     so a future PG-NULL-with-OID-hint path can use it).

3. **Gateway `dispatch_execute` flip.** The first Execute on a portal
   now tries the typed path:
   ```rust
   let typed = preprocess_typed_params(&param_refs, &formats, &param_oids);
   let dispatched = if let Some(typed) = typed {
       dispatch::dispatch_query_with_params(&sql, &typed, engine)
   } else {
       // Fallback — V1 text-substitution.
       dispatch::dispatch_query(&rewritten, engine)
   };
   ```
   The text fallback remains for FLOAT4/FLOAT8/TIMESTAMPTZ/NUMERIC
   AND BYTEA binary (BYTEA binary needs the `'\xHEX'::bytea` cast
   wrapper that only the text-substitute path emits).

4. **`dispatch::dispatch_query_with_params(sql, params, engine)`
   helper.** Mirrors `dispatch_query` shape (RowDescription / DataRow
   / CommandComplete / RFQ) but routes the engine call through
   `engine.apply_sql_with_params(sql, params)` instead of
   `engine.apply_sql(sql)`.

5. **KATs** for the dispatch flip:
   - Typed path becomes default for psycopg2/asyncpg/JDBC patterns.
   - Text-fallback engaged when typed-decode returns `None`.
   - Quote-injection security KAT exercised at the dispatch layer.

### 2.2 V1 — what's out (named V2+ follow-ups)

- **SP-PG-EXTQ-PARSED-INFER** — OID-driven inference at Parse time.
- **SP-PG-EXTQ-PARSED-CACHE** — pre-compiled prepared-statement
  AST cache.
- **SP-PG-EXTQ-PARSED-BYTEA-TYPED** — adding raw-bytes typed support
  so BYTEA binary doesn't need to fall back to text-path.

## 3. Acceptance criteria

V1 (T1..T4) ships when:

1. **`EngineApply::apply_sql_with_params` trait method exists** with
   a default impl that forwards to `apply_sql` after rendering.
2. **`EngineHandle` (kesseldb-server) overrides** with a real impl
   that sends `PARAMETERIZED_SQL_TAG` + decodes on engine thread +
   runs `compile_stmt_with_params` against live catalog.
3. **`dispatch_execute` prefers typed path** when
   `preprocess_typed_params` returns `Some`; falls back to text-
   substitution when `None`.
4. **All V1 KATs still pass** byte-equal. No regression in extq /
   substitute / kessel-sql.
5. **+5-10 new KATs locking the dispatch flip**.
6. **Real-ORM smoke regression-free** on vulcan (psycopg2 + asyncpg).
7. **Quote-injection wire test**: an INSERT of `"; DROP TABLE inj; --`
   via psycopg2 → SELECT confirms the row was stored verbatim AND the
   table was NOT dropped. HEADLINE.
8. **CI green.** `#![forbid(unsafe_code)]` honored. No new external
   deps.

## 4. Task decomposition (T1..T4)

| T# | Scope | KAT delta |
|---|---|---|
| **T1** | Design spec + progress tracker + `EngineApply::apply_sql_with_params` trait + `PARAMETERIZED_SQL_TAG` constant + wire encode/decode helpers + render_params_into_sql fallback helper + 4 wire-encoder KATs + 2 render-rule KATs. | +6 |
| **T2** | Gateway `dispatch_execute` flip + `dispatch::dispatch_query_with_params` helper + `EngineHandle` real impl on the engine side + +5 KATs covering dispatch flip + text-fallback + quote-injection at dispatch layer. | +5 |
| **T3** | vulcan ORM smoke + injection test. No new KATs; the smoke runs against the deployed binary. | 0 |
| **T4** | USAGE §9 note + STATUS row + progress tracker → CLOSED. | 0 |

Estimated V1 total: **+11 KATs across 4 slices**.

## 5. References

- V1 design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparsed-design.md`
- V1 progress tracker (CLOSED): `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgextqparsed-progress.md`
- Parent SP-PG-EXTQ V1 spec: `docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
- Gateway dispatcher: `crates/kessel-pg-gateway/src/extq/mod.rs::dispatch_execute`
- Classifier: `crates/kessel-pg-gateway/src/extq/substitute.rs::preprocess_typed_params`
- kessel-sql entry: `crates/kessel-sql/src/lib.rs::compile_with_params`
- Engine bridge: `crates/kesseldb-server/src/lib.rs` (EngineApply impl on EngineHandle)
