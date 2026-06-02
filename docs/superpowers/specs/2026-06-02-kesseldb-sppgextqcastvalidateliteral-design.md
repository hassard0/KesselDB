## SP-PG-EXTQ-CAST-VALIDATE-LITERAL — extend cast-validation to literal `::TYPE` casts

Date created: 2026-06-02
Parent SP-arcs:
- SP-PG-EXTQ-CAST-VALIDATE V1 (closed 2026-06-02) named this arc as
  a follow-up: "also validate that `::TYPE` casts on literals (not
  just `$N` placeholders) are well-typed".
- SP-PG-EXTQ-CAST-VALIDATE-COMPAT V1 (closed 2026-06-02) widened
  the `$N` validator to PG's `typcategory` table; this arc reuses
  `types::oid_category` + `types::oid_castable` for literal-cast
  category comparisons.

## 1 — Context: V1 + COMPAT only validate `$N::TYPE`

V1 of SP-PG-EXTQ-CAST-VALIDATE tracked `(param_index, declared_oid)`
pairs ONLY when the `::TYPE` operator was preceded by a `$N`
placeholder. A literal cast like `WHERE id = 1::int8` records no
tracking pair (locked by KAT
`tracked_strip_does_not_track_literal_cast`) — the stripper just
drops the `::int8` bytes and the engine sees `WHERE id = 1`.

That's fine when the literal's natural type lines up with the cast
type. But it lets cross-category literal casts through silently:

```
SELECT 'hello'::int8         -- 'S' literal cast to 'N'; PG rejects, V1 strips
SELECT true::int8            -- 'B' literal cast to 'N'; PG rejects, V1 strips
SELECT '2024-01-01'::date    -- 'S' literal cast; V1 strips (PG parses date string)
```

For `'hello'::int8` specifically, V1's strip yields `SELECT 'hello'`,
which the engine type-checker rejects only IF the value reaches a
typed column — in `WHERE id = 'hello'` (id INT8) it errors, but in
`SELECT 'hello'::int8` standalone it returns a TEXT column. That's
the silent-strip hole this arc closes.

## 2 — V1 scope (this arc)

- **In-scope.** Detect `LITERAL::TYPE` patterns in the stripper +
  classify the literal's natural type. Reject cross-category
  literal casts with `42846 cannot_coerce` before the strip even
  rewrites the SQL.
- **In-scope literal shapes.**
  - Bare integer literal (no decimal, no quote, optional leading
    `-`): `0..2147483647` → INT4 (23); `>= 2147483648` or
    `< -2147483648` → INT8 (20). Category `'N'`.
  - Bare float literal (has decimal `.` and no quote, optional
    leading `-`): FLOAT8 (701). Category `'N'`.
  - Single-quoted string literal (`'…'` with PG `''` escape):
    TEXT (25). Category `'S'`.
  - Bool keyword (`true` / `false`, case-insensitive,
    word-boundary): BOOL (16). Category `'B'`.
  - `NULL` keyword (case-insensitive, word-boundary): special-case
    accept regardless of cast type (PG `NULL::TYPE` is the canonical
    typed-NULL idiom).
- **In-scope error shape.** First cross-category mismatch surfaces
  via the same SQLSTATE `42846 cannot_coerce` wire frame V1 already
  uses. Message:
  `cannot cast literal of category '<L>' to type with OID <oid> (category '<C>')`.
- **In-scope KATs.** ~8-12 covering each literal classifier branch
  + cross-category rejection + within-category acceptance + NULL
  pass-through + regression-guard that the existing `$N` tracking
  + byte-equal wrappers still work.
- **In-scope smoke.** vulcan psql smoke for the canonical
  acceptance + rejection shapes.

## 3 — V1 simplifications (out-of-scope, named follow-ups)

- **`SP-PG-EXTQ-CAST-VALIDATE-LITERAL-EXPR`** — recognise
  literal-cast patterns inside expressions (e.g.
  `WHERE id = (1 + 2)::int8`). V1 only inspects the bytes
  IMMEDIATELY before `::`; an arbitrary expression yields no
  classifiable natural type so we don't reject (fall through to V1
  strip + hope at the engine type-checker layer).
- **`SP-PG-EXTQ-CAST-VALIDATE-LITERAL-DATEPARSE`** — PG actually
  ACCEPTS `'2024-01-01'::date` because it parses the literal at
  cast time. V1 rejects this as 'S' vs 'D' cross-category; lift
  when a real workload needs it.
- **`SP-PG-EXTQ-CAST-VALIDATE-LITERAL-NUMSTR`** — PG also accepts
  `'42'::int8` (string→numeric parse). V1 rejects as 'S' vs 'N';
  same follow-up arc as DATEPARSE (it's the same coercion family
  PG handles via input-function parsing).
- **`SP-PG-EXTQ-CAST-VALIDATE-LITERAL-MULTIWORD`** — share the
  multi-word-type-name boundary with the parent arc's
  `SP-PG-EXTQ-CAST-VALIDATE-MULTIWORD` follow-up (V1 strips only
  the first identifier of `TIMESTAMP WITH TIME ZONE` so the
  literal validator can't see the full type).

## 4 — Implementation shape

### 4.1 `crates/kessel-pg-gateway/src/cast_stripper.rs` additions

```rust
/// SP-PG-EXTQ-CAST-VALIDATE-LITERAL — describes a single
/// cross-category literal cast detected by the stripper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiteralCastMismatch {
    /// Inferred natural PG OID of the literal (e.g. 25 TEXT for
    /// `'hello'`, 23 INT4 for `42`).
    pub literal_oid: u32,
    /// PG OID the SQL cast declared (e.g. 20 INT8 for `::int8`).
    pub cast_oid: u32,
    /// PG `typcategory` byte of the literal (per `types::oid_category`).
    pub literal_category: char,
    /// PG `typcategory` byte of the cast type.
    pub cast_category: char,
}

/// SP-PG-EXTQ-CAST-VALIDATE-LITERAL — scan `sql` for any
/// `LITERAL::TYPE` patterns whose literal natural category
/// disagrees with the cast type's category. Returns the FIRST
/// mismatch (first-mismatch-wins ordering, same as the `$N`
/// validator) or `None` if every literal cast is within-category /
/// the literal is `NULL` / there are no literal casts at all.
pub fn find_literal_cast_mismatch(sql: &str) -> Option<LiteralCastMismatch>;
```

Algorithm sketch (single-pass, ~O(sql.len()), zero-alloc beyond a
small lookback window):

```
walk the SQL with the same string/line-comment/block-comment context
the existing stripper uses;
on each `::`:
  classify the bytes immediately before:
    - `NULL`  (case-insensitive, word-boundary): record literal_oid=0
      (the "anytype null" sentinel) -> always accept
    - `true` / `false` (case-insensitive, word-boundary): BOOL
    - quoted string (the byte before `::` is `'` ending a string we
      just exited): TEXT
    - `$N` placeholder: NOT A LITERAL — skip (covered by V1+COMPAT)
    - bare integer / float (digits, optional `.`, optional leading
      `-`): INT4 / INT8 / FLOAT8 based on shape
    - anything else (identifier, `)`, etc): NOT A LITERAL — skip
  if a literal was classified AND the cast type maps to a known OID:
    compute literal_category + cast_category via types::oid_category
    if literal_oid == 0 (NULL): accept
    else if literal_category != cast_category: RECORD MISMATCH, return Some
  else: continue scanning
```

Dispatcher wiring (one new check at each dispatch entry):

```rust
// In dispatch::dispatch_query + dispatch::dispatch_query_with_params:
if let Some(mismatch) = crate::cast_stripper::find_literal_cast_mismatch(sql) {
    return literal_cast_mismatch_response(&mismatch);
}
let stripped = crate::cast_stripper::strip_pg_casts(sql);
// ... existing dispatch ...
```

```rust
// In extq::dispatch_parse:
if let Some(mismatch) = crate::cast_stripper::find_literal_cast_mismatch(&sql) {
    return set_err(state, ExtqError::LiteralCastMismatch { ... });
}
let (_, param_casts) = crate::cast_stripper::strip_pg_casts_tracked(&sql);
```

Adding a new `ExtqError::LiteralCastMismatch` variant + a
`server.rs` renderer follows the V1 `CastOidMismatch` shape exactly.

### 4.2 KAT plan (T2, in `cast_stripper::tests`)

- `literal_int_cast_accepts_within_numeric_category` —
  `find_literal_cast_mismatch("SELECT 1::int8") == None`.
- `literal_string_cast_into_numeric_rejects` —
  `find_literal_cast_mismatch("SELECT 'hello'::int8")` returns
  TEXT vs INT8 ('S' vs 'N') mismatch.
- `literal_float_cast_into_numeric_accepts` —
  `find_literal_cast_mismatch("SELECT 1.5::int8") == None`.
- `literal_string_to_text_accepts` —
  `find_literal_cast_mismatch("SELECT 'hello'::text") == None`.
- `literal_bool_to_bool_accepts` —
  `find_literal_cast_mismatch("SELECT true::bool") == None`.
- `literal_null_cast_always_accepts` —
  every `NULL::TYPE` returns `None`, even `NULL::int8` /
  `NULL::text` / `NULL::bytea`.
- `literal_bool_to_int_rejects` —
  `find_literal_cast_mismatch("SELECT true::int8")` returns BOOL
  vs INT8 ('B' vs 'N') mismatch.
- `literal_negative_integer_accepts_into_int8` —
  `find_literal_cast_mismatch("SELECT -1::int8") == None`.
- `literal_mismatch_first_wins` — mixed
  `'hello'::int8 AND $1::text` returns the literal mismatch first
  (before the `$N` validator runs at Bind time).
- `literal_pure_passthrough_no_casts` —
  `find_literal_cast_mismatch("SELECT 1 FROM t")` returns `None`.
- `literal_dollar_param_cast_not_classified_as_literal` —
  `find_literal_cast_mismatch("SELECT $1::int8") == None` (the
  `$N` validator covers this case).
- `literal_string_with_doubled_quote_classified_as_text` —
  `'O''Reilly'::int8` is TEXT-natural-typed → 'S' vs 'N' mismatch.

KAT delta target: +8-12 (within the design target).

## 5 — Acceptance

1. `'hello'::int8` rejects with `42846 cannot_coerce` (literal
   mismatch).
2. `1::int8` still accepts (within-category numeric narrowing).
3. `NULL::int8` still accepts (NULL is anytype).
4. `'hello'::text` still accepts (within-category 'S').
5. Every existing CAST + CAST-VALIDATE + CAST-VALIDATE-COMPAT KAT
   still passes byte-for-byte (the literal validator is additive
   — `strip_pg_casts` + `strip_pg_casts_tracked` byte outputs are
   unchanged).
6. vulcan smoke: valid `INSERT (1::int8, 'hello'::text)` succeeds;
   `SELECT * FROM lit_smoke WHERE n = 42::int8` rejects (BOOL vs
   ... well, the smoke replaces the value with a known-mismatching
   shape — see T3 plan).

## 6 — Closure shape

2-3 commits per the standing rules:

1. T1 + T2 — design spec + `cast_stripper::find_literal_cast_mismatch` +
   `LiteralCastMismatch` + dispatcher wiring (simple-query +
   typed-params + extq parse) + `ExtqError::LiteralCastMismatch` +
   server.rs renderer + KATs.
2. T3 — vulcan psql smoke + USAGE §9 note.
3. T4 — STATUS row + parent SP-PG-EXTQ-CAST-VALIDATE-COMPAT
   progress tracker follow-up entry pivoted to "CLOSED" + this
   progress tracker CLOSED.

KAT delta target: +8-12.

CI green is the release gate per standing rules; no binaries
(text-classifier-only behaviour change).
