## SP-PG-EXTQ-CAST-VALIDATE-COMPAT — relax strict OID equality to PG type-category compatibility

Date created: 2026-06-02
Parent SP-arc: SP-PG-EXTQ-CAST-VALIDATE V1 (closed 2026-06-02). The
closing note explicitly named this arc as the follow-up that would
relax strict OID equality to PG's type-category compatibility table
once workloads demanded it.

## 1 — Context: strict OID equality is too strict for real ORMs

SP-PG-EXTQ-CAST-VALIDATE V1 closed the "strip + hope" silent-coercion
vector by rejecting any Bind whose declared parameter OID disagrees
with the `$N::TYPE` cast OID in the SQL text. V1 enforced STRICT
equality:

```
Parse SQL: "INSERT INTO t (id, n) VALUES ($1::int8, $2)"
            param_oids: [PG_TYPE_INT4 (23), PG_TYPE_TEXT (25)]
                         ^^^^^^^^^^^^^^^^^
Bind: …
V1 result: 42846 cannot_coerce — INT4 (23) != declared INT8 (20)
```

This rejection is technically correct against the V1 contract — but
it's wrong against actual ORM behaviour. The pattern above is the
DEFAULT pgJDBC binding shape: Java `long` (64-bit) maps to INT8 in
the JDBC type table, but pgJDBC will happily Bind a Java `int` (32-
bit) against an `::int8` cast in the SQL, because PG itself accepts
the widening cast at runtime. psycopg3 has the same behaviour with
Python `int` — its `register_default_adapters` registers `int` →
INT4 by default, but a `WHERE id = %s::int8` clause with `id`
being a small Python int sends INT4 + INT8 mismatched at the wire.

The user-visible bug pre-this-arc: every pgJDBC INT4-against-INT8
prepared statement returns `42846 cannot_coerce`, and the symptom is
"my Java ORM that worked against real PG breaks on KesselDB". The
fix is to relax the strict equality to PG's type-category
compatibility table — within a category (numeric, string), accept;
across categories (numeric vs string), reject.

## 2 — PG type categories (from `pg_type.dat::typcategory`)

PG's catalog tags every type with a single-byte `typcategory`:

| Category | Meaning | V1 OIDs in this category |
|---|---|---|
| `'N'` | Numeric | int2 (21), int4 (23), int8 (20), float4 (700), float8 (701), numeric (1700) |
| `'S'` | String | text (25), varchar (1043), bpchar (1042) |
| `'B'` | Boolean | bool (16) |
| `'D'` | Date/time | date (1082), time (1083), timestamp (1114), timestamptz (1184), interval (1186) |
| `'U'` | User-defined / unknown / binary | bytea (17) + every OID this arc doesn't recognise |
| `'A'` | Array | int4[] (1007), text[] (1009), … |

(PG actually re-uses `'B'` for `bytea` because PG groups bytea with
"Bit-string types" — V1 of this arc keeps bytea isolated in `'U'`
to avoid surprising compatibility with future bit-string types we
don't yet support. The validator effect is the same — bytea ↔
bytea only — so the `'U'` choice doesn't widen any attack surface.)

## 3 — V1 scope (this arc)

- **In-scope.** Add `oid_category(oid: u32) -> char` returning the V1
  type's category byte. Add `oid_castable(param_oid, cast_oid) ->
  bool` returning true iff the cast should be accepted:
  - `param_oid == cast_oid` (V1 strict equality) → true.
  - `param_oid == 0` (Parse omitted OID hint) → true (V1 skip rule
    preserved).
  - `oid_category(param_oid) == oid_category(cast_oid)` → true
    (V2 widening).
  - Otherwise → false (cross-category rejection).
  Replace `dispatch_bind`'s `actual_oid != declared_oid` strict
  check with `!oid_castable(actual_oid, declared_oid)`.
- **In-scope KATs.** ~8-12 new KATs covering the helper + the
  dispatch-side validator widening + the existing V1 strict-mismatch
  regression guard (cross-category cases still reject).
- **In-scope smoke.** psycopg INT4 param + INT8 cast on vulcan now
  succeeds (was: 42846). Cross-category mismatch (TEXT param + INT8
  cast) still rejects with 42846.

## 4 — Acceptance

1. Widening within the `'N'` category (INT4 + INT8 cast, INT8 + INT4
   cast, INT8 + FLOAT8 cast) succeeds at Bind.
2. Widening within the `'S'` category (TEXT + VARCHAR cast, VARCHAR
   + TEXT cast) succeeds at Bind.
3. Cross-category casts (INT + TEXT, BOOL + INT, BYTEA + TEXT) still
   reject with `ExtqError::CastOidMismatch` → `42846 cannot_coerce`.
4. The V1 omitted-OID skip rule (actual_oid == 0) still applies.
5. All existing V1 `cast_validate_t2_*` KATs that lock the strict
   equality + skip + first-mismatch-wins contracts CONTINUE to pass
   byte-for-byte (the V2 widening is additive — same-OID and
   omitted-OID inputs still produce the same outcome).

## 5 — Out-of-scope (named follow-ups)

- **`SP-PG-EXTQ-CAST-VALIDATE-COMPAT-RANGE`** — overflow-check the
  param value against the cast type's range at the gateway (e.g.
  INT4 param value `100000` against INT2 cast). PG actually errors
  with `22003 numeric_value_out_of_range` here; V1 of THIS arc
  punts to the engine type-checker.
- **`SP-PG-EXTQ-CAST-VALIDATE-LITERAL`** — also relax-and-validate
  literal casts (`'42'::int8`). V1 of the parent arc punts literal-
  cast validation; this arc inherits that scope boundary.
- **`SP-PG-EXTQ-CAST-VALIDATE-CATEGORY-CROSS`** — accept SOME
  cross-category casts that PG itself accepts (e.g. TEXT '42' →
  INT8 via `INVOKEMETHOD`-style coercion). V1 of this arc is
  conservative — within-category only.

## 6 — Implementation shape

### 6.1 `crates/kessel-pg-gateway/src/types.rs` additions

```rust
/// SP-PG-EXTQ-CAST-VALIDATE-COMPAT — returns the PG `typcategory`
/// byte for the V1 type OID set. Unknown OIDs return `'U'` (user-
/// defined / unknown) — they form their own degenerate category and
/// only compare-equal to themselves.
pub fn oid_category(oid: u32) -> char {
    match oid {
        PG_TYPE_BOOL => 'B',
        PG_TYPE_BYTEA => 'U', // bytea isolated; only ↔ bytea matches
        PG_TYPE_INT2
        | PG_TYPE_INT4
        | PG_TYPE_INT8
        | PG_TYPE_FLOAT4
        | PG_TYPE_FLOAT8
        | PG_TYPE_NUMERIC => 'N',
        PG_TYPE_TEXT | PG_TYPE_VARCHAR | 1042 /* bpchar */ => 'S',
        PG_TYPE_TIMESTAMPTZ | 1083 /* time */ | 1114 /* timestamp */ => 'D',
        _ => 'U',
    }
}

/// SP-PG-EXTQ-CAST-VALIDATE-COMPAT — returns true iff a Bind whose
/// param has `param_oid` should be accepted against a SQL cast to
/// `cast_oid`. V1 strict equality is the base case; the V2 widening
/// adds intra-category compatibility (any INT/FLOAT ↔ any INT/FLOAT,
/// any TEXT/VARCHAR ↔ any TEXT/VARCHAR, etc.).
pub fn oid_castable(param_oid: u32, cast_oid: u32) -> bool {
    if param_oid == cast_oid { return true; }
    if param_oid == 0 { return true; } // V1 skip rule — omitted hint
    oid_category(param_oid) == oid_category(cast_oid)
}
```

### 6.2 `dispatch_bind` cast-validation loop replacement

Replace the existing `if actual_oid != declared_oid` strict check
with `if !oid_castable(actual_oid, declared_oid)`. The skip rule
for `actual_oid == 0` collapses into the helper (no duplicate
check). The error variant + state set + first-mismatch-wins
ordering are byte-untouched.

## 7 — Closure shape

2-3 commits per the standing rules:

1. T1 + T2 — design spec + `types::oid_category` + `types::
   oid_castable` + `dispatch_bind` widening + KATs.
2. T3 — vulcan smoke (psycopg INT4 + INT8 cast accepted; cross-
   category still rejects) + USAGE §9 note.
3. T4 — STATUS row + parent SP-PG-EXTQ-CAST-VALIDATE progress
   tracker follow-up entry pivoted to "CLOSED" + this progress
   tracker CLOSED.

KAT delta target: +8-12.
