## SP-PG-EXTQ-CAST-VALIDATE — close the V1 "strip + hope" silent-coercion vector

Date created: 2026-06-02 (working branch closed same-day at T4)
Parent SP-arc: SP-PG-EXTQ-CAST V1 (closed 2026-06-02 at T2). The closing
note explicitly named this arc as the follow-up:

> **V1 scope is "strip + hope" — V1 doesn't validate that the cast is
> well-typed.** ... V2 `SP-PG-EXTQ-CAST-VALIDATE` could re-introduce
> the explicit type check.

V1's `cast_stripper::strip_pg_casts(sql) -> String` drops every
`::TYPE[(args)]` operator from the SQL text BEFORE the byte stream
reaches `kessel-sql`. The strip unblocks `psql` + pgJDBC simple-mode
+ PostGIS / pgvector helpers — but it also discards the
type-discriminator semantic the cast carried. For an extended-query
`Bind($1) → Execute("SELECT ... WHERE id = $1::int8")`, V1 lets a
TEXT-typed `$1` flow through the strip + reach the engine as if it
had been declared `int8`. A misbehaving (or hostile) client can craft
a Bind whose declared parameter OID disagrees with the SQL's
`::TYPE` operator and the gateway silently coerces.

This arc closes that gap by tracking which `$N` placeholders had a
`::TYPE` cast in the original SQL, then validating at Bind/Execute
that the bound parameter OID matches the declared cast OID. Mismatch
returns a clean `42846 cannot_coerce` ErrorResponse + sets
`error_state` (skip-until-Sync per spec §6).

## 1 — Context: the attack surface

Reproduction (V1, pre-arc):

```
Client: Parse(stmt="", sql="SELECT * FROM t WHERE id = $1::int8",
              param_oids=[25])      # 25 = TEXT
Client: Bind(portal="", stmt="", param_values=["42"], param_formats=[0])
Client: Execute(portal="", max_rows=0)
Client: Sync

V1 server flow:
  cast_stripper strips "::int8" -> "SELECT * FROM t WHERE id = $1"
  substitute renders $1 = "'42'" (text path, single-quoted)
  engine sees: SELECT * FROM t WHERE id = '42'
  engine type-checker coerces '42' -> id's column type (e.g. BIGINT)
  query returns the row
```

The client's contract with the server says "$1 is text format" but
the SQL declares "$1 should be int8". A correct PG server would
reject this with `42846 cannot_coerce`. V1's strip discards the
declaration and silently coerces. While the engine's type-checker
catches the most extreme mismatches (e.g. injecting non-numeric text
into an INT8 column), a TEXT-OID + numeric-content bind passes
through unchecked — opening an ambiguity vector for SQL injection +
type-confusion attacks against ORMs that compose SQL by string
concatenation but pin parameter types via the protocol layer.

## 2 — V1 scope (this arc)

- **In-scope.** Track `(param_index, declared_cast_oid)` pairs at the
  point the strip runs. At Bind time (when param OIDs are known +
  param formats are known), validate every tracked pair: each
  `param_oids[param_index]` MUST equal the corresponding
  `declared_cast_oid` (V1 = strict equality, no implicit cast
  compatibility table). Mismatch → `42846 cannot_coerce` + set
  `error_state`.
- **In-scope KAT delta.** ~10 KATs covering the strip-with-tracking
  function + the dispatch-side validator + the existing-CAST
  regression guard.
- **In-scope smoke.** `psql` literal-cast round-trip (`SELECT 1 FROM
  t WHERE id = 1::int8`) MUST still PASS — V1 only validates `$N`
  casts, NOT literal casts. The literal-cast SQL has no Bind
  parameters so nothing to validate.
- **Out-of-scope — `SP-PG-EXTQ-CAST-VALIDATE-COMPAT`** — relax strict
  OID equality to the PG type-category table (e.g. INT2/INT4/INT8 are
  mutually compatible; TEXT/VARCHAR/BPCHAR are mutually compatible).
  V1 is strict to make the test deterministic; V2 can relax once
  workloads demand it.
- **Out-of-scope — `SP-PG-EXTQ-CAST-VALIDATE-LITERAL`** — also
  validate that literal casts (e.g. `'hello'::int8`) are well-typed.
  V1 punts to the engine type-checker for literals; only `$N` casts
  get the gateway-level validation.
- **Out-of-scope — `SP-PG-EXTQ-CAST-VALIDATE-MULTIWORD`** — recognise
  `::TIMESTAMP WITH TIME ZONE` correctly. Same V1 boundary as the
  parent SP-PG-EXTQ-CAST arc (pgJDBC uses spaceless aliases like
  `timestamptz`).

## 3 — `strip_pg_casts_tracked` extension

V1's `strip_pg_casts(sql) -> String` becomes
`strip_pg_casts_tracked(sql) -> (String, Vec<(usize, u32)>)`. The
existing `strip_pg_casts` becomes a thin wrapper that drops the
tracking vec (so every byte-equal KAT keeps passing untouched). The
tracked variant is new + exclusively consumed by the extq dispatch
layer at Parse time.

Algorithm extension to the scanner from the parent design (state
machine in §3 of `2026-06-01-kesseldb-sppgextqcast-design.md`): when
the scanner sees `::`, it inspects the BYTES IMMEDIATELY BEFORE the
`::` to detect a `$N` placeholder.

```text
on detecting `::` at position i:
  // look backward for $N
  let mut j = bytes_so_far.len()
  while j > 0 && bytes_so_far[j - 1].is_ascii_digit():
      j -= 1
  if j > 0 and j < bytes_so_far.len() and bytes_so_far[j - 1] == b'$':
      let digits = bytes_so_far[j..]
      if let Ok(n) = parse(digits) and n >= 1:
          record_pending_param_index = Some(n - 1)  # PG 1-based -> 0-based
  // then proceed with the existing strip: skip `::`, type name, optional (args)
  // BEFORE returning, look up the type-name OID + record the (index, oid) pair
```

Type-name → OID lookup uses a small in-module table covering the V1
type set:

| Type name (case-insensitive) | OID |
|---|---|
| `int2`, `smallint` | 21 |
| `int4`, `int`, `integer` | 23 |
| `int8`, `bigint` | 20 |
| `text` | 25 |
| `varchar`, `character varying` (V1 only matches the alias) | 1043 |
| `bool`, `boolean` | 16 |
| `bytea` | 17 |
| `float4`, `real` | 700 |
| `float8`, `double precision` (V1 only matches the alias) | 701 |
| `timestamptz`, `timestamp with time zone` (V1 alias only) | 1184 |
| `numeric`, `decimal` | 1700 |

Type names not in the table do NOT record a tracking pair (V1 decision:
unknown type → skip tracking, fall through to "strip + hope"; lets a
future PG type a workload starts using avoid a hard failure at the
validator).

## 4 — Validator: where it runs

The cleanest dispatch-layer integration is at **Bind time**, where
`dispatch_bind` already has access to both:

- The prepared statement's `param_oids` (from Parse; V1 stores them on
  `PreparedStmt`).
- The bound param values + formats.

We extend `PreparedStmt` with one new field:

```rust
pub struct PreparedStmt {
    pub sql: String,
    pub param_oids: Vec<u32>,
    /// SP-PG-EXTQ-CAST-VALIDATE T2 — pairs of (zero-based param index,
    /// declared cast OID) extracted from the SQL text by
    /// `cast_stripper::strip_pg_casts_tracked` at Parse time. Empty
    /// for SQL without `$N::TYPE` casts. Used by `dispatch_bind` to
    /// reject mismatched param OIDs with `42846 cannot_coerce`.
    pub param_casts: Vec<(usize, u32)>,
}
```

`dispatch_parse` runs the tracked stripper + stores the SQL +
tracking vec. The stored `sql` field stays the ORIGINAL (un-stripped)
text — strip-at-execute keeps the byte equivalence with V1's
behaviour for re-Parse / Describe / re-Bind shapes. (The downstream
dispatcher already runs `strip_pg_casts` at `dispatch_query` entry,
so the strip still happens; we just additionally know which `$N` had
which cast.)

`dispatch_bind` extension (one new check, after the existing
parameter-count / binary-format / portal-cap checks):

```rust
// SP-PG-EXTQ-CAST-VALIDATE T2 — every $N that had a ::TYPE cast in
// the original SQL MUST have a Parse-time param OID equal to the
// declared cast OID. Strict equality only; V2 SP-PG-EXTQ-CAST-
// VALIDATE-COMPAT could relax this to PG's type-category table.
for &(index, declared_oid) in &prep_param_casts {
    // Skip if Parse omitted OID hints for this position (= 0 = "infer").
    // Per spec §3, omitted OID hints are an explicit client signal
    // "trust the SQL"; if the SQL says ::int8 we trust it.
    let actual_oid = prep_param_oids.get(index).copied().unwrap_or(0);
    if actual_oid == 0 {
        continue;
    }
    if actual_oid != declared_oid {
        return set_err(
            state,
            ExtqError::CastOidMismatch {
                position: index,
                declared: declared_oid,
                actual: actual_oid,
            },
        );
    }
}
```

The `actual_oid == 0` skip matches asyncpg / psycopg3 behaviour
(they sometimes Bind without Parse-time OID hints + let the SQL
discriminate). Other clients (pgJDBC, psycopg2) always set both
sides, which is the path the strict-equality check covers.

`ExtqError::CastOidMismatch { position, declared, actual }` maps to
SQLSTATE `42846 cannot_coerce` in `server.rs`'s ExtqError → wire
renderer. The message format:

```
cannot cast parameter $<position+1> from type with OID <actual> to declared cast type OID <declared>
```

## 5 — KAT plan (T2)

**`cast_stripper::tests::*`** (~6 new):

- `tracked_strip_returns_pair_for_dollar_param_cast` — `SELECT $1::int8` → ("SELECT $1", [(0, 20)]).
- `tracked_strip_does_not_track_literal_cast` — `SELECT 1::int8` → ("SELECT 1", []).
- `tracked_strip_handles_multiple_params` — `WHERE id = $1::int8 AND name = $2::text` → ("WHERE id = $1 AND name = $2", [(0, 20), (1, 25)]).
- `tracked_strip_handles_unknown_type_name` — `SELECT $1::weirdtype` → ("SELECT $1", []) (no tracking pair, V1 falls back to no-validate for unknown types).
- `tracked_strip_unknown_param_index_no_record` — `SELECT $0::int8` (PG rejects but stripper is lenient; just don't record) → ("SELECT $0", []).
- `tracked_strip_thin_wrapper_byte_equal_to_v1` — `strip_pg_casts(sql) == strip_pg_casts_tracked(sql).0` for the entire V1 K-CAST-1..15 set + extras (regression-guard locked).

**`extq::tests::*`** (~4 new):

- `dispatch_parse_stores_cast_tracking` — Parse with SQL containing `$1::int8` populates `PreparedStmt.param_casts` with `[(0, 20)]`.
- `dispatch_bind_rejects_oid_mismatch_with_42846` — Parse(`SELECT $1::int8`, param_oids=[25]) + Bind → `ExtqError::CastOidMismatch { position: 0, declared: 20, actual: 25 }`.
- `dispatch_bind_accepts_oid_match` — Parse(`SELECT $1::int8`, param_oids=[20]) + Bind → `ExtqOutcome::Bytes(BindComplete)`.
- `dispatch_bind_skips_validation_when_parse_omitted_oid` — Parse(`SELECT $1::int8`, param_oids=[]) + Bind → `BindComplete` (no validation because actual_oid is 0).
- `dispatch_bind_validates_intra_position_multi_cast` — Parse with two `$N::TYPE` casts, one matching, one mismatching → the first mismatch triggers `42846`.

KAT delta target: +8-12 (within the design target).

## 6 — Acceptance

1. `psql -c 'SELECT * FROM t WHERE id = 1::int8'` still PASSES on vulcan
   (literal cast — no `$N` to validate; regression guard for parent arc).
2. The extq-layer mismatched-Bind KAT
   `dispatch_bind_rejects_oid_mismatch_with_42846` produces the exact
   `ExtqError::CastOidMismatch` variant with the expected position +
   declared + actual triple.
3. `server.rs` renders the variant to `42846 cannot_coerce` on the
   wire with a message naming both OIDs.
4. The existing pg-gateway lib KAT suite passes byte-for-byte; the
   change is additive (`PreparedStmt` gained a field, `dispatch_bind`
   gained a check, `cast_stripper` gained a sibling function).

## 7 — Out-of-scope (named follow-ups)

- **`SP-PG-EXTQ-CAST-VALIDATE-COMPAT`** — replace strict OID equality
  with PG's type-category table (int2/int4/int8 mutually compatible,
  text/varchar/bpchar mutually compatible, etc.).
- **`SP-PG-EXTQ-CAST-VALIDATE-LITERAL`** — also validate that
  `::TYPE` casts on literals (not just `$N` placeholders) are
  well-typed. V1 punts literal-cast validation to the engine
  type-checker.
- **`SP-PG-EXTQ-CAST-VALIDATE-MULTIWORD`** — recognise multi-word PG
  type names like `TIMESTAMP WITH TIME ZONE` for tracking. V1
  identifier-only is fine for every pgJDBC simple-mode emit.

## 8 — Closure shape

2-4 commits per the standing rules:

1. T1 + T2 — design spec + `cast_stripper::strip_pg_casts_tracked` +
   `PreparedStmt.param_casts` field + `dispatch_parse` integration +
   `dispatch_bind` validator + `ExtqError::CastOidMismatch` +
   server.rs renderer + KATs.
2. T3 — vulcan psql smoke transcript (literal-cast regression guard;
   `$N`-cast can't be exercised via psql but is locked by the KAT).
3. T4 — STATUS.md row + parent SP-PG-EXTQ-CAST progress tracker
   follow-up entry pivoted to "CLOSED" + this progress tracker
   CLOSED.

CI green is the release gate per standing rules; no binaries (text-
rewrite-only behaviour change).
