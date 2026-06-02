# SP-PG-EXTQ-CAST-VALIDATE — close the V1 "strip + hope" silent-coercion vector — SP-arc Progress Tracker

Date created: 2026-06-02

**Status: CLOSED — V1 SHIPPED at T2 (2026-06-02).** The HEADLINE
shape — Parse(`SELECT ... WHERE id = $1::int8`, param_oids=[25/* TEXT */])
+ Bind('42') — now returns `42846 cannot_coerce` with message
`cannot cast parameter $1 from type with OID 25 to declared cast
type OID 20` (was: silent strip-and-coerce). Verified via
psycopg3 PQ-layer smoke on vulcan. TaskList #382 ready for
completion.

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqcastvalidate-design.md`
Parent SP-arc: SP-PG-EXTQ-CAST V1 (closed 2026-06-02 at T2). The
closing note explicitly named this arc as the follow-up that would
re-introduce the cast type check the V1 strip discarded.

## What this SP-arc ships

V1 = "every Parse SQL with `$N::TYPE` casts gets the declared OID
tracked + validated against the bound parameter OID at Bind time;
mismatches reject with `42846 cannot_coerce`."

1. `cast_stripper::strip_pg_casts_tracked(sql) -> (String,
   Vec<(usize, u32)>)` extends the V1 stripper with a tracking vec
   pairing each stripped `$N::TYPE` cast with the type's PG OID.
2. `PreparedStmt` gains a `param_casts: Vec<(usize, u32)>` field
   populated at Parse time.
3. `dispatch_bind` validates `prep.param_oids[index] ==
   declared_oid` for every tracked pair; mismatch returns
   `ExtqError::CastOidMismatch { position, declared, actual }`.
4. `server.rs` renders `CastOidMismatch` to SQLSTATE `42846
   cannot_coerce` with a human-readable message.

## Out-of-scope (named, deferred)

- ~~**`SP-PG-EXTQ-CAST-VALIDATE-COMPAT`** — replace strict OID equality
  with PG's type-category compatibility table.~~ → **CLOSED
  2026-06-02 by SP-PG-EXTQ-CAST-VALIDATE-COMPAT V1.** `types::
  oid_category` + `types::oid_castable` ship the PG `typcategory`
  widening (any 'N'-OID pair, any 'S'-OID pair etc. accept;
  cross-category still rejects with the same 42846 wire frame so
  the V1 silent-coercion vector stays closed). Verified via
  psycopg3 PQ-layer 5-case smoke on vulcan
  (`docs/superpowers/sppgextqcastvalidatecompat-t3-smoke-2026-06-02.txt`)
  — HEADLINE pgJDBC INT4+INT8 pattern accepts; cross-category
  TEXT+INT8 still rejects with the exact V1 error message.
- **`SP-PG-EXTQ-CAST-VALIDATE-LITERAL`** — also validate `::TYPE`
  casts on literals (not just `$N`).
- **`SP-PG-EXTQ-CAST-VALIDATE-MULTIWORD`** — recognise multi-word
  PG type names like `TIMESTAMP WITH TIME ZONE`.

## Slice plan (mirrors design spec §8)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + this progress tracker. | **DONE** (folded into T2 commit) | `ad3743c` |
| **T2** | `cast_stripper::strip_pg_casts_tracked` + `PreparedStmt.param_casts` + `dispatch_parse` integration + `dispatch_bind` validator + `ExtqError::CastOidMismatch` + server.rs renderer + KATs. | **DONE** | `ad3743c` |
| **T3** | vulcan psycopg3 PQ-layer 3-case smoke transcript (matching OID succeeds / HEADLINE 42846 mismatch / omitted-OID skip) + USAGE §9 update flipping the parent arc's "V1 is strip + hope" residual gap to CLOSED. | **DONE** | `525969e` |
| **T4** | STATUS row + parent SP-PG-EXTQ-CAST progress tracker follow-up entry → CLOSED + this progress tracker → CLOSED. | **DONE** | (this commit) |

KAT delta: +17 (11 `cast_stripper::tests::tracked_*` + 6
`extq::tests::cast_validate_t2_*`).

## Headline

Parse(`SELECT * FROM t WHERE id = $1::int8`, param_oids=[25/* TEXT */])
+ Bind → `42846 cannot_coerce` (was: silent strip + coerce). Real
JDBC literal-cast (`SELECT 1 FROM t WHERE id = 1::int8` no `$N`) still
PASSES — V1 only validates `$N` casts, not literal casts.
