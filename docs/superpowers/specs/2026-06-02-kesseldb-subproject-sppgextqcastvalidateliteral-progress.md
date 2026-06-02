# SP-PG-EXTQ-CAST-VALIDATE-LITERAL — extend cast-validation to literal `::TYPE` casts — SP-arc Progress Tracker

Date created: 2026-06-02

**Status: CLOSED — V1 SHIPPED at T2 (2026-06-02).** The HEADLINE
shape — `SELECT 'hello'::int8` (TEXT→INT8 cross-category literal
cast) — now rejects with `42846 cannot_coerce` BEFORE the strip
rewrites the SQL, while `1::int8` / `'hello'::text` / `true::bool`
accept within-category and `NULL::int8` passes the validator
(anytype). Verified via vulcan psql smoke
(`docs/superpowers/sppgextqcastvalidateliteral-t3-smoke-2026-06-02.txt`).
TaskList #386 ready for completion.

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqcastvalidateliteral-design.md`

Parent SP-arcs:
- SP-PG-EXTQ-CAST-VALIDATE V1 (closed 2026-06-02) named this arc
  as a follow-up: "also validate that `::TYPE` casts on literals
  (not just `$N` placeholders) are well-typed".
- SP-PG-EXTQ-CAST-VALIDATE-COMPAT V1 (closed 2026-06-02) shipped
  `types::oid_category` + `types::oid_castable` — this arc reuses
  both for the literal-cast category comparison.

## What this SP-arc ships

V1 = "every `LITERAL::TYPE` cast classified by literal natural
type; cross-category literal casts reject with `42846
cannot_coerce` BEFORE the strip rewrites the SQL; NULL is anytype
and accepts unconditionally."

1. `cast_stripper::find_literal_cast_mismatch(sql) ->
   Option<LiteralCastMismatch>` classifies each literal immediately
   before `::` (bare integer / float / quoted string / bool /
   NULL) + compares its `oid_category` against the cast type's
   category.
2. `dispatch::dispatch_query` + `dispatch::dispatch_query_with_params`
   + `extq::dispatch_parse` call the helper BEFORE running the
   strip; cross-category mismatches surface SQLSTATE `42846
   cannot_coerce` via the existing error-frame plumbing.
3. `ExtqError::LiteralCastMismatch { literal_oid, cast_oid,
   literal_category, cast_category }` mirrors the V1
   `CastOidMismatch` variant for the extended-query path.

## Out-of-scope (named, deferred)

- **`SP-PG-EXTQ-CAST-VALIDATE-LITERAL-EXPR`** — recognise literal-
  cast patterns inside expressions (`(1+2)::int8`).
- **`SP-PG-EXTQ-CAST-VALIDATE-LITERAL-DATEPARSE`** — PG actually
  accepts `'2024-01-01'::date`; V1 rejects 'S' vs 'D'.
- **`SP-PG-EXTQ-CAST-VALIDATE-LITERAL-NUMSTR`** — PG accepts
  `'42'::int8`; V1 rejects 'S' vs 'N'.
- **`SP-PG-EXTQ-CAST-VALIDATE-LITERAL-MULTIWORD`** — multi-word
  PG type names (shared boundary with parent arc).

## Slice plan (mirrors design spec §6)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + this progress tracker. | **DONE** (folded into T2 commit) | `02df4a0` |
| **T2** | `cast_stripper::find_literal_cast_mismatch` + `LiteralCastMismatch` struct + dispatcher wiring (simple-query + typed-params + extq parse) + `ExtqError::LiteralCastMismatch` + server.rs renderer + KATs. | **DONE** | `02df4a0` |
| **T3** | vulcan psql smoke transcript (within-category accept + cross-category reject) + USAGE §9 note. | **DONE** | `7260fd7` |
| **T4** | STATUS row + parent SP-PG-EXTQ-CAST-VALIDATE progress tracker follow-up entry pivoted to CLOSED + this progress tracker → CLOSED. | **DONE** | (this commit) |

KAT delta: +28 (cast_stripper::tests literal_* + extq::tests
cast_validate_literal_t2_* + scalar/response companions);
vulcan full pg-gateway lib sweep 962/962 green at HEAD 02df4a0.

## Headline

`SELECT 'hello'::int8` (TEXT→INT8) + `true::int8` (BOOL→INT8) reject
with `42846 cannot_coerce` BEFORE the strip; `1::int8` /
`'hello'::text` / `-1::int8` accept within-category; `NULL::int8`
passes the validator (anytype). Parent arcs' `$N` validator + every
existing CAST/CAST-VALIDATE/COMPAT KAT pass byte-for-byte (the
literal validator is additive — `strip_pg_casts` +
`strip_pg_casts_tracked` byte outputs unchanged). vulcan-verified
(`docs/superpowers/sppgextqcastvalidateliteral-t3-smoke-2026-06-02.txt`).

## What landed

- `cast_stripper::find_literal_cast_mismatch(sql) ->
  Option<LiteralCastMismatch>` — single-pass literal classifier
  (bare int → INT4/INT8 by magnitude, bare float → FLOAT8, quoted
  string → TEXT with `''` escape handling, `true`/`false` → BOOL,
  `NULL` → anytype sentinel) that skips `$N` placeholders (covered
  by V1+COMPAT) and bytes inside string/line/block-comment context.
- `LiteralCastMismatch { literal_oid, cast_oid, literal_category,
  cast_category }` struct.
- Dispatcher wiring at all three entries (`dispatch_query`,
  `dispatch_query_with_params`, `extq::dispatch_parse`) calling the
  helper BEFORE the strip; cross-category mismatch surfaces `42846
  cannot_coerce` via the existing error-frame plumbing.
- `ExtqError::LiteralCastMismatch` variant + server.rs renderer
  mirroring the V1 `CastOidMismatch` shape.
