# SP-PG-EXTQ-CAST-VALIDATE-LITERAL — extend cast-validation to literal `::TYPE` casts — SP-arc Progress Tracker

Date created: 2026-06-02

**Status: OPEN — V1 in flight at T1 (2026-06-02).**

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
| **T1** | Design spec + this progress tracker. | **DONE** (folded into T2 commit) | (pending) |
| **T2** | `cast_stripper::find_literal_cast_mismatch` + `LiteralCastMismatch` struct + dispatcher wiring (simple-query + typed-params + extq parse) + `ExtqError::LiteralCastMismatch` + server.rs renderer + KATs. | **DONE** | (pending) |
| **T3** | vulcan psql smoke transcript (within-category accept + cross-category reject) + USAGE §9 note. | **PENDING** | — |
| **T4** | STATUS row + parent SP-PG-EXTQ-CAST-VALIDATE progress tracker follow-up entry pivoted to CLOSED + this progress tracker → CLOSED. | **PENDING** | — |

KAT delta target: +8-12.

## Headline (pending T3)

Pending vulcan-verified `'hello'::int8` rejection +
`1::int8` / `NULL::int8` acceptance + parent arcs' `$N` validator
regression-guard pass.
