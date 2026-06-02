# SP-PG-EXTQ-CAST-VALIDATE-COMPAT — relax strict OID equality to PG type-category compatibility — SP-arc Progress Tracker

Date created: 2026-06-02

**Status: IN-PROGRESS.**

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqcastvalidatecompat-design.md`
Parent SP-arc: SP-PG-EXTQ-CAST-VALIDATE V1 (closed 2026-06-02). The
closing note explicitly named this arc as the follow-up that would
relax strict OID equality to PG's type-category compatibility table
once workloads demanded it.

## What this SP-arc ships

V1 = "every `$N::TYPE` cast tracked at Parse time, validated at
Bind time using PG's `typcategory` table — within-category
widenings (INT4/INT8, TEXT/VARCHAR, etc.) accepted; cross-category
mismatches rejected with `42846 cannot_coerce`."

1. `types::oid_category(oid: u32) -> char` returns the PG
   `typcategory` byte for the V1 type set.
2. `types::oid_castable(param_oid, cast_oid) -> bool` returns true
   for strict equality + omitted-OID skip + intra-category widening;
   false for cross-category mismatch.
3. `dispatch_bind`'s cast-validation loop swaps strict equality for
   `oid_castable`. The error variant + state set + first-mismatch-
   wins ordering are byte-untouched.

## Out-of-scope (named, deferred)

- **`SP-PG-EXTQ-CAST-VALIDATE-COMPAT-RANGE`** — overflow-check the
  param value against the cast type's range at the gateway (e.g.
  INT4 param value `100000` against INT2 cast).
- **`SP-PG-EXTQ-CAST-VALIDATE-LITERAL`** — also relax-and-validate
  literal casts.
- **`SP-PG-EXTQ-CAST-VALIDATE-CATEGORY-CROSS`** — accept SOME cross-
  category casts that PG itself accepts (TEXT → INT8 coercion).

## Slice plan (mirrors design spec §7)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + this progress tracker. | DONE (folded into T2 commit) | — |
| **T2** | `types::oid_category` + `types::oid_castable` + `dispatch_bind` widening + KATs. | — | — |
| **T3** | vulcan psycopg smoke (INT4 + INT8 cast accepted; cross-category still rejects) + USAGE §9 note. | — | — |
| **T4** | STATUS row + parent SP-PG-EXTQ-CAST-VALIDATE progress tracker follow-up → CLOSED + this progress tracker CLOSED. | — | — |

KAT delta target: +8-12.

## Headline (in-flight)

Parse(`INSERT INTO t VALUES ($1::int8, $2)`, param_oids=[INT4, TEXT])
+ Bind succeeds (was: `42846 cannot_coerce`). Cross-category
mismatch (TEXT param + INT8 cast) STILL rejects with the same
`ExtqError::CastOidMismatch` + 42846 wire frame.
