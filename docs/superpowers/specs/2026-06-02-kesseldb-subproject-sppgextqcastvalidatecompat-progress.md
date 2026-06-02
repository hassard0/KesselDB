# SP-PG-EXTQ-CAST-VALIDATE-COMPAT — relax strict OID equality to PG type-category compatibility — SP-arc Progress Tracker

Date created: 2026-06-02

**Status: CLOSED — V1 SHIPPED at T3 (2026-06-02).** The HEADLINE
shape — Parse(`INSERT INTO compat (id, n) VALUES ($1::int8, $2)`,
param_oids=[INT4 (23), TEXT (25)]) + Bind text "42" — now ACCEPTS
(was: 42846 cannot_coerce). Verified via psycopg3 PQ-layer smoke
on vulcan. Cross-category TEXT + INT8 cast still rejects with the
exact V1 wire message so the silent-coercion vector stays closed.
TaskList #384 ready for completion.

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
| **T1** | Design spec + this progress tracker. | **DONE** (folded into T2 commit) | `d6f2031` |
| **T2** | `types::oid_category` + `types::oid_castable` + `dispatch_bind` widening + KATs. | **DONE** | `d6f2031` |
| **T3** | vulcan psycopg3 PQ-layer 5-case smoke (INT4+INT8 / INT8+INT4 / TEXT+VARCHAR accept; TEXT+INT8 cross-category reject; INT8+INT8 strict equality) + USAGE §9 note. | **DONE** | `6b4ae00` |
| **T4** | STATUS row + parent SP-PG-EXTQ-CAST-VALIDATE progress tracker follow-up → CLOSED + this progress tracker CLOSED. | **DONE** | (this commit) |

KAT delta: +14 (11 `types::tests::cast_compat_*` + 3
`extq::tests::cast_validate_compat_t2_*`). All 919 pg-gateway
library tests pass.

## Headline (shipped)

Parse(`INSERT INTO compat (id, n) VALUES ($1::int8, $2)`,
param_oids=[INT4 (23), TEXT (25)]) + Bind text "42" now ACCEPTS
on vulcan (was: `42846 cannot_coerce`). pgJDBC's default
Java-`int` against `::int8` cast — and psycopg3's Python-`int`
against `::int8` cast — both close the false-rejection gap.
Cross-category mismatch (TEXT param + INT8 cast) STILL rejects
with the exact V1 wire message ("cannot cast parameter $1 from
type with OID 25 to declared cast type OID 20") so the silent-
coercion vector stays closed; only intra-category widenings open.
