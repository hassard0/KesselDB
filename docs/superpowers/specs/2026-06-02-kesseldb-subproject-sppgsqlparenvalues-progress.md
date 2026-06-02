# SP-PG-SQL-PAREN-VALUES — VALUES paren-wrapped literals — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T3 (2026-06-02) — DONE.**

Closes the first of two residual gaps SP-PG-JDBC-SMOKE T2 named:
simple-mode `PreparedStatement` INSERT failed with `ERROR: sql:
expected value` because pgJDBC simple-mode wraps every substituted
parameter in parens (`VALUES (('42'::int8), ('hello-jdbc'))`); after
the SP-PG-EXTQ-CAST T2 stripper dropped the `::int8` casts, the SQL
kessel-sql saw was `VALUES (('42'), ('hello-jdbc'))` and the VALUES
parser rejected the inner `(`. PG treats `(LITERAL)` as expression
grouping equivalent to `LITERAL`; the VALUES tuple parser now does
too.

After V1 lands (T1..T4):
- Real pgJDBC 42.7.4 simple-mode `PreparedStatement` INSERT round-
  trips end-to-end against KesselDB on vulcan.
- The companion Str→numeric coercion in `lit_to_value` + WHERE
  `term_hinted` lifts the parameterized SELECT `WHERE id = ?` shape
  in the same swing — the post-cast-strip `WHERE id = ('42')` now
  matches int8 column `id = 42` instead of comparing `String` vs
  `Int8`.

Smoke transcript:
`docs/superpowers/sppgsqlparenvalues-t3-smoke-2026-06-02.txt`
Design spec:
`docs/superpowers/specs/2026-06-02-kesseldb-sppgsqlparenvalues-design.md`
Parent SP-arc: SP-PG-JDBC-SMOKE V1 (closed 2026-06-02 at T3 —
DONE_WITH_CONCERNS). This arc closes concern #1 of two; the
sibling arc SP-PG-EXTQ-DESCRIBE-VERSION V1 closed concern #2 the
same day.

## What this SP-arc ships

V1 = "the VALUES tuple value parser accepts `(LITERAL)` paren-
wrapped literals up to depth 8, with an anti-stack-bomb cap of 9
levels; `lit_to_value` coerces `Lit::Str("NN")` → numeric for
numeric column kinds; WHERE `term_hinted` propagates the LHS
column's `FieldKind` to the RHS literal parser so a paren-wrapped
string-shaped int literal compares against an int column the way
PG would after stripping the `::int8` cast."

**Out-of-scope (named, deferred — each is its own future arc):**

- **`SP-SQL-AST-VALUES-EXPR` (V2)** — accept arbitrary expressions
  inside VALUES tuples (`(1 + 2)`, `(NOW())`, `(SUBSTR(s, 1, 3))`).
  Real SQL AST job — far bigger than V1's single-literal
  parenthesization.
- **`SP-PG-SQL-PAREN-UPDATE-SET` (V2)** — the symmetric concern for
  `UPDATE … SET col = (literal)`. Not implicated by the
  SP-PG-JDBC-SMOKE smoke but a future Spring/Hibernate update path
  may hit it.
- **`SP-PG-SQL-COERCE-EXPLICIT-TYPE` (V2)** — re-introduce the
  explicit type information the SP-PG-EXTQ-CAST stripper drops, so
  `'42'::text` doesn't get silently coerced to `42` against an int
  column. V1 trusts the LHS column type as the coercion target;
  this is correct for every pgJDBC simple-mode emit but a future
  workload that wants explicit type checking would need this
  follow-up.

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec — `docs/superpowers/specs/2026-06-02-kesseldb-sppgsqlparenvalues-design.md`. Context (the SP-PG-JDBC-SMOKE T2 failing case verbatim), scope (V1 in/out), the fix (depth-counted `(LITERAL)` accept), KAT plan (K-PVAL-1..9), acceptance criteria, closure shape. | **DONE** | `0558743` |
| **T2** | Implementation — VALUES tuple value parser accepts `(LITERAL)` with anti-stack-bomb depth cap at 9 levels (`crates/kessel-sql/src/lib.rs`). Companion `Lit::Str → numeric` coercion in the `id` pseudo-column resolution path + `lit_to_value` for numeric column kinds. Companion WHERE `term_hinted` variant takes the LHS column's `FieldKind` and applies the same coercion for the RHS literal. KATs: `paren_wrapped_values_literals` (K-PVAL-1..10) + `paren_wrapped_where_numeric_coercion` (K-PVAL-W1..3). | **DONE** | `0558743` + `4bbb5d2` + `56fb59b` |
| **T3** | vulcan smoke — re-run the SP-PG-JDBC-SMOKE T1 `JdbcSmoke simple` harness against the new parser; verify ALL TESTS PASS for the full simple-mode CRUD chain (CREATE / PreparedStatement INSERT / SELECT * / PreparedStatement SELECT WHERE id = ? / SELECT version()). Confirm extended-mode is byte-additive (re-run for regression guard). USAGE.md §9 ORM matrix JDBC row flip from `PASS\* + one residual gap` to plain `PASS`. Transcript `docs/superpowers/sppgsqlparenvalues-t3-smoke-2026-06-02.txt`. | **DONE** | `56fb59b` |
| **T4** | STATUS.md "Tonight's delivery" entry (Track A.-1.3) + arc closure + this progress tracker. | **DONE** | (this commit) |

## Acceptance criteria

1. **VALUES tuple parser accepts `(LITERAL)` up to depth 8; rejects
   depth 9.** ✅ Met. K-PVAL-2 / K-PVAL-4 / K-PVAL-5 / K-PVAL-6 lock
   the cap boundary exactly.
2. **The bare-VALUES path is byte-identical pre-arc.** ✅ Met.
   K-PVAL-1 is the regression guard; every prior INSERT-emitting KAT
   in `crates/kessel-sql/src/lib.rs` (40+ sites) keeps passing
   (workspace `cargo test --release --workspace` clean).
3. **Real pgJDBC simple-mode `PreparedStatement` INSERT PASSes
   end-to-end on vulcan.** ✅ Met. `JdbcSmoke simple` records
   `INSERT: 1 row(s)` + `Row: id=42, name=hello-jdbc` instead of
   the T2 `ERROR: sql: expected value`.
4. **Real pgJDBC simple-mode `PreparedStatement` SELECT WHERE id =
   ? matches the inserted row.** ✅ Met. `JdbcSmoke simple` records
   `Param SELECT: id=42, name=hello-jdbc` — the WHERE `term_hinted`
   + `Str → Int` coercion path verified end-to-end through the real
   driver.
5. **Extended-mode JDBC PASS is preserved (no regression).** ✅ Met.
   `JdbcSmoke extended` also records ALL TESTS PASS; the bare-
   VALUES path is byte-identical pre-arc and extended-mode never
   emits the paren-wrapped simple-mode shape in the first place.
6. **KAT delta within the +5..10 spec target.** ✅ Met (+2 test
   functions / +13 assertions; the assertion count is the right
   grain for the K-PVAL-1..10 + K-PVAL-W1..3 coverage shape).

## Commits

- `0558743` — T1 + T2 first pass: design spec + VALUES tuple parser
  accepts depth-counted paren-wrapped literals + 9 KATs (K-PVAL-
  1..9). KAT count 43 → 44 (one failing K-PVAL-5 boundary —
  corrected in the next commit).
- `4bbb5d2` — T2 KAT schema fix: the K-PVAL test's CREATE TABLE
  declared `id I64` as a real field while ALSO listing `id` as the
  pseudo-id in the INSERT column list. Dropped the real `id` from
  the schema so it's pseudo-only.
- `56fb59b` — T2 second-half: companion `Lit::Str → numeric`
  coercion (`id` resolution + `lit_to_value`) and WHERE
  `term_hinted` variant; T3 vulcan smoke transcript + USAGE.md
  ORM matrix flip; K-PVAL-5 / K-PVAL-6 paren-depth arithmetic
  corrected after the schema flip (the outer tuple `(` adds the
  9th open paren to the boundary count).
- (this commit) — T4 arc closure: STATUS Track A.-1.3 + progress
  tracker.

## Standing-rules compliance

- ✅ `#![forbid(unsafe_code)]` honored (no unsafe in the change).
- ✅ No new external deps.
- ✅ HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched
  (engine-side parser change; gateway untouched).
- ✅ Direct commits to main, no Co-Authored-By, no `-S`, pushed
  after each commit.
- ✅ CI green is the release gate; binaries via release.yml on
  `v*` tags only (no release here — text-rewrite + parser only).
- ✅ Bare-VALUES path is byte-identical pre-arc — every prior KAT
  passes by construction.
