## SP-PG-SQL-PAREN-VALUES — kessel-sql VALUES paren-wrapped literals design spec

Date created: 2026-06-02 (working same-day arc on top of SP-PG-JDBC-SMOKE T2)
Parent SP-arc: SP-PG-JDBC-SMOKE V1 (closed 2026-06-02 at T3) — the
"DONE_WITH_CONCERNS" residual it named verbatim:

```
Simple-mode PreparedStatement INSERT still fails with
ERROR: sql: expected value on vulcan. The cast stripper does
its job (the post-strip SQL is INSERT INTO jdbc_smoke (id, name)
VALUES (('42'), ('hello-jdbc'))); kessel-sql's VALUES parser
rejects the parenthesized literals.
```

This arc closes that residual by teaching the `INSERT … VALUES` tuple
value parser to accept `(LITERAL)` as a parenthesized expression
equivalent to `LITERAL`. The fix is entirely under
`crates/kessel-sql/src/lib.rs` — engine-side; the pg-gateway, the
binary I/O surfaces, and the HTTP/1.1/WS surfaces are byte-untouched.

## 1 — Context: the SQL pgJDBC simple-mode emits

Captured verbatim from the SP-PG-JDBC-SMOKE T2 wire trace
(`docs/superpowers/sppgjdbcsmoke-t2-smoke-2026-06-02.txt` §5):

```
FE=> SimpleQuery(query="INSERT INTO jdbc_smoke (id, name)
                       VALUES (('42'::int8), ('hello-jdbc'))")
```

After SP-PG-EXTQ-CAST T2's `cast_stripper::strip_pg_casts` runs at
`dispatch_query` entry the kessel-sql parser sees:

```
INSERT INTO jdbc_smoke (id, name) VALUES (('42'), ('hello-jdbc'))
```

The cast strip is correct; the residual is purely a parser gap. In
real PostgreSQL `VALUES (1, 'hello')` and `VALUES ((1), ('hello'))`
are equivalent — the inner parens are expression grouping. pgJDBC's
simple-mode parameter substitution always wraps each substituted
value in `(…)` defensively (to keep precedence right if the
parameter happens to evaluate to a negative number or any other
expression that would otherwise rebind to the surrounding context).

Today's `kessel-sql` VALUES tuple parser (`crates/kessel-sql/src/lib.rs`
around the `p.expect_kw("VALUES")?` site) accepts ONLY bare literals
(`Tok::Int(n)` / `Tok::Str(s)`); the moment it sees `(` where a value
should appear it errors with `expected value`. This arc lifts that
gap with a tiny additive change.

## 2 — Scope

- **V1 in-scope**: the VALUES tuple value parser accepts `(LITERAL)`
  in addition to bare `LITERAL`. After SP-PG-EXTQ-CAST has stripped
  the `::TYPE` cast the post-strip shape `(LITERAL)` is exactly what
  pgJDBC emits.
- **V1 in-scope (anti-stack-bomb)**: support N-level paren nesting
  (`((LITERAL))`, `(((LITERAL)))`, …) with a fixed depth cap of 8 so
  a malicious or buggy client cannot drive the parser into deep
  recursion / linear-time paren consumption. Above the cap the parser
  returns a clean `too many nested parens` error.
- **V1 out-of-scope** (named below, each a future arc):
  - Arbitrary expressions inside the VALUES parens
    (`(1 + 2)`, `(NOW())`, `(col)` etc) — only single-literal
    parenthesization is admitted. This matches pgJDBC's emit shape
    exactly; richer expressions would be a real SQL AST job.
  - NULL inside VALUES parens — today's bare-VALUES parser does not
    accept the bare `NULL` literal either (the only way an INSERT
    today produces NULL in a field is to omit the column from the
    column list and rely on the SP86 default / nullable-column
    handler); paren-wrapped NULL is the same shape and stays out of
    scope on the symmetry grounds.
  - VALUES UPDATE shapes (UPDATE … SET col = (LITERAL)). pgJDBC's
    simple-mode substitution only paren-wraps inside VALUES tuples in
    the failing trace; the UPDATE SET parser was not implicated by
    the SP-PG-JDBC-SMOKE smoke.

## 3 — The fix

Inside the `INSERT … VALUES` parse loop:

```rust
//   loop {
//       p.punct('(')?;
//       let mut raw = Vec::new();
//       loop {
//           match p.next() {
//               Some(Tok::Int(n)) => raw.push(Lit::Int(n)),
//               Some(Tok::Str(s)) => raw.push(Lit::Str(s)),
//               _ => return Err("expected value".into()),
//           }
//           match p.next() { … }
//       }
//   }
```

becomes

```rust
//   loop {
//       p.punct('(')?;
//       let mut raw = Vec::new();
//       loop {
//           // SP-PG-SQL-PAREN-VALUES: accept (LITERAL) … (((LITERAL)))
//           // (up to depth 8) as equivalent to bare LITERAL.
//           let mut depth = 0usize;
//           while matches!(p.peek(), Some(Tok::Punct('('))) {
//               p.i += 1;
//               depth += 1;
//               if depth > 8 {
//                   return Err("too many nested parens in VALUES".into());
//               }
//           }
//           match p.next() {
//               Some(Tok::Int(n)) => raw.push(Lit::Int(n)),
//               Some(Tok::Str(s)) => raw.push(Lit::Str(s)),
//               _ => return Err("expected value".into()),
//           }
//           for _ in 0..depth {
//               match p.next() {
//                   Some(Tok::Punct(')')) => {}
//                   _ => return Err("expected `)` closing VALUES paren".into()),
//               }
//           }
//           match p.next() { … }
//       }
//   }
```

Pure addition. When `depth == 0` (bare literal — every prior KAT)
both the `while` loop and the trailing `for` loop are no-ops, so the
byte path is unchanged.

## 4 — KAT plan

Locked in `crates/kessel-sql/src/lib.rs` under the existing `tests`
module. KAT delta target +5..10:

- **K-PVAL-1** — `INSERT INTO t (id, v) VALUES (1, 2)` compiles to
  `Op::Create` unchanged (regression guard for the bare path).
- **K-PVAL-2** — `INSERT INTO t (id, v) VALUES ((1), (2))` compiles
  to the SAME `Op::Create` byte-for-byte (1-level paren).
- **K-PVAL-3** — `INSERT INTO t (id, n) VALUES ((42), ('hello'))`
  compiles to an `Op::Create` with the INT8 id + the TEXT name.
  Mirrors the pgJDBC simple-mode failing case verbatim.
- **K-PVAL-4** — `INSERT INTO t (id, v) VALUES (((1)), ((2)))` —
  3-level paren depth accepted.
- **K-PVAL-5** — `INSERT INTO t (id, v) VALUES ((((((((1)))))))), (1))`
  — 8-level paren depth accepted on the first position; bare on the
  second. Anti-stack-bomb-cap boundary.
- **K-PVAL-6** — `INSERT INTO t (id, v) VALUES (((((((((1))))))))), (1))`
  — 9-level paren depth rejected with `too many nested parens in VALUES`.
- **K-PVAL-7** — `INSERT INTO t (id, v) VALUES ((1), 2)` — mixed
  paren + bare in the same tuple works (left paren, right bare).
- **K-PVAL-8** — Multi-row paren VALUES: `INSERT INTO t (id, v)
  VALUES ((1), (2)), ((3), (4))` compiles to a multi-row `Op::Txn`
  with two `Op::Create`s.
- **K-PVAL-9** — Unbalanced paren: `INSERT INTO t (id, v) VALUES ((1, 2)`
  rejects with a clean error (the inner `(` is consumed as paren
  depth, then `1` parses, then `,` arrives where `)` was expected).

## 5 — Acceptance

1. All new KATs green; the existing `kessel-sql` lib KAT suite stays
   byte-identical (the change is a pure addition on the `(` peek; the
   bare path is unchanged).
2. `psql -c "INSERT INTO jdbc_smoke (id, name) VALUES ((42), ('jdbc'))"`
   succeeds against vulcan (was 42601 / `expected value`).
3. `JdbcSmoke simple` (the real pgJDBC 42.7.4 harness from
   SP-PG-JDBC-SMOKE T1) PASSes the PreparedStatement INSERT step on
   vulcan end-to-end.

## 6 — Out-of-scope (named follow-ups)

- **`SP-PG-EXTQ-DESCRIBE-VERSION`** — the second DONE_WITH_CONCERNS
  residual from SP-PG-JDBC-SMOKE T3; orthogonal arc on the gateway
  portal-Describe routing for built-in scalar-function SELECTs.
- **`SP-SQL-AST-VALUES-EXPR`** — accept arbitrary expressions inside
  VALUES tuples (`(1 + 2)`, `(NOW())`, `(SUBSTR(s, 1, 3))`). A real
  SQL AST job — far bigger than this arc.
- **`SP-PG-SQL-PAREN-UPDATE-SET`** — the symmetric concern for
  `UPDATE … SET col = (literal)`. Not implicated by SP-PG-JDBC-SMOKE
  but a future Spring/Hibernate update path may hit it.

## 7 — Closure shape

2-4 commits per the standing rules:

1. T1 + T2 — design spec + `kessel-sql` parser fix + KATs.
2. T3 — vulcan smoke transcript (`docs/superpowers/sppgsqlparenvalues-
   t3-smoke-2026-06-02.txt`) re-running the `JdbcSmoke simple` harness
   from SP-PG-JDBC-SMOKE T1 and verifying PreparedStatement INSERT now
   PASSes; USAGE §9 ORM matrix JDBC row pivot.
3. T4 — STATUS.md "Tonight's delivery" entry + arc closure + the
   SP-PG-JDBC-SMOKE progress tracker pointer flip (concern #1 closed).

CI green is the release gate per standing rules; binaries via
release.yml on `v*` tags only (no release here — engine-side parser
fix, no Cargo.toml version bump).
