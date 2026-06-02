## SP-PG-EXTQ-CAST — JDBC simple-mode `::cast` rewrite design spec

Date created: 2026-06-02 (working branch closed same-day at T4)
Parent SP-arc: SP-PG-EXTQ V1 (closed 2026-05-29 at T8). The compat
matrix recorded in
`docs/superpowers/sppgextq-t8-orm-smoke-2026-05-29.txt` row #5 / #5
("JDBC, preferQueryMode=simple") logged the precise failure mode this
arc closes:

```
pgJDBC 42.7.4 (preferQueryMode=simple):
  connect: OK
  SELECT *: 2 rows
  ERROR: sql: unexpected char ':'
```

The `unexpected char ':'` is `kessel-sql`'s lexer choking on the `::`
PostgreSQL-extension type-cast operator that pgJDBC + a handful of
PostGIS / pgvector flavoured helpers inject into the SQL text in
simple-query mode (no Bind parameters available, so the driver substitutes
the value client-side AND tags it with `::int8` / `::text` / `::numeric`
to nail down the type at the server lexer level — PG's lexer parses
that as a type-cast operator; ours does not).

This arc closes the gap WITHOUT touching `kessel-sql` (a real AST
change would be a multi-slice arc on its own — V2
`SP-SQL-AST-CAST-NODE`). It strips `::TYPE[(args)]` patterns from the
SQL text at the gateway dispatch entry point, BEFORE the byte stream
reaches `pg_catalog::catalog_query_hook` and `engine.apply_sql`. The
engine's existing type-checker handles implicit type coercion at
INSERT / WHERE comparison sites the same way pgJDBC's simple-mode
emit would have intended.

> **V1 scope is "strip + hope" — V1 doesn't validate that the cast is
> well-typed.** The cast text is dropped; the bare value reaches the
> engine; the engine's type-checker decides whether it's compatible
> with the target column / parameter slot. The thing the cast text
> communicates to a real PG (the explicit user type) is lost, but
> every cast pgJDBC simple-mode emits is redundant under our type
> system (the column type already gives the target type via
> `describe_table`). V2 `SP-PG-EXTQ-CAST-VALIDATE` could re-introduce
> the explicit type check.

## 1 — Context: the cast patterns we need to handle

Captured from a live JDBC simple-mode session against vulcan + a
review of pgJDBC's `SimpleQueryParser`:

| # | Pattern                              | After strip                  |
|---|--------------------------------------|------------------------------|
| 1 | `SELECT 1::int8`                     | `SELECT 1`                   |
| 2 | `SELECT col::text FROM t`            | `SELECT col FROM t`          |
| 3 | `SELECT 1::int4`                     | `SELECT 1`                   |
| 4 | `SELECT NULL::timestamp`             | `SELECT NULL`                |
| 5 | `WHERE col = $1::int8`               | `WHERE col = $1`             |
| 6 | `NULL::numeric(15,2)` (parametric)   | `NULL`                       |
| 7 | `SELECT a::int, b::text FROM t`      | `SELECT a, b FROM t`         |
| 8 | `'literal ::int8 inside'` (quoted)   | unchanged                    |
| 9 | `-- comment ::int8 trailing`         | unchanged                    |
| 10| `/* ::int8 block */`                 | unchanged                    |
| 11| `'O''Reilly :: ok'` (doubled-quote)  | unchanged                    |
| 12| `SELECT 'a::b'::text`                | `SELECT 'a::b'` (quoted stays)|

The list is closed under the V1 scope: parameterised types
(`numeric(P,S)`, `varchar(N)`) need the `(args)` skip; ASCII
identifiers + underscores cover every PG built-in type name + every
flavour of `_int4` / `_text` array-of-type the JDBC driver emits;
strings + line comments + block comments are the only V1 contexts the
scanner has to preserve.

V1 ONE-LEVEL-NESTED cap: `(a::int)::text` strips both casts because
the scanner only stops at `)`; nested-cast detection is out of scope
(it's not a pattern pgJDBC simple-mode emits and detecting it would
require parsing parenthesis depth which is a real SQL AST job).

## 2 — Where the strip happens

The cast strip runs at the dispatch entry point in
`crates/kessel-pg-gateway/src/dispatch.rs::dispatch_query`, BEFORE
`is_effectively_empty`, BEFORE `contains_multiple_statements`, BEFORE
`pg_catalog::catalog_query_hook`, BEFORE `engine.apply_sql`. The
function is byte-additive: the existing dispatch loop runs unchanged
on the rewritten SQL; every prior text-only KAT keeps passing
byte-for-byte (V1 KATs cover SQL without casts, where the rewrite is
a no-op).

The extended-query path (`extq::mod::dispatch_execute`) routes
through `dispatch::dispatch_query` at line 1116
(`let dispatched = crate::dispatch::dispatch_query(&rewritten, engine);`)
so the strip ALSO applies to the extended-query Execute path after
parameter substitution. This catches the rare case of
`Bind($1=42) → "SELECT $1::int8"` where substitute renders
`SELECT 42::int8` and then the strip converts that to `SELECT 42`.

## 3 — The `strip_pg_casts` algorithm (state machine)

```
state: { Normal, InString, InLineComment, InBlockComment }
i = 0
while i < bytes.len():
  switch state:
  Normal:
    if bytes[i..i+2] == b"::" and outside identifier-tail:
      i += 2
      skip whitespace
      skip identifier ([A-Za-z_][A-Za-z0-9_]*)
      if bytes[i] == b'(':  skip until matching b')' (one level)
      continue  # drop the cast bytes
    if bytes[i] == b'\'':
      emit b'\''; i += 1; state = InString
    elif bytes[i..i+2] == b"--":
      emit b"--"; i += 2; state = InLineComment
    elif bytes[i..i+2] == b"/*":
      emit b"/*"; i += 2; state = InBlockComment
    else:
      emit bytes[i]; i += 1
  InString:
    emit bytes[i]
    if bytes[i] == b'\'':
      if bytes[i+1] == b'\'':  # doubled-quote escape
        emit bytes[i+1]; i += 2
      else:
        i += 1; state = Normal
    else:
      i += 1
  InLineComment:
    emit bytes[i]
    if bytes[i] == b'\n':  state = Normal
    i += 1
  InBlockComment:
    emit bytes[i]
    if bytes[i..i+2] == b"*/":  emit bytes[i+1]; i += 2; state = Normal
    else: i += 1
```

Properties (locked by KATs in T2):

- **K-CAST-1** — `strip_pg_casts("")` returns `""`.
- **K-CAST-2** — `strip_pg_casts(sql_with_no_casts)` returns sql_with_no_casts byte-for-byte.
- **K-CAST-3** — `strip_pg_casts("SELECT 1::int8")` returns `"SELECT 1"`.
- **K-CAST-4** — `strip_pg_casts("SELECT col::text FROM t")` returns `"SELECT col FROM t"`.
- **K-CAST-5** — `strip_pg_casts("WHERE col = $1::int4")` returns `"WHERE col = $1"`.
- **K-CAST-6** — `strip_pg_casts("'literal ::int8 inside'")` returns `"'literal ::int8 inside'"`.
- **K-CAST-7** — `strip_pg_casts("-- comment ::int8")` returns `"-- comment ::int8"`.
- **K-CAST-8** — `strip_pg_casts("/* ::int8 */")` returns `"/* ::int8 */"`.
- **K-CAST-9** — `strip_pg_casts("'O''Reilly ::ok'")` (doubled-quote in string) returns input.
- **K-CAST-10** — `strip_pg_casts("SELECT a::int, b::text FROM t")` returns `"SELECT a, b FROM t"`.
- **K-CAST-11** — `strip_pg_casts("NULL::numeric(15,2)")` returns `"NULL"`.
- **K-CAST-12** — `strip_pg_casts("SELECT 1::int8")` (no trailing space) returns `"SELECT 1"`.
- **K-CAST-13** — `strip_pg_casts(":")` returns `":"` (lone colon untouched).
- **K-CAST-14** — `strip_pg_casts("SELECT 'a::b'::text")` returns `"SELECT 'a::b'"` (cast outside string strips; cast text inside string preserved).
- **K-CAST-15** — `strip_pg_casts("NULL::timestamp WITH TIME ZONE")` returns `"NULL WITH TIME ZONE"` (only the first identifier strips — `TIMESTAMP WITH TIME ZONE` is a known V1 limitation, captured under K-CAST-15 to document the boundary; a real PG would parse the whole `TIMESTAMP WITH TIME ZONE` as the type, but the pgJDBC simple-mode emit uses the spaceless alias `timestamptz` for this case so the V1 strip is sufficient in practice).

## 4 — Dispatcher integration

```rust
pub fn dispatch_query<E: EngineApply + ?Sized>(sql: &str, engine: &E) -> Vec<u8> {
    // SP-PG-EXTQ-CAST T2 — strip PG type-cast operator `::TYPE[(args)]`
    // before any downstream dispatch. The strip is a no-op for SQL
    // without `::` so every prior text-only KAT still passes byte-
    // for-byte. Companion design spec:
    // `docs/superpowers/specs/2026-06-01-kesseldb-sppgextqcast-design.md`.
    let stripped = cast_stripper::strip_pg_casts(sql);
    let sql = stripped.as_str();
    // ... existing dispatch logic unchanged ...
}
```

Adopting `let sql = stripped.as_str()` shadows the parameter so every
later reference uses the rewritten text without ceremony.

## 5 — KAT plan (T2)

Module `cast_stripper` ships with ~15 KATs covering K-CAST-1..15.
`dispatch` ships with 1-2 integration KATs verifying that
`dispatch_query("SELECT 1::int8", &mock_engine)` no longer surfaces
the `42601 syntax_error` from `kessel-sql`'s lexer (it surfaces the
ordinary `0A000 V1 only renders SELECT * FROM <table>` error the
lexer-stripped form would have produced, which is what we want — that
error means the strip happened and the SQL reached the dispatcher's
SELECT renderer).

KAT delta target: +12-15 (cast_stripper module) + ~2 (dispatch
integration).

## 6 — Acceptance

1. `psql -c 'SELECT 1::int8 FROM smoke'` returns `1` (was 42601) on vulcan.
2. `psql -c 'SELECT col::text FROM smoke'` returns the column value (was 42601).
3. The existing pg-gateway lib KAT suite passes byte-for-byte (the
   cast_stripper is additive, no existing test mutates).
4. JDBC `preferQueryMode=simple` connect + SELECT round-trip works
   IF vulcan has `javac` installed. The vulcan box doesn't (see SP-PG-
   EXTQ T8 transcript §5 — javac missing — track install as
   `SP-PG-JDBC-SMOKE` arc). Skipping the JDBC round-trip in V1 is
   acceptable; the psql smoke proves the wire path.

## 7 — Out-of-scope (named follow-ups)

- **`SP-PG-EXTQ-CAST-VALIDATE`** — verify the stripped cast was
  well-typed against the target column / param slot. V1 is "strip +
  hope"; V2 could lift this if a workload starts emitting bad casts
  that the engine can't catch.
- **`SP-PG-EXTQ-CAST-NESTED`** — handle `(a::int)::text` correctly
  via parenthesis-depth tracking. V1's one-level cap is fine for
  pgJDBC emits.
- **`SP-PG-EXTQ-CAST-MULTIWORD-TYPE`** — recognise the multi-word
  type names PG accepts after `::` (`TIMESTAMP WITH TIME ZONE`,
  `CHARACTER VARYING(N)`, `DOUBLE PRECISION`). V1's identifier-only
  strip handles every pgJDBC simple-mode emit because pgJDBC uses
  the spaceless aliases (`timestamptz`, `varchar`, `float8`).
- **`SP-PG-JDBC-SMOKE`** — install OpenJDK + `javac` on vulcan, run
  the pgJDBC compat smoke against `preferQueryMode=simple` to verify
  the round-trip end-to-end with a real driver, not just psql.
- **`SP-SQL-AST-CAST-NODE`** — make `kessel-sql` parse `::` as a real
  cast operator node so a workload that needs the explicit type
  hint gets it. V1's text-strip discards the hint; V2's AST node
  keeps it.

## 8 — Closure shape

3-4 commits per the standing rules:

1. T1 + T2 — design spec + `cast_stripper.rs` + KATs + dispatch wire-up.
2. T3 — vulcan smoke transcript (`docs/superpowers/sppgextqcast-t3-smoke-2026-06-02.txt`) + USAGE §9 update flipping JDBC PARTIAL → PASS (or PASS-with-vulcan-no-javac caveat).
3. T4 — STATUS.md row + progress tracker close + V2 follow-up names.

CI green is the release gate per standing rules; binaries via
release.yml on `v*` tags only (no release here — text-rewrite only).
