# SP-PG-EXTQ-CAST — JDBC simple-mode `::cast` rewrite — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T2 (2026-06-02).** `psql -c 'SELECT 1::int8'`
on vulcan returns `1` (was `42601 unexpected char ':'` pre-arc). Real
pgJDBC `preferQueryMode=simple` round-trip awaits javac install on
vulcan (tracked as V2 `SP-PG-JDBC-SMOKE`). TaskList #359 ready for
completion.

Design spec: `docs/superpowers/specs/2026-06-01-kesseldb-sppgextqcast-design.md`
Smoke transcript: `docs/superpowers/sppgextqcast-t3-smoke-2026-06-02.txt`
Parent SP-arc: SP-PG-EXTQ V1 (closed 2026-05-29 at T8); the V1
out-of-scope clause named this arc as the JDBC simple-mode unblock
for `::int8` / `::text` / `::numeric(15,2)` cast operators.

## What this SP-arc ships

V1 = "every JDBC `preferQueryMode=simple` SQL with `::TYPE[(args)]`
cast operators reaches the engine in a form `kessel-sql` can parse."
After V1 lands (T1..T2), pgJDBC simple-mode (and PostGIS / pgvector
helpers using the same `::` cast syntax) can:

1. Send `Q` Simple Query containing `SELECT col::int8 FROM t` —
   gateway strips `::int8` at dispatch entry; engine sees
   `SELECT col FROM t`; row returns.
2. Send extended-query `Bind($1=42) → Execute("SELECT $1::int8")`
   — substitute renders `SELECT 42::int8`; gateway strips at
   dispatch entry; engine sees `SELECT 42`; row returns.
3. Send INSERT / UPDATE / DELETE with `::TYPE` casts in VALUES or
   WHERE — same strip, same flow, same success.

**Out-of-scope (named, deferred — each is its own arc):**

- **`SP-PG-EXTQ-CAST-VALIDATE` (V2)** — verify the stripped cast
  was well-typed against the target column / param slot. V1 is
  "strip + hope" because the engine's type-checker already covers
  the common cases.
- **`SP-PG-EXTQ-CAST-NESTED` (V2)** — handle `(a::int)::text`
  correctly via parenthesis-depth tracking. V1's one-level cap is
  fine for pgJDBC emits.
- **`SP-PG-EXTQ-CAST-MULTIWORD-TYPE` (V2)** — recognise multi-word
  PG type names after `::` (`TIMESTAMP WITH TIME ZONE`, `CHARACTER
  VARYING(N)`, `DOUBLE PRECISION`). V1's identifier-only strip
  handles every pgJDBC simple-mode emit because pgJDBC uses the
  spaceless aliases (`timestamptz`, `varchar`, `float8`).
- **`SP-PG-JDBC-SMOKE` (V2)** — install OpenJDK + `javac` on
  vulcan, run the pgJDBC compat smoke against
  `preferQueryMode=simple` to verify the round-trip end-to-end
  with a real driver, not just psql.
- **`SP-SQL-AST-CAST-NODE` (V2)** — make `kessel-sql` parse `::`
  as a real cast operator node so a workload that needs the
  explicit type hint gets it. V1's text-strip discards the hint;
  V2's AST node keeps it.

## Slice plan (mirrors design spec §8)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec (~250 LoC, K-CAST-1..15 lock-list) + module scaffold in `crates/kessel-pg-gateway/src/cast_stripper.rs`. | **DONE** (folded into T2 commit) | `9c7800b` |
| **T2** | `cast_stripper::strip_pg_casts(sql) -> String` implementation (single-pass state-machine scanner; preserves quoted strings + doubled-quote escape + line comments + block comments + parenthesised type args) + 24 module KATs (K-CAST-1..15 + extras) + 2 `dispatch::tests::sppgextqcast_*` integration KATs + wire-up at `dispatch::dispatch_query` entry. Updates `t2bin_dispatch_execute_substitutes_bytea_binary_with_cast` to assert the post-strip form because the engine now receives `'\xdead'` not `'\xdead'::bytea`. | **DONE** | `9c7800b` |
| **T3** | vulcan psql smoke transcript (10 cases covering K-CAST-3 / K-CAST-4 / K-CAST-5 / K-CAST-11 + INSERT VALUES casts + multi-cast WHERE) + USAGE §9 update (JDBC PARTIAL → PASS\*\* with psql-proxy caveat + residual-gaps section pivots to CLOSED + spec reference list). | **DONE** | `b4c8f8c` |
| **T4** | STATUS row + parent SP-PG-EXTQ progress tracker V2 follow-up entry pivoted to "CLOSED 2026-06-02 at T2" + this progress tracker → CLOSED. | **DONE** | (this commit) |

KAT delta: +26 (24 `cast_stripper::tests::*` + 2 `dispatch::tests::sppgextqcast_*`).

## Headline

`psql -c 'SELECT 1::int8'` on vulcan returns `1`. Pre-arc surfaced
`42601 syntax_error: unexpected char ':'`. The strip is a no-op for
SQL without `::` so every prior text-only KAT in the pg-gateway lib
suite passes byte-for-byte (locked by
`cast_stripper::tests::no_cast_pure_passthrough_fuzz`).

## Smoke transcript

`docs/superpowers/sppgextqcast-t3-smoke-2026-06-02.txt` records the
verbatim psql 16.14 round-trip on vulcan covering:

- HEADLINE — `SELECT 1::int8` returns `1`.
- `SELECT 1::int4` PASS.
- `SELECT 1::numeric(15,2)` PASS — parameterised cast `(N,M)` skipped.
- `WHERE id = 1::int8` PASS — JDBC simple-mode parameterised lookup shape.
- Multi-cast WHERE PASS.
- INSERT VALUES `(3::int8, 'three'::text)` PASS.
- Post-INSERT SELECT * returns 3 rows.
- JDBC end-to-end → blocked on javac install, track as V2
  `SP-PG-JDBC-SMOKE`.
