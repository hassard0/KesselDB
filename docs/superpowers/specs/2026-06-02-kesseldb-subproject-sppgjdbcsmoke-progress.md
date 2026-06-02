# SP-PG-JDBC-SMOKE — real pgJDBC end-to-end smoke — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T2 (2026-06-02) — DONE_WITH_CONCERNS.**
Extended (default) JDBC mode CRUD core (CREATE / parameterized INSERT
binary INT8+VARCHAR / SELECT * / parameterized SELECT WHERE id = ?
binary INT8) round-trips end-to-end through real pgJDBC 42.7.4 +
OpenJDK 21 on vulcan. Simple mode (?preferQueryMode=simple) passes
for literal SQL including `WHERE id = 42::int8` (the SP-PG-EXTQ-CAST
T2 cast-stripper works through the real driver). Two residual gaps
surfaced, each a precise new follow-up arc (concerns named, not
masked): `SP-PG-SQL-PAREN-VALUES` (kessel-sql VALUES parser does not
accept `('lit')` paren-wrapped values that pgJDBC always emits in
simple-mode `PreparedStatement`); `SP-PG-EXTQ-DESCRIBE-VERSION`
(extended-mode `SELECT version()` gateway answers `Describe(portal)`
with `NoData` before `RowDescription`, pgJDBC raises). TaskList #364
ready for completion.

Smoke transcript: `docs/superpowers/sppgjdbcsmoke-t2-smoke-2026-06-02.txt`
Harness: `scripts/JdbcSmoke.java`
Parent SP-arc: SP-PG-EXTQ-CAST V1 (closed 2026-06-02 at T2); the V1
out-of-scope clause named this arc as the follow-up to "install
javac on vulcan + run real pgJDBC `preferQueryMode=simple` round-
trip end-to-end."

## What this SP-arc ships

V1 = "real pgJDBC end-to-end against KesselDB on vulcan, both
`preferQueryMode=simple` and default extended mode, with the
verbatim per-scenario PASS/FAIL recorded in a smoke transcript."
No source under `crates/` changes; the surface deliverables are the
checked-in harness + the transcript + the USAGE matrix flip.

After V1 lands (T1..T3), a developer running a Spring/Hibernate/
MyBatis (or any other JDBC-based stack) workload against KesselDB
can see — verbatim — which JDBC scenarios round-trip today and which
two surfaces have named V2 follow-up arcs.

**Out-of-scope (named, deferred — each is its own future arc):**

- **`SP-PG-SQL-PAREN-VALUES` (V2)** — kessel-sql's VALUES parser
  (`crates/kessel-sql/src/lib.rs` ~L1193) accepts bare `Tok::Int` /
  `Tok::Str` literals only; pgJDBC simple-mode `PreparedStatement`
  always wraps each substituted parameter in parentheses
  (`VALUES (('42'::int8), ('hello-jdbc'))`). The cast strip works,
  but the post-strip SQL still contains the parens. V2 accepts
  single-literal parens at the VALUES position. Orthogonal to the
  cast stripper.
- **`SP-PG-EXTQ-DESCRIBE-VERSION` (V2)** — extended-mode `SELECT
  version()` causes the gateway to answer `Describe(portal)` with
  `NoData` before sending `RowDescription` + `DataRow`. pgJDBC
  treats `NoData` as authoritative ("this query returns nothing")
  and raises `IllegalStateException` when DataRow arrives. V2 fixes
  the gateway's portal-Describe routing for built-in scalar-function
  SELECTs so RowDescription is sent in response to Describe.
- **`SP-PG-VULCAN-JDK-APT` (V2)** — install `openjdk-21-jdk`
  system-wide on vulcan via `apt`. V1 worked around the missing
  `javac` by extracting a standalone OpenJDK 21 tarball into
  `~/jdbc-smoke/jdk-21.0.2`; this is fine for the smoke but every
  future JDBC arc has to either reuse the same tarball or re-extract
  it. V2 puts `javac` on `PATH` once and for all so the classifier
  doesn't need a sudo-password each time.

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | `scripts/JdbcSmoke.java` checked in — minimal harness driven by `args[0] in {simple, extended}`; runs CREATE / parameterized INSERT / SELECT * / parameterized SELECT WHERE id = ? / SELECT version(); asserts inline; prints `ALL TESTS PASS` on success. | **DONE** | `3642165` |
| **T2** | vulcan smoke — (1) download OpenJDK 21 tarball + pgJDBC 42.7.4 (user-space, no sudo), (2) build kesseldb-server `--features pg-gateway` with `CARGO_TARGET_DIR=/tmp/kdb-target-jdbcsmoke`, (3) run JdbcSmoke in `simple` + `extended` modes + auxiliary `JdbcSmokeLiteral` (simple-mode literal SQL) + `JdbcSmokeNoVersion` (extended-mode CRUD-only), (4) capture verbatim FE=>/<=BE wire trace via `org.postgresql.level = ALL`, (5) write `docs/superpowers/sppgjdbcsmoke-t2-smoke-2026-06-02.txt` with the per-scenario matrix + two new follow-up arc names + verbatim wire-level diagnostics, (6) pivot USAGE §9 ORM-matrix JDBC row from "PSQL-proxy PASS** + javac install needed" to the verbatim per-scenario verdict + named follow-up arcs. | **DONE** | `d2eba95` |
| **T3** | STATUS.md "Tonight's delivery" entry + arc closure + this progress tracker. | **DONE** | (this commit) |

## Acceptance criteria

1. **Extended (default) JDBC mode CRUD core PASS end-to-end via real
   pgJDBC 42.7.4 on vulcan against KesselDB pg-gateway.** ✅ Met.
   `CREATE TABLE`, parameterized `INSERT` (binary INT8 + VARCHAR
   params), `SELECT *`, parameterized `SELECT WHERE id = ?` (binary
   INT8 param + binary INT8 result column) all round-trip end-to-end
   through the real driver. SP-PG-EXTQ-BIN + SP-PG-EXTQ-BIN-RESULTS
   are now real-driver-verified.
2. **Simple-mode JDBC literal SQL with `::int8` cast PASS via real
   pgJDBC on vulcan.** ✅ Met. `WHERE id = 42::int8` round-trips —
   the SP-PG-EXTQ-CAST T2 cast-stripper works end-to-end through the
   actual driver, validating the psql-proxy proof.
3. **Verbatim per-scenario matrix + two named V2 follow-up arcs for
   the residual gaps captured in a smoke transcript under
   `docs/superpowers/`.** ✅ Met — `sppgjdbcsmoke-t2-smoke-2026-06-02.txt`
   §6 lists 10 scenarios with PASS/FAIL + cause + verbatim wire trace.
4. **USAGE.md §9 ORM matrix JDBC row pivoted to verbatim verdict;
   `SP-PG-JDBC-SMOKE` follow-up arc name removed from the "remaining
   ORM gaps" bullet (closed end-to-end with new arc names listed).**
   ✅ Met.
5. **No source under `crates/` touched — this is a verification arc,
   KAT delta +0.** ✅ Met. `git diff` between `da8f6b2` (pre-arc) and
   the T3 commit shows only `scripts/JdbcSmoke.java` (new), `docs/
   USAGE.md` (matrix row + residual bullet), `docs/STATUS.md` (new
   "Tonight's delivery" entry), `docs/superpowers/sppgjdbcsmoke-t2-
   smoke-2026-06-02.txt` (new), and this progress tracker (new).

## DONE_WITH_CONCERNS — what the concerns are

1. **Simple-mode `PreparedStatement` INSERT still fails** with
   `ERROR: sql: expected value` on vulcan. The cast stripper does its
   job (the post-strip SQL is `INSERT INTO jdbc_smoke (id, name)
   VALUES (('42'), ('hello-jdbc'))`); kessel-sql's VALUES parser
   rejects the parenthesized literals. Reproduces in psql with the
   same paren shape, so this is not a wire-layer bug — it is a
   distinct kessel-sql gap tracked as `SP-PG-SQL-PAREN-VALUES`.
2. **Extended-mode `SELECT version()` fails** with pgJDBC raising
   `IllegalStateException: Received resultset tuples, but no field
   structure for them`. Gateway answers `Describe(portal)` with
   `NoData` then sends `RowDescription` + `DataRow`. Tracked as
   `SP-PG-EXTQ-DESCRIBE-VERSION`. Doesn't affect any
   line-of-business CRUD path; surfaces only when an app probes the
   server-version banner in extended mode.
3. **vulcan still lacks a system-wide `javac`.** The standalone JDK
   under `~/jdbc-smoke/jdk-21.0.2` is the workaround; tracked as
   `SP-PG-VULCAN-JDK-APT` so a future arc that wants `javac` on
   `PATH` has a precise unblock target.

None of these concerns invalidate the headline PASS — extended-mode
JDBC CRUD round-trips end-to-end via real pgJDBC against KesselDB on
vulcan, and the simple-mode `::int8` cast strip is real-driver-PASS.
