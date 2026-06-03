# SP-PG-RETURNING-MULTIROW-STAR — multi-row INSERT RETURNING + RETURNING * — SP-arc Progress Tracker

Date created: 2026-06-02

**Status: CLOSED — DONE (2026-06-03).** Closes the **zero-config
SQLAlchemy** milestone. SP-PG-SERIAL-RETURNING shipped single-row
`INSERT … RETURNING id`, but its smoke needed `use_insertmanyvalues=False`
— NOT the SQLAlchemy default. SQLAlchemy 2.0's DEFAULT
(`use_insertmanyvalues=True`) BATCHES a flush of multiple pending objects
into ONE statement and expects N rows back. KesselDB now works with that
DEFAULT engine config: a batched `session.add_all([a,b,c]); commit()`
reads back every DB-assigned id. **DEFAULT-config CRUD 5/5 on vulcan.**
+20 KATs; all touched-crate suites green (proto 17, sql 95, sm 176,
client 16, pg-gateway 986); determinism PROVEN (VSR 27/27 incl.
`jepsen_3replica_partition_converges_byte_identical` +
`large_seed_corpus_is_deterministic_and_converges`); zero regressions.

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgreturningmultirowstar-design.md`
Smoke transcript: `docs/superpowers/sppgreturningmultirowstar-t5-smoke-2026-06-02.txt`

## Slice plan

| T# | Scope | Status | Commit(s) |
|---|---|---|---|
| T1 | Design (determinism: multi-row = N applications of the proven single-row counter advance within one Txn) | DONE | `8127fd8` |
| T2 | proto `OpResult::CreatedMany{ids}` (tag 16) + SM `Op::Txn` threads per-Create assigned ids; KATs | DONE | `bccdd69` |
| T3 | kessel-sql: `insert_returning` recognizes `RETURNING *` (star sentinel) + `col AS alias`; KATs | DONE | `9bde788`, `0a87614`, `5698624`, `927068a` |
| T4 | gateway: `render_insert_returning` emits N DataRows + expands `RETURNING *`; dispatch routes `CreatedMany`; KATs | DONE | `0a87614` |
| T5 | `insertmanyvalues` rewrite + vulcan SQLAlchemy DEFAULT-config smoke → 5/5 | DONE | `927068a`, `dc3d256` |
| T6 | STATUS + USAGE + CHANGELOG + tracker closure + transcript | DONE | (this commit) |

## How it works

- **Multi-row INSERT → Op::Txn (SP58).** `INSERT … VALUES (…),(…),(…)
  RETURNING id` already compiles to ONE `Op::Txn` of N `Op::Create`s.
  Each Create on a `serial_pk` type carries `SERIAL_SENTINEL`; the SM
  assigns the next per-type sequence value (SP-PG-SERIAL-RETURNING).
- **Threading the ids back.** The `Op::Txn` apply arm now collects each
  inner Create's `OpResult::Created { id }` into an ordered vec and
  returns `OpResult::CreatedMany { ids }` — but ONLY when EVERY inner op
  autoincrement-assigned (else byte-identical `OpResult::Ok`, so every
  prior Txn KAT is unaffected).
- **Determinism.** The counter advances N times on the single
  deterministic apply thread, in op-number order — N applications of the
  proven single-row advance. The ids are a pure function of the committed
  log prefix; `CreatedMany` is transport-only (never enters the digest).
  3-replica byte-identity stays green.
- **Gateway render.** `render_insert_returning` takes the assigned-id list
  and emits ONE RowDescription + N DataRows (one read-back + project per
  id) + `CommandComplete INSERT 0 N`. `RETURNING *` (the `["*"]` star
  sentinel from `kessel_sql::insert_returning`) expands to every table
  column via `describe_table`.
- **SQLAlchemy `insertmanyvalues` rewrite.** SQLAlchemy's DEFAULT batched
  flush emits NOT plain multi-row VALUES but
  `INSERT … SELECT p0::VARCHAR FROM (VALUES ('a',0),('b',1),…) AS
  imp_sen(p0, sen_counter) ORDER BY sen_counter RETURNING …`. The new
  `insertmanyvalues::rewrite_insertmanyvalues` desugars this to the plain
  multi-row form (drops the projection cast, the trailing `sen_counter`
  ordering column, and the FROM-VALUES-ORDER-BY scaffolding). It runs at
  the gateway SQL entry **BEFORE** the literal-cast validator (which would
  otherwise reject `p0::VARCHAR`), on both the simple and extended paths.
  Conservative: a no-op for any non-matching SQL (byte-untouched).

## Named follow-up arcs (still out after this arc)

| Arc | Notes |
|---|---|
| `SP-PG-SQL-RETURNING-DML` | UPDATE/DELETE … RETURNING. |
| `SP-PG-RETURNING-EXPR` | `RETURNING id + 1` / expressions (V1 returns bare columns + `*`). |
| `SP-PG-SEQUENCE-DDL` | `CREATE SEQUENCE` + `nextval`/`setval`. |
| `SP-PG-SERIAL-NONPK` | a SERIAL column that is NOT the primary key. |
| `SP-PG-RETURNING-STAR-ORDER` | `RETURNING *` column order vs PG attribute order (V1 = catalog declared order). |
