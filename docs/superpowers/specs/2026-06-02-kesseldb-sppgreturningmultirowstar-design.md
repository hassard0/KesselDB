# SP-PG-RETURNING-MULTIROW-STAR — multi-row INSERT RETURNING + `RETURNING *` — design

Date: 2026-06-02

## 1. Context

`SP-PG-SERIAL-RETURNING` landed deterministic autoincrement + **single-row**
`INSERT … RETURNING id` — but the SQLAlchemy autoincrement smoke had to
construct its engine with `use_insertmanyvalues=False`. That flag is NOT
SQLAlchemy's default. By default (`use_insertmanyvalues=True`) SQLAlchemy
2.0 batches multiple pending ORM objects into a **single** statement:

```sql
INSERT INTO widgets (name) VALUES ('a'),('b'),('c') RETURNING id
```

and expects **N DataRows back** (one assigned id per inserted row, in
insertion order), which it threads back onto the flushed objects. With the
SERIAL-RETURNING engine, that multi-row RETURNING returned a single (or
wrong) DataRow, so the default config failed — hence the workaround.

This arc closes the gap so KesselDB works with SQLAlchemy's
**out-of-the-box** config: `pip install sqlalchemy psycopg2`, point the
engine URL at KesselDB, and CRUD just works — the "it just works"
milestone.

Two coupled features:

- **Multi-row INSERT RETURNING** — `INSERT … VALUES (…),(…),(…) RETURNING
  id` returns N DataRows, each carrying the row's assigned id, in order.
- **`RETURNING *`** — `RETURNING *` expands to every table column (id +
  all fields), not just an explicit list.

## 2. Determinism analysis (THE critical section)

A multi-row INSERT already compiles (SP58) to **ONE** `Op::Txn { ops:
[Create, Create, Create] }`. Each `Op::Create` on a `serial_pk` type
carries the `SERIAL_SENTINEL` id; the SM assigns the next per-type
sequence value (SP-PG-SERIAL-RETURNING). Because the Txn applies its
inner ops in order on the single deterministic apply thread, the
sequence counter advances `n+1, n+2, n+3` deterministically — **exactly
the same counter writes SP-PG-SERIAL-RETURNING already proves replicate
bit-for-bit**. Multi-row adds NO new non-determinism: it is N applications
of the proven single-row assignment within one Txn.

The ONLY new work is **threading the assigned ids back**: today the
`Op::Txn` apply arm collapses all inner results to `OpResult::Ok`,
discarding the per-Create `OpResult::Created { id }`. We add an additive
result variant `OpResult::CreatedMany { ids: Vec<u128> }` that the Txn arm
populates when (and only when) every inner op was a `Create` that returned
`Created`. The ids are a deterministic function of the committed log
prefix (the counter writes), so the variant carries no clock/RNG state —
the 3-replica byte-identity digest test stays green (the digest covers the
counter storage key, which is identical across replicas; the result
variant is transport-only and never enters the digest).

When a Txn contains a non-Create op (e.g. a multi-statement UPDATE batch),
or any Create returned plain `Ok` (explicit-id, non-serial), the arm
returns `OpResult::Ok` exactly as before — byte-identical to the pre-arc
shape, so every existing Txn KAT passes unchanged.

## 3. Scope (V1)

- **Feature A**: multi-row `INSERT … VALUES (…),(…) RETURNING <cols>` →
  N DataRows. The Txn surfaces each Create's assigned id via
  `OpResult::CreatedMany`; the gateway emits one DataRow per id +
  `CommandComplete INSERT 0 N`.
- **Feature B**: `INSERT … RETURNING *` → a DataRow with EVERY table
  column (the assigned id plus every declared field), in declared order.
  The gateway expands `*` against `describe_table`.
- Proto: `OpResult::CreatedMany { ids: Vec<u128> }` (additive, new tag 16).
- kessel-sql: `insert_returning` recognizes the `RETURNING *` star form
  (returns a sentinel `["*"]` column list the gateway expands).

## 4. V1 out-of-scope (named follow-ups)

- **RETURNING with expressions** (`RETURNING id + 1`, `RETURNING
  upper(name)`) — `SP-PG-RETURNING-EXPR`. V1 returns bare columns + `*`.
- **UPDATE / DELETE multi-row RETURNING** — `SP-PG-SQL-RETURNING-DML`.
  V1 scopes to INSERT RETURNING.
- **Mixed multi-row RETURNING column lists where some rows omit a serial
  column and others supply it** — V1 assumes a uniform shape (the
  SQLAlchemy batched-insert always emits a uniform column list).

## 5. Acceptance

A SQLAlchemy model declared WITHOUT an explicit id —
`id = Column(BigInteger, primary_key=True, autoincrement=True)` — does
full CRUD against KesselDB over the PG wire using SQLAlchemy's **DEFAULT**
engine config (NO `use_insertmanyvalues=False`). A batched
`session.add_all([a, b, c]); session.commit()` flushes one multi-row
`INSERT … RETURNING id`, all three objects read back their DB-assigned
ids, and a subsequent SELECT returns all three rows. Measured on vulcan.
All existing kessel-sql / SM / gateway / proto KATs pass (regression
guard). The seed-7 VSR oracle + 3-replica byte-identity digest test stay
green.

## 6. Weak spots (named, not all fixed in V1)

1. **`CreatedMany` only fires for all-Create Txns** — a Txn mixing
   Creates with Updates returns `Ok` (no ids surfaced). Acceptable: the
   only producer of a `Create`-only Txn is a multi-row INSERT; SQL UPDATE
   batches are a separate (V2) shape.
2. **`RETURNING *` column order** — V1 emits columns in catalog declared
   order (id pseudo-column first, then fields). PG emits the table's
   attribute order; KesselDB's declared order matches for the
   SQLAlchemy-generated DDL shape. Documented; a reorder would be
   `SP-PG-RETURNING-STAR-ORDER`.
3. **Read-back cost** — the gateway reads each inserted row back via a
   `SELECT … WHERE id = <id>` to project non-id RETURNING columns /
   `RETURNING *`. For the common `RETURNING id` (no read-back needed) we
   short-circuit and render the assigned id directly. Multi-column /
   star paths do one read-back per row (N reads); acceptable for the
   batched-flush sizes SQLAlchemy emits (tens of rows).
4. **Expressions** — out of scope (§4).

## 7. Execution

5-7 commits: T1 design, T2 proto `CreatedMany` + SM Txn id-threading +
KATs, T3 kessel-sql multi-row RETURNING + `RETURNING *` parse + KATs, T4
gateway multi-row + star render + KATs, T5 vulcan SQLAlchemy DEFAULT-config
smoke, T6 closure. All cargo on vulcan with
`CARGO_TARGET_DIR=/tmp/kdb-t-mret`. Direct commits to main; CI green is the
gate. Determinism is sacred — the seed-7 oracle + 3-replica byte-identity
digest test PROVE replicas agree; multi-row assignment is N applications of
the proven single-row counter advance within one Txn.
