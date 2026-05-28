# SQL surface

KesselDB compiles SQL server-side against the live catalog. Supported
surface (each item is covered by the test suite):

- DDL — `CREATE TABLE`, `ALTER TABLE … ADD COLUMN` (online, no lock),
  `DROP TABLE`, `CREATE [UNIQUE|RANGE] INDEX`, `DROP INDEX`, `DESCRIBE`,
  `EXPLAIN`.
- DML — `INSERT … VALUES (…),(…)`* (multi-row = one atomic op),
  `UPDATE`, `DELETE`.
- Queries — `SELECT * | <projection>` with `WHERE` (=, !=, <, <=, >,
  >=, AND/OR/NOT, IN, BETWEEN, LIKE, IS [NOT] NULL), `JOIN`, `GROUP BY`,
  `ORDER BY`, `LIMIT/OFFSET`, `COUNT/SUM/MIN/MAX/AVG`.
- Constraints — `NOT NULL`, `UNIQUE`, foreign keys (`ON DELETE
  RESTRICT/CASCADE/SET NULL`), `CHECK`, deterministic triggers,
  deterministic WASM-MVP UDFs.
- Transactions — SQL `BEGIN`/`COMMIT`/`ROLLBACK` (atomic non-interactive
  write batch; reads inside `BEGIN` are rejected by design) plus
  op-level `Op::Txn` for the cluster path.

Full reference (every form, every keyword, every operator):
[Usage guide (full) §4–§6](full-usage.md#4-sql-reference).
