# KesselDB Sub-project 30 — SQL UPDATE (server-side RMW)

**Date:** 2026-05-17  **Status:** spec + build. Completes SQL CRUD
(CREATE / INSERT / **UPDATE** / DELETE / SELECT).

## Why it needs the server

`UPDATE t SET a=…` is a partial mutation: it must read the current row,
apply the SET list, and re-encode — a read-modify-write that needs engine
state. So `kessel-sql` gains `Stmt { Op(Op), Update{type_id,id,sets} }` and
`compile_stmt`; the engine thread executes `Stmt::Update` via
`GetById → kessel_codec::decode → set fields → encode → Op::Update`. Pure
`compile()` still returns an `Op` for non-UPDATE and cleanly errors on
UPDATE (telling the caller to use the server path) — no silent half-support.

## Syntax

`UPDATE <table> ID <n> SET col = val [, col = val ...]`

(explicit object id, consistent with INSERT/DELETE — the engine never
invents ids; determinism preserved). Missing row ⇒ `NotFound` over the wire;
all constraints/indexes/triggers fire on the resulting `Op::Update`.

## Tests

`kesseldb-server::sql_over_tcp` extended: `UPDATE acct ID 1 SET bal=500`
then re-aggregate (1049→1499), and `UPDATE … ID 999` ⇒ `NotFound`.
`kessel-sql` parse tests. 122 tests total green.
