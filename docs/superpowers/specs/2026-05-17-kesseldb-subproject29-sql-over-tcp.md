# KesselDB Sub-project 29 — SQL over TCP

**Date:** 2026-05-17  **Status:** spec + build. Makes KesselDB a *usable
networked SQL database*: connect, send SQL strings, get results.

## Change

The engine thread now receives **raw request frames** and decides:
`[0xFE] ++ utf8` ⇒ compile the SQL against the **live catalog** (must run on
the engine thread — the catalog lives with the non-`Send` StateMachine) then
apply; otherwise `Op::decode` as before. `kessel-client` gains
`Client::sql(&str) -> OpResult`. The Op wire path is unchanged
(`0xFE` is never an Op kind), so this is purely additive.

## Result

```rust
let mut c = Client::connect(addr)?;
c.sql("CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)")?;
c.sql("INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)")?;
c.sql("SELECT SUM(bal) FROM acct WHERE owner = 100")?; // -> i128
```

Real fsync (DirVfs), serial deterministic engine, SQL inherits all
constraints/indexes/triggers. Bad SQL returns a clean `SchemaError` over the
wire (no crash).

## Tests

`kesseldb-server::sql_over_tcp` — CREATE/INSERT/SELECT SUM…WHERE + a
malformed statement, all over a real socket. 122 tests total green.
