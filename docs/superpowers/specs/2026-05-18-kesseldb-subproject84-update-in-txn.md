# KesselDB Sub-project 84 — UPDATE inside a transaction

**Date:** 2026-05-18  **Status:** shipped. Closes the documented
"UPDATE inside a transaction is not yet supported" deferral.

## Why it was deferred

SQL `UPDATE` is a server-side read-modify-write (GetById → decode →
apply SETs → encode → `Op::Update`) done at the connection layer. The
`BEGIN`/`COMMIT` buffer compiles every statement to an `Op` *before*
submitting one atomic `Op::Txn`, so the RMW's read couldn't be
resolved at build time and `Stmt::Update` was rejected.

## Fix — a deterministic replicated RMW op

New `Op::UpdateSet { type_id, id, sets: Vec<(u16, raw bytes)> }`. The
state machine reads the **current** record (overlay-aware `get` ⇒
read-your-writes within the same txn), decodes it, splices the set
fields, re-encodes, and delegates to the proven `Op::Update` path
(triggers / NOT NULL / UNIQUE / FK / CHECK / balance guard / indexes /
overflow GC). Because it is a single replicated op it composes inside
`Op::Txn` and is deterministic.

`Op::UpdateSet` is added to the `Op::Txn` allowed-ops set. The
`TXN_TAG` builder, instead of rejecting `Stmt::Update`, resolves each
`Value` → raw field bytes via the live catalog (new
`kessel_codec::raw_from_value`) and emits `Op::UpdateSet`. The
non-transactional single-statement `UPDATE` path is untouched
(unchanged, still proven).

## Verified

- `kessel-sm::update_set_rmw_composes_in_txn_and_is_deterministic`
  (fast): standalone RMW; `NotFound` on a missing row; composes inside
  `Op::Txn` reading a row created earlier in the *same* batch
  (read-your-writes); a failing member rolls the whole batch back;
  identical op streams ⇒ identical digest.
- `kesseldb-server::sql_update_inside_transaction` (e2e over the
  socket server): `BEGIN; UPDATE; INSERT; COMMIT` applies atomically;
  `BEGIN; UPDATE; ROLLBACK` discards it; an `UPDATE` of a missing row
  inside a txn aborts the whole batch (the buffered `INSERT` does not
  persist).

Full workspace regression green; determinism / VSR partition corpus
(incl. seed 7) unchanged (additive op; existing UPDATE path untouched).

## Honest boundary

`UPDATE … SET col = NULL` *inside a transaction* is the one case not
covered (the raw-bytes patch carries no NULL sentinel) — it returns a
clear error and still works outside a transaction. Cluster (multi-node)
SQL `UPDATE` keeps its existing continuation path; this slice targets
the single-node `BEGIN`/`COMMIT` server, which is what the deferral was
about.
