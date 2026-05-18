# KesselDB Sub-project 96 — cross-shard scatter-gather reads: ASSESSMENT

**Date:** 2026-05-18  **Status:** assessment / scoping only (no
code). Deliverable for the "assess/scope (large)" task — an honest
plan and an incremental slicing, not an implementation.

## Where things stand (verified in `crates/kesseldb-server/src/router.rs`)

`Router::route` today returns:

- `Route::One(shard_of(type_id,id))` — point ops (`GetById`,
  `Create`/`Update`/`Delete`).
- `Route::One(0)` — `Describe` (the catalog is byte-identical on
  every shard, already exploited).
- `Route::All` — DDL (broadcast; replies must match or it's an
  error).
- `Route::Cross` — multi-shard `Op::Txn` (delivered, SP78–83).
- **`Route::Unsupported`** — *every multi-row read*: `Query`,
  `QueryExpr`, `QueryRows`, `FindBy`, `FindByComposite`,
  `FindRange`, `Aggregate`, `GroupAggregate`, `SelectSorted`,
  `SelectFields`, `Join`, `SeqRead`, and the raw SQL `0xFE` frame.

Rows are sharded by rendezvous hash of `row_key(type_id,id)`, so any
non-point read must fan out to **all** shards and merge. The
`Route::All` DDL path is the structural template (fan out in shard
order, combine replies) but its "all replies must be identical"
rule is wrong for reads — reads return *different* partial data per
shard that must be *merged*, not compared.

## Per-op-family analysis (effort + the honest hard parts)

1. **Filter/scan reads** (`Query`, `QueryExpr`, `QueryRows`,
   `FindBy`, `FindByComposite`, `FindRange`, `SelectFields`) —
   *small.* Fan out the identical op; each shard already returns
   matching rows in the length-prefixed `Got` framing; the router
   concatenates payloads. Determinism: each shard is deterministic
   but the *merge* is only deterministic if the router imposes a
   total order. The 16-byte object id is the record-key prefix and
   is router-visible, so **merged order = by (type_id,object id)**
   is cheap and deterministic. No router-side catalog needed.

2. **`Aggregate`** (COUNT/SUM/MIN/MAX/AVG) — *medium.* Combine
   partials: COUNT/SUM → sum; MIN/MAX → extreme; **AVG cannot be
   combined from per-shard AVGs** — route AVG as SUM+COUNT and
   divide at the router. MIN/MAX combine must respect column kind
   (SP93: CHAR/BYTES lexicographic, U128/I128 signed/unsigned) — the
   router has no catalog, so either push the column kind in the
   reply or fetch a catalog snapshot (the catalog is global ⇒ one
   `Describe`). Start numeric-only; byte/wide MIN/MAX combine as a
   follow-up.

3. **`SelectSorted`** (ORDER BY + LIMIT/OFFSET) — *medium.*
   OFFSET cannot be pushed down; each shard must return up to
   `offset+limit` rows, the router does a **k-way merge** on the
   sort key then applies the global LIMIT/OFFSET. Needs the sort
   key per row (re-derive from the record + a catalog snapshot, or
   have the shard prefix it).

4. **`GroupAggregate`** — *medium.* Per-shard partial groups;
   router merges by group key and re-combines per group (same
   AVG/MIN-kind issues as #2 plus key merge).

5. **`Join`** — *large / explicit non-goal for now.* A row on
   shard A joining a row on shard B is a true distributed join
   (broadcast or shuffle). Reject cross-shard `Join` with a clear
   error initially; revisit as its own project.

6. **Raw SQL text (`0xFE`)** — *medium, depends on 1–4.* The router
   forwards opaque frames today; to route SQL it must **compile**
   it (embed `kessel-sql`) to learn the op + shape. Compilation
   needs the catalog — obtainable as a snapshot (catalog is global).

## Consistency boundary (must be stated, not faked)

A scatter read is **not** a cross-shard consistent snapshot: there
is no global read timestamp / MVCC, so concurrent writes on
different shards can make the merged result a non-linearizable mix
(each shard is individually read-committed). A globally consistent
snapshot needs a read-seq barrier through the sequencer (large) —
**explicit non-goal** for the initial slices; it must be documented
wherever scatter reads are described, exactly as the cross-shard
*transaction* boundaries were.

## Recommended slicing (incremental, each independently shippable)

- **SP-A** — scatter filter/scan reads (#1). Smallest, highest
  value, no router catalog. Oracle: K-shard router result == a
  single-node engine over the same rows, for randomized data, with
  the documented (type_id,id) merge order.
- **SP-B** — scatter `Aggregate` (#2): COUNT/SUM/MIN/MAX numeric;
  AVG as SUM+COUNT at the router. Byte/wide MIN/MAX combine = a
  follow-up.
- **SP-C** — `SelectSorted` k-way merge + global LIMIT/OFFSET (#3).
- **SP-D** — `GroupAggregate` merge (#4).
- **SP-E** — SQL-text routing via a catalog snapshot (#6).
- **Non-goals (documented, not faked):** cross-shard `Join` (#5),
  cross-shard consistent snapshot.

## Recommendation

Do **SP-A first** as the next implementation slice: it is
self-contained, needs no router-side catalog, has a clean
single-node-equivalence oracle, and unlocks the most common
multi-row read across shards. SP-B–E follow; Join and snapshot stay
explicit, documented non-goals until separately scoped.
