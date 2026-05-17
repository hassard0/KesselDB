# KesselDB Sub-project 3 — Equality secondary indexes

**Date:** 2026-05-17  **Status:** spec + build (autonomous continuation)
**Builds on:** SP1 (M0–M4) + SP2. North Star access-path decision included
secondary indexes; this slice delivers **equality** indexes only. Range scans
and the multi-index intersection planner remain a later spec (kept out so this
slice is fully correct and tested tonight).

## Goal

`CreateIndex(type_id, field_id)` builds an equality index (deterministically
backfilling existing rows). `FindBy(type_id, field_id, value)` returns the
16-byte object ids of every row whose indexed field equals `value`. Indexes
are maintained on Create/Update/Delete and are replication-correct.

## Design

- **Catalog:** `ObjectType.indexes: Vec<u16>` (indexed `field_id`s),
  persisted in the replicated catalog.
- **Index keyspace:** storage type-slot `0xFFFE0000 | (user_type & 0xFFFF)`;
  key id = `field_id(2) ++ value_digest8 ++ [0;6]`. The entry value holds
  **digest-collision-safe buckets**: per distinct full field value, a sorted
  set of 16-byte object ids. So a digest collision just adds another bucket
  under the same key; correctness never depends on the digest being unique.
- **Determinism:** every index key and byte is derived from committed data +
  `field_id` (not RNG/clock/counter); object-id sets are kept sorted, so two
  replicas build a byte-identical index keyspace. Covered by the state
  digest, so existing convergence/determinism tests transitively guard it,
  plus an explicit replicated test.
- **Storage support:** added `Storage::scan_range(lo,hi)` (sorted merge over
  memtable + SSTables) — used for the `CreateIndex` backfill (scan the type's
  contiguous key range) and reusable by future range queries.
- **Maintenance:** Create adds the new row to each index; Update diff-updates
  only changed indexed fields (remove old bucket entry, add new); Delete
  removes from all indexes. Index reads the *fixed* record bytes at the
  field's layout offset (raw-byte equality == value equality for fixed-width
  fields). `OverflowRef` fields are rejected for indexing (would index the
  handle, not content).

## Scope / non-goals (honest)

- **Equality only.** No range (`<`,`>`,`BETWEEN`) and no multi-index
  intersection planner — explicitly deferred (next spec).
- **Read-modify-write per index op.** Correct but not yet throughput-optimized
  (a hot indexed key serializes writes to it). Optimization is later perf work
  and is documented, not hidden.
- Indexing is per fixed-width field value; composite/expression indexes are
  future work.

## Tests

`equality_index_find_by_after_create_and_backfill` (incl. idempotent
re-create + deterministic backfill of pre-existing rows),
`index_maintained_on_update_and_delete`,
`index_is_deterministic_across_instances` (digest equality over a 600-op
random stream), `scan_range_is_sorted_correct_across_levels` (storage),
`secondary_index_replicates_and_converges` (3-node VSR). 56 tests total green.
