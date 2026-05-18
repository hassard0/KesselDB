# KesselDB Sub-project 76 — overflow-blob GC

**Date:** 2026-05-18  **Status:** shipped. 176 green. Closes the
deferred-GC seam documented since SP2.

## The leak

Variable-length values live in a reserved overflow keyspace
(`0xFFFF_FFFF`), keyed by a deterministic handle
(`(op_number << 20) | field_idx`); the fixed record stores the handle
in the `OverflowRef` field slot. An `UPDATE` materialises **fresh**
handles, and `DELETE` removed the row but not its blobs — so old/last
blobs were unreachable but never freed (explicitly documented as a
known leak, not hidden).

## Reclamation — precise, at mutation time, deterministic

No scan, no epoch sweep. Two small helpers:

- `overflow_handles(type_id, rec)` — the handles a record references
  (the 8-byte value at each `OverflowRef` field offset; 0 = none).
- `reclaim_overflow(op, freed)` — `storage.delete` each freed handle
  key.

Hooks:

- **`UPDATE`**: after the new record is stored, free `old_handles −
  new_handles` (set difference, so a handle the new record still
  references is *kept*; in practice materialise always assigns fresh
  handles, so the superseded blob is freed).
- **`DELETE`**: for every row in the ON DELETE closure, free its
  handles inside the delete's own transaction (atomic with the row
  removal).

Handles are op-number-derived, so every replica deletes exactly the
same keys — deterministic and replication-safe. It is a real keyspace
mutation (the digest reflects the freed blobs), applied identically on
all nodes; the determinism / VSR partition corpus (incl. seed 7) is
unchanged.

## Verified

`overflow_blobs_are_reclaimed_on_update_and_delete` (replaces the old
"no GC — documented" test, a documented behaviour change, not a masked
regression): after an `UPDATE` the superseded blob is `NotFound` and
the new one readable; after `DELETE` the row's blob is `NotFound`; two
identical histories produce the same digest. Full workspace regression
clean.

## Honest boundary

Reclamation is *reference-precise at the mutating op* (the common,
correct case), not a periodic global mark-sweep — a blob can still be
orphaned only by a non-mutation path, and there is none (handles are
reachable solely through their row). No compaction of the overflow
keyspace itself (deleted handle keys tombstone like any other key and
are reclaimed by normal LSM compaction).
