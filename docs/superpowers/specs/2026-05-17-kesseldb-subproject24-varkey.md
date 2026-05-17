# KesselDB Sub-project 24 — Variable-length storage Key

**Date:** 2026-05-17  **Status:** spec + build. The enabler for the #1
documented perf debt fix (per-(value,object) index keys, no RMW).

## Change

`kessel-storage::Key` `[u8;20]` → `Vec<u8>` (lexicographically ordered).
WAL frames and SSTable entries now length-prefix the key (u16). `make_key`
still produces the same 20-byte `type_id‖object_id` for data rows, so **all
existing semantics and key orderings are unchanged** — this SP only removes
the fixed-width ceiling so indexes can later use per-(value,object) keys.

## Blast radius (surprisingly small)

Only one `[u8;20]` site existed (the alias). Everything else used `Key`,
`make_key`, or slicing. Fixes: WAL/SSTable key framing, `scan_range` range
bound (owned clone), and a handful of `*k` Copy-derefs → `.clone()` (cache
eviction, sm put/delete-then-cache, storage merge/scan). On-disk format
bumped (fresh stores; tests unaffected).

## Result

115 tests green, workspace builds clean, zero behavior change. Sets up the
future SP that redesigns the equality index to one LSM entry per
(value,object) — O(1) put/delete, prefix-scan reads, no read-modify-write
bucket (closes the SP16 #1 perf debt for real).
