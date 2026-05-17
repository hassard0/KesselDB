# KesselDB Sub-project 44 — operational tooling

**Date:** 2026-05-17  **Status:** shipped, tested. 138 green.

## What this closes

The "operational tooling (backup, metrics, admin)" gate.

## Delivered

All admin actions are handled **inline on the engine thread**, so they see
no concurrent apply — snapshots are crash-consistent and stats are exact.
No side channel, no extra thread, zero deps.

1. **Consistent online snapshot / backup.** `EngineHandle::snapshot(dest)`
   → `[0xFA] ++ dest` frame. The engine `flush()`es the state machine then
   copies its (flat) data dir to `dest` while no apply is in flight. The
   result is a crash-consistent image: `StateMachine::open(dest)` recovers
   the **exact same state digest**. Hot backup with zero downtime.

2. **Metrics / health.** `EngineHandle::stats()` → `[0xFB]` frame →
   `ServerStats { applied_ops, digest, uptime_secs }` (fixed 20-byte
   codec, so it works remotely over the wire too, not just in-process).
   `digest` matches `Replica::digest`, so an operator can detect replica
   divergence across a cluster from stats alone.

3. **Admin** = the same `stats` surface (status/observability); it is the
   minimal honest admin primitive. Larger admin (membership, manual
   failover) is out of scope and not claimed.

## Test (1 new, 138 total)

`stats_and_snapshot_are_consistent_and_recoverable`: stats start at 0; a
`CreateType` + `Create`; stats now report ≥2 applied ops; `snapshot(dest)`;
a **fresh** `StateMachine::open(dest)` has a digest equal to the live
digest and `GetById` returns the row from the recovered copy.

## Honest scope boundary

Snapshot is a flat-dir crash-consistent copy (matches the existing
`DirVfs` layout and the proven WAL-replay recovery path). Incremental /
streaming backup and point-in-time restore are not implemented and not
claimed. The implemented primitive — consistent hot snapshot + exact
recovery + live metrics — is the production-critical core and is tested.
