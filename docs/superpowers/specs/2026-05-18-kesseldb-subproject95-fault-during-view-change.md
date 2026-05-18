# KesselDB Sub-project 95 — multi-node disk-fault-DURING-view-change

**Date:** 2026-05-18  **Status:** shipped. Closes the honest
residual carried since SP88 and explicitly tracked through SP92/SP94:
a disk fault injected *precisely during a view change*, multi-node,
must not lose a client-acked op and the faulted node must reconverge
with the quorum.

## What

A self-contained faulty cluster in the `kessel-vsr` sim tests
(`mod fault`), specialised to `Replica<FaultVfs<MemVfs>>`. The
public `Cluster` stays `MemVfs`-typed — **no API churn / no ripple**
to the existing corpus. `FCluster` mirrors the real routing/run/
quiesce and adds:

- `crash_recover(i)` — the plumbing SP94 unblocked: disarm the
  fault (the crash it modelled already happened), drop the
  **unsynced tail** (`MemVfs::crash`), **reopen the `StateMachine`
  from the faulted disk**, and rejoin with a *blank* VSR layer
  (`Replica::new`). The SP94 apply-cursor (recovered from the WAL /
  manifest watermark) makes re-feeding the node its already-durable
  committed prefix a state no-op, so it cannot double-apply.

## Scenario / verified

`disk_fault_during_view_change_preserves_committed_and_converges`:

1. 3 replicas; warm up `CreateType` + 6 rows — all acked,
   quorum-durable.
2. Crash the primary (replica 0) ⇒ forces a view change to
   replica 1.
3. **Arm a torn WAL write on replica 1 that fires as it applies the
   recovered log during the post-failover view change**, then drive
   24 more client ops.
4. `crash_recover(1)` from its damaged disk; replica 0 stays down ⇒
   live quorum = {1 (recovered, blank VSR log), 2 (survivor)}.
5. Quiesce for catch-up + reconverge.

Asserts: the fault **actually fired**; the recovered replica
converges to **replica 2's exact digest** (SP94 ⇒ no
double-apply/divergence as the quorum re-feeds it via
`GetState`/`NewState`); **all 24** post-failover client ops stayed
acked (no committed op lost, no hang while a node was disk-faulted);
and the whole fault+recovery run is **deterministic** — a second
full run reconverges to the identical digest.

Full workspace regression **202 green**, determinism corpus /
seed-7 intact.

## Arc

SP92 (`FaultVfs` + clean-prefix proof) → SP94 (crash-recovery
apply-cursor + replay-idempotence guard) → **SP95** (this
end-to-end multi-node harness). The hardening-backlog item "byte
corruption injected precisely during a view change" is now closed,
not deferred. Remaining VSR backlog: cluster membership
reconfiguration (still open, honestly listed).
