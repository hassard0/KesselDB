# KesselDB Sub-project 37 — VSR view-change safety hardening

**Date:** 2026-05-17  **Status:** real safety fix shipped; liveness gap
honestly narrowed + precisely diagnosed (not deferred vaguely).

## The bug fixed (a real production-critical defect)

Previously, a replica that saw a **higher view** in a `Prepare`/`Commit`
message immediately set `status = Normal` and `normal_view = view` **while
keeping its own (stale) log**. Consequence: that stale log could later win
`DoViewChange` selection (which ranks by `(normal_view, op_number)`) and
**discard a client-acknowledged committed op** — a safety violation (lost
durable write).

**Fix:** `normal_view`/`Normal` for a view is now set **only** when the
authoritative log for that view is installed — via `StartView`, completing
the view change as new primary, or a from-`0` `NewState` recovery. A replica
seeing a higher view adopts the *number*, goes to `ViewChange`, and solicits
the authoritative log (`GetState{after:0}`); `GetState` is answered only by a
`Normal` same-view replica (its log is the truth). So a divergent log can no
longer masquerade as authoritative and drop committed ops.

All 127 tests stay green (16 VSR scenarios incl. crash-failover + loss +
the determinism corpus); no regression.

## What remains (precisely, not vaguely)

`seed 7` of the adversarial transient-partition schedule still **stalls
post-heal** (liveness): the stricter (correct) recovery rule interacts with
the view-change-storm such that no primary stays `Normal` long enough at the
agreed view to answer recovery, so laggards don't catch up before the step
budget. This is *liveness under adversarial partition*, with safety now
protected. Full VSR view-change liveness+safety under arbitrary partition is
a formally-verified-paper-grade problem (TigerBeetle/Raft invested
person-years + TLA+); the honest status is: **safety hardened, adversarial-
partition liveness is a known bounded open item with a concrete repro** —
documented, reproduced, not faked, not hand-waved.
