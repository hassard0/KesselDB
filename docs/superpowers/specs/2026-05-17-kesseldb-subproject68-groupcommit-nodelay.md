# KesselDB Sub-project 68 — server group-commit + TCP_NODELAY

**Date:** 2026-05-17  **Status:** shipped, measured on vulcan. 167 green.
Two wins; the second was found only by measuring on the *representative*
(Linux) target.

## Part A — server-side group commit

The single-node engine fsynced the WAL per op (durable floor
~2.3K ops/s). Now the engine sets `autosync=false` and the request loop
**drains every queued request, applies them, fsyncs ONCE, then releases
all replies**. Replies are sent only *after* the group fsync, so an op is
acked only when durable (crash-safe; an un-acked op is retried by the
exactly-once client). Ordering/state/digest unchanged — only fsync
*timing* is batched (the determinism/VSR corpus is unaffected; cluster
engine untouched). Single-op latency is unchanged (drain finds nothing →
one fsync, as before); under concurrency one fsync amortises over the
whole batch — essential on EBS-class network storage.

The per-frame logic was extracted into a `compute` closure (replies →
returns) with no behaviour change — proven by the full socket / cluster /
transaction suite staying green.

## Part B — TCP_NODELAY (the real Linux/EC2 bottleneck)

End-to-end measurement on vulcan exposed the actual problem. A new test
`group_commit_concurrent_durable_throughput` (8 concurrent clients ×
1500 durable inserts; asserts all 12 000 present) ran:

- dev box (Windows): **10,598 ops/s**
- vulcan (Linux), DirVfs: **93 ops/s**
- vulcan, even fsync→tmpfs: **97 ops/s**  ← *not* fsync!

~10 ms/op on a no-disk localhost path on Linux is the classic **Nagle +
delayed-ACK** stall on small synchronous request/response sockets.
Windows loopback masked it; **Linux/EC2 is exactly where it bites**.
Fix: `set_nodelay(true)` on every socket — client (`Client`,
`connect_authed`, `ClusterClient`), server-accepted client sockets
(single-node + cluster), and VSR peer sockets (lower consensus latency).

Re-measured on vulcan after the fix:

| vulcan group-commit (8 clients, 12k durable) | before | after |
|---|---|---|
| time | 123.1 s | **6.4 s** |
| throughput | 97 ops/s | **1,870 ops/s (~19×)** |

All 12 000 rows still durable & present (correctness asserted, 167
green).

## Honest reading

- The win that mattered for the stated target (EC2/Linux) was
  `TCP_NODELAY`, not group commit — and it was only found by measuring on
  Linux, not the dev box. Group commit is still correct and is the
  necessary lever for EBS fsync amortisation.
- vulcan's absolute number (1,870 ops/s at 8 *synchronous* clients on a
  busy box with a 95%-full root disk) is gated by real fsync and limited
  client concurrency: group-commit batch size = in-flight ops, so
  throughput rises with concurrency / request pipelining (the next
  lever). Not overclaimed — measured, with the limiting factors named.
- No new config knob: both changes are strictly better (same single-op
  latency, large concurrent gains) and on by default.
