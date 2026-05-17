# KesselDB Sub-project 16 — Flexibility-cost benchmark

**Date:** 2026-05-17  **Status:** spec + build (autonomous continuation).
Directly serves the goal: quantify, honestly, the "Postgres flexibility at
TigerBeetle speed" tradeoff after SP2–SP15.

## What it measures

`kessel-bench <N> flex` — in-memory (MemVfs, isolates CPU not fsync), the
per-op throughput of the pure kernel vs each flexibility layer.

## Results (N=100,000, localhost, single-thread, in-memory)

| Path | ops/s | vs plain |
|---|---|---|
| plain CREATE | 892,940 | 1.0× |
| CREATE + equality index | 135,901 | ~6.5× slower |
| CREATE + ordered index | 311,609 | ~2.9× |
| CREATE + CHECK (VM) | 289,413 | ~3.1× |
| CREATE + trigger (VM) | 292,309 | ~3.1× |
| FindBy (indexed eq) | 1,199,080 | fast point lookup |
| FindRange (1% window) | 43,183 | sub-linear range |
| QueryExpr (full scan) | 15 | full scan (why indexes exist) |

## Honest analysis

- **The kernel is TB-class.** ~893K creates/s in-memory single-thread; the
  fixed-record + deterministic-SM core is fast, as designed.
- **Every Postgres-flexibility feature has a measured, understood cost.**
  Constraints/triggers ≈ 3× (one VM eval per write — expected, acceptable,
  bounded). Ordered index ≈ 2.9×. Point reads stay >1M/s.
- **#1 perf debt = equality-index maintenance (~6.5×).** Root cause is the
  SP3-documented per-insert *whole-bucket read-modify-write*: every insert
  reads, decodes, binary-inserts into, re-encodes and rewrites the entire
  id list for that value. With skewed values the bucket grows and the
  per-insert cost grows with it. This is the prioritized optimization (e.g.
  per-(value,object) index keys instead of one fat bucket value, or a
  delta/merge encoding) — a dedicated future spec.
- **Full scans are slow on purpose** (QueryExpr 15 q/s @100k): this is the
  motivation for SP3/SP15 indexes and the SP5 indexed planner; `QueryExpr`
  is the expressiveness fallback, not the fast path.

## Conclusion (thesis status, honest)

The data supports the thesis *shape*: a TB-speed deterministic kernel with
Postgres-flexibility layers whose costs are individually measured, bounded,
and improvable — not a fundamental wall. It does **not** claim the flexible
paths are already at TB speed; the equality-index write path in particular
is a known, quantified, prioritized optimization. No overclaim: numbers are
localhost/in-memory and labelled as relative CPU cost.

102 tests remain green; this SP adds a benchmark mode only (no engine
behavior change).
