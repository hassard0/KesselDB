# SP-Hash-Agg-Tune — progress tracker

Drives down the SP-Hash-Agg V1 serial-prefix cost (Vec<Arc<[u8]>>
pre-materialisation + Arc-wrap pass) that bounded per-query lift to
1.46-1.79× instead of the 4× modelled target. Streaming
producer-channel-workers replaces V1's two-phase pre-collect+partition
shape, so workers fold rows AS the producer iterates the scan output.

Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-sphashaggtune-design.md`.

**Arc status: IN-FLIGHT — T1 design landed.**

---

## T1 — design + scaffold + streaming refactor [PENDING]

Commits:
- (pending) docs(perf): SP-Hash-Agg-Tune T1 — design + progress tracker +
  streaming producer-channel-workers refactor

## T2 — streaming-equivalence KATs [PENDING]

## T3 — vulcan TPC-H Q1+Q6 sweep + BENCHMARKS.md update [PENDING]

## T4 — arc closure [PENDING]
