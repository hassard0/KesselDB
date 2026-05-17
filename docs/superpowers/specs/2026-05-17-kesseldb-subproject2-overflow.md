# KesselDB Sub-project 2 — Variable-length overflow store

**Date:** 2026-05-17  **Status:** spec + build (autonomous continuation)
**Builds on:** Sub-project 1 (M0–M4). North Star decision: "fixed-width core
record **+ separate overflow store** for arbitrary TEXT/BLOB."

## Goal

Let an object type have `OverflowRef` fields whose value is arbitrary-length
bytes, while the core record stays fixed-width and the design stays
**deterministic and replication-correct**.

## Design (replication-correct)

The hard constraint: overflow content MUST be part of the replicated op (so
every replica writes identical bytes), and the overflow handle MUST be
derived deterministically (no per-replica counter / RNG / clock).

- **Wire shape (no new Op for writes):** `Create`/`Update` `record` =
  `[fixed-width record][overflow trailer]`. Trailer:
  `[u16 n] then n × ( [u16 field_idx][u32 len][len bytes] )`. Absent trailer =
  no overflow (back-compatible with all Sub-project 1 records).
- **Deterministic handle:** `handle = (op_number << 20) | field_idx`. Unique
  per (op, field), identical on every replica because `op_number` is assigned
  by the VSR primary and replicated. No counter, no allocation state.
- **Overflow keyspace:** reuse the LSM under reserved
  `type_id = 0xFFFF_FFFF`, key id = handle (LE, 16-byte padded). Reuses
  existing crash-safety, recovery, digest, replication — no new storage path.
- **On apply (`Create`/`Update`):** split trailer; for each entry write the
  blob to the overflow keyspace; patch the field's 8-byte `OverflowRef` slot
  in the fixed record with `handle`; store the (now truly fixed-width) record
  under the normal key.
- **Read:** `GetById` returns the fixed record (handles in `OverflowRef`
  fields). New `Op::GetBlob { handle }` returns the blob bytes (or `NotFound`).

## Scope / non-goals (honest)

- **No overflow GC.** `Update` that changes an overflow field orphans the old
  blob (handle is op-derived, so it simply becomes unreferenced). Compaction
  of orphaned overflow blobs is **explicitly deferred** to a later spec and
  documented in STATUS/ARCHITECTURE — not hidden.
- No streaming/chunking: a blob is written/read whole (fine for the kernel;
  chunked large-object IO is future work).
- Determinism digest now includes overflow keyspace automatically (same LSM).

## Tests (TDD)

1. Round-trip: large blob (e.g. 100 KB) via trailer → `GetById` shows handle,
   `GetBlob` returns exact bytes.
2. Handle determinism: same op stream on two SMs → identical handles + digest.
3. Replicated correctness: 3-node VSR cluster with overflow ops → all
   replicas converge (digest equal), `GetBlob` consistent.
4. Back-compat: Sub-project 1 records with no trailer still work unchanged.
5. Update orphans old blob but new blob readable; old handle still resolvable
   (documents the no-GC behavior explicitly via test).
