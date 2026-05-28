# MVCC & VSR

KesselDB ships **Viewstamped Replication** as its replication protocol
(primary assigns op-number + deterministic timestamp; Prepare → f+1
PrepareOk → Commit; backups apply in op-number order; view-change on
primary timeout; client table for exactly-once retried client batches).
Fixed cluster size (3 or 5); membership reconfiguration is out of scope
for Sub-project 1.

The **MVCC keyspace** is a 28-byte
`type_id(4) ‖ object_id(16) ‖ inverted_commit_opnum(8 BE)` layout
living in the same LSM as the 20-byte legacy keyspace; the inverted
op_number puts the newest version first under `scan_range`. The
`data_row_dispatch(key)` discriminator at the storage layer routes
20-byte user-type data-row keys through MVCC primitives at `u64::MAX`
snapshot (reads) and `op_number` commit (writes) — **no apply-arm
rewrites needed**.

Isolation: snapshot reads, SI write-side, Cahill serializable SSI
(write-skew impossible by construction). GC: `Op::AdvanceWatermark`
is a deterministic op in the apply path. The whole stack is
mechanically verified by TLC across 7 layered TLA+ modules
(`kesseldb-tla/MVCC*.tla` + `Replication.tla`).

Full reference:
[Architecture → Replication (VSR)](overview.md#replication-vsr) and
[Architecture → MVCC](overview.md#mvcc-strategic-tier-s2-sp110sp116).

Mechanically-checked rigor artifacts:
[`kesseldb-tla/`](https://github.com/hassard0/KesselDB/tree/main/kesseldb-tla)
(`Replication.tla` TLC: 528M distinct states / depth 21 / 0 violations).
