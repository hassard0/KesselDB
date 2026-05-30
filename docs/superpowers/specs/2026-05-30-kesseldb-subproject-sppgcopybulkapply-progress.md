# SP-PG-COPY-BULKAPPLY — SP-arc Progress Tracker

Date created: 2026-05-30
**Status: OPEN — T1+T2 IN-FLIGHT.**

Parent SP-arc: SP-PG-COPY (V1 SHIPPED 2026-05-30, commit `3222064`).
Named as deferred V2 follow-up in SP-PG-COPY V1 design spec §9
weak-spot #1 + V1 progress tracker §V2-follow-ups.

Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-sppgcopybulkapply-design.md`

## What this SP-arc ships

V1 = "COPY FROM STDIN goes from ~257 rows/sec to ≥ 5000 rows/sec on
vulcan (20× lift) by folding N parsed rows into a single multi-row
`INSERT INTO t (cols) VALUES (...), (...), ...` which kessel-sql
compiles to `Op::Txn { ops: Vec<Op::Create> }` — one apply round-trip
+ one WAL fsync per batch instead of one per row."

## Slice plan (mirrors design spec §7)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1+T2** | Design spec + buffer plumbing + flush + multi-row INSERT synth + per-batch atomicity. CopyInState gains `pending_rows`, `batch_size`, `batch_start_row` fields. `process_copy_data` buffers + flushes at threshold. `finalize_copy_in_success` drains the tail. `finalize_copy_in_failure` drops the buffer. `synthesize_multi_row_insert_sql` joins per-row tuples. NULL-row fallback: any batch with a NULL field flushes per-row to keep the column-omit trick correct. Plus KATs. | DONE | TBD |
| **T3** | Vulcan 100K-row bench: KesselDB-V2 vs KesselDB-V1 vs Postgres. BENCHMARKS.md row + USAGE.md throughput update. | DONE | TBD |
| **T4** | Arc closure — STATUS.md row + progress tracker → CLOSED + TaskList #351 ready. | DONE | TBD |

## Closure notes

- TaskList #351 ready for completion.
- HTTP/1.1 + WS + binary + PG-wire-Simple + PG-wire-Extended +
  PG-wire-COPY surfaces byte-untouched.
- `#![forbid(unsafe_code)]` honored across the touched module.
- Zero new external deps.
