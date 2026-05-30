# SP-PG-COPY-BULKAPPLY — SP-arc Progress Tracker

Date created: 2026-05-30
**Status: CLOSED — V1 SHIPPED at T4 (2026-05-30).** 100K-row COPY FROM
STDIN on vulcan ingests in 1.929s = **51,840 rows/sec** (median of 3
trials) vs the V1 baseline 285 rows/sec — a **181.9× throughput lift**.
KesselDB COPY is now within ~11× of Postgres 16 (578,034 rows/sec)
on the same workload, vs ~2000× behind pre-arc. TaskList #351 ready
for completion.

Parent SP-arc: SP-PG-COPY (V1 SHIPPED 2026-05-30, commit `3222064`).
Named as deferred V2 follow-up in SP-PG-COPY V1 design spec §9
weak-spot #1 + V1 progress tracker §V2-follow-ups.

Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-sppgcopybulkapply-design.md`
Bench transcript: `docs/superpowers/sppgcopybulkapply-t3-bench-2026-05-30.txt`

## What this SP-arc ships

V1 = "COPY FROM STDIN goes from ~285 rows/sec to ~51,840 rows/sec on
vulcan (181.9× lift) by folding N parsed rows into a single multi-row
`INSERT INTO t (cols) VALUES (...), (...), ...` which kessel-sql
compiles to `Op::Txn { ops: Vec<Op::Create> }` — one apply round-trip
+ one WAL fsync per batch instead of one per row."

## Slice plan (mirrors design spec §7)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1+T2** | Design spec + buffer plumbing + flush + multi-row INSERT synth + per-batch atomicity. CopyInState gains `pending_rows`, `batch_size`, `batch_start_row` fields. `process_copy_data` buffers + flushes at threshold. `finalize_copy_in_success` drains the tail. `finalize_copy_in_failure` drops the buffer. `synthesize_multi_row_insert_sql` joins per-row tuples. NULL-row fallback: any batch with a NULL field flushes per-row to keep the column-omit trick correct. Plus KATs. | **DONE** | `2931158` |
| **T3** | Vulcan 100K-row bench: KesselDB V1 (per-row, batch_size=1) vs KesselDB V2 (batch_size=1024) vs Postgres 16. BENCHMARKS.md row + USAGE.md throughput update. | **DONE** | TBD (this commit) |
| **T4** | Arc closure — STATUS.md row + progress tracker → CLOSED + TaskList #351 ready. | **DONE** | TBD (this commit) |

## T1+T2 — what landed (commit `2931158`)

**5 files changed, +1035 LoC**:

- `crates/kessel-pg-gateway/src/copy/mod.rs` — `COPY_BATCH_SIZE`
  const (1024) + `COPY_BATCH_SIZE_MAX` (65536) + `resolve_copy_batch_size()`
  env-var reader + 3 new CopyInState fields (`pending_rows`,
  `batch_size`, `batch_start_row`) + `new_with_kinds` initializes them.
  **+4 KATs** (default-batch-size lock, env-override-changes-batch-size,
  env-override-handles-invalid-values, default-initial-values).
- `crates/kessel-pg-gateway/src/copy/dispatch.rs` — `process_copy_data`
  now buffers parsed rows; flushes when `pending_rows.len() >= batch_size`.
  `flush_pending_batch` (NEW): drains pending as ONE multi-row INSERT
  (fast path) or per-row dispatch (NULL-fallback). `synthesize_multi_row_insert_sql`
  (NEW): joins per-row tuples for the multi-row INSERT. `CopyDoneOutcome`
  enum (NEW): distinguishes tail-drain success from failure.
  `finalize_copy_in_success` now drains the tail BEFORE emitting
  CommandComplete. `finalize_copy_in_success_no_flush` retained for the
  byte-shape lock. **+7 KATs** (buffer-under-threshold, threshold-flush-during-processing,
  NULL-in-batch-falls-back-to-per-row, engine-error-in-batch-tags-batch-range,
  empty-batch-is-noop, CopyDone-drains-tail). Six existing per-row KATs
  preserved by setting `state.batch_size = 1` (V1-baseline shape).
- `crates/kessel-pg-gateway/src/server.rs` — CopyDone branch now calls
  the new `finalize_copy_in_success(&mut state, engine)` (passes the
  engine so the tail-drain can dispatch). Existing
  `t2_run_session_copy_from_stdin_three_rows_full_sequence` KAT
  updated to assert exactly ONE multi-row INSERT (3 rows fold into 1
  Op::Txn).

**Test counts** (release on vulcan, 2026-05-30 after T1+T2):

| Surface | Before this arc | After T1+T2 | Delta |
|---|---|---|---|
| `kessel-pg-gateway` lib | 587 | 596 | +9 |

Workspace-wide lib tests: 172/172 server lib pass.
`#![forbid(unsafe_code)]` honored across all touched modules;
HTTP/1.1 + WS + binary + PG-wire-Simple + PG-wire-Extended +
PG-wire-COPY surfaces byte-untouched.

## T3 — vulcan bench (2026-05-30)

Setup:
- KesselDB build: `cargo build --release --bin kesseldb --features pg-gateway`
  from commit `2931158`. `CARGO_TARGET_DIR=/tmp/kdb-target-copybulk`.
- Listener: PG-wire `127.0.0.1:5532`.
- Reference DB: Postgres 16 in docker `bench-pg`.
- 100K rows of `(BIGINT id, CHAR(64) name)`, ~50-byte text-format rows.
- Three trials per config, table dropped between trials.

Headline results (median of 3 trials):

| Configuration | 100K-row time | rows/sec | vs V1 | vs Postgres |
|---|---|---|---|---|
| KesselDB V1 (`KESSELDB_COPY_BATCH_SIZE=1`) | ~350s (10K-row extrapolation) | **285** | 1.00× | 0.0005× |
| KesselDB V2 (`KESSELDB_COPY_BATCH_SIZE=1024`) | 1.929s | **51,840** | **181.9×** | 0.090× |
| Postgres 16 (reference) | 0.173s | **578,034** | 2027× | 1.00× |

Acceptance criteria target was ≥ 5000 rows/sec (20× lift). **Achieved
51,840 rows/sec (181.9× lift)** — exceeded target by ~10×.

Full transcript: `docs/superpowers/sppgcopybulkapply-t3-bench-2026-05-30.txt`.

## V2 follow-ups (each its own arc)

- **`SP-PG-COPY-BULKAPPLY-WHOLECOPY`** — whole-COPY atomicity via a
  single Op::Txn covering every row. Gated on engine-side streaming-Txn
  shape (`Op::TxnBegin / TxnAppend / TxnCommit`) landing first.
- **`SP-PG-COPY-BULKAPPLY-NULLBATCH`** — restore the BULKAPPLY win for
  batches containing NULL fields (today they fall back to per-row
  dispatch).

## Closure notes

- TaskList #351 ready for completion.
- HTTP/1.1 + WS + binary + PG-wire-Simple + PG-wire-Extended +
  PG-wire-COPY surfaces byte-untouched.
- `#![forbid(unsafe_code)]` honored across the touched module.
- Zero new external deps.
- USAGE.md §9 (PostgreSQL COPY) updated with the throughput numbers
  + the atomicity divergence + the NULL-row fallback caveat + the
  new V2 follow-up arc names.
- STATUS.md current-capabilities header updated with the Track A.3
  SP-PG-COPY-BULKAPPLY entry.
- BENCHMARKS.md §13 added — full bench transcript with the per-trial
  numbers + the V1-baseline / V2 / Postgres-reference table.
