# SP-PG-COPY-BULKAPPLY — batched `Op::Txn` fold for COPY FROM STDIN

> Status: **V1 — design + implementation + bench**. The single arc that
> turns the SP-PG-COPY V1 "make pg_dump / sysbench work at all"
> milestone into the "throughput-competitive" milestone.
>
> SP-arc parent: SP-PG-COPY (V1 SHIPPED 2026-05-30, commit `3222064`).
> Named as a deferred V2 follow-up in §9 weak-spot #1 of the V1 design
> spec + listed in the V1 progress tracker §V2-follow-ups.
>
> Companion progress tracker:
> `docs/superpowers/specs/2026-05-30-kesseldb-subproject-sppgcopybulkapply-progress.md`
>
> Date: 2026-05-30

## §1. Context — why V1 ships at ~257 rows/sec

SP-PG-COPY V1 dispatches one synthesized `INSERT INTO <table> [(cols)]
VALUES (...)` per parsed COPY row through `dispatch::dispatch_query`.
Each dispatch:

1. Compiles the SQL via `kessel-sql::compile_sql`.
2. Submits one `Op::Create` to the apply thread.
3. Blocks on the apply thread until the WAL fsync returns.
4. Returns to the COPY parser, which advances to the next row.

The apply thread + WAL fsync are the cost center — every row pays one
full apply round-trip + one fsync. V1's measured 257 rows/sec on
vulcan (1000-row `(BIGINT, CHAR(32))` table) is dominated by step 3.

What V1 already proves works:

- **kessel-sql understands multi-row INSERT VALUES** — it compiles
  `INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')` to a
  single `Op::Txn { ops: vec![Op::Create, Op::Create, Op::Create] }`.
  Reference: `crates/kessel-sql/src/lib.rs` lines 1183-1260 — the
  multi-tuple parser already produces `Op::Txn` when the tuple count
  is > 1.
- **Op::Txn is all-or-nothing** — per the kessel-proto comment
  (`lib.rs::Op::Txn`): "apply every inner op all-or-nothing. Any
  failure rolls the whole batch back. Replicated as one op." This is
  exactly the PG-compatible COPY atomicity SP-PG-COPY V1's weak-spot
  #3 documents as a divergence.

The V2 lift is therefore "just" a buffering change in the gateway —
instead of dispatching one INSERT per row, buffer N parsed rows in
the CopyInState and dispatch one multi-row `INSERT INTO t (cols)
VALUES (...), (...), ..., (...)` per batch. The engine + kessel-sql
sides are byte-untouched.

## §2. Scope

### §2.1. V1 in-scope

1. **Buffer parsed rows in `CopyInState`.** Add a
   `pending_rows: Vec<Vec<Option<Vec<u8>>>>` field carrying the
   parsed-but-not-yet-flushed rows.
2. **Configurable batch size.** `COPY_BATCH_SIZE: usize = 1024` const
   (overridable via `KESSELDB_COPY_BATCH_SIZE` env at server start).
   1024 picked as the V1 default after the §4 sizing analysis.
3. **Flush triggers.**
   - **Threshold:** when `pending_rows.len() >= COPY_BATCH_SIZE`,
     synthesize one multi-row INSERT, dispatch it, clear the buffer.
   - **CopyDone:** at the CopyDone-finalize call, flush whatever
     remains (0..COPY_BATCH_SIZE) as one final multi-row INSERT.
   - **CopyFail:** drop the buffer without flushing (PG semantics:
     CopyFail aborts the COPY).
4. **Multi-row INSERT synthesis.** Reuse the existing
   `synthesize_insert_sql` per-row helper for the literal-rendering
   logic, factored to produce one VALUES tuple at a time; the new
   `synthesize_multi_row_insert_sql` then joins them with the shared
   column list. The kessel-sql multi-row INSERT path already
   compiles this to `Op::Txn` (lib.rs lines 1245-1260).
5. **Per-batch atomicity.** Each batch is one `Op::Txn` and therefore
   atomic on the engine side — a single constraint failure inside a
   batch rolls back the entire batch. Documented divergence vs PG:
   PG is whole-COPY atomic (an implicit transaction wraps the whole
   COPY), V1 is per-batch atomic. Mitigation: a future V2-of-V2 (call
   it `SP-PG-COPY-BULKAPPLY-WHOLECOPY`) could buffer the entire COPY
   and dispatch as one giant Op::Txn, but the memory cap risk + the
   1024-row-batch already-existing "rows 1-1024 stay committed even
   if row 1025-2048 fails" non-atomicity vs PG is the trade-off we
   take in V1 to keep the buffer bounded.
6. **NULL-in-batch handling.** A NULL field today is rendered by
   omitting the column from both the column list and the VALUES
   tuple. Multi-row INSERT requires the same column list for every
   tuple (kessel-sql enforces this — see `cols.len() != raw.len()`
   check at lib.rs line 1201). The V1-BULKAPPLY pragmatic choice:
   **the batch's INSERT carries the full column list**, and a NULL
   field is rendered via an explicit `NULL`-token marker — except
   kessel-sql doesn't have a NULL literal in VALUES. Workaround: V1
   splits a batch when any row contains a NULL — rows with NULLs are
   flushed as their own per-row sub-batches (one-row Op::Create) so
   the V1 column-omit trick still applies. This recovers the BULKAPPLY
   throughput win for ALL-NON-NULL batches (the sysbench / pg_dump
   common case) while preserving correctness for nullable schemas.
7. **Error semantics preserved.** A field-count mismatch (22023) or
   engine constraint violation (23xxx) still aborts the COPY with
   the row-number-tagged message. The row number is now the
   **first-row-in-failing-batch** rather than the exact row, with a
   "(in batch starting at row N)" tag for transparency.
8. **No-extra-deps invariant preserved.** Pure buffer-side change in
   `kessel-pg-gateway::copy::dispatch`. `#![forbid(unsafe_code)]`.
9. **Wire-byte-untouched.** PG-wire-Simple + PG-wire-Extended +
   HTTP/1.1 + WS + binary surfaces unchanged. COPY is additive.

### §2.2. V1 out-of-scope (named V2+ follow-ups)

- **`SP-PG-COPY-BULKAPPLY-WHOLECOPY`** — full-COPY atomicity via a
  single Op::Txn covering every row. Cap risk: a 100M-row COPY
  buffers all 100M rows in memory before dispatch. Needs an engine-
  side streaming Op::TxnBegin / Op::TxnAppend / Op::TxnCommit shape
  first.
- **`SP-PG-COPY-BULKAPPLY-NULLBATCH`** — restore the BULKAPPLY win
  for batches that contain NULLs. Needs either (a) a kessel-sql NULL
  literal in VALUES, or (b) a typed-binding `Op::TxnInsertRows`
  shape that bypasses SQL synthesis entirely.
- **`SP-PG-COPY-BULKAPPLY-BINARY`** — combine BULKAPPLY with binary-
  format COPY (SP-PG-COPY-BIN). Lands together once both ship.

## §3. Buffer + flush mechanics

```rust
pub struct CopyInState {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub column_count: u16,
    pub column_kinds: Vec<kessel_catalog::FieldKind>,
    pub carry: Vec<u8>,
    pub rows_ingested: u64,
    /// NEW (V1-BULKAPPLY): parsed-but-not-yet-flushed rows. Drained
    /// when the buffer reaches `batch_size` or at CopyDone.
    pub pending_rows: Vec<Vec<Option<Vec<u8>>>>,
    /// NEW: the per-session batch size (resolved once at COPY start
    /// from the env var or the default `COPY_BATCH_SIZE`).
    pub batch_size: usize,
    /// NEW: the row number of the first row in the current pending
    /// batch (1-based). Used for error-message row tagging.
    pub batch_start_row: u64,
}
```

`process_copy_data` flow becomes:

```text
for each complete line in (carry ++ data):
  if line == "\\.": continue
  fields = parse_text_row_bytes(line)
  pending_rows.push(fields)
  if pending_rows.len() >= batch_size:
    flush_pending_batch(state, engine)  // may surface CopyDataOutcome::Failed
```

`flush_pending_batch` synthesizes either a multi-row INSERT (if every
pending row is all-non-NULL) or a sequence of per-row INSERTs (mixed
NULL/non-NULL — the V1 fallback path), then dispatches through
`dispatch::dispatch_query`. The dispatch result is inspected for an
ErrorResponse; if present, the COPY aborts and the bytes go to the
caller.

`finalize_copy_in_success` calls `flush_pending_batch` first (to
drain the tail), then emits `CommandComplete("COPY N") + RFQ`.

`finalize_copy_in_failure` (CopyFail received) simply drops the
buffer — the rows in `pending_rows` are NOT committed (PG semantics).

## §4. Batch-size choice — why 1024

Choice trade-off:

| batch_size | bytes/batch (avg row 50B) | apply round-trips per 1M rows | per-batch fsync cost | atomicity granularity |
|---|---|---|---|---|
| 1 | 50 B | 1,000,000 | dominated by fsync | exact per-PG (V1 baseline) |
| 64 | 3 KiB | 15,625 | 64× win | rows 1-64 atomic |
| 1024 | 50 KiB | 977 | 1024× win | rows 1-1024 atomic |
| 4096 | 200 KiB | 244 | 4096× win | rows 1-4096 atomic |
| 16384 | 800 KiB | 62 | 16384× win | rows 1-16384 atomic |

Above 1024 the per-batch synthesis cost (SQL string allocation +
multi-row Op::Txn allocation) starts to compete with the fsync win
and the buffer RSS-per-connection grows. 1024 sits in the knee:

- ~50 KiB pending buffer per COPY-active connection (negligible for
  any realistic concurrent-COPY count).
- ~1000× fewer fsyncs than V1 — the expected throughput lift bracket
  is 10-50× depending on row-size + disk fsync latency.
- Per-batch atomicity granularity matches the "tolerable partial
  ingestion on hard failure" shape a sysbench prepare phase already
  tolerates.

Configurable via `KESSELDB_COPY_BATCH_SIZE` env at server start so
an operator with a different fsync latency profile can tune. V1
clamps to `[1, 65536]` — below 1 = invalid, above 65536 = pending-
buffer RSS exceeds spec §5 memory bounds.

## §5. Memory bounds

- `pending_rows` Vec<Vec<Option<Vec<u8>>>>:
  worst case 65536 rows × per-row size cap. Each row is bounded by
  the parser's per-frame cap (16 MiB / `MAX_COPY_DATA_BUFFER`).
  Realistic upper bound for a 1024-row batch with 50-byte rows:
  ~50 KiB.
- The carry buffer is unchanged — capped at 16 MiB per the V1 spec
  §5.
- Per-connection RSS overhead vs V1: ~50 KiB for default batch_size.

## §6. Atomicity model — documented per-batch divergence vs PG

| Behaviour | V1 (per-row) | V1-BULKAPPLY (per-batch) | PostgreSQL (whole-COPY) |
|---|---|---|---|
| Row 500 of 1000 NOT NULL violation | rows 1-499 committed, row 500 errors, COPY aborts | rows 1-`batch_start-1` committed, batch starting at `batch_start..=N` rolled back, COPY aborts | nothing committed, COPY aborts |
| Concurrent reader sees partial batch? | yes — sees rows 1..499 mid-COPY | NO during the failing batch; sees prior batches | NO (PG sees nothing until COPY commits) |
| Crash mid-COPY | last successfully-applied row is durable | last successfully-applied BATCH is durable | nothing durable |

V1-BULKAPPLY is closer to PG than V1-baseline but still not identical.
The closing-the-gap arc is named `SP-PG-COPY-BULKAPPLY-WHOLECOPY` per §2.2.

## §7. Task decomposition

| T# | Scope | KAT delta |
|---|---|---|
| **T1** | Design + buffer plumbing — this doc + `CopyInState` fields + `COPY_BATCH_SIZE` const + env-var resolution helper + buffer-append in `process_copy_data` (does NOT yet flush — rows still dispatched per-row to keep the diff isolated). | +3 |
| **T2** | Multi-row INSERT synthesis + flush path. `synthesize_multi_row_insert_sql` (NEW). `flush_pending_batch` (NEW). Plumbing in `process_copy_data` to call `flush_pending_batch` at the threshold; in `finalize_copy_in_success` to drain the tail. NULL-row fallback: rows with any NULL field bypass the batch and dispatch per-row. | +6 |
| **T3** | Vulcan 100K-row bench: KesselDB-V2 vs KesselDB-V1 vs Postgres. BENCHMARKS.md row added. | (bench) |
| **T4** | Arc closure — USAGE.md §9 expansion + STATUS.md row + progress tracker → CLOSED + TaskList #351 ready. | (docs) |

T1 + T2 ship in one commit (the design + buffer + flush are tightly
coupled). T3 + T4 ship in their own commits.

## §8. Acceptance criteria

1. COPY of 100 rows (all non-NULL) emits exactly 1 Op::Txn (multi-row
   INSERT with 100 tuples) to the engine.
2. COPY of 2048 rows emits exactly 2 Op::Txns (1024-row + 1024-row).
3. COPY of 1024 rows emits exactly 1 Op::Txn.
4. COPY of 0 rows emits CommandComplete COPY 0 + RFQ (no Op::Txn).
5. COPY of 1000 rows where row 500 has a constraint violation: ALL
   1000 rows in that batch roll back; COPY aborts with the row-
   tagged error message including "in batch starting at row 1".
6. COPY of 100K rows of `(BIGINT, CHAR(64))` on vulcan ingests in
   ≤ 20s wall-clock (target: ≥ 5000 rows/sec — 20× the V1 baseline).
7. CopyFail mid-stream drops the pending buffer without flushing —
   no partial-batch commit.
8. NULL-row fallback: a COPY where every row has at least one NULL
   field still works correctly (each row dispatches as its own per-
   row INSERT — same throughput as V1 baseline, but correct).
9. `KESSELDB_COPY_BATCH_SIZE=256` env override changes the flush
   threshold from 1024 to 256.
10. `#![forbid(unsafe_code)]` honored across the touched module.
11. PG-wire-Simple + Extended + HTTP/1.1 + WS + binary surfaces
    byte-untouched.
12. CI green at every commit.

## §9. Self-review — weak spots

1. **NULL-row fallback collapses to per-row dispatch.** Any batch
   that contains even one NULL field falls back to dispatching the
   entire batch row-by-row through Op::Create. For a sysbench / pg_dump
   table with mostly-non-NULL data this is fine, but for a CRM-style
   table with many nullable columns the BULKAPPLY win evaporates.
   Lifts in `SP-PG-COPY-BULKAPPLY-NULLBATCH` (named §2.2).
2. **Per-batch atomicity vs PG's whole-COPY atomicity.** Documented
   divergence — lifts in `SP-PG-COPY-BULKAPPLY-WHOLECOPY`.
3. **Multi-row INSERT SQL string can get long.** 1024 rows × ~50
   bytes/tuple = ~50 KiB SQL string per batch. kessel-sql parses
   this without problem (the tuple parser is a tight loop) but
   allocates 50 KiB per batch. Lifts in V2-typed-binding shape.
4. **Pending-buffer doesn't backpressure the client.** A client
   that streams faster than the apply thread can absorb causes the
   pending buffer to fill — but every flush blocks the gateway-side
   loop anyway, so the backpressure naturally lands on the client
   via TCP. V1 doesn't add explicit flow control.
5. **Error row number is approximate.** "in batch starting at row N"
   is less precise than V1's per-row tag. The engine's Op::Txn
   error doesn't carry a per-op index. Lifts in V2 if Op::Txn gains
   `failed_op_index`.
6. **batch_size of 1 produces an Op::Txn { ops: [Op::Create] }** —
   wasteful framing. Mitigation: when batch is exactly one row, fall
   back to the V1 per-row Op::Create path (no Op::Txn wrap). Already
   handled by kessel-sql's lib.rs line 1255-1260 ("if ops.len() == 1,
   ops.pop().unwrap() else Op::Txn { ops }").
7. **A flush that fails leaves `pending_rows` non-empty.** The
   process_copy_data path swaps state to Idle on failure, so the
   leak doesn't outlive the COPY — but the field stays carrying the
   bytes briefly. Cleared by setting `pending_rows = Vec::new()` on
   the failure path.
8. **The COPY 0 case shouldn't allocate a multi-row INSERT.** Guard:
   if `pending_rows.is_empty()`, return early without synthesis.
9. **`CopyInState::new` and `new_with_kinds` need fresh field
   defaults.** Both constructors initialize `pending_rows = Vec::new()`,
   `batch_size = COPY_BATCH_SIZE` (or env override), `batch_start_row = 1`.
10. **No psql 16.14 smoke after the change** — T3 covers the
    throughput bench on vulcan but doesn't re-run the full T4 smoke
    transcript from V1. The existing dispatch / round-trip KATs
    cover the wire-level correctness; the bench validates the
    headline.

## §10. References

- SP-PG-COPY V1 design spec:
  `docs/superpowers/specs/2026-05-30-kesseldb-sppgcopy-design.md`
- SP-PG-COPY V1 progress tracker:
  `docs/superpowers/specs/2026-05-30-kesseldb-subproject-sppgcopy-progress.md`
- kessel-sql multi-row INSERT → Op::Txn: `crates/kessel-sql/src/lib.rs`
  lines 1145-1260.
- kessel-proto Op::Txn: `crates/kessel-proto/src/lib.rs` line 72-74.
- PostgreSQL §SQL-COPY semantics:
  https://www.postgresql.org/docs/current/sql-copy.html
