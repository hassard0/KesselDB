# SP-PG-NULL-INT-RENDER — omitted/explicit-NULL nullable column must render as SQL NULL

**Status:** CLOSED
**Date:** 2026-06-03
**Arc:** Data-correctness — a base-table `SELECT` of a row whose nullable column
was omitted at INSERT must render as SQL NULL over the PG wire, NOT `0`/empty.

---

## DIAGNOSIS (lead) — which layer was wrong + root cause

The bug is in the **SELECT render layer**, specifically the **non-sorted
projection-list path** (`SELECT col FROM t`), NOT in INSERT lowering, NOT in the
on-disk record encoding, and NOT in `SELECT *`.

I traced an omitted-nullable-int INSERT → stored bytes → SELECT render across
all three candidate layers:

1. **INSERT lowering (`crates/kessel-sql/src/lib.rs`)** — CORRECT. When a column
   is omitted from the INSERT column list, the lowering loop pushes
   `Value::Null` for a nullable field (and errors for a NOT NULL field with no
   default). See the `else if f.nullable { values.push(Value::Null) }` arm.

2. **Row encoding / NULL bitmap (`crates/kessel-codec/src/lib.rs`)** — CORRECT.
   `encode()` sets the null-bitmap bit for `Value::Null` (`set_null(&mut bitmap, i)`)
   and rejects NULL in a non-nullable column. The record layout is
   `[schema_ver u32][field_count u16][null bitmap 8B][field data…]`; `decode()`
   honors the bitmap and yields `Value::Null`. So the row's NULL state genuinely
   exists in the stored bytes — which is exactly why FK enforcement correctly
   SKIPS the FK check for the omitted column.

3. **SELECT render (`crates/kessel-pg-gateway/src/dispatch.rs`)** — SPLIT:
   - **`SELECT *`** (`emit_data_rows` → `decode_record`): CORRECT. It reads the
     8-byte null bitmap at record offset 6 and emits `None` (PG NULL, i32 `-1`
     length sentinel) for a NULL field. This path was already faithful.
   - **Projection list `SELECT col`** (`emit_projected_rows`): **THE BUG.** A
     non-sorted projection lowers to `Op::SelectFields`, whose row stream is the
     selected columns' RAW fixed-width bytes concatenated in projection order —
     **with NO null bitmap** (`crates/kessel-sm/src/lib.rs` `Op::SelectFields`
     copies `rec[off..off+w]` directly, never consulting the bitmap). At the
     gateway, `emit_projected_rows` therefore decodes a NULL field's stored zero
     bytes as the value `0` (int) / empty (text) — indistinguishable from a real
     0. This is the documented `SP-PG-SQL-PROJ-NULL` gap, and it is the exact
     `0`-for-NULL the FK-enforce smoke observed (`SELECT id FROM child …`).

**Root cause:** the engine's narrow projection stream (`Op::SelectFields`)
carries no per-row null mask, so the gateway's `emit_projected_rows` cannot tell
a stored-zero NULL from a real 0.

A secondary gap surfaced while building the regression smoke: the INSERT VALUES
parser did **not** accept the bare SQL `NULL` keyword (`VALUES (1, NULL)`) — it
errored with "expected value". So an EXPLICIT NULL could not even be inserted.

---

## FIX (root, render-layer — no storage/wire change)

### 1. Projection NULL fidelity via `SELECT *` re-projection
Rather than add a null mask to the `Op::SelectFields` wire stream (which would
change the bytes the determinism oracles compare), the gateway now renders a
**non-sorted projection by re-issuing the read as `SELECT *`** — which returns
FULL records (`Op::Select`, carrying the on-disk null bitmap) — and re-projects
the requested columns in the gateway through the bitmap-honoring `decode_record`
/ `emit_projected_from_full_records` path. This is the SAME NULL-faithful
machinery the sorted-projection branch already used.

- New `kessel_sql::select_projection_to_star(sql)` rewrites `SELECT c1,c2 FROM t
  [WHERE …]` → `SELECT * FROM t [WHERE …]`, preserving the FROM clause onward
  verbatim (token-boundary-aware, quote-safe FROM finder). Returns `None` for
  anything that is not a plain single-table projection list (aggregates / JOIN /
  `SELECT *`), so no other dispatch path is touched.
- `emit_projected_from_full_records` was made robust to BOTH row-stream shapes —
  the length-prefixed list (`Op::Select` / `Op::SelectSorted`) AND a single bare
  record (`Op::GetById`, which a `SELECT * … WHERE id = N` can compile to).
- On any failure (rewrite returns None, re-issued read isn't `Got`, or the
  full-record re-projection errors) the gateway falls back to the legacy narrow
  `emit_projected_rows` — never worse than V1.

**Determinism:** PURE render-layer change. No `Op`/record/wire format changed, so
`large_seed_corpus_is_deterministic_and_converges`,
`partition_corpus_is_deterministic`,
`jepsen_3replica_partition_converges_byte_identical`, and the `sharded_engine` /
`read_pool` oracles over the `SelectFields` stream are byte-untouched.

### 2. Explicit `NULL` literal in INSERT VALUES
Added a `Lit::Null` variant. The VALUES parser accepts the bare `NULL` keyword
(lexed as `Tok::Ident("NULL")`). In the values-building loop an explicit `NULL`
for a nullable column stores `Value::Null` (bitmap bit set — byte-identical to an
omitted nullable column, so deterministic) and a NOT NULL column / the `id`
pseudo-PK rejects it cleanly (`23502`-style message / "`id` must not be NULL").

## Generic across kinds
The fix is generic: the re-projection path decodes ANY column kind via the
record's null bitmap, so nullable TEXT/CHAR, numeric, etc. all render NULL
faithfully — not int-only. Verified by the smoke's `text_omitted_null` stage.

## Back-compat
- `SELECT *` render unchanged (was already correct).
- A NOT NULL / PK column still reads its real value (the bitmap bit is clear).
- Sorted projection + JOIN + aggregate render paths untouched.

## Remaining limitation (named follow-up)
None structural for the supported projection shapes. The old
`SP-PG-SQL-PROJ-NULL` follow-up (add a null mask to the raw `Op::SelectFields`
stream) is now effectively MOOT for the gateway render — the gateway no longer
relies on that stream for NULL fidelity. The narrow `emit_projected_rows` remains
only as a defensive fallback. If a future caller consumes the raw
`Op::SelectFields` stream directly and needs NULL fidelity there, that engine-
level mask is still the follow-up — but it is no longer on the PG-wire path.
