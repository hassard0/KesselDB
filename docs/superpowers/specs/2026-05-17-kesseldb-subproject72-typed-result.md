# KesselDB Sub-project 72 — self-describing typed result (JOINs render)

**Date:** 2026-05-17  **Status:** shipped, e2e-verified. 172 green.
Closes the boundary SP71 named: `--json` (and text) now covers JOINs,
not just whole-row `SELECT *` / projections.

## The gap

`Op::Join` returned opaque concatenated records with **no schema**, so
the client could not decode them — every JOIN printed `GOT N bytes`.
Projections render only because the client re-`DESCRIBE`s the single
source table; a JOIN has no single table to describe.

## Design — a result that describes itself

`Op::Join` now emits a **typed result**:

```
[b"KTR1"][u32 deflen][type def][ [u32 reclen][record] ]*
```

The embedded type def (standard `kessel_catalog::encode_type_def`) is a
synthetic combined schema: every left column as `<lt>.<col>` then every
right column as `<rt>.<col>`, same kinds/order. Each joined record is a
**properly re-encoded** combined record.

Crucial correctness point found by e2e: a joined row is **not** raw
`left_bytes ++ right_bytes`. Every stored record has its own header +
8-byte null bitmap (`[schema_ver][field_count][bitmap][data]`), so a
naive concat decodes the right-hand columns as garbage/NULL (the first
cut did exactly this — the table rendered but every right column was
`NULL`). The fix decodes each side against its own type, concatenates
the *values*, and re-encodes against the combined type — one valid
record with one header/bitmap. Correctness verified end-to-end (real
values, not NULLs).

## Client — generic, reuses the tested decoder

`kessel-client` gained `render_typed_result` / `render_typed_result_json`
(+ `TYPED_RESULT_MAGIC`): detect the magic, split off the embedded type
def, then call the **existing** `render_rows` / `render_rows_json`. So a
JOIN renders through exactly the same code path (and tests) as a plain
table — no JOIN-specific rendering, and the envelope is reusable for any
future op that wants to be self-describing. The CLI tries it before the
`select_*` paths, in both text and `--json`.

## Honest scope & boundaries

- Closes JOIN specifically. Whole-row `SELECT *` and projections already
  rendered (client knows the schema); aggregates are scalars. So
  "every query shape" is now covered for display.
- `kessel-sm` gained an **internal** dependency on `kessel-codec`
  (already a workspace crate, zero external deps — same family as the
  existing `kessel-catalog` dep). The zero-external-dependency North
  Star is unaffected.
- The 8-byte null bitmap caps a combined row at 64 columns; a JOIN whose
  two tables exceed that together would error at encode (clean
  `SchemaError`, not corruption) — documented, not silently wrong. Wider
  bitmaps are a separate concern if ever needed.
- Read-op only: not part of the replicated state digest; the
  determinism / VSR partition corpus (incl. seed 7) is unchanged. The
  JOIN-shape test was updated to assert the new self-describing contract
  (a behaviour change to a query *result*, documented — like the SP62
  policy-change test update).

## Tests

`kessel-sql::inner_equi_join` rewritten to decode the embedded schema
(asserts columns `usr.uid, ord.owner, ord.amt` and every record decodes
against the combined type, 3 rows). `kessel-client::
typed_result_renders_generically_text_and_json` (aligned table + exact
JSON + graceful `None` on non-typed input). E2E vs a live server:
JOIN text table with correct values, `--json` objects with correct
values, and a zero-match JOIN (clean header, `(0 rows)`).
