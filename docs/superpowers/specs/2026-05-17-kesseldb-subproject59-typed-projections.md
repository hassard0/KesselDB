# KesselDB Sub-project 59 — typed projection rendering

**Date:** 2026-05-17  **Status:** shipped, tested, e2e-verified. 156 green.
Completes the "every result is readable" DX promise (whole-row + projection).

## The gap

SP53 made `SELECT *` print a real aligned table; explicit projections
(`SELECT c1, c2 …`) still printed `GOT N bytes`. SP59 closes that — pure
client-side, zero engine/protocol/determinism change.

## Delivered

- **`kessel_codec::value_from_raw(kind, raw)`** — the per-field raw→Value
  core of `decode`, now public. `decode` was refactored to call it (a
  behaviour-preserving refactor proven by the full suite, incl. the
  determinism/VSR corpus, staying green).
- **`kessel_sql::select_columns(sql)`** — uses the real lexer to return
  `(table, [cols])` for a plain projection `SELECT c1, c2 FROM <t> …`
  (rejects `*`, aggregates `FUNC(`, `JOIN`, non-selects).
- **`kessel_client::render_projection(typedef, cols, rows)`** — decodes
  the projection wire shape (`[u32 rowlen][bare fixed-width field bytes]*`)
  against the `DESCRIBE` schema and renders the shared aligned table
  (`render_table`, factored out of `render_rows`). Returns `None` on
  unknown column or shape mismatch → CLI falls back to opaque bytes.
- **CLI**: a projection result is now `DESCRIBE`d and rendered as columns;
  everything else keeps SP52/SP53 behaviour and exit codes.

## Tests (2 new, 156 total)

`select_columns_only_matches_plain_projection` (6 grammar cases incl.
`*`/aggregate/`JOIN`/DESCRIBE/INSERT → `None`); `render_projection_
decodes_column_oriented_rows` (multi-row, signed values, unknown column →
`None`, rowlen mismatch → `None`). E2E (CLI, live server):
`SELECT owner, bal FROM acct` prints a correct aligned table. Full
workspace green (156) — the `kessel_codec::decode` refactor is verified
behaviour-identical by the entire corpus.

## Honest scope boundary

`JOIN` results are still opaque — their wire shape is composite
(`[left_len][left][right]`) over two schemas, which needs its own
decoder/typed-result envelope (named follow-up). `select_columns`
deliberately returns `None` for `JOIN`/aggregates so the fallback is
correct, never misleading. Whole-row + plain projection — the common
read shapes — are now fully readable with no `DESCRIBE` ceremony for the
user.
