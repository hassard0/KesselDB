# KesselDB Sub-project 53 — typed row rendering (best-in-class CLI DX)

**Date:** 2026-05-17  **Status:** shipped, tested, smoke-verified. 148 green.

## The gap (the one SP52 named)

The `kessel` CLI printed `GOT 32 bytes (use DESCRIBE…)` for `SELECT *` —
the single biggest remaining usability wart. Now it prints a real,
aligned table:

```
owner | bal
------+----
100   | 50
100   | -7
(2 rows)
```

## Delivered

- **`kessel_sql::select_star_table(sql)`** — uses the *real lexer* (no
  string heuristics) to return the source table iff the statement is a
  whole-row, single-table `SELECT * FROM <t> …` (rejects projections,
  `JOIN`, aggregates, non-selects). Unit-tested across all those cases.
- **`kessel_catalog::ObjectType::from_def(name, fields)`** — builds the
  minimal `ObjectType` a client needs to `kessel_codec::decode` rows from
  the wire schema (`DESCRIBE` output).
- **`kessel_client::render_rows(typedef, rows)`** — pure, total. Decodes
  **both** wire shapes: the filtered `[u32 len][rec]*` stream *and* the
  bare single record from the `SELECT * … ID <n>` O(1) fast path. Renders
  an aligned header/sep/rows table with a `(N row[s])` footer; `Uint/Int`
  shown numerically, `Blob` as trimmed text or `0x…` hex, `Null` as
  `NULL`. Returns `None` on any malformation so the CLI falls back to
  opaque bytes — never wrong, only ever less detailed.
- **CLI wiring**: on a `Got` result for a whole-row select, the CLI
  `DESCRIBE`s the table and renders columns; everything else (scalars,
  projections, joins, errors) keeps the SP52 behaviour and exit codes.

## Tests (3 new, 148 total)

`select_star_table_only_matches_whole_row_single_table` (8 grammar cases);
`render_rows_decodes_and_aligns` (multi-row, signed values, malformed →
`None`, bad typedef → `None`, zero rows → header + `(0 rows)`, **single
bare record → 1 row**). E2E smoke against a live server: `SELECT * WHERE`
and `SELECT * … ID n` both print correct aligned tables.

## Honest scope boundary

Projections (`SELECT c1, c2`) and `JOIN` results still print as opaque
bytes — their wire shape isn't a self-describing whole-row stream, so
decoding them needs column metadata the client doesn't have. This is a
named, non-gating follow-up (a typed-result protocol), not a silent gap;
`select_star_table` deliberately returns `None` for them so the fallback
is correct, not misleading.
