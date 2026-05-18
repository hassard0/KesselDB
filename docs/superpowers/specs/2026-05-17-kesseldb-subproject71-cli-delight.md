# KesselDB Sub-project 71 — CLI & output delight

**Date:** 2026-05-17  **Status:** shipped, e2e-verified. 171 green.
The performance work (SP67–70) is done; this makes *using* KesselDB
pleasant for the surface humans and agents actually touch — the `kessel`
CLI — without adding any capability the server doesn't already back.

## What changed

All rendering lives in `kessel-client` (pure, unit-tested); the binary
only wires it. Nothing here invents server behaviour.

1. **`--json` mode** — one stable JSON object per statement, for agents
   and scripts:
   - `format_result_json` — total mapping of every `OpResult`
     (`{"status":"ok"|"error"|"not_found"|…}`, scalar →
     `{"status":"ok","value":N}`), with a zero-dep RFC-8259 string
     escaper (control chars, quotes, backslash).
   - `render_rows_json` — whole-row `SELECT *` → `[{"col":val,…},…]`
     decoded against the `DESCRIBE` typedef (handles the length-prefixed
     and the single-bare-record `… ID n` shapes; `[]` for zero rows).
   - `render_schema_json` — `DESCRIBE` →
     `{"table","columns":[{name,type,nullable}]}`.
   - `EXPLAIN --json` → `{"status":"ok","plan":"…"}`.
   Exit codes unchanged, so `--json` + `$?` is a clean agent contract.

2. **Readable `DESCRIBE` / `\d`** — was "GOT 36 bytes". Now
   `render_schema` decodes the typedef into an aligned
   `column | type | null` table (and `render_schema_json` for `--json`).
   Friendly type names (`CHAR(16)`, `FIXED(scale=2)`, …).

3. **Shell ergonomics** (pipe + interactive, only what existing ops
   back — no half-working illusions):
   - `\?` / `\h` / `\help` — list commands
   - `\d <table>` — describe (maps to `DESCRIBE`)
   - `\timing` — toggle per-statement timing (`time: 672 µs` / `ms`)
   - `\q` / `quit` / `exit` — leave
   - unknown `\x` → a helpful hint, not an opaque SQL error
   `\dt` (list all tables) was deliberately **not** added — no server op
   enumerates tables, and a fake one would be a lie. Named follow-up:
   a real `SHOW TABLES` op, then `\dt`.

4. **Friendlier failures** — a connection error now prints the exact
   command to start a server; a bad meta-command explains itself.

## Tests

`kessel-client`: `json_output_is_well_formed_and_total` (every variant +
escaping + typed rows + empty array) and
`schema_rendering_is_readable_and_json` (text table + exact JSON +
rejects non-typedef). E2E verified against a live server: `--json`
rows/scalar/error(exit 1)/explain, text `DESCRIBE`, the shell
`\?`/`\timing`/`\d`/unknown, and the friendly connect hint. 171 green,
full workspace regression clean (determinism/VSR corpus untouched —
client-only change).

## Honest boundary

JSON `rows` is emitted for whole-row `SELECT *`; projections / JOIN
results still fall back to the scalar/`bytes` object (decoding them needs
the typed-result protocol — a tracked follow-up, not silently wrong).
