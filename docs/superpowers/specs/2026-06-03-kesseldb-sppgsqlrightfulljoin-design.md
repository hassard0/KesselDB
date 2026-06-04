# SP-PG-SQL-RIGHT-FULL-JOIN — design

**Arc:** complete the JOIN-type matrix by adding `RIGHT [OUTER] JOIN` and
`FULL [OUTER] JOIN` to the existing `INNER` + `LEFT` support.

**Date:** 2026-06-03

## Goal

KesselDB already supports `[INNER] JOIN` and `LEFT [OUTER] JOIN` over a binary
(two-table) equi-join, composing with WHERE / ORDER BY / LIMIT / OFFSET / table
aliases, and multi-join INNER chains (3+ tables). This arc adds the two
remaining outer-join flavours so the full matrix INNER / LEFT / RIGHT / FULL is
available, purely additively (no new wire struct field, no oracle-literal churn
beyond the existing `join_type`).

## Semantics

Combined output schema is ALWAYS `a-fields ++ b-fields` (the user wrote
`FROM a <flavour> JOIN b`, so projection `a.x, b.y` keeps resolving). The four
flavours differ only in WHICH unmatched rows they emit:

| flavour | matched pairs | unmatched-LEFT (a row, b.* NULL) | unmatched-RIGHT (b row, a.* NULL) |
|---------|:-:|:-:|:-:|
| INNER   | ✓ | – | – |
| LEFT    | ✓ | ✓ | – |
| RIGHT   | ✓ | – | ✓ |
| FULL    | ✓ | ✓ | ✓ |

- **RIGHT** = LEFT logic with the conceptual drive swapped: every `b` row
  appears at least once; an unmatched `b` row emits a combined row with the
  `a.*` columns NULL — but the OUTPUT column order stays `a.* ++ b.*`.
- **FULL** = LEFT results PLUS the unmatched-RIGHT rows. Matched pairs appear
  exactly once (no duplication).

## Wire format (kessel-proto)

`enum JoinType { Inner, Left, Right, Full }`. The join-type tag byte is emitted
ONLY when non-`Inner`, so:

- `Inner` ⇒ tag NOT emitted ⇒ byte-identical to a pre-outer-join frame.
- `Left` ⇒ tag `1` (unchanged).
- `Right` ⇒ tag `2` (NEW).
- `Full` ⇒ tag `3` (NEW).

`wire_tag()` / `from_wire_tag()` extended for 2 and 3. No new struct field ⇒
existing `Op::Join` construction sites already carry `join_type`; determinism
oracles stay green. Oracle corpus gains a `Right` (no filter) and a `Full`
(filter + ORDER BY DESC + LIMIT + OFFSET) roundtrip case.

## State machine (kessel-sm `apply_join`)

Implementation decomposes the flavour into three independent row-set switches
over the SAME `a ++ b` combined schema:

```
is_left  = Left;   is_right = Right;   is_full = Full
emit_unmatched_left  = is_left  || is_full   // a row, b.* NULL
emit_unmatched_right = is_right || is_full   // b row, a.* NULL
```

- **Combined-schema nullability:** an outer flavour that can emit unmatched rows
  on a side marks the OTHER side's fields nullable. LEFT/FULL ⇒ `b.*` nullable;
  RIGHT/FULL ⇒ `a.*` nullable.
- **Pass 1 (drive over `a`):** matched pairs (all flavours) + unmatched-left
  rows (LEFT/FULL). For RIGHT/FULL it records the set of left join-keys that
  matched (`matched_keys`).
- **Pass 2 (drive over `b`, RIGHT/FULL only):** every `b` row whose join key is
  NOT in `matched_keys` is emitted with `a.*` NULL, in right-table scan-range
  order, appended AFTER the pass-1 rows.
- **Determinism / row order:** matched + unmatched-left rows in the existing
  deterministic left-key/right-scan order, THEN unmatched-right rows in
  right-table scan order. A total, input-determined order, locked by unit tests.
- RIGHT/FULL force collection (instead of streaming) because the full
  matched-key set must be known before deciding which `b` rows are unmatched;
  INNER/LEFT keep streaming (byte-identical) when not sorting/grouping. The
  legacy `limit` cap is applied at the unified emit.
- RIGHT/FULL compose with filter / ORDER BY / LIMIT / OFFSET / GROUP BY through
  the EXISTING collect → filter → sort → paginate / group paths.

### Deferred (named follow-up)

RIGHT/FULL on the base join of a **3+ table chain** (`extra_joins` non-empty) is
rejected with a clean `SchemaError` (outer chains are their own complexity, as
LEFT chains already were). INNER chains keep working. RIGHT/FULL on the
first/only join works.

## SQL parser (kessel-sql)

- Lexer already tokenises `RIGHT` / `FULL` / `OUTER` as clause keywords.
- Base join-type parse: `RIGHT [OUTER] JOIN` → `JoinType::Right`,
  `FULL [OUTER] JOIN` → `JoinType::Full`, `INNER JOIN` / bare `JOIN` → `Inner`,
  `LEFT [OUTER] JOIN` → `Left` (unchanged).
- Single-table fast-path guards (`select_table_only`, `select_columns`) and the
  alias-detection lookahead + `consume_join_kw` recognise `RIGHT` / `FULL` so an
  outer join is never mis-parsed as a single-table SELECT.
- The multi-join chain loop already rejects a mid-chain LEFT/RIGHT/FULL.

## PG gateway (kessel-pg-gateway `render_join_result`)

NO change. RIGHT/FULL produce the SAME `KTR1` combined-schema stream shape as
LEFT/INNER (just different row sets and NULL placement). The renderer is driven
entirely by the embedded typedef + `decode_record` → `encode_data_row` with the
`-1` length sentinel for NULL, so NULL `a.*` / `b.*` columns render as SQL NULL
(read back as Python `None` in psycopg2), exactly like the LEFT NULL path.

## Test plan

- proto: Right/Full roundtrip oracle cases; existing determinism oracles green.
- sm: INNER/LEFT/RIGHT/FULL row-set assertions over a seeded two-table set with
  unmatched rows on BOTH sides (orphan author + homeless book); column order
  `a.*, b.*`; both-side-NULL decode; deterministic re-run equality; chain
  rejection.
- sql: RIGHT/FULL/INNER base-join parse → correct `JoinType`; aliases on
  RIGHT/FULL; `join_projection` recognises them.
- smoke: `scripts/sppgsqlrightfulljoin-smoke.py` hard-asserts INNER/LEFT/RIGHT/
  FULL row sets via real psycopg2, with NULL columns reading `None`.
- regression: an existing JOIN smoke confirms INNER/LEFT/alias/chain unchanged.
