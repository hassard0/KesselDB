# SP-CHAR-PAD-COMPARE — CHAR(N) padding-aware equality + range — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T2 (2026-06-02).** asyncpg `WHERE name = $1`
against `CHAR(32)` now returns the matching row on vulcan (was `0 rows` +
WARN pre-arc — recorded in
`docs/superpowers/sppgextqbinr-t3-smoke-2026-06-01.txt` §47-55).
BETWEEN / NE / range comparison also pass. psycopg2 simple-query
path regression-free (`WHERE name = 'nope'` still returns 0 rows).
TaskList #361 ready for completion.

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-spcharpadcompare-design.md`
Smoke transcript: `docs/superpowers/spcharpadcompare-t3-smoke-2026-06-02.txt`
Parent SP-arc: SP-PG-EXTQ-BIN-RESULTS V1 (closed 2026-06-01 at T3); the
V1 out-of-scope clause named this arc as the engine-side EQ-on-Char
NUL-padding lift surfaced by the T3 smoke (asyncpg parameterized
`WHERE name = $1` returned 0 rows).

## Root cause (re-diagnosed in design §1)

The pre-arc diagnosis in the SP-PG-EXTQ-BIN-RESULTS T3 transcript
("EQ-on-Char ignores padding only on the LEFT side") was slightly
off. The real cause was the engine's expr-VM: `Value::Bytes(Vec<u8>)
PartialEq` is **length-sensitive**, so

- LHS = `Value::Bytes(b"hello\0\0\0…\0")` (32 bytes from `LOAD_FIELD`
  on a NUL-padded CHAR(32) row — engine's universal storage shape
  per `kessel-codec::raw_from_value`)
- RHS = `Value::Bytes(b"hello")` (5 bytes from `PUSH_BYTES` of the
  SQL literal `'hello'`)

return `false` from the `EQ` opcode. Same shape inside the `ord!`
macro for LT/LE/GT/GE, and inside `compile_filter::materialise_cmp`'s
bytes×bytes closure. `kessel-sm::cmp_field` was NOT the root cause
for the asyncpg path (it width-norm's both sides to the field width
before cmp, so it accidentally worked), but the engine-wide helper
was still fixed for consistency with the new expr-VM semantic and
to cover the non-SQL `Op::Query` path.

## What this SP-arc ships

V1 = "trailing padding bytes (NUL 0x00 OR space 0x20) are
insignificant in `Char(_)` / `Bytes(_)` byte comparisons across every
engine path: interpreter, `compile_filter` closure, `cmp_field`
helper." This is the PG SQL §9.20 CHAR comparison semantic,
generalised to NUL because the engine's storage uses NUL as its
universal padding byte (rather than space).

After V1 lands (T1..T2), the asyncpg / psycopg2 / psql clients can:

1. Send `SELECT * FROM t WHERE name = 'hello'` against a CHAR(32)
   column whose stored row is `b"hello\0\0\0…\0"` — engine returns
   the matching row.
2. Send `SELECT * FROM t WHERE name BETWEEN 'g' AND 'i'` — range
   comparison applies the trim consistently on both sides.
3. Send `SELECT * FROM t WHERE name != 'hello'` — NE works.
4. Send `SELECT * FROM t WHERE name = $1` from asyncpg (parameterized
   extended-query Bind) — the gateway's Describe(S) probe now strips
   `$N` placeholders for the table-name lookup so asyncpg's cached
   field count isn't `0`, and the engine fix actually surfaces in
   the DataRow.

**Storage / indexes / hashing UNCHANGED.** Only the comparison layer
trims. Existing data + replicas don't need migration.

**Out-of-scope (named, deferred — each is its own arc):**

- **`SP-CHAR-PAD-LIKE` (V2)** — PG `LIKE` against CHAR(N) matches the
  full padded value; the engine currently trims trailing NULs from
  the value before LIKE matching (a documented edge in
  `kessel-expr::LIKE` opcode). Generalising this is a separate
  semantic decision (might break existing CHECK constraints + smokes).
- **`SP-PG-EXTQ-PARSED` (V2)** — typed-parameter AST. Replaces the
  current `substitute_text_format_params` text-rewrite with a proper
  parameter AST node so the parser sees `$N` as a placeholder, not
  a lex error. Independent of CHAR-pad — both arcs improve the same
  Describe-on-`$N` path but along different axes.
- **`SP-PG-VARCHAR-NATIVE` (V2)** — distinct codec for VARCHAR(N)
  (variable length, no padding). The current codec aliases
  `VARCHAR(N)` to `Char(N)`; the trim makes both behave correctly
  for cmp, so this is purely an optimisation.

## Slice plan (mirrors design spec §8)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec (~250 LoC) at `docs/superpowers/specs/2026-06-02-kesseldb-spcharpadcompare-design.md` covering root cause, scope, semantic, determinism contract, KAT plan. | **DONE** (folded into T2 commit) | `870d887` |
| **T2** | `kessel-expr::right_trim_char_pad` helper (pub fn — re-used by `kessel-sm::cmp_field`). EQ/NE/ord!/materialise_cmp trim integration. `kessel-sm::cmp_field` split Char(_) / Bytes(_) from Ref / OverflowRef. +9 expr KATs (helper + 8 EQ/NE/ord/closure shapes + Int regression + const-vs-const-bytes). +5 sm KATs (cmp_field padded/space/distinct/ref-not-trimmed/width-truncation). | **DONE** | `870d887` |
| **T2.5** | Describe enabler: `row_description_or_no_data_for_sql` substitutes `$N` with literal NULL for the table-name probe. Closes the asyncpg ProtocolError that the engine fix unmasked (pre-arc: 0 rows masked it; post-arc: 1 row exposes the column-count mismatch). +1 pg-gateway KAT. | **DONE** | `6b2c626` |
| **T3** | vulcan smoke — re-run `scripts/sppgextqbinr-asyncpg-smoke.py` (PASS, now 1 row vs 0 pre-arc) + dedicated asyncpg WHERE/BETWEEN/NE check (HEADLINE PASS) + psycopg2 simple-query regression check (PASS, negative case correctly returns 0). USAGE §9 update — strike-through the SP-CHAR-PAD-COMPARE caveat the BIN-RESULTS arc added. Transcript at `docs/superpowers/spcharpadcompare-t3-smoke-2026-06-02.txt`. | **DONE** | `b3f8c75` |
| **T4** | STATUS.md track row + this progress tracker → CLOSED. TaskList #361 ready. | **DONE** | (this commit) |

## KAT deltas

| Crate              | Pre-arc | Post-arc | Delta | New                                |
|--------------------|---------|----------|-------|------------------------------------|
| kessel-expr (lib)  |     25  |      34  |   +9  | helper + EQ/NE/ord/closure shapes  |
| kessel-sm (lib)    |    162  |     167  |   +5  | cmp_field padded/bare/space/etc    |
| kessel-pg-gateway  |    775  |     776  |   +1  | Describe enabler                   |
| kessel-sql (lib)   |     43  |      43  |    0  | (no regressions)                   |
| **TOTAL**          | **1005**| **1020** | **+15**|                                   |

## Determinism contract

`kessel-expr::eval` is the engine's determinism oracle for predicate
evaluation. The trim is a pure function of the operand bytes — no
randomness, no allocation beyond the slice borrow, no clock. The
specialised `compile_filter` closure path stays byte-equal to `eval`
because the bytes×bytes kernel applies the same trim. The KAT
`spcharpadcompare_compile_filter_byte_equals_interpreter` (closure
vs interpreter, 9 program shapes × 8 contents = 72 cases) locks the
equivalence.

`cmp_field` is engine-internal — used in `Op::Query` row verification,
sort-key paths, and aggregate min/max. The trim is consistent with
the new expr-VM semantic, so `Op::Query` and `Op::QueryRows` agree
on padded-vs-bare CHAR comparisons.

Replay safety: an existing pre-arc record `b"hello\0\0\0..."` always
trims to `b"hello"`; a new post-arc literal `b"hello"` trims to
itself. Pre-arc and post-arc disagree on whether they're equal, but
**any record written by the engine pre-arc has the same padded
form post-arc** (storage unchanged), and the verifying program is
re-run from the same stored bytes — so a replay against either
build produces deterministically the same superset of candidate
rows. The trim only ADDS matches, never removes — making the
semantic strictly more permissive (intended), not breaking any
historical row-presence invariant.

## Vulcan smoke evidence (2026-06-02)

```
asyncpg 0.31.0 — SELECT (binary RESULTS) smoke (re-run)
  connect: OK
  CREATE TABLE: OK
  INSERT (literal seed): OK
  SELECT (binary results): OK — 1 rows                     ← was 0 rows
    row: id=42 (type=int), name='first'
  SELECT * (binary results, no params): OK — 2 rows
    row: id=42, name='first'
    row: id=43, name='second'
  === asyncpg SELECT (binary RESULTS) PASS ===
```

Dedicated asyncpg WHERE/BETWEEN/NE:
```
  SELECT * (no WHERE): 2 rows -> [(42, 'hello'), (43, 'world')]
  SELECT WHERE name = $1 ('hello'): 1 rows -> [(42, 'hello')]
  HEADLINE: PASS — WHERE name = $1 returns the matching row
  SELECT WHERE name BETWEEN 'g' AND 'i': 1 rows -> [(42, 'hello')]
  SELECT WHERE name != $1 ('hello'): 1 rows -> [(43, 'world')]
```

psycopg2 regression (negative case correctly empty):
```
  WHERE name = hello  -> [(42, 'hello')]
  WHERE name = world  -> [(43, 'world')]
  WHERE name = nope   -> []                                ← correct empty
  SELECT *            -> [(42, 'hello'), (43, 'world')]
```

## Commits (chronological)

| Commit  | What                                                                         |
|---------|------------------------------------------------------------------------------|
| `870d887` | T1+T2 engine fix in kessel-expr + kessel-sm + design spec + 14 KATs        |
| `6b2c626` | T2.5 pg-gateway Describe enabler ($N substitute) + 1 KAT                   |
| `b3f8c75` | T3 vulcan smoke transcript + USAGE §9 caveat removal                       |
| (this)  | T4 STATUS row + progress tracker → CLOSED                                    |
