## SP-CHAR-PAD-COMPARE — CHAR(N) padding-aware equality + range design spec

Date created: 2026-06-02 (working branch closed same-day at T4)
Parent SP-arc: SP-PG-EXTQ-BIN-RESULTS V1 (closed 2026-06-01 at T3). The
T3 smoke transcript
(`docs/superpowers/sppgextqbinr-t3-smoke-2026-06-01.txt` §47-55)
recorded the precise failure mode this arc closes:

```
asyncpg 0.31.0 — SELECT (binary RESULTS) smoke
  …
  SELECT (binary results): OK — 0 rows
  WARN: expected >=1 row, got 0 — substitution may have failed silently
```

The "0 rows" is a parameterized `WHERE name = $1` against a CHAR(32)
column returning empty even when the row exists. The BIN-RESULTS
arc's headline `SELECT *` (no WHERE) returned the row decoded
correctly — proving the failure is engine-side comparison, not a
binary-RESULT wire issue.

## 1 — Root cause

**Storage:** `kessel-codec::raw_from_value` (lib.rs:104) encodes
`Value::Blob(b"hello")` for a `FieldKind::Char(32)` as

```
vec![0u8; 32]                       // allocate 32 NULs
o[..n].copy_from_slice(&b[..n])     // overwrite first 5 with "hello"
```

so the stored bytes are `b"hello\0\0\0…\0"` (32 bytes, NUL-padded).
This is the engine's universal padding for all `Char(N)` / `Bytes(N)`
values, and changing it would invalidate every existing record AND
break the determinism oracles (`property_roundtrip_random_schemas`
asserts width-equality post-roundtrip — explicit storage contract).

**Comparison (the bug):** `kessel-expr::eval`'s `EQ` opcode is

```rust
EQ => {
    let b = pop!();
    let a = pop!();
    st.push(Value::Int((a == b) as i128));
}
```

For the asyncpg row,
- `a = Value::Bytes(b"hello\0\0\0…\0")` (32 bytes, from `LOAD_FIELD`)
- `b = Value::Bytes(b"hello")` (5 bytes, from `PUSH_BYTES` of the SQL
  literal `'hello'`)

`Value::Bytes(Vec<u8>)` derives `PartialEq`, so `a == b` is `false`
(different lengths). Same byte-cmp shape inside `ord!` for `LT`/`LE`/
`GT`/`GE`. Same byte-cmp shape inside `kessel-expr::materialise_cmp`
for the `compile_filter` specialised path
(`av.as_slice().cmp(bv.as_slice())`).

**Why `cmp_field` (kessel-sm) is NOT the bug for the asyncpg path:**
the SQL planner (`kessel-sql::compile_where`) lowers `WHERE name =
$1` to a `kessel-expr::Program` (`LOAD_FIELD ++ PUSH_BYTES ++ EQ`)
and `Op::QueryRows` evaluates that program with `kessel_expr::eval`
per row (`kessel-sm/src/lib.rs:2675/2693`). `cmp_field` is only
reached on the `Op::Query` path (legacy `kessel-client` predicates),
which width-normalises BOTH sides via `Self::norm(&pr.value, w)`
(`kessel-sm/src/lib.rs:2388`) so it accidentally works (both sides
NUL-pad to N → byte-equal). The SQL path doesn't norm the literal
side because the expr-VM operands are width-agnostic by design.

The smoke transcript's diagnosis ("EQ-on-Char ignores padding only on
the LEFT side") was slightly off — neither side ignores padding. They
both see raw padded bytes from `LOAD_FIELD` and raw literal bytes
from `PUSH_BYTES`, and `Vec<u8> == Vec<u8>` fails on length.

## 2 — SQL §9.20 / PG semantics

PG's `CHAR(N)` comparison semantics treat trailing space (0x20) as
insignificant — `'a' = 'a   '::CHAR(4)` is `true`. The engine's
storage uses NUL (0x00) as its padding byte rather than space, so the
faithful engine-side restatement of the SQL semantic is:

> **trailing padding bytes (NUL OR space) are insignificant in
> `Char(N)` / `Bytes(N)` byte comparisons.**

This widening is safe — it cannot make two previously-distinct
non-padded values compare equal (their content bytes are
unchanged). It only collapses padding ambiguity, which is exactly
the intended SQL semantic.

Precedent inside the engine: the `LIKE` opcode already trims
trailing NULs on the value side before matching
(`kessel-expr/src/lib.rs:416-419`):

```rust
let end = v.iter().rposition(|&c| c != 0).map_or(0, |x| x + 1);
like_match(&v[..end], p)
```

This arc generalises that to all bytes-vs-bytes comparisons + adds
0x20 (space) to the trim set for full PG semantic alignment.

## 3 — Scope

**V1 in:**
- `EQ` / `NE` on `Value::Bytes × Value::Bytes` — right-trim NUL + space
- `LT` / `LE` / `GT` / `GE` on `Value::Bytes × Value::Bytes` — right-trim NUL + space
- `compile_filter::materialise_cmp` bytes×bytes path — same trim
- `kessel-sm::cmp_field` for `FieldKind::Char(_)` and `FieldKind::Bytes(_)` — same trim
- Asymmetric Bytes-vs-non-Bytes cmps (mixed types) — unchanged (interpreter
  returns `false` for mixed kinds; matches PG too — no value here)

**V1 out:**
- Storage encoding — unchanged. `raw_from_value` still NUL-pads.
- Index keys — unchanged. `idx_lookup` / `vorder_key` use the
  width-normalised (padded) bytes; changing index keys would
  invalidate existing indexes and is unnecessary because the
  verifying program runs on every candidate.
- Hashing — unchanged. Determinism oracle (records hash by full
  stored bytes).
- VARCHAR — engine treats `VARCHAR(N)` as `Char(N)` at the codec
  level (same fixed-width row layout); same fix applies, same KAT.
- `Ref` / `OverflowRef` — these are 16-byte ObjectIds; trailing NULs
  in an ObjectId are meaningful (identifies low-id objects). Keep
  byte-equality on those kinds — only `Char(_) | Bytes(_)` get the
  trim.

## 4 — Implementation

Add a single helper in `kessel-expr`:

```rust
/// PG `CHAR(N)` semantic: trailing padding bytes (NUL or space) are
/// insignificant. Also matches engine's storage convention
/// (`raw_from_value` NUL-pads fixed-width fields). Pure.
fn right_trim_char_pad(b: &[u8]) -> &[u8] {
    let end = b.iter().rposition(|&c| c != 0 && c != b' ').map_or(0, |i| i + 1);
    &b[..end]
}
```

Apply at the three call sites:
- `EQ` / `NE`: switch from `a == b` (which uses Vec PartialEq) to a
  type-aware comparison that trims when both sides are `Value::Bytes`.
- `ord!` macro's `Value::Bytes` arm: compare the trimmed slices.
- `materialise_cmp`'s bytes×bytes closure: trim both sides before
  `cmp_apply`.

Mirror the helper in `kessel-sm::cmp_field`:

```rust
match kind {
    // …existing int arms…
    Char(_) | Bytes(_) => {
        right_trim_char_pad(&a).cmp(right_trim_char_pad(&b))
    }
    Ref | OverflowRef => a.cmp(&b),  // ObjectIds — full byte cmp
}
```

The `Ref`/`OverflowRef` arm splits out from the existing combined
arm — it stays full-byte to preserve ObjectId comparison invariants.

LIKE's existing trim (line 416) stays as-is — it trims NUL only and
operates on the value (haystack), not the pattern. Generalising it
is a separate concern (PG's `LIKE` does NOT trim CHAR padding before
match — it's a separate semantic). The existing behaviour already
makes fixed-width text LIKE work; no change needed.

## 5 — Determinism contract

`kessel-expr::eval` is the engine's determinism oracle for predicate
evaluation. The trim is a pure function of the operand bytes — no
randomness, no allocation beyond the slice borrow, no clock. The
specialised `compile_filter` closure path stays byte-equal to `eval`
(its bytes×bytes kernel applies the same trim). The KAT
`compile_filter_byte_equal_to_interpreter_over_random_rows` (now
extended with CHAR-shape programs) continues to lock the equivalence.

`cmp_field` is engine-internal — used in `Op::Query` row verification,
sort-key paths, and aggregate min/max. The trim is consistent with
the new expr-VM semantic, so `Op::Query` and `Op::QueryRows` agree
on `WHERE name = 'hello'` even when one side is padded and the other
is not.

Replay safety: an existing record `b"hello\0\0\0..."` always
trims to `b"hello"`; a new literal `b"hello"` trims to itself.
Pre-arc and post-arc disagree on whether they're equal, but
**any record written by the engine pre-arc has the same padded
form post-arc**, and the verifying program is re-run from the same
stored bytes — so a replay against either build produces
deterministically the same superset of candidate rows, and any
row that matched pre-arc still matches post-arc (the trim only
adds matches, never removes). The state-machine oracle
(`apply(op, …) -> deterministic OpResult`) gets STRICTLY MORE
matches post-arc, which is the explicit fix intent — not a
silent semantics change.

## 6 — Acceptance

T3 vulcan smoke re-runs the original EXTQ-BIN-RESULTS failing case:

```python
await conn.execute("CREATE TABLE charpad_smoke (id BIGINT, name CHAR(32))")
await conn.execute("INSERT INTO charpad_smoke (id, name) VALUES ($1, $2)", 42, "hello")
rows = await conn.fetch("SELECT * FROM charpad_smoke WHERE name = $1", "hello")
# Pre-arc: []   Post-arc: [(42, "hello")]
```

PASS ⇒ HEADLINE.

## 7 — KAT plan (+10–12)

`kessel-expr` (10):
1. `eq_char_pad_lhs_padded_rhs_bare` — `Bytes("hello\0\0\0") == Bytes("hello")` ⇒ true
2. `eq_char_pad_lhs_bare_rhs_padded` — symmetric
3. `eq_char_pad_both_padded_same_content` — true
4. `eq_char_pad_both_bare_same_content` — true (regression — existing path)
5. `eq_char_pad_distinct_content_still_distinct` — `Bytes("hello\0") != Bytes("hi   ")` ⇒ false
6. `eq_char_pad_all_padding_equals_empty` — `Bytes("    ") == Bytes("")` ⇒ true
7. `eq_char_pad_mixed_nul_and_space` — `Bytes("hi \0  ") == Bytes("hi")` ⇒ true
8. `ne_char_pad_trims_too` — NE returns false when EQ returns true
9. `lt_char_pad_trims_both_sides` — `Bytes("hi  ") < Bytes("ho")` ⇒ true
10. `compile_filter_char_pad_eq_byte_equals_interpreter` — closure vs `eval` on padded operands

`kessel-sm` (2-3):
11. `cmp_field_char_lhs_padded_rhs_bare_equal`
12. `cmp_field_char_ref_not_trimmed` — `Ref` (16-byte ObjectId with
    trailing NULs) compares full-byte (regression)

Total target delta: +10 to +12 KATs.

## 8 — Rollout / commits

1. **T1+T2**: design doc + helper + interpreter EQ/NE/ord patches +
   `compile_filter` bytes-cmp patch + `cmp_field` patch + KATs.
2. **T3**: vulcan asyncpg re-smoke + USAGE.md §9 caveat removal +
   smoke transcript checked in.
3. **T4**: STATUS.md track row + progress tracker → CLOSED.

Standing rules per AGENTS.md: direct commits to `main`, no co-author
tags, no `-S`, CI green check after each push.

## 9 — Out of scope / V2 follow-ups

- `SP-CHAR-PAD-LIKE` — PG semantic for `LIKE` against CHAR(N) is "match
  the full padded value" (not what the engine does today — engine trims
  trailing NULs from the value before LIKE matching). Closing this gap
  would change a documented behaviour; out of scope.
- `SP-PG-EXTQ-PARSED` — typed-parameter AST (replaces text-substitution
  for parameters). Independent arc; CHAR pad is orthogonal.
- `SP-PG-VARCHAR-NATIVE` — distinct codec for VARCHAR(N) (variable
  length, not fixed-padded). This arc shares the `Char(N)` codec; both
  benefit from the trim. A future native VARCHAR codec would not need
  the trim because no padding is stored.
