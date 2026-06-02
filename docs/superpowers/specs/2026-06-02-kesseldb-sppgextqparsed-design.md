# SP-PG-EXTQ-PARSED — kessel-sql `$N` parameter token + typed-param threading — DESIGN

**Status:** design — scopes the SP-PG-EXTQ V1 follow-up named in
`docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
§11 weak-spot #1 + §2.2 V2 follow-ups (`SP-PG-EXTQ-PARSED`). SP-PG-EXTQ
V1 closed 2026-05-29 at T8 (commit `f57fa63`); this arc closes the
single biggest residual security-shape concern named in that V1 self-
review: the text-rewrite substitution attack surface.

Companion progress tracker:
`docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgextqparsed-progress.md`.

**Builds on:**
- **SP-PG-EXTQ V1** (closed 2026-05-29) — the
  `extq::substitute::substitute_params` + `preprocess_params` text-
  substitution pipeline + the `dispatch_execute` glue + the
  `PreparedParam::{Null, Text, Raw}` discriminator the binary-format
  path (SP-PG-EXTQ-BIN, SP-PG-EXTQ-BIN-NUMERIC) extends. SP-PG-EXTQ-
  PARSED does NOT touch the V1 wire decoders, the portal storage, or
  any of the binary-format-decode helpers — it ADDS a path from
  `(sql, typed_values) → compiled Op` that bypasses string concatenation.
- **`kessel-sql`** — the SQL parser + lexer + compile pipeline already
  in tree. SP-PG-EXTQ-PARSED ADDS one `Tok::Param(u16)` variant to the
  lexer + ONE compile-time pass that resolves param tokens to bound
  values (or errors if the slot is unbound). The bytecode emitter is
  untouched; the planner is untouched; every existing KAT compiles +
  passes byte-equal.

---

## 1. Context — why typed-param threading

SP-PG-EXTQ V1 §11 weak-spot #1 named the issue verbatim:

> **SQL-text parameter substitution is brittle**. The substitution
> walks the SQL text replacing `$N` outside quoted regions. Edge
> cases the algorithm handles correctly today (single-quoted strings,
> double-quoted identifiers, `--` line comments, `/* */` block
> comments, dollar-quoted strings) but a future SQL extension could
> break.

And #2:

> **No structured AST means SQL-injection prevention relies on the
> text substitution's `'` → `''` escaping**. Specifically, a client
> bound value containing a `'` that the V1 escape rule missed would
> let the value escape the quoted region in the rewritten SQL. KATs
> lock the `O'Brien` / `bobby);` adversarial cases. Still a real
> attack surface.

Today, a client that issues `Bind` with a parameter value `'; DROP
TABLE t; --` flows through:

1. `extq::substitute::preprocess_params` wraps it as
   `PreparedParam::Text(b"'; DROP TABLE t; --")`.
2. `extq::substitute::substitute_params` walks the SQL `WHERE name =
   $1`, finds the `$1`, emits the value as a single-quoted literal
   with `'` → `''` doubling: `WHERE name = '''; DROP TABLE t; --'`.
3. The string is concatenated and handed to `dispatch_query` →
   `kessel_sql::compile` which RE-PARSES the text.

The escape doubling is correct today. But the attack surface is real:
ANY missed quoting (a future SQL feature, a regex-shaped scanner
escape) lets the value escape the quoted region. The structurally-
correct fix is to NEVER concatenate the value into SQL text at all —
bind it as a typed `Value` directly to the AST node that needs it.

This arc adds that path.

## 2. Scope

### 2.1 V1 — what's in (this arc, T1..T4)

1. **Lexer extension.** `kessel-sql`'s lexer recognizes `$N` (1-99) as
   a new `Tok::Param(u16)` variant. The recognition is greedy over
   decimal digits, matching the gateway's text-substitution helper's
   `$N` shape (so the cap is the same 99 but lifted to u16 for
   forward compatibility). Indices outside [1, 99] emit a lexer
   error.
2. **AST/Lit extension.** A new `Lit::Param(u16)` variant. The
   existing `Lit::{Int, Str}` shapes stay unchanged. The lexer keeps
   `Tok::Param` in the token stream; the value-position recognizer
   (the one that builds `Lit` for VALUES/INSERT/UPDATE/predicates)
   accepts `Tok::Param(n)` and emits `Lit::Param(n)`.
3. **`compile_with_params` entry point.** A new public function:
   ```rust
   pub fn compile_with_params(
       sql: &str,
       cat: &Catalog,
       params: &[Option<Value>],
   ) -> Result<Op, SqlError>
   ```
   That accepts a slice of typed `Value`s (the same `kessel_codec::
   Value` the planner already uses for encoded literals) and threads
   them through the existing `compile` pipeline. Internally the
   pass:
   - Lexes the SQL → tokens (now possibly containing `Tok::Param`).
   - REWRITES each `Tok::Param(n)` to the matching `Tok::Int(_)` /
     `Tok::Str(_)` / `Tok::Ident("NULL")` based on the typed `Value`
     at `params[n-1]`. This is a TYPED rewrite — the value's wire
     representation never touches the SQL text; the rewrite happens
     AFTER lexing, BEFORE parsing, on the already-tokenized stream.
   - Hands the rewritten token stream to the existing parser
     (unchanged). The compiled `Op` is byte-identical to what you'd
     get from the equivalent SQL with literal values in place of `$N`.
4. **Bare `compile`/`compile_stmt` backward-compat.** Both keep their
   existing signature + behavior. If a `Tok::Param` shows up during
   compile-without-params (e.g. the engine accidentally got a SQL
   string with raw `$N` and no bound values), the parser returns
   `SqlError::UnboundParameter(n)` rather than silently producing a
   garbage program.
5. **Gateway plumbing — V1 SCAFFOLD ONLY.** The gateway adds a feature
   flag (default OFF in T2/T3, available behind `KESSELDB_EXTQ_PARSED_
   PATH=1` env) that routes Bind/Execute through `compile_with_params`
   when every bound value can be expressed as a typed `Value`. The
   text-substitution path remains the default at V1; flipping the
   default to the typed-param path is a follow-up after the typed
   path covers every extq surface (INSERT, UPDATE, DELETE, SELECT,
   pg_catalog probes, etc.) at byte-equal output.

### 2.2 V1 — what's out (named V2+ follow-ups)

- **Per-position OID-driven type inference at Parse time.** V1 trusts
  the Bind-side OID hints (already carried in `PreparedStmt::param_
  oids`). A future `SP-PG-EXTQ-PARSED-INFER` arc would infer types
  from the parameter's contextual position in the AST (e.g. `WHERE
  int_col = $1` infers `$1: int`) and check Bind-side OIDs against
  the inferred type.
- **Default-flip of gateway's substitute path.** V1 ships the typed-
  param entry point + an opt-in route; the default stays
  text-substitution so we don't risk a silent compat regression. A
  follow-up `SP-PG-EXTQ-PARSED-DEFAULT` arc flips after a soak.
- **Parameterized DDL.** `CREATE TABLE t (col $1)` doesn't get a
  typed-param path — DDL doesn't accept Bind-time parameters in PG
  itself. V1 explicitly returns `SqlError::ParamInDdl` for any
  `Tok::Param` inside a CREATE/DROP/ALTER statement.
- **Identifier substitution.** `SELECT * FROM $1` doesn't work — V1
  rejects `Tok::Param` outside value positions with `SqlError::
  ParamInIdentifierPosition`. Same rule as PG itself.
- **Pre-compiled prepared-statement AST cache.** V1 re-lexes and
  re-parses the SQL on every Execute (matches today's behavior). A
  follow-up `SP-PG-EXTQ-PARSED-CACHE` arc would cache the
  pre-substitution tokenized stream in `PreparedStmt` and only
  re-substitute on Execute.

## 3. The token rewrite — pre-parse substitution

The simplest correctness-preserving incremental shape: after lexing
but BEFORE parsing, walk the token stream and rewrite each
`Tok::Param(n)` to the typed equivalent of `params[n-1]`. The parser
sees an unchanged grammar — every `Lit::Param` reaches it as a
`Lit::Int` or `Lit::Str` AFTER the rewrite, so no parse path needs
to change.

This shape:

- **Removes the SQL-injection attack surface.** The bound value is
  NEVER rendered into SQL text. It enters as a `Value` (typed) and
  emerges in the program as the same `Value` — the path from
  client bytes to bytecode operand involves no concatenation, no
  quoting, no escape rules.
- **Is byte-equal to the existing path** for every input shape the
  existing path handles correctly. The parser sees the same tokens
  it would see if the user had typed the literal SQL directly.
- **Is minimal-risk.** The lexer adds one variant; the parser adds
  zero variants; the rewrite is a single linear pass over the token
  vec.

### 3.1 Token-level rewrite rule

For each token `t` in the lexed stream:

- `Tok::Param(n)` where `params[n-1] = Some(Value::Int(i))` → replace
  with `Tok::Int(i)`.
- `Tok::Param(n)` where `params[n-1] = Some(Value::Uint(u))` → replace
  with `Tok::Int(u as i128)` (fits if `u <= i128::MAX as u128`,
  else `SqlError::ParamOverflow`).
- `Tok::Param(n)` where `params[n-1] = Some(Value::Blob(bytes))` →
  replace with `Tok::Str(String::from_utf8_lossy(&bytes).into_owned())`.
- `Tok::Param(n)` where `params[n-1] = None` → replace with
  `Tok::Ident("NULL")` (the SQL parser accepts `NULL` as a keyword
  in literal positions).
- `Tok::Param(n)` where `params` has fewer than `n` entries →
  `SqlError::UnboundParameter(n)`.
- `Tok::Param(n)` where `n == 0` → `SqlError::ZeroParamIndex` (the
  lexer already rejects `$0`, so this branch is defensive).

### 3.2 What about NULL?

PG NULL is the wire `length=-1` sentinel. The gateway's existing
`PreparedParam::Null` renders as the bare `NULL` keyword in the
text path. In the typed path, the entry point accepts
`params: &[Option<Value>]` — `None` at index `n-1` injects
`Tok::Ident("NULL")` into the stream, which the SQL parser
already accepts in literal positions.

### 3.3 The gateway bridge

`extq::substitute::preprocess_params` already classifies each
parameter into `PreparedParam::{Null, Text(bytes), Raw(sql)}`. The
typed-param path needs a similar classifier that emits
`Option<Value>` instead. Per-type rules:

| Bind format + OID | Wire bytes | Typed `Option<Value>` |
|---|---|---|
| NULL (length=-1) | — | `None` |
| text format, no OID | `b"42"` | `Some(Value::Blob(b"42".to_vec()))` — let the parser coerce |
| text format, INT4/INT8/INT2 OID | `b"42"` | `Some(Value::Int(42))` if parses, else `Value::Blob` fallback |
| text format, BOOL OID | `b"true"`/`b"false"` | `Some(Value::Uint(1))`/`Value::Uint(0)` |
| text format, TEXT/VARCHAR OID | `b"foo"` | `Some(Value::Blob(b"foo".to_vec()))` |
| binary format, INT2/INT4/INT8 | BE bytes | decode → `Some(Value::Int(n))` |
| binary format, FLOAT4/FLOAT8 | BE bytes | text format fallback (V1 doesn't add Value::Float) |
| binary format, BYTEA | raw bytes | `Some(Value::Blob(bytes.to_vec()))` |
| binary format, TIMESTAMPTZ | i64 µs | text format fallback (text path emits `'ISO'::timestamptz`) |
| binary format, BOOL | 0x00/0x01 | `Some(Value::Uint(0))`/`Value::Uint(1)` |

Where the typed path can't represent a parameter cleanly (FLOAT,
TIMESTAMPTZ, NUMERIC) V1 falls back to the text-substitution path
for that whole Bind. This keeps the V1 shape backwards-compatible
while letting the typed path handle the common int/text/bytea/bool
cases that cover ~90% of real ORM Bind traffic.

## 4. Task decomposition (T1..T4)

| T# | Scope | KAT delta |
|---|---|---|
| **T1** | (this commit) Design spec + progress tracker + lexer extension (`Tok::Param(u16)` recognition for `$1..$99` + `$0` error + `$100+` error + bare-`$` error) + 7 lexer KATs locking `$N` token shape. Parser still rejects `Tok::Param` in any position — until T2 lands, a `Tok::Param` reaching the parser falls through to the existing `_ => Err(...)` arms. | +7 |
| **T2** | `compile_with_params(sql, cat, params: &[Option<Value>])` entry point + typed-param token rewrite + KATs covering INSERT VALUES, UPDATE SET, WHERE predicate, SELECT, NULL injection, out-of-bounds rejection, multi-row VALUES, JOIN ON, AND the headline quote-injection adversarial KAT. The existing `compile`/`compile_stmt` stay backward-compatible; SQL containing `$N` without bound values returns `SqlError::UnboundParameter`. | +8-12 |
| **T3** | Gateway scaffold — opt-in route through `compile_with_params` for the int/text/bytea/bool subset; text-substitution path stays as default fallback. KATs: typed path covers `pgJDBC setInt(1, 42)` + `psycopg2 (b"hello",)`; text path unchanged for FLOAT/TIMESTAMPTZ. NO default flip (still text by default; the typed path is exercised only by the KAT harness). | +4-6 |
| **T4** | USAGE §9 note + STATUS row + progress tracker → CLOSED. | +0 |

Estimated V1 total: **+19-25 KATs across 4 slices**.

## 5. Acceptance criteria

V1 (T1-T4) ships when:

1. **Lexer recognizes `$1..$99` as `Tok::Param`** — locked KAT.
2. **`Tok::Param` in a value position parses as the substituted
   literal under `compile_with_params`** — locked KAT.
3. **`compile_with_params(sql, cat, &[Some(Value::Int(42))])` for
   `"SELECT * FROM t WHERE id = $1"` emits the same `Op` as
   `compile("SELECT * FROM t WHERE id = 42", cat)`** — byte-equal
   regression check.
4. **Quote-injection adversarial KAT**: `compile_with_params(
   "SELECT * FROM t WHERE name = $1", cat, &[Some(Value::Blob(b"'; DROP
   TABLE t; --".to_vec()))])` produces an Op where the bound value
   is a `Value::Blob` operand at the EQ comparison, NOT a SQL string
   that the parser would re-parse. The DROP TABLE never reaches the
   engine.
5. **Out-of-bounds `$N`** returns `SqlError::UnboundParameter(n)`.
6. **Bare `compile`** still works on SQL without `$N` (regression
   check).
7. **All existing kessel-sql + kessel-pg-gateway KATs pass byte-
   equal** — no engine-side or wire-side regression.
8. **No new external dep.** `#![forbid(unsafe_code)]` honored. CI
   green.

## 6. References

- SP-PG-EXTQ V1 design spec:
  `docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
- SP-PG-EXTQ V1 progress tracker (CLOSED):
  `docs/superpowers/specs/2026-05-28-kesseldb-subproject-sppgextq-progress.md`
- `crates/kessel-sql/src/lib.rs` — lexer + parser + compile
- `crates/kessel-pg-gateway/src/extq/substitute.rs` — text-
  substitution helper (kept; not replaced)
- `crates/kessel-pg-gateway/src/extq/mod.rs::dispatch_execute` —
  Execute pipeline that calls substitute; SP-PG-EXTQ-PARSED T3
  adds an opt-in branch
