//! kessel-sql: a minimal SQL text layer that compiles single statements to
//! KesselDB `Op`s. Catalog-aware (resolves table/column names, encodes
//! values via the codec, compiles WHERE to a deterministic kessel-expr
//! program). Deliberately a constrained, well-defined subset — every
//! supported form maps cleanly onto an existing Op; nothing is faked.

#![forbid(unsafe_code)]

use kessel_catalog::{
    encode_field, encode_type_def, Catalog, Field, FieldKind, ObjectType,
};
use kessel_codec::{encode, Value};
use kessel_expr::Program;
use kessel_proto::{ObjectId, Op};

pub type SqlError = String;

// ---------------------------------------------------------------- tokenizer
#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Ident(String),
    Int(i128),
    Str(String),
    /// SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 — raw-bytes literal that
    /// preserves arbitrary byte sequences (including non-UTF8) for
    /// param-bound BYTES/CHAR column values. NOT producible by the
    /// lexer (no surface syntax binds to it); reserved exclusively
    /// for the `rewrite_param_tokens` blob path. Value-position
    /// parsers (INSERT VALUES, UPDATE SET, WHERE comparison RHS)
    /// accept it alongside `Tok::Str` and route to `Lit::Bytes`.
    Bytes(Vec<u8>),
    Punct(char),  // ( ) , * ;
    Cmp(&'static str), // = != < <= > >=
    Plus,
    Minus,
    Star,
    /// SP-PG-EXTQ-PARSED T1 — `$N` 1-based positional parameter
    /// placeholder. `N` is in the range [1, 99]. Recognized by the
    /// lexer; T2 wires `compile_with_params` to resolve to a typed
    /// `Value` BEFORE the parser runs (no SQL text concatenation;
    /// closes the SP-PG-EXTQ V1 §11 weak-spot #1 attack surface).
    /// Until T2 lands, a `Tok::Param` reaching the parser falls
    /// through to the existing `_ => Err(...)` arms.
    Param(u16),
}

fn lex(s: &str) -> Result<Vec<Tok>, SqlError> {
    let b = s.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i] as char;
        if c.is_whitespace() {
            i += 1;
        } else if c == '\'' {
            // SP-PG-ORM-REALAPP — SQL-standard string literal with the
            // doubled-quote escape. PG (§4.1.2.1 / SQL-92) escapes a literal
            // single quote inside a string by DOUBLING it: `'bob''s post'`
            // is the four-character value `bob's post`. ANY real app whose
            // data contains an apostrophe emits this (psycopg2's own literal
            // quoting produces `''`), so the previous lexer — which stopped
            // at the first inner `'` — silently truncated the string and
            // then choked on the trailing `s post'`. We now mirror the `"`
            // delimited-identifier escape below: a doubled `''` stays in the
            // string and appends a single `'`; a lone `'` closes it. A
            // string with NO embedded quote is byte-identical to the
            // pre-arc token, so every prior KAT keeps passing.
            i += 1;
            let mut st = String::new();
            loop {
                if i >= b.len() {
                    return Err("unterminated string".into());
                }
                if b[i] as char == '\'' {
                    // Doubled `''` is an escaped literal quote — stay in the
                    // string and append a single `'`.
                    if i + 1 < b.len() && b[i + 1] as char == '\'' {
                        st.push('\'');
                        i += 2;
                        continue;
                    }
                    // Lone `'` closes the string.
                    i += 1;
                    break;
                }
                st.push(b[i] as char);
                i += 1;
            }
            out.push(Tok::Str(st));
        } else if c == '"' {
            // SP-PG-SQL-QUOTED-IDENT — SQL-standard delimited identifier.
            // Django (and any client using `quote_name`) double-quotes
            // EVERY identifier (`"smokeapp_author"."id"`, `"name"`, …).
            // A delimited identifier is CASE-PRESERVING and may contain
            // characters a bare identifier can't (spaces, reserved-word
            // spellings). The escape for a literal `"` inside the
            // identifier is a doubled `""` (PG §4.1.1 / SQL-92).
            //
            // We emit a plain `Tok::Ident(contents)` — byte-identical to
            // the token a bare identifier of the same (already-correct
            // case) name produces. Because KesselDB's catalog stores
            // names case-sensitively and bare identifiers are NOT folded
            // (keyword recognition is the only case-insensitive step, via
            // `eq_ignore_ascii_case`), a quoted `"id"` and a bare `id`
            // resolve to the SAME catalog column. This is exactly the
            // round-trip Django needs: it quotes the SAME names in its
            // DDL and its DML, so `CREATE TABLE "t" ("id" …)` and
            // `INSERT INTO "t" ("id") …` agree on the identifier string.
            //
            // V1 limitation (documented in the design spec §3): because a
            // quoted identifier lowers to the same `Tok::Ident` as a bare
            // one, a delimited identifier that SPELLS a reserved keyword
            // in a position where the grammar also accepts that keyword
            // (`SELECT "from" FROM t` with a column literally named
            // `from`) is not distinguishable from the keyword. No ORM
            // emits this and KesselDB's catalog rejects keyword-spelled
            // bare names at CREATE time, so it does not arise in practice;
            // the strict fix is the follow-up `SP-PG-SQL-QUOTED-KEYWORD`.
            i += 1;
            let mut st = String::new();
            loop {
                if i >= b.len() {
                    return Err("unterminated quoted identifier".into());
                }
                if b[i] as char == '"' {
                    // Doubled `""` is an escaped literal quote — stay in
                    // the identifier and append a single `"`.
                    if i + 1 < b.len() && b[i + 1] as char == '"' {
                        st.push('"');
                        i += 2;
                        continue;
                    }
                    // Lone `"` closes the identifier.
                    i += 1;
                    break;
                }
                st.push(b[i] as char);
                i += 1;
            }
            if st.is_empty() {
                // PG rejects `""` (a zero-length delimited identifier).
                return Err("zero-length delimited identifier".into());
            }
            out.push(Tok::Ident(st));
        } else if c.is_ascii_digit() || (c == '-' && i + 1 < b.len() && (b[i + 1] as char).is_ascii_digit()
            && matches!(out.last(), None | Some(Tok::Punct('(')) | Some(Tok::Punct(',')) | Some(Tok::Cmp(_)))) {
            let start = i;
            if c == '-' {
                i += 1;
            }
            while i < b.len() && (b[i] as char).is_ascii_digit() {
                i += 1;
            }
            let n: i128 = s[start..i].parse().map_err(|_| "bad number".to_string())?;
            out.push(Tok::Int(n));
        } else if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < b.len() && {
                let ch = b[i] as char;
                ch.is_alphanumeric() || ch == '_'
            } {
                i += 1;
            }
            out.push(Tok::Ident(s[start..i].to_string()));
        } else if c == '$' {
            // SP-PG-EXTQ-PARSED T1 — `$N` positional-parameter
            // placeholder. Greedy decimal-digit scan after the `$`.
            // V1 caps N at 99 (matches the SP-PG-EXTQ-BIN T2 SQL-
            // text scanner; `MAX_PARAMETERS_PER_BIND` on the wire
            // is 65535 but the practical ORM cap is well below 99
            // and the cap lets us keep `n: u16` without overflow
            // worry). `$0` is rejected (PG `$N` is 1-based). Bare
            // `$` with no following digit is rejected — the lexer
            // is strict so a typo doesn't silently become an
            // identifier. The gateway-side text scanner is
            // permissive (passes bare `$` through verbatim) because
            // it processes pre-parsed SQL bytes; here we're the
            // parser-side authority.
            let start = i;
            i += 1;
            let digit_start = i;
            while i < b.len() && (b[i] as char).is_ascii_digit() {
                i += 1;
            }
            if i == digit_start {
                return Err(format!(
                    "expected digit after `$` (got `{}`)",
                    &s[start..(digit_start + 1).min(b.len())]
                ));
            }
            let n: u32 = s[digit_start..i].parse().map_err(|_| {
                "bad parameter index after `$`".to_string()
            })?;
            if n == 0 {
                return Err("`$0` is invalid (PG `$N` indices are 1-based)".into());
            }
            if n > 99 {
                return Err(format!("`${n}` exceeds the V1 limit of 99 parameters per statement"));
            }
            out.push(Tok::Param(n as u16));
        } else {
            match c {
                // SP-PG-SQL-ORM-PARSE T4 — `[` / `]` lexed as punctuation
                // so `ARRAY[...]` (SQLAlchemy's `create_all` relkind probe
                // `relkind = ANY (ARRAY['r','p',...])` + general IN-list
                // lowering) tokenizes instead of hitting `unexpected char
                // '['`. Bare `[`/`]` outside `ARRAY[...]` is a parse error
                // downstream (no other grammar consumes them).
                '(' | ')' | ',' | ';' | '.' | '[' | ']' => {
                    out.push(Tok::Punct(c));
                    i += 1;
                }
                '*' => {
                    out.push(Tok::Star);
                    i += 1;
                }
                '+' => {
                    out.push(Tok::Plus);
                    i += 1;
                }
                '-' => {
                    out.push(Tok::Minus);
                    i += 1;
                }
                '=' => {
                    out.push(Tok::Cmp("="));
                    i += 1;
                }
                '!' if i + 1 < b.len() && b[i + 1] as char == '=' => {
                    out.push(Tok::Cmp("!="));
                    i += 2;
                }
                // SP-PG-SQL-HAVING — SQL-standard `<>` inequality (alongside the
                // existing `!=`). Recognized BEFORE the bare `<` arm so the two
                // characters lex as ONE comparison token. The WHERE/HAVING
                // parsers map both `<>` and `!=` to the same comparison.
                '<' if i + 1 < b.len() && b[i + 1] as char == '>' => {
                    out.push(Tok::Cmp("<>"));
                    i += 2;
                }
                '<' if i + 1 < b.len() && b[i + 1] as char == '=' => {
                    out.push(Tok::Cmp("<="));
                    i += 2;
                }
                '>' if i + 1 < b.len() && b[i + 1] as char == '=' => {
                    out.push(Tok::Cmp(">="));
                    i += 2;
                }
                '<' => {
                    out.push(Tok::Cmp("<"));
                    i += 1;
                }
                '>' => {
                    out.push(Tok::Cmp(">"));
                    i += 1;
                }
                _ => return Err(format!("unexpected char '{c}'")),
            }
        }
    }
    Ok(out)
}

// ------------------------------------------------------------------ parser
struct P<'a> {
    t: Vec<Tok>,
    i: usize,
    cat: &'a Catalog,
}

impl<'a> P<'a> {
    fn peek(&self) -> Option<&Tok> {
        self.t.get(self.i)
    }
    fn next(&mut self) -> Option<Tok> {
        let v = self.t.get(self.i).cloned();
        if v.is_some() {
            self.i += 1;
        }
        v
    }
    fn kw(&mut self, k: &str) -> bool {
        if let Some(Tok::Ident(s)) = self.peek() {
            if s.eq_ignore_ascii_case(k) {
                self.i += 1;
                return true;
            }
        }
        false
    }
    fn expect_kw(&mut self, k: &str) -> Result<(), SqlError> {
        if self.kw(k) {
            Ok(())
        } else {
            Err(format!("expected `{k}`"))
        }
    }
    fn ident(&mut self) -> Result<String, SqlError> {
        match self.next() {
            Some(Tok::Ident(s)) => Ok(s),
            _ => Err("expected identifier".into()),
        }
    }
    /// SP-PG-SQL-ORM-PARSE T2 — a column reference that MAY be qualified
    /// with a table name or alias: `IDENT (DOT IDENT)?`. Returns the
    /// LAST ident (the bare column name). The qualifier is accepted and
    /// IGNORED in V1 (lenient): `orm_users.id`, `t.id`, and bare `id`
    /// all resolve to the column `id`. SQLAlchemy / Django / Rails ALL
    /// qualify every column with the table name, so lenient acceptance
    /// maximizes ORM compatibility. Strict validation (reject
    /// `wrong_table.id`) is the named follow-up `SP-PG-SQL-QUALIFIER-
    /// STRICT`. A bare `IDENT` with no trailing `.IDENT` is byte-
    /// identical to the old `ident()` path, so every prior KAT that fed
    /// unqualified columns produces the SAME compiled Op.
    fn col_ident(&mut self) -> Result<String, SqlError> {
        let first = self.ident()?;
        if matches!(self.peek(), Some(Tok::Punct('.'))) {
            self.i += 1; // consume `.`
            // `t.col` — the qualifier `first` is the table/alias; the
            // column is the second ident. (A third `.` — schema-
            // qualified `db.t.col` — is not produced by the ORMs we
            // target; reject so a typo doesn't silently swallow tokens.)
            let col = self.ident()?;
            if matches!(self.peek(), Some(Tok::Punct('.'))) {
                return Err(
                    "schema-qualified column `a.b.c` not supported (V1 \
                     accepts `table.col` or bare `col`)"
                        .into(),
                );
            }
            Ok(col)
        } else {
            Ok(first)
        }
    }
    fn punct(&mut self, c: char) -> Result<(), SqlError> {
        match self.next() {
            Some(Tok::Punct(p)) if p == c => Ok(()),
            _ => Err(format!("expected `{c}`")),
        }
    }
    fn type_named(&self, name: &str) -> Result<&'a ObjectType, SqlError> {
        self.cat
            .types
            .iter()
            .find(|t| t.name == name)
            .ok_or_else(|| {
                let candidates: Vec<&str> =
                    self.cat.types.iter().map(|t| t.name.as_str()).collect();
                match suggest(name, &candidates) {
                    Some(s) => {
                        format!("unknown table `{name}` — did you mean `{s}`?")
                    }
                    None if candidates.is_empty() => format!(
                        "unknown table `{name}` (no tables defined yet — use \
                         CREATE TABLE first)"
                    ),
                    None => format!("unknown table `{name}`"),
                }
            })
    }
}

/// SP-PG-ORM-RELATIONSHIPS / SP-PG-DDL-FK-ENFORCE — consume trailing
/// referential-action clauses on a `REFERENCES`/`FOREIGN KEY` constraint:
/// `ON DELETE <action>` / `ON UPDATE <action>` where `<action>` is
/// `CASCADE`, `RESTRICT`, `NO ACTION`, `SET NULL`, or `SET DEFAULT`.
///
/// Returns the engine `on_delete` action code parsed from the `ON DELETE`
/// clause (the engine enforces referential integrity since
/// SP-PG-DDL-FK-ENFORCE): `0=NO ACTION 1=RESTRICT 2=CASCADE 3=SET NULL
/// 4=SET DEFAULT`. The default (no `ON DELETE` clause) is `0` (NO ACTION),
/// matching PostgreSQL. `ON UPDATE` actions are still parsed-and-IGNORED
/// (the engine does not yet enforce ON UPDATE — named follow-up
/// `SP-PG-DDL-FK-ON-UPDATE`). Stops at the first token that is not part of
/// an `ON DELETE/UPDATE` clause, leaving the cursor for the caller's
/// `,`/`)` handling.
fn parse_referential_actions(p: &mut P<'_>) -> u8 {
    let mut on_delete: u8 = 0; // NO ACTION default (PostgreSQL semantics)
    while p.kw("ON") {
        // `DELETE` | `UPDATE`
        let is_delete = p.kw("DELETE");
        if !is_delete {
            let _ = p.kw("UPDATE");
        }
        // `NO ACTION` | `SET NULL` | `SET DEFAULT` | `CASCADE` | `RESTRICT`
        let action: u8 = if p.kw("NO") {
            let _ = p.kw("ACTION");
            0
        } else if p.kw("SET") {
            if p.kw("NULL") {
                3
            } else {
                let _ = p.kw("DEFAULT");
                4
            }
        } else if p.kw("CASCADE") {
            2
        } else if p.kw("RESTRICT") {
            1
        } else {
            // No recognized action keyword followed `ON DELETE/UPDATE`;
            // treat as NO ACTION (don't consume a non-action token — the
            // caller's `,`/`)` handling needs it). Matches the pre-arc
            // skip semantics for the common ORM-emitted clauses.
            0
        };
        // Only ON DELETE drives the engine action; ON UPDATE is ignored.
        if is_delete {
            on_delete = action;
        }
    }
    on_delete
}

/// Return the best near-match for `name` from `candidates`, or `None` if
/// none is close enough. Zero-dep: case-insensitive prefix match wins over
/// edit-distance ≤ 2 (Damerau-Levenshtein-lite over ASCII). Designed so
/// the suggestion never embarrasses us with a wildly unrelated string.
///
/// Public so the SQL layer's other "unknown X" sites can reuse the same
/// suggestion shape, and so the server-side `apply_one` path can wrap the
/// raw `compile_stmt` error in a richer message later if it wants to.
pub fn suggest<'a>(name: &str, candidates: &'a [&'a str]) -> Option<&'a str> {
    if candidates.is_empty() {
        return None;
    }
    let needle = name.to_ascii_lowercase();
    // 1) Exact case-insensitive: if the user typed wrong case, suggest the
    //    canonical spelling.
    for &c in candidates {
        if c.eq_ignore_ascii_case(name) && c != name {
            return Some(c);
        }
    }
    // 2) Case-insensitive prefix or substring containment (length ≥ 3 so
    //    we don't suggest every short noise match).
    if needle.len() >= 3 {
        for &c in candidates {
            let cl = c.to_ascii_lowercase();
            if cl.starts_with(&needle) || needle.starts_with(&cl) {
                return Some(c);
            }
        }
    }
    // 3) Edit distance ≤ max(1, len/4). Picks the lexicographically first
    //    among ties so suggestions are deterministic.
    let max_edits = (name.len() / 4).max(1);
    let mut best: Option<(&str, usize)> = None;
    for &c in candidates {
        let d = edit_distance(&needle, &c.to_ascii_lowercase(), max_edits + 1);
        if d <= max_edits {
            match best {
                None => best = Some((c, d)),
                Some((_, bd)) if d < bd => best = Some((c, d)),
                _ => {}
            }
        }
    }
    best.map(|(c, _)| c)
}

/// Render an "unknown column `col` on table `t`" error with a did-you-mean
/// suggestion from the table's actual column list. Centralized so every
/// `unknown column` site in this crate emits the same shape; safe to call
/// even when the table has zero columns. Public for use in tests.
pub fn unknown_column_err(col: &str, ot: &ObjectType) -> String {
    let candidates: Vec<&str> =
        ot.fields.iter().map(|f| f.name.as_str()).collect();
    match suggest(col, &candidates) {
        Some(s) => format!(
            "unknown column `{col}` on table `{t}` — did you mean `{s}`?",
            t = ot.name
        ),
        None => {
            // Inline up to 4 column names so users see the shape without
            // an extra DESCRIBE round-trip.
            let mut hint = String::new();
            let mut first = true;
            for c in candidates.iter().take(4) {
                if first {
                    hint.push_str("; have: ");
                    first = false;
                } else {
                    hint.push_str(", ");
                }
                hint.push('`');
                hint.push_str(c);
                hint.push('`');
            }
            if candidates.len() > 4 {
                hint.push_str(", …");
            }
            format!(
                "unknown column `{col}` on table `{t}`{hint}",
                t = ot.name
            )
        }
    }
}

/// Bounded Levenshtein distance — returns `cap` as soon as the running
/// distance can no longer fall below it. Two-row DP, O(a.len()*b.len())
/// time, O(min(a,b)) space. Pure, zero-dep.
fn edit_distance(a: &str, b: &str, cap: usize) -> usize {
    let av: Vec<u8> = a.bytes().collect();
    let bv: Vec<u8> = b.bytes().collect();
    let (a, b) = if av.len() < bv.len() { (&av, &bv) } else { (&bv, &av) };
    if b.len() - a.len() >= cap {
        return cap;
    }
    let mut prev: Vec<usize> = (0..=a.len()).collect();
    let mut curr: Vec<usize> = vec![0; a.len() + 1];
    for (j, bj) in b.iter().enumerate() {
        curr[0] = j + 1;
        let mut row_min = curr[0];
        for (i, ai) in a.iter().enumerate() {
            let cost = if ai.eq_ignore_ascii_case(bj) { 0 } else { 1 };
            curr[i + 1] = (prev[i + 1] + 1)
                .min(curr[i] + 1)
                .min(prev[i] + cost);
            if curr[i + 1] < row_min {
                row_min = curr[i + 1];
            }
        }
        if row_min >= cap {
            return cap;
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[a.len()].min(cap)
}

fn kind_of(name: &str, arg: Option<i128>) -> Result<FieldKind, SqlError> {
    Ok(match name.to_ascii_uppercase().as_str() {
        "U8" => FieldKind::U8,
        "U16" => FieldKind::U16,
        "U32" => FieldKind::U32,
        "U64" => FieldKind::U64,
        "U128" => FieldKind::U128,
        "I8" => FieldKind::I8,
        "I16" => FieldKind::I16,
        "I32" => FieldKind::I32,
        "I64" => FieldKind::I64,
        "I128" => FieldKind::I128,
        "BOOL" => FieldKind::Bool,
        "TS" | "TIMESTAMP" => FieldKind::Timestamp,
        "REF" => FieldKind::Ref,
        "CHAR" => FieldKind::Char(arg.ok_or("CHAR needs (n)")? as u16),
        "BYTES" => FieldKind::Bytes(arg.ok_or("BYTES needs (n)")? as u16),
        // SP-PG-CAT-T8 — canonical PostgreSQL type aliases. A real
        // psql / pgcli / JDBC client sends `BIGINT` / `INTEGER` /
        // `SMALLINT` / `BOOLEAN` (PG SQL-standard names) instead of
        // KesselDB's narrow integer-width spellings. Pure alias: same
        // FieldKind, same on-wire layout, same MVCC semantics.
        // KesselDB's `I8`/`I16`/`I32` already use those spellings for
        // their narrow widths, so PG's internal `INT8`/`INT4`/`INT2`
        // names are NOT aliased here (would collide).
        "BIGINT" => FieldKind::I64,
        "INTEGER" | "INT" => FieldKind::I32,
        "SMALLINT" => FieldKind::I16,
        "BOOLEAN" => FieldKind::Bool,
        // SP-PG-ORM-SQLALCHEMY — `VARCHAR(n)` DDL alias. A SQLAlchemy
        // `Column(String(32))` (and Django/Rails/Diesel string columns)
        // renders `VARCHAR(32)` in `CREATE TABLE`, which previously hit
        // the `unknown type` arm and broke the entire ORM `create_all`
        // DDL path. Aliased to `Char(n)` — the existing fixed-width CHAR
        // FieldKind — same on-wire layout, same MVCC semantics, and the
        // gateway already encodes the result/cast OID as `varchar` (1043)
        // on the read side (`cast_stripper` / `binary_results`). The
        // CHAR-pad vs VARCHAR-trim semantic difference is the named
        // follow-up `SP-PG-DDL-VARCHAR-NATIVE` (true variable-length
        // storage); for fixed-bound `String(n)` columns the alias is a
        // faithful match. Multi-word `CHARACTER VARYING` and bare
        // unbounded `VARCHAR`/`TEXT` are NOT handled here (single-token,
        // `(n)`-required) — named follow-up `SP-PG-DDL-VARCHAR-UNBOUNDED`.
        "VARCHAR" => FieldKind::Char(arg.ok_or("VARCHAR needs (n)")? as u16),
        // SP-PG-SQL-ORM-PARSE T5 — SERIAL-family DDL aliases. SQLAlchemy
        // 2.0 renders a `BigInteger`/`Integer` PRIMARY KEY as
        // `BIGSERIAL`/`SERIAL` (autoincrement) in `create_all`. KesselDB
        // has no sequence/autoincrement engine yet, so these alias to the
        // plain integer width: BIGSERIAL→I64, SERIAL→I32, SMALLSERIAL→I16.
        // The row's `id` is the ObjectId pseudo-PK, which the ORM supplies
        // EXPLICITLY for this model (`User(id=1, …)`), so dropping the
        // auto-sequence is faithful for explicit-id inserts. Server-
        // generated autoincrement + `INSERT … RETURNING id` is the named
        // follow-up `SP-PG-SERIAL` / `SP-PG-RETURNING` (needed only when a
        // model omits the PK and relies on the DB to assign it). Pure
        // alias: same FieldKind / on-wire layout as the BIGINT/INTEGER
        // names already mapped above.
        "BIGSERIAL" | "SERIAL8" => FieldKind::I64,
        "SERIAL" | "SERIAL4" => FieldKind::I32,
        "SMALLSERIAL" | "SERIAL2" => FieldKind::I16,
        other => return Err(format!("unknown type `{other}`")),
    })
}

/// SP-PG-SERIAL-RETURNING: is `tname` a SERIAL-family type name? Case-
/// insensitive, matches the aliases handled by `kind_of`. A SERIAL
/// column that is also the PRIMARY KEY becomes a deterministic
/// autoincrement (the engine assigns the value); a SERIAL column that is
/// NOT the PK is a plain integer the caller must supply (V1 — the non-PK
/// serial case is the named follow-up `SP-PG-SERIAL-NONPK`).
pub fn is_serial_type(tname: &str) -> bool {
    matches!(
        tname.to_ascii_uppercase().as_str(),
        "BIGSERIAL" | "SERIAL8" | "SERIAL" | "SERIAL4" | "SMALLSERIAL" | "SERIAL2"
    )
}

/// A compiled statement. Most map to a single `Op`; `UPDATE` needs a
/// server-side read-modify-write (the engine reads the current row, applies
/// the SET list, re-encodes) so it is its own variant. `Clone` so a
/// compiled statement can be cached and replayed (SP47).
#[derive(Clone)]
pub enum Stmt {
    Op(Op),
    Update {
        type_id: u32,
        id: u128,
        sets: Vec<(u16, Value)>,
    },
    /// SP-PG-SQL-DML-GENERAL — general-WHERE UPDATE (`UPDATE t SET … WHERE
    /// <pred>`, multi-row). `program` is the kessel-expr predicate
    /// (the SAME bytes `Op::Select`/`Op::QueryExpr` consume). The server
    /// resolves the matching ids via `Op::QueryExpr`, then builds a
    /// concrete `Op::Txn` of per-id `Op::UpdateSet` — the replicated
    /// artifact is the concrete Txn (Path A; see the
    /// SP-PG-SQL-DML-GENERAL design). `returning` is `None` (no clause),
    /// `Some(["*"])` (star sentinel), or `Some([col, …])`.
    UpdateWhere {
        type_id: u32,
        program: Vec<u8>,
        sets: Vec<(u16, Value)>,
        returning: Option<Vec<String>>,
        /// SP-PG-SQL-DML-GENERAL — when `Some(id)`, the matched row set is
        /// EXACTLY this one primary-key id (a by-PK `WHERE id = n` that
        /// carried a RETURNING clause): the server skips the predicate
        /// scan and mutates the single id directly. `None` ⇒ resolve the
        /// matches by scanning `program`.
        by_pk_id: Option<u128>,
    },
    /// SP-PG-SQL-DML-GENERAL — general-WHERE DELETE (`DELETE FROM t WHERE
    /// <pred>`, multi-row). Same Path-A shape as `UpdateWhere` but the
    /// inner ops are `Op::Delete`.
    DeleteWhere {
        type_id: u32,
        program: Vec<u8>,
        returning: Option<Vec<String>>,
        /// See `UpdateWhere::by_pk_id` — `Some(id)` ⇒ delete exactly this
        /// primary-key row (a by-PK `WHERE id = n RETURNING`).
        by_pk_id: Option<u128>,
    },
    /// `EXPLAIN <stmt>` — a precomputed, human-readable query plan. The
    /// inner statement is *not* executed; the server just returns this
    /// text. Pure planner output (SP64).
    Explain(String),
}

/// If `sql` is a whole-row, single-table select
/// (`SELECT * FROM <table> ...`, i.e. no projection list and no `JOIN`),
/// return the source table name so a client can `DESCRIBE` it and decode
/// the returned rows. `None` for projections, joins, aggregates or
/// non-selects (the caller then leaves the bytes opaque). Uses the real
/// lexer — no string heuristics.
pub fn select_star_table(sql: &str) -> Option<String> {
    let toks = lex(sql).ok()?;
    let mut it = toks.iter();
    match it.next()? {
        Tok::Ident(k) if k.eq_ignore_ascii_case("SELECT") => {}
        _ => return None,
    }
    match it.next()? {
        Tok::Star => {}
        _ => return None, // projection list ⇒ not a whole-row select
    }
    match it.next()? {
        Tok::Ident(k) if k.eq_ignore_ascii_case("FROM") => {}
        _ => return None,
    }
    let table = match it.next()? {
        Tok::Ident(t) => t.clone(),
        _ => return None,
    };
    // A JOIN (`JOIN` or `LEFT [OUTER] JOIN`) produces composite rows
    // (different wire shape) — bail out. (SP-PG-SQL-OUTER-JOIN: `LEFT` is the
    // first token after the table for a LEFT join, so check it too.)
    if let Some(Tok::Ident(k)) = it.next() {
        if k.eq_ignore_ascii_case("JOIN") || k.eq_ignore_ascii_case("LEFT") {
            return None;
        }
    }
    Some(table)
}

/// If `sql` is a plain projection `SELECT c1, c2, ... FROM <table> ...`
/// (explicit column list, single table, no `*`, no aggregate function
/// call, no `JOIN`), return `(table, [c1, c2, ...])` so a client can
/// `DESCRIBE` the table and decode the projected (column-oriented) result.
/// `None` otherwise (caller leaves the bytes opaque). Uses the real lexer.
pub fn select_columns(sql: &str) -> Option<(String, Vec<String>)> {
    let toks = lex(sql).ok()?;
    let mut it = toks.iter().peekable();
    match it.next()? {
        Tok::Ident(k) if k.eq_ignore_ascii_case("SELECT") => {}
        _ => return None,
    }
    let mut cols = Vec::new();
    loop {
        match it.next()? {
            Tok::Ident(c) if !c.eq_ignore_ascii_case("FROM") => {
                // SP-PG-SQL-ORM-PARSE T3 — qualified projection column
                // `table.col` (the ORM's actual shape): if a `.` follows,
                // consume it + the bare column ident and use the column
                // name (lenient qualifier, matches the parser's
                // `col_ident`). The gateway renders the column by its
                // bare name, so the RowDescription matches the engine's
                // `Op::SelectFields` projected output order.
                let mut col = c.clone();
                if matches!(it.peek(), Some(Tok::Punct('.'))) {
                    it.next(); // consume `.`
                    match it.next()? {
                        Tok::Ident(real) => col = real.clone(),
                        _ => return None,
                    }
                }
                // `FUNC(` ⇒ aggregate/expr — not a plain column list.
                if matches!(it.peek(), Some(Tok::Punct('('))) {
                    return None;
                }
                // SP-PG-SERIAL-RETURNING (incidental ORM unblock): a
                // column may carry an output alias `col AS alias`
                // (SQLAlchemy's post-flush refresh SELECT emits `SELECT
                // widgets.id AS widgets_id, …`). Accept-and-skip the
                // alias — V1 projects + names by the SOURCE column (the
                // engine's projected output order is by source column;
                // result mapping is positional). True alias-named
                // RowDescription output is the named follow-up
                // `SP-PG-SQL-PROJ-ALIAS`.
                if matches!(it.peek(), Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("AS"))
                {
                    it.next(); // consume `AS`
                    match it.next()? {
                        Tok::Ident(_alias) => {}
                        _ => return None,
                    }
                }
                cols.push(col);
            }
            _ => return None, // `*`, `FROM` with no cols, etc.
        }
        match it.next()? {
            Tok::Punct(',') => continue,
            Tok::Ident(k) if k.eq_ignore_ascii_case("FROM") => break,
            _ => return None,
        }
    }
    let table = match it.next()? {
        Tok::Ident(t) => t.clone(),
        _ => return None,
    };
    if let Some(Tok::Ident(k)) = it.next() {
        if k.eq_ignore_ascii_case("JOIN") || k.eq_ignore_ascii_case("LEFT") {
            return None; // composite rows (incl. LEFT JOIN) — different wire shape
        }
    }
    Some((table, cols))
}

/// SP-PG-ORM-REALAPP — does a single-table projection-list SELECT carry an
/// `ORDER BY` clause? `select_columns` accepts both `SELECT c1, c2 FROM t`
/// and `SELECT c1, c2 FROM t ORDER BY c LIMIT n` (it stops at the table name),
/// but the engine compiles the two DIFFERENTLY: a plain projection lowers to
/// `Op::SelectFields` (which emits a NARROW projected-byte stream), while an
/// `ORDER BY` projection lowers to `Op::SelectSorted` (which emits the FULL
/// record stream — the projection is dropped at the engine layer, the sort
/// wins the `match` arm). The gateway needs to know which stream shape it got
/// so it can decode correctly. Returns `true` IFF `sql` is a single-table
/// projection-list SELECT (NOT `*`, NOT an aggregate, NOT a JOIN) that has a
/// trailing `ORDER BY`. Uses the real lexer — no string heuristics.
pub fn select_projection_is_sorted(sql: &str) -> bool {
    // Must first be a plain single-table projection list (reuses the exact
    // acceptance of `select_columns`; if that returns None this is not our
    // shape and we report false).
    if select_columns(sql).is_none() {
        return false;
    }
    let toks = match lex(sql) {
        Ok(t) => t,
        Err(_) => return false,
    };
    // Any top-level `ORDER` keyword after the projection implies the sorted
    // lowering. (A projection list cannot contain a bare `ORDER` column —
    // `select_columns` would have rejected a keyword-spelled column, and the
    // engine's catalog rejects keyword-named columns at CREATE time.)
    toks.iter()
        .any(|t| matches!(t, Tok::Ident(k) if k.eq_ignore_ascii_case("ORDER")))
}

/// SP-PG-NULL-INT-RENDER — rewrite a single-table projection-list SELECT
/// (`SELECT c1, c2 FROM t [WHERE …]`) into its `SELECT * FROM t [WHERE …]`
/// equivalent, preserving everything from the FROM clause onward verbatim.
///
/// The gateway uses this to fetch the FULL record stream (`Op::Select`, which
/// carries the on-disk null bitmap) for a non-sorted projection, then
/// re-projects the requested columns in the gateway with NULL fidelity. The
/// engine's narrow `Op::SelectFields` stream has NO null mask — an omitted /
/// explicit-NULL field's stored zero bytes are indistinguishable from a real
/// 0 at that layer (the root cause of SP-PG-NULL-INT-RENDER). By going through
/// `SELECT *` + the bitmap-honoring `decode_record`, a projected NULL renders
/// as a real PG NULL.
///
/// Returns `None` if `sql` is NOT a plain single-table projection list (the
/// exact acceptance of `select_columns`): aggregates, JOINs, `SELECT *`, and
/// any shape with a parenthesised/qualified-subquery projection are excluded,
/// so the FROM keyword we splice on is unambiguously the top-level one (the
/// projection list before it has no parens by construction). The match is
/// case-insensitive and token-boundary aware (it won't fire on `FROM` inside a
/// quoted string or an identifier like `from_date`). Used by the gateway's
/// non-sorted projection render path ONLY; a no-op (None) for any other SQL so
/// every prior dispatch path is byte-untouched.
pub fn select_projection_to_star(sql: &str) -> Option<String> {
    // Gate on the exact projection-list shape (no aggregate / JOIN / `*`).
    select_columns(sql)?;
    // Find the top-level FROM keyword as a case-insensitive token boundary,
    // skipping any single/double-quoted spans (defensive — a projection list
    // can't actually contain a string literal, but we stay quote-safe).
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    // Skip leading whitespace + the SELECT keyword.
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if sql[i..].len() < 6 || !sql[i..i + 6].eq_ignore_ascii_case("SELECT") {
        return None;
    }
    let from_rel = find_keyword_boundary(&sql[i..], "FROM")?;
    let from_abs = i + from_rel;
    Some(format!("SELECT * {}", &sql[from_abs..]))
}

/// Find the byte offset of a standalone keyword (case-insensitive, surrounded
/// by token boundaries — start/whitespace/punct on each side), skipping any
/// `'…'` / `"…"` quoted spans so a keyword inside a literal/identifier is not
/// matched. Returns the offset of the keyword's first byte, or `None`.
fn find_keyword_boundary(s: &str, kw: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let kwl = kw.len();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\'' || c == b'"' {
            // Skip the quoted span (no escape handling needed — the projection
            // list this runs on has no string literals; this is pure defense).
            let q = c;
            i += 1;
            while i < bytes.len() && bytes[i] != q {
                i += 1;
            }
            i += 1; // consume closing quote (or run off the end)
            continue;
        }
        let is_boundary_before = i == 0 || !is_ident_byte(bytes[i - 1]);
        if is_boundary_before
            && i + kwl <= bytes.len()
            && s[i..i + kwl].eq_ignore_ascii_case(kw)
            && (i + kwl == bytes.len() || !is_ident_byte(bytes[i + kwl]))
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// SP-PG-ORM-RELATIONSHIPS — a JOIN projection item: the (optional) table
/// qualifier + the column name, preserved separately so the gateway can map
/// it onto the JOIN's combined schema (whose columns are named
/// `<table>.<col>`). `qualifier` is `None` for a bare column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinProjCol {
    pub qualifier: Option<String>,
    pub column: String,
}

/// SP-PG-SQL-JOIN-ALIAS — a table reference in a FROM/JOIN clause: its real
/// table name plus the optional alias it was introduced under (`users u`,
/// `users AS u`). Used to build the alias→table map that resolves a
/// `<alias>.<col>` qualifier back to the full `<table>.<col>` the engine's
/// combined `KTR1` schema (and the catalog) names columns by.
#[derive(Debug, Clone, PartialEq, Eq)]
struct JoinTableRef {
    table: String,
    alias: Option<String>,
}

/// SP-PG-SQL-JOIN-ALIAS — resolve a column qualifier (a table name OR an alias
/// introduced in the FROM/JOIN clause) to the FULL table name. A bare full
/// table name resolves to itself (back-compat); an alias resolves to its table.
/// An ambiguous match (the qualifier equals an alias AND a different table's
/// name) or no match is a clean `Err`, never a silent mis-resolution.
///
/// Resolution order is unambiguous by construction because duplicate aliases /
/// alias-shadows-table collisions are rejected when the map is built
/// (`build_alias_map`). So at lookup time at most one entry can match either as
/// a full table name or as an alias.
fn resolve_join_qualifier(refs: &[JoinTableRef], qual: &str) -> Result<String, SqlError> {
    // A full table name wins (back-compat: `users.id` keeps resolving even if
    // some OTHER table was aliased). Exact, case-sensitive (catalog names are).
    if refs.iter().any(|r| r.table == qual) {
        return Ok(qual.to_string());
    }
    // Otherwise it must be an alias of exactly one table.
    let mut hit: Option<&str> = None;
    for r in refs {
        if r.alias.as_deref() == Some(qual) {
            if hit.is_some() {
                // Should be unreachable (build_alias_map rejects dup aliases),
                // but guard against a silent mis-resolution regardless.
                return Err(format!(
                    "join qualifier `{qual}` is ambiguous (matches multiple aliases)"
                ));
            }
            hit = Some(&r.table);
        }
    }
    match hit {
        Some(t) => Ok(t.to_string()),
        None => {
            let known: Vec<String> = refs
                .iter()
                .map(|r| match &r.alias {
                    Some(a) => format!("{} (alias {a})", r.table),
                    None => r.table.clone(),
                })
                .collect();
            Err(format!(
                "qualifier `{qual}` does not name any table or alias in the \
                 FROM/JOIN clause {known:?}"
            ))
        }
    }
}

/// SP-PG-SQL-JOIN-ALIAS — validate a freshly-collected list of join table refs:
/// reject a duplicate/ambiguous alias and an alias that shadows a DIFFERENT
/// table's real name (both would let a qualifier resolve two ways). A table
/// joined more than once under DISTINCT aliases (self-join) would also produce
/// duplicate `<table>.<col>` names in the combined `KTR1` schema, so it is a
/// named follow-up (rejected here, not silently mis-resolved).
fn validate_join_refs(refs: &[JoinTableRef]) -> Result<(), SqlError> {
    for (i, r) in refs.iter().enumerate() {
        // Self-join (same table appears twice) — named follow-up.
        if refs[..i].iter().any(|p| p.table == r.table) {
            return Err(format!(
                "table `{}` is joined more than once — self-joins with aliases \
                 are a named follow-up (the combined schema would have duplicate \
                 `{}.<col>` names)",
                r.table, r.table
            ));
        }
        if let Some(a) = &r.alias {
            // Duplicate alias.
            if refs[..i].iter().any(|p| p.alias.as_deref() == Some(a)) {
                return Err(format!("duplicate table alias `{a}` in FROM/JOIN clause"));
            }
            // Alias shadows a different table's real name.
            if refs.iter().any(|p| &p.table == a && p.table != r.table) {
                return Err(format!(
                    "table alias `{a}` shadows another table's name in the \
                     FROM/JOIN clause"
                ));
            }
        }
    }
    Ok(())
}

/// SP-PG-SQL-JOIN-ALIAS — consume an optional `[AS] <alias>` after a table name
/// using the shared `P` parser. Returns the alias if present. `AS` is optional
/// (PG/SQL-92: `users u` and `users AS u` are equivalent). A following keyword
/// that begins the next clause (`ON`, `JOIN`, `INNER`/`LEFT`/`RIGHT`/`FULL`,
/// `WHERE`, `GROUP`, `ORDER`, `LIMIT`, `OFFSET`, `ON`) is NOT an alias.
fn parse_optional_alias(p: &mut P) -> Result<Option<String>, SqlError> {
    if p.kw("AS") {
        // Explicit `AS` — an alias ident MUST follow.
        return Ok(Some(p.ident()?));
    }
    // Implicit alias: a bare ident that is not a clause-starting keyword.
    if let Some(Tok::Ident(s)) = p.peek() {
        if !is_join_clause_keyword(s) {
            let a = p.ident()?;
            return Ok(Some(a));
        }
    }
    Ok(None)
}

/// SP-PG-SQL-JOIN-ALIAS — keywords that can follow a table name in a JOIN query
/// and therefore are NOT an implicit table alias.
fn is_join_clause_keyword(s: &str) -> bool {
    matches!(
        s.to_ascii_uppercase().as_str(),
        "ON" | "JOIN"
            | "INNER"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "OUTER"
            | "CROSS"
            | "WHERE"
            | "GROUP"
            | "ORDER"
            | "LIMIT"
            | "OFFSET"
            | "HAVING"
            | "UNION"
    )
}

/// SP-PG-ORM-RELATIONSHIPS — if `sql` is an inner-equi-`JOIN` SELECT
/// (`SELECT <proj> FROM a JOIN b ON a.x = b.y [LIMIT n]`), return its
/// projection: `Some((cols, is_star))`. `is_star` is `true` for `SELECT *`
/// (project EVERY combined column in schema order); otherwise `cols` is the
/// explicit qualified projection list (`SELECT authors.name, books.title …`),
/// each item carrying its optional `<table>` qualifier + column so the
/// gateway can disambiguate same-named columns across the two joined tables.
/// `None` for any non-JOIN SELECT (the caller falls back to the single-table
/// render shapes). Uses the real lexer — no string heuristics.
///
/// The engine compiles a JOIN to `Op::Join` which discards the projection
/// (it returns ALL combined columns in a self-describing `KTR1` result);
/// this helper recovers the projection from the SQL text so the gateway can
/// render exactly the requested columns. Aggregates / function calls in the
/// projection ⇒ `None` (the JOIN-aggregate render is a separate follow-up).
pub fn join_projection(sql: &str) -> Option<(Vec<JoinProjCol>, bool)> {
    let toks = lex(sql).ok()?;
    let mut it = toks.iter().peekable();
    match it.next()? {
        Tok::Ident(k) if k.eq_ignore_ascii_case("SELECT") => {}
        _ => return None,
    }
    // Two projection shapes: `*` or an explicit qualified column list.
    let mut is_star = false;
    let mut cols: Vec<JoinProjCol> = Vec::new();
    if matches!(it.peek(), Some(Tok::Star)) {
        it.next();
        is_star = true;
        match it.next()? {
            Tok::Ident(k) if k.eq_ignore_ascii_case("FROM") => {}
            _ => return None,
        }
    } else {
        loop {
            let first = match it.next()? {
                Tok::Ident(c) if !c.eq_ignore_ascii_case("FROM") => c.clone(),
                _ => return None,
            };
            // Qualified `table.col`?
            let item = if matches!(it.peek(), Some(Tok::Punct('.'))) {
                it.next(); // consume `.`
                match it.next()? {
                    Tok::Ident(real) => JoinProjCol {
                        qualifier: Some(first),
                        column: real.clone(),
                    },
                    _ => return None,
                }
            } else {
                JoinProjCol { qualifier: None, column: first }
            };
            // `FUNC(` ⇒ aggregate/expr — not a plain JOIN projection.
            if matches!(it.peek(), Some(Tok::Punct('('))) {
                return None;
            }
            // Optional output alias `col AS alias` — accept-and-skip.
            if matches!(it.peek(), Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("AS"))
            {
                it.next();
                match it.next()? {
                    Tok::Ident(_alias) => {}
                    _ => return None,
                }
            }
            cols.push(item);
            match it.next()? {
                Tok::Punct(',') => continue,
                Tok::Ident(k) if k.eq_ignore_ascii_case("FROM") => break,
                _ => return None,
            }
        }
    }
    // FROM <table> [[AS] <alias>] [LEFT [OUTER]] JOIN <table> [[AS] <alias>] …
    // — the JOIN keyword (optionally preceded by `LEFT [OUTER]`,
    // SP-PG-SQL-OUTER-JOIN) is what makes this a join shape. SP-PG-SQL-JOIN-
    // ALIAS captures each table's optional alias into `refs` so a projection
    // qualifier spelled with an alias (`u.name`) resolves to the FULL table
    // name (`users.name`) the combined `KTR1` schema uses.
    let mut refs: Vec<JoinTableRef> = Vec::new();
    let first_table = match it.next()? {
        Tok::Ident(t) => t.clone(),
        _ => return None,
    };
    let first_alias = next_optional_alias(&mut it)?;
    refs.push(JoinTableRef { table: first_table, alias: first_alias });
    // First (mandatory, for a join) JOIN — optionally `LEFT [OUTER]`/`INNER`.
    if !consume_join_kw(&mut it)? {
        return None; // single-table — not the JOIN shape
    }
    // The right table of the base join + its optional alias.
    let rtable = match it.next()? {
        Tok::Ident(t) => t.clone(),
        _ => return None,
    };
    let ralias = next_optional_alias(&mut it)?;
    refs.push(JoinTableRef { table: rtable, alias: ralias });
    // SP-PG-SQL-MULTI-JOIN chain: scan the remaining tokens for further
    // `[INNER] JOIN <table> [[AS] <alias>]` segments so a 3+-table aliased join
    // contributes every table to the alias map. We only need to capture each
    // joined table's name + alias here; the ON predicates (and any trailing
    // WHERE/ORDER BY/…) are skipped — they don't affect the projection-qualifier
    // resolution. We advance one token at a time, treating a top-level JOIN /
    // INNER JOIN keyword as the start of the next table segment.
    while let Some(tok) = it.next() {
        let is_inner_join = match tok {
            Tok::Ident(k) if k.eq_ignore_ascii_case("JOIN") => true,
            Tok::Ident(k) if k.eq_ignore_ascii_case("INNER") => {
                // `INNER` must be immediately followed by `JOIN`.
                match it.peek() {
                    Some(Tok::Ident(j)) if j.eq_ignore_ascii_case("JOIN") => {
                        it.next();
                        true
                    }
                    _ => false,
                }
            }
            _ => false,
        };
        if !is_inner_join {
            continue; // an ON-clause / WHERE / … token — skip it.
        }
        let ct = match it.next()? {
            Tok::Ident(t) => t.clone(),
            _ => return None,
        };
        let ca = next_optional_alias(&mut it)?;
        refs.push(JoinTableRef { table: ct, alias: ca });
    }
    // Reject ambiguous / duplicate aliases + self-joins (named follow-up).
    if validate_join_refs(&refs).is_err() {
        return None;
    }
    // Resolve every projection qualifier (alias OR full table name) to the FULL
    // table name so it matches the combined `<table>.<col>` `KTR1` schema. A
    // bare (unqualified) column is left untouched. An unresolvable qualifier ⇒
    // `None` (the gateway then renders the standard 42703 column error).
    for c in cols.iter_mut() {
        if let Some(q) = &c.qualifier {
            match resolve_join_qualifier(&refs, q) {
                Ok(full) => c.qualifier = Some(full),
                Err(_) => return None,
            }
        }
    }
    Some((cols, is_star))
}

/// SP-PG-SQL-JOIN-ALIAS — iterator-based `[AS] <alias>` consumer for the
/// gateway text helpers (`join_projection`/`join_group_aggregate`), which walk
/// a raw `Tok` iterator rather than the catalog-bound `P` parser. Mirrors
/// `parse_optional_alias`: `AS` is optional; a clause-starting keyword is not an
/// alias.
fn next_optional_alias<'a, I>(it: &mut std::iter::Peekable<I>) -> Option<Option<String>>
where
    I: Iterator<Item = &'a Tok>,
{
    // Explicit `AS <ident>`.
    if matches!(it.peek(), Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("AS")) {
        it.next();
        return match it.next() {
            Some(Tok::Ident(a)) => Some(Some(a.clone())),
            _ => None, // `AS` with no ident — malformed.
        };
    }
    // Implicit alias: a bare ident that is not a clause keyword.
    if let Some(Tok::Ident(s)) = it.peek() {
        if !is_join_clause_keyword(s) {
            let a = match it.next() {
                Some(Tok::Ident(a)) => a.clone(),
                _ => return None,
            };
            return Some(Some(a));
        }
    }
    Some(None)
}

/// SP-PG-SQL-JOIN-ALIAS — consume an optional `LEFT [OUTER]` / `INNER` then a
/// mandatory `JOIN`, returning `Some(true)` if a JOIN was consumed, `Some(false)`
/// if the next token is not a join, `None` on a malformed `LEFT …`/`INNER …`.
fn consume_join_kw<'a, I>(it: &mut std::iter::Peekable<I>) -> Option<bool>
where
    I: Iterator<Item = &'a Tok>,
{
    match it.peek() {
        Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("JOIN") => {
            it.next();
            Some(true)
        }
        Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("INNER") => {
            it.next();
            match it.next() {
                Some(Tok::Ident(j)) if j.eq_ignore_ascii_case("JOIN") => Some(true),
                _ => None,
            }
        }
        Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("LEFT") => {
            it.next();
            if matches!(it.peek(), Some(Tok::Ident(o)) if o.eq_ignore_ascii_case("OUTER")) {
                it.next();
            }
            match it.next() {
                Some(Tok::Ident(j)) if j.eq_ignore_ascii_case("JOIN") => Some(true),
                _ => None,
            }
        }
        _ => Some(false),
    }
}

/// SP-PG-SQL-JOIN-AGG — one aggregate in a join-group-aggregate projection:
/// its kind code + optional output column name (alias or the PG default like
/// `count`). The arg column is not needed for rendering (the engine returns the
/// computed value); only the OUTPUT name + kind matter for RowDescription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinAggOut {
    /// 0 COUNT / 1 SUM / 2 MIN / 3 MAX / 4 AVG.
    pub kind: u8,
    /// RowDescription column name (alias if given, else the PG default name).
    pub out_name: String,
}

/// SP-PG-SQL-JOIN-AGG — the gateway-facing description of a join-group-aggregate
/// SELECT (`SELECT a.name, COUNT(b.id) [AS c] FROM a JOIN b ON … GROUP BY a.name
/// [ORDER BY …]`). Carries the GROUP BY column's qualifier + column (so the
/// gateway can resolve its FieldKind via the qualifier's table schema and name
/// the RowDescription column) + the ordered aggregate outputs. The engine result
/// is value-only (`[u32 ngroups]…`), so this recovers the column shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupAggProj {
    /// GROUP BY column qualifier (table name) — always present (V1 requires it).
    pub group_qualifier: String,
    /// GROUP BY column name.
    pub group_column: String,
    /// One per aggregate, in projection order.
    pub aggregates: Vec<JoinAggOut>,
}

/// SP-PG-SQL-JOIN-AGG — if `sql` is a join-group-aggregate SELECT
/// (`SELECT <group col>, <AGG>(…)+ [AS …] FROM a [LEFT [OUTER]] JOIN b ON …
/// [WHERE …] GROUP BY <group col> [ORDER BY …] [LIMIT …]`), return its
/// `JoinGroupAggProj`. `None` for any other shape (the caller falls back to the
/// existing render paths). Lexer-backed; mirrors `join_projection`.
///
/// The engine compiles this same SQL to `Op::Join { group_aggregate: Some(..) }`
/// and returns the group-aggregate `[u32 ngroups][u32 keylen][key][16B × n]…`
/// stream; this helper recovers the OUTPUT column shape (group col + agg names)
/// so the gateway can emit a RowDescription + decode the value stream.
pub fn join_group_aggregate(sql: &str) -> Option<JoinGroupAggProj> {
    fn agg_code(w: &str) -> Option<u8> {
        match w.to_ascii_uppercase().as_str() {
            "COUNT" => Some(0),
            "SUM" => Some(1),
            "MIN" => Some(2),
            "MAX" => Some(3),
            "AVG" => Some(4),
            _ => None,
        }
    }
    let toks = lex(sql).ok()?;
    let mut it = toks.iter().peekable();
    match it.next()? {
        Tok::Ident(k) if k.eq_ignore_ascii_case("SELECT") => {}
        _ => return None,
    }
    // Leading group column `t.c` (V1 requires a qualifier).
    let group_qualifier = match it.next()? {
        Tok::Ident(q) => q.clone(),
        _ => return None,
    };
    match it.next()? {
        Tok::Punct('.') => {}
        _ => return None, // V1: the group column must be qualified
    }
    let group_column = match it.next()? {
        Tok::Ident(c) => c.clone(),
        _ => return None,
    };
    // Optional `AS <alias>` on the group column — accept-and-skip (rendered by
    // its source name, like the projection paths).
    if matches!(it.peek(), Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("AS")) {
        it.next();
        match it.next()? {
            Tok::Ident(_) => {}
            _ => return None,
        }
    }
    // Then a comma + ≥1 aggregate.
    match it.next()? {
        Tok::Punct(',') => {}
        _ => return None, // a group-agg projection has the agg after the group col
    }
    let mut aggregates: Vec<JoinAggOut> = Vec::new();
    loop {
        let kind = match it.next()? {
            Tok::Ident(w) => agg_code(w)?,
            _ => return None,
        };
        match it.next()? {
            Tok::Punct('(') => {}
            _ => return None,
        }
        // arg: `*` | `[t.]col`
        match it.next()? {
            Tok::Star => {}
            Tok::Ident(_) => {
                if matches!(it.peek(), Some(Tok::Punct('.'))) {
                    it.next();
                    match it.next()? {
                        Tok::Ident(_) => {}
                        _ => return None,
                    }
                }
            }
            _ => return None,
        }
        match it.next()? {
            Tok::Punct(')') => {}
            _ => return None,
        }
        // optional `AS <alias>`
        let out_name = if matches!(it.peek(), Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("AS")) {
            it.next();
            match it.next()? {
                Tok::Ident(a) => a.clone(),
                _ => return None,
            }
        } else {
            agg_default_name(kind).to_string()
        };
        aggregates.push(JoinAggOut { kind, out_name });
        match it.peek() {
            Some(Tok::Punct(',')) => {
                it.next();
                continue;
            }
            _ => break,
        }
    }
    if aggregates.is_empty() {
        return None;
    }
    // `FROM <a> [[AS] <alias>] [LEFT [OUTER]] JOIN <b> [[AS] <alias>]` — confirm
    // the join shape AND capture each table's optional alias (SP-PG-SQL-JOIN-
    // ALIAS) so the GROUP BY qualifier (`u.id`) resolves to the FULL table name
    // (`users.id`) the gateway's `describe_table` + RowDescription expect.
    match it.next()? {
        Tok::Ident(k) if k.eq_ignore_ascii_case("FROM") => {}
        _ => return None,
    }
    let mut refs: Vec<JoinTableRef> = Vec::new();
    let lt = match it.next()? {
        Tok::Ident(t) => t.clone(),
        _ => return None,
    };
    let la = next_optional_alias(&mut it)?;
    refs.push(JoinTableRef { table: lt, alias: la });
    if !consume_join_kw(&mut it)? {
        return None;
    }
    let rt = match it.next()? {
        Tok::Ident(t) => t.clone(),
        _ => return None,
    };
    let ra = next_optional_alias(&mut it)?;
    refs.push(JoinTableRef { table: rt, alias: ra });
    if validate_join_refs(&refs).is_err() {
        return None;
    }
    // Resolve the GROUP BY column's qualifier (alias OR table name) to the FULL
    // table name. An unresolvable qualifier ⇒ `None` (gateway falls through).
    let group_qualifier = match resolve_join_qualifier(&refs, &group_qualifier) {
        Ok(full) => full,
        Err(_) => return None,
    };
    // Confirm a GROUP BY appears somewhere downstream (it must, for this to be a
    // group-aggregate; the engine compile requires it). Scan the remaining
    // tokens for a top-level `GROUP BY`.
    let mut saw_group = false;
    while let Some(t) = it.next() {
        if let Tok::Ident(k) = t {
            if k.eq_ignore_ascii_case("GROUP") {
                if matches!(it.peek(), Some(Tok::Ident(b)) if b.eq_ignore_ascii_case("BY")) {
                    saw_group = true;
                    break;
                }
            }
        }
    }
    if !saw_group {
        return None;
    }
    Some(JoinGroupAggProj { group_qualifier, group_column, aggregates })
}

/// SP-PG-SERIAL-RETURNING: if `sql` is an `INSERT INTO <table> … RETURNING
/// col1, col2, …`, return `(table, [col1, col2, …])` so the gateway can
/// emit a RowDescription + DataRow of the returned (e.g. engine-assigned)
/// values. `None` for an INSERT without a RETURNING clause, or any non-
/// INSERT statement (the caller then emits a bare CommandComplete). Uses
/// the real lexer; a leading qualifier on a returned column (`t.id`) is
/// stripped (lenient, matching `select_columns`). V1 scopes to INSERT
/// RETURNING — UPDATE/DELETE RETURNING is `SP-PG-SQL-RETURNING-DML`.
pub fn insert_returning(sql: &str) -> Option<(String, Vec<String>)> {
    let toks = lex(sql).ok()?;
    let mut it = toks.iter().peekable();
    match it.next()? {
        Tok::Ident(k) if k.eq_ignore_ascii_case("INSERT") => {}
        _ => return None,
    }
    match it.next()? {
        Tok::Ident(k) if k.eq_ignore_ascii_case("INTO") => {}
        _ => return None,
    }
    let table = match it.next()? {
        Tok::Ident(t) => t.clone(),
        _ => return None,
    };
    // Scan forward for a top-level RETURNING keyword (the INSERT body is
    // already validated by the parser; here we only need to locate the
    // clause and read its column list).
    let mut found = false;
    for t in it.by_ref() {
        if let Tok::Ident(k) = t {
            if k.eq_ignore_ascii_case("RETURNING") {
                found = true;
                break;
            }
        }
    }
    if !found {
        return None;
    }
    // SP-PG-RETURNING-MULTIROW-STAR: `RETURNING *` → the star sentinel
    // `["*"]`. The gateway expands it to every table column (the assigned
    // id pseudo-column + all declared fields) via `describe_table`. The
    // lexer emits `*` as `Tok::Star`; we detect it immediately after
    // RETURNING (optionally followed by a trailing `;` the lexer dropped).
    if matches!(it.peek(), Some(Tok::Star)) {
        it.next();
        // `RETURNING *` must be the whole clause (no `*, col` mixing in V1).
        // A trailing `;` is tolerated (the lexer keeps it as a token).
        match it.peek() {
            None => return Some((table, vec!["*".to_string()])),
            Some(Tok::Punct(';')) => return Some((table, vec!["*".to_string()])),
            _ => return None,
        }
    }
    let mut cols = Vec::new();
    loop {
        let mut col = match it.next()? {
            Tok::Ident(c) => c.clone(),
            _ => return None,
        };
        // Lenient qualifier: `RETURNING t.id` → `id`.
        if matches!(it.peek(), Some(Tok::Punct('.'))) {
            it.next();
            match it.next()? {
                Tok::Ident(real) => col = real.clone(),
                _ => return None,
            }
        }
        cols.push(col);
        // SP-PG-RETURNING-MULTIROW-STAR: accept-and-skip a column alias
        // `RETURNING t.id AS id__1` — SQLAlchemy's insertmanyvalues form
        // emits `RETURNING widgets.id, widgets.id AS id__1`. The alias is
        // dropped; the projection still maps to the source column.
        if matches!(it.peek(), Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("AS")) {
            it.next(); // consume AS
            match it.next() {
                Some(Tok::Ident(_)) => {} // consume the alias ident
                _ => return None,
            }
        }
        match it.peek() {
            Some(Tok::Punct(',')) => {
                it.next();
                continue;
            }
            _ => break, // end of list (or trailing `;` already lexed out)
        }
    }
    if cols.is_empty() {
        return None;
    }
    Some((table, cols))
}

/// SP-PG-SQL-AGG-ALIAS-RENDER — a single scalar-aggregate SELECT over a
/// FROM table, as Django's `.count()` / `.exists()` / `.aggregate()`
/// emit: `SELECT COUNT(*) AS "__count" FROM "t"` (or bare
/// `SELECT COUNT(*) FROM t`). The gateway uses this to render the scalar
/// (the engine's `Op::Aggregate` returns a 16-byte LE i128 in
/// `OpResult::Got`, which has NO column name / shape the wire path can
/// otherwise describe).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectAgg {
    /// FROM table name.
    pub table: String,
    /// Aggregate kind code (0=COUNT, 1=SUM, 2=MIN, 3=MAX, 4=AVG) — the
    /// same canonical codes the parser/engine use.
    pub kind: u8,
    /// Aggregate argument column (`None` for `COUNT(*)`), qualifier
    /// stripped.
    pub field: Option<String>,
    /// Output column name from an `AS <alias>` clause, if any.
    pub alias: Option<String>,
}

/// The lowercase default output column name PostgreSQL assigns an
/// unaliased aggregate (`COUNT(*)` → `count`, `SUM(x)` → `sum`, …).
pub fn agg_default_name(kind: u8) -> &'static str {
    match kind {
        0 => "count",
        1 => "sum",
        2 => "min",
        3 => "max",
        4 => "avg",
        _ => "agg",
    }
}

/// If `sql` is a single scalar-aggregate SELECT over a FROM table —
/// `SELECT <AGG>( * | [t.]col ) [AS alias] FROM <table>` with NO leading
/// projection columns, NO GROUP BY, NO JOIN — return the `SelectAgg`.
/// `None` for anything else (multi-aggregate, GROUP BY, plain
/// projection, `SELECT *`), so the existing render shapes are unchanged.
/// Lexer-backed; mirrors `select_columns` / `select_star_table`.
pub fn select_aggregate(sql: &str) -> Option<SelectAgg> {
    fn agg_code(w: &str) -> Option<u8> {
        match w.to_ascii_uppercase().as_str() {
            "COUNT" => Some(0),
            "SUM" => Some(1),
            "MIN" => Some(2),
            "MAX" => Some(3),
            "AVG" => Some(4),
            _ => None,
        }
    }
    let toks = lex(sql).ok()?;
    let mut it = toks.iter().peekable();
    match it.next()? {
        Tok::Ident(k) if k.eq_ignore_ascii_case("SELECT") => {}
        _ => return None,
    }
    // `<AGG> (`
    let kind = match it.next()? {
        Tok::Ident(w) => agg_code(w)?,
        _ => return None,
    };
    match it.next()? {
        Tok::Punct('(') => {}
        _ => return None,
    }
    // `*` | `[t.]col`
    let field = match it.next()? {
        Tok::Star => None,
        Tok::Ident(c) => {
            let mut col = c.clone();
            if matches!(it.peek(), Some(Tok::Punct('.'))) {
                it.next(); // consume `.`
                match it.next()? {
                    Tok::Ident(real) => col = real.clone(),
                    _ => return None,
                }
            }
            Some(col)
        }
        _ => return None,
    };
    match it.next()? {
        Tok::Punct(')') => {}
        _ => return None,
    }
    // Optional `AS <alias>`.
    let mut alias = None;
    if matches!(it.peek(), Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("AS")) {
        it.next(); // consume AS
        match it.next()? {
            Tok::Ident(a) => alias = Some(a.clone()),
            _ => return None,
        }
    }
    // `FROM <table>` — and that must be the END (no GROUP BY / JOIN /
    // extra projection; those keep their existing single-/multi-agg paths
    // and are not single-scalar wire renders).
    match it.next()? {
        Tok::Ident(k) if k.eq_ignore_ascii_case("FROM") => {}
        _ => return None,
    }
    let table = match it.next()? {
        Tok::Ident(t) => t.clone(),
        _ => return None,
    };
    // Tolerate a trailing `;` but reject any further clause (WHERE / GROUP
    // BY / JOIN / etc.) — those are NOT the bare-scalar shape this helper
    // renders. (A WHERE-filtered COUNT is a valid follow-up render; V1
    // scopes the bare `.count()` / `.aggregate()` shape Django emits.)
    match it.peek() {
        None => {}
        Some(Tok::Punct(';')) => {}
        _ => return None,
    }
    Some(SelectAgg { table, kind, field, alias })
}

/// SP-PG-SQL-PLAIN-GROUP-RENDER — one aggregate in a plain (non-JOIN)
/// group-aggregate projection. Carries the kind code, the OUTPUT column
/// name (alias or PG default like `count`), and the source argument column
/// (`None` for `COUNT(*)`) so the gateway can type the RowDescription OID
/// (MIN/MAX inherit the source column's type; AVG → numeric; COUNT/SUM →
/// int8). The arg column is NOT needed to decode the engine's value stream
/// (the engine returns the computed i128), only to assign the OID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainAggOut {
    /// 0 COUNT / 1 SUM / 2 MIN / 3 MAX / 4 AVG.
    pub kind: u8,
    /// RowDescription column name (alias if given, else the PG default name).
    pub out_name: String,
    /// Source argument column, qualifier stripped (`None` for `COUNT(*)`).
    pub source_column: Option<String>,
}

/// SP-PG-SQL-PLAIN-GROUP-RENDER — the gateway-facing description of a plain
/// (non-JOIN) group-aggregate SELECT (`SELECT g, COUNT(*) [AS c], SUM(p) …
/// FROM t GROUP BY g [HAVING …] [ORDER BY …] [LIMIT …] [OFFSET …]`). Carries
/// the table + the GROUP BY column name (so the gateway can resolve the group
/// key's FieldKind via the table schema and name the RowDescription column) +
/// the ordered aggregate outputs. The engine result is value-only
/// (`[u32 ngroups][u32 keylen][key][16B i128 × n_aggs]…`), so this recovers
/// the output column shape (group col + agg names + types).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainGroupAggProj {
    /// FROM table name.
    pub table: String,
    /// GROUP BY column name (qualifier stripped).
    pub group_column: String,
    /// One per aggregate, in projection order.
    pub aggregates: Vec<PlainAggOut>,
}

/// SP-PG-SQL-PLAIN-GROUP-RENDER — if `sql` is a PLAIN (non-JOIN)
/// group-aggregate SELECT, return its `PlainGroupAggProj`. `None` for any
/// other shape — crucially `None` for a JOIN group-aggregate (which has its
/// own `join_group_aggregate` recognizer + render path) and `None` for a
/// single scalar aggregate with no GROUP BY (`select_aggregate`). This keeps
/// every existing render path byte-untouched; the gateway gates the plain
/// group-aggregate render branch on `Some`.
///
/// Accepted shape (mirrors what the planner compiles to
/// `Op::GroupAggregate` / `Op::GroupAggregateMulti`):
/// ```text
/// SELECT <group col> , <AGG>(<*|[t.]col>) [AS a] [, <AGG>(…) [AS a]]*
///   FROM <table>
///   [WHERE …]
///   GROUP BY <group col>
///   [HAVING …]            -- render ignores HAVING (the SM already filtered)
///   [ORDER BY …]          -- SP-PG-SQL-GROUP-SORT-LIMIT: threaded into the Op's
///   [LIMIT n] [OFFSET n]  -- `GroupSort` and APPLIED by the engine (groups are
///                         -- reordered + windowed; render emits in engine order)
/// ```
/// The leading group column may be bare (`category`) or qualified
/// (`products.category` / `t.category`) — the qualifier is stripped. A JOIN
/// in the FROM clause makes this return `None` (the JOIN path owns it).
/// Lexer-backed; mirrors `select_aggregate` / `join_group_aggregate`.
pub fn plain_group_aggregate(sql: &str) -> Option<PlainGroupAggProj> {
    fn agg_code(w: &str) -> Option<u8> {
        match w.to_ascii_uppercase().as_str() {
            "COUNT" => Some(0),
            "SUM" => Some(1),
            "MIN" => Some(2),
            "MAX" => Some(3),
            "AVG" => Some(4),
            _ => None,
        }
    }
    let toks = lex(sql).ok()?;
    let mut it = toks.iter().peekable();
    match it.next()? {
        Tok::Ident(k) if k.eq_ignore_ascii_case("SELECT") => {}
        _ => return None,
    }
    // Leading group column — bare `c` or qualified `t.c` (qualifier stripped).
    // It must NOT itself be an aggregate function call (`AGG(`), which is the
    // single-scalar / no-leading-col shape `select_aggregate` owns.
    let group_column = match it.next()? {
        Tok::Ident(c) => {
            // Reject `AGG(` as the leading token — that's the scalar-agg shape.
            if agg_code(c).is_some() && matches!(it.peek(), Some(Tok::Punct('('))) {
                return None;
            }
            let mut col = c.clone();
            if matches!(it.peek(), Some(Tok::Punct('.'))) {
                it.next(); // consume `.`
                match it.next()? {
                    Tok::Ident(real) => col = real.clone(),
                    _ => return None,
                }
            }
            col
        }
        _ => return None,
    };
    // Optional `AS <alias>` on the group column — accept-and-skip (rendered by
    // its source name, like the projection paths).
    if matches!(it.peek(), Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("AS")) {
        it.next();
        match it.next()? {
            Tok::Ident(_) => {}
            _ => return None,
        }
    }
    // Then a comma + ≥1 aggregate (a plain group-agg projection has the
    // group column first, then the aggregate(s)).
    match it.next()? {
        Tok::Punct(',') => {}
        _ => return None,
    }
    let mut aggregates: Vec<PlainAggOut> = Vec::new();
    loop {
        let kind = match it.next()? {
            Tok::Ident(w) => agg_code(w)?,
            _ => return None,
        };
        match it.next()? {
            Tok::Punct('(') => {}
            _ => return None,
        }
        // arg: `*` | `[t.]col`
        let source_column = match it.next()? {
            Tok::Star => None,
            Tok::Ident(c) => {
                let mut col = c.clone();
                if matches!(it.peek(), Some(Tok::Punct('.'))) {
                    it.next();
                    match it.next()? {
                        Tok::Ident(real) => col = real.clone(),
                        _ => return None,
                    }
                }
                Some(col)
            }
            _ => return None,
        };
        match it.next()? {
            Tok::Punct(')') => {}
            _ => return None,
        }
        // optional `AS <alias>`
        let out_name = if matches!(it.peek(), Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("AS")) {
            it.next();
            match it.next()? {
                Tok::Ident(a) => a.clone(),
                _ => return None,
            }
        } else {
            agg_default_name(kind).to_string()
        };
        aggregates.push(PlainAggOut { kind, out_name, source_column });
        match it.peek() {
            Some(Tok::Punct(',')) => {
                it.next();
                continue;
            }
            _ => break,
        }
    }
    if aggregates.is_empty() {
        return None;
    }
    // `FROM <table>` — and the table must be a single bare table (no JOIN:
    // the JOIN group-aggregate has its own recognizer + render path).
    match it.next()? {
        Tok::Ident(k) if k.eq_ignore_ascii_case("FROM") => {}
        _ => return None,
    }
    let table = match it.next()? {
        Tok::Ident(t) => t.clone(),
        _ => return None,
    };
    // Scan the remaining tokens: a top-level GROUP BY MUST appear, and a
    // JOIN keyword MUST NOT (a JOIN routes to `join_group_aggregate`). Other
    // clauses (WHERE / HAVING / ORDER BY / LIMIT / OFFSET) are tolerated —
    // the engine applies WHERE + HAVING; the render emits whatever group
    // stream the engine returns.
    let mut saw_group = false;
    while let Some(t) = it.next() {
        if let Tok::Ident(k) = t {
            if k.eq_ignore_ascii_case("JOIN") {
                return None; // a JOIN is the join-group-aggregate shape
            }
            if k.eq_ignore_ascii_case("GROUP") {
                if matches!(it.peek(), Some(Tok::Ident(b)) if b.eq_ignore_ascii_case("BY")) {
                    saw_group = true;
                }
            }
        }
    }
    if !saw_group {
        return None;
    }
    Some(PlainGroupAggProj { table, group_column, aggregates })
}

/// SP-PG-SQL-DML-GENERAL — if `sql` is an `UPDATE <table> … RETURNING
/// <cols | *>` or `DELETE FROM <table> … RETURNING <cols | *>`, return
/// `(table, [cols] | ["*"])` so the gateway can render the affected
/// rows. `None` for an UPDATE/DELETE without a RETURNING clause, or any
/// non-UPDATE/DELETE statement. Mirrors `insert_returning`: lenient
/// qualifier strip, `RETURNING *` → `["*"]` star sentinel, column
/// aliases (`RETURNING t.id AS x`) accepted-and-skipped. The leading
/// `UPDATE t` / `DELETE FROM t` shape gives the table; the RETURNING
/// clause is located by a forward scan for the top-level keyword.
pub fn dml_returning(sql: &str) -> Option<(String, Vec<String>)> {
    let toks = lex(sql).ok()?;
    let mut it = toks.iter().peekable();
    let table = match it.next()? {
        Tok::Ident(k) if k.eq_ignore_ascii_case("UPDATE") => {
            // `UPDATE <table> …`
            match it.next()? {
                Tok::Ident(t) => t.clone(),
                _ => return None,
            }
        }
        Tok::Ident(k) if k.eq_ignore_ascii_case("DELETE") => {
            // `DELETE FROM <table> …`
            match it.next()? {
                Tok::Ident(k2) if k2.eq_ignore_ascii_case("FROM") => {}
                _ => return None,
            }
            match it.next()? {
                Tok::Ident(t) => t.clone(),
                _ => return None,
            }
        }
        _ => return None,
    };
    // Locate the top-level RETURNING keyword.
    let mut found = false;
    for t in it.by_ref() {
        if let Tok::Ident(k) = t {
            if k.eq_ignore_ascii_case("RETURNING") {
                found = true;
                break;
            }
        }
    }
    if !found {
        return None;
    }
    // `RETURNING *` → star sentinel (whole clause, trailing `;` tolerated).
    if matches!(it.peek(), Some(Tok::Star)) {
        it.next();
        match it.peek() {
            None => return Some((table, vec!["*".to_string()])),
            Some(Tok::Punct(';')) => return Some((table, vec!["*".to_string()])),
            _ => return None,
        }
    }
    let mut cols = Vec::new();
    loop {
        let mut col = match it.next()? {
            Tok::Ident(c) => c.clone(),
            _ => return None,
        };
        if matches!(it.peek(), Some(Tok::Punct('.'))) {
            it.next();
            match it.next()? {
                Tok::Ident(real) => col = real.clone(),
                _ => return None,
            }
        }
        cols.push(col);
        if matches!(it.peek(), Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("AS")) {
            it.next();
            match it.next() {
                Some(Tok::Ident(_)) => {}
                _ => return None,
            }
        }
        match it.peek() {
            Some(Tok::Punct(',')) => {
                it.next();
                continue;
            }
            _ => break,
        }
    }
    if cols.is_empty() {
        return None;
    }
    Some((table, cols))
}

/// Human-readable query plan for `EXPLAIN`. Describes how the compiled
/// statement will actually run (index-narrowed vs full scan, which
/// columns / composite index), so users can see the SP62/SP63 planner at
/// work. Pure; no execution.
fn plan_string(stmt: &Stmt, cat: &Catalog) -> String {
    let tname = |tid: u32| {
        cat.get(tid)
            .map(|t| t.name.clone())
            .unwrap_or_else(|| format!("type#{tid}"))
    };
    let cols = |tid: u32, fids: &[u16]| -> String {
        let ot = cat.get(tid);
        fids.iter()
            .map(|fid| {
                ot.and_then(|t| t.fields.iter().find(|f| f.field_id == *fid))
                    .map(|f| f.name.clone())
                    .unwrap_or_else(|| format!("f#{fid}"))
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    match stmt {
        Stmt::Explain(_) => "EXPLAIN".to_string(),
        Stmt::Update { type_id, id, .. } => format!(
            "Read-Modify-Write on {} (id {id})",
            tname(*type_id)
        ),
        Stmt::UpdateWhere { type_id, .. } => format!(
            "Seq Scan on {} → filter → per-row Update (atomic Txn)",
            tname(*type_id)
        ),
        Stmt::DeleteWhere { type_id, .. } => format!(
            "Seq Scan on {} → filter → per-row Delete (atomic Txn)",
            tname(*type_id)
        ),
        Stmt::Op(op) => match op {
            Op::QueryRows { type_id, eq_preds, range_preds, .. } => {
                if eq_preds.is_empty() {
                    if !range_preds.is_empty() {
                        let rf: Vec<u16> =
                            range_preds.iter().map(|(f, _, _)| *f).collect();
                        return format!(
                            "Range Index Scan on {} on [{}] → verify full \
                             WHERE",
                            tname(*type_id),
                            cols(*type_id, &rf)
                        );
                    }
                    return format!(
                        "Seq Scan on {} → filter (no usable index)",
                        tname(*type_id)
                    );
                }
                let range_note = if range_preds.is_empty() {
                    String::new()
                } else {
                    let rf: Vec<u16> =
                        range_preds.iter().map(|(f, _, _)| *f).collect();
                    format!(" + range on [{}]", cols(*type_id, &rf))
                };
                let fids: Vec<u16> = eq_preds.iter().map(|(f, _)| *f).collect();
                let fset: std::collections::BTreeSet<u16> =
                    fids.iter().copied().collect();
                let composite = cat.get(*type_id).and_then(|t| {
                    t.composite.iter().find(|c| {
                        c.len() == fset.len()
                            && c.iter().copied().collect::<std::collections::BTreeSet<_>>()
                                == fset
                    })
                });
                if let Some(c) = composite {
                    format!(
                        "Composite Index Scan on {} using ({}){} → verify",
                        tname(*type_id),
                        cols(*type_id, c),
                        range_note
                    )
                } else {
                    format!(
                        "Index Scan on {} narrowed by [{}]{} → verify full \
                         WHERE",
                        tname(*type_id),
                        cols(*type_id, &fids),
                        range_note
                    )
                }
            }
            Op::GetById { type_id, .. } => {
                format!("Primary-Key Lookup on {} (O(1))", tname(*type_id))
            }
            Op::Select { type_id, .. } | Op::SelectFields { type_id, .. } => {
                format!("Seq Scan on {} → filter", tname(*type_id))
            }
            Op::SelectSorted { type_id, .. } => {
                format!("Seq Scan on {} → filter → sort", tname(*type_id))
            }
            Op::Aggregate { type_id, .. }
            | Op::GroupAggregate { type_id, .. }
            | Op::GroupAggregateMulti { type_id, .. } => {
                format!("Aggregate over Seq Scan on {}", tname(*type_id))
            }
            Op::Join { left_type, right_type, .. } => format!(
                "Hash Join {} ⋈ {}",
                tname(*left_type),
                tname(*right_type)
            ),
            Op::Txn { ops } => format!("Atomic Txn ({} ops)", ops.len()),
            Op::Create { type_id, .. } => format!("Insert into {}", tname(*type_id)),
            Op::Delete { type_id, .. } => format!("Delete from {}", tname(*type_id)),
            Op::Update { type_id, .. } => format!("Update {}", tname(*type_id)),
            Op::CreateType { .. } => "Create Table (online DDL)".to_string(),
            Op::DropType { type_id } => format!("Drop Table {}", tname(*type_id)),
            Op::DropIndex { type_id, fields } => format!(
                "Drop Index on {} ({})",
                tname(*type_id),
                cols(*type_id, fields)
            ),
            Op::DropField { type_id, field_id } => format!(
                "Drop Column {} ({}) — re-encode rows",
                tname(*type_id),
                cols(*type_id, &[*field_id])
            ),
            Op::RenameField { type_id, .. } => {
                format!("Rename Column on {} (catalog only)", tname(*type_id))
            }
            Op::AddBalanceGuard { type_id, field_id } => format!(
                "Add Balance Guard on {} ({} >= 0)",
                tname(*type_id),
                cols(*type_id, &[*field_id])
            ),
            Op::AlterTypeAddField { type_id, .. } => {
                format!("Alter {} Add Column (online, no lock)", tname(*type_id))
            }
            Op::CreateIndex { type_id, .. }
            | Op::AddUnique { type_id, .. }
            | Op::AddOrderedIndex { type_id, .. }
            | Op::AddCompositeIndex { type_id, .. } => {
                format!("Build Index on {} (backfill)", tname(*type_id))
            }
            Op::Describe { type_id } => format!("Describe {}", tname(*type_id)),
            other => format!("{:?}", other.kind()),
        },
    }
}

/// Compile one SQL statement, including `UPDATE`.
pub fn compile_stmt(sql: &str, cat: &Catalog) -> Result<Stmt, SqlError> {
    // EXPLAIN <stmt> — compile the inner statement and describe its plan
    // WITHOUT executing it. Pure planner output.
    {
        let t = sql.trim_start();
        if t.len() >= 8 && t[..7].eq_ignore_ascii_case("EXPLAIN") {
            let rest = t[7..].trim_start();
            if rest.is_empty() {
                return Err("EXPLAIN needs a statement".into());
            }
            let inner = compile_stmt(rest, cat)?;
            return Ok(Stmt::Explain(plan_string(&inner, cat)));
        }
    }
    compile_stmt_from_tokens(lex(sql)?, sql, cat, &[])
}

/// SP-PG-EXTQ-PARSED T2 — body of `compile_stmt` extracted so the
/// `compile_stmt_with_params` entry point can share it. Takes the
/// pre-(re)lexed token vec, the original SQL string (for the
/// fall-through `compile_from_tokens` call), the catalog, and the
/// bound params slice (so the UPDATE fall-through path can re-rewrite
/// — but the typical V1 shape is empty params on the bare path).
///
/// The compile path forks into UPDATE (the dedicated `Stmt::Update`
/// shape that `compile` itself rejects) and everything else (which
/// delegates to `compile_from_tokens`). Both paths see the SAME
/// pre-rewritten token vec — no double-rewrite, no parameter shape
/// drift between paths.
fn compile_stmt_from_tokens(
    toks: Vec<Tok>,
    _sql: &str,
    cat: &Catalog,
    _params: &[Option<Value>],
) -> Result<Stmt, SqlError> {
    // Try the UPDATE arm first. If the first keyword isn't UPDATE we
    // fall through to `compile_from_tokens` which handles every other
    // statement type.
    {
        let mut p = P { t: toks.clone(), i: 0, cat };
        if p.kw("UPDATE") {
            let tname = p.ident()?;
            // Two row-targeting shapes:
            //   (legacy)   UPDATE t ID <n> SET ...
            //   (standard) UPDATE t SET ... WHERE [t.]id = <n>
            // SQLAlchemy / Django / Rails ALL emit the standard shape
            // (`SET ... WHERE pk = n`), qualifying the WHERE column with
            // the table name. KesselDB rows are keyed by the `id`
            // pseudo-column (the ObjectId), so a `WHERE id = <int>` on
            // the primary key maps DIRECTLY to the id-based update RMW.
            // (SP-PG-SQL-ORM-PARSE T2)
            let legacy_id: Option<u128> = if p.kw("ID") {
                match p.next() {
                    Some(Tok::Int(n)) => Some(n as u128),
                    _ => return Err("UPDATE needs `ID <int>`".into()),
                }
            } else {
                None
            };
            p.expect_kw("SET")?;
            let ot = p.type_named(&tname)?.clone();
            let mut sets = Vec::new();
            loop {
                // SP-PG-SQL-ORM-PARSE T2 — SET target column may be
                // qualified (`SET orm_users.name = 'x'`); strip the
                // qualifier. (SQLAlchemy emits bare `SET name=$1` today,
                // but accept the qualified form per the parser contract.)
                let col = p.col_ident()?;
                match p.next() {
                    Some(Tok::Cmp("=")) => {}
                    _ => return Err("expected `=`".into()),
                }
                let lit = match p.next() {
                    Some(Tok::Int(n)) => Lit::Int(n),
                    Some(Tok::Str(s)) => Lit::Str(s),
                    // SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 — accept the
                    // raw-bytes shape emitted by the param-rewriter
                    // for `Value::Blob` bindings.
                    Some(Tok::Bytes(b)) => Lit::Bytes(b),
                    _ => return Err("expected value".into()),
                };
                let f = ot
                    .fields
                    .iter()
                    .find(|f| f.name == col)
                    .ok_or_else(|| unknown_column_err(&col, &ot))?;
                sets.push((f.field_id, lit_to_value(&lit, f.kind)?));
                match p.peek() {
                    Some(Tok::Punct(',')) => {
                        p.i += 1;
                        continue;
                    }
                    _ => break,
                }
            }
            // Resolve the target row(s):
            //   - legacy `ID <n>` already captured a single id.
            //   - else try the by-PK fast path `WHERE [t.]id = <n>` →
            //     single-row `Stmt::Update`.
            //   - SP-PG-SQL-DML-GENERAL: if the by-PK fast path rejects
            //     (non-`id` column, non-`=` op, multi-predicate), restore
            //     the cursor to the WHERE clause and compile the GENERAL
            //     predicate → `Stmt::UpdateWhere` (server resolves the
            //     matching ids + builds a concrete Txn).
            if let Some(n) = legacy_id {
                return Ok(Stmt::Update { type_id: ot.type_id, id: n, sets });
            }
            let where_start = p.i;
            match parse_where_id_eq(&mut p) {
                Ok(id) => {
                    // by-PK fast path. If a RETURNING clause follows, route
                    // through the (server-side, read-back-capable)
                    // UpdateWhere path with `by_pk_id` set — the engine
                    // skips the scan and mutates this single id, then reads
                    // it back for RETURNING. Otherwise the plain single-row
                    // RMW (byte-identical to before).
                    let returning = parse_returning(&mut p)?;
                    if let Some(ret) = returning {
                        return Ok(Stmt::UpdateWhere {
                            type_id: ot.type_id,
                            program: Vec::new(),
                            sets,
                            returning: Some(ret),
                            by_pk_id: Some(id),
                        });
                    }
                    return Ok(Stmt::Update { type_id: ot.type_id, id, sets });
                }
                Err(_) => {
                    // General WHERE: rewind to the WHERE keyword and
                    // compile the predicate program (reuses the SELECT
                    // `compile_where` grammar). The leading `WHERE` is
                    // required (a SET with NO WHERE would update every
                    // row — V1 rejects the unguarded form to avoid a
                    // silent table-wide mutation footgun).
                    p.i = where_start;
                    if !p.kw("WHERE") {
                        return Err(
                            "UPDATE needs `WHERE <predicate>` (or the \
                             legacy `ID <int>`); an unguarded table-wide \
                             UPDATE is rejected in V1"
                                .into(),
                        );
                    }
                    let program = compile_where(&mut p, &ot)?;
                    let returning = parse_returning(&mut p)?;
                    return Ok(Stmt::UpdateWhere {
                        type_id: ot.type_id,
                        program,
                        sets,
                        returning,
                        by_pk_id: None,
                    });
                }
            }
        }
    }
    // SP-PG-SQL-DML-GENERAL — DELETE general-WHERE. The by-PK + legacy
    // `ID <n>` DELETE shapes stay in `compile_from_tokens` (return
    // `Op::Delete`); here we intercept ONLY the general-predicate case
    // and produce `Stmt::DeleteWhere`. We peek the by-PK fast path; if
    // it rejects, rewind and compile the general predicate + RETURNING.
    {
        let mut p = P { t: toks.clone(), i: 0, cat };
        if p.kw("DELETE") {
            p.expect_kw("FROM")?;
            let tname = p.ident()?;
            let ot = p.type_named(&tname)?.clone();
            // Legacy `ID <n>` and by-PK `WHERE id = n` → fall through to
            // the Op::Delete path in `compile_from_tokens`.
            let is_legacy_id = matches!(p.peek(), Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("ID"));
            if !is_legacy_id {
                let where_start = p.i;
                match parse_where_id_eq(&mut p) {
                    Ok(id) => {
                        // by-PK DELETE. A RETURNING clause routes through
                        // the server-side read-back path (DeleteWhere with
                        // `by_pk_id`); else fall through to `Op::Delete`.
                        let returning = parse_returning(&mut p)?;
                        if let Some(ret) = returning {
                            return Ok(Stmt::DeleteWhere {
                                type_id: ot.type_id,
                                program: Vec::new(),
                                returning: Some(ret),
                                by_pk_id: Some(id),
                            });
                        }
                        // fall through to Op::Delete below
                    }
                    Err(_) => {
                        p.i = where_start;
                        if !p.kw("WHERE") {
                            return Err(
                                "DELETE needs `WHERE <predicate>` (or the \
                                 legacy `ID <int>`); an unguarded table-wide \
                                 DELETE is rejected in V1"
                                    .into(),
                            );
                        }
                        let program = compile_where(&mut p, &ot)?;
                        let returning = parse_returning(&mut p)?;
                        return Ok(Stmt::DeleteWhere {
                            type_id: ot.type_id,
                            program,
                            returning,
                            by_pk_id: None,
                        });
                    }
                }
            }
        }
    }
    Ok(Stmt::Op(compile_from_tokens(toks, cat)?))
}

/// SP-PG-SQL-DML-GENERAL — parse an optional trailing `RETURNING
/// <cols | *>` clause inside the parser (after a general-WHERE
/// UPDATE/DELETE predicate). Returns `None` (no clause), `Some(["*"])`
/// (star sentinel — the gateway expands to every column via
/// `describe_table`), or `Some([col, …])` (lenient qualifier strip,
/// `col AS alias` accepted-and-skipped). Mirrors `dml_returning`'s
/// clause grammar but operates on the live parser cursor.
fn parse_returning(p: &mut P) -> Result<Option<Vec<String>>, SqlError> {
    if !p.kw("RETURNING") {
        return Ok(None);
    }
    // `RETURNING *`
    if matches!(p.peek(), Some(Tok::Star)) {
        p.i += 1;
        return Ok(Some(vec!["*".to_string()]));
    }
    let mut cols = Vec::new();
    loop {
        let col = p.col_ident()?; // strips an optional `t.` qualifier
        cols.push(col);
        // accept-and-skip `col AS alias`
        if p.kw("AS") {
            let _alias = p.ident()?;
        }
        match p.peek() {
            Some(Tok::Punct(',')) => {
                p.i += 1;
                continue;
            }
            _ => break,
        }
    }
    if cols.is_empty() {
        return Err("RETURNING needs ≥1 column (or `*`)".into());
    }
    Ok(Some(cols))
}

/// SP-PG-SQL-ORM-PARSE T2 — parse a `WHERE [table.]id = <int>` clause
/// and return the integer id. The standard ORM UPDATE/DELETE shapes
/// target a row by its primary key (`WHERE orm_users.id = $1`), and
/// KesselDB keys every row by the `id` pseudo-column (the ObjectId), so
/// this is the by-PK row selector. The qualifier (`orm_users.`) is
/// stripped (lenient); the column MUST be `id` (the pseudo-PK) and the
/// comparison MUST be `=` against an integer. Anything else
/// (non-`id` column, non-eq operator, multi-predicate WHERE) is the
/// named follow-up `SP-PG-SQL-UPDATE-WHERE-GENERAL` (needs a server-side
/// scan-resolve-RMW that V1 doesn't have) and returns a precise error.
fn parse_where_id_eq(p: &mut P) -> Result<u128, SqlError> {
    if !p.kw("WHERE") {
        return Err(
            "UPDATE/DELETE needs `WHERE id = <int>` (or the legacy \
             `ID <int>`)"
                .into(),
        );
    }
    // `[table.]id` — strip the optional qualifier, require column `id`.
    let col = p.col_ident()?;
    if !col.eq_ignore_ascii_case("id") {
        return Err(format!(
            "V1 UPDATE/DELETE WHERE targets the primary key only \
             (`WHERE id = <int>`); `{col}` is not the row id \
             (SP-PG-SQL-UPDATE-WHERE-GENERAL)"
        ));
    }
    match p.next() {
        Some(Tok::Cmp("=")) => {}
        _ => return Err("UPDATE/DELETE WHERE id needs `= <int>`".into()),
    }
    let id = match p.next() {
        Some(Tok::Int(n)) => n as u128,
        // pgJDBC simple-mode / cast-stripped `'42'` arrives as a string;
        // coerce a clean decimal the same way the INSERT id path does.
        Some(Tok::Str(s)) => s
            .parse::<i128>()
            .map(|n| n as u128)
            .map_err(|_| "WHERE id must be an integer".to_string())?,
        Some(Tok::Bytes(b)) => std::str::from_utf8(&b)
            .ok()
            .and_then(|s| s.parse::<i128>().ok())
            .map(|n| n as u128)
            .ok_or_else(|| "WHERE id must be an integer".to_string())?,
        _ => return Err("WHERE id needs an integer value".into()),
    };
    // Reject a trailing AND/OR — V1 only resolves a single by-PK
    // predicate. (A residual multi-predicate WHERE would otherwise be
    // silently ignored, which would be a correctness footgun.)
    if let Some(Tok::Ident(k)) = p.peek() {
        if k.eq_ignore_ascii_case("AND") || k.eq_ignore_ascii_case("OR") {
            return Err(
                "V1 UPDATE/DELETE WHERE supports a single `id = <int>` \
                 predicate (SP-PG-SQL-UPDATE-WHERE-GENERAL)"
                    .into(),
            );
        }
    }
    Ok(id)
}

/// Compile one SQL statement to an `Op`. `cat` is needed for everything
/// except `CREATE TABLE`. `UPDATE` is rejected here (use `compile_stmt` +
/// a server that can read-modify-write).
pub fn compile(sql: &str, cat: &Catalog) -> Result<Op, SqlError> {
    compile_from_tokens(lex(sql)?, cat)
}

/// SP-PG-EXTQ-PARSED T2 — like `compile`, but accepts a slice of bound
/// `Option<Value>` parameters. Any `$N` placeholder in the SQL
/// resolves to `params[n-1]` at the TOKEN level — the bound value is
/// NEVER concatenated into SQL text, closing the SP-PG-EXTQ V1 §11
/// weak-spot #1 attack surface. Per the design spec §3.1 rewrite
/// rules: `Some(Value::Int(i))` → `Tok::Int(i)`, `Some(Value::Uint(u))`
/// → `Tok::Int(u as i128)` (errors on overflow), `Some(Value::Blob(b))`
/// → `Tok::Bytes(b)` (SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 — raw bytes
/// preserved verbatim, NO UTF-8 round-trip), `Some(Value::Null)` /
/// `None` → `Tok::Ident("NULL")`. Out-of-bounds `$N` returns
/// `SqlError`. The rewritten token stream is handed to the existing
/// parser unchanged — the compiled `Op` is byte-identical to what
/// you'd get from the equivalent SQL with literal values in place of
/// the placeholders.
pub fn compile_with_params(
    sql: &str,
    cat: &Catalog,
    params: &[Option<Value>],
) -> Result<Op, SqlError> {
    let toks = rewrite_param_tokens(lex(sql)?, params)?;
    compile_from_tokens(toks, cat)
}

/// SP-PG-EXTQ-PARSED T2 — like `compile_stmt`, but accepts a slice of
/// bound `Option<Value>` parameters. Same token-rewrite shape as
/// `compile_with_params`; works for the UPDATE path that
/// `compile_stmt` handles in addition to the everything-else
/// `compile` path. EXPLAIN inside compile_stmt_with_params just
/// delegates to compile_stmt_with_params recursively against the
/// inner statement (params apply to the inner statement, same as
/// any other compile path).
pub fn compile_stmt_with_params(
    sql: &str,
    cat: &Catalog,
    params: &[Option<Value>],
) -> Result<Stmt, SqlError> {
    // EXPLAIN <stmt> — compile the inner statement and describe its plan
    // WITHOUT executing it. Same prefix-handling as `compile_stmt`.
    let t = sql.trim_start();
    if t.len() >= 8 && t[..7].eq_ignore_ascii_case("EXPLAIN") {
        let rest = t[7..].trim_start();
        if rest.is_empty() {
            return Err("EXPLAIN needs a statement".into());
        }
        let inner = compile_stmt_with_params(rest, cat, params)?;
        return Ok(Stmt::Explain(plan_string(&inner, cat)));
    }
    let toks = rewrite_param_tokens(lex(sql)?, params)?;
    compile_stmt_from_tokens(toks, sql, cat, params)
}

/// SP-PG-EXTQ-PARSED T2 — token-level rewrite of `Tok::Param(n)` to
/// the concrete token for `params[n-1]`. Per design spec §3.1:
///
/// - `Some(Value::Int(i))` → `Tok::Int(i)`.
/// - `Some(Value::Uint(u))` → `Tok::Int(u as i128)` if it fits,
///   else `SqlError`.
/// - `Some(Value::Blob(b))` → `Tok::Bytes(b)` (SP-PG-EXTQ-PARSED-
///   BYTEA-TYPED T2 — preserves arbitrary bytes including non-UTF8
///   sequences). The parser's value-position arms accept
///   `Tok::Bytes` alongside `Tok::Str` and route to `Lit::Bytes`
///   which `lit_to_value` materializes as `Value::Blob` for
///   CHAR/BYTES/Ref columns, or attempts UTF-8 + decimal coercion
///   for numeric columns (mirrors the SP-PG-SQL-PAREN-VALUES path).
/// - `Some(Value::Null)` or `None` → `Tok::Ident("NULL")`. The
///   parser already accepts the bare `NULL` keyword in literal
///   positions.
/// - `n == 0` → defensive `SqlError` (the lexer already rejects
///   `\$0` so this branch is unreachable in practice).
/// - `n > params.len()` → `SqlError::unbound parameter \$N`.
///
/// SECURITY: the bound value's bytes never enter the SQL text. They
/// enter as a typed `Value`, get materialized as a typed `Tok`,
/// and emerge in the program as the original `Value` — no
/// concatenation, no quoting, no escape rules.
fn rewrite_param_tokens(
    toks: Vec<Tok>,
    params: &[Option<Value>],
) -> Result<Vec<Tok>, SqlError> {
    let mut out = Vec::with_capacity(toks.len());
    for tok in toks {
        match tok {
            Tok::Param(n) => {
                if n == 0 {
                    return Err(
                        "unreachable: `$0` should have been rejected at lex time".into(),
                    );
                }
                let idx = (n as usize).saturating_sub(1);
                if idx >= params.len() {
                    return Err(format!(
                        "unbound parameter `${n}` (only {bound} bound)",
                        bound = params.len()
                    ));
                }
                match &params[idx] {
                    None | Some(Value::Null) => {
                        out.push(Tok::Ident("NULL".to_string()));
                    }
                    Some(Value::Int(i)) => out.push(Tok::Int(*i)),
                    Some(Value::Uint(u)) => {
                        if *u > i128::MAX as u128 {
                            return Err(format!(
                                "parameter `${n}` value {u} overflows i128 (V1 limit)"
                            ));
                        }
                        out.push(Tok::Int(*u as i128));
                    }
                    Some(Value::Blob(b)) => {
                        // SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 — emit a
                        // `Tok::Bytes` so non-UTF8 byte sequences are
                        // preserved verbatim. The prior V1 shape did
                        // `String::from_utf8_lossy(b)` here, which
                        // corrupts any byte outside the valid-UTF-8
                        // grammar (0xC0-0xC1, 0xF5-0xFF, isolated
                        // continuation bytes 0x80-0xBF) BEFORE the
                        // bytes reach the storage layer. The new
                        // `Tok::Bytes` shape carries the bytes through
                        // to `Lit::Bytes` → `lit_to_value` which
                        // produces `Value::Blob` byte-for-byte. Bytes
                        // still NEVER touch SQL text.
                        out.push(Tok::Bytes(b.clone()));
                    }
                }
            }
            other => out.push(other),
        }
    }
    Ok(out)
}

/// SP-PG-EXTQ-PARSED T2 — body of `compile` extracted so
/// `compile_with_params` can share it. The lex step happens in the
/// caller; this function takes the (possibly pre-rewritten) token
/// vec and runs the existing parser dispatch path against it.
fn compile_from_tokens(toks: Vec<Tok>, cat: &Catalog) -> Result<Op, SqlError> {
    let mut p = P {
        t: toks,
        i: 0,
        cat,
    };
    if p.kw("UPDATE") {
        return Err("UPDATE requires server-side execution (compile_stmt)".into());
    }
    if p.kw("DESCRIBE") || p.kw("DESC") {
        let tname = p.ident()?;
        let ot = p.type_named(&tname)?;
        return Ok(Op::Describe { type_id: ot.type_id });
    }
    // REFRESH <name> — trigger a pull of an external source (SP91).
    if p.kw("REFRESH") {
        let name = p.ident()?;
        return Ok(Op::RefreshExternalSource { name });
    }
    if p.kw("DROP") {
        // DROP EXTERNAL SOURCE <name> — destructive DDL (SP91). Checked
        // first; only consumes when `EXTERNAL` matches so DROP INDEX /
        // DROP TABLE still parse.
        if p.kw("EXTERNAL") {
            p.expect_kw("SOURCE")?;
            let name = p.ident()?;
            return Ok(Op::DropExternalSource { name });
        }
        // DROP INDEX ON <t> (cols) — destructive DDL (SP74). Drops the
        // index(es) on exactly those columns; queries still work
        // (verified scan fallback), just un-accelerated.
        if p.kw("INDEX") {
            p.expect_kw("ON")?;
            let tname = p.ident()?;
            let ot = p.type_named(&tname)?.clone();
            p.punct('(')?;
            let mut cols = Vec::new();
            loop {
                let c = p.ident()?;
                let f = ot
                    .fields
                    .iter()
                    .find(|f| f.name == c)
                    .ok_or_else(|| unknown_column_err(&c, &ot))?;
                cols.push(f.field_id);
                match p.next() {
                    Some(Tok::Punct(',')) => continue,
                    Some(Tok::Punct(')')) => break,
                    _ => return Err("expected `,` or `)`".into()),
                }
            }
            return Ok(Op::DropIndex { type_id: ot.type_id, fields: cols });
        }
        // DROP TABLE <name> — destructive DDL (Sub-project 54).
        p.expect_kw("TABLE")?;
        let tname = p.ident()?;
        let ot = p.type_named(&tname)?;
        return Ok(Op::DropType { type_id: ot.type_id });
    }

    if p.kw("ALTER") {
        // ALTER TABLE <t> ADD [COLUMN] <name> <type>[(n)] [NOT NULL]
        // — online schema evolution (no table lock). The engine assigns
        // the new field id and enforces the online-DDL rule that an added
        // column must be nullable; a `NOT NULL` add surfaces as a clean
        // SchemaError at apply.
        p.expect_kw("TABLE")?;
        let tname = p.ident()?;
        let ot = p.type_named(&tname)?.clone();
        // SP75: destructive ALTER — DROP / RENAME COLUMN.
        if p.kw("DROP") {
            let _ = p.kw("COLUMN"); // optional noise word
            let c = p.ident()?;
            let f = ot
                .fields
                .iter()
                .find(|f| f.name == c)
                .ok_or_else(|| unknown_column_err(&c, &ot))?;
            return Ok(Op::DropField {
                type_id: ot.type_id,
                field_id: f.field_id,
            });
        }
        if p.kw("RENAME") {
            let _ = p.kw("COLUMN");
            let c = p.ident()?;
            let f = ot
                .fields
                .iter()
                .find(|f| f.name == c)
                .ok_or_else(|| unknown_column_err(&c, &ot))?
                .field_id;
            p.expect_kw("TO")?;
            let newname = p.ident()?;
            return Ok(Op::RenameField {
                type_id: ot.type_id,
                field_id: f,
                name: newname,
            });
        }
        p.expect_kw("ADD")?;
        // SP77: ALTER TABLE t ADD BALANCE GUARD [ON] <col>
        if p.kw("BALANCE") {
            p.expect_kw("GUARD")?;
            let _ = p.kw("ON"); // optional noise word
            let c = p.ident()?;
            let f = ot
                .fields
                .iter()
                .find(|f| f.name == c)
                .ok_or_else(|| unknown_column_err(&c, &ot))?;
            return Ok(Op::AddBalanceGuard {
                type_id: ot.type_id,
                field_id: f.field_id,
            });
        }
        let _ = p.kw("COLUMN"); // optional noise word
        let cname = p.ident()?;
        let tyname = p.ident()?;
        let mut arg = None;
        if matches!(p.peek(), Some(Tok::Punct('('))) {
            p.punct('(')?;
            match p.next() {
                Some(Tok::Int(n)) => arg = Some(n),
                _ => return Err("expected size".into()),
            }
            p.punct(')')?;
        }
        let mut nullable = true;
        if p.kw("NOT") {
            p.expect_kw("NULL")?;
            nullable = false;
        }
        let field = Field {
            field_id: 0,
            name: cname,
            kind: kind_of(&tyname, arg)?,
            nullable,
        };
        return Ok(Op::AlterTypeAddField {
            type_id: ot.type_id,
            field: encode_field(&field),
        });
    }

    if p.kw("CREATE") {
        // CREATE EXTERNAL SOURCE name (col TYPE[(n)] [NOT NULL] FROM 'src',
        // ...) FROM 'url' FORMAT JSON|CSV KEY col
        // [AUTH BEARER ENV 'E' | AUTH HEADER 'H' ENV 'E'] — SP91.
        // Checked before the index/table forms; only consumes when
        // `EXTERNAL` matches.
        if p.kw("EXTERNAL") {
            p.expect_kw("SOURCE")?;
            let name = p.ident()?;
            p.punct('(')?;
            if matches!(p.peek(), Some(Tok::Punct(')'))) {
                return Err("EXTERNAL SOURCE must declare at least one column".into());
            }
            let mut fields = Vec::new();
            let mut mapping: Vec<(u16, String)> = Vec::new();
            let mut next_fid: u16 = 1;
            loop {
                let cname = p.ident()?;
                let tyname = p.ident()?;
                let mut arg = None;
                if matches!(p.peek(), Some(Tok::Punct('('))) {
                    p.punct('(')?;
                    match p.next() {
                        Some(Tok::Int(n)) => arg = Some(n),
                        _ => return Err("expected size".into()),
                    }
                    p.punct(')')?;
                }
                let mut nullable = true;
                if p.kw("NOT") {
                    p.expect_kw("NULL")?;
                    nullable = false;
                }
                p.expect_kw("FROM")?;
                let src = match p.next() {
                    Some(Tok::Str(s)) => s,
                    _ => return Err("expected 'source' string".into()),
                };
                fields.push(Field {
                    field_id: 0,
                    name: cname,
                    kind: kind_of(&tyname, arg)?,
                    nullable,
                });
                mapping.push((next_fid, src));
                next_fid += 1;
                match p.next() {
                    Some(Tok::Punct(',')) => continue,
                    Some(Tok::Punct(')')) => break,
                    _ => return Err("expected `,` or `)`".into()),
                }
            }
            p.expect_kw("FROM")?;
            let url = match p.next() {
                Some(Tok::Str(s)) => s,
                _ => return Err("expected 'url' string".into()),
            };
            p.expect_kw("FORMAT")?;
            let format = if p.kw("JSON") {
                0u8
            } else if p.kw("CSV") {
                1u8
            } else if p.kw("NDJSON") {
                2u8
            } else if p.kw("PARQUET") {
                3u8
            } else {
                return Err("FORMAT must be JSON, CSV, or NDJSON".into());
            };
            p.expect_kw("KEY")?;
            let key_name = p.ident()?;
            let key_field_id = fields
                .iter()
                .position(|f| f.name == key_name)
                .map(|i| (i as u16) + 1)
                .ok_or_else(|| {
                    format!("KEY `{key_name}` is not a declared column")
                })?;
            let is_obj = url.starts_with("s3://") || url.starts_with("az://");
            let is_s3 = url.starts_with("s3://");
            let mut region: Option<String> = None;
            let mut endpoint: Option<String> = None;
            if p.kw("REGION") {
                region = Some(match p.next() {
                    Some(Tok::Str(s)) => s,
                    _ => return Err("expected 'region' string after REGION".into()),
                });
            }
            if p.kw("ENDPOINT") {
                endpoint = Some(match p.next() {
                    Some(Tok::Str(s)) => s,
                    _ => return Err("expected 'endpoint' url after ENDPOINT".into()),
                });
            }
            let (mut auth_kind, mut auth_a, mut auth_b) =
                (0u8, String::new(), String::new());
            let mut obj: Option<(u8, String)> = None;
            if p.kw("AUTH") {
                if p.kw("BEARER") {
                    p.expect_kw("ENV")?;
                    auth_kind = 1;
                    auth_a = match p.next() {
                        Some(Tok::Str(s)) => s,
                        _ => return Err("expected 'ENV_NAME'".into()),
                    };
                } else if p.kw("HEADER") {
                    auth_kind = 2;
                    auth_a = match p.next() {
                        Some(Tok::Str(s)) => s,
                        _ => return Err("expected 'Header-Name'".into()),
                    };
                    p.expect_kw("ENV")?;
                    auth_b = match p.next() {
                        Some(Tok::Str(s)) => s,
                        _ => return Err("expected 'ENV_NAME'".into()),
                    };
                } else if p.kw("OBJSTORE") {
                    auth_kind = 3;
                    if p.kw("S3") {
                        p.expect_kw("KEYID")?;
                        p.expect_kw("ENV")?;
                        auth_a = match p.next() {
                            Some(Tok::Str(s)) => s,
                            _ => return Err("expected 'KEYID_ENV'".into()),
                        };
                        p.expect_kw("SECRET")?;
                        p.expect_kw("ENV")?;
                        auth_b = match p.next() {
                            Some(Tok::Str(s)) => s,
                            _ => return Err("expected 'SECRET_ENV'".into()),
                        };
                        obj = Some((1u8, String::new()));
                    } else if p.kw("AZURE") {
                        let acct = if p.kw("ACCOUNT") {
                            match p.next() {
                                Some(Tok::Str(s)) => s,
                                _ => return Err("expected 'account' string after ACCOUNT".into()),
                            }
                        } else {
                            String::new()
                        };
                        p.expect_kw("KEY")?;
                        p.expect_kw("ENV")?;
                        auth_a = match p.next() {
                            Some(Tok::Str(s)) => s,
                            _ => return Err("expected 'ACCOUNT_KEY_ENV'".into()),
                        };
                        obj = Some((2u8, acct));
                    } else {
                        return Err("AUTH OBJSTORE must be S3 KEYID ENV '..' SECRET ENV '..' | AZURE [ACCOUNT '<a>'] KEY ENV '..'".into());
                    }
                } else {
                    return Err(
                        "AUTH must be BEARER ENV '..' or HEADER '..' ENV '..' or OBJSTORE S3|AZURE .."
                            .into(),
                    );
                }
            }
            let mut rows_path: Option<String> = None;
            if p.kw("ROWS") {
                rows_path = Some(match p.next() {
                    Some(Tok::Str(s)) => s,
                    _ => return Err("expected 'rows-path' string after ROWS".into()),
                });
            }
            let mut pagination: Option<(u8, String, String)> = None;
            if p.kw("PAGE") {
                if p.kw("NEXT") {
                    if p.kw("JSON") {
                        let path = match p.next() {
                            Some(Tok::Str(s)) => s,
                            _ => return Err("expected 'path' string after PAGE NEXT JSON".into()),
                        };
                        pagination = Some((1, path, String::new()));
                    } else if p.kw("LINK") {
                        pagination = Some((2, String::new(), String::new()));
                    } else {
                        return Err("PAGE NEXT must be JSON '<path>' or LINK".into());
                    }
                } else if p.kw("CURSOR") {
                    p.expect_kw("JSON")?;
                    let path = match p.next() {
                        Some(Tok::Str(s)) => s,
                        _ => return Err("expected 'path' string after PAGE CURSOR JSON".into()),
                    };
                    p.expect_kw("PARAM")?;
                    let param = match p.next() {
                        Some(Tok::Str(s)) => s,
                        _ => return Err("expected 'param' string after PARAM".into()),
                    };
                    pagination = Some((3, path, param));
                } else {
                    return Err("PAGE must be NEXT JSON|LINK or CURSOR JSON '<p>' PARAM '<q>'".into());
                }
            }
            // Compatibility matrix (CREATE-time, before building the op):
            let body_cursor = matches!(pagination, Some((1, _, _)) | Some((3, _, _)));
            if body_cursor {
                if format == 0 && rows_path.is_none() {
                    return Err("FORMAT JSON with a body cursor (PAGE NEXT JSON / PAGE CURSOR JSON) requires ROWS '<path>'".into());
                }
                if format == 1 {
                    return Err("FORMAT CSV cannot use a body cursor; use PAGE NEXT LINK or omit PAGE".into());
                }
                if format == 2 {
                    return Err("FORMAT NDJSON cannot use a body cursor (no envelope object); use PAGE NEXT LINK or omit PAGE".into());
                }
            }
            let objstore: Option<(u8, String, String, String)> = if is_obj {
                if format == 3 {
                    if pagination.is_some() {
                        return Err("PAGE clauses are not supported with FORMAT PARQUET".into());
                    }
                    if rows_path.is_some() {
                        return Err("ROWS is not applicable to FORMAT PARQUET".into());
                    }
                    // PARQUET over object store: accepted (OBJ-2a).
                }
                if pagination.is_some() {
                    return Err("PAGE clauses are not supported for object store (s3://|az://) sources".into());
                }
                if let Some(ep) = &endpoint {
                    if !ep.starts_with("https://") {
                        return Err("object-store ENDPOINT must be https://".into());
                    }
                }
                let (prov, acct) = obj.ok_or_else(|| "object-store (s3://|az://) requires AUTH OBJSTORE S3 …|AZURE …".to_string())?;
                if is_s3 && region.is_none() && endpoint.is_none() {
                    return Err("S3 (s3://) source requires REGION '<r>' (or an ENDPOINT)".into());
                }
                if !is_s3 {
                    // az://: exactly one of AUTH OBJSTORE AZURE ACCOUNT
                    // '<a>' XOR ENDPOINT '<url>' (the storage account is
                    // an identity, not a path component).
                    let has_acct = !acct.is_empty();
                    let has_ep = endpoint.is_some();
                    if has_acct == has_ep {
                        return Err("az:// requires exactly one of AUTH OBJSTORE AZURE ACCOUNT '<a>' or ENDPOINT '<url>'".into());
                    }
                }
                Some((prov, acct, region.unwrap_or_default(), endpoint.unwrap_or_default()))
            } else {
                if obj.is_some() {
                    return Err("AUTH OBJSTORE is only valid for s3://|az:// sources".into());
                }
                if format == 3 {
                    return Err("FORMAT PARQUET is only supported for object-store (s3://|az://) sources".into());
                }
                None
            };
            let type_def = encode_type_def(&name, &fields);
            return Ok(Op::CreateExternalSource {
                name,
                type_def,
                url,
                format,
                key_field_id,
                auth_kind,
                auth_a,
                auth_b,
                mapping,
                rows_path,
                pagination,
                objstore,
            });
        }
        // CREATE [UNIQUE|RANGE] INDEX ON t (cols) — DDL for indexes.
        let unique = p.kw("UNIQUE");
        let range = p.kw("RANGE");
        if unique || range || {
            // lookahead: `INDEX` (without consuming if it's TABLE)
            matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("INDEX"))
        } {
            p.expect_kw("INDEX")?;
            p.expect_kw("ON")?;
            let tname = p.ident()?;
            let ot = p.type_named(&tname)?.clone();
            p.punct('(')?;
            let mut cols = Vec::new();
            loop {
                let c = p.ident()?;
                let f = ot
                    .fields
                    .iter()
                    .find(|f| f.name == c)
                    .ok_or_else(|| unknown_column_err(&c, &ot))?;
                cols.push(f.field_id);
                match p.next() {
                    Some(Tok::Punct(',')) => continue,
                    Some(Tok::Punct(')')) => break,
                    _ => return Err("expected `,` or `)`".into()),
                }
            }
            if cols.len() > 1 {
                if unique || range {
                    return Err("UNIQUE/RANGE index must be single-column".into());
                }
                return Ok(Op::AddCompositeIndex {
                    type_id: ot.type_id,
                    fields: cols,
                });
            }
            let fid = cols[0];
            return Ok(if unique {
                Op::AddUnique { type_id: ot.type_id, field_id: fid }
            } else if range {
                Op::AddOrderedIndex { type_id: ot.type_id, field_id: fid }
            } else {
                Op::CreateIndex { type_id: ot.type_id, field_id: fid }
            });
        }
        p.expect_kw("TABLE")?;
        let name = p.ident()?;
        p.punct('(')?;
        let mut fields = Vec::new();
        // SP86: per-column DEFAULT, keyed by the field id the engine
        // will assign (positional, 1-based — matches CreateType).
        let mut defaults: Vec<(u16, Vec<u8>)> = Vec::new();
        // SP-PG-SERIAL-RETURNING: track which columns were declared with a
        // SERIAL-family type, and which column(s) are the PRIMARY KEY
        // (inline modifier or table-level constraint). A column that is
        // BOTH serial AND the PK ⇒ deterministic autoincrement on the
        // `id` pseudo-PK / ObjectId.
        let mut serial_col_names: Vec<String> = Vec::new();
        let mut pk_col_names: Vec<String> = Vec::new();
        // SP-PG-DDL-FK-ENFORCE: FOREIGN KEY descriptors captured BY NAME
        // during the DDL parse. The child type_id is only minted when the
        // CreateType op APPLIES, so we thread these names through the
        // type-def blob and resolve `ref_type_id`/`field_id` at apply time.
        let mut fk_specs: Vec<kessel_catalog::FkSpec> = Vec::new();
        loop {
            // SP-PG-SQL-ORM-PARSE T5 — table-level `PRIMARY KEY (col, ...)`
            // constraint (SQLAlchemy's `create_all` emits a trailing
            // `PRIMARY KEY (id)` clause). KesselDB keys every row by the
            // `id` pseudo-column (the ObjectId), so an explicit PK
            // declaration is metadata we accept-and-skip rather than a
            // column definition. Consume the whole `(col [, col]*)` group.
            // (Composite / non-`id` PKs are honored only insofar as the
            // row id is the ObjectId; a true multi-column PK index is the
            // named follow-up `SP-PG-DDL-COMPOSITE-PK`.)
            if p.kw("PRIMARY") {
                p.expect_kw("KEY")?;
                p.punct('(')?;
                loop {
                    // SP-PG-SERIAL-RETURNING: capture the PK column name(s)
                    // (still accept-and-skip as a stored column, but we
                    // need the name to mark a SERIAL PK as autoincrement).
                    pk_col_names.push(p.col_ident()?);
                    match p.next() {
                        Some(Tok::Punct(',')) => continue,
                        Some(Tok::Punct(')')) => break,
                        _ => return Err("expected `,` or `)` in PRIMARY KEY".into()),
                    }
                }
                // After a table constraint, either the column list ends
                // (`)`) or another `,`-separated item follows.
                match p.next() {
                    Some(Tok::Punct(',')) => continue,
                    Some(Tok::Punct(')')) => break,
                    _ => return Err("expected `,` or `)` after PRIMARY KEY".into()),
                }
            }
            // SP-PG-ORM-RELATIONSHIPS — table-level `FOREIGN KEY (col [,col]*)
            // REFERENCES tbl [(col [,col]*)] [ON DELETE/UPDATE action]`
            // constraint. SQLAlchemy's `create_all` renders a child model's
            // `ForeignKey("authors.id")` as this trailing table constraint.
            // KesselDB keys every row by the `id` pseudo-PK and stores the FK
            // column as its declared type, so the constraint is metadata we
            // accept-and-skip at the SQL-DDL layer (V1 does NOT enforce
            // referential integrity here — that is the engine `Op::AddForeignKey`
            // path, named follow-up `SP-PG-DDL-FK-ENFORCE`). Consume the whole
            // `FOREIGN KEY (…) REFERENCES tbl [(…)] [ON …]` group.
            if p.kw("FOREIGN") {
                p.expect_kw("KEY")?;
                p.punct('(')?;
                // SP-PG-DDL-FK-ENFORCE: capture the child column name(s).
                // V1 enforces single-column FKs (every row is keyed by the
                // `id` pseudo-PK); a composite FK captures only its FIRST
                // column for enforcement (named follow-up
                // `SP-PG-DDL-COMPOSITE-FK`).
                let mut child_cols: Vec<String> = Vec::new();
                loop {
                    child_cols.push(p.col_ident()?);
                    match p.next() {
                        Some(Tok::Punct(',')) => continue,
                        Some(Tok::Punct(')')) => break,
                        _ => return Err("expected `,` or `)` in FOREIGN KEY".into()),
                    }
                }
                p.expect_kw("REFERENCES")?;
                let ref_tbl = p.ident()?;
                // Optional `( col [,col]* )` referenced-column list.
                let mut ref_cols: Vec<String> = Vec::new();
                if matches!(p.peek(), Some(Tok::Punct('('))) {
                    p.i += 1; // consume `(`
                    loop {
                        ref_cols.push(p.col_ident()?);
                        match p.next() {
                            Some(Tok::Punct(',')) => continue,
                            Some(Tok::Punct(')')) => break,
                            _ => {
                                return Err(
                                    "expected `,` or `)` in REFERENCES column list"
                                        .into(),
                                )
                            }
                        }
                    }
                }
                let on_delete = parse_referential_actions(&mut p);
                if let Some(child_col) = child_cols.into_iter().next() {
                    fk_specs.push(kessel_catalog::FkSpec {
                        child_col,
                        ref_table: ref_tbl,
                        ref_col: ref_cols.into_iter().next(),
                        on_delete,
                    });
                }
                match p.next() {
                    Some(Tok::Punct(',')) => continue,
                    Some(Tok::Punct(')')) => break,
                    _ => return Err("expected `,` or `)` after FOREIGN KEY".into()),
                }
            }
            let cname = p.ident()?;
            let tname = p.ident()?;
            let mut arg = None;
            if matches!(p.peek(), Some(Tok::Punct('('))) {
                p.punct('(')?;
                match p.next() {
                    Some(Tok::Int(n)) => arg = Some(n),
                    _ => return Err("expected size".into()),
                }
                p.punct(')')?;
            }
            // SP-PG-SERIAL-RETURNING: remember a SERIAL-typed column by
            // name (the type itself still maps to its plain integer width
            // via `kind_of`). A SERIAL column that is also the PK becomes
            // a deterministic autoincrement below.
            if is_serial_type(&tname) {
                serial_col_names.push(cname.clone());
            }
            let kind = kind_of(&tname, arg)?;
            // Per-column modifier run. SP-PG-DDL-IDENTITY: Django 6 emits
            // the run `NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS
            // IDENTITY`, so the modifiers are parsed order-independently
            // (any of NOT NULL / PRIMARY KEY / GENERATED…IDENTITY / DEFAULT
            // in any order, each at most meaningfully once) rather than in
            // a fixed sequence. A bare `id BIGSERIAL PRIMARY KEY` still
            // takes the identical path (PRIMARY KEY branch).
            let mut nullable = true;
            loop {
                if p.kw("NOT") {
                    p.expect_kw("NULL")?;
                    nullable = false;
                    continue;
                }
                // SP-PG-SQL-ORM-PARSE T5 — inline column `PRIMARY KEY`
                // modifier (`id BIGSERIAL PRIMARY KEY`). Accept-and-skip
                // (the row id is the ObjectId pseudo-PK); also implies NOT
                // NULL, which we honor.
                if p.kw("PRIMARY") {
                    p.expect_kw("KEY")?;
                    nullable = false;
                    pk_col_names.push(cname.clone());
                    continue;
                }
                // SP-PG-DDL-IDENTITY — `GENERATED { ALWAYS | BY DEFAULT }
                // AS IDENTITY [ ( seq_options ) ]` is the SQL-standard
                // autoincrement spelling Django 6's default `BigAutoField`
                // renders (NOT `BIGSERIAL`). Treat it identically to a
                // SERIAL-family type: mark the column SERIAL so a column
                // that is ALSO the PK becomes the deterministic
                // autoincrement (reusing the SP-PG-SERIAL counter). The
                // declared type (`bigint` → I64) is preserved. The
                // optional `( … )` sequence-options group (`START WITH n`,
                // `INCREMENT BY n`) is parsed-and-IGNORED in V1 (the
                // deterministic counter starts at 1, increments by 1 —
                // named follow-up `SP-PG-IDENTITY-SEQOPTS`).
                if p.kw("GENERATED") {
                    // `ALWAYS` | `BY DEFAULT`
                    if p.kw("ALWAYS") {
                        // ok
                    } else if p.kw("BY") {
                        p.expect_kw("DEFAULT")?;
                    } else {
                        return Err(
                            "GENERATED must be followed by ALWAYS or BY DEFAULT"
                                .into(),
                        );
                    }
                    p.expect_kw("AS")?;
                    p.expect_kw("IDENTITY")?;
                    // Optional `( sequence_options )` — parse-and-ignore by
                    // consuming a balanced parenthesis group.
                    if matches!(p.peek(), Some(Tok::Punct('('))) {
                        p.i += 1; // consume `(`
                        let mut depth = 1usize;
                        while depth > 0 {
                            match p.next() {
                                Some(Tok::Punct('(')) => depth += 1,
                                Some(Tok::Punct(')')) => depth -= 1,
                                Some(_) => {}
                                None => {
                                    return Err(
                                        "unterminated IDENTITY sequence options"
                                            .into(),
                                    )
                                }
                            }
                        }
                    }
                    if !serial_col_names.iter().any(|c| c == &cname) {
                        serial_col_names.push(cname.clone());
                    }
                    continue;
                }
                if p.kw("DEFAULT") {
                    let v = match p.next() {
                        Some(Tok::Int(n)) => lit_to_value(&Lit::Int(n), kind)?,
                        Some(Tok::Str(s)) => lit_to_value(&Lit::Str(s), kind)?,
                        _ => return Err("DEFAULT needs a literal".into()),
                    };
                    let raw = kessel_codec::raw_from_value(kind, &v).ok_or(
                        "DEFAULT NULL is not supported (omit the column instead)",
                    )?;
                    defaults.push(((fields.len() + 1) as u16, raw));
                    continue;
                }
                // SP-PG-ORM-RELATIONSHIPS — inline column FK modifier
                // `REFERENCES tbl [(col)] [ON DELETE/UPDATE action]`
                // (the per-column FK spelling some ORMs emit, e.g.
                // `author_id BIGINT REFERENCES authors (id)`). Accept-and-skip
                // identically to the table-level FOREIGN KEY constraint; the
                // column is still stored as its declared `kind`.
                if p.kw("REFERENCES") {
                    let ref_tbl = p.ident()?;
                    let mut ref_cols: Vec<String> = Vec::new();
                    if matches!(p.peek(), Some(Tok::Punct('('))) {
                        p.i += 1; // consume `(`
                        loop {
                            ref_cols.push(p.col_ident()?);
                            match p.next() {
                                Some(Tok::Punct(',')) => continue,
                                Some(Tok::Punct(')')) => break,
                                _ => {
                                    return Err(
                                        "expected `,` or `)` in REFERENCES column list"
                                            .into(),
                                    )
                                }
                            }
                        }
                    }
                    let on_delete = parse_referential_actions(&mut p);
                    // SP-PG-DDL-FK-ENFORCE: inline column FK → same descriptor
                    // shape as the table-level FOREIGN KEY constraint, keyed by
                    // THIS column name.
                    fk_specs.push(kessel_catalog::FkSpec {
                        child_col: cname.clone(),
                        ref_table: ref_tbl,
                        ref_col: ref_cols.into_iter().next(),
                        on_delete,
                    });
                    continue;
                }
                break;
            }
            fields.push(Field {
                field_id: 0,
                name: cname,
                kind,
                nullable,
            });
            match p.next() {
                Some(Tok::Punct(',')) => continue,
                Some(Tok::Punct(')')) => break,
                _ => return Err("expected `,` or `)`".into()),
            }
        }
        // SP-PG-SERIAL-RETURNING: a column that is BOTH the PRIMARY KEY
        // and declared SERIAL becomes a deterministic autoincrement on
        // the `id` pseudo-PK / ObjectId. We pin the serial column's
        // stored field id (1-based positional, matching CreateType's
        // field-id assignment) so the SM patches the assigned value back
        // into the record (a later `SELECT id` reads it). V1 supports one
        // serial PK per table (the first PK column that is also serial).
        let serial_field_id: Option<u16> = pk_col_names
            .iter()
            .find(|pk| serial_col_names.iter().any(|s| s == *pk))
            .and_then(|pk| {
                fields.iter().position(|f| &f.name == pk).map(|i| (i + 1) as u16)
            });
        let serial_pk = serial_field_id.is_some();
        // SP-PG-DDL-FK-ENFORCE: thread captured FK descriptors (BY NAME)
        // through the type-def blob. When `fk_specs` is empty this is
        // BYTE-IDENTICAL to `encode_type_def_full` (no FK trailer emitted),
        // so a no-FK CREATE TABLE produces the same op frame as before this
        // arc. The engine resolves the names to ids and registers the FKs at
        // apply time (the child type_id is only assigned then).
        return Ok(Op::CreateType {
            def: kessel_catalog::encode_type_def_full_fk(
                &name, &fields, &defaults, serial_pk, serial_field_id, &fk_specs,
            ),
        });
    }

    if p.kw("INSERT") {
        p.expect_kw("INTO")?;
        let tname = p.ident()?;

        // Two forms:
        //  (legacy)  INSERT INTO t ID <n> (cols) VALUES (v..)
        //  (general) INSERT INTO t (id, cols) VALUES (v..)[, (v..)]*
        // The general form treats a pseudo-column `id` as the 128-bit row
        // id and supports multi-row inserts, which compile to one atomic
        // Op::Txn — one replicated round-trip for the whole batch.
        let legacy_id: Option<u128> = if p.kw("ID") {
            match p.next() {
                Some(Tok::Int(n)) => Some(n as u128),
                _ => return Err("INSERT needs `ID <int>`".into()),
            }
        } else {
            None
        };

        let ot = p.type_named(&tname)?.clone();
        p.punct('(')?;
        let mut cols = Vec::new();
        loop {
            cols.push(p.ident()?);
            match p.next() {
                Some(Tok::Punct(',')) => continue,
                Some(Tok::Punct(')')) => break,
                _ => return Err("expected `,` or `)`".into()),
            }
        }
        let id_pos = cols.iter().position(|c| c.eq_ignore_ascii_case("id"));
        // SP-PG-SERIAL-RETURNING: a `serial_pk` type whose INSERT omits
        // the row id (`INSERT INTO t (name) VALUES (…)`) autoincrements:
        // the engine assigns the id deterministically. We compile this to
        // an `Op::Create` carrying the `SERIAL_SENTINEL` id; the SM swaps
        // in the next per-type sequence value. The serial column's stored
        // field (if any) is filled with a 0 placeholder that the SM
        // patches with the assigned value.
        let serial_auto =
            legacy_id.is_none() && id_pos.is_none() && ot.serial_pk;
        if legacy_id.is_none() && id_pos.is_none() && !serial_auto {
            return Err(
                "INSERT needs a row id: either `ID <n>` or an `id` column".into(),
            );
        }

        p.expect_kw("VALUES")?;
        // One or more parenthesised value tuples.
        let mut ops: Vec<Op> = Vec::new();
        loop {
            p.punct('(')?;
            let mut raw = Vec::new();
            loop {
                // SP-PG-SQL-PAREN-VALUES: pgJDBC simple-mode
                // PreparedStatement wraps every substituted parameter in
                // parentheses (`VALUES (('42'), ('hello'))`). After the
                // SP-PG-EXTQ-CAST stripper drops the `::TYPE` the SQL the
                // parser sees still contains those expression-grouping
                // parens. PG treats `(LITERAL)` as equivalent to
                // `LITERAL`, so admit `(LITERAL)` up to a fixed nesting
                // depth (anti-stack-bomb cap). When `depth == 0` the
                // bare-literal path is byte-identical to the pre-arc
                // shape, so every prior KAT keeps passing.
                let mut depth = 0usize;
                while matches!(p.peek(), Some(Tok::Punct('('))) {
                    p.i += 1;
                    depth += 1;
                    if depth > 8 {
                        return Err(
                            "too many nested parens in VALUES".into(),
                        );
                    }
                }
                match p.next() {
                    Some(Tok::Int(n)) => raw.push(Lit::Int(n)),
                    Some(Tok::Str(s)) => raw.push(Lit::Str(s)),
                    // SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 — accept the
                    // raw-bytes shape emitted by the param-rewriter
                    // for `Value::Blob` bindings (non-UTF8 byte
                    // preservation through INSERT VALUES).
                    Some(Tok::Bytes(b)) => raw.push(Lit::Bytes(b)),
                    // SP-PG-NULL-INT-RENDER — the bare `NULL` keyword (lexed
                    // as an Ident) is an explicit SQL NULL value.
                    Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("NULL") => {
                        raw.push(Lit::Null)
                    }
                    _ => return Err("expected value".into()),
                }
                for _ in 0..depth {
                    match p.next() {
                        Some(Tok::Punct(')')) => {}
                        _ => {
                            return Err(
                                "expected `)` closing VALUES paren".into(),
                            )
                        }
                    }
                }
                match p.next() {
                    Some(Tok::Punct(',')) => continue,
                    Some(Tok::Punct(')')) => break,
                    _ => return Err("expected `,` or `)`".into()),
                }
            }
            if cols.len() != raw.len() {
                return Err("column/value count mismatch".into());
            }
            // Resolve the row id for this tuple. SP-PG-SQL-PAREN-VALUES:
            // accept `Lit::Str` whose contents parse as a decimal integer
            // (PG simple-mode `PreparedStatement.setLong(1, 42)` arrives
            // post-SP-PG-EXTQ-CAST-strip as the quoted literal `'42'`,
            // and the engine's type-checker for the `id` pseudo-column
            // must coerce that the way PG would coerce `'42'::int8`).
            let id = if serial_auto {
                // SERIAL autoincrement: the engine assigns the id. Carry
                // the reserved sentinel; the SM swaps in the real value.
                u128::MAX
            } else {
            match (legacy_id, id_pos) {
                (Some(n), _) => n,
                (None, Some(ip)) => match &raw[ip] {
                    Lit::Int(n) => *n as u128,
                    Lit::Str(s) => s
                        .parse::<i128>()
                        .map(|n| n as u128)
                        .map_err(|_| {
                            "`id` must be an integer".to_string()
                        })?,
                    // SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 — `id` bound
                    // via `Value::Blob` (e.g. psycopg2 `Binary(b"42")`)
                    // routes through `Tok::Bytes` → `Lit::Bytes`.
                    // Accept the same UTF-8 + decimal parse the
                    // `Lit::Str` path takes; reject non-numeric bytes.
                    Lit::Bytes(b) => std::str::from_utf8(b)
                        .ok()
                        .and_then(|s| s.parse::<i128>().ok())
                        .map(|n| n as u128)
                        .ok_or_else(|| {
                            "`id` must be an integer".to_string()
                        })?,
                    // SP-PG-NULL-INT-RENDER — the `id` pseudo-PK can never be
                    // NULL (it is the row's ObjectId).
                    Lit::Null => {
                        return Err(
                            "`id` must not be NULL (it is the primary key)"
                                .to_string(),
                        )
                    }
                },
                _ => unreachable!(),
            }
            };
            // Build field values in schema order (the `id` pseudo-column is
            // not a field; unlisted nullable fields => Null).
            let mut values = Vec::with_capacity(ot.fields.len());
            for f in &ot.fields {
                match cols.iter().position(|c| *c == f.name) {
                    Some(idx) => {
                        // SP-PG-NULL-INT-RENDER — an EXPLICIT `NULL` value
                        // (`INSERT INTO t (id, c) VALUES (1, NULL)`) stores a
                        // real SQL NULL (null-bitmap bit set) for a nullable
                        // column; a NOT NULL column rejects it cleanly. This
                        // mirrors the omitted-nullable-column path below so an
                        // explicit NULL round-trips identically to an omission.
                        if matches!(&raw[idx], Lit::Null) {
                            if f.nullable {
                                values.push(Value::Null);
                            } else {
                                return Err(format!(
                                    "null value in column `{}` violates \
                                     NOT NULL constraint",
                                    f.name
                                ));
                            }
                        } else {
                            values.push(lit_to_value(&raw[idx], f.kind)?);
                        }
                    }
                    None => {
                        // SP-PG-SERIAL-RETURNING: the omitted SERIAL PK
                        // column gets a non-null `0` placeholder; the SM
                        // patches it with the assigned autoincrement value
                        // (so it is never left NULL-violating, and a later
                        // `SELECT id` reads the engine-assigned id).
                        if serial_auto
                            && Some(f.field_id) == ot.serial_field_id
                        {
                            values.push(
                                kessel_codec::value_from_raw(
                                    f.kind,
                                    &vec![0u8; f.kind.width() as usize],
                                ),
                            );
                        }
                        // SP86: an omitted column takes its DEFAULT if
                        // one was declared; else NULL (nullable) or a
                        // clean error (NOT NULL, no default).
                        else if let Some((_, d)) =
                            ot.defaults.iter().find(|(fid, _)| *fid == f.field_id)
                        {
                            values.push(kessel_codec::value_from_raw(
                                f.kind, d,
                            ));
                        } else if f.nullable {
                            values.push(Value::Null);
                        } else {
                            return Err(format!(
                                "missing NOT NULL column `{}` (no default)",
                                f.name
                            ));
                        }
                    }
                }
            }
            let record =
                encode(&ot, &values).map_err(|e| format!("encode: {e:?}"))?;
            ops.push(Op::Create {
                type_id: ot.type_id,
                id: ObjectId::from_u128(id),
                record,
            });
            match p.peek() {
                Some(Tok::Punct(',')) => {
                    p.i += 1;
                    continue;
                }
                _ => break,
            }
        }
        return Ok(if ops.len() == 1 {
            ops.pop().unwrap()
        } else {
            // Multi-row: all rows land atomically in one replicated op.
            Op::Txn { ops }
        });
    }

    if p.kw("DELETE") {
        p.expect_kw("FROM")?;
        let tname = p.ident()?;
        // Two row-targeting shapes (mirrors UPDATE):
        //   (legacy)   DELETE FROM t ID <n>
        //   (standard) DELETE FROM t WHERE [t.]id = <n>   (ORM shape)
        // (SP-PG-SQL-ORM-PARSE T2)
        let id = if p.kw("ID") {
            match p.next() {
                Some(Tok::Int(n)) => n as u128,
                _ => return Err("DELETE needs `ID <int>`".into()),
            }
        } else {
            parse_where_id_eq(&mut p)?
        };
        let ot = p.type_named(&tname)?;
        return Ok(Op::Delete {
            type_id: ot.type_id,
            id: ObjectId::from_u128(id),
        });
    }

    if p.kw("SELECT") {
        // Fast path: `SELECT * FROM t [WHERE c=v [AND c=v]*] [LIMIT n]`
        // compiles to Op::QueryRows so equality on indexed columns is
        // sub-linear. Anything outside this restricted grammar restores the
        // cursor and falls back to the general (scan) planner.
        let save = p.i;
        if let Some(op) = try_query_rows(&mut p) {
            return Ok(op);
        }
        p.i = save;
        return compile_select(&mut p);
    }

    Err("unsupported statement".into())
}

#[derive(Clone)]
enum Lit {
    Int(i128),
    Str(String),
    /// SP-PG-NULL-INT-RENDER — the bare SQL `NULL` keyword in an INSERT
    /// VALUES tuple. Resolves to `Value::Null` for a nullable target column
    /// (sets the record's null bitmap bit) and a clean error for a NOT NULL
    /// column or the `id` pseudo-PK.
    Null,
    /// SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 — raw bytes from a bound
    /// `Value::Blob` parameter. Threads non-UTF8 byte sequences
    /// through to `lit_to_value` without the UTF-8 round-trip that
    /// `Lit::Str` requires.
    Bytes(Vec<u8>),
}

fn lit_to_value(l: &Lit, k: FieldKind) -> Result<Value, SqlError> {
    use FieldKind::*;
    Ok(match (l, k) {
        (Lit::Int(n), I8 | I16 | I32 | I64 | I128 | Fixed { .. }) => Value::Int(*n),
        (Lit::Int(n), U8 | U16 | U32 | U64 | U128 | Bool | Timestamp) => {
            Value::Uint(*n as u128)
        }
        (Lit::Str(s), Char(_) | Bytes(_) | Ref | OverflowRef) => {
            Value::Blob(s.clone().into_bytes())
        }
        (Lit::Int(n), Ref) => Value::Blob((*n as u128).to_le_bytes().to_vec()),
        // SP-PG-SQL-PAREN-VALUES: pgJDBC simple-mode
        // `PreparedStatement.setLong(1, 42)` arrives as the quoted
        // literal `'42'::int8`; the SP-PG-EXTQ-CAST T2 stripper drops
        // the `::int8` cast, leaving `Lit::Str("42")`. PG would coerce
        // that to int8 via the cast text; here we coerce it the same
        // way for the numeric column kinds when the string is a clean
        // decimal integer. Mismatches (non-numeric string, overflow)
        // fall through to the existing `literal/column type mismatch`
        // error.
        (Lit::Str(s), I8 | I16 | I32 | I64 | I128 | Fixed { .. })
            if s.parse::<i128>().is_ok() =>
        {
            Value::Int(s.parse::<i128>().unwrap())
        }
        (Lit::Str(s), U8 | U16 | U32 | U64 | U128 | Bool | Timestamp)
            if s.parse::<u128>().is_ok() =>
        {
            Value::Uint(s.parse::<u128>().unwrap())
        }
        // SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 — raw bytes from a
        // `Value::Blob` parameter binding. For CHAR/BYTES/Ref
        // columns the bytes flow through verbatim (NO UTF-8 round-
        // trip — this is the bug-fix headline: `String::from_utf8_
        // lossy` in the prior path corrupted non-UTF8 bytes here).
        (Lit::Bytes(b), Char(_) | Bytes(_) | Ref | OverflowRef) => {
            Value::Blob(b.clone())
        }
        // For numeric column kinds, attempt UTF-8 + decimal parse so
        // a pyscopg2 `cursor.execute("INSERT ... VALUES (%s)",
        // (b"42",))` bound to an integer column still works the same
        // way `Lit::Str("42")` does. Mismatches fall through to the
        // generic error.
        (Lit::Bytes(b), I8 | I16 | I32 | I64 | I128 | Fixed { .. })
            if std::str::from_utf8(b).ok()
                .and_then(|s| s.parse::<i128>().ok())
                .is_some() =>
        {
            Value::Int(
                std::str::from_utf8(b)
                    .unwrap()
                    .parse::<i128>()
                    .unwrap(),
            )
        }
        (Lit::Bytes(b), U8 | U16 | U32 | U64 | U128 | Bool | Timestamp)
            if std::str::from_utf8(b).ok()
                .and_then(|s| s.parse::<u128>().ok())
                .is_some() =>
        {
            Value::Uint(
                std::str::from_utf8(b)
                    .unwrap()
                    .parse::<u128>()
                    .unwrap(),
            )
        }
        _ => return Err("literal/column type mismatch".into()),
    })
}

/// SP-PG-SQL-ORM-PARSE T2 — collapse qualified column references
/// (`IDENT DOT IDENT` → the trailing column `IDENT`) in a WHERE token
/// span so the index-hint extractors (`eq_preds` walk + range
/// `extract_range_preds`) treat `t.id = 1` IDENTICALLY to `id = 1`.
/// The compiled WHERE *program* is already qualifier-stripped by
/// `term_hinted`; this makes the index *hints* match too, so a
/// qualified query compiles to the BYTE-IDENTICAL Op as its bare
/// equivalent (the determinism contract). A span with no `.` is
/// returned token-for-token unchanged, so every prior hint KAT is
/// preserved.
fn strip_span_qualifiers(span: &[Tok]) -> Vec<Tok> {
    let mut out: Vec<Tok> = Vec::with_capacity(span.len());
    let mut i = 0;
    while i < span.len() {
        // `IDENT . IDENT` → push only the column ident, skip the
        // qualifier + dot. Guard the lookahead so a trailing `.` (which
        // can't legally occur in a compiled span) doesn't panic.
        if let Tok::Ident(_) = &span[i] {
            if i + 2 < span.len()
                && matches!(span[i + 1], Tok::Punct('.'))
                && matches!(span[i + 2], Tok::Ident(_))
            {
                out.push(span[i + 2].clone());
                i += 3;
                continue;
            }
        }
        out.push(span[i].clone());
        i += 1;
    }
    out
}

/// SP-Analytic-Plan T3: extract `(field_id, op, value)` half-range
/// hints from a WHERE token span, mirroring the exact shape
/// `try_query_rows` uses for `Op::QueryRows`. Conjunct-safety gate
/// (no top-level `OR` / `NOT` / parens) is part of the helper too —
/// a disjunctive WHERE silently returns an empty vec (safe: the
/// program-only path == verified full scan).
///
/// `op` encoding: 0=`>`, 1=`>=`, 2=`<`, 3=`<=`. The field must have
/// an order index (numeric ≤8B or CHAR/BYTES); otherwise the hint is
/// silently dropped (caller's narrowing helper would ignore it too).
///
/// Used by BOTH `try_query_rows` (Op::QueryRows) AND `compile_select`'s
/// `Proj::Agg` branch (Op::Aggregate / Op::GroupAggregate) so an
/// aggregate WHERE with `d >= LO AND d < HI` on an order-indexed `d`
/// gains the same scan-narrowing acceleration as the row query.
fn extract_range_preds(
    ot: &ObjectType,
    span: &[Tok],
) -> Vec<(u16, u8, Vec<u8>)> {
    let unsafe_for_hints = span.iter().any(|t| {
        matches!(t, Tok::Punct('('))
            || matches!(t, Tok::Ident(k)
                if k.eq_ignore_ascii_case("OR")
                || k.eq_ignore_ascii_case("NOT"))
    });
    if unsafe_for_hints {
        return Vec::new();
    }
    let mut out: Vec<(u16, u8, Vec<u8>)> = Vec::new();
    let mut i = 0;
    while i + 2 < span.len() {
        // SP70: `col {> >= < <=} int` on an order-indexed column.
        if let (Tok::Ident(c), Tok::Cmp(cmp), Tok::Int(n)) =
            (&span[i], &span[i + 1], &span[i + 2])
        {
            let rop = match *cmp {
                ">" => Some(0u8),
                ">=" => Some(1u8),
                "<" => Some(2u8),
                "<=" => Some(3u8),
                _ => None,
            };
            if let (Some(rop), Some(f)) =
                (rop, ot.fields.iter().find(|f| &f.name == c))
            {
                if ot.ordered.contains(&f.field_id) {
                    let w = f.kind.width() as usize;
                    out.push((
                        f.field_id,
                        rop,
                        n.to_le_bytes()[..w.min(16)].to_vec(),
                    ));
                }
            }
        }
        // SP90: `col {> >= < <=} 'str'` on an order-indexed CHAR/BYTES.
        if let (Tok::Ident(c), Tok::Cmp(cmp), Tok::Str(s)) =
            (&span[i], &span[i + 1], &span[i + 2])
        {
            let rop = match *cmp {
                ">" => Some(0u8),
                ">=" => Some(1u8),
                "<" => Some(2u8),
                "<=" => Some(3u8),
                _ => None,
            };
            if let (Some(rop), Some(f)) =
                (rop, ot.fields.iter().find(|f| &f.name == c))
            {
                if ot.ordered.contains(&f.field_id)
                    && matches!(
                        f.kind,
                        kessel_catalog::FieldKind::Char(_)
                            | kessel_catalog::FieldKind::Bytes(_)
                    )
                {
                    out.push((f.field_id, rop, s.clone().into_bytes()));
                }
            }
        }
        // SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 — same range-hint shape but
        // for a raw-bytes literal from a `Value::Blob` parameter
        // binding. Preserves non-UTF8 bytes through the index hint.
        if let (Tok::Ident(c), Tok::Cmp(cmp), Tok::Bytes(b)) =
            (&span[i], &span[i + 1], &span[i + 2])
        {
            let rop = match *cmp {
                ">" => Some(0u8),
                ">=" => Some(1u8),
                "<" => Some(2u8),
                "<=" => Some(3u8),
                _ => None,
            };
            if let (Some(rop), Some(f)) =
                (rop, ot.fields.iter().find(|f| &f.name == c))
            {
                if ot.ordered.contains(&f.field_id)
                    && matches!(
                        f.kind,
                        kessel_catalog::FieldKind::Char(_)
                            | kessel_catalog::FieldKind::Bytes(_)
                    )
                {
                    out.push((f.field_id, rop, b.clone()));
                }
            }
        }
        i += 1;
    }
    out
}

/// Try the restricted `SELECT * FROM t [WHERE c=v [AND c=v]*] [LIMIT n]`
/// grammar -> `Op::QueryRows`. Returns None (caller restores + falls back)
/// on anything outside it.
fn try_query_rows(p: &mut P) -> Option<Op> {
    if !matches!(p.peek(), Some(Tok::Star)) {
        return None;
    }
    p.i += 1;
    if !p.kw("FROM") {
        return None;
    }
    let tname = match p.next() {
        Some(Tok::Ident(s)) => s,
        _ => return None,
    };
    let ot = p.type_named(&tname).ok()?.clone();
    let mut eq_preds: Vec<(u16, Vec<u8>)> = Vec::new();
    // SP70: half-range hints on order-indexed columns. Same safety gate
    // as eq hints (mandatory conjunct only); the engine narrows via the
    // order index and the full program still verifies every candidate,
    // so the result is identical to a scan — only faster.
    let mut range_preds: Vec<(u16, u8, Vec<u8>)> = Vec::new();
    // SP62: the FULL `WHERE` is compiled to the verifying program (every
    // predicate kind: =, range, IN/BETWEEN/LIKE/IS NULL, AND/OR/NOT). The
    // engine re-verifies every candidate with it, so the result is
    // *always* identical to a scan regardless of the candidate set —
    // index hints can only speed it up, never change the answer.
    let program: Vec<u8> = if p.kw("WHERE") {
        let ws = p.i;
        let prog = compile_where(p, &ot).ok()?;
        // SP-PG-SQL-ORM-PARSE T2 — normalize qualified column refs
        // (`t.id` → `id`) BEFORE the hint walk so a qualified WHERE
        // emits the SAME eq/range hints as its bare equivalent (Op
        // byte-identity / determinism contract).
        let span_owned = strip_span_qualifiers(&p.t[ws..p.i]);
        let span: &[Tok] = &span_owned;
        // A `col = literal` hint is only SAFE if it is a *mandatory*
        // conjunct — i.e. the WHERE has NO top-level OR/NOT/parentheses,
        // so every comparison must hold. Otherwise emit no hints (the
        // program-only path == a verified full scan: still correct).
        let unsafe_for_hints = span.iter().any(|t| {
            matches!(t, Tok::Punct('('))
                || matches!(t, Tok::Ident(k)
                    if k.eq_ignore_ascii_case("OR")
                    || k.eq_ignore_ascii_case("NOT"))
        });
        if !unsafe_for_hints {
            let mut i = 0;
            while i + 2 < span.len() {
                if let (Tok::Ident(c), Tok::Cmp("="), lit) =
                    (&span[i], &span[i + 1], &span[i + 2])
                {
                    if let Some(f) = ot.fields.iter().find(|f| &f.name == c) {
                        // Hint if the column is single-indexed OR a member
                        // of a composite index (the engine then picks the
                        // single or composite lookup). SP62/SP63.
                        let usable = ot.indexes.contains(&f.field_id)
                            || ot
                                .composite
                                .iter()
                                .any(|ci| ci.contains(&f.field_id));
                        if usable {
                            let w = f.kind.width() as usize;
                            let hint = match lit {
                                Tok::Int(n) => {
                                    n.to_le_bytes()[..w.min(16)].to_vec()
                                }
                                Tok::Str(s) => s.clone().into_bytes(),
                                // SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 —
                                // raw-bytes from a `Value::Blob` param
                                // binding; preserves non-UTF8 bytes.
                                Tok::Bytes(b) => b.clone(),
                                _ => {
                                    i += 1;
                                    continue;
                                }
                            };
                            eq_preds.push((f.field_id, hint));
                        }
                    }
                }
                i += 1;
            }
            // SP-Analytic-Plan T3: range hints via shared helper (same
            // conjunct-safety gate already enforced above). Replaces the
            // SP70/SP90 inline walks formerly here — see extract_range_preds.
            range_preds = extract_range_preds(&ot, span);
        }
        prog
    } else {
        Program::new().push_int(1).bytes() // no WHERE ⇒ always true
    };
    let mut limit = 0u32;
    if p.kw("LIMIT") {
        match p.next() {
            Some(Tok::Int(n)) => limit = n as u32,
            _ => return None,
        }
    }
    // restricted grammar only — anything left (GROUP/ORDER/OFFSET/...) =>
    // bail to the general planner.
    match p.peek() {
        None | Some(Tok::Punct(';')) => {}
        _ => return None,
    }
    Some(Op::QueryRows {
        type_id: ot.type_id,
        eq_preds,
        program,
        limit,
        range_preds,
    })
}

fn compile_select(p: &mut P) -> Result<Op, SqlError> {
    // SP-Analytic-Plan-MULTI T3: projection parser now accepts a
    // comma-separated mix of plain identifiers (leading group cols) and
    // aggregate calls. Shapes (`g` = ident, `A(...)` = COUNT/SUM/MIN/
    // MAX/AVG):
    //   `*`                       → Proj::Star
    //   `g [, g]*`                → Proj::Cols
    //   `A(x)`                    → Proj::Aggs(1 agg, 0 leading cols)
    //   `A(x), B(y) [, …]`        → Proj::Aggs(≥2 aggs, 0 leading cols)
    //   `g [, g]*, A(x) [, A(y)]*` → Proj::Aggs(≥1 agg, ≥1 leading col)
    // Once an aggregate has been seen, subsequent plain identifiers are
    // an error (would imply a non-aggregated, non-GROUP-BY column).
    // SP-PG-SQL-AGG-ALIAS-RENDER — `alias` captures an `AS <ident>` output
    // name (`COUNT(*) AS "__count"` — Django's `.count()`). The alias does
    // NOT change the emitted `Op` (the Op proto carries no output name); it
    // exists so the sibling `select_aggregate` text-helper can name the
    // gateway RowDescription column. The single-aggregate emit stays
    // byte-identical.
    struct AggSpec {
        kind: u8,
        field: Option<String>,
        // SP-PG-SQL-JOIN-AGG: the aggregate arg's table qualifier, preserved so
        // the JOIN group-aggregate path can resolve `COUNT(b.id)` against the
        // combined `(a ++ b)` schema unambiguously even when `id` exists in both
        // tables. `None` for `COUNT(*)` or an unqualified arg. The single-table
        // aggregate path ignores this (it resolves against one table).
        qualifier: Option<String>,
        // The alias is captured so `AGG(...) AS alias` parses without
        // error; the gateway's `select_aggregate` text-helper (not this
        // in-parser struct) reads it to name the RowDescription column, so
        // the field is write-only here.
        #[allow(dead_code)]
        alias: Option<String>,
    }
    enum Proj {
        Star,
        Cols(Vec<String>),
        Aggs { leading_cols: Vec<String>, aggs: Vec<AggSpec> },
    }
    // Sniff a token as the start of an aggregate call (case-insensitive
    // keyword followed by `(`). Returns the canonical kind code (0..=4).
    fn agg_kind(w: &str) -> Option<u8> {
        match w.to_ascii_uppercase().as_str() {
            "COUNT" => Some(0),
            "SUM" => Some(1),
            "MIN" => Some(2),
            "MAX" => Some(3),
            "AVG" => Some(4),
            _ => None,
        }
    }
    fn parse_agg(p: &mut P) -> Result<AggSpec, SqlError> {
        // The caller has confirmed the next token is an aggregate ident.
        let kind = match p.next() {
            Some(Tok::Ident(w)) => agg_kind(&w).ok_or("not an aggregate")?,
            _ => return Err("aggregate name expected".into()),
        };
        p.punct('(')?;
        let mut qualifier: Option<String> = None;
        let field = if matches!(p.peek(), Some(Tok::Star)) {
            p.i += 1;
            None
        } else {
            // SP-PG-SQL-ORM-PARSE T2 — aggregate arg may be qualified
            // (`COUNT(orm_users.id)`); the single-table path strips the
            // qualifier, but SP-PG-SQL-JOIN-AGG needs it to disambiguate
            // `COUNT(b.id)` across the two joined tables, so capture it.
            let first = p.ident()?;
            if matches!(p.peek(), Some(Tok::Punct('.'))) {
                p.i += 1; // consume `.`
                let col = p.ident()?;
                if matches!(p.peek(), Some(Tok::Punct('.'))) {
                    return Err(
                        "schema-qualified column `a.b.c` not supported".into(),
                    );
                }
                qualifier = Some(first);
                Some(col)
            } else {
                Some(first)
            }
        };
        p.punct(')')?;
        // SP-PG-SQL-AGG-ALIAS-RENDER — optional output alias `AS <ident>`
        // (`COUNT(*) AS "__count"`). The quoted alias lexes as an Ident
        // after the quoted-identifier arc. Captured for the gateway's
        // RowDescription column name; does not affect the emitted Op.
        let alias = if p.kw("AS") {
            Some(p.ident()?)
        } else {
            None
        };
        Ok(AggSpec { kind, field, qualifier, alias })
    }
    // SP-PG-SQL-HAVING — a parsed HAVING predicate, BEFORE the aggregate is
    // matched to a projection index. `kind` + `field` (+ `qualifier`) identify
    // the aggregate by the same (function, arg) shape as the projection, `op`
    // is the canonical comparison wire code, `value` the i128 literal.
    struct RawHaving {
        kind: u8,
        field: Option<String>,
        qualifier: Option<String>,
        op: u8,
        value: i128,
    }
    // SP-PG-SQL-HAVING — parse `<AGG>(arg) <cmp> <int>`. The aggregate is one
    // of COUNT/SUM/MIN/MAX/AVG (same set as the projection); the comparison is
    // `> >= < <= = <> !=`; the RHS is an integer/numeric literal. The aggregate
    // is matched against (and MUST be one of) the SELECT projection's
    // aggregates by the caller — V1 does not compute extra aggregates for a
    // HAVING that references one not in the projection (it is cleanly rejected
    // there with a clear error).
    fn parse_having(p: &mut P) -> Result<RawHaving, SqlError> {
        // <AGG> ( arg )
        let kind = match p.next() {
            Some(Tok::Ident(w)) => agg_kind(&w).ok_or(
                "HAVING V1 supports only an aggregate predicate \
                 (COUNT/SUM/MIN/MAX/AVG) — e.g. `HAVING COUNT(*) > 2`",
            )?,
            _ => return Err("HAVING expects an aggregate".into()),
        };
        p.punct('(')?;
        let mut qualifier: Option<String> = None;
        let field = if matches!(p.peek(), Some(Tok::Star)) {
            p.i += 1;
            None
        } else {
            let first = p.ident()?;
            if matches!(p.peek(), Some(Tok::Punct('.'))) {
                p.i += 1;
                let col = p.ident()?;
                qualifier = Some(first);
                Some(col)
            } else {
                Some(first)
            }
        };
        p.punct(')')?;
        // comparison operator
        let op = match p.next() {
            Some(Tok::Cmp(c)) => kessel_proto::HavingPred::op_code(c)
                .ok_or_else(|| format!("HAVING: unsupported comparison `{c}`"))?,
            other => {
                return Err(format!(
                    "HAVING expects a comparison operator after the aggregate, \
                     got {other:?}"
                ))
            }
        };
        // RHS integer/numeric literal. A negative literal lexes as Minus then
        // Int (the lexer only emits unsigned Int); fold the sign here.
        let neg = matches!(p.peek(), Some(Tok::Minus));
        if neg {
            p.i += 1;
        }
        let value = match p.next() {
            Some(Tok::Int(n)) => {
                if neg {
                    -n
                } else {
                    n
                }
            }
            other => {
                return Err(format!(
                    "HAVING comparison RHS must be an integer/numeric literal, \
                     got {other:?}"
                ))
            }
        };
        Ok(RawHaving { kind, field, qualifier, op, value })
    }
    let proj = if matches!(p.peek(), Some(Tok::Star)) {
        p.i += 1;
        Proj::Star
    } else if let Some(Tok::Ident(_)) = p.peek() {
        // Walk the comma-separated projection list, splitting items into
        // leading group cols vs aggregates as we go. The first aggregate
        // flips a mode bit; after that, plain identifiers are an error.
        let mut leading_cols: Vec<String> = Vec::new();
        let mut aggs: Vec<AggSpec> = Vec::new();
        let mut have_agg = false;
        loop {
            // Look at the next ident WITHOUT consuming the cursor — the
            // aggregate-vs-column choice depends on whether `(` follows.
            let is_agg = match (p.peek().cloned(), p.t.get(p.i + 1).cloned()) {
                (Some(Tok::Ident(w)), Some(Tok::Punct('(')))
                    if agg_kind(&w).is_some() => true,
                _ => false,
            };
            if is_agg {
                aggs.push(parse_agg(p)?);
                have_agg = true;
            } else {
                if have_agg {
                    return Err(
                        "plain column after aggregate not allowed (move it before \
                         the aggregates or wrap it in MIN/MAX)"
                            .into(),
                    );
                }
                // SP-PG-SQL-ORM-PARSE T2 — projection columns may be
                // qualified (`orm_users.id`); strip the qualifier.
                leading_cols.push(p.col_ident()?);
                // SP-PG-SERIAL-RETURNING — a projection column may carry
                // an output alias `col AS alias` (SQLAlchemy emits
                // `SELECT widgets.id AS widgets_id, …` for its post-flush
                // refresh). Accept-and-skip the alias: V1 projects + names
                // by the SOURCE column (the engine's `Op::SelectFields`
                // output order; result mapping is positional). A named
                // RowDescription alias is the follow-up `SP-PG-SQL-PROJ-ALIAS`.
                if p.kw("AS") {
                    let _alias = p.ident()?;
                }
            }
            if matches!(p.peek(), Some(Tok::Punct(','))) {
                p.i += 1;
                continue;
            }
            break;
        }
        if aggs.is_empty() {
            Proj::Cols(leading_cols)
        } else {
            Proj::Aggs { leading_cols, aggs }
        }
    } else {
        return Err("bad SELECT projection".into());
    };
    p.expect_kw("FROM")?;
    let tname = p.ident()?;
    let ot = p.type_named(&tname)?.clone();
    // SP-PG-SQL-JOIN-ALIAS — the FROM table may carry an optional `[AS] <alias>`
    // (`FROM users u`, `FROM users AS u`) but ONLY in a JOIN query (a single-
    // table `SELECT * FROM users u` is a separate shape handled by the paths
    // below, which this arc leaves byte-identical). So we speculatively consume
    // an alias here and KEEP it only if a JOIN keyword follows; otherwise we
    // restore the cursor so the single-table fall-through is unchanged.
    let left_alias: Option<String> = {
        let save = p.i;
        let a = parse_optional_alias(p)?;
        // A join only if `[LEFT [OUTER]|INNER] JOIN` follows the (optional) alias.
        let join_follows = matches!(p.peek(), Some(Tok::Ident(k))
            if k.eq_ignore_ascii_case("JOIN")
                || k.eq_ignore_ascii_case("LEFT")
                || k.eq_ignore_ascii_case("INNER"));
        if join_follows {
            a
        } else {
            p.i = save; // not a join — give the alias token back.
            None
        }
    };
    // Equi-join: `SELECT * FROM a [LEFT [OUTER]] JOIN b ON a.x = b.y
    // [WHERE …] [LIMIT n]`. A bare `JOIN` is INNER (only matching left rows);
    // `LEFT [OUTER] JOIN` (SP-PG-SQL-OUTER-JOIN) emits EVERY left row, with
    // NULL `b.*` fields for left rows that have no right match.
    let join_type = if p.kw("LEFT") {
        // `OUTER` is an optional noise word in `LEFT OUTER JOIN`.
        let _ = p.kw("OUTER");
        p.expect_kw("JOIN")?;
        Some(kessel_proto::JoinType::Left)
    } else if p.kw("JOIN") {
        Some(kessel_proto::JoinType::Inner)
    } else {
        None // not a join shape — fall through to single-table paths below.
    };
    if let Some(join_type) = join_type {
        let rname = p.ident()?;
        let rt = p.type_named(&rname)?.clone();
        // SP-PG-SQL-JOIN-ALIAS — the right table's optional `[AS] <alias>`.
        let right_alias = parse_optional_alias(p)?;
        // SP-PG-SQL-JOIN-ALIAS — the running alias→table map. Every qualifier in
        // ON / WHERE / GROUP BY / ORDER BY is resolved THROUGH this so an alias
        // (`u`) AND a full table name (`users`) both reach the combined
        // `<table>.<col>` schema. The map grows as the multi-join chain is
        // parsed. We validate it incrementally (dup alias / self-join) so an
        // ambiguous spelling errors cleanly rather than mis-resolving.
        let mut refs: Vec<JoinTableRef> = vec![
            JoinTableRef { table: tname.clone(), alias: left_alias.clone() },
            JoinTableRef { table: rname.clone(), alias: right_alias.clone() },
        ];
        validate_join_refs(&refs)?;
        p.expect_kw("ON")?;
        let a1 = p.ident()?;
        p.punct('.')?;
        let c1 = p.ident()?;
        if !matches!(p.next(), Some(Tok::Cmp("="))) {
            return Err("JOIN ON needs `=`".into());
        }
        let a2 = p.ident()?;
        p.punct('.')?;
        let c2 = p.ident()?;
        // SP-PG-SQL-JOIN-ALIAS — resolve each ON qualifier (alias OR table name)
        // to the FULL table name before matching it against the two joined
        // tables, so `ON u.id = p.user_id` works exactly like the spelled-out
        // `ON users.id = posts.user_id`.
        let a1 = resolve_join_qualifier(&refs, &a1)?;
        let a2 = resolve_join_qualifier(&refs, &a2)?;
        // a1/a2 must name the two tables (either order)
        let (lf_tbl, lf_col, rf_tbl, rf_col) = if a1 == tname && a2 == rname {
            (&ot, c1, &rt, c2)
        } else if a1 == rname && a2 == tname {
            (&ot, c2, &rt, c1)
        } else {
            return Err("JOIN ON columns must reference the joined tables".into());
        };
        let lfid = lf_tbl
            .fields
            .iter()
            .find(|f| f.name == lf_col)
            .ok_or_else(|| unknown_column_err(&lf_col, lf_tbl))?
            .field_id;
        let rfid = rf_tbl
            .fields
            .iter()
            .find(|f| f.name == rf_col)
            .ok_or_else(|| unknown_column_err(&rf_col, rf_tbl))?
            .field_id;
        // SP-PG-SQL-MULTI-JOIN: consume any additional chained
        // `[INNER] JOIN <table> ON <a.x> = <b.y>` segments after the base join.
        // Each step's ON references the NEW table on one side and an ALREADY-
        // joined table on the other. We resolve the already-joined side to a
        // combined-schema field id over the running `(a ++ b ++ …)` schema and
        // the new side to the new table's own field id, emitting one `JoinStep`.
        // V1 is INNER only (`JOIN` / `INNER JOIN`); a `LEFT`/`RIGHT`/`FULL` in
        // the chain is a named follow-up (rejected). Tables are tracked by name
        // (aliases in a multi-join chain are a named follow-up).
        let mut joined: Vec<ObjectType> = vec![ot.clone(), rt.clone()];
        let mut extra_joins: Vec<kessel_proto::JoinStep> = Vec::new();
        // Peek for a chained JOIN (optionally `INNER JOIN`). A bare `JOIN` or
        // `INNER JOIN` continues the INNER chain; anything else ends it.
        loop {
            // Detect `INNER JOIN` / `JOIN`. A `LEFT`/`RIGHT`/`FULL` here is an
            // unsupported mixed-outer chain ⇒ explicit error.
            let is_join = if let Some(Tok::Ident(k)) = p.peek() {
                if k.eq_ignore_ascii_case("JOIN") {
                    p.i += 1;
                    true
                } else if k.eq_ignore_ascii_case("INNER") {
                    p.i += 1;
                    p.expect_kw("JOIN")?;
                    true
                } else if k.eq_ignore_ascii_case("LEFT")
                    || k.eq_ignore_ascii_case("RIGHT")
                    || k.eq_ignore_ascii_case("FULL")
                {
                    return Err(
                        "multi-join V1 supports only chained INNER joins \
                         (mixing LEFT/RIGHT/FULL into a chain is a follow-up)"
                            .into(),
                    );
                } else {
                    false
                }
            } else {
                false
            };
            if !is_join {
                break;
            }
            let cname = p.ident()?;
            if joined.iter().any(|t| t.name == cname) {
                return Err(format!(
                    "multi-join: table `{cname}` is already joined (self-joins with \
                     aliases are a named follow-up)"
                ));
            }
            let ct = p.type_named(&cname)?.clone();
            // SP-PG-SQL-JOIN-ALIAS — this chained table's optional `[AS] <alias>`.
            // Register it in `refs` BEFORE its ON clause so the ON qualifiers
            // (which name the new table + one already-joined table) can be
            // spelled with aliases.
            let calias = parse_optional_alias(p)?;
            refs.push(JoinTableRef { table: cname.clone(), alias: calias });
            validate_join_refs(&refs)?;
            p.expect_kw("ON")?;
            let q1 = p.ident()?;
            p.punct('.')?;
            let oc1 = p.ident()?;
            if !matches!(p.next(), Some(Tok::Cmp("="))) {
                return Err("JOIN ON needs `=`".into());
            }
            let q2 = p.ident()?;
            p.punct('.')?;
            let oc2 = p.ident()?;
            // SP-PG-SQL-JOIN-ALIAS — resolve both ON qualifiers (alias OR table
            // name) to full table names before the new-vs-already-joined check.
            let q1 = resolve_join_qualifier(&refs, &q1)?;
            let q2 = resolve_join_qualifier(&refs, &q2)?;
            // Exactly one side must be the NEW table `cname`; the other an
            // already-joined table.
            let (prev_tbl, prev_col, new_col) = if q2 == cname && q1 != cname {
                (q1, oc1, oc2)
            } else if q1 == cname && q2 != cname {
                (q2, oc2, oc1)
            } else {
                return Err(format!(
                    "multi-join ON must reference the new table `{cname}` and one \
                     already-joined table"
                ));
            };
            // Resolve the new table's join column to its own field id.
            let right_field = ct
                .fields
                .iter()
                .find(|f| f.name == new_col)
                .ok_or_else(|| unknown_column_err(&new_col, &ct))?
                .field_id;
            // Resolve the already-joined side to a combined field id over the
            // running combined schema (the tables joined SO FAR, in order).
            let prev_refs: Vec<&ObjectType> = joined.iter().collect();
            let prev_combined = combined_join_type_multi(&prev_refs);
            let prev_name = format!("{prev_tbl}.{prev_col}");
            // The qualifier must name one of the already-joined tables.
            if !joined.iter().any(|t| t.name == prev_tbl) {
                return Err(format!(
                    "multi-join ON qualifier `{prev_tbl}` is not an already-joined \
                     table"
                ));
            }
            let left_combined_field = prev_combined
                .fields
                .iter()
                .find(|f| f.name == prev_name)
                .ok_or_else(|| unknown_column_err(&prev_name, &prev_combined))?
                .field_id;
            extra_joins.push(kessel_proto::JoinStep {
                right_type: ct.type_id,
                left_combined_field,
                right_field,
            });
            joined.push(ct);
        }
        // The combined schema spanning ALL joined tables — used to resolve the
        // WHERE / ORDER BY columns when the chain has extra joins (for a plain
        // 2-table join `joined == [ot, rt]`, so this equals `combined_join_type`).
        let multi_refs: Vec<&ObjectType> = joined.iter().collect();
        let multi_cot = combined_join_type_multi(&multi_refs);
        let has_extra = !extra_joins.is_empty();
        // SP-PG-SQL-JOIN-WHERE: optional `WHERE <pred>` after the ON clause.
        // The predicate filters the COMBINED join rows, so it compiles against
        // the SAME combined schema the engine builds (left fields `<lt>.<col>`
        // then right fields `<rt>.<col>`, field ids reassigned `0..nL+nR`).
        // Qualified columns (`a.x`, `b.y`) resolve by the combined `<table>.
        // <col>` name; a bare `col` resolves by suffix with an ambiguity check.
        let filter = if p.kw("WHERE") {
            if has_extra {
                // SP-PG-SQL-MULTI-JOIN: resolve the WHERE over the full chain's
                // combined schema (qualifiers may name ANY joined table). SP-PG-
                // SQL-JOIN-ALIAS threads `refs` so an alias qualifier resolves.
                let names: Vec<&str> = joined.iter().map(|t| t.name.as_str()).collect();
                compile_join_where_multi(p, &multi_cot, &names, &refs)?
            } else {
                let cot = combined_join_type(&ot, &rt);
                compile_join_where(p, &cot, &ot.name, &rt.name, &refs)?
            }
        } else {
            Vec::new()
        };
        // SP-PG-SQL-JOIN-AGG: optional `GROUP BY <qualified col>` after the
        // optional WHERE. Present ⇒ the projection MUST be `<group col>, <agg>+`
        // (a `Proj::Aggs`); we resolve the group column + each aggregate arg
        // against the SAME combined `(a ++ b)` schema the engine builds and emit
        // an `Op::Join { group_aggregate: Some(..) }`. The engine groups the
        // combined rows + aggregates per group, returning the group-aggregate
        // result encoding. Composes with the WHERE filter above.
        let cot_ga = combined_join_type(&ot, &rt);
        let resolve_combined = |qt: &str, qc: &str| -> Result<u16, SqlError> {
            // SP-PG-SQL-JOIN-ALIAS — resolve an alias/table-name qualifier to the
            // FULL table name first (GROUP BY / aggregate / HAVING all flow here).
            let qt = resolve_join_qualifier(&refs, qt)?;
            if qt != ot.name && qt != rt.name {
                return Err(format!(
                    "qualifier `{qt}` is not one of the joined tables `{}` / `{}`",
                    ot.name, rt.name
                ));
            }
            let cn = format!("{qt}.{qc}");
            cot_ga
                .fields
                .iter()
                .find(|f| f.name == cn)
                .map(|f| f.field_id)
                .ok_or_else(|| unknown_column_err(&cn, &cot_ga))
        };
        let mut group_aggregate: Option<kessel_proto::JoinGroupAgg> = None;
        if p.kw("GROUP") {
            // SP-PG-SQL-MULTI-JOIN: GROUP BY over a chained (3+ table) join is a
            // named follow-up — the engine rejects multi-join + group_aggregate.
            if has_extra {
                return Err(
                    "GROUP BY over a chained multi-join is not supported in V1 \
                     (a named follow-up)".into(),
                );
            }
            p.expect_kw("BY")?;
            // qualified `a.c` (V1 requires a qualifier so the combined-schema
            // resolution is unambiguous across the two tables).
            let gqt = p.ident()?;
            p.punct('.')?;
            let gqc = p.ident()?;
            let group_field = resolve_combined(&gqt, &gqc)?;
            // The projection must be the aggregate shape. Pull the leading group
            // column(s) + aggregates out of `proj` (parsed up-front).
            let (leading_cols, aggs) = match &proj {
                Proj::Aggs { leading_cols, aggs } => (leading_cols, aggs),
                _ => {
                    return Err(
                        "GROUP BY over a JOIN requires an aggregate projection \
                         (e.g. `SELECT a.name, COUNT(b.id) … GROUP BY a.name`)"
                            .into(),
                    )
                }
            };
            // V1: exactly one leading group column, and it must match the GROUP
            // BY column (by its bare column name — `col_ident` already stripped
            // the qualifier when the projection was parsed).
            if leading_cols.len() != 1 {
                return Err(
                    "JOIN GROUP BY V1 supports exactly one leading group column \
                     matching the GROUP BY column"
                        .into(),
                );
            }
            if leading_cols[0] != gqc {
                return Err(format!(
                    "GROUP BY column `{gqc}` must match the leading projection \
                     column `{}`",
                    leading_cols[0]
                ));
            }
            // Resolve each aggregate's argument to a combined field id. `COUNT(*)`
            // (no arg) ⇒ the COUNT_STAR sentinel; `COUNT(col)` / SUM / MIN / MAX /
            // AVG carry the real combined field id. The arg may be qualified
            // (`b.id`) — `parse_agg` stored the bare column (qualifier stripped),
            // so we resolve it by suffix against the combined schema.
            let mut aggregates: Vec<(u8, u16)> = Vec::with_capacity(aggs.len());
            for a in aggs {
                let fid = match (&a.field, &a.qualifier) {
                    (None, _) => kessel_proto::COUNT_STAR_FIELD, // COUNT(*)
                    // Qualified `COUNT(b.id)` → resolve the exact combined name.
                    (Some(col), Some(qt)) => resolve_combined(qt, col)?,
                    // Bare `COUNT(id)` → resolve by `.<col>` suffix across the
                    // combined schema; ambiguous (in both tables) ⇒ error.
                    (Some(col), None) => {
                        let matches: Vec<u16> = cot_ga
                            .fields
                            .iter()
                            .filter(|f| {
                                f.name
                                    .rsplit('.')
                                    .next()
                                    .map(|tail| tail == col)
                                    .unwrap_or(false)
                            })
                            .map(|f| f.field_id)
                            .collect();
                        match matches.as_slice() {
                            [one] => *one,
                            [] => return Err(unknown_column_err(col, &cot_ga)),
                            _ => {
                                return Err(format!(
                                    "aggregate column `{col}` is ambiguous across \
                                     the joined tables — qualify it"
                                ))
                            }
                        }
                    }
                };
                aggregates.push((a.kind, fid));
            }
            // SP-PG-SQL-HAVING — optional `HAVING <AGG>(arg) <cmp> <int>` over a
            // JOIN group-aggregate, parsed after GROUP BY and before ORDER BY.
            // The HAVING aggregate is matched against the combined-schema
            // aggregate list just built (by kind + resolved combined field id),
            // exactly as the projection aggregates were resolved. V1: it MUST
            // match one of the projected aggregates (no extra aggregate is
            // computed for a HAVING-only aggregate — that is cleanly rejected).
            let having: Option<kessel_proto::HavingPred> = if p.kw("HAVING") {
                let h_kind = match p.next() {
                    Some(Tok::Ident(w)) => match w.to_ascii_uppercase().as_str() {
                        "COUNT" => 0,
                        "SUM" => 1,
                        "MIN" => 2,
                        "MAX" => 3,
                        "AVG" => 4,
                        _ => return Err(
                            "HAVING V1 supports only an aggregate predicate \
                             (COUNT/SUM/MIN/MAX/AVG)".into(),
                        ),
                    },
                    _ => return Err("HAVING expects an aggregate".into()),
                };
                p.punct('(')?;
                // arg: `*` | `[t.]col`
                let (h_qual, h_col): (Option<String>, Option<String>) =
                    if matches!(p.peek(), Some(Tok::Star)) {
                        p.i += 1;
                        (None, None)
                    } else {
                        let first = p.ident()?;
                        if matches!(p.peek(), Some(Tok::Punct('.'))) {
                            p.i += 1;
                            let c = p.ident()?;
                            (Some(first), Some(c))
                        } else {
                            (None, Some(first))
                        }
                    };
                p.punct(')')?;
                let h_op = match p.next() {
                    Some(Tok::Cmp(c)) => kessel_proto::HavingPred::op_code(c)
                        .ok_or_else(|| format!("HAVING: unsupported comparison `{c}`"))?,
                    other => {
                        return Err(format!(
                            "HAVING expects a comparison operator, got {other:?}"
                        ))
                    }
                };
                let neg = matches!(p.peek(), Some(Tok::Minus));
                if neg {
                    p.i += 1;
                }
                let h_val: i128 = match p.next() {
                    Some(Tok::Int(n)) => {
                        if neg {
                            -n
                        } else {
                            n
                        }
                    }
                    other => {
                        return Err(format!(
                            "HAVING comparison RHS must be an integer literal, \
                             got {other:?}"
                        ))
                    }
                };
                // Resolve the HAVING aggregate's arg to the same combined field
                // id the projection aggregates use (COUNT(*) ⇒ COUNT_STAR_FIELD).
                let h_field: u16 = match (&h_col, &h_qual) {
                    (None, _) => kessel_proto::COUNT_STAR_FIELD,
                    (Some(col), Some(qt)) => resolve_combined(qt, col)?,
                    (Some(col), None) => {
                        let matches: Vec<u16> = cot_ga
                            .fields
                            .iter()
                            .filter(|f| {
                                f.name.rsplit('.').next().map(|t| t == col).unwrap_or(false)
                            })
                            .map(|f| f.field_id)
                            .collect();
                        match matches.as_slice() {
                            [one] => *one,
                            [] => return Err(unknown_column_err(col, &cot_ga)),
                            _ => {
                                return Err(format!(
                                    "HAVING aggregate column `{col}` is ambiguous \
                                     across the joined tables — qualify it"
                                ))
                            }
                        }
                    }
                };
                let idx = aggregates
                    .iter()
                    .position(|(k, f)| *k == h_kind && *f == h_field);
                match idx {
                    Some(i) => Some(kessel_proto::HavingPred {
                        agg_index: i as u16,
                        op: h_op,
                        value: h_val,
                    }),
                    None => {
                        return Err(
                            "HAVING references an aggregate not in the SELECT list \
                             — V1 requires the HAVING aggregate to match one of the \
                             projected aggregates".into(),
                        )
                    }
                }
            } else {
                None
            };
            group_aggregate =
                Some(kessel_proto::JoinGroupAgg { group_field, aggregates, having });
        }
        // SP-PG-SQL-JOIN-QUERY: optional `ORDER BY <qualified col> [ASC|DESC]`
        // then `LIMIT n` / `OFFSET m`, composing with the optional WHERE above.
        // The ORDER BY column resolves against the SAME combined `(a ++ b)`
        // schema the engine builds (field names `<table>.<col>`, ids
        // `0..nL+nR`), so the emitted sort field id is the engine's combined
        // field id by construction.
        let mut order_by: Option<(u16, bool)> = None;
        if p.kw("ORDER") {
            p.expect_kw("BY")?;
            // qualified `a.c` (V1 requires a qualifier so the combined-schema
            // resolution is unambiguous across the two tables).
            let qt = p.ident()?;
            p.punct('.')?;
            let qc = p.ident()?;
            // SP-PG-SQL-JOIN-ALIAS — resolve an alias/table-name qualifier to the
            // FULL table name first, so `ORDER BY u.name` works like
            // `ORDER BY users.name`.
            let qt = resolve_join_qualifier(&refs, &qt)?;
            // SP-PG-SQL-MULTI-JOIN: resolve ORDER BY over the FULL chain schema
            // when there are extra joins (qualifier may name ANY joined table);
            // for a plain binary join this equals `combined_join_type(&ot,&rt)`.
            if !joined.iter().any(|t| t.name == qt) {
                let names: Vec<&str> = joined.iter().map(|t| t.name.as_str()).collect();
                return Err(format!(
                    "ORDER BY qualifier `{qt}` is not one of the joined tables {names:?}"
                ));
            }
            let combined_name = format!("{qt}.{qc}");
            let cot = if has_extra {
                multi_cot.clone()
            } else {
                combined_join_type(&ot, &rt)
            };
            let sfid = cot
                .fields
                .iter()
                .find(|f| f.name == combined_name)
                .ok_or_else(|| unknown_column_err(&combined_name, &cot))?
                .field_id;
            // optional ASC (default) / DESC
            let desc = if p.kw("DESC") {
                true
            } else {
                let _ = p.kw("ASC");
                false
            };
            order_by = Some((sfid, desc));
        }
        // LIMIT / OFFSET (either order, PG accepts both `LIMIT n OFFSET m` and
        // `OFFSET m LIMIT n`).
        let mut limit = 0u32;
        let mut limit_n: Option<u64> = None;
        let mut offset_n: Option<u64> = None;
        loop {
            if p.kw("LIMIT") {
                match p.next() {
                    Some(Tok::Int(n)) => limit_n = Some(n as u64),
                    _ => return Err("LIMIT needs int".into()),
                }
            } else if p.kw("OFFSET") {
                match p.next() {
                    Some(Tok::Int(n)) => offset_n = Some(n as u64),
                    _ => return Err("OFFSET needs int".into()),
                }
            } else {
                break;
            }
        }
        // Byte-identity: a bare `JOIN … LIMIT n` (NO ORDER BY, NO OFFSET) keeps
        // using the legacy pre-sort `limit` field so an existing join frame is
        // wire-unchanged. Any ORDER BY or OFFSET routes pagination to the
        // post-sort `limit_n`/`offset_n` (legacy `limit` = 0 = unbounded
        // pre-sort) so the full result is sorted before paginating.
        if order_by.is_none() && offset_n.is_none() {
            if let Some(n) = limit_n.take() {
                limit = n as u32;
            }
        }
        return Ok(Op::Join {
            left_type: ot.type_id,
            right_type: rt.type_id,
            left_field: lfid,
            right_field: rfid,
            limit,
            filter,
            join_type,
            order_by,
            limit_n,
            offset_n,
            group_aggregate,
            extra_joins,
        });
    }
    // Primary-key fast path: `SELECT ... FROM t ID <n>` -> O(1) GetById
    // (returns the whole record; projection/WHERE not applied to a
    // single-row id fetch — documented).
    if p.kw("ID") {
        let id = match p.next() {
            Some(Tok::Int(n)) => n as u128,
            _ => return Err("SELECT ... ID needs `<int>`".into()),
        };
        return Ok(Op::GetById {
            type_id: ot.type_id,
            id: ObjectId::from_u128(id),
        });
    }
    let fid = |n: &str| -> Result<u16, SqlError> {
        ot.fields
            .iter()
            .find(|f| f.name == n)
            .map(|f| f.field_id)
            .ok_or_else(|| unknown_column_err(n, &ot))
    };

    // SP-Analytic-Plan T3: capture the WHERE token span so we can
    // (separately) walk it for `(field, op, val)` half-range hints to
    // populate `range_preds` on aggregate Ops (mirroring exactly what
    // try_query_rows does for Op::QueryRows). The full WHERE still
    // compiles to the verifying `program` below, so the engine's
    // result is always identical to a scan — range hints only
    // accelerate.
    let (program, agg_range_preds) = if p.kw("WHERE") {
        let ws = p.i;
        let prog = compile_where(p, &ot)?;
        // SP-PG-SQL-ORM-PARSE T2 — qualifier-normalize the span so an
        // aggregate WHERE with `t.d >= LO` gains the same range hint as
        // the bare `d >= LO` (Op byte-identity).
        let span_owned = strip_span_qualifiers(&p.t[ws..p.i]);
        let rp = extract_range_preds(&ot, &span_owned);
        (prog, rp)
    } else {
        (Program::new().push_int(1).bytes(), Vec::new())
    };

    let mut group: Option<String> = None;
    let mut sort: Option<(String, bool)> = None;
    let mut limit: u32 = 0;
    let mut offset: u32 = 0;
    if p.kw("GROUP") {
        p.expect_kw("BY")?;
        // SP-PG-SQL-ORM-PARSE T2 — GROUP BY column may be qualified.
        group = Some(p.col_ident()?);
    }
    // SP-PG-SQL-HAVING — optional `HAVING <AGG>(arg) <cmp> <int>` after
    // GROUP BY, before ORDER BY. Parse the raw aggregate spec + comparison
    // + literal here; it is resolved against the projection's aggregate list
    // (matched by kind + arg column) inside the `Proj::Aggs` branch below.
    // A HAVING with NO GROUP BY is rejected (HAVING requires grouping).
    let having_raw: Option<RawHaving> = if p.kw("HAVING") {
        // V1: HAVING filters aggregate groups, so the projection MUST be an
        // aggregate projection (the HAVING aggregate is matched to one of the
        // SELECT aggregates). A HAVING over a non-aggregate SELECT is rejected.
        if !matches!(proj, Proj::Aggs { .. }) {
            return Err(
                "HAVING requires an aggregate projection (e.g. \
                 `SELECT g, COUNT(*) … GROUP BY g HAVING COUNT(*) > 2`)"
                    .into(),
            );
        }
        Some(parse_having(p)?)
    } else {
        None
    };
    // SP-PG-SQL-GROUP-SORT-LIMIT — ORDER BY target, captured richly enough to
    // support the plain-GROUP-BY cases: a column/alias ident, a 1-based
    // projection position (`ORDER BY 2`), or an aggregate expression
    // (`ORDER BY COUNT(*)` / `ORDER BY SUM(x)`). The non-aggregate
    // (SelectSorted) path only ever uses the `Ident` form (`sort`), so this is
    // purely additive there.
    enum RawOrderTarget {
        Ident(String),
        Position(usize),
        Agg { kind: u8, field: Option<String> },
    }
    let mut order_target: Option<(RawOrderTarget, bool)> = None;
    if p.kw("ORDER") {
        p.expect_kw("BY")?;
        // ORDER BY 2 — projection position (1-based).
        if let Some(Tok::Int(n)) = p.peek() {
            let n = *n;
            p.i += 1;
            let desc = p.kw("DESC");
            if !desc {
                let _ = p.kw("ASC");
            }
            order_target = Some((RawOrderTarget::Position(n as usize), desc));
        } else if matches!(p.peek(), Some(Tok::Ident(w)) if agg_kind(w).is_some())
            && matches!(p.t.get(p.i + 1), Some(Tok::Punct('(')))
        {
            // ORDER BY COUNT(*) / SUM(col) — an aggregate expression. Parse the
            // call shape (qualifier stripped — the single-table plain GROUP BY
            // path resolves against the one table's schema).
            let kind = match p.next() {
                Some(Tok::Ident(w)) => agg_kind(&w).ok_or("not an aggregate")?,
                _ => return Err("aggregate name expected in ORDER BY".into()),
            };
            p.punct('(')?;
            let field = if matches!(p.peek(), Some(Tok::Star)) {
                p.i += 1;
                None
            } else {
                Some(p.col_ident()?)
            };
            p.punct(')')?;
            let desc = p.kw("DESC");
            if !desc {
                let _ = p.kw("ASC");
            }
            order_target = Some((RawOrderTarget::Agg { kind, field }, desc));
        } else {
            // SP-PG-SQL-ORM-PARSE T2 — ORDER BY column may be qualified.
            let c = p.col_ident()?;
            let desc = p.kw("DESC");
            if !desc {
                let _ = p.kw("ASC");
            }
            // Keep `sort` populated for the existing SelectSorted path.
            sort = Some((c.clone(), desc));
            order_target = Some((RawOrderTarget::Ident(c), desc));
        }
    }
    if p.kw("LIMIT") {
        limit = match p.next() {
            Some(Tok::Int(n)) => n as u32,
            _ => return Err("LIMIT needs int".into()),
        };
    }
    if p.kw("OFFSET") {
        offset = match p.next() {
            Some(Tok::Int(n)) => n as u32,
            _ => return Err("OFFSET needs int".into()),
        };
    }

    match proj {
        Proj::Aggs { leading_cols, aggs } => {
            // SP-Analytic-Plan-MULTI T3: choose between the byte-identical
            // single-aggregate path (Op::Aggregate / Op::GroupAggregate)
            // and the multi-aggregate path (Op::GroupAggregateMulti).
            //
            //   1 agg + 0 leading cols + no GROUP BY → Op::Aggregate
            //   1 agg + 0 leading cols + GROUP BY g  → Op::GroupAggregate
            //   1 agg + 1 leading col   (=> GROUP BY implied)
            //                                        → Op::GroupAggregateMulti
            //   ≥2 aggs                              → Op::GroupAggregateMulti
            //
            // When `leading_cols` is non-empty AND there's also an explicit
            // GROUP BY, they must agree (V1 single-column GROUP BY only;
            // matches Op::GroupAggregate's constraint).
            let resolve_agg = |a: &AggSpec| -> Result<(u8, u16), SqlError> {
                let af = match &a.field {
                    Some(c) => fid(c)?,
                    None => 0,
                };
                Ok((a.kind, af))
            };
            // SP-PG-SQL-HAVING — resolve a RawHaving to a wire HavingPred by
            // matching its (kind, resolved agg field) against the SELECT's
            // aggregate list. V1: the HAVING aggregate MUST be one of the
            // projected aggregates (matched by kind + arg field id) — a HAVING
            // over an aggregate NOT in the projection is cleanly rejected (we
            // do not silently compute an extra aggregate). `resolved` mirrors
            // the engine's per-group result order exactly, so the matched
            // index is the engine's `agg_index`.
            let resolve_having =
                |raw: &RawHaving, resolved: &[(u8, u16)]| -> Result<kessel_proto::HavingPred, SqlError> {
                    let want_field: u16 = match &raw.field {
                        Some(c) => fid(c)?,
                        None => 0, // COUNT(*) — field id 0 by the resolve_agg convention
                    };
                    let idx = resolved.iter().position(|(k, f)| {
                        *k == raw.kind && *f == want_field
                    });
                    match idx {
                        Some(i) => Ok(kessel_proto::HavingPred {
                            agg_index: i as u16,
                            op: raw.op,
                            value: raw.value,
                        }),
                        None => Err(
                            "HAVING references an aggregate that is not in the \
                             SELECT list — V1 requires the HAVING aggregate to \
                             match one of the projected aggregates (e.g. \
                             `SELECT g, COUNT(*) … GROUP BY g HAVING COUNT(*) > 2`)"
                                .into(),
                        ),
                    }
                };
            // SP-PG-SQL-GROUP-SORT-LIMIT — resolve a captured ORDER BY target
            // into a wire `GroupSort` over a plain GROUP BY's per-group result.
            // `group_name` is the group-key column (so an `ORDER BY g` / by its
            // projection position resolves to the key). `resolved` is the
            // aggregate output sequence (so `ORDER BY n`/`ORDER BY COUNT(*)` /
            // by position resolves to an aggregate slot). The projection output
            // order is `[group key, agg0, agg1, …]` (1-based positions). Returns
            // `Ok(None)` when there is no ORDER BY, and an `Err` for an
            // unresolvable target (out-of-range position, unknown alias/column,
            // or an aggregate expr not in the SELECT list).
            let resolve_group_sort =
                |group_name: &str,
                 resolved: &[(u8, u16)],
                 agg_aliases: &[Option<String>]|
                 -> Result<Option<kessel_proto::GroupSort>, SqlError> {
                    let (target, desc) = match &order_target {
                        Some(t) => t,
                        None => return Ok(None),
                    };
                    let gst = match target {
                        RawOrderTarget::Position(n) => {
                            // 1-based: position 1 = group key, 2.. = aggregates.
                            if *n == 0 {
                                return Err("ORDER BY position must be ≥ 1".into());
                            }
                            if *n == 1 {
                                kessel_proto::GroupSortTarget::Key
                            } else {
                                let ai = *n - 2;
                                if ai >= resolved.len() {
                                    return Err(format!(
                                        "ORDER BY position {n} is out of range \
                                         (projection has {} columns)",
                                        resolved.len() + 1
                                    ));
                                }
                                kessel_proto::GroupSortTarget::Agg(ai as u16)
                            }
                        }
                        RawOrderTarget::Agg { kind, field } => {
                            let want_field: u16 = match field {
                                Some(c) => fid(c)?,
                                None => 0,
                            };
                            match resolved
                                .iter()
                                .position(|(k, f)| *k == *kind && *f == want_field)
                            {
                                Some(i) => kessel_proto::GroupSortTarget::Agg(i as u16),
                                None => {
                                    return Err(
                                        "ORDER BY references an aggregate that is \
                                         not in the SELECT list (V1 requires the \
                                         ORDER BY aggregate to match a projected \
                                         aggregate)"
                                            .into(),
                                    )
                                }
                            }
                        }
                        RawOrderTarget::Ident(name) => {
                            // Group-key column (by name) ⇒ sort by key. Else an
                            // aggregate output alias ⇒ sort by that aggregate.
                            if name == group_name {
                                kessel_proto::GroupSortTarget::Key
                            } else if let Some(i) = agg_aliases
                                .iter()
                                .position(|a| a.as_deref() == Some(name.as_str()))
                            {
                                kessel_proto::GroupSortTarget::Agg(i as u16)
                            } else {
                                return Err(format!(
                                    "ORDER BY `{name}` does not match the GROUP BY \
                                     column or any projected aggregate alias"
                                ));
                            }
                        }
                    };
                    Ok(Some(kessel_proto::GroupSort {
                        target: gst,
                        desc: *desc,
                        limit: if limit == 0 { None } else { Some(limit as u64) },
                        offset: if offset == 0 { None } else { Some(offset as u64) },
                    }))
                };
            // Single-aggregate back-compat path (byte-identical emit when no
            // HAVING; HAVING adds the marker-guarded trailing block only).
            if aggs.len() == 1 && leading_cols.is_empty() {
                let (k, af) = resolve_agg(&aggs[0])?;
                if let Some(g) = group {
                    let having = match &having_raw {
                        Some(raw) => Some(resolve_having(raw, &[(k, af)])?),
                        None => None,
                    };
                    let sort_spec =
                        resolve_group_sort(&g, &[(k, af)], &[aggs[0].alias.clone()])?;
                    return Ok(Op::GroupAggregate {
                        type_id: ot.type_id,
                        program,
                        group_field: fid(&g)?,
                        kind: k,
                        agg_field: af,
                        range_preds: agg_range_preds,
                        having,
                        sort: sort_spec,
                    });
                } else {
                    if having_raw.is_some() {
                        return Err(
                            "HAVING requires GROUP BY (a scalar aggregate has no \
                             groups to filter)"
                                .into(),
                        );
                    }
                    return Ok(Op::Aggregate {
                        type_id: ot.type_id,
                        program,
                        kind: k,
                        field_id: af,
                        range_preds: agg_range_preds,
                    });
                }
            }
            // Multi-aggregate / leading-col path.
            // Determine the single group field (V1: one column) + its NAME (so
            // SP-PG-SQL-GROUP-SORT-LIMIT can resolve `ORDER BY g` to the key).
            let (group_field, group_name): (u16, String) = match (group, leading_cols.as_slice()) {
                (Some(g), []) => (fid(&g)?, g),
                (None, [c]) => (fid(c)?, c.clone()),
                (Some(g), [c]) => {
                    if g != *c {
                        return Err(format!(
                            "GROUP BY column `{g}` must match leading projection `{c}`"
                        ));
                    }
                    (fid(&g)?, g)
                }
                (None, []) => {
                    // ≥2 aggs but no group field — there's no group key. V1
                    // requires a GROUP BY (single-column) for the Multi op;
                    // a "no GROUP BY" multi-aggregate (one row, N values)
                    // is a follow-on shape (out of scope for V1).
                    return Err(
                        "multi-aggregate SELECT requires GROUP BY (V1)".into(),
                    );
                }
                (_, _) => {
                    return Err(
                        "multi-column GROUP BY not supported in V1 (use one \
                         leading group column)"
                            .into(),
                    );
                }
            };
            let mut resolved: Vec<(u8, u16)> = Vec::with_capacity(aggs.len());
            for a in &aggs {
                resolved.push(resolve_agg(a)?);
            }
            let having = match &having_raw {
                Some(raw) => Some(resolve_having(raw, &resolved)?),
                None => None,
            };
            let agg_aliases: Vec<Option<String>> =
                aggs.iter().map(|a| a.alias.clone()).collect();
            let sort_spec = resolve_group_sort(&group_name, &resolved, &agg_aliases)?;
            Ok(Op::GroupAggregateMulti {
                type_id: ot.type_id,
                program,
                group_field,
                aggregates: resolved,
                range_preds: agg_range_preds,
                having,
                sort: sort_spec,
            })
        }
        _ if sort.is_some() => {
            let (c, desc) = sort.unwrap();
            Ok(Op::SelectSorted {
                type_id: ot.type_id,
                program,
                sort_field: fid(&c)?,
                desc,
                offset,
                limit,
            })
        }
        Proj::Cols(cols) => {
            let mut fs = Vec::new();
            for c in &cols {
                fs.push(fid(c)?);
            }
            Ok(Op::SelectFields {
                type_id: ot.type_id,
                program,
                fields: fs,
                limit,
            })
        }
        Proj::Star => Ok(Op::Select {
            type_id: ot.type_id,
            program,
            limit,
        }),
    }
}

// SP-PG-SQL-JOIN-WHERE: build the COMBINED join `ObjectType` exactly as the
// engine's `Op::Join` apply does — every left column as `<lt>.<col>` then every
// right column as `<rt>.<col>`, field ids reassigned `0..nL+nR`. The join-WHERE
// predicate compiles against THIS type so its field ids match the records the
// engine encodes for the combined `KTR1` result. The two recipes are identical
// by construction, so the predicate's field offsets line up with the engine's.
fn combined_join_type(lt: &ObjectType, rt: &ObjectType) -> ObjectType {
    let mut fields: Vec<Field> = Vec::with_capacity(lt.fields.len() + rt.fields.len());
    let mut fid: u16 = 0;
    for (src, f) in lt
        .fields
        .iter()
        .map(|f| (&lt.name, f))
        .chain(rt.fields.iter().map(|f| (&rt.name, f)))
    {
        fields.push(Field {
            field_id: fid,
            name: format!("{src}.{}", f.name),
            kind: f.kind,
            nullable: f.nullable,
        });
        fid += 1;
    }
    ObjectType::from_def(format!("{}+{}", lt.name, rt.name), fields)
}

/// SP-PG-SQL-MULTI-JOIN: combined schema over an ORDERED list of joined tables
/// (`a ++ b ++ c …`). Mirrors `combined_join_type` for N tables: each column is
/// renamed `<table>.<col>` and assigned a fresh sequential combined field id,
/// matching the engine's `apply_multi_join` schema build EXACTLY (same order,
/// same names, same ids). The combined type name is `a+b+c…`.
fn combined_join_type_multi(tables: &[&ObjectType]) -> ObjectType {
    let mut fields: Vec<Field> = Vec::new();
    let mut fid: u16 = 0;
    let mut name = String::new();
    for (i, t) in tables.iter().enumerate() {
        if i > 0 {
            name.push('+');
        }
        name.push_str(&t.name);
        for f in &t.fields {
            fields.push(Field {
                field_id: fid,
                name: format!("{}.{}", t.name, f.name),
                kind: f.kind,
                nullable: f.nullable,
            });
            fid += 1;
        }
    }
    ObjectType::from_def(name, fields)
}

// WHERE keywords that an `Ident` may legitimately be in the WHERE grammar —
// these are NEVER column references, so the join-WHERE rewriter leaves them
// untouched. (Values are `Int`/`Str`/`Bytes` tokens, not idents.)
fn is_where_keyword(s: &str) -> bool {
    matches!(
        s.to_ascii_uppercase().as_str(),
        "AND" | "OR" | "NOT" | "IN" | "BETWEEN" | "LIKE" | "IS" | "NULL"
            | "ANY" | "ARRAY" | "ASC" | "DESC"
    )
}

// SP-PG-SQL-JOIN-WHERE: compile the predicate after `JOIN … ON … WHERE` against
// the combined schema `cot` (field names `<table>.<col>`). The shared WHERE
// compiler (`compile_where` → `term`) resolves a column by its `f.name`, so we
// REWRITE the predicate's column tokens to the combined names first:
//   * qualified `a.x` (`Ident(a) '.' Ident(x)`) → single `Ident("a.x")` when
//     `a` names one of the two joined tables (else an error — wrong qualifier);
//   * bare `x` → the combined name `<table>.x` resolved by suffix; ambiguous
//     (present in BOTH tables) ⇒ a precise error, mirroring PG.
// The rewritten token region is then compiled by the EXISTING grammar against
// `cot`, so AND / OR / NOT / IN / BETWEEN / LIKE all work over the join for free.
fn compile_join_where(
    p: &mut P,
    cot: &ObjectType,
    ltbl: &str,
    rtbl: &str,
    refs: &[JoinTableRef],
) -> Result<Vec<u8>, SqlError> {
    // The binary-join case is the 2-table multi case.
    compile_join_where_multi(p, cot, &[ltbl, rtbl], refs)
}

/// SP-PG-SQL-MULTI-JOIN: join-WHERE compiler generalized to N joined tables.
/// `tables` is the ordered list of joined table names (for qualifier validation
/// + error messages); `cot` is the combined schema spanning them all. Qualified
/// `t.col` rewrites to the combined `<t>.<col>` (qualifier must name a joined
/// table); a bare `col` resolves by `.<col>` suffix with an ambiguity check
/// across ALL joined tables. Identical to the 2-table path when `tables.len()
/// == 2`, so the binary join is unchanged.
fn compile_join_where_multi(
    p: &mut P,
    cot: &ObjectType,
    tables: &[&str],
    refs: &[JoinTableRef],
) -> Result<Vec<u8>, SqlError> {
    // The WHERE region runs from p.i up to a top-level LIMIT (the only clause
    // that can follow a join-WHERE in V1) or end of input. Collect + rewrite.
    let mut rewritten: Vec<Tok> = Vec::new();
    while let Some(tok) = p.peek().cloned() {
        // Stop at a trailing clause (left for the caller to parse).
        // SP-PG-SQL-JOIN-QUERY: ORDER / OFFSET join the pre-existing LIMIT
        // as terminators so a `JOIN … WHERE … ORDER BY …` splits cleanly.
        if let Tok::Ident(k) = &tok {
            if k.eq_ignore_ascii_case("LIMIT")
                || k.eq_ignore_ascii_case("ORDER")
                || k.eq_ignore_ascii_case("OFFSET")
            {
                break;
            }
        }
        match tok {
            Tok::Ident(name) if !is_where_keyword(&name) => {
                p.i += 1;
                // Qualified `name . col`?
                let qualified = matches!(p.peek(), Some(Tok::Punct('.')));
                if qualified {
                    p.i += 1; // consume `.`
                    let col = match p.next() {
                        Some(Tok::Ident(c)) => c,
                        _ => return Err("expected column after `table.`".into()),
                    };
                    // SP-PG-SQL-JOIN-ALIAS — resolve an alias/table-name qualifier
                    // to the FULL table name before matching the combined schema.
                    let name = resolve_join_qualifier(refs, &name)?;
                    if !tables.iter().any(|t| *t == name) {
                        return Err(format!(
                            "join-WHERE qualifier `{name}` is not one of the \
                             joined tables {tables:?}"
                        ));
                    }
                    let combined = format!("{name}.{col}");
                    if !cot.fields.iter().any(|f| f.name == combined) {
                        return Err(unknown_column_err(&combined, cot));
                    }
                    rewritten.push(Tok::Ident(combined));
                } else {
                    // Bare column — resolve by suffix `.<name>` with an
                    // ambiguity check across ALL joined tables.
                    let suffix = format!(".{name}");
                    let mut hits = cot
                        .fields
                        .iter()
                        .filter(|f| f.name.ends_with(&suffix));
                    let first = hits.next();
                    match (first, hits.next()) {
                        (Some(f), None) => rewritten.push(Tok::Ident(f.name.clone())),
                        (Some(_), Some(_)) => {
                            return Err(format!(
                                "column `{name}` is ambiguous in join-WHERE \
                                 (present in multiple joined tables {tables:?}); \
                                 qualify it"
                            ))
                        }
                        (None, _) => return Err(unknown_column_err(&name, cot)),
                    }
                }
            }
            _ => {
                p.i += 1;
                rewritten.push(tok);
            }
        }
    }
    // Compile the rewritten predicate against the combined schema using the
    // existing grammar (a fresh sub-parser over just the rewritten tokens).
    let mut sub = P { t: rewritten, i: 0, cat: p.cat };
    let prog = compile_where(&mut sub, cot)?;
    if sub.i != sub.t.len() {
        return Err("trailing tokens after join-WHERE predicate".into());
    }
    Ok(prog)
}

// WHERE -> kessel-expr program. Grammar: or := and (OR and)* ;
// and := not (AND not)* ; not := [NOT] cmp ; cmp := term [OP term] ;
// term := col | int | str | '(' or ')'.
fn compile_where(p: &mut P, ot: &ObjectType) -> Result<Vec<u8>, SqlError> {
    let prog = or_expr(p, ot)?;
    Ok(prog.bytes())
}

fn or_expr(p: &mut P, ot: &ObjectType) -> Result<Program, SqlError> {
    let mut prog = and_expr(p, ot)?;
    while p.kw("OR") {
        let rhs = and_expr(p, ot)?;
        prog = splice(prog, rhs, "OR");
    }
    Ok(prog)
}

fn and_expr(p: &mut P, ot: &ObjectType) -> Result<Program, SqlError> {
    let mut prog = cmp_expr(p, ot)?;
    while p.kw("AND") {
        let rhs = cmp_expr(p, ot)?;
        prog = splice(prog, rhs, "AND");
    }
    Ok(prog)
}

// Build a fresh Program = encode(a) ++ encode(b) ++ op by re-emitting bytes.
fn splice(a: Program, b: Program, op: &str) -> Program {
    let mut raw = a.bytes();
    raw.extend_from_slice(&b.bytes());
    // op bytecodes mirror kessel_expr: AND=14, OR=15, NOT=16
    raw.push(match op {
        "AND" => 14,
        "OR" => 15,
        _ => 16,
    });
    Program::from_raw(raw)
}

fn cmp_expr(p: &mut P, ot: &ObjectType) -> Result<Program, SqlError> {
    let negate = p.kw("NOT");
    let lb = term(p, ot)?.bytes(); // lhs program bytes (reused per disjunct)
    // `col IS NULL` / `col IS NOT NULL` — uses the expr-VM IS_NULL opcode
    // (2) on the column's field id; `IS NOT NULL` wraps it in NOT (16).
    if p.kw("IS") {
        let is_not = p.kw("NOT");
        if !p.kw("NULL") {
            return Err("expected NULL after IS [NOT]".into());
        }
        // lb must be a bare column load: [LOAD_FIELD=1][field_id:2 LE].
        if lb.len() != 3 || lb[0] != 1 {
            return Err("IS NULL requires a column".into());
        }
        let mut raw = vec![2u8]; // IS_NULL
        raw.extend_from_slice(&lb[1..3]); // field_id
        if is_not {
            raw.push(16); // NOT  -> IS NOT NULL
        }
        if negate {
            raw.push(16); // outer prefix NOT
        }
        return Ok(Program::from_raw(raw));
    }
    // Optional post-column NOT for `col NOT IN (..)` / `col NOT BETWEEN ..`.
    let post_not = p.kw("NOT");
    let prog = if p.kw("IN") {
        // `col IN (v1, v2, ...)` ≡ `(col=v1) OR (col=v2) OR ...`.
        p.punct('(')?;
        let mut acc: Option<Program> = None;
        loop {
            let v = term(p, ot)?;
            let mut raw = lb.clone();
            raw.extend_from_slice(&v.bytes());
            raw.push(3); // EQ
            let eqp = Program::from_raw(raw);
            acc = Some(match acc {
                None => eqp,
                Some(a) => splice(a, eqp, "OR"),
            });
            match p.peek() {
                Some(Tok::Punct(',')) => p.i += 1,
                _ => break,
            }
        }
        p.punct(')')?;
        let mut prog = acc.ok_or_else(|| "IN () needs ≥1 value".to_string())?;
        if post_not {
            let mut r = prog.bytes();
            r.push(16); // NOT  -> NOT IN
            prog = Program::from_raw(r);
        }
        prog
    } else if p.kw("BETWEEN") {
        // `col BETWEEN lo AND hi` ≡ `(col>=lo) AND (col<=hi)`.
        let lo = term(p, ot)?;
        if !p.kw("AND") {
            return Err("BETWEEN needs `lo AND hi`".into());
        }
        let hi = term(p, ot)?;
        let mut ge = lb.clone();
        ge.extend_from_slice(&lo.bytes());
        ge.push(8); // >=
        let mut le = lb.clone();
        le.extend_from_slice(&hi.bytes());
        le.push(6); // <=
        let mut prog = splice(Program::from_raw(ge), Program::from_raw(le), "AND");
        if post_not {
            let mut r = prog.bytes();
            r.push(16); // NOT  -> NOT BETWEEN
            prog = Program::from_raw(r);
        }
        prog
    } else if p.kw("LIKE") {
        // `col LIKE 'pat'` / `col NOT LIKE 'pat'` — SQL wildcard match
        // (`%` any run, `_` one char) via the expr-VM LIKE opcode (20).
        let pat = term(p, ot)?;
        let mut raw = lb.clone();
        raw.extend_from_slice(&pat.bytes());
        raw.push(20); // LIKE
        let mut prog = Program::from_raw(raw);
        if post_not {
            let mut r = prog.bytes();
            r.push(16); // NOT  -> NOT LIKE
            prog = Program::from_raw(r);
        }
        prog
    } else if post_not {
        return Err("expected IN, BETWEEN or LIKE after NOT".into());
    } else if matches!(p.peek(), Some(Tok::Cmp("=")))
        && matches!(p.t.get(p.i + 1), Some(Tok::Ident(k)) if k.eq_ignore_ascii_case("ANY"))
    {
        // SP-PG-SQL-ORM-PARSE T4 — `col = ANY (ARRAY[v1, v2, ...])`
        // desugars to `col IN (v1, v2, ...)` ≡ OR-of-eq, reusing the
        // SP56 IN lowering. SQLAlchemy emits this for IN-list filters
        // AND for the `create_all` relkind existence probe
        // (`relkind = ANY (ARRAY['r','p','f','v','m'])`). Only the
        // ARRAY-literal form is desugared; `= ANY (SELECT ...)`
        // (subquery) is the named follow-up `SP-PG-SQL-ANY-SUBQUERY`.
        p.i += 1; // consume `=`
        p.i += 1; // consume `ANY`
        p.punct('(')?;
        // `ARRAY` keyword then `[`.
        if !p.kw("ARRAY") {
            return Err(
                "`= ANY (...)` expects an `ARRAY[...]` literal (subquery \
                 ANY is SP-PG-SQL-ANY-SUBQUERY)"
                    .into(),
            );
        }
        match p.next() {
            Some(Tok::Punct('[')) => {}
            _ => return Err("`ANY (ARRAY` must be followed by `[`".into()),
        }
        // Empty `ARRAY[]` → `col = ANY (empty)` is always FALSE. Emit a
        // constant-false program (push 0) so the row never matches —
        // mirrors PG semantics and keeps the OR-of-eq accumulator total.
        let mut acc: Option<Program> = None;
        if !matches!(p.peek(), Some(Tok::Punct(']'))) {
            loop {
                let v = term(p, ot)?;
                let mut raw = lb.clone();
                raw.extend_from_slice(&v.bytes());
                raw.push(3); // EQ
                let eqp = Program::from_raw(raw);
                acc = Some(match acc {
                    None => eqp,
                    Some(a) => splice(a, eqp, "OR"),
                });
                match p.peek() {
                    Some(Tok::Punct(',')) => p.i += 1,
                    _ => break,
                }
            }
        }
        match p.next() {
            Some(Tok::Punct(']')) => {}
            _ => return Err("unterminated `ARRAY[...]` (expected `]`)".into()),
        }
        p.punct(')')?;
        // Empty array → constant FALSE (push int 0). The expr-VM treats
        // a non-zero top-of-stack as true, so 0 is a guaranteed no-match.
        acc.unwrap_or_else(|| Program::new().push_int(0))
    } else if let Some(Tok::Cmp(c)) = p.peek().cloned() {
        p.i += 1;
        // SP-PG-SQL-PAREN-VALUES: if LHS is a bare load of a numeric
        // column (LOAD_FIELD=1 followed by field_id_lo/hi), hint the
        // RHS term parser to coerce a `'NN'`-shaped string literal
        // to the matching numeric kind. pgJDBC simple-mode emits
        // `WHERE id = ('42'::int8)`; after the SP-PG-EXTQ-CAST T2
        // strip the kessel-sql lexer sees `WHERE id = ('42')`, which
        // without the hint would push the bytes `b"42"` and the EQ
        // opcode would compare Int×Bytes (always false).
        let hint = lhs_numeric_kind_hint(&lb, ot);
        let rhs = term_hinted(p, ot, hint)?;
        let mut raw = lb.clone();
        raw.extend_from_slice(&rhs.bytes());
        raw.push(match c {
            "=" => 3,
            // SP-PG-SQL-HAVING — `<>` is the SQL-standard spelling of `!=`;
            // both map to the same inequality opcode now that `<>` lexes.
            "!=" | "<>" => 4,
            "<" => 5,
            "<=" => 6,
            ">" => 7,
            ">=" => 8,
            _ => return Err("bad comparator".into()),
        });
        Program::from_raw(raw)
    } else {
        Program::from_raw(lb)
    };
    if negate {
        let mut raw = prog.bytes();
        raw.push(16); // NOT
        Ok(Program::from_raw(raw))
    } else {
        Ok(prog)
    }
}

fn term(p: &mut P, ot: &ObjectType) -> Result<Program, SqlError> {
    term_hinted(p, ot, None)
}

/// SP-PG-SQL-PAREN-VALUES: WHERE-term parser with an optional numeric
/// column-kind hint. When the surrounding `cmp_expr` knows its LHS is
/// a bare load of a numeric column AND the RHS is a string literal
/// whose contents parse as a decimal integer, the literal is pushed
/// as an int — matching PG's `'42'::int8` coercion that the
/// SP-PG-EXTQ-CAST stripper drops at the wire. Without the hint
/// (today's behaviour for non-comparison contexts: IN-tuple values,
/// BETWEEN bounds, LIKE patterns, nested parens) the literal is
/// pushed as bytes, preserving every prior WHERE KAT byte-for-byte.
fn term_hinted(
    p: &mut P,
    ot: &ObjectType,
    hint: Option<FieldKind>,
) -> Result<Program, SqlError> {
    match p.next() {
        Some(Tok::Punct('(')) => {
            // Inside a parenthesised expression the hint still applies —
            // pgJDBC simple-mode emits `WHERE id = ('42'::int8)`, which
            // after the SP-PG-EXTQ-CAST strip becomes `WHERE id = ('42')`.
            // The `(` consumes here; the recursive `or_expr` walks into
            // a fresh `cmp_expr`/`term` chain that doesn't know about
            // the outer LHS column, so we re-enter `term_hinted` after
            // `or_expr` would have returned. The simplest faithful
            // implementation: if the parenthesised inner is a single
            // bare literal (no operators inside), apply the hint at
            // this level by peeking ahead.
            //
            // Detect that single-literal shape: `(LITERAL)` only —
            // anything else (operators, nested `(`, identifiers, etc)
            // falls back to the generic or_expr path.
            let save = p.i;
            let single_lit = match (p.peek().cloned(), p.t.get(save + 1).cloned()) {
                (Some(Tok::Int(_)), Some(Tok::Punct(')'))) => true,
                (Some(Tok::Str(_)), Some(Tok::Punct(')'))) => true,
                // SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 — `Tok::Bytes` is
                // a bare-literal value the same way `Tok::Str` is.
                (Some(Tok::Bytes(_)), Some(Tok::Punct(')'))) => true,
                _ => false,
            };
            if single_lit {
                let inner = term_hinted(p, ot, hint)?;
                p.punct(')')?;
                return Ok(inner);
            }
            let inner = or_expr(p, ot)?;
            p.punct(')')?;
            Ok(inner)
        }
        Some(Tok::Int(n)) => Ok(Program::new().push_int(n)),
        Some(Tok::Str(s)) => {
            // Coerce to int IF the column kind is numeric and the
            // string is a clean decimal integer. Mirrors the
            // SP-PG-EXTQ-CAST-stripped `'NN'::int8` shape.
            use FieldKind::*;
            let numeric = matches!(
                hint,
                Some(I8 | I16 | I32 | I64 | I128
                    | U8 | U16 | U32 | U64 | U128
                    | Bool | Timestamp | Fixed { .. })
            );
            if numeric {
                if let Ok(n) = s.parse::<i128>() {
                    return Ok(Program::new().push_int(n));
                }
            }
            Ok(Program::new().push_bytes(s.as_bytes()))
        }
        // SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 — raw-bytes literal from
        // a `Value::Blob` parameter binding. Numeric coercion still
        // applies IF the bytes happen to parse as UTF-8 + decimal
        // (matches the `Tok::Str` numeric-coercion shape so a
        // psycopg2 `cursor.execute("...WHERE x = %s", (b"42",))`
        // bound to a numeric column still works); else push the
        // bytes as a verbatim bytes literal.
        Some(Tok::Bytes(b)) => {
            use FieldKind::*;
            let numeric = matches!(
                hint,
                Some(I8 | I16 | I32 | I64 | I128
                    | U8 | U16 | U32 | U64 | U128
                    | Bool | Timestamp | Fixed { .. })
            );
            if numeric {
                if let Ok(s) = std::str::from_utf8(&b) {
                    if let Ok(n) = s.parse::<i128>() {
                        return Ok(Program::new().push_int(n));
                    }
                }
            }
            Ok(Program::new().push_bytes(&b))
        }
        Some(Tok::Ident(name)) => {
            // SP-PG-SQL-ORM-PARSE T2 — qualified column reference in a
            // WHERE term: `table.col` / `t.col`. The lexer tokenizes
            // `.` as `Punct('.')`; if it follows the ident, consume it
            // plus the bare column ident and IGNORE the qualifier
            // (lenient V1). Bare `col` (no trailing `.`) is unchanged.
            let name = if matches!(p.peek(), Some(Tok::Punct('.'))) {
                p.i += 1; // consume `.`
                match p.next() {
                    Some(Tok::Ident(col)) => col,
                    _ => return Err("expected column after `table.`".into()),
                }
            } else {
                name
            };
            let f = ot
                .fields
                .iter()
                .find(|f| f.name == name)
                .ok_or_else(|| unknown_column_err(&name, ot))?;
            Ok(Program::new().load(f.field_id))
        }
        _ => Err("bad WHERE term".into()),
    }
}

/// SP-PG-SQL-PAREN-VALUES helper: if `lb` is a bare `LOAD_FIELD` (the
/// 3-byte opcode shape emitted by `Program::load(field_id)`), look up
/// that column's `FieldKind` and return it; otherwise return None.
/// Used by `cmp_expr` to hint the RHS `term_hinted` parser to coerce
/// a string literal to the matching numeric kind.
fn lhs_numeric_kind_hint(lb: &[u8], ot: &ObjectType) -> Option<FieldKind> {
    // LOAD_FIELD opcode = 1; field_id is little-endian u16 at lb[1..3].
    if lb.len() != 3 || lb[0] != 1 {
        return None;
    }
    let fid = u16::from_le_bytes([lb[1], lb[2]]);
    ot.fields
        .iter()
        .find(|f| f.field_id == fid)
        .map(|f| f.kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_io::MemVfs;
    use kessel_proto::OpResult;
    use kessel_sm::StateMachine;

    /// SP-PG-CAT-T8 — canonical PG type names map to the right
    /// KesselDB FieldKind. A real psql `CREATE TABLE foo (x BIGINT)`
    /// session must compile; the V1 surface previously rejected
    /// `BIGINT` / `INTEGER` / `SMALLINT` / `BOOLEAN` with
    /// `sql: unknown type ...`.
    #[test]
    fn pg_type_aliases_map_to_kessel_fieldkinds() {
        assert!(matches!(kind_of("BIGINT", None), Ok(FieldKind::I64)));
        assert!(matches!(kind_of("bigint", None), Ok(FieldKind::I64)));
        assert!(matches!(kind_of("INTEGER", None), Ok(FieldKind::I32)));
        assert!(matches!(kind_of("integer", None), Ok(FieldKind::I32)));
        assert!(matches!(kind_of("INT", None), Ok(FieldKind::I32)));
        assert!(matches!(kind_of("SMALLINT", None), Ok(FieldKind::I16)));
        assert!(matches!(kind_of("BOOLEAN", None), Ok(FieldKind::Bool)));
        assert!(matches!(kind_of("boolean", None), Ok(FieldKind::Bool)));
        // Existing KesselDB-native names still work — pure addition.
        assert!(matches!(kind_of("I64", None), Ok(FieldKind::I64)));
        assert!(matches!(kind_of("I32", None), Ok(FieldKind::I32)));
        assert!(matches!(kind_of("BOOL", None), Ok(FieldKind::Bool)));
    }

    /// SP-PG-ORM-SQLALCHEMY — `VARCHAR(n)` DDL alias. SQLAlchemy's
    /// `Column(String(32))` renders `VARCHAR(32)` in the `create_all`
    /// DDL; pre-fix this hit `unknown type \`VARCHAR\``. Now aliased to
    /// the fixed-width `Char(n)` FieldKind (same on-wire layout). Bare
    /// `VARCHAR` without `(n)` is a clear error (named follow-up
    /// `SP-PG-DDL-VARCHAR-UNBOUNDED`).
    #[test]
    fn pg_varchar_alias_maps_to_char() {
        assert!(matches!(
            kind_of("VARCHAR", Some(32)),
            Ok(FieldKind::Char(32))
        ));
        assert!(matches!(
            kind_of("varchar", Some(255)),
            Ok(FieldKind::Char(255))
        ));
        // `(n)` is required — bare VARCHAR is rejected with a precise reason.
        assert!(kind_of("VARCHAR", None)
            .unwrap_err()
            .to_string()
            .contains("VARCHAR needs (n)"));
        // The native CHAR(n) spelling is untouched (pure addition).
        assert!(matches!(kind_of("CHAR", Some(8)), Ok(FieldKind::Char(8))));
    }

    #[test]
    fn parse_create_external_source() {
        let cat = Catalog::default();
        let sql = "CREATE EXTERNAL SOURCE feed (\
            id U64 NOT NULL FROM 'id', \
            nm CHAR(8) NOT NULL FROM 'u.name') \
            FROM 'http://h/p' FORMAT JSON KEY id \
            AUTH BEARER ENV 'TOK_ENV'";
        match compile(sql, &cat).expect("compile") {
            Op::CreateExternalSource {
                name,
                url,
                format,
                key_field_id,
                auth_kind,
                auth_a,
                mapping,
                ..
            } => {
                assert_eq!(name, "feed");
                assert_eq!(url, "http://h/p");
                assert_eq!(format, 0);
                assert_eq!(auth_kind, 1);
                assert_eq!(auth_a, "TOK_ENV");
                assert_eq!(key_field_id, 1);
                assert_eq!(
                    mapping,
                    vec![(1, "id".to_string()), (2, "u.name".to_string())]
                );
            }
            o => panic!("got {o:?}"),
        }
    }

    #[test]
    fn parse_external_source_pagination_forms() {
        let cat = Catalog::default();
        match compile("CREATE EXTERNAL SOURCE f (id U64 NOT NULL FROM 'id') \
            FROM 'http://h' FORMAT JSON KEY id \
            ROWS 'data.items' PAGE NEXT JSON 'p.next'", &cat).unwrap() {
            Op::CreateExternalSource{ rows_path, pagination, format, .. } => {
                assert_eq!(format, 0);
                assert_eq!(rows_path.as_deref(), Some("data.items"));
                assert_eq!(pagination, Some((1,"p.next".to_string(),String::new())));
            } o=>panic!("{o:?}"),
        }
        match compile("CREATE EXTERNAL SOURCE g (id U64 NOT NULL FROM 'id') \
            FROM 'http://h' FORMAT NDJSON KEY id PAGE NEXT LINK", &cat).unwrap() {
            Op::CreateExternalSource{ format, rows_path, pagination, .. } => {
                assert_eq!(format, 2); assert_eq!(rows_path, None);
                assert_eq!(pagination, Some((2,String::new(),String::new())));
            } o=>panic!("{o:?}"),
        }
        match compile("CREATE EXTERNAL SOURCE h (id U64 NOT NULL FROM 'id') \
            FROM 'http://h' FORMAT JSON KEY id ROWS 'items' \
            PAGE CURSOR JSON 'm.c' PARAM 'cursor'", &cat).unwrap() {
            Op::CreateExternalSource{ pagination, .. } =>
                assert_eq!(pagination, Some((3,"m.c".to_string(),"cursor".to_string()))),
            o=>panic!("{o:?}"),
        }
        // no pagination, NDJSON, no ROWS => still valid (None/None, format 2)
        match compile("CREATE EXTERNAL SOURCE n (id U64 NOT NULL FROM 'id') \
            FROM 'http://h' FORMAT NDJSON KEY id", &cat).unwrap() {
            Op::CreateExternalSource{ format, rows_path, pagination, .. } => {
                assert_eq!(format,2); assert_eq!(rows_path,None); assert_eq!(pagination,None);
            } o=>panic!("{o:?}"),
        }
    }

    #[test]
    fn external_source_compat_matrix_rejected() {
        let cat = Catalog::default();
        // JSON + body cursor WITHOUT ROWS => error
        assert!(compile("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
            FROM 'http://h' FORMAT JSON KEY id PAGE NEXT JSON 'p.next'",&cat).is_err());
        // NDJSON + body cursor => error
        assert!(compile("CREATE EXTERNAL SOURCE b (id U64 NOT NULL FROM 'id') \
            FROM 'http://h' FORMAT NDJSON KEY id PAGE NEXT JSON 'p.next'",&cat).is_err());
        // CSV + body cursor => error
        assert!(compile("CREATE EXTERNAL SOURCE c (id U64 NOT NULL FROM 'id') \
            FROM 'http://h' FORMAT CSV KEY id PAGE CURSOR JSON 'm' PARAM 'p'",&cat).is_err());
        // CSV + LINK => OK
        assert!(compile("CREATE EXTERNAL SOURCE d (id U64 NOT NULL FROM 'id') \
            FROM 'http://h' FORMAT CSV KEY id PAGE NEXT LINK",&cat).is_ok());
        // JSON + body cursor WITH ROWS => OK
        assert!(compile("CREATE EXTERNAL SOURCE e (id U64 NOT NULL FROM 'id') \
            FROM 'http://h' FORMAT JSON KEY id ROWS 'd' PAGE NEXT JSON 'p.next'",&cat).is_ok());
    }

    #[test]
    fn parse_external_source_objstore_s3() {
        let cat = Catalog::default();
        let op = compile(
            "CREATE EXTERNAL SOURCE feed (id U64 NOT NULL FROM 'id') \
             FROM 's3://bucket/data/x.json' FORMAT JSON KEY id \
             REGION 'us-east-1' \
             AUTH OBJSTORE S3 KEYID ENV 'AWS_ID' SECRET ENV 'AWS_SEC'",
            &cat,
        ).unwrap();
        match op {
            Op::CreateExternalSource { url, auth_kind, auth_a, auth_b, objstore, .. } => {
                assert_eq!(url, "s3://bucket/data/x.json");
                assert_eq!(auth_kind, 3);
                assert_eq!(auth_a, "AWS_ID");
                assert_eq!(auth_b, "AWS_SEC");
                assert_eq!(objstore, Some((1, String::new(), "us-east-1".into(), String::new())));
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn parse_external_source_objstore_azure_account_only() {
        let cat = Catalog::default();
        let op = compile(
            "CREATE EXTERNAL SOURCE f (id U64 NOT NULL FROM 'id') \
             FROM 'az://cont/blob.csv' FORMAT CSV KEY id \
             AUTH OBJSTORE AZURE ACCOUNT 'acct' KEY ENV 'AZ_KEY'",
            &cat,
        ).unwrap();
        match op {
            Op::CreateExternalSource { url, auth_kind, auth_a, objstore, .. } => {
                assert_eq!(url, "az://cont/blob.csv");
                assert_eq!(auth_kind, 3);
                assert_eq!(auth_a, "AZ_KEY");
                assert_eq!(objstore, Some((2, "acct".into(), String::new(), String::new())));
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn parse_external_source_objstore_azure_endpoint_only() {
        // The OTHER valid az:// form: ENDPOINT, no ACCOUNT.
        let cat = Catalog::default();
        let op = compile(
            "CREATE EXTERNAL SOURCE g (id U64 NOT NULL FROM 'id') \
             FROM 'az://cont/blob.csv' FORMAT CSV KEY id \
             ENDPOINT 'https://acct.blob.core.windows.net' \
             AUTH OBJSTORE AZURE KEY ENV 'AZ_KEY'",
            &cat,
        ).unwrap();
        match op {
            Op::CreateExternalSource { auth_kind, objstore, .. } => {
                assert_eq!(auth_kind, 3);
                assert_eq!(objstore, Some((2, String::new(), String::new(), "https://acct.blob.core.windows.net".into())));
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn objstore_rejections_at_create() {
        let cat = Catalog::default();
        let bad = |sql: &str| compile(sql, &cat).unwrap_err();
        // OBJ-2a: FORMAT PARQUET over s3:// is now ACCEPTED (flipped from OBJ-1 rejection).
        assert!(compile("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') FROM 's3://b/k' FORMAT PARQUET KEY id REGION 'r' AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S'", &cat).is_ok());
        assert!(bad("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') FROM 's3://b/k' FORMAT JSON KEY id REGION 'r' AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S' PAGE NEXT LINK").to_lowercase().contains("object store"));
        assert!(bad("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') FROM 's3://b/k' FORMAT JSON KEY id REGION 'r' ENDPOINT 'http://x' AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S'").to_lowercase().contains("https"));
        assert!(bad("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') FROM 's3://b/k' FORMAT JSON KEY id").to_lowercase().contains("auth objstore"));
        assert!(bad("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') FROM 's3://b/k' FORMAT JSON KEY id AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S'").to_lowercase().contains("region"));
        assert!(bad("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') FROM 'az://c/b' FORMAT JSON KEY id ENDPOINT 'https://h' AUTH OBJSTORE AZURE ACCOUNT 'acct' KEY ENV 'K'").to_lowercase().contains("exactly one"));
        assert!(compile("CREATE EXTERNAL SOURCE ok (id U64 NOT NULL FROM 'id') FROM 'http://h/p' FORMAT JSON KEY id AUTH BEARER ENV 'T'", &cat).is_ok());
    }

    #[test]
    fn parquet_accepted_for_object_store() {
        let cat = Catalog::default();
        let op = compile(
            "CREATE EXTERNAL SOURCE p (id U64 NOT NULL FROM 'id') \
             FROM 's3://b/k.parquet' FORMAT PARQUET KEY id \
             REGION 'us-east-1' \
             AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S'",
            &cat,
        ).unwrap();
        match op {
            Op::CreateExternalSource { format, url, .. } => {
                assert_eq!(format, 3);
                assert_eq!(url, "s3://b/k.parquet");
            }
            o => panic!("{o:?}"),
        }
        // az:// too
        assert!(compile(
            "CREATE EXTERNAL SOURCE q (id U64 NOT NULL FROM 'id') \
             FROM 'az://c/b.parquet' FORMAT PARQUET KEY id \
             AUTH OBJSTORE AZURE ACCOUNT 'a' KEY ENV 'K'", &cat).is_ok());
    }

    #[test]
    fn parquet_rejected_off_object_store_or_with_page_rows() {
        let cat = Catalog::default();
        let bad = |s: &str| compile(s, &cat).unwrap_err();
        assert!(bad("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
            FROM 'http://h/x.parquet' FORMAT PARQUET KEY id")
            .to_lowercase().contains("object-store"));
        assert!(bad("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
            FROM 'https://h/x.parquet' FORMAT PARQUET KEY id")
            .to_lowercase().contains("object-store"));
        assert!(bad("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
            FROM 's3://b/k' FORMAT PARQUET KEY id REGION 'r' \
            AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S' PAGE NEXT LINK")
            .to_lowercase().contains("page"));
        assert!(bad("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
            FROM 's3://b/k' FORMAT PARQUET KEY id REGION 'r' \
            AUTH OBJSTORE S3 KEYID ENV 'I' SECRET ENV 'S' ROWS 'd'")
            .to_lowercase().contains("rows"));
    }

    #[test]
    fn parse_refresh_and_drop_external_source() {
        let cat = Catalog::default();
        assert!(matches!(
            compile("REFRESH feed", &cat).unwrap(),
            Op::RefreshExternalSource { name } if name == "feed"
        ));
        assert!(matches!(
            compile("DROP EXTERNAL SOURCE feed", &cat).unwrap(),
            Op::DropExternalSource { name } if name == "feed"
        ));
        match compile(
            "CREATE EXTERNAL SOURCE c (a U32 NOT NULL FROM 'a') FROM 'http://h' FORMAT CSV KEY a",
            &cat,
        )
        .unwrap()
        {
            Op::CreateExternalSource {
                format, auth_kind, ..
            } => {
                assert_eq!(format, 1);
                assert_eq!(auth_kind, 0);
            }
            o => panic!("got {o:?}"),
        }
        // HEADER auth variant
        match compile(
            "CREATE EXTERNAL SOURCE h (a U32 NOT NULL FROM 'a') FROM 'http://h' FORMAT JSON KEY a AUTH HEADER 'X-Key' ENV 'KENV'",
            &cat,
        )
        .unwrap()
        {
            Op::CreateExternalSource {
                auth_kind,
                auth_a,
                auth_b,
                ..
            } => {
                assert_eq!(auth_kind, 2);
                assert_eq!(auth_a, "X-Key");
                assert_eq!(auth_b, "KENV");
            }
            o => panic!("got {o:?}"),
        }
        // FIX 1: KEY naming a non-declared column names it in the error.
        let e = compile("CREATE EXTERNAL SOURCE k (a U32 NOT NULL FROM 'a') FROM 'http://h' FORMAT JSON KEY zzz", &cat).unwrap_err();
        assert!(e.contains("zzz"), "KEY error must name the column, got: {e}");
        // FIX 2: empty column list is a clear error.
        let e2 = compile("CREATE EXTERNAL SOURCE e () FROM 'http://h' FORMAT JSON KEY a", &cat).unwrap_err();
        assert!(e2.to_lowercase().contains("at least one column"), "empty col list error, got: {e2}");
    }

    #[test]
    fn select_star_table_only_matches_whole_row_single_table() {
        assert_eq!(select_star_table("SELECT * FROM acct"), Some("acct".into()));
        assert_eq!(
            select_star_table("select * from acct where owner = 1"),
            Some("acct".into())
        );
        assert_eq!(
            select_star_table("SELECT * FROM acct ID 7"),
            Some("acct".into())
        );
        assert_eq!(
            select_star_table("SELECT * FROM t ORDER BY v LIMIT 5"),
            Some("t".into())
        );
        // Not whole-row / not single-table / not select:
        assert_eq!(select_star_table("SELECT owner, bal FROM acct"), None);
        assert_eq!(select_star_table("SELECT COUNT(*) FROM acct"), None);
        assert_eq!(select_star_table("SELECT * FROM a JOIN b ON a.x = b.y"), None);
        assert_eq!(select_star_table("DESCRIBE acct"), None);
        assert_eq!(select_star_table("INSERT INTO t ID 1 (v) VALUES (1)"), None);
        assert_eq!(select_star_table("garbage ;;"), None);
    }

    fn run(sm: &mut StateMachine<MemVfs>, op: u64, sql: &str) -> OpResult {
        let o = compile(sql, sm.catalog()).expect("compile");
        sm.apply(op, o)
    }

    /// SP90: a range-indexed CHAR column makes `SELECT * … WHERE s …`
    /// index-narrowed (the SP87 0xFFFC ordered index is wired into the
    /// SP70 planner), and — the real SP62/63/70 superset-verify
    /// invariant — the index-narrowed answer is *byte-identical* to the
    /// same WHERE run as a pure Seq Scan over the same rows. We prove
    /// this against an unindexed twin table, so the oracle makes no
    /// assumption about CHAR comparison semantics (fixed-width padded
    /// LHS vs raw literal): whatever the engine's WHERE means, the index
    /// path must mean exactly the same thing. EXPLAIN confirms the
    /// accelerator is engaged.
    #[test]
    fn string_range_planner_narrows_and_equals_scan() {
        use kessel_proto::Rng;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        // `t` has the range index; `u` is the identical unindexed twin
        // that defines ground truth via a full Seq Scan.
        run(&mut sm, 1, "CREATE TABLE t (s CHAR(8) NOT NULL, n U32 NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE u (s CHAR(8) NOT NULL, n U32 NOT NULL)");
        run(&mut sm, 3, "CREATE RANGE INDEX ON t (s)");
        let ot = sm.catalog().get(1).unwrap().clone();
        let mut rng = Rng::new(0x57_9A);
        // Monotonic op-numbers (real VSR never decreases them; the
        // SP94 recovery guard short-circuits a mutating op whose
        // op-number is ≤ the durable cursor).
        let mut iop = 3u64;
        for id in 1..=140u32 {
            let len = rng.below(5) as usize;
            let mut s = String::new();
            for _ in 0..len {
                s.push((b'a' + rng.below(6) as u8) as char);
            }
            iop += 1;
            run(
                &mut sm,
                iop,
                &format!("INSERT INTO t (id, s, n) VALUES ({id}, '{s}', {id})"),
            );
            iop += 1;
            run(
                &mut sm,
                iop,
                &format!("INSERT INTO u (id, s, n) VALUES ({id}, '{s}', {id})"),
            );
        }
        // The planner must emit a string range predicate on `s` (not a
        // bare Seq Scan): proves the SP87 0xFFFC ordered index is wired
        // into SP70 narrowing for CHAR columns.
        let rsel = "SELECT * FROM t WHERE s >= 'b' AND s <= 'd'";
        match compile(rsel, sm.catalog()).expect("compile range select") {
            Op::QueryRows { range_preds, eq_preds, .. } => {
                assert!(eq_preds.is_empty(), "no eq preds expected");
                let sfid = ot
                    .fields
                    .iter()
                    .find(|f| f.name == "s")
                    .unwrap()
                    .field_id;
                assert!(
                    range_preds.iter().any(|(f, _, _)| *f == sfid),
                    "string RANGE INDEX must surface a range pred on `s`, \
                     got {range_preds:?}"
                );
            }
            o => panic!("expected QueryRows, got {o:?}"),
        }
        // EXPLAIN (planner-only, via compile_stmt) confirms the human plan
        // names the range accelerator.
        match compile_stmt(&format!("EXPLAIN {rsel}"), sm.catalog())
            .expect("compile EXPLAIN")
        {
            Stmt::Explain(plan) => assert!(
                plan.contains("range") || plan.contains("Range"),
                "EXPLAIN should show range narrowing for a string \
                 RANGE INDEX, got: {plan}"
            ),
            _ => panic!("EXPLAIN did not compile to Stmt::Explain"),
        }
        let decode_n = |res: OpResult| -> Vec<u32> {
            let b = match res {
                OpResult::Got(b) => b,
                o => panic!("unexpected {o:?}"),
            };
            let mut out = Vec::new();
            let mut p = 0;
            while p + 4 <= b.len() {
                let l =
                    u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                p += 4;
                let v = kessel_codec::decode(&ot, &b[p..p + l]).unwrap();
                p += l;
                if let kessel_codec::Value::Uint(u) = v[1] {
                    out.push(u as u32);
                }
            }
            out.sort_unstable();
            out
        };
        // Sanity: the twin `u` really is unindexed (a Seq Scan), so the
        // equality below is index-vs-fullscan, not index-vs-index.
        match compile(
            "SELECT * FROM u WHERE s >= 'b' AND s <= 'd'",
            sm.catalog(),
        )
        .expect("compile twin select")
        {
            Op::QueryRows { range_preds, eq_preds, .. } => assert!(
                range_preds.is_empty() && eq_preds.is_empty(),
                "twin `u` must be a pure Seq Scan, got {range_preds:?}"
            ),
            o => panic!("expected QueryRows, got {o:?}"),
        }
        let mut op = 1000u64;
        let mut both = |sm: &mut StateMachine<MemVfs>, op: &mut u64, w: &str| {
            *op += 1;
            let a = decode_n(run(sm, *op, &format!("SELECT * FROM t WHERE {w}")));
            *op += 1;
            let b = decode_n(run(sm, *op, &format!("SELECT * FROM u WHERE {w}")));
            (a, b)
        };
        for _ in 0..30 {
            let mk = |rng: &mut Rng| {
                let len = rng.below(4) as usize;
                let mut s = String::new();
                for _ in 0..len {
                    s.push((b'a' + rng.below(6) as u8) as char);
                }
                s
            };
            let (a, b) = (mk(&mut rng), mk(&mut rng));
            let w = format!("s >= '{a}' AND s <= '{b}'");
            let (idx, scan) = both(&mut sm, &mut op, &w);
            assert_eq!(
                idx, scan,
                "index-narrowed != Seq Scan for `WHERE {w}`"
            );
        }
        // Single open bounds also narrow + match the full scan exactly.
        for w in ["s > 'm'", "s >= 'c'", "s < 'e'", "s <= 'bb'"] {
            let (idx, scan) = both(&mut sm, &mut op, w);
            assert_eq!(idx, scan, "index-narrowed != Seq Scan for `WHERE {w}`");
        }
    }

    /// SP91: a `RANGE INDEX` on a `U128` / `I128` column makes
    /// `SELECT … WHERE v …` index-narrowed through the SP70 planner,
    /// byte-identical to the same `WHERE` over an unindexed twin —
    /// including I128 ranges that straddle zero (negatives sort
    /// below positives).
    #[test]
    fn u128_i128_range_planner_narrows_and_equals_scan() {
        use kessel_proto::Rng;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (v U128 NOT NULL, n U32 NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE u (v U128 NOT NULL, n U32 NOT NULL)");
        run(&mut sm, 3, "CREATE RANGE INDEX ON t (v)");
        run(&mut sm, 4, "CREATE TABLE ti (v I128 NOT NULL, n U32 NOT NULL)");
        run(&mut sm, 5, "CREATE TABLE ui (v I128 NOT NULL, n U32 NOT NULL)");
        run(&mut sm, 6, "CREATE RANGE INDEX ON ti (v)");
        let ot = sm.catalog().get(1).unwrap().clone();
        let oti = sm.catalog().get(4).unwrap().clone();
        let mut rng = Rng::new(0x91_5C);
        // U128 values up to i128::MAX (SQL integer literals are i128).
        let mut uvals = Vec::new();
        let mut ivals = Vec::new();
        // Monotonic op-numbers (SP94 recovery guard short-circuits a
        // mutating op whose op-number is ≤ the durable cursor).
        let mut iop = 6u64;
        for id in 1..=120u32 {
            let uv = (rng.below(u64::MAX) as u128) << 60
                | rng.below(u64::MAX) as u128;
            let mag =
                (rng.below(u64::MAX) as i128) << 20 | rng.below(u64::MAX) as i128;
            let iv = if rng.below(2) == 0 { -mag } else { mag };
            iop += 1;
            run(&mut sm, iop,
                &format!("INSERT INTO t (id, v, n) VALUES ({id}, {uv}, {id})"));
            iop += 1;
            run(&mut sm, iop,
                &format!("INSERT INTO u (id, v, n) VALUES ({id}, {uv}, {id})"));
            iop += 1;
            run(&mut sm, iop,
                &format!("INSERT INTO ti (id, v, n) VALUES ({id}, {iv}, {id})"));
            iop += 1;
            run(&mut sm, iop,
                &format!("INSERT INTO ui (id, v, n) VALUES ({id}, {iv}, {id})"));
            uvals.push(uv);
            ivals.push(iv);
        }
        // Planner must emit a range pred on the 16-byte column.
        match compile("SELECT * FROM t WHERE v >= 5 AND v <= 9", sm.catalog())
            .expect("compile")
        {
            Op::QueryRows { range_preds, .. } => assert!(
                !range_preds.is_empty(),
                "U128 RANGE INDEX must surface a range pred"
            ),
            o => panic!("expected QueryRows, got {o:?}"),
        }
        let decode_n = |res: OpResult, t: &kessel_catalog::ObjectType| -> Vec<u32> {
            let b = match res {
                OpResult::Got(b) => b,
                o => panic!("unexpected {o:?}"),
            };
            let mut out = Vec::new();
            let mut p = 0;
            while p + 4 <= b.len() {
                let l =
                    u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                p += 4;
                let v = kessel_codec::decode(t, &b[p..p + l]).unwrap();
                p += l;
                if let kessel_codec::Value::Uint(u) = v[1] {
                    out.push(u as u32);
                }
            }
            out.sort_unstable();
            out
        };
        let mut op = 5000u64;
        for _ in 0..30 {
            let mut pick = |r: &mut Rng, v: &[u128]| v[r.below(v.len() as u64) as usize];
            let (mut a, mut b) = (pick(&mut rng, &uvals), pick(&mut rng, &uvals));
            if b < a {
                std::mem::swap(&mut a, &mut b);
            }
            op += 1;
            let idx = decode_n(
                run(&mut sm, op, &format!("SELECT * FROM t WHERE v >= {a} AND v <= {b}")),
                &ot,
            );
            op += 1;
            let scan = decode_n(
                run(&mut sm, op, &format!("SELECT * FROM u WHERE v >= {a} AND v <= {b}")),
                &ot,
            );
            assert_eq!(idx, scan, "U128 index-narrowed != Seq Scan [{a},{b}]");

            let mut pi = |r: &mut Rng| ivals[r.below(ivals.len() as u64) as usize];
            let (mut c, mut d) = (pi(&mut rng), pi(&mut rng));
            if d < c {
                std::mem::swap(&mut c, &mut d);
            }
            op += 1;
            let iidx = decode_n(
                run(&mut sm, op, &format!("SELECT * FROM ti WHERE v >= {c} AND v <= {d}")),
                &oti,
            );
            op += 1;
            let iscan = decode_n(
                run(&mut sm, op, &format!("SELECT * FROM ui WHERE v >= {c} AND v <= {d}")),
                &oti,
            );
            assert_eq!(iidx, iscan, "I128 index-narrowed != Seq Scan [{c},{d}]");
        }
        // An I128 window straddling zero must include both signs and
        // still match the full scan exactly.
        op += 1;
        let zi = decode_n(
            run(&mut sm, op, "SELECT * FROM ti WHERE v >= -1000000 AND v <= 1000000"),
            &oti,
        );
        op += 1;
        let zs = decode_n(
            run(&mut sm, op, "SELECT * FROM ui WHERE v >= -1000000 AND v <= 1000000"),
            &oti,
        );
        assert_eq!(zi, zs, "I128 zero-straddling window != Seq Scan");
    }

    /// #73 (SQL surface): `SELECT MIN(s)/MAX(s)` on a CHAR column and
    /// `MIN(u)/MAX(u)` on a U128 column compile to `Op::Aggregate` and
    /// return the brute-force extreme bytes — previously these were a
    /// hard `SchemaError` ("must be numeric ≤8B").
    #[test]
    fn sql_min_max_over_string_and_u128() {
        use kessel_proto::Rng;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (s CHAR(8) NOT NULL, u U128 NOT NULL)");
        run(&mut sm, 2, "CREATE RANGE INDEX ON t (s)"); // fast path for s
        let mut rng = Rng::new(0x73_5C);
        let mut ss: Vec<[u8; 8]> = Vec::new();
        let mut us: Vec<u128> = Vec::new();
        for id in 1..=80u32 {
            let len = rng.below(5) as usize;
            let mut s = [0u8; 8];
            for c in s.iter_mut().take(len) {
                *c = b'a' + rng.below(6) as u8;
            }
            let txt: String = s.iter().take(len).map(|&c| c as char).collect();
            let u = (rng.below(u64::MAX) as u128) << 60
                | rng.below(u64::MAX) as u128;
            run(&mut sm, 10 + id as u64,
                &format!("INSERT INTO t (id, s, u) VALUES ({id}, '{txt}', {u})"));
            ss.push(s);
            us.push(u);
        }
        let scalar = |sm: &mut StateMachine<MemVfs>, op: u64, q: &str| -> Vec<u8> {
            match run(sm, op, q) {
                OpResult::Got(b) => b.to_vec(),
                o => panic!("expected Got for `{q}`, got {o:?}"),
            }
        };
        assert_eq!(
            scalar(&mut sm, 900, "SELECT MIN(s) FROM t"),
            ss.iter().min().unwrap().to_vec(),
            "SQL MIN(s)"
        );
        assert_eq!(
            scalar(&mut sm, 901, "SELECT MAX(s) FROM t"),
            ss.iter().max().unwrap().to_vec(),
            "SQL MAX(s)"
        );
        assert_eq!(
            scalar(&mut sm, 902, "SELECT MIN(u) FROM t"),
            us.iter().min().unwrap().to_le_bytes().to_vec(),
            "SQL MIN(u)"
        );
        assert_eq!(
            scalar(&mut sm, 903, "SELECT MAX(u) FROM t"),
            us.iter().max().unwrap().to_le_bytes().to_vec(),
            "SQL MAX(u)"
        );
    }

    /// SP-Analytic-Plan T3: `SELECT SUM(x) FROM t WHERE d >= LO AND d
    /// < HI` compiles to `Op::Aggregate { range_preds: [(d, 1, LO), (d,
    /// 2, HI)] }` when an order index on `d` exists. Same compilation
    /// path for `SELECT COUNT(*) ... GROUP BY g` (Op::GroupAggregate).
    /// Conjunct-safety gate: `OR` in the WHERE drops the hints
    /// (program-only path = full scan, still correct).
    #[test]
    fn sp_analytic_plan_sql_planner_emits_range_preds_for_aggregate() {
        use kessel_proto::Op;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (g U8 NOT NULL, d I32 NOT NULL, x I64 NOT NULL)");
        run(&mut sm, 2, "CREATE RANGE INDEX ON t (d)");
        // Op::Aggregate — SUM with a half-range WHERE on `d`.
        let op = compile("SELECT SUM(x) FROM t WHERE d >= 100 AND d < 200", sm.catalog()).expect("compile");
        match op {
            Op::Aggregate { range_preds, .. } => {
                assert_eq!(range_preds.len(), 2, "expected two range hints (>= and <), got {range_preds:?}");
                let d_fid = sm.catalog().get(1).unwrap().fields.iter()
                    .find(|f| f.name == "d").unwrap().field_id;
                let ge = &range_preds[0];
                let lt = &range_preds[1];
                assert_eq!(ge.0, d_fid);
                assert_eq!(ge.1, 1u8, "`d >= 100` should encode op=1 (>=)");
                assert_eq!(lt.0, d_fid);
                assert_eq!(lt.1, 2u8, "`d < 200` should encode op=2 (<)");
                // Numeric value: I32 LE-truncated to 4 bytes.
                assert_eq!(ge.2, 100i32.to_le_bytes().to_vec());
                assert_eq!(lt.2, 200i32.to_le_bytes().to_vec());
            }
            other => panic!("expected Op::Aggregate, got {other:?}"),
        }
        // Op::GroupAggregate — GROUP BY + half-range WHERE on `d`.
        let op = compile("SELECT COUNT(x) FROM t WHERE d >= 50 GROUP BY g", sm.catalog()).expect("compile");
        match op {
            Op::GroupAggregate { range_preds, .. } => {
                assert_eq!(range_preds.len(), 1, "expected one range hint, got {range_preds:?}");
                assert_eq!(range_preds[0].1, 1u8, "`d >= 50` should encode op=1 (>=)");
            }
            other => panic!("expected Op::GroupAggregate, got {other:?}"),
        }
        // OR at top level — conjunct-safety gate drops the hints.
        let op = compile("SELECT SUM(x) FROM t WHERE d >= 100 OR d < 50", sm.catalog()).expect("compile");
        match op {
            Op::Aggregate { range_preds, .. } => {
                assert!(range_preds.is_empty(), "OR WHERE must drop range hints, got {range_preds:?}");
            }
            other => panic!("expected Op::Aggregate, got {other:?}"),
        }
        // Aggregate WITHOUT a WHERE — no hints (no token span).
        let op = compile("SELECT COUNT(x) FROM t", sm.catalog()).expect("compile");
        match op {
            Op::Aggregate { range_preds, .. } => {
                assert!(range_preds.is_empty(), "no WHERE ⇒ no range hints");
            }
            other => panic!("expected Op::Aggregate, got {other:?}"),
        }
        // Aggregate on a non-ordered column — hint silently dropped.
        let op = compile("SELECT SUM(x) FROM t WHERE g >= 5 AND g < 7", sm.catalog()).expect("compile");
        match op {
            Op::Aggregate { range_preds, .. } => {
                assert!(range_preds.is_empty(), "non-ordered column ⇒ no hints (g has no RANGE INDEX), got {range_preds:?}");
            }
            other => panic!("expected Op::Aggregate, got {other:?}"),
        }
    }

    /// SP-Analytic-Plan-MULTI T3: a SELECT with ≥2 aggregates (or a
    /// leading group column + ≥1 aggregate) compiles to a single
    /// `Op::GroupAggregateMulti`, not N separate `Op::GroupAggregate`
    /// calls. Single-aggregate paths stay byte-identical for back-compat.
    #[test]
    fn sp_analytic_plan_multi_sql_planner_emits_group_aggregate_multi() {
        use kessel_proto::Op;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (g U8 NOT NULL, d I32 NOT NULL, x I64 NOT NULL, y I64 NOT NULL)");
        run(&mut sm, 2, "CREATE RANGE INDEX ON t (d)");

        // ≥2 aggregates with GROUP BY g → Op::GroupAggregateMulti.
        let op = compile(
            "SELECT SUM(x), SUM(y) FROM t WHERE d >= 100 AND d < 200 GROUP BY g",
            sm.catalog(),
        ).expect("compile multi");
        match op {
            Op::GroupAggregateMulti { aggregates, range_preds, group_field, .. } => {
                assert_eq!(aggregates.len(), 2, "expected 2 aggs, got {aggregates:?}");
                assert_eq!(aggregates[0].0, 1u8, "first agg = SUM");
                assert_eq!(aggregates[1].0, 1u8, "second agg = SUM");
                assert_eq!(range_preds.len(), 2, "range_preds must carry both hints");
                let g_fid = sm.catalog().get(1).unwrap().fields.iter()
                    .find(|f| f.name == "g").unwrap().field_id;
                assert_eq!(group_field, g_fid);
            }
            other => panic!("expected Op::GroupAggregateMulti, got {other:?}"),
        }

        // Leading group col + 1 agg → Op::GroupAggregateMulti.
        let op = compile(
            "SELECT g, COUNT(x) FROM t WHERE d >= 50",
            sm.catalog(),
        ).expect("compile leading-col+agg");
        match op {
            Op::GroupAggregateMulti { aggregates, group_field, range_preds, .. } => {
                assert_eq!(aggregates.len(), 1);
                assert_eq!(aggregates[0].0, 0u8, "COUNT");
                let g_fid = sm.catalog().get(1).unwrap().fields.iter()
                    .find(|f| f.name == "g").unwrap().field_id;
                assert_eq!(group_field, g_fid);
                assert_eq!(range_preds.len(), 1, "one half-range hint");
            }
            other => panic!("expected Op::GroupAggregateMulti, got {other:?}"),
        }

        // Q1-shape: `SELECT g, SUM(x), SUM(y), COUNT(*) FROM t WHERE d
        // <= 200 GROUP BY g` → 3 aggregates.
        let op = compile(
            "SELECT g, SUM(x), SUM(y), COUNT(x) FROM t WHERE d <= 200 GROUP BY g",
            sm.catalog(),
        ).expect("compile q1-shape");
        match op {
            Op::GroupAggregateMulti { aggregates, .. } => {
                assert_eq!(aggregates.len(), 3, "expected 3 aggs, got {aggregates:?}");
                assert_eq!(aggregates[0].0, 1u8, "SUM(x)");
                assert_eq!(aggregates[1].0, 1u8, "SUM(y)");
                assert_eq!(aggregates[2].0, 0u8, "COUNT");
            }
            other => panic!("expected Op::GroupAggregateMulti, got {other:?}"),
        }

        // Back-compat: single agg without leading col stays Op::Aggregate /
        // Op::GroupAggregate (byte-identical to pre-arc emit).
        let op = compile("SELECT SUM(x) FROM t WHERE d >= 50", sm.catalog())
            .expect("compile single-agg");
        assert!(matches!(op, Op::Aggregate { .. }), "single agg = Aggregate, got {op:?}");
        let op = compile("SELECT SUM(x) FROM t WHERE d >= 50 GROUP BY g", sm.catalog())
            .expect("compile single-agg + GROUP BY");
        assert!(matches!(op, Op::GroupAggregate { .. }),
            "single agg + GROUP BY = GroupAggregate, got {op:?}");

        // Plain-column after aggregate is rejected.
        let err = compile("SELECT SUM(x), g FROM t GROUP BY g", sm.catalog());
        assert!(err.is_err(), "plain col after agg must error, got {err:?}");

        // Multi without GROUP BY (and no leading col) is rejected in V1.
        let err = compile("SELECT SUM(x), SUM(y) FROM t", sm.catalog());
        assert!(err.is_err(), "multi-agg w/o GROUP BY must error in V1");
    }

    /// SP-PG-SQL-GROUP-SORT-LIMIT — the SQL planner threads ORDER BY / LIMIT /
    /// OFFSET on a PLAIN GROUP BY into the new `GroupSort` on
    /// `Op::GroupAggregate` / `Op::GroupAggregateMulti`, resolving the ORDER BY
    /// target by alias, by position, by aggregate expr, and by the group key.
    #[test]
    fn sp_pg_sql_group_sort_limit_planner_attaches_sort() {
        use kessel_proto::{GroupSortTarget, Op};
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (g U8 NOT NULL, x I64 NOT NULL)");

        // Single-agg + GROUP BY, ORDER BY the aggregate alias DESC LIMIT 5 OFFSET 1.
        let op = compile(
            "SELECT g, COUNT(*) AS n FROM t GROUP BY g ORDER BY n DESC LIMIT 5 OFFSET 1",
            sm.catalog(),
        ).expect("compile alias-order");
        // (`g, COUNT(*)` is leading-col + 1 agg ⇒ Op::GroupAggregateMulti.)
        match op {
            Op::GroupAggregateMulti { sort, .. } => {
                let s = sort.expect("sort threaded");
                assert_eq!(s.target, GroupSortTarget::Agg(0), "alias n ⇒ agg 0");
                assert!(s.desc, "DESC");
                assert_eq!(s.limit, Some(5));
                assert_eq!(s.offset, Some(1));
            }
            other => panic!("expected Multi+sort, got {other:?}"),
        }

        // ORDER BY position 2 (the aggregate) ASC.
        let op = compile(
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY 2",
            sm.catalog(),
        ).expect("compile position-order");
        match op {
            Op::GroupAggregateMulti { sort, .. } => {
                let s = sort.expect("sort threaded");
                assert_eq!(s.target, GroupSortTarget::Agg(0), "position 2 ⇒ agg 0");
                assert!(!s.desc, "ASC default");
            }
            other => panic!("expected Multi+sort, got {other:?}"),
        }

        // ORDER BY the aggregate EXPRESSION COUNT(*) DESC, single-aggregate path.
        let op = compile(
            "SELECT COUNT(*) FROM t GROUP BY g ORDER BY COUNT(*) DESC LIMIT 2",
            sm.catalog(),
        ).expect("compile aggexpr-order");
        match op {
            Op::GroupAggregate { sort, .. } => {
                let s = sort.expect("sort threaded");
                assert_eq!(s.target, GroupSortTarget::Agg(0));
                assert!(s.desc);
                assert_eq!(s.limit, Some(2));
            }
            other => panic!("expected GroupAggregate+sort, got {other:?}"),
        }

        // ORDER BY the group key column ⇒ sort by Key.
        let op = compile(
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g",
            sm.catalog(),
        ).expect("compile key-order");
        match op {
            Op::GroupAggregateMulti { sort, .. } => {
                assert_eq!(sort.expect("sort").target, GroupSortTarget::Key, "ORDER BY g ⇒ key");
            }
            other => panic!("expected Multi+sort, got {other:?}"),
        }

        // ORDER BY position 1 ⇒ the group key.
        let op = compile(
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY 1 DESC",
            sm.catalog(),
        ).expect("compile pos1");
        match op {
            Op::GroupAggregateMulti { sort, .. } => {
                let s = sort.expect("sort");
                assert_eq!(s.target, GroupSortTarget::Key);
                assert!(s.desc);
            }
            other => panic!("expected Multi+sort, got {other:?}"),
        }

        // No ORDER BY ⇒ sort is None (byte-identical to pre-arc emit).
        let op = compile("SELECT COUNT(*) FROM t GROUP BY g", sm.catalog())
            .expect("compile no-order");
        match op {
            Op::GroupAggregate { sort, .. } => assert!(sort.is_none(), "no ORDER BY ⇒ no sort"),
            other => panic!("expected GroupAggregate, got {other:?}"),
        }

        // ORDER BY an aggregate NOT in the SELECT list is rejected.
        let err = compile(
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY SUM(x)",
            sm.catalog(),
        );
        assert!(err.is_err(), "ORDER BY non-projected aggregate must error");

        // ORDER BY out-of-range position is rejected.
        let err = compile(
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY 9",
            sm.catalog(),
        );
        assert!(err.is_err(), "ORDER BY out-of-range position must error");
    }

    /// SP-PG-SQL-HAVING — the SQL planner attaches a HAVING predicate to the
    /// single-aggregate (Op::GroupAggregate), multi-aggregate
    /// (Op::GroupAggregateMulti), and join-group-aggregate paths, matching the
    /// HAVING aggregate to a projected aggregate by (kind, field). It rejects a
    /// HAVING aggregate not in the SELECT list and a HAVING without grouping.
    #[test]
    fn sp_pg_sql_having_planner_attaches_predicate() {
        use kessel_proto::{HavingPred, Op};
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (g U8 NOT NULL, x I64 NOT NULL)");

        // Single-aggregate GROUP BY + HAVING COUNT(*) >= 3 → Op::GroupAggregate
        // with having { agg_index: 0, op: 1 (>=), value: 3 }.
        let op = compile(
            "SELECT g, COUNT(*) FROM t GROUP BY g HAVING COUNT(*) >= 3",
            sm.catalog(),
        ).expect("compile single-agg HAVING");
        match op {
            Op::GroupAggregateMulti { having, .. } | // leading col => Multi
            Op::GroupAggregate { having, .. } => {
                assert_eq!(having, Some(HavingPred { agg_index: 0, op: 1, value: 3 }));
            }
            other => panic!("expected a group-aggregate, got {other:?}"),
        }

        // Plain single-agg (no leading col) GROUP BY + HAVING → Op::GroupAggregate.
        let op = compile(
            "SELECT SUM(x) FROM t GROUP BY g HAVING SUM(x) > 100",
            sm.catalog(),
        ).expect("compile plain single-agg HAVING");
        match op {
            Op::GroupAggregate { having, kind, .. } => {
                assert_eq!(kind, 1, "SUM");
                assert_eq!(having, Some(HavingPred { agg_index: 0, op: 0, value: 100 }));
            }
            other => panic!("expected Op::GroupAggregate, got {other:?}"),
        }

        // Multi-aggregate: HAVING on the 2nd projected aggregate → agg_index 1.
        let op = compile(
            "SELECT g, COUNT(*), SUM(x) FROM t GROUP BY g HAVING SUM(x) <= 50",
            sm.catalog(),
        ).expect("compile multi HAVING");
        match op {
            Op::GroupAggregateMulti { having, aggregates, .. } => {
                assert_eq!(aggregates.len(), 2);
                assert_eq!(having, Some(HavingPred { agg_index: 1, op: 3, value: 50 }));
            }
            other => panic!("expected Op::GroupAggregateMulti, got {other:?}"),
        }

        // Every comparison operator maps to the right wire code.
        for (sql_op, code) in [(">", 0u8), (">=", 1), ("<", 2), ("<=", 3), ("=", 4), ("<>", 5), ("!=", 5)] {
            let op = compile(
                &format!("SELECT g, COUNT(*) FROM t GROUP BY g HAVING COUNT(*) {sql_op} 2"),
                sm.catalog(),
            ).unwrap_or_else(|e| panic!("compile HAVING {sql_op}: {e}"));
            let having = match op {
                Op::GroupAggregateMulti { having, .. } | Op::GroupAggregate { having, .. } => having,
                other => panic!("unexpected {other:?}"),
            };
            assert_eq!(having.unwrap().op, code, "op `{sql_op}` → {code}");
        }

        // Negative literal RHS is supported.
        let op = compile(
            "SELECT g, SUM(x) FROM t GROUP BY g HAVING SUM(x) > -5",
            sm.catalog(),
        ).expect("compile negative RHS");
        let having = match op {
            Op::GroupAggregateMulti { having, .. } | Op::GroupAggregate { having, .. } => having,
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(having.unwrap().value, -5);

        // HAVING over an aggregate NOT in the SELECT list is rejected (V1).
        let err = compile(
            "SELECT g, COUNT(*) FROM t GROUP BY g HAVING SUM(x) > 1",
            sm.catalog(),
        );
        assert!(err.is_err(), "HAVING agg not in projection must error, got {err:?}");

        // HAVING with a non-aggregate projection is rejected.
        let err = compile("SELECT g FROM t GROUP BY g HAVING COUNT(*) > 1", sm.catalog());
        assert!(err.is_err(), "HAVING on non-aggregate projection must error");

        // No HAVING ⇒ no predicate attached (byte-identity prerequisite).
        let op = compile("SELECT g, COUNT(*) FROM t GROUP BY g", sm.catalog()).unwrap();
        let having = match op {
            Op::GroupAggregateMulti { having, .. } | Op::GroupAggregate { having, .. } => having,
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(having, None, "no HAVING ⇒ None");
    }

    /// SP-Analytic-Plan-MULTI T3 end-to-end oracle: a multi-aggregate
    /// SQL query executed via the planner-emitted Op::GroupAggregateMulti
    /// MUST produce per-aggregate per-group values byte-identical to the
    /// sequence of separate Op::GroupAggregate calls (the old shape).
    #[test]
    fn sp_analytic_plan_multi_sql_indexed_equals_n_single_aggregate() {
        use kessel_proto::Op;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (g U8 NOT NULL, x I64 NOT NULL, y I64 NOT NULL)");
        let mut next_op = 10u64;
        for id in 1..=80u32 {
            let g = (id % 4) as u8;
            let x = (id * 3) as i64;
            let y = (id * 5) as i64 - 17;
            run(&mut sm, next_op,
                &format!("INSERT INTO t (id, g, x, y) VALUES ({id}, {g}, {x}, {y})"));
            next_op += 1;
        }
        // Multi via SQL.
        let multi_op = compile(
            "SELECT g, SUM(x), SUM(y), MIN(x), MAX(y), COUNT(x) FROM t GROUP BY g",
            sm.catalog(),
        ).expect("compile");
        let multi_bytes = match sm.apply(next_op, multi_op) {
            OpResult::Got(b) => b.to_vec(),
            o => panic!("{o:?}"),
        };
        next_op += 1;
        // Parse multi result (key is U8 = 1 byte).
        let parse_multi = |b: &[u8], n_aggs: usize| -> Vec<(u8, Vec<i128>)> {
            let mut out = Vec::new();
            let n = u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize;
            let mut p = 4;
            for _ in 0..n {
                let kl = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                p += 4;
                let key = b[p];
                p += kl;
                let mut vs = Vec::with_capacity(n_aggs);
                for _ in 0..n_aggs {
                    vs.push(i128::from_le_bytes(b[p..p + 16].try_into().unwrap()));
                    p += 16;
                }
                out.push((key, vs));
            }
            out
        };
        let parse_single = |b: &[u8]| -> Vec<(u8, i128)> {
            let mut out = Vec::new();
            let n = u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize;
            let mut p = 4;
            for _ in 0..n {
                let kl = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                p += 4;
                let key = b[p];
                p += kl;
                let val = i128::from_le_bytes(b[p..p + 16].try_into().unwrap());
                p += 16;
                out.push((key, val));
            }
            out
        };
        let multi = parse_multi(&multi_bytes, 5);

        // Compare slot-by-slot against SUM(x), SUM(y), MIN(x), MAX(y), COUNT(x).
        let queries = [
            "SELECT SUM(x) FROM t GROUP BY g",
            "SELECT SUM(y) FROM t GROUP BY g",
            "SELECT MIN(x) FROM t GROUP BY g",
            "SELECT MAX(y) FROM t GROUP BY g",
            "SELECT COUNT(x) FROM t GROUP BY g",
        ];
        for (slot, q) in queries.iter().enumerate() {
            let op = compile(q, sm.catalog()).expect("compile single");
            assert!(matches!(op, Op::GroupAggregate { .. }), "single-agg path");
            let single_bytes = match sm.apply(next_op, op) {
                OpResult::Got(b) => b.to_vec(),
                o => panic!("{o:?}"),
            };
            next_op += 1;
            let single = parse_single(&single_bytes);
            assert_eq!(multi.len(), single.len(), "group count differs at slot {slot}");
            for ((mk, mv), (sk, sv)) in multi.iter().zip(single.iter()) {
                assert_eq!(mk, sk, "key mismatch at slot {slot}");
                assert_eq!(mv[slot], *sv,
                    "slot {slot} value mismatch for group {mk} (query {q})");
            }
        }
    }

    /// SP-Analytic-Plan T3 oracle: the planner-emitted range_preds
    /// produce a result byte-identical to the same SQL run against an
    /// un-indexed twin table (where the planner emits empty
    /// range_preds and the engine does a full scan). End-to-end proof
    /// that the planner's emission + the SM's narrowing are
    /// semantically equivalent.
    #[test]
    fn sp_analytic_plan_aggregate_indexed_equals_unindexed_twin() {
        use kessel_proto::Rng;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        // `idx` has the range index; `noidx` is the un-indexed twin.
        run(&mut sm, 1, "CREATE TABLE idx (g U8 NOT NULL, d I32 NOT NULL, x I64 NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE noidx (g U8 NOT NULL, d I32 NOT NULL, x I64 NOT NULL)");
        run(&mut sm, 3, "CREATE RANGE INDEX ON idx (d)");
        let mut rng = Rng::new(0xA1A1_B2B2_C3C3);
        // Monotonic op_number is mandatory — SP94 replay guard rejects
        // non-monotonic ops as "already applied". Interleave with a
        // single counter so each insert gets its own op.
        let mut next_op = 100u64;
        for id in 1..=200u32 {
            let g = (id % 5) as u8;
            let d = (rng.below(1000)) as i32 - 100; // -100..900
            let x = rng.below(10_000) as i64;
            run(&mut sm, next_op,
                &format!("INSERT INTO idx (id, g, d, x) VALUES ({id}, {g}, {d}, {x})"));
            next_op += 1;
            run(&mut sm, next_op,
                &format!("INSERT INTO noidx (id, g, d, x) VALUES ({id}, {g}, {d}, {x})"));
            next_op += 1;
        }
        let scalar = |sm: &mut StateMachine<MemVfs>, op: u64, q: &str| -> Vec<u8> {
            match run(sm, op, q) {
                OpResult::Got(b) => b.to_vec(),
                o => panic!("expected Got for `{q}`, got {o:?}"),
            }
        };
        // For each shape: ensure indexed table (range_preds emitted)
        // and unindexed twin (range_preds empty) agree byte-for-byte.
        let pairs: &[(&str, &str)] = &[
            ("SELECT SUM(x) FROM idx WHERE d >= 100 AND d < 500",
             "SELECT SUM(x) FROM noidx WHERE d >= 100 AND d < 500"),
            ("SELECT COUNT(x) FROM idx WHERE d >= 0",
             "SELECT COUNT(x) FROM noidx WHERE d >= 0"),
            ("SELECT MIN(x) FROM idx WHERE d > 200 AND d <= 750",
             "SELECT MIN(x) FROM noidx WHERE d > 200 AND d <= 750"),
            ("SELECT MAX(x) FROM idx WHERE d <= -50",
             "SELECT MAX(x) FROM noidx WHERE d <= -50"),
            ("SELECT COUNT(x) FROM idx WHERE d >= 50 AND d < 250 GROUP BY g",
             "SELECT COUNT(x) FROM noidx WHERE d >= 50 AND d < 250 GROUP BY g"),
            ("SELECT SUM(x) FROM idx WHERE d >= 50 AND d < 250 GROUP BY g",
             "SELECT SUM(x) FROM noidx WHERE d >= 50 AND d < 250 GROUP BY g"),
            // Empty match window
            ("SELECT SUM(x) FROM idx WHERE d >= 999999 AND d < 9999999",
             "SELECT SUM(x) FROM noidx WHERE d >= 999999 AND d < 9999999"),
        ];
        let mut op = next_op + 100;
        for (q_idx, q_noidx) in pairs {
            let r_idx = scalar(&mut sm, op, q_idx); op += 1;
            let r_no = scalar(&mut sm, op, q_noidx); op += 1;
            assert_eq!(
                r_idx, r_no,
                "indexed (range_preds) vs unindexed (full scan) diverged for `{q_idx}`"
            );
        }
    }

    /// SP86: a column `DEFAULT` is applied to omitted INSERT columns
    /// (including a `NOT NULL` column that has a default), an explicit
    /// value overrides it, a `NOT NULL` column with no default still
    /// errors, the default survives a catalog round-trip, and it is
    /// deterministic.
    #[test]
    fn column_default_is_applied_and_persists() {
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            run(
                &mut sm,
                1,
                "CREATE TABLE t (a U32 NOT NULL, b I64 DEFAULT 7, \
                 c U32 NOT NULL DEFAULT 3)",
            );
            sm
        };
        let row = |sm: &mut StateMachine<MemVfs>, op, id: u128| -> (u32, i64, u32) {
            match sm.apply(op, compile(&format!("SELECT * FROM t ID {id}"), sm.catalog()).unwrap())
            {
                OpResult::Got(r) => {
                    let ot = sm.catalog().get(1).unwrap().clone();
                    let v = kessel_codec::decode(&ot, &r).unwrap();
                    let a = match v[0] { Value::Uint(u) => u as u32, _ => panic!() };
                    let b = match v[1] { Value::Int(i) => i as i64, _ => panic!() };
                    let c = match v[2] { Value::Uint(u) => u as u32, _ => panic!() };
                    (a, b, c)
                }
                o => panic!("unexpected {o:?}"),
            }
        };
        let mut sm = build();
        // Omit b and c → both take their declared defaults (c is NOT
        // NULL but has a default, so it's satisfied).
        assert_eq!(
            run(&mut sm, 2, "INSERT INTO t (id, a) VALUES (1, 5)"),
            OpResult::Ok
        );
        assert_eq!(row(&mut sm, 3, 1), (5, 7, 3));
        // Explicit values override the defaults.
        assert_eq!(
            run(&mut sm, 4, "INSERT INTO t (id, a, b, c) VALUES (2, 9, -1, 100)"),
            OpResult::Ok
        );
        assert_eq!(row(&mut sm, 5, 2), (9, -1, 100));
        // A NOT NULL column WITHOUT a default is still required.
        run(&mut sm, 6, "CREATE TABLE u (x U32 NOT NULL)");
        assert!(
            compile("INSERT INTO u (id) VALUES (1)", sm.catalog()).is_err(),
            "NOT NULL with no default must still be required"
        );
        // Default survives a full catalog encode/decode round-trip.
        let cat2 = kessel_catalog::Catalog::decode(
            &sm.catalog().encode(),
        )
        .unwrap();
        assert_eq!(
            cat2.get(1).unwrap().defaults,
            sm.catalog().get(1).unwrap().defaults,
            "defaults must persist through the catalog blob"
        );
        assert!(
            !cat2.get(1).unwrap().defaults.is_empty(),
            "two defaults were declared"
        );
        // Deterministic.
        let a = build();
        let b2 = build();
        assert_eq!(a.digest(), b2.digest());
    }

    /// SP-PG-SQL-PAREN-VALUES — pgJDBC simple-mode `PreparedStatement`
    /// wraps every substituted parameter in `(…)` so the SQL the
    /// engine sees is `INSERT INTO t (id, name) VALUES (('42'),
    /// ('hello'))` (post SP-PG-EXTQ-CAST `::TYPE` strip). PG treats
    /// `(LITERAL)` as equivalent to `LITERAL` (expression grouping);
    /// the VALUES tuple parser must too. K-PVAL-1..9 lock the parser
    /// shape: bare path unchanged, 1-/3-/8-level parens accepted,
    /// 9-level parens rejected (anti-stack-bomb cap), mixed bare+paren
    /// in the same tuple works, multi-row paren VALUES works, and an
    /// unbalanced paren errors cleanly.
    #[test]
    fn paren_wrapped_values_literals() {
        let cat = {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            // `id` is the pseudo row-id pulled from the VALUES tuple;
            // the only declared fields are `v` (I64) and `name`
            // (nullable CHAR(16)). This matches the convention used
            // throughout the rest of the INSERT KATs in this module.
            run(
                &mut sm,
                1,
                "CREATE TABLE t (v I64 NOT NULL, name CHAR(16))",
            );
            sm.catalog().clone()
        };

        // Helper — compile then assert this is an Op::Create whose
        // record decodes to the expected (v, name) tuple with the
        // expected pseudo row-id.
        let assert_create = |sql: &str, want_id: u128, want_v: i64,
                             want_name: &str| {
            let op = compile(sql, &cat)
                .unwrap_or_else(|e| panic!("compile `{sql}`: {e}"));
            match op {
                Op::Create { id, record, .. } => {
                    assert_eq!(
                        id,
                        ObjectId::from_u128(want_id),
                        "row id for `{sql}`"
                    );
                    let ot = cat.get(1).unwrap();
                    let v = kessel_codec::decode(ot, &record)
                        .expect("decode");
                    let got_v = match v[0] {
                        Value::Int(i) => i as i64,
                        _ => panic!("v not Int for `{sql}`: {:?}", v[0]),
                    };
                    assert_eq!(got_v, want_v, "v for `{sql}`");
                    let got_name = match &v[1] {
                        Value::Blob(b) => {
                            // Char(16) is NUL-padded; trim the
                            // padding for the comparison.
                            let end = b
                                .iter()
                                .position(|&c| c == 0)
                                .unwrap_or(b.len());
                            String::from_utf8(b[..end].to_vec())
                                .expect("utf8")
                        }
                        Value::Null => String::new(),
                        _ => panic!(
                            "name not Blob/Null for `{sql}`: {:?}",
                            v[1]
                        ),
                    };
                    assert_eq!(got_name, want_name, "name for `{sql}`");
                }
                o => panic!("expected Op::Create for `{sql}`, got {o:?}"),
            }
        };

        // K-PVAL-1: bare path regression — unchanged behavior.
        assert_create(
            "INSERT INTO t (id, v, name) VALUES (1, 2, 'a')",
            1,
            2,
            "a",
        );

        // K-PVAL-2: 1-level paren — `((1), (2), ('a'))` ≡ `(1, 2, 'a')`.
        assert_create(
            "INSERT INTO t (id, v, name) VALUES ((1), (2), ('a'))",
            1,
            2,
            "a",
        );

        // K-PVAL-3: pgJDBC simple-mode failing case verbatim
        // (post-strip): INT + TEXT paren-wrapped.
        assert_create(
            "INSERT INTO t (id, v, name) VALUES ((42), (7), ('hello'))",
            42,
            7,
            "hello",
        );

        // K-PVAL-4: 3-level paren depth.
        assert_create(
            "INSERT INTO t (id, v, name) VALUES (((1)), ((2)), (('a')))",
            1,
            2,
            "a",
        );

        // K-PVAL-5: 8-level paren depth accepted on the first
        // position; bare on the others. Cap boundary. 8 levels of
        // expression-grouping `(` …`)` wrap the value `1`; the outer
        // tuple `(` adds the 9th open. Closes balance: 8 grouping
        // `)`s match the 8 grouping `(`s; the final `)` closes the
        // outer tuple.
        assert_create(
            "INSERT INTO t (id, v, name) VALUES \
             (((((((((1)))))))), 2, 'a')",
            1,
            2,
            "a",
        );

        // K-PVAL-6: 9-level paren depth rejected (anti-stack-bomb).
        // 9 grouping `(`s before `1` + outer tuple `(` = 10 total
        // opens before `1`; the parser hits the depth cap at 9
        // grouping levels.
        let e = compile(
            "INSERT INTO t (id, v, name) VALUES \
             ((((((((((1))))))))), 2, 'a')",
            &cat,
        )
        .expect_err("9-level paren depth must reject");
        assert!(
            e.to_lowercase().contains("nested parens"),
            "expected nested-parens error, got: {e}"
        );

        // K-PVAL-7: mixed paren + bare in the same tuple.
        assert_create(
            "INSERT INTO t (id, v, name) VALUES ((1), 2, 'a')",
            1,
            2,
            "a",
        );

        // K-PVAL-8: multi-row paren VALUES — both rows land atomically.
        let op = compile(
            "INSERT INTO t (id, v, name) VALUES \
             ((1), (2), ('a')), ((3), (4), ('b'))",
            &cat,
        )
        .expect("compile multi-row paren VALUES");
        match op {
            Op::Txn { ops } => {
                assert_eq!(ops.len(), 2, "two rows expected");
                for (i, sub) in ops.iter().enumerate() {
                    let want_id = if i == 0 { 1u128 } else { 3u128 };
                    match sub {
                        Op::Create { id, .. } => assert_eq!(
                            *id,
                            ObjectId::from_u128(want_id),
                            "row {i} id"
                        ),
                        o => panic!("expected Op::Create, got {o:?}"),
                    }
                }
            }
            o => panic!("expected Op::Txn, got {o:?}"),
        }

        // K-PVAL-9: unbalanced opening paren rejects cleanly (the
        // inner `(` is consumed as paren depth, `1` parses, then `,`
        // arrives where `)` was expected).
        let e2 = compile(
            "INSERT INTO t (id, v, name) VALUES ((1, 2, 'a')",
            &cat,
        )
        .expect_err("unbalanced paren must reject");
        assert!(
            !e2.is_empty(),
            "unbalanced paren error must be non-empty"
        );

        // K-PVAL-10: pseudo `id` resolution accepts a `Lit::Str` whose
        // contents parse as a decimal integer — pgJDBC simple-mode
        // emits `VALUES (('42'::int8), …)` which after SP-PG-EXTQ-CAST
        // is `VALUES (('42'), …)`. The id resolution path now coerces
        // `'42'` → 42 to land the right pseudo row-id.
        assert_create(
            "INSERT INTO t (id, v, name) VALUES (('42'), 7, 'hello')",
            42,
            7,
            "hello",
        );
    }

    /// SP-PG-SQL-PAREN-VALUES — when the WHERE-clause LHS is a numeric
    /// column and the RHS is a string literal whose contents parse as
    /// a decimal integer (the post-SP-PG-EXTQ-CAST shape of pgJDBC
    /// simple-mode `WHERE id = ('42'::int8)`), the SQL compiler
    /// coerces the literal to the matching int. K-PVAL-W1..3 lock the
    /// shape: bare paren-wrapped int literal works, mixed numeric
    /// column types are coerced, and a non-numeric column with a
    /// numeric-shaped string preserves byte semantics (no coercion).
    #[test]
    fn paren_wrapped_where_numeric_coercion() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        // Schema declares `v` (I64) + `name` (CHAR(16)); `id` is the
        // pseudo row-id only (not a queryable column).
        run(
            &mut sm,
            1,
            "CREATE TABLE t (v I64 NOT NULL, name CHAR(16))",
        );
        // Insert pseudo id=42, v=7, name='hello'.
        assert_eq!(
            run(
                &mut sm,
                2,
                "INSERT INTO t (id, v, name) VALUES (42, 7, 'hello')"
            ),
            OpResult::Ok
        );
        // K-PVAL-W1: paren-wrapped string-shaped int literal matches.
        // This is the exact pgJDBC simple-mode emit shape after the
        // SP-PG-EXTQ-CAST strip drops the `::int8` cast.
        let res = run(&mut sm, 3, "SELECT * FROM t WHERE v = ('7')");
        let bytes = match &res {
            OpResult::Got(b) => b.clone(),
            other => panic!("expected Got, got {other:?}"),
        };
        // At least one row in the result set (length-prefixed records).
        assert!(
            bytes.len() > 4,
            "WHERE v = ('7') should match v=7; got empty result \
             ({} bytes)",
            bytes.len()
        );

        // K-PVAL-W2: bare string-shaped int literal also coerces.
        let res2 = run(&mut sm, 4, "SELECT * FROM t WHERE v = '7'");
        let bytes2 = match &res2 {
            OpResult::Got(b) => b.clone(),
            other => panic!("expected Got, got {other:?}"),
        };
        assert!(
            bytes2.len() > 4,
            "WHERE v = '7' should match v=7; got empty result \
             ({} bytes)",
            bytes2.len()
        );

        // K-PVAL-W3: non-numeric column (name CHAR(16)) keeps byte
        // semantics — the string literal stays as bytes, no coercion.
        // The row stored 'hello'; WHERE name = 'hello' must still
        // match (regression guard for the CHAR comparison path).
        let res3 = run(&mut sm, 5, "SELECT * FROM t WHERE name = 'hello'");
        let bytes3 = match &res3 {
            OpResult::Got(b) => b.clone(),
            other => panic!("expected Got, got {other:?}"),
        };
        assert!(
            bytes3.len() > 4,
            "WHERE name = 'hello' regression: expected match, got \
             empty result ({} bytes)",
            bytes3.len()
        );
    }

    /// SP70: a selective range query must be sub-linear with a RANGE
    /// index — i.e. materially faster than the full-scan + verify path,
    /// while returning the *identical* rows (correctness is the oracle's
    /// job; this asserts the speed-up is real and the answers match).
    #[test]
    fn range_index_is_sublinear_and_correct() {
        let n = 40_000u128;
        // Same dataset twice: one table WITHOUT a range index (forced
        // full scan + program verify) and one WITH it (order-index
        // narrowed). Identical rows + a large speed-up.
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE a (v I64 NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE b (v I64 NOT NULL)");
        run(&mut sm, 3, "CREATE RANGE INDEX ON b (v)");
        let mut o = 100u64;
        for id in 1..=n {
            let v = (id as i64 * 2654435761) % 1_000_000; // scattered
            o += 1;
            run(&mut sm, o, &format!("INSERT INTO a (id, v) VALUES ({id}, {v})"));
            o += 1;
            run(&mut sm, o, &format!("INSERT INTO b (id, v) VALUES ({id}, {v})"));
        }
        let count = |res: &OpResult| -> usize {
            let b = match res {
                OpResult::Got(b) => b,
                x => panic!("unexpected {x:?}"),
            };
            let (mut p, mut c) = (0usize, 0usize);
            while p + 4 <= b.len() {
                let l = u32::from_le_bytes(b[p..p + 4].try_into().unwrap())
                    as usize;
                p += 4 + l;
                c += 1;
            }
            c
        };
        // A narrow window (≈0.2% of the domain).
        let q = "WHERE v >= 100000 AND v <= 102000";
        let t0 = std::time::Instant::now();
        o += 1;
        let scan = run(&mut sm, o, &format!("SELECT * FROM a {q}"));
        let scan_us = t0.elapsed().as_micros();
        let t1 = std::time::Instant::now();
        o += 1;
        let idx = run(&mut sm, o, &format!("SELECT * FROM b {q}"));
        let idx_us = t1.elapsed().as_micros();
        let (cs, ci) = (count(&scan), count(&idx));
        assert_eq!(cs, ci, "range-index result must equal the full scan");
        assert!(cs > 0 && cs < n as usize, "sanity: a real subset matched");
        println!(
            "[range-index] {n} rows, {cs} matched: full-scan {scan_us}µs vs \
             range-index {idx_us}µs  (~{:.0}x)",
            scan_us as f64 / idx_us.max(1) as f64
        );
        assert!(
            idx_us * 3 < scan_us,
            "range index must be materially sub-linear (got idx={idx_us}µs \
             scan={scan_us}µs)"
        );
    }

    /// SP62 oracle: for randomized data + randomized WHEREs (mixing
    /// equality on an INDEXED column with range / OR / mixed predicates),
    /// the planned `SELECT *` result must EXACTLY equal an independent
    /// brute-force filter. This guards that index-narrowing can never
    /// drop a matching row — the only way the planner could be unsafe.
    #[test]
    fn planner_equivalence_oracle() {
        use kessel_proto::Rng;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (k U32 NOT NULL, v I64 NOT NULL)");
        run(&mut sm, 2, "CREATE INDEX ON t (k)"); // single-col index on k
        run(&mut sm, 3, "CREATE INDEX ON t (k, v)"); // composite (k, v) — SP63
        run(&mut sm, 4, "CREATE RANGE INDEX ON t (v)"); // SP70 order index
        let ot = sm.catalog().get(1).unwrap().clone();

        let mut rng = Rng::new(0xA5A5_1234);
        let mut model: Vec<(u128, u64, i64)> = Vec::new(); // (id,k,v)
        for id in 1..=120u128 {
            let k = (rng.below(8)) as u64; // small domain ⇒ many dup keys
            let v = (rng.below(40) as i64) - 20; // -20..19
            run(
                &mut sm,
                100 + id as u64,
                &format!("INSERT INTO t (id, k, v) VALUES ({id}, {k}, {v})"),
            );
            model.push((id, k, v));
        }

        // Decode a `SELECT *` result into the multiset of (k, v).
        let decode_kv = |res: OpResult| -> Vec<(u64, i64)> {
            let b = match res {
                OpResult::Got(b) => b,
                o => panic!("unexpected {o:?}"),
            };
            let mut out = Vec::new();
            let mut p = 0;
            while p + 4 <= b.len() {
                let l =
                    u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                p += 4;
                let vals = kessel_codec::decode(&ot, &b[p..p + l]).unwrap();
                p += l;
                let k = match vals[0] {
                    Value::Uint(u) => u as u64,
                    _ => panic!(),
                };
                let v = match vals[1] {
                    Value::Int(i) => i as i64,
                    _ => panic!(),
                };
                out.push((k, v));
            }
            out
        };

        let mut op = 1000u64;
        for _ in 0..60 {
            let kk = rng.below(8);
            let mm = (rng.below(40) as i64) - 20;
            // a representative mix; each is a top-level AND chain, an OR,
            // or a non-equality — all must round-trip exactly.
            let queries: Vec<(String, Box<dyn Fn(u64, i64) -> bool>)> = vec![
                (
                    format!("k = {kk}"),
                    Box::new(move |k, _v| k == kk),
                ),
                (
                    format!("k = {kk} AND v > {mm}"),
                    Box::new(move |k, v| k == kk && v > mm),
                ),
                (
                    format!("k = {kk} AND v <= {mm} AND v >= {}", mm - 5),
                    Box::new(move |k, v| k == kk && v <= mm && v >= mm - 5),
                ),
                (
                    format!("k = {kk} OR v = {mm}"),
                    Box::new(move |k, v| k == kk || v == mm),
                ),
                (
                    // exact composite (k, v) equality — SP63 path
                    format!("k = {kk} AND v = {mm}"),
                    Box::new(move |k, v| k == kk && v == mm),
                ),
                (
                    // composite-covered eq + extra range conjunct
                    format!("v = {mm} AND k = {kk} AND v >= {}", mm - 1),
                    Box::new(move |k, v| v == mm && k == kk && v >= mm - 1),
                ),
                (
                    format!("v > {mm} AND k = {kk}"),
                    Box::new(move |k, v| v > mm && k == kk),
                ),
                (
                    // SP70: pure range, NO equality — exercises the
                    // range-only narrowing path (cand starts from the
                    // order index, not an eq index).
                    format!("v >= {mm}"),
                    Box::new(move |_k, v| v >= mm),
                ),
                (
                    // SP70: a band (two half-range hints on one ordered
                    // column intersect to the interval).
                    format!("v >= {} AND v <= {}", mm - 5, mm + 5),
                    Box::new(move |_k, v| v >= mm - 5 && v <= mm + 5),
                ),
                (
                    format!("NOT (k = {kk})"),
                    Box::new(move |k, _v| !(k == kk)),
                ),
            ];
            for (w, pred) in queries {
                op += 1;
                let mut got =
                    decode_kv(run(&mut sm, op, &format!("SELECT * FROM t WHERE {w}")));
                got.sort();
                let mut want: Vec<(u64, i64)> = model
                    .iter()
                    .filter(|(_, k, v)| pred(*k, *v))
                    .map(|(_, k, v)| (*k, *v))
                    .collect();
                want.sort();
                assert_eq!(got, want, "WHERE {w}: planner result != brute force");
            }
        }
    }

    #[test]
    fn drop_table_sql() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        assert!(matches!(
            run(&mut sm, 1, "CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)"),
            OpResult::TypeCreated(1)
        ));
        assert_eq!(
            run(&mut sm, 2, "INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)"),
            OpResult::Ok
        );
        assert_eq!(run(&mut sm, 3, "DROP TABLE acct"), OpResult::Ok);
        // The table (and its row) are gone; compiling against it now fails.
        assert!(compile("SELECT * FROM acct ID 1", sm.catalog()).is_err());
        assert!(compile("DROP TABLE acct", sm.catalog()).is_err()); // unknown name
        // Re-create with the freed name.
        assert!(matches!(
            run(&mut sm, 4, "CREATE TABLE acct (owner U32 NOT NULL)"),
            OpResult::TypeCreated(_)
        ));
    }

    /// SP74: DROP INDEX removes the index(es) and their entries but the
    /// answer to every query is unchanged (the planner falls back to a
    /// verified scan); it is idempotent-clean, re-creatable, and
    /// deterministic across runs.
    #[test]
    fn drop_index_keeps_results_and_is_deterministic() {
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            run(&mut sm, 1, "CREATE TABLE t (k U32 NOT NULL, v I64 NOT NULL)");
            run(&mut sm, 2, "CREATE INDEX ON t (k)");
            run(&mut sm, 3, "CREATE RANGE INDEX ON t (v)");
            run(&mut sm, 4, "CREATE INDEX ON t (k, v)"); // composite
            for i in 0..40u64 {
                run(
                    &mut sm,
                    10 + i,
                    &format!(
                        "INSERT INTO t (id, k, v) VALUES ({i}, {}, {})",
                        i % 5,
                        (i as i64) - 20
                    ),
                );
            }
            sm
        };
        let queries = [
            "SELECT * FROM t WHERE k = 3",
            "SELECT * FROM t WHERE v >= -5 AND v <= 5",
            "SELECT * FROM t WHERE k = 2 AND v = -18",
            "SELECT COUNT(*) FROM t WHERE k = 1",
        ];
        let mut sm = build();
        // Results WITH every index.
        let before: Vec<OpResult> = queries
            .iter()
            .enumerate()
            .map(|(i, q)| run(&mut sm, 100 + i as u64, q))
            .collect();

        // Drop all three indexes via SQL.
        assert_eq!(run(&mut sm, 200, "DROP INDEX ON t (k)"), OpResult::Ok);
        assert_eq!(run(&mut sm, 201, "DROP INDEX ON t (v)"), OpResult::Ok);
        assert_eq!(run(&mut sm, 202, "DROP INDEX ON t (k, v)"), OpResult::Ok);
        // Catalog reflects the drops.
        {
            let ot = sm.catalog().get(1).unwrap();
            assert!(ot.indexes.is_empty() && ot.ordered.is_empty());
            assert!(ot.composite.iter().all(|c| c.is_empty()));
        }
        // Dropping again ⇒ clean NotFound (not a crash, not Ok).
        assert_eq!(run(&mut sm, 203, "DROP INDEX ON t (k)"), OpResult::NotFound);

        // Same queries, identical answers — only un-accelerated now.
        let after: Vec<OpResult> = queries
            .iter()
            .enumerate()
            .map(|(i, q)| run(&mut sm, 300 + i as u64, q))
            .collect();
        assert_eq!(before, after, "DROP INDEX must not change any result");

        // Re-create one and it still answers correctly.
        assert_eq!(run(&mut sm, 400, "CREATE INDEX ON t (k)"), OpResult::Ok);
        assert_eq!(run(&mut sm, 401, queries[0]), before[0]);

        // Deterministic: a second identical history yields the same digest.
        let mut a = build();
        let mut b = build();
        for (i, op) in [200u64, 201, 202].iter().enumerate() {
            run(&mut a, *op, ["DROP INDEX ON t (k)", "DROP INDEX ON t (v)", "DROP INDEX ON t (k, v)"][i]);
            run(&mut b, *op, ["DROP INDEX ON t (k)", "DROP INDEX ON t (v)", "DROP INDEX ON t (k, v)"][i]);
        }
        assert_eq!(a.digest(), b.digest(), "DROP INDEX must be deterministic");
    }

    /// SP75: ALTER TABLE DROP COLUMN physically removes the column
    /// (re-encodes rows, shrinks schema, drops its indexes) with
    /// surviving data intact and nothing downstream special-cased;
    /// RENAME COLUMN is catalog-only; both deterministic; guards hold.
    #[test]
    fn alter_drop_and_rename_column() {
        let cols = |sm: &StateMachine<MemVfs>| -> Vec<String> {
            let ot = sm.catalog().get(1).unwrap();
            ot.fields.iter().map(|f| f.name.clone()).collect()
        };
        let scalar = |sm: &mut StateMachine<MemVfs>, op, q: &str| -> i128 {
            match run(sm, op, q) {
                OpResult::Got(b) => i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()),
                o => panic!("{o:?}"),
            }
        };
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            run(&mut sm, 1, "CREATE TABLE t (a U32 NOT NULL, b I64 NOT NULL, c U32 NOT NULL)");
            run(&mut sm, 2, "CREATE INDEX ON t (a)");
            run(&mut sm, 3, "CREATE RANGE INDEX ON t (b)");
            run(&mut sm, 4, "CREATE INDEX ON t (a, c)"); // composite incl. c
            for i in 0..30u64 {
                run(
                    &mut sm,
                    10 + i,
                    &format!(
                        "INSERT INTO t (id, a, b, c) VALUES ({i}, {}, {}, {})",
                        i % 4,
                        i as i64 - 10,
                        i * 100
                    ),
                );
            }
            sm
        };
        let mut sm = build();
        let sum_a_before = scalar(&mut sm, 100, "SELECT SUM(a) FROM t");
        let by_b_before = scalar(
            &mut sm,
            101,
            "SELECT COUNT(*) FROM t WHERE b >= -5 AND b <= 5",
        );

        // RENAME b -> bal: catalog-only, indexes keyed by field id.
        assert_eq!(
            run(&mut sm, 200, "ALTER TABLE t RENAME COLUMN b TO bal"),
            OpResult::Ok
        );
        assert_eq!(cols(&sm), ["a", "bal", "c"]);
        assert!(compile("SELECT * FROM t WHERE b = 1", sm.catalog()).is_err());
        // Range index still works under the new name (same field id).
        assert_eq!(
            scalar(&mut sm, 201, "SELECT COUNT(*) FROM t WHERE bal >= -5 AND bal <= 5"),
            by_b_before
        );

        // DROP COLUMN c: physically removed, schema shrinks, surviving
        // data intact, composite (a,c) emptied, c's lookups gone.
        assert_eq!(
            run(&mut sm, 210, "ALTER TABLE t DROP COLUMN c"),
            OpResult::Ok
        );
        assert_eq!(cols(&sm), ["a", "bal"]);
        assert_eq!(
            scalar(&mut sm, 211, "SELECT SUM(a) FROM t"),
            sum_a_before,
            "surviving column data must be intact after re-encode"
        );
        assert_eq!(
            scalar(&mut sm, 212, "SELECT COUNT(*) FROM t WHERE bal >= -5 AND bal <= 5"),
            by_b_before,
            "untouched index stays correct after DROP COLUMN"
        );
        assert!(compile("SELECT * FROM t WHERE c = 1", sm.catalog()).is_err());
        {
            let ot = sm.catalog().get(1).unwrap();
            assert!(ot.composite.iter().all(|x| x.is_empty()), "composite with c emptied");
            assert!(ot.fields.iter().all(|f| f.name != "c"));
        }
        // Re-add a column then it's usable (schema truly mutable).
        assert_eq!(
            run(&mut sm, 220, "ALTER TABLE t ADD COLUMN note U32"),
            OpResult::Ok
        );
        assert_eq!(cols(&sm), ["a", "bal", "note"]);

        // Guards. Unknown column is rejected at compile (names are
        // resolved in the parser).
        assert!(compile("ALTER TABLE t DROP COLUMN nope", sm.catalog()).is_err());
        // A table must keep at least one column (use DROP TABLE instead).
        let mut g = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut g, 1, "CREATE TABLE one (x U32 NOT NULL)");
        assert!(
            matches!(
                run(&mut g, 2, "ALTER TABLE one DROP COLUMN x"),
                OpResult::SchemaError(_)
            ),
            "dropping the last column must be refused"
        );

        // Determinism: identical histories ⇒ identical digest.
        let mut a = build();
        let mut b = build();
        for (op, q) in [
            (200u64, "ALTER TABLE t RENAME COLUMN b TO bal"),
            (210, "ALTER TABLE t DROP COLUMN c"),
        ] {
            run(&mut a, op, q);
            run(&mut b, op, q);
        }
        assert_eq!(a.digest(), b.digest(), "destructive ALTER must be deterministic");
    }

    /// SP77: a balance guard is a named `col >= 0` invariant enforced
    /// on every write (incl. inside a transaction), validates existing
    /// rows when added, requires a signed numeric column, and is
    /// deterministic.
    #[test]
    fn balance_guard_enforces_non_negative() {
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            run(&mut sm, 1, "CREATE TABLE acct (bal I64 NOT NULL)");
            run(&mut sm, 2, "INSERT INTO acct (id, bal) VALUES (1, 100)");
            run(&mut sm, 3, "ALTER TABLE acct ADD BALANCE GUARD bal");
            sm
        };
        let mut sm = build();
        let ot = sm.catalog().get(1).unwrap().clone();
        let bal_rec = |v: i128| {
            kessel_codec::encode(&ot, &[kessel_codec::Value::Int(v)]).unwrap()
        };
        let upd = |s: &mut StateMachine<MemVfs>, op: u64, v: i128| {
            s.apply(
                op,
                kessel_proto::Op::Update {
                    type_id: 1,
                    id: kessel_proto::ObjectId::from_u128(1),
                    record: bal_rec(v),
                },
            )
        };
        // Within the guard: fine (INSERT via SQL, UPDATE via engine —
        // SQL UPDATE is a server-side RMW, out of the compile path).
        assert_eq!(
            run(&mut sm, 10, "INSERT INTO acct (id, bal) VALUES (2, 0)"),
            OpResult::Ok
        );
        assert_eq!(upd(&mut sm, 11, 5), OpResult::Ok);
        // Negative INSERT and UPDATE are rejected (no effect).
        assert!(matches!(
            run(&mut sm, 12, "INSERT INTO acct (id, bal) VALUES (3, -1)"),
            OpResult::Constraint(_)
        ));
        assert!(matches!(upd(&mut sm, 13, -7), OpResult::Constraint(_)));
        // The rejected update had no effect (row 1 still bal = 5).
        assert_eq!(
            sm.apply(
                14,
                kessel_proto::Op::GetById {
                    type_id: 1,
                    id: kessel_proto::ObjectId::from_u128(1)
                }
            ),
            OpResult::Got(bal_rec(5).into())
        );

        // Adding the guard when a current row already violates it fails
        // (and the guard is not installed).
        let mut bad = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut bad, 1, "CREATE TABLE a (bal I64 NOT NULL)");
        run(&mut bad, 2, "INSERT INTO a (id, bal) VALUES (1, -3)");
        assert!(matches!(
            run(&mut bad, 3, "ALTER TABLE a ADD BALANCE GUARD bal"),
            OpResult::Constraint(_)
        ));
        assert_eq!(
            run(&mut bad, 4, "INSERT INTO a (id, bal) VALUES (2, -9)"),
            OpResult::Ok,
            "guard must NOT have been installed after the failed add"
        );

        // Signed-column requirement: a guard on an unsigned column is
        // refused (it would be vacuously true).
        let mut u = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut u, 1, "CREATE TABLE w (n U32 NOT NULL)");
        assert!(matches!(
            run(&mut u, 2, "ALTER TABLE w ADD BALANCE GUARD n"),
            OpResult::SchemaError(_)
        ));

        // Enforced atomically inside a transaction: one negative member
        // rolls the whole batch back (Op::Txn is the engine-level form;
        // BEGIN/COMMIT are a server-connection concern).
        let mut t = build();
        let ins = |s: &mut StateMachine<MemVfs>, q: &str| {
            compile(q, s.catalog()).expect("compile")
        };
        let o1 = ins(&mut t, "INSERT INTO acct (id, bal) VALUES (50, 10)");
        let o2 = ins(&mut t, "INSERT INTO acct (id, bal) VALUES (51, -2)");
        assert_ne!(
            t.apply(20, kessel_proto::Op::Txn { ops: vec![o1, o2] }),
            OpResult::Ok
        );
        assert!(
            matches!(
                run(&mut t, 24, "SELECT * FROM acct ID 50"),
                OpResult::NotFound
            ),
            "a balance-guard violation must roll back the whole txn"
        );

        // Deterministic.
        let a = build();
        let b = build();
        assert_eq!(a.digest(), b.digest(), "balance guard must be deterministic");
    }

    #[test]
    fn end_to_end_sql() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        assert!(matches!(
            run(&mut sm, 1, "CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)"),
            OpResult::TypeCreated(1)
        ));
        assert_eq!(run(&mut sm, 2, "INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)"), OpResult::Ok);
        assert_eq!(run(&mut sm, 3, "INSERT INTO acct ID 2 (owner, bal) VALUES (100, 999)"), OpResult::Ok);
        assert_eq!(run(&mut sm, 4, "INSERT INTO acct ID 3 (owner, bal) VALUES (200, 7)"), OpResult::Ok);

        // SELECT COUNT(*) WHERE owner = 100  -> 2
        match run(&mut sm, 5, "SELECT COUNT(*) FROM acct WHERE owner = 100") {
            OpResult::Got(b) => assert_eq!(i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()), 2),
            o => panic!("{o:?}"),
        }
        // SELECT SUM(bal) WHERE owner = 100  -> 1049
        match run(&mut sm, 6, "SELECT SUM(bal) FROM acct WHERE owner = 100") {
            OpResult::Got(b) => assert_eq!(i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()), 1049),
            o => panic!("{o:?}"),
        }
        // SELECT * WHERE bal >= 50 AND owner = 100  -> 2 rows
        match run(&mut sm, 7, "SELECT * FROM acct WHERE bal >= 50 AND owner = 100") {
            OpResult::Got(b) => {
                let mut p = 0;
                let mut n = 0;
                while p + 4 <= b.len() {
                    let l = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                    p += 4 + l;
                    n += 1;
                }
                assert_eq!(n, 2);
            }
            o => panic!("{o:?}"),
        }
        // ORDER BY bal DESC LIMIT 1 -> the 999 row
        match run(&mut sm, 8, "SELECT * FROM acct ORDER BY bal DESC LIMIT 1") {
            OpResult::Got(b) => {
                assert!(b.len() > 4);
                let l = u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize;
                // exactly one row returned
                assert_eq!(4 + l, b.len());
            }
            o => panic!("{o:?}"),
        }
        // DELETE then COUNT
        assert_eq!(run(&mut sm, 9, "DELETE FROM acct ID 3"), OpResult::Ok);
        match run(&mut sm, 10, "SELECT COUNT(*) FROM acct WHERE owner >= 0") {
            OpResult::Got(b) => assert_eq!(i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()), 2),
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn select_columns_only_matches_plain_projection() {
        assert_eq!(
            select_columns("SELECT owner, bal FROM acct"),
            Some(("acct".into(), vec!["owner".into(), "bal".into()]))
        );
        assert_eq!(
            select_columns("select a FROM t WHERE a = 1"),
            Some(("t".into(), vec!["a".into()]))
        );
        // Not plain projections:
        assert_eq!(select_columns("SELECT * FROM acct"), None);
        assert_eq!(select_columns("SELECT COUNT(*) FROM acct"), None);
        assert_eq!(select_columns("SELECT a, b FROM x JOIN y ON x.a = y.b"), None);
        assert_eq!(select_columns("DESCRIBE acct"), None);
        assert_eq!(select_columns("INSERT INTO t (id) VALUES (1)"), None);
    }

    #[test]
    fn multi_row_insert_is_atomic() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        assert!(matches!(
            run(&mut sm, 1, "CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)"),
            OpResult::TypeCreated(1)
        ));
        // legacy single-row form still works (back-compat)
        assert_eq!(
            run(&mut sm, 2, "INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)"),
            OpResult::Ok
        );
        // new id-column single-row form
        assert_eq!(
            run(&mut sm, 3, "INSERT INTO acct (id, owner, bal) VALUES (2, 100, 60)"),
            OpResult::Ok
        );
        // multi-row: one statement, all rows land atomically
        assert_eq!(
            run(
                &mut sm,
                4,
                "INSERT INTO acct (id, owner, bal) VALUES (3, 7, 1), (4, 7, 2), (5, 7, 3)"
            ),
            OpResult::Ok
        );
        let cnt = |sm: &mut StateMachine<MemVfs>, op, q: &str| -> i128 {
            match run(sm, op, q) {
                OpResult::Got(b) => i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()),
                o => panic!("{q}: {o:?}"),
            }
        };
        assert_eq!(cnt(&mut sm, 5, "SELECT COUNT(*) FROM acct"), 5);
        assert_eq!(cnt(&mut sm, 6, "SELECT COUNT(*) FROM acct WHERE owner = 7"), 3);

        // Atomicity: a duplicate id inside the batch rejects the WHOLE
        // statement — none of its rows are inserted.
        let r = run(
            &mut sm,
            7,
            "INSERT INTO acct (id, owner, bal) VALUES (9, 1, 1), (3, 1, 1)",
        ); // id 3 already exists
        assert_ne!(r, OpResult::Ok, "batch with a dup id must not commit");
        assert_eq!(
            cnt(&mut sm, 8, "SELECT COUNT(*) FROM acct"),
            5,
            "failed batch must insert nothing (id 9 rolled back too)"
        );

        // Missing row id is a clean error.
        assert!(compile(
            "INSERT INTO acct (owner, bal) VALUES (1, 2)",
            sm.catalog()
        )
        .is_err());
    }

    #[test]
    fn explain_shows_the_plan() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a U32 NOT NULL, b U32 NOT NULL, v I64 NOT NULL)");
        run(&mut sm, 2, "CREATE INDEX ON t (a)");
        run(&mut sm, 3, "CREATE INDEX ON t (a, b)"); // composite
        let cat = sm.catalog().clone();
        let plan = |q: &str| match compile_stmt(q, &cat).expect("compile") {
            Stmt::Explain(s) => s,
            o => panic!("expected Explain, got non-explain ({:?})", std::mem::discriminant(&o)),
        };
        // single-column index narrowing
        let p1 = plan("EXPLAIN SELECT * FROM t WHERE a = 1 AND v > 5");
        assert!(p1.contains("Index Scan") && p1.contains("t"), "{p1}");
        // composite index
        let p2 = plan("EXPLAIN SELECT * FROM t WHERE a = 1 AND b = 2");
        assert!(p2.to_lowercase().contains("composite"), "{p2}");
        // OR ⇒ no usable index ⇒ seq scan
        let p3 = plan("EXPLAIN SELECT * FROM t WHERE a = 1 OR v = 2");
        assert!(p3.contains("Seq Scan"), "{p3}");
        // primary-key fast path
        let p4 = plan("EXPLAIN SELECT * FROM t ID 7");
        assert!(p4.contains("Primary-Key Lookup"), "{p4}");
        // DDL / write plans
        assert!(plan("EXPLAIN CREATE TABLE z (x U8 NOT NULL)").contains("Create Table"));
        assert!(plan("EXPLAIN INSERT INTO t (id,a,b,v) VALUES (1,1,1,1)")
            .to_lowercase()
            .contains("insert"));
        // case-insensitive keyword; nothing is executed (table z absent)
        assert!(matches!(
            compile_stmt("explain SELECT * FROM t WHERE a = 1", &cat),
            Ok(Stmt::Explain(_))
        ));
        assert!(compile_stmt("EXPLAIN", &cat).is_err());
    }

    #[test]
    fn alter_table_add_column() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        assert!(matches!(
            run(&mut sm, 1, "CREATE TABLE t (a U64 NOT NULL)"),
            OpResult::TypeCreated(1)
        ));
        assert_eq!(run(&mut sm, 2, "INSERT INTO t (id, a) VALUES (1, 10)"), OpResult::Ok);

        // Online ADD COLUMN (must be nullable) — no lock, existing rows
        // up-project the new column as NULL.
        assert_eq!(run(&mut sm, 3, "ALTER TABLE t ADD COLUMN note I64"), OpResult::Ok);
        assert_eq!(
            run(&mut sm, 4, "ALTER TABLE t ADD tag U16"), // COLUMN optional
            OpResult::Ok
        );
        // New schema is visible: insert using the new columns.
        assert_eq!(
            run(&mut sm, 5, "INSERT INTO t (id, a, note, tag) VALUES (2, 20, 7, 9)"),
            OpResult::Ok
        );
        let cnt = |sm: &mut StateMachine<MemVfs>, op, q: &str| -> i128 {
            match run(sm, op, q) {
                OpResult::Got(b) => i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()),
                o => panic!("{q}: {o:?}"),
            }
        };
        assert_eq!(cnt(&mut sm, 6, "SELECT COUNT(*) FROM t"), 2);
        // The old row reads back with note = NULL (up-projected).
        assert_eq!(cnt(&mut sm, 7, "SELECT COUNT(*) FROM t WHERE note IS NULL"), 1);
        assert_eq!(cnt(&mut sm, 8, "SELECT COUNT(*) FROM t WHERE note = 7"), 1);

        // The online-DDL rule: a NOT NULL add is rejected by the engine.
        assert!(matches!(
            run(&mut sm, 9, "ALTER TABLE t ADD COLUMN bad U32 NOT NULL"),
            OpResult::SchemaError(_)
        ));
        // Unknown table -> compile error.
        assert!(compile("ALTER TABLE nope ADD COLUMN x U8", sm.catalog()).is_err());
    }

    #[test]
    fn like_predicate() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        assert!(matches!(
            run(&mut sm, 1, "CREATE TABLE u (name CHAR(16) NOT NULL)"),
            OpResult::TypeCreated(1)
        ));
        for (i, n) in ["Alice", "Albert", "Bob", "Alicia"].iter().enumerate() {
            assert_eq!(
                run(
                    &mut sm,
                    (i + 2) as u64,
                    &format!("INSERT INTO u (id, name) VALUES ({}, '{n}')", i + 1)
                ),
                OpResult::Ok
            );
        }
        let cnt = |sm: &mut StateMachine<MemVfs>, op, q: &str| -> i128 {
            match run(sm, op, q) {
                OpResult::Got(b) => i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()),
                o => panic!("{q}: {o:?}"),
            }
        };
        assert_eq!(cnt(&mut sm, 10, "SELECT COUNT(*) FROM u WHERE name LIKE 'Al%'"), 3);
        assert_eq!(
            cnt(&mut sm, 11, "SELECT COUNT(*) FROM u WHERE name LIKE 'Alic_'"),
            1 // Alice (Alicia is 6 chars)
        );
        assert_eq!(
            cnt(&mut sm, 12, "SELECT COUNT(*) FROM u WHERE name LIKE '%b%'"),
            2 // Albert, Bob
        );
        assert_eq!(
            cnt(&mut sm, 13, "SELECT COUNT(*) FROM u WHERE name NOT LIKE 'Al%'"),
            1 // Bob
        );
        assert_eq!(
            cnt(
                &mut sm,
                14,
                "SELECT COUNT(*) FROM u WHERE name LIKE 'A%' AND name LIKE '%e'"
            ),
            1 // Alice
        );
    }

    #[test]
    fn is_null_predicate() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        assert!(matches!(
            run(&mut sm, 1, "CREATE TABLE t (a U64 NOT NULL, note I64)"),
            OpResult::TypeCreated(1)
        ));
        // row 1: `note` omitted -> NULL.  row 2: note = 7.
        assert_eq!(run(&mut sm, 2, "INSERT INTO t ID 1 (a) VALUES (10)"), OpResult::Ok);
        assert_eq!(
            run(&mut sm, 3, "INSERT INTO t ID 2 (a, note) VALUES (20, 7)"),
            OpResult::Ok
        );
        let cnt = |sm: &mut StateMachine<MemVfs>, op, q: &str| -> i128 {
            match run(sm, op, q) {
                OpResult::Got(b) => i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()),
                o => panic!("{q}: {o:?}"),
            }
        };
        assert_eq!(cnt(&mut sm, 4, "SELECT COUNT(*) FROM t WHERE note IS NULL"), 1);
        assert_eq!(
            cnt(&mut sm, 5, "SELECT COUNT(*) FROM t WHERE note IS NOT NULL"),
            1
        );
        // composes with other predicates
        assert_eq!(
            cnt(
                &mut sm,
                6,
                "SELECT COUNT(*) FROM t WHERE a >= 0 AND note IS NULL"
            ),
            1
        );
        assert_eq!(
            cnt(
                &mut sm,
                7,
                "SELECT COUNT(*) FROM t WHERE note IS NULL OR note IS NOT NULL"
            ),
            2
        );
        // a non-column LHS is a clean error, not a panic
        assert!(compile("SELECT * FROM t WHERE 5 IS NULL", sm.catalog()).is_err());
    }

    #[test]
    fn in_and_between_predicates() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        assert!(matches!(
            run(&mut sm, 1, "CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)"),
            OpResult::TypeCreated(1)
        ));
        for (i, (o, b)) in
            [(10u32, 5i64), (20, 15), (30, 25), (40, 35)].iter().enumerate()
        {
            assert_eq!(
                run(
                    &mut sm,
                    (i + 2) as u64,
                    &format!("INSERT INTO acct ID {} (owner, bal) VALUES ({o}, {b})", i + 1)
                ),
                OpResult::Ok
            );
        }
        let cnt = |sm: &mut StateMachine<MemVfs>, op, q: &str| -> i128 {
            match run(sm, op, q) {
                OpResult::Got(b) => i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()),
                o => panic!("{q}: {o:?}"),
            }
        };
        // IN — owner in {10,30,99} -> rows 10 and 30
        assert_eq!(
            cnt(&mut sm, 20, "SELECT COUNT(*) FROM acct WHERE owner IN (10, 30, 99)"),
            2
        );
        // NOT IN — exclude {10,30} -> 20 and 40
        assert_eq!(
            cnt(&mut sm, 21, "SELECT COUNT(*) FROM acct WHERE owner NOT IN (10, 30)"),
            2
        );
        // BETWEEN — bal in [15,35] -> 15,25,35 = 3 rows
        assert_eq!(
            cnt(&mut sm, 22, "SELECT COUNT(*) FROM acct WHERE bal BETWEEN 15 AND 35"),
            3
        );
        // NOT BETWEEN — bal outside [15,35] -> just bal=5
        assert_eq!(
            cnt(&mut sm, 23, "SELECT COUNT(*) FROM acct WHERE bal NOT BETWEEN 15 AND 35"),
            1
        );
        // composed with AND/OR — still works
        assert_eq!(
            cnt(
                &mut sm,
                24,
                "SELECT COUNT(*) FROM acct WHERE owner IN (10, 20) AND bal BETWEEN 0 AND 10"
            ),
            1
        );
    }

    #[test]
    fn select_star_eq_compiles_to_query_rows_and_is_correct() {
        use kessel_proto::Op;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE rec (owner U32 NOT NULL, v U32 NOT NULL)");
        // index on owner (field_id 1) so QueryRows narrows via the index
        assert_eq!(sm.apply(2, Op::CreateIndex { type_id: 1, field_id: 1 }), OpResult::Ok);
        for i in 0..10u64 {
            run(
                &mut sm,
                3 + i,
                &format!(
                    "INSERT INTO rec ID {i} (owner, v) VALUES ({}, {i})",
                    if i < 4 { 100 } else { 200 }
                ),
            );
        }
        // restricted grammar -> QueryRows
        let cat = sm.catalog().clone();
        match compile("SELECT * FROM rec WHERE owner = 100", &cat).unwrap() {
            Op::QueryRows { eq_preds, .. } => assert_eq!(eq_preds.len(), 1),
            o => panic!("expected QueryRows, got {o:?}"),
        }
        // SP62: an OR query still plans to QueryRows (full verifying
        // program), but with NO equality hints — `owner = 100` is not a
        // mandatory conjunct under OR, so using it to narrow candidates
        // would be unsound. Empty `eq_preds` == a verified full scan:
        // correct, just not index-accelerated (proven by the oracle).
        match compile("SELECT * FROM rec WHERE owner = 100 OR v = 1", &cat).unwrap() {
            Op::QueryRows { eq_preds, .. } => {
                assert!(eq_preds.is_empty(), "no hints allowed under OR")
            }
            o => panic!("expected QueryRows (no hints) for OR, got {o:?}"),
        }
        // correctness: indexed query returns exactly the 4 owner=100 rows
        match run(&mut sm, 20, "SELECT * FROM rec WHERE owner = 100") {
            OpResult::Got(b) => {
                let mut p = 0;
                let mut n = 0;
                while p + 4 <= b.len() {
                    let l = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                    p += 4 + l;
                    n += 1;
                }
                assert_eq!(n, 4);
            }
            o => panic!("{o:?}"),
        }
        // multi-eq AND, and the fallback Select must agree with QueryRows
        let q = run(&mut sm, 21, "SELECT * FROM rec WHERE owner = 200 AND v = 7");
        assert!(matches!(&q, OpResult::Got(b) if b.len() > 0));
    }

    #[test]
    fn create_index_ddl_all_forms() {
        use kessel_proto::Op;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a U32 NOT NULL, b U32 NOT NULL, c I64 NOT NULL)");
        let cat = sm.catalog().clone();
        assert!(matches!(
            compile("CREATE INDEX ON t (a)", &cat).unwrap(),
            Op::CreateIndex { field_id: 1, .. }
        ));
        assert!(matches!(
            compile("CREATE UNIQUE INDEX ON t (b)", &cat).unwrap(),
            Op::AddUnique { field_id: 2, .. }
        ));
        assert!(matches!(
            compile("CREATE RANGE INDEX ON t (c)", &cat).unwrap(),
            Op::AddOrderedIndex { field_id: 3, .. }
        ));
        match compile("CREATE INDEX ON t (a, b)", &cat).unwrap() {
            Op::AddCompositeIndex { fields, .. } => assert_eq!(fields, vec![1, 2]),
            o => panic!("{o:?}"),
        }
        assert!(compile("CREATE UNIQUE INDEX ON t (a, b)", &cat).is_err());
        // CREATE TABLE still works (not mistaken for an index)
        assert!(matches!(
            compile("CREATE TABLE z (x U8 NOT NULL)", &Catalog::default()).unwrap(),
            Op::CreateType { .. }
        ));

        // end-to-end: pure-SQL index then index-accelerated query
        assert_eq!(run(&mut sm, 2, "CREATE INDEX ON t (a)"), OpResult::Ok);
        for i in 0..6u64 {
            run(&mut sm, 3 + i, &format!("INSERT INTO t ID {i} (a, b, c) VALUES ({}, {i}, {i})", if i < 3 { 7 } else { 8 }));
        }
        let cat2 = sm.catalog().clone();
        assert!(matches!(
            compile("SELECT * FROM t WHERE a = 7", &cat2).unwrap(),
            Op::QueryRows { .. }
        ));
        match run(&mut sm, 20, "SELECT * FROM t WHERE a = 7") {
            OpResult::Got(b) => {
                let mut p = 0;
                let mut n = 0;
                while p + 4 <= b.len() {
                    let l = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                    p += 4 + l;
                    n += 1;
                }
                assert_eq!(n, 3);
            }
            o => panic!("{o:?}"),
        }
    }

    /// SP-PG-SQL-MULTI-JOIN: a chained 3-way `JOIN … JOIN …` compiles to one
    /// `Op::Join` with TWO entries in `extra_joins` (the base join + 1 step is
    /// the 3-table chain; here 3 JOINs ⇒ base + 2 steps ⇒ 4 tables). Verifies
    /// the step ON columns resolve to the right combined / new-table field ids.
    #[test]
    fn multi_join_compiles_to_extra_joins() {
        use kessel_proto::Op;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE users (id U64 NOT NULL, name CHAR(16) NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE posts (id U64 NOT NULL, user_id U64 NOT NULL, title CHAR(16) NOT NULL)");
        run(&mut sm, 3, "CREATE TABLE comments (id U64 NOT NULL, post_id U64 NOT NULL, body CHAR(16) NOT NULL)");
        let cat = sm.catalog().clone();
        let sql = "SELECT users.name, posts.title, comments.body FROM users JOIN posts \
                   ON users.id = posts.user_id JOIN comments ON posts.id = comments.post_id";
        match compile(sql, &cat).unwrap() {
            Op::Join { left_field, right_field, extra_joins, group_aggregate, .. } => {
                // Catalog assigns field_ids starting at 1: users.id=1,
                // posts.user_id=2. Base ON: users.id (1) = posts.user_id (2).
                assert_eq!(left_field, 1);
                assert_eq!(right_field, 2);
                assert!(group_aggregate.is_none());
                assert_eq!(extra_joins.len(), 1, "one chained step (3 tables)");
                let step = &extra_joins[0];
                // COMBINED schema reassigns ids 0..n: users.id=0, users.name=1,
                // posts.id=2 ⇒ left_combined_field 2. comments.post_id is the
                // catalog field 2 in comments.
                assert_eq!(step.left_combined_field, 2, "posts.id combined id");
                assert_eq!(step.right_field, 2, "comments.post_id field id");
            }
            o => panic!("expected Op::Join, got {o:?}"),
        }
        // A bare 2-table JOIN still has EMPTY extra_joins (byte-identical path).
        match compile(
            "SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id",
            &cat,
        ).unwrap() {
            Op::Join { extra_joins, .. } => assert!(extra_joins.is_empty()),
            o => panic!("expected Op::Join, got {o:?}"),
        }
        // GROUP BY over a chain is rejected (named follow-up).
        assert!(compile(
            "SELECT users.name, COUNT(comments.id) FROM users JOIN posts ON users.id = posts.user_id \
             JOIN comments ON posts.id = comments.post_id GROUP BY users.name",
            &cat,
        ).is_err(), "GROUP BY over a multi-join must error");
    }

    /// SP-PG-SQL-JOIN-ALIAS: an aliased join (`FROM users u JOIN posts p ON
    /// u.id = p.user_id`) compiles to the IDENTICAL `Op` as the spelled-out
    /// full-table-name form — the alias is resolved away in the SQL layer, so
    /// the wire `Op` is byte-unchanged (no determinism risk) and back-compat is
    /// proven by Op equality. Covers binary + 3-way + WHERE + ORDER BY + `AS`.
    #[test]
    fn alias_join_compiles_identically_to_full_names() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE users (id U64 NOT NULL, name CHAR(16) NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE posts (id U64 NOT NULL, user_id U64 NOT NULL, title CHAR(16) NOT NULL)");
        run(&mut sm, 3, "CREATE TABLE comments (id U64 NOT NULL, post_id U64 NOT NULL, body CHAR(16) NOT NULL)");
        let cat = sm.catalog().clone();

        // 1. Binary join: implicit alias == full names == `AS` alias.
        let full = compile(
            "SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id",
            &cat,
        ).unwrap();
        let aliased = compile(
            "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id",
            &cat,
        ).unwrap();
        let as_form = compile(
            "SELECT u.name, p.title FROM users AS u JOIN posts AS p ON u.id = p.user_id",
            &cat,
        ).unwrap();
        assert_eq!(full, aliased, "implicit alias must compile identically");
        assert_eq!(full, as_form, "AS-alias must compile identically");

        // 2. Mixed: alias on one table, full name on the other.
        let mixed = compile(
            "SELECT u.name, posts.title FROM users u JOIN posts ON u.id = posts.user_id",
            &cat,
        ).unwrap();
        assert_eq!(full, mixed, "mixed alias/full-name must compile identically");

        // 3. 3-way aliased chain == full-name chain.
        let full3 = compile(
            "SELECT users.name, posts.title, comments.body FROM users JOIN posts \
             ON users.id = posts.user_id JOIN comments ON posts.id = comments.post_id",
            &cat,
        ).unwrap();
        let aliased3 = compile(
            "SELECT u.name, p.title, c.body FROM users u JOIN posts p \
             ON u.id = p.user_id JOIN comments c ON p.id = c.post_id",
            &cat,
        ).unwrap();
        assert_eq!(full3, aliased3, "3-way aliased chain must compile identically");

        // 4. Aliased WHERE + ORDER BY == full-name WHERE + ORDER BY.
        let fullw = compile(
            "SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id \
             WHERE users.id = 1 ORDER BY posts.title",
            &cat,
        ).unwrap();
        let aliasedw = compile(
            "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id \
             WHERE u.id = 1 ORDER BY p.title",
            &cat,
        ).unwrap();
        assert_eq!(fullw, aliasedw, "aliased WHERE+ORDER BY must compile identically");

        // 5. Errors at compile: an unknown ON qualifier, a duplicate alias, and
        // an alias shadowing another table's name all error cleanly rather than
        // mis-resolving. (A bad *projection* qualifier is caught at the gateway
        // `join_projection`/`render_join_result` layer, since the engine's
        // `Op::Join` discards the projection — verified in
        // `join_projection_resolves_aliases`.)
        assert!(compile(
            "SELECT u.name, p.title FROM users u JOIN posts p ON x.id = p.user_id",
            &cat,
        ).is_err(), "unknown ON qualifier must error, not mis-resolve");
        assert!(compile(
            "SELECT u.name FROM users u JOIN posts u ON u.id = u.user_id",
            &cat,
        ).is_err(), "duplicate alias must error");
        assert!(compile(
            "SELECT users.name FROM users u JOIN posts users ON u.id = users.user_id",
            &cat,
        ).is_err(), "alias shadowing another table's name must error");
    }

    /// SP-PG-SQL-JOIN-ALIAS: the gateway `join_projection` text helper resolves
    /// alias qualifiers to FULL table names so they match the `KTR1` combined
    /// schema (`<table>.<col>`). Full-name + `AS` forms resolve too.
    #[test]
    fn join_projection_resolves_aliases() {
        // Implicit alias → full table name in the JoinProjCol qualifier.
        let (cols, star) = join_projection(
            "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id",
        ).unwrap();
        assert!(!star);
        assert_eq!(cols, vec![
            JoinProjCol { qualifier: Some("users".into()), column: "name".into() },
            JoinProjCol { qualifier: Some("posts".into()), column: "title".into() },
        ]);
        // AS form resolves the same.
        let (cols2, _) = join_projection(
            "SELECT u.name, p.title FROM users AS u JOIN posts AS p ON u.id = p.user_id",
        ).unwrap();
        assert_eq!(cols, cols2);
        // 3-way aliased chain.
        let (cols3, _) = join_projection(
            "SELECT u.name, p.title, c.body FROM users u JOIN posts p ON u.id = p.user_id \
             JOIN comments c ON p.id = c.post_id",
        ).unwrap();
        assert_eq!(cols3, vec![
            JoinProjCol { qualifier: Some("users".into()), column: "name".into() },
            JoinProjCol { qualifier: Some("posts".into()), column: "title".into() },
            JoinProjCol { qualifier: Some("comments".into()), column: "body".into() },
        ]);
        // Full table names still resolve to themselves (back-compat).
        let (colsf, _) = join_projection(
            "SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id",
        ).unwrap();
        assert_eq!(colsf, vec![
            JoinProjCol { qualifier: Some("users".into()), column: "name".into() },
            JoinProjCol { qualifier: Some("posts".into()), column: "title".into() },
        ]);
        // SELECT * stays a star projection.
        let (_c, star2) = join_projection(
            "SELECT * FROM users u JOIN posts p ON u.id = p.user_id",
        ).unwrap();
        assert!(star2);
        // An unknown projection qualifier ⇒ None (the gateway then renders the
        // standard 42703 column-does-not-exist error rather than mis-resolving).
        assert!(join_projection(
            "SELECT x.name FROM users u JOIN posts p ON u.id = p.user_id",
        ).is_none(), "unknown projection qualifier must not resolve");
        // A duplicate alias ⇒ None (clean rejection, not a mis-render).
        assert!(join_projection(
            "SELECT u.name FROM users u JOIN posts u ON u.id = u.user_id",
        ).is_none(), "duplicate alias must not resolve");
    }

    #[test]
    fn describe_lets_client_decode_rows() {
        use kessel_catalog::decode_type_def;
        use kessel_codec::{decode, Value};
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)");
        run(&mut sm, 2, "INSERT INTO acct ID 1 (owner, bal) VALUES (100, -7)");
        // DESCRIBE -> serialized (name, fields); a client rebuilds the type
        let def = match run(&mut sm, 3, "DESCRIBE acct") {
            OpResult::Got(b) => b,
            o => panic!("{o:?}"),
        };
        let (name, fields) = decode_type_def(&def).unwrap();
        assert_eq!(name, "acct");
        assert_eq!(fields.len(), 2);
        let ot = kessel_catalog::ObjectType {
            type_id: 1, name, schema_ver: 1, fields,
            indexes: vec![], unique: vec![], fks: vec![], checks: vec![],
            triggers: vec![], ordered: vec![], composite: vec![],
            defaults: vec![],
            serial_pk: false,
            serial_field_id: None,
        };
        // fetch the row and decode it using ONLY the described schema
        match run(&mut sm, 4, "SELECT * FROM acct ID 1") {
            OpResult::Got(rec) => {
                let vals = decode(&ot, &rec).unwrap();
                assert_eq!(vals[0], Value::Uint(100));
                assert_eq!(vals[1], Value::Int(-7));
            }
            o => panic!("{o:?}"),
        }
        assert!(compile("DESC nope", sm.catalog()).is_err());
    }

    #[test]
    fn inner_equi_join() {
        use kessel_proto::Op;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE usr (uid U32 NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE ord (owner U32 NOT NULL, amt U32 NOT NULL)");
        // users 1,2,3 ; orders: owner 1 x2, owner 2 x1, owner 9 (no match)
        run(&mut sm, 3, "INSERT INTO usr ID 1 (uid) VALUES (1)");
        run(&mut sm, 4, "INSERT INTO usr ID 2 (uid) VALUES (2)");
        run(&mut sm, 5, "INSERT INTO usr ID 3 (uid) VALUES (3)");
        run(&mut sm, 6, "INSERT INTO ord ID 10 (owner, amt) VALUES (1, 100)");
        run(&mut sm, 7, "INSERT INTO ord ID 11 (owner, amt) VALUES (1, 200)");
        run(&mut sm, 8, "INSERT INTO ord ID 12 (owner, amt) VALUES (2, 50)");
        run(&mut sm, 9, "INSERT INTO ord ID 13 (owner, amt) VALUES (9, 7)");

        // compiles to Op::Join
        let cat = sm.catalog().clone();
        match compile("SELECT * FROM usr JOIN ord ON usr.uid = ord.owner", &cat).unwrap() {
            Op::Join { left_field, right_field, .. } => {
                assert_eq!((left_field, right_field), (1, 1));
            }
            o => panic!("expected Join, got {o:?}"),
        }
        // execute: SP72 self-describing typed result. The payload is
        // [b"KTR1"][u32 deflen][type def][ [u32 reclen][rec] ]*, and a
        // joined record decodes against the embedded combined schema
        // (left cols `usr.*` then right cols `ord.*`). 3 rows expected
        // (uid1×2 orders + uid2×1).
        match run(&mut sm, 20, "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner") {
            OpResult::Got(b) => {
                assert_eq!(&b[..4], b"KTR1", "typed-result magic");
                let dl = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;
                let (jname, jfields) =
                    kessel_catalog::decode_type_def(&b[8..8 + dl]).unwrap();
                let jot = kessel_catalog::ObjectType::from_def(jname, jfields);
                let names: Vec<&str> =
                    jot.fields.iter().map(|f| f.name.as_str()).collect();
                assert_eq!(names, ["usr.uid", "ord.owner", "ord.amt"]);
                let mut p = 8 + dl;
                let mut rows = 0;
                while p + 4 <= b.len() {
                    let rl = u32::from_le_bytes(
                        b[p..p + 4].try_into().unwrap(),
                    ) as usize;
                    p += 4;
                    // Each record decodes cleanly against the combined
                    // type — i.e. the result is genuinely self-describing.
                    let vals =
                        kessel_codec::decode(&jot, &b[p..p + rl]).unwrap();
                    assert_eq!(vals.len(), 3);
                    p += rl;
                    rows += 1;
                }
                assert_eq!(p, b.len(), "consumed exactly");
                assert_eq!(rows, 3);
            }
            o => panic!("{o:?}"),
        }
        // ON columns may be written in either table order
        assert!(matches!(
            compile("SELECT * FROM usr JOIN ord ON ord.owner = usr.uid", &cat).unwrap(),
            Op::Join { .. }
        ));
        // bad ON columns rejected
        assert!(compile("SELECT * FROM usr JOIN ord ON usr.uid = usr.uid", &cat).is_err());
    }

    /// SP-PG-SQL-JOIN-WHERE — `JOIN … ON … WHERE <pred>` compiles the
    /// predicate against the COMBINED (a++b) schema and emits an Op::Join
    /// carrying a non-empty `filter`, and the engine returns ONLY matching
    /// combined rows. The bare-join path (no WHERE) still emits an empty
    /// filter (regression).
    #[test]
    fn join_where_filters_combined_rows() {
        use kessel_proto::Op;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE usr (uid U32 NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE ord (owner U32 NOT NULL, amt U32 NOT NULL)");
        run(&mut sm, 3, "INSERT INTO usr ID 1 (uid) VALUES (1)");
        run(&mut sm, 4, "INSERT INTO usr ID 2 (uid) VALUES (2)");
        run(&mut sm, 6, "INSERT INTO ord ID 10 (owner, amt) VALUES (1, 100)");
        run(&mut sm, 7, "INSERT INTO ord ID 11 (owner, amt) VALUES (1, 200)");
        run(&mut sm, 8, "INSERT INTO ord ID 12 (owner, amt) VALUES (2, 50)");
        let cat = sm.catalog().clone();

        // Bare join (regression): empty filter.
        match compile("SELECT * FROM usr JOIN ord ON usr.uid = ord.owner", &cat).unwrap() {
            Op::Join { filter, .. } => assert!(filter.is_empty(), "bare join has no filter"),
            o => panic!("expected Join, got {o:?}"),
        }

        // Helper: run a join SQL and count the combined KTR1 rows.
        fn join_rows(sm: &mut StateMachine<MemVfs>, seq: u64, sql: &str) -> usize {
            match run(sm, seq, sql) {
                OpResult::Got(b) => {
                    assert_eq!(&b[..4], b"KTR1");
                    let dl = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;
                    let mut p = 8 + dl;
                    let mut rows = 0;
                    while p + 4 <= b.len() {
                        let rl = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                        p += 4 + rl;
                        rows += 1;
                    }
                    assert_eq!(p, b.len(), "consumed exactly");
                    rows
                }
                o => panic!("{o:?}"),
            }
        }

        // Bare join: 3 combined rows (uid1×2 + uid2×1).
        assert_eq!(
            join_rows(&mut sm, 20, "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner"),
            3
        );
        // Filter on a RIGHT-table column (qualified): only amt=200 → 1 row.
        match compile(
            "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner WHERE ord.amt = 200",
            &cat,
        )
        .unwrap()
        {
            Op::Join { filter, .. } => assert!(!filter.is_empty(), "filtered join carries filter"),
            o => panic!("{o:?}"),
        }
        assert_eq!(
            join_rows(
                &mut sm,
                21,
                "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner WHERE ord.amt = 200"
            ),
            1
        );
        // Filter on a LEFT-table column (qualified): uid=1 → 2 rows.
        assert_eq!(
            join_rows(
                &mut sm,
                22,
                "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner WHERE usr.uid = 1"
            ),
            2
        );
        // AND of a left-col and a right-col predicate → uid=1 AND amt>=200 → 1.
        assert_eq!(
            join_rows(
                &mut sm,
                23,
                "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner \
                 WHERE usr.uid = 1 AND ord.amt >= 200"
            ),
            1
        );
        // Filter matching 0 rows → empty result.
        assert_eq!(
            join_rows(
                &mut sm,
                24,
                "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner WHERE ord.amt = 999"
            ),
            0
        );
        // Bare unambiguous column (no qualifier): `amt` lives only in ord.
        assert_eq!(
            join_rows(
                &mut sm,
                25,
                "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner WHERE amt = 50"
            ),
            1
        );
        // WHERE then LIMIT — the WHERE region stops at LIMIT and LIMIT parses.
        match compile(
            "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner WHERE usr.uid = 1 LIMIT 1",
            &cat,
        )
        .unwrap()
        {
            Op::Join { filter, limit, .. } => {
                assert!(!filter.is_empty());
                assert_eq!(limit, 1);
            }
            o => panic!("{o:?}"),
        }
        // Wrong qualifier (`foo.x` names neither table) → error.
        assert!(compile(
            "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner WHERE foo.amt = 1",
            &cat,
        )
        .is_err());
        // Unknown qualified column → error.
        assert!(compile(
            "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner WHERE ord.nope = 1",
            &cat,
        )
        .is_err());
    }

    /// SP-PG-SQL-JOIN-WHERE — a bare column present in BOTH joined tables is
    /// ambiguous and must error with a qualify-it hint (weak-spot #2).
    #[test]
    fn join_where_bare_ambiguous_column_errors() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        // Both tables carry a `tag` column → bare `tag` is ambiguous.
        run(&mut sm, 1, "CREATE TABLE a (k U32 NOT NULL, tag U32 NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE b (k U32 NOT NULL, tag U32 NOT NULL)");
        let cat = sm.catalog().clone();
        let err = compile(
            "SELECT * FROM a JOIN b ON a.k = b.k WHERE tag = 1",
            &cat,
        )
        .unwrap_err();
        assert!(err.contains("ambiguous"), "got: {err}");
        // Qualified resolves fine.
        assert!(matches!(
            compile("SELECT * FROM a JOIN b ON a.k = b.k WHERE a.tag = 1", &cat).unwrap(),
            kessel_proto::Op::Join { .. }
        ));
    }

    /// SP-PG-SQL-OUTER-JOIN — `LEFT [OUTER] JOIN` parses to
    /// `Op::Join { join_type: Left }`; a bare `JOIN` stays `Inner` (regression).
    #[test]
    fn left_join_parses_to_left_join_type() {
        use kessel_proto::{JoinType, Op};
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE usr (uid U32 NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE ord (owner U32 NOT NULL, amt U32 NOT NULL)");
        let cat = sm.catalog().clone();

        // LEFT JOIN → Left.
        match compile("SELECT * FROM usr LEFT JOIN ord ON usr.uid = ord.owner", &cat).unwrap() {
            Op::Join { join_type, .. } => assert_eq!(join_type, JoinType::Left),
            o => panic!("expected Join, got {o:?}"),
        }
        // LEFT OUTER JOIN → Left (OUTER is a noise word).
        match compile("SELECT * FROM usr LEFT OUTER JOIN ord ON usr.uid = ord.owner", &cat).unwrap() {
            Op::Join { join_type, .. } => assert_eq!(join_type, JoinType::Left),
            o => panic!("expected Join, got {o:?}"),
        }
        // bare JOIN stays Inner (regression).
        match compile("SELECT * FROM usr JOIN ord ON usr.uid = ord.owner", &cat).unwrap() {
            Op::Join { join_type, .. } => assert_eq!(join_type, JoinType::Inner),
            o => panic!("expected Join, got {o:?}"),
        }
        // qualified-projection LEFT join also parses to Left.
        match compile("SELECT usr.uid, ord.amt FROM usr LEFT JOIN ord ON usr.uid = ord.owner", &cat).unwrap() {
            Op::Join { join_type, .. } => assert_eq!(join_type, JoinType::Left),
            o => panic!("expected Join, got {o:?}"),
        }
        // join_projection() must recognise the LEFT/LEFT OUTER shapes so the
        // gateway routes them to render_join_result.
        assert!(join_projection("SELECT usr.uid, ord.amt FROM usr LEFT JOIN ord ON usr.uid = ord.owner").is_some());
        assert!(join_projection("SELECT * FROM usr LEFT OUTER JOIN ord ON usr.uid = ord.owner").is_some());
    }

    /// SP-PG-SQL-JOIN-QUERY — `JOIN … [WHERE] ORDER BY / LIMIT / OFFSET` parses
    /// to the additive `Op::Join { order_by, limit_n, offset_n }` fields. The
    /// ORDER BY column resolves to its id in the COMBINED `(a ++ b)` schema:
    /// usr.uid=0, ord.owner=1, ord.amt=2.
    #[test]
    fn join_order_by_limit_offset_parses() {
        use kessel_proto::{JoinType, Op};
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE usr (uid U32 NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE ord (owner U32 NOT NULL, amt U32 NOT NULL)");
        let cat = sm.catalog().clone();

        // ORDER BY a right column (ord.amt → combined field 2), default ASC.
        match compile(
            "SELECT usr.uid, ord.amt FROM usr JOIN ord ON usr.uid = ord.owner ORDER BY ord.amt",
            &cat,
        ).unwrap() {
            Op::Join { order_by, limit_n, offset_n, limit, .. } => {
                assert_eq!(order_by, Some((2, false)));
                assert_eq!(limit_n, None);
                assert_eq!(offset_n, None);
                assert_eq!(limit, 0, "ORDER BY routes pagination off the legacy limit");
            }
            o => panic!("expected Join, got {o:?}"),
        }

        // ORDER BY a LEFT column DESC + LIMIT + OFFSET.
        match compile(
            "SELECT usr.uid, ord.amt FROM usr JOIN ord ON usr.uid = ord.owner \
             ORDER BY usr.uid DESC LIMIT 5 OFFSET 10",
            &cat,
        ).unwrap() {
            Op::Join { order_by, limit_n, offset_n, limit, .. } => {
                assert_eq!(order_by, Some((0, true)));
                assert_eq!(limit_n, Some(5));
                assert_eq!(offset_n, Some(10));
                assert_eq!(limit, 0);
            }
            o => panic!("{o:?}"),
        }

        // WHERE + ORDER BY + LIMIT compose (WHERE stops at ORDER).
        match compile(
            "SELECT usr.uid, ord.amt FROM usr JOIN ord ON usr.uid = ord.owner \
             WHERE ord.amt > 1 ORDER BY usr.uid LIMIT 3",
            &cat,
        ).unwrap() {
            Op::Join { filter, order_by, limit_n, .. } => {
                assert!(!filter.is_empty(), "WHERE compiled");
                assert_eq!(order_by, Some((0, false)));
                assert_eq!(limit_n, Some(3));
            }
            o => panic!("{o:?}"),
        }

        // LEFT join carries ORDER BY too.
        match compile(
            "SELECT usr.uid, ord.amt FROM usr LEFT JOIN ord ON usr.uid = ord.owner ORDER BY ord.amt DESC",
            &cat,
        ).unwrap() {
            Op::Join { join_type, order_by, .. } => {
                assert_eq!(join_type, JoinType::Left);
                assert_eq!(order_by, Some((2, true)));
            }
            o => panic!("{o:?}"),
        }

        // Regression: a bare `JOIN … LIMIT n` (NO ORDER BY / OFFSET) keeps using
        // the legacy pre-sort `limit` field — wire-identical to the pre-arc op.
        match compile(
            "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner LIMIT 2",
            &cat,
        ).unwrap() {
            Op::Join { order_by, limit_n, offset_n, limit, .. } => {
                assert_eq!(order_by, None);
                assert_eq!(limit_n, None);
                assert_eq!(offset_n, None);
                assert_eq!(limit, 2, "bare LIMIT stays on the legacy field");
            }
            o => panic!("{o:?}"),
        }

        // Bare join (no trailing clauses): all new fields None, legacy limit 0.
        match compile("SELECT * FROM usr JOIN ord ON usr.uid = ord.owner", &cat).unwrap() {
            Op::Join { order_by, limit_n, offset_n, limit, .. } => {
                assert_eq!((order_by, limit_n, offset_n, limit), (None, None, None, 0));
            }
            o => panic!("{o:?}"),
        }

        // Unknown ORDER BY column → error.
        assert!(compile(
            "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner ORDER BY ord.nope",
            &cat,
        ).is_err());
        // Wrong ORDER BY qualifier (names neither table) → error.
        assert!(compile(
            "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner ORDER BY foo.amt",
            &cat,
        ).is_err());
    }

    /// SP-PG-SQL-JOIN-QUERY — end-to-end engine: ORDER BY a right column +
    /// LIMIT paginates the SORTED combined rows. The headline shape.
    #[test]
    fn join_order_by_limit_runs_sorted() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE a (id U64 NOT NULL, name CHAR(16) NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE b (id U64 NOT NULL, aid U64 NOT NULL, title CHAR(16) NOT NULL)");
        run(&mut sm, 3, "INSERT INTO a ID 1 (id, name) VALUES (1, 'tolkien')");
        run(&mut sm, 4, "INSERT INTO b ID 1 (id, aid, title) VALUES (1, 1, 'lotr')");
        run(&mut sm, 5, "INSERT INTO b ID 2 (id, aid, title) VALUES (2, 1, 'hobbit')");
        run(&mut sm, 6, "INSERT INTO b ID 3 (id, aid, title) VALUES (3, 1, 'silmarillion')");

        // Combined schema: a.id(0), a.name(1), b.id(2), b.aid(3), b.title(4).
        fn titles(sm: &mut StateMachine<MemVfs>, seq: u64, sql: &str) -> Vec<String> {
            match run(sm, seq, sql) {
                OpResult::Got(bytes) => {
                    assert_eq!(&bytes[..4], b"KTR1");
                    let dl = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
                    let (jn, jf) = kessel_catalog::decode_type_def(&bytes[8..8 + dl]).unwrap();
                    let cot = ObjectType::from_def(jn, jf);
                    let mut p = 8 + dl;
                    let mut out = Vec::new();
                    while p + 4 <= bytes.len() {
                        let rl = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
                        p += 4;
                        let vals = kessel_codec::decode(&cot, &bytes[p..p + rl]).unwrap();
                        p += rl;
                        if let kessel_codec::Value::Blob(v) = &vals[4] {
                            out.push(String::from_utf8_lossy(kessel_expr::right_trim_char_pad(v)).into_owned());
                        }
                    }
                    out
                }
                o => panic!("{o:?}"),
            }
        }

        // ORDER BY b.title LIMIT 2 ⇒ hobbit, lotr.
        assert_eq!(
            titles(&mut sm, 20, "SELECT a.name, b.title FROM a JOIN b ON a.id=b.aid ORDER BY b.title LIMIT 2"),
            vec!["hobbit".to_string(), "lotr".to_string()],
        );
        // OFFSET 1 LIMIT 2 ⇒ lotr, silmarillion.
        assert_eq!(
            titles(&mut sm, 21, "SELECT a.name, b.title FROM a JOIN b ON a.id=b.aid ORDER BY b.title LIMIT 2 OFFSET 1"),
            vec!["lotr".to_string(), "silmarillion".to_string()],
        );
        // DESC ⇒ silmarillion, lotr, hobbit.
        assert_eq!(
            titles(&mut sm, 22, "SELECT a.name, b.title FROM a JOIN b ON a.id=b.aid ORDER BY b.title DESC"),
            vec!["silmarillion".to_string(), "lotr".to_string(), "hobbit".to_string()],
        );
    }

    /// SP-PG-SQL-JOIN-AGG: `SELECT a.name, COUNT(b.id) FROM a JOIN b … GROUP BY
    /// a.name` compiles to `Op::Join { group_aggregate: Some(..) }` with the
    /// group + agg field ids resolved against the combined `(a ++ b)` schema.
    /// COUNT(*) → the sentinel field id; qualified `COUNT(b.id)` disambiguates
    /// `id` across the two tables. A bare join (no GROUP BY) stays
    /// `group_aggregate: None` (regression).
    #[test]
    fn join_group_aggregate_parses() {
        use kessel_proto::{Op, COUNT_STAR_FIELD};
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE a (id U64 NOT NULL, name CHAR(16) NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE b (id U64 NOT NULL, aid U64 NOT NULL, title CHAR(16) NOT NULL)");
        let cat = sm.catalog().clone();
        // Combined schema: a.id(0), a.name(1), b.id(2), b.aid(3), b.title(4).
        // GROUP BY a.name (1), COUNT(b.id) (2).
        match compile("SELECT a.name, COUNT(b.id) FROM a JOIN b ON a.id=b.aid GROUP BY a.name", &cat).unwrap() {
            Op::Join { group_aggregate: Some(ga), .. } => {
                assert_eq!(ga.group_field, 1);
                assert_eq!(ga.aggregates, vec![(0u8, 2u16)]);
            }
            o => panic!("expected join-agg, got {o:?}"),
        }
        // COUNT(*) ⇒ sentinel field id.
        match compile("SELECT a.name, COUNT(*) FROM a JOIN b ON a.id=b.aid GROUP BY a.name", &cat).unwrap() {
            Op::Join { group_aggregate: Some(ga), .. } => {
                assert_eq!(ga.aggregates, vec![(0u8, COUNT_STAR_FIELD)]);
            }
            o => panic!("expected join-agg COUNT(*), got {o:?}"),
        }
        // Multi-aggregate: COUNT(b.id), SUM(b.aid) over a LEFT join + alias.
        match compile("SELECT a.name, COUNT(b.id) AS c, SUM(b.aid) FROM a LEFT JOIN b ON a.id=b.aid GROUP BY a.name", &cat).unwrap() {
            Op::Join { join_type, group_aggregate: Some(ga), .. } => {
                assert_eq!(join_type, kessel_proto::JoinType::Left);
                assert_eq!(ga.group_field, 1);
                assert_eq!(ga.aggregates, vec![(0u8, 2u16), (1u8, 3u16)]);
            }
            o => panic!("expected multi join-agg, got {o:?}"),
        }
        // Regression: a bare join with NO GROUP BY stays group_aggregate=None.
        match compile("SELECT a.name, b.title FROM a JOIN b ON a.id=b.aid", &cat).unwrap() {
            Op::Join { group_aggregate: None, .. } => {}
            o => panic!("bare join must have no group_aggregate, got {o:?}"),
        }
        // A GROUP BY column that doesn't match the leading projection col errors.
        assert!(compile("SELECT a.name, COUNT(b.id) FROM a JOIN b ON a.id=b.aid GROUP BY b.title", &cat).is_err());
    }

    /// SP-PG-SQL-PLAIN-GROUP-RENDER — the gateway recognizer accepts a plain
    /// (non-JOIN) group-aggregate SELECT and recovers the output column shape.
    #[test]
    fn plain_group_aggregate_recognizer() {
        // Single COUNT(*) — default name `count`, no source column.
        let p = plain_group_aggregate(
            "SELECT category, COUNT(*) FROM products GROUP BY category",
        )
        .expect("single COUNT(*) plain group-agg");
        assert_eq!(p.table, "products");
        assert_eq!(p.group_column, "category");
        assert_eq!(p.aggregates.len(), 1);
        assert_eq!(p.aggregates[0].kind, 0);
        assert_eq!(p.aggregates[0].out_name, "count");
        assert_eq!(p.aggregates[0].source_column, None);

        // Multi-aggregate with aliases + HAVING + ORDER BY + LIMIT (all the
        // trailing clauses are tolerated; the render ignores them).
        let p = plain_group_aggregate(
            "SELECT category, COUNT(*) AS n, SUM(price), AVG(price), MIN(price), MAX(price) \
             FROM products GROUP BY category HAVING COUNT(*) > 1 ORDER BY n DESC LIMIT 5 OFFSET 2",
        )
        .expect("multi-agg plain group-agg with all clauses");
        assert_eq!(p.group_column, "category");
        let names: Vec<&str> = p.aggregates.iter().map(|a| a.out_name.as_str()).collect();
        assert_eq!(names, vec!["n", "sum", "avg", "min", "max"]);
        let kinds: Vec<u8> = p.aggregates.iter().map(|a| a.kind).collect();
        assert_eq!(kinds, vec![0, 1, 4, 2, 3]);
        // SUM/AVG/MIN/MAX carry their source column for OID typing
        // (indices 1=sum, 2=avg, 3=min, 4=max; 0=count has no source).
        assert_eq!(p.aggregates[0].source_column, None);
        assert_eq!(p.aggregates[1].source_column.as_deref(), Some("price"));
        assert_eq!(p.aggregates[2].source_column.as_deref(), Some("price"));
        assert_eq!(p.aggregates[3].source_column.as_deref(), Some("price"));
        assert_eq!(p.aggregates[4].source_column.as_deref(), Some("price"));

        // Qualified group column + qualified aggregate arg — qualifier stripped.
        let p = plain_group_aggregate(
            "SELECT products.category, SUM(products.price) FROM products GROUP BY products.category",
        )
        .expect("qualified plain group-agg");
        assert_eq!(p.group_column, "category");
        assert_eq!(p.aggregates[0].source_column.as_deref(), Some("price"));

        // None cases — the existing render paths own these shapes:
        //   * a JOIN group-agg (owned by join_group_aggregate)
        assert!(plain_group_aggregate(
            "SELECT a.name, COUNT(b.id) FROM a JOIN b ON a.id=b.aid GROUP BY a.name"
        )
        .is_none());
        //   * a single scalar aggregate, no GROUP BY (owned by select_aggregate)
        assert!(plain_group_aggregate("SELECT COUNT(*) FROM products").is_none());
        //   * a plain projection, no aggregate
        assert!(plain_group_aggregate("SELECT category FROM products GROUP BY category").is_none());
        //   * no GROUP BY at all
        assert!(plain_group_aggregate("SELECT category, COUNT(*) FROM products").is_none());
        //   * SELECT *
        assert!(plain_group_aggregate("SELECT * FROM products").is_none());
    }

    /// SP-PG-SQL-JOIN-AGG end-to-end: COUNT(b.id) per author over the join,
    /// run through the engine, returns the group-aggregate stream with the
    /// expected per-group counts (tolkien 2, lewis 1). Groups ascending.
    #[test]
    fn join_group_aggregate_runs() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE a (id U64 NOT NULL, name CHAR(16) NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE b (id U64 NOT NULL, aid U64 NOT NULL, title CHAR(16) NOT NULL)");
        run(&mut sm, 3, "INSERT INTO a ID 1 (id, name) VALUES (1, 'tolkien')");
        run(&mut sm, 4, "INSERT INTO a ID 2 (id, name) VALUES (2, 'lewis')");
        run(&mut sm, 5, "INSERT INTO b ID 1 (id, aid, title) VALUES (1, 1, 'lotr')");
        run(&mut sm, 6, "INSERT INTO b ID 2 (id, aid, title) VALUES (2, 1, 'hobbit')");
        run(&mut sm, 7, "INSERT INTO b ID 3 (id, aid, title) VALUES (3, 2, 'narnia')");

        // Decode the group-aggregate result into (name, count) pairs.
        fn counts(sm: &mut StateMachine<MemVfs>, seq: u64, sql: &str) -> Vec<(String, i128)> {
            match run(sm, seq, sql) {
                OpResult::Got(b) => {
                    let ng = u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize;
                    let mut p = 4;
                    let mut out = Vec::new();
                    for _ in 0..ng {
                        let kl = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                        p += 4;
                        let name = String::from_utf8_lossy(
                            kessel_expr::right_trim_char_pad(&b[p..p + kl])).into_owned();
                        p += kl;
                        let v = i128::from_le_bytes(b[p..p + 16].try_into().unwrap());
                        p += 16;
                        out.push((name, v));
                    }
                    assert_eq!(p, b.len());
                    out
                }
                o => panic!("{o:?}"),
            }
        }
        // HEADLINE: count related rows per parent — tolkien 2, lewis 1 (groups
        // ascending: lewis, tolkien).
        assert_eq!(
            counts(&mut sm, 20, "SELECT a.name, COUNT(b.id) FROM a JOIN b ON a.id=b.aid GROUP BY a.name"),
            vec![("lewis".to_string(), 1), ("tolkien".to_string(), 2)],
        );
        // COUNT(*) = group size (same here, every combined row is matched).
        assert_eq!(
            counts(&mut sm, 21, "SELECT a.name, COUNT(*) FROM a JOIN b ON a.id=b.aid GROUP BY a.name"),
            vec![("lewis".to_string(), 1), ("tolkien".to_string(), 2)],
        );
    }

    /// SP-PG-SQL-OUTER-JOIN — engine semantics. When every left row matches,
    /// LEFT == INNER. When a left row has NO match, it appears ONCE with NULL
    /// right (`ord.*`) fields. LEFT + WHERE on a right column drops the
    /// unmatched (NULL) rows (PostgreSQL semantics).
    #[test]
    fn left_join_emits_unmatched_left_with_null_right() {
        use kessel_proto::Op;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE usr (uid U32 NOT NULL)");
        run(&mut sm, 2, "CREATE TABLE ord (owner U32 NOT NULL, amt U32 NOT NULL)");
        // usr 1 has 2 orders; usr 2 (orphan) has none.
        run(&mut sm, 3, "INSERT INTO usr ID 1 (uid) VALUES (1)");
        run(&mut sm, 4, "INSERT INTO usr ID 2 (uid) VALUES (2)");
        run(&mut sm, 5, "INSERT INTO ord ID 10 (owner, amt) VALUES (1, 100)");
        run(&mut sm, 6, "INSERT INTO ord ID 11 (owner, amt) VALUES (1, 200)");

        // Decode the KTR1 result into (row_count, per_row null-bitmap of the
        // combined record). Combined schema = usr.uid, ord.owner, ord.amt
        // (3 fields: index 0 = left, indices 1,2 = right).
        fn rows_and_nulls(sm: &mut StateMachine<MemVfs>, seq: u64, sql: &str)
            -> Vec<[bool; 3]>
        {
            match run(sm, seq, sql) {
                OpResult::Got(b) => {
                    assert_eq!(&b[..4], b"KTR1");
                    let dl = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;
                    let mut p = 8 + dl;
                    let mut out = Vec::new();
                    while p + 4 <= b.len() {
                        let rl = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                        let rec = &b[p + 4..p + 4 + rl];
                        // null bitmap lives at record bytes 6..14.
                        let bitmap = &rec[6..14];
                        let null = |i: usize| bitmap[i / 8] & (1 << (i % 8)) != 0;
                        out.push([null(0), null(1), null(2)]);
                        p += 4 + rl;
                    }
                    assert_eq!(p, b.len(), "consumed exactly");
                    out
                }
                o => panic!("{o:?}"),
            }
        }

        // INNER: orphan usr 2 produces NO row → 2 rows (both for usr 1).
        let inner = rows_and_nulls(&mut sm, 20,
            "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner");
        assert_eq!(inner.len(), 2, "inner drops the orphan");
        assert!(inner.iter().all(|n| n == &[false, false, false]),
            "inner rows carry no NULLs");

        // LEFT, all-match subset (only usr 1 exists): LEFT == INNER shape.
        // (Validate via the full table below; here check the 3-row LEFT.)
        let left = rows_and_nulls(&mut sm, 21,
            "SELECT * FROM usr LEFT JOIN ord ON usr.uid = ord.owner");
        assert_eq!(left.len(), 3, "LEFT emits the orphan too (2 matched + 1 orphan)");
        // exactly one row (the orphan) has NULL right fields.
        let orphan_rows: Vec<_> = left.iter().filter(|n| n[1] && n[2]).collect();
        assert_eq!(orphan_rows.len(), 1, "exactly one orphan row");
        // the orphan's LEFT field (usr.uid) is NOT null.
        assert!(left.iter().any(|n| !n[0] && n[1] && n[2]),
            "orphan keeps its left value, NULLs only on the right");
        // the two matched rows have no NULLs.
        assert_eq!(left.iter().filter(|n| n == &&[false, false, false]).count(), 2);

        // LEFT + WHERE on a RIGHT column: the predicate on a NULL right column
        // is false ⇒ the orphan row is dropped (PG semantics). amt=200 → 1 row,
        // and it is a matched (non-NULL) row.
        let filtered = rows_and_nulls(&mut sm, 22,
            "SELECT * FROM usr LEFT JOIN ord ON usr.uid = ord.owner WHERE ord.amt = 200");
        assert_eq!(filtered.len(), 1, "WHERE on a NULL right col drops the orphan");
        assert_eq!(filtered[0], [false, false, false]);

        // LEFT + WHERE on the LEFT column keeps the orphan when it matches.
        let left_filter = rows_and_nulls(&mut sm, 23,
            "SELECT * FROM usr LEFT JOIN ord ON usr.uid = ord.owner WHERE usr.uid = 2");
        assert_eq!(left_filter.len(), 1, "orphan usr 2 survives a left-col filter");
        assert!(left_filter[0][1] && left_filter[0][2], "and is still a NULL-right row");
    }

    #[test]
    fn fk_table_constraint_ddl_parses() {
        use kessel_proto::Op;
        // SP-PG-DDL-FK-ENFORCE — SQLAlchemy `create_all` renders a child
        // model's ForeignKey as a trailing table constraint. It must parse
        // AND now capture an FK descriptor (BY NAME) in the type-def blob, so
        // the engine can resolve + enforce it at apply time. The base
        // name+fields decode is UNCHANGED (the FK rides a trailer), and a
        // table WITHOUT an FK is byte-identical to before this arc.
        let cat = Catalog::default();
        let with_fk = compile(
            "CREATE TABLE books (id I64 NOT NULL, title CHAR(64), author_id I64, \
             FOREIGN KEY(author_id) REFERENCES authors (id))",
            &cat,
        )
        .unwrap();
        let without_fk = compile(
            "CREATE TABLE books (id I64 NOT NULL, title CHAR(64), author_id I64)",
            &cat,
        )
        .unwrap();
        match (&with_fk, &without_fk) {
            (Op::CreateType { def: a }, Op::CreateType { def: b }) => {
                // The FK clause NOW changes the encoded def (it carries the FK
                // descriptor), but the FK is the ONLY difference: base
                // name+fields decode identically.
                assert_ne!(a, b, "FK clause now carries a descriptor in the def");
                let (na, fa) = kessel_catalog::decode_type_def(a).unwrap();
                let (nb, fb) = kessel_catalog::decode_type_def(b).unwrap();
                assert_eq!((na, fa), (nb, fb), "base name+fields unchanged");
                // The captured FK descriptor: author_id -> authors(id), NO
                // ACTION (no ON DELETE clause → default 0).
                let fks = kessel_catalog::decode_type_fks(a);
                assert_eq!(fks.len(), 1);
                assert_eq!(fks[0].child_col, "author_id");
                assert_eq!(fks[0].ref_table, "authors");
                assert_eq!(fks[0].ref_col.as_deref(), Some("id"));
                assert_eq!(fks[0].on_delete, 0, "NO ACTION default");
                // The no-FK table carries NO FK trailer (byte-identical to
                // the pre-arc shape).
                assert!(kessel_catalog::decode_type_fks(b).is_empty());
            }
            o => panic!("expected CreateType pair, got {o:?}"),
        }
        // FK + ON DELETE CASCADE maps to on_delete=2.
        match compile(
            "CREATE TABLE books (id I64 NOT NULL, author_id I64, \
             FOREIGN KEY(author_id) REFERENCES authors (id) ON DELETE CASCADE)",
            &cat,
        )
        .unwrap()
        {
            Op::CreateType { def } => {
                let fks = kessel_catalog::decode_type_fks(&def);
                assert_eq!(fks.len(), 1);
                assert_eq!(fks[0].on_delete, 2, "ON DELETE CASCADE → 2");
            }
            o => panic!("{o:?}"),
        }
        // ON DELETE RESTRICT → 1; ON DELETE SET NULL → 3; SET DEFAULT → 4.
        for (clause, expect) in [
            ("ON DELETE RESTRICT", 1u8),
            ("ON DELETE SET NULL", 3),
            ("ON DELETE SET DEFAULT", 4),
            ("ON DELETE NO ACTION", 0),
        ] {
            let sql = format!(
                "CREATE TABLE books (id I64 NOT NULL, author_id I64, \
                 FOREIGN KEY(author_id) REFERENCES authors (id) {clause})"
            );
            match compile(&sql, &cat).unwrap() {
                Op::CreateType { def } => {
                    let fks = kessel_catalog::decode_type_fks(&def);
                    assert_eq!(fks[0].on_delete, expect, "{clause}");
                }
                o => panic!("{o:?}"),
            }
        }
        // ON UPDATE actions are parsed but do NOT drive on_delete (still 0).
        match compile(
            "CREATE TABLE books (id I64 NOT NULL, author_id I64, \
             FOREIGN KEY(author_id) REFERENCES authors (id) ON UPDATE CASCADE)",
            &cat,
        )
        .unwrap()
        {
            Op::CreateType { def } => {
                assert_eq!(kessel_catalog::decode_type_fks(&def)[0].on_delete, 0);
            }
            o => panic!("{o:?}"),
        }
        // Inline column REFERENCES modifier captures the FK too, keyed by the
        // column it modifies; the FK col is still stored as a field.
        match compile(
            "CREATE TABLE books (id I64 NOT NULL, author_id I64 REFERENCES authors (id))",
            &cat,
        )
        .unwrap()
        {
            Op::CreateType { def } => {
                let (_n, fields) = kessel_catalog::decode_type_def(&def).unwrap();
                let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
                assert_eq!(names, ["id", "author_id"], "FK col still stored");
                let fks = kessel_catalog::decode_type_fks(&def);
                assert_eq!(fks.len(), 1);
                assert_eq!(fks[0].child_col, "author_id");
                assert_eq!(fks[0].ref_table, "authors");
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn join_projection_extracts_qualified_cols() {
        // SELECT * over a JOIN -> star projection.
        let (cols, star) = join_projection(
            "SELECT * FROM authors JOIN books ON authors.id = books.author_id",
        )
        .unwrap();
        assert!(star);
        assert!(cols.is_empty());
        // Explicit qualified projection over a JOIN.
        let (cols, star) = join_projection(
            "SELECT authors.name, books.title FROM authors JOIN books \
             ON authors.id = books.author_id",
        )
        .unwrap();
        assert!(!star);
        assert_eq!(
            cols,
            vec![
                JoinProjCol { qualifier: Some("authors".into()), column: "name".into() },
                JoinProjCol { qualifier: Some("books".into()), column: "title".into() },
            ]
        );
        // A single-table SELECT is NOT a JOIN projection.
        assert_eq!(join_projection("SELECT a, b FROM t"), None);
        assert_eq!(join_projection("SELECT * FROM t"), None);
        // Output alias accepted-and-skipped.
        let (cols, _) = join_projection(
            "SELECT authors.name AS an, books.title FROM authors JOIN books \
             ON authors.id = books.author_id",
        )
        .unwrap();
        assert_eq!(cols[0].column, "name");
    }

    #[test]
    fn parse_errors_are_clean() {
        let cat = Catalog::default();
        assert!(compile("SELECT", &cat).is_err());
        assert!(compile("DROP TABLE x", &cat).is_err());
        assert!(compile("INSERT INTO nope ID 1 (a) VALUES (1)", &cat).is_err());
        assert!(compile("CREATE TABLE t (a NOPETYPE)", &cat).is_err());
    }

    /// SP-DX: unknown-table error carries a did-you-mean suggestion
    /// when a near-match exists; sites that previously rendered raw
    /// `unknown table \`foo\`` now render the friendlier form. The
    /// suggestion is deterministic.
    #[test]
    fn unknown_table_suggests_near_match() {
        let mut cat = Catalog::default();
        cat.types.push(ObjectType::from_def(
            "accounts".into(),
            vec![Field {
                field_id: 1,
                name: "id".into(),
                kind: FieldKind::U64,
                nullable: false,
            }],
        ));
        let e = compile("SELECT * FROM acconts", &cat).unwrap_err();
        assert!(e.contains("unknown table"), "{e}");
        assert!(e.contains("did you mean"), "{e}");
        assert!(e.contains("`accounts`"), "{e}");

        // Wildly unrelated → no false suggestion.
        let e = compile("SELECT * FROM xyzzy12345", &cat).unwrap_err();
        assert!(e.contains("unknown table"), "{e}");
        assert!(!e.contains("did you mean"), "no spurious suggestion: {e}");

        // Empty catalog → educational message.
        let cat0 = Catalog::default();
        let e = compile("SELECT * FROM nope", &cat0).unwrap_err();
        assert!(e.contains("no tables defined"), "{e}");
    }

    /// SP-DX: unknown-column errors include the table name and either
    /// a did-you-mean or the head of the column list — agents/users
    /// don't need a separate DESCRIBE round-trip to see the schema.
    #[test]
    fn unknown_column_includes_table_context() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)");
        let cat = sm.catalog();
        // Typo close to a real column → suggestion.
        let e = compile("SELECT * FROM acct WHERE owne = 1", cat).unwrap_err();
        assert!(e.contains("unknown column `owne`"), "{e}");
        assert!(e.contains("on table `acct`"), "{e}");
        assert!(e.contains("did you mean `owner`"), "{e}");
        // Unrelated name → falls back to listing available columns.
        let e = compile("SELECT * FROM acct WHERE zzz = 1", cat).unwrap_err();
        assert!(e.contains("unknown column `zzz`"), "{e}");
        assert!(e.contains("on table `acct`"), "{e}");
        assert!(e.contains("`owner`") && e.contains("`bal`"), "{e}");
    }

    /// SP-DX: `suggest` is total + deterministic + zero-dep.
    #[test]
    fn suggest_helper_basic_shape() {
        let cands = ["accounts", "orders", "users"];
        assert_eq!(suggest("acconts", &cands), Some("accounts"));
        assert_eq!(suggest("user", &cands), Some("users"));
        assert_eq!(suggest("ORDER", &cands), Some("orders"));
        assert_eq!(suggest("zzz", &cands), None);
        assert_eq!(suggest("anything", &[]), None);
        // Stable across calls.
        let a = suggest("acconts", &cands);
        let b = suggest("acconts", &cands);
        assert_eq!(a, b);
    }

    #[test]
    fn where_or_not_paren() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I32 NOT NULL)");
        for (i, v) in [1i64, 2, 3, 4, 5].iter().enumerate() {
            run(&mut sm, 2 + i as u64, &format!("INSERT INTO t ID {} (a) VALUES ({})", i, v));
        }
        // a = 1 OR a >= 4  -> {1,4,5} = 3
        match run(&mut sm, 10, "SELECT COUNT(*) FROM t WHERE a = 1 OR a >= 4") {
            OpResult::Got(b) => assert_eq!(i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()), 3),
            o => panic!("{o:?}"),
        }
        // NOT (a = 3) -> 4
        match run(&mut sm, 11, "SELECT COUNT(*) FROM t WHERE NOT (a = 3)") {
            OpResult::Got(b) => assert_eq!(i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()), 4),
            o => panic!("{o:?}"),
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ-PARSED T1 — lexer recognizes `$N` as `Tok::Param(N)`.
    // T1 ships JUST the lexer (the parser still rejects `Tok::Param`
    // because no value-position acceptance exists yet — that's T2);
    // these KATs lock the lexical shape so T2 can build on it without
    // worrying the lexer accidentally drifts. Companion design spec:
    // `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqparsed-design.md`
    // §3.1 token-rewrite shape.
    // ─────────────────────────────────────────────────────────────────

    /// `$1` lexes as `Tok::Param(1)`.
    #[test]
    fn t1parsed_lex_dollar_one() {
        let toks = lex("SELECT $1").expect("lex ok");
        assert_eq!(
            toks,
            vec![Tok::Ident("SELECT".to_string()), Tok::Param(1)]
        );
    }

    /// `$N` in a WHERE predicate position lexes as `Tok::Param(N)`.
    #[test]
    fn t1parsed_lex_dollar_in_where_position() {
        let toks =
            lex("SELECT * FROM t WHERE id = $1").expect("lex ok");
        // The relevant suffix is `id = $1` → Ident, Cmp("="), Param(1).
        assert!(toks.contains(&Tok::Param(1)));
        // Locate the `=` index and assert the next tok is the Param.
        let cmp_idx = toks
            .iter()
            .position(|t| matches!(t, Tok::Cmp("=")))
            .expect("`=` present");
        assert_eq!(toks[cmp_idx + 1], Tok::Param(1));
    }

    /// `$10` lexes greedily as `Tok::Param(10)` (NOT `$1` followed by
    /// literal `0`). Mirrors the gateway substitute scanner.
    #[test]
    fn t1parsed_lex_two_digit_index() {
        let toks = lex("SELECT $10").expect("lex ok");
        assert_eq!(
            toks,
            vec![Tok::Ident("SELECT".to_string()), Tok::Param(10)]
        );
    }

    /// `$1, $2` ordering preserved — the multi-position case.
    #[test]
    fn t1parsed_lex_multiple_params_in_order() {
        let toks = lex("SELECT $1, $2").expect("lex ok");
        assert_eq!(
            toks,
            vec![
                Tok::Ident("SELECT".to_string()),
                Tok::Param(1),
                Tok::Punct(','),
                Tok::Param(2)
            ]
        );
    }

    /// `$0` is rejected — PG `$N` indices are 1-based. The error
    /// message names the V1 weak-spot so a future contributor sees
    /// why the strictness exists.
    #[test]
    fn t1parsed_lex_zero_index_rejected() {
        let err = lex("SELECT $0").unwrap_err();
        assert!(
            err.contains("1-based") || err.contains("`$0`"),
            "expected the lexer error to name the 1-based rule, got `{err}`"
        );
    }

    /// `$100` exceeds the V1 cap.
    #[test]
    fn t1parsed_lex_overlimit_index_rejected() {
        let err = lex("SELECT $100").unwrap_err();
        assert!(
            err.contains("99"),
            "expected the lexer error to name the V1 cap, got `{err}`"
        );
    }

    /// Bare `$` with no following digit is rejected — defensive against
    /// typos and unbound dollar-sign uses in SQL (PG itself doesn't
    /// have a use for bare `$` outside `$N` and dollar-quoted strings).
    /// The gateway-side text scanner is permissive (passes bare `$`
    /// through verbatim) because it processes pre-parsed SQL bytes;
    /// here we are the parser-side authority and reject the ambiguity.
    #[test]
    fn t1parsed_lex_bare_dollar_with_no_digit_rejected() {
        let err = lex("SELECT $").unwrap_err();
        assert!(
            err.contains("digit"),
            "expected the lexer to name the missing digit, got `{err}`"
        );
    }

    // ─────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ-PARSED T2 KATs — `compile_with_params` typed-param
    // threading. The bound `Value` enters as a typed token (NOT a
    // SQL-text concatenation) and emerges in the program as the
    // same `Value`. Closes the SP-PG-EXTQ V1 §11 weak-spot #1 attack
    // surface for every typed-path-eligible parameter.
    // ─────────────────────────────────────────────────────────────────

    /// Headline regression: `compile_with_params(sql_with_$N, params)`
    /// emits the same `Op` as `compile(sql_with_literal_in_place_of_$N)`.
    /// Byte-equal proof that the typed-param path is a drop-in for the
    /// literal-substituted shape (which is what the gateway's text-
    /// substitution path produces today).
    #[test]
    fn t2parsed_compile_with_params_byte_equal_to_literal_substitution() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id I64 NOT NULL)");
        let cat = sm.catalog();
        let via_params = compile_with_params(
            "SELECT * FROM t WHERE id = $1",
            cat,
            &[Some(Value::Int(42))],
        )
        .expect("compile_with_params ok");
        let via_literal = compile(
            "SELECT * FROM t WHERE id = 42",
            cat,
        )
        .expect("compile literal ok");
        // Op enum is structurally comparable; byte-equal on the
        // serialized form (which the engine sees).
        assert_eq!(format!("{via_params:?}"), format!("{via_literal:?}"));
    }

    /// HEADLINE SECURITY KAT — a quote-injection attempt produces an
    /// Op whose bound value is a `Value::Blob` operand at the EQ
    /// comparison, NOT a SQL string that the parser would re-parse.
    /// The `DROP TABLE` never reaches the engine because the bound
    /// bytes were carried through the AST as a typed value, never
    /// concatenated into SQL text.
    ///
    /// This is the V1 §11 weak-spot #1 fix verified by KAT: even if
    /// a future SQL extension or a regex-shaped scanner bug breaks
    /// the existing text-substitution path's `'` → `''` doubling,
    /// THIS path stays safe because it never escapes a quote — it
    /// never enters a quote at all.
    #[test]
    fn t2parsed_quote_injection_attempt_does_not_inject_sql() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id I64 NOT NULL, name CHAR(64) NOT NULL)");
        let cat = sm.catalog();
        // The classic bobby-tables payload as a bound string param.
        let payload = b"'; DROP TABLE t; --";
        let via_params = compile_with_params(
            "SELECT * FROM t WHERE name = $1",
            cat,
            &[Some(Value::Blob(payload.to_vec()))],
        )
        .expect("compile_with_params ok — the bound value is a typed Value");
        // The bound value must survive verbatim as the EQ rhs operand.
        // Any SQL injection would re-parse `; DROP TABLE t; --` and
        // produce a different Op (likely an Op::DropType or a parse
        // error). The CORRECT outcome is a Query/QueryRows Op whose
        // program operand contains the payload bytes as-is.
        match via_params {
            Op::QueryRows { program, .. } => {
                // The program byte stream contains a literal Bytes-push
                // for the payload. Search for the payload bytes inside
                // the program — if they appear, the bound value
                // survived as a typed operand.
                let prog_has_payload = program
                    .windows(payload.len())
                    .any(|w| w == payload);
                assert!(
                    prog_has_payload,
                    "expected bound payload bytes to survive verbatim \
                     in the program operand; instead got program = {program:?}",
                );
            }
            other => panic!(
                "expected Op::QueryRows with the payload bytes carried \
                 as a typed operand; got {other:?} which suggests the \
                 injected SQL took effect (SECURITY REGRESSION)",
            ),
        }
    }

    /// `$1, $2` ordering preserved — each param resolves to its own
    /// slot, multi-param case.
    #[test]
    fn t2parsed_compile_with_params_multi_position_ordering() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL, b I64 NOT NULL)");
        let cat = sm.catalog();
        let via_params = compile_with_params(
            "SELECT * FROM t WHERE a = $1 AND b = $2",
            cat,
            &[Some(Value::Int(10)), Some(Value::Int(20))],
        )
        .expect("ok");
        let via_literal = compile(
            "SELECT * FROM t WHERE a = 10 AND b = 20",
            cat,
        )
        .expect("ok");
        assert_eq!(format!("{via_params:?}"), format!("{via_literal:?}"));
    }

    /// Same `$N` referenced multiple times resolves to the same value
    /// at each occurrence (mirror's the gateway's `$N` repeat semantics).
    #[test]
    fn t2parsed_compile_with_params_same_index_used_twice() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL, b I64 NOT NULL)");
        let cat = sm.catalog();
        let via_params = compile_with_params(
            "SELECT * FROM t WHERE a = $1 OR b = $1",
            cat,
            &[Some(Value::Int(42))],
        )
        .expect("ok");
        let via_literal = compile(
            "SELECT * FROM t WHERE a = 42 OR b = 42",
            cat,
        )
        .expect("ok");
        assert_eq!(format!("{via_params:?}"), format!("{via_literal:?}"));
    }

    /// NULL injection via `None`. The token rewrite emits
    /// `Tok::Ident("NULL")` which the parser accepts in literal
    /// positions. Mirrors the gateway substitute's bare-NULL
    /// keyword shape.
    #[test]
    fn t2parsed_compile_with_params_null_injects_as_null_keyword() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL)");
        let cat = sm.catalog();
        // `WHERE a IS NULL` is the supported predicate form; with the
        // rewritten `Tok::Ident("NULL")` injected at the literal slot,
        // the parser sees `WHERE a IS NULL` which compiles to the
        // IS_NULL opcode against `a`.
        let via_params = compile_with_params(
            "SELECT * FROM t WHERE a IS $1",
            cat,
            &[None],
        );
        // Either path (Ok with IS_NULL program, or Err if the parser
        // can't handle `IS NULL` from the rewritten token) is valid;
        // we lock the Ok shape that mirrors the gateway substitute.
        match via_params {
            Ok(Op::QueryRows { program, .. }) => {
                // The program should start with the IS_NULL opcode (2)
                // for the field `a`. The exact byte shape: [2,
                // field_id_lo, field_id_hi].
                assert_eq!(program.first(), Some(&2u8),
                    "expected IS_NULL opcode at the start of the program, got {program:?}");
            }
            other => panic!("expected Ok(QueryRows) with IS_NULL program, got {other:?}"),
        }
    }

    /// Out-of-bounds `$N` returns `SqlError::unbound parameter $N`.
    #[test]
    fn t2parsed_compile_with_params_out_of_bounds_index_rejected() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id I64 NOT NULL)");
        let cat = sm.catalog();
        let err = compile_with_params(
            "SELECT * FROM t WHERE id = $3",
            cat,
            &[Some(Value::Int(1)), Some(Value::Int(2))],
        )
        .unwrap_err();
        assert!(
            err.contains("$3") && err.contains("unbound"),
            "expected the error to name the unbound parameter index, got `{err}`"
        );
    }

    /// SQL with `$N` but compile (no params) fails because the
    /// parser doesn't accept `Tok::Param` in any value position.
    /// Lock: `compile()` is the bare path; only `compile_with_params`
    /// can rewrite `Tok::Param`.
    #[test]
    fn t2parsed_compile_without_params_rejects_dollar_n() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id I64 NOT NULL)");
        let cat = sm.catalog();
        let err = compile(
            "SELECT * FROM t WHERE id = $1",
            cat,
        )
        .unwrap_err();
        // The exact error message comes from the parser's `_ =>
        // Err("...")` arm in `cmp_expr` / `term`; we only assert
        // that SOME error happens (compile didn't silently produce
        // an incoherent Op).
        assert!(
            !err.is_empty(),
            "expected a non-empty error for $N without bound params"
        );
    }

    /// `compile_with_params` of SQL WITHOUT any `$N` should be a
    /// no-op pass-through — byte-equal to `compile`.
    #[test]
    fn t2parsed_compile_with_params_no_placeholders_byte_equal_to_compile() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id I64 NOT NULL)");
        let cat = sm.catalog();
        let via_params = compile_with_params(
            "SELECT * FROM t WHERE id = 99",
            cat,
            &[],
        ).expect("ok");
        let via_compile = compile(
            "SELECT * FROM t WHERE id = 99",
            cat,
        ).expect("ok");
        assert_eq!(format!("{via_params:?}"), format!("{via_compile:?}"));
    }

    /// `$N` inside INSERT VALUES — pgJDBC / asyncpg / SQLAlchemy
    /// all emit Bind with `INSERT INTO t (a, b) VALUES ($1, $2)`.
    /// The typed-param path produces the same Op as the literal-
    /// substituted form.
    #[test]
    fn t2parsed_compile_with_params_insert_values() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL, b CHAR(64) NOT NULL)");
        let cat = sm.catalog();
        let via_params = compile_with_params(
            "INSERT INTO t ID 5 (a, b) VALUES ($1, $2)",
            cat,
            &[Some(Value::Int(42)), Some(Value::Blob(b"hello".to_vec()))],
        ).expect("ok");
        let via_literal = compile(
            "INSERT INTO t ID 5 (a, b) VALUES (42, 'hello')",
            cat,
        ).expect("ok");
        assert_eq!(format!("{via_params:?}"), format!("{via_literal:?}"));
    }

    /// `$N` mixed with bare literals in the same WHERE — the param
    /// resolves at its slot; the literal stays literal.
    #[test]
    fn t2parsed_compile_with_params_mixed_with_bare_literals() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL, b I64 NOT NULL)");
        let cat = sm.catalog();
        let via_params = compile_with_params(
            "SELECT * FROM t WHERE a = $1 AND b = 7",
            cat,
            &[Some(Value::Int(42))],
        ).expect("ok");
        let via_literal = compile(
            "SELECT * FROM t WHERE a = 42 AND b = 7",
            cat,
        ).expect("ok");
        assert_eq!(format!("{via_params:?}"), format!("{via_literal:?}"));
    }

    /// `Value::Uint(u128)` parameter coerces to `Tok::Int(i128)`
    /// when it fits in the signed range.
    #[test]
    fn t2parsed_compile_with_params_uint_value_coerces_to_int_token() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id U64 NOT NULL)");
        let cat = sm.catalog();
        let via_params = compile_with_params(
            "SELECT * FROM t WHERE id = $1",
            cat,
            &[Some(Value::Uint(123u128))],
        ).expect("ok");
        let via_literal = compile(
            "SELECT * FROM t WHERE id = 123",
            cat,
        ).expect("ok");
        assert_eq!(format!("{via_params:?}"), format!("{via_literal:?}"));
    }

    // ─────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 KATs — `Value::Blob` parameter
    // bindings flow through `Tok::Bytes` → `Lit::Bytes` → `Value::Blob`
    // without the UTF-8 round-trip that the V1 `Tok::Str(from_utf8_lossy)`
    // path imposed. The headline guarantee: non-UTF8 byte sequences
    // (0xFF, 0xFE, isolated continuation bytes) round-trip byte-equal
    // through the parser into the engine's storage layer.
    // ─────────────────────────────────────────────────────────────────

    /// Helper: pull the first `Op::Create`'s encoded record out of an
    /// Op tree. Single-row INSERTs in this SQL surface always wrap in
    /// `Op::Txn { ops: [Op::Create { record, .. }] }`.
    fn extract_create_record(op: &Op) -> Vec<u8> {
        match op {
            Op::Create { record, .. } => record.clone(),
            Op::Txn { ops, .. } => ops
                .iter()
                .find_map(|o| match o {
                    Op::Create { record, .. } => Some(record.clone()),
                    _ => None,
                })
                .expect("expected an Op::Create inside Op::Txn"),
            other => panic!("expected Op::Create or Op::Txn; got {other:?}"),
        }
    }

    /// Headline byte-preservation KAT: a `Value::Blob` containing
    /// non-UTF8 bytes (0x00, 0xFF, 0xFE, 0xFD) bound to a `$1` in an
    /// INSERT VALUES clause produces an `Op::Create` whose stored
    /// record carries those exact bytes verbatim.
    #[test]
    fn t2byteatyped_value_blob_non_utf8_bytes_preserved_through_insert() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id I64 NOT NULL, data BYTES(8) NOT NULL)");
        let cat = sm.catalog();
        let payload: Vec<u8> = vec![0x00, 0xFF, 0xFE, 0xFD, 0xAB, 0xCD, 0xEF, 0x80];
        let op = compile_with_params(
            "INSERT INTO t (id, data) VALUES ($1, $2)",
            cat,
            &[Some(Value::Int(1)), Some(Value::Blob(payload.clone()))],
        )
        .expect("compile_with_params ok");
        // The resulting Op::Create carries the payload bytes verbatim
        // in the encoded record body. Search for the byte sequence in
        // the encoded form. (Single-row INSERT may wrap in Op::Txn.)
        let record_bytes = extract_create_record(&op);
        let has_payload = record_bytes
            .windows(payload.len())
            .any(|w| w == payload.as_slice());
        assert!(
            has_payload,
            "expected payload bytes {payload:?} to appear verbatim \
             in the encoded record {record_bytes:?}",
        );
    }

    /// `rewrite_param_tokens` direct: `Value::Blob` with non-UTF8
    /// bytes produces `Tok::Bytes` (NOT `Tok::Str` with lossy
    /// conversion). Pins the new variant on the typed-path output.
    #[test]
    fn t2byteatyped_rewrite_param_tokens_emits_tok_bytes_for_blob() {
        // SQL with a single `$1` placeholder. The lexer emits
        // `Tok::Param(1)`; the rewriter replaces it.
        let toks = lex("SELECT $1").expect("lex ok");
        let payload: Vec<u8> = vec![0xFF, 0xFE, 0xFD];
        let rewritten = rewrite_param_tokens(
            toks,
            &[Some(Value::Blob(payload.clone()))],
        )
        .expect("rewrite ok");
        // Find the rewritten param token.
        let bytes_tok = rewritten
            .iter()
            .find(|t| matches!(t, Tok::Bytes(_)));
        assert!(
            bytes_tok.is_some(),
            "expected `Tok::Bytes` in rewritten stream {rewritten:?}",
        );
        if let Some(Tok::Bytes(b)) = bytes_tok {
            assert_eq!(*b, payload, "Tok::Bytes must carry the exact bytes");
        }
        // No `Tok::Str` should appear (the lossy-UTF8 regression).
        let str_tok = rewritten.iter().find(|t| matches!(t, Tok::Str(_)));
        assert!(
            str_tok.is_none(),
            "Tok::Str must NOT appear (would indicate lossy-UTF8 regression); \
             got {rewritten:?}",
        );
    }

    /// WHERE clause: `data = $1` with a non-UTF8 `Value::Blob` bound
    /// produces a program operand carrying those exact bytes. The
    /// match-against-stored-row path is byte-equal to a literal
    /// hex-bytes comparison.
    #[test]
    fn t2byteatyped_where_eq_non_utf8_bytes_program_has_payload() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id I64 NOT NULL, data BYTES(4) NOT NULL)");
        let cat = sm.catalog();
        let payload: Vec<u8> = vec![0xCA, 0xFE, 0xBA, 0xBE];
        let op = compile_with_params(
            "SELECT * FROM t WHERE data = $1",
            cat,
            &[Some(Value::Blob(payload.clone()))],
        )
        .expect("compile_with_params ok");
        match op {
            Op::QueryRows { program, .. } => {
                let has = program.windows(payload.len()).any(|w| w == payload);
                assert!(
                    has,
                    "expected payload {payload:?} in program {program:?}",
                );
            }
            other => panic!("expected QueryRows; got {other:?}"),
        }
    }

    /// Verbatim `0xFF` byte (always invalid UTF-8 start) survives
    /// through the typed-path round-trip. This is the canary bug-
    /// fix proof: the prior `String::from_utf8_lossy(b)` path would
    /// replace `0xFF` with `U+FFFD REPLACEMENT CHARACTER`
    /// (`0xEF 0xBF 0xBD` as UTF-8), corrupting the data.
    #[test]
    fn t2byteatyped_lone_ff_byte_not_replaced_by_utf8_replacement() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id I64 NOT NULL, data BYTES(1) NOT NULL)");
        let cat = sm.catalog();
        let payload: Vec<u8> = vec![0xFF];
        let op = compile_with_params(
            "INSERT INTO t (id, data) VALUES ($1, $2)",
            cat,
            &[Some(Value::Int(1)), Some(Value::Blob(payload.clone()))],
        )
        .expect("compile_with_params ok");
        // The U+FFFD UTF-8 replacement bytes are `0xEF 0xBF 0xBD`.
        // They must NOT appear in the encoded record (they would
        // indicate the lossy-UTF8 regression took effect).
        let record_bytes = extract_create_record(&op);
        let replacement_appears = record_bytes
            .windows(3)
            .any(|w| w == [0xEF, 0xBF, 0xBD]);
        assert!(
            !replacement_appears,
            "the U+FFFD replacement bytes must NOT appear in the record \
             (indicates lossy-UTF8 regression); record = {record_bytes:?}",
        );
        // The 0xFF byte itself must appear at SOME offset (the data
        // payload).
        assert!(
            record_bytes.contains(&0xFF),
            "expected the 0xFF payload byte to appear verbatim in the \
             record {record_bytes:?}",
        );
    }

    /// `Value::Blob(b"42")` (valid UTF-8 ASCII decimal) bound to a
    /// numeric column still works — the UTF-8 + decimal coercion
    /// in `lit_to_value` for `Lit::Bytes` matches the `Lit::Str`
    /// path's SP-PG-SQL-PAREN-VALUES coercion. Locks the backward-
    /// compatible shape so existing psycopg2-binary-int patterns
    /// don't regress.
    #[test]
    fn t2byteatyped_blob_bytes_numeric_coerces_for_int_columns() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id I64 NOT NULL, n I64 NOT NULL)");
        let cat = sm.catalog();
        let via_params = compile_with_params(
            "INSERT INTO t (id, n) VALUES ($1, $2)",
            cat,
            &[
                Some(Value::Blob(b"1".to_vec())),
                Some(Value::Blob(b"42".to_vec())),
            ],
        )
        .expect("compile_with_params ok");
        let via_literal = compile(
            "INSERT INTO t (id, n) VALUES (1, 42)",
            cat,
        )
        .expect("compile literal ok");
        assert_eq!(format!("{via_params:?}"), format!("{via_literal:?}"));
    }

    /// UPDATE SET via `Tok::Bytes` route — the same UPDATE path that
    /// `compile_stmt_with_params` handles. Non-UTF8 bytes survive
    /// into the `Stmt::Update` sets vec.
    #[test]
    fn t2byteatyped_compile_stmt_update_set_blob_preserves_bytes() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id I64 NOT NULL, data BYTES(4) NOT NULL)");
        run(&mut sm, 2, "INSERT INTO t ID 7 (id, data) VALUES (7, 'init')");
        let cat = sm.catalog();
        let payload: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let stmt = compile_stmt_with_params(
            "UPDATE t ID 7 SET data = $1",
            cat,
            &[Some(Value::Blob(payload.clone()))],
        )
        .expect("compile_stmt_with_params ok");
        match stmt {
            Stmt::Update { sets, .. } => {
                let (_fid, v) = sets.first().expect("one SET");
                match v {
                    Value::Blob(b) => assert_eq!(
                        b,
                        &payload,
                        "Stmt::Update::sets must carry the exact payload \
                         bytes; got {b:?} vs payload {payload:?}",
                    ),
                    other => panic!("expected Value::Blob; got {other:?}"),
                }
            }
            _ => panic!("expected Stmt::Update"),
        }
    }

    /// `compile_stmt_with_params` threads params through the UPDATE
    /// path that bare `compile_stmt` handles. Stmt doesn't impl Debug
    /// so we destructure both into `Stmt::Update { type_id, id, sets }`
    /// and compare field-by-field.
    #[test]
    fn t2parsed_compile_stmt_with_params_update_set() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL)");
        // Seed a row so the UPDATE has a target.
        run(&mut sm, 2, "INSERT INTO t ID 7 (a) VALUES (1)");
        let cat = sm.catalog();
        let via_params = compile_stmt_with_params(
            "UPDATE t ID 7 SET a = $1",
            cat,
            &[Some(Value::Int(99))],
        ).expect("ok");
        let via_literal = compile_stmt(
            "UPDATE t ID 7 SET a = 99",
            cat,
        ).expect("ok");
        match (via_params, via_literal) {
            (
                Stmt::Update { type_id: t1, id: i1, sets: s1 },
                Stmt::Update { type_id: t2, id: i2, sets: s2 },
            ) => {
                assert_eq!(t1, t2);
                assert_eq!(i1, i2);
                assert_eq!(s1.len(), s2.len());
                for ((f1, v1), (f2, v2)) in s1.iter().zip(s2.iter()) {
                    assert_eq!(f1, f2);
                    // Value impls PartialEq for the simple variants
                    // we exercise (Int, Uint, Blob).
                    assert_eq!(v1, v2);
                }
            }
            (a, b) => {
                let _ = a; let _ = b;
                panic!("expected both compile paths to produce Stmt::Update");
            }
        }
    }

    // ============================================================
    // SP-PG-SQL-ORM-PARSE T2 — qualified columns (`table.col`).
    // SQLAlchemy / Django / Rails ALWAYS qualify columns with the
    // table name; these KATs lock the lenient-qualifier contract:
    // `t.col` compiles to the BYTE-IDENTICAL Op as bare `col`.
    // ============================================================

    /// `SELECT t.id, t.name FROM t` parses to a projection of [id, name]
    /// — the qualifier is stripped, and the compiled Op is identical to
    /// the bare-column `SELECT id, name FROM t`.
    #[test]
    fn ormparse_qualified_projection_strips_qualifier() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        // The ORM's `create_all` declares `id` as a real BIGINT column
        // (`CREATE TABLE orm_users (id BIGINT NOT NULL, name VARCHAR(32))`),
        // so `id` IS a projectable stored field here — exactly the
        // shape `SELECT orm_users.id, orm_users.name` must resolve.
        run(
            &mut sm,
            1,
            "CREATE TABLE orm_users (id BIGINT NOT NULL, name CHAR(32))",
        );
        let cat = sm.catalog();
        let qualified = compile(
            "SELECT orm_users.id, orm_users.name FROM orm_users",
            cat,
        )
        .expect("qualified projection compiles");
        let bare = compile("SELECT id, name FROM orm_users", cat)
            .expect("bare projection compiles");
        // Qualified and bare must be the SAME Op. Compare byte-for-byte.
        assert_eq!(
            qualified.encode(),
            bare.encode(),
            "qualified projection must compile byte-identically to bare"
        );
        // And it must be a SelectFields projection (not Select *).
        assert!(
            matches!(qualified, Op::SelectFields { .. }),
            "explicit projection list must emit Op::SelectFields, got {:?}",
            qualified.kind()
        );
    }

    /// `SELECT * FROM t WHERE t.id = 1` — qualified WHERE column on the
    /// PK. Must compile identically to the bare `WHERE id = 1` (same
    /// eq-hint, same program → same Op bytes).
    #[test]
    fn ormparse_qualified_where_pk_byte_identical() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (n U32 NOT NULL)");
        let cat = sm.catalog();
        let qualified =
            compile("SELECT * FROM t WHERE t.n = 5", cat).expect("ok");
        let bare = compile("SELECT * FROM t WHERE n = 5", cat).expect("ok");
        assert_eq!(
            qualified.encode(),
            bare.encode(),
            "qualified WHERE must compile byte-identically to bare WHERE \
             (determinism contract)"
        );
    }

    /// `SELECT users.id FROM users` — qualifier equals the table name,
    /// accepted; resolves to column `id`.
    #[test]
    fn ormparse_qualifier_equals_table_name_accepted() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE users (age U32 NOT NULL)");
        let cat = sm.catalog();
        // `users.age` qualified projection compiles (age is a real field).
        let op = compile("SELECT users.age FROM users", cat)
            .expect("qualifier=table name accepted");
        assert!(matches!(op, Op::SelectFields { .. }));
    }

    /// Qualified col in WHERE with a param: `WHERE t.id = $1`.
    /// After param substitution the qualified clause must compile
    /// identically to the bare param clause.
    #[test]
    fn ormparse_qualified_where_with_param() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (n U32 NOT NULL)");
        let cat = sm.catalog();
        let qualified = compile_with_params(
            "SELECT * FROM t WHERE t.n = $1",
            cat,
            &[Some(Value::Int(42))],
        )
        .expect("qualified param WHERE compiles");
        let bare = compile_with_params(
            "SELECT * FROM t WHERE n = $1",
            cat,
            &[Some(Value::Int(42))],
        )
        .expect("bare param WHERE compiles");
        assert_eq!(qualified.encode(), bare.encode());
    }

    /// Bare columns still compile (regression guard): the qualifier
    /// branch is purely additive.
    #[test]
    fn ormparse_bare_columns_regression() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a U32 NOT NULL, b CHAR(8))");
        let cat = sm.catalog();
        assert!(compile("SELECT * FROM t", cat).is_ok());
        assert!(compile("SELECT a, b FROM t", cat).is_ok());
        assert!(compile("SELECT * FROM t WHERE a = 1", cat).is_ok());
        assert!(compile("SELECT a FROM t ORDER BY a", cat).is_ok());
        assert!(compile("SELECT COUNT(*) FROM t", cat).is_ok());
    }

    /// ORM UPDATE shape: `UPDATE t SET name=$1 WHERE t.id = $2`. The
    /// standard `SET ... WHERE [t.]id = <n>` form maps to the id-based
    /// `Stmt::Update`, byte-identical to the legacy `UPDATE t ID n SET`.
    #[test]
    fn ormparse_update_set_where_id() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL)");
        run(&mut sm, 2, "INSERT INTO t ID 7 (a) VALUES (1)");
        let cat = sm.catalog();
        let ormstyle =
            compile_stmt("UPDATE t SET a = 99 WHERE t.id = 7", cat)
                .expect("ORM-style UPDATE compiles");
        let legacy = compile_stmt("UPDATE t ID 7 SET a = 99", cat)
            .expect("legacy UPDATE compiles");
        match (ormstyle, legacy) {
            (
                Stmt::Update { type_id: t1, id: i1, sets: s1 },
                Stmt::Update { type_id: t2, id: i2, sets: s2 },
            ) => {
                assert_eq!(t1, t2);
                assert_eq!(i1, i2, "WHERE id must resolve the same row id");
                assert_eq!(s1, s2, "SET clause must be identical");
            }
            _ => panic!("both must be Stmt::Update"),
        }
    }

    /// ORM DELETE shape: `DELETE FROM t WHERE t.id = $1` maps to the
    /// id-based Op::Delete, byte-identical to legacy `DELETE FROM t ID n`.
    #[test]
    fn ormparse_delete_where_id() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL)");
        let cat = sm.catalog();
        let ormstyle = compile("DELETE FROM t WHERE t.id = 3", cat)
            .expect("ORM-style DELETE compiles");
        let legacy =
            compile("DELETE FROM t ID 3", cat).expect("legacy DELETE compiles");
        assert_eq!(ormstyle.encode(), legacy.encode());
        assert!(matches!(ormstyle, Op::Delete { .. }));
    }

    // ---- SP-PG-SQL-DML-GENERAL (T3) — general-WHERE UPDATE/DELETE + RETURNING ----

    /// A non-PK WHERE in UPDATE now compiles to `Stmt::UpdateWhere`
    /// (general predicate), NOT the by-PK error. The carried `program`
    /// is the SAME kessel-expr bytes the equivalent SELECT WHERE emits.
    #[test]
    fn dmlgen_update_where_general_predicate() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL, b I64 NOT NULL)");
        let cat = sm.catalog();
        let stmt = compile_stmt(
            "UPDATE t SET b = 0 WHERE a < 150",
            cat,
        )
        .expect("general-WHERE UPDATE compiles");
        match stmt {
            Stmt::UpdateWhere { program, sets, returning, .. } => {
                assert_eq!(sets.len(), 1, "one SET");
                assert!(returning.is_none(), "no RETURNING");
                assert!(!program.is_empty(), "predicate program present");
                // Determinism: the SAME UPDATE compiles to the SAME
                // predicate program bytes every time.
                let again = compile_stmt("UPDATE t SET b = 0 WHERE a < 150", cat)
                    .expect("recompiles");
                match again {
                    Stmt::UpdateWhere { program: p2, .. } => assert_eq!(
                        program, p2,
                        "general-WHERE UPDATE must compile to a byte-identical \
                         predicate program (determinism contract)"
                    ),
                    _ => panic!("expected Stmt::UpdateWhere on recompile"),
                }
            }
            _ => panic!("expected Stmt::UpdateWhere"),
        }
    }

    /// `DELETE FROM t WHERE <general pred>` → `Stmt::DeleteWhere`.
    #[test]
    fn dmlgen_delete_where_general_predicate() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL)");
        let cat = sm.catalog();
        let stmt = compile_stmt("DELETE FROM t WHERE a = 7", cat)
            .expect("general-WHERE DELETE compiles");
        assert!(matches!(stmt, Stmt::DeleteWhere { .. }));
    }

    /// String-predicate DELETE: `DELETE FROM t WHERE s = 'expired'`.
    #[test]
    fn dmlgen_delete_where_string() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (s CHAR(8) NOT NULL)");
        let cat = sm.catalog();
        assert!(matches!(
            compile_stmt("DELETE FROM t WHERE s = 'expired'", cat)
                .expect("string DELETE compiles"),
            Stmt::DeleteWhere { .. }
        ));
    }

    /// `UPDATE … WHERE <pred> RETURNING *` captures the star sentinel.
    #[test]
    fn dmlgen_update_where_returning_star() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL, b I64 NOT NULL)");
        let cat = sm.catalog();
        match compile_stmt("UPDATE t SET b = 1 WHERE a = 2 RETURNING *", cat)
            .expect("UPDATE RETURNING * compiles")
        {
            Stmt::UpdateWhere { returning, .. } => {
                assert_eq!(returning, Some(vec!["*".to_string()]));
            }
            _ => panic!("expected Stmt::UpdateWhere"),
        }
    }

    /// `DELETE … RETURNING a, b` captures an explicit (qualifier-stripped)
    /// column list.
    #[test]
    fn dmlgen_delete_where_returning_cols() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL, b I64 NOT NULL)");
        let cat = sm.catalog();
        match compile_stmt(
            "DELETE FROM t WHERE a > 5 RETURNING t.a, b",
            cat,
        )
        .expect("DELETE RETURNING cols compiles")
        {
            Stmt::DeleteWhere { returning, .. } => {
                assert_eq!(
                    returning,
                    Some(vec!["a".to_string(), "b".to_string()])
                );
            }
            _ => panic!("expected Stmt::DeleteWhere"),
        }
    }

    /// By-PK `WHERE id = n` UPDATE/DELETE still take the single-row
    /// fast path (regression): UPDATE → `Stmt::Update`, DELETE →
    /// `Op::Delete` (NOT the general WHERE path).
    #[test]
    fn dmlgen_by_pk_fast_path_regression() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL)");
        let cat = sm.catalog();
        assert!(matches!(
            compile_stmt("UPDATE t SET a = 1 WHERE id = 3", cat).unwrap(),
            Stmt::Update { .. }
        ));
        assert!(matches!(
            compile_stmt("DELETE FROM t WHERE id = 3", cat).unwrap(),
            Stmt::Op(Op::Delete { .. })
        ));
    }

    /// An unguarded table-wide UPDATE/DELETE (no WHERE) is rejected in V1
    /// (footgun guard), naming neither a panic nor a silent full-table
    /// mutation.
    #[test]
    fn dmlgen_unguarded_update_delete_rejected() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I64 NOT NULL)");
        let cat = sm.catalog();
        assert!(compile_stmt("UPDATE t SET a = 1", cat).is_err());
        assert!(compile_stmt("DELETE FROM t", cat).is_err());
    }

    /// `dml_returning` (the gateway-side detector) parses UPDATE/DELETE
    /// RETURNING and returns `None` for the no-RETURNING case.
    #[test]
    fn dmlgen_dml_returning_helper() {
        assert_eq!(
            dml_returning("UPDATE t SET a = 1 WHERE b = 2 RETURNING *"),
            Some(("t".to_string(), vec!["*".to_string()]))
        );
        assert_eq!(
            dml_returning("DELETE FROM t WHERE b = 2 RETURNING t.a, b"),
            Some(("t".to_string(), vec!["a".to_string(), "b".to_string()]))
        );
        assert_eq!(dml_returning("UPDATE t SET a = 1 WHERE b = 2"), None);
        assert_eq!(dml_returning("SELECT * FROM t"), None);
    }

    /// SP-PG-SQL-ORM-PARSE T5 — SERIAL-family DDL aliases (SQLAlchemy
    /// renders a `BigInteger` PK as `BIGSERIAL`). Aliased to the plain
    /// integer width (no sequence; explicit-id inserts are faithful).
    #[test]
    fn ormparse_serial_aliases() {
        assert!(matches!(kind_of("BIGSERIAL", None), Ok(FieldKind::I64)));
        assert!(matches!(kind_of("bigserial", None), Ok(FieldKind::I64)));
        assert!(matches!(kind_of("SERIAL", None), Ok(FieldKind::I32)));
        assert!(matches!(kind_of("SMALLSERIAL", None), Ok(FieldKind::I16)));
        // A real CREATE TABLE with the ORM's exact BIGSERIAL PK compiles.
        let cat = Catalog::default();
        assert!(compile(
            "CREATE TABLE orm_users (id BIGSERIAL NOT NULL, name VARCHAR(32))",
            &cat
        )
        .is_ok());
    }

    /// SP-PG-SQL-ORM-PARSE T5 — the EXACT `create_all` DDL shape:
    /// table-level `PRIMARY KEY (id)` constraint is accept-and-skipped;
    /// inline `PRIMARY KEY` modifier also accepted.
    #[test]
    fn ormparse_create_table_primary_key() {
        let cat = Catalog::default();
        // Table-level PK constraint (SQLAlchemy create_all shape).
        let op = compile(
            "CREATE TABLE orm_users (id BIGSERIAL NOT NULL, \
             name VARCHAR(32), PRIMARY KEY (id))",
            &cat,
        )
        .expect("table-level PRIMARY KEY compiles");
        // The stored type has exactly the 2 declared columns (PK clause
        // is metadata, not a 3rd column).
        match op {
            Op::CreateType { def } => {
                let (nm, flds) =
                    kessel_catalog::decode_type_def(&def).expect("decode");
                assert_eq!(nm, "orm_users");
                assert_eq!(flds.len(), 2, "PK clause must NOT add a column");
                assert_eq!(flds[0].name, "id");
                assert_eq!(flds[1].name, "name");
            }
            o => panic!("expected CreateType, got {:?}", o.kind()),
        }
        // Inline PK modifier.
        assert!(compile(
            "CREATE TABLE t2 (id BIGSERIAL PRIMARY KEY, v U32)",
            &cat
        )
        .is_ok());
    }

    /// SP-PG-SQL-ORM-PARSE T4 — `col = ANY (ARRAY[v1, v2, v3])` desugars
    /// to `col IN (v1, v2, v3)` ≡ OR-of-eq, compiling to the
    /// BYTE-IDENTICAL Op as the explicit IN list.
    #[test]
    fn ormparse_any_array_desugars_to_in() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (k U32 NOT NULL)");
        let cat = sm.catalog();
        let any =
            compile("SELECT * FROM t WHERE k = ANY(ARRAY[1,2,3])", cat)
                .expect("ANY(ARRAY[...]) compiles");
        let in_list =
            compile("SELECT * FROM t WHERE k IN (1,2,3)", cat)
                .expect("IN list compiles");
        assert_eq!(
            any.encode(),
            in_list.encode(),
            "= ANY(ARRAY[...]) must compile byte-identically to IN (...)"
        );
    }

    /// `= ANY (ARRAY[...])` with a single element + with string literals
    /// (the create_all `relkind = ANY (ARRAY['r','p'])` shape) both
    /// parse and match their IN equivalents.
    #[test]
    fn ormparse_any_array_strings_and_singleton() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (s CHAR(4) NOT NULL)");
        let cat = sm.catalog();
        let any = compile(
            "SELECT * FROM t WHERE s = ANY (ARRAY['r', 'p'])",
            cat,
        )
        .expect("string ANY(ARRAY) compiles");
        let in_list =
            compile("SELECT * FROM t WHERE s IN ('r', 'p')", cat)
                .expect("string IN compiles");
        assert_eq!(any.encode(), in_list.encode());
        // Singleton.
        assert!(compile(
            "SELECT * FROM t WHERE s = ANY(ARRAY['x'])",
            cat
        )
        .is_ok());
    }

    /// The lexer no longer rejects `[`; a bare/garbage `[` outside
    /// `ARRAY[...]` still produces a clean parse error (not a panic).
    #[test]
    fn ormparse_bracket_lexes_not_unexpected_char() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (k U32 NOT NULL)");
        let cat = sm.catalog();
        // `[` no longer "unexpected char '['" — it tokenizes; the parser
        // then rejects the malformed shape with a grammar error.
        let err = compile("SELECT * FROM t WHERE k = [1,2]", cat)
            .unwrap_err();
        assert!(
            !err.contains("unexpected char '['"),
            "`[` must lex (no unexpected-char error); got: {err}"
        );
    }

    /// `select_columns` (the gateway's projection detector) accepts the
    /// qualified shape and returns the BARE column names in order, so the
    /// RowDescription matches the engine's projected output.
    #[test]
    fn ormparse_select_columns_qualified() {
        assert_eq!(
            select_columns("SELECT t.id, t.name FROM t"),
            Some(("t".to_string(), vec!["id".to_string(), "name".to_string()]))
        );
        // Bare still works (regression).
        assert_eq!(
            select_columns("SELECT id, name FROM t"),
            Some(("t".to_string(), vec!["id".to_string(), "name".to_string()]))
        );
        // `SELECT *` is NOT a projection list → None (the star path).
        assert_eq!(select_columns("SELECT * FROM t"), None);
    }

    // ---- SP-PG-SERIAL-RETURNING (T3) ----

    /// `CREATE TABLE … id BIGSERIAL PRIMARY KEY` flags the type as a
    /// deterministic-autoincrement serial PK (inline-modifier shape).
    #[test]
    fn serial_pk_inline_create_table_flags_the_type() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE widgets (id BIGSERIAL PRIMARY KEY, name VARCHAR(8))");
        let t = sm.catalog().types.iter().find(|t| t.name == "widgets").unwrap();
        assert!(t.serial_pk, "serial PK must be flagged");
        // The serial column `id` is field 1 (first column).
        assert_eq!(t.serial_field_id, Some(1));
    }

    /// The table-level `PRIMARY KEY (id)` constraint shape (SQLAlchemy
    /// create_all) also flags the serial PK.
    #[test]
    fn serial_pk_table_constraint_create_table_flags_the_type() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE w (id BIGSERIAL NOT NULL, name VARCHAR(8), PRIMARY KEY (id))");
        let t = sm.catalog().types.iter().find(|t| t.name == "w").unwrap();
        assert!(t.serial_pk);
        assert_eq!(t.serial_field_id, Some(1));
    }

    // ---- SP-PG-DDL-IDENTITY — `GENERATED … AS IDENTITY` (Django 6 PK) ----

    /// Django 6's default `BigAutoField` PK DDL —
    /// `"id" bigint NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY`
    /// — flags the column as a deterministic-autoincrement serial PK,
    /// identical to `id BIGSERIAL PRIMARY KEY`. The declared type stays
    /// I64 (bigint).
    #[test]
    fn identity_by_default_pk_flags_serial_like_bigserial() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(
            &mut sm,
            1,
            "CREATE TABLE t (\"id\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, \"name\" VARCHAR(32))",
        );
        let t = sm.catalog().types.iter().find(|t| t.name == "t").unwrap();
        assert!(t.serial_pk, "IDENTITY PK must flag serial autoincrement");
        assert_eq!(t.serial_field_id, Some(1));
        assert!(matches!(t.fields[0].kind, FieldKind::I64), "bigint → I64");
    }

    /// `GENERATED ALWAYS AS IDENTITY` is also accepted.
    #[test]
    fn identity_always_pk_flags_serial() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(
            &mut sm,
            1,
            "CREATE TABLE t (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name VARCHAR(8))",
        );
        let t = sm.catalog().types.iter().find(|t| t.name == "t").unwrap();
        assert!(t.serial_pk);
        assert_eq!(t.serial_field_id, Some(1));
    }

    /// The exact Django 6 modifier RUN order
    /// (`NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY`) parses,
    /// flags serial PK, and honors NOT NULL.
    #[test]
    fn identity_django_modifier_run_order() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(
            &mut sm,
            1,
            "CREATE TABLE \"smokeapp_author\" (\"id\" bigint NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY, \"name\" varchar(32) NOT NULL)",
        );
        let t = sm
            .catalog()
            .types
            .iter()
            .find(|t| t.name == "smokeapp_author")
            .unwrap();
        assert!(t.serial_pk);
        assert_eq!(t.serial_field_id, Some(1));
        assert!(!t.fields[0].nullable, "PRIMARY KEY / NOT NULL ⇒ not null");
    }

    /// Sequence options `( START WITH 1 INCREMENT BY 1 )` are
    /// parsed-and-ignored (V1) — the table still flags serial PK.
    #[test]
    fn identity_with_sequence_options_parsed_and_ignored() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(
            &mut sm,
            1,
            "CREATE TABLE t (id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY, name VARCHAR(8))",
        );
        let t = sm.catalog().types.iter().find(|t| t.name == "t").unwrap();
        assert!(t.serial_pk);
        assert_eq!(t.serial_field_id, Some(1));
    }

    /// An INSERT that OMITS the IDENTITY column triggers the same
    /// SERIAL_SENTINEL autoincrement path as BIGSERIAL (engine assigns).
    #[test]
    fn identity_insert_omitting_id_compiles_to_sentinel_create() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(
            &mut sm,
            1,
            "CREATE TABLE t (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name VARCHAR(8))",
        );
        let cat = sm.catalog();
        let op = compile("INSERT INTO t (name) VALUES ('x')", cat)
            .expect("IDENTITY autoincrement INSERT compiles");
        match op {
            Op::Create { id, .. } => {
                assert_eq!(id, ObjectId::from_u128(u128::MAX), "SERIAL_SENTINEL");
            }
            other => panic!("expected Op::Create with sentinel, got {other:?}"),
        }
    }

    /// A plain (non-serial) PK does NOT flag autoincrement — regression
    /// guard so ordinary tables keep requiring an explicit id.
    #[test]
    fn non_serial_pk_is_not_autoincrement() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE p (id I64 NOT NULL, name VARCHAR(8), PRIMARY KEY (id))");
        let t = sm.catalog().types.iter().find(|t| t.name == "p").unwrap();
        assert!(!t.serial_pk);
        assert_eq!(t.serial_field_id, None);
    }

    /// An INSERT that OMITS the id on a serial_pk type compiles to an
    /// `Op::Create` carrying the SERIAL_SENTINEL id (u128::MAX) so the SM
    /// autoincrements. A non-serial table still errors on a missing id.
    #[test]
    fn serial_insert_omitting_id_compiles_to_sentinel_create() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE widgets (id BIGSERIAL PRIMARY KEY, name VARCHAR(8))");
        let cat = sm.catalog();
        let op = compile("INSERT INTO widgets (name) VALUES ('gadget')", cat)
            .expect("autoincrement INSERT compiles");
        match op {
            Op::Create { id, .. } => {
                assert_eq!(id, ObjectId::from_u128(u128::MAX), "must carry the serial sentinel");
            }
            o => panic!("expected Create, got {:?}", o.kind()),
        }
        // RETURNING is tolerated (the parser returns before the clause).
        assert!(compile(
            "INSERT INTO widgets (name) VALUES ('x') RETURNING id",
            cat
        )
        .is_ok());
    }

    /// SQLAlchemy's post-flush refresh `SELECT col AS alias … FROM t
    /// WHERE id = n` compiles: the projection alias is accept-and-skipped
    /// and the source columns project as usual.
    #[test]
    fn select_projection_with_as_alias_compiles() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE widgets (id BIGSERIAL PRIMARY KEY, name VARCHAR(8))");
        let cat = sm.catalog();
        // Both the parser (compile) and the gateway projection detector
        // (select_columns) must accept the aliased shape.
        assert!(compile(
            "SELECT widgets.id AS widgets_id, widgets.name AS widgets_name FROM widgets WHERE widgets.id = 1",
            cat
        )
        .is_ok(), "aliased projection must compile");
        assert_eq!(
            select_columns("SELECT widgets.id AS widgets_id, widgets.name AS widgets_name FROM widgets"),
            Some(("widgets".to_string(), vec!["id".to_string(), "name".to_string()]))
        );
    }

    // ---- SP-PG-SQL-AGG-ALIAS-RENDER — aggregate alias parse + helper ----

    /// `SELECT COUNT(*) FROM t` compiles to `Op::Aggregate` COUNT (kind 0).
    #[test]
    fn select_count_star_from_compiles_to_aggregate() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id BIGSERIAL PRIMARY KEY, name VARCHAR(8))");
        let cat = sm.catalog();
        let op = compile("SELECT COUNT(*) FROM t", cat).expect("compile");
        match op {
            Op::Aggregate { kind, .. } => assert_eq!(kind, 0, "COUNT"),
            other => panic!("expected Op::Aggregate, got {other:?}"),
        }
    }

    /// `SELECT COUNT(*) AS "__count" FROM t` (Django's `.count()`) compiles
    /// — the quoted alias is captured and accept-and-skipped; the emitted
    /// Op is byte-identical to the unaliased COUNT.
    #[test]
    fn select_count_star_with_quoted_alias_compiles() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id BIGSERIAL PRIMARY KEY, name VARCHAR(8))");
        let cat = sm.catalog();
        let aliased = compile("SELECT COUNT(*) AS \"__count\" FROM t", cat)
            .expect("aliased COUNT compiles");
        let bare = compile("SELECT COUNT(*) FROM t", cat).expect("bare COUNT compiles");
        assert_eq!(
            format!("{aliased:?}"),
            format!("{bare:?}"),
            "alias must not change the emitted Op"
        );
    }

    /// `SELECT SUM("bal") AS "total" FROM "acct"` parses (quoted ident +
    /// quoted alias).
    #[test]
    fn select_sum_with_alias_compiles() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE acct (id BIGSERIAL PRIMARY KEY, bal I64 NOT NULL)");
        let cat = sm.catalog();
        let op = compile("SELECT SUM(\"bal\") AS \"total\" FROM \"acct\"", cat)
            .expect("SUM alias compiles");
        match op {
            Op::Aggregate { kind, .. } => assert_eq!(kind, 1, "SUM"),
            other => panic!("expected Op::Aggregate, got {other:?}"),
        }
    }

    /// `select_aggregate` detects the bare + aliased scalar-aggregate shape
    /// and returns kind/field/alias; `None` for non-aggregate SELECTs.
    #[test]
    fn select_aggregate_helper_detects_shape() {
        // Bare COUNT(*) — default name applies (alias None).
        assert_eq!(
            select_aggregate("SELECT COUNT(*) FROM t"),
            Some(SelectAgg { table: "t".into(), kind: 0, field: None, alias: None })
        );
        // Aliased COUNT(*) (Django quotes the alias → lexes as Ident).
        assert_eq!(
            select_aggregate("SELECT COUNT(*) AS \"__count\" FROM \"t\""),
            Some(SelectAgg {
                table: "t".into(),
                kind: 0,
                field: None,
                alias: Some("__count".into()),
            })
        );
        // SUM over a column with qualifier + alias.
        assert_eq!(
            select_aggregate("SELECT SUM(acct.bal) AS total FROM acct"),
            Some(SelectAgg {
                table: "acct".into(),
                kind: 1,
                field: Some("bal".into()),
                alias: Some("total".into()),
            })
        );
        // Trailing `;` tolerated.
        assert_eq!(
            select_aggregate("SELECT COUNT(*) FROM t;"),
            Some(SelectAgg { table: "t".into(), kind: 0, field: None, alias: None })
        );
        // NOT a scalar aggregate → None (so existing render shapes win).
        assert_eq!(select_aggregate("SELECT * FROM t"), None);
        assert_eq!(select_aggregate("SELECT id, name FROM t"), None);
        // GROUP BY is the multi/grouped path, not the bare scalar shape.
        assert_eq!(select_aggregate("SELECT COUNT(*) FROM t GROUP BY g"), None);
        // Default-name lookup.
        assert_eq!(agg_default_name(0), "count");
        assert_eq!(agg_default_name(1), "sum");
    }

    /// `insert_returning` parses the clause + column list (qualified or
    /// bare), and returns None for an INSERT without RETURNING.
    #[test]
    fn insert_returning_parses_columns() {
        assert_eq!(
            insert_returning("INSERT INTO widgets (name) VALUES ('a') RETURNING id"),
            Some(("widgets".to_string(), vec!["id".to_string()]))
        );
        assert_eq!(
            insert_returning("INSERT INTO w (name) VALUES ('a') RETURNING id, name"),
            Some(("w".to_string(), vec!["id".to_string(), "name".to_string()]))
        );
        // Qualified returned column is stripped (lenient).
        assert_eq!(
            insert_returning("INSERT INTO w (name) VALUES ('a') RETURNING w.id"),
            Some(("w".to_string(), vec!["id".to_string()]))
        );
        // Trailing semicolon tolerated.
        assert_eq!(
            insert_returning("INSERT INTO w (name) VALUES ('a') RETURNING id;"),
            Some(("w".to_string(), vec!["id".to_string()]))
        );
        // No RETURNING → None.
        assert_eq!(insert_returning("INSERT INTO w (name) VALUES ('a')"), None);
        // Non-INSERT → None.
        assert_eq!(insert_returning("SELECT id FROM w"), None);
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-RETURNING-MULTIROW-STAR (T3) — multi-row INSERT RETURNING +
    // `RETURNING *`.
    // ───────────────────────────────────────────────────────────────────

    /// A multi-row INSERT compiles to ONE `Op::Txn` of N Creates (SP58),
    /// AND `insert_returning` still locates the trailing RETURNING clause
    /// past the multi-tuple VALUES body — so the gateway knows to surface
    /// the per-row ids.
    #[test]
    fn multirow_insert_returning_parses() {
        // Compile → Op::Txn (the multi-row INSERT shape).
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE widgets (id BIGSERIAL PRIMARY KEY, name VARCHAR(8))");
        let cat = sm.catalog();
        let op = compile(
            "INSERT INTO widgets (name) VALUES ('a'),('b'),('c') RETURNING id",
            cat,
        )
        .expect("multi-row INSERT RETURNING compiles");
        match op {
            Op::Txn { ops } => assert_eq!(ops.len(), 3, "3 Creates in the Txn"),
            other => panic!("expected Op::Txn, got {:?}", other.kind()),
        }
        // The RETURNING clause is found past the multi-tuple VALUES body.
        assert_eq!(
            insert_returning(
                "INSERT INTO widgets (name) VALUES ('a'),('b'),('c') RETURNING id"
            ),
            Some(("widgets".to_string(), vec!["id".to_string()]))
        );
    }

    /// `RETURNING *` → the star sentinel `["*"]` the gateway expands to
    /// every table column.
    #[test]
    fn insert_returning_star_yields_star_sentinel() {
        assert_eq!(
            insert_returning("INSERT INTO w (name) VALUES ('a') RETURNING *"),
            Some(("w".to_string(), vec!["*".to_string()]))
        );
        // Trailing semicolon tolerated.
        assert_eq!(
            insert_returning("INSERT INTO w (name) VALUES ('a') RETURNING *;"),
            Some(("w".to_string(), vec!["*".to_string()]))
        );
        // Multi-row + `RETURNING *` also yields the star sentinel.
        assert_eq!(
            insert_returning(
                "INSERT INTO w (name) VALUES ('a'),('b') RETURNING *"
            ),
            Some(("w".to_string(), vec!["*".to_string()]))
        );
        // `RETURNING *, id` (star mixed with a column) is NOT supported in
        // V1 → None (not a silent partial parse).
        assert_eq!(
            insert_returning("INSERT INTO w (name) VALUES ('a') RETURNING *, id"),
            None
        );
    }

    /// Explicit column list still parses (regression — the star branch
    /// must not swallow the named-column path).
    #[test]
    fn insert_returning_explicit_list_unaffected_by_star_branch() {
        assert_eq!(
            insert_returning("INSERT INTO w (name) VALUES ('a') RETURNING id, name"),
            Some(("w".to_string(), vec!["id".to_string(), "name".to_string()]))
        );
    }

    /// SP-PG-RETURNING-MULTIROW-STAR: SQLAlchemy's insertmanyvalues
    /// RETURNING clause `RETURNING widgets.id, widgets.id AS id__1`
    /// accept-and-skips the `AS <alias>` and the qualifier — both
    /// positions resolve to the `id` source column (SQLAlchemy maps both).
    #[test]
    fn insert_returning_skips_as_alias() {
        assert_eq!(
            insert_returning(
                "INSERT INTO widgets (name) VALUES ('a') RETURNING widgets.id, widgets.id AS id__1"
            ),
            Some(("widgets".to_string(), vec!["id".to_string(), "id".to_string()]))
        );
        // Single aliased column.
        assert_eq!(
            insert_returning("INSERT INTO w (n) VALUES ('a') RETURNING id AS x"),
            Some(("w".to_string(), vec!["id".to_string()]))
        );
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-SQL-QUOTED-IDENT KATs — the lexer accepts SQL-standard
    // double-quoted (delimited) identifiers everywhere a bare identifier
    // works. Django double-quotes EVERY identifier; this is the P0
    // keystone that unblocks the Django ORM CRUD surface.
    // ───────────────────────────────────────────────────────────────────

    /// Lexer: a quoted identifier lowers to the SAME `Tok::Ident` a bare
    /// identifier of that (case-preserved) name produces — so every
    /// identifier-position consumer works unchanged.
    #[test]
    fn quoted_ident_lexes_as_plain_ident() {
        assert_eq!(
            lex(r#"SELECT "id", "name" FROM "users""#).unwrap(),
            vec![
                Tok::Ident("SELECT".into()),
                Tok::Ident("id".into()),
                Tok::Punct(','),
                Tok::Ident("name".into()),
                Tok::Ident("FROM".into()),
                Tok::Ident("users".into()),
            ]
        );
    }

    /// Lexer: a quoted-on-both-sides qualified ref `"t"."col"` lexes as
    /// `Ident(t) . Ident(col)` — identical to the bare `t.col` stream.
    #[test]
    fn quoted_qualified_ref_lexes() {
        assert_eq!(
            lex(r#""t"."col""#).unwrap(),
            vec![
                Tok::Ident("t".into()),
                Tok::Punct('.'),
                Tok::Ident("col".into()),
            ]
        );
    }

    /// Lexer: a delimited identifier preserves a space (a bare identifier
    /// can't contain one) and case.
    #[test]
    fn quoted_ident_preserves_space_and_case() {
        assert_eq!(
            lex(r#""My Col""#).unwrap(),
            vec![Tok::Ident("My Col".into())]
        );
    }

    /// Lexer: the doubled-`""` escape yields a single literal `"` inside
    /// the identifier (`"a""b"` → `a"b`).
    #[test]
    fn quoted_ident_doubled_quote_escape() {
        assert_eq!(
            lex(r#""a""b""#).unwrap(),
            vec![Tok::Ident("a\"b".into())]
        );
    }

    /// Lexer: an unterminated quoted identifier is a clean error (not a
    /// panic / not silently swallowing the rest of the statement).
    #[test]
    fn quoted_ident_unterminated_errors() {
        assert!(lex(r#"SELECT "id FROM t"#)
            .unwrap_err()
            .contains("unterminated quoted identifier"));
    }

    /// Lexer: a zero-length delimited identifier `""` is rejected (PG
    /// rejects it too).
    #[test]
    fn quoted_ident_zero_length_errors() {
        assert!(lex(r#"SELECT "" FROM t"#)
            .unwrap_err()
            .contains("zero-length delimited identifier"));
    }

    /// Parse parity: a fully-quoted SELECT compiles to the SAME `Op` as
    /// the bare-identifier equivalent. This is the core compat invariant —
    /// quoting is transparent at the compiled-Op layer.
    #[test]
    fn quoted_select_compiles_same_op_as_bare() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR(32) NOT NULL)");
        let cat = sm.catalog();
        let bare = compile("SELECT * FROM users WHERE id = 1", cat).unwrap();
        let quoted = compile(r#"SELECT * FROM "users" WHERE "id" = 1"#, cat).unwrap();
        assert_eq!(bare, quoted);
        // Qualified-on-both-sides `"users"."id"` resolves the same.
        let qualified =
            compile(r#"SELECT * FROM "users" WHERE "users"."id" = 1"#, cat).unwrap();
        assert_eq!(bare, qualified);
    }

    /// Parse: a quoted projection list `SELECT "id", "name" FROM "users"`
    /// is recognized by the gateway's `select_columns` detector exactly
    /// like the bare form.
    #[test]
    fn quoted_projection_detected_by_select_columns() {
        assert_eq!(
            select_columns(r#"SELECT "id", "name" FROM "users""#),
            Some((
                "users".to_string(),
                vec!["id".to_string(), "name".to_string()]
            ))
        );
        // Qualified-quoted columns strip to the bare column name.
        assert_eq!(
            select_columns(
                r#"SELECT "u"."id", "u"."name" FROM "users""#
            ),
            Some((
                "users".to_string(),
                vec!["id".to_string(), "name".to_string()]
            ))
        );
    }

    /// Parse: quoted CREATE TABLE then quoted INSERT then quoted
    /// SELECT/UPDATE/DELETE all round-trip on the SAME identifier strings —
    /// the exact Django CRUD shape (minus the IDENTITY DDL spelling, which
    /// is the separate `SP-PG-DDL-IDENTITY` follow-up).
    #[test]
    fn quoted_full_crud_round_trip() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        // Quoted DDL — case-preserved names stored in the catalog.
        run(
            &mut sm,
            1,
            r#"CREATE TABLE "t" ("id" BIGINT NOT NULL, "name" VARCHAR(32) NOT NULL)"#,
        );
        // The catalog stored the UNquoted spelling (quotes are delimiters,
        // not part of the name).
        assert!(sm.catalog().types.iter().any(|ty| ty.name == "t"));
        // Quoted INSERT agrees with the quoted DDL identifiers.
        run(
            &mut sm,
            2,
            r#"INSERT INTO "t" ("id", "name") VALUES (1, 'tolkien')"#,
        );
        // Quoted SELECT projection.
        match run(&mut sm, 3, r#"SELECT "id", "name" FROM "t""#) {
            OpResult::Got(_) => {}
            o => panic!("quoted SELECT projection failed: {o:?}"),
        }
        // Quoted by-PK UPDATE / DELETE go through the server-side
        // `compile_stmt` path (read-modify-write / general WHERE), not the
        // `compile`→apply helper. Assert the quoted forms COMPILE against
        // the catalog exactly like the bare forms — proving the quoting is
        // transparent through the full statement compiler, not just the
        // single-Op `compile` path.
        let cat = sm.catalog();
        assert!(
            compile_stmt(r#"UPDATE "t" SET "name" = 'x' WHERE "id" = 1"#, cat).is_ok(),
            "quoted UPDATE must compile"
        );
        assert!(
            compile_stmt("UPDATE t SET name = 'x' WHERE id = 1", cat).is_ok(),
            "bare UPDATE must compile (parity)"
        );
        assert!(
            compile_stmt(r#"DELETE FROM "t" WHERE "id" = 1"#, cat).is_ok(),
            "quoted DELETE must compile"
        );
    }

    /// Parse: a mix of quoted and bare identifiers in one statement
    /// resolves to the same columns (`SELECT "id", name FROM t`).
    #[test]
    fn quoted_and_bare_mix_compiles() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (id BIGINT NOT NULL, name VARCHAR(32) NOT NULL)");
        let cat = sm.catalog();
        let all_bare = select_columns("SELECT id, name FROM t");
        let mixed = select_columns(r#"SELECT "id", name FROM t"#);
        assert_eq!(all_bare, mixed);
        // And it actually compiles against the catalog.
        assert!(compile(r#"SELECT "id", name FROM "t""#, cat).is_ok());
    }

    /// Regression: bare identifiers still lex + compile byte-identically
    /// (the quoted-ident lexer arm must not perturb the bare path).
    #[test]
    fn bare_idents_unchanged_regression() {
        assert_eq!(
            lex("SELECT id, name FROM users").unwrap(),
            vec![
                Tok::Ident("SELECT".into()),
                Tok::Ident("id".into()),
                Tok::Punct(','),
                Tok::Ident("name".into()),
                Tok::Ident("FROM".into()),
                Tok::Ident("users".into()),
            ]
        );
    }

    /// Parse: quoted RETURNING clause (`RETURNING "t"."id"`) — the exact
    /// shape Django's autoincrement INSERT emits — strips to the bare
    /// `id` column, same as the unquoted form.
    #[test]
    fn quoted_returning_detected() {
        assert_eq!(
            insert_returning(
                r#"INSERT INTO "t" ("name") VALUES ('a') RETURNING "t"."id""#
            ),
            Some(("t".to_string(), vec!["id".to_string()]))
        );
    }

    /// SP-PG-ORM-REALAPP — `select_projection_is_sorted` flags the projection
    /// + ORDER BY shape (which lowers to `Op::SelectSorted` → full records) so
    /// the gateway decodes the right stream shape.
    #[test]
    fn projection_sorted_detection() {
        assert!(select_projection_is_sorted(
            "SELECT title FROM posts ORDER BY title LIMIT 2"
        ));
        assert!(select_projection_is_sorted(
            "SELECT posts.title FROM posts ORDER BY posts.title"
        ));
        assert!(select_projection_is_sorted(
            "SELECT id, title FROM posts ORDER BY title DESC"
        ));
        // No ORDER BY → narrow projection path (false).
        assert!(!select_projection_is_sorted("SELECT title FROM posts"));
        assert!(!select_projection_is_sorted(
            "SELECT title FROM posts LIMIT 2"
        ));
        // SELECT * (not a projection list) → false (rendered by a different
        // path that already handles full records).
        assert!(!select_projection_is_sorted(
            "SELECT * FROM posts ORDER BY title"
        ));
        // Aggregate → false.
        assert!(!select_projection_is_sorted(
            "SELECT COUNT(*) FROM posts ORDER BY title"
        ));
    }

    /// SP-PG-ORM-REALAPP — the SQL-standard doubled-quote escape inside a
    /// string literal. `'bob''s post'` is the single value `bob's post`,
    /// NOT a truncated `bob` plus a parse error. Any app whose data carries
    /// an apostrophe emits this.
    #[test]
    fn doubled_quote_string_escape() {
        assert_eq!(
            lex("'bob''s post'").unwrap(),
            vec![Tok::Str("bob's post".into())]
        );
        // Multiple escapes + an empty escaped-quote-only string.
        assert_eq!(
            lex("'a''b''c'").unwrap(),
            vec![Tok::Str("a'b'c".into())]
        );
        assert_eq!(lex("''''").unwrap(), vec![Tok::Str("'".into())]);
        // A plain string with no embedded quote is byte-identical to the
        // pre-arc token (regression guard for every prior literal KAT).
        assert_eq!(
            lex("'hello world'").unwrap(),
            vec![Tok::Str("hello world".into())]
        );
        // A trailing lone quote still closes the string cleanly; an empty
        // string is empty.
        assert_eq!(lex("''").unwrap(), vec![Tok::Str("".into())]);
    }

    // ── SP-PG-NULL-INT-RENDER — projection→`SELECT *` rewrite ──────────────

    #[test]
    fn select_projection_to_star_basic() {
        assert_eq!(
            select_projection_to_star("SELECT a, b FROM t").as_deref(),
            Some("SELECT * FROM t")
        );
    }

    #[test]
    fn select_projection_to_star_preserves_where() {
        assert_eq!(
            select_projection_to_star("SELECT id FROM child WHERE id = 12")
                .as_deref(),
            Some("SELECT * FROM child WHERE id = 12")
        );
    }

    #[test]
    fn select_projection_to_star_single_col_qualified() {
        // Qualified projection `t.col` still rewrites to `SELECT *`.
        assert_eq!(
            select_projection_to_star("SELECT child.parent_id FROM child")
                .as_deref(),
            Some("SELECT * FROM child")
        );
    }

    #[test]
    fn select_projection_to_star_rejects_star_and_join_and_agg() {
        // `SELECT *` is not a projection list → None (the caller already
        // renders it via the full-record path).
        assert_eq!(select_projection_to_star("SELECT * FROM t"), None);
        // Aggregate → None.
        assert_eq!(select_projection_to_star("SELECT COUNT(*) FROM t"), None);
        // JOIN → None (different wire shape).
        assert_eq!(
            select_projection_to_star("SELECT a.x FROM a JOIN b ON a.id = b.id"),
            None
        );
    }

    #[test]
    fn select_projection_to_star_keyword_boundary_not_in_ident() {
        // A column named with an embedded `from` substring must NOT confuse
        // the FROM-keyword finder (token boundary aware).
        assert_eq!(
            select_projection_to_star("SELECT from_date FROM events").as_deref(),
            Some("SELECT * FROM events")
        );
    }

    #[test]
    fn find_keyword_boundary_skips_quoted_from() {
        // A `FROM` inside a quoted span is not matched; the real keyword after
        // the string is. (Defensive — projection lists have no literals.)
        let s = "'a from b' FROM t";
        let off = find_keyword_boundary(s, "FROM").unwrap();
        assert_eq!(&s[off..off + 4], "FROM");
        assert!(off > 9, "must skip the FROM inside the quoted span");
    }
}
