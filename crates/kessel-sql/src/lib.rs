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
            i += 1;
            let mut st = String::new();
            while i < b.len() && b[i] as char != '\'' {
                st.push(b[i] as char);
                i += 1;
            }
            if i >= b.len() {
                return Err("unterminated string".into());
            }
            i += 1;
            out.push(Tok::Str(st));
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
                '(' | ')' | ',' | ';' | '.' => {
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
        other => return Err(format!("unknown type `{other}`")),
    })
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
    // A JOIN produces composite rows (different wire shape) — bail out.
    if let Some(Tok::Ident(k)) = it.next() {
        if k.eq_ignore_ascii_case("JOIN") {
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
                // `FUNC(` ⇒ aggregate/expr — not a plain column list.
                if matches!(it.peek(), Some(Tok::Punct('('))) {
                    return None;
                }
                cols.push(c.clone());
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
        if k.eq_ignore_ascii_case("JOIN") {
            return None; // composite rows — different wire shape
        }
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
            p.expect_kw("ID")?;
            let id = match p.next() {
                Some(Tok::Int(n)) => n as u128,
                _ => return Err("UPDATE needs `ID <int>`".into()),
            };
            p.expect_kw("SET")?;
            let ot = p.type_named(&tname)?.clone();
            let mut sets = Vec::new();
            loop {
                let col = p.ident()?;
                match p.next() {
                    Some(Tok::Cmp("=")) => {}
                    _ => return Err("expected `=`".into()),
                }
                let lit = match p.next() {
                    Some(Tok::Int(n)) => Lit::Int(n),
                    Some(Tok::Str(s)) => Lit::Str(s),
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
            return Ok(Stmt::Update {
                type_id: ot.type_id,
                id,
                sets,
            });
        }
    }
    Ok(Stmt::Op(compile_from_tokens(toks, cat)?))
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
/// → `Tok::Str(utf8-lossy-cast)`, `Some(Value::Null)` / `None` →
/// `Tok::Ident("NULL")`. Out-of-bounds `$N` returns `SqlError`. The
/// rewritten token stream is handed to the existing parser unchanged —
/// the compiled `Op` is byte-identical to what you'd get from the
/// equivalent SQL with literal values in place of the placeholders.
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
/// - `Some(Value::Blob(b))` → `Tok::Str(utf8-lossy-cast)`. The bytes
///   are LITERAL string content; the parser will route them to
///   `lit_to_value` which already accepts string-shaped numerics
///   for numeric columns (SP-PG-SQL-PAREN-VALUES coercion).
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
                        // utf8-lossy is correct for V1: the parser
                        // accepts the string in literal positions
                        // (numeric coercion via lit_to_value for
                        // numeric columns; bytes pass-through for
                        // CHAR/BYTES). The bytes NEVER touch SQL
                        // text — they become a single `Tok::Str`
                        // operand carried verbatim into `Lit::Str`.
                        out.push(Tok::Str(
                            String::from_utf8_lossy(b).into_owned(),
                        ));
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
        loop {
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
            let mut nullable = true;
            if p.kw("NOT") {
                p.expect_kw("NULL")?;
                nullable = false;
            }
            let kind = kind_of(&tname, arg)?;
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
        return Ok(Op::CreateType {
            def: kessel_catalog::encode_type_def_with_defaults(
                &name, &fields, &defaults,
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
        if legacy_id.is_none() && id_pos.is_none() {
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
            let id = match (legacy_id, id_pos) {
                (Some(n), _) => n,
                (None, Some(ip)) => match &raw[ip] {
                    Lit::Int(n) => *n as u128,
                    Lit::Str(s) => s
                        .parse::<i128>()
                        .map(|n| n as u128)
                        .map_err(|_| {
                            "`id` must be an integer".to_string()
                        })?,
                },
                _ => unreachable!(),
            };
            // Build field values in schema order (the `id` pseudo-column is
            // not a field; unlisted nullable fields => Null).
            let mut values = Vec::with_capacity(ot.fields.len());
            for f in &ot.fields {
                match cols.iter().position(|c| *c == f.name) {
                    Some(idx) => values.push(lit_to_value(&raw[idx], f.kind)?),
                    None => {
                        // SP86: an omitted column takes its DEFAULT if
                        // one was declared; else NULL (nullable) or a
                        // clean error (NOT NULL, no default).
                        if let Some((_, d)) =
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
        p.expect_kw("ID")?;
        let id = match p.next() {
            Some(Tok::Int(n)) => n as u128,
            _ => return Err("DELETE needs `ID <int>`".into()),
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
        _ => return Err("literal/column type mismatch".into()),
    })
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
        let span: &[Tok] = &p.t[ws..p.i];
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
    struct AggSpec { kind: u8, field: Option<String> }
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
        let field = if matches!(p.peek(), Some(Tok::Star)) {
            p.i += 1;
            None
        } else {
            Some(p.ident()?)
        };
        p.punct(')')?;
        Ok(AggSpec { kind, field })
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
                leading_cols.push(p.ident()?);
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
    // Inner equi-join: `SELECT * FROM a JOIN b ON a.x = b.y [LIMIT n]`.
    if p.kw("JOIN") {
        let rname = p.ident()?;
        let rt = p.type_named(&rname)?.clone();
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
        let mut limit = 0u32;
        if p.kw("LIMIT") {
            match p.next() {
                Some(Tok::Int(n)) => limit = n as u32,
                _ => return Err("LIMIT needs int".into()),
            }
        }
        return Ok(Op::Join {
            left_type: ot.type_id,
            right_type: rt.type_id,
            left_field: lfid,
            right_field: rfid,
            limit,
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
        let span: &[Tok] = &p.t[ws..p.i];
        let rp = extract_range_preds(&ot, span);
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
        group = Some(p.ident()?);
    }
    if p.kw("ORDER") {
        p.expect_kw("BY")?;
        let c = p.ident()?;
        let desc = p.kw("DESC");
        if !desc {
            let _ = p.kw("ASC");
        }
        sort = Some((c, desc));
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
            // Single-aggregate back-compat path (byte-identical emit).
            if aggs.len() == 1 && leading_cols.is_empty() {
                let (k, af) = resolve_agg(&aggs[0])?;
                if let Some(g) = group {
                    return Ok(Op::GroupAggregate {
                        type_id: ot.type_id,
                        program,
                        group_field: fid(&g)?,
                        kind: k,
                        agg_field: af,
                        range_preds: agg_range_preds,
                    });
                } else {
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
            // Determine the single group field (V1: one column).
            let group_field = match (group, leading_cols.as_slice()) {
                (Some(g), []) => fid(&g)?,
                (None, [c]) => fid(c)?,
                (Some(g), [c]) => {
                    if g != *c {
                        return Err(format!(
                            "GROUP BY column `{g}` must match leading projection `{c}`"
                        ));
                    }
                    fid(&g)?
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
            Ok(Op::GroupAggregateMulti {
                type_id: ot.type_id,
                program,
                group_field,
                aggregates: resolved,
                range_preds: agg_range_preds,
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
            "!=" => 4,
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
        Some(Tok::Ident(name)) => {
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
}
