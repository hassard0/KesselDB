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
            .ok_or_else(|| format!("unknown table `{name}`"))
    }
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
            | Op::GroupAggregate { type_id, .. } => {
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
    {
        let mut p = P { t: lex(sql)?, i: 0, cat };
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
                    .ok_or_else(|| format!("unknown column `{col}`"))?;
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
    Ok(Stmt::Op(compile(sql, cat)?))
}

/// Compile one SQL statement to an `Op`. `cat` is needed for everything
/// except `CREATE TABLE`. `UPDATE` is rejected here (use `compile_stmt` +
/// a server that can read-modify-write).
pub fn compile(sql: &str, cat: &Catalog) -> Result<Op, SqlError> {
    let mut p = P {
        t: lex(sql)?,
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
    if p.kw("DROP") {
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
                    .ok_or_else(|| format!("unknown column `{c}`"))?;
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
                .ok_or_else(|| format!("unknown column `{c}`"))?;
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
                .ok_or_else(|| format!("unknown column `{c}`"))?
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
                .ok_or_else(|| format!("unknown column `{c}`"))?;
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
                    .ok_or_else(|| format!("unknown column `{c}`"))?;
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
                match p.next() {
                    Some(Tok::Int(n)) => raw.push(Lit::Int(n)),
                    Some(Tok::Str(s)) => raw.push(Lit::Str(s)),
                    _ => return Err("expected value".into()),
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
            // Resolve the row id for this tuple.
            let id = match (legacy_id, id_pos) {
                (Some(n), _) => n,
                (None, Some(ip)) => match &raw[ip] {
                    Lit::Int(n) => *n as u128,
                    _ => return Err("`id` must be an integer".into()),
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
        _ => return Err("literal/column type mismatch".into()),
    })
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
                            range_preds.push((
                                f.field_id,
                                rop,
                                n.to_le_bytes()[..w.min(16)].to_vec(),
                            ));
                        }
                    }
                }
                // SP90: `col {> >= < <=} 'str'` on an order-indexed
                // CHAR/BYTES column — the value bytes are the string
                // itself (the engine width-normalises lexicographically).
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
                            range_preds.push((
                                f.field_id,
                                rop,
                                s.clone().into_bytes(),
                            ));
                        }
                    }
                }
                i += 1;
            }
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
    // projection
    enum Proj {
        Star,
        Cols(Vec<String>),
        Agg(u8, Option<String>), // 0 COUNT,1 SUM,2 MIN,3 MAX
    }
    let proj = if matches!(p.peek(), Some(Tok::Star)) {
        p.i += 1;
        Proj::Star
    } else if let Some(Tok::Ident(w)) = p.peek().cloned() {
        let up = w.to_ascii_uppercase();
        if matches!(up.as_str(), "COUNT" | "SUM" | "MIN" | "MAX" | "AVG") {
            p.i += 1;
            p.punct('(')?;
            let f = if matches!(p.peek(), Some(Tok::Star)) {
                p.i += 1;
                None
            } else {
                Some(p.ident()?)
            };
            p.punct(')')?;
            let k = match up.as_str() {
                "COUNT" => 0,
                "SUM" => 1,
                "MIN" => 2,
                "MAX" => 3,
                _ => 4, // AVG
            };
            Proj::Agg(k, f)
        } else {
            let mut cols = vec![p.ident()?];
            while matches!(p.peek(), Some(Tok::Punct(','))) {
                p.i += 1;
                cols.push(p.ident()?);
            }
            Proj::Cols(cols)
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
            .ok_or_else(|| format!("unknown column `{lf_col}`"))?
            .field_id;
        let rfid = rf_tbl
            .fields
            .iter()
            .find(|f| f.name == rf_col)
            .ok_or_else(|| format!("unknown column `{rf_col}`"))?
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
            .ok_or_else(|| format!("unknown column `{n}`"))
    };

    let program = if p.kw("WHERE") {
        compile_where(p, &ot)?
    } else {
        Program::new().push_int(1).bytes() // always true
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
        Proj::Agg(k, f) => {
            let af = match &f {
                Some(c) => fid(c)?,
                None => 0,
            };
            if let Some(g) = group {
                Ok(Op::GroupAggregate {
                    type_id: ot.type_id,
                    program,
                    group_field: fid(&g)?,
                    kind: k,
                    agg_field: af,
                })
            } else {
                Ok(Op::Aggregate {
                    type_id: ot.type_id,
                    program,
                    kind: k,
                    field_id: af,
                })
            }
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
        let rhs = term(p, ot)?;
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
    match p.next() {
        Some(Tok::Punct('(')) => {
            let inner = or_expr(p, ot)?;
            p.punct(')')?;
            Ok(inner)
        }
        Some(Tok::Int(n)) => Ok(Program::new().push_int(n)),
        Some(Tok::Str(s)) => Ok(Program::new().push_bytes(s.as_bytes())),
        Some(Tok::Ident(name)) => {
            let f = ot
                .fields
                .iter()
                .find(|f| f.name == name)
                .ok_or_else(|| format!("unknown column `{name}`"))?;
            Ok(Program::new().load(f.field_id))
        }
        _ => Err("bad WHERE term".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_io::MemVfs;
    use kessel_proto::OpResult;
    use kessel_sm::StateMachine;

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
                OpResult::Got(b) => b,
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
                OpResult::Got(b) => i128::from_le_bytes(b.try_into().unwrap()),
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
            OpResult::Got(bal_rec(5))
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
            OpResult::Got(b) => assert_eq!(i128::from_le_bytes(b.try_into().unwrap()), 2),
            o => panic!("{o:?}"),
        }
        // SELECT SUM(bal) WHERE owner = 100  -> 1049
        match run(&mut sm, 6, "SELECT SUM(bal) FROM acct WHERE owner = 100") {
            OpResult::Got(b) => assert_eq!(i128::from_le_bytes(b.try_into().unwrap()), 1049),
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
            OpResult::Got(b) => assert_eq!(i128::from_le_bytes(b.try_into().unwrap()), 2),
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
                OpResult::Got(b) => i128::from_le_bytes(b.try_into().unwrap()),
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
                OpResult::Got(b) => i128::from_le_bytes(b.try_into().unwrap()),
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
                OpResult::Got(b) => i128::from_le_bytes(b.try_into().unwrap()),
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
                OpResult::Got(b) => i128::from_le_bytes(b.try_into().unwrap()),
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
                OpResult::Got(b) => i128::from_le_bytes(b.try_into().unwrap()),
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

    #[test]
    fn where_or_not_paren() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        run(&mut sm, 1, "CREATE TABLE t (a I32 NOT NULL)");
        for (i, v) in [1i64, 2, 3, 4, 5].iter().enumerate() {
            run(&mut sm, 2 + i as u64, &format!("INSERT INTO t ID {} (a) VALUES ({})", i, v));
        }
        // a = 1 OR a >= 4  -> {1,4,5} = 3
        match run(&mut sm, 10, "SELECT COUNT(*) FROM t WHERE a = 1 OR a >= 4") {
            OpResult::Got(b) => assert_eq!(i128::from_le_bytes(b.try_into().unwrap()), 3),
            o => panic!("{o:?}"),
        }
        // NOT (a = 3) -> 4
        match run(&mut sm, 11, "SELECT COUNT(*) FROM t WHERE NOT (a = 3)") {
            OpResult::Got(b) => assert_eq!(i128::from_le_bytes(b.try_into().unwrap()), 4),
            o => panic!("{o:?}"),
        }
    }
}
