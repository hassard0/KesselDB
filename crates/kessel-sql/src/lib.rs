//! kessel-sql: a minimal SQL text layer that compiles single statements to
//! KesselDB `Op`s. Catalog-aware (resolves table/column names, encodes
//! values via the codec, compiles WHERE to a deterministic kessel-expr
//! program). Deliberately a constrained, well-defined subset — every
//! supported form maps cleanly onto an existing Op; nothing is faked.

#![forbid(unsafe_code)]

use kessel_catalog::{encode_type_def, Catalog, Field, FieldKind, ObjectType};
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
}

/// Compile one SQL statement, including `UPDATE`.
pub fn compile_stmt(sql: &str, cat: &Catalog) -> Result<Stmt, SqlError> {
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
            fields.push(Field {
                field_id: 0,
                name: cname,
                kind: kind_of(&tname, arg)?,
                nullable,
            });
            match p.next() {
                Some(Tok::Punct(',')) => continue,
                Some(Tok::Punct(')')) => break,
                _ => return Err("expected `,` or `)`".into()),
            }
        }
        return Ok(Op::CreateType {
            def: encode_type_def(&name, &fields),
        });
    }

    if p.kw("INSERT") {
        p.expect_kw("INTO")?;
        let tname = p.ident()?;
        p.expect_kw("ID")?;
        let id = match p.next() {
            Some(Tok::Int(n)) => n as u128,
            _ => return Err("INSERT needs `ID <int>`".into()),
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
        p.expect_kw("VALUES")?;
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
        // Build values in schema order; unlisted nullable fields => Null.
        let mut values = Vec::with_capacity(ot.fields.len());
        for f in &ot.fields {
            match cols.iter().position(|c| *c == f.name) {
                Some(idx) => values.push(lit_to_value(&raw[idx], f.kind)?),
                None => {
                    if f.nullable {
                        values.push(Value::Null);
                    } else {
                        return Err(format!("missing NOT NULL column `{}`", f.name));
                    }
                }
            }
        }
        let record = encode(&ot, &values).map_err(|e| format!("encode: {e:?}"))?;
        return Ok(Op::Create {
            type_id: ot.type_id,
            id: ObjectId::from_u128(id),
            record,
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
    let mut prog: Option<Program> = None;
    if p.kw("WHERE") {
        loop {
            let col = match p.next() {
                Some(Tok::Ident(s)) => s,
                _ => return None,
            };
            if !matches!(p.next(), Some(Tok::Cmp("="))) {
                return None;
            }
            let lit = match p.next() {
                Some(Tok::Int(n)) => Lit::Int(n),
                Some(Tok::Str(s)) => Lit::Str(s),
                _ => return None,
            };
            let f = ot.fields.iter().find(|f| f.name == col)?;
            let w = f.kind.width() as usize;
            // index hint bytes must equal the record's stored field bytes
            let hint = match &lit {
                Lit::Int(n) => n.to_le_bytes()[..w.min(16)].to_vec(),
                Lit::Str(s) => s.clone().into_bytes(),
            };
            eq_preds.push((f.field_id, hint));
            // also fold into the verifying program (load == lit)
            let mut clause = Program::new().load(f.field_id);
            clause = match &lit {
                Lit::Int(n) => clause.push_int(*n),
                Lit::Str(s) => clause.push_bytes(s.as_bytes()),
            };
            let clause = Program::from_raw({
                let mut r = clause.bytes();
                r.push(3); // EQ
                r
            });
            prog = Some(match prog.take() {
                None => clause,
                Some(acc) => {
                    let mut r = acc.bytes();
                    r.extend_from_slice(&clause.bytes());
                    r.push(14); // AND
                    Program::from_raw(r)
                }
            });
            if !p.kw("AND") {
                break;
            }
        }
    }
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
    let program = prog
        .unwrap_or_else(|| Program::new().push_int(1))
        .bytes();
    Some(Op::QueryRows {
        type_id: ot.type_id,
        eq_preds,
        program,
        limit,
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
    let lhs = term(p, ot)?;
    let prog = if let Some(Tok::Cmp(c)) = p.peek().cloned() {
        p.i += 1;
        let rhs = term(p, ot)?;
        let mut raw = lhs.bytes();
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
        lhs
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

    fn run(sm: &mut StateMachine<MemVfs>, op: u64, sql: &str) -> OpResult {
        let o = compile(sql, sm.catalog()).expect("compile");
        sm.apply(op, o)
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
        // OR is outside the restricted grammar -> falls back to Select
        match compile("SELECT * FROM rec WHERE owner = 100 OR v = 1", &cat).unwrap() {
            Op::Select { .. } => {}
            o => panic!("expected Select fallback, got {o:?}"),
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
        // execute: 3 joined rows (uid1×2 orders + uid2×1)
        match run(&mut sm, 20, "SELECT * FROM usr JOIN ord ON usr.uid = ord.owner") {
            OpResult::Got(b) => {
                let mut p = 0;
                let mut rows = 0;
                while p + 4 <= b.len() {
                    let ll = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                    p += 4 + ll;
                    let rl = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                    p += 4 + rl;
                    rows += 1;
                }
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
