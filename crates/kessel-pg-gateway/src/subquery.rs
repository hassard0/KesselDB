//! SP-PG-SQL-SUBQUERY-WHERE — non-correlated subqueries in a WHERE clause.
//!
//! Supports the three universal SQL shapes:
//!
//! ```sql
//! SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100);
//! SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM banned);
//! SELECT name FROM products WHERE price = (SELECT MAX(price) FROM products);
//! ```
//!
//! ## Design — two-phase at the gateway (engine-light)
//!
//! The feature is implemented entirely at the dispatch layer with NO engine,
//! `Op`, or wire-format change (so the determinism oracles over the apply path
//! are byte-untouched):
//!
//! 1. **Detect** — `kessel_sql::find_where_subquery` locates the FIRST
//!    `<IN|NOT IN|cmp> ( SELECT … )` in the WHERE clause via a quote-skipping,
//!    paren-balancing byte scan.
//! 2. **Run the inner SELECT first** — through the SAME engine SQL render path
//!    the outer query uses (`dispatch_query`), then parse its PG-wire DataRows
//!    to collect the inner's SINGLE projected column's values + their type OID
//!    (read from the inner RowDescription, so int values splice bare and text
//!    values splice single-quoted + `'`-escaped). Running the inner through
//!    `dispatch_query` means whatever SELECT shapes already render (projection,
//!    `MAX(price)` aggregate, WHERE, …) are valid inner queries for free.
//! 3. **Splice** the collected values into the outer query as a literal list /
//!    scalar and re-dispatch the outer through the normal path:
//!    - `col IN (SELECT …)`     → `col IN (v1, v2, …)`
//!    - `col NOT IN (SELECT …)` → `col NOT IN (v1, v2, …)`
//!    - `col <op> (SELECT …)`   → `col <op> <value>` (scalar; one row/one col)
//!
//! ## Edge cases (locked by KATs + the smoke)
//!
//! - **Inner projects ≠ 1 column** → clean `42601` error.
//! - **Scalar subquery returns > 1 row** → clean `21000` cardinality error.
//! - **Scalar subquery returns 0 rows** → the scalar is NULL; the comparison is
//!   NULL/false, so the outer returns NO rows. Spliced as a self-contradiction.
//! - **IN with empty inner result** → no rows match (`col IN (∅)` is false).
//!   Spliced as `col <> col` (a per-row contradiction for non-NULL `col`).
//! - **NOT IN with empty inner result** → spliced as `col = col`, which matches
//!   every non-NULL `col`. (PG would also match NULL rows here; KesselDB's
//!   non-NULL rows are returned — the NULL-row edge is a documented V1 limit.)
//!
//! ## V1 scope / named follow-ups
//!
//! - NON-correlated only. A correlated inner (referencing an outer column)
//!   runs as a standalone SELECT and surfaces a clean `unknown column` engine
//!   error — never silently-wrong rows (`SP-PG-SQL-CORRELATED-SUBQUERY`).
//! - ONE subquery per WHERE (`SP-PG-SQL-MULTI-SUBQUERY`).
//! - EXISTS / NOT EXISTS (`SP-PG-SQL-EXISTS`), subqueries in FROM
//!   (`SP-PG-SQL-FROM-SUBQUERY`), subqueries in the SELECT list
//!   (`SP-PG-SQL-SELECT-SUBQUERY`).

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::engine::EngineApply;
use crate::proto::{
    BE_DATA_ROW, BE_ERROR_RESPONSE, BE_ROW_DESCRIPTION, PG_TYPE_INT2, PG_TYPE_INT4,
    PG_TYPE_INT8,
};
use kessel_sql::{find_where_subquery, SubqueryOp};

/// One collected inner-column value, carrying enough type info to splice it
/// back as a SQL literal.
#[derive(Debug, Clone, PartialEq, Eq)]
enum InnerValue {
    /// A SQL NULL cell (DataRow length -1). Skipped when building an IN-list
    /// (a NULL never equals anything); makes a scalar subquery yield NULL.
    Null,
    /// A bare numeric literal (int2/int4/int8) — spliced without quotes.
    Number(String),
    /// A text/other value — spliced single-quoted with `'` doubled.
    Text(String),
}

impl InnerValue {
    /// Render this value as a SQL literal for splicing into the outer query.
    /// `None` for `Null` (the caller decides what a NULL means per op).
    fn as_literal(&self) -> Option<String> {
        match self {
            InnerValue::Null => None,
            InnerValue::Number(n) => Some(n.clone()),
            InnerValue::Text(t) => {
                let mut out = String::with_capacity(t.len() + 2);
                out.push('\'');
                for c in t.chars() {
                    if c == '\'' {
                        out.push_str("''");
                    } else {
                        out.push(c);
                    }
                }
                out.push('\'');
                Some(out)
            }
        }
    }
}

/// SP-PG-SQL-SUBQUERY-WHERE — if `sql` carries a WHERE-clause subquery, run
/// the inner SELECT, splice its values into the outer query, and return the
/// rewritten outer SQL (`Ok(Some(rewritten))`). Returns `Ok(None)` when there
/// is no subquery (the caller dispatches `sql` unchanged). Returns
/// `Err(error_response_bytes)` (a complete `ErrorResponse` + `ReadyForQuery`
/// frame) when the subquery is malformed (wrong column count, cardinality
/// violation, inner error).
///
/// `dispatch` is the gateway's own single-query dispatcher (`dispatch_query`)
/// — passed in to avoid a module cycle and so the inner SELECT runs through
/// the IDENTICAL render path the outer query would.
pub fn rewrite_where_subquery<E, F>(
    sql: &str,
    engine: &E,
    dispatch: F,
) -> Result<Option<String>, Vec<u8>>
where
    E: EngineApply + ?Sized,
    F: Fn(&str, &E) -> Vec<u8>,
{
    let sq = match find_where_subquery(sql) {
        Some(s) => s,
        None => return Ok(None),
    };

    // Phase 2 — run the inner SELECT through the normal render path.
    let inner_bytes = dispatch(&sq.inner_sql, engine);

    // If the inner produced an ErrorResponse, surface it verbatim (re-tagged
    // so the client sees the inner failure rather than a confusing outer one).
    if let Some(msg) = first_error_message(&inner_bytes) {
        return Err(crate::error::encode_error_response(
            crate::error::SEVERITY_ERROR,
            "42601",
            &format!("subquery failed: {msg}"),
        )
        .into_iter()
        .chain(crate::response::encode_ready_for_query(b'I'))
        .collect());
    }

    // Parse the inner RowDescription + DataRows.
    let (ncols, type_oid) = match parse_row_description(&inner_bytes) {
        Some(v) => v,
        None => {
            // No RowDescription — the inner wasn't a SELECT (or rendered
            // nothing describable). Treat as an empty/invalid result.
            return Err(err_frame(
                "42601",
                "subquery did not return a result set (inner must be a SELECT)",
            ));
        }
    };
    if ncols != 1 {
        return Err(err_frame(
            "42601",
            &format!(
                "subquery must project exactly ONE column (projects {ncols})"
            ),
        ));
    }
    let values = match collect_first_column(&inner_bytes, type_oid) {
        Some(v) => v,
        None => {
            return Err(err_frame(
                "XX000",
                "subquery result decode failed",
            ));
        }
    };

    // Phase 3 — splice.
    let prefix = &sql[..sq.paren_open];
    let suffix = &sql[sq.paren_close + 1..];
    let replacement = match &sq.op {
        SubqueryOp::In | SubqueryOp::NotIn => {
            let lits: Vec<String> =
                values.iter().filter_map(|v| v.as_literal()).collect();
            if lits.is_empty() {
                // Empty inner result → rewrite the WHOLE predicate to a
                // per-row contradiction (IN) / tautology (NOT IN). We need
                // the LHS column text, which sits just before the operator.
                let col = lhs_column_text(prefix, &sq.op);
                let pred = match &sq.op {
                    SubqueryOp::NotIn => format!("{col} = {col}"),
                    _ => format!("{col} <> {col}"),
                };
                // Replace `col [NOT] IN (subq)` entirely with the predicate.
                let head = &prefix[..prefix.len() - operator_lhs_len(prefix, &sq.op)];
                return Ok(Some(format!("{head}{pred}{suffix}")));
            }
            let kw = if matches!(sq.op, SubqueryOp::NotIn) {
                "NOT IN"
            } else {
                "IN"
            };
            // Rebuild `col <kw> (list)`. The prefix already ends with the
            // operator keyword; replace it cleanly by trimming the operator.
            let head = &prefix[..prefix.len() - operator_lhs_len(prefix, &sq.op)];
            let col = lhs_column_text(prefix, &sq.op);
            format!("{head}{col} {kw} ({})", lits.join(", "))
        }
        SubqueryOp::Cmp(op) => {
            // Scalar subquery: require 0 or 1 row.
            if values.len() > 1 {
                return Err(err_frame(
                    "21000",
                    &format!(
                        "scalar subquery returned {} rows (expected at most 1)",
                        values.len()
                    ),
                ));
            }
            match values.first().and_then(|v| v.as_literal()) {
                Some(lit) => format!("{prefix}{lit}"),
                None => {
                    // 0 rows OR a NULL scalar → comparison is NULL → no rows.
                    let col = lhs_column_text(prefix, &sq.op);
                    let head =
                        &prefix[..prefix.len() - operator_lhs_len(prefix, &sq.op)];
                    let _ = op;
                    return Ok(Some(format!("{head}{col} <> {col}{suffix}")));
                }
            }
        }
    };
    Ok(Some(format!("{replacement}{suffix}")))
}

/// Build a complete `ErrorResponse` + `ReadyForQuery('I')` frame.
fn err_frame(sqlstate: &str, msg: &str) -> Vec<u8> {
    crate::error::encode_error_response(crate::error::SEVERITY_ERROR, sqlstate, msg)
        .into_iter()
        .chain(crate::response::encode_ready_for_query(b'I'))
        .collect()
}

/// Length (in bytes) of the operator token at the END of `prefix` (including
/// the whitespace between the LHS column and the `(`). Used to trim the
/// operator + column so we can rebuild the predicate cleanly.
///
/// For IN / NOT IN this is `<col> <kw>`'s `<kw>` plus trailing whitespace.
/// We compute it as: everything from the last column-ident boundary to the
/// end of `prefix`. Because `lhs_column_text` finds the column, this returns
/// `prefix.len() - <start of column>`.
fn operator_lhs_len(prefix: &str, op: &SubqueryOp) -> usize {
    prefix.len() - lhs_column_start(prefix, op)
}

/// Byte index in `prefix` where the LHS column identifier begins.
fn lhs_column_start(prefix: &str, op: &SubqueryOp) -> usize {
    let bytes = prefix.as_bytes();
    // Walk back from the end over: trailing ws, the operator token, ws, the
    // column ident (possibly `table.col`).
    let mut i = bytes.len();
    // trailing whitespace before `(`
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    // operator token
    match op {
        SubqueryOp::In => {
            // `IN`
            i = i.saturating_sub(2);
        }
        SubqueryOp::NotIn => {
            // `IN`
            i = i.saturating_sub(2);
            // ws
            while i > 0 && bytes[i - 1].is_ascii_whitespace() {
                i -= 1;
            }
            // `NOT`
            i = i.saturating_sub(3);
        }
        SubqueryOp::Cmp(c) => {
            i = i.saturating_sub(c.len());
        }
    }
    // whitespace between column and operator
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    // the column identifier (alnum / _ / . for `table.col`)
    let end = i;
    while i > 0 && (is_ident_byte(bytes[i - 1]) || bytes[i - 1] == b'.') {
        i -= 1;
    }
    let _ = end;
    i
}

/// The LHS column text immediately preceding the operator in `prefix`.
fn lhs_column_text(prefix: &str, op: &SubqueryOp) -> String {
    let start = lhs_column_start(prefix, op);
    // The column ends where the operator's whitespace begins. Recompute the
    // column's end by walking forward over ident bytes from `start`.
    let bytes = prefix.as_bytes();
    let mut e = start;
    while e < bytes.len() && (is_ident_byte(bytes[e]) || bytes[e] == b'.') {
        e += 1;
    }
    prefix[start..e].to_string()
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Scan a backend byte stream for the FIRST `ErrorResponse` ('E') and return
/// its human-readable message (the 'M' field), if any.
fn first_error_message(bytes: &[u8]) -> Option<String> {
    let mut i = 0usize;
    while i + 5 <= bytes.len() {
        let tag = bytes[i];
        let len = u32::from_be_bytes([
            bytes[i + 1],
            bytes[i + 2],
            bytes[i + 3],
            bytes[i + 4],
        ]) as usize;
        let body_end = i + 1 + len;
        if len < 4 || body_end > bytes.len() {
            return None;
        }
        if tag == BE_ERROR_RESPONSE {
            // Body is a sequence of [field_code:1][value:cstring], terminated
            // by a 0 field code. Find the 'M' (message) field.
            let body = &bytes[i + 5..body_end];
            let mut p = 0usize;
            while p < body.len() && body[p] != 0 {
                let code = body[p];
                p += 1;
                let start = p;
                while p < body.len() && body[p] != 0 {
                    p += 1;
                }
                let val = String::from_utf8_lossy(&body[start..p]).to_string();
                p += 1; // skip the cstring NUL
                if code == b'M' {
                    return Some(val);
                }
            }
            return Some("subquery error".to_string());
        }
        i = body_end;
    }
    None
}

/// Parse the FIRST `RowDescription` ('T') frame → (column_count, first column
/// type OID). Returns `None` if no RowDescription is present.
fn parse_row_description(bytes: &[u8]) -> Option<(usize, u32)> {
    let mut i = 0usize;
    while i + 5 <= bytes.len() {
        let tag = bytes[i];
        let len = u32::from_be_bytes([
            bytes[i + 1],
            bytes[i + 2],
            bytes[i + 3],
            bytes[i + 4],
        ]) as usize;
        let body_end = i + 1 + len;
        if len < 4 || body_end > bytes.len() {
            return None;
        }
        if tag == BE_ROW_DESCRIPTION {
            let body = &bytes[i + 5..body_end];
            if body.len() < 2 {
                return None;
            }
            let ncols = u16::from_be_bytes([body[0], body[1]]) as usize;
            // Type OID of the first field: name(cstring) + table_oid(4) +
            // col_attr(2) then type_oid(4).
            let mut p = 2usize;
            // skip name cstring
            while p < body.len() && body[p] != 0 {
                p += 1;
            }
            p += 1; // NUL
            p += 4 + 2; // table_oid + col_attr
            let oid = if p + 4 <= body.len() {
                u32::from_be_bytes([body[p], body[p + 1], body[p + 2], body[p + 3]])
            } else {
                0
            };
            return Some((ncols, oid));
        }
        i = body_end;
    }
    None
}

/// Collect the FIRST column's value from every `DataRow` ('D') frame in the
/// stream, typing each as Number (int OIDs) / Text / Null per `type_oid`.
fn collect_first_column(bytes: &[u8], type_oid: u32) -> Option<Vec<InnerValue>> {
    let numeric =
        matches!(type_oid, PG_TYPE_INT2 | PG_TYPE_INT4 | PG_TYPE_INT8);
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 5 <= bytes.len() {
        let tag = bytes[i];
        let len = u32::from_be_bytes([
            bytes[i + 1],
            bytes[i + 2],
            bytes[i + 3],
            bytes[i + 4],
        ]) as usize;
        let body_end = i + 1 + len;
        if len < 4 || body_end > bytes.len() {
            return None;
        }
        if tag == BE_DATA_ROW {
            let body = &bytes[i + 5..body_end];
            if body.len() < 2 {
                return None;
            }
            let ncols = u16::from_be_bytes([body[0], body[1]]) as usize;
            if ncols == 0 {
                out.push(InnerValue::Null);
                i = body_end;
                continue;
            }
            // First column: [len:i32][bytes:len] (len -1 == NULL).
            let mut p = 2usize;
            if p + 4 > body.len() {
                return None;
            }
            let clen = i32::from_be_bytes([
                body[p],
                body[p + 1],
                body[p + 2],
                body[p + 3],
            ]);
            p += 4;
            if clen < 0 {
                out.push(InnerValue::Null);
            } else {
                let clen = clen as usize;
                if p + clen > body.len() {
                    return None;
                }
                let val = String::from_utf8_lossy(&body[p..p + clen]).to_string();
                if numeric {
                    out.push(InnerValue::Number(val));
                } else {
                    out.push(InnerValue::Text(val));
                }
            }
        }
        i = body_end;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response::{encode_data_row, encode_row_description, FieldMeta};

    #[test]
    fn inner_value_literal_quoting() {
        assert_eq!(InnerValue::Number("42".into()).as_literal().unwrap(), "42");
        assert_eq!(
            InnerValue::Text("alice".into()).as_literal().unwrap(),
            "'alice'"
        );
        // `'` is doubled.
        assert_eq!(
            InnerValue::Text("O'Brien".into()).as_literal().unwrap(),
            "'O''Brien'"
        );
        assert!(InnerValue::Null.as_literal().is_none());
    }

    #[test]
    fn parse_row_description_extracts_count_and_oid() {
        let rd = encode_row_description(&[FieldMeta {
            name: "user_id".into(),
            type_oid: PG_TYPE_INT8,
        }]);
        let (n, oid) = parse_row_description(&rd).unwrap();
        assert_eq!(n, 1);
        assert_eq!(oid, PG_TYPE_INT8);
    }

    #[test]
    fn collect_first_column_numbers_and_text() {
        let mut stream = encode_row_description(&[FieldMeta {
            name: "id".into(),
            type_oid: PG_TYPE_INT8,
        }]);
        stream.extend_from_slice(&encode_data_row(&[Some(b"1")]));
        stream.extend_from_slice(&encode_data_row(&[Some(b"2")]));
        stream.extend_from_slice(&encode_data_row(&[None]));
        let vals = collect_first_column(&stream, PG_TYPE_INT8).unwrap();
        assert_eq!(
            vals,
            vec![
                InnerValue::Number("1".into()),
                InnerValue::Number("2".into()),
                InnerValue::Null,
            ]
        );
    }

    #[test]
    fn lhs_column_text_for_each_op() {
        assert_eq!(
            lhs_column_text("SELECT name FROM users WHERE id IN ", &SubqueryOp::In),
            "id"
        );
        assert_eq!(
            lhs_column_text(
                "SELECT name FROM users WHERE id NOT IN ",
                &SubqueryOp::NotIn
            ),
            "id"
        );
        assert_eq!(
            lhs_column_text(
                "SELECT name FROM products WHERE price = ",
                &SubqueryOp::Cmp("=".into())
            ),
            "price"
        );
    }
}
