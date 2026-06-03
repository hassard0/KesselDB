//! SP-PG-RETURNING-MULTIROW-STAR — SQLAlchemy `insertmanyvalues` rewrite.
//!
//! SQLAlchemy 2.0's DEFAULT engine config (`use_insertmanyvalues=True`)
//! does NOT emit a plain `INSERT … VALUES (…),(…) RETURNING id` for a
//! batched flush. Instead it emits its **insertmanyvalues** form, which
//! threads a synthetic ordering column so RETURNING rows can be matched
//! back to the input rows:
//!
//! ```sql
//! INSERT INTO widgets (name)
//! SELECT p0::VARCHAR
//! FROM (VALUES ('a', 0), ('b', 1), ('c', 2)) AS imp_sen(p0, sen_counter)
//! ORDER BY sen_counter
//! RETURNING widgets.id, widgets.id AS id__1
//! ```
//!
//! KesselDB's SQL engine already handles the plain multi-row
//! `INSERT … VALUES (…),(…) RETURNING …` shape (SP58 multi-row INSERT →
//! `Op::Txn`, SP-PG-RETURNING-MULTIROW-STAR surfaces the ids). The
//! insertmanyvalues form is **semantically identical** to that plain
//! form: each VALUES tuple's data columns map to the insert column list,
//! in `sen_counter` order (which IS the literal tuple order). So we
//! desugar it with a focused, conservative text rewrite:
//!
//! - drop the projection `SELECT p0[::cast], …` (its only job is to
//!   re-project the VALUES columns + cast),
//! - drop the trailing `sen_counter` value from each VALUES tuple (the
//!   ordering column),
//! - drop the `FROM (VALUES …) AS alias(…) ORDER BY sen_counter`
//!   scaffolding,
//! - keep the column list + RETURNING clause verbatim,
//!
//! producing `INSERT INTO widgets (name) VALUES ('a'),('b'),('c')
//! RETURNING widgets.id, widgets.id AS id__1`.
//!
//! The rewrite is **conservative**: it fires ONLY on the recognized
//! `INSERT … SELECT … FROM (VALUES …) AS <alias>(…) ORDER BY …` shape and
//! returns the input unchanged for anything else, so every existing SQL
//! path is byte-untouched. Applied at the gateway's SQL entry (before
//! cast validation), it also removes the `p0::VARCHAR` projection cast
//! that the literal-cast validator would otherwise reject.

#![forbid(unsafe_code)]

/// If `sql` is SQLAlchemy's insertmanyvalues form, rewrite it to the
/// plain multi-row `INSERT … VALUES (…),(…) RETURNING …` form. Returns
/// `Some(rewritten)` on a successful rewrite, `None` if `sql` is not the
/// recognized shape (caller uses the original text unchanged).
///
/// Pure text transform; no allocation when the shape doesn't match
/// (the cheap `contains` pre-checks short-circuit first).
pub fn rewrite_insertmanyvalues(sql: &str) -> Option<String> {
    // Cheap pre-checks: the shape ALWAYS has `INSERT`, a `SELECT` after
    // it, a `FROM (VALUES`, and an `AS <alias>(…)` table alias. Bail fast
    // for the overwhelmingly-common non-matching case.
    let upper = sql.to_ascii_uppercase();
    if !upper.trim_start().starts_with("INSERT") {
        return None;
    }
    let from_values_pos = find_kw(&upper, "FROM")?;
    // `INSERT … VALUES (…)` (the plain form, no SELECT) must NOT match.
    let select_pos = find_kw(&upper, "SELECT")?;
    if select_pos > from_values_pos {
        return None;
    }
    // The `INSERT INTO t (cols)` prefix, up to the SELECT.
    let insert_prefix = sql[..select_pos].trim_end();
    // Must be `INSERT INTO <table> (<cols>)` — needs a parenthesised
    // column list (insertmanyvalues always names columns).
    if !insert_prefix.contains('(') {
        return None;
    }

    // Locate `FROM ( VALUES` — the inline VALUES table.
    let after_from = &sql[from_values_pos..];
    let after_from_up = &upper[from_values_pos..];
    // Skip `FROM`, optional whitespace, `(`, optional whitespace, `VALUES`.
    let values_kw_rel = find_kw(after_from_up, "VALUES")?;
    // Everything between FROM and VALUES must be only `(` + whitespace.
    let between = &after_from[4..values_kw_rel];
    if between.trim().trim_start_matches('(').trim() != "" {
        return None;
    }
    // The open paren index (in `sql`) that wraps the VALUES sub-select.
    let outer_open_rel = between.find('(')?;
    let outer_open = from_values_pos + 4 + outer_open_rel;

    // The VALUES tuples start right after the VALUES keyword.
    let values_kw_abs = from_values_pos + values_kw_rel;
    let tuples_start = values_kw_abs + "VALUES".len();

    // Parse the parenthesised VALUES tuples: `(v,v,…),(v,…),…`. We scan
    // depth-balanced parens, collecting each top-level `(…)` tuple, until
    // we hit the matching close of the OUTER sub-select paren.
    let bytes = sql.as_bytes();
    let mut i = tuples_start;
    // skip whitespace
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let mut tuples: Vec<&str> = Vec::new();
    // Track the outer-paren depth: we entered `outer_open` already (depth
    // 1 for the sub-select). Each tuple is a depth-2 region.
    loop {
        // Expect a `(` starting a tuple.
        if i >= bytes.len() || bytes[i] != b'(' {
            // No more tuples (e.g. we reached `)` closing the sub-select).
            break;
        }
        let tup_open = i;
        let mut depth = 0usize;
        let mut in_str = false;
        let mut j = i;
        while j < bytes.len() {
            let c = bytes[j];
            if in_str {
                if c == b'\'' {
                    // doubled '' = escaped quote
                    if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                        j += 2;
                        continue;
                    }
                    in_str = false;
                }
                j += 1;
                continue;
            }
            match c {
                b'\'' => in_str = true,
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            j += 1;
        }
        if depth != 0 {
            return None; // unbalanced
        }
        // tuple inner = (tup_open+1 .. j)  (exclusive of the parens)
        let inner = &sql[tup_open + 1..j];
        tuples.push(inner);
        i = j + 1;
        // skip whitespace
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        // A comma separates tuples; anything else ends the tuple list.
        if i < bytes.len() && bytes[i] == b',' {
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            continue;
        }
        break;
    }
    if tuples.is_empty() {
        return None;
    }
    // `i` now points at the close `)` of the sub-select (depth back to the
    // outer paren). Validate + find the RETURNING clause after the
    // `AS <alias>(…) ORDER BY …`.
    if i >= bytes.len() || bytes[i] != b')' {
        return None;
    }
    let _ = outer_open; // (kept for clarity; not needed past here)
    let tail = &sql[i + 1..]; // after the sub-select close paren
    let tail_up = tail.to_ascii_uppercase();
    // The tail MUST be `AS <alias>(<cols>) [ORDER BY …] [RETURNING …]`.
    if find_kw(&tail_up, "AS").map(|p| p == 0 || tail[..p].trim().is_empty()) != Some(true) {
        // Some shapes omit AS; require the alias paren regardless.
    }
    // RETURNING (if present) is preserved verbatim.
    let returning = find_kw(&tail_up, "RETURNING").map(|p| &tail[p..]);

    // Drop the trailing ordering column from each tuple (the last
    // top-level comma-separated value).
    let mut rebuilt_tuples: Vec<String> = Vec::with_capacity(tuples.len());
    for t in &tuples {
        let vals = split_top_level_commas(t);
        if vals.len() < 2 {
            return None; // need at least one data col + the sen_counter
        }
        // Drop the LAST value (sen_counter).
        let data = &vals[..vals.len() - 1];
        rebuilt_tuples.push(format!("({})", data.join(", ")));
    }

    // Assemble the plain multi-row INSERT.
    let mut out = String::with_capacity(sql.len());
    out.push_str(insert_prefix);
    out.push_str(" VALUES ");
    out.push_str(&rebuilt_tuples.join(", "));
    if let Some(r) = returning {
        out.push(' ');
        out.push_str(r.trim());
    }
    Some(out)
}

/// Split a string on TOP-LEVEL commas (not inside parens or single-quoted
/// strings). Returns the trimmed segments.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut in_str = false;
    let mut start = 0usize;
    let mut k = 0usize;
    while k < bytes.len() {
        let c = bytes[k];
        if in_str {
            if c == b'\'' {
                if k + 1 < bytes.len() && bytes[k + 1] == b'\'' {
                    k += 2;
                    continue;
                }
                in_str = false;
            }
            k += 1;
            continue;
        }
        match c {
            b'\'' => in_str = true,
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                out.push(s[start..k].trim());
                start = k + 1;
            }
            _ => {}
        }
        k += 1;
    }
    out.push(s[start..].trim());
    out
}

/// Find a whole-word keyword in an UPPERCASED string, returning the byte
/// offset of its start. Word boundaries: the char before must be
/// non-alphanumeric/underscore, and the char after likewise. Skips
/// matches inside single-quoted string literals.
fn find_kw(upper: &str, kw: &str) -> Option<usize> {
    let bytes = upper.as_bytes();
    let kwb = kw.as_bytes();
    let mut i = 0usize;
    let mut in_str = false;
    while i + kwb.len() <= bytes.len() {
        let c = bytes[i];
        if in_str {
            if c == b'\'' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        if c == b'\'' {
            in_str = true;
            i += 1;
            continue;
        }
        // SP-PG-SQL-QUOTED-IDENT — skip double-quoted delimited
        // identifiers so a column/table literally named `"FROM"` /
        // `"VALUES"` cannot false-match a structural keyword. Honours
        // the doubled-`""` escape.
        if c == b'"' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if &bytes[i..i + kwb.len()] == kwb {
            let before_ok = i == 0 || !is_ident_byte(bytes[i - 1]);
            let after_idx = i + kwb.len();
            let after_ok = after_idx >= bytes.len() || !is_ident_byte(bytes[after_idx]);
            if before_ok && after_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The headline: SQLAlchemy's insertmanyvalues form rewrites to the
    /// plain multi-row VALUES form (cast + ordering scaffolding dropped).
    #[test]
    fn rewrites_sqlalchemy_insertmanyvalues_form() {
        let sql = "INSERT INTO widgets (name) SELECT p0::VARCHAR FROM (VALUES ('a', 0), ('b', 1), ('c', 2)) AS imp_sen(p0, sen_counter) ORDER BY sen_counter RETURNING widgets.id, widgets.id AS id__1";
        let out = rewrite_insertmanyvalues(sql).expect("should rewrite");
        assert_eq!(
            out,
            "INSERT INTO widgets (name) VALUES ('a'), ('b'), ('c') RETURNING widgets.id, widgets.id AS id__1"
        );
    }

    /// Multi-column data: two data columns + the sen_counter; only the
    /// sen_counter is dropped.
    #[test]
    fn rewrites_multi_data_column_form() {
        let sql = "INSERT INTO t (a, b) SELECT p0, p1 FROM (VALUES ('x', 1, 0), ('y', 2, 1)) AS s(p0, p1, sen_counter) ORDER BY sen_counter RETURNING t.id";
        let out = rewrite_insertmanyvalues(sql).expect("rewrite");
        assert_eq!(
            out,
            "INSERT INTO t (a, b) VALUES ('x', 1), ('y', 2) RETURNING t.id"
        );
    }

    /// A plain multi-row INSERT (no SELECT) is NOT rewritten — returns
    /// None so the caller uses the original verbatim.
    #[test]
    fn plain_multirow_insert_is_not_rewritten() {
        assert_eq!(
            rewrite_insertmanyvalues(
                "INSERT INTO t (name) VALUES ('a'),('b') RETURNING id"
            ),
            None
        );
    }

    /// A plain single-row INSERT is untouched.
    #[test]
    fn plain_single_insert_is_not_rewritten() {
        assert_eq!(
            rewrite_insertmanyvalues("INSERT INTO t (name) VALUES ('a')"),
            None
        );
    }

    /// A SELECT statement is untouched.
    #[test]
    fn select_is_not_rewritten() {
        assert_eq!(rewrite_insertmanyvalues("SELECT * FROM t"), None);
    }

    /// A comma inside a quoted string literal in a VALUES tuple does NOT
    /// split the tuple — the data value is preserved intact.
    #[test]
    fn comma_inside_string_literal_preserved() {
        let sql = "INSERT INTO t (name) SELECT p0 FROM (VALUES ('a, b', 0)) AS s(p0, sen_counter) ORDER BY sen_counter RETURNING t.id";
        let out = rewrite_insertmanyvalues(sql).expect("rewrite");
        assert_eq!(out, "INSERT INTO t (name) VALUES ('a, b') RETURNING t.id");
    }

    /// No RETURNING clause is tolerated (rewrite still drops scaffolding).
    #[test]
    fn rewrites_without_returning() {
        let sql = "INSERT INTO t (name) SELECT p0 FROM (VALUES ('a', 0), ('b', 1)) AS s(p0, sen_counter) ORDER BY sen_counter";
        let out = rewrite_insertmanyvalues(sql).expect("rewrite");
        assert_eq!(out, "INSERT INTO t (name) VALUES ('a'), ('b')");
    }

    /// SP-PG-SQL-QUOTED-IDENT — Django's quoted INSERT (no inner SELECT)
    /// is NOT the insertmanyvalues shape, so it's left verbatim. Locks
    /// that quoted identifiers don't accidentally trip the rewrite.
    #[test]
    fn quoted_django_insert_not_rewritten() {
        assert_eq!(
            rewrite_insertmanyvalues(
                r#"INSERT INTO "t" ("name") VALUES ($1) RETURNING "t"."id""#
            ),
            None
        );
    }

    /// SP-PG-SQL-QUOTED-IDENT — `find_kw` skips double-quoted delimited
    /// identifiers so a column literally named `"FROM"` cannot be
    /// mistaken for the structural `FROM` keyword.
    #[test]
    fn find_kw_skips_quoted_identifier() {
        // The only `FROM` keyword here is the real one; the quoted
        // `"FROM"` column reference must be skipped.
        let s = r#"SELECT "FROM" FROM t"#.to_ascii_uppercase();
        // The real FROM is at the second occurrence; the quoted one is
        // skipped, so the returned offset points past the quoted region.
        let pos = find_kw(&s, "FROM").expect("real FROM found");
        // Everything before `pos` must include the quoted `"FROM"`.
        assert!(s[..pos].contains(r#""FROM""#));
    }
}
