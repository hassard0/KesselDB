//! SP143: Dremel-style record assembly for nested Parquet columns.
//!
//! Takes the parallel (rep_levels, def_levels, values) streams produced by
//! `decode_page_v1_nested` / `decode_data_page_v2_nested` and reconstructs
//! one `PqValue` per top-level record (where each record's value at a
//! LIST<primitive> column is `PqValue::List(Vec<PqValue>)` or `PqValue::Null`).
//!
//! V1 SP143: only single-level LIST<primitive> (max_rep_level == 1). SP144
//! adds Map+struct; SP145 adds deep nesting (max_rep_level >= 2).

#![allow(dead_code)]

use crate::{PqValue, PqError};

/// Assemble a stream of (rep, def, value) into one PqValue per record for a
/// LIST<primitive> column. Each record's value is either `PqValue::Null`
/// (when outer LIST is null) or `PqValue::List(items)`.
///
/// Parameters:
/// - `rep_levels`: per-position repetition level (∈ {0, 1} for single-level LIST)
/// - `def_levels`: per-position definition level (∈ {0..=max_def_level})
/// - `values`: actual primitive values, length = count of def == max_def
/// - `max_def_level`: from schema (e.g. 3 for OPT-OPT-OPT, 1 for REQ-REQ-REQ)
/// - `outer_optional`: is the outer LIST group OPTIONAL?
/// - `element_optional`: is the inner element OPTIONAL?
///
/// Returns `Vec<PqValue>` — one per top-level record. The number of records
/// is the count of rep == 0 entries (or 1 if rep_levels is empty? — see below).
///
/// Errors on malformed inputs: level value > max, rep level > 1, value stream
/// length mismatch, value present but values vec exhausted.
pub fn assemble_list_primitive(
    rep_levels: &[u32],
    def_levels: &[u32],
    values: &[PqValue],
    max_def_level: u32,
    outer_optional: bool,
    element_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    // Length agreement.
    if rep_levels.len() != def_levels.len() {
        return Err(PqError::Bad(format!(
            "rep/def length mismatch: rep={} def={}",
            rep_levels.len(), def_levels.len()
        )));
    }

    let n = rep_levels.len();
    if n == 0 {
        return Ok(Vec::new());
    }

    let mut out: Vec<PqValue> = Vec::new();
    let mut current_list: Option<Vec<PqValue>> = None;
    let mut current_is_null: bool = false;
    let mut value_cursor = 0usize;

    for i in 0..n {
        let rep = rep_levels[i];
        let def = def_levels[i];

        if rep > 1 {
            return Err(PqError::Bad(format!(
                "rep level {rep} > max 1 for single-level LIST (position {i})"
            )));
        }
        if def > max_def_level {
            return Err(PqError::Bad(format!(
                "def level {def} > max {max_def_level} (position {i})"
            )));
        }

        if rep == 0 {
            // Flush previous record.
            if let Some(list) = current_list.take() {
                out.push(PqValue::List(list));
            } else if i > 0 && current_is_null {
                out.push(PqValue::Null);
            } else if i > 0 {
                // No active list and not marked null — shouldn't happen
                // with well-formed input. Defensive: push empty list.
                out.push(PqValue::List(Vec::new()));
            }
            current_is_null = false;

            // Start the new record based on def level. Priority of branches
            // matters: item-present (def == max_def) wins over "empty list"
            // when max_def == 0 (REQ-REQ-REQ shape where def is always 0 and
            // always means "item present").
            //
            // Disambiguation for the intermediate def value (def==1 when outer
            // optional, def==0 when outer required, both with max_def > that
            // value): the same def can mean "empty list" OR "first item is
            // null" depending on whether more rep==1 continuations follow.
            // Look ahead one position: if next rep == 1 we're starting a
            // non-empty list whose first item is null; otherwise empty list.
            let empty_or_null_def = if outer_optional { 1 } else { 0 };
            if outer_optional && def == 0 {
                // Outer LIST is null.
                current_list = None;
                current_is_null = true;
            } else if def == max_def_level {
                // Item present, first item of new list.
                let v = values.get(value_cursor).cloned().ok_or_else(||
                    PqError::Bad(format!("value stream exhausted at position {i}")))?;
                value_cursor += 1;
                current_list = Some(vec![v]);
            } else if def == empty_or_null_def {
                // Outer present, def below max_def. Two sub-cases:
                //   - next position has rep==1 → this is a null first item of
                //     a multi-item list (only valid if element_optional)
                //   - else → empty list
                let next_is_continuation = i + 1 < n && rep_levels[i + 1] == 1;
                if next_is_continuation {
                    if !element_optional {
                        return Err(PqError::Bad(format!(
                            "def {def} with continuation implies item-null but element is REQUIRED (position {i})"
                        )));
                    }
                    current_list = Some(vec![PqValue::Null]);
                } else {
                    current_list = Some(Vec::new());
                }
            } else {
                // Item null (def is between empty_or_null_def and max_def —
                // only meaningful when element_optional). First item of new
                // list.
                if !element_optional {
                    return Err(PqError::Bad(format!(
                        "def {def} implies item-null but element is REQUIRED (position {i})"
                    )));
                }
                current_list = Some(vec![PqValue::Null]);
            }
        } else {
            // rep == 1: continuing current list.
            let list = current_list.as_mut().ok_or_else(||
                PqError::Bad(format!("rep=1 without active list (position {i})")))?;

            if def == max_def_level {
                let v = values.get(value_cursor).cloned().ok_or_else(||
                    PqError::Bad(format!("value stream exhausted at position {i}")))?;
                value_cursor += 1;
                list.push(v);
            } else {
                // Item null.
                if !element_optional {
                    return Err(PqError::Bad(format!(
                        "def {def} implies item-null but element is REQUIRED (position {i})"
                    )));
                }
                list.push(PqValue::Null);
            }
        }
    }

    // Flush trailing record.
    if let Some(list) = current_list.take() {
        out.push(PqValue::List(list));
    } else if current_is_null {
        out.push(PqValue::Null);
    }

    // Validate value stream was fully consumed.
    if value_cursor != values.len() {
        return Err(PqError::Bad(format!(
            "values not fully consumed: cursor={value_cursor} len={}", values.len()
        )));
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn req_list_of_req_one_record_three_items() {
        // List<i64> REQUIRED-REQUIRED-REQUIRED:
        //   max_def_level = 0
        //   max_rep_level = 1
        //   def stream is all 0s (no nullability anywhere)
        //   3 items: [1, 2, 3]
        //
        // rep = [0, 1, 1], def = [0, 0, 0], values = [1, 2, 3]
        // outer_optional = false, element_optional = false
        let r = vec![0u32, 1, 1];
        let d = vec![0u32, 0, 0];
        let v = vec![PqValue::I64(1), PqValue::I64(2), PqValue::I64(3)];
        let out = assemble_list_primitive(&r, &d, &v, 0, false, false).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![
            PqValue::I64(1), PqValue::I64(2), PqValue::I64(3)
        ])]);
    }

    #[test]
    fn req_list_of_opt_one_record_with_null_item() {
        // List<Optional<i64>> REQUIRED-REQUIRED-OPTIONAL:
        //   max_def_level = 1 (element OPTIONAL contributes +1)
        //   max_rep_level = 1
        //   def == 1 → item present; def == 0 → item null
        //
        // 3 items: [10, null, 20]
        // rep = [0, 1, 1], def = [1, 0, 1], values = [10, 20]
        // outer_optional = false, element_optional = true
        let r = vec![0u32, 1, 1];
        let d = vec![1u32, 0, 1];
        let v = vec![PqValue::I64(10), PqValue::I64(20)];
        let out = assemble_list_primitive(&r, &d, &v, 1, false, true).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![
            PqValue::I64(10), PqValue::Null, PqValue::I64(20)
        ])]);
    }

    #[test]
    fn opt_list_of_req_one_record_two_items() {
        // Optional<List<i64>> OPTIONAL-REQUIRED-REQUIRED:
        //   max_def_level = 1
        //   max_rep_level = 1
        //   def == 0 → list null; def == 1 → item present
        // outer_optional = true, element_optional = false
        let r = vec![0u32, 1];
        let d = vec![1u32, 1];
        let v = vec![PqValue::I64(7), PqValue::I64(8)];
        let out = assemble_list_primitive(&r, &d, &v, 1, true, false).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![PqValue::I64(7), PqValue::I64(8)])]);
    }

    #[test]
    fn opt_list_of_opt_full_matrix() {
        // List<Optional<i64>>: OPTIONAL-REQUIRED-OPTIONAL
        //   max_def_level = 2
        //   rep ∈ {0,1}, def ∈ {0,1,2}
        //
        // Records:
        //   R0: null (outer list is NULL)
        //   R1: [] (empty list)
        //   R2: [42]
        //   R3: [null, 99]
        //
        // rep = [0,    0,   0,  0,   1]
        // def = [0,    1,   2,  1,   2]  // R0=0(null), R1=1(empty), R2=2(item), R3=1(null item) + 2(item)
        // values = [42, 99]
        let r = vec![0u32, 0, 0, 0, 1];
        let d = vec![0u32, 1, 2, 1, 2];
        let v = vec![PqValue::I64(42), PqValue::I64(99)];
        let out = assemble_list_primitive(&r, &d, &v, 2, true, true).unwrap();
        assert_eq!(out, vec![
            PqValue::Null,
            PqValue::List(vec![]),
            PqValue::List(vec![PqValue::I64(42)]),
            PqValue::List(vec![PqValue::Null, PqValue::I64(99)]),
        ]);
    }

    #[test]
    fn empty_input() {
        let out = assemble_list_primitive(&[], &[], &[], 1, true, false).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn multiple_records_simple() {
        // Three independent records, each a single-item list.
        // List<i64> REQ-REQ-REQ:
        //   max_def = 0
        //   rep = [0, 0, 0], def = [0, 0, 0], values = [1, 2, 3]
        let r = vec![0u32, 0, 0];
        let d = vec![0u32, 0, 0];
        let v = vec![PqValue::I64(1), PqValue::I64(2), PqValue::I64(3)];
        let out = assemble_list_primitive(&r, &d, &v, 0, false, false).unwrap();
        assert_eq!(out, vec![
            PqValue::List(vec![PqValue::I64(1)]),
            PqValue::List(vec![PqValue::I64(2)]),
            PqValue::List(vec![PqValue::I64(3)]),
        ]);
    }

    #[test]
    fn rejects_rep_level_overflow() {
        // rep=2 is invalid for single-level LIST (max_rep=1).
        let r = vec![0u32, 2];
        let d = vec![0u32, 0];
        let v = vec![PqValue::I64(1), PqValue::I64(2)];
        let err = assemble_list_primitive(&r, &d, &v, 0, false, false).unwrap_err();
        assert!(format!("{err:?}").contains("rep level 2"), "got {err:?}");
    }

    #[test]
    fn rejects_def_level_overflow() {
        // def=3 with max_def=2.
        let r = vec![0u32];
        let d = vec![3u32];
        let v = vec![PqValue::I64(1)];
        let err = assemble_list_primitive(&r, &d, &v, 2, true, true).unwrap_err();
        assert!(format!("{err:?}").contains("def level 3"), "got {err:?}");
    }

    #[test]
    fn rejects_value_underflow() {
        // def says 2 items present but values vec has only 1.
        let r = vec![0u32, 1];
        let d = vec![1u32, 1];
        let v = vec![PqValue::I64(1)];  // only one value
        let err = assemble_list_primitive(&r, &d, &v, 1, false, false).unwrap_err();
        assert!(format!("{err:?}").contains("value stream exhausted"), "got {err:?}");
    }

    #[test]
    fn rejects_value_unconsumed_overflow() {
        // def says 1 item present but values vec has 2.
        let r = vec![0u32];
        let d = vec![1u32];
        let v = vec![PqValue::I64(1), PqValue::I64(2)];
        let err = assemble_list_primitive(&r, &d, &v, 1, false, false).unwrap_err();
        assert!(format!("{err:?}").contains("values not fully consumed"), "got {err:?}");
    }
}
