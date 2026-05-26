//! SP143: Dremel-style record assembly for nested Parquet columns.
//!
//! Takes the parallel (rep_levels, def_levels, values) streams produced by
//! `decode_page_v1_nested` / `decode_data_page_v2_nested` and reconstructs
//! one `PqValue` per top-level record (where each record's value at a
//! LIST<primitive> column is `PqValue::List(Vec<PqValue>)` or `PqValue::Null`).
//!
//! V1 SP143: only single-level LIST<primitive> (max_rep_level == 1). SP144
//! adds Map+struct; SP145 adds deep nesting (max_rep_level >= 2).
//!
//! Standard Parquet def-level semantics (Dremel paper §4.1 + parquet-format
//! LogicalTypes.md). For the canonical 3-node List<primitive> encoding:
//!
//!   [OPT|REQ] group <name> (LIST) {
//!     REPEATED group element {
//!       [OPT|REQ] <PRIMITIVE> item
//!     }
//!   }
//!
//! max_def_level = (outer_optional as u32) + 1 /*REP*/ + (element_optional as u32)
//!
//! Def value classification (rep == 0, starting a new record):
//!   - outer_optional && def == 0                  → outer null
//!   - def == empty_list_threshold                 → empty list
//!         where threshold = outer_optional as u32
//!   - def == max_def_level                        → item present (consume one value)
//!   - else (strictly between threshold and max)   → item null (requires element_optional)
//!
//! Within a continuation (rep == 1), the outer is by construction present, so
//! the def value is either max_def_level (item present) or the item-null level.
//! No look-ahead is needed — the def value alone uniquely identifies the case.

#![allow(dead_code)]

use crate::{PqValue, PqError};

#[derive(Copy, Clone, Debug)]
enum DefCase {
    OuterNull,
    EmptyList,
    ItemNull,
    ItemPresent,
}

fn classify(
    def: u32,
    max_def_level: u32,
    outer_optional: bool,
    element_optional: bool,
    pos: usize,
) -> Result<DefCase, PqError> {
    if def > max_def_level {
        return Err(PqError::Bad(format!(
            "def level {def} > max {max_def_level} (position {pos})"
        )));
    }
    // Order matters: when max_def_level == 0 (REQ-REP-REQ with no REP? — not
    // representable; the canonical LIST always has REP contributing +1, so
    // max_def_level >= 1 always for a real LIST), the threshold equals max
    // only in degenerate shapes. For REQ-REP-REQ specifically, max_def=1 and
    // threshold=0, so d=0 → EmptyList and d=1 → ItemPresent. For OPT-REP-REQ,
    // max_def=2 and threshold=1, so d=0 → OuterNull, d=1 → EmptyList,
    // d=2 → ItemPresent.
    let empty_list_threshold = if outer_optional { 1 } else { 0 };

    if outer_optional && def == 0 {
        Ok(DefCase::OuterNull)
    } else if def == max_def_level {
        // Item present wins when max == threshold (no degenerate cases in real
        // LIST shapes, but defensively check max first so an OPT-REP-REQ with
        // max_def=2 still routes d=2 to ItemPresent rather than triggering the
        // threshold check incorrectly).
        Ok(DefCase::ItemPresent)
    } else if def == empty_list_threshold {
        Ok(DefCase::EmptyList)
    } else {
        // def strictly between threshold and max → item null
        if !element_optional {
            return Err(PqError::Bad(format!(
                "def {def} implies item null but element is REQUIRED (position {pos})"
            )));
        }
        Ok(DefCase::ItemNull)
    }
}

/// Assemble a stream of (rep, def, value) into one PqValue per record for a
/// LIST<primitive> column. Each record's value is either `PqValue::Null`
/// (when outer LIST is null) or `PqValue::List(items)`.
///
/// Parameters:
/// - `rep_levels`: per-position repetition level (∈ {0, 1} for single-level LIST)
/// - `def_levels`: per-position definition level (∈ {0..=max_def_level})
/// - `values`: actual primitive values, length = count of def == max_def_level
/// - `max_def_level`: from schema. For canonical LIST<primitive> this is
///     (outer_optional as u32) + 1 (REP) + (element_optional as u32).
/// - `outer_optional`: is the outer LIST group OPTIONAL?
/// - `element_optional`: is the inner element OPTIONAL?
///
/// Returns `Vec<PqValue>` — one per top-level record. Errors on malformed
/// inputs: level value > max, rep level > 1, value stream length mismatch,
/// item-null def with REQUIRED element, etc.
pub fn assemble_list_primitive(
    rep_levels: &[u32],
    def_levels: &[u32],
    values: &[PqValue],
    max_def_level: u32,
    outer_optional: bool,
    element_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    if rep_levels.len() != def_levels.len() {
        return Err(PqError::Bad(format!(
            "rep/def length mismatch: rep={} def={}",
            rep_levels.len(), def_levels.len()
        )));
    }

    let n = rep_levels.len();
    if n == 0 {
        // SP143 T10: even when there are no levels, a non-empty values
        // vec is malformed input (the value stream MUST be drained by
        // present-item def levels). Reject typed Bad — never silently
        // discard.
        if !values.is_empty() {
            return Err(PqError::Bad(format!(
                "no levels but {} values supplied", values.len()
            )));
        }
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

        let def_case = classify(def, max_def_level, outer_optional, element_optional, i)?;

        if rep == 0 {
            // Flush previous record (if any).
            if let Some(list) = current_list.take() {
                out.push(PqValue::List(list));
            } else if i > 0 && current_is_null {
                out.push(PqValue::Null);
            }
            current_is_null = false;

            match def_case {
                DefCase::OuterNull => {
                    current_list = None;
                    current_is_null = true;
                }
                DefCase::EmptyList => {
                    current_list = Some(Vec::new());
                }
                DefCase::ItemPresent => {
                    let v = values.get(value_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("value stream exhausted at position {i}")))?;
                    value_cursor += 1;
                    current_list = Some(vec![v]);
                }
                DefCase::ItemNull => {
                    current_list = Some(vec![PqValue::Null]);
                }
            }
        } else {
            // rep == 1: continuing the current list. Outer is by construction
            // present (a continuation only makes sense when the outer list
            // exists and is non-null).
            let list = current_list.as_mut().ok_or_else(||
                PqError::Bad(format!("rep=1 without active list (position {i})")))?;
            match def_case {
                DefCase::OuterNull => {
                    return Err(PqError::Bad(format!(
                        "rep=1 with outer-null def (position {i})"
                    )));
                }
                DefCase::EmptyList => {
                    return Err(PqError::Bad(format!(
                        "rep=1 with empty-list def — implies list both empty and continuing (position {i})"
                    )));
                }
                DefCase::ItemPresent => {
                    let v = values.get(value_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("value stream exhausted at position {i}")))?;
                    value_cursor += 1;
                    list.push(v);
                }
                DefCase::ItemNull => {
                    list.push(PqValue::Null);
                }
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
        // REQ-REP-REQ: max_def_level = 1 (REPEATED contributes +1).
        // 3 items, all present:
        //   rep = [0, 1, 1], def = [1, 1, 1], values = [1, 2, 3]
        // outer_optional = false, element_optional = false.
        let r = vec![0u32, 1, 1];
        let d = vec![1u32, 1, 1];
        let v = vec![PqValue::I64(1), PqValue::I64(2), PqValue::I64(3)];
        let out = assemble_list_primitive(&r, &d, &v, 1, false, false).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![
            PqValue::I64(1), PqValue::I64(2), PqValue::I64(3)
        ])]);
    }

    #[test]
    fn req_list_of_opt_one_record_with_null_item() {
        // REQ-REP-OPT: max_def_level = 2 (REP + inner OPT).
        // [10, null, 20]:
        //   rep = [0, 1, 1], def = [2, 1, 2], values = [10, 20]
        // outer_optional = false, element_optional = true.
        let r = vec![0u32, 1, 1];
        let d = vec![2u32, 1, 2];
        let v = vec![PqValue::I64(10), PqValue::I64(20)];
        let out = assemble_list_primitive(&r, &d, &v, 2, false, true).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![
            PqValue::I64(10), PqValue::Null, PqValue::I64(20)
        ])]);
    }

    #[test]
    fn opt_list_of_req_one_record_two_items() {
        // OPT-REP-REQ: max_def_level = 2 (outer OPT + REP).
        // [7, 8]:
        //   rep = [0, 1], def = [2, 2], values = [7, 8]
        // outer_optional = true, element_optional = false.
        let r = vec![0u32, 1];
        let d = vec![2u32, 2];
        let v = vec![PqValue::I64(7), PqValue::I64(8)];
        let out = assemble_list_primitive(&r, &d, &v, 2, true, false).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![PqValue::I64(7), PqValue::I64(8)])]);
    }

    #[test]
    fn opt_list_of_opt_full_matrix() {
        // OPT-REP-OPT: max_def_level = 3 (outer OPT + REP + inner OPT).
        // Records:
        //   R0: null         → def=0 (OuterNull)
        //   R1: []           → def=1 (EmptyList, threshold=1)
        //   R2: [42]         → def=3 (ItemPresent)
        //   R3: [null, 99]   → def=2 (ItemNull), rep=1 def=3 (ItemPresent)
        // rep = [0, 0, 0, 0, 1]
        // def = [0, 1, 3, 2, 3]
        // values = [42, 99]
        let r = vec![0u32, 0, 0, 0, 1];
        let d = vec![0u32, 1, 3, 2, 3];
        let v = vec![PqValue::I64(42), PqValue::I64(99)];
        let out = assemble_list_primitive(&r, &d, &v, 3, true, true).unwrap();
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
        // REQ-REP-REQ: 3 records, each a single-item list. max_def_level = 1.
        //   rep = [0, 0, 0], def = [1, 1, 1], values = [1, 2, 3]
        let r = vec![0u32, 0, 0];
        let d = vec![1u32, 1, 1];
        let v = vec![PqValue::I64(1), PqValue::I64(2), PqValue::I64(3)];
        let out = assemble_list_primitive(&r, &d, &v, 1, false, false).unwrap();
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
        let v = vec![PqValue::I64(1)];
        let err = assemble_list_primitive(&r, &d, &v, 0, false, false).unwrap_err();
        // Either rep-level error OR a def-classification error may fire first;
        // both are acceptable failure modes for malformed input.
        let msg = format!("{err:?}");
        assert!(
            msg.contains("rep level 2") || msg.contains("def"),
            "got {err:?}"
        );
    }

    #[test]
    fn rejects_def_level_overflow() {
        // OPT-REP-OPT has max_def=3; d=4 must be rejected.
        let r = vec![0u32];
        let d = vec![4u32];
        let v = vec![PqValue::I64(1)];
        let err = assemble_list_primitive(&r, &d, &v, 3, true, true).unwrap_err();
        assert!(format!("{err:?}").contains("def level 4"), "got {err:?}");
    }

    #[test]
    fn rejects_value_underflow() {
        // OPT-REP-REQ, max_def=2. 2 items present require 2 values; only 1
        // given.
        let r = vec![0u32, 1];
        let d = vec![2u32, 2];
        let v = vec![PqValue::I64(1)];
        let err = assemble_list_primitive(&r, &d, &v, 2, true, false).unwrap_err();
        assert!(format!("{err:?}").contains("value stream exhausted"), "got {err:?}");
    }

    #[test]
    fn rejects_value_unconsumed_overflow() {
        // OPT-REP-REQ, max_def=2. 1 item present consumes 1 value; 2 given.
        let r = vec![0u32];
        let d = vec![2u32];
        let v = vec![PqValue::I64(1), PqValue::I64(2)];
        let err = assemble_list_primitive(&r, &d, &v, 2, true, false).unwrap_err();
        assert!(format!("{err:?}").contains("values not fully consumed"), "got {err:?}");
    }
}

/// SP144 T3: Dremel-style record assembly for Map<K, V> columns.
///
/// Canonical 3-node Map encoding (parquet-format LogicalTypes.md):
///
///   [OPT|REQ] group <name> (MAP) {
///     REPEATED group key_value {
///       REQUIRED <PRIMITIVE> key;
///       [OPT|REQ] <PRIMITIVE> value;
///     }
///   }
///
/// Schema-derived levels (for the V column-chunk, which carries the
/// authoritative rep/def streams used here):
///   max_def_level = (outer_optional as u32) + 1 /*REP middle*/ + (value_optional as u32)
///   max_rep_level = 1
///
/// The K column-chunk shares the same ancestor path (OPT outer + REP middle),
/// so its rep_levels are byte-identical to V's. Its def stream differs only
/// when V is OPTIONAL: K's max_def = V's max_def - 1 because the K leaf is
/// REQUIRED. Callers supply the already-decoded `keys` and `values` slices;
/// this assembler consumes them in parallel per the V-stream's def-level
/// classification.
///
/// Def value classification (V's def, since the V stream is authoritative):
///   - outer_optional && def == 0   → outer null (whole map is null)
///   - def == empty_list_threshold  → empty map (middle group absent)
///         where threshold = outer_optional as u32
///   - def == max_def_level         → item present (consume K and V)
///   - else (strictly between)      → value null (consume K only, push V_null);
///         only valid when value_optional
///
/// Within a continuation (rep == 1), the outer is by construction present;
/// the def value is either max_def_level (item present) or the value-null
/// level.
pub fn assemble_map_kv(
    rep_levels: &[u32],
    def_levels: &[u32],
    keys: &[PqValue],
    values: &[PqValue],
    max_def_level: u32,
    outer_optional: bool,
    value_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    if rep_levels.len() != def_levels.len() {
        return Err(PqError::Bad(format!(
            "rep/def length mismatch: rep={} def={}",
            rep_levels.len(), def_levels.len()
        )));
    }

    let n = rep_levels.len();
    if n == 0 {
        if !keys.is_empty() || !values.is_empty() {
            return Err(PqError::Bad(format!(
                "no levels but {} keys and {} values supplied",
                keys.len(), values.len()
            )));
        }
        return Ok(Vec::new());
    }

    let empty_list_threshold: u32 = if outer_optional { 1 } else { 0 };

    #[derive(Copy, Clone, Debug)]
    enum MapDefCase {
        OuterNull,
        EmptyMap,
        ValueNull,
        ItemPresent,
    }

    let classify = |def: u32, pos: usize| -> Result<MapDefCase, PqError> {
        if def > max_def_level {
            return Err(PqError::Bad(format!(
                "def level {def} > max {max_def_level} (position {pos})"
            )));
        }
        if outer_optional && def == 0 {
            Ok(MapDefCase::OuterNull)
        } else if def == max_def_level {
            // Item present wins when max == threshold (defensive ordering,
            // mirrors assemble_list_primitive::classify).
            Ok(MapDefCase::ItemPresent)
        } else if def == empty_list_threshold {
            Ok(MapDefCase::EmptyMap)
        } else {
            // def strictly between threshold and max → value null.
            if !value_optional {
                return Err(PqError::Bad(format!(
                    "def {def} implies value null but value is REQUIRED (position {pos})"
                )));
            }
            Ok(MapDefCase::ValueNull)
        }
    };

    let mut out: Vec<PqValue> = Vec::new();
    let mut current_map: Option<Vec<(PqValue, PqValue)>> = None;
    let mut current_is_null: bool = false;
    let mut k_cursor = 0usize;
    let mut v_cursor = 0usize;

    for i in 0..n {
        let rep = rep_levels[i];
        let def = def_levels[i];
        if rep > 1 {
            return Err(PqError::Bad(format!(
                "rep level {rep} > max 1 for Map (position {i})"
            )));
        }
        let dc = classify(def, i)?;

        if rep == 0 {
            // Flush previous record (if any).
            if let Some(map) = current_map.take() {
                out.push(PqValue::Map(map));
            } else if i > 0 && current_is_null {
                out.push(PqValue::Null);
            }
            current_is_null = false;

            match dc {
                MapDefCase::OuterNull => {
                    current_map = None;
                    current_is_null = true;
                }
                MapDefCase::EmptyMap => {
                    current_map = Some(Vec::new());
                }
                MapDefCase::ValueNull => {
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("key stream exhausted at position {i}")))?;
                    k_cursor += 1;
                    current_map = Some(vec![(k, PqValue::Null)]);
                }
                MapDefCase::ItemPresent => {
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("key stream exhausted at position {i}")))?;
                    k_cursor += 1;
                    let v = values.get(v_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("value stream exhausted at position {i}")))?;
                    v_cursor += 1;
                    current_map = Some(vec![(k, v)]);
                }
            }
        } else {
            // rep == 1: continuing the current map.
            let map = current_map.as_mut().ok_or_else(||
                PqError::Bad(format!("rep=1 without active map (position {i})")))?;
            match dc {
                MapDefCase::OuterNull => {
                    return Err(PqError::Bad(format!(
                        "rep=1 with outer-null def (position {i})"
                    )));
                }
                MapDefCase::EmptyMap => {
                    return Err(PqError::Bad(format!(
                        "rep=1 with empty-map def — implies map both empty and continuing (position {i})"
                    )));
                }
                MapDefCase::ValueNull => {
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("key stream exhausted at position {i}")))?;
                    k_cursor += 1;
                    map.push((k, PqValue::Null));
                }
                MapDefCase::ItemPresent => {
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("key stream exhausted at position {i}")))?;
                    k_cursor += 1;
                    let v = values.get(v_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("value stream exhausted at position {i}")))?;
                    v_cursor += 1;
                    map.push((k, v));
                }
            }
        }
    }

    // Flush trailing record.
    if let Some(map) = current_map.take() {
        out.push(PqValue::Map(map));
    } else if current_is_null {
        out.push(PqValue::Null);
    }

    if k_cursor != keys.len() {
        return Err(PqError::Bad(format!(
            "keys not fully consumed: cursor={k_cursor} len={}", keys.len()
        )));
    }
    if v_cursor != values.len() {
        return Err(PqError::Bad(format!(
            "values not fully consumed: cursor={v_cursor} len={}", values.len()
        )));
    }

    Ok(out)
}

#[cfg(test)]
mod map_tests {
    use super::*;

    #[test]
    fn req_map_of_req_req_one_record_two_items() {
        // REQ-REP-REQ-REQ: max_def=1
        // Map {"a"->1, "b"->2}: rep=[0,1], def=[1,1], keys=["a","b"], values=[1,2]
        let r = vec![0u32, 1];
        let d = vec![1u32, 1];
        let k = vec![PqValue::Bytes(b"a".to_vec()), PqValue::Bytes(b"b".to_vec())];
        let v = vec![PqValue::I64(1), PqValue::I64(2)];
        let out = assemble_map_kv(&r, &d, &k, &v, 1, false, false).unwrap();
        assert_eq!(out, vec![PqValue::Map(vec![
            (PqValue::Bytes(b"a".to_vec()), PqValue::I64(1)),
            (PqValue::Bytes(b"b".to_vec()), PqValue::I64(2)),
        ])]);
    }

    #[test]
    fn req_map_of_req_opt_one_value_null() {
        // REQ-REP-REQ-OPT: max_def=2
        // Map {"a"->1, "b"->null}: rep=[0,1], def=[2,1], keys=["a","b"], values=[1]
        let r = vec![0u32, 1];
        let d = vec![2u32, 1];
        let k = vec![PqValue::Bytes(b"a".to_vec()), PqValue::Bytes(b"b".to_vec())];
        let v = vec![PqValue::I64(1)];
        let out = assemble_map_kv(&r, &d, &k, &v, 2, false, true).unwrap();
        assert_eq!(out, vec![PqValue::Map(vec![
            (PqValue::Bytes(b"a".to_vec()), PqValue::I64(1)),
            (PqValue::Bytes(b"b".to_vec()), PqValue::Null),
        ])]);
    }

    #[test]
    fn opt_map_of_req_req_two_records_first_null() {
        // OPT-REP-REQ-REQ: max_def=2
        // [null, {"x"->7}]: rep=[0,0], def=[0,2], keys=["x"], values=[7]
        let r = vec![0u32, 0];
        let d = vec![0u32, 2];
        let k = vec![PqValue::Bytes(b"x".to_vec())];
        let v = vec![PqValue::I64(7)];
        let out = assemble_map_kv(&r, &d, &k, &v, 2, true, false).unwrap();
        assert_eq!(out, vec![
            PqValue::Null,
            PqValue::Map(vec![(PqValue::Bytes(b"x".to_vec()), PqValue::I64(7))]),
        ]);
    }

    #[test]
    fn opt_map_of_req_opt_full_matrix() {
        // OPT-REP-REQ-OPT: max_def=3
        // R0 null              → def=0
        // R1 {}                → def=1
        // R2 {"a"->1}          → def=3 (item present)
        // R3 {"b"->null, "c"->9} → def=2 (value null) then rep=1 def=3 (item present)
        // rep=[0,0,0,0,1], def=[0,1,3,2,3]
        // keys=["a","b","c"] (3 middle-present slots)
        // values=[1, 9] (2 V-present slots — R3's "b"->null does NOT consume V)
        let r = vec![0u32, 0, 0, 0, 1];
        let d = vec![0u32, 1, 3, 2, 3];
        let k = vec![
            PqValue::Bytes(b"a".to_vec()),
            PqValue::Bytes(b"b".to_vec()),
            PqValue::Bytes(b"c".to_vec()),
        ];
        let v = vec![PqValue::I64(1), PqValue::I64(9)];
        let out = assemble_map_kv(&r, &d, &k, &v, 3, true, true).unwrap();
        assert_eq!(out, vec![
            PqValue::Null,
            PqValue::Map(Vec::new()),
            PqValue::Map(vec![(PqValue::Bytes(b"a".to_vec()), PqValue::I64(1))]),
            PqValue::Map(vec![
                (PqValue::Bytes(b"b".to_vec()), PqValue::Null),
                (PqValue::Bytes(b"c".to_vec()), PqValue::I64(9)),
            ]),
        ]);
    }

    #[test]
    fn empty_input() {
        let out = assemble_map_kv(&[], &[], &[], &[], 1, true, false).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn rejects_key_stream_truncated() {
        let r = vec![0u32, 1];
        let d = vec![1u32, 1];
        let k = vec![PqValue::Bytes(b"a".to_vec())]; // only 1, need 2
        let v = vec![PqValue::I64(1), PqValue::I64(2)];
        let err = assemble_map_kv(&r, &d, &k, &v, 1, false, false).unwrap_err();
        assert!(format!("{err:?}").contains("key stream exhausted"), "got {err:?}");
    }

    #[test]
    fn rejects_value_stream_truncated() {
        let r = vec![0u32, 1];
        let d = vec![1u32, 1];
        let k = vec![PqValue::Bytes(b"a".to_vec()), PqValue::Bytes(b"b".to_vec())];
        let v = vec![PqValue::I64(1)]; // only 1, need 2
        let err = assemble_map_kv(&r, &d, &k, &v, 1, false, false).unwrap_err();
        assert!(format!("{err:?}").contains("value stream exhausted"), "got {err:?}");
    }

    #[test]
    fn rejects_keys_not_fully_consumed() {
        let r = vec![0u32];
        let d = vec![1u32];
        let k = vec![PqValue::Bytes(b"a".to_vec()), PqValue::Bytes(b"b".to_vec())]; // extra
        let v = vec![PqValue::I64(1)];
        let err = assemble_map_kv(&r, &d, &k, &v, 1, false, false).unwrap_err();
        assert!(format!("{err:?}").contains("keys not fully consumed"), "got {err:?}");
    }
}

/// SP144 T4: struct-column "assembler" — a zip of N already-decoded
/// primitive field columns into one `PqValue::Struct(Vec<(String, PqValue)>)`
/// per output row.
///
/// V1 limitations:
/// - All fields must be primitive (no nested LIST/MAP/struct within a
///   struct — rejected upstream in `classify_column_plan` as SP145).
/// - Outer-OPTIONAL detection uses the post-zip heuristic: if
///   `outer_optional` is true AND every field's value at row i is
///   `PqValue::Null`, the row emits `PqValue::Null` (the struct itself
///   was null). This aliases with a non-null struct whose fields all
///   happen to be Null — accepted V1 trade-off; documented here.
///   SP145 may revisit using an explicit def-level read.
///
/// Errors:
/// - `field_names.len() != field_columns.len()` — shape mismatch
/// - any two field columns have different lengths — row-count mismatch
pub fn assemble_struct(
    field_names: &[String],
    field_columns: &[Vec<PqValue>],
    outer_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    if field_names.len() != field_columns.len() {
        return Err(PqError::Bad(format!(
            "struct field_names len {} != field_columns len {}",
            field_names.len(), field_columns.len()
        )));
    }
    if field_columns.is_empty() {
        return Err(PqError::Bad("struct with no fields".into()));
    }
    let num_rows = field_columns[0].len();
    for (i, col) in field_columns.iter().enumerate() {
        if col.len() != num_rows {
            return Err(PqError::Bad(format!(
                "struct field '{}' length {} != row count {}",
                field_names[i], col.len(), num_rows
            )));
        }
    }

    let mut out: Vec<PqValue> = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        // Outer-OPTIONAL heuristic: if outer is OPTIONAL AND all fields
        // are Null at this row, treat the struct itself as null.
        if outer_optional {
            let all_null = field_columns.iter().all(|col| col[row] == PqValue::Null);
            if all_null {
                out.push(PqValue::Null);
                continue;
            }
        }
        let pairs: Vec<(String, PqValue)> = field_columns
            .iter()
            .enumerate()
            .map(|(i, col)| (field_names[i].clone(), col[row].clone()))
            .collect();
        out.push(PqValue::Struct(pairs));
    }

    Ok(out)
}

#[cfg(test)]
mod struct_tests {
    use super::*;

    #[test]
    fn req_struct_two_fields_three_rows() {
        // REQ struct {id: i64, name: String}; 3 rows
        let names = vec!["id".to_string(), "name".to_string()];
        let cols = vec![
            vec![PqValue::I64(1), PqValue::I64(2), PqValue::I64(3)],
            vec![
                PqValue::Bytes(b"alice".to_vec()),
                PqValue::Bytes(b"bob".to_vec()),
                PqValue::Bytes(b"carol".to_vec()),
            ],
        ];
        let out = assemble_struct(&names, &cols, false).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], PqValue::Struct(vec![
            ("id".into(), PqValue::I64(1)),
            ("name".into(), PqValue::Bytes(b"alice".to_vec())),
        ]));
        assert_eq!(out[1], PqValue::Struct(vec![
            ("id".into(), PqValue::I64(2)),
            ("name".into(), PqValue::Bytes(b"bob".to_vec())),
        ]));
        assert_eq!(out[2], PqValue::Struct(vec![
            ("id".into(), PqValue::I64(3)),
            ("name".into(), PqValue::Bytes(b"carol".to_vec())),
        ]));
    }

    #[test]
    fn opt_struct_with_null_row() {
        // OPT struct, second row is null (all fields Null)
        let names = vec!["id".to_string(), "name".to_string()];
        let cols = vec![
            vec![PqValue::I64(1), PqValue::Null, PqValue::I64(3)],
            vec![
                PqValue::Bytes(b"alice".to_vec()),
                PqValue::Null,
                PqValue::Bytes(b"carol".to_vec()),
            ],
        ];
        let out = assemble_struct(&names, &cols, true).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], PqValue::Struct(vec![
            ("id".into(), PqValue::I64(1)),
            ("name".into(), PqValue::Bytes(b"alice".to_vec())),
        ]));
        assert_eq!(out[1], PqValue::Null, "all-Null row in OPT struct → Null");
        assert_eq!(out[2], PqValue::Struct(vec![
            ("id".into(), PqValue::I64(3)),
            ("name".into(), PqValue::Bytes(b"carol".to_vec())),
        ]));
    }

    #[test]
    fn req_struct_three_fields_one_row() {
        let names = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let cols = vec![
            vec![PqValue::I64(1)],
            vec![PqValue::Bool(true)],
            vec![PqValue::F64(3.14)],
        ];
        let out = assemble_struct(&names, &cols, false).unwrap();
        assert_eq!(out, vec![PqValue::Struct(vec![
            ("a".into(), PqValue::I64(1)),
            ("b".into(), PqValue::Bool(true)),
            ("c".into(), PqValue::F64(3.14)),
        ])]);
    }

    #[test]
    #[allow(non_snake_case)]
    fn opt_struct_with_partial_null_NOT_null() {
        // OPT struct, row has SOME but not ALL fields Null → struct is
        // present (not null), with the null fields preserved as PqValue::Null
        // inside.
        let names = vec!["id".to_string(), "name".to_string()];
        let cols = vec![
            vec![PqValue::I64(1)],
            vec![PqValue::Null], // only one field is null, not all
        ];
        let out = assemble_struct(&names, &cols, true).unwrap();
        assert_eq!(out, vec![PqValue::Struct(vec![
            ("id".into(), PqValue::I64(1)),
            ("name".into(), PqValue::Null),
        ])]);
    }

    #[test]
    fn rejects_field_length_mismatch() {
        let names = vec!["a".to_string(), "b".to_string()];
        let cols = vec![
            vec![PqValue::I64(1), PqValue::I64(2)],
            vec![PqValue::Bool(true)], // length 1 vs 2
        ];
        let err = assemble_struct(&names, &cols, false).unwrap_err();
        assert!(format!("{err:?}").contains("length") && format!("{err:?}").contains("row count"),
                "got {err:?}");
    }

    #[test]
    fn rejects_names_columns_mismatch() {
        let names = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let cols = vec![
            vec![PqValue::I64(1)],
            vec![PqValue::Bool(true)],
            // missing third column
        ];
        let err = assemble_struct(&names, &cols, false).unwrap_err();
        assert!(format!("{err:?}").contains("field_names"), "got {err:?}");
    }

    #[test]
    fn rejects_empty_fields() {
        let names: Vec<String> = vec![];
        let cols: Vec<Vec<PqValue>> = vec![];
        let err = assemble_struct(&names, &cols, false).unwrap_err();
        assert!(format!("{err:?}").contains("no fields"), "got {err:?}");
    }
}

// ──────────────────────────────────────────────────────────────────────────
// SP145: deep nesting (final OBJ-2c-5 slice). Per-shape compositional
// assemblers for the 4 cases SP145 lifts:
//   - List<List<primitive>>       → assemble_list_of_list_primitive
//   - List<struct<primitives>>    → assemble_list_of_struct
//   - Map<K, struct<primitives>>  → assemble_map_of_struct
//   - struct<List/struct>         → assemble_struct (UNCHANGED — feed it
//                                   recursively-built field columns)
// Plus 2 BOLD cross-products:
//   - Map<K, List<primitive>>     → assemble_map_of_list
//   - struct fields can hold ANY  → reuse assemble_struct with nested cols
//
// The unifying property: each new assembler takes the SAME (rep, def)
// stream that the inner leaves share (they share the outer REPEATED
// ancestor by construction). The inner-shape decode is fed the SAME
// per-position (rep, def) classification and produces N parallel
// streams that we either zip (struct) or split per item slot
// (list of struct / map of struct) before composing into outer
// PqValue::List / PqValue::Map / PqValue::Struct records.
// ──────────────────────────────────────────────────────────────────────────

/// SP145: assemble a stream of (rep, def, value) into one PqValue per
/// record for a `List<List<primitive>>` column (max_rep_level = 2).
///
/// Schema shape (pyarrow canonical):
///
///   [OPT|REQ] group outer (LIST) {
///     REPEATED group list_outer {
///       [OPT|REQ] group element (LIST) {
///         REPEATED group list_inner {
///           [OPT|REQ] <PRIMITIVE> item
///         }
///       }
///     }
///   }
///
/// Level math:
///   max_rep_level = 2
///   max_def_level = (outer_optional as u32) + 1 /*REP outer*/
///                   + (inner_optional as u32) + 1 /*REP inner*/
///                   + (item_optional as u32)
///
/// Def classification (let d = def value):
///   d == 0 && outer_optional   → outer LIST is null
///   d == empty_outer_threshold → outer LIST is empty
///         where threshold = outer_optional as u32
///   d == inner_null_threshold  → outer is non-empty, inner LIST is null
///         where threshold = (outer_optional as u32) + 1
///         (only valid when inner_optional)
///   d == empty_inner_threshold → inner LIST is empty (no items)
///         where threshold = (outer_optional as u32) + 1 + (inner_optional as u32)
///   d == item_null_threshold   → inner has an item slot but item is null
///         where threshold = max_def_level - 1
///         (only valid when item_optional)
///   d == max_def_level         → item present (consume value)
///
/// Rep handling:
///   rep == 0 → start NEW outer record; flush previous
///   rep == 1 → close current inner list; start NEW inner list inside outer
///   rep == 2 → continue current inner list (append item)
pub fn assemble_list_of_list_primitive(
    rep_levels: &[u32],
    def_levels: &[u32],
    values: &[PqValue],
    max_def_level: u32,
    outer_optional: bool,
    inner_optional: bool,
    item_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    if rep_levels.len() != def_levels.len() {
        return Err(PqError::Bad(format!(
            "rep/def length mismatch: rep={} def={}",
            rep_levels.len(), def_levels.len()
        )));
    }
    let n = rep_levels.len();
    if n == 0 {
        if !values.is_empty() {
            return Err(PqError::Bad(format!(
                "no levels but {} values supplied", values.len()
            )));
        }
        return Ok(Vec::new());
    }

    let empty_outer_threshold: u32 = outer_optional as u32;
    let inner_null_threshold: u32 = (outer_optional as u32) + 1;
    let empty_inner_threshold: u32 =
        (outer_optional as u32) + 1 + (inner_optional as u32);
    let item_null_threshold: u32 = if max_def_level > 0 {
        max_def_level - 1
    } else {
        0
    };

    #[derive(Copy, Clone, Debug)]
    enum LoLCase {
        OuterNull,
        OuterEmpty,
        InnerNull,
        InnerEmpty,
        ItemNull,
        ItemPresent,
    }

    let classify = |def: u32, pos: usize| -> Result<LoLCase, PqError> {
        if def > max_def_level {
            return Err(PqError::Bad(format!(
                "def level {def} > max {max_def_level} (position {pos})"
            )));
        }
        // Order matters: ItemPresent wins when max == another threshold
        // (defensive, mirrors SP143's classify).
        if def == max_def_level {
            return Ok(LoLCase::ItemPresent);
        }
        if outer_optional && def == 0 {
            return Ok(LoLCase::OuterNull);
        }
        if def == empty_outer_threshold {
            return Ok(LoLCase::OuterEmpty);
        }
        if inner_optional && def == inner_null_threshold {
            return Ok(LoLCase::InnerNull);
        }
        if def == empty_inner_threshold {
            return Ok(LoLCase::InnerEmpty);
        }
        if item_optional && def == item_null_threshold {
            return Ok(LoLCase::ItemNull);
        }
        Err(PqError::Bad(format!(
            "unclassified def {def} (max={max_def_level}, \
             outer_opt={outer_optional}, inner_opt={inner_optional}, \
             item_opt={item_optional}) at position {pos}"
        )))
    };

    let mut out: Vec<PqValue> = Vec::new();
    // Outer accumulator: list of inner-lists.
    let mut current_outer: Option<Vec<PqValue>> = None;
    let mut current_outer_is_null: bool = false;
    // Inner accumulator: list of items (within the current outer).
    let mut current_inner: Option<Vec<PqValue>> = None;
    let mut current_inner_is_null: bool = false;
    let mut value_cursor = 0usize;

    let flush_inner = |outer: &mut Option<Vec<PqValue>>,
                       inner: &mut Option<Vec<PqValue>>,
                       inner_null: &mut bool|
     -> Result<(), PqError> {
        if let Some(inner_items) = inner.take() {
            outer
                .as_mut()
                .ok_or_else(|| PqError::Bad(
                    "flush_inner with no active outer".into()))?
                .push(PqValue::List(inner_items));
        } else if *inner_null {
            outer
                .as_mut()
                .ok_or_else(|| PqError::Bad(
                    "flush_inner null with no active outer".into()))?
                .push(PqValue::Null);
            *inner_null = false;
        }
        Ok(())
    };

    let flush_outer = |out: &mut Vec<PqValue>,
                       outer: &mut Option<Vec<PqValue>>,
                       outer_null: &mut bool| {
        if let Some(outer_items) = outer.take() {
            out.push(PqValue::List(outer_items));
        } else if *outer_null {
            out.push(PqValue::Null);
            *outer_null = false;
        }
    };

    for i in 0..n {
        let rep = rep_levels[i];
        let def = def_levels[i];
        if rep > 2 {
            return Err(PqError::Bad(format!(
                "rep level {rep} > max 2 for List<List> (position {i})"
            )));
        }
        let dc = classify(def, i)?;

        if rep == 0 {
            // New outer record. Flush previous outer (and any in-flight inner).
            flush_inner(&mut current_outer, &mut current_inner, &mut current_inner_is_null)?;
            flush_outer(&mut out, &mut current_outer, &mut current_outer_is_null);

            match dc {
                LoLCase::OuterNull => {
                    current_outer = None;
                    current_outer_is_null = true;
                    current_inner = None;
                    current_inner_is_null = false;
                }
                LoLCase::OuterEmpty => {
                    current_outer = Some(Vec::new());
                    current_inner = None;
                    current_inner_is_null = false;
                }
                LoLCase::InnerNull => {
                    current_outer = Some(Vec::new());
                    current_inner = None;
                    current_inner_is_null = true;
                }
                LoLCase::InnerEmpty => {
                    current_outer = Some(Vec::new());
                    current_inner = Some(Vec::new());
                    current_inner_is_null = false;
                }
                LoLCase::ItemNull => {
                    current_outer = Some(Vec::new());
                    current_inner = Some(vec![PqValue::Null]);
                    current_inner_is_null = false;
                }
                LoLCase::ItemPresent => {
                    let v = values.get(value_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("value stream exhausted at position {i}")))?;
                    value_cursor += 1;
                    current_outer = Some(Vec::new());
                    current_inner = Some(vec![v]);
                    current_inner_is_null = false;
                }
            }
        } else if rep == 1 {
            // New inner list within current outer. Outer must exist + be present.
            if current_outer.is_none() {
                return Err(PqError::Bad(format!(
                    "rep=1 without active outer list (position {i})"
                )));
            }
            // Flush previous inner into outer.
            flush_inner(&mut current_outer, &mut current_inner, &mut current_inner_is_null)?;

            match dc {
                LoLCase::OuterNull => {
                    return Err(PqError::Bad(format!(
                        "rep=1 with outer-null def (position {i})"
                    )));
                }
                LoLCase::OuterEmpty => {
                    return Err(PqError::Bad(format!(
                        "rep=1 with outer-empty def (position {i})"
                    )));
                }
                LoLCase::InnerNull => {
                    current_inner = None;
                    current_inner_is_null = true;
                }
                LoLCase::InnerEmpty => {
                    current_inner = Some(Vec::new());
                    current_inner_is_null = false;
                }
                LoLCase::ItemNull => {
                    current_inner = Some(vec![PqValue::Null]);
                    current_inner_is_null = false;
                }
                LoLCase::ItemPresent => {
                    let v = values.get(value_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("value stream exhausted at position {i}")))?;
                    value_cursor += 1;
                    current_inner = Some(vec![v]);
                    current_inner_is_null = false;
                }
            }
        } else {
            // rep == 2: continue inner list.
            let inner = current_inner.as_mut().ok_or_else(||
                PqError::Bad(format!("rep=2 without active inner list (position {i})")))?;
            match dc {
                LoLCase::OuterNull | LoLCase::OuterEmpty | LoLCase::InnerNull | LoLCase::InnerEmpty => {
                    return Err(PqError::Bad(format!(
                        "rep=2 with non-item def {def:?} (position {i})", def = dc
                    )));
                }
                LoLCase::ItemNull => {
                    inner.push(PqValue::Null);
                }
                LoLCase::ItemPresent => {
                    let v = values.get(value_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("value stream exhausted at position {i}")))?;
                    value_cursor += 1;
                    inner.push(v);
                }
            }
        }
    }

    // Flush trailing record.
    flush_inner(&mut current_outer, &mut current_inner, &mut current_inner_is_null)?;
    flush_outer(&mut out, &mut current_outer, &mut current_outer_is_null);

    if value_cursor != values.len() {
        return Err(PqError::Bad(format!(
            "values not fully consumed: cursor={value_cursor} len={}", values.len()
        )));
    }

    Ok(out)
}

#[cfg(test)]
mod lol_tests {
    use super::*;

    #[test]
    fn req_req_req_one_outer_two_inner() {
        // REQ outer, REQ inner, REQ item: max_def=2, max_rep=2.
        // Record: [[1,2], [3]]
        // Levels:
        //   (rep=0, def=2, val=1)
        //   (rep=2, def=2, val=2)
        //   (rep=1, def=2, val=3)
        let r = vec![0u32, 2, 1];
        let d = vec![2u32, 2, 2];
        let v = vec![PqValue::I64(1), PqValue::I64(2), PqValue::I64(3)];
        let out = assemble_list_of_list_primitive(&r, &d, &v, 2, false, false, false).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![
            PqValue::List(vec![PqValue::I64(1), PqValue::I64(2)]),
            PqValue::List(vec![PqValue::I64(3)]),
        ])]);
    }

    #[test]
    fn req_opt_req_inner_null() {
        // REQ outer, OPT inner, REQ item: max_def=3, max_rep=2.
        // Record: [[1], null, [2,3]]
        // For "null" inner-list inside, encoding:
        //   (rep=0, def=3, val=1)  → outer starts, inner-list with item 1
        //   (rep=1, def=1, _)      → new inner-list, inner is null (def==inner_null_thr=1)
        //   (rep=1, def=3, val=2)  → new inner-list with item 2
        //   (rep=2, def=3, val=3)  → continue inner-list, item 3
        let r = vec![0u32, 1, 1, 2];
        let d = vec![3u32, 1, 3, 3];
        let v = vec![PqValue::I64(1), PqValue::I64(2), PqValue::I64(3)];
        let out = assemble_list_of_list_primitive(&r, &d, &v, 3, false, true, false).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![
            PqValue::List(vec![PqValue::I64(1)]),
            PqValue::Null,
            PqValue::List(vec![PqValue::I64(2), PqValue::I64(3)]),
        ])]);
    }

    #[test]
    fn req_opt_req_inner_empty_within() {
        // Record: [[], [10, 20]]
        // For empty inner, def == empty_inner_threshold = 0 + 1 + 1 = 2
        //   (rep=0, def=2, _)      → outer starts, inner empty
        //   (rep=1, def=3, val=10) → new inner-list, item 10
        //   (rep=2, def=3, val=20) → continue, item 20
        let r = vec![0u32, 1, 2];
        let d = vec![2u32, 3, 3];
        let v = vec![PqValue::I64(10), PqValue::I64(20)];
        let out = assemble_list_of_list_primitive(&r, &d, &v, 3, false, true, false).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![
            PqValue::List(vec![]),
            PqValue::List(vec![PqValue::I64(10), PqValue::I64(20)]),
        ])]);
    }

    #[test]
    fn empty_outer_list() {
        // REQ outer, OPT inner, REQ item: max_def=3.
        // Record: [] (empty outer)
        //   (rep=0, def=0, _)  → outer empty (threshold = outer_opt = 0)
        let r = vec![0u32];
        let d = vec![0u32];
        let v: Vec<PqValue> = vec![];
        let out = assemble_list_of_list_primitive(&r, &d, &v, 3, false, true, false).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![])]);
    }

    #[test]
    fn opt_outer_null_record() {
        // OPT outer, REQ inner, REQ item: max_def=2, threshold outer=1
        // Record: null (outer is null)
        //   (rep=0, def=0, _)  → outer null
        let r = vec![0u32];
        let d = vec![0u32];
        let v: Vec<PqValue> = vec![];
        let out = assemble_list_of_list_primitive(&r, &d, &v, 2, true, false, false).unwrap();
        assert_eq!(out, vec![PqValue::Null]);
    }

    #[test]
    fn rejects_rep_overflow() {
        let r = vec![0u32, 3];
        let d = vec![2u32, 2];
        let v = vec![PqValue::I64(1), PqValue::I64(2)];
        let err = assemble_list_of_list_primitive(&r, &d, &v, 2, false, false, false).unwrap_err();
        assert!(format!("{err:?}").contains("rep level 3"), "got {err:?}");
    }

    #[test]
    fn rejects_value_underflow() {
        let r = vec![0u32, 2];
        let d = vec![2u32, 2];
        let v = vec![PqValue::I64(1)]; // need 2
        let err = assemble_list_of_list_primitive(&r, &d, &v, 2, false, false, false).unwrap_err();
        assert!(format!("{err:?}").contains("value stream exhausted"), "got {err:?}");
    }

    #[test]
    fn rejects_value_unconsumed() {
        let r = vec![0u32];
        let d = vec![2u32];
        let v = vec![PqValue::I64(1), PqValue::I64(2)]; // extra
        let err = assemble_list_of_list_primitive(&r, &d, &v, 2, false, false, false).unwrap_err();
        assert!(format!("{err:?}").contains("values not fully consumed"), "got {err:?}");
    }
}

/// SP145: assemble `List<struct<F1, F2, ...>>` records.
///
/// Strategy: every struct field column shares the SAME REPEATED outer
/// LIST ancestor, so all field columns have IDENTICAL rep streams at
/// max_rep_level = 1 (one REPEATED ancestor). The def streams differ
/// only in whether each field is OPT vs REQ — but the LIST-level
/// boundaries (outer-null, empty-list, item-present) are determined by
/// a SHARED authoritative leaf's (rep, def) stream.
///
/// Inputs:
///   - `rep_levels`, `def_levels`: the shared LIST-level rep + def
///     stream (taken from the FIRST field's column — any field's
///     stream would be equivalent at the LIST boundary; the per-field
///     differences come from inner OPTs at the leaf level, which we
///     don't surface here since all SP145 V1 struct fields are REQUIRED).
///   - `field_names`: struct field names in declared order.
///   - `field_values`: per-field flat value vec, ALREADY produced by
///     the upstream leaf decode. Length of each vec = count of
///     item-present slots in the shared rep/def stream (= count of
///     def == max_def_level entries).
///   - `max_def_level`: the LIST-level max def
///     (outer_optional + 1 /*REP*/ + 0 /*struct is REQ inside LIST*/).
///   - `outer_optional`: is the outer LIST group OPTIONAL?
///
/// Output: one `PqValue::List(Vec<PqValue::Struct>)` per top-level
/// record. Outer-null records emit `PqValue::Null`.
pub fn assemble_list_of_struct(
    rep_levels: &[u32],
    def_levels: &[u32],
    field_names: &[String],
    field_values: &[Vec<PqValue>],
    max_def_level: u32,
    outer_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    if rep_levels.len() != def_levels.len() {
        return Err(PqError::Bad(format!(
            "rep/def length mismatch: rep={} def={}",
            rep_levels.len(), def_levels.len()
        )));
    }
    if field_names.len() != field_values.len() {
        return Err(PqError::Bad(format!(
            "list_of_struct: field_names len {} != field_values len {}",
            field_names.len(), field_values.len()
        )));
    }
    if field_names.is_empty() {
        return Err(PqError::Bad("list_of_struct: no fields".into()));
    }
    // All field columns must have the same length (one entry per
    // item-present slot).
    let item_count = field_values[0].len();
    for (i, col) in field_values.iter().enumerate() {
        if col.len() != item_count {
            return Err(PqError::Bad(format!(
                "list_of_struct: field '{}' length {} != first-field length {}",
                field_names[i], col.len(), item_count
            )));
        }
    }

    // Use the same SP143 classify primitive (single-level LIST classify).
    // The struct value sits at the item slot; max_def is the LIST-level
    // max (struct itself is REQ inside the LIST middle).
    let n = rep_levels.len();
    if n == 0 {
        if item_count != 0 {
            return Err(PqError::Bad(format!(
                "list_of_struct: no levels but {item_count} struct items supplied"
            )));
        }
        return Ok(Vec::new());
    }

    let mut out: Vec<PqValue> = Vec::new();
    let mut current_list: Option<Vec<PqValue>> = None;
    let mut current_is_null: bool = false;
    let mut item_cursor = 0usize;

    let make_struct = |idx: usize| -> Result<PqValue, PqError> {
        let pairs: Vec<(String, PqValue)> = field_names
            .iter()
            .enumerate()
            .map(|(fi, name)| (name.clone(), field_values[fi][idx].clone()))
            .collect();
        Ok(PqValue::Struct(pairs))
    };

    for i in 0..n {
        let rep = rep_levels[i];
        let def = def_levels[i];
        if rep > 1 {
            return Err(PqError::Bad(format!(
                "rep level {rep} > max 1 for list_of_struct (position {i})"
            )));
        }
        let def_case = classify(def, max_def_level, outer_optional, false, i)?;
        if rep == 0 {
            if let Some(list) = current_list.take() {
                out.push(PqValue::List(list));
            } else if i > 0 && current_is_null {
                out.push(PqValue::Null);
            }
            current_is_null = false;

            match def_case {
                DefCase::OuterNull => {
                    current_list = None;
                    current_is_null = true;
                }
                DefCase::EmptyList => {
                    current_list = Some(Vec::new());
                }
                DefCase::ItemPresent => {
                    if item_cursor >= item_count {
                        return Err(PqError::Bad(format!(
                            "list_of_struct: item cursor exhausted at position {i}"
                        )));
                    }
                    let s = make_struct(item_cursor)?;
                    item_cursor += 1;
                    current_list = Some(vec![s]);
                }
                DefCase::ItemNull => {
                    // SP145 V1: structs inside lists are REQUIRED, so ItemNull
                    // shouldn't fire (we pass element_optional=false). If
                    // classify returned it anyway, treat it as a malformed
                    // input.
                    return Err(PqError::Bad(format!(
                        "list_of_struct: unexpected ItemNull def case (position {i})"
                    )));
                }
            }
        } else {
            let list = current_list.as_mut().ok_or_else(||
                PqError::Bad(format!("rep=1 without active list (position {i})")))?;
            match def_case {
                DefCase::ItemPresent => {
                    if item_cursor >= item_count {
                        return Err(PqError::Bad(format!(
                            "list_of_struct: item cursor exhausted at position {i}"
                        )));
                    }
                    let s = make_struct(item_cursor)?;
                    item_cursor += 1;
                    list.push(s);
                }
                _ => {
                    return Err(PqError::Bad(format!(
                        "list_of_struct: rep=1 with non-item def case (position {i})"
                    )));
                }
            }
        }
    }

    if let Some(list) = current_list.take() {
        out.push(PqValue::List(list));
    } else if current_is_null {
        out.push(PqValue::Null);
    }

    if item_cursor != item_count {
        return Err(PqError::Bad(format!(
            "list_of_struct: items not fully consumed: cursor={item_cursor} len={item_count}"
        )));
    }

    Ok(out)
}

#[cfg(test)]
mod los_tests {
    use super::*;

    #[test]
    fn req_list_of_struct_two_items() {
        // REQ outer, REQ struct, REQ fields: max_def=1, max_rep=1.
        // Record: [{id:1, name:"a"}, {id:2, name:"b"}]
        let r = vec![0u32, 1];
        let d = vec![1u32, 1];
        let names = vec!["id".to_string(), "name".to_string()];
        let cols = vec![
            vec![PqValue::I64(1), PqValue::I64(2)],
            vec![PqValue::Bytes(b"a".to_vec()), PqValue::Bytes(b"b".to_vec())],
        ];
        let out = assemble_list_of_struct(&r, &d, &names, &cols, 1, false).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![
            PqValue::Struct(vec![
                ("id".into(), PqValue::I64(1)),
                ("name".into(), PqValue::Bytes(b"a".to_vec())),
            ]),
            PqValue::Struct(vec![
                ("id".into(), PqValue::I64(2)),
                ("name".into(), PqValue::Bytes(b"b".to_vec())),
            ]),
        ])]);
    }

    #[test]
    fn opt_list_of_struct_null_record() {
        // OPT outer, REQ struct: max_def=2.
        // [null, [{id:99, name:"z"}]]
        let r = vec![0u32, 0];
        let d = vec![0u32, 2];
        let names = vec!["id".to_string(), "name".to_string()];
        let cols = vec![
            vec![PqValue::I64(99)],
            vec![PqValue::Bytes(b"z".to_vec())],
        ];
        let out = assemble_list_of_struct(&r, &d, &names, &cols, 2, true).unwrap();
        assert_eq!(out, vec![
            PqValue::Null,
            PqValue::List(vec![PqValue::Struct(vec![
                ("id".into(), PqValue::I64(99)),
                ("name".into(), PqValue::Bytes(b"z".to_vec())),
            ])]),
        ]);
    }

    #[test]
    fn rejects_field_length_mismatch() {
        let r = vec![0u32, 1];
        let d = vec![1u32, 1];
        let names = vec!["id".to_string(), "name".to_string()];
        let cols = vec![
            vec![PqValue::I64(1), PqValue::I64(2)],
            vec![PqValue::Bytes(b"a".to_vec())], // length 1 vs 2
        ];
        let err = assemble_list_of_struct(&r, &d, &names, &cols, 1, false).unwrap_err();
        assert!(format!("{err:?}").contains("first-field length"), "got {err:?}");
    }

    #[test]
    fn rejects_item_cursor_overflow() {
        // 2 item-present slots in levels but only 1 value supplied per field.
        let r = vec![0u32, 1];
        let d = vec![1u32, 1];
        let names = vec!["id".to_string()];
        let cols = vec![vec![PqValue::I64(1)]]; // only 1
        let err = assemble_list_of_struct(&r, &d, &names, &cols, 1, false).unwrap_err();
        assert!(format!("{err:?}").contains("item cursor exhausted")
                || format!("{err:?}").contains("first-field length")
                || format!("{err:?}").contains("not fully consumed"),
                "got {err:?}");
    }
}

/// SP145: assemble `Map<K, struct<F1, F2, ...>>` records.
///
/// Strategy: identical to SP144's `assemble_map_kv` except the V slot
/// is a struct built by zipping N value-field columns. The shared
/// REPEATED middle group ancestor means all V field columns + K share
/// the same rep stream at max_rep_level=1.
///
/// Inputs:
///   - `rep_levels`, `def_levels`: from the AUTHORITATIVE V-side stream
///     (any V-field column's leaf stream works — they're identical at
///     this layer since each V field is REQ inside the struct).
///   - `keys`: K values, one per "key_value middle present" slot.
///   - `value_field_names`, `value_field_columns`: per-V-field values,
///     one per "key_value middle present" slot (each column same length).
///   - `max_def_level`: V's max-def (outer_opt + 1 /*REP middle*/ + 0
///     /*struct V is REQ — V's struct itself doesn't add a level*/).
///   - `outer_optional`: is the outer MAP group OPTIONAL?
///
/// Output: one `PqValue::Map(Vec<(K, Struct)>)` per top-level record.
/// Outer-null records emit `PqValue::Null`.
pub fn assemble_map_of_struct(
    rep_levels: &[u32],
    def_levels: &[u32],
    keys: &[PqValue],
    value_field_names: &[String],
    value_field_columns: &[Vec<PqValue>],
    max_def_level: u32,
    outer_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    if rep_levels.len() != def_levels.len() {
        return Err(PqError::Bad(format!(
            "rep/def length mismatch: rep={} def={}",
            rep_levels.len(), def_levels.len()
        )));
    }
    if value_field_names.len() != value_field_columns.len() {
        return Err(PqError::Bad(format!(
            "map_of_struct: value_field_names len {} != value_field_columns len {}",
            value_field_names.len(), value_field_columns.len()
        )));
    }
    if value_field_names.is_empty() {
        return Err(PqError::Bad("map_of_struct: no value fields".into()));
    }
    let item_count = value_field_columns[0].len();
    for (i, col) in value_field_columns.iter().enumerate() {
        if col.len() != item_count {
            return Err(PqError::Bad(format!(
                "map_of_struct: value field '{}' length {} != first {}",
                value_field_names[i], col.len(), item_count
            )));
        }
    }
    if keys.len() != item_count {
        return Err(PqError::Bad(format!(
            "map_of_struct: keys len {} != value-field length {}",
            keys.len(), item_count
        )));
    }

    let n = rep_levels.len();
    if n == 0 {
        if item_count != 0 {
            return Err(PqError::Bad(format!(
                "map_of_struct: no levels but {item_count} items supplied"
            )));
        }
        return Ok(Vec::new());
    }

    let empty_threshold: u32 = outer_optional as u32;

    #[derive(Copy, Clone, Debug)]
    enum MosCase {
        OuterNull,
        EmptyMap,
        ItemPresent,
    }

    let classify = |def: u32, pos: usize| -> Result<MosCase, PqError> {
        if def > max_def_level {
            return Err(PqError::Bad(format!(
                "def level {def} > max {max_def_level} (position {pos})"
            )));
        }
        if def == max_def_level {
            return Ok(MosCase::ItemPresent);
        }
        if outer_optional && def == 0 {
            return Ok(MosCase::OuterNull);
        }
        if def == empty_threshold {
            return Ok(MosCase::EmptyMap);
        }
        Err(PqError::Bad(format!(
            "map_of_struct: unclassified def {def} (max={max_def_level}, \
             outer_opt={outer_optional}) at position {pos}"
        )))
    };

    let make_struct = |idx: usize| -> PqValue {
        let pairs: Vec<(String, PqValue)> = value_field_names
            .iter()
            .enumerate()
            .map(|(fi, name)| (name.clone(), value_field_columns[fi][idx].clone()))
            .collect();
        PqValue::Struct(pairs)
    };

    let mut out: Vec<PqValue> = Vec::new();
    let mut current_map: Option<Vec<(PqValue, PqValue)>> = None;
    let mut current_is_null: bool = false;
    let mut item_cursor = 0usize;

    for i in 0..n {
        let rep = rep_levels[i];
        let def = def_levels[i];
        if rep > 1 {
            return Err(PqError::Bad(format!(
                "rep level {rep} > max 1 for map_of_struct (position {i})"
            )));
        }
        let dc = classify(def, i)?;
        if rep == 0 {
            if let Some(map) = current_map.take() {
                out.push(PqValue::Map(map));
            } else if i > 0 && current_is_null {
                out.push(PqValue::Null);
            }
            current_is_null = false;

            match dc {
                MosCase::OuterNull => {
                    current_map = None;
                    current_is_null = true;
                }
                MosCase::EmptyMap => {
                    current_map = Some(Vec::new());
                }
                MosCase::ItemPresent => {
                    if item_cursor >= item_count {
                        return Err(PqError::Bad(format!(
                            "map_of_struct: item cursor exhausted at position {i}"
                        )));
                    }
                    let k = keys[item_cursor].clone();
                    let v = make_struct(item_cursor);
                    item_cursor += 1;
                    current_map = Some(vec![(k, v)]);
                }
            }
        } else {
            let map = current_map.as_mut().ok_or_else(||
                PqError::Bad(format!("rep=1 without active map (position {i})")))?;
            match dc {
                MosCase::ItemPresent => {
                    if item_cursor >= item_count {
                        return Err(PqError::Bad(format!(
                            "map_of_struct: item cursor exhausted at position {i}"
                        )));
                    }
                    let k = keys[item_cursor].clone();
                    let v = make_struct(item_cursor);
                    item_cursor += 1;
                    map.push((k, v));
                }
                _ => {
                    return Err(PqError::Bad(format!(
                        "map_of_struct: rep=1 with non-item def case (position {i})"
                    )));
                }
            }
        }
    }

    if let Some(map) = current_map.take() {
        out.push(PqValue::Map(map));
    } else if current_is_null {
        out.push(PqValue::Null);
    }

    if item_cursor != item_count {
        return Err(PqError::Bad(format!(
            "map_of_struct: items not fully consumed: cursor={item_cursor} len={item_count}"
        )));
    }

    Ok(out)
}

#[cfg(test)]
mod mos_tests {
    use super::*;

    #[test]
    fn req_map_of_struct_two_entries() {
        // REQ outer, REQ struct V: max_def=1, max_rep=1.
        let r = vec![0u32, 1];
        let d = vec![1u32, 1];
        let keys = vec![
            PqValue::Bytes(b"a".to_vec()),
            PqValue::Bytes(b"b".to_vec()),
        ];
        let vnames = vec!["count".to_string(), "ratio".to_string()];
        let vcols = vec![
            vec![PqValue::I64(1), PqValue::I64(2)],
            vec![PqValue::F64(0.5), PqValue::F64(1.5)],
        ];
        let out = assemble_map_of_struct(&r, &d, &keys, &vnames, &vcols, 1, false).unwrap();
        assert_eq!(out, vec![PqValue::Map(vec![
            (PqValue::Bytes(b"a".to_vec()), PqValue::Struct(vec![
                ("count".into(), PqValue::I64(1)),
                ("ratio".into(), PqValue::F64(0.5)),
            ])),
            (PqValue::Bytes(b"b".to_vec()), PqValue::Struct(vec![
                ("count".into(), PqValue::I64(2)),
                ("ratio".into(), PqValue::F64(1.5)),
            ])),
        ])]);
    }

    #[test]
    fn opt_map_of_struct_with_null() {
        // OPT outer, REQ struct V: max_def=2.
        // [null, {"k"->{a:9,b:0.1}}]
        let r = vec![0u32, 0];
        let d = vec![0u32, 2];
        let keys = vec![PqValue::Bytes(b"k".to_vec())];
        let vnames = vec!["a".to_string(), "b".to_string()];
        let vcols = vec![
            vec![PqValue::I64(9)],
            vec![PqValue::F64(0.1)],
        ];
        let out = assemble_map_of_struct(&r, &d, &keys, &vnames, &vcols, 2, true).unwrap();
        assert_eq!(out, vec![
            PqValue::Null,
            PqValue::Map(vec![(
                PqValue::Bytes(b"k".to_vec()),
                PqValue::Struct(vec![
                    ("a".into(), PqValue::I64(9)),
                    ("b".into(), PqValue::F64(0.1)),
                ]),
            )]),
        ]);
    }

    #[test]
    fn rejects_keys_count_mismatch() {
        let r = vec![0u32, 1];
        let d = vec![1u32, 1];
        let keys = vec![PqValue::Bytes(b"a".to_vec())]; // 1 key
        let vnames = vec!["x".to_string()];
        let vcols = vec![vec![PqValue::I64(1), PqValue::I64(2)]]; // 2 values
        let err = assemble_map_of_struct(&r, &d, &keys, &vnames, &vcols, 1, false).unwrap_err();
        assert!(format!("{err:?}").contains("keys len"), "got {err:?}");
    }

    #[test]
    fn rejects_value_field_length_mismatch() {
        let r = vec![0u32, 1];
        let d = vec![1u32, 1];
        let keys = vec![PqValue::Bytes(b"a".to_vec()), PqValue::Bytes(b"b".to_vec())];
        let vnames = vec!["x".to_string(), "y".to_string()];
        let vcols = vec![
            vec![PqValue::I64(1), PqValue::I64(2)],
            vec![PqValue::I64(10)], // length 1 vs 2
        ];
        let err = assemble_map_of_struct(&r, &d, &keys, &vnames, &vcols, 1, false).unwrap_err();
        assert!(format!("{err:?}").contains("value field 'y'"), "got {err:?}");
    }
}

/// SP145: assemble `Map<K, List<T>>` records.
///
/// Strategy: same as `assemble_map_of_struct` but each V slot is a
/// list of items rather than a struct. The V-side leaf has TWO
/// REPEATED ancestors (the MAP middle + the LIST middle), so
/// `max_rep_level = 2` on the V leaf stream. The K leaf still has
/// `max_rep_level = 1` (only the MAP middle is REPEATED for K).
///
/// We DRIVE the assembly off the V leaf's stream (it carries all the
/// MAP + LIST boundary information). K's values are consumed in
/// parallel — one K per "key_value middle present + LIST opens" event.
///
/// Inputs:
///   - `v_rep_levels`, `v_def_levels`: V leaf's full rep+def stream,
///     max_rep_level=2.
///   - `keys`: K values, one per key_value middle present slot.
///   - `v_values`: V leaf values, one per leaf-present slot
///     (def == max_def_level).
///   - `max_def_level`: V leaf's max-def
///     (outer_optional + 1 /*MAP REP*/ + value_list_outer_optional? no — value group is REQ
///      + 1 /*LIST inner REP*/ + value_item_optional as u32).
///   - `outer_optional`: is the outer MAP group OPTIONAL?
///   - `value_item_optional`: is the inner LIST element OPTIONAL?
///
/// V's REP ancestors:
///   - MAP middle (always REPEATED, +1 rep)
///   - LIST middle (always REPEATED, +1 rep)
///   So max_rep_level = 2.
///
/// Def classification (for max_def = outer_opt + 1 + 1 + item_opt = e.g. 2 or 3):
///   d == 0 && outer_opt          → outer MAP null
///   d == outer_opt               → empty MAP (no key_value entries)
///   d == outer_opt + 1           → key_value present but V-LIST is empty
///   d == max_def - (1 if item_opt else 0) — only for item_opt → V item null
///   d == max_def                 → V item present
///
/// Rep handling:
///   rep == 0 → new outer record
///   rep == 1 → new MAP entry (new K + start V LIST)
///   rep == 2 → continue current V LIST (append item)
pub fn assemble_map_of_list(
    v_rep_levels: &[u32],
    v_def_levels: &[u32],
    keys: &[PqValue],
    v_values: &[PqValue],
    max_def_level: u32,
    outer_optional: bool,
    value_item_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    if v_rep_levels.len() != v_def_levels.len() {
        return Err(PqError::Bad(format!(
            "rep/def length mismatch: rep={} def={}",
            v_rep_levels.len(), v_def_levels.len()
        )));
    }
    let n = v_rep_levels.len();
    if n == 0 {
        if !keys.is_empty() || !v_values.is_empty() {
            return Err(PqError::Bad(format!(
                "map_of_list: no levels but {} keys + {} values",
                keys.len(), v_values.len()
            )));
        }
        return Ok(Vec::new());
    }

    let empty_map_threshold: u32 = outer_optional as u32;
    let empty_list_threshold: u32 = (outer_optional as u32) + 1;
    let item_null_threshold: u32 = if max_def_level > 0 { max_def_level - 1 } else { 0 };

    #[derive(Copy, Clone, Debug)]
    enum MolCase {
        OuterNull,
        EmptyMap,
        EmptyValueList,
        ItemNull,
        ItemPresent,
    }

    let classify = |def: u32, pos: usize| -> Result<MolCase, PqError> {
        if def > max_def_level {
            return Err(PqError::Bad(format!(
                "def level {def} > max {max_def_level} (position {pos})"
            )));
        }
        if def == max_def_level {
            return Ok(MolCase::ItemPresent);
        }
        if outer_optional && def == 0 {
            return Ok(MolCase::OuterNull);
        }
        if def == empty_map_threshold {
            return Ok(MolCase::EmptyMap);
        }
        if def == empty_list_threshold {
            return Ok(MolCase::EmptyValueList);
        }
        if value_item_optional && def == item_null_threshold {
            return Ok(MolCase::ItemNull);
        }
        Err(PqError::Bad(format!(
            "map_of_list: unclassified def {def} (max={max_def_level}, \
             outer_opt={outer_optional}, item_opt={value_item_optional}) \
             at position {pos}"
        )))
    };

    let mut out: Vec<PqValue> = Vec::new();
    let mut current_map: Option<Vec<(PqValue, PqValue)>> = None;
    let mut current_is_null: bool = false;
    let mut current_v_list: Option<Vec<PqValue>> = None;
    let mut current_k: Option<PqValue> = None;
    let mut k_cursor = 0usize;
    let mut v_cursor = 0usize;

    let flush_kv = |map: &mut Option<Vec<(PqValue, PqValue)>>,
                    k: &mut Option<PqValue>,
                    v_list: &mut Option<Vec<PqValue>>|
     -> Result<(), PqError> {
        if let Some(vl) = v_list.take() {
            let key = k.take().ok_or_else(||
                PqError::Bad("flush_kv: V list with no K".into()))?;
            map.as_mut()
                .ok_or_else(|| PqError::Bad("flush_kv: no active map".into()))?
                .push((key, PqValue::List(vl)));
        }
        Ok(())
    };

    for i in 0..n {
        let rep = v_rep_levels[i];
        let def = v_def_levels[i];
        if rep > 2 {
            return Err(PqError::Bad(format!(
                "rep level {rep} > max 2 for map_of_list (position {i})"
            )));
        }
        let dc = classify(def, i)?;

        if rep == 0 {
            // Flush previous V-list into map, then flush map into out.
            flush_kv(&mut current_map, &mut current_k, &mut current_v_list)?;
            if let Some(m) = current_map.take() {
                out.push(PqValue::Map(m));
            } else if i > 0 && current_is_null {
                out.push(PqValue::Null);
            }
            current_is_null = false;

            match dc {
                MolCase::OuterNull => {
                    current_map = None;
                    current_is_null = true;
                    current_v_list = None;
                    current_k = None;
                }
                MolCase::EmptyMap => {
                    current_map = Some(Vec::new());
                    current_v_list = None;
                    current_k = None;
                }
                MolCase::EmptyValueList => {
                    current_map = Some(Vec::new());
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("map_of_list: key exhausted at {i}")))?;
                    k_cursor += 1;
                    current_k = Some(k);
                    current_v_list = Some(Vec::new());
                }
                MolCase::ItemNull => {
                    current_map = Some(Vec::new());
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("map_of_list: key exhausted at {i}")))?;
                    k_cursor += 1;
                    current_k = Some(k);
                    current_v_list = Some(vec![PqValue::Null]);
                }
                MolCase::ItemPresent => {
                    current_map = Some(Vec::new());
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("map_of_list: key exhausted at {i}")))?;
                    k_cursor += 1;
                    let v = v_values.get(v_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("map_of_list: value exhausted at {i}")))?;
                    v_cursor += 1;
                    current_k = Some(k);
                    current_v_list = Some(vec![v]);
                }
            }
        } else if rep == 1 {
            // New MAP entry (new key_value middle), start fresh V-list.
            if current_map.is_none() {
                return Err(PqError::Bad(format!(
                    "rep=1 without active map (position {i})"
                )));
            }
            flush_kv(&mut current_map, &mut current_k, &mut current_v_list)?;
            match dc {
                MolCase::EmptyValueList => {
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("map_of_list: key exhausted at {i}")))?;
                    k_cursor += 1;
                    current_k = Some(k);
                    current_v_list = Some(Vec::new());
                }
                MolCase::ItemNull => {
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("map_of_list: key exhausted at {i}")))?;
                    k_cursor += 1;
                    current_k = Some(k);
                    current_v_list = Some(vec![PqValue::Null]);
                }
                MolCase::ItemPresent => {
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("map_of_list: key exhausted at {i}")))?;
                    k_cursor += 1;
                    let v = v_values.get(v_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("map_of_list: value exhausted at {i}")))?;
                    v_cursor += 1;
                    current_k = Some(k);
                    current_v_list = Some(vec![v]);
                }
                _ => {
                    return Err(PqError::Bad(format!(
                        "rep=1 with non-entry def (position {i})"
                    )));
                }
            }
        } else {
            // rep == 2: continue current V-LIST.
            let vl = current_v_list.as_mut().ok_or_else(||
                PqError::Bad(format!("rep=2 without active V-list (position {i})")))?;
            match dc {
                MolCase::ItemPresent => {
                    let v = v_values.get(v_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("map_of_list: value exhausted at {i}")))?;
                    v_cursor += 1;
                    vl.push(v);
                }
                MolCase::ItemNull => {
                    vl.push(PqValue::Null);
                }
                _ => {
                    return Err(PqError::Bad(format!(
                        "rep=2 with non-item def (position {i})"
                    )));
                }
            }
        }
    }

    // Flush trailing record.
    flush_kv(&mut current_map, &mut current_k, &mut current_v_list)?;
    if let Some(m) = current_map.take() {
        out.push(PqValue::Map(m));
    } else if current_is_null {
        out.push(PqValue::Null);
    }

    if k_cursor != keys.len() {
        return Err(PqError::Bad(format!(
            "map_of_list: keys not fully consumed: cursor={k_cursor} len={}", keys.len()
        )));
    }
    if v_cursor != v_values.len() {
        return Err(PqError::Bad(format!(
            "map_of_list: values not fully consumed: cursor={v_cursor} len={}", v_values.len()
        )));
    }

    Ok(out)
}

#[cfg(test)]
mod mol_tests {
    use super::*;

    #[test]
    fn req_map_string_list_string_one_record() {
        // REQ outer, REQ V-list, REQ item: max_def=2 (REP+REP), max_rep=2.
        // Record: {"langs": ["rust", "go"]}
        // Levels (single entry, 2 items in V-list):
        //   (rep=0, def=2, k="langs", v="rust")
        //   (rep=2, def=2, _,        v="go")
        let r = vec![0u32, 2];
        let d = vec![2u32, 2];
        let keys = vec![PqValue::Bytes(b"langs".to_vec())];
        let v_vals = vec![
            PqValue::Bytes(b"rust".to_vec()),
            PqValue::Bytes(b"go".to_vec()),
        ];
        let out = assemble_map_of_list(&r, &d, &keys, &v_vals, 2, false, false).unwrap();
        assert_eq!(out, vec![PqValue::Map(vec![
            (PqValue::Bytes(b"langs".to_vec()),
             PqValue::List(vec![
                 PqValue::Bytes(b"rust".to_vec()),
                 PqValue::Bytes(b"go".to_vec()),
             ])),
        ])]);
    }

    #[test]
    fn req_map_two_entries_each_one_item() {
        // {"a": ["x"], "b": ["y"]}: rep=[0,1], def=[2,2]
        let r = vec![0u32, 1];
        let d = vec![2u32, 2];
        let keys = vec![
            PqValue::Bytes(b"a".to_vec()),
            PqValue::Bytes(b"b".to_vec()),
        ];
        let v_vals = vec![
            PqValue::Bytes(b"x".to_vec()),
            PqValue::Bytes(b"y".to_vec()),
        ];
        let out = assemble_map_of_list(&r, &d, &keys, &v_vals, 2, false, false).unwrap();
        assert_eq!(out, vec![PqValue::Map(vec![
            (PqValue::Bytes(b"a".to_vec()), PqValue::List(vec![PqValue::Bytes(b"x".to_vec())])),
            (PqValue::Bytes(b"b".to_vec()), PqValue::List(vec![PqValue::Bytes(b"y".to_vec())])),
        ])]);
    }

    #[test]
    fn rejects_rep_overflow() {
        let r = vec![0u32, 3];
        let d = vec![2u32, 2];
        let keys = vec![PqValue::Bytes(b"a".to_vec())];
        let v_vals = vec![PqValue::Bytes(b"x".to_vec()), PqValue::Bytes(b"y".to_vec())];
        let err = assemble_map_of_list(&r, &d, &keys, &v_vals, 2, false, false).unwrap_err();
        assert!(format!("{err:?}").contains("rep level 3"), "got {err:?}");
    }
}

/// SP146: assemble `List<List<List<T>>>` records (3-deep LIST nesting,
/// max_rep_level=3). Generalization of `assemble_list_of_list_primitive`
/// one more level deep: there are now THREE nested accumulators
/// (outer / middle / inner) and the rep level ranges {0,1,2,3}.
///
/// Rep semantics:
///   rep == 0 → start NEW outer record; flush previous outer + middle + inner
///   rep == 1 → flush middle + inner; start NEW middle list within outer
///   rep == 2 → flush inner; start NEW inner list within middle
///   rep == 3 → continue innermost list (append item)
///
/// Def classification (max_def = outer_opt + 1 + middle_opt + 1 + inner_opt + 1 + item_opt):
///   d == 0 && outer_opt              → OuterNull
///   d == outer_opt                   → OuterEmpty
///   d == outer_opt + 1 + middle_opt  → MiddleNull (when middle_opt) or MiddleEmpty (when !middle_opt → degenerate, never EmptyMiddle below)
///   Actually the math is:
///     outer_empty_thr = outer_opt
///     middle_null_thr = outer_opt + 1
///     middle_empty_thr = outer_opt + 1 + middle_opt
///     inner_null_thr = outer_opt + 1 + middle_opt + 1
///     inner_empty_thr = outer_opt + 1 + middle_opt + 1 + inner_opt
///     item_null_thr  = max_def_level - 1   (only when item_opt)
///     item_present   = max_def_level
#[allow(clippy::too_many_arguments)]
pub fn assemble_list_of_list_of_list_primitive(
    rep_levels: &[u32],
    def_levels: &[u32],
    values: &[PqValue],
    max_def_level: u32,
    outer_optional: bool,
    middle_optional: bool,
    inner_optional: bool,
    item_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    if rep_levels.len() != def_levels.len() {
        return Err(PqError::Bad(format!(
            "rep/def length mismatch: rep={} def={}",
            rep_levels.len(), def_levels.len()
        )));
    }
    let n = rep_levels.len();
    if n == 0 {
        if !values.is_empty() {
            return Err(PqError::Bad(format!(
                "no levels but {} values supplied", values.len()
            )));
        }
        return Ok(Vec::new());
    }

    let outer_empty_thr: u32 = outer_optional as u32;
    let middle_null_thr: u32 = (outer_optional as u32) + 1;
    let middle_empty_thr: u32 = (outer_optional as u32) + 1 + (middle_optional as u32);
    let inner_null_thr: u32 = (outer_optional as u32) + 1 + (middle_optional as u32) + 1;
    let inner_empty_thr: u32 =
        (outer_optional as u32) + 1 + (middle_optional as u32) + 1 + (inner_optional as u32);
    let item_null_thr: u32 = if max_def_level > 0 { max_def_level - 1 } else { 0 };

    #[derive(Copy, Clone, Debug)]
    enum LolLCase {
        OuterNull,
        OuterEmpty,
        MiddleNull,
        MiddleEmpty,
        InnerNull,
        InnerEmpty,
        ItemNull,
        ItemPresent,
    }

    let classify = |def: u32, pos: usize| -> Result<LolLCase, PqError> {
        if def > max_def_level {
            return Err(PqError::Bad(format!(
                "def level {def} > max {max_def_level} (position {pos})"
            )));
        }
        // Order: ItemPresent first (priority — wins when max == any threshold).
        if def == max_def_level {
            return Ok(LolLCase::ItemPresent);
        }
        if outer_optional && def == 0 {
            return Ok(LolLCase::OuterNull);
        }
        if def == outer_empty_thr {
            return Ok(LolLCase::OuterEmpty);
        }
        if middle_optional && def == middle_null_thr {
            return Ok(LolLCase::MiddleNull);
        }
        if def == middle_empty_thr {
            return Ok(LolLCase::MiddleEmpty);
        }
        if inner_optional && def == inner_null_thr {
            return Ok(LolLCase::InnerNull);
        }
        if def == inner_empty_thr {
            return Ok(LolLCase::InnerEmpty);
        }
        if item_optional && def == item_null_thr {
            return Ok(LolLCase::ItemNull);
        }
        Err(PqError::Bad(format!(
            "unclassified def {def} (max={max_def_level}, \
             outer_opt={outer_optional}, middle_opt={middle_optional}, \
             inner_opt={inner_optional}, item_opt={item_optional}) at position {pos}"
        )))
    };

    let mut out: Vec<PqValue> = Vec::new();
    let mut current_outer: Option<Vec<PqValue>> = None;
    let mut current_outer_null: bool = false;
    let mut current_middle: Option<Vec<PqValue>> = None;
    let mut current_middle_null: bool = false;
    let mut current_inner: Option<Vec<PqValue>> = None;
    let mut current_inner_null: bool = false;
    let mut value_cursor = 0usize;

    let flush_inner = |middle: &mut Option<Vec<PqValue>>,
                       inner: &mut Option<Vec<PqValue>>,
                       inner_null: &mut bool|
     -> Result<(), PqError> {
        if let Some(items) = inner.take() {
            middle.as_mut()
                .ok_or_else(|| PqError::Bad("flush_inner with no active middle".into()))?
                .push(PqValue::List(items));
        } else if *inner_null {
            middle.as_mut()
                .ok_or_else(|| PqError::Bad("flush_inner null with no active middle".into()))?
                .push(PqValue::Null);
            *inner_null = false;
        }
        Ok(())
    };

    let flush_middle = |outer: &mut Option<Vec<PqValue>>,
                        middle: &mut Option<Vec<PqValue>>,
                        middle_null: &mut bool|
     -> Result<(), PqError> {
        if let Some(items) = middle.take() {
            outer.as_mut()
                .ok_or_else(|| PqError::Bad("flush_middle with no active outer".into()))?
                .push(PqValue::List(items));
        } else if *middle_null {
            outer.as_mut()
                .ok_or_else(|| PqError::Bad("flush_middle null with no active outer".into()))?
                .push(PqValue::Null);
            *middle_null = false;
        }
        Ok(())
    };

    let flush_outer = |out: &mut Vec<PqValue>,
                       outer: &mut Option<Vec<PqValue>>,
                       outer_null: &mut bool| {
        if let Some(items) = outer.take() {
            out.push(PqValue::List(items));
        } else if *outer_null {
            out.push(PqValue::Null);
            *outer_null = false;
        }
    };

    for i in 0..n {
        let rep = rep_levels[i];
        let def = def_levels[i];
        if rep > 3 {
            return Err(PqError::Bad(format!(
                "rep level {rep} > max 3 for List<List<List>> (position {i})"
            )));
        }
        let dc = classify(def, i)?;

        if rep == 0 {
            // Flush 3 levels of state in inner→middle→outer order.
            flush_inner(&mut current_middle, &mut current_inner, &mut current_inner_null)?;
            flush_middle(&mut current_outer, &mut current_middle, &mut current_middle_null)?;
            flush_outer(&mut out, &mut current_outer, &mut current_outer_null);

            match dc {
                LolLCase::OuterNull => {
                    current_outer = None;
                    current_outer_null = true;
                    current_middle = None;
                    current_middle_null = false;
                    current_inner = None;
                    current_inner_null = false;
                }
                LolLCase::OuterEmpty => {
                    current_outer = Some(Vec::new());
                    current_middle = None;
                    current_middle_null = false;
                    current_inner = None;
                    current_inner_null = false;
                }
                LolLCase::MiddleNull => {
                    current_outer = Some(Vec::new());
                    current_middle = None;
                    current_middle_null = true;
                    current_inner = None;
                    current_inner_null = false;
                }
                LolLCase::MiddleEmpty => {
                    current_outer = Some(Vec::new());
                    current_middle = Some(Vec::new());
                    current_inner = None;
                    current_inner_null = false;
                }
                LolLCase::InnerNull => {
                    current_outer = Some(Vec::new());
                    current_middle = Some(Vec::new());
                    current_inner = None;
                    current_inner_null = true;
                }
                LolLCase::InnerEmpty => {
                    current_outer = Some(Vec::new());
                    current_middle = Some(Vec::new());
                    current_inner = Some(Vec::new());
                    current_inner_null = false;
                }
                LolLCase::ItemNull => {
                    current_outer = Some(Vec::new());
                    current_middle = Some(Vec::new());
                    current_inner = Some(vec![PqValue::Null]);
                    current_inner_null = false;
                }
                LolLCase::ItemPresent => {
                    let v = values.get(value_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("value stream exhausted at position {i}")))?;
                    value_cursor += 1;
                    current_outer = Some(Vec::new());
                    current_middle = Some(Vec::new());
                    current_inner = Some(vec![v]);
                    current_inner_null = false;
                }
            }
        } else if rep == 1 {
            // New middle list within current outer. Flush inner → middle first.
            if current_outer.is_none() {
                return Err(PqError::Bad(format!(
                    "rep=1 without active outer list (position {i})"
                )));
            }
            flush_inner(&mut current_middle, &mut current_inner, &mut current_inner_null)?;
            flush_middle(&mut current_outer, &mut current_middle, &mut current_middle_null)?;

            match dc {
                LolLCase::OuterNull | LolLCase::OuterEmpty => {
                    return Err(PqError::Bad(format!(
                        "rep=1 with outer-level def {dc:?} (position {i})"
                    )));
                }
                LolLCase::MiddleNull => {
                    current_middle = None;
                    current_middle_null = true;
                    current_inner = None;
                    current_inner_null = false;
                }
                LolLCase::MiddleEmpty => {
                    current_middle = Some(Vec::new());
                    current_inner = None;
                    current_inner_null = false;
                }
                LolLCase::InnerNull => {
                    current_middle = Some(Vec::new());
                    current_inner = None;
                    current_inner_null = true;
                }
                LolLCase::InnerEmpty => {
                    current_middle = Some(Vec::new());
                    current_inner = Some(Vec::new());
                    current_inner_null = false;
                }
                LolLCase::ItemNull => {
                    current_middle = Some(Vec::new());
                    current_inner = Some(vec![PqValue::Null]);
                    current_inner_null = false;
                }
                LolLCase::ItemPresent => {
                    let v = values.get(value_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("value stream exhausted at position {i}")))?;
                    value_cursor += 1;
                    current_middle = Some(Vec::new());
                    current_inner = Some(vec![v]);
                    current_inner_null = false;
                }
            }
        } else if rep == 2 {
            // New inner list within current middle. Flush inner first.
            if current_middle.is_none() {
                return Err(PqError::Bad(format!(
                    "rep=2 without active middle list (position {i})"
                )));
            }
            flush_inner(&mut current_middle, &mut current_inner, &mut current_inner_null)?;

            match dc {
                LolLCase::InnerNull => {
                    current_inner = None;
                    current_inner_null = true;
                }
                LolLCase::InnerEmpty => {
                    current_inner = Some(Vec::new());
                    current_inner_null = false;
                }
                LolLCase::ItemNull => {
                    current_inner = Some(vec![PqValue::Null]);
                    current_inner_null = false;
                }
                LolLCase::ItemPresent => {
                    let v = values.get(value_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("value stream exhausted at position {i}")))?;
                    value_cursor += 1;
                    current_inner = Some(vec![v]);
                    current_inner_null = false;
                }
                _ => {
                    return Err(PqError::Bad(format!(
                        "rep=2 with non-inner def {dc:?} (position {i})"
                    )));
                }
            }
        } else {
            // rep == 3: continue innermost list.
            let inner = current_inner.as_mut().ok_or_else(||
                PqError::Bad(format!("rep=3 without active inner list (position {i})")))?;
            match dc {
                LolLCase::ItemNull => {
                    inner.push(PqValue::Null);
                }
                LolLCase::ItemPresent => {
                    let v = values.get(value_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("value stream exhausted at position {i}")))?;
                    value_cursor += 1;
                    inner.push(v);
                }
                _ => {
                    return Err(PqError::Bad(format!(
                        "rep=3 with non-item def {dc:?} (position {i})"
                    )));
                }
            }
        }
    }

    // Final flush in inner → middle → outer order.
    flush_inner(&mut current_middle, &mut current_inner, &mut current_inner_null)?;
    flush_middle(&mut current_outer, &mut current_middle, &mut current_middle_null)?;
    flush_outer(&mut out, &mut current_outer, &mut current_outer_null);

    if value_cursor != values.len() {
        return Err(PqError::Bad(format!(
            "values not fully consumed: cursor={value_cursor} len={}", values.len()
        )));
    }

    Ok(out)
}

#[cfg(test)]
mod loll_tests {
    use super::*;

    /// REQ-REP-REQ-REP-REQ-REP-REQ List<List<List<i64>>>
    /// Record: [[[1,2],[3]],[[4]]]
    /// outer_opt=false, middle_opt=false, inner_opt=false, item_opt=false
    /// max_def = 0 + 1 + 0 + 1 + 0 + 1 + 0 = 3
    /// max_rep = 3
    /// Levels for [[[1,2],[3]],[[4]]]:
    ///   item 1: rep=0, def=3  (new outer; new middle; new inner; item=1)
    ///   item 2: rep=3, def=3  (continue inner; item=2)
    ///   item 3: rep=2, def=3  (new inner within same middle; item=3)
    ///   item 4: rep=1, def=3  (new middle within same outer; new inner; item=4)
    #[test]
    fn req_req_req_req_one_outer_two_middle_three_inner_items() {
        let r = vec![0u32, 3, 2, 1];
        let d = vec![3u32, 3, 3, 3];
        let v = vec![PqValue::I64(1), PqValue::I64(2), PqValue::I64(3), PqValue::I64(4)];
        let out = assemble_list_of_list_of_list_primitive(
            &r, &d, &v, 3, false, false, false, false,
        ).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![
            PqValue::List(vec![
                PqValue::List(vec![PqValue::I64(1), PqValue::I64(2)]),
                PqValue::List(vec![PqValue::I64(3)]),
            ]),
            PqValue::List(vec![
                PqValue::List(vec![PqValue::I64(4)]),
            ]),
        ])]);
    }

    /// Two top-level records to exercise the rep=0 flush logic.
    /// R0: [[[10]]]  → r=0,d=3
    /// R1: [[[20],[30]]]  → r=0,d=3,v=20  +  r=2,d=3,v=30
    #[test]
    fn req_req_req_req_two_records() {
        let r = vec![0u32, 0, 2];
        let d = vec![3u32, 3, 3];
        let v = vec![PqValue::I64(10), PqValue::I64(20), PqValue::I64(30)];
        let out = assemble_list_of_list_of_list_primitive(
            &r, &d, &v, 3, false, false, false, false,
        ).unwrap();
        assert_eq!(out, vec![
            PqValue::List(vec![PqValue::List(vec![PqValue::List(vec![PqValue::I64(10)])])]),
            PqValue::List(vec![PqValue::List(vec![
                PqValue::List(vec![PqValue::I64(20)]),
                PqValue::List(vec![PqValue::I64(30)]),
            ])]),
        ]);
    }

    /// Empty innermost list. OPT-REP-OPT-REP-OPT-REP-OPT: every layer OPT.
    /// max_def = 1+1+1+1+1+1+1 = 7
    /// Empty INNER list def: outer_opt + 1 + middle_opt + 1 + inner_opt = 1+1+1+1+1 = 5
    /// Record: [[[]]]
    ///   r=0, d=5 → outer open, middle open, inner empty
    #[test]
    fn opt_opt_opt_opt_empty_inner_list() {
        let r = vec![0u32];
        let d = vec![5u32];
        let v: Vec<PqValue> = vec![];
        let out = assemble_list_of_list_of_list_primitive(
            &r, &d, &v, 7, true, true, true, true,
        ).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![
            PqValue::List(vec![PqValue::List(vec![])]),
        ])]);
    }

    /// Outer-null OPT outer: max_def varies, rep=0 d=0 → null record.
    #[test]
    fn opt_outer_null_record() {
        let r = vec![0u32];
        let d = vec![0u32];
        let v: Vec<PqValue> = vec![];
        // OPT-REQ-OPT-REQ-OPT-REQ-OPT: max_def = 1+1+0+1+0+1+0 = 4
        let out = assemble_list_of_list_of_list_primitive(
            &r, &d, &v, 4, true, false, false, false,
        ).unwrap();
        assert_eq!(out, vec![PqValue::Null]);
    }

    #[test]
    fn rejects_rep_overflow() {
        let r = vec![0u32, 5];
        let d = vec![3u32, 3];
        let v = vec![PqValue::I64(1), PqValue::I64(2)];
        let err = assemble_list_of_list_of_list_primitive(
            &r, &d, &v, 3, false, false, false, false,
        ).unwrap_err();
        assert!(format!("{err:?}").contains("rep level 5"), "got {err:?}");
    }

    #[test]
    fn rejects_value_underflow() {
        let r = vec![0u32, 3];
        let d = vec![3u32, 3];
        let v = vec![PqValue::I64(1)]; // need 2
        let err = assemble_list_of_list_of_list_primitive(
            &r, &d, &v, 3, false, false, false, false,
        ).unwrap_err();
        assert!(format!("{err:?}").contains("value stream exhausted"), "got {err:?}");
    }

    #[test]
    fn rejects_value_unconsumed() {
        let r = vec![0u32];
        let d = vec![3u32];
        let v = vec![PqValue::I64(1), PqValue::I64(2)];
        let err = assemble_list_of_list_of_list_primitive(
            &r, &d, &v, 3, false, false, false, false,
        ).unwrap_err();
        assert!(format!("{err:?}").contains("values not fully consumed"), "got {err:?}");
    }
}

/// SP146: assemble `List<Map<K, V>>` records (outer LIST of inner Maps).
///
/// V leaf's max_rep_level = 2 (outer LIST REP + MAP key_value REP).
/// K leaf shares the same REPEATED ancestors so its rep stream is
/// identical at max_rep=2. We DRIVE assembly off the V leaf's
/// (rep, def) stream and consume K values + V values in parallel.
///
/// Rep semantics:
///   rep == 0 → new outer record (flush previous outer list of inner maps)
///   rep == 1 → new item in outer list (= new inner Map; flush previous inner map)
///   rep == 2 → continue current inner Map (append a (K, V) pair)
///
/// Def classification (max_def = outer_opt + 1 + 1 + value_opt):
///   d == 0 && outer_opt              → OuterListNull
///   d == outer_opt                   → EmptyOuterList
///   d == outer_opt + 1               → EmptyInnerMap (outer-list item present but inner-map empty)
///   d == max_def - 1 (when value_opt) → ValueNull
///   d == max_def                     → ValuePresent
#[allow(clippy::too_many_arguments)]
pub fn assemble_list_of_map_kv(
    rep_levels: &[u32],
    def_levels: &[u32],
    keys: &[PqValue],
    values: &[PqValue],
    max_def_level: u32,
    outer_optional: bool,
    value_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    if rep_levels.len() != def_levels.len() {
        return Err(PqError::Bad(format!(
            "rep/def length mismatch: rep={} def={}",
            rep_levels.len(), def_levels.len()
        )));
    }
    let n = rep_levels.len();
    if n == 0 {
        if !keys.is_empty() || !values.is_empty() {
            return Err(PqError::Bad(format!(
                "list_of_map: no levels but {} keys + {} values",
                keys.len(), values.len()
            )));
        }
        return Ok(Vec::new());
    }

    let empty_outer_thr: u32 = outer_optional as u32;
    let empty_inner_thr: u32 = (outer_optional as u32) + 1;
    let value_null_thr: u32 = if max_def_level > 0 { max_def_level - 1 } else { 0 };

    #[derive(Copy, Clone, Debug)]
    enum LomCase {
        OuterListNull,
        EmptyOuterList,
        EmptyInnerMap,
        ValueNull,
        ValuePresent,
    }

    let classify = |def: u32, pos: usize| -> Result<LomCase, PqError> {
        if def > max_def_level {
            return Err(PqError::Bad(format!(
                "def level {def} > max {max_def_level} (position {pos})"
            )));
        }
        if def == max_def_level {
            return Ok(LomCase::ValuePresent);
        }
        if outer_optional && def == 0 {
            return Ok(LomCase::OuterListNull);
        }
        if def == empty_outer_thr {
            return Ok(LomCase::EmptyOuterList);
        }
        if def == empty_inner_thr {
            return Ok(LomCase::EmptyInnerMap);
        }
        if value_optional && def == value_null_thr {
            return Ok(LomCase::ValueNull);
        }
        Err(PqError::Bad(format!(
            "list_of_map: unclassified def {def} (max={max_def_level}, \
             outer_opt={outer_optional}, value_opt={value_optional}) at position {pos}"
        )))
    };

    let mut out: Vec<PqValue> = Vec::new();
    // Outer accumulator: Vec of inner-Map values (PqValue::Map or Null per slot).
    let mut current_outer: Option<Vec<PqValue>> = None;
    let mut current_outer_is_null: bool = false;
    // Inner accumulator: in-flight Map<K,V>.
    let mut current_inner: Option<Vec<(PqValue, PqValue)>> = None;
    let mut k_cursor = 0usize;
    let mut v_cursor = 0usize;

    let flush_inner = |outer: &mut Option<Vec<PqValue>>,
                       inner: &mut Option<Vec<(PqValue, PqValue)>>|
     -> Result<(), PqError> {
        if let Some(im) = inner.take() {
            outer.as_mut()
                .ok_or_else(|| PqError::Bad(
                    "list_of_map: flush_inner with no active outer".into()))?
                .push(PqValue::Map(im));
        }
        Ok(())
    };

    for i in 0..n {
        let rep = rep_levels[i];
        let def = def_levels[i];
        if rep > 2 {
            return Err(PqError::Bad(format!(
                "rep level {rep} > max 2 for list_of_map (position {i})"
            )));
        }
        let dc = classify(def, i)?;

        if rep == 0 {
            // Flush previous inner + outer.
            flush_inner(&mut current_outer, &mut current_inner)?;
            if let Some(o) = current_outer.take() {
                out.push(PqValue::List(o));
            } else if current_outer_is_null {
                out.push(PqValue::Null);
                current_outer_is_null = false;
            }

            match dc {
                LomCase::OuterListNull => {
                    current_outer = None;
                    current_outer_is_null = true;
                    current_inner = None;
                }
                LomCase::EmptyOuterList => {
                    current_outer = Some(Vec::new());
                    current_inner = None;
                }
                LomCase::EmptyInnerMap => {
                    current_outer = Some(Vec::new());
                    current_inner = Some(Vec::new());
                }
                LomCase::ValueNull => {
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("list_of_map: key exhausted at {i}")))?;
                    k_cursor += 1;
                    current_outer = Some(Vec::new());
                    current_inner = Some(vec![(k, PqValue::Null)]);
                }
                LomCase::ValuePresent => {
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("list_of_map: key exhausted at {i}")))?;
                    k_cursor += 1;
                    let v = values.get(v_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("list_of_map: value exhausted at {i}")))?;
                    v_cursor += 1;
                    current_outer = Some(Vec::new());
                    current_inner = Some(vec![(k, v)]);
                }
            }
        } else if rep == 1 {
            // New item in outer LIST (= new inner Map). Flush previous inner.
            if current_outer.is_none() {
                return Err(PqError::Bad(format!(
                    "rep=1 without active outer list (position {i})"
                )));
            }
            flush_inner(&mut current_outer, &mut current_inner)?;
            match dc {
                LomCase::EmptyInnerMap => {
                    current_inner = Some(Vec::new());
                }
                LomCase::ValueNull => {
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("list_of_map: key exhausted at {i}")))?;
                    k_cursor += 1;
                    current_inner = Some(vec![(k, PqValue::Null)]);
                }
                LomCase::ValuePresent => {
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("list_of_map: key exhausted at {i}")))?;
                    k_cursor += 1;
                    let v = values.get(v_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("list_of_map: value exhausted at {i}")))?;
                    v_cursor += 1;
                    current_inner = Some(vec![(k, v)]);
                }
                _ => {
                    return Err(PqError::Bad(format!(
                        "list_of_map: rep=1 with non-item def {dc:?} (position {i})"
                    )));
                }
            }
        } else {
            // rep == 2: continue current inner Map (append KV).
            let inner = current_inner.as_mut().ok_or_else(||
                PqError::Bad(format!("rep=2 without active inner map (position {i})")))?;
            match dc {
                LomCase::ValueNull => {
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("list_of_map: key exhausted at {i}")))?;
                    k_cursor += 1;
                    inner.push((k, PqValue::Null));
                }
                LomCase::ValuePresent => {
                    let k = keys.get(k_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("list_of_map: key exhausted at {i}")))?;
                    k_cursor += 1;
                    let v = values.get(v_cursor).cloned().ok_or_else(||
                        PqError::Bad(format!("list_of_map: value exhausted at {i}")))?;
                    v_cursor += 1;
                    inner.push((k, v));
                }
                _ => {
                    return Err(PqError::Bad(format!(
                        "list_of_map: rep=2 with non-item def {dc:?} (position {i})"
                    )));
                }
            }
        }
    }

    // Final flush.
    flush_inner(&mut current_outer, &mut current_inner)?;
    if let Some(o) = current_outer.take() {
        out.push(PqValue::List(o));
    } else if current_outer_is_null {
        out.push(PqValue::Null);
    }

    if k_cursor != keys.len() {
        return Err(PqError::Bad(format!(
            "list_of_map: keys not fully consumed: cursor={k_cursor} len={}", keys.len()
        )));
    }
    if v_cursor != values.len() {
        return Err(PqError::Bad(format!(
            "list_of_map: values not fully consumed: cursor={v_cursor} len={}", values.len()
        )));
    }

    Ok(out)
}

#[cfg(test)]
mod lom_tests {
    use super::*;

    /// REQ outer LIST, REQ inner Map<K, V>, REQ V.
    /// max_def = 0 + 1 + 1 + 0 = 2, max_rep = 2.
    /// Record: [{"a": 1, "b": 2}, {"c": 3}]
    /// Levels:
    ///   (rep=0, def=2, k="a", v=1)  → new outer; new inner; first pair
    ///   (rep=2, def=2, k="b", v=2)  → continue inner; append (b,2)
    ///   (rep=1, def=2, k="c", v=3)  → new inner within outer; pair (c,3)
    #[test]
    fn req_req_req_one_outer_two_inner_three_pairs() {
        let r = vec![0u32, 2, 1];
        let d = vec![2u32, 2, 2];
        let keys = vec![
            PqValue::Bytes(b"a".to_vec()),
            PqValue::Bytes(b"b".to_vec()),
            PqValue::Bytes(b"c".to_vec()),
        ];
        let vals = vec![PqValue::I64(1), PqValue::I64(2), PqValue::I64(3)];
        let out = assemble_list_of_map_kv(&r, &d, &keys, &vals, 2, false, false).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![
            PqValue::Map(vec![
                (PqValue::Bytes(b"a".to_vec()), PqValue::I64(1)),
                (PqValue::Bytes(b"b".to_vec()), PqValue::I64(2)),
            ]),
            PqValue::Map(vec![
                (PqValue::Bytes(b"c".to_vec()), PqValue::I64(3)),
            ]),
        ])]);
    }

    /// OPT outer LIST, REQ V. Record: [null] (one record, outer is null).
    /// max_def = 1+1+1+0 = 3. Outer null: rep=0, def=0.
    #[test]
    fn opt_outer_null_record() {
        let r = vec![0u32];
        let d = vec![0u32];
        let keys: Vec<PqValue> = vec![];
        let vals: Vec<PqValue> = vec![];
        let out = assemble_list_of_map_kv(&r, &d, &keys, &vals, 3, true, false).unwrap();
        assert_eq!(out, vec![PqValue::Null]);
    }

    /// REQ outer, empty outer list (no inner maps).
    /// max_def = 0+1+1+0 = 2. Empty outer: rep=0, def=0.
    #[test]
    fn empty_outer_list() {
        let r = vec![0u32];
        let d = vec![0u32];
        let keys: Vec<PqValue> = vec![];
        let vals: Vec<PqValue> = vec![];
        let out = assemble_list_of_map_kv(&r, &d, &keys, &vals, 2, false, false).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![])]);
    }

    /// REQ outer, REQ-empty inner map. Record: [{}]
    /// max_def = 2. Empty inner: rep=0, def=1.
    #[test]
    fn one_empty_inner_map_in_outer() {
        let r = vec![0u32];
        let d = vec![1u32];
        let keys: Vec<PqValue> = vec![];
        let vals: Vec<PqValue> = vec![];
        let out = assemble_list_of_map_kv(&r, &d, &keys, &vals, 2, false, false).unwrap();
        assert_eq!(out, vec![PqValue::List(vec![PqValue::Map(vec![])])]);
    }

    #[test]
    fn rejects_rep_overflow() {
        let r = vec![0u32, 5];
        let d = vec![2u32, 2];
        let keys = vec![PqValue::Bytes(b"a".to_vec())];
        let vals = vec![PqValue::I64(1), PqValue::I64(2)];
        let err = assemble_list_of_map_kv(&r, &d, &keys, &vals, 2, false, false).unwrap_err();
        assert!(format!("{err:?}").contains("rep level 5"), "got {err:?}");
    }

    #[test]
    fn rejects_value_underflow() {
        let r = vec![0u32, 2];
        let d = vec![2u32, 2];
        let keys = vec![PqValue::Bytes(b"a".to_vec()), PqValue::Bytes(b"b".to_vec())];
        let vals = vec![PqValue::I64(1)]; // need 2
        let err = assemble_list_of_map_kv(&r, &d, &keys, &vals, 2, false, false).unwrap_err();
        assert!(format!("{err:?}").contains("value exhausted"), "got {err:?}");
    }
}

/// SP146: assemble `Map<K1, Map<K2, V>>` records (outer Map whose V is
/// an inner Map with primitive K2 and V). T4 stub — full impl in next commit.
#[allow(clippy::too_many_arguments)]
pub fn assemble_map_of_map_kv(
    _inner_rep_levels: &[u32],
    _inner_def_levels: &[u32],
    _outer_keys: &[PqValue],
    _inner_keys: &[PqValue],
    _inner_values: &[PqValue],
    _max_def_level: u32,
    _outer_optional: bool,
    _inner_value_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    Err(PqError::Bad("assemble_map_of_map_kv: SP146 T4 not yet implemented".into()))
}
