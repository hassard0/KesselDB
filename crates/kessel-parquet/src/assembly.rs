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
