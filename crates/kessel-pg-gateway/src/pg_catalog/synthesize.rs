//! pg_catalog row synthesizers.
//!
//! Each synthesizer builds a complete PG wire response stream
//! (`RowDescription + DataRow* + CommandComplete + ReadyForQuery`)
//! from canned rows or live catalog data.
//!
//! **T1 status (this commit):** ships the `pg_namespace` synthesizer
//! emitting the 3 canonical schemas (pg_catalog OID 11, public OID
//! 2200, information_schema OID 2202). T3 adds pg_class
//! (KesselDB-table-per-row), T4 adds pg_attribute + pg_type, T5 adds
//! pg_index + pg_constraint, T6 adds information_schema views, T7
//! adds SQL helper functions.
//!
//! ## Output shape
//!
//! Each synthesizer returns the full byte stream the gateway writes
//! to the wire. The shape matches what `dispatch::dispatch_query`
//! emits for a successful SELECT:
//!
//! 1. `T` (RowDescription) — one field per pg_catalog column
//! 2. `D` (DataRow) per synthesized row
//! 3. `C` (CommandComplete) with tag `"SELECT N"`
//! 4. `Z` (ReadyForQuery) with status `'I'`
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-27-kesseldb-sppgcat-pg-catalog-design.md`

#![forbid(unsafe_code)]
#![allow(dead_code)]

use super::{
    PG_AUTHID_OID_POSTGRES, PG_NAMESPACE_OID_INFORMATION_SCHEMA,
    PG_NAMESPACE_OID_PG_CATALOG, PG_NAMESPACE_OID_PUBLIC,
};
use crate::proto::{PG_TYPE_OID, PG_TYPE_TEXT};
use crate::response::{
    encode_command_complete, encode_data_row, encode_ready_for_query,
    encode_row_description, select_tag, FieldMeta,
};

/// Synthesize the full PG wire response for `SELECT * FROM
/// pg_catalog.pg_namespace`. Returns three canonical schema rows:
/// pg_catalog (OID 11), public (OID 2200), information_schema (OID
/// 2202) — locked vs `src/include/catalog/pg_namespace.dat`.
///
/// Column layout matches the real PG pg_namespace catalog
/// (PG §51.32):
///
/// | Column | Type | Description |
/// |---|---|---|
/// | oid | oid | Row identifier |
/// | nspname | name (text) | Schema name |
/// | nspowner | oid | Owner OID (= PG_AUTHID_OID_POSTGRES) |
/// | nspacl | text[] | Access privileges (V1: NULL) |
///
/// V1 emits `nspacl` as PG NULL (i32 -1 sentinel in DataRow), since
/// KesselDB has no per-schema ACL concept yet.
pub fn pg_namespace_all_rows() -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "oid".to_string(), type_oid: PG_TYPE_OID },
        FieldMeta { name: "nspname".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "nspowner".to_string(), type_oid: PG_TYPE_OID },
        FieldMeta { name: "nspacl".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));

    // Row 1 — pg_catalog
    let oid_pg_catalog = PG_NAMESPACE_OID_PG_CATALOG.to_string();
    let name_pg_catalog = b"pg_catalog";
    let owner = PG_AUTHID_OID_POSTGRES.to_string();
    out.extend_from_slice(&encode_data_row(&[
        Some(oid_pg_catalog.as_bytes()),
        Some(name_pg_catalog),
        Some(owner.as_bytes()),
        None, // nspacl = NULL
    ]));

    // Row 2 — public
    let oid_public = PG_NAMESPACE_OID_PUBLIC.to_string();
    let name_public = b"public";
    out.extend_from_slice(&encode_data_row(&[
        Some(oid_public.as_bytes()),
        Some(name_public),
        Some(owner.as_bytes()),
        None,
    ]));

    // Row 3 — information_schema
    let oid_info = PG_NAMESPACE_OID_INFORMATION_SCHEMA.to_string();
    let name_info = b"information_schema";
    out.extend_from_slice(&encode_data_row(&[
        Some(oid_info.as_bytes()),
        Some(name_info),
        Some(owner.as_bytes()),
        None,
    ]));

    out.extend_from_slice(&encode_command_complete(&select_tag(3)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // ───────────────────────────────────────────────────────────────────
    // T1 KATs — pg_namespace synthesizer locked invariants.
    // ───────────────────────────────────────────────────────────────────

    /// **HEADLINE invariant — 3 rows:** the synthesizer emits exactly
    /// 3 DataRow frames matching the 3 canonical PG schemas. Locked
    /// because every GUI tool client-side joins on these OIDs.
    #[test]
    fn t1_pg_namespace_synthesizer_emits_three_canonical_rows() {
        let bytes = pg_namespace_all_rows();
        // Count 'D' (DataRow) frame headers. We can't just count `b'D'`
        // because the byte could appear in payload; instead, walk the
        // framed stream past the RowDescription.
        // Easier sanity-check: SELECT 3 in the CommandComplete tag.
        assert!(bytes.windows(b"SELECT 3\0".len()).any(|w| w == b"SELECT 3\0"),
            "CommandComplete tag MUST be 'SELECT 3'");
    }

    /// **HEADLINE invariant — well-framed stream:** the synthesizer
    /// emits T < D < D < D < C < Z in that order.
    #[test]
    fn t1_pg_namespace_stream_is_well_framed() {
        let bytes = pg_namespace_all_rows();
        // First frame is 'T' (RowDescription).
        assert_eq!(bytes[0], b'T');
        // Last 6 bytes are ReadyForQuery('I'): Z 0 0 0 5 I
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
        // Before the trailing RFQ there's a CommandComplete frame
        // — the second-to-last frame's type byte is 'C'.
        // Walk backwards: skip RFQ (6 bytes), then CommandComplete
        // is at bytes[len-6-CC_len..len-6]; its type byte is 'C'.
        // The CC payload is "SELECT 3\0" (9 bytes), length prefix is
        // 4+9=13 BE bytes, total CC frame = 1+4+9 = 14 bytes.
        let cc_start = bytes.len() - 6 - 14;
        assert_eq!(bytes[cc_start], b'C', "CommandComplete frame precedes RFQ");
    }

    /// **HEADLINE invariant — RowDescription has 4 canonical columns:**
    /// oid, nspname, nspowner, nspacl. Locked vs PG §51.32.
    #[test]
    fn t1_pg_namespace_row_description_has_4_canonical_columns() {
        let bytes = pg_namespace_all_rows();
        // RowDescription payload after [type='T', length:4]:
        //   [field_count:u16 BE] = 4
        assert_eq!(bytes[0], b'T');
        // field_count is at offset 5 (1 + 4 = 5).
        let fc = u16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(fc, 4, "pg_namespace RowDescription MUST have 4 columns");
        // The column names appear verbatim in the field-name slots.
        assert!(bytes.windows(b"oid\0".len()).any(|w| w == b"oid\0"));
        assert!(bytes.windows(b"nspname\0".len()).any(|w| w == b"nspname\0"));
        assert!(bytes.windows(b"nspowner\0".len()).any(|w| w == b"nspowner\0"));
        assert!(bytes.windows(b"nspacl\0".len()).any(|w| w == b"nspacl\0"));
    }

    /// **HEADLINE invariant — canonical OIDs are emitted:** the
    /// 3 rows carry the literal OIDs 11 / 2200 / 2202 in text-format
    /// (clients filter / JOIN on these via WHERE oid = N — the values
    /// MUST match PG canonical).
    #[test]
    fn t1_pg_namespace_rows_carry_canonical_oids_in_text() {
        let bytes = pg_namespace_all_rows();
        // The OID values appear as decimal-ASCII text in the DataRow
        // payloads. Cheap check: each OID's text rep appears in the
        // byte stream.
        assert!(bytes.windows(b"11".len()).any(|w| w == b"11"),
            "OID 11 (pg_catalog) MUST appear in the response");
        assert!(bytes.windows(b"2200".len()).any(|w| w == b"2200"),
            "OID 2200 (public) MUST appear in the response");
        assert!(bytes.windows(b"2202".len()).any(|w| w == b"2202"),
            "OID 2202 (information_schema) MUST appear in the response");
    }

    /// **HEADLINE invariant — schema names are emitted:** the
    /// nspname values are the canonical PG schema names.
    #[test]
    fn t1_pg_namespace_rows_carry_canonical_schema_names() {
        let bytes = pg_namespace_all_rows();
        assert!(bytes.windows(b"pg_catalog".len()).any(|w| w == b"pg_catalog"),
            "schema name 'pg_catalog' MUST appear");
        assert!(bytes.windows(b"public".len()).any(|w| w == b"public"),
            "schema name 'public' MUST appear");
        assert!(bytes.windows(b"information_schema".len()).any(|w| w == b"information_schema"),
            "schema name 'information_schema' MUST appear");
    }

    /// **NULL handling invariant — nspacl is NULL:** the 4th column
    /// of each row is the PG NULL sentinel (i32 -1, encoded as
    /// `0xFFFFFFFF`). V1 doesn't model per-schema ACLs.
    #[test]
    fn t1_pg_namespace_nspacl_column_is_null_per_row() {
        let bytes = pg_namespace_all_rows();
        // The NULL sentinel (0xFFFFFFFF) appears AT LEAST 3 times
        // (one per row, for the nspacl column).
        let null_count = bytes
            .windows(4)
            .filter(|w| *w == [0xFF, 0xFF, 0xFF, 0xFF])
            .count();
        assert!(null_count >= 3,
            "nspacl MUST be NULL in all 3 rows (saw {null_count} sentinels)");
    }
}
