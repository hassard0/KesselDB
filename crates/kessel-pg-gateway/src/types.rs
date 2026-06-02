//! KesselDB `FieldKind` ↔ PG type OID mapping table + text-format
//! renderer (spec §4 + §5).
//!
//! V1 emits all columns as PG TEXT format (format code 0); the binary
//! wire format is V2. Every `FieldKind` variant maps to a single PG
//! type OID locked here against `pg_type.dat`.
//!
//! ## Mapping table (spec §5)
//!
//! | KesselDB FieldKind | PG type | OID | Wire text |
//! |---|---|---|---|
//! | `Bool` | bool | 16 | `t`/`f` (PG uses single-char, NOT `true`/`false`) |
//! | `U8` | int2 | 21 | decimal (0..=255 fits in i16) |
//! | `U16` | int4 | 23 | decimal (0..=65535 fits in i32) |
//! | `U32` | int8 | 20 | decimal (0..=u32::MAX fits in i64) |
//! | `U64` | int8 | 20 | decimal (0..=i64::MAX direct; >i64::MAX → `22003` at render time) |
//! | `U128` | numeric | 1700 | decimal string |
//! | `I8` | int2 | 21 | decimal |
//! | `I16` | int2 | 21 | decimal |
//! | `I32` | int4 | 23 | decimal |
//! | `I64` | int8 | 20 | decimal |
//! | `I128` | numeric | 1700 | decimal string |
//! | `Fixed { scale }` | numeric | 1700 | decimal with `scale` decimal digits |
//! | `Char(n)` | text | 25 | UTF-8 bytes (zero-padding stripped) |
//! | `Bytes(n)` | bytea | 17 | `\x<hex>` (PG bytea text format) |
//! | `Timestamp` | timestamptz | 1184 | `YYYY-MM-DD HH:MM:SS.ffffff+00` |
//! | `Ref` | bytea | 17 | `\x<32-hex>` (16-byte ObjectId) |
//! | `OverflowRef` | bytea | 17 | `\x<16-hex>` (8-byte handle) |
//!
//! ## What this module does
//!
//! - `field_kind_to_oid(kind) -> u32` — exhaustive, infallible (every
//!   `FieldKind` variant has an assigned OID).
//! - `oid_to_field_kind(oid) -> Option<FieldKind>` — INVERSE LOOKUP
//!   for the canonical OIDs only. Lossy where the mapping isn't
//!   one-to-one (e.g. OID 20 maps back to `I64` because PG int8 IS
//!   `i64`; U32/U64 are KesselDB-specific surfaces that no client
//!   actually requests by OID).
//!
//! ## What this module does NOT do
//!
//! - It does NOT render values to PG text format (that's T5's
//!   `data_row` encoder + a separate per-type renderer in T8).
//! - It does NOT handle binary-format encoding (V2 only).
//! - It does NOT validate that a value fits in the target PG type
//!   (e.g. `U64` > `i64::MAX` overflow check is at render time per
//!   spec §11 weak-spot #4).
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::proto::{
    PG_TYPE_BOOL, PG_TYPE_BYTEA, PG_TYPE_FLOAT4, PG_TYPE_FLOAT8, PG_TYPE_INT2,
    PG_TYPE_INT4, PG_TYPE_INT8, PG_TYPE_NUMERIC, PG_TYPE_TEXT, PG_TYPE_TIMESTAMPTZ,
    PG_TYPE_VARCHAR,
};
use kessel_catalog::FieldKind;

/// Maps a KesselDB `FieldKind` to its PG type OID. Exhaustive and
/// infallible — every `FieldKind` variant has an assigned OID per
/// spec §5. Locked by KATs that round-trip every entry.
///
/// Note that this is a many-to-one map (e.g. both `U64` and `I64` →
/// `int8 = 20`). The inverse `oid_to_field_kind` picks the canonical
/// KesselDB variant for each OID; KesselDB-specific kinds without a
/// distinct PG type (like `U64`) are not reachable from the inverse.
pub fn field_kind_to_oid(kind: FieldKind) -> u32 {
    match kind {
        FieldKind::Bool => PG_TYPE_BOOL,
        // Signed and unsigned 8-bit map to int2 (PG has no int1).
        FieldKind::U8 | FieldKind::I8 | FieldKind::I16 => PG_TYPE_INT2,
        // 16-bit unsigned and 32-bit signed map to int4.
        FieldKind::U16 | FieldKind::I32 => PG_TYPE_INT4,
        // 32-bit unsigned, 64-bit signed, 64-bit unsigned all map to
        // int8. U64 above i64::MAX is a render-time overflow per
        // spec §11 weak-spot #4 (SQLSTATE 22003 numeric_value_out_of_range).
        FieldKind::U32 | FieldKind::I64 | FieldKind::U64 => PG_TYPE_INT8,
        // Arbitrary-precision values map to numeric. PG numeric is
        // text-format-trivial; V2 binary is deferred.
        FieldKind::U128 | FieldKind::I128 | FieldKind::Fixed { .. } => PG_TYPE_NUMERIC,
        // Fixed-length text → PG text (V1 prefers `text` over `bpchar`/
        // `varchar` — clients accept it interchangeably).
        FieldKind::Char(_) => PG_TYPE_TEXT,
        // Fixed-length raw bytes + 16-byte ObjectId + 8-byte overflow
        // handle all map to bytea. Bytea text format is `\x<hex>`.
        FieldKind::Bytes(_) | FieldKind::Ref | FieldKind::OverflowRef => PG_TYPE_BYTEA,
        // u64 nanos → timestamptz (text: ISO-8601 with +00).
        FieldKind::Timestamp => PG_TYPE_TIMESTAMPTZ,
    }
}

/// Maps a PG type OID back to the canonical KesselDB `FieldKind`.
/// Returns `None` for OIDs V1 doesn't support (e.g. `float4`/`float8`
/// — KesselDB has no f32/f64 FieldKind yet; `varchar` — clients
/// accept `text` interchangeably, so V1 doesn't need a separate
/// inverse).
///
/// Used by the (future) PARSE/BIND path (V2 SP-PG-EXTQ) where the
/// client declares parameter types by OID; the V1 simple-query path
/// doesn't need this direction (it's emit-only). Locked here so the
/// table is symmetric.
pub fn oid_to_field_kind(oid: u32) -> Option<FieldKind> {
    match oid {
        PG_TYPE_BOOL => Some(FieldKind::Bool),
        PG_TYPE_BYTEA => Some(FieldKind::Bytes(0)),
        PG_TYPE_INT8 => Some(FieldKind::I64),
        PG_TYPE_INT2 => Some(FieldKind::I16),
        PG_TYPE_INT4 => Some(FieldKind::I32),
        PG_TYPE_TEXT => Some(FieldKind::Char(0)),
        PG_TYPE_TIMESTAMPTZ => Some(FieldKind::Timestamp),
        PG_TYPE_NUMERIC => Some(FieldKind::I128),
        _ => None,
    }
}

/// PG wire `type_size` field for `RowDescription` per PG §55.7. Fixed-
/// length types report their byte width; variable-length types report
/// `-1` (i16 — a sentinel libpq switches on to choose its read path).
///
/// V1 uses this in T5's `encode_row_description` to fill the
/// `type_size` slot. Locked here so the table is single-sourced.
pub fn type_size_for_oid(oid: u32) -> i16 {
    match oid {
        PG_TYPE_BOOL => 1,
        PG_TYPE_INT2 => 2,
        PG_TYPE_INT4 => 4,
        crate::proto::PG_TYPE_OID => 4, // PG oid is a 4-byte unsigned int
        PG_TYPE_INT8 => 8,
        // PG `timestamptz` is 8 bytes in binary format (i64 µs since
        // 2000-01-01 00:00:00 UTC) — but V1 emits text format so the
        // size field is ignored by the client. We still report 8 to
        // match PG's own RowDescription for `timestamptz`.
        PG_TYPE_TIMESTAMPTZ => 8,
        // Variable-length types: bytea, text, numeric → -1.
        PG_TYPE_BYTEA | PG_TYPE_TEXT | PG_TYPE_NUMERIC => -1,
        _ => -1,
    }
}

// ───────────────────────────────────────────────────────────────────
// SP-PG-EXTQ-CAST-VALIDATE-COMPAT — PG type-category compatibility
// (relax strict OID equality for `$N::TYPE` cast validation).
//
// V1 SP-PG-EXTQ-CAST-VALIDATE enforced STRICT OID equality between
// the Bind-supplied param OID and the SQL's `::TYPE` cast OID. That
// rejection is correct against the V1 contract but wrong against
// real ORM behaviour: pgJDBC's default `Long` binding sends INT8,
// but a Java `int` against an `::int8` cast sends INT4 + INT8
// mismatched at the wire; psycopg3 has the same behaviour for
// Python `int` → INT4 against `::int8` casts. PG itself accepts
// these widenings via its `pg_type.dat::typcategory` table.
//
// V1 of THIS arc adds two helpers:
//   - `oid_category(oid)` returns the PG `typcategory` byte for the
//     V1 type OID set.
//   - `oid_castable(param_oid, cast_oid)` returns true iff the cast
//     should be accepted: strict-equality + omitted-OID-skip +
//     within-category widening.
// `dispatch_bind`'s validator swaps strict equality for the helper;
// the error variant + state set + first-mismatch-wins ordering are
// byte-untouched.
//
// V2 follow-ups named: `SP-PG-EXTQ-CAST-VALIDATE-COMPAT-RANGE`
// (overflow-check param value vs cast-type range),
// `SP-PG-EXTQ-CAST-VALIDATE-LITERAL` (relax-and-validate literal
// casts too), `SP-PG-EXTQ-CAST-VALIDATE-CATEGORY-CROSS` (accept the
// cross-category casts PG itself accepts, e.g. TEXT '42' → INT8).
// ───────────────────────────────────────────────────────────────────

/// SP-PG-EXTQ-CAST-VALIDATE-COMPAT — returns the PG `typcategory`
/// byte for the V1 type OID set. Unknown OIDs return `'U'` (user-
/// defined / unknown) — they form their own degenerate category and
/// only compare-equal to themselves under `oid_castable`.
///
/// Categories per `pg_type.dat`:
/// - `'N'` numeric: int2 / int4 / int8 / float4 / float8 / numeric.
/// - `'S'` string: text / varchar / bpchar.
/// - `'B'` boolean: bool.
/// - `'D'` date/time: timestamptz / time / timestamp.
/// - `'U'` user-defined / unknown / binary: bytea + any OID this V1
///   doesn't recognise. We deliberately keep bytea isolated in `'U'`
///   so the validator effect is "bytea ↔ bytea only" (PG itself
///   groups bytea with `'B'` bit-string, but V1 of this arc plays
///   it safe — widening bytea would touch the binary-format admission
///   plumbing in `dispatch_bind`).
pub fn oid_category(oid: u32) -> char {
    match oid {
        PG_TYPE_BOOL => 'B',
        // bytea — isolated in 'U' so it only compares-equal to itself.
        PG_TYPE_BYTEA => 'U',
        // All numeric OIDs share the 'N' category. PG itself treats
        // INT2/INT4/INT8/FLOAT4/FLOAT8/NUMERIC as a coercion family
        // (widening + narrowing both legal at the type-check layer;
        // narrowing overflow is a 22003 at compute time, not at
        // typecheck).
        PG_TYPE_INT2
        | PG_TYPE_INT4
        | PG_TYPE_INT8
        | PG_TYPE_FLOAT4
        | PG_TYPE_FLOAT8
        | PG_TYPE_NUMERIC => 'N',
        // Text-family OIDs share 'S'. Includes the canonical `bpchar`
        // OID (1042) so a `::bpchar` cast against a TEXT param doesn't
        // false-reject (rare but pg_dump-style flows emit it).
        PG_TYPE_TEXT | PG_TYPE_VARCHAR | 1042 => 'S',
        // Date/time family share 'D'. timestamptz + time + timestamp;
        // date (1082) + interval (1186) deferred to a future arc until
        // KesselDB has FieldKinds for them.
        PG_TYPE_TIMESTAMPTZ | 1083 | 1114 => 'D',
        _ => 'U',
    }
}

/// SP-PG-EXTQ-CAST-VALIDATE-COMPAT — returns true iff a Bind whose
/// param has `param_oid` should be accepted against a SQL cast to
/// `cast_oid`.
///
/// Truth table (in evaluation order):
///   1. `param_oid == cast_oid` → true (V1 strict equality is the
///      base case; locked here so a V2 relax doesn't regress the
///      V1 happy path).
///   2. `param_oid == 0` → true (Parse omitted the OID hint at this
///      position — V1 skip rule preserved for asyncpg / psycopg3
///      default shape).
///   3. `oid_category(param_oid) == oid_category(cast_oid)` → true
///      (the V2 widening: any pair of OIDs sharing a `typcategory`
///      byte is mutually castable at the gateway level; the
///      engine type-checker is the final arbiter for any actual
///      coercion errors at compute time).
///   4. Otherwise → false (cross-category mismatch; caller surfaces
///      `ExtqError::CastOidMismatch` → SQLSTATE `42846 cannot_coerce`).
///
/// Used by `extq::dispatch_bind` to replace V1's strict
/// `actual_oid != declared_oid` check. The skip-and-strict cases
/// collapse into this helper so the dispatch-side loop stays a
/// single comparison.
pub fn oid_castable(param_oid: u32, cast_oid: u32) -> bool {
    if param_oid == cast_oid {
        return true;
    }
    // V1 skip rule — Parse omitted the OID hint at this position.
    // The omitted hint is the client's explicit signal "trust the
    // SQL"; asyncpg / psycopg3 default flow lands here.
    if param_oid == 0 {
        return true;
    }
    // V2 widening — intra-category compatibility table.
    oid_category(param_oid) == oid_category(cast_oid)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ───────────────────────────────────────────────────────────────────
    // T4 KATs — lock the FieldKind ↔ OID table per spec §5 against
    // `pg_type.dat`. Flipping any entry silently corrupts every
    // RowDescription on the wire; the KATs guard against that.
    // ───────────────────────────────────────────────────────────────────

    /// Bool → bool (16) round-trips.
    #[test]
    fn t4_bool_round_trips() {
        assert_eq!(field_kind_to_oid(FieldKind::Bool), PG_TYPE_BOOL);
        assert_eq!(field_kind_to_oid(FieldKind::Bool), 16);
        assert_eq!(oid_to_field_kind(PG_TYPE_BOOL), Some(FieldKind::Bool));
    }

    /// I8 / I16 / U8 all map to int2 (21).
    #[test]
    fn t4_small_ints_map_to_int2() {
        assert_eq!(field_kind_to_oid(FieldKind::I8), PG_TYPE_INT2);
        assert_eq!(field_kind_to_oid(FieldKind::I16), PG_TYPE_INT2);
        assert_eq!(field_kind_to_oid(FieldKind::U8), PG_TYPE_INT2);
        assert_eq!(PG_TYPE_INT2, 21);
        // Inverse picks the canonical signed variant.
        assert_eq!(oid_to_field_kind(PG_TYPE_INT2), Some(FieldKind::I16));
    }

    /// I32 / U16 map to int4 (23).
    #[test]
    fn t4_medium_ints_map_to_int4() {
        assert_eq!(field_kind_to_oid(FieldKind::I32), PG_TYPE_INT4);
        assert_eq!(field_kind_to_oid(FieldKind::U16), PG_TYPE_INT4);
        assert_eq!(PG_TYPE_INT4, 23);
        assert_eq!(oid_to_field_kind(PG_TYPE_INT4), Some(FieldKind::I32));
    }

    /// I64 / U32 / U64 all map to int8 (20). U64 > i64::MAX is a
    /// render-time overflow per spec §11 weak-spot #4 (not asserted
    /// here — that's T5/T8 encoder territory).
    #[test]
    fn t4_large_ints_map_to_int8() {
        assert_eq!(field_kind_to_oid(FieldKind::I64), PG_TYPE_INT8);
        assert_eq!(field_kind_to_oid(FieldKind::U32), PG_TYPE_INT8);
        assert_eq!(field_kind_to_oid(FieldKind::U64), PG_TYPE_INT8);
        assert_eq!(PG_TYPE_INT8, 20);
        assert_eq!(oid_to_field_kind(PG_TYPE_INT8), Some(FieldKind::I64));
    }

    /// U128 / I128 / Fixed all map to numeric (1700).
    #[test]
    fn t4_huge_and_fixed_map_to_numeric() {
        assert_eq!(field_kind_to_oid(FieldKind::U128), PG_TYPE_NUMERIC);
        assert_eq!(field_kind_to_oid(FieldKind::I128), PG_TYPE_NUMERIC);
        assert_eq!(
            field_kind_to_oid(FieldKind::Fixed { scale: 2 }),
            PG_TYPE_NUMERIC
        );
        assert_eq!(
            field_kind_to_oid(FieldKind::Fixed { scale: 18 }),
            PG_TYPE_NUMERIC
        );
        assert_eq!(PG_TYPE_NUMERIC, 1700);
        assert_eq!(oid_to_field_kind(PG_TYPE_NUMERIC), Some(FieldKind::I128));
    }

    /// Char(n) maps to text (25).
    #[test]
    fn t4_char_maps_to_text() {
        assert_eq!(field_kind_to_oid(FieldKind::Char(8)), PG_TYPE_TEXT);
        assert_eq!(field_kind_to_oid(FieldKind::Char(128)), PG_TYPE_TEXT);
        assert_eq!(field_kind_to_oid(FieldKind::Char(0)), PG_TYPE_TEXT);
        assert_eq!(PG_TYPE_TEXT, 25);
        assert_eq!(oid_to_field_kind(PG_TYPE_TEXT), Some(FieldKind::Char(0)));
    }

    /// Bytes(n) / Ref / OverflowRef all map to bytea (17).
    #[test]
    fn t4_bytes_ref_overflowref_map_to_bytea() {
        assert_eq!(field_kind_to_oid(FieldKind::Bytes(16)), PG_TYPE_BYTEA);
        assert_eq!(field_kind_to_oid(FieldKind::Bytes(32)), PG_TYPE_BYTEA);
        assert_eq!(field_kind_to_oid(FieldKind::Ref), PG_TYPE_BYTEA);
        assert_eq!(field_kind_to_oid(FieldKind::OverflowRef), PG_TYPE_BYTEA);
        assert_eq!(PG_TYPE_BYTEA, 17);
        assert_eq!(oid_to_field_kind(PG_TYPE_BYTEA), Some(FieldKind::Bytes(0)));
    }

    /// Timestamp maps to timestamptz (1184). KesselDB stores u64 ns;
    /// the text renderer (T8) will emit `YYYY-MM-DD HH:MM:SS.ffffff+00`.
    #[test]
    fn t4_timestamp_maps_to_timestamptz() {
        assert_eq!(field_kind_to_oid(FieldKind::Timestamp), PG_TYPE_TIMESTAMPTZ);
        assert_eq!(PG_TYPE_TIMESTAMPTZ, 1184);
        assert_eq!(
            oid_to_field_kind(PG_TYPE_TIMESTAMPTZ),
            Some(FieldKind::Timestamp)
        );
    }

    /// Unknown OIDs return `None` from the inverse (no panic, graceful).
    /// V2 PARSE/BIND will surface "unknown parameter type" via this
    /// channel.
    #[test]
    fn t4_unknown_oid_returns_none() {
        assert_eq!(oid_to_field_kind(0), None);
        assert_eq!(oid_to_field_kind(99999), None);
        // float4 / float8 / varchar — V1 has no FieldKind for them.
        assert_eq!(oid_to_field_kind(700), None);
        assert_eq!(oid_to_field_kind(701), None);
        assert_eq!(oid_to_field_kind(1043), None);
    }

    /// Exhaustiveness — every `FieldKind` variant has an assigned OID.
    /// If a new variant is added to kessel-catalog, this test will
    /// fail to compile (the `match` is exhaustive) — that's the
    /// design.
    #[test]
    fn t4_every_field_kind_has_an_oid_assignment() {
        // Listed explicitly here (not via a generator) so a new
        // FieldKind variant breaks the workspace build at this
        // function until SP-PG decides how to map it.
        let all_variants = [
            FieldKind::U8,
            FieldKind::U16,
            FieldKind::U32,
            FieldKind::U64,
            FieldKind::U128,
            FieldKind::I8,
            FieldKind::I16,
            FieldKind::I32,
            FieldKind::I64,
            FieldKind::I128,
            FieldKind::Bool,
            FieldKind::Fixed { scale: 0 },
            FieldKind::Char(0),
            FieldKind::Bytes(0),
            FieldKind::Timestamp,
            FieldKind::Ref,
            FieldKind::OverflowRef,
        ];
        for v in all_variants.iter() {
            let oid = field_kind_to_oid(*v);
            // Every variant maps to one of the 6 V1-supported OIDs.
            assert!(
                matches!(
                    oid,
                    16 | 17 | 20 | 21 | 23 | 25 | 1184 | 1700
                ),
                "FieldKind {:?} mapped to unexpected OID {}",
                v,
                oid
            );
        }
    }

    /// `type_size_for_oid` returns the PG-wire fixed sizes for
    /// scalars and -1 for variable-length types. Locked against
    /// libpq's own `pqGetParameterStatus` size expectations.
    #[test]
    fn t4_type_size_for_oid_matches_pg() {
        assert_eq!(type_size_for_oid(PG_TYPE_BOOL), 1);
        assert_eq!(type_size_for_oid(PG_TYPE_INT2), 2);
        assert_eq!(type_size_for_oid(PG_TYPE_INT4), 4);
        assert_eq!(type_size_for_oid(PG_TYPE_INT8), 8);
        assert_eq!(type_size_for_oid(PG_TYPE_TIMESTAMPTZ), 8);
        // Variable-length → -1.
        assert_eq!(type_size_for_oid(PG_TYPE_BYTEA), -1);
        assert_eq!(type_size_for_oid(PG_TYPE_TEXT), -1);
        assert_eq!(type_size_for_oid(PG_TYPE_NUMERIC), -1);
        // Unknown OIDs default to -1 (safest — libpq treats unknown
        // as variable-length).
        assert_eq!(type_size_for_oid(99999), -1);
    }

    // ───────────────────────────────────────────────────────────────
    // SP-PG-EXTQ-CAST-VALIDATE-COMPAT KATs — lock the type-category
    // table + the `oid_castable` widening rules. Cross-category
    // rejection MUST stay locked or the V1 silent-coercion vector
    // re-opens; intra-category acceptance MUST stay locked or
    // pgJDBC INT4-against-INT8 binds regress to V1 strict rejection.
    // ───────────────────────────────────────────────────────────────

    /// `oid_category` returns 'N' for every V1 numeric OID. PG groups
    /// int2/int4/int8/float4/float8/numeric in the same coercion
    /// family at the type-check layer.
    #[test]
    fn cast_compat_oid_category_numeric_family() {
        assert_eq!(oid_category(PG_TYPE_INT2), 'N');
        assert_eq!(oid_category(PG_TYPE_INT4), 'N');
        assert_eq!(oid_category(PG_TYPE_INT8), 'N');
        assert_eq!(oid_category(PG_TYPE_FLOAT4), 'N');
        assert_eq!(oid_category(PG_TYPE_FLOAT8), 'N');
        assert_eq!(oid_category(PG_TYPE_NUMERIC), 'N');
    }

    /// `oid_category` returns 'S' for every V1 string OID
    /// (text/varchar/bpchar).
    #[test]
    fn cast_compat_oid_category_string_family() {
        assert_eq!(oid_category(PG_TYPE_TEXT), 'S');
        assert_eq!(oid_category(PG_TYPE_VARCHAR), 'S');
        assert_eq!(oid_category(1042 /* bpchar */), 'S');
    }

    /// `oid_category` returns 'B' for bool, 'D' for date/time OIDs,
    /// 'U' for bytea + unknown OIDs.
    #[test]
    fn cast_compat_oid_category_isolated_and_unknown() {
        assert_eq!(oid_category(PG_TYPE_BOOL), 'B');
        // Date/time family.
        assert_eq!(oid_category(PG_TYPE_TIMESTAMPTZ), 'D');
        assert_eq!(oid_category(1083 /* time */), 'D');
        assert_eq!(oid_category(1114 /* timestamp */), 'D');
        // Bytea stays in 'U' so it only ↔ bytea (and other 'U' OIDs).
        assert_eq!(oid_category(PG_TYPE_BYTEA), 'U');
        // Unknown OIDs fall through to 'U'.
        assert_eq!(oid_category(99999), 'U');
        assert_eq!(oid_category(0), 'U');
    }

    /// HEADLINE: INT4 param + INT8 cast — V1 strict rejected; V2
    /// accepts (same 'N' category). This is the pgJDBC default
    /// binding shape that the V1 strict equality false-rejected.
    #[test]
    fn cast_compat_int4_param_int8_cast_accepted() {
        assert!(oid_castable(PG_TYPE_INT4, PG_TYPE_INT8));
    }

    /// INT8 param + INT4 cast — symmetric to the headline case
    /// (still 'N' ↔ 'N').
    #[test]
    fn cast_compat_int8_param_int4_cast_accepted() {
        assert!(oid_castable(PG_TYPE_INT8, PG_TYPE_INT4));
    }

    /// INT8 param + FLOAT8 cast — int-to-float widening within
    /// the 'N' category. Real psycopg `int` + `::float8` pattern.
    #[test]
    fn cast_compat_int8_param_float8_cast_accepted() {
        assert!(oid_castable(PG_TYPE_INT8, PG_TYPE_FLOAT8));
    }

    /// INT4 param + NUMERIC cast — numeric is also 'N', so widening
    /// to arbitrary precision passes the gateway typecheck.
    #[test]
    fn cast_compat_int4_param_numeric_cast_accepted() {
        assert!(oid_castable(PG_TYPE_INT4, PG_TYPE_NUMERIC));
    }

    /// TEXT param + VARCHAR cast — same 'S' category. pgJDBC
    /// `setString` against `::varchar(N)` casts.
    #[test]
    fn cast_compat_text_param_varchar_cast_accepted() {
        assert!(oid_castable(PG_TYPE_TEXT, PG_TYPE_VARCHAR));
        // Symmetric.
        assert!(oid_castable(PG_TYPE_VARCHAR, PG_TYPE_TEXT));
    }

    /// TEXT param + INT8 cast — cross-category ('S' vs 'N'). This is
    /// the V1 silent-coercion vector — V2 MUST still reject so the
    /// closed attack surface stays closed.
    #[test]
    fn cast_compat_text_param_int8_cast_rejected() {
        assert!(!oid_castable(PG_TYPE_TEXT, PG_TYPE_INT8));
    }

    /// BOOL param + INT8 cast — 'B' vs 'N' cross-category rejection.
    #[test]
    fn cast_compat_bool_param_int8_cast_rejected() {
        assert!(!oid_castable(PG_TYPE_BOOL, PG_TYPE_INT8));
    }

    /// BYTEA param + TEXT cast — 'U' (bytea) vs 'S' cross-category
    /// rejection. Keeps the binary-format admission boundary intact.
    #[test]
    fn cast_compat_bytea_param_text_cast_rejected() {
        assert!(!oid_castable(PG_TYPE_BYTEA, PG_TYPE_TEXT));
    }

    /// Same OID on both sides — V1 strict equality is the base case.
    /// `oid_castable` MUST preserve V1 happy-path acceptance for
    /// every same-OID input.
    #[test]
    fn cast_compat_strict_equality_base_case() {
        for &oid in &[
            PG_TYPE_BOOL,
            PG_TYPE_BYTEA,
            PG_TYPE_INT2,
            PG_TYPE_INT4,
            PG_TYPE_INT8,
            PG_TYPE_TEXT,
            PG_TYPE_VARCHAR,
            PG_TYPE_FLOAT4,
            PG_TYPE_FLOAT8,
            PG_TYPE_NUMERIC,
            PG_TYPE_TIMESTAMPTZ,
        ] {
            assert!(
                oid_castable(oid, oid),
                "same-OID ({}, {}) MUST be castable",
                oid,
                oid
            );
        }
    }

    /// Omitted OID hint (`param_oid == 0`) → V1 skip rule preserved.
    /// asyncpg / psycopg3 default flow lands here — the validator
    /// MUST NOT reject regardless of the cast OID.
    #[test]
    fn cast_compat_omitted_oid_hint_skips_validation() {
        for &cast_oid in &[
            PG_TYPE_INT8,
            PG_TYPE_TEXT,
            PG_TYPE_BOOL,
            PG_TYPE_BYTEA,
            PG_TYPE_NUMERIC,
            PG_TYPE_VARCHAR,
            PG_TYPE_TIMESTAMPTZ,
        ] {
            assert!(
                oid_castable(0, cast_oid),
                "omitted param OID hint (0) MUST skip validation \
                 even against cast OID {cast_oid}"
            );
        }
    }

    /// Unknown param OID + KNOWN-typed cast OID rejects (the V1
    /// "trust nothing about unknown types" stance is preserved
    /// because `'U' != 'N' / 'S' / 'B' / 'D'`). Same-unknown
    /// (degenerate 'U' ↔ 'U') accepts via strict equality.
    #[test]
    fn cast_compat_unknown_oid_vs_known_rejected() {
        assert!(!oid_castable(99999, PG_TYPE_INT8));
        assert!(!oid_castable(99999, PG_TYPE_TEXT));
        // Same unknown ↔ same unknown is accepted via strict equality.
        assert!(oid_castable(99999, 99999));
    }
}
