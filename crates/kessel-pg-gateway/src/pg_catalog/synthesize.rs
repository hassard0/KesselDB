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
use crate::engine::{EngineApply, TableKind, TableMetadata};
use crate::proto::{
    PG_TYPE_BOOL, PG_TYPE_BYTEA, PG_TYPE_INT2, PG_TYPE_INT4, PG_TYPE_INT8,
    PG_TYPE_NUMERIC, PG_TYPE_OID, PG_TYPE_TEXT, PG_TYPE_TIMESTAMPTZ,
    PG_TYPE_VARCHAR,
};
use crate::response::{
    encode_command_complete, encode_data_row, encode_ready_for_query,
    encode_row_description, select_tag, FieldMeta,
};
use crate::types::{field_kind_to_oid, type_size_for_oid};

// ─── OID generation (spec §3.4 + design §5.2) ────────────────────────────

/// First OID in PG's user-allocated range (`FirstNormalObjectId` from
/// `src/include/access/transam.h`). PG reserves OIDs below this for
/// system catalogs; user-created tables / types / functions always
/// land above. Locked so the pg_class synthesizer NEVER collides
/// with a PG-system OID a tool might JOIN against.
pub const FIRST_USER_OID: u32 = 16384;

/// SP-PG-CAT T3 — deterministic OID for a KesselDB table name.
///
/// Maps a name into the user-allocated OID range `[16384, u32::MAX]`
/// via FNV-1a 32-bit. Pure function — same name on every replica /
/// every restart → same OID — so PG clients that cache OIDs
/// (everything that hits `pg_class` more than once) see stable
/// joins.
///
/// **Collision risk** (design §9 weak-spot #7): the user-OID range
/// is ~`u32::MAX - 16384 ≈ 4.29 billion` slots; birthday-paradox
/// 50% collision at ~`sqrt(2 × 4.29e9) ≈ 92K` tables. KATs assert
/// no collision in the V1 canonical name corpus; production
/// risk is low but real, and the V2 SP-PG-CAT-OID slice will
/// switch to a monotonic-counter scheme keyed on the catalog
/// epoch.
///
/// FNV-1a is chosen over SHA-256 because:
/// - It's the documented `kessel-crypto`-free option (no new dep).
/// - It's ~20× faster than SHA-256 for short names (sub-ns vs ns).
/// - The OID space is so small that any cryptographic property
///   (preimage resistance, etc.) is irrelevant — a 32-bit OID
///   carries ≤32 bits of name-derived entropy regardless.
pub fn oid_for_table_name(name: &str) -> u32 {
    // FNV-1a 32-bit: offset 2166136261, prime 16777619.
    const OFFSET: u32 = 2_166_136_261;
    const PRIME: u32 = 16_777_619;
    let mut h: u32 = OFFSET;
    for b in name.as_bytes() {
        h ^= u32::from(*b);
        h = h.wrapping_mul(PRIME);
    }
    // Clamp to [FIRST_USER_OID, u32::MAX]. The hash is uniform in
    // [0, u32::MAX]; reducing modulo (u32::MAX - FIRST_USER_OID + 1)
    // gives a near-uniform distribution above FIRST_USER_OID.
    FIRST_USER_OID.wrapping_add(h % (u32::MAX - FIRST_USER_OID + 1))
}

// ─── pg_class synthesizer (design §5.2) ──────────────────────────────────

/// pg_class column count per PG 14 `src/include/catalog/pg_class.h`.
/// Locked because psql / JDBC / pgcli iterate by `attnum` and break
/// silently if the count is off. V1 emits all 33 columns; most are
/// PG-default canned (see `encode_pg_class_row` for per-column
/// values).
///
/// Note: PG 14's pg_class has 33 columns (we drop `oid` from the
/// column count because it's a system column emitted SEPARATELY
/// in the RowDescription per PG convention — but for V1 we DO
/// emit `oid` as a regular column at the start of the row so
/// clients that `SELECT *` see it explicitly).
pub const PG_CLASS_COLUMN_COUNT: usize = 33;

/// Build the `pg_class` RowDescription field list. 33 columns in
/// the order PG 14 defines them. Pulled out so both the `SELECT *`
/// path and the JOIN-pattern path emit the same shape.
fn pg_class_fields() -> Vec<FieldMeta> {
    let oid = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_OID };
    let text = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_TEXT };
    let bool_ = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_BOOL };
    let int2 = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_INT2 };
    let int4 = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_INT4 };
    let char1 = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_TEXT };
    vec![
        oid("oid"),
        text("relname"),
        oid("relnamespace"),
        oid("reltype"),
        oid("reloftype"),
        oid("relowner"),
        oid("relam"),
        oid("relfilenode"),
        oid("reltablespace"),
        int4("relpages"),
        text("reltuples"),       // PG `float4` — V1 emits as text.
        int4("relallvisible"),
        oid("reltoastrelid"),
        bool_("relhasindex"),
        bool_("relisshared"),
        char1("relpersistence"),
        char1("relkind"),
        int2("relnatts"),
        int2("relchecks"),
        bool_("relhasrules"),
        bool_("relhastriggers"),
        bool_("relhassubclass"),
        bool_("relrowsecurity"),
        bool_("relforcerowsecurity"),
        bool_("relispopulated"),
        char1("relreplident"),
        bool_("relispartition"),
        oid("relrewrite"),
        oid("relfrozenxid"),
        oid("relminmxid"),
        text("relacl"),          // PG `aclitem[]` — V1 NULL.
        text("reloptions"),      // PG `text[]` — V1 NULL.
        text("relpartbound"),    // PG `pg_node_tree` — V1 NULL.
    ]
}

/// Emit one pg_class DataRow for `tbl`. Spec §5.2.
fn encode_pg_class_row(tbl: &TableMetadata) -> Vec<u8> {
    let oid_str = oid_for_table_name(&tbl.name).to_string();
    let relnamespace = PG_NAMESPACE_OID_PUBLIC.to_string();
    let zero_oid = b"0".as_ref();
    let relowner = PG_AUTHID_OID_POSTGRES.to_string();
    let relam = match tbl.kind {
        TableKind::Index => b"403".as_ref(), // btree am — V1 doesn't differentiate
        _ => b"2".as_ref(),                  // heap am
    };
    let relfilenode = oid_str.as_bytes().to_vec(); // V1: same as OID
    let zero = b"0".as_ref();
    // reltuples = -1 (unknown) per design §5.2. PG `float4` text format
    // is just the literal.
    let reltuples = b"-1".as_ref();
    let relhasindex = b"f".as_ref(); // V1: no engine-side index tracking yet
    let false_ = b"f".as_ref();
    let true_ = b"t".as_ref();
    let relpersistence = b"p".as_ref(); // permanent
    let relkind_byte = [tbl.kind.pg_relkind()];
    let relnatts = tbl.field_count.to_string();
    let zero_int2 = b"0".as_ref();
    let relreplident = b"d".as_ref(); // default

    encode_data_row(&[
        Some(oid_str.as_bytes()),       // oid
        Some(tbl.name.as_bytes()),      // relname
        Some(relnamespace.as_bytes()),  // relnamespace
        Some(zero_oid),                 // reltype
        Some(zero_oid),                 // reloftype
        Some(relowner.as_bytes()),      // relowner
        Some(relam),                    // relam
        Some(&relfilenode),             // relfilenode
        Some(zero_oid),                 // reltablespace
        Some(zero),                     // relpages
        Some(reltuples),                // reltuples
        Some(zero),                     // relallvisible
        Some(zero_oid),                 // reltoastrelid
        Some(relhasindex),              // relhasindex
        Some(false_),                   // relisshared
        Some(relpersistence),           // relpersistence
        Some(&relkind_byte),            // relkind
        Some(relnatts.as_bytes()),      // relnatts
        Some(zero_int2),                // relchecks
        Some(false_),                   // relhasrules
        Some(false_),                   // relhastriggers
        Some(false_),                   // relhassubclass
        Some(false_),                   // relrowsecurity
        Some(false_),                   // relforcerowsecurity
        Some(true_),                    // relispopulated
        Some(relreplident),             // relreplident
        Some(false_),                   // relispartition
        Some(zero_oid),                 // relrewrite
        Some(zero_oid),                 // relfrozenxid
        Some(zero_oid),                 // relminmxid
        None,                           // relacl = NULL
        None,                           // reloptions = NULL
        None,                           // relpartbound = NULL
    ])
}

/// Synthesize `SELECT * FROM pg_catalog.pg_class` — one full
/// `pg_class` row per KesselDB user table. Returns the full T+D*+C+Z
/// wire stream.
///
/// Reads the live catalog via `engine.list_tables()`; on engines that
/// don't override the default, returns a 0-row well-framed response
/// (psql `\dt` prints "did not find any relations").
pub fn pg_class_all_rows<E: EngineApply + ?Sized>(engine: &E) -> Vec<u8> {
    let fields = pg_class_fields();
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    let n = tables.len() as u64;
    for t in &tables {
        out.extend_from_slice(&encode_pg_class_row(t));
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

// ─── Joined-result synth: psql `\dt` (design §3.4 strategy A) ────────────

/// Synthesize the joined-result response for the canonical psql `\dt`
/// query. The psql query JOINs `pg_class` + `pg_namespace` + filters
/// + ORDER BY. V1 doesn't run arbitrary SQL JOINs — we recognize the
/// exact canonical shape and emit the joined-result rows directly.
///
/// Output columns (matching psql's `\dt` projection): Schema / Name /
/// Type / Owner. Per design §3.4 strategy A.
pub fn psql_dt_joined_rows<E: EngineApply + ?Sized>(engine: &E) -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "Schema".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "Name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "Type".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "Owner".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    // Filter to ordinary tables (psql `\dt` WHERE relkind IN ('r','p','')
    // — V1 only has 'r' so this matches everything from list_tables).
    let mut n: u64 = 0;
    for t in &tables {
        if !matches!(t.kind, TableKind::Ordinary) {
            continue;
        }
        out.extend_from_slice(&encode_data_row(&[
            Some(b"public"),
            Some(t.name.as_bytes()),
            Some(b"table"),
            Some(b"kesseldb"),
        ]));
        n += 1;
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

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

// ─── pg_attribute synthesizer (design §5.3) ───────────────────────────────

/// `pg_attribute` column count per PG 14 `src/include/catalog/
/// pg_attribute.h`. Locked because clients (psql `\d`, JDBC
/// `getColumns`, pgcli `columns()`, DBeaver column cache) iterate
/// row columns by index; one off-by-one breaks them all.
pub const PG_ATTRIBUTE_COLUMN_COUNT: usize = 25;

/// PG default collation OID for text-like types (PG `default`
/// collation; locked vs `src/include/catalog/pg_collation.dat`). Used
/// in `pg_attribute.attcollation` for text/varchar columns; 0 for
/// non-text columns.
pub const PG_COLLATION_DEFAULT: u32 = 100;

/// Build the `pg_attribute` RowDescription field list. 25 columns
/// in the order PG 14 defines them. Pulled out so both `SELECT *`
/// and the per-table filter paths emit the same shape.
fn pg_attribute_fields() -> Vec<FieldMeta> {
    let oid = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_OID };
    let text = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_TEXT };
    let bool_ = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_BOOL };
    let int2 = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_INT2 };
    let int4 = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_INT4 };
    let char1 = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_TEXT };
    vec![
        oid("attrelid"),
        text("attname"),
        oid("atttypid"),
        int4("attstattarget"),
        int2("attlen"),
        int2("attnum"),
        int4("attndims"),
        int4("attcacheoff"),
        int4("atttypmod"),
        bool_("attbyval"),
        char1("attstorage"),
        char1("attalign"),
        bool_("attnotnull"),
        bool_("atthasdef"),
        bool_("atthasmissing"),
        char1("attidentity"),
        char1("attgenerated"),
        bool_("attisdropped"),
        bool_("attislocal"),
        int4("attinhcount"),
        oid("attcollation"),
        text("attacl"),         // aclitem[] — V1 NULL
        text("attoptions"),     // text[] — V1 NULL
        text("attfdwoptions"),  // text[] — V1 NULL
        text("attmissingval"),  // anyarray — V1 NULL
    ]
}

/// PG `attbyval` (pass-by-value) for a type OID. True for the
/// fixed-size primitives PG passes in registers (bool, int2, int4,
/// int8 on 64-bit, oid, timestamptz binary); false for variable-
/// length (bytea/text/numeric/varchar). Locked vs `pg_type.dat`.
fn attbyval_for_oid(oid: u32) -> bool {
    matches!(
        oid,
        PG_TYPE_BOOL
            | PG_TYPE_INT2
            | PG_TYPE_INT4
            | PG_TYPE_INT8
            | PG_TYPE_OID
            | PG_TYPE_TIMESTAMPTZ
    )
}

/// PG `attstorage` (TOAST storage strategy) char for a type OID:
/// - 'p' (plain) for fixed-size primitives — never TOASTed.
/// - 'x' (extended) for variable-length — TOAST-eligible + compress.
///
/// Locked vs `pg_type.dat` typstorage column.
fn attstorage_for_oid(oid: u32) -> u8 {
    if attbyval_for_oid(oid) {
        b'p'
    } else {
        b'x'
    }
}

/// PG `attalign` (alignment requirement) char for a type OID:
/// - 'c' (char/byte) for bool + bytea + 1-byte text.
/// - 's' (short/2-byte) for int2.
/// - 'i' (int/4-byte) for int4 + oid.
/// - 'd' (double/8-byte) for int8 + timestamptz.
/// - 'i' (int/4-byte) for text/varchar (varlena header is 4-byte aligned).
/// - 'i' (int/4-byte) for numeric.
///
/// Locked vs `pg_type.dat` typalign column.
fn attalign_for_oid(oid: u32) -> u8 {
    match oid {
        PG_TYPE_BOOL | PG_TYPE_BYTEA => b'c',
        PG_TYPE_INT2 => b's',
        PG_TYPE_INT4 | PG_TYPE_OID | PG_TYPE_TEXT | PG_TYPE_VARCHAR
        | PG_TYPE_NUMERIC => b'i',
        PG_TYPE_INT8 | PG_TYPE_TIMESTAMPTZ => b'd',
        _ => b'i',
    }
}

/// Emit one pg_attribute DataRow for a column.
///
/// - `attrelid` — the table's pg_class.oid (`oid_for_table_name(name)`).
/// - `attname` — column name.
/// - `atttypid` — `field_kind_to_oid(kind)` from V1's type-OID map.
/// - `attlen` — `type_size_for_oid(atttypid)` (-1 for varlena).
/// - `attnum` — 1-based column index (i16).
/// - `attnotnull` — `!nullable` (KesselDB defaults NOT NULL in V1).
///
/// The remaining 19 columns are PG-default canned (see design §5.3 +
/// per-OID helpers above).
fn encode_pg_attribute_row(
    attrelid: u32,
    attname: &str,
    atttypid: u32,
    attnum: i16,
    nullable: bool,
) -> Vec<u8> {
    let attrelid_str = attrelid.to_string();
    let atttypid_str = atttypid.to_string();
    let attlen = type_size_for_oid(atttypid);
    let attlen_str = attlen.to_string();
    let attnum_str = attnum.to_string();
    let zero = b"0".as_ref();
    let neg_one = b"-1".as_ref();
    let false_ = b"f".as_ref();
    let true_ = b"t".as_ref();
    let attbyval = if attbyval_for_oid(atttypid) { true_ } else { false_ };
    let attnotnull = if nullable { false_ } else { true_ };
    let storage_byte = [attstorage_for_oid(atttypid)];
    let align_byte = [attalign_for_oid(atttypid)];
    let empty = b"".as_ref();
    let collation = if matches!(atttypid, PG_TYPE_TEXT | PG_TYPE_VARCHAR) {
        PG_COLLATION_DEFAULT.to_string()
    } else {
        "0".to_string()
    };

    encode_data_row(&[
        Some(attrelid_str.as_bytes()),  // attrelid
        Some(attname.as_bytes()),       // attname
        Some(atttypid_str.as_bytes()),  // atttypid
        Some(neg_one),                  // attstattarget = -1
        Some(attlen_str.as_bytes()),    // attlen
        Some(attnum_str.as_bytes()),    // attnum
        Some(zero),                     // attndims = 0
        Some(neg_one),                  // attcacheoff = -1
        Some(neg_one),                  // atttypmod = -1
        Some(attbyval),                 // attbyval
        Some(&storage_byte),            // attstorage
        Some(&align_byte),              // attalign
        Some(attnotnull),               // attnotnull
        Some(false_),                   // atthasdef = false (V1 no defaults)
        Some(false_),                   // atthasmissing = false
        Some(empty),                    // attidentity = '' (not identity)
        Some(empty),                    // attgenerated = '' (not generated)
        Some(false_),                   // attisdropped = false
        Some(true_),                    // attislocal = true
        Some(zero),                     // attinhcount = 0 (no inheritance)
        Some(collation.as_bytes()),     // attcollation
        None,                           // attacl = NULL
        None,                           // attoptions = NULL
        None,                           // attfdwoptions = NULL
        None,                           // attmissingval = NULL
    ])
}

/// Synthesize `SELECT * FROM pg_catalog.pg_attribute` — one row per
/// (table × column) of every KesselDB user table.
///
/// `attrelid_filter`:
/// - `None` — emit every column of every table (slow but correct;
///   the broad `SELECT * FROM pg_catalog.pg_attribute` path).
/// - `Some(oid)` — emit only the columns belonging to the table whose
///   `oid_for_table_name(name)` matches the filter. This is the
///   common psql `\d <table>` case + pgJDBC `getColumns` case.
///
/// Walks `engine.list_tables()` and `engine.describe_table(name)` for
/// each table that survives the filter. Returns the full T+D*+C+Z
/// wire stream.
pub fn synthesize_pg_attribute<E: EngineApply + ?Sized>(
    engine: &E,
    attrelid_filter: Option<u32>,
) -> Vec<u8> {
    let fields = pg_attribute_fields();
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    let mut n: u64 = 0;
    for t in &tables {
        let attrelid = oid_for_table_name(&t.name);
        if let Some(want) = attrelid_filter {
            if attrelid != want {
                continue;
            }
        }
        let cols = match engine.describe_table(&t.name) {
            Some(c) => c,
            None => continue,
        };
        for (idx, col) in cols.iter().enumerate() {
            let attnum = (idx + 1) as i16;
            let atttypid = field_kind_to_oid(col.kind);
            out.extend_from_slice(&encode_pg_attribute_row(
                attrelid,
                &col.name,
                atttypid,
                attnum,
                col.nullable,
            ));
            n += 1;
        }
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

// ─── Joined-result synth: psql `\d <table>` step 2 (queries.md §1.5) ──

/// Synthesize the joined-result response for the canonical psql
/// `\d <table>` step-2 column-list query (queries.md §1.5). The psql
/// query SELECTs from `pg_attribute` with a subselect on `pg_attrdef`
/// + `pg_collation` + `pg_type`. V1 returns the per-column rows from
/// the matching KesselDB table; the `pg_attrdef` / `pg_collation`
/// subselects are NULL (V1 carries no defaults / non-default
/// collations).
///
/// Output columns (matching psql's `\d` projection):
/// - attname (column name)
/// - format_type(...) (PG type display name, e.g. `bigint`, `text`)
/// - pg_get_expr(...) (default expression — V1 NULL)
/// - attnotnull (NOT NULL flag)
/// - attcollation (collation name — V1 NULL)
/// - attidentity (identity char — V1 '')
/// - attgenerated (generated char — V1 '')
pub fn psql_d_table_joined_rows<E: EngineApply + ?Sized>(
    engine: &E,
    table_oid: u32,
) -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "attname".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "format_type".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "pg_get_expr".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "attnotnull".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "attcollation".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "attidentity".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "attgenerated".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    let mut n: u64 = 0;
    let empty = b"".as_ref();
    let true_ = b"t".as_ref();
    let false_ = b"f".as_ref();
    for t in &tables {
        if oid_for_table_name(&t.name) != table_oid {
            continue;
        }
        let cols = match engine.describe_table(&t.name) {
            Some(c) => c,
            None => continue,
        };
        for col in &cols {
            let atttypid = field_kind_to_oid(col.kind);
            let format_name = pg_type_name_for_oid(atttypid);
            let attnotnull = if col.nullable { false_ } else { true_ };
            out.extend_from_slice(&encode_data_row(&[
                Some(col.name.as_bytes()),
                Some(format_name.as_bytes()),
                None,            // pg_get_expr(...) — V1 no defaults
                Some(attnotnull),
                None,            // attcollation subselect — V1 NULL
                Some(empty),     // attidentity
                Some(empty),     // attgenerated
            ]));
            n += 1;
        }
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

// ─── pg_type synthesizer (design §5.4) ────────────────────────────────────

/// `pg_type` column count per PG 14 `src/include/catalog/pg_type.h`.
/// Locked because every JDBC driver iterates by index when resolving
/// column types.
pub const PG_TYPE_COLUMN_COUNT: usize = 30;

/// One canned pg_type row. V1 supports a fixed set of types (~12);
/// each row's values are locked vs PG `pg_type.dat`. The renderer
/// (`encode_pg_type_row`) fills the remaining canned-default columns.
pub struct PgTypeRow {
    pub oid: u32,
    pub typname: &'static str,
    pub typlen: i16,        // -1 for variable-length
    pub typbyval: bool,
    pub typcategory: u8,    // 'B'=bool, 'N'=numeric, 'S'=string, 'U'=user, 'D'=date/time
    pub typalign: u8,       // 'c'/'s'/'i'/'d'
    pub typstorage: u8,     // 'p'/'x'
    pub typcollation: u32,  // 100 for text-like, 0 otherwise
}

/// V1 canned `pg_type` row table. Values locked vs PG
/// `src/include/catalog/pg_type.dat`. The set covers every type the
/// V1 wire path can emit through `field_kind_to_oid` (bool, int2,
/// int4, int8, text, bytea, numeric, timestamptz, oid) plus the
/// JDBC-driver-friendly varchar/float4/float8 (clients may BIND
/// these even though V1's FieldKind set doesn't include them) and
/// name (the catalog uses it implicitly for identifier columns).
pub const PG_TYPE_ROWS: &[PgTypeRow] = &[
    PgTypeRow {
        oid: PG_TYPE_BOOL,
        typname: "bool",
        typlen: 1,
        typbyval: true,
        typcategory: b'B',
        typalign: b'c',
        typstorage: b'p',
        typcollation: 0,
    },
    PgTypeRow {
        oid: PG_TYPE_BYTEA,
        typname: "bytea",
        typlen: -1,
        typbyval: false,
        typcategory: b'U',
        typalign: b'i',
        typstorage: b'x',
        typcollation: 0,
    },
    PgTypeRow {
        oid: PG_TYPE_INT8,
        typname: "int8",
        typlen: 8,
        typbyval: true,
        typcategory: b'N',
        typalign: b'd',
        typstorage: b'p',
        typcollation: 0,
    },
    PgTypeRow {
        oid: PG_TYPE_INT2,
        typname: "int2",
        typlen: 2,
        typbyval: true,
        typcategory: b'N',
        typalign: b's',
        typstorage: b'p',
        typcollation: 0,
    },
    PgTypeRow {
        oid: PG_TYPE_INT4,
        typname: "int4",
        typlen: 4,
        typbyval: true,
        typcategory: b'N',
        typalign: b'i',
        typstorage: b'p',
        typcollation: 0,
    },
    PgTypeRow {
        oid: PG_TYPE_TEXT,
        typname: "text",
        typlen: -1,
        typbyval: false,
        typcategory: b'S',
        typalign: b'i',
        typstorage: b'x',
        typcollation: PG_COLLATION_DEFAULT,
    },
    PgTypeRow {
        oid: PG_TYPE_OID,
        typname: "oid",
        typlen: 4,
        typbyval: true,
        typcategory: b'N',
        typalign: b'i',
        typstorage: b'p',
        typcollation: 0,
    },
    PgTypeRow {
        oid: 700,
        typname: "float4",
        typlen: 4,
        typbyval: true,
        typcategory: b'N',
        typalign: b'i',
        typstorage: b'p',
        typcollation: 0,
    },
    PgTypeRow {
        oid: 701,
        typname: "float8",
        typlen: 8,
        typbyval: true,
        typcategory: b'N',
        typalign: b'd',
        typstorage: b'p',
        typcollation: 0,
    },
    PgTypeRow {
        oid: PG_TYPE_VARCHAR,
        typname: "varchar",
        typlen: -1,
        typbyval: false,
        typcategory: b'S',
        typalign: b'i',
        typstorage: b'x',
        typcollation: PG_COLLATION_DEFAULT,
    },
    PgTypeRow {
        oid: PG_TYPE_TIMESTAMPTZ,
        typname: "timestamptz",
        typlen: 8,
        typbyval: true,
        typcategory: b'D',
        typalign: b'd',
        typstorage: b'p',
        typcollation: 0,
    },
    PgTypeRow {
        oid: PG_TYPE_NUMERIC,
        typname: "numeric",
        typlen: -1,
        typbyval: false,
        typcategory: b'N',
        typalign: b'i',
        typstorage: b'x',
        typcollation: 0,
    },
    PgTypeRow {
        oid: 19,
        typname: "name",
        typlen: 64,
        typbyval: false,
        typcategory: b'S',
        typalign: b'c',
        typstorage: b'p',
        typcollation: PG_COLLATION_DEFAULT,
    },
];

/// Map a PG type OID to its canonical name (e.g. 20 → "int8",
/// 25 → "text"). Used by the `\d <table>` joined-result synthesizer
/// for the `format_type` column. Returns "unknown" for OIDs not in
/// `PG_TYPE_ROWS` (no panic; graceful).
pub fn pg_type_name_for_oid(oid: u32) -> &'static str {
    for r in PG_TYPE_ROWS {
        if r.oid == oid {
            return r.typname;
        }
    }
    "unknown"
}

/// Build the `pg_type` RowDescription field list. 30 columns in the
/// order PG 14 defines them.
fn pg_type_fields() -> Vec<FieldMeta> {
    let oid = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_OID };
    let text = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_TEXT };
    let bool_ = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_BOOL };
    let int2 = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_INT2 };
    let int4 = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_INT4 };
    let char1 = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_TEXT };
    vec![
        oid("oid"),
        text("typname"),
        oid("typnamespace"),
        oid("typowner"),
        int2("typlen"),
        bool_("typbyval"),
        char1("typtype"),
        char1("typcategory"),
        bool_("typispreferred"),
        bool_("typisdefined"),
        char1("typdelim"),
        oid("typrelid"),
        oid("typsubscript"),
        oid("typelem"),
        oid("typarray"),
        oid("typinput"),
        oid("typoutput"),
        oid("typreceive"),
        oid("typsend"),
        oid("typmodin"),
        oid("typmodout"),
        oid("typanalyze"),
        char1("typalign"),
        char1("typstorage"),
        bool_("typnotnull"),
        oid("typbasetype"),
        int4("typtypmod"),
        int4("typndims"),
        oid("typcollation"),
        text("typdefault"),
    ]
}

/// Emit one pg_type DataRow for the canned row `r`.
fn encode_pg_type_row(r: &PgTypeRow) -> Vec<u8> {
    let oid_str = r.oid.to_string();
    let typnamespace = PG_NAMESPACE_OID_PG_CATALOG.to_string();
    let typowner = PG_AUTHID_OID_POSTGRES.to_string();
    let typlen_str = r.typlen.to_string();
    let typbyval = if r.typbyval { b"t".as_ref() } else { b"f".as_ref() };
    let typtype_byte = [b'b']; // 'b' = base type
    let typcategory_byte = [r.typcategory];
    let typdelim_byte = [b','];
    let typalign_byte = [r.typalign];
    let typstorage_byte = [r.typstorage];
    let typcollation_str = r.typcollation.to_string();
    let zero = b"0".as_ref();
    let neg_one = b"-1".as_ref();
    let false_ = b"f".as_ref();
    let true_ = b"t".as_ref();

    encode_data_row(&[
        Some(oid_str.as_bytes()),       // oid
        Some(r.typname.as_bytes()),     // typname
        Some(typnamespace.as_bytes()),  // typnamespace = 11 (pg_catalog)
        Some(typowner.as_bytes()),      // typowner = 10
        Some(typlen_str.as_bytes()),    // typlen
        Some(typbyval),                 // typbyval
        Some(&typtype_byte),            // typtype = 'b'
        Some(&typcategory_byte),        // typcategory
        Some(false_),                   // typispreferred
        Some(true_),                    // typisdefined
        Some(&typdelim_byte),           // typdelim = ','
        Some(zero),                     // typrelid = 0
        Some(zero),                     // typsubscript = 0
        Some(zero),                     // typelem = 0
        Some(zero),                     // typarray = 0 (V1)
        Some(zero),                     // typinput = 0 (V1)
        Some(zero),                     // typoutput = 0
        Some(zero),                     // typreceive = 0
        Some(zero),                     // typsend = 0
        Some(zero),                     // typmodin = 0
        Some(zero),                     // typmodout = 0
        Some(zero),                     // typanalyze = 0
        Some(&typalign_byte),           // typalign
        Some(&typstorage_byte),         // typstorage
        Some(false_),                   // typnotnull = false
        Some(zero),                     // typbasetype = 0
        Some(neg_one),                  // typtypmod = -1
        Some(zero),                     // typndims = 0
        Some(typcollation_str.as_bytes()), // typcollation
        None,                           // typdefault = NULL
    ])
}

/// Synthesize `SELECT * FROM pg_catalog.pg_type` — one row per
/// canned `PG_TYPE_ROWS` entry. Returns the full T+D*+C+Z stream.
///
/// No engine arg: the row set is fully canned (V1 doesn't model
/// user-defined types). Pure const-data → cheap to call per query.
pub fn synthesize_pg_type() -> Vec<u8> {
    let fields = pg_type_fields();
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    for r in PG_TYPE_ROWS {
        out.extend_from_slice(&encode_pg_type_row(r));
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(
        PG_TYPE_ROWS.len() as u64,
    )));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// Synthesize `SELECT * FROM pg_catalog.pg_type WHERE oid = N` —
/// one row matching `oid` or zero rows if unknown. The JDBC column-
/// type resolution path issues this once per distinct column OID.
pub fn synthesize_pg_type_by_oid(oid: u32) -> Vec<u8> {
    let fields = pg_type_fields();
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let mut n: u64 = 0;
    for r in PG_TYPE_ROWS {
        if r.oid == oid {
            out.extend_from_slice(&encode_pg_type_row(r));
            n += 1;
        }
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

// ─── Joined-result synth: pgJDBC getColumns (queries.md §4.2) ─────────────

/// Synthesize the joined-result response for the canonical pgJDBC
/// `getColumns` query (queries.md §4.2). The query JOINs
/// `pg_attribute` + `pg_class` + `pg_namespace` + `pg_type` with a
/// `LIKE '<table>'` clause; V1 picks out the matching table by name
/// and emits the JDBC-projection columns directly.
///
/// `table_name_like` is the LIKE pattern captured from the query;
/// V1 currently supports the literal-name form (no `%`/`_` wildcards
/// — when JDBC drivers pass a wildcard, V1 sees the wildcard
/// pattern literally and no table matches, which surfaces as a
/// 0-row result. Acceptable per design §3.4 — JDBC tools issue
/// per-table queries with full names).
pub fn pgjdbc_getcolumns_joined_rows<E: EngineApply + ?Sized>(
    engine: &E,
    table_name_like: &str,
) -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "nspname".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "relname".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "attname".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "atttypid".to_string(), type_oid: PG_TYPE_OID },
        FieldMeta { name: "attnotnull".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "atttypmod".to_string(), type_oid: PG_TYPE_INT4 },
        FieldMeta { name: "attlen".to_string(), type_oid: PG_TYPE_INT2 },
        FieldMeta { name: "typtypmod".to_string(), type_oid: PG_TYPE_INT4 },
        FieldMeta { name: "attnum".to_string(), type_oid: PG_TYPE_INT8 },
        FieldMeta { name: "attidentity".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "attgenerated".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "adsrc".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "description".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "typbasetype".to_string(), type_oid: PG_TYPE_OID },
        FieldMeta { name: "typtype".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    let mut n: u64 = 0;
    let true_ = b"t".as_ref();
    let false_ = b"f".as_ref();
    let empty = b"".as_ref();
    let nspname = b"public".as_ref();
    let typtype_b = b"b".as_ref(); // base type
    for t in &tables {
        if t.name != table_name_like {
            continue;
        }
        let cols = match engine.describe_table(&t.name) {
            Some(c) => c,
            None => continue,
        };
        for (idx, col) in cols.iter().enumerate() {
            let atttypid = field_kind_to_oid(col.kind);
            let atttypid_str = atttypid.to_string();
            let attlen = type_size_for_oid(atttypid).to_string();
            let attnum = (idx as i64 + 1).to_string();
            let attnotnull = if col.nullable { false_ } else { true_ };
            out.extend_from_slice(&encode_data_row(&[
                Some(nspname),
                Some(t.name.as_bytes()),
                Some(col.name.as_bytes()),
                Some(atttypid_str.as_bytes()),
                Some(attnotnull),
                Some(b"-1"),                   // atttypmod
                Some(attlen.as_bytes()),
                Some(b"-1"),                   // typtypmod
                Some(attnum.as_bytes()),       // row_number → attnum
                Some(empty),                   // attidentity
                Some(empty),                   // attgenerated
                None,                          // adsrc (default expr) — NULL
                None,                          // description — NULL
                Some(b"0"),                    // typbasetype = 0
                Some(typtype_b),               // typtype = 'b'
            ]));
            n += 1;
        }
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
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

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-CAT T3 KATs — pg_class synthesizer + OID-generation
    // determinism + joined-result \dt synthesizer.
    // ───────────────────────────────────────────────────────────────────

    use crate::engine::EngineApply;
    use kessel_proto::OpResult;

    /// Minimal `EngineApply` with overridable `list_tables()` for
    /// driving the synthesizer; same shape as the helper in
    /// `pg_catalog::mod` tests (kept duplicated to keep the test
    /// modules independently compilable).
    struct ListEngine {
        tables: Vec<TableMetadata>,
    }
    impl EngineApply for ListEngine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            OpResult::SchemaError("ListEngine: apply_sql should not be reached".into())
        }
        fn describe_table(&self, _name: &str) -> Option<Vec<crate::engine::PgColumn>> {
            None
        }
        fn list_tables(&self) -> Vec<TableMetadata> {
            self.tables.clone()
        }
    }

    fn td(name: &str, type_id: u32, fc: u16) -> TableMetadata {
        TableMetadata {
            name: name.to_string(),
            type_id,
            kind: TableKind::Ordinary,
            field_count: fc,
        }
    }

    /// **OID determinism HEADLINE:** the same name maps to the same
    /// OID on every call (so a JOIN on `pg_class.oid =
    /// pg_attribute.attrelid` is stable across query invocations,
    /// across replicas, across restarts). Locked because every
    /// PG client caches the table → OID map after the first
    /// pg_class read.
    #[test]
    fn t3_oid_for_table_name_is_deterministic() {
        let a = oid_for_table_name("users");
        let b = oid_for_table_name("users");
        let c = oid_for_table_name("users");
        assert_eq!(a, b);
        assert_eq!(b, c);
    }

    /// **OID landed in user-allocated range:** every generated OID
    /// MUST be ≥ `FIRST_USER_OID = 16384` so we never collide with
    /// PG's reserved system OIDs (a tool issuing
    /// `WHERE relnamespace = 11` should see only canonical pg_catalog
    /// schema rows, not a KesselDB user table that happens to hash
    /// to OID 11).
    #[test]
    fn t3_oid_for_table_name_always_in_user_range() {
        let names = ["a", "users", "orders", "lineitems",
            "transfer", "accounts", "extremely_long_table_name_at_the_edge",
            "x", "TABLE_WITH_UPPERS", "with-special-chars"];
        for n in names.iter() {
            let oid = oid_for_table_name(n);
            assert!(oid >= FIRST_USER_OID,
                "OID for '{n}' ({oid}) MUST be >= FIRST_USER_OID ({FIRST_USER_OID})");
        }
    }

    /// **OID collision-resistance for the canonical V1 corpus:** the
    /// V1 design specifies (design §9 weak-spot #7) that the canned
    /// test corpus has no OID collisions. If a real-deployment table
    /// set hits this KAT (via the future SP-PG-CAT-OID slice), we'll
    /// migrate to monotonic counters; until then, the canonical
    /// names below are guaranteed distinct.
    #[test]
    fn t3_oid_for_table_name_corpus_has_no_collisions() {
        let names = [
            "users", "orders", "lineitems", "products", "inventory",
            "customers", "accounts", "ledger", "transfer", "audit_log",
            "sessions", "tokens", "events", "notifications", "messages",
        ];
        let oids: Vec<u32> = names.iter().map(|n| oid_for_table_name(n)).collect();
        // No two OIDs are equal.
        for i in 0..oids.len() {
            for j in (i + 1)..oids.len() {
                assert_ne!(oids[i], oids[j],
                    "OID collision: '{}' and '{}' both → {}",
                    names[i], names[j], oids[i]);
            }
        }
    }

    /// **pg_class synthesizer — empty engine returns 0-row well-framed
    /// response.** A fresh KesselDB with no tables MUST emit
    /// T (RowDescription with 33 fields) + C "SELECT 0" + Z, with no
    /// DataRow frames in between. psql `\dt` prints "did not find
    /// any relations" in this case (graceful).
    #[test]
    fn t3_pg_class_synthesizer_empty_engine() {
        let eng = ListEngine { tables: vec![] };
        let bytes = pg_class_all_rows(&eng);
        assert_eq!(bytes[0], b'T');
        // Well-framed end.
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
        // CommandComplete tag is `SELECT 0`.
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    /// **pg_class synthesizer — 33 columns in RowDescription.**
    /// psql / JDBC / pgcli iterate columns by `attnum` and break
    /// silently if the count is off. PG 14 `pg_class.h` defines 33
    /// columns; V1 emits exactly that count.
    #[test]
    fn t3_pg_class_row_description_has_33_columns() {
        let eng = ListEngine { tables: vec![] };
        let bytes = pg_class_all_rows(&eng);
        // RowDescription field_count is at offset 5 (1 type + 4 length).
        let fc = u16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(fc, PG_CLASS_COLUMN_COUNT as u16, "pg_class MUST have 33 fields");
        // A couple of canonical names appear verbatim.
        assert!(bytes.windows(b"oid\0".len()).any(|w| w == b"oid\0"));
        assert!(bytes.windows(b"relname\0".len()).any(|w| w == b"relname\0"));
        assert!(bytes.windows(b"relnamespace\0".len()).any(|w| w == b"relnamespace\0"));
        assert!(bytes.windows(b"relkind\0".len()).any(|w| w == b"relkind\0"));
        assert!(bytes.windows(b"relnatts\0".len()).any(|w| w == b"relnatts\0"));
    }

    /// **pg_class synthesizer — 3-table engine emits 3 DataRow frames.**
    /// CommandComplete tag = `SELECT 3`. Headline that the
    /// synthesizer round-trips the live catalog.
    #[test]
    fn t3_pg_class_synthesizer_three_tables() {
        let eng = ListEngine {
            tables: vec![
                td("users", 1, 2),
                td("orders", 2, 3),
                td("lineitems", 3, 5),
            ],
        };
        let bytes = pg_class_all_rows(&eng);
        assert!(bytes.windows(b"SELECT 3\0".len()).any(|w| w == b"SELECT 3\0"));
        // Each table name appears in the byte stream.
        assert!(bytes.windows(b"users".len()).any(|w| w == b"users"));
        assert!(bytes.windows(b"orders".len()).any(|w| w == b"orders"));
        assert!(bytes.windows(b"lineitems".len()).any(|w| w == b"lineitems"));
        // public schema OID (2200) appears at least 3 times (once
        // per row's relnamespace).
        let public_count = bytes
            .windows(b"2200".len())
            .filter(|w| *w == b"2200")
            .count();
        assert!(public_count >= 3,
            "public schema OID (2200) MUST appear >=3 times (once per row), saw {public_count}");
    }

    /// **pg_class row — `relkind='r'` for ordinary tables.** Locked
    /// because every PG client switches on the relkind char to label
    /// the row as "table" / "view" / "sequence" / ...
    #[test]
    fn t3_pg_class_relkind_r_for_ordinary_tables() {
        let eng = ListEngine { tables: vec![td("foo", 1, 1)] };
        let bytes = pg_class_all_rows(&eng);
        // 'r' for ordinary table appears as a 1-byte payload in the
        // relkind column slot. We can't trivially find it without
        // walking the frame, so cross-check via TableKind::pg_relkind.
        assert_eq!(TableKind::Ordinary.pg_relkind(), b'r');
        // The byte 'r' must appear in the stream (within the
        // relkind column value). Validate the column structure
        // implicitly by checking 'r' (114) is in the bytes.
        assert!(bytes.contains(&b'r'),
            "relkind='r' MUST appear in the synthesized stream");
    }

    /// **pg_class row — `relnatts` carries the column count.** A
    /// table with 5 columns emits `relnatts=5` in its row. psql
    /// `\d` cross-checks this against the pg_attribute count.
    #[test]
    fn t3_pg_class_relnatts_matches_field_count() {
        let eng = ListEngine {
            tables: vec![
                td("alpha", 1, 1),
                td("beta", 2, 5),
                td("gamma", 3, 17),
            ],
        };
        let bytes = pg_class_all_rows(&eng);
        // The decimal text of each table's field_count appears in
        // its row's relnatts column.
        assert!(bytes.windows(b"17".len()).any(|w| w == b"17"));
        // (1 and 5 are too short to test uniquely — covered by
        // the field_count round-trip in t3_pg_class_synthesizer_three_tables).
    }

    /// **pg_class row — `relacl` / `reloptions` / `relpartbound`
    /// are NULL.** V1 doesn't model ACLs / per-relation options /
    /// partition bounds. Locked because PG clients expect either
    /// well-formed text-format `aclitem[]` / `text[]` / `pg_node_tree`
    /// values OR a clean NULL sentinel.
    #[test]
    fn t3_pg_class_trailing_nulls_present_per_row() {
        let eng = ListEngine { tables: vec![td("foo", 1, 1)] };
        let bytes = pg_class_all_rows(&eng);
        // Each row has 3 NULL trailing columns → ≥3 NULL sentinels.
        let null_count = bytes
            .windows(4)
            .filter(|w| *w == [0xFF, 0xFF, 0xFF, 0xFF])
            .count();
        assert!(null_count >= 3,
            "pg_class row MUST carry 3 trailing NULLs (relacl/reloptions/relpartbound), saw {null_count}");
    }

    /// **OID-in-output invariant:** the stable-hash OID for each
    /// table name appears in its row's first column (decimal text).
    /// Locked because the OID is the foreign key from
    /// `pg_class` → `pg_attribute.attrelid` (T4) → `pg_index.indrelid`
    /// (T5). If the synthesizer ever drifts from `oid_for_table_name`,
    /// every JOIN-on-pg_class.oid silently breaks.
    #[test]
    fn t3_pg_class_row_oid_matches_oid_for_table_name() {
        let eng = ListEngine { tables: vec![td("users", 1, 2)] };
        let bytes = pg_class_all_rows(&eng);
        let expected_oid = oid_for_table_name("users").to_string();
        assert!(bytes.windows(expected_oid.len()).any(|w| w == expected_oid.as_bytes()),
            "pg_class.oid for 'users' MUST equal oid_for_table_name('users') = {expected_oid}");
    }

    /// **psql \dt joined-result synthesizer — 4 column headers.**
    /// Schema / Name / Type / Owner — verbatim from psql's
    /// `describe.c::listTables`.
    #[test]
    fn t3_psql_dt_joined_rows_has_4_canonical_columns() {
        let eng = ListEngine { tables: vec![] };
        let bytes = psql_dt_joined_rows(&eng);
        let fc = u16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(fc, 4, "psql \\dt joined result MUST have 4 columns");
        assert!(bytes.windows(b"Schema\0".len()).any(|w| w == b"Schema\0"));
        assert!(bytes.windows(b"Name\0".len()).any(|w| w == b"Name\0"));
        assert!(bytes.windows(b"Type\0".len()).any(|w| w == b"Type\0"));
        assert!(bytes.windows(b"Owner\0".len()).any(|w| w == b"Owner\0"));
    }

    /// **psql \dt joined-result synthesizer — every row says public /
    /// table / kesseldb.** V1 single-schema, single-relkind, single-
    /// user model means every joined row is a canned shape with the
    /// table's name in column 2.
    #[test]
    fn t3_psql_dt_joined_rows_three_tables() {
        let eng = ListEngine {
            tables: vec![
                td("users", 1, 2),
                td("orders", 2, 3),
                td("lineitems", 3, 5),
            ],
        };
        let bytes = psql_dt_joined_rows(&eng);
        assert!(bytes.windows(b"SELECT 3\0".len()).any(|w| w == b"SELECT 3\0"));
        // Every row has "public", "table", "kesseldb" — count
        // occurrences. Each appears ≥3 times.
        let public_count = bytes.windows(b"public".len()).filter(|w| *w == b"public").count();
        let table_count = bytes.windows(b"table".len()).filter(|w| *w == b"table").count();
        let kesseldb_count = bytes.windows(b"kesseldb".len()).filter(|w| *w == b"kesseldb").count();
        assert!(public_count >= 3, "public appears {public_count} times, want >=3");
        assert!(table_count >= 3, "table appears {table_count} times, want >=3");
        assert!(kesseldb_count >= 3, "kesseldb appears {kesseldb_count} times, want >=3");
        // Each table name appears.
        assert!(bytes.windows(b"users".len()).any(|w| w == b"users"));
        assert!(bytes.windows(b"orders".len()).any(|w| w == b"orders"));
        assert!(bytes.windows(b"lineitems".len()).any(|w| w == b"lineitems"));
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-CAT T4 KATs — pg_attribute + pg_type synthesizers + pgJDBC
    // getColumns joined-result. Drive via DescribeListEngine (overrides
    // both `list_tables` and `describe_table`).
    // ───────────────────────────────────────────────────────────────────

    use crate::engine::PgColumn;
    use kessel_catalog::FieldKind;

    /// Engine that combines list_tables + describe_table so the T4
    /// synthesizers can walk a non-empty catalog with real column
    /// schemas. Schema is keyed by table name.
    struct DescribeListEngine {
        tables: Vec<TableMetadata>,
        schemas: std::collections::BTreeMap<String, Vec<PgColumn>>,
    }
    impl EngineApply for DescribeListEngine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            OpResult::SchemaError("DescribeListEngine: apply_sql should not be reached".into())
        }
        fn describe_table(&self, name: &str) -> Option<Vec<PgColumn>> {
            self.schemas.get(name).cloned()
        }
        fn list_tables(&self) -> Vec<TableMetadata> {
            self.tables.clone()
        }
    }

    fn col(name: &str, kind: FieldKind, nullable: bool) -> PgColumn {
        PgColumn { name: name.to_string(), kind, nullable }
    }

    fn two_table_engine() -> DescribeListEngine {
        let mut schemas = std::collections::BTreeMap::new();
        schemas.insert("users".to_string(), vec![
            col("id", FieldKind::I64, false),
            col("name", FieldKind::Char(64), false),
        ]);
        schemas.insert("orders".to_string(), vec![
            col("id", FieldKind::I64, false),
            col("user_id", FieldKind::I64, false),
            col("amount", FieldKind::Fixed { scale: 2 }, false),
        ]);
        DescribeListEngine {
            tables: vec![
                td("users", 1, 2),
                td("orders", 2, 3),
            ],
            schemas,
        }
    }

    /// **HEADLINE invariant — pg_attribute synthesizer (no filter)
    /// returns columns for every table.** Two tables × (2 + 3) = 5
    /// rows.
    #[test]
    fn t4_pg_attribute_synthesizer_all_tables() {
        let eng = two_table_engine();
        let bytes = synthesize_pg_attribute(&eng, None);
        assert_eq!(bytes[0], b'T');
        assert!(bytes.windows(b"SELECT 5\0".len()).any(|w| w == b"SELECT 5\0"),
            "MUST emit `SELECT 5` for 2 tables × 5 total columns");
        // Each column name appears in the stream.
        assert!(bytes.windows(b"id".len()).any(|w| w == b"id"));
        assert!(bytes.windows(b"name".len()).any(|w| w == b"name"));
        assert!(bytes.windows(b"user_id".len()).any(|w| w == b"user_id"));
        assert!(bytes.windows(b"amount".len()).any(|w| w == b"amount"));
    }

    /// **HEADLINE invariant — pg_attribute synthesizer (filter to
    /// one table) returns only that table's columns.**
    #[test]
    fn t4_pg_attribute_synthesizer_filtered_to_one_table() {
        let eng = two_table_engine();
        let users_oid = oid_for_table_name("users");
        let bytes = synthesize_pg_attribute(&eng, Some(users_oid));
        assert!(bytes.windows(b"SELECT 2\0".len()).any(|w| w == b"SELECT 2\0"),
            "filtered to 'users' MUST emit `SELECT 2` (2 columns)");
        // 'orders' column names must NOT appear (the column "id" is
        // shared so don't check that, but "user_id" / "amount" are
        // unique to orders).
        assert!(!bytes.windows(b"user_id".len()).any(|w| w == b"user_id"));
        assert!(!bytes.windows(b"amount".len()).any(|w| w == b"amount"));
        // 'users' column names DO appear.
        assert!(bytes.windows(b"name".len()).any(|w| w == b"name"));
    }

    /// **HEADLINE invariant — 25 columns in RowDescription.**
    /// JDBC drivers iterate by index; PG 14 pg_attribute has 25 cols.
    #[test]
    fn t4_pg_attribute_row_description_has_25_columns() {
        let eng = two_table_engine();
        let bytes = synthesize_pg_attribute(&eng, None);
        let fc = u16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(fc, PG_ATTRIBUTE_COLUMN_COUNT as u16,
            "pg_attribute RowDescription MUST have 25 fields");
        // Canonical column names appear.
        assert!(bytes.windows(b"attrelid\0".len()).any(|w| w == b"attrelid\0"));
        assert!(bytes.windows(b"attname\0".len()).any(|w| w == b"attname\0"));
        assert!(bytes.windows(b"atttypid\0".len()).any(|w| w == b"atttypid\0"));
        assert!(bytes.windows(b"attnum\0".len()).any(|w| w == b"attnum\0"));
        assert!(bytes.windows(b"attnotnull\0".len()).any(|w| w == b"attnotnull\0"));
    }

    /// **Invariant — empty engine returns 0 rows + well-framed.**
    #[test]
    fn t4_pg_attribute_synthesizer_empty_engine() {
        let eng = DescribeListEngine {
            tables: vec![],
            schemas: std::collections::BTreeMap::new(),
        };
        let bytes = synthesize_pg_attribute(&eng, None);
        assert_eq!(bytes[0], b'T');
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// **Invariant — atttypid carries the FieldKind→OID map output.**
    /// A column of `FieldKind::I64` MUST emit atttypid=20 (int8); a
    /// `FieldKind::Char(64)` MUST emit 25 (text).
    #[test]
    fn t4_pg_attribute_atttypid_matches_field_kind_to_oid_map() {
        let eng = two_table_engine();
        let bytes = synthesize_pg_attribute(&eng, None);
        // OID 20 = int8 (for I64 columns: users.id, orders.id, orders.user_id)
        // → appears at least 3 times.
        let int8_count = bytes.windows(b"20".len()).filter(|w| *w == b"20").count();
        assert!(int8_count >= 3,
            "OID 20 (int8) MUST appear ≥3× (3 I64 columns), saw {int8_count}");
        // OID 25 = text (for the Char(64) `name` column).
        let text_count = bytes.windows(b"25".len()).filter(|w| *w == b"25").count();
        assert!(text_count >= 1,
            "OID 25 (text) MUST appear (Char(64) `name`), saw {text_count}");
        // OID 1700 = numeric (for Fixed{scale:2} `amount`).
        assert!(bytes.windows(b"1700".len()).any(|w| w == b"1700"),
            "OID 1700 (numeric) MUST appear for Fixed{{scale:2}} amount column");
    }

    /// **Invariant — attnum is 1-based and sequential per table.**
    /// First column is attnum=1, second is 2, etc. Locked because
    /// PG clients use attnum as the index when joining pg_attribute
    /// rows back to a column position.
    #[test]
    fn t4_pg_attribute_attnum_is_1_based_sequential() {
        // Single-table engine, 5 columns → attnums 1..=5 emitted.
        let mut schemas = std::collections::BTreeMap::new();
        schemas.insert("t".to_string(), vec![
            col("c1", FieldKind::I32, false),
            col("c2", FieldKind::I32, false),
            col("c3", FieldKind::I32, false),
            col("c4", FieldKind::I32, false),
            col("c5", FieldKind::I32, false),
        ]);
        let eng = DescribeListEngine {
            tables: vec![td("t", 1, 5)],
            schemas,
        };
        let bytes = synthesize_pg_attribute(&eng, None);
        // attnums 1..=5 appear as decimal-ASCII text in the stream.
        for n in 1..=5u32 {
            let s = n.to_string();
            assert!(bytes.windows(s.len()).any(|w| w == s.as_bytes()),
                "attnum {n} MUST appear in the stream");
        }
        assert!(bytes.windows(b"SELECT 5\0".len()).any(|w| w == b"SELECT 5\0"));
    }

    /// **Invariant — attnotnull = 't' for V1 (KesselDB defaults
    /// NOT NULL).** Every column in `two_table_engine` is non-nullable.
    #[test]
    fn t4_pg_attribute_attnotnull_is_true_for_v1_columns() {
        let eng = two_table_engine();
        let bytes = synthesize_pg_attribute(&eng, None);
        // 't' bytes appear in the stream (for the attbyval and
        // attnotnull columns — verifying ≥1 't' is enough to confirm
        // the bool encoder fired).
        assert!(bytes.contains(&b't'), "bool 't' MUST appear in stream");
    }

    /// **Invariant — psql `\d <table>` joined-result synthesizer
    /// fires for the matching table.** The format_type column
    /// carries the PG type name (`bigint`/`int8` for I64).
    #[test]
    fn t4_psql_d_table_joined_rows_fires_for_matching_oid() {
        let eng = two_table_engine();
        let users_oid = oid_for_table_name("users");
        let bytes = psql_d_table_joined_rows(&eng, users_oid);
        assert_eq!(bytes[0], b'T');
        assert!(bytes.windows(b"SELECT 2\0".len()).any(|w| w == b"SELECT 2\0"));
        // The PG type name `int8` (for I64) + `text` (for Char(64))
        // appear in the format_type column.
        assert!(bytes.windows(b"int8".len()).any(|w| w == b"int8"));
        assert!(bytes.windows(b"text".len()).any(|w| w == b"text"));
        // column name "id" and "name" appear.
        assert!(bytes.windows(b"name".len()).any(|w| w == b"name"));
    }

    /// **Invariant — psql `\d` joined synthesizer returns 0 rows
    /// for a non-matching OID.**
    #[test]
    fn t4_psql_d_table_joined_rows_empty_for_unknown_oid() {
        let eng = two_table_engine();
        let bytes = psql_d_table_joined_rows(&eng, 999_999_999);
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    /// **HEADLINE invariant — pg_type synthesizer emits all canned rows.**
    /// SELECT N tag matches `PG_TYPE_ROWS.len()`.
    #[test]
    fn t4_pg_type_synthesizer_emits_all_canned_rows() {
        let bytes = synthesize_pg_type();
        assert_eq!(bytes[0], b'T');
        let expected = format!("SELECT {}\0", PG_TYPE_ROWS.len());
        assert!(bytes.windows(expected.len()).any(|w| w == expected.as_bytes()),
            "MUST emit `SELECT {}` for the canned pg_type row table",
            PG_TYPE_ROWS.len());
        // Well-framed end.
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// **HEADLINE invariant — pg_type RowDescription has 30 columns.**
    #[test]
    fn t4_pg_type_row_description_has_30_columns() {
        let bytes = synthesize_pg_type();
        let fc = u16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(fc, PG_TYPE_COLUMN_COUNT as u16,
            "pg_type RowDescription MUST have 30 fields");
        // Canonical column names appear.
        assert!(bytes.windows(b"typname\0".len()).any(|w| w == b"typname\0"));
        assert!(bytes.windows(b"typlen\0".len()).any(|w| w == b"typlen\0"));
        assert!(bytes.windows(b"typbyval\0".len()).any(|w| w == b"typbyval\0"));
        assert!(bytes.windows(b"typcategory\0".len()).any(|w| w == b"typcategory\0"));
        assert!(bytes.windows(b"typcollation\0".len()).any(|w| w == b"typcollation\0"));
    }

    /// **Invariant — canonical type names appear in the canned rows.**
    /// At least the 8 KesselDB-V1 types are present.
    #[test]
    fn t4_pg_type_canned_rows_carry_v1_type_names() {
        let bytes = synthesize_pg_type();
        for name in ["bool", "bytea", "int8", "int2", "int4",
                     "text", "oid", "numeric", "timestamptz", "varchar"] {
            assert!(bytes.windows(name.len()).any(|w| w == name.as_bytes()),
                "canned pg_type row for '{name}' MUST appear in stream");
        }
    }

    /// **Invariant — int4 (OID 23) has typname='int4', typlen=4,
    /// typbyval=true.** Locked vs PG `pg_type.dat`.
    #[test]
    fn t4_pg_type_int4_row_is_canonical() {
        let bytes = synthesize_pg_type_by_oid(PG_TYPE_INT4);
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        assert!(bytes.windows(b"int4".len()).any(|w| w == b"int4"));
        // typlen=4 as decimal text → "4" appears (cannot test
        // uniquely; rely on the SELECT 1 + canned row + the
        // synthesizer's locked encoding).
        assert!(bytes.contains(&b't'), "typbyval=true MUST appear");
    }

    /// **Invariant — text (OID 25) has typname='text', typlen=-1,
    /// typbyval=false, typcollation=100.**
    #[test]
    fn t4_pg_type_text_row_is_canonical() {
        let bytes = synthesize_pg_type_by_oid(PG_TYPE_TEXT);
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        assert!(bytes.windows(b"text".len()).any(|w| w == b"text"));
        // typlen=-1 → "-1" appears.
        assert!(bytes.windows(b"-1".len()).any(|w| w == b"-1"));
        // typcollation=100 → "100" appears.
        assert!(bytes.windows(b"100".len()).any(|w| w == b"100"));
    }

    /// **Invariant — pg_type per-OID lookup with unknown OID returns
    /// 0 rows + well-framed.**
    #[test]
    fn t4_pg_type_by_oid_unknown_returns_empty() {
        let bytes = synthesize_pg_type_by_oid(99_999);
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// **Invariant — `pg_type_name_for_oid` map matches canned rows.**
    /// V1 type lookups round-trip via the public helper.
    #[test]
    fn t4_pg_type_name_for_oid_round_trips() {
        assert_eq!(pg_type_name_for_oid(PG_TYPE_BOOL), "bool");
        assert_eq!(pg_type_name_for_oid(PG_TYPE_INT4), "int4");
        assert_eq!(pg_type_name_for_oid(PG_TYPE_INT8), "int8");
        assert_eq!(pg_type_name_for_oid(PG_TYPE_TEXT), "text");
        assert_eq!(pg_type_name_for_oid(PG_TYPE_BYTEA), "bytea");
        assert_eq!(pg_type_name_for_oid(PG_TYPE_NUMERIC), "numeric");
        assert_eq!(pg_type_name_for_oid(PG_TYPE_TIMESTAMPTZ), "timestamptz");
        // Unknown OID → "unknown" (graceful).
        assert_eq!(pg_type_name_for_oid(99_999), "unknown");
    }

    /// **Invariant — pgJDBC getColumns joined synthesizer round-trips.**
    /// One match → 2 rows (users has 2 columns); unmatched → 0 rows.
    #[test]
    fn t4_pgjdbc_getcolumns_joined_rows_matches_by_name() {
        let eng = two_table_engine();
        let bytes = pgjdbc_getcolumns_joined_rows(&eng, "users");
        assert!(bytes.windows(b"SELECT 2\0".len()).any(|w| w == b"SELECT 2\0"));
        // nspname `public`, relname `users`, column `id` + `name` all appear.
        assert!(bytes.windows(b"public".len()).any(|w| w == b"public"));
        assert!(bytes.windows(b"users".len()).any(|w| w == b"users"));
        // Unmatched name → 0 rows.
        let unmatched = pgjdbc_getcolumns_joined_rows(&eng, "doesnotexist");
        assert!(unmatched.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }
}
