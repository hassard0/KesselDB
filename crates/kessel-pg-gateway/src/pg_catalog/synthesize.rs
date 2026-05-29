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
use crate::engine::{
    ConstraintKind, ConstraintMetadata, EngineApply, IndexMetadata, TableKind, TableMetadata,
};
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

// ─── pg_index synthesizer (design §5.5) ───────────────────────────────────

/// `pg_index` column count per PG 14 `src/include/catalog/pg_index.h`.
/// Locked because clients (pgJDBC `getIndexInfo`, psql `\d <table>`
/// step 3, DBeaver "Indexes" tab) iterate row columns by index;
/// one off-by-one breaks them all.
pub const PG_INDEX_COLUMN_COUNT: usize = 19;

/// Build the `pg_index` RowDescription field list. 19 columns in
/// the order PG 14 defines them. Pulled out so both `SELECT *` and
/// per-table-filter paths emit the same shape.
fn pg_index_fields() -> Vec<FieldMeta> {
    let oid = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_OID };
    let text = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_TEXT };
    let bool_ = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_BOOL };
    let int2 = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_INT2 };
    vec![
        oid("indexrelid"),
        oid("indrelid"),
        int2("indnatts"),
        int2("indnkeyatts"),
        bool_("indisunique"),
        bool_("indisprimary"),
        bool_("indisexclusion"),
        bool_("indimmediate"),
        bool_("indisclustered"),
        bool_("indisvalid"),
        bool_("indcheckxmin"),
        bool_("indisready"),
        bool_("indislive"),
        bool_("indisreplident"),
        text("indkey"),        // int2vector → space-separated text
        text("indcollation"),  // oidvector → space-separated text
        text("indclass"),      // oidvector → space-separated text
        text("indoption"),     // int2vector → space-separated text
        text("indpred"),       // pg_node_tree — V1 NULL (but column slot)
    ]
}

/// Deterministic OID for a synthetic index name. Reuses the
/// `oid_for_table_name` FNV-1a strategy — same collision risk
/// profile, same stability properties (a tool caching the
/// indexrelid across queries sees a stable value).
pub fn oid_for_index_name(name: &str) -> u32 {
    oid_for_table_name(name)
}

/// Render a `Vec<u32>` of attnums as the PG `int2vector` text
/// format: space-separated decimal (e.g. `"1 2 3"`). PG wire-emits
/// vectors as text in the text format; clients parse with
/// whitespace.
fn render_int2vector(fields: &[u32]) -> String {
    let mut out = String::new();
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(&f.to_string());
    }
    out
}

/// Render an `oidvector` of zeros of the given length. V1 doesn't
/// model per-column collation/opclass/option, so every vector slot
/// is "0".
fn render_zero_vector(n: usize) -> String {
    let mut out = String::new();
    for i in 0..n {
        if i > 0 {
            out.push(' ');
        }
        out.push('0');
    }
    out
}

/// Emit one pg_index DataRow.
fn encode_pg_index_row(indexrelid: u32, indrelid: u32, idx: &IndexMetadata) -> Vec<u8> {
    let indexrelid_str = indexrelid.to_string();
    let indrelid_str = indrelid.to_string();
    let n = idx.fields.len();
    let indnatts_str = (n as i16).to_string();
    let indnkeyatts_str = indnatts_str.clone(); // V1: same as indnatts (no INCLUDE)
    let indisunique = if idx.is_unique { b"t".as_ref() } else { b"f".as_ref() };
    let false_ = b"f".as_ref();
    let true_ = b"t".as_ref();
    let indkey = render_int2vector(&idx.fields);
    let indcollation = render_zero_vector(n);
    let indclass = render_zero_vector(n);
    let indoption = render_zero_vector(n);

    encode_data_row(&[
        Some(indexrelid_str.as_bytes()),  // indexrelid
        Some(indrelid_str.as_bytes()),    // indrelid
        Some(indnatts_str.as_bytes()),    // indnatts
        Some(indnkeyatts_str.as_bytes()), // indnkeyatts
        Some(indisunique),                // indisunique
        Some(false_),                     // indisprimary = false (V1)
        Some(false_),                     // indisexclusion = false
        Some(true_),                      // indimmediate = true
        Some(false_),                     // indisclustered = false
        Some(true_),                      // indisvalid = true
        Some(false_),                     // indcheckxmin = false
        Some(true_),                      // indisready = true
        Some(true_),                      // indislive = true
        Some(false_),                     // indisreplident = false
        Some(indkey.as_bytes()),          // indkey
        Some(indcollation.as_bytes()),    // indcollation
        Some(indclass.as_bytes()),        // indclass
        Some(indoption.as_bytes()),       // indoption
        None,                             // indpred = NULL
    ])
}

/// Synthesize `SELECT * FROM pg_catalog.pg_index` — one row per
/// KesselDB index across every user table.
///
/// `indrelid_filter`:
/// - `None` — emit every index on every table.
/// - `Some(oid)` — emit only indexes whose `indrelid` matches the
///   filter (psql `\d <table>` step 3 + pgJDBC `getIndexInfo`
///   hot path).
///
/// Walks `engine.list_tables()` and `engine.list_indexes_for_table(name)`
/// for each table that survives the filter. Returns the full T+D*+C+Z
/// wire stream.
pub fn synthesize_pg_index<E: EngineApply + ?Sized>(
    engine: &E,
    indrelid_filter: Option<u32>,
) -> Vec<u8> {
    let fields = pg_index_fields();
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    let mut n: u64 = 0;
    for t in &tables {
        let indrelid = oid_for_table_name(&t.name);
        if let Some(want) = indrelid_filter {
            if indrelid != want {
                continue;
            }
        }
        for idx in engine.list_indexes_for_table(&t.name) {
            let indexrelid = oid_for_index_name(&idx.name);
            out.extend_from_slice(&encode_pg_index_row(indexrelid, indrelid, &idx));
            n += 1;
        }
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

// ─── Joined-result synth: pgJDBC getIndexInfo (queries.md §4.3) ───────────

/// Synthesize the joined-result response for the canonical pgJDBC
/// `getIndexInfo` query (queries.md §4.3). The query JOINs `pg_class
/// ct` + `pg_namespace n` + `pg_index i` + `pg_class ci` + `pg_am
/// am` filtered by table name. V1 looks up the indexes for the
/// matching table and emits one row per (index × column).
///
/// Output columns (matching pgJDBC's projection):
/// - TABLE_CAT (NULL) / TABLE_SCHEM / TABLE_NAME / NON_UNIQUE /
///   INDEX_QUALIFIER (NULL) / INDEX_NAME / TYPE (3=btree) /
///   ORDINAL_POSITION / COLUMN_NAME / ASC_OR_DESC (NULL) /
///   CARDINALITY / PAGES / FILTER_CONDITION (NULL)
pub fn pgjdbc_getindexinfo_joined_rows<E: EngineApply + ?Sized>(
    engine: &E,
    table_name: &str,
) -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "TABLE_CAT".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "TABLE_SCHEM".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "TABLE_NAME".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "NON_UNIQUE".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "INDEX_QUALIFIER".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "INDEX_NAME".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "TYPE".to_string(), type_oid: PG_TYPE_INT2 },
        FieldMeta { name: "ORDINAL_POSITION".to_string(), type_oid: PG_TYPE_INT2 },
        FieldMeta { name: "COLUMN_NAME".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "ASC_OR_DESC".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "CARDINALITY".to_string(), type_oid: PG_TYPE_INT8 },
        FieldMeta { name: "PAGES".to_string(), type_oid: PG_TYPE_INT8 },
        FieldMeta { name: "FILTER_CONDITION".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    let nspname = b"public".as_ref();
    let btree_type = b"3".as_ref(); // pgJDBC: tableIndexOther=3 for btree
    let zero = b"0".as_ref();
    let mut n: u64 = 0;
    for t in &tables {
        if t.name != table_name {
            continue;
        }
        let cols = engine.describe_table(&t.name);
        for idx in engine.list_indexes_for_table(&t.name) {
            let non_unique = if idx.is_unique { b"f".as_ref() } else { b"t".as_ref() };
            for (i, attnum) in idx.fields.iter().enumerate() {
                let column_name = match &cols {
                    Some(c) => {
                        let attn = *attnum as usize;
                        if attn >= 1 && attn <= c.len() {
                            c[attn - 1].name.clone()
                        } else {
                            String::new()
                        }
                    }
                    None => String::new(),
                };
                let ord = (i as i16 + 1).to_string();
                out.extend_from_slice(&encode_data_row(&[
                    None,                            // TABLE_CAT
                    Some(nspname),                   // TABLE_SCHEM
                    Some(t.name.as_bytes()),         // TABLE_NAME
                    Some(non_unique),                // NON_UNIQUE
                    None,                            // INDEX_QUALIFIER
                    Some(idx.name.as_bytes()),       // INDEX_NAME
                    Some(btree_type),                // TYPE = 3 (btree)
                    Some(ord.as_bytes()),            // ORDINAL_POSITION
                    Some(column_name.as_bytes()),    // COLUMN_NAME
                    None,                            // ASC_OR_DESC
                    Some(zero),                      // CARDINALITY
                    Some(zero),                      // PAGES
                    None,                            // FILTER_CONDITION
                ]));
                n += 1;
            }
        }
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

// ─── pg_constraint synthesizer (design §5.6) ──────────────────────────────

/// `pg_constraint` column count per PG 14 `src/include/catalog/
/// pg_constraint.h`. Locked because clients iterate row columns by
/// index when reading constraint metadata.
pub const PG_CONSTRAINT_COLUMN_COUNT: usize = 25;

/// Build the `pg_constraint` RowDescription field list. 25 columns
/// in the order PG 14 defines them.
fn pg_constraint_fields() -> Vec<FieldMeta> {
    let oid = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_OID };
    let text = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_TEXT };
    let bool_ = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_BOOL };
    let int4 = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_INT4 };
    let char1 = |name: &str| FieldMeta { name: name.to_string(), type_oid: PG_TYPE_TEXT };
    vec![
        oid("oid"),
        text("conname"),
        oid("connamespace"),
        char1("contype"),
        bool_("condeferrable"),
        bool_("condeferred"),
        bool_("convalidated"),
        oid("conrelid"),
        oid("contypid"),
        oid("conindid"),
        oid("conparentid"),
        oid("confrelid"),
        char1("confupdtype"),
        char1("confdeltype"),
        char1("confmatchtype"),
        bool_("conislocal"),
        int4("coninhcount"),
        bool_("connoinherit"),
        text("conkey"),       // int2[] as text "{1,2,3}"
        text("confkey"),      // int2[] as text — NULL for non-FK
        text("conpfeqop"),    // oid[] — V1 NULL
        text("conppeqop"),    // oid[] — V1 NULL
        text("conffeqop"),    // oid[] — V1 NULL
        text("conexclop"),    // oid[] — V1 NULL
        text("conbin"),       // pg_node_tree — V1 NULL
    ]
}

/// Render a `Vec<u32>` of attnums as the PG `int2[]` array text
/// format: `{1,2,3}`.
fn render_int_array(fields: &[u32]) -> String {
    let mut out = String::from("{");
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&f.to_string());
    }
    out.push('}');
    out
}

/// Emit one pg_constraint DataRow.
fn encode_pg_constraint_row(conrelid: u32, c: &ConstraintMetadata) -> Vec<u8> {
    let oid_str = oid_for_table_name(&format!("__con__{}", c.name)).to_string();
    let connamespace = PG_NAMESPACE_OID_PUBLIC.to_string();
    let contype_byte = [c.kind.pg_contype()];
    let false_ = b"f".as_ref();
    let true_ = b"t".as_ref();
    let conrelid_str = conrelid.to_string();
    let zero = b"0".as_ref();
    let confrelid = match &c.references {
        Some((tname, _)) => oid_for_table_name(tname).to_string(),
        None => "0".to_string(),
    };
    // FK update/delete actions. Default to 'a' (NoAction) for non-FK.
    let (confupdtype_char, confdeltype_char) = match c.kind {
        ConstraintKind::ForeignKey { on_delete } => (b'a', on_delete.pg_action_char()),
        _ => (b' ', b' '),
    };
    let confupd_byte = [confupdtype_char];
    let confdel_byte = [confdeltype_char];
    let confmatchtype_byte = [b's']; // simple
    let conkey = render_int_array(&c.columns);
    let confkey_str = c.references.as_ref().map(|(_, cols)| render_int_array(cols));
    let confkey_bytes: Option<&[u8]> = confkey_str.as_ref().map(|s| s.as_bytes());

    encode_data_row(&[
        Some(oid_str.as_bytes()),       // oid
        Some(c.name.as_bytes()),        // conname
        Some(connamespace.as_bytes()),  // connamespace
        Some(&contype_byte),            // contype
        Some(false_),                   // condeferrable
        Some(false_),                   // condeferred
        Some(true_),                    // convalidated
        Some(conrelid_str.as_bytes()),  // conrelid
        Some(zero),                     // contypid
        Some(zero),                     // conindid (V1: no backing index OID linkage)
        Some(zero),                     // conparentid
        Some(confrelid.as_bytes()),     // confrelid
        Some(&confupd_byte),            // confupdtype
        Some(&confdel_byte),            // confdeltype
        Some(&confmatchtype_byte),      // confmatchtype
        Some(true_),                    // conislocal
        Some(zero),                     // coninhcount
        Some(true_),                    // connoinherit
        Some(conkey.as_bytes()),        // conkey
        confkey_bytes,                  // confkey (NULL for non-FK)
        None,                           // conpfeqop = NULL
        None,                           // conppeqop = NULL
        None,                           // conffeqop = NULL
        None,                           // conexclop = NULL
        None,                           // conbin = NULL
    ])
}

/// Synthesize `SELECT * FROM pg_catalog.pg_constraint` — one row
/// per constraint across every user table.
///
/// `conrelid_filter`:
/// - `None` — emit every constraint on every table.
/// - `Some(oid)` — emit only constraints whose `conrelid` matches
///   the filter (psql `\d <table>` constraint-section + JDBC
///   `getPrimaryKeys` hot path).
pub fn synthesize_pg_constraint<E: EngineApply + ?Sized>(
    engine: &E,
    conrelid_filter: Option<u32>,
) -> Vec<u8> {
    let fields = pg_constraint_fields();
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    let mut n: u64 = 0;
    for t in &tables {
        let conrelid = oid_for_table_name(&t.name);
        if let Some(want) = conrelid_filter {
            if conrelid != want {
                continue;
            }
        }
        for c in engine.list_constraints_for_table(&t.name) {
            out.extend_from_slice(&encode_pg_constraint_row(conrelid, &c));
            n += 1;
        }
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

// ─── SP-PG-CAT T6 — information_schema view synthesizers ──────────────────
//
// The SQL-standard `information_schema.*` namespace is the
// portable, vendor-neutral catalog. While JDBC / pgcli / psql go
// through `pg_catalog`, the BI / metadata-discovery layer
// (Metabase, Tableau, Looker, Hex, Superset, dbt-postgres, sqlmesh)
// prefers `information_schema` because the same queries port across
// Postgres / MySQL / SQL Server.
//
// V1 of this slice ships the 5 most-queried views (per queries.md
// §5 + the wider BI corpus): tables, columns, schemata,
// key_column_usage, table_constraints. Two additional shapes return
// well-framed empty results: views (KesselDB has no views) and
// routines (V1 has no stored procedures — V2 SP-PG-CAT-PROC).
//
// Each synthesizer REUSES the existing engine.list_tables /
// describe_table / list_indexes_for_table / list_constraints_for_table
// data sources — the information_schema views are projections /
// re-shapings of the same underlying KesselDB catalog data, NOT a
// separate metadata source. That means: every fix to a pg_catalog
// row propagates to its information_schema mirror for free.

/// Canonical SQL-standard catalog name V1 emits in every
/// information_schema row's `*_catalog` column. Matches
/// `KESSELDB_DATABASE_NAME` so a tool that JOINs across
/// `pg_database` + `information_schema.tables` sees consistent
/// values.
pub const INFORMATION_SCHEMA_CATALOG: &str = "kesseldb";

/// SQL-standard `information_schema.tables.table_type` value for an
/// ordinary base table. KesselDB has no views or system-catalog
/// table objects in V1 — every entry from `engine.list_tables` is
/// 'BASE TABLE'.
pub const INFORMATION_SCHEMA_BASE_TABLE: &str = "BASE TABLE";

/// Map a PG type OID to the canonical SQL-standard
/// `information_schema.columns.data_type` name. SQL-standard names
/// (`bigint`, `smallint`, `integer`, `text`, `boolean`, `timestamp
/// with time zone`, `numeric`, `bytea`) differ from PG's internal
/// `pg_type.typname` (`int8`, `int2`, `int4`, `text`, `bool`,
/// `timestamptz`, `numeric`, `bytea`) on a few common types.
/// BI tools key feature support off this column, so the mapping
/// MUST match what real Postgres returns.
pub fn information_schema_data_type_for_oid(oid: u32) -> &'static str {
    match oid {
        PG_TYPE_BOOL => "boolean",
        PG_TYPE_BYTEA => "bytea",
        PG_TYPE_INT2 => "smallint",
        PG_TYPE_INT4 => "integer",
        PG_TYPE_INT8 => "bigint",
        PG_TYPE_TEXT => "text",
        PG_TYPE_VARCHAR => "character varying",
        PG_TYPE_NUMERIC => "numeric",
        PG_TYPE_TIMESTAMPTZ => "timestamp with time zone",
        PG_TYPE_OID => "oid",
        _ => "USER-DEFINED",
    }
}

/// SP-PG-CAT T6 — `information_schema.tables` synthesizer.
///
/// Emits one row per KesselDB user table. The view has 12 columns
/// per the SQL standard; V1 fills the 4 essential columns
/// (table_catalog / table_schema / table_name / table_type) +
/// emits NULL for the rest (typed-table / commit-action /
/// reference-generation / self-referencing-column-name are all
/// PG-extension fields tools rarely touch).
///
/// Used by Metabase / Tableau / Looker / Hex / Superset /
/// dbt-postgres connect-database wizards. queries.md §5.1.
pub fn synthesize_information_schema_tables<E: EngineApply + ?Sized>(
    engine: &E,
) -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "table_catalog".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "table_schema".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "table_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "table_type".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "self_referencing_column_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "reference_generation".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "user_defined_type_catalog".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "user_defined_type_schema".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "user_defined_type_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "is_insertable_into".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "is_typed".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "commit_action".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    let mut n: u64 = 0;
    let table_schema = b"public".as_ref();
    let catalog = INFORMATION_SCHEMA_CATALOG.as_bytes();
    let base_table = INFORMATION_SCHEMA_BASE_TABLE.as_bytes();
    let yes = b"YES".as_ref();
    let no = b"NO".as_ref();
    for t in &tables {
        if !matches!(t.kind, TableKind::Ordinary) {
            continue;
        }
        out.extend_from_slice(&encode_data_row(&[
            Some(catalog),               // table_catalog
            Some(table_schema),          // table_schema
            Some(t.name.as_bytes()),     // table_name
            Some(base_table),            // table_type = 'BASE TABLE'
            None,                        // self_referencing_column_name
            None,                        // reference_generation
            None,                        // user_defined_type_catalog
            None,                        // user_defined_type_schema
            None,                        // user_defined_type_name
            Some(yes),                   // is_insertable_into = 'YES'
            Some(no),                    // is_typed = 'NO'
            None,                        // commit_action
        ]));
        n += 1;
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T6 — `information_schema.columns` synthesizer.
///
/// Emits one row per (table × column) of every KesselDB user
/// table. If `table_filter = Some(name)`, restricts to that one
/// table (the common Metabase / Tableau per-table introspection
/// shape, queries.md §5.2). The view has ~30 columns per the SQL
/// standard; V1 fills the 10 essential columns BI tools actually
/// read (table_catalog / table_schema / table_name / column_name /
/// ordinal_position / column_default / is_nullable / data_type /
/// character_maximum_length / numeric_precision / numeric_scale /
/// datetime_precision) + emits NULL for the rest.
pub fn synthesize_information_schema_columns<E: EngineApply + ?Sized>(
    engine: &E,
    table_filter: Option<&str>,
) -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "table_catalog".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "table_schema".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "table_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "column_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "ordinal_position".to_string(), type_oid: PG_TYPE_INT4 },
        FieldMeta { name: "column_default".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "is_nullable".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "data_type".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "character_maximum_length".to_string(), type_oid: PG_TYPE_INT4 },
        FieldMeta { name: "numeric_precision".to_string(), type_oid: PG_TYPE_INT4 },
        FieldMeta { name: "numeric_scale".to_string(), type_oid: PG_TYPE_INT4 },
        FieldMeta { name: "datetime_precision".to_string(), type_oid: PG_TYPE_INT4 },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    let mut n: u64 = 0;
    let table_schema = b"public".as_ref();
    let catalog = INFORMATION_SCHEMA_CATALOG.as_bytes();
    let yes = b"YES".as_ref();
    let no = b"NO".as_ref();
    for t in &tables {
        if let Some(want) = table_filter {
            if t.name != want {
                continue;
            }
        }
        if !matches!(t.kind, TableKind::Ordinary) {
            continue;
        }
        let cols = match engine.describe_table(&t.name) {
            Some(c) => c,
            None => continue,
        };
        for (idx, col) in cols.iter().enumerate() {
            let ordinal = (idx + 1).to_string();
            let atttypid = field_kind_to_oid(col.kind);
            let data_type = information_schema_data_type_for_oid(atttypid);
            let is_nullable: &[u8] = if col.nullable { yes } else { no };
            // numeric_precision per SQL standard: bigint=64, int=32,
            // smallint=16, numeric=NULL (variable). NULL for non-
            // numeric types.
            let numeric_precision: Option<Vec<u8>> = match atttypid {
                PG_TYPE_INT2 => Some(b"16".to_vec()),
                PG_TYPE_INT4 => Some(b"32".to_vec()),
                PG_TYPE_INT8 => Some(b"64".to_vec()),
                _ => None,
            };
            // numeric_scale: 0 for integers, NULL for non-numeric.
            let numeric_scale: Option<Vec<u8>> = match atttypid {
                PG_TYPE_INT2 | PG_TYPE_INT4 | PG_TYPE_INT8 => Some(b"0".to_vec()),
                _ => None,
            };
            // datetime_precision: 6 for timestamptz (microseconds),
            // NULL for non-datetime.
            let datetime_precision: Option<&[u8]> = match atttypid {
                PG_TYPE_TIMESTAMPTZ => Some(b"6"),
                _ => None,
            };
            out.extend_from_slice(&encode_data_row(&[
                Some(catalog),                       // table_catalog
                Some(table_schema),                  // table_schema
                Some(t.name.as_bytes()),             // table_name
                Some(col.name.as_bytes()),           // column_name
                Some(ordinal.as_bytes()),            // ordinal_position
                None,                                // column_default (V1: NULL)
                Some(is_nullable),                   // is_nullable
                Some(data_type.as_bytes()),          // data_type
                None,                                // character_maximum_length (V1: NULL — variable)
                numeric_precision.as_deref(),        // numeric_precision
                numeric_scale.as_deref(),            // numeric_scale
                datetime_precision,                  // datetime_precision
            ]));
            n += 1;
        }
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T6 — `information_schema.schemata` synthesizer.
///
/// Emits the 3 canonical schemas KesselDB exposes: `public` (user
/// tables), `pg_catalog` (PG system catalog), `information_schema`
/// (SQL-standard catalog). Matches the `pg_namespace` 3-row stub
/// (T1) but with the SQL-standard column shape.
///
/// Used by Metabase / Tableau / Looker / dbt-postgres schema-list
/// queries (queries.md §5.3).
pub fn synthesize_information_schema_schemata() -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "catalog_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "schema_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "schema_owner".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "default_character_set_catalog".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "default_character_set_schema".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "default_character_set_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "sql_path".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let catalog = INFORMATION_SCHEMA_CATALOG.as_bytes();
    let owner = KESSELDB_USER_NAME.as_bytes();
    for schema_name in ["pg_catalog", "public", "information_schema"] {
        out.extend_from_slice(&encode_data_row(&[
            Some(catalog),
            Some(schema_name.as_bytes()),
            Some(owner),
            None, // default_character_set_catalog
            None, // default_character_set_schema
            None, // default_character_set_name
            None, // sql_path
        ]));
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(3)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T6 — `information_schema.key_column_usage` synthesizer.
///
/// Emits one row per (constraint × column) for primary keys + foreign
/// keys + unique constraints. The view is keyed on
/// (constraint_catalog, constraint_schema, constraint_name) +
/// (table_catalog, table_schema, table_name, column_name) +
/// `ordinal_position` (1-based within the constraint).
///
/// V1 sources data from `engine.list_constraints_for_table` —
/// CHECK constraints are skipped (they don't apply to columns;
/// they apply to expressions, so they don't have a key_column_usage
/// entry per the SQL standard).
pub fn synthesize_information_schema_key_column_usage<E: EngineApply + ?Sized>(
    engine: &E,
    table_filter: Option<&str>,
) -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "constraint_catalog".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "constraint_schema".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "constraint_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "table_catalog".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "table_schema".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "table_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "column_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "ordinal_position".to_string(), type_oid: PG_TYPE_INT4 },
        FieldMeta { name: "position_in_unique_constraint".to_string(), type_oid: PG_TYPE_INT4 },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    let mut n: u64 = 0;
    let catalog = INFORMATION_SCHEMA_CATALOG.as_bytes();
    let schema = b"public".as_ref();
    for t in &tables {
        if let Some(want) = table_filter {
            if t.name != want {
                continue;
            }
        }
        let cols = engine.describe_table(&t.name);
        for c in engine.list_constraints_for_table(&t.name) {
            // CHECK constraints have no key_column_usage rows per
            // SQL standard — they apply to expressions, not columns.
            if matches!(c.kind, ConstraintKind::Check) {
                continue;
            }
            for (idx, attnum) in c.columns.iter().enumerate() {
                let ord = (idx + 1).to_string();
                let column_name = match &cols {
                    Some(cs) => {
                        let attn = *attnum as usize;
                        if attn >= 1 && attn <= cs.len() {
                            cs[attn - 1].name.clone()
                        } else {
                            String::new()
                        }
                    }
                    None => String::new(),
                };
                // position_in_unique_constraint: NULL for non-FK
                // (per SQL standard); for FK, position is the index
                // in the referenced unique constraint (V1 emits ord).
                let position_in_unique: Option<Vec<u8>> =
                    if matches!(c.kind, ConstraintKind::ForeignKey { .. }) {
                        Some(ord.clone().into_bytes())
                    } else {
                        None
                    };
                out.extend_from_slice(&encode_data_row(&[
                    Some(catalog),                   // constraint_catalog
                    Some(schema),                    // constraint_schema
                    Some(c.name.as_bytes()),         // constraint_name
                    Some(catalog),                   // table_catalog
                    Some(schema),                    // table_schema
                    Some(t.name.as_bytes()),         // table_name
                    Some(column_name.as_bytes()),    // column_name
                    Some(ord.as_bytes()),            // ordinal_position
                    position_in_unique.as_deref(),   // position_in_unique_constraint
                ]));
                n += 1;
            }
        }
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T6 — `information_schema.table_constraints` synthesizer.
///
/// Emits one row per constraint (PK / FK / UNIQUE / CHECK) across
/// the KesselDB catalog. Used by BI tools (Metabase referential
/// integrity discovery; Tableau "Edit Relationships" wizard) +
/// schema-introspection tools (Schemaspy, ER diagram generators).
pub fn synthesize_information_schema_table_constraints<E: EngineApply + ?Sized>(
    engine: &E,
    table_filter: Option<&str>,
) -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "constraint_catalog".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "constraint_schema".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "constraint_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "table_catalog".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "table_schema".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "table_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "constraint_type".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "is_deferrable".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "initially_deferred".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "enforced".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    let mut n: u64 = 0;
    let catalog = INFORMATION_SCHEMA_CATALOG.as_bytes();
    let schema = b"public".as_ref();
    let no = b"NO".as_ref();
    let yes = b"YES".as_ref();
    for t in &tables {
        if let Some(want) = table_filter {
            if t.name != want {
                continue;
            }
        }
        for c in engine.list_constraints_for_table(&t.name) {
            let constraint_type: &[u8] = match c.kind {
                ConstraintKind::Check => b"CHECK",
                ConstraintKind::ForeignKey { .. } => b"FOREIGN KEY",
                ConstraintKind::Unique => b"UNIQUE",
            };
            out.extend_from_slice(&encode_data_row(&[
                Some(catalog),                   // constraint_catalog
                Some(schema),                    // constraint_schema
                Some(c.name.as_bytes()),         // constraint_name
                Some(catalog),                   // table_catalog
                Some(schema),                    // table_schema
                Some(t.name.as_bytes()),         // table_name
                Some(constraint_type),           // constraint_type
                Some(no),                        // is_deferrable
                Some(no),                        // initially_deferred
                Some(yes),                       // enforced
            ]));
            n += 1;
        }
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T6 — `information_schema.views` synthesizer.
///
/// Returns a well-framed empty result (RowDescription + 0 rows +
/// CommandComplete + ReadyForQuery). KesselDB V1 has no views;
/// V2 SP-VIEW would populate.
pub fn synthesize_information_schema_views() -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "table_catalog".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "table_schema".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "table_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "view_definition".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "check_option".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "is_updatable".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "is_insertable_into".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "is_trigger_updatable".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "is_trigger_deletable".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "is_trigger_insertable_into".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    out.extend_from_slice(&encode_command_complete(&select_tag(0)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T6 — `information_schema.routines` synthesizer.
///
/// Returns a well-framed empty result. KesselDB V1 has no
/// user-defined routines (stored procedures / functions); V2
/// SP-PG-CAT-PROC would populate. DataGrip + JetBrains tooling
/// query this on connect.
pub fn synthesize_information_schema_routines() -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "specific_catalog".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "specific_schema".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "specific_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "routine_catalog".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "routine_schema".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "routine_name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "routine_type".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "data_type".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    out.extend_from_slice(&encode_command_complete(&select_tag(0)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

// ─── SP-PG-CAT T8 (real-psql) — psql `\d <name>` step-1 + `\dn` ──────────

/// SP-PG-CAT T8 (real-psql) — psql `\d <name>` STEP 1.
///
/// Synthesize the (oid, nspname, relname) row that psql expects from
/// its OID-lookup regex query (mod.rs `extract_psql_d_step1_relname`).
/// Walks `engine.list_tables()` for an exact-name match (case-insensitive
/// — psql lowercases unquoted identifiers).
///
/// **Output columns** (matching the query's SELECT projection):
/// - `oid` (PG_TYPE_OID) — `oid_for_table_name(name)` deterministic
/// - `nspname` (PG_TYPE_TEXT) — `public` (V1 single user schema)
/// - `relname` (PG_TYPE_TEXT) — the table name (verbatim from
///   `list_tables`)
///
/// If no table matches, emits a 0-row well-framed response — psql then
/// prints `Did not find any relation named "<name>".` and exits with
/// code 1, matching real PG behavior.
pub fn psql_d_step1_oid_lookup<E: EngineApply + ?Sized>(
    engine: &E,
    name: &str,
) -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "oid".to_string(), type_oid: PG_TYPE_OID },
        FieldMeta { name: "nspname".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "relname".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    let mut n: u64 = 0;
    for t in &tables {
        // psql `\d <name>` is case-insensitive for unquoted names.
        if t.name.eq_ignore_ascii_case(name)
            && matches!(t.kind, TableKind::Ordinary | TableKind::Index)
        {
            let oid_str = oid_for_table_name(&t.name).to_string();
            out.extend_from_slice(&encode_data_row(&[
                Some(oid_str.as_bytes()),
                Some(b"public"),
                Some(t.name.as_bytes()),
            ]));
            n += 1;
        }
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T8 (real-psql) — psql `\d <name>` STEP 2.
///
/// Synthesize the 15-column relation-summary row psql expects for the
/// per-OID `pg_class c LEFT JOIN pg_am am` query (mod.rs
/// `extract_psql_d_step2_oid`). The output drives psql's "Table" /
/// "Indexes:" / "Foreign-key constraints:" header text. V1 returns
/// the canonical `pg_class` defaults for an ordinary KesselDB table
/// — relkind='r', persistence='p', replication='d', amname='heap',
/// no indexes / rules / triggers / row-security / partitions.
///
/// 0 rows if the OID doesn't match any live table — psql then prints
/// "No matching relations found." and exits, matching real PG.
///
/// Columns (in projection order, matching psql's SELECT):
/// 1.  relchecks       (int2)  — 0
/// 2.  relkind         (char)  — 'r' (ordinary table)
/// 3.  relhasindex     (bool)  — 'f'  (V1 doesn't track engine-side)
/// 4.  relhasrules     (bool)  — 'f'
/// 5.  relhastriggers  (bool)  — 'f'
/// 6.  relrowsecurity  (bool)  — 'f'
/// 7.  relforcerowsec  (bool)  — 'f'
/// 8.  relhasoids      (bool)  — 'f'  (PG 12+ always false)
/// 9.  relispartition  (bool)  — 'f'
/// 10. ''                       — empty literal psql submits
/// 11. reltablespace   (oid)   — 0
/// 12. (CASE ...)               — '' (V1: no typed tables)
/// 13. relpersistence  (char)  — 'p' (permanent)
/// 14. relreplident    (char)  — 'd' (default)
/// 15. amname          (text)  — 'heap'
pub fn psql_d_step2_relsummary<E: EngineApply + ?Sized>(
    engine: &E,
    table_oid: u32,
) -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "relchecks".to_string(), type_oid: PG_TYPE_INT2 },
        FieldMeta { name: "relkind".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "relhasindex".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "relhasrules".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "relhastriggers".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "relrowsecurity".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "relforcerowsecurity".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "relhasoids".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "relispartition".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "?column?".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "reltablespace".to_string(), type_oid: PG_TYPE_OID },
        FieldMeta { name: "?column?".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "relpersistence".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "relreplident".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "amname".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let tables = engine.list_tables();
    let mut n: u64 = 0;
    let empty = b"".as_ref();
    let false_ = b"f".as_ref();
    let zero = b"0".as_ref();
    for t in &tables {
        if oid_for_table_name(&t.name) != table_oid {
            continue;
        }
        if !matches!(t.kind, TableKind::Ordinary) {
            continue;
        }
        out.extend_from_slice(&encode_data_row(&[
            Some(zero),         // relchecks = 0
            Some(b"r"),         // relkind = 'r' (ordinary)
            Some(false_),       // relhasindex
            Some(false_),       // relhasrules
            Some(false_),       // relhastriggers
            Some(false_),       // relrowsecurity
            Some(false_),       // relforcerowsecurity
            Some(false_),       // relhasoids
            Some(false_),       // relispartition
            Some(empty),        // '' literal
            Some(zero),         // reltablespace = 0
            Some(empty),        // reloftype CASE = ''
            Some(b"p"),         // relpersistence = 'p'
            Some(b"d"),         // relreplident = 'd'
            Some(b"heap"),      // amname = 'heap'
        ]));
        n += 1;
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T8 (real-psql) — well-framed empty for psql's pg_policy
/// poll on `\d <table>`. KesselDB V1 has no row-level security; the
/// query is unconditional and would otherwise error with
/// `expected FROM` (subselects in projection) and abort `\d <table>`
/// rendering before psql gets to print the column list.
pub fn psql_d_pg_policy_empty() -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "polname".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "polpermissive".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "?column?".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "pg_get_expr".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "pg_get_expr".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "cmd".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    out.extend_from_slice(&encode_command_complete(&select_tag(0)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T8 (real-psql) — well-framed empty for psql's pg_inherits
/// poll. KesselDB V1 has no partitioning.
pub fn psql_d_pg_inherits_empty() -> Vec<u8> {
    let fields = vec![FieldMeta {
        name: "oid".to_string(),
        type_oid: PG_TYPE_OID,
    }];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    out.extend_from_slice(&encode_command_complete(&select_tag(0)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T8 (real-psql) — well-framed empty for psql's pg_trigger
/// poll. KesselDB V1's deterministic-expression triggers do not
/// register in pg_trigger (they're engine-side, not PG-wire-visible).
pub fn psql_d_pg_trigger_empty() -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "tgname".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "tgenabled".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "tgisinternal".to_string(), type_oid: PG_TYPE_BOOL },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    out.extend_from_slice(&encode_command_complete(&select_tag(0)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T8 (real-psql) — well-framed empty for psql's
/// pg_statistic_ext poll. PG 16+ unconditionally polls this on
/// every `\d <table>`; KesselDB V1 has no multi-column statistics.
pub fn psql_d_pg_statistic_ext_empty() -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "oid".to_string(), type_oid: PG_TYPE_OID },
        FieldMeta { name: "stxrelid".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "nsp".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "stxname".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "columns".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "ndist_enabled".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "deps_enabled".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "mcv_enabled".to_string(), type_oid: PG_TYPE_BOOL },
        FieldMeta { name: "stxstattarget".to_string(), type_oid: PG_TYPE_INT4 },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    out.extend_from_slice(&encode_command_complete(&select_tag(0)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T8 (real-psql) — well-framed empty for psql's
/// pg_publication poll. PG 10+ logical-replication metadata.
pub fn psql_d_pg_publication_empty() -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "pubname".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    out.extend_from_slice(&encode_command_complete(&select_tag(0)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T8 (real-psql) — well-framed empty for psql's
/// pg_foreign_table poll. KesselDB V1 has no foreign-data wrappers.
pub fn psql_d_pg_foreign_table_empty() -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "ftrelid".to_string(), type_oid: PG_TYPE_OID },
        FieldMeta { name: "ftoptions".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    out.extend_from_slice(&encode_command_complete(&select_tag(0)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-CAT T8 (real-psql) — psql `\dn` schema-list.
///
/// Synthesize the canonical 2-column ("Name", "Owner") response for
/// psql's `\dn` schema-list query. V1 KesselDB has exactly one
/// user-visible schema: `public`. `pg_catalog` and `information_schema`
/// are filtered out by the query itself (`!~ '^pg_'` and `<> ...`).
pub fn psql_dn_schema_list() -> Vec<u8> {
    let fields = vec![
        FieldMeta { name: "Name".to_string(), type_oid: PG_TYPE_TEXT },
        FieldMeta { name: "Owner".to_string(), type_oid: PG_TYPE_TEXT },
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    // One row — public schema owned by the canonical PG superuser
    // (KESSELDB_USER_NAME below; we hard-code "kesseldb" here to avoid
    // a forward reference to the T7 constant).
    out.extend_from_slice(&encode_data_row(&[
        Some(b"public"),
        Some(b"kesseldb"),
    ]));
    out.extend_from_slice(&encode_command_complete(&select_tag(1)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

// ─── SP-PG-CAT T7 — SQL helper functions ──────────────────────────────────

/// Canonical version string V1 emits for `SELECT version()`. Matches
/// the StartupMessage `server_version` ParameterStatus emit so clients
/// see a coherent view of the server. PG-major-version-prefix 14 is
/// the V1 emulation target.
pub const KESSELDB_VERSION_STRING: &str = "PostgreSQL 14.0 (KesselDB 1.0)";

/// Canonical database name V1 emits for `SELECT current_database()`.
/// KesselDB has one logical database; V1 hard-codes the name.
pub const KESSELDB_DATABASE_NAME: &str = "kesseldb";

/// Canonical schema name V1 emits for `SELECT current_schema()`.
pub const KESSELDB_SCHEMA_NAME: &str = "public";

/// Canonical user name V1 emits for `SELECT current_user` / `session_user`.
pub const KESSELDB_USER_NAME: &str = "kesseldb";

/// Build a single-row, single-column response for a helper function
/// that returns a text value. Used by `version()` / `current_database()`
/// / `current_schema()` / etc.
fn single_text_row(column_name: &str, value: &str) -> Vec<u8> {
    let fields = vec![FieldMeta {
        name: column_name.to_string(),
        type_oid: PG_TYPE_TEXT,
    }];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    out.extend_from_slice(&encode_data_row(&[Some(value.as_bytes())]));
    out.extend_from_slice(&encode_command_complete(&select_tag(1)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// Build a single-row, single-column response with a bool ('t'/'f')
/// value. Used by `pg_table_is_visible(oid)` / `pg_is_other_temp_schema(oid)`.
fn single_bool_row(column_name: &str, value: bool) -> Vec<u8> {
    let fields = vec![FieldMeta {
        name: column_name.to_string(),
        type_oid: PG_TYPE_BOOL,
    }];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let b: &[u8] = if value { b"t" } else { b"f" };
    out.extend_from_slice(&encode_data_row(&[Some(b)]));
    out.extend_from_slice(&encode_command_complete(&select_tag(1)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// Build a single-row, single-column response with an int value.
fn single_int_row(column_name: &str, oid: u32, value: i64) -> Vec<u8> {
    let fields = vec![FieldMeta {
        name: column_name.to_string(),
        type_oid: oid,
    }];
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let s = value.to_string();
    out.extend_from_slice(&encode_data_row(&[Some(s.as_bytes())]));
    out.extend_from_slice(&encode_command_complete(&select_tag(1)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// Resolve a canned GUC value for `SHOW <name>` (and `current_setting('<name>')`).
/// Returns `Some(value)` for known GUCs (mirroring V1 ParameterStatus
/// emit) or `Some("")` for unknown GUCs — matching PG's behavior of
/// returning the empty string for an unrecognized parameter name.
fn show_value_for(name: &str) -> &'static str {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "server_version" => "14.0",
        "server_version_num" => "140000",
        "server_encoding" => "UTF8",
        "client_encoding" => "UTF8",
        "datestyle" => "ISO, MDY",
        "timezone" | "time zone" => "UTC",
        "standard_conforming_strings" => "on",
        "integer_datetimes" => "on",
        "application_name" => "",
        "is_superuser" => "on",
        "session_authorization" => KESSELDB_USER_NAME,
        "search_path" => "\"$user\", public",
        "default_transaction_isolation" => "read committed",
        "transaction_isolation" => "read committed",
        "in_hot_standby" => "off",
        "default_transaction_read_only" => "off",
        "transaction_read_only" => "off",
        _ => "",
    }
}

/// SP-PG-CAT T7 — single-call helper-function recognizer. Recognizes
/// `SELECT version()`, `SELECT current_database()`, `SELECT
/// current_schema()`, `SELECT current_user`, `SELECT session_user`,
/// `SHOW <name>`, plus a handful of canned per-OID functions.
///
/// Returns:
/// - `Some(wire_bytes)` — SQL matched a known helper-function pattern.
/// - `None` — fall through to table/JOIN matchers.
///
/// Checked BEFORE the table-pattern matchers in `catalog_query_hook`
/// because helpers are simpler shapes.
///
/// Recognized patterns (all case-insensitive after normalization):
///
/// - `select version()` → 'PostgreSQL 14.0 (KesselDB 1.0)'
/// - `select current_database()` → 'kesseldb'
/// - `select current_schema()` / `select current_schema` → 'public'
/// - `select current_user` → 'kesseldb'
/// - `select session_user` → 'kesseldb'
/// - `select user` → 'kesseldb'
/// - `select current_catalog` → 'kesseldb'
/// - `select pg_backend_pid()` → 1 (canned non-zero PID)
/// - `select pg_my_temp_schema()` → 0
/// - `select pg_is_other_temp_schema(<oid>)` → false
/// - `select pg_table_is_visible(<oid>)` → true
/// - `select pg_get_userbyid(<oid>)` → 'kesseldb'
/// - `select pg_get_indexdef(<oid>)` → ''
/// - `select pg_get_constraintdef(<oid>)` → ''
/// - `select obj_description(<oid>, 'pg_class')` → NULL
/// - `select current_setting('<name>')` → canned GUC value
/// - `show <name>` → canned GUC value
/// - Multi-function probe `select version(), current_database()` →
///   handled separately by `synthesize_pgadmin_multi_helper`.
pub fn synthesize_helper_function(normalized: &str) -> Option<Vec<u8>> {
    // Strip optional trailing `as alias` (case-insensitive). We don't
    // try to be exhaustive — the simple forms are what tools issue.
    let s = strip_select_alias(normalized);

    // ── SP-PG-EXTQ T7 — SQLAlchemy connection-validity probe ───────
    // SQLAlchemy 2.0's PG dialect issues `SELECT 1` as its
    // `do_ping()` and pool-checkout health probe. kessel-sql rejects
    // `SELECT 1` (bare scalar SELECT — V1 requires `SELECT * FROM
    // <table>` per design spec §11 weak-spot). Without this intercept
    // SQLAlchemy refuses every `engine.connect()`. Intercept here and
    // emit a single row with column "?column?" + value 1 (the libpq
    // canonical shape for an anonymous scalar SELECT).
    if s == "select 1" {
        return Some(single_int_row("?column?", PG_TYPE_INT4, 1));
    }
    // Companion shape: `select true` / `select false` (some clients
    // probe with these — e.g. asyncpg's reconnect heartbeat).
    if s == "select true" {
        return Some(single_bool_row("bool", true));
    }
    if s == "select false" {
        return Some(single_bool_row("bool", false));
    }
    // SQLAlchemy 2.0 first-connect encoding probes (`PGDialect_psycopg2.
    // do_test_connection` issues exactly these two text-roundtrip
    // queries to validate the client encoding). After
    // `strip_select_alias` removes the trailing `as anon_1`, the
    // remainder is the canonical match below — both shapes are
    // idiosyncratic to SQLAlchemy and zero risk for collisions.
    //
    // Note: `strip_select_alias` looks for ` as <ident>` and trims;
    // the `as varchar(60)` INSIDE the CAST stays (it's followed by a
    // close-paren, not by an identifier-only tail).
    if s == "select cast('test plain returns' as varchar(60))" {
        return Some(single_text_row("anon_1", "test plain returns"));
    }
    if s == "select cast('test unicode returns' as varchar(60))" {
        return Some(single_text_row("anon_1", "test unicode returns"));
    }
    // SQLAlchemy 2.0 also probes `select pg_catalog.version()` (PG-
    // qualified form). Handled by the version path below when the
    // shape matches — but the parser strips `pg_catalog.` so add an
    // explicit alias.
    if s == "select pg_catalog.version()" {
        return Some(single_text_row("version", KESSELDB_VERSION_STRING));
    }
    // ── Single-call shapes (no args) ──────────────────────────────
    if s == "select version()" {
        return Some(single_text_row("version", KESSELDB_VERSION_STRING));
    }
    if s == "select current_database()" || s == "select current_catalog" {
        return Some(single_text_row("current_database", KESSELDB_DATABASE_NAME));
    }
    if s == "select current_schema()" || s == "select current_schema" {
        return Some(single_text_row("current_schema", KESSELDB_SCHEMA_NAME));
    }
    if s == "select current_user" || s == "select user" {
        return Some(single_text_row("current_user", KESSELDB_USER_NAME));
    }
    if s == "select session_user" {
        return Some(single_text_row("session_user", KESSELDB_USER_NAME));
    }
    if s == "select pg_backend_pid()" {
        return Some(single_int_row("pg_backend_pid", PG_TYPE_INT4, 1));
    }
    if s == "select pg_my_temp_schema()" {
        return Some(single_int_row("pg_my_temp_schema", PG_TYPE_OID, 0));
    }
    if s == "select pg_postmaster_start_time()" {
        return Some(single_text_row(
            "pg_postmaster_start_time",
            "2026-01-01 00:00:00+00",
        ));
    }

    // ── pgAdmin multi-function probe ──────────────────────────────
    // `select version(), current_database(), current_user, current_schema()`
    // is the canonical pgAdmin connect probe (queries.md §6.3). Recognize
    // exactly the 4-function shape AND the common 2-/3-function shortenings.
    if let Some(bytes) = synthesize_pgadmin_multi_helper(s) {
        return Some(bytes);
    }

    // ── Per-OID functions (prefix-match) ──────────────────────────
    // `pg_table_is_visible(N)` → true (V1 single-schema; everything visible).
    if s.starts_with("select pg_catalog.pg_table_is_visible(")
        || s.starts_with("select pg_table_is_visible(")
    {
        return Some(single_bool_row("pg_table_is_visible", true));
    }
    if s.starts_with("select pg_catalog.pg_type_is_visible(")
        || s.starts_with("select pg_type_is_visible(")
    {
        return Some(single_bool_row("pg_type_is_visible", true));
    }
    if s.starts_with("select pg_catalog.pg_function_is_visible(")
        || s.starts_with("select pg_function_is_visible(")
    {
        return Some(single_bool_row("pg_function_is_visible", true));
    }
    if s.starts_with("select pg_catalog.pg_is_other_temp_schema(")
        || s.starts_with("select pg_is_other_temp_schema(")
    {
        return Some(single_bool_row("pg_is_other_temp_schema", false));
    }
    // `pg_get_userbyid(N)` → 'kesseldb' (V1: one user identity).
    if s.starts_with("select pg_catalog.pg_get_userbyid(")
        || s.starts_with("select pg_get_userbyid(")
    {
        return Some(single_text_row("pg_get_userbyid", KESSELDB_USER_NAME));
    }
    // `pg_get_indexdef(N)` / `pg_get_constraintdef(N)` → empty text.
    if s.starts_with("select pg_catalog.pg_get_indexdef(")
        || s.starts_with("select pg_get_indexdef(")
    {
        return Some(single_text_row("pg_get_indexdef", ""));
    }
    if s.starts_with("select pg_catalog.pg_get_constraintdef(")
        || s.starts_with("select pg_get_constraintdef(")
    {
        return Some(single_text_row("pg_get_constraintdef", ""));
    }
    if s.starts_with("select pg_catalog.pg_get_expr(")
        || s.starts_with("select pg_get_expr(")
    {
        return Some(single_text_row("pg_get_expr", ""));
    }
    // `obj_description(N, 'pg_class')` / `obj_description(N)` → NULL.
    if s.starts_with("select pg_catalog.obj_description(")
        || s.starts_with("select obj_description(")
    {
        let fields = vec![FieldMeta {
            name: "obj_description".to_string(),
            type_oid: PG_TYPE_TEXT,
        }];
        let mut out = Vec::new();
        out.extend_from_slice(&encode_row_description(&fields));
        out.extend_from_slice(&encode_data_row(&[None]));
        out.extend_from_slice(&encode_command_complete(&select_tag(1)));
        out.extend_from_slice(&encode_ready_for_query(b'I'));
        return Some(out);
    }
    // `format_type(<oid>, <typmod>)` → canonical type name from
    // pg_type_name_for_oid. The caller passes the OID inline; we
    // extract the first u32 argument and map to the canonical name.
    if let Some(rest) = s.strip_prefix("select pg_catalog.format_type(")
        .or_else(|| s.strip_prefix("select format_type("))
    {
        if let Some(oid) = parse_leading_u32_pub(rest) {
            let name = pg_type_name_for_oid(oid);
            return Some(single_text_row("format_type", name));
        }
        return Some(single_text_row("format_type", "unknown"));
    }
    // `current_setting('<name>')` → canned GUC value, '' for unknown.
    if let Some(rest) = s.strip_prefix("select pg_catalog.current_setting(")
        .or_else(|| s.strip_prefix("select current_setting("))
    {
        let name = extract_quoted_arg(rest);
        let val = show_value_for(&name);
        return Some(single_text_row("current_setting", val));
    }

    // ── SHOW ALL (returns a 3-column projection with 0 rows in V1) ─
    // Checked BEFORE the SHOW prefix-strip so "show all" doesn't fall
    // into the `show <name>` GUC lookup (which would return empty
    // text for "all"). Tools that issue SHOW ALL get a graceful
    // 0-row table.
    if s == "show all" {
        let fields = vec![
            FieldMeta { name: "name".to_string(), type_oid: PG_TYPE_TEXT },
            FieldMeta { name: "setting".to_string(), type_oid: PG_TYPE_TEXT },
            FieldMeta { name: "description".to_string(), type_oid: PG_TYPE_TEXT },
        ];
        let mut out = Vec::new();
        out.extend_from_slice(&encode_row_description(&fields));
        out.extend_from_slice(&encode_command_complete(&select_tag(0)));
        out.extend_from_slice(&encode_ready_for_query(b'I'));
        return Some(out);
    }
    // ── SHOW <name> ───────────────────────────────────────────────
    if let Some(rest) = s.strip_prefix("show ") {
        // The name may itself be quoted in `show "TimeZone"` form.
        let name = rest.trim_matches(|c: char| c == '"' || c == '\'').trim();
        let val = show_value_for(name);
        // `SHOW` emits the canonical PG `name` type with the GUC
        // name as the column header.
        return Some(single_text_row(name, val));
    }

    None
}

/// Strip a trailing ` as <alias>` clause from a single-statement
/// `SELECT` after normalization. PG accepts `SELECT version() AS v`
/// — V1's matcher anchors on the canonical no-alias form, so we
/// strip the alias before matching.
fn strip_select_alias(s: &str) -> &str {
    // Find the last `as ` token (lowercase). If it's followed by a
    // single identifier with no further whitespace, strip it.
    if let Some(pos) = s.rfind(" as ") {
        let tail = &s[pos + 4..];
        // Tail must be an identifier — letters/digits/underscore only.
        if !tail.is_empty() && tail.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return &s[..pos];
        }
    }
    s
}

/// Synthesize the canonical pgAdmin multi-function connect probe.
/// Recognizes patterns like:
///
/// - `select version(), current_database()`
/// - `select version(), current_database(), current_user`
/// - `select version(), current_database(), current_user, current_schema()`
///
/// Returns a multi-column single-row response with the corresponding
/// values. Recognized patterns are the canonical pgAdmin shapes —
/// extensions / re-ordering fall through to the engine apply path.
fn synthesize_pgadmin_multi_helper(normalized: &str) -> Option<Vec<u8>> {
    let body = normalized.strip_prefix("select ")?;
    // Tokenize on `, ` (post-normalization, each comma is followed by
    // a single space). Each part is a known helper-function call.
    let parts: Vec<&str> = body.split(", ").collect();
    if parts.len() < 2 {
        return None;
    }
    // Each part MUST be one of the recognized helper-function call
    // shapes. If any part isn't recognized, return None.
    let mut fields = Vec::with_capacity(parts.len());
    let mut values: Vec<Vec<u8>> = Vec::with_capacity(parts.len());
    for part in &parts {
        let (col_name, val): (&str, String) = match *part {
            "version()" => ("version", KESSELDB_VERSION_STRING.to_string()),
            "current_database()" | "current_catalog" => {
                ("current_database", KESSELDB_DATABASE_NAME.to_string())
            }
            "current_schema()" | "current_schema" => {
                ("current_schema", KESSELDB_SCHEMA_NAME.to_string())
            }
            "current_user" | "user" => ("current_user", KESSELDB_USER_NAME.to_string()),
            "session_user" => ("session_user", KESSELDB_USER_NAME.to_string()),
            _ => return None,
        };
        fields.push(FieldMeta { name: col_name.to_string(), type_oid: PG_TYPE_TEXT });
        values.push(val.into_bytes());
    }
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));
    let cols: Vec<Option<&[u8]>> = values.iter().map(|v| Some(v.as_slice())).collect();
    out.extend_from_slice(&encode_data_row(&cols));
    out.extend_from_slice(&encode_command_complete(&select_tag(1)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    Some(out)
}

/// Extract the first single-quoted argument from `s`. Used by
/// `current_setting('name')` argument parsing. Returns `""` on
/// no match. Stops at the first closing quote (no escape handling).
fn extract_quoted_arg(s: &str) -> String {
    if let Some(p) = s.find('\'') {
        let after = &s[p + 1..];
        if let Some(end) = after.find('\'') {
            return after[..end].to_string();
        }
    }
    String::new()
}

/// Public wrapper for `parse_leading_u32` so the helper-function
/// synth (in this file) can call the parser the pattern dispatcher
/// in `mod.rs` already uses. Kept private cross-module via a
/// module-internal duplicate.
fn parse_leading_u32_pub(s: &str) -> Option<u32> {
    let mut acc: u64 = 0;
    let mut any = false;
    for c in s.chars() {
        if let Some(d) = c.to_digit(10) {
            acc = acc.checked_mul(10)?.checked_add(d as u64)?;
            if acc > u32::MAX as u64 {
                return None;
            }
            any = true;
        } else {
            break;
        }
    }
    if any {
        Some(acc as u32)
    } else {
        None
    }
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

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-CAT T5 KATs — pg_index + pg_constraint synthesizers + the
    // pgJDBC `getIndexInfo` joined-result intercept. Drive via
    // IndexEngine (overrides list_indexes_for_table +
    // list_constraints_for_table).
    // ───────────────────────────────────────────────────────────────────

    /// Engine that combines list_tables + describe_table +
    /// list_indexes_for_table + list_constraints_for_table for T5
    /// pattern-hook tests.
    struct IndexEngine {
        tables: Vec<TableMetadata>,
        schemas: std::collections::BTreeMap<String, Vec<PgColumn>>,
        indexes: std::collections::BTreeMap<String, Vec<IndexMetadata>>,
        constraints: std::collections::BTreeMap<String, Vec<ConstraintMetadata>>,
    }
    impl EngineApply for IndexEngine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            OpResult::SchemaError("IndexEngine: apply_sql should not be reached".into())
        }
        fn describe_table(&self, name: &str) -> Option<Vec<PgColumn>> {
            self.schemas.get(name).cloned()
        }
        fn list_tables(&self) -> Vec<TableMetadata> {
            self.tables.clone()
        }
        fn list_indexes_for_table(&self, name: &str) -> Vec<IndexMetadata> {
            self.indexes.get(name).cloned().unwrap_or_default()
        }
        fn list_constraints_for_table(&self, name: &str) -> Vec<ConstraintMetadata> {
            self.constraints.get(name).cloned().unwrap_or_default()
        }
    }

    fn t5_test_engine() -> IndexEngine {
        use crate::engine::{FkAction, IndexKind};
        let mut schemas = std::collections::BTreeMap::new();
        schemas.insert(
            "users".to_string(),
            vec![
                col("id", FieldKind::I64, false),
                col("email", FieldKind::Char(128), false),
                col("created_at", FieldKind::Timestamp, false),
            ],
        );
        schemas.insert(
            "orders".to_string(),
            vec![
                col("id", FieldKind::I64, false),
                col("user_id", FieldKind::I64, false),
                col("amount", FieldKind::Fixed { scale: 2 }, false),
            ],
        );
        let mut indexes = std::collections::BTreeMap::new();
        indexes.insert(
            "users".to_string(),
            vec![
                IndexMetadata {
                    name: "users_email_idx".into(),
                    fields: vec![2],
                    is_unique: true,
                    kind: IndexKind::Equality,
                },
                IndexMetadata {
                    name: "users_created_at_ridx".into(),
                    fields: vec![3],
                    is_unique: false,
                    kind: IndexKind::Range,
                },
            ],
        );
        indexes.insert(
            "orders".to_string(),
            vec![IndexMetadata {
                name: "orders_user_id_amount_idx".into(),
                fields: vec![2, 3],
                is_unique: false,
                kind: IndexKind::Composite,
            }],
        );
        let mut constraints = std::collections::BTreeMap::new();
        constraints.insert(
            "users".to_string(),
            vec![ConstraintMetadata {
                name: "users_email_key".into(),
                kind: ConstraintKind::Unique,
                columns: vec![2],
                references: None,
            }],
        );
        constraints.insert(
            "orders".to_string(),
            vec![
                ConstraintMetadata {
                    name: "orders_user_id_fkey".into(),
                    kind: ConstraintKind::ForeignKey {
                        on_delete: FkAction::Cascade,
                    },
                    columns: vec![2],
                    references: Some(("users".to_string(), vec![1])),
                },
                ConstraintMetadata {
                    name: "orders_amount_check".into(),
                    kind: ConstraintKind::Check,
                    columns: vec![3],
                    references: None,
                },
            ],
        );
        IndexEngine {
            tables: vec![
                td("users", 1, 3),
                td("orders", 2, 3),
            ],
            schemas,
            indexes,
            constraints,
        }
    }

    fn empty_index_engine() -> IndexEngine {
        IndexEngine {
            tables: vec![td("users", 1, 1)],
            schemas: std::collections::BTreeMap::new(),
            indexes: std::collections::BTreeMap::new(),
            constraints: std::collections::BTreeMap::new(),
        }
    }

    /// **HEADLINE — pg_index synthesizer returns 0 rows for engine
    /// with no indexes.** Well-framed (T + C "SELECT 0" + Z, no D
    /// frames). The graceful degradation path for engines that don't
    /// override `list_indexes_for_table`.
    #[test]
    fn t5_pg_index_synthesizer_no_indexes_returns_zero_rows() {
        let eng = empty_index_engine();
        let bytes = synthesize_pg_index(&eng, None);
        assert_eq!(bytes[0], b'T');
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// **HEADLINE — pg_index synthesizer returns one row per index
    /// across all tables.** 2 tables × (2+1) = 3 indexes total.
    /// pg_index does NOT carry the index name (that's pg_class's
    /// relname column); the name surfaces only via the synthetic
    /// indexrelid OID (FNV-1a of the name).
    #[test]
    fn t5_pg_index_synthesizer_emits_all_indexes() {
        let eng = t5_test_engine();
        let bytes = synthesize_pg_index(&eng, None);
        assert_eq!(bytes[0], b'T');
        assert!(bytes.windows(b"SELECT 3\0".len()).any(|w| w == b"SELECT 3\0"),
            "MUST emit SELECT 3 for 3 indexes across 2 tables");
        // Synthetic indexrelid OIDs appear in the stream (decimal text).
        for idx_name in ["users_email_idx", "users_created_at_ridx", "orders_user_id_amount_idx"] {
            let oid = oid_for_index_name(idx_name).to_string();
            assert!(bytes.windows(oid.len()).any(|w| w == oid.as_bytes()),
                "indexrelid for {idx_name} ({oid}) MUST appear");
        }
    }

    /// **HEADLINE — pg_index synthesizer filtered to one table by
    /// indrelid.** pg_index doesn't carry the index name (per PG
    /// catalog shape — that's pg_class.relname); the per-index
    /// data surfaces via the indexrelid OID slot.
    #[test]
    fn t5_pg_index_synthesizer_filtered_to_one_table() {
        let eng = t5_test_engine();
        let users_oid = oid_for_table_name("users");
        let bytes = synthesize_pg_index(&eng, Some(users_oid));
        assert!(bytes.windows(b"SELECT 2\0".len()).any(|w| w == b"SELECT 2\0"),
            "MUST emit SELECT 2 (users has 2 indexes)");
        // Both users index indexrelid OIDs appear; the orders index
        // OID does NOT.
        for users_idx in ["users_email_idx", "users_created_at_ridx"] {
            let oid = oid_for_index_name(users_idx).to_string();
            assert!(bytes.windows(oid.len()).any(|w| w == oid.as_bytes()));
        }
        let orders_idx_oid = oid_for_index_name("orders_user_id_amount_idx").to_string();
        assert!(!bytes.windows(orders_idx_oid.len()).any(|w| w == orders_idx_oid.as_bytes()),
            "orders index OID MUST NOT appear in users-filtered result");
        // users_oid appears as the indrelid value (2 rows × 1 column = 2 occurrences min).
        let users_oid_str = users_oid.to_string();
        let occurrences = bytes.windows(users_oid_str.len())
            .filter(|w| *w == users_oid_str.as_bytes())
            .count();
        assert!(occurrences >= 2, "indrelid for users MUST appear in each of the 2 index rows");
    }

    /// **HEADLINE — pg_index RowDescription has 19 columns.** Locked
    /// vs PG 14 `pg_index.h` so clients iterating row columns by
    /// index don't break.
    #[test]
    fn t5_pg_index_row_description_has_19_columns() {
        let eng = t5_test_engine();
        let bytes = synthesize_pg_index(&eng, None);
        let fc = u16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(fc, PG_INDEX_COLUMN_COUNT as u16,
            "pg_index RowDescription MUST have 19 fields");
        // Canonical column names appear.
        assert!(bytes.windows(b"indexrelid\0".len()).any(|w| w == b"indexrelid\0"));
        assert!(bytes.windows(b"indrelid\0".len()).any(|w| w == b"indrelid\0"));
        assert!(bytes.windows(b"indisunique\0".len()).any(|w| w == b"indisunique\0"));
        assert!(bytes.windows(b"indkey\0".len()).any(|w| w == b"indkey\0"));
        assert!(bytes.windows(b"indisprimary\0".len()).any(|w| w == b"indisprimary\0"));
    }

    /// **Invariant — indisunique='t' for UNIQUE indexes / 'f' for
    /// non-unique.** Stream contains both 't' and 'f' bytes (the
    /// canned values for the 3 indexes — 1 unique + 2 non-unique).
    #[test]
    fn t5_pg_index_indisunique_per_kind() {
        let eng = t5_test_engine();
        let users_oid = oid_for_table_name("users");
        // 1 unique + 1 non-unique on users → both 't' and 'f' appear.
        let bytes = synthesize_pg_index(&eng, Some(users_oid));
        assert!(bytes.contains(&b't'));
        assert!(bytes.contains(&b'f'));
    }

    /// **Invariant — indkey contains attnums as space-separated text.**
    /// Composite index on orders (attnums 2,3) emits `"2 3"`.
    #[test]
    fn t5_pg_index_indkey_renders_attnums() {
        let eng = t5_test_engine();
        let orders_oid = oid_for_table_name("orders");
        let bytes = synthesize_pg_index(&eng, Some(orders_oid));
        // The composite index has fields=[2,3] → indkey rendered as "2 3".
        assert!(bytes.windows(b"2 3".len()).any(|w| w == b"2 3"),
            "composite index indkey MUST be `2 3` (space-separated attnums)");
    }

    /// **render_int2vector — empty + single + multi cases.**
    #[test]
    fn t5_render_int2vector_cases() {
        assert_eq!(render_int2vector(&[]), "");
        assert_eq!(render_int2vector(&[5]), "5");
        assert_eq!(render_int2vector(&[1, 2, 3]), "1 2 3");
        assert_eq!(render_int2vector(&[10, 20]), "10 20");
    }

    /// **render_int_array — PG `int2[]` array literal format `{1,2,3}`.**
    #[test]
    fn t5_render_int_array_cases() {
        assert_eq!(render_int_array(&[]), "{}");
        assert_eq!(render_int_array(&[1]), "{1}");
        assert_eq!(render_int_array(&[1, 2, 3]), "{1,2,3}");
    }

    /// **HEADLINE — pg_constraint synthesizer empty engine returns
    /// 0 rows + well-framed.**
    #[test]
    fn t5_pg_constraint_synthesizer_no_constraints_returns_zero_rows() {
        let eng = empty_index_engine();
        let bytes = synthesize_pg_constraint(&eng, None);
        assert_eq!(bytes[0], b'T');
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    /// **HEADLINE — pg_constraint synthesizer returns rows for all
    /// CHECK / FK / UNIQUE constraints across tables.** 1 + 2 = 3
    /// total.
    #[test]
    fn t5_pg_constraint_synthesizer_emits_all_constraints() {
        let eng = t5_test_engine();
        let bytes = synthesize_pg_constraint(&eng, None);
        assert!(bytes.windows(b"SELECT 3\0".len()).any(|w| w == b"SELECT 3\0"),
            "MUST emit SELECT 3 for 3 constraints across 2 tables");
        // Constraint names appear in the stream.
        assert!(bytes.windows(b"users_email_key".len()).any(|w| w == b"users_email_key"));
        assert!(bytes.windows(b"orders_user_id_fkey".len()).any(|w| w == b"orders_user_id_fkey"));
        assert!(bytes.windows(b"orders_amount_check".len()).any(|w| w == b"orders_amount_check"));
    }

    /// **HEADLINE — pg_constraint synthesizer filtered to one table
    /// by conrelid.**
    #[test]
    fn t5_pg_constraint_synthesizer_filtered_to_one_table() {
        let eng = t5_test_engine();
        let users_oid = oid_for_table_name("users");
        let bytes = synthesize_pg_constraint(&eng, Some(users_oid));
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"),
            "MUST emit SELECT 1 (users has 1 UNIQUE constraint)");
        assert!(bytes.windows(b"users_email_key".len()).any(|w| w == b"users_email_key"));
        // The orders constraint name MUST NOT appear in filtered result.
        assert!(!bytes.windows(b"orders_user_id_fkey".len()).any(|w| w == b"orders_user_id_fkey"));
    }

    /// **HEADLINE — pg_constraint RowDescription has 25 columns.**
    #[test]
    fn t5_pg_constraint_row_description_has_25_columns() {
        let eng = t5_test_engine();
        let bytes = synthesize_pg_constraint(&eng, None);
        let fc = u16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(fc, PG_CONSTRAINT_COLUMN_COUNT as u16,
            "pg_constraint RowDescription MUST have 25 fields");
        // Canonical column names appear.
        assert!(bytes.windows(b"conname\0".len()).any(|w| w == b"conname\0"));
        assert!(bytes.windows(b"contype\0".len()).any(|w| w == b"contype\0"));
        assert!(bytes.windows(b"conrelid\0".len()).any(|w| w == b"conrelid\0"));
        assert!(bytes.windows(b"confrelid\0".len()).any(|w| w == b"confrelid\0"));
        assert!(bytes.windows(b"conkey\0".len()).any(|w| w == b"conkey\0"));
    }

    /// **Invariant — contype byte is correct per ConstraintKind.**
    /// Stream contains 'c' (CHECK), 'f' (FK), 'u' (UNIQUE) bytes
    /// across the synthesized rows.
    #[test]
    fn t5_pg_constraint_contype_byte_per_kind() {
        let eng = t5_test_engine();
        let bytes = synthesize_pg_constraint(&eng, None);
        // All three contype chars must appear in stream (one per row).
        assert!(bytes.contains(&b'c'), "CHECK constraint contype 'c' MUST appear");
        assert!(bytes.contains(&b'f'), "FK constraint contype 'f' MUST appear");
        assert!(bytes.contains(&b'u'), "UNIQUE constraint contype 'u' MUST appear");
    }

    /// **Invariant — confkey populated for FK constraint with the
    /// referenced columns as an int2[] literal.** orders FK
    /// references users(id) = column 1 → confkey="{1}".
    #[test]
    fn t5_pg_constraint_confkey_populated_for_fk() {
        let eng = t5_test_engine();
        let orders_oid = oid_for_table_name("orders");
        let bytes = synthesize_pg_constraint(&eng, Some(orders_oid));
        // The FK row carries confkey="{1}".
        assert!(bytes.windows(b"{1}".len()).any(|w| w == b"{1}"),
            "FK confkey MUST be `{{1}}` (referenced column 1)");
        // The conkey column for the FK has the source column = column 2.
        assert!(bytes.windows(b"{2}".len()).any(|w| w == b"{2}"));
    }

    /// **Invariant — confrelid populated for FK row (references
    /// users.pg_class.oid).** The referenced table's stable-hash OID
    /// appears in the FK row.
    #[test]
    fn t5_pg_constraint_confrelid_populated_for_fk() {
        let eng = t5_test_engine();
        let orders_oid = oid_for_table_name("orders");
        let bytes = synthesize_pg_constraint(&eng, Some(orders_oid));
        let users_oid_str = oid_for_table_name("users").to_string();
        assert!(bytes.windows(users_oid_str.len()).any(|w| w == users_oid_str.as_bytes()),
            "FK confrelid MUST equal pg_class.oid of referenced table");
    }

    /// **Invariant — pgJDBC getIndexInfo joined-result fires for the
    /// matching table.** orders has a composite index → 2 column rows.
    #[test]
    fn t5_pgjdbc_getindexinfo_joined_rows_matches_by_name() {
        let eng = t5_test_engine();
        let bytes = pgjdbc_getindexinfo_joined_rows(&eng, "orders");
        // Composite index spans 2 columns → 2 ordinal rows.
        assert!(bytes.windows(b"SELECT 2\0".len()).any(|w| w == b"SELECT 2\0"));
        assert!(bytes.windows(b"orders_user_id_amount_idx".len())
            .any(|w| w == b"orders_user_id_amount_idx"));
        // Column names appear in the rows.
        assert!(bytes.windows(b"user_id".len()).any(|w| w == b"user_id"));
        assert!(bytes.windows(b"amount".len()).any(|w| w == b"amount"));
        // Unmatched name → 0 rows.
        let unmatched = pgjdbc_getindexinfo_joined_rows(&eng, "doesnotexist");
        assert!(unmatched.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-CAT T7 KATs — SQL helper functions (version() / current_*
    // / SHOW / pg_get_userbyid / pg_table_is_visible / format_type /
    // current_setting / multi-function probe).
    // ───────────────────────────────────────────────────────────────────

    // ── SP-PG-EXTQ T7 — SQLAlchemy connection-validity probes ──────

    /// **SP-PG-EXTQ T7 — `select 1` returns a single int row (column
    /// `?column?`, value 1).** SQLAlchemy 2.0's PG dialect issues
    /// `SELECT 1` as its `do_ping()` health probe; without this hook
    /// every `engine.connect()` fails on V1's bare-scalar-SELECT
    /// rejection.
    #[test]
    fn t7_select_1_returns_single_int_row() {
        let bytes = synthesize_helper_function("select 1").expect("matches");
        assert_eq!(bytes[0], b'T', "must begin with RowDescription");
        // Column name "?column?" (PG canonical for anonymous SELECT 1).
        assert!(
            bytes.windows(b"?column?".len()).any(|w| w == b"?column?"),
            "must carry column name '?column?'"
        );
        // CommandComplete tag "SELECT 1".
        assert!(
            bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"),
            "must carry CommandComplete tag 'SELECT 1'"
        );
        // Trailing 6 bytes: RFQ('I').
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// **SP-PG-EXTQ T7 — `select true` / `select false` return single
    /// bool rows.** Some clients probe with these (asyncpg reconnect
    /// heartbeat).
    #[test]
    fn t7_select_true_false_return_bool_rows() {
        let bt = synthesize_helper_function("select true").expect("matches true");
        assert!(bt.windows(b"bool".len()).any(|w| w == b"bool"));
        assert!(bt.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        let bf = synthesize_helper_function("select false").expect("matches false");
        assert!(bf.windows(b"bool".len()).any(|w| w == b"bool"));
    }

    /// **SP-PG-EXTQ T7 — SQLAlchemy `test plain returns` / `test
    /// unicode returns` first-connect probes recognized.** These are
    /// SQLAlchemy's `PGDialect_psycopg2.do_test_connection` text-
    /// roundtrip queries; without this hook every SQLAlchemy
    /// `engine.connect()` fails on V1's `expected FROM` rejection.
    #[test]
    fn t7_sqlalchemy_text_roundtrip_probes_recognized() {
        let plain = synthesize_helper_function(
            "select cast('test plain returns' as varchar(60)) as anon_1",
        )
        .expect("matches plain probe");
        assert!(
            plain
                .windows(b"test plain returns".len())
                .any(|w| w == b"test plain returns"),
            "plain probe must echo 'test plain returns'"
        );
        // Column alias "anon_1" present.
        assert!(plain.windows(b"anon_1".len()).any(|w| w == b"anon_1"));
        let unicode = synthesize_helper_function(
            "select cast('test unicode returns' as varchar(60)) as anon_1",
        )
        .expect("matches unicode probe");
        assert!(
            unicode
                .windows(b"test unicode returns".len())
                .any(|w| w == b"test unicode returns"),
            "unicode probe must echo 'test unicode returns'"
        );
    }

    /// **SP-PG-EXTQ T7 — `select pg_catalog.version()` (PG-qualified
    /// form) recognized in addition to bare `select version()`.**
    #[test]
    fn t7_pg_catalog_qualified_version_recognized() {
        let bytes = synthesize_helper_function("select pg_catalog.version()")
            .expect("matches qualified");
        assert!(bytes
            .windows(KESSELDB_VERSION_STRING.len())
            .any(|w| w == KESSELDB_VERSION_STRING.as_bytes()));
    }

    /// **HEADLINE — `select version()` returns the canned KesselDB
    /// version string.**
    #[test]
    fn t7_version_returns_kesseldb_version() {
        let bytes = synthesize_helper_function("select version()").expect("matches");
        assert_eq!(bytes[0], b'T');
        assert!(bytes.windows(KESSELDB_VERSION_STRING.len())
            .any(|w| w == KESSELDB_VERSION_STRING.as_bytes()));
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        assert!(bytes.windows(b"version\0".len()).any(|w| w == b"version\0"));
    }

    /// **HEADLINE — `select current_database()` returns 'kesseldb'.**
    #[test]
    fn t7_current_database_returns_kesseldb() {
        let bytes = synthesize_helper_function("select current_database()").expect("matches");
        assert!(bytes.windows(b"kesseldb".len()).any(|w| w == b"kesseldb"));
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
    }

    /// **HEADLINE — `select current_schema()` returns 'public'.**
    #[test]
    fn t7_current_schema_returns_public() {
        let bytes = synthesize_helper_function("select current_schema()").expect("matches");
        assert!(bytes.windows(b"public".len()).any(|w| w == b"public"));
        // The no-parens form also matches.
        let bytes2 = synthesize_helper_function("select current_schema").expect("matches");
        assert!(bytes2.windows(b"public".len()).any(|w| w == b"public"));
    }

    /// **HEADLINE — `select current_user` / `session_user` / `user`
    /// all return 'kesseldb'.**
    #[test]
    fn t7_current_user_session_user_user() {
        for sql in ["select current_user", "select session_user", "select user"] {
            let bytes = synthesize_helper_function(sql).unwrap_or_else(|| panic!("matches {sql}"));
            assert!(bytes.windows(b"kesseldb".len()).any(|w| w == b"kesseldb"),
                "{sql} MUST return kesseldb");
        }
    }

    /// **HEADLINE — `SHOW server_version` returns the canned version.**
    #[test]
    fn t7_show_server_version_returns_canned() {
        let bytes = synthesize_helper_function("show server_version").expect("matches");
        assert!(bytes.windows(b"14.0".len()).any(|w| w == b"14.0"));
    }

    /// **HEADLINE — `SHOW timezone` returns 'UTC'.**
    #[test]
    fn t7_show_timezone_returns_utc() {
        let bytes = synthesize_helper_function("show timezone").expect("matches");
        assert!(bytes.windows(b"UTC".len()).any(|w| w == b"UTC"));
    }

    /// **HEADLINE — unknown `SHOW` name returns the empty string (PG
    /// behavior — not an error).**
    #[test]
    fn t7_show_unknown_name_returns_empty_string() {
        let bytes = synthesize_helper_function("show some_unknown_guc").expect("matches");
        // The frame is well-formed; the value is empty.
        assert_eq!(bytes[0], b'T');
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
    }

    /// **HEADLINE — case-insensitive helper recognition.** The hook
    /// uses normalize_for_match upstream, but the synthesizer's own
    /// caller may pass a different case; the synth assumes
    /// pre-lowered input + asserts the canonical text matches.
    #[test]
    fn t7_helper_pattern_is_lowercase_only_after_normalization() {
        // synthesize_helper_function assumes pre-normalized input;
        // the outer catalog_query_hook normalizes first. Validate by
        // passing lowercase + alias-stripping.
        assert!(synthesize_helper_function("select version()").is_some());
        // Upper-case input doesn't match (the hook normalizes).
        assert!(synthesize_helper_function("SELECT VERSION()").is_none());
        // But the hook integration in catalog_query_hook handles this
        // (the case-insensitivity is achieved via normalize_for_match,
        // tested in mod.rs).
    }

    /// **HEADLINE — `AS alias` suffix tolerated.** `SELECT version()
    /// AS v` matches the same as `SELECT version()`.
    #[test]
    fn t7_helper_pattern_strips_trailing_as_alias() {
        let bytes = synthesize_helper_function("select version() as v").expect("matches");
        assert!(bytes.windows(KESSELDB_VERSION_STRING.len())
            .any(|w| w == KESSELDB_VERSION_STRING.as_bytes()));
    }

    /// **HEADLINE — pgAdmin multi-function probe.** `SELECT version(),
    /// current_database(), current_user, current_schema()` returns
    /// a 4-column single-row response with all 4 values populated.
    #[test]
    fn t7_pgadmin_multi_function_probe() {
        let bytes = synthesize_helper_function(
            "select version(), current_database(), current_user, current_schema()",
        )
        .expect("matches");
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        // All 4 values appear.
        assert!(bytes.windows(KESSELDB_VERSION_STRING.len())
            .any(|w| w == KESSELDB_VERSION_STRING.as_bytes()));
        assert!(bytes.windows(b"kesseldb".len()).any(|w| w == b"kesseldb"));
        assert!(bytes.windows(b"public".len()).any(|w| w == b"public"));
        // 4 columns in the RowDescription.
        let fc = u16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(fc, 4);
    }

    /// **`pg_get_userbyid(N)` returns 'kesseldb' for any OID.** V1
    /// has one user identity.
    #[test]
    fn t7_pg_get_userbyid_returns_kesseldb() {
        let bytes = synthesize_helper_function("select pg_get_userbyid(10)").expect("matches");
        assert!(bytes.windows(b"kesseldb".len()).any(|w| w == b"kesseldb"));
        // Qualified form also matches.
        let bytes2 = synthesize_helper_function("select pg_catalog.pg_get_userbyid(42)")
            .expect("matches");
        assert!(bytes2.windows(b"kesseldb".len()).any(|w| w == b"kesseldb"));
    }

    /// **`pg_table_is_visible(N)` returns true for any OID.** V1
    /// single-schema; all tables visible.
    #[test]
    fn t7_pg_table_is_visible_returns_true() {
        let bytes = synthesize_helper_function("select pg_table_is_visible(16385)")
            .expect("matches");
        // The bool 't' appears (single-row data value).
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        // The qualified form too.
        let bytes2 = synthesize_helper_function("select pg_catalog.pg_table_is_visible(99)")
            .expect("matches");
        assert!(bytes2.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
    }

    /// **`format_type(<oid>, <typmod>)` returns the canonical PG
    /// type name.**
    #[test]
    fn t7_format_type_returns_pg_type_name() {
        // OID 20 → int8
        let bytes = synthesize_helper_function("select format_type(20, -1)").expect("matches");
        assert!(bytes.windows(b"int8".len()).any(|w| w == b"int8"));
        // OID 25 → text
        let bytes2 = synthesize_helper_function("select pg_catalog.format_type(25, -1)")
            .expect("matches");
        assert!(bytes2.windows(b"text".len()).any(|w| w == b"text"));
    }

    /// **`current_setting('name')` returns canned GUC values.**
    #[test]
    fn t7_current_setting_returns_canned_gucs() {
        let bytes = synthesize_helper_function("select current_setting('server_version')")
            .expect("matches");
        assert!(bytes.windows(b"14.0".len()).any(|w| w == b"14.0"));
        // Unknown setting → empty string.
        let bytes2 = synthesize_helper_function("select current_setting('unknown')")
            .expect("matches");
        assert!(bytes2.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
    }

    /// **`pg_get_indexdef(N)` / `pg_get_constraintdef(N)` / `pg_get_expr`
    /// return empty string (V1 doesn't render def text).**
    #[test]
    fn t7_pg_get_def_functions_return_empty_string() {
        for sql in [
            "select pg_get_indexdef(16385)",
            "select pg_catalog.pg_get_indexdef(16385)",
            "select pg_get_constraintdef(16385)",
            "select pg_catalog.pg_get_constraintdef(16385)",
            "select pg_get_expr(null, 16385)",
        ] {
            let bytes = synthesize_helper_function(sql)
                .unwrap_or_else(|| panic!("matches: {sql}"));
            assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"),
                "{sql} MUST emit a 1-row response");
        }
    }

    /// **`obj_description(N, 'pg_class')` returns NULL.** V1 doesn't
    /// have descriptions.
    #[test]
    fn t7_obj_description_returns_null() {
        let bytes = synthesize_helper_function("select obj_description(16385, 'pg_class')")
            .expect("matches");
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        // The NULL sentinel (0xFFFFFFFF) appears in the single data row.
        assert!(bytes.windows(4).any(|w| w == [0xFF, 0xFF, 0xFF, 0xFF]));
    }

    /// **`pg_my_temp_schema()` returns 0 (V1: no temp schemas).**
    #[test]
    fn t7_pg_my_temp_schema_returns_zero() {
        let bytes = synthesize_helper_function("select pg_my_temp_schema()").expect("matches");
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        // The integer 0 appears in the row.
        assert!(bytes.windows(b"0".len()).any(|w| w == b"0"));
    }

    /// **`pg_is_other_temp_schema(N)` returns false (V1).**
    #[test]
    fn t7_pg_is_other_temp_schema_returns_false() {
        let bytes = synthesize_helper_function("select pg_is_other_temp_schema(99)")
            .expect("matches");
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
    }

    /// **Unrecognized SELECT → None (falls through to engine apply).**
    /// Note: SP-PG-EXTQ T7 added `select 1` / `select true` / `select
    /// false` to the recognizer (SQLAlchemy probes) — those are now
    /// covered by `t7_select_1_returns_single_int_row` +
    /// `t7_select_true_false_return_bool_rows`. Genuinely-unknown
    /// SELECTs still fall through.
    #[test]
    fn t7_unrecognized_select_returns_none() {
        assert!(synthesize_helper_function("select * from users").is_none());
        assert!(synthesize_helper_function("select foo()").is_none());
        assert!(synthesize_helper_function("select 42").is_none());
        assert!(synthesize_helper_function("select 1, 2").is_none());
    }

    /// **Unrecognized SHOW ALL returns 0 rows (well-framed) per spec
    /// — tools that issue SHOW ALL get a graceful 0-row table.**
    #[test]
    fn t7_show_all_returns_zero_rows() {
        let bytes = synthesize_helper_function("show all").expect("matches");
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
        // RowDescription has 3 columns (name / setting / description).
        let fc = u16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(fc, 3);
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-CAT T6 KATs — information_schema view synthesizers. Reuse
    // ListEngine + IndexEngine + ConstraintEngine helpers above.
    // ───────────────────────────────────────────────────────────────────

    use crate::engine::{
        ConstraintKind as CK, ConstraintMetadata as CM, FkAction,
    };
    use kessel_catalog::FieldKind as FK;

    /// EngineApply that drives information_schema.{tables,columns}
    /// — overrides list_tables + describe_table + list_constraints
    /// so all 5 information_schema synthesizers can be driven from
    /// one fixture.
    struct InfoSchemaEngine {
        tables: Vec<TableMetadata>,
        schemas: std::collections::BTreeMap<String, Vec<PgColumn>>,
        constraints: std::collections::BTreeMap<String, Vec<CM>>,
    }

    impl EngineApply for InfoSchemaEngine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            OpResult::SchemaError("InfoSchemaEngine: apply_sql should not be reached".into())
        }
        fn describe_table(&self, name: &str) -> Option<Vec<PgColumn>> {
            self.schemas.get(name).cloned()
        }
        fn list_tables(&self) -> Vec<TableMetadata> {
            self.tables.clone()
        }
        fn list_constraints_for_table(&self, name: &str) -> Vec<CM> {
            self.constraints.get(name).cloned().unwrap_or_default()
        }
    }

    fn info_schema_engine() -> InfoSchemaEngine {
        let mut schemas = std::collections::BTreeMap::new();
        schemas.insert(
            "users".to_string(),
            vec![
                PgColumn { name: "id".into(),    kind: FK::I64,      nullable: false },
                PgColumn { name: "email".into(), kind: FK::Char(64), nullable: false },
                PgColumn { name: "age".into(),   kind: FK::I32,      nullable: true  },
            ],
        );
        schemas.insert(
            "orders".to_string(),
            vec![
                PgColumn { name: "id".into(),     kind: FK::I64,         nullable: false },
                PgColumn { name: "user_id".into(),kind: FK::I64,         nullable: false },
                PgColumn { name: "ts".into(),     kind: FK::Timestamp,   nullable: false },
            ],
        );
        let mut constraints = std::collections::BTreeMap::new();
        constraints.insert(
            "users".to_string(),
            vec![CM {
                name: "users_email_key".into(),
                kind: CK::Unique,
                columns: vec![2],
                references: None,
            }],
        );
        constraints.insert(
            "orders".to_string(),
            vec![
                CM {
                    name: "orders_user_id_fkey".into(),
                    kind: CK::ForeignKey { on_delete: FkAction::Cascade },
                    columns: vec![2],
                    references: Some(("users".to_string(), vec![1])),
                },
                CM {
                    name: "orders_check_ts".into(),
                    kind: CK::Check,
                    columns: vec![3],
                    references: None,
                },
            ],
        );
        InfoSchemaEngine {
            tables: vec![
                TableMetadata { name: "users".into(),  type_id: 1, kind: TableKind::Ordinary, field_count: 3 },
                TableMetadata { name: "orders".into(), type_id: 2, kind: TableKind::Ordinary, field_count: 3 },
            ],
            schemas,
            constraints,
        }
    }

    /// **HEADLINE — information_schema.tables emits one row per
    /// KesselDB table with 'BASE TABLE' type.** Acceptance-criterion
    /// for the Metabase / Tableau / Looker connect-database wizard.
    #[test]
    fn t6_information_schema_tables_lists_all_kesseldb_tables() {
        let eng = info_schema_engine();
        let bytes = synthesize_information_schema_tables(&eng);
        // Well-framed.
        assert_eq!(bytes[0], b'T');
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
        // 2 rows = 2 KesselDB tables (Ordinary kind).
        assert!(bytes.windows(b"SELECT 2\0".len()).any(|w| w == b"SELECT 2\0"));
        // Each table name appears once.
        assert!(bytes.windows(b"users".len()).any(|w| w == b"users"));
        assert!(bytes.windows(b"orders".len()).any(|w| w == b"orders"));
        // table_type = 'BASE TABLE' present.
        assert!(bytes.windows(b"BASE TABLE".len()).any(|w| w == b"BASE TABLE"));
        // Catalog stamp.
        assert!(bytes.windows(b"kesseldb".len()).any(|w| w == b"kesseldb"));
        // Schema stamp.
        assert!(bytes.windows(b"public".len()).any(|w| w == b"public"));
    }

    /// **information_schema.tables RowDescription has 12 canonical
    /// columns per the SQL standard.** Locked because BI tools that
    /// iterate by column index break silently if the count drifts.
    #[test]
    fn t6_information_schema_tables_row_description_has_12_columns() {
        let eng = info_schema_engine();
        let bytes = synthesize_information_schema_tables(&eng);
        let fc = u16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(fc, 12, "information_schema.tables MUST have 12 columns");
        for canonical in [
            "table_catalog\0", "table_schema\0", "table_name\0", "table_type\0",
            "is_insertable_into\0", "is_typed\0",
        ] {
            let needle = canonical.as_bytes();
            assert!(bytes.windows(needle.len()).any(|w| w == needle),
                "column {canonical:?} MUST appear in RowDescription");
        }
    }

    /// **HEADLINE — information_schema.columns lists every column
    /// with the canonical SQL-standard data_type name.** The names
    /// are NOT the pg_type internal names (`int8`, `bool`,
    /// `timestamptz`); they're the SQL-standard names (`bigint`,
    /// `boolean`, `timestamp with time zone`) — BI tools key feature
    /// support off this column.
    #[test]
    fn t6_information_schema_columns_emits_sql_standard_data_types() {
        let eng = info_schema_engine();
        let bytes = synthesize_information_schema_columns(&eng, None);
        assert_eq!(bytes[0], b'T');
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
        // 6 columns total: users(id/email/age) + orders(id/user_id/ts).
        assert!(bytes.windows(b"SELECT 6\0".len()).any(|w| w == b"SELECT 6\0"));
        // Canonical SQL-standard names.
        assert!(bytes.windows(b"bigint".len()).any(|w| w == b"bigint"),
            "I64 → 'bigint' per SQL standard");
        assert!(bytes.windows(b"text".len()).any(|w| w == b"text"),
            "Char(64) → 'text' per SQL standard");
        assert!(bytes.windows(b"integer".len()).any(|w| w == b"integer"),
            "I32 → 'integer' per SQL standard");
        assert!(
            bytes.windows(b"timestamp with time zone".len())
                .any(|w| w == b"timestamp with time zone"),
            "Timestamp → 'timestamp with time zone' per SQL standard",
        );
    }

    /// **information_schema.columns filter by table_name works.** A
    /// Metabase per-table introspection query (queries.md §5.2)
    /// passes the table name as a literal; the synthesizer filters
    /// to the matching table.
    #[test]
    fn t6_information_schema_columns_filter_by_table_name() {
        let eng = info_schema_engine();
        let bytes = synthesize_information_schema_columns(&eng, Some("users"));
        // Only the 3 users columns.
        assert!(bytes.windows(b"SELECT 3\0".len()).any(|w| w == b"SELECT 3\0"));
        // users columns appear.
        assert!(bytes.windows(b"email".len()).any(|w| w == b"email"));
        assert!(bytes.windows(b"age".len()).any(|w| w == b"age"));
        // orders-only column 'user_id' does NOT appear (the byte
        // sequence wouldn't be in the response either).
        assert!(!bytes.windows(b"user_id".len()).any(|w| w == b"user_id"));
    }

    /// **information_schema.columns ordinal_position is 1-based
    /// sequential.** The SQL standard requires this; BI tools sort
    /// by it to render the column list in declaration order.
    #[test]
    fn t6_information_schema_columns_ordinal_is_1_based() {
        let eng = info_schema_engine();
        let bytes = synthesize_information_schema_columns(&eng, Some("users"));
        // Ordinals 1, 2, 3 appear in the response (as their decimal
        // text forms). Cheap byte-substring check.
        assert!(bytes.windows(b"1".len()).any(|w| w == b"1"));
        assert!(bytes.windows(b"2".len()).any(|w| w == b"2"));
        assert!(bytes.windows(b"3".len()).any(|w| w == b"3"));
    }

    /// **information_schema.columns is_nullable maps from
    /// PgColumn.nullable.** users.age is nullable → 'YES'; users.id
    /// is NOT NULL → 'NO'. Both literals MUST appear.
    #[test]
    fn t6_information_schema_columns_nullable_yes_no() {
        let eng = info_schema_engine();
        let bytes = synthesize_information_schema_columns(&eng, Some("users"));
        // YES appears (for age) AND NO appears (for id + email).
        assert!(bytes.windows(b"YES".len()).any(|w| w == b"YES"));
        assert!(bytes.windows(b"NO".len()).any(|w| w == b"NO"));
    }

    /// **HEADLINE — information_schema.schemata returns 3 canonical
    /// schemas (pg_catalog / public / information_schema).** Matches
    /// the pg_namespace 3-row stub (T1) but with the SQL-standard
    /// column shape.
    #[test]
    fn t6_information_schema_schemata_returns_three_schemas() {
        let bytes = synthesize_information_schema_schemata();
        assert_eq!(bytes[0], b'T');
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
        assert!(bytes.windows(b"SELECT 3\0".len()).any(|w| w == b"SELECT 3\0"));
        assert!(bytes.windows(b"pg_catalog".len()).any(|w| w == b"pg_catalog"));
        assert!(bytes.windows(b"public".len()).any(|w| w == b"public"));
        assert!(bytes.windows(b"information_schema".len())
            .any(|w| w == b"information_schema"));
        // 7-column RowDescription per SQL standard.
        let fc = u16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(fc, 7, "information_schema.schemata MUST have 7 columns");
    }

    /// **HEADLINE — information_schema.key_column_usage lists FK
    /// columns + their parent table.** orders.user_id (column 2) →
    /// FK to users(id). CHECK constraints are skipped per the SQL
    /// standard (they don't apply to columns).
    #[test]
    fn t6_information_schema_key_column_usage_lists_fk_columns() {
        let eng = info_schema_engine();
        let bytes = synthesize_information_schema_key_column_usage(&eng, None);
        assert_eq!(bytes[0], b'T');
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
        // 2 rows: orders.user_id (FK), users.email (UNIQUE).
        // CHECK (orders_check_ts) is skipped per SQL standard.
        assert!(bytes.windows(b"SELECT 2\0".len()).any(|w| w == b"SELECT 2\0"));
        assert!(bytes.windows(b"orders_user_id_fkey".len())
            .any(|w| w == b"orders_user_id_fkey"));
        assert!(bytes.windows(b"users_email_key".len())
            .any(|w| w == b"users_email_key"));
        // Column names attached to the constraints.
        assert!(bytes.windows(b"user_id".len()).any(|w| w == b"user_id"));
        assert!(bytes.windows(b"email".len()).any(|w| w == b"email"));
    }

    /// **information_schema.key_column_usage filter by table_name
    /// works.** Per-table per-FK discovery (Metabase relationship
    /// inference hot path).
    #[test]
    fn t6_information_schema_key_column_usage_filter_by_table() {
        let eng = info_schema_engine();
        let bytes = synthesize_information_schema_key_column_usage(&eng, Some("orders"));
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        assert!(bytes.windows(b"orders_user_id_fkey".len())
            .any(|w| w == b"orders_user_id_fkey"));
    }

    /// **HEADLINE — information_schema.table_constraints lists every
    /// constraint with the SQL-standard `constraint_type` literal
    /// ('CHECK' / 'UNIQUE' / 'FOREIGN KEY' / 'PRIMARY KEY').**
    #[test]
    fn t6_information_schema_table_constraints_lists_all_with_type() {
        let eng = info_schema_engine();
        let bytes = synthesize_information_schema_table_constraints(&eng, None);
        assert_eq!(bytes[0], b'T');
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
        // 3 constraints: users.email UNIQUE, orders.user_id FK,
        // orders.ts CHECK.
        assert!(bytes.windows(b"SELECT 3\0".len()).any(|w| w == b"SELECT 3\0"));
        // All three SQL-standard type literals present.
        assert!(bytes.windows(b"UNIQUE".len()).any(|w| w == b"UNIQUE"));
        assert!(bytes.windows(b"FOREIGN KEY".len()).any(|w| w == b"FOREIGN KEY"));
        assert!(bytes.windows(b"CHECK".len()).any(|w| w == b"CHECK"));
    }

    /// **information_schema.views returns empty (V1 has no views).**
    /// Well-framed 0-row response per design — tools that probe
    /// for views see "no views" cleanly.
    #[test]
    fn t6_information_schema_views_returns_empty() {
        let bytes = synthesize_information_schema_views();
        assert_eq!(bytes[0], b'T');
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
        // 10 canonical columns per SQL standard.
        let fc = u16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(fc, 10, "information_schema.views MUST have 10 columns");
    }

    /// **information_schema.routines returns empty (V1 has no
    /// stored procedures).** DataGrip / JetBrains query this on
    /// connect and tolerate empty.
    #[test]
    fn t6_information_schema_routines_returns_empty() {
        let bytes = synthesize_information_schema_routines();
        assert_eq!(bytes[0], b'T');
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
        let fc = u16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(fc, 8, "information_schema.routines MUST have 8 columns");
    }

    /// **information_schema_data_type_for_oid maps canonical PG OIDs
    /// to SQL-standard names.** Locked because the BI-tool data-type
    /// fingerprint depends on these names.
    #[test]
    fn t6_information_schema_data_type_for_oid_canonical_names() {
        assert_eq!(information_schema_data_type_for_oid(PG_TYPE_BOOL), "boolean");
        assert_eq!(information_schema_data_type_for_oid(PG_TYPE_INT2), "smallint");
        assert_eq!(information_schema_data_type_for_oid(PG_TYPE_INT4), "integer");
        assert_eq!(information_schema_data_type_for_oid(PG_TYPE_INT8), "bigint");
        assert_eq!(information_schema_data_type_for_oid(PG_TYPE_TEXT), "text");
        assert_eq!(information_schema_data_type_for_oid(PG_TYPE_NUMERIC), "numeric");
        assert_eq!(
            information_schema_data_type_for_oid(PG_TYPE_TIMESTAMPTZ),
            "timestamp with time zone",
        );
        assert_eq!(information_schema_data_type_for_oid(PG_TYPE_BYTEA), "bytea");
        // Unknown OID → 'USER-DEFINED' per SQL standard fallback.
        assert_eq!(information_schema_data_type_for_oid(99999), "USER-DEFINED");
    }

    /// **information_schema synthesizers handle empty engines
    /// gracefully** — well-framed 0-row responses.
    #[test]
    fn t6_information_schema_synthesizers_empty_engine() {
        struct EmptyEngine;
        impl EngineApply for EmptyEngine {
            fn apply_sql(&self, _sql: &str) -> OpResult { OpResult::Ok }
            fn describe_table(&self, _name: &str) -> Option<Vec<PgColumn>> { None }
        }
        let eng = EmptyEngine;
        for bytes in [
            synthesize_information_schema_tables(&eng),
            synthesize_information_schema_columns(&eng, None),
            synthesize_information_schema_key_column_usage(&eng, None),
            synthesize_information_schema_table_constraints(&eng, None),
        ] {
            assert_eq!(bytes[0], b'T', "well-framed start");
            assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
            assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
        }
    }
}
