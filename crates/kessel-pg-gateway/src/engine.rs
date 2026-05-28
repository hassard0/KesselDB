//! `EngineApply` trait — the dispatch boundary between the PG-wire
//! gateway and KesselDB's engine.
//!
//! **T8 status (this commit):** defines the trait V1 needs. The
//! `kesseldb-server::EngineHandle` will `impl EngineApply for
//! EngineHandle` under a future `pg-gateway` feature gate (T12), so
//! the gateway can stay zero-dep + the dependency direction stays
//! one-way (`kesseldb-server` → `kessel-pg-gateway`, never the
//! reverse — same shape as `kessel-http-gateway::engine`).
//!
//! The trait has TWO methods:
//!
//! 1. `apply_sql(sql) -> OpResult` — the existing dispatch path
//!    (mirrors `kessel-http-gateway::EngineApply::apply_sql`).
//! 2. `describe_table(name) -> Option<Vec<(String, FieldKind)>>` —
//!    schema lookup the gateway needs BEFORE the SELECT path can
//!    emit `RowDescription`. Returns columns in declared order,
//!    paired with their KesselDB `FieldKind` (the PG OID is
//!    derived via `crate::types::field_kind_to_oid`).
//!
//! `describe_table` is pure read-only — no engine apply, no
//! mutation, no transaction. The implementation in `kesseldb-server`
//! reads from the live `Catalog` directly (the same data Op::Describe
//! returns, but keyed by name instead of type_id).
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`

#![forbid(unsafe_code)]
#![allow(dead_code)]

use kessel_catalog::FieldKind;
use kessel_proto::OpResult;

/// One column in a table's schema: declared name + KesselDB type
/// kind. The gateway converts the kind to a PG type OID via
/// `crate::types::field_kind_to_oid` at RowDescription emit time.
///
/// `nullable` matters at INSERT validation (NOT NULL violations
/// surface as `OpResult::Constraint`) but doesn't affect the wire
/// format of RowDescription — PG carries nullability in
/// `pg_attribute.attnotnull`, which V1 doesn't expose because we
/// don't ship `pg_catalog` (V2 SP-PG-PGCATALOG).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgColumn {
    pub name: String,
    pub kind: FieldKind,
    pub nullable: bool,
}

/// SP-PG-CAT T3 — metadata for one KesselDB table, surfaced via
/// `EngineApply::list_tables()` to the `pg_class` synthesizer.
///
/// Carries just enough to fill the V1 `pg_class` rows:
///
/// - `name` — `pg_class.relname` (also drives the stable-hash OID).
/// - `type_id` — KesselDB's internal type identifier. Kept here for
///   forward compatibility (T4 `pg_attribute` may JOIN on it to
///   reduce engine round-trips); the V1 `pg_class` synthesizer
///   ignores it (OIDs are name-derived per design spec §3.7).
/// - `kind` — `pg_class.relkind` ('r' for ordinary tables, 'i' for
///   indexes — V1 only emits 'r').
/// - `field_count` — `pg_class.relnatts` (number of user columns).
///
/// The struct is deliberately minimal — anything else (column
/// list, index list) round-trips through `describe_table` so this
/// list is cheap to assemble and cheap to clone per query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMetadata {
    pub name: String,
    pub type_id: u32,
    pub kind: TableKind,
    pub field_count: u16,
}

/// SP-PG-CAT T3 — the V1 `pg_class.relkind` shape that maps to
/// each KesselDB catalog entry. KesselDB V1 only has ordinary
/// tables, so `list_tables()` always returns `Ordinary` today;
/// the other variants are listed so a later catalog evolution
/// (materialized views, sequences) plugs in cleanly without a
/// breaking `EngineApply` change.
///
/// Maps to PG canonical `relkind` chars per `src/include/catalog/
/// pg_class.h`:
///
/// | TableKind | relkind | PG meaning |
/// |---|---|---|
/// | `Ordinary` | 'r' | ordinary table |
/// | `Index` | 'i' | index (V1: not emitted via list_tables — indexes are not first-class catalog entries in KesselDB) |
/// | `View` | 'v' | view (V1: KesselDB has no views) |
/// | `Sequence` | 'S' | sequence (V1: KesselDB has no sequences) |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKind {
    Ordinary,
    Index,
    View,
    Sequence,
}

impl TableKind {
    /// Map to the canonical PG `pg_class.relkind` char per
    /// `src/include/catalog/pg_class.h`. Used by `pg_class` synth.
    pub fn pg_relkind(self) -> u8 {
        match self {
            TableKind::Ordinary => b'r',
            TableKind::Index => b'i',
            TableKind::View => b'v',
            TableKind::Sequence => b'S',
        }
    }
}

/// Dispatch boundary the PG-wire gateway uses to talk to the engine.
///
/// Implemented by `kesseldb-server::EngineHandle` under a future
/// `pg-gateway` feature gate (T12). The trait is `Send + Sync +
/// 'static` so the per-connection thread can hold an
/// `Arc<dyn EngineApply>` without lifetime gymnastics — same shape as
/// `kessel-http-gateway::EngineApply`.
///
/// V1 has only two methods. T9 may add `apply_sql_with_session` (for
/// exactly-once dedup on long-running PG connections) — deferred
/// until a real client needs it.
pub trait EngineApply: Send + Sync + 'static {
    /// Apply raw SQL text. The engine compiles against the live
    /// catalog on its dedicated thread and returns the result. V1
    /// uses this as the catch-all dispatch — the gateway parses the
    /// SQL leading keyword (SELECT/INSERT/UPDATE/DELETE/CREATE/DROP/
    /// SET) ONLY for the CommandComplete tag inference, not for
    /// routing.
    fn apply_sql(&self, sql: &str) -> OpResult;

    /// Look up a table's columns by name. Returns `None` if no table
    /// with that name exists. Used by the gateway to emit
    /// `RowDescription` BEFORE a SELECT runs.
    ///
    /// **Read-only invariant:** this method MUST NOT mutate engine
    /// state, advance op-number, or take a snapshot. The
    /// `kesseldb-server` impl reads from the live `Catalog` without
    /// going through the apply path.
    ///
    /// **V1 limitations** (documented for the design audit):
    /// - Schema/database namespacing is not supported (PG's `schema.
    ///   table` notation collapses to bare `table`). V2 SP-PG-NS
    ///   would add it.
    /// - Lookup is case-sensitive (KesselDB table names ARE case-
    ///   sensitive even though PG normally folds unquoted identifiers
    ///   to lowercase). V2 follow-up.
    fn describe_table(&self, table_name: &str) -> Option<Vec<PgColumn>>;

    /// T9 — Apply SQL and ALSO surface the number of affected rows.
    /// Default impl returns count=1 for any `Ok`-shaped success and
    /// count=0 for any error / `NotFound`, which is accurate for
    /// single-row INSERT / UPDATE / DELETE on the ID-fast-path (the
    /// V1 grammar's hot DML shape — `INSERT INTO t (id, ...) VALUES
    /// (...)`, `UPDATE t ID <n> SET ...`, `DELETE FROM t ID <n>`).
    ///
    /// **Lossy edge** (acknowledged): multi-row INSERT VALUES tuples
    /// compile into one atomic `Op::Txn` whose `OpResult::Ok` doesn't
    /// carry a count — the gateway recovers N by counting top-level
    /// `(...)` tuples in the SQL text via `dispatch::count_insert_values`.
    /// WHERE-clause UPDATE/DELETE (V2 SP-SQL extension) would land
    /// here lossy at count=1 until either a real `affected_rows` field
    /// lands on `OpResult::Ok` (V2 enhancement) or the engine routes
    /// such ops through `Op::Txn`.
    ///
    /// Default impl is provided so existing `EngineApply` impls don't
    /// have to change at the T9 commit boundary — the
    /// `kesseldb-server::EngineHandle` impl (T12) can override for the
    /// `Op::Txn`-returns-`TxCommitted` path.
    fn apply_sql_with_count(&self, sql: &str) -> (OpResult, u64) {
        let r = self.apply_sql(sql);
        let count = match &r {
            OpResult::Ok | OpResult::TxCommitted { .. } => 1,
            _ => 0,
        };
        (r, count)
    }

    /// SP-PG-CAT T3 — enumerate every user-visible table in the
    /// live KesselDB catalog. Used by the `pg_class` synthesizer to
    /// emit one `pg_class` row per table.
    ///
    /// **Default impl returns empty `Vec`** — engines that don't
    /// implement this gracefully fall back to a no-rows `pg_class`
    /// response (psql `\dt` prints "did not find any relations").
    /// The default lets SP-PG-CAT T3 land before / independent of
    /// any engine-side work — the in-tree `kesseldb-server::EngineHandle`
    /// overrides per design §5.2.
    ///
    /// **Read-only invariant** — same shape as `describe_table`:
    /// this method MUST NOT mutate engine state, advance op-number,
    /// or take a snapshot. The `kesseldb-server` impl reads from the
    /// live `Catalog` without going through the apply path (via
    /// the `LIST_TABLES_TAG` admin frame, mirroring
    /// `DESCRIBE_BY_NAME_TAG`).
    ///
    /// **Listing order** — tables are returned in catalog
    /// declaration order (the same order `Catalog.types` carries
    /// them). The `pg_class` synthesizer further orders rows for
    /// stable wire output, but the trait MAKES NO ordering
    /// promise — V2 may sort by name for human-friendly output.
    fn list_tables(&self) -> Vec<TableMetadata> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pure in-memory `EngineApply` for KAT use. Holds canned
    /// describe_table responses + a closure-driven apply_sql so a
    /// test can dictate exactly what the engine will return.
    pub(crate) struct MockEngine {
        pub schema: std::collections::BTreeMap<String, Vec<PgColumn>>,
        pub apply: std::sync::Mutex<Vec<OpResult>>,
    }

    impl EngineApply for MockEngine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            self.apply
                .lock()
                .unwrap()
                .pop()
                .unwrap_or(OpResult::SchemaError("no canned result".into()))
        }
        fn describe_table(&self, table_name: &str) -> Option<Vec<PgColumn>> {
            self.schema.get(table_name).cloned()
        }
    }

    /// `describe_table` on a known table returns the right columns
    /// in declared order.
    #[test]
    fn t8_describe_table_returns_columns_in_order() {
        let mut schema = std::collections::BTreeMap::new();
        schema.insert(
            "users".to_string(),
            vec![
                PgColumn {
                    name: "id".into(),
                    kind: FieldKind::I64,
                    nullable: false,
                },
                PgColumn {
                    name: "name".into(),
                    kind: FieldKind::Char(64),
                    nullable: true,
                },
            ],
        );
        let eng = MockEngine {
            schema,
            apply: std::sync::Mutex::new(Vec::new()),
        };
        let cols = eng.describe_table("users").expect("table exists");
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].kind, FieldKind::I64);
        assert!(!cols[0].nullable);
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].kind, FieldKind::Char(64));
        assert!(cols[1].nullable);
    }

    /// `describe_table` on a missing table returns `None`.
    #[test]
    fn t8_describe_table_missing_returns_none() {
        let eng = MockEngine {
            schema: std::collections::BTreeMap::new(),
            apply: std::sync::Mutex::new(Vec::new()),
        };
        assert!(eng.describe_table("nope").is_none());
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-CAT T3 KATs — `list_tables()` trait extension + the
    // `TableMetadata` / `TableKind` shape.
    // ───────────────────────────────────────────────────────────────────

    /// **Default-impl invariant:** the trait's default `list_tables()`
    /// returns an empty Vec, so an engine that doesn't override gets
    /// a graceful empty-pg_class response (psql `\dt` says
    /// "did not find any relations" instead of crashing). Locked
    /// because any future change that drops the default would
    /// silently break every existing `EngineApply` impl outside the
    /// in-tree `kesseldb-server` one.
    #[test]
    fn t3_list_tables_default_impl_returns_empty_vec() {
        // MockEngine inherits the default — it does NOT override
        // list_tables(). The empty result confirms the default fires.
        let eng = MockEngine {
            schema: std::collections::BTreeMap::new(),
            apply: std::sync::Mutex::new(Vec::new()),
        };
        let tables = eng.list_tables();
        assert!(
            tables.is_empty(),
            "default list_tables() MUST return an empty Vec — got {} entries",
            tables.len()
        );
    }

    /// **TableKind → relkind char lock:** the four V1 TableKind
    /// variants map to the canonical PG `pg_class.relkind` chars per
    /// `src/include/catalog/pg_class.h`. If a future refactor
    /// renumbers these, every PG client's CASE-on-relkind logic
    /// (psql `\dt`, JDBC `getTables`, pgcli `tables()`) silently
    /// breaks because they switch on the literal byte.
    #[test]
    fn t3_table_kind_maps_to_canonical_pg_relkind_chars() {
        assert_eq!(TableKind::Ordinary.pg_relkind(), b'r',
            "ordinary table is 'r' per pg_class.h");
        assert_eq!(TableKind::Index.pg_relkind(), b'i',
            "index is 'i' per pg_class.h");
        assert_eq!(TableKind::View.pg_relkind(), b'v',
            "view is 'v' per pg_class.h");
        assert_eq!(TableKind::Sequence.pg_relkind(), b'S',
            "sequence is 'S' (capital!) per pg_class.h");
    }

    /// **TableMetadata shape lock:** all four fields are populated +
    /// the struct is Clone + PartialEq for KAT-friendly assertions.
    /// Locked because the `pg_class` synthesizer assumes this exact
    /// shape (name → relname/OID; type_id → kept for forward
    /// compat; kind → relkind; field_count → relnatts).
    #[test]
    fn t3_table_metadata_carries_v1_pg_class_columns() {
        let md = TableMetadata {
            name: "users".to_string(),
            type_id: 7,
            kind: TableKind::Ordinary,
            field_count: 3,
        };
        assert_eq!(md.name, "users");
        assert_eq!(md.type_id, 7);
        assert_eq!(md.kind, TableKind::Ordinary);
        assert_eq!(md.field_count, 3);
        // Clone + PartialEq round-trip — used by every KAT below.
        let md2 = md.clone();
        assert_eq!(md, md2);
    }

    /// **Engine can override** — MockEngine doesn't override (the
    /// default returns empty), but a wrapper that DOES override
    /// surfaces its tables through the trait method. Locks the
    /// dispatch path the `kesseldb-server::EngineHandle` impl uses.
    #[test]
    fn t3_list_tables_overridable_via_trait_impl() {
        struct OverridingEngine;
        impl EngineApply for OverridingEngine {
            fn apply_sql(&self, _sql: &str) -> OpResult {
                OpResult::SchemaError("not used".into())
            }
            fn describe_table(&self, _name: &str) -> Option<Vec<PgColumn>> {
                None
            }
            fn list_tables(&self) -> Vec<TableMetadata> {
                vec![
                    TableMetadata {
                        name: "users".to_string(),
                        type_id: 1,
                        kind: TableKind::Ordinary,
                        field_count: 2,
                    },
                    TableMetadata {
                        name: "orders".to_string(),
                        type_id: 2,
                        kind: TableKind::Ordinary,
                        field_count: 5,
                    },
                ]
            }
        }
        let eng = OverridingEngine;
        let tables = eng.list_tables();
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "users");
        assert_eq!(tables[1].name, "orders");
        assert_eq!(tables[0].kind, TableKind::Ordinary);
        assert_eq!(tables[1].field_count, 5);
    }
}
