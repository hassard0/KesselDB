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
}
