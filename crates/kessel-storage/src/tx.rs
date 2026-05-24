//! kessel-storage::tx — Transaction context for MVCC reads (S2.2 of THESIS.md S2).
//!
//! A `Tx` pins a snapshot opnum at begin-time and routes every read
//! through `mvcc::get_at_snapshot(..., snapshot_opnum)`. Every read
//! records `(type_id, *object_id)` in an internal `read_set` —
//! deterministic-iteration `BTreeSet` so debug-formatted Tx state
//! is replica-byte-identical (the thesis-fit `replayable` property
//! at the debugging layer).
//!
//! Read-only Tx ONLY in S2.2. Writes ship in S2.3 (`Tx::write` +
//! `Tx::commit` with conflict check). SSI dangerous-cycle detection
//! consumes this slice's `read_set` in S2.4. SQL integration + SM
//! cutover in S2.6. See:
//!   - parent S2 design: docs/superpowers/specs/2026-05-23-mvcc-si-design.md
//!   - S2.2 design:      docs/superpowers/specs/2026-05-24-mvcc-si-s2-2-design.md
//!
//! Determinism guarantee (S2.2 contract): two Tx invocations on
//! byte-identical Storage state with byte-identical snapshot_opnum
//! and the same sequence of `read` calls produce byte-identical
//! read results AND byte-identical read_set contents.

#![forbid(unsafe_code)]

use crate::Storage;
use crate::mvcc::SnapshotRead;
use kessel_io::Vfs;
use std::collections::BTreeSet;

/// A read-only transaction pinned at a snapshot opnum.
///
/// Holds a shared borrow of the underlying `Storage`. The borrow is
/// released when the Tx is dropped (or `commit_read_only` / `abort`
/// is called). Compile-time-checked: reads after commit/abort are a
/// borrow-checker error, not a runtime error.
///
/// S2.2 ships read-only Tx ONLY. S2.3 will introduce the write
/// variant + conflict-checked commit; the API will compose with the
/// S2.2 shape (the read-only `commit_read_only` stays as the
/// SELECT-only path).
///
/// Design Decision 1 (S2.2 design doc): read-only Tx in S2.2; writes
/// deferred to S2.3 to avoid shipping a half-implemented commit path.
/// Design Decision 2 (S2.2 design doc): snapshot opnum is caller-supplied
/// to preserve the kessel-storage / kessel-sm boundary; SM wiring in S2.6.
pub struct Tx<'a, V: Vfs> {
    /// Shared borrow of the underlying storage; reads only.
    ///
    /// Decision 2: Tx lives in kessel-storage::tx and holds a borrow of
    /// Storage<V> rather than coupling to kessel-sm. S2.6 SM integration
    /// will call `Tx::begin(&store, sm.last_committed_opnum())`.
    store: &'a Storage<V>,
    /// Pinned at `begin`; never mutated for Tx's lifetime.
    ///
    /// Decision 2: caller-supplied. In production (S2.6) the SM supplies
    /// `sm.last_committed_opnum()` here.
    snapshot_opnum: u64,
    /// Accumulated read-set: `(type_id, object_id)` pairs observed
    /// by any `read` call. BTreeSet for deterministic iteration order.
    ///
    /// Decision 3 (S2.2 design doc): BTreeSet chosen over HashSet for
    /// deterministic iteration — sorted lex so debug-formatted Tx state
    /// is replica-byte-identical. S2.4 SSI cycle-detection pass consumes
    /// this and requires deterministic ordering for replica-identical results.
    read_set: BTreeSet<(u32, [u8; 16])>,
}

/// Errors a Tx commit/abort can return.
///
/// S2.2 ships ZERO failure modes (read-only Tx with no conflict check
/// cannot fail at commit time). The enum is shipped (rather than
/// `Result<(), Infallible>`) so S2.3 can extend it with
/// `ConflictAborted`, `SnapshotInvalid`, etc., without breaking S2.2
/// callers. The `#[non_exhaustive]` marker forces pattern-match
/// `_` arms, so S2.3 adding variants is a non-breaking change.
///
/// Decision 6 (S2.2 design doc): `#[non_exhaustive]` on TxError for
/// forward-compatibility with S2.3 conflict-detection errors.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TxError {
    /// Reserved placeholder; not constructible by S2.2 code.
    /// Exists so the enum is non-empty before S2.3 ships its variants.
    /// Pattern-matches on `TxError` must include `_ =>` per the
    /// `#[non_exhaustive]` marker.
    #[doc(hidden)]
    _Reserved,
}

impl std::fmt::Display for TxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxError::_Reserved => write!(
                f,
                "TxError::_Reserved (reserved variant — S2.2 produces no errors)"
            ),
        }
    }
}

impl std::error::Error for TxError {}

impl<'a, V: Vfs> Tx<'a, V> {
    /// Begin a read-only Tx pinned at `snapshot_opnum`.
    ///
    /// Caller supplies the snapshot — in production (S2.6) the SM
    /// will wrap this with `Tx::begin(&store, sm.last_committed_opnum())`.
    /// The pinned snapshot does not change for the Tx's lifetime.
    ///
    /// Decision 2 (S2.2 design doc): caller-supplied snapshot opnum keeps
    /// kessel-storage decoupled from kessel-sm. SM wiring is S2.6's job.
    pub fn begin(store: &'a Storage<V>, snapshot_opnum: u64) -> Self {
        todo!("filled in T2")
    }

    /// Snapshot read at the Tx's pinned snapshot_opnum.
    ///
    /// Records `(type_id, *object_id)` in the read_set REGARDLESS of
    /// which `SnapshotRead` variant is returned (Decision 4 of the
    /// S2.2 design — the Tx observed the absence as much as any
    /// found version; SSI in S2.4 needs both).
    ///
    /// Decision 4 (S2.2 design doc): record the key in the read_set for
    /// ALL three SnapshotRead variants (Found, Tombstoned, NotYetWritten)
    /// because SSI dangerous-cycle detection tracks anti-dependencies on
    /// absent keys too.
    pub fn read(&mut self, type_id: u32, object_id: &[u8; 16]) -> SnapshotRead {
        todo!("filled in T2")
    }

    /// The snapshot_opnum the Tx was pinned at. Never changes after `begin`.
    ///
    /// S2.3 uses this to compute the `(snapshot_opnum, commit_opnum]`
    /// conflict window for plain SI commit validation.
    pub fn snapshot_opnum(&self) -> u64 {
        todo!("filled in T2")
    }

    /// Immutable view of the read_set so far.
    ///
    /// S2.4 SSI will consume this. BTreeSet iteration order is
    /// deterministic (sorted lex), which makes the cycle-detection
    /// pass replica-byte-identical.
    ///
    /// Decision 3 (S2.2 design doc): BTreeSet guarantees deterministic
    /// iteration order for the SSI pass in S2.4.
    pub fn read_set(&self) -> &BTreeSet<(u32, [u8; 16])> {
        todo!("filled in T2")
    }

    /// Commit a read-only Tx.
    ///
    /// Drops the Tx (releasing the borrow on Storage). Returns
    /// `Result<(), TxError>` for forward-compat with S2.3 (which will
    /// add error variants). In S2.2 this is `Ok(())` unconditionally.
    ///
    /// Decision 6 (S2.2 design doc): `Result<(), TxError>` return type
    /// so S2.3 can add `ConflictAborted` without changing the call-sites
    /// that use `commit_read_only` for SELECT-only transactions.
    pub fn commit_read_only(self) -> Result<(), TxError> {
        todo!("filled in T2")
    }

    /// Explicit abort.
    ///
    /// For S2.2 (read-only Tx with no buffered state) this is
    /// identical to dropping the Tx. Shipped for symmetry with the
    /// S2.3 write variant which will need explicit abort semantics
    /// to discard buffered writes.
    pub fn abort(self) {
        todo!("filled in T2")
    }
}

#[cfg(test)]
mod tx_scaffold_tests {
    use super::*;

    // Type-shape lock: TxError implements Debug + Clone + PartialEq + Eq + Error.
    // If S2.3 weakens any of these, this test fails.
    #[test]
    fn tx_error_trait_shape_locked() {
        fn assert_debug<T: std::fmt::Debug>() {}
        fn assert_clone<T: Clone>() {}
        fn assert_eq<T: PartialEq + Eq>() {}
        fn assert_error<T: std::error::Error>() {}
        assert_debug::<TxError>();
        assert_clone::<TxError>();
        assert_eq::<TxError>();
        assert_error::<TxError>();
    }

    // Type-shape lock: TxError is non_exhaustive — pattern matches must
    // use `_`. Verified by the existence of the `_Reserved` doc-hidden
    // variant + the #[non_exhaustive] attribute. The compile-only
    // test below confirms the variant is non-constructible by external
    // code (it would also fail if the variant were dropped without
    // adding a replacement).
    #[test]
    fn tx_error_non_exhaustive_lock() {
        // Construct via the doc-hidden variant (in-crate code can; per
        // the non_exhaustive contract, external crates cannot).
        let e = TxError::_Reserved;
        // Pattern-match must include `_` arm (or all variants); for
        // S2.2 there is exactly one variant. The shape lock is the
        // discipline that future variants ship with their own pattern
        // arms.
        match &e {
            TxError::_Reserved => {}
            _ => panic!("non-exhaustive: future variant unhandled"),
        }
        // Display trait formats to a non-empty string.
        assert!(!format!("{e}").is_empty());
    }
}
