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
use std::collections::BTreeMap;
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
    /// Buffered writes (S2.3 / SP112). Same-key writes coalesce
    /// last-write-wins via BTreeMap insertion. `None` value = buffered
    /// tombstone. Deterministic iteration (sorted lex by
    /// (type_id, object_id)) so Op::CommitTx's wire encoding is
    /// replica-byte-identical. Decision 2 of the S2.3 design.
    write_set: BTreeMap<(u32, [u8; 16]), Option<Vec<u8>>>,
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
    /// Hostile / malformed commit: `snapshot_opnum > commit_opnum`. The
    /// SM cursor-stall semantics for `snapshot_opnum > current_opnum`
    /// ship in S2.6; S2.3 treats this case as malformed input.
    SnapshotOutOfRange { snapshot: u64, commit: u64 },
    /// `put_versioned` failed during commit's apply phase. Wraps the
    /// underlying I/O error kind + message so callers can recover or
    /// escalate. Uses `std::io::ErrorKind` (which IS `Clone + Eq`) instead
    /// of `std::io::Error` (which is NOT) to preserve the `Clone + Eq`
    /// derive on `TxError` (Decision 3, S2.3 design — picks option (2)
    /// to preserve SP111's trait-shape contract). T2 ships the
    /// `From<std::io::Error>` conversion that extracts kind + message.
    StorageIo { kind: std::io::ErrorKind, message: String },
}

impl std::fmt::Display for TxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxError::_Reserved => write!(
                f,
                "TxError::_Reserved (reserved variant — S2.2 produces no errors)"
            ),
            TxError::SnapshotOutOfRange { snapshot, commit } => write!(
                f,
                "TxError::SnapshotOutOfRange {{ snapshot: {snapshot}, commit: {commit} }} \
                 — snapshot_opnum must not exceed commit_opnum"
            ),
            TxError::StorageIo { kind, message } => write!(
                f,
                "TxError::StorageIo {{ kind: {kind:?}, message: {message:?} }}"
            ),
        }
    }
}

impl std::error::Error for TxError {}

/// Outcome of a conflict-checked commit (S2.3 / SP112). `Committed`
/// echoes the `commit_opnum` back for audit. `Aborted` carries the
/// `(type_id, object_id)` of the FIRST conflicting key encountered.
///
/// IMPORTANT: an `Aborted` outcome is `Ok(_)`, NOT `Err(_)` — an SI
/// conflict is a normal/expected outcome (the Tx must retry with a
/// fresher snapshot), not an error. `TxError` is reserved for
/// malformed input + infrastructure failures. Decision 6 of the S2.3
/// design.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TxCommitOutcome {
    /// The transaction committed successfully at `commit_opnum`.
    Committed { commit_opnum: u64 },
    /// The transaction was aborted due to a write-write conflict on
    /// `conflicting_key`. The caller should retry with a fresher snapshot.
    Aborted { conflicting_key: (u32, [u8; 16]) },
}

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
        Self {
            store,
            snapshot_opnum,
            read_set: BTreeSet::new(),
            write_set: BTreeMap::new(),
        }
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
        // Decision 4: insert UNCONDITIONALLY — the Tx observed the key
        // regardless of whether it found a live version, a tombstone, or
        // nothing. The BTreeSet deduplicates re-reads at no extra cost.
        // Dereferencing *object_id copies 16 bytes into the set entry;
        // that is the expected per-read cost and is documented here for
        // future profiling reference.
        self.read_set.insert((type_id, *object_id));
        crate::mvcc::get_at_snapshot(self.store, type_id, object_id, self.snapshot_opnum)
    }

    /// The snapshot_opnum the Tx was pinned at. Never changes after `begin`.
    ///
    /// S2.3 uses this to compute the `(snapshot_opnum, commit_opnum]`
    /// conflict window for plain SI commit validation.
    pub fn snapshot_opnum(&self) -> u64 {
        self.snapshot_opnum
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
        &self.read_set
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
        // S2.2 read-only Tx has no failure mode. Drop self (releases the
        // &Storage borrow), return Ok. S2.3 will insert the conflict-check
        // here before returning and may return Err(TxError::ConflictAborted).
        Ok(())
    }

    /// Explicit abort.
    ///
    /// For S2.2 (read-only Tx with no buffered state) this is
    /// identical to dropping the Tx. Shipped for symmetry with the
    /// S2.3 write variant which will need explicit abort semantics
    /// to discard buffered writes.
    pub fn abort(self) {
        // Drop self. No buffered writes to discard in S2.2. S2.3 will
        // add write-set rollback logic here.
    }

    /// Buffer a write of `value` (or a tombstone if `value == None`)
    /// for `(type_id, object_id)`. Same-key writes coalesce
    /// (last-write-wins via BTreeMap insertion). Visible to subsequent
    /// `Tx::read` calls in this Tx via the read-your-writes overlay
    /// (Decision 3 of the S2.3 design). Per-Tx semantic only — no other
    /// Tx observes the buffered write until this Tx commits.
    pub fn write(&mut self, type_id: u32, object_id: &[u8; 16], value: Option<Vec<u8>>) {
        todo!("S2.3 T2: insert into self.write_set");
    }

    /// Immutable view of the buffered writes. S2.4 SSI consumes this for
    /// rw-antidependency cycle detection. Deterministic iteration
    /// (sorted lex) by BTreeMap discipline (Decision 2).
    pub fn write_set(&self) -> &BTreeMap<(u32, [u8; 16]), Option<Vec<u8>>> {
        todo!("S2.3 T2: return &self.write_set");
    }

    /// Conflict-checked commit (S2.3 / SP112).
    ///
    /// In standalone form (this S2.3 cut), runs the SAME deterministic
    /// conflict check that the SM apply path runs — directly against
    /// `self.store`. In production (S2.6), this will be replaced by an
    /// `Op::CommitTx` submission to VSR + the verdict coming back via
    /// the SM apply callback. The S2.3 standalone form runs the check
    /// locally for testability + the dormant-module discipline.
    ///
    /// Returns:
    /// - `Ok(TxCommitOutcome::Committed { commit_opnum })` if no conflict
    ///   was found and every write was installed at `commit_opnum`.
    /// - `Ok(TxCommitOutcome::Aborted { conflicting_key })` if a
    ///   write-write conflict was detected (first conflicting key wins).
    /// - `Err(TxError::SnapshotOutOfRange { snapshot, commit })` if
    ///   `snapshot_opnum > commit_opnum` (malformed input).
    /// - `Err(TxError::StorageIo { kind, message })` if `put_versioned`
    ///   fails during apply.
    ///
    /// Edge cases:
    /// - Empty write_set => `Ok(Committed { commit_opnum })` (no-op).
    /// - `commit_opnum == 0` => conflict check SKIPPED (no prior versions
    ///   can exist below opnum=0). Edge case from Decision 5.
    pub fn commit(self, commit_opnum: u64) -> Result<TxCommitOutcome, TxError> {
        todo!("S2.3 T2: run conflict check then install writes");
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
        // Pattern-match must include `_` arm (or all variants).
        match &e {
            TxError::_Reserved => {}
            TxError::SnapshotOutOfRange { .. } => {}
            TxError::StorageIo { .. } => {}
            _ => panic!("non-exhaustive: future variant unhandled"),
        }
        // Display trait formats to a non-empty string.
        assert!(!format!("{e}").is_empty());
    }

    // SP112 T1 scaffold test 1: TxCommitOutcome derives Debug + Clone + PartialEq + Eq.
    #[test]
    fn tx_commit_outcome_trait_shape_locked() {
        fn assert_debug<T: std::fmt::Debug>() {}
        fn assert_clone<T: Clone>() {}
        fn assert_partial_eq<T: PartialEq>() {}
        fn assert_eq_trait<T: Eq>() {}
        assert_debug::<TxCommitOutcome>();
        assert_clone::<TxCommitOutcome>();
        assert_partial_eq::<TxCommitOutcome>();
        assert_eq_trait::<TxCommitOutcome>();
        // Verify both variants are constructible and clone correctly.
        let committed = TxCommitOutcome::Committed { commit_opnum: 42 };
        assert_eq!(committed.clone(), committed);
        let aborted = TxCommitOutcome::Aborted { conflicting_key: (1u32, [0u8; 16]) };
        assert_eq!(aborted.clone(), aborted);
        assert_ne!(committed, aborted);
    }

    // SP112 T1 scaffold test 2: extended TxError variants are constructible
    // and Display + Clone + PartialEq + Eq all hold.
    #[test]
    fn tx_error_extends_with_snapshot_out_of_range() {
        let e1 = TxError::SnapshotOutOfRange { snapshot: 10, commit: 5 };
        // Display returns a non-empty string.
        let display = format!("{e1}");
        assert!(!display.is_empty(), "Display must produce non-empty string");
        // Clone produces an equal value.
        assert_eq!(e1.clone(), e1);
        // matches! macro works (confirms the variant is constructible and matchable).
        assert!(matches!(e1, TxError::SnapshotOutOfRange { snapshot: 10, commit: 5 }));

        let e2 = TxError::StorageIo {
            kind: std::io::ErrorKind::Other,
            message: "disk full".to_string(),
        };
        let display2 = format!("{e2}");
        assert!(!display2.is_empty());
        assert_eq!(e2.clone(), e2);
        assert!(matches!(e2, TxError::StorageIo { .. }));
    }
}

#[cfg(test)]
mod tx_kats {
    use super::*;
    use crate::mvcc::{put_versioned, SnapshotRead};
    use crate::Storage;
    use kessel_io::MemVfs;

    /// Construct a 16-byte object_id with `n` in the last byte.
    fn obj(n: u8) -> [u8; 16] {
        let mut a = [0u8; 16];
        a[15] = n;
        a
    }

    // KAT-1: Tx::begin pins snapshot_opnum; snapshot_opnum() returns exactly
    // the value supplied. read_set is empty immediately after begin.
    // Hand-derived: begin(store, 42) → snapshot_opnum()==42, read_set empty.
    #[test]
    fn kat_begin_pins_snapshot_opnum() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();
        let tx = Tx::begin(&store, 42);
        assert_eq!(tx.snapshot_opnum(), 42, "snapshot pin must equal begin arg");
        assert!(tx.read_set().is_empty(), "read_set must be empty on begin");
        // Consume tx so the borrow is released.
        tx.abort();
    }

    // KAT-2: Tx::read returns SnapshotRead::Found for a written key visible
    // at the snapshot, AND records the key in read_set.
    // Hand-derived: put_versioned at commit_opnum=0 with value=[0xAA];
    // Tx pinned at snapshot=0; read(type_id=1, obj(1)) → Found([0xAA]);
    // read_set contains (1, obj(1)).
    #[test]
    fn kat_read_returns_found_with_value() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();
        let mut tx = Tx::begin(&store, 0);
        match tx.read(1, &obj(1)) {
            SnapshotRead::Found(v) => assert_eq!(v, vec![0xAA], "value must be byte-identical"),
            other => panic!("expected Found([0xAA]), got {:?}", other),
        }
        assert_eq!(tx.read_set().len(), 1, "exactly one entry in read_set");
        assert!(
            tx.read_set().contains(&(1u32, obj(1))),
            "read_set must contain the key"
        );
        tx.abort();
    }

    // KAT-3: Tx::read returns SnapshotRead::NotYetWritten for a key that has
    // never been written; read_set STILL records (type_id, object_id).
    // Decision 4: absence-observation is a read-set entry.
    // Hand-derived: fresh store; Tx at snapshot=100; read(7, obj(7)) →
    // NotYetWritten; read_set contains (7, obj(7)).
    #[test]
    fn kat_read_never_written_still_in_read_set() {
        let store = Storage::open(MemVfs::new()).unwrap();
        let mut tx = Tx::begin(&store, 100);
        assert!(
            matches!(tx.read(7, &obj(7)), SnapshotRead::NotYetWritten),
            "unwritten key must return NotYetWritten"
        );
        assert_eq!(
            tx.read_set().len(),
            1,
            "absence-observation must enter read_set (Decision 4)"
        );
        assert!(
            tx.read_set().contains(&(7u32, obj(7))),
            "read_set must contain the absence-observed key"
        );
        tx.abort();
    }

    // KAT-4: Tx::read returns SnapshotRead::Tombstoned for a deleted key;
    // read_set STILL records (type_id, object_id) per Decision 4.
    // Hand-derived: put at commit_opnum=0 (live), put None at commit_opnum=1
    // (tombstone); Tx at snapshot=1; read(1, obj(1)) → Tombstoned;
    // read_set contains (1, obj(1)).
    #[test]
    fn kat_read_tombstoned_still_in_read_set() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();
        put_versioned(&mut store, 1, &obj(1), 1, None).unwrap(); // tombstone at opnum=1
        let mut tx = Tx::begin(&store, 1);
        assert!(
            matches!(tx.read(1, &obj(1)), SnapshotRead::Tombstoned),
            "tombstoned key must return Tombstoned"
        );
        assert_eq!(
            tx.read_set().len(),
            1,
            "tombstone-observation must enter read_set (Decision 4)"
        );
        assert!(
            tx.read_set().contains(&(1u32, obj(1))),
            "read_set must contain the tombstone-observed key"
        );
        tx.abort();
    }

    // KAT-5: Re-reading the same key multiple times produces exactly one
    // entry in read_set (BTreeSet set-semantics deduplication).
    // Hand-derived: 3 reads of same key → read_set.len()==1.
    #[test]
    fn kat_re_read_same_key_no_dup_in_read_set() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();
        let mut tx = Tx::begin(&store, 0);
        let _ = tx.read(1, &obj(1));
        let _ = tx.read(1, &obj(1));
        let _ = tx.read(1, &obj(1));
        assert_eq!(
            tx.read_set().len(),
            1,
            "BTreeSet must deduplicate re-reads of the same key"
        );
        tx.abort();
    }

    // KAT-6: read_set iteration order is sorted by (type_id, object_id) lex.
    // Keys inserted in reverse-sorted order; iteration must yield them sorted.
    // Hand-derived: read (type_id=2, obj(2)), (type_id=1, obj(2)),
    // (type_id=1, obj(1)) in that order; sorted iteration yields:
    //   (1, obj(1)) < (1, obj(2)) < (2, obj(2))
    // because (1 < 2) for the type_id field and obj(1)[15]=1 < obj(2)[15]=2
    // for the object_id field.
    #[test]
    fn kat_read_set_sorted_iteration() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();
        put_versioned(&mut store, 1, &obj(2), 0, Some(vec![0xBB])).unwrap();
        put_versioned(&mut store, 2, &obj(2), 0, Some(vec![0xCC])).unwrap();
        let mut tx = Tx::begin(&store, 0);
        // Insert in reverse-sorted order to exercise the BTreeSet sort.
        let _ = tx.read(2, &obj(2));
        let _ = tx.read(1, &obj(2));
        let _ = tx.read(1, &obj(1));
        let actual: Vec<(u32, [u8; 16])> = tx.read_set().iter().cloned().collect();
        let expected: Vec<(u32, [u8; 16])> = vec![
            (1u32, obj(1)),
            (1u32, obj(2)),
            (2u32, obj(2)),
        ];
        assert_eq!(
            actual, expected,
            "BTreeSet iteration must be sorted lex by (type_id, object_id)"
        );
        tx.abort();
    }

    // KAT-7: Snapshot pin is honored — a write at opnum > snapshot_opnum is
    // invisible to the Tx.
    // Hand-derived: put at commit_opnum=0 value=[0xAA]; Tx at snapshot=0;
    // then put at commit_opnum=1 value=[0xBB] (future write); Tx::read
    // returns Found([0xAA]) — the snapshot=0 view, not the opnum=1 version.
    //
    // Lifetime note: Tx holds &store; we cannot take &mut store while the Tx
    // borrow is live. We instead call put_versioned with its own &mut borrow
    // BEFORE we begin the Tx for the "later write" step. The effect is
    // identical: Storage contains both versions (opnum=0 and opnum=1); the
    // Tx pinned at snapshot=0 must not see the opnum=1 entry.
    #[test]
    fn kat_snapshot_pin_invisible_future_write() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        // Write opnum=0 (the "before-snapshot" version).
        put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();
        // Write opnum=1 (the "after-snapshot" version — future write).
        put_versioned(&mut store, 1, &obj(1), 1, Some(vec![0xBB])).unwrap();
        // Tx pinned at snapshot=0 must see opnum=0 version only.
        let mut tx = Tx::begin(&store, 0);
        match tx.read(1, &obj(1)) {
            SnapshotRead::Found(v) => {
                assert_eq!(v, vec![0xAA], "snapshot pin must suppress opnum=1 write");
            }
            other => panic!(
                "snapshot pin broken: expected Found([0xAA]), got {:?}",
                other
            ),
        }
        tx.abort();
    }

    // KAT-8: commit_read_only consumes the Tx and returns Ok(()).
    // The consumption is compile-time-checked (using tx after
    // commit_read_only would be a borrow-of-moved-value error).
    // This test verifies the runtime Ok(()) contract.
    #[test]
    fn kat_commit_read_only_ok_and_consumes_tx() {
        let store = Storage::open(MemVfs::new()).unwrap();
        let tx = Tx::begin(&store, 0);
        let result = tx.commit_read_only();
        assert!(result.is_ok(), "commit_read_only must return Ok(()) in S2.2");
        // `tx` is consumed above; `tx.snapshot_opnum()` here would be a
        // compile-time "use of moved value" error — this is the
        // borrow-checker-enforced lifecycle contract.
    }

    // KAT-9: abort consumes the Tx and returns (). Identical lifecycle
    // contract to commit_read_only from the borrow-checker's perspective.
    #[test]
    fn kat_abort_consumes_tx() {
        let store = Storage::open(MemVfs::new()).unwrap();
        let tx = Tx::begin(&store, 0);
        tx.abort(); // consumes tx
        // `tx` is consumed above; `tx.snapshot_opnum()` here would be a
        // compile-time "use of moved value" error — the lifecycle is
        // compile-time-enforced.
    }
}

#[cfg(test)]
mod tx_coverage {
    use super::*;
    use crate::mvcc::{put_versioned, SnapshotRead};
    use crate::Storage;
    use kessel_io::MemVfs;

    fn obj(n: u8) -> [u8; 16] {
        let mut a = [0u8; 16];
        a[15] = n;
        a
    }

    // CV-1: Tx with zero reads — read_set stays empty; commit_read_only Ok.
    #[test]
    fn cv_tx_with_zero_reads() {
        let store = Storage::open(MemVfs::new()).unwrap();
        let tx = Tx::begin(&store, 0);
        assert!(tx.read_set().is_empty());
        assert!(tx.commit_read_only().is_ok());
    }

    // CV-2: Re-read same key 100 times — read_set stays at size 1 (set semantics).
    #[test]
    fn cv_re_read_same_key_100x_size_1() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();
        let mut tx = Tx::begin(&store, 0);
        for _ in 0..100 {
            let _ = tx.read(1, &obj(1));
        }
        assert_eq!(tx.read_set().len(), 1);
    }

    // CV-3: Large read-set scaling — 1000 distinct keys, read_set size == 1000.
    #[test]
    fn cv_large_read_set_1000_keys() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        for i in 0..1000u32 {
            let mut k = [0u8; 16];
            k[12..16].copy_from_slice(&i.to_be_bytes());
            put_versioned(&mut store, 1, &k, i as u64, Some(vec![(i & 0xFF) as u8])).unwrap();
        }
        let mut tx = Tx::begin(&store, 999);
        for i in 0..1000u32 {
            let mut k = [0u8; 16];
            k[12..16].copy_from_slice(&i.to_be_bytes());
            let _ = tx.read(1, &k);
        }
        assert_eq!(tx.read_set().len(), 1000);
    }

    // CV-4: read_set is clone-equivalent — cloning the BTreeSet yields an equal set
    // with deterministic iteration (the property S2.4 SSI will rely on).
    #[test]
    fn cv_read_set_clone_equivalence() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();
        put_versioned(&mut store, 2, &obj(2), 0, Some(vec![0xBB])).unwrap();
        let mut tx = Tx::begin(&store, 0);
        let _ = tx.read(1, &obj(1));
        let _ = tx.read(2, &obj(2));
        let original: Vec<_> = tx.read_set().iter().cloned().collect();
        let cloned: Vec<_> = tx.read_set().clone().into_iter().collect();
        assert_eq!(original, cloned);
    }

    // CV-5: commit_read_only after many reads — still Ok (no failure mode in S2.2).
    #[test]
    fn cv_commit_after_many_reads_ok() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        for i in 0..50u8 {
            put_versioned(&mut store, 1, &obj(i), i as u64, Some(vec![i])).unwrap();
        }
        let mut tx = Tx::begin(&store, 49);
        for i in 0..50u8 {
            let _ = tx.read(1, &obj(i));
        }
        assert_eq!(tx.read_set().len(), 50);
        assert!(tx.commit_read_only().is_ok());
    }
}
