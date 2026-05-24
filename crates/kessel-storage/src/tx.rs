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

/// Internal: the kind of borrow a Tx holds against Storage. SP112 T2
/// introduces the `Exclusive` variant so `Tx::commit` can call
/// `put_versioned(&mut Storage)` to install writes. The `Shared` variant
/// is the SP111 read-only path (which allows e.g. multi-Tx tests where
/// tx_a/tx_b/tx_c all borrow the same `&store` concurrently).
///
/// SP112 T2-DECIDED CHOICE — see the doc on `Tx::store` for the full
/// rationale. The enum is `pub(crate)` because external callers route
/// through `Tx::begin` (Shared) or `Tx::begin_rw` (Exclusive); the enum
/// itself is an implementation detail.
pub(crate) enum TxStore<'a, V: Vfs> {
    /// Shared borrow (`&Storage`). SP111 read-only flow. Multi-Tx
    /// tests in tx_integration.rs depend on this allowing several
    /// Tx to coexist against one `&store`.
    Shared(&'a Storage<V>),
    /// Exclusive borrow (`&mut Storage`). SP112 write-capable flow.
    /// `Tx::commit` requires this variant; `Tx::commit` on a `Shared`
    /// Tx returns `Err(TxError::ReadOnlyCannotCommit)`.
    Exclusive(&'a mut Storage<V>),
}

impl<'a, V: Vfs> TxStore<'a, V> {
    /// View as `&Storage` regardless of variant. Used by all read paths
    /// (`Tx::read`, `Tx::commit`'s conflict check via
    /// `has_version_in_range(&Storage, ...)`) so they work uniformly on
    /// both Shared and Exclusive Tx.
    #[inline]
    fn as_ref(&self) -> &Storage<V> {
        match self {
            TxStore::Shared(s) => s,
            TxStore::Exclusive(s) => s,
        }
    }
}

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
    /// Borrow of the underlying storage. Shared (SP111 read-only) OR
    /// exclusive (SP112 write-capable). The exclusive variant is required
    /// for `Tx::commit` (which calls `put_versioned(&mut Storage)` to
    /// install writes); the shared variant supports the SP111 read-only
    /// flow + multi-Tx tests that hold several `&store` borrows at once.
    ///
    /// Decision 2: Tx lives in kessel-storage::tx and holds a borrow of
    /// Storage<V> rather than coupling to kessel-sm. S2.6 SM integration
    /// will call `Tx::begin(&store, sm.last_committed_opnum())` for
    /// read-only flows and `Tx::begin_rw(&mut store, ...)` for committing
    /// flows.
    ///
    /// SP112 T2-DECIDED CHOICE (storage-mutability) — Strategy (b) per
    /// the design's Tx::commit-mutability note. Rationale:
    ///   - Strategy (a) "interior mutability" is not viable: SP110's
    ///     `mvcc::put_versioned` signature requires `&mut Storage<V>` and
    ///     T2 is forbidden from changing mvcc.rs (T2 only composes its
    ///     primitives).
    ///   - Strategy (b) "add a Tx::begin_rw constructor" is the
    ///     minimum-churn safe-Rust path. The enum-of-shared-or-mut keeps
    ///     SP111 read-only call-sites (e.g., the multi-Tx tests in
    ///     tx_integration.rs that hold tx_a/tx_b/tx_c against the same
    ///     `&store`) compiling byte-identically. `Tx::begin` keeps its
    ///     S2.2 signature (`&'a Storage<V>`); the new `Tx::begin_rw`
    ///     takes `&'a mut Storage<V>` for the commit-capable flow.
    ///   - Per-method behavior: `read`, `read_set`, `snapshot_opnum`,
    ///     `write`, `write_set`, `commit_read_only`, `abort` work
    ///     uniformly on BOTH variants (read-only Tx may still buffer
    ///     writes but cannot commit them — `commit` on a Shared Tx
    ///     returns `Err(TxError::ReadOnlyCannotCommit)`).
    store: TxStore<'a, V>,
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
    /// `Tx::commit` was called on a Tx constructed via `Tx::begin`
    /// (shared borrow — read-only). To commit writes, construct the Tx
    /// via `Tx::begin_rw(&mut store, snapshot_opnum)` instead. This is
    /// SP112's T2-decided storage-mutability choice surfacing — see
    /// the `Tx::store` field doc for the rationale.
    ReadOnlyCannotCommit,
    /// SP114 / S2.5: The requested snapshot_opnum is below the storage's
    /// low_water_mark; the versions that would be visible have been
    /// reclaimed by a prior Op::AdvanceWatermark apply. Replay with a
    /// fresh snapshot >= low_water_mark.
    ///
    /// Note: `snapshot` field is NOT included here (only `low_water_mark`)
    /// because the caller already knows which snapshot they requested;
    /// `low_water_mark` is the new floor they need to beat.
    SnapshotTooOld { low_water_mark: u64 },
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
            TxError::ReadOnlyCannotCommit => write!(
                f,
                "TxError::ReadOnlyCannotCommit — Tx::commit requires the \
                 Tx to be constructed via Tx::begin_rw(&mut store, ...); \
                 a Tx::begin(&store, ...) Tx is read-only"
            ),
            TxError::SnapshotTooOld { low_water_mark } => write!(
                f,
                "TxError::SnapshotTooOld {{ low_water_mark: {low_water_mark} }} \
                 — snapshot_opnum is below the storage low_water_mark; \
                 versions have been reclaimed; retry with a fresh snapshot \
                 >= {low_water_mark}"
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
    /// SP113 / S2.4: SSI-specific dangerous-structure abort. The
    /// committing Tx had a dangerous rw-antidependency structure in the
    /// SM's pending_txs window; Cahill SSI aborts the committing Tx
    /// (Decision 3) to preserve serializability. `other_commit_opnum`
    /// surfaces the other Tx in the chain for debugging; the caller
    /// should retry with a fresh snapshot.
    AbortedDangerousStructure { other_commit_opnum: u64 },
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
    ///
    /// SP114 / S2.5 T1 BREAKING CHANGE: returns `Result<Self, TxError>`.
    /// T2 will add the snapshot-too-old check (`snapshot_opnum <
    /// store.low_water_mark()` => `Err(TxError::SnapshotTooOld)`).
    /// At watermark=0 (the T1 default), this always returns `Ok(...)`,
    /// preserving byte-net-0 behaviour for all SP110/111/112/113 callers.
    pub fn begin(store: &'a Storage<V>, snapshot_opnum: u64) -> Result<Self, TxError> {
        // SP114 / S2.5 T2: snapshot-too-old check (Decision 7). At
        // watermark=0 (the SP110-113 default), every snapshot >= 0 is
        // valid; byte-net-0 for all SP110-113 callers.
        // STRICT less-than rejection: snapshot == low_water_mark is
        // SERVEABLE (the at-watermark version is the oldest preserved
        // by GC — Decision 3); only snapshot < low_water_mark fails.
        let lwm = store.low_water_mark();
        if snapshot_opnum < lwm {
            return Err(TxError::SnapshotTooOld { low_water_mark: lwm });
        }
        Ok(Self {
            store: TxStore::Shared(store),
            snapshot_opnum,
            read_set: BTreeSet::new(),
            write_set: BTreeMap::new(),
        })
    }

    /// Begin a write-capable Tx pinned at `snapshot_opnum`. The exclusive
    /// `&mut Storage<V>` borrow allows `Tx::commit` to install writes via
    /// `mvcc::put_versioned`.
    ///
    /// SP112 T2-DECIDED CHOICE (storage-mutability) — see the
    /// `Tx::store` field doc for the full rationale. SP111's
    /// `Tx::begin(&store, ...)` is left untouched for the read-only
    /// path; write-capable callers must use `begin_rw` instead.
    ///
    /// SP114 / S2.5 T1 BREAKING CHANGE: returns `Result<Self, TxError>`.
    /// T2 adds the snapshot-too-old check. At watermark=0 always Ok.
    pub fn begin_rw(store: &'a mut Storage<V>, snapshot_opnum: u64) -> Result<Self, TxError> {
        // SP114 / S2.5 T2: snapshot-too-old check (Decision 7). See
        // `Tx::begin` for the strict-less-than rationale + at-watermark
        // serveable contract.
        let lwm = store.low_water_mark();
        if snapshot_opnum < lwm {
            return Err(TxError::SnapshotTooOld { low_water_mark: lwm });
        }
        Ok(Self {
            store: TxStore::Exclusive(store),
            snapshot_opnum,
            read_set: BTreeSet::new(),
            write_set: BTreeMap::new(),
        })
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
        // S2.2 Decision 4: insert UNCONDITIONALLY — the Tx observed the
        // key regardless of which path serves it (overlay or snapshot)
        // or which `SnapshotRead` variant returns. BTreeSet dedups
        // re-reads at no extra cost. *object_id copies 16 bytes into
        // the set entry. S2.3 Decision 3 explicitly carries this
        // discipline forward: the buffered-read overlay path STILL
        // records the read_set entry; SSI in S2.4 consumes both
        // overlay-served and snapshot-served observations.
        self.read_set.insert((type_id, *object_id));
        // S2.3 Decision 3: read-your-writes overlay. Check write_set
        // FIRST. If the key has a buffered write or tombstone in this
        // Tx, return it instead of the snapshot version (per-Tx-only
        // semantic — other Tx do not see this Tx's buffered writes
        // until commit).
        if let Some(buffered) = self.write_set.get(&(type_id, *object_id)) {
            return match buffered {
                Some(v) => SnapshotRead::Found(v.clone()),
                None => SnapshotRead::Tombstoned,
            };
        }
        // Fall through to the S2.2 snapshot-read path (unchanged).
        crate::mvcc::get_at_snapshot(self.store.as_ref(), type_id, object_id, self.snapshot_opnum)
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
        // Decision 2: BTreeMap::insert REPLACES any prior buffered write
        // for the same `(type_id, *object_id)` key — last-write-wins
        // coalescing within a single Tx. The 16-byte object_id is copied
        // (dereferenced) into the map entry; same cost shape as read_set.
        // Decision 3 (carried): write does NOT update read_set; read_set
        // tracks OBSERVED reads only. A Tx may write without ever reading.
        self.write_set.insert((type_id, *object_id), value);
    }

    /// Immutable view of the buffered writes. S2.4 SSI consumes this for
    /// rw-antidependency cycle detection. Deterministic iteration
    /// (sorted lex) by BTreeMap discipline (Decision 2).
    pub fn write_set(&self) -> &BTreeMap<(u32, [u8; 16]), Option<Vec<u8>>> {
        &self.write_set
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
        // Decision 4 + 5: malformed snapshot_opnum is rejected before any
        // check or apply. Boundary: `snapshot_opnum == commit_opnum` is
        // ALLOWED (commit at the same opnum we snapshotted at — the
        // conflict window `(snapshot, commit-1]` is empty so any single
        // writer trivially commits). Only `snapshot > commit` is the
        // error boundary.
        if self.snapshot_opnum > commit_opnum {
            return Err(TxError::SnapshotOutOfRange {
                snapshot: self.snapshot_opnum,
                commit: commit_opnum,
            });
        }
        // SP112 T2-DECIDED CHOICE (storage-mutability): commit requires
        // the Exclusive borrow (constructed via Tx::begin_rw). A
        // Tx::begin Tx (Shared) cannot commit — return a typed error so
        // the caller can switch constructors. This decision is detailed
        // in the `Tx::store` field doc.
        let store_mut = match self.store {
            TxStore::Exclusive(s) => s,
            TxStore::Shared(_) => return Err(TxError::ReadOnlyCannotCommit),
        };
        // Conflict check (the SI thesis-fit). Skip for commit_opnum == 0
        // edge: no prior versions can exist below opnum=0, and
        // `commit_opnum - 1` would underflow u64. Decision 5.
        if commit_opnum > 0 {
            let hi = commit_opnum - 1;
            for ((type_id, object_id), _new_value) in &self.write_set {
                if crate::mvcc::has_version_in_range(
                    store_mut,
                    *type_id,
                    object_id,
                    self.snapshot_opnum,
                    hi,
                ) {
                    return Ok(TxCommitOutcome::Aborted {
                        conflicting_key: (*type_id, *object_id),
                    });
                }
            }
        }
        // No conflict: install every write at commit_opnum. Iteration is
        // sorted lex by (type_id, object_id) via BTreeMap discipline
        // (Decision 2) — the install order is replica-byte-identical.
        for ((type_id, object_id), value) in self.write_set {
            crate::mvcc::put_versioned(store_mut, type_id, &object_id, commit_opnum, value)
                .map_err(|e| TxError::StorageIo {
                    kind: e.kind(),
                    message: e.to_string(),
                })?;
        }
        Ok(TxCommitOutcome::Committed { commit_opnum })
    }

    /// Begin an SSI-mode write-capable Tx pinned at `snapshot_opnum`.
    /// SP113 / S2.4. Structurally identical to `begin_rw` at the
    /// storage-borrow level; differs only in the eventual commit path:
    /// `commit_ssi` ships the read_set over the wire so the SM can run
    /// Cahill's dangerous-structure check.
    ///
    /// The SI/SSI distinction is purely per-call-site (which commit
    /// method is invoked) — there is no SSI-mode flag on the Tx struct.
    /// See S2.4 design Decision 6.
    /// SP114 / S2.5 T1 BREAKING CHANGE: returns `Result<Self, TxError>`.
    /// T2 adds the snapshot-too-old check. At watermark=0 always Ok.
    pub fn begin_ssi(store: &'a mut Storage<V>, snapshot_opnum: u64) -> Result<Self, TxError> {
        // SP114 / S2.5 T2: snapshot-too-old check (Decision 7). See
        // `Tx::begin` for the strict-less-than rationale + at-watermark
        // serveable contract.
        let lwm = store.low_water_mark();
        if snapshot_opnum < lwm {
            return Err(TxError::SnapshotTooOld { low_water_mark: lwm });
        }
        Ok(Self {
            store: TxStore::Exclusive(store),
            snapshot_opnum,
            read_set: BTreeSet::new(),
            write_set: BTreeMap::new(),
        })
    }

    /// Conflict-checked SSI commit (Cahill SSI). Ships the Tx's read_set
    /// + write_set + snapshot_opnum in `Op::CommitTx`; the SM's apply arm
    /// derives rw-antidependency edges against its pending_txs window and
    /// aborts on a dangerous structure. Behaviour identical to `commit`
    /// (SI mode) on the SI write-write check + on the install path; the
    /// SSI step runs between them, gated on `!read_set.is_empty()`.
    ///
    /// SP113 / S2.4. Outcome shape extends `TxCommitOutcome::Aborted` to
    /// surface the `AbortedDangerousStructure` variant.
    ///
    /// IMPORTANT: like `commit`, the standalone form here runs the SM
    /// apply path locally for testability. In production (S2.6 SM caller
    /// integration), `commit_ssi` will construct an `Op::CommitTx` payload
    /// with the read_set populated and submit it to VSR; the verdict will
    /// arrive back via the SM apply callback.
    pub fn commit_ssi(self, commit_opnum: u64) -> Result<TxCommitOutcome, TxError> {
        // Decisions 4 + 5 (SP112-carried): malformed snapshot rejected
        // before any check or apply. `snapshot == commit` is allowed.
        if self.snapshot_opnum > commit_opnum {
            return Err(TxError::SnapshotOutOfRange {
                snapshot: self.snapshot_opnum,
                commit: commit_opnum,
            });
        }
        // SP112 T2 storage-mutability discipline (carried): commit_ssi
        // requires the Exclusive borrow (constructed via Tx::begin_ssi
        // or Tx::begin_rw). A Tx::begin (Shared) Tx cannot commit —
        // return the typed error so callers can switch constructors.
        let store_mut = match self.store {
            TxStore::Exclusive(s) => s,
            TxStore::Shared(_) => return Err(TxError::ReadOnlyCannotCommit),
        };
        // SP112 SI write-write conflict check — fires FIRST so SP112's
        // verdict precedence (WW > SSI) holds even on the standalone
        // path. commit_opnum == 0 skips the check (no prior versions
        // can exist below opnum=0).
        if commit_opnum > 0 {
            let hi = commit_opnum - 1;
            for ((type_id, object_id), _new_value) in &self.write_set {
                if crate::mvcc::has_version_in_range(
                    store_mut,
                    *type_id,
                    object_id,
                    self.snapshot_opnum,
                    hi,
                ) {
                    return Ok(TxCommitOutcome::Aborted {
                        conflicting_key: (*type_id, *object_id),
                    });
                }
            }
        }
        // SP113 / S2.4 SSI dangerous-structure detector — runs
        // against a LOCAL empty pending_txs map (the standalone form
        // has no access to the SM's pending_txs; documented
        // limitation per the plan T2 nuance). On an empty
        // pending_txs no rw-edges can form, so this branch can never
        // abort a non-conflicting commit. The branch exists so the
        // standalone form structurally composes byte-identically
        // with the SM apply form for the empty-pending_txs case
        // (verified by T3's byte-equivalence test). When read_set is
        // empty (Decision 8 SP112 fast path), skip the branch
        // entirely — preserves byte-net-0 vs `Tx::commit` for that
        // shape.
        if !self.read_set.is_empty() {
            let mut local_pending: std::collections::BTreeMap<
                u64,
                crate::ssi::PendingTxRecord,
            > = std::collections::BTreeMap::new();
            // BTreeSet iteration is sorted ⇒ Vec is sorted.
            let sorted_read_set: Vec<(u32, [u8; 16])> =
                self.read_set.iter().copied().collect();
            // BTreeMap iteration is sorted ⇒ Vec is sorted.
            let sorted_write_keys: Vec<(u32, [u8; 16])> =
                self.write_set.keys().copied().collect();
            if let Some(other_commit_opnum) =
                crate::ssi::detect_dangerous_structure(
                    &mut local_pending,
                    self.snapshot_opnum,
                    &sorted_read_set,
                    &sorted_write_keys,
                    commit_opnum,
                )
            {
                return Ok(TxCommitOutcome::AbortedDangerousStructure {
                    other_commit_opnum,
                });
            }
        }
        // No conflict: install every write at commit_opnum. Same
        // shape as Tx::commit — sorted lex by (type_id, object_id)
        // via BTreeMap discipline. The standalone form does NOT
        // record into a pending_txs (no map exists here); the SM
        // apply path handles that for the production flow.
        for ((type_id, object_id), value) in self.write_set {
            crate::mvcc::put_versioned(store_mut, type_id, &object_id, commit_opnum, value)
                .map_err(|e| TxError::StorageIo {
                    kind: e.kind(),
                    message: e.to_string(),
                })?;
        }
        Ok(TxCommitOutcome::Committed { commit_opnum })
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
        let tx = Tx::begin(&store, 42).expect("SP114 T1: watermark=0; begin always Ok");
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
        let mut tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
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
        let mut tx = Tx::begin(&store, 100).expect("SP114 T1: watermark=0; begin always Ok");
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
        let mut tx = Tx::begin(&store, 1).expect("SP114 T1: watermark=0; begin always Ok");
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
        let mut tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
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
        let mut tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
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
        let mut tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
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
        let tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
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
        let tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
        tx.abort(); // consumes tx
        // `tx` is consumed above; `tx.snapshot_opnum()` here would be a
        // compile-time "use of moved value" error — the lifecycle is
        // compile-time-enforced.
    }

    // ========================================================================
    // SP114 / S2.5 T2 — Tx::begin* snapshot-too-old KAT (1 of 11).
    //
    // Decision 7: STRICT less-than. snapshot_opnum < low_water_mark is
    // rejected with Err(TxError::SnapshotTooOld { low_water_mark }).
    // snapshot_opnum == low_water_mark is SERVEABLE (the at-watermark
    // version is the oldest preserved by GC — Decision 3). Each of
    // Tx::begin / Tx::begin_rw / Tx::begin_ssi enforces the same rule.
    // ========================================================================

    /// KAT-6 (plan): Tx::begin* rejects snapshot below the watermark.
    /// Claim:    With Storage::low_water_mark = 10, begin(snap=5) /
    ///           begin_rw(snap=5) / begin_ssi(snap=5) all return
    ///           Err(SnapshotTooOld { low_water_mark: 10 }). With the
    ///           SAME watermark, begin(snap=10) / begin(snap=11) all
    ///           return Ok (>= 10 is serveable, strict less-than).
    /// Workload: Open Storage; set_low_water_mark(10); attempt all
    ///           three constructors at snap in {5, 10, 11}.
    /// Expected: snap=5  → Err(SnapshotTooOld { low_water_mark: 10 })
    ///                     for all three constructors.
    ///           snap=10 → Ok (at-watermark serveable).
    ///           snap=11 → Ok (post-watermark serveable).
    #[test]
    fn kat_tx_begin_rejects_below_watermark() {
        // ---- shared: begin (Shared) ----
        let store = Storage::open(MemVfs::new()).unwrap();
        // First: with default lwm=0, begin(snap=5) must Ok (preserves
        // SP110-113 byte-net-0 behaviour).
        assert!(
            Tx::begin(&store, 5).is_ok(),
            "lwm=0 default: snap=5 must Ok (byte-net-0)",
        );

        // ---- Now lift the watermark and test all three constructors. ----
        let mut store = Storage::open(MemVfs::new()).unwrap();
        store.set_low_water_mark(10);

        // begin (Shared): snap=5 strictly less than lwm=10 → rejected.
        // Use match (Tx doesn't impl Debug; expect_err can't print it).
        match Tx::begin(&store, 5) {
            Err(TxError::SnapshotTooOld { low_water_mark: 10 }) => {}
            Err(other) => panic!(
                "snap=5 < lwm=10 expected SnapshotTooOld{{10}}, got {other:?}"
            ),
            Ok(_) => panic!("snap=5 < lwm=10 must reject — got Ok(_)"),
        }
        // begin (Shared): snap=10 at watermark → serveable (Decision 3+7).
        assert!(
            Tx::begin(&store, 10).is_ok(),
            "snap=10 == lwm=10 must Ok (at-watermark serveable)",
        );
        // begin (Shared): snap=11 above watermark → serveable.
        assert!(
            Tx::begin(&store, 11).is_ok(),
            "snap=11 > lwm=10 must Ok (post-watermark serveable)",
        );

        // begin_rw (Exclusive): same contract.
        match Tx::begin_rw(&mut store, 5) {
            Err(TxError::SnapshotTooOld { low_water_mark: 10 }) => {}
            Err(other) => panic!(
                "begin_rw snap=5 < lwm=10 expected SnapshotTooOld{{10}}, got {other:?}"
            ),
            Ok(_) => panic!("begin_rw snap=5 < lwm=10 must reject — got Ok(_)"),
        }
        assert!(
            Tx::begin_rw(&mut store, 10).is_ok(),
            "begin_rw snap=10 == lwm=10 must Ok (at-watermark serveable)",
        );

        // begin_ssi (Exclusive, SSI commit path): same contract.
        match Tx::begin_ssi(&mut store, 5) {
            Err(TxError::SnapshotTooOld { low_water_mark: 10 }) => {}
            Err(other) => panic!(
                "begin_ssi snap=5 < lwm=10 expected SnapshotTooOld{{10}}, got {other:?}"
            ),
            Ok(_) => panic!("begin_ssi snap=5 < lwm=10 must reject — got Ok(_)"),
        }
        assert!(
            Tx::begin_ssi(&mut store, 10).is_ok(),
            "begin_ssi snap=10 == lwm=10 must Ok (at-watermark serveable)",
        );
    }
}

#[cfg(test)]
mod tx_si_kats {
    //! SP112 T2 hand-derived KATs for the SI write-side path:
    //! Tx::write buffering, read-your-writes overlay, Tx::commit's
    //! conflict-check + apply, snapshot/commit boundary validation,
    //! and the commit_opnum=0 edge case.
    //!
    //! Each KAT writes the expected output BY HAND before running the
    //! code (KAT discipline — never capture-and-assert). Tests exercise
    //! the SI contract surface described in the S2.3 design Decisions
    //! 2-6 (BTreeMap write_set, read-your-writes overlay, apply-time
    //! conflict check at SM, commit API split).
    use super::*;
    use crate::mvcc::{put_versioned, SnapshotRead};
    use crate::Storage;
    use kessel_io::MemVfs;

    fn obj(n: u8) -> [u8; 16] {
        let mut a = [0u8; 16];
        a[15] = n;
        a
    }

    // KAT-1 (`Tx::write buffers`): Tx::write inserts into write_set.
    // Hand-derived: empty write_set; write(1, obj(1), Some([0xAA])) =>
    // write_set has exactly one entry ((1, obj(1)) -> Some([0xAA])).
    #[test]
    fn kat_write_inserts_into_write_set() {
        let store = Storage::open(MemVfs::new()).unwrap();
        let mut tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
        tx.write(1, &obj(1), Some(vec![0xAA]));
        assert_eq!(tx.write_set().len(), 1, "exactly one buffered write");
        assert_eq!(
            tx.write_set().get(&(1u32, obj(1))),
            Some(&Some(vec![0xAA])),
            "value must be Some([0xAA])"
        );
        tx.abort();
    }

    // KAT-2 (`read-your-writes Found`): write(k, Some(v_new)); read(k) =>
    // Found(v_new), NOT the snapshot version.
    // Hand-derived: put_versioned(1, obj(1), 0, [0xAA]); Tx at snap=0;
    // write(1, obj(1), Some([0xBB])); read(1, obj(1)) => Found([0xBB])
    // (the buffered value wins over the snapshot value [0xAA]).
    #[test]
    fn kat_read_your_writes_returns_buffered_found() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();
        let mut tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
        tx.write(1, &obj(1), Some(vec![0xBB]));
        match tx.read(1, &obj(1)) {
            SnapshotRead::Found(v) => assert_eq!(
                v,
                vec![0xBB],
                "buffered [0xBB] wins over snapshot [0xAA]"
            ),
            other => panic!("expected Found([0xBB]); got {:?}", other),
        }
    }

    // KAT-3 (`read-your-writes Tombstoned`): write(k, None); read(k) =>
    // Tombstoned, honoring the buffered tombstone even though the
    // snapshot has a Found value.
    // Hand-derived: put_versioned(1, obj(1), 0, [0xAA]); Tx at snap=0;
    // write(1, obj(1), None); read => Tombstoned (buffered tombstone
    // shadows the snapshot's live [0xAA]).
    #[test]
    fn kat_read_your_writes_buffered_tombstone() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();
        let mut tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
        tx.write(1, &obj(1), None);
        assert!(
            matches!(tx.read(1, &obj(1)), SnapshotRead::Tombstoned),
            "buffered tombstone must return Tombstoned"
        );
    }

    // KAT-4 (`read-then-write doesn't override`): read returns the
    // snapshot value first; then write shadows it on subsequent reads.
    // Hand-derived: put_versioned(1, obj(1), 0, [0xAA]); Tx at snap=0;
    // read1 => Found([0xAA]) (snapshot path); write(1, obj(1), Some([0xCC]));
    // read2 => Found([0xCC]) (overlay path now wins).
    #[test]
    fn kat_read_then_write_subsequent_read_sees_buffer() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();
        let mut tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
        match tx.read(1, &obj(1)) {
            SnapshotRead::Found(v) => assert_eq!(
                v,
                vec![0xAA],
                "pre-write read returns snapshot [0xAA]"
            ),
            other => panic!("expected Found([0xAA]); got {:?}", other),
        }
        tx.write(1, &obj(1), Some(vec![0xCC]));
        match tx.read(1, &obj(1)) {
            SnapshotRead::Found(v) => assert_eq!(
                v,
                vec![0xCC],
                "post-write read returns buffered [0xCC]"
            ),
            other => panic!("expected Found([0xCC]); got {:?}", other),
        }
    }

    // KAT-5 (`same-key coalesce`): write(k, v1); write(k, v2). BTreeMap
    // insert REPLACES — buffer has one entry with v2 (last-write-wins).
    // Hand-derived: write(1, obj(1), Some([0xAA])); write(1, obj(1),
    // Some([0xBB])); write_set.len() == 1; value == Some([0xBB]).
    #[test]
    fn kat_same_key_coalesce_last_write_wins() {
        let store = Storage::open(MemVfs::new()).unwrap();
        let mut tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
        tx.write(1, &obj(1), Some(vec![0xAA]));
        tx.write(1, &obj(1), Some(vec![0xBB]));
        assert_eq!(tx.write_set().len(), 1, "same-key writes coalesce");
        assert_eq!(
            tx.write_set().get(&(1u32, obj(1))),
            Some(&Some(vec![0xBB])),
            "last-write [0xBB] wins"
        );
        tx.abort();
    }

    // KAT-6 (`commit empty write-set`): Tx with no writes, commit =>
    // Committed { commit_opnum }; no MVCC writes happen.
    // Hand-derived: empty store; begin_rw at snap=0; commit(5) =>
    // Committed{5}; reading any key still returns NotYetWritten.
    #[test]
    fn kat_commit_empty_write_set_succeeds() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let tx = Tx::begin_rw(&mut store, 0).expect("SP114 T1: watermark=0; begin_rw always Ok");
        let out = tx.commit(5).expect("commit must not err");
        assert_eq!(out, TxCommitOutcome::Committed { commit_opnum: 5 });
        // Verify no writes leaked: any read returns NotYetWritten.
        assert!(matches!(
            crate::mvcc::get_at_snapshot(&store, 1, &obj(1), 5),
            SnapshotRead::NotYetWritten
        ));
    }

    // KAT-7 (`commit single write succeeds`): single write commit;
    // verify get_at_snapshot returns the value at commit_opnum.
    // Hand-derived: empty store; Tx at snap=0; write(1, obj(1),
    // Some([0xAA])); commit(1) => Committed{1};
    // get_at_snapshot(1, obj(1), 1) => Found([0xAA]).
    #[test]
    fn kat_commit_single_write_visible_at_commit_opnum() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        {
            let mut tx = Tx::begin_rw(&mut store, 0).expect("SP114 T1: watermark=0; begin_rw always Ok");
            tx.write(1, &obj(1), Some(vec![0xAA]));
            let out = tx.commit(1).expect("no err");
            assert_eq!(out, TxCommitOutcome::Committed { commit_opnum: 1 });
        }
        match crate::mvcc::get_at_snapshot(&store, 1, &obj(1), 1) {
            SnapshotRead::Found(v) => assert_eq!(v, vec![0xAA]),
            other => panic!("expected Found([0xAA]); got {:?}", other),
        }
    }

    // KAT-8 (`commit conflict detection`): Tx_A at snap=10 writes k;
    // Tx_A commits at 20 => Committed. Tx_B at snap=10 writes same k;
    // Tx_B commits at 30 => Aborted{conflicting_key=k} (because k now
    // has a version at opnum=20, which is in (snap=10, commit-1=29]).
    // Hand-derived: the first committer wins; the second sees a
    // version in its conflict window and must abort.
    #[test]
    fn kat_commit_write_write_conflict_aborts_second() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        {
let mut tx_a = Tx::begin_rw(&mut store, 10).expect("SP114 T1: watermark=0; begin always Ok");
            tx_a.write(1, &obj(1), Some(vec![0xAA]));
            let out_a = tx_a.commit(20).expect("a no-err");
            assert_eq!(
                out_a,
                TxCommitOutcome::Committed { commit_opnum: 20 },
                "first committer wins"
            );
        }
        // Tx_B at the SAME snapshot (10) — pinned BEFORE tx_a committed.
        // tx_b sees the snapshot=10 world (no version of k); writes k;
        // commits at 30. The has_version_in_range(10, 29) check finds
        // tx_a's commit at opnum=20 — CONFLICT.
let mut tx_b = Tx::begin_rw(&mut store, 10).expect("SP114 T1: watermark=0; begin always Ok");
        tx_b.write(1, &obj(1), Some(vec![0xBB]));
        let out_b = tx_b.commit(30).expect("b no-err");
        assert_eq!(
            out_b,
            TxCommitOutcome::Aborted { conflicting_key: (1u32, obj(1)) },
            "second committer aborts on the first's commit"
        );
    }

    // KAT-9 (`commit no conflict on disjoint`): Tx_A writes k1; Tx_B
    // writes k2; both at same snap; both commit at different opnums =>
    // both Committed.
    // Hand-derived: disjoint keys never conflict — has_version_in_range
    // for k1 finds nothing in tx_b's window, and vice versa.
    #[test]
    fn kat_commit_disjoint_keys_both_succeed() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        {
let mut tx_a = Tx::begin_rw(&mut store, 0).expect("SP114 T1: watermark=0; begin always Ok");
            tx_a.write(1, &obj(1), Some(vec![0xAA]));
            let out_a = tx_a.commit(1).expect("a no-err");
            assert_eq!(out_a, TxCommitOutcome::Committed { commit_opnum: 1 });
        }
let mut tx_b = Tx::begin_rw(&mut store, 0).expect("SP114 T1: watermark=0; begin always Ok");
        tx_b.write(1, &obj(2), Some(vec![0xBB])); // disjoint key
        let out_b = tx_b.commit(2).expect("b no-err");
        assert_eq!(out_b, TxCommitOutcome::Committed { commit_opnum: 2 });
    }

    // KAT-10 (`commit_opnum=0 skip-check`): Tx writes; commit at opnum=0
    // => Committed (no underflow; conflict-check skipped per Decision 5).
    // Hand-derived: empty store; Tx at snap=0; write(1, obj(1),
    // Some([0xAA])); commit(0) => Committed{0}. The `commit_opnum - 1`
    // underflow guard must NOT trip.
    #[test]
    fn kat_commit_opnum_zero_skips_conflict_check() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let mut tx = Tx::begin_rw(&mut store, 0).expect("SP114 T1: watermark=0; begin_rw always Ok");
        tx.write(1, &obj(1), Some(vec![0xAA]));
        let out = tx.commit(0).expect("no err — no underflow");
        assert_eq!(out, TxCommitOutcome::Committed { commit_opnum: 0 });
    }

    // KAT-11 (`snapshot > commit_opnum rejection`): Tx at snap=20;
    // commit at opnum=10 => Err(SnapshotOutOfRange{snap:20, commit:10}).
    // Hand-derived: malformed input — snapshot must NOT exceed commit;
    // boundary is strict `>`.
    #[test]
    fn kat_commit_snapshot_greater_than_commit_errors() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let mut tx = Tx::begin_rw(&mut store, 20).expect("SP114 T1: watermark=0; begin_rw always Ok");
        tx.write(1, &obj(1), Some(vec![0xAA]));
        let err = tx.commit(10).expect_err("snapshot > commit must err");
        assert!(
            matches!(
                err,
                TxError::SnapshotOutOfRange { snapshot: 20, commit: 10 }
            ),
            "expected SnapshotOutOfRange{{snap:20, commit:10}}; got {:?}",
            err
        );
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
        let tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
        assert!(tx.read_set().is_empty());
        assert!(tx.commit_read_only().is_ok());
    }

    // CV-2: Re-read same key 100 times — read_set stays at size 1 (set semantics).
    #[test]
    fn cv_re_read_same_key_100x_size_1() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();
        let mut tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
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
        let mut tx = Tx::begin(&store, 999).expect("SP114 T1: watermark=0; begin always Ok");
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
        let mut tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
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
        let mut tx = Tx::begin(&store, 49).expect("SP114 T1: watermark=0; begin always Ok");
        for i in 0..50u8 {
            let _ = tx.read(1, &obj(i));
        }
        assert_eq!(tx.read_set().len(), 50);
        assert!(tx.commit_read_only().is_ok());
    }
}

#[cfg(test)]
mod tx_si_coverage {
    //! SP112 T4 coverage tests for SI edge cases.
    //!
    //! Five scenarios that exercise the boundary behaviour of the
    //! SI write-side path added in SP112 T2/T3 but not covered by the
    //! 11 KATs or 5 integration tests:
    //!   CV-1  empty write-set commit is a no-op
    //!   CV-2  abort discards buffered writes
    //!   CV-3  same-key writes coalesce (last-write-wins within Tx)
    //!   CV-4  1000-write commit (large write-set, spot-check visibility)
    //!   CV-5  mixed write→tombstone→write same key (tombstone overwritten)
    use super::*;
    use crate::mvcc::{get_at_snapshot, put_versioned, SnapshotRead};
    use crate::Storage;
    use kessel_io::MemVfs;

    fn obj(n: u8) -> [u8; 16] {
        let mut a = [0u8; 16];
        a[15] = n;
        a
    }

    // CV-1: Empty write_set commit is a no-op — storage state unchanged.
    // Hand-derived: begin_rw at snap=0; no writes; commit(5) =>
    // Committed{5}; get_at_snapshot(obj(1), 5) still returns the pre-existing
    // opnum=0 version (no new MVCC entry was installed by the Tx).
    #[test]
    fn cv_empty_write_set_commit_no_op() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();
        let tx = Tx::begin_rw(&mut store, 0).expect("SP114 T1: watermark=0; begin_rw always Ok");
        let out = tx.commit(5).unwrap();
        assert_eq!(
            out,
            TxCommitOutcome::Committed { commit_opnum: 5 },
            "empty write-set must commit with Committed{{5}}"
        );
        // No new version installed; snapshot at opnum=5 still sees only opnum=0 version.
        match get_at_snapshot(&store, 1, &obj(1), 5) {
            SnapshotRead::Found(v) => assert_eq!(v, vec![0xAA], "pre-existing value unchanged"),
            other => panic!("got {:?}", other),
        }
    }

    // CV-2: Tx::abort discards buffered writes — no version installed.
    // Hand-derived: begin(&store, 0); write(obj(1), [0xAA]); write(obj(2), [0xBB]);
    // abort(); get_at_snapshot(obj(1), 100) => NotYetWritten;
    // get_at_snapshot(obj(2), 100) => NotYetWritten.
    #[test]
    fn cv_abort_discards_buffered_writes() {
        let store = Storage::open(MemVfs::new()).unwrap();
        let mut tx = Tx::begin(&store, 0).expect("SP114 T1: watermark=0; begin always Ok");
        tx.write(1, &obj(1), Some(vec![0xAA]));
        tx.write(1, &obj(2), Some(vec![0xBB]));
        tx.abort();
        // No version was installed for either key — abort drops the write_set.
        assert!(
            matches!(get_at_snapshot(&store, 1, &obj(1), 100), SnapshotRead::NotYetWritten),
            "aborted write on obj(1) must not appear in storage"
        );
        assert!(
            matches!(get_at_snapshot(&store, 1, &obj(2), 100), SnapshotRead::NotYetWritten),
            "aborted write on obj(2) must not appear in storage"
        );
    }

    // CV-3: Same-key writes coalesce in write_set (last-write-wins);
    // only the final value lands at commit.
    // Hand-derived: begin_rw at snap=0; write(obj(1), [0xAA]); write(obj(1), [0xBB]);
    // write(obj(1), [0xCC]); write_set.len()==1 (coalesced); commit(1) =>
    // Committed{1}; get_at_snapshot(obj(1), 1) => Found([0xCC]).
    #[test]
    fn cv_same_key_writes_coalesce_in_write_set() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let mut tx = Tx::begin_rw(&mut store, 0).expect("SP114 T1: watermark=0; begin_rw always Ok");
        tx.write(1, &obj(1), Some(vec![0xAA]));
        tx.write(1, &obj(1), Some(vec![0xBB]));
        tx.write(1, &obj(1), Some(vec![0xCC]));
        assert_eq!(tx.write_set().len(), 1, "three writes to same key must coalesce to 1 entry");
        let out = tx.commit(1).unwrap();
        assert_eq!(
            out,
            TxCommitOutcome::Committed { commit_opnum: 1 },
            "coalesced commit must succeed"
        );
        match get_at_snapshot(&store, 1, &obj(1), 1) {
            SnapshotRead::Found(v) => assert_eq!(v, vec![0xCC], "last write wins"),
            other => panic!("got {:?}", other),
        }
    }

    // CV-4: Large write_set — 1000 distinct writes commit cleanly;
    // all 1000 entries are visible at the commit snapshot.
    // Hand-derived: begin_rw at snap=0; write 1000 keys with deterministic
    // key `[0,0,0,0, 0,0,0,0, 0,0,0,0, i>>24, i>>16, i>>8, i&0xFF]`
    // and value `[(i & 0xFF) as u8]`; write_set.len()==1000; commit(1) =>
    // Committed{1}; spot-check key 500 => Found([500 & 0xFF]).
    #[test]
    fn cv_large_write_set_1000_writes_commit() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let mut tx = Tx::begin_rw(&mut store, 0).expect("SP114 T1: watermark=0; begin_rw always Ok");
        for i in 0..1000u32 {
            let mut k = [0u8; 16];
            k[12..16].copy_from_slice(&i.to_be_bytes());
            tx.write(1, &k, Some(vec![(i & 0xFF) as u8]));
        }
        assert_eq!(tx.write_set().len(), 1000, "1000 distinct keys must not coalesce");
        let out = tx.commit(1).unwrap();
        assert_eq!(
            out,
            TxCommitOutcome::Committed { commit_opnum: 1 },
            "1000-write commit must succeed"
        );
        // Spot-check: key 500 has value [500 & 0xFF == 244].
        let mut k = [0u8; 16];
        k[12..16].copy_from_slice(&500u32.to_be_bytes());
        match get_at_snapshot(&store, 1, &k, 1) {
            SnapshotRead::Found(v) => assert_eq!(v, vec![(500u32 & 0xFF) as u8], "key 500 value matches"),
            other => panic!("got {:?}", other),
        }
    }

    // CV-5: Mixed write-tombstone-write same key — final state is the
    // last write. The intermediate tombstone is overwritten before commit.
    // Hand-derived: begin_rw at snap=0; write(obj(1), Some([0xAA]));
    // write(obj(1), None) [tombstone]; write(obj(1), Some([0xCC]));
    // write_set.len()==1 (coalesced); commit(1) => Committed{1};
    // get_at_snapshot(obj(1), 1) => Found([0xCC]) (not Tombstoned).
    #[test]
    fn cv_mixed_write_tombstone_write_same_key() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let mut tx = Tx::begin_rw(&mut store, 0).expect("SP114 T1: watermark=0; begin_rw always Ok");
        tx.write(1, &obj(1), Some(vec![0xAA])); // initial write
        tx.write(1, &obj(1), None);             // intermediate tombstone
        tx.write(1, &obj(1), Some(vec![0xCC])); // final write overwrites tombstone
        assert_eq!(tx.write_set().len(), 1, "write-tombstone-write coalesces to 1 entry");
        let out = tx.commit(1).unwrap();
        assert_eq!(
            out,
            TxCommitOutcome::Committed { commit_opnum: 1 },
            "mixed write-tombstone-write must commit"
        );
        match get_at_snapshot(&store, 1, &obj(1), 1) {
            SnapshotRead::Found(v) => assert_eq!(
                v,
                vec![0xCC],
                "final write [0xCC] wins after intermediate tombstone"
            ),
            other => panic!("expected Found([0xCC]) after tombstone overwrite, got {:?}", other),
        }
    }
}
