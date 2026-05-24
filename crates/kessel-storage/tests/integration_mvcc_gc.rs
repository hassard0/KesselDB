//! SP114 / S2.5: Integration tests for the GC + dynamic watermark surface.
//!
//! T1 scaffold: establish type shapes. T2/T3/T4 will add reclamation KATs,
//! SP113-supersession proofs, 3-replica byte-identity, and full coverage tests.

#![forbid(unsafe_code)]

// ---- Scaffold tests: verify the new types are constructible ----

/// T1 scaffold: TxError::SnapshotTooOld is constructible and pattern-matches.
#[test]
fn it_scaffold_tx_error_snapshot_too_old_constructible() {
    use kessel_storage::tx::TxError;
    let e = TxError::SnapshotTooOld { low_water_mark: 42 };
    assert!(
        matches!(e, TxError::SnapshotTooOld { low_water_mark: 42 }),
        "TxError::SnapshotTooOld must be constructible with low_water_mark=42"
    );
    // Verify Display doesn't panic.
    let msg = e.to_string();
    assert!(msg.contains("42"), "Display should contain the low_water_mark value");
}

/// T1 scaffold: Op::AdvanceWatermark is constructible, round-trips through
/// encode/decode, and carries the correct low_water_mark field.
#[test]
fn it_scaffold_op_advance_watermark_encode_decode_roundtrip() {
    use kessel_proto::Op;

    // Constructibility.
    let op = Op::AdvanceWatermark { low_water_mark: 7 };
    assert!(
        matches!(op, Op::AdvanceWatermark { low_water_mark: 7 }),
        "Op::AdvanceWatermark must be constructible with low_water_mark=7"
    );

    // Wire tag = 45.
    assert_eq!(op.kind(), 45, "Op::AdvanceWatermark must have wire tag 45");

    // Encode/decode round-trip.
    let encoded = op.encode();
    let decoded = Op::decode(&encoded).expect("Op::decode must succeed for a valid AdvanceWatermark frame");
    assert!(
        matches!(decoded, Op::AdvanceWatermark { low_water_mark: 7 }),
        "Op::decode must return Op::AdvanceWatermark {{ low_water_mark: 7 }} after encoding"
    );

    // Boundary: low_water_mark = 0 round-trips.
    let op_zero = Op::AdvanceWatermark { low_water_mark: 0 };
    let enc_zero = op_zero.encode();
    let dec_zero = Op::decode(&enc_zero).expect("Op::decode must succeed for low_water_mark=0");
    assert!(matches!(dec_zero, Op::AdvanceWatermark { low_water_mark: 0 }));

    // Boundary: u64::MAX round-trips.
    let op_max = Op::AdvanceWatermark { low_water_mark: u64::MAX };
    let enc_max = op_max.encode();
    let dec_max = Op::decode(&enc_max).expect("Op::decode must succeed for low_water_mark=u64::MAX");
    assert!(matches!(dec_max, Op::AdvanceWatermark { low_water_mark: u64::MAX }));
}
