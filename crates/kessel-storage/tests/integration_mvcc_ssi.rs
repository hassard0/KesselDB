//! SP113 / S2.4: Integration tests for the SSI promotion path.
//! T2/T3/T4 will populate this file with the dangerous-structure +
//! 3-replica byte-identity + coverage tests.

#![forbid(unsafe_code)]

// T1 scaffold-only tests — establish type shapes for the SSI surface.

#[test]
fn it_scaffold_tx_commitoutcome_aborteddangerousstructure_constructible() {
    use kessel_storage::tx::TxCommitOutcome;
    let o = TxCommitOutcome::AbortedDangerousStructure { other_commit_opnum: 7 };
    assert!(matches!(o, TxCommitOutcome::AbortedDangerousStructure { other_commit_opnum: 7 }));
}

#[test]
fn it_scaffold_abortreason_dangerousstructure_constructible() {
    use kessel_proto::AbortReason;
    let r = AbortReason::DangerousStructure { other_commit_opnum: 11 };
    assert!(matches!(r, AbortReason::DangerousStructure { other_commit_opnum: 11 }));
}
