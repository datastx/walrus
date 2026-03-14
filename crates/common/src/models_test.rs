use super::*;

#[test]
fn test_lsn_parse_and_display() {
    let lsn = Lsn::parse("0/16B6C50").unwrap();
    assert_eq!(lsn.0, 0x16B6C50);
    assert_eq!(lsn.to_string(), "0/16B6C50");

    let lsn2 = Lsn::parse("16/B374D848").unwrap();
    assert_eq!(lsn2.0, 0x16_B374D848);
    assert_eq!(lsn2.to_string(), "16/B374D848");
}

#[test]
fn test_lsn_ordering() {
    let a = Lsn::parse("0/100").unwrap();
    let b = Lsn::parse("0/200").unwrap();
    assert!(a < b);
}

#[test]
fn test_lsn_zero() {
    assert_eq!(Lsn::ZERO.to_string(), "0/0");
}

#[test]
fn test_table_phase_roundtrip() {
    for phase in [
        TablePhase::Pending,
        TablePhase::Backfilling,
        TablePhase::Streaming,
    ] {
        assert_eq!(phase.as_str().parse::<TablePhase>().unwrap(), phase);
    }
}

#[test]
fn test_ctid_partition_equality() {
    let a = CtidPartition {
        id: 0,
        start_page: 0,
        end_page: 100,
    };
    let b = CtidPartition {
        id: 0,
        start_page: 0,
        end_page: 100,
    };
    assert_eq!(a, b);
}
