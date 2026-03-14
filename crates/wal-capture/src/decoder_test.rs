use super::*;

fn make_begin_bytes(lsn: u64, ts: i64, xid: u32) -> Vec<u8> {
    let mut buf = vec![b'B'];
    buf.extend_from_slice(&lsn.to_be_bytes());
    buf.extend_from_slice(&ts.to_be_bytes());
    buf.extend_from_slice(&xid.to_be_bytes());
    buf
}

fn make_commit_bytes(flags: u8, commit_lsn: u64, end_lsn: u64, ts: i64) -> Vec<u8> {
    let mut buf = vec![b'C', flags];
    buf.extend_from_slice(&commit_lsn.to_be_bytes());
    buf.extend_from_slice(&end_lsn.to_be_bytes());
    buf.extend_from_slice(&ts.to_be_bytes());
    buf
}

#[test]
fn test_decode_begin() {
    let data = make_begin_bytes(0x16B6C50, 1000000, 42);
    let msg = decode_pgoutput(&data).unwrap();
    match msg {
        PgOutputMessage::Begin {
            final_lsn,
            commit_ts,
            xid,
        } => {
            assert_eq!(final_lsn.0, 0x16B6C50);
            assert_eq!(commit_ts, 1000000);
            assert_eq!(xid, 42);
        }
        _ => panic!("Expected Begin, got {:?}", msg),
    }
}

#[test]
fn test_decode_commit() {
    let data = make_commit_bytes(0, 100, 200, 999);
    let msg = decode_pgoutput(&data).unwrap();
    match msg {
        PgOutputMessage::Commit {
            commit_lsn,
            end_lsn,
            commit_ts,
            ..
        } => {
            assert_eq!(commit_lsn.0, 100);
            assert_eq!(end_lsn.0, 200);
            assert_eq!(commit_ts, 999);
        }
        _ => panic!("Expected Commit"),
    }
}

#[test]
fn test_decode_unknown() {
    let data = vec![b'Z', 1, 2, 3];
    let msg = decode_pgoutput(&data).unwrap();
    match msg {
        PgOutputMessage::Unknown(t) => assert_eq!(t, b'Z'),
        _ => panic!("Expected Unknown"),
    }
}

#[test]
fn test_relation_cache() {
    let mut cache = RelationCache::new();
    cache.update(
        12345,
        "public".to_string(),
        "users".to_string(),
        vec![RelationColumn {
            flags: 1,
            name: "id".to_string(),
            type_oid: 23,
            type_modifier: -1,
        }],
    );

    let rel = cache.get(12345).unwrap();
    assert_eq!(rel.schema, "public");
    assert_eq!(rel.name, "users");
    assert_eq!(rel.columns[0].name, "id");

    assert!(cache.get(99999).is_none());
}
