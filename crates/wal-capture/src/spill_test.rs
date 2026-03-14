use super::*;
use pgiceberg_common::models::{CdcColumn, CdcOp, CdcRecord, Lsn};
use tempfile::TempDir;

fn make_record(schema: &str, table: &str, op: CdcOp, id: i64) -> CdcRecord {
    CdcRecord {
        table_schema: schema.to_string(),
        table_name: table.to_string(),
        op,
        columns: vec![
            CdcColumn {
                name: "id".to_string(),
                type_oid: 23, // int4
                value: Some(id.to_string().into_bytes()),
            },
            CdcColumn {
                name: "data".to_string(),
                type_oid: 25, // text
                value: Some(format!("row_{}", id).into_bytes()),
            },
        ],
        estimated_bytes: 128,
        commit_lsn: Lsn::ZERO,
        commit_ts: 0,
        seq: 0,
    }
}

#[test]
fn test_spill_roundtrip_small_batch() {
    let tmp = TempDir::new().unwrap();
    let mut spill = SpillFile::create(tmp.path()).unwrap();

    let records: Vec<CdcRecord> = (0..50)
        .map(|i| make_record("public", "users", CdcOp::Insert, i))
        .collect();

    spill.write_batch(&records).unwrap();
    assert_eq!(spill.record_count(), 50);

    let mut reader = spill.into_reader().unwrap();
    let chunk = reader.read_chunk(100).unwrap();
    assert_eq!(chunk.len(), 50);

    for (i, rec) in chunk.iter().enumerate() {
        assert_eq!(rec.table_schema, "public");
        assert_eq!(rec.table_name, "users");
        assert_eq!(rec.op, CdcOp::Insert);
        assert_eq!(
            rec.columns[0].value.as_ref().unwrap(),
            &i.to_string().into_bytes()
        );
    }

    // No more records
    let empty = reader.read_chunk(100).unwrap();
    assert!(empty.is_empty());
}

#[test]
fn test_spill_chunked_reading() {
    let tmp = TempDir::new().unwrap();
    let mut spill = SpillFile::create(tmp.path()).unwrap();

    for i in 0..2500 {
        let record = make_record("myschema", "orders", CdcOp::Update, i);
        spill.append(&record).unwrap();
    }

    let mut reader = spill.into_reader().unwrap();
    let mut total = 0;
    loop {
        let chunk = reader.read_chunk(1000).unwrap();
        if chunk.is_empty() {
            break;
        }
        total += chunk.len();
    }
    assert_eq!(total, 2500);
}

#[test]
fn test_spill_mixed_ops() {
    let tmp = TempDir::new().unwrap();
    let mut spill = SpillFile::create(tmp.path()).unwrap();

    spill
        .append(&make_record("public", "t", CdcOp::Insert, 1))
        .unwrap();
    spill
        .append(&make_record("public", "t", CdcOp::Update, 2))
        .unwrap();
    spill
        .append(&make_record("public", "t", CdcOp::Delete, 3))
        .unwrap();

    let mut reader = spill.into_reader().unwrap();
    let chunk = reader.read_chunk(10).unwrap();
    assert_eq!(chunk.len(), 3);
    assert_eq!(chunk[0].op, CdcOp::Insert);
    assert_eq!(chunk[1].op, CdcOp::Update);
    assert_eq!(chunk[2].op, CdcOp::Delete);
}

#[test]
fn test_spill_null_column_values() {
    let tmp = TempDir::new().unwrap();
    let mut spill = SpillFile::create(tmp.path()).unwrap();

    let record = CdcRecord {
        table_schema: "public".to_string(),
        table_name: "t".to_string(),
        op: CdcOp::Insert,
        columns: vec![
            CdcColumn {
                name: "id".to_string(),
                type_oid: 23,
                value: Some(b"1".to_vec()),
            },
            CdcColumn {
                name: "nullable_col".to_string(),
                type_oid: 25,
                value: None,
            },
        ],
        estimated_bytes: 64,
        commit_lsn: Lsn::ZERO,
        commit_ts: 0,
        seq: 0,
    };

    spill.append(&record).unwrap();
    let mut reader = spill.into_reader().unwrap();
    let chunk = reader.read_chunk(10).unwrap();
    assert_eq!(chunk.len(), 1);
    assert!(chunk[0].columns[1].value.is_none());
}

#[test]
fn test_spill_cleanup_on_drop() {
    let tmp = TempDir::new().unwrap();
    let spill_dir = tmp.path().join("spill");

    let spill = SpillFile::create(tmp.path()).unwrap();
    assert!(spill_dir.exists());
    // Count files in spill dir
    let count_before = std::fs::read_dir(&spill_dir).unwrap().count();
    assert_eq!(count_before, 1);

    // Drop without converting to reader (simulates abort)
    drop(spill);

    let count_after = std::fs::read_dir(&spill_dir).unwrap().count();
    assert_eq!(count_after, 0);
}

#[test]
fn test_spill_reader_cleanup_on_drop() {
    let tmp = TempDir::new().unwrap();
    let spill_dir = tmp.path().join("spill");

    let mut spill = SpillFile::create(tmp.path()).unwrap();
    spill
        .append(&make_record("public", "t", CdcOp::Insert, 1))
        .unwrap();
    let reader = spill.into_reader().unwrap();

    let count_before = std::fs::read_dir(&spill_dir).unwrap().count();
    assert_eq!(count_before, 1);

    drop(reader);

    let count_after = std::fs::read_dir(&spill_dir).unwrap().count();
    assert_eq!(count_after, 0);
}

#[test]
fn test_spill_large_transaction() {
    let tmp = TempDir::new().unwrap();
    let mut spill = SpillFile::create(tmp.path()).unwrap();

    // Simulate a large transaction with 100K records
    let record_count = 100_000;
    for i in 0..record_count {
        let record = make_record("public", "big_table", CdcOp::Insert, i);
        spill.append(&record).unwrap();
    }
    assert_eq!(spill.record_count(), record_count as usize);

    let mut reader = spill.into_reader().unwrap();
    let mut total = 0;
    loop {
        let chunk = reader.read_chunk(SPILL_CHUNK_SIZE).unwrap();
        if chunk.is_empty() {
            break;
        }
        total += chunk.len();
    }
    assert_eq!(total, record_count as usize);
}
