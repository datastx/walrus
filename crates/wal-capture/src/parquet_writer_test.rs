use super::*;

use crate::decoder::{CachedRelation, RelationColumn};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pgiceberg_common::models::{CdcColumn, CdcOp, CdcRecord, Lsn};
use tempfile::TempDir;

fn test_relation() -> CachedRelation {
    CachedRelation {
        schema: "public".to_string(),
        name: "users".to_string(),
        columns: vec![
            RelationColumn {
                flags: 1,
                name: "id".to_string(),
                type_oid: 23, // int4
                type_modifier: -1,
            },
            RelationColumn {
                flags: 0,
                name: "name".to_string(),
                type_oid: 25, // text
                type_modifier: -1,
            },
        ],
    }
}

fn make_record(id: &str, name: Option<&str>, op: CdcOp, ts: i64, seq: i64) -> CdcRecord {
    CdcRecord {
        table_schema: "public".to_string(),
        table_name: "users".to_string(),
        op,
        columns: vec![
            CdcColumn {
                name: "id".to_string(),
                type_oid: 23,
                value: Some(id.as_bytes().to_vec()),
            },
            CdcColumn {
                name: "name".to_string(),
                type_oid: 25,
                value: name.map(|s| s.as_bytes().to_vec()),
            },
        ],
        estimated_bytes: 64,
        commit_lsn: Lsn(100),
        commit_ts: ts,
        seq,
    }
}

fn read_parquet_columns(path: &std::path::Path) -> Vec<String> {
    let file = std::fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = reader.schema().clone();
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

fn read_parquet_row_count(path: &std::path::Path) -> usize {
    let file = std::fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut total = 0;
    for batch in reader {
        total += batch.unwrap().num_rows();
    }
    total
}

// ── Test 1: Metadata columns exist with correct values ──────────────────────

#[test]
fn test_write_cdc_includes_metadata_columns() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.parquet");
    let rel = test_relation();

    let records = vec![make_record("1", Some("alice"), CdcOp::Insert, 1000, 0)];
    write_cdc_records_to_parquet(&records, &rel, &path).unwrap();

    let columns = read_parquet_columns(&path);
    assert!(columns.contains(&"_pgiceberg_op".to_string()));
    assert!(columns.contains(&"_pgiceberg_ts".to_string()));
    assert!(columns.contains(&"_pgiceberg_seq".to_string()));

    // Verify values via DuckDB (available in this workspace)
    let file = std::fs::File::open(&path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();

    let op_col = batch
        .column_by_name("_pgiceberg_op")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(op_col.value(0), "I");

    let ts_col = batch
        .column_by_name("_pgiceberg_ts")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(ts_col.value(0), 1000);

    let seq_col = batch
        .column_by_name("_pgiceberg_seq")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(seq_col.value(0), 0);
}

// ── Test 2: Op values written correctly ─────────────────────────────────────

#[test]
fn test_write_cdc_op_values() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.parquet");
    let rel = test_relation();

    let records = vec![
        make_record("1", Some("alice"), CdcOp::Insert, 100, 0),
        make_record("2", Some("bob"), CdcOp::Update, 200, 1),
        make_record("3", Some("charlie"), CdcOp::Delete, 300, 2),
    ];
    write_cdc_records_to_parquet(&records, &rel, &path).unwrap();

    let file = std::fs::File::open(&path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();

    let op_col = batch
        .column_by_name("_pgiceberg_op")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(op_col.value(0), "I");
    assert_eq!(op_col.value(1), "U");
    assert_eq!(op_col.value(2), "D");
}

// ── Test 3: Null columns ────────────────────────────────────────────────────

#[test]
fn test_write_cdc_null_columns() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.parquet");
    let rel = test_relation();

    let records = vec![make_record("1", None, CdcOp::Delete, 100, 0)];
    write_cdc_records_to_parquet(&records, &rel, &path).unwrap();

    let file = std::fs::File::open(&path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();

    let name_col = batch
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert!(name_col.is_null(0));
}

// ── Test 4: Empty records ───────────────────────────────────────────────────

#[test]
fn test_write_cdc_empty_records() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.parquet");
    let rel = test_relation();

    let result = write_cdc_records_to_parquet(&[], &rel, &path);
    assert!(result.is_ok());
    assert!(!path.exists()); // No file written for empty input
}

// ── Test 5: Large batch (tests 10K chunking) ────────────────────────────────

#[test]
fn test_write_cdc_large_batch() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.parquet");
    let rel = test_relation();

    let records: Vec<CdcRecord> = (0..15_000)
        .map(|i| {
            let id = i.to_string();
            CdcRecord {
                table_schema: "public".to_string(),
                table_name: "users".to_string(),
                op: CdcOp::Insert,
                columns: vec![
                    CdcColumn {
                        name: "id".to_string(),
                        type_oid: 23,
                        value: Some(id.as_bytes().to_vec()),
                    },
                    CdcColumn {
                        name: "name".to_string(),
                        type_oid: 25,
                        value: Some(format!("user_{}", i).as_bytes().to_vec()),
                    },
                ],
                estimated_bytes: 64,
                commit_lsn: Lsn(100),
                commit_ts: 1000,
                seq: i as i64,
            }
        })
        .collect();

    write_cdc_records_to_parquet(&records, &rel, &path).unwrap();

    let row_count = read_parquet_row_count(&path);
    assert_eq!(row_count, 15_000);
}
