use super::*;

use arrow::record_batch::RecordBatch;
use arrow_array::{Int32Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper: write a Parquet file from column data.
/// Schema: (pk INT32, value UTF8, _pgiceberg_op UTF8, _pgiceberg_ts INT64, _pgiceberg_seq INT64)
fn write_test_parquet(
    dir: &std::path::Path,
    name: &str,
    rows: &[(i32, &str, &str, i64, i64)],
) -> String {
    let schema = test_schema();
    let path = dir.join(name);

    let pks: Vec<i32> = rows.iter().map(|r| r.0).collect();
    let vals: Vec<&str> = rows.iter().map(|r| r.1).collect();
    let ops: Vec<&str> = rows.iter().map(|r| r.2).collect();
    let tss: Vec<i64> = rows.iter().map(|r| r.3).collect();
    let seqs: Vec<i64> = rows.iter().map(|r| r.4).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(pks)),
            Arc::new(StringArray::from(vals)),
            Arc::new(StringArray::from(ops)),
            Arc::new(Int64Array::from(tss)),
            Arc::new(Int64Array::from(seqs)),
        ],
    )
    .unwrap();

    let file = std::fs::File::create(&path).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    path.to_string_lossy().into_owned()
}

/// Helper: write a Parquet file with composite PK.
/// Schema: (pk1 INT32, pk2 INT32, value UTF8, _pgiceberg_op UTF8, _pgiceberg_ts INT64, _pgiceberg_seq INT64)
fn write_composite_pk_parquet(
    dir: &std::path::Path,
    name: &str,
    rows: &[(i32, i32, &str, &str, i64, i64)],
) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk1", DataType::Int32, false),
        Field::new("pk2", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
        Field::new("_pgiceberg_op", DataType::Utf8, false),
        Field::new("_pgiceberg_ts", DataType::Int64, false),
        Field::new("_pgiceberg_seq", DataType::Int64, false),
    ]));
    let path = dir.join(name);

    let pk1s: Vec<i32> = rows.iter().map(|r| r.0).collect();
    let pk2s: Vec<i32> = rows.iter().map(|r| r.1).collect();
    let vals: Vec<&str> = rows.iter().map(|r| r.2).collect();
    let ops: Vec<&str> = rows.iter().map(|r| r.3).collect();
    let tss: Vec<i64> = rows.iter().map(|r| r.4).collect();
    let seqs: Vec<i64> = rows.iter().map(|r| r.5).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(pk1s)),
            Arc::new(Int32Array::from(pk2s)),
            Arc::new(StringArray::from(vals)),
            Arc::new(StringArray::from(ops)),
            Arc::new(Int64Array::from(tss)),
            Arc::new(Int64Array::from(seqs)),
        ],
    )
    .unwrap();

    let file = std::fs::File::create(&path).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    path.to_string_lossy().into_owned()
}

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
        Field::new("_pgiceberg_op", DataType::Utf8, false),
        Field::new("_pgiceberg_ts", DataType::Int64, false),
        Field::new("_pgiceberg_seq", DataType::Int64, false),
    ]))
}

fn pk_cols() -> Vec<String> {
    vec!["pk".to_string()]
}

/// Query a DuckDB table and return all rows as (col_name, values) for inspection.
fn query_table_i32_str(engine: &DuckDbEngine, table: &str, pk_col: &str) -> Vec<(i32, String)> {
    let sql = format!("SELECT {pk_col}, value FROM {table} ORDER BY {pk_col}");
    let mut stmt = engine.conn.prepare(&sql).unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap();
    rows.map(|r| r.unwrap()).collect()
}

fn query_delete_keys(engine: &DuckDbEngine, pk_col: &str) -> Vec<i32> {
    let sql = format!("SELECT {pk_col} FROM delete_keys ORDER BY {pk_col}");
    let mut stmt = engine.conn.prepare(&sql).unwrap();
    let rows = stmt.query_map([], |row| row.get::<_, i32>(0)).unwrap();
    rows.map(|r| r.unwrap()).collect()
}

fn query_upsert_count(engine: &DuckDbEngine) -> u64 {
    engine.table_count("to_upsert").unwrap()
}

fn _query_delete_count(engine: &DuckDbEngine) -> u64 {
    engine.table_count("delete_keys").unwrap()
}

// ── Test 1: Dedup different timestamps ──────────────────────────────────────

#[test]
fn test_dedup_different_timestamps() {
    let dir = TempDir::new().unwrap();
    let path = write_test_parquet(
        dir.path(),
        "test.parquet",
        &[(1, "old", "I", 100, 0), (1, "new", "U", 200, 1)],
    );

    let engine = DuckDbEngine::new().unwrap();
    engine.load_staged_files(&[path]).unwrap();
    engine.dedup_by_pk(&pk_cols()).unwrap();
    engine.separate_operations(&pk_cols()).unwrap();

    let upserts = query_table_i32_str(&engine, "to_upsert", "pk");
    assert_eq!(upserts.len(), 1);
    assert_eq!(upserts[0], (1, "new".to_string()));
}

// ── Test 2: Same timestamp uses seq tiebreaker ──────────────────────────────

#[test]
fn test_dedup_same_timestamp_uses_seq_tiebreaker() {
    let dir = TempDir::new().unwrap();
    // DELETE then INSERT within same transaction (same ts)
    let path = write_test_parquet(
        dir.path(),
        "test.parquet",
        &[(1, "deleted", "D", 100, 0), (1, "inserted", "I", 100, 1)],
    );

    let engine = DuckDbEngine::new().unwrap();
    engine.load_staged_files(&[path]).unwrap();
    engine.dedup_by_pk(&pk_cols()).unwrap();
    engine.separate_operations(&pk_cols()).unwrap();

    // INSERT (seq=1) should win over DELETE (seq=0)
    let upserts = query_table_i32_str(&engine, "to_upsert", "pk");
    assert_eq!(upserts.len(), 1);
    assert_eq!(upserts[0], (1, "inserted".to_string()));
}

// ── Test 3: DELETE→INSERT→DELETE→INSERT same PK ─────────────────────────────

#[test]
fn test_delete_insert_delete_insert_same_pk() {
    let dir = TempDir::new().unwrap();
    let path = write_test_parquet(
        dir.path(),
        "test.parquet",
        &[
            (1, "v0", "D", 100, 0),
            (1, "v1", "I", 100, 1),
            (1, "v1", "D", 100, 2),
            (1, "v2", "I", 100, 3),
        ],
    );

    let engine = DuckDbEngine::new().unwrap();
    engine.load_staged_files(&[path]).unwrap();
    engine.dedup_by_pk(&pk_cols()).unwrap();
    engine.separate_operations(&pk_cols()).unwrap();

    // Final INSERT (seq=3) should win
    let upserts = query_table_i32_str(&engine, "to_upsert", "pk");
    assert_eq!(upserts.len(), 1);
    assert_eq!(upserts[0], (1, "v2".to_string()));

    // Delete key emitted for the PK
    let deletes = query_delete_keys(&engine, "pk");
    assert_eq!(deletes, vec![1]);
}

// ── Test 4: INSERT-only deduped row still emits delete key ──────────────────

#[test]
fn test_separate_ops_insert_emits_delete_key() {
    let dir = TempDir::new().unwrap();
    let path = write_test_parquet(dir.path(), "test.parquet", &[(1, "hello", "I", 100, 0)]);

    let engine = DuckDbEngine::new().unwrap();
    engine.load_staged_files(&[path]).unwrap();
    engine.dedup_by_pk(&pk_cols()).unwrap();
    engine.separate_operations(&pk_cols()).unwrap();

    // Upsert should exist
    assert_eq!(query_upsert_count(&engine), 1);

    // Delete key should ALSO exist (Bug 2 fix: emit for all PKs)
    let deletes = query_delete_keys(&engine, "pk");
    assert_eq!(deletes, vec![1]);
}

// ── Test 5: DELETE-only → delete_keys only, no upserts ──────────────────────

#[test]
fn test_separate_ops_delete_only() {
    let dir = TempDir::new().unwrap();
    let path = write_test_parquet(dir.path(), "test.parquet", &[(1, "gone", "D", 100, 0)]);

    let engine = DuckDbEngine::new().unwrap();
    engine.load_staged_files(&[path]).unwrap();
    engine.dedup_by_pk(&pk_cols()).unwrap();
    engine.separate_operations(&pk_cols()).unwrap();

    assert_eq!(query_upsert_count(&engine), 0);
    let deletes = query_delete_keys(&engine, "pk");
    assert_eq!(deletes, vec![1]);
}

// ── Test 6: UPDATE emits both upsert and delete key ─────────────────────────

#[test]
fn test_separate_ops_update_emits_both() {
    let dir = TempDir::new().unwrap();
    let path = write_test_parquet(dir.path(), "test.parquet", &[(1, "updated", "U", 100, 0)]);

    let engine = DuckDbEngine::new().unwrap();
    engine.load_staged_files(&[path]).unwrap();
    engine.dedup_by_pk(&pk_cols()).unwrap();
    engine.separate_operations(&pk_cols()).unwrap();

    let upserts = query_table_i32_str(&engine, "to_upsert", "pk");
    assert_eq!(upserts.len(), 1);
    assert_eq!(upserts[0], (1, "updated".to_string()));

    let deletes = query_delete_keys(&engine, "pk");
    assert_eq!(deletes, vec![1]);
}

// ── Test 7: Composite PK dedup ──────────────────────────────────────────────

#[test]
fn test_dedup_composite_pk() {
    let dir = TempDir::new().unwrap();
    let path = write_composite_pk_parquet(
        dir.path(),
        "test.parquet",
        &[
            (1, 1, "old", "I", 100, 0),
            (1, 1, "new", "U", 200, 1),
            (1, 2, "other", "I", 100, 0),
        ],
    );

    let pk_cols = vec!["pk1".to_string(), "pk2".to_string()];
    let engine = DuckDbEngine::new().unwrap();
    engine.load_staged_files(&[path]).unwrap();
    engine.dedup_by_pk(&pk_cols).unwrap();
    engine.separate_operations(&pk_cols).unwrap();

    let upserts = {
        let mut stmt = engine
            .conn
            .prepare("SELECT pk1, pk2, value FROM to_upsert ORDER BY pk1, pk2")
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i32>(0)?,
                    row.get::<_, i32>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap();
        rows.map(|r| r.unwrap()).collect::<Vec<_>>()
    };

    assert_eq!(upserts.len(), 2);
    assert_eq!(upserts[0], (1, 1, "new".to_string()));
    assert_eq!(upserts[1], (1, 2, "other".to_string()));
}

// ── Test 8: Multiple PKs with mixed ops ─────────────────────────────────────

#[test]
fn test_multiple_pks_mixed_ops() {
    let dir = TempDir::new().unwrap();
    let path = write_test_parquet(
        dir.path(),
        "test.parquet",
        &[
            (1, "inserted", "I", 100, 0),
            (2, "deleted", "D", 100, 1),
            (3, "updated", "U", 100, 2),
        ],
    );

    let engine = DuckDbEngine::new().unwrap();
    engine.load_staged_files(&[path]).unwrap();
    engine.dedup_by_pk(&pk_cols()).unwrap();
    engine.separate_operations(&pk_cols()).unwrap();

    // Upserts: pk=1 (I) and pk=3 (U)
    let upserts = query_table_i32_str(&engine, "to_upsert", "pk");
    assert_eq!(upserts.len(), 2);
    assert_eq!(upserts[0], (1, "inserted".to_string()));
    assert_eq!(upserts[1], (3, "updated".to_string()));

    // Delete keys: all three PKs (Bug 2 fix)
    let deletes = query_delete_keys(&engine, "pk");
    assert_eq!(deletes, vec![1, 2, 3]);
}

// ── Test 9: Export roundtrip ────────────────────────────────────────────────

#[test]
fn test_export_roundtrip() {
    let dir = TempDir::new().unwrap();
    let input = write_test_parquet(
        dir.path(),
        "input.parquet",
        &[(1, "a", "I", 100, 0), (2, "b", "U", 200, 1)],
    );

    let engine = DuckDbEngine::new().unwrap();
    engine.load_staged_files(&[input]).unwrap();
    engine.dedup_by_pk(&pk_cols()).unwrap();
    engine.separate_operations(&pk_cols()).unwrap();

    let upsert_path = dir.path().join("upserts.parquet");
    let delete_path = dir.path().join("deletes.parquet");

    let upsert_count = engine.export_upserts(&upsert_path).unwrap();
    let delete_count = engine.export_deletes(&delete_path).unwrap();

    assert_eq!(upsert_count, 2);
    assert_eq!(delete_count, 2);
    assert!(upsert_path.exists());
    assert!(delete_path.exists());

    // Read back and verify
    let engine2 = DuckDbEngine::new().unwrap();
    let sql = format!(
        "SELECT pk, value FROM read_parquet('{}') ORDER BY pk",
        upsert_path.display()
    );
    let mut stmt = engine2.conn.prepare(&sql).unwrap();
    let rows: Vec<(i32, String)> = stmt
        .query_map([], |row| {
            Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "a".to_string()));
    assert_eq!(rows[1], (2, "b".to_string()));
}

// ── Test 10: Empty input ────────────────────────────────────────────────────

#[test]
fn test_empty_input() {
    let dir = TempDir::new().unwrap();
    let path = write_test_parquet(dir.path(), "empty.parquet", &[]);

    let engine = DuckDbEngine::new().unwrap();
    engine.load_staged_files(&[path]).unwrap();
    engine.dedup_by_pk(&pk_cols()).unwrap();
    engine.separate_operations(&pk_cols()).unwrap();

    let upsert_path = dir.path().join("upserts.parquet");
    let count = engine.export_upserts(&upsert_path).unwrap();
    assert_eq!(count, 0);
    assert!(!upsert_path.exists()); // no file written for 0 rows
}

// ── Test 11: Resync diff with matching schemas ──────────────────────────────

#[test]
fn test_resync_diff_matching_schemas() {
    let dir = TempDir::new().unwrap();

    // Helper: write backfill-style Parquet (no CDC metadata columns)
    let write_backfill = |name: &str, rows: &[(i32, &str)]| -> String {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let path = dir.path().join(name);
        let pks: Vec<i32> = rows.iter().map(|r| r.0).collect();
        let vals: Vec<&str> = rows.iter().map(|r| r.1).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(pks)),
                Arc::new(StringArray::from(vals)),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path.to_string_lossy().into_owned()
    };

    let old_path = write_backfill("old.parquet", &[(1, "a"), (2, "b"), (3, "c")]);
    let new_path = write_backfill("new.parquet", &[(1, "a"), (2, "changed"), (4, "new")]);

    let engine = DuckDbEngine::new().unwrap();
    engine
        .load_resync_data_with_deletes(&[old_path], &[], &[new_path], &pk_cols())
        .unwrap();
    engine.compute_resync_diff(&pk_cols()).unwrap();

    // pk=2 changed, pk=4 is new → 2 upserts
    assert_eq!(engine.table_count("to_upsert").unwrap(), 2);
    // pk=3 was deleted
    let deletes = query_delete_keys(&engine, "pk");
    assert_eq!(deletes, vec![3]);
}

// ── Test 12: Resync diff with schema mismatch (PK-based fallback) ───────────

#[test]
fn test_resync_diff_schema_mismatch() {
    let dir = TempDir::new().unwrap();

    // Old data: (pk, value)
    let old_schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let old_path = dir.path().join("old.parquet");
    let batch = RecordBatch::try_new(
        old_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(&old_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, old_schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    // New data: (pk, value, extra_col) — column added
    let new_schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
        Field::new("extra_col", DataType::Utf8, true),
    ]));
    let new_path = dir.path().join("new.parquet");
    let batch = RecordBatch::try_new(
        new_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 4])),
            Arc::new(StringArray::from(vec!["a", "changed", "new"])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(&new_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, new_schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let engine = DuckDbEngine::new().unwrap();
    engine
        .load_resync_data_with_deletes(
            &[old_path.to_string_lossy().into_owned()],
            &[],
            &[new_path.to_string_lossy().into_owned()],
            &pk_cols(),
        )
        .unwrap();

    // This should NOT error — falls back to PK-based diff
    engine.compute_resync_diff(&pk_cols()).unwrap();

    // All 3 rows from new_data are upserts (schema changed)
    assert_eq!(engine.table_count("to_upsert").unwrap(), 3);
    // pk=3 was deleted
    let deletes = query_delete_keys(&engine, "pk");
    assert_eq!(deletes, vec![3]);
}

// ── Test 13: Cleanup drops tables ───────────────────────────────────────────

#[test]
fn test_cleanup_drops_tables() {
    let dir = TempDir::new().unwrap();
    let path = write_test_parquet(dir.path(), "test.parquet", &[(1, "x", "I", 100, 0)]);

    let engine = DuckDbEngine::new().unwrap();
    engine.load_staged_files(&[path]).unwrap();
    engine.dedup_by_pk(&pk_cols()).unwrap();
    engine.separate_operations(&pk_cols()).unwrap();

    // Tables should exist
    assert!(engine.table_count("staged").is_ok());
    assert!(engine.table_count("deduped").is_ok());

    // Cleanup should not error
    engine.cleanup().unwrap();

    // Tables should be gone
    assert!(engine.table_count("staged").is_err());
}
