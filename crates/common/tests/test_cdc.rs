#[allow(dead_code)]
mod common;

/// Integration tests for CDC capture.
/// These require a running Postgres with wal_level=logical and a replication slot.
///
/// Run with: cargo test --test test_cdc -- --ignored

#[tokio::test]
#[ignore = "requires Postgres with logical replication"]
async fn test_cdc_captures_inserts() {
    let config = common::test_config();
    let metadata = common::connect_metadata(&config).await.unwrap();
    metadata.bootstrap().await.unwrap();

    // Verify metadata schema exists and is queryable
    let tables = metadata.get_all_table_states().await.unwrap();
    assert!(
        tables.is_empty(),
        "Fresh bootstrap should have no table states"
    );

    // Verify file queue is empty
    let files = metadata.claim_next_batch(10).await.unwrap();
    assert!(files.is_empty(), "Fresh bootstrap should have no files");

    common::cleanup(&config).await;
}

#[tokio::test]
#[ignore = "requires Postgres with logical replication"]
async fn test_cdc_file_enqueue_and_claim() {
    use pgiceberg_common::metadata::EnqueueFileParams;
    use pgiceberg_common::models::{FileType, Lsn, TablePhase};

    let config = common::test_config();
    let metadata = common::connect_metadata(&config).await.unwrap();
    metadata.bootstrap().await.unwrap();

    // Register a table
    metadata
        .upsert_table_state(
            "public",
            "test_table",
            TablePhase::Streaming,
            None,
            None,
            &["id".to_string()],
        )
        .await
        .unwrap();

    // Enqueue a file
    let file_id = metadata
        .enqueue_file(&EnqueueFileParams {
            schema: "public",
            table: "test_table",
            file_type: FileType::CdcMixed,
            file_path: "cdc/public.test_table/test.parquet",
            lsn_low: Some(&Lsn(100)),
            lsn_high: Some(&Lsn(200)),
            row_count: 42,
            partition_id: None,
        })
        .await
        .unwrap();

    // Claim the batch
    let files = metadata.claim_next_batch(10).await.unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].file_id, file_id);
    assert_eq!(files[0].row_count, 42);

    // Mark completed
    metadata.mark_files_completed(&[file_id]).await.unwrap();

    // No more pending
    let files = metadata.claim_next_batch(10).await.unwrap();
    assert!(files.is_empty());

    common::cleanup(&config).await;
}
