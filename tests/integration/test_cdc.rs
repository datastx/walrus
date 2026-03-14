mod common;

/// Placeholder integration tests for CDC.
/// These require a running Postgres with wal_level=logical and a replication slot.
///
/// Run with: cargo test --test test_cdc -- --ignored
#[tokio::test]
#[ignore = "requires Postgres with logical replication"]
async fn test_cdc_captures_inserts() {
    let config = common::test_config();
    // TODO: full CDC integration test
    // 1. Bootstrap slot + publication
    // 2. Insert rows into source
    // 3. Run CDC loop briefly
    // 4. Verify file_queue has entries
    // 5. Verify Parquet files contain correct data
    common::cleanup(&config).await;
}
