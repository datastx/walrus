#[allow(dead_code)]
mod common;

// Integration tests below require a running Postgres with wal_level=logical.
// They are gated by the TEST_PG_HOST env var.

#[tokio::test]
async fn test_metadata_bootstrap() {
    if std::env::var("TEST_PG_HOST").is_err() {
        eprintln!("Skipping integration test — TEST_PG_HOST not set");
        return;
    }

    let config = common::test_config();
    let metadata = common::connect_metadata(&config).await.unwrap();

    // Bootstrap should be idempotent
    metadata.bootstrap().await.unwrap();
    metadata.bootstrap().await.unwrap();

    // Table should exist now
    let state = metadata.get_replication_state("nonexistent").await.unwrap();
    assert!(state.is_none());

    common::cleanup(&config).await;
}
