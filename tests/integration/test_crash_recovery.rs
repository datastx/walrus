mod common;

/// Placeholder for crash recovery integration tests.
/// These simulate partial operations and verify recovery on restart.
#[tokio::test]
#[ignore = "requires Postgres with logical replication"]
async fn test_service2_reclaims_stale_processing_files() {
    if std::env::var("TEST_PG_HOST").is_err() {
        return;
    }

    let config = common::test_config();
    let metadata = common::connect_metadata(&config).await.unwrap();
    metadata.bootstrap().await.unwrap();

    // Simulate: claim a file then "crash" (just leave it in processing)
    // On next startup, reclaim_stale_processing should reset it
    let reclaimed = metadata.reclaim_stale_processing().await.unwrap();
    // No files to reclaim in a fresh DB
    assert_eq!(reclaimed, 0);

    common::cleanup(&config).await;
}
