#[allow(dead_code)]
mod common;

/// Crash recovery integration tests.
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

    // No files to reclaim in a fresh DB
    let reclaimed = metadata.reclaim_stale_processing().await.unwrap();
    assert_eq!(reclaimed, 0);

    common::cleanup(&config).await;
}

#[tokio::test]
#[ignore = "requires Postgres with logical replication"]
async fn test_backfill_partition_tracking() {
    use pgiceberg_common::models::TablePhase;

    if std::env::var("TEST_PG_HOST").is_err() {
        return;
    }

    let config = common::test_config();
    let metadata = common::connect_metadata(&config).await.unwrap();
    metadata.bootstrap().await.unwrap();

    // Register a table in backfilling state
    metadata
        .upsert_table_state(
            "public",
            "test_recovery",
            TablePhase::Backfilling,
            Some(5),
            Some("fake_snapshot"),
            &["id".to_string()],
        )
        .await
        .unwrap();

    // Advance partitions
    let done = metadata
        .advance_backfill_partition("public", "test_recovery")
        .await
        .unwrap();
    assert_eq!(done, 1);

    let done = metadata
        .advance_backfill_partition("public", "test_recovery")
        .await
        .unwrap();
    assert_eq!(done, 2);

    // Verify state
    let state = metadata
        .get_table_state("public", "test_recovery")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.backfill_done_partitions, 2);
    assert_eq!(state.backfill_total_partitions, Some(5));

    // Transition to streaming
    metadata
        .set_table_phase("public", "test_recovery", TablePhase::Streaming)
        .await
        .unwrap();

    let state = metadata
        .get_table_state("public", "test_recovery")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.phase, TablePhase::Streaming);

    common::cleanup(&config).await;
}

#[tokio::test]
#[ignore = "requires Postgres with logical replication"]
async fn test_reset_tables_for_rebootstrap() {
    use pgiceberg_common::models::TablePhase;

    if std::env::var("TEST_PG_HOST").is_err() {
        return;
    }

    let config = common::test_config();
    let metadata = common::connect_metadata(&config).await.unwrap();
    metadata.bootstrap().await.unwrap();

    // Register tables in different phases
    metadata
        .upsert_table_state(
            "public",
            "table_backfilling",
            TablePhase::Backfilling,
            Some(10),
            Some("snap_123"),
            &["id".to_string()],
        )
        .await
        .unwrap();

    metadata
        .upsert_table_state(
            "public",
            "table_streaming",
            TablePhase::Streaming,
            None,
            None,
            &["id".to_string()],
        )
        .await
        .unwrap();

    // Reset for rebootstrap
    metadata.reset_tables_for_rebootstrap().await.unwrap();

    // Backfilling table should be reset to pending
    let state = metadata
        .get_table_state("public", "table_backfilling")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.phase, TablePhase::Pending);
    assert_eq!(state.backfill_done_partitions, 0);

    // Streaming table should be unchanged
    let state = metadata
        .get_table_state("public", "table_streaming")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.phase, TablePhase::Streaming);

    common::cleanup(&config).await;
}
