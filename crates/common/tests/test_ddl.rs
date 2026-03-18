#[allow(dead_code)]
mod common;

/// DDL integration tests.
/// DDL parsing is tested via unit tests in the ddl_handler module.
/// These tests verify DDL event storage and retrieval in the metadata store.

#[tokio::test]
#[ignore = "requires Postgres with logical replication"]
async fn test_ddl_event_lifecycle() {
    let config = common::test_config();
    let metadata = common::connect_metadata(&config).await.unwrap();
    metadata.bootstrap().await.unwrap();

    // Insert a DDL event
    metadata
        .insert_ddl_event(
            Some("12345"),
            "ALTER TABLE",
            "public",
            "users",
            "ALTER TABLE users ADD COLUMN age INT",
            Some("0/1A00"),
        )
        .await
        .unwrap();

    // Retrieve pending events
    let events = metadata.get_pending_ddl_events().await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].ddl_tag, "ALTER TABLE");
    assert_eq!(events[0].target_table, "users");
    assert_eq!(
        events[0].status,
        pgiceberg_common::models::DdlEventStatus::Pending
    );

    // Mark as applied
    metadata.mark_ddl_applied(events[0].event_id).await.unwrap();

    // No more pending
    let events = metadata.get_pending_ddl_events().await.unwrap();
    assert!(events.is_empty());

    common::cleanup(&config).await;
}

#[tokio::test]
#[ignore = "requires Postgres with logical replication"]
async fn test_multiple_ddl_events_ordered() {
    let config = common::test_config();
    let metadata = common::connect_metadata(&config).await.unwrap();
    metadata.bootstrap().await.unwrap();

    // Insert multiple events
    metadata
        .insert_ddl_event(
            None,
            "ALTER TABLE",
            "public",
            "t1",
            "ALTER TABLE t1 ADD COLUMN a INT",
            Some("0/1000"),
        )
        .await
        .unwrap();
    metadata
        .insert_ddl_event(None, "DROP TABLE", "public", "t2", "DROP TABLE t2", Some("0/1100"))
        .await
        .unwrap();
    metadata
        .insert_ddl_event(
            None,
            "ALTER TABLE",
            "public",
            "t1",
            "ALTER TABLE t1 DROP COLUMN b",
            Some("0/1200"),
        )
        .await
        .unwrap();

    let events = metadata.get_pending_ddl_events().await.unwrap();
    assert_eq!(events.len(), 3);
    // Events should be ordered by event_id
    assert!(events[0].event_id < events[1].event_id);
    assert!(events[1].event_id < events[2].event_id);

    common::cleanup(&config).await;
}
