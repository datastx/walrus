use pgiceberg_common::config::{
    AppConfig, IcebergWriterConfig, SourceConfig, StagingConfig, WalCaptureConfig,
};
use pgiceberg_common::metadata::MetadataStore;
use std::path::PathBuf;

/// Test helper: build a config pointing at the CI Postgres.
pub fn test_config() -> AppConfig {
    let host = std::env::var("TEST_PG_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port: u16 = std::env::var("TEST_PG_PORT")
        .unwrap_or_else(|_| "5433".to_string())
        .parse()
        .unwrap();
    let database = std::env::var("TEST_PG_DATABASE").unwrap_or_else(|_| "testdb".to_string());
    let user = std::env::var("TEST_PG_USER").unwrap_or_else(|_| "replicator".to_string());
    let password = std::env::var("TEST_PG_PASSWORD").unwrap_or_else(|_| "testpass".to_string());

    std::env::set_var("PG_PASSWORD", &password);

    AppConfig {
        source: SourceConfig {
            host,
            port,
            database,
            user,
            password_env: "PG_PASSWORD".to_string(),
            slot_name: format!(
                "test_slot_{}",
                &uuid::Uuid::new_v4().to_string().replace('-', "")[..8]
            ),
            publication_name: format!(
                "test_pub_{}",
                &uuid::Uuid::new_v4().to_string().replace('-', "")[..8]
            ),
            tables: std::collections::HashMap::new(),
        },
        staging: StagingConfig {
            root: PathBuf::from("/tmp/pgiceberg-test-staging"),
            cleanup_after_hours: 1,
        },
        wal_capture: WalCaptureConfig {
            rows_per_partition: 1000,
            max_batch_rows: 100,
            ..Default::default()
        },
        iceberg_writer: IcebergWriterConfig {
            warehouse_path: PathBuf::from("/tmp/pgiceberg-test-warehouse"),
            catalog_db_path: PathBuf::from("/tmp/pgiceberg-test-warehouse/catalog.db"),
            ..Default::default()
        },
    }
}

pub async fn connect_metadata(config: &AppConfig) -> anyhow::Result<MetadataStore> {
    MetadataStore::connect(&config.source.connection_string()).await
}

/// Clean up test artifacts.
pub async fn cleanup(config: &AppConfig) {
    let conn_str = config.source.connection_string();
    if let Ok(_meta) = MetadataStore::connect(&conn_str).await {
        let client = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .ok();
        if let Some((client, conn)) = client {
            tokio::spawn(async move {
                conn.await.ok();
            });

            // Drop slot if it exists
            client
                .execute(
                    &format!(
                        "SELECT pg_drop_replication_slot('{}') FROM pg_replication_slots WHERE slot_name = '{}'",
                        config.source.slot_name, config.source.slot_name
                    ),
                    &[],
                )
                .await
                .ok();

            // Drop publication
            client
                .execute(
                    &format!(
                        "DROP PUBLICATION IF EXISTS {}",
                        config.source.publication_name
                    ),
                    &[],
                )
                .await
                .ok();
        }
    }

    // Remove staging/warehouse dirs
    std::fs::remove_dir_all(&config.staging.root).ok();
    std::fs::remove_dir_all(&config.iceberg_writer.warehouse_path).ok();
}
