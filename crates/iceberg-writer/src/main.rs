mod backfill_processor;
mod catalog;
mod cdc_processor;
mod compaction;
pub mod ddl_handler;
mod duckdb_engine;
mod processor;

use clap::Parser;
use pgiceberg_common::config::AppConfig;
use pgiceberg_common::health::{serve_health, HealthState};
use pgiceberg_common::metadata::MetadataStore;
use std::path::PathBuf;
use tokio::sync::watch;
use tracing::info;

#[derive(Parser)]
#[command(
    name = "pgiceberg-iceberg-writer",
    about = "Iceberg Writer service for pg-iceberg-cdc"
)]
struct Cli {
    #[arg(
        short,
        long,
        default_value = "/etc/pgiceberg/pgiceberg.toml",
        env = "PGICEBERG_CONFIG"
    )]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    let cli = Cli::parse();
    let config = AppConfig::load(&cli.config)?;

    info!(
        warehouse = %config.iceberg_writer.warehouse_path.display(),
        tables = config.source.tables.len(),
        "Starting Iceberg Writer service"
    );

    // Graceful shutdown
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("SIGTERM/SIGINT received — shutting down");
        let _ = shutdown_tx.send(true);
    });

    // Health server
    let health = HealthState::default();
    let health_clone = health.clone();
    let health_port = config.iceberg_writer.health_port;
    tokio::spawn(async move {
        if let Err(e) = serve_health(health_port, health_clone).await {
            tracing::error!("Health server error: {}", e);
        }
    });

    // Connect to metadata store
    let metadata = MetadataStore::connect(&config.source.connection_string()).await?;

    // Recovery: reclaim stale processing files
    let reclaimed = metadata.reclaim_stale_processing().await?;
    if reclaimed > 0 {
        info!(reclaimed, "Reclaimed stale processing files");
    }

    // Initialize Iceberg catalog
    let iceberg_catalog = catalog::init_catalog(&config.iceberg_writer).await?;

    // Ensure Iceberg tables exist for all configured source tables
    for (schema, table) in config.source.table_list() {
        let pk = config.source.pk_for_table(&schema, &table);
        let pk_from_db = if pk.is_empty() {
            metadata
                .discover_primary_keys(&schema, &table)
                .await
                .unwrap_or_default()
        } else {
            pk
        };

        match catalog::ensure_iceberg_table(
            &iceberg_catalog,
            &metadata,
            &schema,
            &table,
            &pk_from_db,
        )
        .await
        {
            Ok(_) => info!(schema = %schema, table = %table, "Iceberg table ready"),
            Err(e) => tracing::warn!(
                schema = %schema,
                table = %table,
                error = %e,
                "Failed to ensure Iceberg table — will retry on first file"
            ),
        }
    }

    health.set_ready(true);
    health.set_alive(true);

    // Spawn compaction on a timer
    let compaction_interval = config.iceberg_writer.compaction_interval_hours;
    let compaction_threshold = config.iceberg_writer.compaction_delete_threshold;
    let compaction_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(
            compaction_interval as u64 * 3600,
        ));
        loop {
            interval.tick().await;
            if *compaction_shutdown.borrow() {
                break;
            }
            if let Err(e) =
                compaction::maybe_run_compaction(compaction_interval, compaction_threshold).await
            {
                tracing::warn!("Compaction error: {}", e);
            }
        }
    });

    // Run main processing loop
    processor::run_processing_loop(
        &config.source,
        &config.iceberg_writer,
        &metadata,
        &iceberg_catalog,
        &config.staging.root,
        shutdown_rx,
    )
    .await?;

    // Cleanup old completed files before exit
    let cleaned = metadata
        .cleanup_completed(config.staging.cleanup_after_hours)
        .await?;
    for path in &cleaned {
        let full = config.staging.root.join(path);
        if full.exists() {
            std::fs::remove_file(&full).ok();
        }
    }
    if !cleaned.is_empty() {
        info!(count = cleaned.len(), "Cleaned up old staged files");
    }

    info!("Iceberg Writer service stopped cleanly");
    Ok(())
}
