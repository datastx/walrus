pub mod backfill;
mod bootstrap;
mod cdc;
pub mod decoder;
pub mod parquet_writer;
mod snapshot;

use clap::Parser;
use pgiceberg_common::config::AppConfig;
use pgiceberg_common::health::{serve_health, HealthState};
use pgiceberg_common::metadata::MetadataStore;
use pgiceberg_common::models::{Lsn, TablePhase};
use std::path::PathBuf;
use tokio::sync::watch;
use tracing::info;

#[derive(Parser)]
#[command(
    name = "pgiceberg-wal-capture",
    about = "WAL Capture service for pg-iceberg-cdc"
)]
struct Cli {
    /// Path to config file
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
        host = %config.source.host,
        database = %config.source.database,
        slot = %config.source.slot_name,
        tables = config.source.tables.len(),
        "Starting WAL Capture service"
    );

    // Setup graceful shutdown
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("SIGTERM/SIGINT received — shutting down");
        let _ = shutdown_tx.send(true);
    });

    // Health server
    let health = HealthState::default();
    let health_clone = health.clone();
    tokio::spawn(async move {
        if let Err(e) = serve_health(config.wal_capture.health_port, health_clone).await {
            tracing::error!("Health server error: {}", e);
        }
    });

    // Connect to metadata store
    let metadata = MetadataStore::connect(&config.source.connection_string()).await?;

    // Bootstrap: creates schema, slot, publication, table states (idempotent)
    let slot_info = bootstrap::bootstrap(&config.source, &metadata).await?;

    // Determine the snapshot name (either from fresh bootstrap or from persisted state)
    let repl_state = metadata
        .get_replication_state(&config.source.slot_name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No replication state found after bootstrap"))?;

    let start_lsn = repl_state
        .last_flushed_lsn
        .or(repl_state.consistent_point)
        .unwrap_or(Lsn::ZERO);

    health.set_ready(true);
    health.set_alive(true);

    // ── CONCURRENT ARCHITECTURE ──
    //
    // Three tasks run simultaneously:
    //   1. Snapshot Holder — keeps the export snapshot alive for backfill workers
    //   2. Backfill Manager — CTID scans tables that need backfilling
    //   3. WAL Consumer — reads WAL immediately, stages ALL tables' changes
    //
    // The WAL consumer does NOT wait for backfill to finish.  It stages CDC files
    // for tables still backfilling.  Service 2 is responsible for not processing
    // CDC files until backfill is complete for each table.

    let tables = metadata.get_all_table_states().await?;
    let has_backfill = tables
        .iter()
        .any(|t| matches!(t.phase, TablePhase::Backfilling));

    let snapshot_holder = if has_backfill {
        let snapshot_name = slot_info
            .as_ref()
            .map(|s| s.snapshot_name.clone())
            .or(repl_state.snapshot_name.clone());

        match snapshot_name {
            Some(snap) => {
                info!(snapshot = %snap, "Starting snapshot holder for backfill");
                Some(snapshot::SnapshotHolder::start(
                    config.source.connection_string(),
                    snap,
                ))
            }
            None => {
                tracing::warn!("No snapshot available for backfill — tables may need re-bootstrap");
                None
            }
        }
    } else {
        None
    };

    // Spawn backfill task
    let backfill_handle = if has_backfill {
        let src = config.source.clone();
        let wal_cfg = config.wal_capture.clone();
        let meta = metadata.clone();
        let root = config.staging.root.clone();
        Some(tokio::spawn(async move {
            let result = backfill::run_backfill(&src, &wal_cfg, &meta, &root).await;
            if let Err(ref e) = result {
                tracing::error!("Backfill failed: {}", e);
            }
            result
        }))
    } else {
        info!("All tables already streaming — skipping backfill");
        None
    };

    // Spawn CDC loop (starts immediately, doesn't wait for backfill)
    let cdc_handle = {
        let src = config.source.clone();
        let wal_cfg = config.wal_capture.clone();
        let meta = metadata.clone();
        let root = config.staging.root.clone();
        let shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            cdc::run_cdc_loop(
                &src.host,
                src.port,
                &src.user,
                &src.password(),
                &src.database,
                &src.slot_name,
                &src.publication_name,
                start_lsn,
                &wal_cfg,
                &meta,
                &root,
                shutdown,
            )
            .await
        })
    };

    // Wait for backfill to complete, then release snapshot
    if let Some(handle) = backfill_handle {
        handle.await??;
        info!("Backfill complete — releasing snapshot holder");
    }
    drop(snapshot_holder);

    // Wait for CDC loop (runs until shutdown signal)
    cdc_handle.await??;

    info!("WAL Capture service stopped cleanly");
    Ok(())
}
