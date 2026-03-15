pub(crate) mod backfill;
mod bootstrap;
mod cdc;
pub(crate) mod copy_export;
pub(crate) mod decoder;
mod heartbeat;
pub(crate) mod parquet_writer;
mod snapshot;
pub(crate) mod spill;

use clap::Parser;
use pgiceberg_common::config::AppConfig;
use pgiceberg_common::health::{install_metrics_recorder, serve_health, HealthState};
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

    let metrics_handle = install_metrics_recorder();

    info!(
        host = %config.source.host,
        database = %config.source.database,
        slot = %config.source.slot_name,
        tables = config.source.tables.len(),
        tls_mode = ?config.source.tls_mode,
        "Starting WAL Capture service"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("SIGTERM/SIGINT received — shutting down");
        let _ = shutdown_tx.send(true);
    });

    let health = HealthState {
        metrics_handle: Some(metrics_handle),
        ..Default::default()
    };
    let health_clone = health.clone();
    tokio::spawn(async move {
        if let Err(e) = serve_health(config.wal_capture.health_port, health_clone).await {
            tracing::error!("Health server error: {}", e);
        }
    });

    let metadata = MetadataStore::connect_with_tls(
        &config.source.connection_string(),
        &config.source.tls_mode,
        config.source.tls_ca_cert.as_deref(),
    )
    .await?;
    let slot_info = bootstrap::bootstrap(&config.source, &config.wal_capture, &metadata).await?;

    let repl_state = metadata
        .get_replication_state(&config.source.slot_name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No replication state found after bootstrap"))?;

    let start_lsn = repl_state
        .last_flushed_lsn
        .or(repl_state.consistent_point)
        .unwrap_or(Lsn::ZERO);

    if let Err(e) = cleanup_orphan_files(&metadata, &config.staging.root).await {
        tracing::warn!("Orphan file cleanup error: {}", e);
    }

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
                    config.source.tls_mode.clone(),
                    config.source.tls_ca_cert.clone(),
                ))
            }
            None => {
                tracing::warn!(
                    "No snapshot available for backfill — dropping stale slot and re-bootstrapping"
                );

                bootstrap::drop_replication_slot(&config.source).await?;
                metadata.reset_tables_for_rebootstrap().await?;
                metadata
                    .delete_replication_state(&config.source.slot_name)
                    .await?;

                let fresh_slot =
                    bootstrap::bootstrap(&config.source, &config.wal_capture, &metadata).await?;
                if let Some(ref slot) = fresh_slot {
                    info!(snapshot = %slot.snapshot_name, "Re-bootstrapped — starting snapshot holder");
                    Some(snapshot::SnapshotHolder::start(
                        config.source.connection_string(),
                        slot.snapshot_name.clone(),
                        config.source.tls_mode.clone(),
                        config.source.tls_ca_cert.clone(),
                    ))
                } else {
                    tracing::error!("Re-bootstrap did not produce a snapshot — cannot backfill");
                    None
                }
            }
        }
    } else {
        None
    };

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

    let heartbeat_handle = {
        let meta = metadata.clone();
        let interval = config.wal_capture.heartbeat_interval_seconds;
        let shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            heartbeat::run_heartbeat_loop(meta, interval, shutdown).await;
        })
    };

    let cdc_handle = {
        let src = config.source.clone();
        let wal_cfg = config.wal_capture.clone();
        let meta = metadata.clone();
        let root = config.staging.root.clone();
        let shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            let params = cdc::CdcLoopParams {
                source: &src,
                start_lsn,
                config: &wal_cfg,
                metadata: &meta,
                staging_root: &root,
            };
            cdc::run_cdc_loop(&params, shutdown).await
        })
    };

    if let Some(handle) = backfill_handle {
        handle.await??;
        info!("Backfill complete — releasing snapshot holder");
    }
    drop(snapshot_holder);

    cdc_handle.await??;
    heartbeat_handle.abort();

    info!("WAL Capture service stopped cleanly");
    Ok(())
}

/// Scan staging directories and delete Parquet files that are not tracked in the
/// file_queue (orphans from crashes between write and enqueue). Only deletes files
/// older than 1 hour to avoid racing with in-flight writes.
async fn cleanup_orphan_files(
    metadata: &MetadataStore,
    staging_root: &std::path::Path,
) -> anyhow::Result<()> {
    let tracked = metadata.get_all_tracked_file_paths().await?;
    let tracked_set: std::collections::HashSet<String> = tracked.into_iter().collect();

    let one_hour_ago = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
    let mut orphans_removed = 0u64;

    for subdir in &["backfill", "cdc"] {
        let dir = staging_root.join(subdir);
        if !dir.exists() {
            continue;
        }
        scan_and_remove_orphans(
            &dir,
            staging_root,
            &tracked_set,
            one_hour_ago,
            &mut orphans_removed,
        )?;
    }

    if orphans_removed > 0 {
        info!(count = orphans_removed, "Cleaned up orphan staging files");
    }
    Ok(())
}

fn is_removable_orphan(
    path: &std::path::Path,
    staging_root: &std::path::Path,
    tracked: &std::collections::HashSet<String>,
    older_than: std::time::SystemTime,
) -> bool {
    if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
        return false;
    }
    let relative = match path.strip_prefix(staging_root) {
        Ok(r) => r,
        Err(_) => return false,
    };
    let rel_str = relative.to_string_lossy().to_string();
    if tracked.contains(&rel_str) {
        return false;
    }
    let modified = std::fs::metadata(path).ok().and_then(|m| m.modified().ok());
    matches!(modified, Some(t) if t < older_than)
}

fn scan_and_remove_orphans(
    dir: &std::path::Path,
    staging_root: &std::path::Path,
    tracked: &std::collections::HashSet<String>,
    older_than: std::time::SystemTime,
    count: &mut u64,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            scan_and_remove_orphans(&path, staging_root, tracked, older_than, count)?;
        } else if is_removable_orphan(&path, staging_root, tracked, older_than) {
            std::fs::remove_file(&path).ok();
            *count += 1;
        }
    }
    Ok(())
}
