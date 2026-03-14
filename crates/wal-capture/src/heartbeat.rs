use pgiceberg_common::metadata::MetadataStore;
use std::time::Duration;
use tokio::sync::watch;

/// Periodically calls `pg_logical_emit_message()` to generate lightweight WAL
/// entries that flow through the replication slot. This allows the CDC consumer
/// to advance its confirmed LSN even when no user writes are happening,
/// preventing unbounded WAL growth on the source database.
///
/// Returns immediately if `interval_seconds` is 0 (disabled).
pub async fn run_heartbeat_loop(
    metadata: MetadataStore,
    interval_seconds: u64,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    if interval_seconds == 0 {
        tracing::info!("Heartbeat disabled (interval = 0)");
        return;
    }

    tracing::info!(
        interval_seconds,
        "Starting heartbeat loop (pg_logical_emit_message)"
    );

    let mut interval = tokio::time::interval(Duration::from_secs(interval_seconds));
    interval.tick().await; // first tick fires immediately — skip it

    loop {
        tokio::select! {
            _ = interval.tick() => {
                match metadata.tick_heartbeat().await {
                    Ok(()) => {
                        tracing::trace!("Heartbeat tick sent");
                        metrics::counter!("walrus_heartbeat_ticks_total").increment(1);
                    }
                    Err(e) => {
                        tracing::warn!("Heartbeat tick failed: {}", e);
                        metrics::counter!("walrus_heartbeat_errors_total").increment(1);
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                tracing::info!("Heartbeat loop shutting down");
                return;
            }
        }
    }
}
