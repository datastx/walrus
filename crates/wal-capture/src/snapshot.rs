use tokio::sync::watch;
use tokio_postgres::NoTls;
use tracing::{info, warn};

/// Holds a Postgres transaction with `SET TRANSACTION SNAPSHOT` alive for the
/// duration of all backfills.
///
/// The snapshot was created by `pg_create_logical_replication_slot`.  As long as
/// at least one transaction keeps the snapshot pinned, backfill workers can open
/// new connections and `SET TRANSACTION SNAPSHOT` to read a consistent view.
///
/// When all backfills are done, drop the holder to release the connection.
pub struct SnapshotHolder {
    _cancel_tx: watch::Sender<bool>,
}

impl SnapshotHolder {
    /// Spawns a background task that holds the snapshot alive.
    /// Returns a handle; dropping it signals the holder to close.
    pub fn start(conn_string: String, snapshot_name: String) -> Self {
        let (cancel_tx, mut cancel_rx) = watch::channel(false);

        tokio::spawn(async move {
            match hold_snapshot(&conn_string, &snapshot_name, &mut cancel_rx).await {
                Ok(()) => info!("Snapshot holder released cleanly"),
                Err(e) => warn!("Snapshot holder exited with error: {}", e),
            }
        });

        SnapshotHolder {
            _cancel_tx: cancel_tx,
        }
    }
}

async fn hold_snapshot(
    conn_string: &str,
    snapshot_name: &str,
    cancel_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (client, conn) = tokio_postgres::connect(conn_string, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("Snapshot holder connection error: {}", e);
        }
    });

    client
        .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await?;
    client
        .batch_execute(&format!("SET TRANSACTION SNAPSHOT '{}'", snapshot_name))
        .await?;

    info!(snapshot = snapshot_name, "Snapshot holder active");

    // Hold the transaction open until signalled to stop
    let _ = cancel_rx.changed().await;

    client.batch_execute("COMMIT").await?;
    Ok(())
}
