use pgiceberg_common::config::TlsMode;
use pgiceberg_common::sql::validate_snapshot_name;
use pgiceberg_common::tls;
use std::path::PathBuf;
use tokio::sync::watch;
use tracing::{info, warn};

/// Holds a Postgres transaction with `SET TRANSACTION SNAPSHOT` alive for the
/// duration of all backfills.
pub struct SnapshotHolder {
    _cancel_tx: watch::Sender<bool>,
}

impl SnapshotHolder {
    pub fn start(
        conn_string: String,
        snapshot_name: String,
        tls_mode: TlsMode,
        tls_ca_cert: Option<PathBuf>,
    ) -> Self {
        let (cancel_tx, mut cancel_rx) = watch::channel(false);

        tokio::spawn(async move {
            match hold_snapshot(
                &conn_string,
                &snapshot_name,
                &tls_mode,
                tls_ca_cert.as_deref(),
                &mut cancel_rx,
            )
            .await
            {
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
    tls_mode: &TlsMode,
    tls_ca_cert: Option<&std::path::Path>,
    cancel_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (client, _conn_handle) = tls::pg_connect(conn_string, tls_mode, tls_ca_cert).await?;

    validate_snapshot_name(snapshot_name)?;

    client
        .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await?;
    client
        .batch_execute(&format!("SET TRANSACTION SNAPSHOT '{}'", snapshot_name))
        .await?;

    info!(snapshot = snapshot_name, "Snapshot holder active");

    // Periodically run a no-op query to prevent idle_in_transaction_session_timeout
    // from killing the snapshot-holding session.
    let mut keepalive_interval = tokio::time::interval(std::time::Duration::from_secs(30));
    keepalive_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel_rx.changed() => break,
            _ = keepalive_interval.tick() => {
                if let Err(e) = client.batch_execute("SELECT 1").await {
                    warn!("Snapshot holder keepalive failed: {} — snapshot may be lost", e);
                    return Err(e.into());
                }
            }
        }
    }

    client.batch_execute("COMMIT").await?;
    Ok(())
}
