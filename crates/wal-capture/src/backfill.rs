use crate::copy_export;
use pgiceberg_common::config::{SourceConfig, WalCaptureConfig};
use pgiceberg_common::metadata::{EnqueueFileParams, MetadataStore};
use pgiceberg_common::models::{CtidPartition, FileType, TablePhase, TableState};
use pgiceberg_common::sql::validate_snapshot_name;
use pgiceberg_common::tls;
use std::path::Path;
use tracing::info;

/// Calculate CTID partitions for a table.
pub fn calculate_ctid_partitions(
    relpages: u64,
    rows_per_page: f64,
    rows_per_partition: u64,
) -> Vec<CtidPartition> {
    if relpages == 0 {
        return vec![CtidPartition {
            id: 0,
            start_page: 0,
            end_page: 0,
        }];
    }

    let pages_per_partition = if rows_per_page > 0.0 {
        (rows_per_partition as f64 / rows_per_page).ceil() as u64
    } else {
        relpages
    };
    let pages_per_partition = pages_per_partition.max(1);

    let mut partitions = Vec::new();
    let mut start = 0u64;
    let mut id = 0i32;

    while start < relpages {
        let end = (start + pages_per_partition).min(relpages);
        partitions.push(CtidPartition {
            id,
            start_page: start,
            end_page: end,
        });
        start = end;
        id += 1;
    }

    partitions
}

/// Run backfill for all tables that need it.
///
/// This function spawns parallel workers across tables.  Within a single table,
/// partitions are scanned sequentially (the snapshot must be held for each
/// connection).
///
/// Recovery: if we crashed mid-backfill, `table_state.backfill_done_partitions`
/// tells us exactly where to resume.  The snapshot holder keeps the snapshot
/// alive.  If the snapshot expired (service was down too long), the caller must
/// recreate the slot and restart everything.
pub async fn run_backfill(
    source_config: &SourceConfig,
    wal_config: &WalCaptureConfig,
    metadata: &MetadataStore,
    staging_root: &Path,
) -> anyhow::Result<()> {
    let tables = metadata.get_all_table_states().await?;
    let backfill_tables: Vec<_> = tables
        .into_iter()
        .filter(|t| matches!(t.phase, TablePhase::Backfilling))
        .collect();

    if backfill_tables.is_empty() {
        info!("No tables need backfill");
        return Ok(());
    }

    info!(count = backfill_tables.len(), "Starting backfill");

    let semaphore =
        std::sync::Arc::new(tokio::sync::Semaphore::new(wal_config.max_parallel_tables));

    let mut handles = Vec::new();

    for table_state in backfill_tables {
        let permit = semaphore.clone().acquire_owned().await?;
        let src = source_config.clone();
        let meta = metadata.clone();
        let root = staging_root.to_path_buf();
        let rows_per_part = wal_config.rows_per_partition;

        let handle = tokio::spawn(async move {
            let result =
                backfill_single_table(&src, &meta, &root, &table_state, rows_per_part).await;
            drop(permit);
            result
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    info!("All backfills complete");
    Ok(())
}

async fn backfill_single_table(
    source_config: &SourceConfig,
    metadata: &MetadataStore,
    staging_root: &Path,
    state: &TableState,
    rows_per_partition: u64,
) -> anyhow::Result<()> {
    let schema = &state.table_schema;
    let table = &state.table_name;
    let snapshot_name = state
        .backfill_snapshot_name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("No snapshot name for {}.{}", schema, table))?;

    let (client, _conn_handle) = tls::pg_connect(
        &source_config.connection_string(),
        &source_config.tls_mode,
        source_config.tls_ca_cert.as_deref(),
    )
    .await?;

    let row = client
        .query_one(
            "SELECT COALESCE(relpages, 0)::bigint, GREATEST(reltuples, 0)::float8, \
             c.relkind \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = $1 AND n.nspname = $2",
            &[&table, &schema],
        )
        .await?;

    let relpages: i64 = row.get(0);
    let reltuples: f64 = row.get(1);
    let relkind: String = row.get(2);

    // Partitioned parent tables (relkind = 'p') have relpages=0 and reltuples=0
    // because data lives in child partitions. CTID scans on the parent yield no rows.
    if relkind == "p" {
        tracing::warn!(
            schema,
            table,
            "Table is a partitioned parent — CTID backfill will export 0 rows. \
             Configure individual child partitions in [source.tables] instead."
        );
    }
    let rows_per_page = if relpages > 0 {
        reltuples / relpages as f64
    } else {
        0.0
    };

    let partitions = calculate_ctid_partitions(relpages as u64, rows_per_page, rows_per_partition);
    let total = partitions.len();
    let done = state.backfill_done_partitions as usize;

    info!(
        schema,
        table,
        total_partitions = total,
        already_done = done,
        "Backfilling table"
    );

    for partition in partitions.into_iter().skip(done) {
        backfill_partition(
            source_config,
            metadata,
            staging_root,
            schema,
            table,
            snapshot_name,
            &partition,
            total,
        )
        .await?;
    }

    metadata
        .set_table_phase(schema, table, TablePhase::BackfillComplete)
        .await?;
    info!(
        schema,
        table, "Backfill export complete — awaiting Iceberg Writer to finish processing"
    );

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn backfill_partition(
    source_config: &SourceConfig,
    metadata: &MetadataStore,
    staging_root: &Path,
    schema: &str,
    table: &str,
    snapshot_name: &str,
    partition: &CtidPartition,
    total_partitions: usize,
) -> anyhow::Result<()> {
    let (client, _conn_handle) = tls::pg_connect(
        &source_config.connection_string(),
        &source_config.tls_mode,
        source_config.tls_ca_cert.as_deref(),
    )
    .await?;

    validate_snapshot_name(snapshot_name)?;

    client
        .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await?;
    client
        .batch_execute(&format!("SET TRANSACTION SNAPSHOT '{}'", snapshot_name))
        .await?;

    // Query column metadata for binary COPY parsing
    let columns = copy_export::get_columns(&client, schema, table).await?;

    // Run COPY export within the snapshot transaction
    let copy_data = copy_export::run_copy(
        &client,
        schema,
        table,
        &columns,
        partition.start_page,
        partition.end_page,
    )
    .await?;

    client.batch_execute("COMMIT").await?;

    let file_path = staging_root
        .join("backfill")
        .join(format!("{}.{}", schema, table))
        .join(format!("{}.parquet", partition.id));

    let row_count = tokio::task::spawn_blocking(move || -> anyhow::Result<i64> {
        copy_export::write_binary_to_parquet(&copy_data, &columns, &file_path)
    })
    .await??;

    let relative_path = format!("backfill/{}.{}/{}.parquet", schema, table, partition.id);
    metadata
        .enqueue_file(&EnqueueFileParams {
            schema,
            table,
            file_type: FileType::Backfill,
            file_path: &relative_path,
            lsn_low: None,
            lsn_high: None,
            row_count,
            partition_id: Some(partition.id),
        })
        .await?;

    let done = metadata.advance_backfill_partition(schema, table).await?;
    info!(
        schema,
        table,
        partition = partition.id,
        done,
        total = total_partitions,
        row_count,
        "Partition complete"
    );

    Ok(())
}

#[cfg(test)]
#[path = "backfill_test.rs"]
mod backfill_test;
