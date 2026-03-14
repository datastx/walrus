use crate::parquet_writer::write_rows_to_parquet;
use pgiceberg_common::config::{SourceConfig, WalCaptureConfig};
use pgiceberg_common::metadata::MetadataStore;
use pgiceberg_common::models::{CtidPartition, TablePhase, TableState};
use std::path::Path;
use tokio_postgres::NoTls;
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

    let (client, conn) = tokio_postgres::connect(&source_config.connection_string(), NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("backfill connection error: {}", e);
        }
    });

    // Get page statistics for partition calculation
    let row = client
        .query_one(
            "SELECT COALESCE(relpages, 0)::bigint, GREATEST(reltuples, 0)::float8 \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = $1 AND n.nspname = $2",
            &[&table, &schema],
        )
        .await?;

    let relpages: i64 = row.get(0);
    let reltuples: f64 = row.get(1);
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
        .set_table_phase(schema, table, TablePhase::Streaming)
        .await?;
    info!(
        schema,
        table, "Backfill complete — transitioning to streaming"
    );

    Ok(())
}

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
    let (client, conn) = tokio_postgres::connect(&source_config.connection_string(), NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("partition connection error: {}", e);
        }
    });

    // Use the snapshot for consistent reads
    client
        .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await?;
    client
        .batch_execute(&format!("SET TRANSACTION SNAPSHOT '{}'", snapshot_name))
        .await?;

    let query = if partition.start_page == 0 && partition.end_page == 0 {
        format!("SELECT * FROM {}.{} WHERE false", schema, table)
    } else {
        format!(
            "SELECT * FROM {}.{} WHERE ctid >= '({},0)'::tid AND ctid < '({},0)'::tid",
            schema, table, partition.start_page, partition.end_page
        )
    };

    let rows = client.query(&query, &[]).await?;

    client.batch_execute("COMMIT").await?;

    let row_count = rows.len() as i64;

    // Write Parquet file
    let dir = staging_root
        .join("backfill")
        .join(format!("{}.{}", schema, table));
    std::fs::create_dir_all(&dir)?;
    let file_path = dir.join(format!("{}.parquet", partition.id));

    if !rows.is_empty() {
        write_rows_to_parquet(&rows, &file_path)?;
    } else {
        // Write an empty Parquet with schema only
        write_rows_to_parquet(&rows, &file_path)?;
    }

    // Enqueue the file
    let relative_path = format!("backfill/{}.{}/{}.parquet", schema, table, partition.id);
    metadata
        .enqueue_file(
            schema,
            table,
            "backfill",
            &relative_path,
            None,
            None,
            row_count,
            Some(partition.id),
        )
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
mod tests {
    use super::*;

    #[test]
    fn test_partition_calculation_small_table() {
        let partitions = calculate_ctid_partitions(10, 100.0, 500);
        assert_eq!(partitions.len(), 2);
        assert_eq!(
            partitions[0],
            CtidPartition {
                id: 0,
                start_page: 0,
                end_page: 5
            }
        );
        assert_eq!(
            partitions[1],
            CtidPartition {
                id: 1,
                start_page: 5,
                end_page: 10
            }
        );
    }

    #[test]
    fn test_partition_calculation_empty_table() {
        let partitions = calculate_ctid_partitions(0, 0.0, 500);
        assert_eq!(partitions.len(), 1);
        assert_eq!(
            partitions[0],
            CtidPartition {
                id: 0,
                start_page: 0,
                end_page: 0
            }
        );
    }

    #[test]
    fn test_partition_calculation_single_page() {
        let partitions = calculate_ctid_partitions(1, 50.0, 500);
        assert_eq!(partitions.len(), 1);
        assert_eq!(
            partitions[0],
            CtidPartition {
                id: 0,
                start_page: 0,
                end_page: 1
            }
        );
    }

    #[test]
    fn test_partition_calculation_exact_fit() {
        let partitions = calculate_ctid_partitions(100, 100.0, 5000);
        // 5000 rows / 100 rpp = 50 pages per partition → 2 partitions
        assert_eq!(partitions.len(), 2);
    }

    #[test]
    fn test_partition_calculation_large_table() {
        let partitions = calculate_ctid_partitions(10000, 50.0, 500_000);
        // 500000 / 50 = 10000 pages per partition → exactly 1 partition
        assert_eq!(partitions.len(), 1);
    }

    #[test]
    fn test_partition_calculation_zero_rows_per_page() {
        let partitions = calculate_ctid_partitions(100, 0.0, 500);
        // When rpp is 0, use entire table as one partition
        assert_eq!(partitions.len(), 1);
    }
}
