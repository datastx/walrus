use crate::backfill_processor;
use crate::catalog;
use crate::cdc_processor;
use crate::ddl_handler;
use iceberg_catalog_sql::SqlCatalog;
use pgiceberg_common::config::{IcebergWriterConfig, SourceConfig};
use pgiceberg_common::metadata::MetadataStore;
use pgiceberg_common::models::FileQueueEntry;
use std::collections::HashMap;
use std::path::Path;
use tokio::sync::watch;
use tracing::{info, warn};

/// Main processing loop for the Iceberg Writer service.
///
/// Polls the file_queue for pending files and processes them.
/// Per-table ordering is enforced: backfill files first, then CDC.
///
/// Recovery: on startup, stale `processing` files are reclaimed.
/// Iceberg commits are atomic — partial commits are impossible.
/// If we crash after writing to Iceberg but before marking files as
/// completed, re-processing produces duplicate data files + equality
/// deletes which cancel out (correct via PK dedup).
pub async fn run_processing_loop(
    source_config: &SourceConfig,
    writer_config: &IcebergWriterConfig,
    metadata: &MetadataStore,
    catalog: &SqlCatalog,
    staging_root: &Path,
    mut shutdown_rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let poll_interval = std::time::Duration::from_secs(writer_config.poll_interval_seconds);

    // Cache of table state (PK columns, loaded Iceberg table handles)
    let mut pk_cache: HashMap<String, Vec<String>> = HashMap::new();

    loop {
        if *shutdown_rx.borrow() {
            info!("Shutdown signal — stopping processing loop");
            break;
        }

        // Step 1: Process any pending DDL events first
        if let Err(e) = ddl_handler::process_ddl_events(metadata).await {
            warn!("DDL processing error: {}", e);
        }

        // Step 2: Claim next batch of files
        let files = metadata
            .claim_next_batch(writer_config.max_files_per_batch)
            .await?;

        if files.is_empty() {
            tokio::select! {
                _ = tokio::time::sleep(poll_interval) => {},
                _ = shutdown_rx.changed() => continue,
            }
            continue;
        }

        let table_key = format!("{}.{}", files[0].table_schema, files[0].table_name);
        info!(
            table = %table_key,
            file_count = files.len(),
            file_type = %files[0].file_type,
            "Claimed batch for processing"
        );

        // Ensure we know the PK columns for this table
        let pk_columns = if let Some(cached) = pk_cache.get(&table_key) {
            cached.clone()
        } else {
            // Re-discover from source (stateless recovery)
            let pk = discover_pk_for_table(source_config, metadata, &files[0]).await?;
            pk_cache.insert(table_key.clone(), pk.clone());
            pk
        };

        // Ensure Iceberg table exists
        let mut table = catalog::ensure_iceberg_table(
            catalog,
            metadata,
            &files[0].table_schema,
            &files[0].table_name,
            &pk_columns,
        )
        .await?;

        // Process based on file type
        let file_ids: Vec<uuid::Uuid> = files.iter().map(|f| f.file_id).collect();

        let result = if files.iter().all(|f| f.file_type == "backfill") {
            backfill_processor::process_backfill_batch(&mut table, &files, staging_root, catalog)
                .await
        } else {
            cdc_processor::process_cdc_batch(&mut table, &files, &pk_columns, staging_root, catalog)
                .await
        };

        match result {
            Ok(()) => {
                metadata.mark_files_completed(&file_ids).await?;
                info!(table = %table_key, files = file_ids.len(), "Batch completed");
            }
            Err(e) => {
                let retry_exceeded = files
                    .iter()
                    .any(|f| f.retry_count >= writer_config.max_retries);
                if retry_exceeded {
                    warn!(
                        table = %table_key,
                        error = %e,
                        "Batch failed permanently — max retries exceeded"
                    );
                }
                metadata
                    .mark_files_failed(&file_ids, &e.to_string())
                    .await?;
                warn!(table = %table_key, error = %e, "Batch failed — will retry");
            }
        }
    }

    Ok(())
}

async fn discover_pk_for_table(
    source_config: &SourceConfig,
    metadata: &MetadataStore,
    file: &FileQueueEntry,
) -> anyhow::Result<Vec<String>> {
    // First check config
    let from_config = source_config.pk_for_table(&file.table_schema, &file.table_name);
    if !from_config.is_empty() {
        return Ok(from_config);
    }

    // Then check persisted table_state
    if let Some(state) = metadata
        .get_table_state(&file.table_schema, &file.table_name)
        .await?
    {
        if !state.primary_key_columns.is_empty() {
            return Ok(state.primary_key_columns);
        }
    }

    // Finally discover from pg_index
    let pk = metadata
        .discover_primary_keys(&file.table_schema, &file.table_name)
        .await?;

    if pk.is_empty() {
        warn!(
            schema = %file.table_schema,
            table = %file.table_name,
            "No primary key found"
        );
    }

    Ok(pk)
}
