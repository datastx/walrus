use crate::duckdb_engine::DuckDbEngine;
use arrow::record_batch::RecordBatch;
use iceberg::spec::ManifestContentType;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::Catalog;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::path::Path;
use tracing::info;

/// Resync result counts for logging.
pub struct ResyncResult {
    pub upsert_count: u64,
    pub delete_count: u64,
}

/// Process a resync by diffing backfill data against existing Iceberg data.
///
/// Instead of dropping and recreating the Iceberg table, this computes
/// the diff (upserts and deletes) between the new backfill snapshot and
/// the existing Iceberg data, then writes only the changes.  This
/// preserves Iceberg snapshot history.
pub async fn process_resync(
    table: &mut Table,
    backfill_paths: &[String],
    pk_columns: &[String],
    staging_root: &Path,
    catalog: &dyn Catalog,
) -> anyhow::Result<ResyncResult> {
    if pk_columns.is_empty() {
        anyhow::bail!("Cannot process resync without primary key columns");
    }

    // Resolve backfill paths to absolute filesystem paths, filtering missing files
    let new_file_paths: Vec<String> = backfill_paths
        .iter()
        .map(|p| staging_root.join(p).to_string_lossy().to_string())
        .filter(|p| Path::new(p).exists())
        .collect();

    if new_file_paths.is_empty() {
        tracing::warn!("No backfill files found on disk for resync — skipping");
        return Ok(ResyncResult {
            upsert_count: 0,
            delete_count: 0,
        });
    }

    // Get existing Iceberg data + delete file paths from the current snapshot.
    // We need both to correctly compute the diff: equality deletes must be
    // anti-joined against data files to get the true current state.
    let (old_data_paths, old_delete_paths) = get_existing_file_paths(table).await?;

    // If no existing data, all backfill data is new — fast_append like normal backfill
    if old_data_paths.is_empty() {
        info!("No existing Iceberg data files — treating resync as initial backfill append");
        let row_count = append_backfill_files(table, &new_file_paths, catalog).await?;
        return Ok(ResyncResult {
            upsert_count: row_count,
            delete_count: 0,
        });
    }

    let temp_dir = tempfile::tempdir()?;
    let upsert_path = temp_dir.path().join("resync_upsert.parquet");
    let delete_path = temp_dir.path().join("resync_delete_keys.parquet");

    let pk_cols = pk_columns.to_vec();
    let up = upsert_path.clone();
    let dp = delete_path.clone();
    let (upsert_count, delete_count) =
        tokio::task::spawn_blocking(move || -> anyhow::Result<(u64, u64)> {
            let engine = DuckDbEngine::new()?;
            engine.load_resync_data_with_deletes(&old_data_paths, &old_delete_paths, &new_file_paths, &pk_cols)?;
            engine.compute_resync_diff(&pk_cols)?;
            let uc = engine.export_upserts(&up)?;
            let dc = engine.export_deletes(&dp)?;
            engine.cleanup()?;
            Ok((uc, dc))
        })
        .await??;

    // If identical data, skip the commit
    if upsert_count == 0 && delete_count == 0 {
        info!("Resync diff is empty — existing data matches new snapshot, skipping commit");
        return Ok(ResyncResult {
            upsert_count: 0,
            delete_count: 0,
        });
    }

    let mut all_data_files = Vec::new();

    if upsert_count > 0 {
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&upsert_path)?)?
            .build()?;
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_gen = DefaultFileNameGenerator::new(
            "resync-data".to_string(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let pw_builder = ParquetWriterBuilder::new(
            Default::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            pw_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
        for batch in &batches {
            writer.write(batch.clone()).await?;
        }
        all_data_files.extend(writer.close().await?);
    }

    if delete_count > 0 {
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&delete_path)?)?
            .build()?;
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;

        let iceberg_schema = table.metadata().current_schema();
        let pk_field_ids: Vec<i32> = pk_columns
            .iter()
            .filter_map(|name| iceberg_schema.as_struct().field_by_name(name).map(|f| f.id))
            .collect();

        let config = EqualityDeleteWriterConfig::new(pk_field_ids, iceberg_schema.clone())?;

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_gen = DefaultFileNameGenerator::new(
            "resync-delete".to_string(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let pw_builder = ParquetWriterBuilder::new(
            Default::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            pw_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        let mut del_writer = EqualityDeleteFileWriterBuilder::new(rolling, config)
            .build(None)
            .await?;

        for batch in &batches {
            del_writer.write(batch.clone()).await?;
        }
        all_data_files.extend(del_writer.close().await?);
    }

    if !all_data_files.is_empty() {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(all_data_files);
        let tx = action.apply(tx)?;
        *table = tx.commit(catalog).await?;

        info!(
            upserts = upsert_count,
            deletes = delete_count,
            "Resync merge committed to Iceberg — snapshot history preserved"
        );
    }

    Ok(ResyncResult {
        upsert_count,
        delete_count,
    })
}

/// Extract existing data and delete file paths from the current Iceberg snapshot.
///
/// Returns `(data_files, delete_files)` with `file://` prefix stripped for
/// DuckDB compatibility. The delete files must be applied (anti-joined) against
/// the data files to get the true current state. Without this, "deleted" rows
/// still present in data files would appear in the resync diff.
async fn get_existing_file_paths(table: &Table) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    let metadata = table.metadata();
    let snapshot = match metadata.current_snapshot() {
        Some(s) => s,
        None => return Ok((Vec::new(), Vec::new())),
    };

    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await?;

    let mut data_file_paths = Vec::new();
    let mut delete_file_paths = Vec::new();
    for entry in manifest_list.entries() {
        let manifest = entry.load_manifest(table.file_io()).await?;
        for manifest_entry in manifest.entries() {
            let path = manifest_entry.data_file().file_path().to_string();
            let local_path = if let Some(stripped) = path.strip_prefix("file://") {
                stripped.to_string()
            } else {
                path
            };
            match entry.content {
                ManifestContentType::Data => data_file_paths.push(local_path),
                ManifestContentType::Deletes => delete_file_paths.push(local_path),
            }
        }
    }

    Ok((data_file_paths, delete_file_paths))
}


/// Append backfill files directly (used when there are no existing Iceberg data files).
async fn append_backfill_files(
    table: &mut Table,
    file_paths: &[String],
    catalog: &dyn Catalog,
) -> anyhow::Result<u64> {
    let mut total_rows = 0u64;
    let mut all_data_files = Vec::new();

    for file_path in file_paths {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(file_path)?)?.build()?;
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
        if batches.is_empty() {
            continue;
        }

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_gen = DefaultFileNameGenerator::new(
            "resync-backfill".to_string(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let pw_builder = ParquetWriterBuilder::new(
            Default::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            pw_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
        for batch in &batches {
            writer.write(batch.clone()).await?;
            total_rows += batch.num_rows() as u64;
        }
        all_data_files.extend(writer.close().await?);
    }

    if !all_data_files.is_empty() {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(all_data_files);
        let tx = action.apply(tx)?;
        *table = tx.commit(catalog).await?;
    }

    Ok(total_rows)
}
