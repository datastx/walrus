use crate::duckdb_engine::DuckDbEngine;
use arrow::record_batch::RecordBatch;
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
use pgiceberg_common::models::FileQueueEntry;
use std::path::Path;
use tracing::info;

/// Process CDC Parquet files through the merge pipeline.
pub async fn process_cdc_batch(
    table: &mut Table,
    files: &[FileQueueEntry],
    pk_columns: &[String],
    staging_root: &Path,
    catalog: &dyn Catalog,
) -> anyhow::Result<()> {
    if pk_columns.is_empty() {
        anyhow::bail!(
            "Cannot process CDC without primary key columns for {}.{}",
            files[0].table_schema,
            files[0].table_name
        );
    }

    let engine = DuckDbEngine::new()?;
    let temp_dir = tempfile::tempdir()?;

    let file_paths: Vec<String> = files
        .iter()
        .map(|f| {
            staging_root
                .join(&f.file_path)
                .to_string_lossy()
                .to_string()
        })
        .filter(|p| Path::new(p).exists())
        .collect();

    if file_paths.is_empty() {
        tracing::warn!("No CDC files found on disk — skipping batch");
        return Ok(());
    }

    engine.load_staged_files(&file_paths)?;
    engine.dedup_by_pk(pk_columns)?;
    engine.separate_operations(pk_columns)?;

    let upsert_path = temp_dir.path().join("upsert.parquet");
    let delete_path = temp_dir.path().join("delete_keys.parquet");

    let upsert_count = engine.export_upserts(&upsert_path)?;
    let delete_count = engine.export_deletes(&delete_path)?;
    engine.cleanup()?;

    let mut all_data_files = Vec::new();

    if upsert_count > 0 {
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&upsert_path)?)?
            .build()?;
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_gen = DefaultFileNameGenerator::new(
            "cdc-data".to_string(),
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
            "cdc-delete".to_string(),
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
            table = %files[0].table_name,
            upserts = upsert_count,
            deletes = delete_count,
            files = files.len(),
            "CDC batch committed to Iceberg"
        );
    }

    Ok(())
}
