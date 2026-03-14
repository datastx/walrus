use arrow::record_batch::RecordBatch;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
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

/// Process backfill Parquet files by appending them to the Iceberg table.
pub async fn process_backfill_batch(
    table: &mut Table,
    files: &[FileQueueEntry],
    staging_root: &Path,
    catalog: &dyn Catalog,
) -> anyhow::Result<()> {
    let mut total_rows = 0i64;
    let mut all_data_files = Vec::new();

    for file in files {
        let full_path = staging_root.join(&file.file_path);
        if !full_path.exists() {
            tracing::warn!(path = %file.file_path, "Backfill file missing — skipping");
            continue;
        }

        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&full_path)?)?.build()?;

        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
        if batches.is_empty() {
            continue;
        }

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_gen = DefaultFileNameGenerator::new(
            "backfill".to_string(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let pw_builder = ParquetWriterBuilder::new(
            Default::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pw_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        let mut writer = DataFileWriterBuilder::new(rolling_builder)
            .build(None)
            .await?;

        for batch in &batches {
            writer.write(batch.clone()).await?;
            total_rows += batch.num_rows() as i64;
        }

        all_data_files.extend(writer.close().await?);
    }

    if !all_data_files.is_empty() {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(all_data_files);
        let tx = action.apply(tx)?;
        *table = tx.commit(catalog).await?;
    }

    info!(
        files = files.len(),
        total_rows, "Backfill batch committed to Iceberg"
    );
    Ok(())
}
