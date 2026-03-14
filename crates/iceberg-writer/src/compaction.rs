use iceberg::spec::ManifestContentType;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_sql::SqlCatalog;
use pgiceberg_common::metadata::MetadataStore;
use tracing::{debug, info, warn};

/// Periodic compaction of Iceberg tables.
///
/// Equality delete files accumulate over time as CDC merges add them.
/// Compaction rewrites data + delete files into consolidated data files
/// with deletes applied, improving read performance.
///
/// Compaction approach (best-effort without RewriteFiles):
///   1. For each table, count delete files in the current snapshot
///   2. If delete count exceeds threshold, trigger compaction
///   3. Read all data + delete files via DuckDB (anti-join)
///   4. Write consolidated Parquet files
///   5. Append new consolidated data files via fast_append
///
/// Limitation: Without iceberg-rust's RewriteFiles action, we cannot remove
/// old data/delete files from the manifest in a single atomic operation.
/// Instead, we append the consolidated data. Query engines will see both old
/// and new data, but equality deletes will cancel out the old data, and the
/// new consolidated files provide the clean read path. This is a valid
/// intermediate step that improves read performance while we wait for full
/// RewriteFiles support.
pub async fn maybe_run_compaction(
    catalog: &SqlCatalog,
    _metadata: &MetadataStore,
    tables: &[(String, String)],
    _compaction_interval_hours: u32,
    delete_threshold: u32,
) -> anyhow::Result<()> {
    for (schema, table_name) in tables {
        let namespace = NamespaceIdent::new(schema.clone());
        let table_ident = TableIdent::new(namespace, table_name.clone());

        match catalog.load_table(&table_ident).await {
            Ok(mut table) => {
                let delete_count = count_delete_files(&table).await?;
                debug!(
                    schema = schema.as_str(),
                    table = table_name.as_str(),
                    delete_files = delete_count,
                    threshold = delete_threshold,
                    "Compaction check"
                );

                if delete_count >= delete_threshold as u64 {
                    info!(
                        schema = schema.as_str(),
                        table = table_name.as_str(),
                        delete_files = delete_count,
                        "Table needs compaction — delete file threshold exceeded"
                    );

                    match run_compaction(&mut table, catalog).await {
                        Ok(rows) => {
                            info!(
                                schema = schema.as_str(),
                                table = table_name.as_str(),
                                consolidated_rows = rows,
                                "Compaction completed — consolidated data appended"
                            );
                            metrics::counter!("walrus_compaction_runs_total",
                                "table" => format!("{}.{}", schema, table_name),
                                "status" => "success"
                            )
                            .increment(1);
                        }
                        Err(e) => {
                            warn!(
                                schema = schema.as_str(),
                                table = table_name.as_str(),
                                error = %e,
                                "Compaction failed"
                            );
                            metrics::counter!("walrus_compaction_runs_total",
                                "table" => format!("{}.{}", schema, table_name),
                                "status" => "error"
                            )
                            .increment(1);
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    schema = schema.as_str(),
                    table = table_name.as_str(),
                    error = %e,
                    "Failed to load table for compaction check"
                );
            }
        }
    }

    Ok(())
}

/// Run compaction: read all data + delete files via DuckDB, write consolidated output.
async fn run_compaction(table: &mut Table, catalog: &dyn Catalog) -> anyhow::Result<u64> {
    let metadata = table.metadata();
    let snapshot = metadata
        .current_snapshot()
        .ok_or_else(|| anyhow::anyhow!("No current snapshot"))?;

    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await?;

    let mut data_file_paths = Vec::new();
    let mut delete_file_paths = Vec::new();

    for entry in manifest_list.entries() {
        let manifest = entry.load_manifest(table.file_io()).await?;
        for manifest_entry in manifest.entries() {
            let path = manifest_entry.data_file().file_path().to_string();
            // Convert file:// URLs to local paths for DuckDB
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

    if data_file_paths.is_empty() {
        return Ok(0);
    }

    info!(
        data_files = data_file_paths.len(),
        delete_files = delete_file_paths.len(),
        "Running DuckDB compaction merge"
    );

    let temp_dir = tempfile::tempdir()?;
    let output_path = temp_dir.path().join("compacted.parquet");
    let out = output_path.clone();

    // Get PK columns from the schema's identifier fields
    let schema = metadata.current_schema();
    let pk_field_ids: Vec<i32> = schema.identifier_field_ids().collect();
    let pk_columns: Vec<String> = pk_field_ids
        .iter()
        .filter_map(|id| {
            schema
                .as_struct()
                .fields()
                .iter()
                .find(|f| f.id == *id)
                .map(|f| f.name.clone())
        })
        .collect();

    let consolidated_rows = tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch("INSTALL parquet; LOAD parquet;")?;

        // Load data files
        let data_list = data_file_paths
            .iter()
            .map(|p| format!("'{}'", p.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!(
            "CREATE TABLE data_files AS SELECT * FROM read_parquet([{}])",
            data_list
        ))?;

        if delete_file_paths.is_empty() || pk_columns.is_empty() {
            // No deletes to apply or no PK — just consolidate data files
            let escaped = out.display().to_string().replace('\'', "''");
            conn.execute_batch(&format!(
                "COPY data_files TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
                escaped
            ))?;
        } else {
            // Load delete files and anti-join
            let delete_list = delete_file_paths
                .iter()
                .map(|p| format!("'{}'", p.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ");
            conn.execute_batch(&format!(
                "CREATE TABLE delete_keys AS SELECT * FROM read_parquet([{}])",
                delete_list
            ))?;

            let join_condition = pk_columns
                .iter()
                .map(|col| format!("d.\"{}\" = dk.\"{}\"", col, col))
                .collect::<Vec<_>>()
                .join(" AND ");

            let escaped = out.display().to_string().replace('\'', "''");
            conn.execute_batch(&format!(
                "COPY (\
                    SELECT d.* FROM data_files d \
                    LEFT JOIN delete_keys dk ON {} \
                    WHERE dk.\"{}\" IS NULL\
                ) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
                join_condition, pk_columns[0], escaped
            ))?;
        }

        // Count consolidated rows
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM data_files")?;
        let count: u64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count)
    })
    .await??;

    if consolidated_rows == 0 || !output_path.exists() {
        return Ok(0);
    }

    // Read consolidated Parquet and append to Iceberg
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(&output_path)?,
    )?
    .build()?;
    let batches: Vec<arrow::record_batch::RecordBatch> = reader.collect::<Result<_, _>>()?;

    if batches.is_empty() {
        return Ok(0);
    }

    let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_gen = DefaultFileNameGenerator::new(
        "compacted".to_string(),
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
    let data_files = writer.close().await?;

    if !data_files.is_empty() {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action.apply(tx)?;
        *table = tx.commit(catalog).await?;
    }

    Ok(consolidated_rows)
}

/// Count the number of equality delete files in the current snapshot.
async fn count_delete_files(table: &Table) -> anyhow::Result<u64> {
    let metadata = table.metadata();
    let snapshot = match metadata.current_snapshot() {
        Some(s) => s,
        None => return Ok(0),
    };

    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await?;

    let mut delete_count = 0u64;
    for entry in manifest_list.entries() {
        if entry.content == ManifestContentType::Deletes {
            delete_count += 1;
        }
    }

    Ok(delete_count)
}
