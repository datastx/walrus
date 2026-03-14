use iceberg::spec::ManifestContentType;
use iceberg::table::Table;
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
/// Compaction flow:
///   1. For each table, count delete files in the current snapshot
///   2. If delete count exceeds threshold, trigger compaction
///   3. Read all data + delete files via DuckDB (anti-join)
///   4. Write consolidated Parquet files
///   5. Commit a rewrite operation that replaces old files
///
/// Note: Full compaction (steps 3-5) requires manually constructing
/// manifests, as iceberg-rust 0.8.0 lacks a RewriteFiles action.
/// For now, we implement threshold detection and log when compaction
/// is needed. The actual rewrite will be added when iceberg-rust
/// supports it or via manual manifest construction.
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
            Ok(table) => {
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
                    // TODO: Implement actual compaction:
                    // 1. Read data + delete files via DuckDB anti-join
                    // 2. Write consolidated Parquet files
                    // 3. Build new manifest entries
                    // 4. Commit via TableUpdate::AddSnapshot + SetSnapshotRef
                    //
                    // This is deferred until iceberg-rust adds a RewriteFiles action
                    // or we implement manual manifest construction.
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
            // Use the existing_rows_count as an approximation of delete file count
            // since each manifest entry represents one manifest file
            delete_count += 1;
        }
    }

    Ok(delete_count)
}
