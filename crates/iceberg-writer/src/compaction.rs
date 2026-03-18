use iceberg_catalog_sql::SqlCatalog;
use pgiceberg_common::metadata::MetadataStore;
use tracing::debug;

/// Periodic compaction of Iceberg tables.
///
/// **DISABLED**: Compaction is disabled because iceberg-rust 0.8.0 does not
/// support `RewriteFiles`. Without it, `fast_append` adds consolidated data
/// files but cannot remove the old data/delete files. For rows that were
/// never touched by equality deletes (e.g., backfill-only rows), both the
/// old data file and the new compacted file contain the same row, causing
/// query engines to see duplicates.
///
/// Compaction will be re-enabled when iceberg-rust supports `RewriteFiles`,
/// allowing atomic replacement of old files with consolidated ones.
pub async fn maybe_run_compaction(
    _catalog: &SqlCatalog,
    _metadata: &MetadataStore,
    _tables: &[(String, String)],
    _compaction_interval_hours: u32,
    _delete_threshold: u32,
) -> anyhow::Result<()> {
    debug!(
        "Compaction is disabled — iceberg-rust 0.8.0 lacks RewriteFiles support. \
         Without it, compaction duplicates backfill-only rows."
    );
    Ok(())
}
