use duckdb::Connection;
use std::path::Path;
use tracing::debug;

/// Wrapper around an in-memory DuckDB connection used for CDC merge transforms.
///
/// DuckDB is used purely for Parquet reading + SQL transforms.  Iceberg
/// operations are handled by iceberg-rust.
pub struct DuckDbEngine {
    conn: Connection,
}

impl DuckDbEngine {
    pub fn new() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("INSTALL parquet; LOAD parquet;")?;
        Ok(Self { conn })
    }

    /// Load one or more staged Parquet files into a temp table called `staged`.
    pub fn load_staged_files(&self, file_paths: &[String]) -> anyhow::Result<()> {
        let file_list = file_paths
            .iter()
            .map(|p| format!("'{}'", p.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "CREATE OR REPLACE TABLE staged AS SELECT * FROM read_parquet([{}])",
            file_list
        );
        debug!(sql = %sql, "Loading staged files into DuckDB");
        self.conn.execute_batch(&sql)?;
        Ok(())
    }

    /// Deduplicate staged records: for each PK, keep only the latest operation
    /// (by `_pgiceberg_ts`).
    pub fn dedup_by_pk(&self, pk_columns: &[String]) -> anyhow::Result<()> {
        let pk_csv = pk_columns.join(", ");
        let sql = format!(
            "CREATE OR REPLACE TABLE deduped AS \
             WITH ranked AS ( \
               SELECT *, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY _pgiceberg_ts DESC, _pgiceberg_seq DESC) AS rn \
               FROM staged \
             ) \
             SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1",
            pk_csv
        );
        self.conn.execute_batch(&sql)?;
        Ok(())
    }

    /// Separate deduped records into upsert rows and delete keys.
    pub fn separate_operations(&self, pk_columns: &[String]) -> anyhow::Result<()> {
        let pk_csv = pk_columns.join(", ");

        self.conn.execute_batch(
            "CREATE OR REPLACE TABLE to_upsert AS \
             SELECT * EXCLUDE (_pgiceberg_op, _pgiceberg_ts, _pgiceberg_seq) \
             FROM deduped WHERE _pgiceberg_op IN ('I', 'U')",
        )?;

        let sql = format!(
            "CREATE OR REPLACE TABLE delete_keys AS \
             SELECT {} FROM deduped",
            pk_csv
        );
        self.conn.execute_batch(&sql)?;

        Ok(())
    }

    /// Export the `to_upsert` table to a Parquet file.
    pub fn export_upserts(&self, output_path: &Path) -> anyhow::Result<u64> {
        let count = self.table_count("to_upsert")?;
        if count > 0 {
            let escaped = output_path.display().to_string().replace('\'', "''");
            let sql = format!(
                "COPY to_upsert TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
                escaped
            );
            self.conn.execute_batch(&sql)?;
        }
        Ok(count)
    }

    /// Export the `delete_keys` table to a Parquet file.
    pub fn export_deletes(&self, output_path: &Path) -> anyhow::Result<u64> {
        let count = self.table_count("delete_keys")?;
        if count > 0 {
            let escaped = output_path.display().to_string().replace('\'', "''");
            let sql = format!(
                "COPY delete_keys TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
                escaped
            );
            self.conn.execute_batch(&sql)?;
        }
        Ok(count)
    }

    fn table_count(&self, table: &str) -> anyhow::Result<u64> {
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT COUNT(*) FROM {}", table))?;
        let count: u64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count)
    }

    // ── Resync operations ──────────────────────────────────────────

    /// Load existing Iceberg data files (with equality deletes applied) and new
    /// backfill files into separate DuckDB tables.
    ///
    /// Creates `old_data` (existing Iceberg rows after applying deletes) and
    /// `new_data` (fresh backfill rows). Without applying delete files, rows
    /// that were "deleted" via equality deletes would still appear in old_data,
    /// causing incorrect diff results.
    pub fn load_resync_data_with_deletes(
        &self,
        old_data_files: &[String],
        old_delete_files: &[String],
        new_files: &[String],
        pk_columns: &[String],
    ) -> anyhow::Result<()> {
        let old_list = old_data_files
            .iter()
            .map(|p| format!("'{}'", p.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        let new_list = new_files
            .iter()
            .map(|p| format!("'{}'", p.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");

        if old_delete_files.is_empty() || pk_columns.is_empty() {
            // No delete files to apply — load data files directly
            let sql = format!(
                "CREATE OR REPLACE TABLE old_data AS SELECT * FROM read_parquet([{}])",
                old_list
            );
            debug!(sql = %sql, "Loading existing Iceberg data into DuckDB");
            self.conn.execute_batch(&sql)?;
        } else {
            // Load data files, then anti-join against delete files to get true state
            let delete_list = old_delete_files
                .iter()
                .map(|p| format!("'{}'", p.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ");

            let sql = format!(
                "CREATE OR REPLACE TABLE _raw_old_data AS SELECT * FROM read_parquet([{}])",
                old_list
            );
            self.conn.execute_batch(&sql)?;

            let sql = format!(
                "CREATE OR REPLACE TABLE _delete_keys AS SELECT * FROM read_parquet([{}])",
                delete_list
            );
            self.conn.execute_batch(&sql)?;

            let join_cond = pk_columns
                .iter()
                .map(|c| format!("d.\"{}\" = dk.\"{}\"", c, c))
                .collect::<Vec<_>>()
                .join(" AND ");

            let sql = format!(
                "CREATE OR REPLACE TABLE old_data AS \
                 SELECT d.* FROM _raw_old_data d \
                 LEFT JOIN _delete_keys dk ON {} \
                 WHERE dk.\"{}\" IS NULL",
                join_cond, pk_columns[0]
            );
            debug!(
                delete_files = old_delete_files.len(),
                "Applying equality deletes to old data for resync diff"
            );
            self.conn.execute_batch(&sql)?;
            self.conn.execute_batch(
                "DROP TABLE IF EXISTS _raw_old_data; DROP TABLE IF EXISTS _delete_keys;",
            )?;
        }

        let sql = format!(
            "CREATE OR REPLACE TABLE new_data AS SELECT * FROM read_parquet([{}])",
            new_list
        );
        debug!(sql = %sql, "Loading new backfill data into DuckDB");
        self.conn.execute_batch(&sql)?;

        Ok(())
    }

    /// Compute the diff between new_data and old_data.
    ///
    /// Uses EXCEPT when schemas match (fast path). Falls back to PK-based
    /// anti-join when schemas differ (e.g., column added/dropped since last
    /// sync), avoiding the DuckDB error that EXCEPT requires matching columns.
    ///
    /// Creates:
    ///   - `to_upsert`: rows in new_data that are new or changed
    ///   - `delete_keys`: PK values in old_data that no longer exist in new_data
    pub fn compute_resync_diff(&self, pk_columns: &[String]) -> anyhow::Result<()> {
        let pk_csv = pk_columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        // Try EXCEPT first (fast path for matching schemas)
        let except_ok = self
            .conn
            .execute_batch(
                "CREATE OR REPLACE TABLE to_upsert AS \
                 SELECT * FROM new_data EXCEPT SELECT * FROM old_data",
            )
            .is_ok();

        if !except_ok {
            // Schema mismatch: fall back to PK-based diff.
            // Since the schema changed, all rows from new_data are upserted
            // (the entire table needs rewriting with the new schema).
            debug!("Resync using PK-based diff — old and new schemas differ");
            let pk_join = pk_columns
                .iter()
                .map(|c| format!("n.\"{}\" = o.\"{}\"", c, c))
                .collect::<Vec<_>>()
                .join(" AND ");

            self.conn.execute_batch(&format!(
                "CREATE OR REPLACE TABLE to_upsert AS \
                 SELECT n.* FROM new_data n \
                 LEFT JOIN old_data o ON {pk_join} \
                 WHERE o.\"{first_pk}\" IS NULL",
                pk_join = pk_join,
                first_pk = pk_columns[0]
            ))?;
            // Also include rows where PK exists in both — schema changed so
            // values need rewriting even if they look the same.
            self.conn.execute_batch(&format!(
                "INSERT INTO to_upsert \
                 SELECT n.* FROM new_data n \
                 INNER JOIN old_data o ON {pk_join}",
                pk_join = pk_join
            ))?;
        }

        // Deletes: PKs in old but not in new (PK-only EXCEPT always works)
        let sql = format!(
            "CREATE OR REPLACE TABLE delete_keys AS \
             SELECT {pk} FROM old_data EXCEPT SELECT {pk} FROM new_data",
            pk = pk_csv
        );
        self.conn.execute_batch(&sql)?;

        Ok(())
    }

    /// Clean up temp tables.
    pub fn cleanup(&self) -> anyhow::Result<()> {
        self.conn.execute_batch(
            "DROP TABLE IF EXISTS staged; \
             DROP TABLE IF EXISTS deduped; \
             DROP TABLE IF EXISTS to_upsert; \
             DROP TABLE IF EXISTS delete_keys; \
             DROP TABLE IF EXISTS old_data; \
             DROP TABLE IF EXISTS new_data;",
        )?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "duckdb_engine_test.rs"]
mod duckdb_engine_test;
