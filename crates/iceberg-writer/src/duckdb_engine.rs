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
               SELECT *, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY _pgiceberg_ts DESC) AS rn \
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
             SELECT * EXCLUDE (_pgiceberg_op, _pgiceberg_ts) \
             FROM deduped WHERE _pgiceberg_op IN ('I', 'U')",
        )?;

        let sql = format!(
            "CREATE OR REPLACE TABLE delete_keys AS \
             SELECT {} FROM deduped WHERE _pgiceberg_op IN ('U', 'D')",
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

    /// Clean up temp tables.
    pub fn cleanup(&self) -> anyhow::Result<()> {
        self.conn.execute_batch(
            "DROP TABLE IF EXISTS staged; \
             DROP TABLE IF EXISTS deduped; \
             DROP TABLE IF EXISTS to_upsert; \
             DROP TABLE IF EXISTS delete_keys;",
        )?;
        Ok(())
    }
}
