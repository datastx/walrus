use crate::config::TlsMode;
use crate::models::*;
use crate::tls;
use deadpool_postgres::{Config, Pool};
use uuid::Uuid;

pub struct EnqueueFileParams<'a> {
    pub schema: &'a str,
    pub table: &'a str,
    pub file_type: FileType,
    pub file_path: &'a str,
    pub lsn_low: Option<&'a Lsn>,
    pub lsn_high: Option<&'a Lsn>,
    pub row_count: i64,
    pub partition_id: Option<i32>,
}

pub struct EnqueueCdcFileParams<'a> {
    pub slot_name: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
    pub file_path: &'a str,
    pub lsn_low: &'a Lsn,
    pub lsn_high: &'a Lsn,
    pub row_count: i64,
}

/// Client for the `_pgiceberg` metadata schema in the source Postgres.
///
/// All state that both services need to coordinate lives here.  On startup each
/// service calls `bootstrap()` which runs idempotent DDL (IF NOT EXISTS
/// everywhere) so restarts are safe.
///
/// Recovery contract: every public method is idempotent.  If a service is
/// killed mid-operation and restarted, calling the same method with the same
/// arguments produces the same observable result.
#[derive(Clone)]
pub struct MetadataStore {
    pool: Pool,
}

impl MetadataStore {
    /// Connect with TLS disabled (backward compatible).
    pub async fn connect(conn_string: &str) -> anyhow::Result<Self> {
        Self::connect_with_tls(conn_string, &TlsMode::Disable, None).await
    }

    /// Connect with configurable TLS mode.
    pub async fn connect_with_tls(
        conn_string: &str,
        tls_mode: &TlsMode,
        ca_cert_path: Option<&std::path::Path>,
    ) -> anyhow::Result<Self> {
        let mut cfg = Config::new();
        for part in conn_string.split_whitespace() {
            let kv: Vec<&str> = part.splitn(2, '=').collect();
            if kv.len() != 2 {
                continue;
            }
            match kv[0] {
                "host" => cfg.host = Some(kv[1].to_string()),
                "port" => cfg.port = kv[1].parse().ok(),
                "dbname" => cfg.dbname = Some(kv[1].to_string()),
                "user" => cfg.user = Some(kv[1].to_string()),
                "password" => cfg.password = Some(kv[1].to_string()),
                _ => {}
            }
        }
        let pool = tls::create_pg_pool(cfg, tls_mode, ca_cert_path)?;
        Ok(Self { pool })
    }

    /// Idempotent schema bootstrap.  Safe to call on every startup.
    pub async fn bootstrap(&self) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client.batch_execute(BOOTSTRAP_SQL).await?;
        Ok(())
    }

    // ── replication_state ──────────────────────────────────────────

    pub async fn get_replication_state(
        &self,
        slot_name: &str,
    ) -> anyhow::Result<Option<ReplicationState>> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT slot_name, publication_name, \
                 consistent_point::text, snapshot_name, \
                 last_flushed_lsn::text, last_acked_lsn::text \
                 FROM _pgiceberg.replication_state WHERE slot_name = $1",
                &[&slot_name],
            )
            .await?;
        Ok(row.map(|r| ReplicationState {
            slot_name: r.get(0),
            publication_name: r.get(1),
            consistent_point: r
                .get::<_, Option<String>>(2)
                .and_then(|s| Lsn::parse(&s).ok()),
            snapshot_name: r.get(3),
            last_flushed_lsn: r
                .get::<_, Option<String>>(4)
                .and_then(|s| Lsn::parse(&s).ok()),
            last_acked_lsn: r
                .get::<_, Option<String>>(5)
                .and_then(|s| Lsn::parse(&s).ok()),
        }))
    }

    pub async fn insert_replication_state(
        &self,
        slot_name: &str,
        publication_name: &str,
        consistent_point: &str,
        snapshot_name: &str,
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "INSERT INTO _pgiceberg.replication_state \
                 (slot_name, publication_name, consistent_point, snapshot_name) \
                 VALUES ($1, $2, $3::text::pg_lsn, $4) \
                 ON CONFLICT (slot_name) DO UPDATE SET \
                   consistent_point = EXCLUDED.consistent_point, \
                   snapshot_name = EXCLUDED.snapshot_name, \
                   updated_at = now()",
                &[
                    &slot_name,
                    &publication_name,
                    &consistent_point,
                    &snapshot_name,
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn update_flushed_lsn(&self, slot_name: &str, lsn: &Lsn) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        let lsn_str = lsn.to_string();
        client
            .execute(
                "UPDATE _pgiceberg.replication_state \
                 SET last_flushed_lsn = $2::text::pg_lsn, updated_at = now() \
                 WHERE slot_name = $1",
                &[&slot_name, &lsn_str],
            )
            .await?;
        Ok(())
    }

    // ── table_state ───────────────────────────────────────────────

    pub async fn get_table_state(
        &self,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<Option<TableState>> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT table_schema, table_name, phase, \
                 backfill_total_partitions, backfill_done_partitions, \
                 backfill_snapshot_name, \
                 writer_backfill_files_done, writer_last_committed_lsn::text, \
                 streaming_since, \
                 iceberg_schema_version, primary_key_columns, \
                 needs_resync \
                 FROM _pgiceberg.table_state \
                 WHERE table_schema = $1 AND table_name = $2",
                &[&schema, &table],
            )
            .await?;
        Ok(row.map(Self::row_to_table_state))
    }

    pub async fn get_all_table_states(&self) -> anyhow::Result<Vec<TableState>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT table_schema, table_name, phase, \
                 backfill_total_partitions, backfill_done_partitions, \
                 backfill_snapshot_name, \
                 writer_backfill_files_done, writer_last_committed_lsn::text, \
                 streaming_since, \
                 iceberg_schema_version, primary_key_columns, \
                 needs_resync \
                 FROM _pgiceberg.table_state",
                &[],
            )
            .await?;
        Ok(rows.into_iter().map(Self::row_to_table_state).collect())
    }

    fn row_to_table_state(r: tokio_postgres::Row) -> TableState {
        TableState {
            table_schema: r.get(0),
            table_name: r.get(1),
            phase: r.get::<_, String>(2).parse().unwrap(),
            backfill_total_partitions: r.get(3),
            backfill_done_partitions: r.get(4),
            backfill_snapshot_name: r.get(5),
            writer_backfill_files_done: r.get(6),
            writer_last_committed_lsn: r
                .get::<_, Option<String>>(7)
                .and_then(|s| Lsn::parse(&s).ok()),
            streaming_since: r.get(8),
            iceberg_schema_version: r.get(9),
            primary_key_columns: r.get::<_, Vec<String>>(10),
            needs_resync: r.get(11),
        }
    }

    pub async fn upsert_table_state(
        &self,
        schema: &str,
        table: &str,
        phase: TablePhase,
        total_partitions: Option<i32>,
        snapshot_name: Option<&str>,
        pk_columns: &[String],
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        let phase_str = phase.as_str();
        client
            .execute(
                "INSERT INTO _pgiceberg.table_state \
                 (table_schema, table_name, phase, backfill_total_partitions, \
                  backfill_snapshot_name, primary_key_columns) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT (table_schema, table_name) DO UPDATE SET \
                   phase = EXCLUDED.phase, \
                   backfill_total_partitions = COALESCE(EXCLUDED.backfill_total_partitions, \
                       _pgiceberg.table_state.backfill_total_partitions), \
                   backfill_snapshot_name = COALESCE(EXCLUDED.backfill_snapshot_name, \
                       _pgiceberg.table_state.backfill_snapshot_name), \
                   primary_key_columns = CASE WHEN array_length(EXCLUDED.primary_key_columns, 1) > 0 \
                       THEN EXCLUDED.primary_key_columns \
                       ELSE _pgiceberg.table_state.primary_key_columns END, \
                   updated_at = now()",
                &[&schema, &table, &phase_str, &total_partitions, &snapshot_name, &pk_columns],
            )
            .await?;
        Ok(())
    }

    pub async fn advance_backfill_partition(
        &self,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<i32> {
        let client = self.pool.get().await?;
        let row = client
            .query_one(
                "UPDATE _pgiceberg.table_state \
                 SET backfill_done_partitions = backfill_done_partitions + 1, \
                     updated_at = now() \
                 WHERE table_schema = $1 AND table_name = $2 \
                 RETURNING backfill_done_partitions",
                &[&schema, &table],
            )
            .await?;
        Ok(row.get(0))
    }

    pub async fn set_table_phase(
        &self,
        schema: &str,
        table: &str,
        phase: TablePhase,
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        let phase_str = phase.as_str();
        client
            .execute(
                "UPDATE _pgiceberg.table_state SET phase = $3, updated_at = now() \
                 WHERE table_schema = $1 AND table_name = $2",
                &[&schema, &table, &phase_str],
            )
            .await?;
        Ok(())
    }

    /// Update only the primary key columns for a table without touching the phase.
    /// Used during recovery to refresh PK info from pg_index without overwriting
    /// the persisted lifecycle phase.
    pub async fn update_pk_columns(
        &self,
        schema: &str,
        table: &str,
        pk_columns: &[String],
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "UPDATE _pgiceberg.table_state \
                 SET primary_key_columns = $3, updated_at = now() \
                 WHERE table_schema = $1 AND table_name = $2",
                &[&schema, &table, &pk_columns],
            )
            .await?;
        Ok(())
    }

    /// Transition a table from backfill_complete to streaming.
    ///
    /// Called by the Iceberg Writer after it has processed all backfill files
    /// for a table.  Only transitions if:
    ///   1. The table phase is currently 'backfill_complete'
    ///   2. No pending or processing backfill files remain in the file_queue
    ///
    /// Returns true if the transition was made.
    pub async fn try_transition_to_streaming(
        &self,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<bool> {
        let client = self.pool.get().await?;
        let result = client
            .execute(
                "UPDATE _pgiceberg.table_state \
                 SET phase = 'streaming', streaming_since = now(), \
                     needs_resync = FALSE, updated_at = now() \
                 WHERE table_schema = $1 AND table_name = $2 \
                   AND phase = 'backfill_complete' \
                   AND NOT EXISTS ( \
                     SELECT 1 FROM _pgiceberg.file_queue \
                     WHERE table_schema = $1 AND table_name = $2 \
                       AND file_type = 'backfill' \
                       AND status NOT IN ('completed', 'deleted', 'failed') \
                   )",
                &[&schema, &table],
            )
            .await?;
        Ok(result > 0)
    }

    /// Get all pending backfill file paths for a specific table.
    ///
    /// Returns `(file_id, file_path)` for every backfill file with status
    /// 'pending' or 'processing' for this table, ordered by creation time.
    /// Used during resync to collect the complete new snapshot for diffing.
    pub async fn get_all_pending_backfill_paths(
        &self,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<Vec<(uuid::Uuid, String)>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT file_id, file_path FROM _pgiceberg.file_queue \
                 WHERE table_schema = $1 AND table_name = $2 \
                   AND file_type = 'backfill' \
                   AND status IN ('pending', 'processing') \
                 ORDER BY created_at",
                &[&schema, &table],
            )
            .await?;
        Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    /// Clear the needs_resync flag after the Iceberg table has been dropped
    /// and recreated.  Subsequent backfill batches for this table should append
    /// normally rather than triggering another drop.
    pub async fn clear_resync_flag(&self, schema: &str, table: &str) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "UPDATE _pgiceberg.table_state \
                 SET needs_resync = FALSE, updated_at = now() \
                 WHERE table_schema = $1 AND table_name = $2",
                &[&schema, &table],
            )
            .await?;
        Ok(())
    }

    // ── writer progress ────────────────────────────────────────────

    /// Increment the count of backfill files the writer has committed to Iceberg.
    /// Called after each successful backfill batch.
    pub async fn advance_writer_backfill_progress(
        &self,
        schema: &str,
        table: &str,
        files_done: i32,
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "UPDATE _pgiceberg.table_state \
                 SET writer_backfill_files_done = writer_backfill_files_done + $3, \
                     updated_at = now() \
                 WHERE table_schema = $1 AND table_name = $2",
                &[&schema, &table, &files_done],
            )
            .await?;
        Ok(())
    }

    /// Record the highest CDC LSN the writer has committed to Iceberg.
    /// Called after each successful CDC batch.
    pub async fn update_writer_committed_lsn(
        &self,
        schema: &str,
        table: &str,
        lsn: &Lsn,
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        let lsn_str = lsn.to_string();
        client
            .execute(
                "UPDATE _pgiceberg.table_state \
                 SET writer_last_committed_lsn = $3::text::pg_lsn, \
                     updated_at = now() \
                 WHERE table_schema = $1 AND table_name = $2",
                &[&schema, &table, &lsn_str],
            )
            .await?;
        Ok(())
    }

    // ── file_queue ────────────────────────────────────────────────

    pub async fn enqueue_file(&self, params: &EnqueueFileParams<'_>) -> anyhow::Result<Uuid> {
        let client = self.pool.get().await?;
        let lsn_low_str = params.lsn_low.map(|l| l.to_string());
        let lsn_high_str = params.lsn_high.map(|l| l.to_string());
        let file_type_str = params.file_type.as_str();
        let row = client
            .query_one(
                "INSERT INTO _pgiceberg.file_queue \
                 (table_schema, table_name, file_type, file_path, \
                  lsn_low, lsn_high, row_count, partition_id, status) \
                 VALUES ($1, $2, $3, $4, $5::text::pg_lsn, $6::text::pg_lsn, $7, $8, 'pending') \
                 RETURNING file_id",
                &[
                    &params.schema,
                    &params.table,
                    &file_type_str,
                    &params.file_path,
                    &lsn_low_str,
                    &lsn_high_str,
                    &params.row_count,
                    &params.partition_id,
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Atomically enqueue a CDC file AND update the flushed LSN in one transaction.
    pub async fn enqueue_cdc_file_and_update_lsn(
        &self,
        params: &EnqueueCdcFileParams<'_>,
    ) -> anyhow::Result<()> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let lsn_low_str = params.lsn_low.to_string();
        let lsn_high_str = params.lsn_high.to_string();
        txn.execute(
            "INSERT INTO _pgiceberg.file_queue \
             (table_schema, table_name, file_type, file_path, \
              lsn_low, lsn_high, row_count, status) \
             VALUES ($1, $2, 'cdc_mixed', $3, $4::text::pg_lsn, $5::text::pg_lsn, $6, 'pending')",
            &[
                &params.schema,
                &params.table,
                &params.file_path,
                &lsn_low_str,
                &lsn_high_str,
                &params.row_count,
            ],
        )
        .await?;
        txn.execute(
            "UPDATE _pgiceberg.replication_state \
             SET last_flushed_lsn = $2::text::pg_lsn, updated_at = now() \
             WHERE slot_name = $1",
            &[&params.slot_name, &lsn_high_str],
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    /// Claim the next batch of pending files for the oldest-pending table.
    /// Returns files in correct order: backfill files first, then CDC by creation time.
    pub async fn claim_next_batch(&self, max_files: i64) -> anyhow::Result<Vec<FileQueueEntry>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "WITH next_table AS ( \
                   SELECT table_schema, table_name \
                   FROM _pgiceberg.file_queue fq \
                   JOIN _pgiceberg.table_state ts USING (table_schema, table_name) \
                   WHERE fq.status = 'pending' \
                     AND ( \
                       (fq.file_type = 'backfill' AND ts.phase IN ('backfilling', 'backfill_complete')) \
                       OR \
                       (fq.file_type != 'backfill' AND ts.phase = 'streaming' \
                        AND NOT EXISTS ( \
                          SELECT 1 FROM _pgiceberg.file_queue bf \
                          WHERE bf.table_schema = fq.table_schema \
                            AND bf.table_name = fq.table_name \
                            AND bf.file_type = 'backfill' \
                            AND bf.status NOT IN ('completed', 'deleted', 'failed') \
                        ) \
                        AND NOT EXISTS ( \
                          SELECT 1 FROM _pgiceberg.ddl_events de \
                          WHERE de.target_schema = fq.table_schema \
                            AND de.target_table = fq.table_name \
                            AND de.status IN ('pending', 'applying', 'failed') \
                            AND (de.ddl_lsn IS NULL OR de.ddl_lsn < fq.lsn_low) \
                        )) \
                     ) \
                   ORDER BY fq.created_at \
                   LIMIT 1 \
                 ) \
                 UPDATE _pgiceberg.file_queue fq \
                 SET status = 'processing', processing_started = now() \
                 FROM next_table nt \
                 WHERE fq.table_schema = nt.table_schema \
                   AND fq.table_name = nt.table_name \
                   AND fq.status = 'pending' \
                   AND fq.file_id IN ( \
                     SELECT file_id FROM _pgiceberg.file_queue sub \
                     WHERE sub.table_schema = nt.table_schema \
                       AND sub.table_name = nt.table_name \
                       AND sub.status = 'pending' \
                     ORDER BY \
                       CASE sub.file_type WHEN 'backfill' THEN 0 ELSE 1 END, \
                       COALESCE(sub.partition_id, 0), \
                       sub.created_at \
                     LIMIT $1 \
                   ) \
                 RETURNING fq.file_id, fq.table_schema, fq.table_name, \
                   fq.file_type, fq.file_path, fq.lsn_low::text, fq.lsn_high::text, \
                   fq.row_count, fq.partition_id, fq.status, fq.created_at, \
                   fq.retry_count, fq.error_message",
                &[&max_files],
            )
            .await?;
        Ok(rows.into_iter().map(Self::row_to_file_entry).collect())
    }

    pub async fn mark_files_completed(&self, file_ids: &[Uuid]) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "UPDATE _pgiceberg.file_queue \
                 SET status = 'completed', completed_at = now() \
                 WHERE file_id = ANY($1)",
                &[&file_ids],
            )
            .await?;
        Ok(())
    }

    pub async fn mark_files_failed(&self, file_ids: &[Uuid], error: &str) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "UPDATE _pgiceberg.file_queue \
                 SET status = 'pending', \
                     retry_count = retry_count + 1, \
                     error_message = $2, \
                     processing_started = NULL \
                 WHERE file_id = ANY($1)",
                &[&file_ids, &error],
            )
            .await?;
        Ok(())
    }

    /// Mark files as permanently failed (max retries exceeded).
    /// These files will not be retried.
    pub async fn mark_files_permanently_failed(
        &self,
        file_ids: &[Uuid],
        error: &str,
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "UPDATE _pgiceberg.file_queue \
                 SET status = 'failed', \
                     error_message = $2, \
                     completed_at = now(), \
                     processing_started = NULL \
                 WHERE file_id = ANY($1)",
                &[&file_ids, &error],
            )
            .await?;
        Ok(())
    }

    /// On startup recovery: reclaim files stuck in 'processing' for > 10 min.
    pub async fn reclaim_stale_processing(&self) -> anyhow::Result<u64> {
        let client = self.pool.get().await?;
        let count = client
            .execute(
                "UPDATE _pgiceberg.file_queue \
                 SET status = 'pending', retry_count = retry_count + 1, \
                     processing_started = NULL \
                 WHERE status = 'processing' \
                   AND processing_started < now() - interval '10 minutes'",
                &[],
            )
            .await?;
        Ok(count)
    }

    /// Cleanup completed files older than the configured retention.
    pub async fn cleanup_completed(&self, retention_hours: u32) -> anyhow::Result<Vec<String>> {
        let client = self.pool.get().await?;
        let hours = retention_hours as f64;
        let rows = client
            .query(
                "UPDATE _pgiceberg.file_queue \
                 SET status = 'deleted' \
                 WHERE status = 'completed' \
                   AND completed_at < now() - make_interval(hours => $1) \
                 RETURNING file_path",
                &[&hours],
            )
            .await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    /// Delete replication state for a slot (used during re-bootstrap).
    pub async fn delete_replication_state(&self, slot_name: &str) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "DELETE FROM _pgiceberg.replication_state WHERE slot_name = $1",
                &[&slot_name],
            )
            .await?;
        Ok(())
    }

    /// Reset tables for re-bootstrap after a slot invalidation.
    ///
    /// Tables mid-backfill are reset to pending (no Iceberg data to worry about).
    /// Tables that were streaming are also reset to pending but flagged with
    /// `needs_resync = true` — the Iceberg Writer will drop and recreate the
    /// Iceberg table before processing backfill files, ensuring no silent data
    /// loss from the gap in WAL continuity.
    pub async fn reset_tables_for_rebootstrap(&self) -> anyhow::Result<()> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;

        // Reset backfilling and backfill_complete tables to pending (clean slate)
        txn.execute(
            "UPDATE _pgiceberg.table_state \
             SET phase = 'pending', \
                 backfill_done_partitions = 0, \
                 backfill_snapshot_name = NULL, \
                 writer_backfill_files_done = 0, \
                 updated_at = now() \
             WHERE phase IN ('backfilling', 'backfill_complete')",
            &[],
        )
        .await?;

        // Reset streaming tables to pending AND flag for resync.
        // These tables have data in Iceberg but the WAL gap means changes
        // were lost.  The writer must drop + recreate the Iceberg table.
        txn.execute(
            "UPDATE _pgiceberg.table_state \
             SET phase = 'pending', \
                 backfill_done_partitions = 0, \
                 backfill_snapshot_name = NULL, \
                 writer_backfill_files_done = 0, \
                 writer_last_committed_lsn = NULL, \
                 streaming_since = NULL, \
                 needs_resync = TRUE, \
                 updated_at = now() \
             WHERE phase = 'streaming'",
            &[],
        )
        .await?;

        // Delete pending/processing backfill file queue entries (stale)
        txn.execute(
            "DELETE FROM _pgiceberg.file_queue \
             WHERE file_type = 'backfill' AND status IN ('pending', 'processing')",
            &[],
        )
        .await?;

        // Delete pending/processing CDC file queue entries — the LSN range
        // is discontinuous after slot recreation so these are meaningless.
        txn.execute(
            "DELETE FROM _pgiceberg.file_queue \
             WHERE file_type = 'cdc_mixed' AND status IN ('pending', 'processing')",
            &[],
        )
        .await?;

        txn.commit().await?;
        Ok(())
    }

    /// Return all tracked file paths (not yet deleted) for orphan detection.
    pub async fn get_all_tracked_file_paths(&self) -> anyhow::Result<Vec<String>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT file_path FROM _pgiceberg.file_queue WHERE status != 'deleted'",
                &[],
            )
            .await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    // ── heartbeat ─────────────────────────────────────────────────

    /// Emit a lightweight transactional logical message into WAL.
    /// This generates a Begin/Message/Commit sequence that flows through the
    /// replication slot, allowing the consumer to advance the confirmed LSN
    /// and PostgreSQL to reclaim old WAL segments.
    pub async fn tick_heartbeat(&self) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "SELECT pg_logical_emit_message(true, 'walrus_heartbeat', 'tick')",
                &[],
            )
            .await?;
        Ok(())
    }

    // ── ddl_events ────────────────────────────────────────────────

    pub async fn insert_ddl_event(
        &self,
        source_txn: Option<&str>,
        ddl_tag: &str,
        target_schema: &str,
        target_table: &str,
        ddl_sql: &str,
        ddl_lsn: Option<&str>,
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "INSERT INTO _pgiceberg.ddl_events \
                 (source_txn, ddl_tag, target_schema, target_table, ddl_sql, ddl_lsn) \
                 VALUES ($1, $2, $3, $4, $5, $6::pg_lsn)",
                &[
                    &source_txn,
                    &ddl_tag,
                    &target_schema,
                    &target_table,
                    &ddl_sql,
                    &ddl_lsn,
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn get_pending_ddl_events(&self) -> anyhow::Result<Vec<DdlEvent>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT event_id, source_txn, ddl_tag, target_schema, target_table, ddl_sql, \
                 ddl_lsn::text, status \
                 FROM _pgiceberg.ddl_events \
                 WHERE status = 'pending' \
                 ORDER BY event_id",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| DdlEvent {
                event_id: r.get(0),
                source_txn: r.get(1),
                ddl_tag: r.get(2),
                target_schema: r.get(3),
                target_table: r.get(4),
                ddl_sql: r.get(5),
                ddl_lsn: r.get(6),
                status: r.get::<_, String>(7).parse().unwrap(),
            })
            .collect())
    }

    pub async fn mark_ddl_applied(&self, event_id: i64) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        // Update the event status and clear any DDL block on the table
        let row = client
            .query_opt(
                "UPDATE _pgiceberg.ddl_events \
                 SET status = 'applied', applied_to_iceberg = TRUE, applied_at = now() \
                 WHERE event_id = $1 \
                 RETURNING target_schema, target_table",
                &[&event_id],
            )
            .await?;
        if let Some(r) = row {
            let schema: String = r.get(0);
            let table: String = r.get(1);
            client
                .execute(
                    "UPDATE _pgiceberg.table_state \
                     SET ddl_blocked_since = NULL, updated_at = now() \
                     WHERE table_schema = $1 AND table_name = $2 \
                       AND ddl_blocked_since IS NOT NULL",
                    &[&schema, &table],
                )
                .await?;
        }
        Ok(())
    }

    /// Atomically claim a DDL event for processing (crash guard).
    /// Returns true if the event was claimed, false if already claimed.
    pub async fn claim_ddl_event(&self, event_id: i64) -> anyhow::Result<bool> {
        let client = self.pool.get().await?;
        let result = client
            .execute(
                "UPDATE _pgiceberg.ddl_events \
                 SET status = 'applying' \
                 WHERE event_id = $1 AND status = 'pending'",
                &[&event_id],
            )
            .await?;
        Ok(result > 0)
    }

    /// Mark a DDL event as failed and block the table's CDC pipeline.
    pub async fn mark_ddl_failed(&self, event_id: i64, error_msg: &str) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "UPDATE _pgiceberg.ddl_events \
                 SET status = 'failed', error_message = $2 \
                 WHERE event_id = $1 \
                 RETURNING target_schema, target_table",
                &[&event_id, &error_msg],
            )
            .await?;
        if let Some(r) = row {
            let schema: String = r.get(0);
            let table: String = r.get(1);
            client
                .execute(
                    "UPDATE _pgiceberg.table_state \
                     SET ddl_blocked_since = now(), updated_at = now() \
                     WHERE table_schema = $1 AND table_name = $2",
                    &[&schema, &table],
                )
                .await?;
        }
        Ok(())
    }

    /// Mark a DDL event as skipped (irrelevant to Iceberg schema).
    pub async fn mark_ddl_skipped(&self, event_id: i64) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "UPDATE _pgiceberg.ddl_events \
                 SET status = 'skipped', applied_to_iceberg = TRUE, applied_at = now() \
                 WHERE event_id = $1",
                &[&event_id],
            )
            .await?;
        Ok(())
    }

    /// Reclaim DDL events stuck in 'applying' for > 10 minutes (crash recovery).
    pub async fn reclaim_stale_ddl_events(&self) -> anyhow::Result<u64> {
        let client = self.pool.get().await?;
        let count = client
            .execute(
                "UPDATE _pgiceberg.ddl_events \
                 SET status = 'pending' \
                 WHERE status = 'applying' \
                   AND applied_at IS NULL \
                   AND captured_at < now() - interval '10 minutes'",
                &[],
            )
            .await?;
        Ok(count)
    }

    // ── Primary key discovery (for stateless recovery) ────────────

    /// Discover primary key columns from pg_index.  Called on startup so the
    /// service doesn't need to persist PK info — it re-discovers from the
    /// source catalog each time.
    pub async fn discover_primary_keys(
        &self,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<Vec<String>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT a.attname \
                 FROM pg_index i \
                 JOIN pg_attribute a ON a.attrelid = i.indrelid \
                   AND a.attnum = ANY(i.indkey) \
                 WHERE i.indrelid = ($1 || '.' || $2)::regclass \
                   AND i.indisprimary \
                 ORDER BY array_position(i.indkey, a.attnum)",
                &[&schema, &table],
            )
            .await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    /// Discover column metadata from information_schema.  Used on startup to
    /// rebuild Arrow schemas without persisting them.
    pub async fn discover_columns(
        &self,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<Vec<(String, String, bool)>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT column_name, udt_name, is_nullable::text = 'YES' as nullable \
                 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 \
                 ORDER BY ordinal_position",
                &[&schema, &table],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect())
    }

    fn row_to_file_entry(r: tokio_postgres::Row) -> FileQueueEntry {
        FileQueueEntry {
            file_id: r.get(0),
            table_schema: r.get(1),
            table_name: r.get(2),
            file_type: r.get::<_, String>(3).parse().unwrap(),
            file_path: r.get(4),
            lsn_low: r
                .get::<_, Option<String>>(5)
                .and_then(|s| Lsn::parse(&s).ok()),
            lsn_high: r
                .get::<_, Option<String>>(6)
                .and_then(|s| Lsn::parse(&s).ok()),
            row_count: r.get(7),
            partition_id: r.get(8),
            status: r.get::<_, String>(9).parse().unwrap(),
            created_at: r.get(10),
            retry_count: r.get(11),
            error_message: r.get(12),
        }
    }
}

const BOOTSTRAP_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS _pgiceberg;

CREATE TABLE IF NOT EXISTS _pgiceberg.replication_state (
    slot_name           TEXT PRIMARY KEY,
    publication_name    TEXT NOT NULL,
    consistent_point    PG_LSN,
    snapshot_name       TEXT,
    last_flushed_lsn    PG_LSN,
    last_acked_lsn      PG_LSN,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS _pgiceberg.table_state (
    table_schema        TEXT NOT NULL,
    table_name          TEXT NOT NULL,
    phase               TEXT NOT NULL DEFAULT 'pending',
    -- WAL Capture progress: export from source → staging Parquet
    backfill_total_partitions   INT,
    backfill_done_partitions    INT DEFAULT 0,
    backfill_snapshot_name      TEXT,
    -- Iceberg Writer progress: staging Parquet → Iceberg table
    writer_backfill_files_done  INT DEFAULT 0,
    writer_last_committed_lsn   PG_LSN,
    streaming_since             TIMESTAMPTZ,
    -- Shared
    iceberg_schema_version      INT DEFAULT 0,
    primary_key_columns         TEXT[] DEFAULT '{}',
    needs_resync                BOOLEAN DEFAULT FALSE,
    updated_at                  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (table_schema, table_name)
);

CREATE TABLE IF NOT EXISTS _pgiceberg.file_queue (
    file_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_schema        TEXT NOT NULL,
    table_name          TEXT NOT NULL,
    file_type           TEXT NOT NULL,
    file_path           TEXT NOT NULL,
    lsn_low             PG_LSN,
    lsn_high            PG_LSN,
    row_count           BIGINT NOT NULL,
    partition_id        INT,
    status              TEXT NOT NULL DEFAULT 'pending',
    created_at          TIMESTAMPTZ DEFAULT now(),
    processing_started  TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    error_message       TEXT,
    retry_count         INT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_file_queue_pending
    ON _pgiceberg.file_queue (table_schema, table_name, created_at)
    WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS _pgiceberg.ddl_events (
    event_id            BIGSERIAL PRIMARY KEY,
    source_txn          TEXT,
    ddl_tag             TEXT NOT NULL,
    target_schema       TEXT NOT NULL,
    target_table        TEXT NOT NULL DEFAULT '',
    ddl_sql             TEXT NOT NULL,
    ddl_lsn             PG_LSN,
    status              TEXT NOT NULL DEFAULT 'pending',
    captured_at         TIMESTAMPTZ DEFAULT now(),
    applied_to_iceberg  BOOLEAN DEFAULT FALSE,
    applied_at          TIMESTAMPTZ,
    error_message       TEXT
);

-- Idempotent migrations for existing installations
ALTER TABLE _pgiceberg.ddl_events ADD COLUMN IF NOT EXISTS target_table TEXT NOT NULL DEFAULT '';
ALTER TABLE _pgiceberg.table_state ADD COLUMN IF NOT EXISTS writer_backfill_files_done INT DEFAULT 0;
ALTER TABLE _pgiceberg.table_state ADD COLUMN IF NOT EXISTS writer_last_committed_lsn PG_LSN;
ALTER TABLE _pgiceberg.table_state ADD COLUMN IF NOT EXISTS streaming_since TIMESTAMPTZ;
ALTER TABLE _pgiceberg.table_state ADD COLUMN IF NOT EXISTS needs_resync BOOLEAN DEFAULT FALSE;

-- DDL lifecycle migrations
ALTER TABLE _pgiceberg.ddl_events ADD COLUMN IF NOT EXISTS ddl_lsn PG_LSN;
ALTER TABLE _pgiceberg.ddl_events ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending';
-- Backfill status from legacy boolean column
UPDATE _pgiceberg.ddl_events SET status = 'applied' WHERE applied_to_iceberg = TRUE AND status = 'pending';
-- Add ddl_blocked_since to table_state
ALTER TABLE _pgiceberg.table_state ADD COLUMN IF NOT EXISTS ddl_blocked_since TIMESTAMPTZ;
"#;
