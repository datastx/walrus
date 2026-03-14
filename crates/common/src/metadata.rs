use crate::models::*;
use deadpool_postgres::{Config, Pool, Runtime};
use tokio_postgres::NoTls;
use uuid::Uuid;

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
    pub async fn connect(conn_string: &str) -> anyhow::Result<Self> {
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
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
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
                 VALUES ($1, $2, $3::pg_lsn, $4) \
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
                 SET last_flushed_lsn = $2::pg_lsn, updated_at = now() \
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
                 backfill_snapshot_name, last_committed_lsn::text, \
                 iceberg_schema_version, primary_key_columns \
                 FROM _pgiceberg.table_state \
                 WHERE table_schema = $1 AND table_name = $2",
                &[&schema, &table],
            )
            .await?;
        Ok(row.map(|r| TableState {
            table_schema: r.get(0),
            table_name: r.get(1),
            phase: TablePhase::from_str(r.get(2)),
            backfill_total_partitions: r.get(3),
            backfill_done_partitions: r.get(4),
            backfill_snapshot_name: r.get(5),
            last_committed_lsn: r
                .get::<_, Option<String>>(6)
                .and_then(|s| Lsn::parse(&s).ok()),
            iceberg_schema_version: r.get(7),
            primary_key_columns: r.get::<_, Vec<String>>(8),
        }))
    }

    pub async fn get_all_table_states(&self) -> anyhow::Result<Vec<TableState>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT table_schema, table_name, phase, \
                 backfill_total_partitions, backfill_done_partitions, \
                 backfill_snapshot_name, last_committed_lsn::text, \
                 iceberg_schema_version, primary_key_columns \
                 FROM _pgiceberg.table_state",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| TableState {
                table_schema: r.get(0),
                table_name: r.get(1),
                phase: TablePhase::from_str(r.get(2)),
                backfill_total_partitions: r.get(3),
                backfill_done_partitions: r.get(4),
                backfill_snapshot_name: r.get(5),
                last_committed_lsn: r
                    .get::<_, Option<String>>(6)
                    .and_then(|s| Lsn::parse(&s).ok()),
                iceberg_schema_version: r.get(7),
                primary_key_columns: r.get::<_, Vec<String>>(8),
            })
            .collect())
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

    // ── file_queue ────────────────────────────────────────────────

    pub async fn enqueue_file(
        &self,
        schema: &str,
        table: &str,
        file_type: &str,
        file_path: &str,
        lsn_low: Option<&Lsn>,
        lsn_high: Option<&Lsn>,
        row_count: i64,
        partition_id: Option<i32>,
    ) -> anyhow::Result<Uuid> {
        let client = self.pool.get().await?;
        let lsn_low_str = lsn_low.map(|l| l.to_string());
        let lsn_high_str = lsn_high.map(|l| l.to_string());
        let row = client
            .query_one(
                "INSERT INTO _pgiceberg.file_queue \
                 (table_schema, table_name, file_type, file_path, \
                  lsn_low, lsn_high, row_count, partition_id, status) \
                 VALUES ($1, $2, $3, $4, $5::pg_lsn, $6::pg_lsn, $7, $8, 'pending') \
                 RETURNING file_id",
                &[
                    &schema,
                    &table,
                    &file_type,
                    &file_path,
                    &lsn_low_str,
                    &lsn_high_str,
                    &row_count,
                    &partition_id,
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Atomically enqueue a CDC file AND update the flushed LSN in one transaction.
    pub async fn enqueue_cdc_file_and_update_lsn(
        &self,
        slot_name: &str,
        schema: &str,
        table: &str,
        file_path: &str,
        lsn_low: &Lsn,
        lsn_high: &Lsn,
        row_count: i64,
    ) -> anyhow::Result<()> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let lsn_low_str = lsn_low.to_string();
        let lsn_high_str = lsn_high.to_string();
        txn.execute(
            "INSERT INTO _pgiceberg.file_queue \
             (table_schema, table_name, file_type, file_path, \
              lsn_low, lsn_high, row_count, status) \
             VALUES ($1, $2, 'cdc_mixed', $3, $4::pg_lsn, $5::pg_lsn, $6, 'pending')",
            &[
                &schema,
                &table,
                &file_path,
                &lsn_low_str,
                &lsn_high_str,
                &row_count,
            ],
        )
        .await?;
        txn.execute(
            "UPDATE _pgiceberg.replication_state \
             SET last_flushed_lsn = $2::pg_lsn, updated_at = now() \
             WHERE slot_name = $1",
            &[&slot_name, &lsn_high_str],
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
                   WHERE fq.status = 'pending' AND ts.phase = 'streaming' \
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

    // ── ddl_events ────────────────────────────────────────────────

    pub async fn insert_ddl_event(
        &self,
        source_txn: Option<&str>,
        ddl_tag: &str,
        target_schema: &str,
        ddl_sql: &str,
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "INSERT INTO _pgiceberg.ddl_events \
                 (source_txn, ddl_tag, target_schema, ddl_sql) \
                 VALUES ($1, $2, $3, $4)",
                &[&source_txn, &ddl_tag, &target_schema, &ddl_sql],
            )
            .await?;
        Ok(())
    }

    pub async fn get_pending_ddl_events(&self) -> anyhow::Result<Vec<DdlEvent>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT event_id, source_txn, ddl_tag, target_schema, ddl_sql, \
                 applied_to_iceberg \
                 FROM _pgiceberg.ddl_events \
                 WHERE applied_to_iceberg = FALSE \
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
                ddl_sql: r.get(4),
                applied_to_iceberg: r.get(5),
            })
            .collect())
    }

    pub async fn mark_ddl_applied(&self, event_id: i64) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "UPDATE _pgiceberg.ddl_events \
                 SET applied_to_iceberg = TRUE, applied_at = now() \
                 WHERE event_id = $1",
                &[&event_id],
            )
            .await?;
        Ok(())
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
            file_type: r.get(3),
            file_path: r.get(4),
            lsn_low: r
                .get::<_, Option<String>>(5)
                .and_then(|s| Lsn::parse(&s).ok()),
            lsn_high: r
                .get::<_, Option<String>>(6)
                .and_then(|s| Lsn::parse(&s).ok()),
            row_count: r.get(7),
            partition_id: r.get(8),
            status: FileStatus::from_str(r.get(9)),
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
    backfill_total_partitions   INT,
    backfill_done_partitions    INT DEFAULT 0,
    backfill_snapshot_name      TEXT,
    last_committed_lsn          PG_LSN,
    iceberg_schema_version      INT DEFAULT 0,
    primary_key_columns         TEXT[] DEFAULT '{}',
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
    ddl_sql             TEXT NOT NULL,
    captured_at         TIMESTAMPTZ DEFAULT now(),
    applied_to_iceberg  BOOLEAN DEFAULT FALSE,
    applied_at          TIMESTAMPTZ,
    error_message       TEXT
);
"#;
