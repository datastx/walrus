use crate::decoder::*;
use crate::parquet_writer::write_cdc_records_to_parquet;
use pgiceberg_common::config::{SourceConfig, WalCaptureConfig};
use pgiceberg_common::metadata::{EnqueueCdcFileParams, MetadataStore};
use pgiceberg_common::models::*;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;
use tokio::sync::watch;
use uuid::Uuid;

use pgwire_replication::{ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};

pub struct CdcLoopParams<'a> {
    pub source: &'a SourceConfig,
    pub start_lsn: Lsn,
    pub config: &'a WalCaptureConfig,
    pub metadata: &'a MetadataStore,
    pub staging_root: &'a Path,
}

/// Per-table buffer of CDC records waiting to be flushed.
struct TableBuffer {
    records: Vec<CdcRecord>,
    total_bytes: usize,
    min_lsn: Lsn,
    max_lsn: Lsn,
}

impl TableBuffer {
    fn new() -> Self {
        Self {
            records: Vec::new(),
            total_bytes: 0,
            min_lsn: Lsn(u64::MAX),
            max_lsn: Lsn(0),
        }
    }

    fn push(&mut self, record: CdcRecord) {
        if record.commit_lsn < self.min_lsn {
            self.min_lsn = record.commit_lsn;
        }
        if record.commit_lsn > self.max_lsn {
            self.max_lsn = record.commit_lsn;
        }
        self.total_bytes += record.estimated_bytes;
        self.records.push(record);
    }

    fn clear(&mut self) {
        self.records.clear();
        self.total_bytes = 0;
        self.min_lsn = Lsn(u64::MAX);
        self.max_lsn = Lsn(0);
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

struct TxnState {
    xid: u32,
    records: Vec<CdcRecord>,
}

struct CdcState<'a> {
    relation_cache: RelationCache,
    table_buffers: HashMap<String, TableBuffer>,
    current_txn: Option<TxnState>,
    last_flush: Instant,
    total_buffered_rows: usize,
    total_buffered_bytes: usize,
    metadata: &'a MetadataStore,
    slot_name: &'a str,
    staging_root: &'a Path,
    config: &'a WalCaptureConfig,
}

impl<'a> CdcState<'a> {
    fn new(
        metadata: &'a MetadataStore,
        slot_name: &'a str,
        staging_root: &'a Path,
        config: &'a WalCaptureConfig,
    ) -> Self {
        Self {
            relation_cache: RelationCache::new(),
            table_buffers: HashMap::new(),
            current_txn: None,
            last_flush: Instant::now(),
            total_buffered_rows: 0,
            total_buffered_bytes: 0,
            metadata,
            slot_name,
            staging_root,
            config,
        }
    }

    fn handle_begin(&mut self, xid: u32) {
        self.current_txn = Some(TxnState {
            xid,
            records: Vec::new(),
        });
    }

    fn handle_commit(&mut self, end_lsn: Lsn, commit_ts: i64) {
        if let Some(txn) = self.current_txn.take() {
            tracing::trace!(xid = txn.xid, records = txn.records.len(), "Commit");
            for mut record in txn.records {
                record.commit_lsn = end_lsn;
                record.commit_ts = commit_ts;
                let key = format!("{}.{}", record.table_schema, record.table_name);
                self.total_buffered_rows += 1;
                self.total_buffered_bytes += record.estimated_bytes;
                self.table_buffers
                    .entry(key)
                    .or_insert_with(TableBuffer::new)
                    .push(record);
            }
        }
    }

    fn handle_relation(
        &mut self,
        oid: u32,
        schema: String,
        name: String,
        columns: Vec<RelationColumn>,
    ) {
        self.relation_cache.update(oid, schema, name, columns);
    }

    async fn handle_insert(&mut self, relation_oid: u32, tuple: &TupleData) -> anyhow::Result<()> {
        let Some(ref mut txn) = self.current_txn else {
            return Ok(());
        };
        let Some(rel) = self.relation_cache.get(relation_oid) else {
            return Ok(());
        };
        if rel.name == "awsdms_ddl_audit" {
            handle_ddl_insert(tuple, rel, self.metadata).await?;
        } else {
            let record = tuple_to_cdc_record(rel, CdcOp::Insert, tuple, Lsn::ZERO, 0);
            txn.records.push(record);
        }
        Ok(())
    }

    fn handle_update(&mut self, relation_oid: u32, new_tuple: &TupleData) {
        let Some(ref mut txn) = self.current_txn else {
            return;
        };
        let Some(rel) = self.relation_cache.get(relation_oid) else {
            return;
        };
        let record = tuple_to_cdc_record(rel, CdcOp::Update, new_tuple, Lsn::ZERO, 0);
        txn.records.push(record);
    }

    fn handle_delete(&mut self, relation_oid: u32, old_tuple: &TupleData) {
        let Some(ref mut txn) = self.current_txn else {
            return;
        };
        let Some(rel) = self.relation_cache.get(relation_oid) else {
            return;
        };
        let record = tuple_to_cdc_record(rel, CdcOp::Delete, old_tuple, Lsn::ZERO, 0);
        txn.records.push(record);
    }

    fn should_flush(&self) -> bool {
        let rows = self.config.max_batch_rows > 0
            && self.total_buffered_rows >= self.config.max_batch_rows;
        let bytes = self.config.max_batch_bytes > 0
            && self.total_buffered_bytes >= self.config.max_batch_bytes;
        rows || bytes
    }

    fn should_flush_timer(&self) -> bool {
        self.config.flush_interval_seconds > 0
            && self.last_flush.elapsed().as_secs() >= self.config.flush_interval_seconds
            && !self.table_buffers.values().all(|b| b.is_empty())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        flush_all_buffers(
            &mut self.table_buffers,
            &self.relation_cache,
            self.metadata,
            self.slot_name,
            self.staging_root,
        )
        .await?;
        self.total_buffered_rows = 0;
        self.total_buffered_bytes = 0;
        self.last_flush = Instant::now();
        Ok(())
    }
}

/// Runs the CDC streaming loop.
///
/// Starts reading WAL immediately from the replication slot's consistent_point.
/// Stages changes for ALL tables (even those still backfilling) into Parquet
/// files on disk, then enqueues them in `file_queue`.  Service 2 is responsible
/// for not processing CDC files for a table until its backfill is done.
///
/// Flush policy (configurable, any threshold triggers a flush):
///   - `max_batch_rows`: total row count across all table buffers
///   - `max_batch_bytes`: estimated memory usage of buffered records
///   - `flush_interval_seconds`: wall-clock time since last flush
///
/// Recovery: on restart, reads `last_flushed_lsn` from metadata and resumes
/// from there.  Any duplicate WAL re-reads produce idempotent results because
/// Service 2 deduplicates on PK.
pub async fn run_cdc_loop(
    params: &CdcLoopParams<'_>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let source = params.source;
    let start_lsn = params.start_lsn;
    let slot_name = &source.slot_name;

    let repl_lsn = pgwire_replication::Lsn::parse(&start_lsn.to_string())
        .map_err(|e| anyhow::anyhow!("Invalid start LSN: {}", e))?;

    let repl_config = ReplicationConfig {
        host: source.host.clone(),
        port: source.port,
        user: source.user.clone(),
        password: source.password(),
        database: source.database.clone(),
        tls: TlsConfig::disabled(),
        slot: source.slot_name.clone(),
        publication: source.publication_name.clone(),
        start_lsn: repl_lsn,
        stop_at_lsn: None,
        status_interval: std::time::Duration::from_secs(params.config.idle_timeout_seconds),
        idle_wakeup_interval: std::time::Duration::from_secs(params.config.idle_timeout_seconds),
        buffer_events: 8192,
    };

    tracing::info!(
        start_lsn = %start_lsn,
        slot = slot_name,
        "Starting CDC loop"
    );

    let mut client = ReplicationClient::connect(repl_config).await?;
    let mut state = CdcState::new(
        params.metadata,
        slot_name,
        params.staging_root,
        params.config,
    );

    loop {
        if *shutdown_rx.borrow() {
            tracing::info!("Shutdown signal received — flushing remaining buffers");
            state.flush().await?;
            client.stop();
            break;
        }

        if state.should_flush_timer() {
            state.flush().await?;
        }

        let event = tokio::select! {
            ev = client.recv() => ev,
            _ = shutdown_rx.changed() => continue,
        };

        match event {
            Ok(Some(ReplicationEvent::XLogData { wal_end, data, .. })) => {
                let msg = decode_pgoutput(&data)?;

                match msg {
                    PgOutputMessage::Begin { xid, .. } => state.handle_begin(xid),
                    PgOutputMessage::Commit {
                        end_lsn, commit_ts, ..
                    } => {
                        state.handle_commit(end_lsn, commit_ts);
                        client.update_applied_lsn(
                            pgwire_replication::Lsn::parse(&Lsn(wal_end.into()).to_string())
                                .unwrap_or(repl_lsn),
                        );
                    }
                    PgOutputMessage::Relation {
                        oid,
                        schema,
                        name,
                        columns,
                        ..
                    } => state.handle_relation(oid, schema, name, columns),
                    PgOutputMessage::Insert {
                        relation_oid,
                        tuple,
                    } => state.handle_insert(relation_oid, &tuple).await?,
                    PgOutputMessage::Update {
                        relation_oid,
                        new_tuple,
                        ..
                    } => state.handle_update(relation_oid, &new_tuple),
                    PgOutputMessage::Delete {
                        relation_oid,
                        old_tuple,
                    } => state.handle_delete(relation_oid, &old_tuple),
                    _ => {}
                }

                if state.should_flush() {
                    tracing::debug!(
                        rows = state.total_buffered_rows,
                        bytes = state.total_buffered_bytes,
                        "Flush threshold reached"
                    );
                    state.flush().await?;
                }
            }
            Ok(Some(ReplicationEvent::KeepAlive { .. })) => {}
            Ok(Some(ReplicationEvent::StoppedAt { .. })) => {
                tracing::info!("Replication stream ended");
                break;
            }
            Ok(Some(_)) => {}
            Ok(None) => {
                tracing::info!("Replication stream closed");
                break;
            }
            Err(e) => {
                tracing::error!("Replication error: {}", e);
                return Err(e.into());
            }
        }
    }

    Ok(())
}

fn tuple_to_cdc_record(
    rel: &CachedRelation,
    op: CdcOp,
    tuple: &TupleData,
    commit_lsn: Lsn,
    commit_ts: i64,
) -> CdcRecord {
    let mut estimated_bytes = 64;
    let columns: Vec<CdcColumn> = rel
        .columns
        .iter()
        .zip(tuple.columns.iter())
        .map(|(rel_col, tuple_col)| {
            let value = match tuple_col {
                TupleColumn::Text(data) => {
                    estimated_bytes += data.len();
                    Some(data.clone())
                }
                TupleColumn::Binary(data) => {
                    estimated_bytes += data.len();
                    Some(data.clone())
                }
                TupleColumn::Null | TupleColumn::UnchangedToast => None,
            };
            CdcColumn {
                name: rel_col.name.clone(),
                type_oid: rel_col.type_oid,
                value,
            }
        })
        .collect();

    CdcRecord {
        table_schema: rel.schema.clone(),
        table_name: rel.name.clone(),
        op,
        columns,
        estimated_bytes,
        commit_lsn,
        commit_ts,
    }
}

async fn handle_ddl_insert(
    tuple: &TupleData,
    _rel: &CachedRelation,
    metadata: &MetadataStore,
) -> anyhow::Result<()> {
    let get_text = |idx: usize| -> Option<String> {
        tuple.columns.get(idx).and_then(|c| match c {
            TupleColumn::Text(data) => String::from_utf8(data.clone()).ok(),
            _ => None,
        })
    };

    let txn = get_text(3);
    let tag = get_text(4).unwrap_or_default();
    let table_name = get_text(6).unwrap_or_default();
    let schema = get_text(7).unwrap_or_else(|| "public".to_string());
    let ddl_sql = get_text(8).unwrap_or_default();

    if !tag.is_empty() {
        tracing::info!(tag = %tag, schema = %schema, table = %table_name, "Captured DDL event from WAL");
        metadata
            .insert_ddl_event(txn.as_deref(), &tag, &schema, &table_name, &ddl_sql)
            .await?;
    }
    Ok(())
}

async fn flush_all_buffers(
    table_buffers: &mut HashMap<String, TableBuffer>,
    relation_cache: &RelationCache,
    metadata: &MetadataStore,
    slot_name: &str,
    staging_root: &Path,
) -> anyhow::Result<()> {
    for (table_key, buffer) in table_buffers.iter_mut() {
        if buffer.is_empty() {
            continue;
        }

        let parts: Vec<&str> = table_key.splitn(2, '.').collect();
        let (schema, table_name) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            ("public", parts[0])
        };

        let rel = relation_cache.find_by_name(schema, table_name);

        if let Some(rel) = rel {
            let batch_id = Uuid::new_v4();
            let dir = staging_root.join("cdc").join(table_key);
            let file_path = dir.join(format!("{}.parquet", batch_id));
            let relative_path = format!("cdc/{}/{}.parquet", table_key, batch_id);

            let records = buffer.records.clone();
            let rel = rel.clone();
            let fp = file_path.clone();
            tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                std::fs::create_dir_all(fp.parent().unwrap())?;
                write_cdc_records_to_parquet(&records, &rel, &fp)?;
                Ok(())
            })
            .await??;

            metadata
                .enqueue_cdc_file_and_update_lsn(&EnqueueCdcFileParams {
                    slot_name,
                    schema,
                    table: table_name,
                    file_path: &relative_path,
                    lsn_low: &buffer.min_lsn,
                    lsn_high: &buffer.max_lsn,
                    row_count: buffer.records.len() as i64,
                })
                .await?;

            tracing::info!(
                schema,
                table = table_name,
                records = buffer.records.len(),
                bytes = buffer.total_bytes,
                lsn_low = %buffer.min_lsn,
                lsn_high = %buffer.max_lsn,
                "Flushed CDC batch"
            );
        } else {
            tracing::warn!(
                table_key,
                records = buffer.records.len(),
                "No relation info found — skipping flush"
            );
        }

        buffer.clear();
    }

    Ok(())
}
