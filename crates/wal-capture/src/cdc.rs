use crate::decoder::*;
use crate::parquet_writer::write_cdc_records_to_parquet;
use pgiceberg_common::config::WalCaptureConfig;
use pgiceberg_common::metadata::MetadataStore;
use pgiceberg_common::models::*;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;
use tokio::sync::watch;
use uuid::Uuid;

use pgwire_replication::{ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};

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
    source_host: &str,
    source_port: u16,
    source_user: &str,
    source_password: &str,
    source_database: &str,
    slot_name: &str,
    publication_name: &str,
    start_lsn: Lsn,
    config: &WalCaptureConfig,
    metadata: &MetadataStore,
    staging_root: &Path,
    mut shutdown_rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let repl_lsn = pgwire_replication::Lsn::parse(&start_lsn.to_string())
        .map_err(|e| anyhow::anyhow!("Invalid start LSN: {}", e))?;

    let repl_config = ReplicationConfig {
        host: source_host.into(),
        port: source_port,
        user: source_user.into(),
        password: source_password.into(),
        database: source_database.into(),
        tls: TlsConfig::disabled(),
        slot: slot_name.into(),
        publication: publication_name.into(),
        start_lsn: repl_lsn,
        stop_at_lsn: None,
        status_interval: std::time::Duration::from_secs(config.idle_timeout_seconds),
        idle_wakeup_interval: std::time::Duration::from_secs(config.idle_timeout_seconds),
        buffer_events: 8192,
    };

    tracing::info!(
        start_lsn = %start_lsn,
        slot = slot_name,
        "Starting CDC loop"
    );

    let mut client = ReplicationClient::connect(repl_config).await?;
    let mut relation_cache = RelationCache::new();
    let mut table_buffers: HashMap<String, TableBuffer> = HashMap::new();
    let mut current_txn: Option<TxnState> = None;
    let mut last_flush = Instant::now();
    let mut total_buffered_rows = 0usize;
    let mut total_buffered_bytes = 0usize;

    loop {
        // Check shutdown signal
        if *shutdown_rx.borrow() {
            tracing::info!("Shutdown signal received — flushing remaining buffers");
            flush_all_buffers(
                &mut table_buffers,
                &relation_cache,
                metadata,
                slot_name,
                staging_root,
            )
            .await?;
            client.stop();
            break;
        }

        // Check time-based flush
        let elapsed = last_flush.elapsed().as_secs();
        if config.flush_interval_seconds > 0
            && elapsed >= config.flush_interval_seconds
            && !all_empty(&table_buffers)
        {
            flush_all_buffers(
                &mut table_buffers,
                &relation_cache,
                metadata,
                slot_name,
                staging_root,
            )
            .await?;
            total_buffered_rows = 0;
            total_buffered_bytes = 0;
            last_flush = Instant::now();
        }

        let event = tokio::select! {
            ev = client.recv() => ev,
            _ = shutdown_rx.changed() => continue,
        };

        match event {
            Ok(Some(ReplicationEvent::XLogData { wal_end, data, .. })) => {
                let msg = decode_pgoutput(&data)?;

                match msg {
                    PgOutputMessage::Begin {
                        final_lsn,
                        commit_ts,
                        xid,
                    } => {
                        current_txn = Some(TxnState {
                            xid,
                            commit_lsn: final_lsn,
                            commit_ts,
                            records: Vec::new(),
                        });
                    }
                    PgOutputMessage::Commit {
                        end_lsn, commit_ts, ..
                    } => {
                        if let Some(txn) = current_txn.take() {
                            for mut record in txn.records {
                                record.commit_lsn = end_lsn;
                                record.commit_ts = commit_ts;
                                let key = format!("{}.{}", record.table_schema, record.table_name);
                                total_buffered_rows += 1;
                                total_buffered_bytes += record.estimated_bytes;
                                table_buffers
                                    .entry(key)
                                    .or_insert_with(TableBuffer::new)
                                    .push(record);
                            }
                        }
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
                    } => {
                        // Check if this is the DDL audit table
                        relation_cache.update(oid, schema, name, columns);
                    }
                    PgOutputMessage::Insert {
                        relation_oid,
                        tuple,
                    } => {
                        if let Some(ref mut txn) = current_txn {
                            if let Some(rel) = relation_cache.get(relation_oid) {
                                if rel.name == "awsdms_ddl_audit" {
                                    handle_ddl_insert(&tuple, rel, metadata).await?;
                                } else {
                                    let record = tuple_to_cdc_record(
                                        rel,
                                        CdcOp::Insert,
                                        &tuple,
                                        Lsn::ZERO,
                                        0,
                                    );
                                    txn.records.push(record);
                                }
                            }
                        }
                    }
                    PgOutputMessage::Update {
                        relation_oid,
                        new_tuple,
                        ..
                    } => {
                        if let Some(ref mut txn) = current_txn {
                            if let Some(rel) = relation_cache.get(relation_oid) {
                                let record = tuple_to_cdc_record(
                                    rel,
                                    CdcOp::Update,
                                    &new_tuple,
                                    Lsn::ZERO,
                                    0,
                                );
                                txn.records.push(record);
                            }
                        }
                    }
                    PgOutputMessage::Delete {
                        relation_oid,
                        old_tuple,
                    } => {
                        if let Some(ref mut txn) = current_txn {
                            if let Some(rel) = relation_cache.get(relation_oid) {
                                let record = tuple_to_cdc_record(
                                    rel,
                                    CdcOp::Delete,
                                    &old_tuple,
                                    Lsn::ZERO,
                                    0,
                                );
                                txn.records.push(record);
                            }
                        }
                    }
                    _ => {}
                }

                // Check row/byte flush thresholds
                let should_flush_rows =
                    config.max_batch_rows > 0 && total_buffered_rows >= config.max_batch_rows;
                let should_flush_bytes =
                    config.max_batch_bytes > 0 && total_buffered_bytes >= config.max_batch_bytes;

                if should_flush_rows || should_flush_bytes {
                    tracing::debug!(
                        rows = total_buffered_rows,
                        bytes = total_buffered_bytes,
                        trigger = if should_flush_rows { "rows" } else { "bytes" },
                        "Flush threshold reached"
                    );
                    flush_all_buffers(
                        &mut table_buffers,
                        &relation_cache,
                        metadata,
                        slot_name,
                        staging_root,
                    )
                    .await?;
                    total_buffered_rows = 0;
                    total_buffered_bytes = 0;
                    last_flush = Instant::now();
                }
            }
            Ok(Some(ReplicationEvent::KeepAlive { .. })) => {
                // Nothing to do — pgwire-replication handles standby status
            }
            Ok(Some(ReplicationEvent::StoppedAt { .. })) => {
                tracing::info!("Replication stream ended");
                break;
            }
            Ok(Some(_)) => {
                // Begin, Commit, Message — handled internally by pgwire-replication
            }
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

struct TxnState {
    #[allow(dead_code)]
    xid: u32,
    #[allow(dead_code)]
    commit_lsn: Lsn,
    #[allow(dead_code)]
    commit_ts: i64,
    records: Vec<CdcRecord>,
}

fn tuple_to_cdc_record(
    rel: &CachedRelation,
    op: CdcOp,
    tuple: &TupleData,
    commit_lsn: Lsn,
    commit_ts: i64,
) -> CdcRecord {
    let mut estimated_bytes = 64; // base overhead
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
    // DDL audit table columns: c_key, c_time, c_user, c_txn, c_tag, c_oid, c_name, c_schema, c_ddlqry
    let get_text = |idx: usize| -> Option<String> {
        tuple.columns.get(idx).and_then(|c| match c {
            TupleColumn::Text(data) => String::from_utf8(data.clone()).ok(),
            _ => None,
        })
    };

    let txn = get_text(3);
    let tag = get_text(4).unwrap_or_default();
    let schema = get_text(7).unwrap_or_else(|| "public".to_string());
    let ddl_sql = get_text(8).unwrap_or_default();

    if !tag.is_empty() {
        tracing::info!(tag = %tag, schema = %schema, "Captured DDL event from WAL");
        metadata
            .insert_ddl_event(txn.as_deref(), &tag, &schema, &ddl_sql)
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

        // Find relation info for this table
        let rel = relation_cache
            .relations
            .values()
            .find(|r| r.schema == schema && r.name == table_name);

        if let Some(rel) = rel {
            let batch_id = Uuid::new_v4();
            let dir = staging_root.join("cdc").join(table_key);
            std::fs::create_dir_all(&dir)?;
            let file_path = dir.join(format!("{}.parquet", batch_id));
            let relative_path = format!("cdc/{}/{}.parquet", table_key, batch_id);

            write_cdc_records_to_parquet(&buffer.records, rel, &file_path)?;

            metadata
                .enqueue_cdc_file_and_update_lsn(
                    slot_name,
                    schema,
                    table_name,
                    &relative_path,
                    &buffer.min_lsn,
                    &buffer.max_lsn,
                    buffer.records.len() as i64,
                )
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

fn all_empty(buffers: &HashMap<String, TableBuffer>) -> bool {
    buffers.values().all(|b| b.is_empty())
}
