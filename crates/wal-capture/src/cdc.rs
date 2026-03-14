use crate::decoder::*;
use crate::parquet_writer::write_cdc_records_to_parquet;
use crate::spill::{SpillFile, SPILL_CHUNK_SIZE};
use pgiceberg_common::config::{SourceConfig, TlsMode, WalCaptureConfig};
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
    spill_file: Option<SpillFile>,
    total_records: usize,
    total_bytes: usize,
}

struct CdcState<'a> {
    relation_cache: RelationCache,
    table_buffers: HashMap<String, TableBuffer>,
    current_txn: Option<TxnState>,
    /// Per-xid state for streaming transactions (protocol v2, PG14+).
    /// Multiple streaming transactions can exist concurrently.
    streaming_txns: HashMap<u32, TxnState>,
    /// The xid of the currently active streaming segment (between StreamStart/StreamStop).
    /// When Some, data messages are routed to this streaming transaction.
    current_streaming_xid: Option<u32>,
    last_flush: Instant,
    total_buffered_rows: usize,
    total_buffered_bytes: usize,
    metadata: &'a MetadataStore,
    slot_name: &'a str,
    staging_root: &'a Path,
    config: &'a WalCaptureConfig,
    next_seq: i64,
    max_txn_records: usize,
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
            streaming_txns: HashMap::new(),
            current_streaming_xid: None,
            last_flush: Instant::now(),
            total_buffered_rows: 0,
            total_buffered_bytes: 0,
            metadata,
            slot_name,
            staging_root,
            config,
            next_seq: 0,
            max_txn_records: 0,
        }
    }

    fn handle_begin(&mut self, xid: u32) {
        self.current_txn = Some(TxnState {
            xid,
            records: Vec::new(),
            spill_file: None,
            total_records: 0,
            total_bytes: 0,
        });
    }

    async fn handle_commit(&mut self, end_lsn: Lsn, commit_ts: i64) -> anyhow::Result<()> {
        if let Some(txn) = self.current_txn.take() {
            let total_records = txn.total_records;
            tracing::trace!(xid = txn.xid, records = total_records, "Commit");

            // Track largest transaction for monitoring
            if total_records > self.max_txn_records {
                self.max_txn_records = total_records;
                metrics::gauge!("walrus_cdc_txn_max_records").set(total_records as f64);
            }

            if let Some(spill_file) = txn.spill_file {
                // Spilled transaction: read back from disk in chunks
                metrics::counter!("walrus_cdc_txn_spill_bytes_total")
                    .increment(txn.total_bytes as u64);

                let mut reader = spill_file.into_reader()?;
                loop {
                    let chunk = reader.read_chunk(SPILL_CHUNK_SIZE)?;
                    if chunk.is_empty() {
                        break;
                    }
                    self.apply_committed_records(chunk, end_lsn, commit_ts);

                    // Flush between chunks to keep memory bounded
                    if self.should_flush() {
                        self.flush().await?;
                    }
                }
                // reader dropped here -> spill file deleted
            } else {
                // Normal in-memory transaction (unchanged fast path)
                self.apply_committed_records(txn.records, end_lsn, commit_ts);
            }
        }
        Ok(())
    }

    fn apply_committed_records(&mut self, records: Vec<CdcRecord>, end_lsn: Lsn, commit_ts: i64) {
        for mut record in records {
            record.commit_lsn = end_lsn;
            record.commit_ts = commit_ts;
            record.seq = self.next_seq;
            self.next_seq += 1;
            let key = format!("{}.{}", record.table_schema, record.table_name);
            self.total_buffered_rows += 1;
            self.total_buffered_bytes += record.estimated_bytes;
            self.table_buffers
                .entry(key)
                .or_insert_with(TableBuffer::new)
                .push(record);
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
        let Some(rel) = self.relation_cache.get(relation_oid) else {
            return Ok(());
        };
        if rel.name == "awsdms_ddl_audit" {
            handle_ddl_insert(tuple, rel, self.metadata).await?;
            return Ok(());
        }
        let record = tuple_to_cdc_record(rel, CdcOp::Insert, tuple, Lsn::ZERO, 0);
        self.push_txn_record(record)
    }

    fn handle_update(&mut self, relation_oid: u32, new_tuple: &TupleData) -> anyhow::Result<()> {
        let Some(rel) = self.relation_cache.get(relation_oid) else {
            return Ok(());
        };
        let record = tuple_to_cdc_record(rel, CdcOp::Update, new_tuple, Lsn::ZERO, 0);
        self.push_txn_record(record)
    }

    fn handle_delete(&mut self, relation_oid: u32, old_tuple: &TupleData) -> anyhow::Result<()> {
        let Some(rel) = self.relation_cache.get(relation_oid) else {
            return Ok(());
        };
        let record = tuple_to_cdc_record(rel, CdcOp::Delete, old_tuple, Lsn::ZERO, 0);
        self.push_txn_record(record)
    }

    /// Push a record into the active transaction (regular or streaming),
    /// spilling to disk if the memory threshold is exceeded.
    fn push_txn_record(&mut self, record: CdcRecord) -> anyhow::Result<()> {
        // Route to streaming transaction if inside a Stream Start/Stop block
        let txn = if let Some(xid) = self.current_streaming_xid {
            self.streaming_txns.get_mut(&xid)
        } else {
            self.current_txn.as_mut()
        };

        let Some(txn) = txn else {
            return Ok(());
        };
        let max_mem = self.config.max_txn_memory_bytes;
        txn.total_bytes += record.estimated_bytes;
        txn.total_records += 1;

        if max_mem > 0 && txn.total_bytes > max_mem {
            if txn.spill_file.is_none() {
                let mut spill = SpillFile::create(self.staging_root)?;
                // Flush existing in-memory records to disk
                spill.write_batch(&txn.records)?;
                txn.records.clear();
                txn.spill_file = Some(spill);
                tracing::warn!(
                    xid = txn.xid,
                    bytes = txn.total_bytes,
                    records = txn.total_records,
                    "Transaction exceeded memory threshold, spilling to disk"
                );
                metrics::counter!("walrus_cdc_txn_spills_total").increment(1);
            }
            txn.spill_file.as_mut().unwrap().append(&record)?;
        } else {
            txn.records.push(record);
        }
        Ok(())
    }

    // ── Protocol v2 streaming handlers (PG14+) ──

    fn handle_stream_start(&mut self, xid: u32, first_segment: bool) {
        if first_segment {
            self.streaming_txns.insert(
                xid,
                TxnState {
                    xid,
                    records: Vec::new(),
                    spill_file: None,
                    total_records: 0,
                    total_bytes: 0,
                },
            );
            tracing::trace!(xid, "Stream start (first segment)");
        } else {
            tracing::trace!(xid, "Stream start (continuation)");
        }
        self.current_streaming_xid = Some(xid);
    }

    fn handle_stream_stop(&mut self) {
        if let Some(xid) = self.current_streaming_xid.take() {
            tracing::trace!(xid, "Stream stop");
        }
    }

    async fn handle_stream_commit(
        &mut self,
        xid: u32,
        end_lsn: Lsn,
        commit_ts: i64,
    ) -> anyhow::Result<()> {
        // Clear streaming xid in case commit arrives without a preceding stop
        if self.current_streaming_xid == Some(xid) {
            self.current_streaming_xid = None;
        }

        if let Some(txn) = self.streaming_txns.remove(&xid) {
            let total_records = txn.total_records;
            tracing::debug!(xid, records = total_records, "Stream commit");

            if total_records > self.max_txn_records {
                self.max_txn_records = total_records;
                metrics::gauge!("walrus_cdc_txn_max_records").set(total_records as f64);
            }

            if let Some(spill_file) = txn.spill_file {
                metrics::counter!("walrus_cdc_txn_spill_bytes_total")
                    .increment(txn.total_bytes as u64);

                let mut reader = spill_file.into_reader()?;
                loop {
                    let chunk = reader.read_chunk(SPILL_CHUNK_SIZE)?;
                    if chunk.is_empty() {
                        break;
                    }
                    self.apply_committed_records(chunk, end_lsn, commit_ts);

                    if self.should_flush() {
                        self.flush().await?;
                    }
                }
            } else {
                self.apply_committed_records(txn.records, end_lsn, commit_ts);
            }

            metrics::counter!("walrus_cdc_stream_commits_total").increment(1);
        }
        Ok(())
    }

    fn handle_stream_abort(&mut self, xid: u32) {
        if self.current_streaming_xid == Some(xid) {
            self.current_streaming_xid = None;
        }
        if let Some(txn) = self.streaming_txns.remove(&xid) {
            tracing::debug!(
                xid,
                records = txn.total_records,
                bytes = txn.total_bytes,
                "Stream abort — discarding records"
            );
            metrics::counter!("walrus_cdc_stream_aborts_total").increment(1);
            // TxnState dropped here; SpillFile's Drop cleans up the temp file
        }
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

/// Map our TlsMode to pgwire-replication's TlsConfig.
fn make_replication_tls(source: &SourceConfig) -> TlsConfig {
    match source.tls_mode {
        TlsMode::Disable => TlsConfig::disabled(),
        TlsMode::Prefer | TlsMode::Require => TlsConfig::require(),
        TlsMode::VerifyCa => TlsConfig::verify_ca(source.tls_ca_cert.clone()),
        TlsMode::VerifyFull => TlsConfig::verify_full(source.tls_ca_cert.clone()),
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
        tls: make_replication_tls(source),
        slot: source.slot_name.clone(),
        publication: source.publication_name.clone(),
        start_lsn: repl_lsn,
        stop_at_lsn: None,
        status_interval: std::time::Duration::from_secs(params.config.idle_timeout_seconds),
        idle_wakeup_interval: std::time::Duration::from_secs(params.config.idle_timeout_seconds),
        buffer_events: 8192,
        streaming: params.config.streaming,
    };

    tracing::info!(
        start_lsn = %start_lsn,
        slot = slot_name,
        tls_mode = ?source.tls_mode,
        streaming = params.config.streaming,
        "Starting CDC loop"
    );

    // Connect with retry/backoff
    let mut client = connect_with_retry(repl_config.clone(), &mut shutdown_rx).await?;
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
                        state.handle_commit(end_lsn, commit_ts).await?;
                        client.update_applied_lsn(
                            pgwire_replication::Lsn::parse(&Lsn(wal_end.into()).to_string())
                                .unwrap_or(repl_lsn),
                        );
                        metrics::counter!("walrus_cdc_commits_total").increment(1);
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
                    } => {
                        state.handle_insert(relation_oid, &tuple).await?;
                        metrics::counter!("walrus_cdc_rows_total", "op" => "insert").increment(1);
                    }
                    PgOutputMessage::Update {
                        relation_oid,
                        new_tuple,
                        ..
                    } => {
                        state.handle_update(relation_oid, &new_tuple)?;
                        metrics::counter!("walrus_cdc_rows_total", "op" => "update").increment(1);
                    }
                    PgOutputMessage::Delete {
                        relation_oid,
                        old_tuple,
                    } => {
                        state.handle_delete(relation_oid, &old_tuple)?;
                        metrics::counter!("walrus_cdc_rows_total", "op" => "delete").increment(1);
                    }
                    // Protocol v2 streaming messages (PG14+)
                    PgOutputMessage::StreamStart { xid, first_segment } => {
                        state.handle_stream_start(xid, first_segment);
                    }
                    PgOutputMessage::StreamStop => {
                        state.handle_stream_stop();
                    }
                    PgOutputMessage::StreamCommit {
                        xid,
                        end_lsn,
                        commit_ts,
                        ..
                    } => {
                        state.handle_stream_commit(xid, end_lsn, commit_ts).await?;
                        client.update_applied_lsn(
                            pgwire_replication::Lsn::parse(&Lsn(wal_end.into()).to_string())
                                .unwrap_or(repl_lsn),
                        );
                        metrics::counter!("walrus_cdc_commits_total").increment(1);
                    }
                    PgOutputMessage::StreamAbort { xid, .. } => {
                        state.handle_stream_abort(xid);
                    }
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
                metrics::counter!("walrus_cdc_errors_total").increment(1);

                // Attempt reconnection with backoff instead of crashing
                tracing::info!("Attempting to reconnect to replication stream...");
                state.flush().await.ok(); // best-effort flush before reconnect
                match connect_with_retry(repl_config.clone(), &mut shutdown_rx).await {
                    Ok(new_client) => {
                        client = new_client;
                        metrics::counter!("walrus_cdc_reconnects_total").increment(1);
                        tracing::info!("Reconnected to replication stream");
                        continue;
                    }
                    Err(reconnect_err) => {
                        tracing::error!("Failed to reconnect after retries: {}", reconnect_err);
                        return Err(reconnect_err);
                    }
                }
            }
        }
    }

    Ok(())
}

/// Connect to the replication stream with exponential backoff.
/// Retries up to 5 times with 1s initial delay, 30s max.
async fn connect_with_retry(
    config: ReplicationConfig,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<ReplicationClient> {
    let backoff = backoff::ExponentialBackoffBuilder::new()
        .with_initial_interval(std::time::Duration::from_secs(1))
        .with_max_interval(std::time::Duration::from_secs(30))
        .with_max_elapsed_time(Some(std::time::Duration::from_secs(300)))
        .build();

    let op = || async {
        if *shutdown_rx.borrow() {
            return Err(backoff::Error::Permanent(anyhow::anyhow!(
                "Shutdown during reconnect"
            )));
        }
        ReplicationClient::connect(config.clone())
            .await
            .map_err(|e| {
                tracing::warn!("Replication connect failed: {} — retrying", e);
                metrics::counter!("walrus_cdc_connect_retries_total").increment(1);
                backoff::Error::transient(anyhow::anyhow!(e))
            })
    };

    backoff::future::retry(backoff, op).await
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
        seq: 0,
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
            metrics::counter!("walrus_cdc_flushes_total", "table" => table_key.clone())
                .increment(1);
            metrics::counter!("walrus_cdc_rows_flushed_total", "table" => table_key.clone())
                .increment(buffer.records.len() as u64);
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
