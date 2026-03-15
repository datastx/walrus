# Walrus

Postgres to Apache Iceberg CDC replication.

Walrus continuously replicates PostgreSQL tables into Apache Iceberg, producing an open lakehouse copy of your operational data. It is built as two independent services that coordinate through a shared metadata store and a staging directory. Either service can be killed and restarted at any time without data loss.

---

## How It Works

### Two Services, No Orchestrator

Walrus is split into a **WAL Capture** service and an **Iceberg Writer** service. They never talk to each other directly. All coordination happens through a small set of metadata tables in the source Postgres database and a shared filesystem directory where staged Parquet files are written and read.

This means there is no Temporal, no Airflow, no message broker. Each service is a loop: read state, do work, write state. Kubernetes (or any process supervisor) handles restarts.

### The Metadata Store

A dedicated schema in the source Postgres database holds four tables:

- **Replication state** -- tracks the logical replication slot, the LSN (log sequence number) that has been safely staged, and the snapshot used during initial export.
- **Table state** -- tracks each table's lifecycle phase (pending, backfilling, backfill complete, or streaming), how far along backfill has progressed, and the primary key columns. Importantly, the two services own different phase transitions: WAL Capture moves a table from pending through backfilling to backfill complete, and the Iceberg Writer is the authority that promotes a table to streaming once it has confirmed all backfill data is written to Iceberg. The table state also tracks the writer's own progress separately from WAL Capture's export progress, so you can always see both how much data has been exported to staging files and how much of that data has actually been committed to Iceberg. If the replication slot was ever invalidated and the table had to be re-exported, a resync flag records that fact so the writer knows to replace the existing Iceberg data rather than appending to it.
- **File queue** -- a work queue of staged Parquet files. WAL Capture inserts entries, Iceberg Writer claims and processes them.
- **DDL events** -- captures schema changes (add column, drop column, etc.) so they can be propagated to the Iceberg table.

Using the source database for metadata avoids introducing another stateful dependency. The metadata volume is tiny and every operation is idempotent.

---

## Startup and Preflight

Every time WAL Capture starts, it runs a series of preflight checks before doing any real work. The goal is to verify that all the infrastructure Walrus depends on is in place, and to fail fast with a clear error if something is missing.

First, the service ensures the metadata schema and its four tables exist in the source database. This check is idempotent so it is safe to run on every startup, whether this is a fresh deployment or a restart after a crash.

Second, it verifies that the DDL audit infrastructure is installed on the source database. This includes the audit table, the event trigger function, and the event trigger itself. These objects could have been dropped by a DBA or lost during a database restore, so Walrus checks and recreates them on every startup rather than assuming they survived from the original bootstrap.

Third, it checks whether a replication slot and its associated state already exist. If they do, this is a recovery scenario and the service picks up where it left off. If they do not, this is a fresh start and the service creates a new logical replication slot, captures a snapshot identifier for the initial export, and persists both the slot position and the snapshot name to the replication state table. Persisting the snapshot name is essential for crash recovery: if the service goes down mid-backfill and comes back up, it needs the original snapshot to resume the export from the exact point-in-time it started with.

On recovery, the service also verifies that the replication slot actually still exists in the source database. The metadata might say a slot exists, but Postgres could have invalidated it while the service was down — for example, if WAL retention limits were exceeded and the database decided to discard the slot rather than let disk fill up. If the slot is gone, the service cannot simply resume from where it left off because there is a gap in the change history. Any changes that happened between the last confirmed position and now are lost from the slot's perspective. Walrus detects this condition and triggers a full re-bootstrap, which is described in detail in the Crash Recovery section below.

Fourth, it gathers metadata about every table it has been configured to replicate. The most important piece of metadata is the primary key. Walrus discovers primary keys from the source database catalog on every startup so it never relies on stale cached information. If a table has no primary key constraint and none is specified in the configuration file, the service refuses to start. A primary key is non-negotiable because the entire CDC merge pipeline depends on it for deduplication, upsert resolution, and delete propagation. Without one, data in the Iceberg table would silently diverge from the source.

Finally, it checks the table state metadata to understand where each table is in its lifecycle. A table might be pending its first export, actively backfilling, finished exporting but waiting for the Iceberg Writer to absorb the data, or fully streaming. This determines which of the concurrent tasks need to run.

---

## Lifecycle of a Table

Each table moves through four phases. The two services share ownership of these transitions, and the phase a table is in determines what kind of work each service will do for it.

### Pending

A table starts in the pending phase when it is first registered. No data has been read from it yet. On the next startup where backfill work is needed, WAL Capture will pick it up.

### Backfilling

WAL Capture moves a table from pending to backfilling when it begins reading the table's contents at the snapshot point. It scans the table using range queries on Postgres's internal row addresses (CTID ranges), writing each range as a Parquet file and enqueuing it in the file queue. Progress is checkpointed per-range, so if the service crashes mid-export, it resumes from the last completed range rather than starting over. Multiple tables are exported in parallel with configurable concurrency.

While this is happening, the WAL consumer is already running on a separate path, pulling changes off the replication slot for all tables including the ones still being backfilled. This is a critical design decision: the WAL stream must always be consumed so the replication slot does not hold back WAL on the source database. The backfill and the streaming operate independently, and neither waits for the other.

The CDC files that accumulate for a table during backfill are staged to disk just like any other CDC data, but the Iceberg Writer knows not to process them yet. They sit in the file queue until the table is ready.

### Backfill Complete

When WAL Capture finishes exporting all partitions for a table, it moves the table to backfill complete. This signals that the export side of the work is done, but the Iceberg Writer has not yet confirmed that it has processed all of those backfill files and written them into the Iceberg table.

This distinction matters because the Iceberg Writer is the authority on whether the initial data is safely in Iceberg. WAL Capture knows it finished producing the files, but only the writer knows whether it has consumed them all and committed them. Until that happens, the table stays in this intermediate phase.

### Streaming

The Iceberg Writer transitions a table from backfill complete to streaming after it has successfully processed every backfill file for that table. At that point, the writer begins processing the CDC files that have been accumulating in the queue. From here on, the table receives live changes with no further handoff between the services.

This phase ownership model means the phase column in the metadata always reflects ground truth. A table marked as streaming genuinely has all of its initial data in Iceberg and is ready for live change processing. There is no window where the phase says streaming but backfill files are still in flight.

### Why the WAL Consumer Never Waits

The WAL consumer runs from the moment the service starts and never pauses, regardless of how many tables are still backfilling. This is intentional. The replication slot is the single point of WAL retention on the source database. If the consumer stops reading, Postgres holds onto WAL segments, and disk usage grows. By always consuming the stream, Walrus keeps the slot moving forward and avoids putting pressure on the source.

The consequence is that CDC data arrives for tables that are not yet ready to process it. That is fine. The data is staged as Parquet files in the file queue, and the Iceberg Writer simply skips those tables until they reach the streaming phase. The ordering guarantee is enforced at the writer, not the consumer.

### Change Data Capture Details

Changes from the WAL stream (inserts, updates, deletes) are buffered in memory per-table and flushed to Parquet files on disk when any of three configurable thresholds is reached:

- **Row count** -- flush after accumulating N rows (default: 50,000)
- **Memory size** -- flush after the in-memory buffer reaches N bytes (default: 64 MB)
- **Time** -- flush after N seconds since the last flush (default: 30 seconds)

Each flush atomically writes the Parquet file, enqueues it in the file queue, and advances the replication slot's checkpoint. This ordering guarantees that the slot is never advanced past data that hasn't been safely staged.

#### Handling Large Transactions

A single PostgreSQL transaction that touches millions of rows creates three related problems: unbounded memory growth on the consumer, WAL retention bloat on the source (the slot's [`restart_lsn`](https://www.postgresql.org/docs/current/view-pg-replication-slots.html) is pinned until the transaction commits), and a latency spike when all the changes are delivered in a burst at commit time. These are well-documented challenges in the CDC space -- [PeerDB](https://blog.peerdb.io/handling-initial-snapshots-for-large-tables-in-postgres-cdc), [Debezium](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-wal-disk-space), and [CockroachDB changefeeds](https://www.cockroachlabs.com/docs/stable/create-changefeed#memory-usage) all have mechanisms to address them.

Walrus tackles this from two angles:

**Transaction spill-to-disk.** When an in-flight transaction's in-memory buffer exceeds a configurable threshold (`max_txn_memory_bytes`, default 128 MB), Walrus spills the buffered records to a temporary file on disk and continues appending there. On commit, records are read back in chunks and flushed to table buffers incrementally so memory stays bounded throughout. On abort, the spill file is cleaned up automatically. This approach is similar to how PostgreSQL's own [`logical_decoding_work_mem`](https://www.postgresql.org/docs/current/runtime-config-resource.html#GUC-LOGICAL-DECODING-WORK-MEM) works server-side -- when the ReorderBuffer exceeds the configured memory limit, decoded changes are [spilled to disk](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html) under the `pg_replslot/` directory. Walrus applies the same principle on the consumer side, which works regardless of PostgreSQL version.

**pgoutput protocol v2 streaming (PostgreSQL 14+).** PostgreSQL 14 introduced [streaming mode for logical replication](https://www.postgresql.org/docs/current/logical-replication-config.html) (`streaming = on`). Instead of buffering an entire large transaction server-side until commit, the server begins sending in-progress transaction changes incrementally once `logical_decoding_work_mem` is exceeded. This is controlled by the [pgoutput protocol version 2](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html), which adds four new message types: Stream Start, Stream Stop, Stream Commit, and Stream Abort. Walrus includes a vendored fork of `pgwire-replication` that negotiates protocol v2 and passes `streaming 'on'` when this feature is enabled. The WAL Capture service maintains per-transaction state for each streamed xid (using the same spill-to-disk infrastructure for memory safety) and only materializes the records into table buffers on Stream Commit.

When streaming is enabled, the server-side benefits are significant: the [`ReorderBuffer`](https://www.postgresql.org/docs/current/reorder-buffer.html) no longer needs to hold the full transaction in memory or on disk, WAL segments are released earlier because the slot's restart LSN can advance during the transaction, and the "dam burst" latency spike at commit time is eliminated because data arrives incrementally. PostgreSQL 16 extended this further with [`streaming = parallel`](https://www.postgresql.org/docs/16/logical-replication-config.html), which applies streamed transactions in parallel workers for a reported 50-60% throughput improvement, though Walrus does not yet distinguish between the two modes.

To enable streaming, set `streaming = true` in the WAL Capture configuration (or the `CDC_STREAMING` environment variable).

#### Idle WAL Reclamation (Heartbeat)

When no writes happen on the source database, the replication slot's `confirmed_flush_lsn` never advances. PostgreSQL cannot reclaim old WAL segments, leading to unbounded disk growth. Every major CDC tool (PeerDB, Debezium, Fivetran) solves this by generating periodic small writes that flow through the replication slot.

Walrus uses two complementary mechanisms:

**KeepAlive LSN advancement.** When the CDC loop is truly idle (no in-flight transaction, no streaming transactions, all table buffers empty), the server's periodic KeepAlive message includes the current WAL tip. Since nothing is pending, Walrus reports this position as its applied LSN, allowing PostgreSQL to reclaim WAL up to that point. This is a zero-cost optimization that requires no configuration.

**`pg_logical_emit_message()` heartbeat.** A background task periodically calls `SELECT pg_logical_emit_message(true, 'walrus_heartbeat', 'tick')`, which writes a lightweight transactional logical message directly into WAL. This generates a Begin/Message/Commit sequence that flows through the replication slot, causing the CDC consumer to process the commit and advance its confirmed LSN. Unlike the heartbeat-table approach used by other tools, this requires no extra table, no publication changes, and no record filtering -- it is the lightest possible mechanism. The interval is configurable via `heartbeat_interval_seconds` (default: 5 minutes, set to 0 to disable).

**Source-side guardrails.** Independent of the above, the source PostgreSQL should be configured to limit the blast radius of large transactions:

- [`max_slot_wal_keep_size`](https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE) (PG13+) caps WAL retention per slot. If exceeded, the slot is invalidated rather than letting disk fill up.
- [`statement_timeout`](https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STATEMENT-TIMEOUT) and [`idle_in_transaction_session_timeout`](https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-IDLE-IN-TRANSACTION-SESSION-TIMEOUT) limit runaway statements and abandoned transactions.
- [`logical_decoding_work_mem`](https://www.postgresql.org/docs/current/runtime-config-resource.html#GUC-LOGICAL-DECODING-WORK-MEM) controls when the server spills to disk (default 64 MB). Raising this reduces disk I/O but increases memory usage.
- Monitoring `pg_stat_replication.write_lag` and `pg_replication_slots.wal_status` provides early warning of replication lag and WAL accumulation.

### Iceberg Merge

The Iceberg Writer polls the file queue for pending work. For each batch of files:

1. **Backfill files** are simple appends -- the data is written directly into the Iceberg table as new data files. After each batch of backfill files is processed, the writer checks whether that table has any remaining backfill files in the queue. If the table's phase is backfill complete and no backfill files remain, the writer transitions the table to streaming. This is the moment the initial export is truly done from end to end.

2. **CDC files** go through a merge pipeline:
   - The staged Parquet files are loaded and deduplicated by primary key, keeping only the latest operation for each key.
   - Records are separated into upserts (inserts and updates) and deletes.
   - Upserts produce new Iceberg data files containing the full row.
   - Deletes produce Iceberg equality delete files containing just the primary key values of removed rows.
   - Both are committed atomically in a single Iceberg snapshot.

The Iceberg Writer will only process CDC files for a table once that table has reached the streaming phase. This is the primary ordering guarantee: the writer is the authority that decides when a table is ready for live changes, and it only makes that decision after it has confirmed all backfill data is safely in Iceberg.

---

## How Concurrency Works

The single WAL stream is the fundamental constraint: Postgres delivers changes from one replication slot in commit order across all published tables. Walrus handles this by separating concerns:

```
                        WAL Capture Service
                  ┌──────────────────────────────┐
                  │                              │
                  │  Snapshot Holder              │
                  │  (keeps export snapshot alive)│
                  │                              │
                  │  Backfill Manager             │
                  │  (parallel table exports)     │
                  │                              │
                  │  WAL Consumer                 │
                  │  (reads ALL changes from      │
                  │   the single WAL stream)      │
                  └──────────────────────────────┘
```

- The **Snapshot Holder** keeps a database connection open holding the export snapshot alive for the duration of all backfills.
- The **Backfill Manager** runs CTID-range scans against the snapshot, writing Parquet files for each range. Multiple tables are processed concurrently.
- The **WAL Consumer** reads the change stream from the replication slot starting immediately. It stages changes for all tables, including those still being exported.

The Iceberg Writer enforces per-table ordering through the phase model. It processes backfill files as they arrive, and once all backfill files for a table are absorbed, it transitions that table to streaming and begins processing its CDC files. A small table that finishes its export in seconds reaches streaming almost immediately, while a billion-row table can take hours to export without blocking any other table. The WAL consumer never stops reading during any of this -- it stages CDC data for every table regardless of phase, and the writer decides when each table is ready to consume it.

---

## Crash Recovery

Every step is designed to be safe across restarts. On startup, WAL Capture always re-verifies the DDL audit infrastructure (the event trigger, the audit table, and its publication membership) regardless of whether this is a fresh start or recovery. It also re-discovers primary keys from the source catalog without overwriting any persisted lifecycle phase, so a table that was in the middle of backfilling or waiting for the writer to finish stays in exactly the phase it was in before the crash.

- **WAL Capture crashes during export** -- the metadata store records which ranges are done. On restart, the service resumes from the next incomplete range. If the snapshot has expired (the service was down too long), the slot is recreated, any tables that were mid-backfill or waiting for the writer are reset to pending, and the export starts over.

- **WAL Capture crashes during CDC** -- in-memory buffers are lost. On restart, the WAL is re-read from the last checkpointed LSN. Any Parquet files that were written but not enqueued are orphaned and cleaned up later.

- **Iceberg Writer crashes during processing** -- files stuck in "processing" for more than ten minutes are automatically reclaimed on the next startup. If an Iceberg commit completed before the crash, re-processing the same files produces duplicate data files and equality deletes that cancel out (correct by primary key). If the commit didn't complete, it's as if the files were never processed. If the writer crashes after processing some but not all backfill files for a table, the table stays in its current phase and the remaining files are picked up on restart.

- **Source Postgres restarts** -- the replication slot persists. WAL Capture reconnects and resumes from where it left off.

The replication slot is never advanced past what has been safely written to disk and enqueued. This means Postgres retains WAL from the last checkpoint while the service is down, trading source disk usage for zero data loss.

### Slot Invalidation and Resync

The most serious recovery scenario is when the replication slot itself is gone. This happens when the service is down long enough that Postgres decides to discard the slot — typically because WAL retention limits were exceeded. When this happens, there is a gap in the change history: any inserts, updates, or deletes that occurred between the last confirmed position and the moment the new slot is created are not captured by either the old slot or the new one.

This is not just a problem for tables that were mid-backfill. Tables that were already fully streaming are also affected. If a table was happily receiving live changes before the outage, and changes continued to happen during the outage, those changes are lost when the slot is recreated. Simply continuing to stream from the new slot would leave a hole in the data — the Iceberg table would be missing every change that happened in that window, with no indication that anything was wrong.

Walrus handles this by treating slot invalidation as a full reset. When it detects the slot is gone on startup, it resets every table back to the beginning of the lifecycle and re-exports them from scratch using the new snapshot. Tables that were mid-backfill are straightforward — they had incomplete data and just start over. Tables that were already streaming require more care, because they already have data in Iceberg that may now be out of date.

For those previously-streaming tables, Walrus marks them with a resync flag. This flag tells the Iceberg Writer that the table's existing data cannot be trusted. When the writer encounters backfill files for a table flagged for resync, it drops the existing Iceberg table and recreates it from the new snapshot data. This means the table is rebuilt from the ground up — every row comes from the fresh export, and there is no possibility of stale or missing data surviving from before the outage.

The tradeoff is that dropping the Iceberg table discards its snapshot history. Any previous versions of the data are gone. But the alternative — silently continuing with a gap in the change history — is worse, because it produces an Iceberg table that looks correct but is quietly missing data. A clean rebuild is always preferable to silent data loss.

After the resync backfill completes and the writer finishes processing all the new files, the table transitions back to streaming and the resync flag is cleared. From that point forward, the table behaves exactly as it would after a normal first-time backfill. Any stale CDC files from before the re-bootstrap are discarded during the reset, since their positions refer to the old slot and have no meaning in the context of the new one.

---

## DDL Change Propagation

Postgres does not replicate DDL (schema changes) through logical replication. Walrus works around this using an event trigger on the source database that captures DDL statements into an audit table. The audit table is included in the replication publication, so its inserts appear in the WAL stream.

When WAL Capture sees an insert into the audit table, it extracts the DDL metadata (what kind of change, which table, the SQL statement) and writes it to the DDL events table in the metadata store. The Iceberg Writer checks for pending DDL events before processing data files and applies schema changes to the Iceberg table (add column, drop column, type changes) before any data that depends on the new schema arrives.

---

## Staging Directory

The two services share a filesystem directory (a Kubernetes PersistentVolumeClaim in production) organized as:

```
/data/staging/
├── backfill/
│   └── <schema>.<table>/
│       ├── 0.parquet
│       ├── 1.parquet
│       └── ...
├── cdc/
│   └── <schema>.<table>/
│       ├── <batch-uuid>.parquet
│       └── ...
└── spill/
    └── spill_<uuid>.tmp   (temporary, auto-cleaned)
```

WAL Capture writes files here. Iceberg Writer reads them. Completed files are cleaned up after a configurable retention period. The `spill/` directory holds temporary files for large transactions that exceed the in-memory threshold; these are automatically deleted on transaction commit or abort.

---

## Iceberg Output

Walrus writes to an Apache Iceberg warehouse on the local filesystem using a SQLite-backed catalog. The warehouse is organized as:

```
/data/iceberg/
├── catalog.db          (SQLite Iceberg catalog)
├── <namespace>/
│   └── <table>/
│       ├── metadata/
│       │   ├── v1.metadata.json
│       │   └── ...
│       └── data/
│           ├── backfill-*.parquet
│           ├── cdc-data-*.parquet
│           └── cdc-delete-*.parquet
```

The Iceberg tables can be queried by any engine that reads Iceberg: DuckDB, Spark, Trino, Snowflake, or BigQuery.

---

## Configuration

Both services read from a shared TOML configuration file with environment variable overrides for secrets and deployment-specific values.

Key settings:

| Setting | Description | Default |
|---------|-------------|---------|
| Source connection | Host, port, database, user, password | -- |
| Slot name | Logical replication slot identifier | pgiceberg_slot |
| Publication name | Postgres publication for table filtering | pgiceberg_pub |
| Table list | Which tables to replicate, with optional PK override | -- |
| Staging root | Shared directory for Parquet staging | /data/staging |
| Flush row threshold | CDC rows before flush | 50,000 |
| Flush byte threshold | CDC buffer bytes before flush | 64 MB |
| Flush time threshold | Seconds before time-based flush | 30 |
| Max txn memory | In-flight transaction bytes before spilling to disk | 128 MB |
| Streaming | Enable pgoutput v2 streaming for large transactions | false |
| Heartbeat interval | Seconds between heartbeat ticks for idle WAL reclamation (0 = disabled) | 300 |
| Backfill parallelism | Concurrent tables during export | 4 |
| Rows per partition | CTID range size for export chunks | 500,000 |
| Warehouse path | Iceberg warehouse directory | /data/iceberg |
| Poll interval | How often writer checks for new files | 5 seconds |
| Max retries | Failed file processing retry limit | 3 |
| Cleanup retention | Hours before completed files are deleted | 24 |

Primary keys are auto-detected from the source database catalog on every startup. Every table must have a primary key -- either a constraint on the source table or an explicit override in the configuration file. If Walrus cannot find a primary key for any configured table, the service refuses to start. This is a hard requirement because the entire CDC merge pipeline depends on primary keys for deduplication, upsert resolution, and delete propagation.

---

## Deployment

Walrus is designed for Kubernetes:

- **WAL Capture** runs as a single-replica StatefulSet with a headless Service. A StatefulSet guarantees ordered pod termination -- the old pod is fully shut down before a replacement starts -- preventing split-brain WAL consumption that would cause duplicate or lost data.
- **Iceberg Writer** runs as a Deployment that can potentially scale to multiple replicas (file queue claiming is atomic and per-table).
- Both services expose health check endpoints for liveness and readiness probes.
- The staging directory is a ReadWriteMany PVC (EFS, NFS, or similar).
- The Iceberg warehouse is a PVC mounted by the writer and optionally read-only by query pods.

---

## Postgres Requirements

The source database needs:

- **PostgreSQL 14 or later** -- required for pgoutput protocol v2 streaming of large in-progress transactions and `pg_logical_emit_message()` support used by the idle WAL reclamation heartbeat
- `wal_level = logical`
- At least one available replication slot and WAL sender
- A user with `REPLICATION` privilege and `SELECT` on the replicated tables
- Superuser (or equivalent, e.g. `rds_superuser` on managed databases) for creating the DDL event trigger

---

## License

MIT
