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
- **Table state** -- tracks each table's lifecycle phase (pending, backfilling, or streaming), how far along backfill has progressed, and the primary key columns.
- **File queue** -- a work queue of staged Parquet files. WAL Capture inserts entries, Iceberg Writer claims and processes them.
- **DDL events** -- captures schema changes (add column, drop column, etc.) so they can be propagated to the Iceberg table.

Using the source database for metadata avoids introducing another stateful dependency. The metadata volume is tiny and every operation is idempotent.

---

## Lifecycle of a Table

### Phase 1: Initial Export (Backfill)

When Walrus first starts, it creates a logical replication slot in Postgres. This slot pins a consistent point in the write-ahead log -- everything committed before that point is the "snapshot," and everything after is the ongoing change stream.

For each configured table, Walrus reads the full contents at that snapshot point using range scans on Postgres's internal row addresses (CTID ranges). Each range is written as a Parquet file and enqueued in the file queue. Progress is checkpointed per-range, so if the service crashes mid-export, it resumes from the last completed range rather than starting over.

Multiple tables are exported in parallel with configurable concurrency.

### Phase 2: Change Data Capture (CDC)

The WAL consumer starts reading the change stream immediately -- it does not wait for backfill to finish. This is the key concurrency design: small tables finish their export quickly and start receiving live changes right away, while large tables continue exporting in the background without blocking anything.

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

To enable streaming, set `streaming = true` in the WAL Capture configuration (or the `CDC_STREAMING` environment variable). This requires PostgreSQL 14 or later. On older versions, leave streaming disabled -- the spill-to-disk mechanism still protects against OOM.

**Source-side guardrails.** Independent of the above, the source PostgreSQL should be configured to limit the blast radius of large transactions:

- [`max_slot_wal_keep_size`](https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE) (PG13+) caps WAL retention per slot. If exceeded, the slot is invalidated rather than letting disk fill up.
- [`statement_timeout`](https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STATEMENT-TIMEOUT) and [`idle_in_transaction_session_timeout`](https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-IDLE-IN-TRANSACTION-SESSION-TIMEOUT) limit runaway statements and abandoned transactions.
- [`logical_decoding_work_mem`](https://www.postgresql.org/docs/current/runtime-config-resource.html#GUC-LOGICAL-DECODING-WORK-MEM) controls when the server spills to disk (default 64 MB). Raising this reduces disk I/O but increases memory usage.
- Monitoring `pg_stat_replication.write_lag` and `pg_replication_slots.wal_status` provides early warning of replication lag and WAL accumulation.

### Phase 3: Iceberg Merge

The Iceberg Writer polls the file queue for pending work. For each batch of files:

1. **Backfill files** are simple appends -- the data is written directly into the Iceberg table as new data files.

2. **CDC files** go through a merge pipeline:
   - The staged Parquet files are loaded and deduplicated by primary key, keeping only the latest operation for each key.
   - Records are separated into upserts (inserts and updates) and deletes.
   - Upserts produce new Iceberg data files containing the full row.
   - Deletes produce Iceberg equality delete files containing just the primary key values of removed rows.
   - Both are committed atomically in a single Iceberg snapshot.

The Iceberg Writer only processes CDC files for a table after that table's backfill is complete, ensuring correct ordering.

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

The Iceberg Writer then enforces per-table ordering: it will not process CDC files for a table until all backfill files for that table are done. This means a small table that finishes its export in seconds starts receiving live changes almost immediately, while a billion-row table can take hours to export without blocking any other table.

---

## Crash Recovery

Every step is designed to be safe across restarts:

- **WAL Capture crashes during export** -- the metadata store records which ranges are done. On restart, the service resumes from the next incomplete range. If the snapshot has expired (the service was down too long), the slot is recreated and export starts over.

- **WAL Capture crashes during CDC** -- in-memory buffers are lost. On restart, the WAL is re-read from the last checkpointed LSN. Any Parquet files that were written but not enqueued are orphaned and cleaned up later.

- **Iceberg Writer crashes during processing** -- files stuck in "processing" for more than ten minutes are automatically reclaimed on the next startup. If an Iceberg commit completed before the crash, re-processing the same files produces duplicate data files and equality deletes that cancel out (correct by primary key). If the commit didn't complete, it's as if the files were never processed.

- **Source Postgres restarts** -- the replication slot persists. WAL Capture reconnects and resumes from where it left off.

The replication slot is never advanced past what has been safely written to disk and enqueued. This means Postgres retains WAL from the last checkpoint while the service is down, trading source disk usage for zero data loss.

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
| Streaming | Enable pgoutput v2 streaming for large transactions (PG14+) | false |
| Backfill parallelism | Concurrent tables during export | 4 |
| Rows per partition | CTID range size for export chunks | 500,000 |
| Warehouse path | Iceberg warehouse directory | /data/iceberg |
| Poll interval | How often writer checks for new files | 5 seconds |
| Max retries | Failed file processing retry limit | 3 |
| Cleanup retention | Hours before completed files are deleted | 24 |

Primary keys are auto-detected from the source database on every startup. If a table has no primary key, one can be specified in the configuration.

---

## Deployment

Walrus is designed for Kubernetes:

- **WAL Capture** runs as a single-replica Deployment with Recreate strategy (only one instance can consume a replication slot).
- **Iceberg Writer** runs as a Deployment that can potentially scale to multiple replicas (file queue claiming is atomic and per-table).
- Both services expose health check endpoints for liveness and readiness probes.
- The staging directory is a ReadWriteMany PVC (EFS, NFS, or similar).
- The Iceberg warehouse is a PVC mounted by the writer and optionally read-only by query pods.

---

## Postgres Requirements

The source database needs:

- `wal_level = logical`
- At least one available replication slot and WAL sender
- A user with `REPLICATION` privilege and `SELECT` on the replicated tables
- Superuser (or `rds_superuser` on RDS) for creating the DDL event trigger

---

## License

MIT
