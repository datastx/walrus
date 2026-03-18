# Walrus — Workflow Diagrams

## High-Level Architecture

```mermaid
graph TB
    subgraph Source["Source PostgreSQL"]
        Tables["Replicated Tables"]
        WAL["WAL Stream"]
        Meta["Metadata Schema<br/>(replication state, table state,<br/>file queue, DDL events)"]
        DDLTrigger["DDL Event Trigger"]
    end

    subgraph Capture["WAL Capture Service"]
        Preflight["Preflight Checks"]
        Snapshot["Snapshot Holder"]
        Backfill["Backfill Manager<br/>(parallel CTID-range scans)"]
        Consumer["WAL Consumer<br/>(always running)"]
    end

    subgraph Staging["Staging Directory (PVC)"]
        BFiles["backfill/*.parquet"]
        CFiles["cdc/*.parquet"]
        Spill["spill/*.tmp"]
    end

    subgraph Writer["Iceberg Writer Service"]
        Poller["File Queue Poller"]
        Merge["CDC Merge Pipeline"]
        SchemaEvo["DDL Schema Evolution"]
    end

    subgraph Warehouse["Iceberg Warehouse"]
        Catalog["catalog.db (SQLite)"]
        IceData["data/<br/>backfill-*.parquet<br/>cdc-data-*.parquet<br/>cdc-delete-*.parquet"]
        IceMeta["metadata/<br/>v*.metadata.json"]
    end

    Tables -->|snapshot read| Backfill
    WAL -->|logical replication| Consumer
    DDLTrigger -->|audit inserts| WAL

    Backfill --> BFiles
    Consumer --> CFiles
    Consumer --> Spill

    Capture <-->|read/write state| Meta
    Writer <-->|claim files, update phases| Meta

    BFiles --> Poller
    CFiles --> Poller
    Poller --> Merge
    Poller --> SchemaEvo
    Merge --> IceData
    SchemaEvo --> IceMeta
    Merge --> IceMeta

    style Source fill:#e1f5ee,stroke:#0f6e56,color:#04342c
    style Capture fill:#eeedfe,stroke:#534ab7,color:#26215c
    style Staging fill:#f1efe8,stroke:#5f5e5a,color:#2c2c2a
    style Writer fill:#faece7,stroke:#993c1d,color:#4a1b0c
    style Warehouse fill:#eaf3de,stroke:#3b6d11,color:#173404
```

## Table Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> Pending: Table registered

    Pending --> Backfilling: WAL Capture starts export

    Backfilling --> BackfillComplete: WAL Capture finishes<br/>all CTID ranges

    BackfillComplete --> Streaming: Iceberg Writer confirms<br/>all backfill files committed

    Streaming --> Streaming: CDC merge loop

    note right of Pending
        No data read yet.
        Picked up on next startup.
    end note

    note right of Backfilling
        CTID-range scans write parquet files.
        WAL consumer is already capturing
        CDC data in parallel (staged, not processed).
    end note

    note right of BackfillComplete
        Export done, but Writer hasn't
        confirmed all files are in Iceberg yet.
        Split ownership prevents race conditions.
    end note

    note right of Streaming
        Writer processes CDC files.
        Upserts → data files.
        Deletes → equality delete files.
        DDL events gate CDC within streaming:
        pending DDL blocks CDC files with
        higher LSNs until resolved.
    end note

    Streaming --> Pending: Slot invalidated (resync)
    Backfilling --> Pending: Slot invalidated (resync)
    BackfillComplete --> Pending: Slot invalidated (resync)
```

## Table State — Who Changes What

The table state metadata is shared between the two services, but each service owns specific transitions and fields. WAL Capture drives a table from registration through export, while the Iceberg Writer drives it from export-complete into live streaming. Neither service touches the other's fields.

```mermaid
flowchart TD
    subgraph WalCapture["WAL Capture"]
        Register["Register table<br/>+ discover primary keys<br/>+ record export plan"]
        Export["Export CTID ranges<br/>+ track partition progress"]
        ExportDone["Mark export complete"]
        SlotLost["Slot invalidation<br/>reset all tables to Pending<br/>flag streaming tables for resync"]
    end

    subgraph IcebergWriter["Iceberg Writer"]
        AbsorbBackfill["Absorb backfill files<br/>+ track writer progress"]
        ResyncMerge["Resync merge complete<br/>clear resync flag"]
        Promote["Promote to Streaming<br/>only when all backfill<br/>files are in Iceberg"]
        CommitCDC["Commit CDC batch<br/>+ advance committed LSN"]
    end

    subgraph TableState["Table State"]
        Phase["phase"]
        BackfillProgress["backfill progress<br/>(total partitions,<br/>done partitions,<br/>snapshot name)"]
        PKCols["primary key columns"]
        WriterProgress["writer progress<br/>(backfill files done,<br/>last committed LSN)"]
        StreamingSince["streaming since"]
        Resync["needs resync"]
    end

    Register -->|"phase → Backfilling"| Phase
    Register --> BackfillProgress
    Register --> PKCols

    Export --> BackfillProgress

    ExportDone -->|"phase → Backfill Complete"| Phase

    SlotLost -->|"phase → Pending<br/>reset all progress"| Phase
    SlotLost -->|"TRUE for formerly<br/>streaming tables"| Resync
    SlotLost --> BackfillProgress
    SlotLost --> WriterProgress

    AbsorbBackfill --> WriterProgress

    ResyncMerge -->|"FALSE"| Resync

    Promote -->|"phase → Streaming"| Phase
    Promote --> StreamingSince
    Promote -->|"FALSE"| Resync

    CommitCDC --> WriterProgress

    style WalCapture fill:#eeedfe,stroke:#534ab7,color:#26215c
    style IcebergWriter fill:#faece7,stroke:#993c1d,color:#4a1b0c
    style TableState fill:#f1efe8,stroke:#5f5e5a,color:#2c2c2a
    style SlotLost fill:#fcebeb,stroke:#a32d2d
    style Promote fill:#faeeda,stroke:#854f0b
```

## WAL Capture — Startup & Preflight

```mermaid
flowchart TD
    Start([Service Starts]) --> EnsureMeta["Ensure metadata infrastructure<br/>create schema + four tables<br/>(replication state, table state,<br/>file queue, DDL events)<br/>if they don't already exist"]

    EnsureMeta --> EnsureDDL["Ensure DDL audit infrastructure<br/>verify audit table, trigger function,<br/>and event trigger exist;<br/>recreate any missing pieces"]

    EnsureDDL --> GatherConfig["Gather table configuration<br/>read which tables to replicate,<br/>discover primary keys from<br/>source database catalog"]

    GatherConfig --> HasPK{Every table has<br/>a primary key?}
    HasPK -->|No| Fail([Refuse to start])
    HasPK -->|Yes| Reconcile["Reconcile with table state<br/>compare configured tables against<br/>what's already tracked; register<br/>new tables — existing tables<br/>keep their current phase"]

    Reconcile --> CheckSlot{Check replication<br/>slot status}

    CheckSlot -->|"Slot exists and valid"| Resume["Resume from<br/>last checkpoint"]
    CheckSlot -->|"Slot in metadata but<br/>gone from Postgres"| ReBootstrap["Full re-bootstrap<br/>create new slot,<br/>reset all tables to Pending,<br/>flag previously-streaming<br/>tables for resync"]
    CheckSlot -->|"No slot at all<br/>(fresh start)"| FreshStart["Create replication slot,<br/>capture export snapshot,<br/>move all tables<br/>into Backfilling"]

    Resume --> Launch
    ReBootstrap --> Launch
    FreshStart --> Launch

    Launch["Launch concurrent tasks"] --> SnapshotHolder["Snapshot Holder<br/>(if tables need backfill)"]
    Launch --> BackfillMgr["Backfill Manager"]
    Launch --> WALConsumer["WAL Consumer"]
    Launch --> Heartbeat["Heartbeat"]

    style Start fill:#eeedfe,stroke:#534ab7
    style Fail fill:#fcebeb,stroke:#a32d2d
    style Launch fill:#e1f5ee,stroke:#0f6e56
    style ReBootstrap fill:#faeeda,stroke:#854f0b
```

## CDC Processing Flow

```mermaid
flowchart TD
    WAL([WAL Stream]) --> Decode[Decode pgoutput messages<br/>inserts / updates / deletes]

    Decode --> IsDDL{Insert into<br/>walrus_ddl_audit?}

    IsDDL -->|Yes| BufferDDL[Buffer DDL event<br/>on in-flight txn state]
    IsDDL -->|No| IsStreaming{pgoutput v2<br/>streaming enabled?}

    BufferDDL --> WaitCommit[Wait for commit<br/>to capture end_lsn]
    WaitCommit --> FlushDDL[Flush DDL event to<br/>ddl_events table<br/>with ddl_lsn = end_lsn]

    IsStreaming -->|Yes| StreamMsg{Message type?}
    IsStreaming -->|No| BufferStd[Buffer in per-table<br/>in-memory buffer]

    StreamMsg -->|Stream Start| OpenTxn[Open in-flight<br/>txn state for xid]
    StreamMsg -->|Stream Data| AppendTxn[Append to txn buffer]
    StreamMsg -->|Stream Commit| MaterializeTxn[Materialize txn records<br/>into table buffers]
    StreamMsg -->|Stream Abort| DiscardTxn[Discard txn + cleanup spill]
    StreamMsg -->|Normal commit| BufferStd

    AppendTxn --> SpillCheck{Txn buffer ><br/>max_txn_memory_bytes?}
    SpillCheck -->|Yes| SpillDisk[Spill to disk<br/>staging/spill/*.tmp]
    SpillCheck -->|No| AppendTxn

    MaterializeTxn --> BufferStd

    BufferStd --> FlushCheck{Flush threshold<br/>reached?}

    FlushCheck -->|"≥ 50k rows"| Flush
    FlushCheck -->|"≥ 64 MB"| Flush
    FlushCheck -->|"≥ 30s elapsed"| Flush
    FlushCheck -->|No| BufferStd

    Flush[Write parquet file<br/>+ enqueue in file queue<br/>+ advance slot checkpoint] --> WAL

    style WAL fill:#faece7,stroke:#993c1d
    style Flush fill:#e1f5ee,stroke:#0f6e56
    style BufferDDL fill:#faeeda,stroke:#854f0b
    style FlushDDL fill:#faeeda,stroke:#854f0b
```

## Iceberg Writer — Merge Pipeline

```mermaid
flowchart TD
    Poll([Poll file queue]) --> ReclaimStale{Stale reclaim<br/>interval elapsed?}

    ReclaimStale -->|Yes| Reclaim[Reclaim files/DDL events<br/>stuck in processing/applying > 10 min]
    ReclaimStale -->|No| ProcessDDL
    Reclaim --> ProcessDDL

    ProcessDDL[Process pending DDL events<br/>per-table, oldest first] --> DDLFound{Pending DDL<br/>for any table?}

    DDLFound -->|Yes| ClaimDDL[Claim oldest event<br/>status → applying]
    DDLFound -->|No| Claim

    ClaimDDL --> ApplyDDL{Apply schema change<br/>to Iceberg table}
    ApplyDDL -->|Success| DDLApplied[status → applied]
    ApplyDDL -->|Failure| DDLFailed[status → failed<br/>table CDC blocked<br/>until operator resolves]

    DDLApplied --> Claim
    DDLFailed --> Claim

    Claim[claim_next_batch<br/>DDL-aware: CDC files blocked<br/>if unapplied DDL with lower LSN] --> HasFiles{Files<br/>claimed?}

    HasFiles -->|No| Sleep[Sleep poll_interval<br/>or shutdown signal]
    Sleep --> Poll

    HasFiles -->|Yes| FileType{File type?}

    FileType -->|Backfill| ResyncCheck{Table flagged<br/>for resync?}
    FileType -->|CDC| Dedup[Deduplicate by PK<br/>keep latest operation per key]

    ResyncCheck -->|No| Append[Append data files<br/>directly to Iceberg table]
    ResyncCheck -->|Yes| DiffMerge[Diff new snapshot vs<br/>existing Iceberg data by PK]

    DiffMerge --> DiffResult[Write only actual changes:<br/>upserts for added/modified rows<br/>deletes for removed rows<br/>skip identical rows]

    DiffResult --> BackfillDone

    Append --> BackfillDone{Table phase =<br/>BackfillComplete<br/>AND no remaining<br/>backfill files?}

    BackfillDone -->|Yes| Promote[Transition table<br/>to Streaming<br/>clear resync flag if set]
    BackfillDone -->|No| MarkDone[Mark files complete]

    Promote --> MarkDone

    Dedup --> Separate[Separate into<br/>upserts + deletes]

    Separate --> Upserts[Write new Iceberg<br/>data files<br/>full row content]
    Separate --> Deletes[Write equality<br/>delete files<br/>PK values only]

    Upserts --> Commit
    Deletes --> Commit

    Commit[Atomic Iceberg snapshot<br/>commit both file types] --> MarkDone

    MarkDone[Mark files as complete<br/>in file queue] --> Poll

    style Poll fill:#faece7,stroke:#993c1d
    style Commit fill:#e1f5ee,stroke:#0f6e56
    style Promote fill:#faeeda,stroke:#854f0b
    style DiffMerge fill:#e6f1fb,stroke:#185fa5
    style DDLFailed fill:#fcebeb,stroke:#a32d2d
    style ProcessDDL fill:#faeeda,stroke:#854f0b
    style Claim fill:#e6f1fb,stroke:#185fa5
```

## DDL Event Status State Machine

```mermaid
stateDiagram-v2
    [*] --> pending: DDL detected in WAL<br/>buffered until commit<br/>flushed with ddl_lsn

    pending --> applying: Writer claims event<br/>(crash guard)
    pending --> skipped: DDL type irrelevant<br/>(CREATE TABLE, RENAME, index ops)

    applying --> applied: Schema change<br/>committed to Iceberg
    applying --> failed: Schema change error<br/>(type incompatibility, etc.)
    applying --> pending: Crash recovery<br/>(stuck > 10 min)

    failed --> pending: Operator retry<br/>(fix and reset)
    failed --> skipped: Operator skip<br/>(not relevant to Iceberg)

    note right of pending
        Blocks CDC files for this table
        where file.lsn_low > ddl_lsn.
        Backfill files unaffected.
    end note

    note right of failed
        Table's CDC pipeline blocked.
        Requires operator intervention:
        fix + retry, skip, or manual apply.
        ddl_blocked_since set on table_state.
    end note

    note right of skipped
        Treated same as applied
        for blocking purposes.
    end note
```

## Per-Table DDL Blocking

```mermaid
flowchart LR
    subgraph DDLState["DDL Events (metadata store)"]
        DDL_Orders["orders: ALTER TABLE<br/>ADD COLUMN email<br/>ddl_lsn = 0/1A00<br/>status = pending"]
        DDL_None["customers:<br/>no pending DDL"]
    end

    subgraph FileQueue["File Queue"]
        F1["orders CDC file<br/>lsn_low = 0/1800<br/>✅ eligible (before DDL)"]
        F2["orders CDC file<br/>lsn_low = 0/1B00<br/>❌ blocked (after DDL)"]
        F3["orders CDC file<br/>lsn_low = 0/1F00<br/>❌ blocked (after DDL)"]
        F4["customers CDC file<br/>lsn_low = 0/1C00<br/>✅ eligible (no DDL)"]
        F5["customers CDC file<br/>lsn_low = 0/1E00<br/>✅ eligible (no DDL)"]
    end

    subgraph Writer["Iceberg Writer"]
        Process["Processes:<br/>• orders file @ 0/1800<br/>• customers file @ 0/1C00<br/>• customers file @ 0/1E00"]
        Blocked["Blocked until DDL applied:<br/>• orders file @ 0/1B00<br/>• orders file @ 0/1F00"]
    end

    DDL_Orders -.->|blocks| F2
    DDL_Orders -.->|blocks| F3
    F1 --> Process
    F4 --> Process
    F5 --> Process
    F2 --> Blocked
    F3 --> Blocked

    style DDL_Orders fill:#faeeda,stroke:#854f0b
    style DDL_None fill:#e1f5ee,stroke:#0f6e56
    style F1 fill:#e1f5ee,stroke:#0f6e56
    style F2 fill:#fcebeb,stroke:#a32d2d
    style F3 fill:#fcebeb,stroke:#a32d2d
    style F4 fill:#e1f5ee,stroke:#0f6e56
    style F5 fill:#e1f5ee,stroke:#0f6e56
    style Process fill:#e1f5ee,stroke:#0f6e56
    style Blocked fill:#fcebeb,stroke:#a32d2d
```

## Crash Recovery Decision Tree

```mermaid
flowchart TD
    Crash([Service restarts]) --> Which{Which service<br/>crashed?}

    Which -->|WAL Capture| CapturePhase{What was it doing?}
    Which -->|Iceberg Writer| WriterPhase{What was it doing?}

    CapturePhase -->|Backfill export| CheckSnapshot{Export snapshot<br/>still valid?}
    CapturePhase -->|CDC consumption| CDCResume[Re-read WAL from<br/>last checkpointed LSN<br/>in-memory buffers lost]

    CheckSnapshot -->|Yes| ResumeBackfill[Resume from next<br/>incomplete CTID range]
    CheckSnapshot -->|No — expired| FullReset

    CDCResume --> OrphanCleanup[Orphaned parquet files<br/>written but not enqueued<br/>cleaned up later]

    WriterPhase -->|Processing files| ReclaimStuck[Reclaim files stuck<br/>in 'processing' > 10 min]
    WriterPhase -->|Mid-Iceberg commit| CommitCheck{Did the Iceberg<br/>commit complete?}

    CommitCheck -->|Yes| Idempotent[Re-processing produces<br/>duplicate data + deletes<br/>that cancel out by PK]
    CommitCheck -->|No| AsIfNever[As if files were<br/>never processed —<br/>retry from scratch]

    FullReset[Full re-bootstrap<br/>new slot + new snapshot] --> ResyncCheck{Were any tables<br/>already Streaming?}

    ResyncCheck -->|Yes| DiffMerge[Flag for resync —<br/>diff new snapshot vs<br/>existing Iceberg data by PK<br/>write only actual changes]
    ResyncCheck -->|No| CleanRestart[All tables restart<br/>from Pending normally]

    DiffMerge --> Efficient[Efficient: billion-row table<br/>with 3 changed rows =<br/>3 upserts, 0 rewrites]

    style Crash fill:#fcebeb,stroke:#a32d2d
    style FullReset fill:#faeeda,stroke:#854f0b
    style Efficient fill:#e1f5ee,stroke:#0f6e56
```

## Idle WAL Reclamation

```mermaid
flowchart LR
    subgraph Passive["Zero-cost (passive)"]
        Idle{CDC loop idle?<br/>No in-flight txns<br/>No buffered data} -->|Yes| KeepAlive[Report server's<br/>WAL tip as<br/>applied LSN]
        KeepAlive --> Reclaim1[Postgres reclaims<br/>WAL segments]
    end

    subgraph Active["Heartbeat (active)"]
        Timer["Every N seconds<br/>(default: 300)"] --> Emit["pg_logical_emit_message()<br/>'walrus_heartbeat'"]
        Emit --> Flows[Begin/Message/Commit<br/>flows through slot]
        Flows --> Advance[Consumer processes<br/>commit, advances<br/>confirmed LSN]
        Advance --> Reclaim2[Postgres reclaims<br/>WAL segments]
    end

    style Passive fill:#eaf3de,stroke:#3b6d11
    style Active fill:#e6f1fb,stroke:#185fa5
```