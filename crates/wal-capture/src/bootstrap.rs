use pgiceberg_common::config::SourceConfig;
use pgiceberg_common::config::WalCaptureConfig;
use pgiceberg_common::metadata::MetadataStore;
use pgiceberg_common::models::TablePhase;
use pgiceberg_common::sql::quote_ident;
use pgiceberg_common::tls;
use tokio_postgres::Client;
use tracing::{info, warn};

/// Slot creation result from `CREATE_REPLICATION_SLOT`.
pub struct SlotInfo {
    pub consistent_point: String,
    pub snapshot_name: String,
}

/// Bootstrap everything needed for replication.
///
/// This function is idempotent: re-running after a crash is safe.
///
/// Recovery contract:
///   - If no replication_state row exists → fresh start (create slot, publication, table states).
///   - If row exists → recover from persisted state (skip creation, resume from checkpoints).
///   - Primary keys are always re-discovered from `pg_index` on startup so nothing needs to
///     be persisted.  Config-supplied PKs override if present.
pub async fn bootstrap(
    config: &SourceConfig,
    wal_config: &WalCaptureConfig,
    metadata: &MetadataStore,
) -> anyhow::Result<Option<SlotInfo>> {
    metadata.bootstrap().await?;

    let existing = metadata.get_replication_state(&config.slot_name).await?;

    if existing.is_some() {
        info!(slot = %config.slot_name, "Found existing replication state — recovering");
        refresh_pk_columns(config, metadata).await?;
        return Ok(None);
    }

    info!(slot = %config.slot_name, "No existing state — fresh bootstrap");

    let (client, _conn_handle) = tls::pg_connect(
        &config.connection_string(),
        &config.tls_mode,
        config.tls_ca_cert.as_deref(),
    )
    .await?;

    ensure_publication(&client, config).await?;
    let slot_info = create_replication_slot(&client, config).await?;
    bootstrap_ddl_audit(&client, config).await?;

    metadata
        .insert_replication_state(
            &config.slot_name,
            &config.publication_name,
            &slot_info.consistent_point,
            &slot_info.snapshot_name,
        )
        .await?;

    for (schema, table) in config.table_list() {
        let pk_cols = discover_or_config_pk(config, metadata, &schema, &table).await?;
        let partitions =
            compute_partitions(&client, &schema, &table, wal_config.rows_per_partition).await?;
        metadata
            .upsert_table_state(
                &schema,
                &table,
                TablePhase::Backfilling,
                Some(partitions as i32),
                Some(&slot_info.snapshot_name),
                &pk_cols,
            )
            .await?;
        info!(schema, table, partitions, "Table registered for backfill");
    }

    Ok(Some(slot_info))
}

/// Drop a replication slot. Used for recovery when the snapshot has expired.
pub async fn drop_replication_slot(config: &SourceConfig) -> anyhow::Result<()> {
    let (client, _conn_handle) = tls::pg_connect(
        &config.connection_string(),
        &config.tls_mode,
        config.tls_ca_cert.as_deref(),
    )
    .await?;

    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&config.slot_name],
        )
        .await?
        .get(0);

    if exists {
        client
            .execute("SELECT pg_drop_replication_slot($1)", &[&config.slot_name])
            .await?;
        info!(slot = %config.slot_name, "Dropped stale replication slot");
    }
    Ok(())
}

async fn ensure_publication(client: &Client, config: &SourceConfig) -> anyhow::Result<()> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&config.publication_name],
        )
        .await?
        .get(0);

    if exists {
        info!(pub_name = %config.publication_name, "Publication already exists");
        sync_publication_tables(client, config).await?;
    } else {
        let table_list: Vec<String> = config
            .table_list()
            .iter()
            .map(|(s, t)| format!("{}.{}", quote_ident(s), quote_ident(t)))
            .collect();
        let sql = format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            quote_ident(&config.publication_name),
            table_list.join(", ")
        );
        client.batch_execute(&sql).await?;
        info!(pub_name = %config.publication_name, "Created publication");
    }
    Ok(())
}

async fn sync_publication_tables(client: &Client, config: &SourceConfig) -> anyhow::Result<()> {
    let rows = client
        .query(
            "SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1",
            &[&config.publication_name],
        )
        .await?;
    let current: std::collections::HashSet<String> =
        rows.iter().map(|r| r.get::<_, String>(0)).collect();

    for (schema, table) in config.table_list() {
        let full = format!("{}.{}", schema, table);
        if !current.contains(&full) {
            let sql = format!(
                "ALTER PUBLICATION {} ADD TABLE {}.{}",
                quote_ident(&config.publication_name),
                quote_ident(&schema),
                quote_ident(&table)
            );
            client.batch_execute(&sql).await?;
            info!(table = %full, "Added table to publication");
        }
    }
    Ok(())
}

async fn create_replication_slot(
    client: &Client,
    config: &SourceConfig,
) -> anyhow::Result<SlotInfo> {
    let row = client
        .query_one(
            "SELECT lsn::text, snapshot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&config.slot_name],
        )
        .await?;

    let consistent_point: String = row.get(0);
    let snapshot_name: String = row.get(1);

    info!(
        slot = %config.slot_name,
        consistent_point,
        snapshot_name,
        "Created replication slot"
    );

    Ok(SlotInfo {
        consistent_point,
        snapshot_name,
    })
}

async fn bootstrap_ddl_audit(client: &Client, config: &SourceConfig) -> anyhow::Result<()> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_name = 'awsdms_ddl_audit')",
            &[],
        )
        .await?
        .get(0);

    if !exists {
        client
            .batch_execute(
                "CREATE TABLE public.awsdms_ddl_audit (
                    c_key     bigserial    PRIMARY KEY,
                    c_time    timestamp,
                    c_user    varchar(64),
                    c_txn     varchar(16),
                    c_tag     varchar(24),
                    c_oid     integer,
                    c_name    varchar(64),
                    c_schema  varchar(64),
                    c_ddlqry  text
                )",
            )
            .await?;
        info!("Created awsdms_ddl_audit table");
    }

    client
        .batch_execute(
            "CREATE OR REPLACE FUNCTION public.awsdms_intercept_ddl()
             RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
             DECLARE _qry text;
             DECLARE _tbl_name text;
             DECLARE _tbl_schema text;
             DECLARE _r record;
             BEGIN
                 IF (tg_tag = 'CREATE TABLE' OR tg_tag = 'ALTER TABLE' OR
                     tg_tag = 'DROP TABLE'   OR tg_tag = 'CREATE TABLE AS') THEN
                     SELECT current_query() INTO _qry;
                     _tbl_name := '';
                     _tbl_schema := current_schema;
                     IF tg_tag != 'DROP TABLE' THEN
                         FOR _r IN SELECT objid, schema_name, object_identity
                                   FROM pg_event_trigger_ddl_commands() LOOP
                             _tbl_name := split_part(_r.object_identity, '.', 2);
                             _tbl_schema := _r.schema_name;
                             EXIT;
                         END LOOP;
                     END IF;
                     INSERT INTO public.awsdms_ddl_audit
                     VALUES (default, current_timestamp, current_user,
                             cast(TXID_CURRENT() AS varchar(16)),
                             tg_tag, 0, _tbl_name, _tbl_schema, _qry);
                     DELETE FROM public.awsdms_ddl_audit;
                 END IF;
             END;
             $$",
        )
        .await?;

    let trigger_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_event_trigger WHERE evtname = 'awsdms_intercept_ddl')",
            &[],
        )
        .await?
        .get(0);

    if !trigger_exists {
        client
            .batch_execute(
                "CREATE EVENT TRIGGER awsdms_intercept_ddl \
                 ON ddl_command_end \
                 EXECUTE PROCEDURE public.awsdms_intercept_ddl()",
            )
            .await?;
        info!("Created DDL audit event trigger");
    }

    // Make sure the audit table is in the publication
    let in_pub: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication_tables \
             WHERE pubname = $1 AND schemaname = 'public' AND tablename = 'awsdms_ddl_audit')",
            &[&config.publication_name],
        )
        .await?
        .get(0);

    if !in_pub {
        let sql = format!(
            "ALTER PUBLICATION {} ADD TABLE public.awsdms_ddl_audit",
            quote_ident(&config.publication_name)
        );
        client.batch_execute(&sql).await?;
    }

    Ok(())
}

async fn compute_partitions(
    client: &Client,
    schema: &str,
    table: &str,
    rows_per_partition: u64,
) -> anyhow::Result<u64> {
    let row = client
        .query_one(
            "SELECT COALESCE(relpages, 0)::bigint, \
             GREATEST(reltuples, 0)::float8 \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = $1 AND n.nspname = $2",
            &[&table, &schema],
        )
        .await?;

    let relpages: i64 = row.get(0);
    let reltuples: f64 = row.get(1);

    if relpages == 0 {
        return Ok(1);
    }

    let rows_per_page = reltuples / relpages as f64;

    let pages_per_partition = if rows_per_page > 0.0 {
        (rows_per_partition as f64 / rows_per_page).ceil() as u64
    } else {
        relpages as u64
    };

    let total = (relpages as u64).max(1).div_ceil(pages_per_partition);
    Ok(total.max(1))
}

/// Re-discover PKs from pg_index for all configured tables on startup.
/// This makes the service stateless — it never depends on previously stored PK info
/// except as a cache that we refresh.
async fn refresh_pk_columns(config: &SourceConfig, metadata: &MetadataStore) -> anyhow::Result<()> {
    for (schema, table) in config.table_list() {
        let pk_cols = discover_or_config_pk(config, metadata, &schema, &table).await?;
        if !pk_cols.is_empty() {
            metadata
                .upsert_table_state(
                    &schema,
                    &table,
                    // Don't change the phase — use whatever is persisted
                    TablePhase::Pending, // upsert_table_state preserves existing phase on conflict
                    None,
                    None,
                    &pk_cols,
                )
                .await?;
        }
    }
    Ok(())
}

async fn discover_or_config_pk(
    config: &SourceConfig,
    metadata: &MetadataStore,
    schema: &str,
    table: &str,
) -> anyhow::Result<Vec<String>> {
    let from_config = config.pk_for_table(schema, table);
    if !from_config.is_empty() {
        return Ok(from_config);
    }
    let from_pg = metadata.discover_primary_keys(schema, table).await?;
    if from_pg.is_empty() {
        warn!(
            schema,
            table, "No primary key found — CDC merges will not work correctly"
        );
    }
    Ok(from_pg)
}
