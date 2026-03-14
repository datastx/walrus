use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_sql::SqlCatalog;
use pgiceberg_common::metadata::MetadataStore;
use pgiceberg_common::models::DdlEvent;
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tracing::{info, warn};

/// Parsed column change from ALTER TABLE DDL.
#[derive(Debug, Clone, PartialEq)]
pub enum DdlColumnChange {
    AddColumn {
        column_name: String,
        data_type: String,
    },
    DropColumn {
        column_name: String,
    },
    AlterColumn {
        column_name: String,
        new_type: Option<String>,
    },
    Unparsed(String),
}

impl std::fmt::Display for DdlColumnChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DdlColumnChange::AddColumn {
                column_name,
                data_type,
            } => write!(f, "ADD COLUMN {} {}", column_name, data_type),
            DdlColumnChange::DropColumn { column_name } => {
                write!(f, "DROP COLUMN {}", column_name)
            }
            DdlColumnChange::AlterColumn {
                column_name,
                new_type,
            } => write!(
                f,
                "ALTER COLUMN {} TYPE {}",
                column_name,
                new_type.as_deref().unwrap_or("unknown")
            ),
            DdlColumnChange::Unparsed(sql) => write!(f, "UNPARSED: {}", sql),
        }
    }
}

/// Process pending DDL events, applying schema changes to Iceberg tables.
///
/// Note: iceberg-rust 0.8.0 does not expose schema evolution through its public
/// Transaction API (`TransactionAction` and `TableCommit::builder().build()` are
/// both `pub(crate)`). Therefore:
///   - DROP TABLE: fully supported via `catalog.drop_table()`
///   - ALTER TABLE ADD/DROP/ALTER COLUMN: parsed and logged, but schema evolution
///     is deferred to when iceberg-rust adds a public schema evolution action.
///     The Iceberg table will be updated on the next full re-creation or when a
///     newer iceberg-rust version is adopted.
pub async fn process_ddl_events(
    metadata: &MetadataStore,
    catalog: &SqlCatalog,
) -> anyhow::Result<()> {
    let events = metadata.get_pending_ddl_events().await?;

    if events.is_empty() {
        return Ok(());
    }

    info!(count = events.len(), "Processing pending DDL events");

    for event in &events {
        match process_single_event(event, catalog).await {
            Ok(()) => {
                metadata.mark_ddl_applied(event.event_id).await?;
                info!(
                    event_id = event.event_id,
                    tag = %event.ddl_tag,
                    "DDL event applied"
                );
            }
            Err(e) => {
                warn!(
                    event_id = event.event_id,
                    tag = %event.ddl_tag,
                    error = %e,
                    "DDL event failed — marking as applied to avoid blocking"
                );
                metadata.mark_ddl_applied(event.event_id).await?;
            }
        }
    }

    Ok(())
}

async fn process_single_event(event: &DdlEvent, catalog: &SqlCatalog) -> anyhow::Result<()> {
    match event.ddl_tag.as_str() {
        "ALTER TABLE" => {
            let changes = parse_alter_table(&event.ddl_sql);
            for change in &changes {
                info!(
                    change = %change,
                    schema = %event.target_schema,
                    table = %event.target_table,
                    "DDL change detected — schema evolution not yet supported by iceberg-rust 0.8.0"
                );
            }
            // Schema evolution will be applied when iceberg-rust adds a public
            // UpdateSchema transaction action. Until then, the Iceberg schema
            // diverges from Postgres. New columns will appear as NULL in reads,
            // and dropped columns will have their data preserved.
            Ok(())
        }
        "DROP TABLE" => {
            if event.target_table.is_empty() {
                info!(
                    schema = %event.target_schema,
                    sql = %event.ddl_sql,
                    "DROP TABLE detected but no target_table — skipping Iceberg drop"
                );
                return Ok(());
            }
            apply_drop_table(catalog, &event.target_schema, &event.target_table).await
        }
        "CREATE TABLE" | "CREATE TABLE AS" => {
            info!(
                schema = %event.target_schema,
                "CREATE TABLE detected — will be handled when data arrives"
            );
            Ok(())
        }
        other => {
            warn!(tag = other, "Unhandled DDL type");
            Ok(())
        }
    }
}

/// Drop an Iceberg table from the catalog.
async fn apply_drop_table(
    catalog: &SqlCatalog,
    schema_name: &str,
    table_name: &str,
) -> anyhow::Result<()> {
    let namespace = NamespaceIdent::new(schema_name.to_string());
    let table_ident = TableIdent::new(namespace, table_name.to_string());

    if !catalog.table_exists(&table_ident).await? {
        info!(
            schema = schema_name,
            table = table_name,
            "Iceberg table does not exist — nothing to drop"
        );
        return Ok(());
    }

    catalog.drop_table(&table_ident).await?;
    info!(
        schema = schema_name,
        table = table_name,
        "Dropped Iceberg table"
    );

    Ok(())
}

/// Parse ALTER TABLE SQL to extract structured column changes using sqlparser.
pub fn parse_alter_table(sql: &str) -> Vec<DdlColumnChange> {
    let dialect = PostgreSqlDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return vec![DdlColumnChange::Unparsed(sql.to_string())],
    };

    let mut changes = Vec::new();

    for stmt in stmts {
        if let Statement::AlterTable { operations, .. } = stmt {
            for op in operations {
                use sqlparser::ast::AlterTableOperation;
                match op {
                    AlterTableOperation::AddColumn { column_def, .. } => {
                        changes.push(DdlColumnChange::AddColumn {
                            column_name: column_def.name.value.clone(),
                            data_type: column_def.data_type.to_string(),
                        });
                    }
                    AlterTableOperation::DropColumn { column_name, .. } => {
                        changes.push(DdlColumnChange::DropColumn {
                            column_name: column_name.value.clone(),
                        });
                    }
                    AlterTableOperation::AlterColumn {
                        column_name, op, ..
                    } => {
                        use sqlparser::ast::AlterColumnOperation;
                        let new_type = match op {
                            AlterColumnOperation::SetDataType { data_type, .. } => {
                                Some(data_type.to_string())
                            }
                            _ => None,
                        };
                        changes.push(DdlColumnChange::AlterColumn {
                            column_name: column_name.value.clone(),
                            new_type,
                        });
                    }
                    _ => {
                        changes.push(DdlColumnChange::Unparsed(format!("{}", op)));
                    }
                }
            }
        }
    }

    if changes.is_empty() {
        changes.push(DdlColumnChange::Unparsed(sql.to_string()));
    }

    changes
}

#[cfg(test)]
#[path = "ddl_handler_test.rs"]
mod ddl_handler_test;
