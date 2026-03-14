use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_sql::SqlCatalog;
use pgiceberg_common::metadata::MetadataStore;
use pgiceberg_common::models::DdlEvent;
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

/// Parse ALTER TABLE SQL to extract structured column changes.
pub fn parse_alter_table(sql: &str) -> Vec<DdlColumnChange> {
    let sql_upper = sql.to_uppercase();
    let mut changes = Vec::new();

    if sql_upper.contains("ADD COLUMN") {
        if let Some(pos) = sql_upper.find("ADD COLUMN") {
            let rest = &sql[pos + "ADD COLUMN".len()..].trim_start();
            let tokens: Vec<&str> = rest.splitn(3, char::is_whitespace).collect();
            if tokens.len() >= 2 {
                let col_name = tokens[0].trim_matches('"');
                let data_type = if tokens.len() >= 3 {
                    format!("{} {}", tokens[1], tokens[2])
                        .trim_end_matches(';')
                        .trim()
                        .to_string()
                } else {
                    tokens[1].trim_end_matches(';').to_string()
                };
                changes.push(DdlColumnChange::AddColumn {
                    column_name: col_name.to_string(),
                    data_type,
                });
            }
        }
    }

    if sql_upper.contains("DROP COLUMN") {
        if let Some(pos) = sql_upper.find("DROP COLUMN") {
            let rest = &sql[pos + "DROP COLUMN".len()..].trim_start();
            let col_name = rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('"')
                .trim_end_matches(';');
            if !col_name.is_empty() {
                changes.push(DdlColumnChange::DropColumn {
                    column_name: col_name.to_string(),
                });
            }
        }
    }

    if sql_upper.contains("ALTER COLUMN") {
        if let Some(pos) = sql_upper.find("ALTER COLUMN") {
            let rest = &sql[pos + "ALTER COLUMN".len()..].trim_start();
            let tokens: Vec<&str> = rest.split_whitespace().collect();
            if !tokens.is_empty() {
                let col_name = tokens[0].trim_matches('"');
                let new_type = if let Some(type_pos) = sql_upper[pos..].find("TYPE") {
                    let type_rest = &sql[pos + type_pos + "TYPE".len()..].trim_start();
                    Some(type_rest.split(';').next().unwrap_or("").trim().to_string())
                } else {
                    None
                };
                changes.push(DdlColumnChange::AlterColumn {
                    column_name: col_name.to_string(),
                    new_type,
                });
            }
        }
    }

    if changes.is_empty() {
        changes.push(DdlColumnChange::Unparsed(sql.to_string()));
    }

    changes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_add_column() {
        let changes = parse_alter_table("ALTER TABLE users ADD COLUMN age INT");
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            DdlColumnChange::AddColumn {
                column_name,
                data_type,
            } => {
                assert_eq!(column_name, "age");
                assert_eq!(data_type, "INT");
            }
            other => panic!("Expected AddColumn, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_add_column_with_type() {
        let changes = parse_alter_table("ALTER TABLE users ADD COLUMN name varchar(255)");
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            DdlColumnChange::AddColumn {
                column_name,
                data_type,
            } => {
                assert_eq!(column_name, "name");
                assert!(data_type.contains("varchar"));
            }
            other => panic!("Expected AddColumn, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_drop_column() {
        let changes = parse_alter_table("ALTER TABLE users DROP COLUMN email");
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            DdlColumnChange::DropColumn { column_name } => {
                assert_eq!(column_name, "email");
            }
            other => panic!("Expected DropColumn, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_alter_column_type() {
        let changes = parse_alter_table("ALTER TABLE users ALTER COLUMN name TYPE varchar(255)");
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            DdlColumnChange::AlterColumn {
                column_name,
                new_type,
            } => {
                assert_eq!(column_name, "name");
                assert!(new_type.as_ref().unwrap().contains("varchar"));
            }
            other => panic!("Expected AlterColumn, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_unknown_alter() {
        let changes = parse_alter_table("ALTER TABLE users RENAME TO customers");
        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], DdlColumnChange::Unparsed(_)));
    }

    #[test]
    fn test_ddl_column_change_display() {
        let add = DdlColumnChange::AddColumn {
            column_name: "age".to_string(),
            data_type: "INT".to_string(),
        };
        assert_eq!(add.to_string(), "ADD COLUMN age INT");

        let drop = DdlColumnChange::DropColumn {
            column_name: "email".to_string(),
        };
        assert_eq!(drop.to_string(), "DROP COLUMN email");
    }
}
