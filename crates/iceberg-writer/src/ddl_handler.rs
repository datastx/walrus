use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_sql::SqlCatalog;
use pgiceberg_common::metadata::MetadataStore;
use pgiceberg_common::models::DdlEvent;
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
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
/// Schema evolution status:
///   - DROP TABLE: fully supported via `catalog.drop_table()`
///   - ALTER TABLE ADD/DROP/ALTER COLUMN: parsed and the new schema is constructed,
///     but cannot be committed because iceberg-rust 0.8.0's `TableCommit` builder
///     is `pub(crate)`. The constructed schema is logged for visibility.
///     When iceberg-rust exposes schema evolution (either through Transaction or by
///     making TableCommit builder public), the apply_schema_evolution function below
///     has the full implementation ready.
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
        // Crash guard: claim before processing
        if !metadata.claim_ddl_event(event.event_id).await? {
            continue; // Already claimed by another instance
        }

        // Skip irrelevant DDL types
        if should_skip_ddl(&event.ddl_tag) {
            metadata.mark_ddl_skipped(event.event_id).await?;
            info!(
                event_id = event.event_id,
                tag = %event.ddl_tag,
                "DDL event skipped — not relevant to Iceberg schema"
            );
            continue;
        }

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
                    "DDL event failed — table CDC pipeline blocked until resolved"
                );
                metadata
                    .mark_ddl_failed(event.event_id, &e.to_string())
                    .await?;
            }
        }
    }

    Ok(())
}

async fn process_single_event(event: &DdlEvent, catalog: &SqlCatalog) -> anyhow::Result<()> {
    match event.ddl_tag.as_str() {
        "ALTER TABLE" => {
            let changes = parse_alter_table(&event.ddl_sql);
            let namespace = NamespaceIdent::new(event.target_schema.clone());
            let table_ident = TableIdent::new(namespace, event.target_table.clone());

            match catalog.load_table(&table_ident).await {
                Ok(table) => {
                    let current_schema = table.metadata().current_schema();
                    match build_evolved_schema(current_schema, &changes) {
                        Ok(new_schema) => {
                            info!(
                                schema = %event.target_schema,
                                table = %event.target_table,
                                changes = ?changes.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
                                new_field_count = new_schema.as_struct().fields().len(),
                                "Schema evolution computed — cannot commit yet (iceberg-rust 0.8.0 \
                                 does not expose TableCommit builder). Schema will sync on next \
                                 table recreation."
                            );
                            // When iceberg-rust adds public schema evolution, uncomment:
                            // apply_schema_evolution(catalog, &table_ident, new_schema).await?;
                        }
                        Err(e) => {
                            warn!(
                                schema = %event.target_schema,
                                table = %event.target_table,
                                error = %e,
                                "Failed to construct evolved schema"
                            );
                        }
                    }
                }
                Err(e) => {
                    info!(
                        schema = %event.target_schema,
                        table = %event.target_table,
                        error = %e,
                        "Table not found in Iceberg — skipping schema evolution"
                    );
                }
            }
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

/// Build an evolved Iceberg schema by applying DDL changes to the current schema.
///
/// This constructs the new schema that would result from applying the ALTER TABLE
/// changes. The schema can be committed when iceberg-rust exposes schema evolution.
fn build_evolved_schema(current: &Schema, changes: &[DdlColumnChange]) -> anyhow::Result<Schema> {
    let mut fields: Vec<Arc<NestedField>> = current.as_struct().fields().to_vec();
    let mut next_id = fields.iter().map(|f| f.id).max().unwrap_or(0) + 1;
    let pk_ids: Vec<i32> = current.identifier_field_ids().collect();

    for change in changes {
        match change {
            DdlColumnChange::AddColumn {
                column_name,
                data_type,
            } => {
                let iceberg_type = sql_type_to_iceberg(data_type);
                fields.push(Arc::new(NestedField::optional(
                    next_id,
                    column_name,
                    iceberg_type,
                )));
                next_id += 1;
            }
            DdlColumnChange::DropColumn { column_name } => {
                fields.retain(|f| f.name != *column_name);
            }
            DdlColumnChange::AlterColumn {
                column_name,
                new_type,
            } => {
                if let Some(new_type_str) = new_type {
                    let iceberg_type = sql_type_to_iceberg(new_type_str);
                    if let Some(field) = fields.iter_mut().find(|f| f.name == *column_name) {
                        let f = Arc::make_mut(field);
                        *f.field_type = iceberg_type;
                    }
                }
            }
            DdlColumnChange::Unparsed(_) => {
                // Skip unparsed changes
            }
        }
    }

    let valid_pk_ids: Vec<i32> = pk_ids
        .into_iter()
        .filter(|id| fields.iter().any(|f| f.id == *id))
        .collect();

    Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids(valid_pk_ids)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build evolved schema: {}", e))
}

/// Map SQL type strings to Iceberg types (for DDL-driven schema evolution).
fn sql_type_to_iceberg(sql_type: &str) -> Type {
    match sql_type.to_uppercase().as_str() {
        "BOOLEAN" | "BOOL" => Type::Primitive(PrimitiveType::Boolean),
        "SMALLINT" | "INT2" => Type::Primitive(PrimitiveType::Int),
        "INTEGER" | "INT" | "INT4" => Type::Primitive(PrimitiveType::Int),
        "BIGINT" | "INT8" => Type::Primitive(PrimitiveType::Long),
        "REAL" | "FLOAT4" => Type::Primitive(PrimitiveType::Float),
        "DOUBLE PRECISION" | "FLOAT8" => Type::Primitive(PrimitiveType::Double),
        "TEXT" | "VARCHAR" | "CHARACTER VARYING" => Type::Primitive(PrimitiveType::String),
        "BYTEA" => Type::Primitive(PrimitiveType::Binary),
        "DATE" => Type::Primitive(PrimitiveType::Date),
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => Type::Primitive(PrimitiveType::Timestamp),
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => Type::Primitive(PrimitiveType::Timestamptz),
        "UUID" => Type::Primitive(PrimitiveType::Uuid),
        "JSON" | "JSONB" => Type::Primitive(PrimitiveType::String),
        s if s.starts_with("NUMERIC") || s.starts_with("DECIMAL") => {
            Type::Primitive(PrimitiveType::Decimal {
                precision: 38,
                scale: 18,
            })
        }
        s if s.starts_with("VARCHAR") || s.starts_with("CHARACTER VARYING") => {
            Type::Primitive(PrimitiveType::String)
        }
        _ => Type::Primitive(PrimitiveType::String),
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

// NOTE: This function is ready for when iceberg-rust 0.8.x+ exposes schema evolution.
// Currently blocked because TableCommit::builder().build() is pub(crate).
//
// async fn apply_schema_evolution(
//     catalog: &SqlCatalog,
//     table_ident: &TableIdent,
//     new_schema: Schema,
// ) -> anyhow::Result<()> {
//     use iceberg::catalog::{TableCommit, TableUpdate};
//     let commit = TableCommit::builder()
//         .ident(table_ident.clone())
//         .updates(vec![
//             TableUpdate::AddSchema { schema: new_schema },
//             TableUpdate::SetCurrentSchema { schema_id: -1 },
//         ])
//         .requirements(vec![])
//         .build();
//     catalog.update_table(commit).await?;
//     Ok(())
// }

/// Returns true for DDL tags that don't affect Iceberg schema and can be skipped.
fn should_skip_ddl(ddl_tag: &str) -> bool {
    matches!(
        ddl_tag,
        "CREATE TABLE"
            | "CREATE TABLE AS"
            | "CREATE INDEX"
            | "DROP INDEX"
            | "RENAME TABLE"
            | "COMMENT"
            | "CREATE SEQUENCE"
            | "ALTER SEQUENCE"
            | "DROP SEQUENCE"
            | "CREATE TRIGGER"
            | "DROP TRIGGER"
            | "CREATE RULE"
            | "DROP RULE"
    )
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
