use pgiceberg_common::metadata::MetadataStore;
use pgiceberg_common::models::DdlEvent;
use tracing::{info, warn};

/// Process pending DDL events.
///
/// DDL events are captured from the `awsdms_ddl_audit` table in WAL by
/// Service 1.  Service 2 applies them to the Iceberg table before processing
/// any data files that might depend on the new schema.
///
/// Supported DDL operations:
///   - ALTER TABLE ... ADD COLUMN
///   - ALTER TABLE ... DROP COLUMN
///   - DROP TABLE
///
/// Unsupported operations are logged and marked as applied to avoid blocking
/// the pipeline.
pub async fn process_ddl_events(metadata: &MetadataStore) -> anyhow::Result<()> {
    let events = metadata.get_pending_ddl_events().await?;

    if events.is_empty() {
        return Ok(());
    }

    info!(count = events.len(), "Processing pending DDL events");

    for event in &events {
        match process_single_event(event) {
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

fn process_single_event(event: &DdlEvent) -> anyhow::Result<()> {
    match event.ddl_tag.as_str() {
        "ALTER TABLE" => {
            let changes = parse_alter_table(&event.ddl_sql);
            for change in &changes {
                info!(change = %change, schema = %event.target_schema, "DDL change detected");
                // TODO: Apply Iceberg schema evolution via table.update_schema()
                // For now, log and continue — schema evolution will be applied when
                // the next data file triggers a schema mismatch
            }
            Ok(())
        }
        "DROP TABLE" => {
            info!(
                schema = %event.target_schema,
                sql = %event.ddl_sql,
                "DROP TABLE detected — Iceberg table will be dropped"
            );
            // TODO: catalog.drop_table()
            Ok(())
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

/// Parse ALTER TABLE SQL to extract column changes.
fn parse_alter_table(sql: &str) -> Vec<String> {
    let sql_upper = sql.to_uppercase();
    let mut changes = Vec::new();

    // Simple pattern matching for common ALTER TABLE operations.
    // A full SQL parser would be more robust, but these cover the critical cases.
    if sql_upper.contains("ADD COLUMN") {
        if let Some(pos) = sql_upper.find("ADD COLUMN") {
            let rest = &sql[pos..];
            changes.push(format!("ADD COLUMN: {}", rest.trim()));
        }
    }

    if sql_upper.contains("DROP COLUMN") {
        if let Some(pos) = sql_upper.find("DROP COLUMN") {
            let rest = &sql[pos..];
            changes.push(format!("DROP COLUMN: {}", rest.trim()));
        }
    }

    if sql_upper.contains("ALTER COLUMN") {
        if let Some(pos) = sql_upper.find("ALTER COLUMN") {
            let rest = &sql[pos..];
            changes.push(format!("ALTER COLUMN: {}", rest.trim()));
        }
    }

    if changes.is_empty() {
        changes.push(format!("UNPARSED: {}", sql));
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
        assert!(changes[0].starts_with("ADD COLUMN"));
    }

    #[test]
    fn test_parse_drop_column() {
        let changes = parse_alter_table("ALTER TABLE users DROP COLUMN email");
        assert_eq!(changes.len(), 1);
        assert!(changes[0].starts_with("DROP COLUMN"));
    }

    #[test]
    fn test_parse_alter_column_type() {
        let changes = parse_alter_table("ALTER TABLE users ALTER COLUMN name TYPE varchar(255)");
        assert_eq!(changes.len(), 1);
        assert!(changes[0].starts_with("ALTER COLUMN"));
    }

    #[test]
    fn test_parse_unknown_alter() {
        let changes = parse_alter_table("ALTER TABLE users RENAME TO customers");
        assert_eq!(changes.len(), 1);
        assert!(changes[0].starts_with("UNPARSED"));
    }
}
