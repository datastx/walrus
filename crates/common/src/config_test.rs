use super::*;

#[test]
fn test_parse_minimal_config() {
    let toml_str = r#"
[source]
host = "localhost"
port = 5432
database = "mydb"
user = "repl"

[staging]
root = "/tmp/staging"
"#;
    let config: AppConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.source.host, "localhost");
    assert_eq!(config.source.port, 5432);
    assert_eq!(config.staging.root, PathBuf::from("/tmp/staging"));
    assert_eq!(config.wal_capture.max_batch_rows, 50_000);
    assert_eq!(config.wal_capture.max_batch_bytes, 64 * 1024 * 1024);
    assert_eq!(config.wal_capture.flush_interval_seconds, 30);
}

#[test]
fn test_table_list_parsing() {
    let toml_str = r#"
[source]
host = "localhost"
port = 5432
database = "mydb"
user = "repl"

[source.tables]
"public.users" = {}
"myschema.orders" = { pk = ["id"] }

[staging]
root = "/tmp/staging"
"#;
    let config: AppConfig = toml::from_str(toml_str).unwrap();
    let tables = config.source.table_list();
    assert_eq!(tables.len(), 2);
    assert_eq!(config.source.pk_for_table("myschema", "orders"), vec!["id"]);
}

#[test]
fn test_default_values() {
    let wc = WalCaptureConfig::default();
    assert_eq!(wc.max_batch_rows, 50_000);
    assert_eq!(wc.max_batch_bytes, 64 * 1024 * 1024);
    assert_eq!(wc.flush_interval_seconds, 30);
    assert_eq!(wc.max_parallel_tables, 4);
}
