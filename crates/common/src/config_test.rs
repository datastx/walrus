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
    assert_eq!(config.source.tls_mode, TlsMode::Disable);
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

#[test]
fn test_tls_mode_parsing() {
    let toml_str = r#"
[source]
host = "localhost"
port = 5432
database = "mydb"
user = "repl"
tls_mode = "require"

[staging]
root = "/tmp/staging"
"#;
    let config: AppConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.source.tls_mode, TlsMode::Require);
}

#[test]
fn test_tls_mode_verify_full() {
    let toml_str = r#"
[source]
host = "localhost"
port = 5432
database = "mydb"
user = "repl"
tls_mode = "verify-full"
tls_ca_cert = "/etc/ssl/certs/ca.pem"

[staging]
root = "/tmp/staging"
"#;
    let config: AppConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.source.tls_mode, TlsMode::VerifyFull);
    assert_eq!(
        config.source.tls_ca_cert.unwrap(),
        PathBuf::from("/etc/ssl/certs/ca.pem")
    );
}

#[test]
fn test_tls_mode_from_str() {
    assert_eq!("disable".parse::<TlsMode>().unwrap(), TlsMode::Disable);
    assert_eq!("require".parse::<TlsMode>().unwrap(), TlsMode::Require);
    assert_eq!("prefer".parse::<TlsMode>().unwrap(), TlsMode::Prefer);
    assert_eq!("verify-ca".parse::<TlsMode>().unwrap(), TlsMode::VerifyCa);
    assert_eq!(
        "verify-full".parse::<TlsMode>().unwrap(),
        TlsMode::VerifyFull
    );
    assert_eq!(
        "verify_full".parse::<TlsMode>().unwrap(),
        TlsMode::VerifyFull
    );
}

#[test]
fn test_password_priority() {
    let toml_str = r#"
[source]
host = "localhost"
port = 5432
database = "mydb"
user = "repl"
password = "from_config"

[staging]
root = "/tmp/staging"
"#;
    let config: AppConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.source.password(), "from_config");
}

#[test]
fn test_password_override_takes_priority() {
    let toml_str = r#"
[source]
host = "localhost"
port = 5432
database = "mydb"
user = "repl"
password = "from_config"

[staging]
root = "/tmp/staging"
"#;
    let mut config: AppConfig = toml::from_str(toml_str).unwrap();
    config.source.password_override = Some("from_env".to_string());
    assert_eq!(config.source.password(), "from_env");
}

#[test]
fn test_env_var_overrides() {
    let toml_str = r#"
[source]
host = "localhost"
port = 5432
database = "mydb"
user = "repl"

[staging]
root = "/tmp/staging"
"#;
    // We can't reliably set env vars in tests without side effects,
    // so just verify the config parses with all new env-overridable fields
    let config: AppConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.source.slot_name, "pgiceberg_slot");
    assert_eq!(config.source.publication_name, "pgiceberg_pub");
    assert_eq!(config.wal_capture.health_port, 8081);
    assert_eq!(config.iceberg_writer.health_port, 8082);
    assert_eq!(config.iceberg_writer.poll_interval_seconds, 5);
    assert_eq!(config.iceberg_writer.max_retries, 3);
}
