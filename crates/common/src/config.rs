use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub source: SourceConfig,
    pub staging: StagingConfig,
    #[serde(default)]
    pub wal_capture: WalCaptureConfig,
    #[serde(default)]
    pub iceberg_writer: IcebergWriterConfig,
}

/// TLS mode for Postgres connections.
///
/// Maps to PostgreSQL's `sslmode` parameter:
///   - `disable`: No TLS
///   - `prefer`: Try TLS, fall back to plaintext
///   - `require`: Require TLS, skip certificate verification
///   - `verify-ca`: Require TLS + verify CA chain
///   - `verify-full`: Require TLS + verify CA chain + hostname
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TlsMode {
    #[default]
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl std::str::FromStr for TlsMode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disable" | "disabled" | "none" => Ok(TlsMode::Disable),
            "prefer" => Ok(TlsMode::Prefer),
            "require" => Ok(TlsMode::Require),
            "verify-ca" | "verify_ca" => Ok(TlsMode::VerifyCa),
            "verify-full" | "verify_full" => Ok(TlsMode::VerifyFull),
            _ => Err(anyhow::anyhow!("Invalid TLS mode: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    /// Direct password (set by env var processing or config file).
    #[serde(default)]
    pub password: Option<String>,
    /// Legacy: name of the env var that holds the password.
    /// Kept for backward compatibility but prefer `password` or PG_PASSWORD env var.
    #[serde(default = "default_password_env")]
    pub password_env: String,
    /// Direct password override (set by env var processing, not config file).
    #[serde(default)]
    pub password_override: Option<String>,
    #[serde(default = "default_slot_name")]
    pub slot_name: String,
    #[serde(default = "default_publication_name")]
    pub publication_name: String,
    #[serde(default)]
    pub tables: HashMap<String, TableConfig>,
    /// TLS mode for Postgres connections.
    #[serde(default)]
    pub tls_mode: TlsMode,
    /// Path to CA certificate PEM file for TLS verification.
    /// If not set and tls_mode is verify-ca or verify-full, system/Mozilla roots are used.
    #[serde(default)]
    pub tls_ca_cert: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TableConfig {
    #[serde(default)]
    pub pk: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StagingConfig {
    pub root: PathBuf,
    #[serde(default = "default_cleanup_after_hours")]
    pub cleanup_after_hours: u32,
}

/// CDC flush policy: a flush triggers when ANY of the three thresholds is
/// reached. Set a value to 0 to disable that particular trigger.
#[derive(Debug, Clone, Deserialize)]
pub struct WalCaptureConfig {
    #[serde(default = "default_max_parallel_tables")]
    pub max_parallel_tables: usize,
    #[serde(default = "default_max_parallel_workers")]
    pub max_parallel_workers_per_table: usize,
    #[serde(default = "default_rows_per_partition")]
    pub rows_per_partition: u64,

    /// Flush when the in-memory buffer reaches this many rows (0 = disabled).
    #[serde(default = "default_max_batch_rows")]
    pub max_batch_rows: usize,
    /// Flush when the estimated in-memory buffer size reaches this many bytes (0 = disabled).
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: usize,
    /// Flush after this many seconds since the last flush (0 = disabled).
    #[serde(default = "default_flush_interval_seconds")]
    pub flush_interval_seconds: u64,
    /// Keepalive / standby status interval.
    #[serde(default = "default_idle_timeout_seconds")]
    pub idle_timeout_seconds: u64,

    /// Memory threshold (bytes) before spilling an in-flight transaction to disk.
    /// Prevents OOM from large transactions. Set to 0 to disable spilling.
    #[serde(default = "default_max_txn_memory_bytes")]
    pub max_txn_memory_bytes: usize,

    /// Enable pgoutput protocol v2 streaming for large transactions.
    /// Requires PostgreSQL 14+. When enabled, PG streams large in-progress
    /// transactions incrementally instead of buffering until commit, reducing
    /// WAL retention and server-side memory pressure.
    #[serde(default)]
    pub streaming: bool,

    #[serde(default = "default_health_port_capture")]
    pub health_port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IcebergWriterConfig {
    pub warehouse_path: PathBuf,
    pub catalog_db_path: PathBuf,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_seconds: u64,
    #[serde(default = "default_max_files_per_batch")]
    pub max_files_per_batch: i64,
    #[serde(default = "default_compaction_interval")]
    pub compaction_interval_hours: u32,
    #[serde(default = "default_compaction_threshold")]
    pub compaction_delete_threshold: u32,
    #[serde(default = "default_max_retries")]
    pub max_retries: i32,
    #[serde(default = "default_health_port_writer")]
    pub health_port: u16,
}

impl SourceConfig {
    pub fn password(&self) -> String {
        // Priority: password_override (from PG_PASSWORD env) > password (from config) > password_env indirection
        if let Some(ref pw) = self.password_override {
            return pw.clone();
        }
        if let Some(ref pw) = self.password {
            return pw.clone();
        }
        std::env::var(&self.password_env).unwrap_or_default()
    }

    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={}",
            self.host,
            self.port,
            self.database,
            self.user,
            self.password()
        )
    }

    pub fn table_list(&self) -> Vec<(String, String)> {
        self.tables
            .keys()
            .map(|full| {
                let parts: Vec<&str> = full.splitn(2, '.').collect();
                if parts.len() == 2 {
                    (parts[0].to_string(), parts[1].to_string())
                } else {
                    ("public".to_string(), parts[0].to_string())
                }
            })
            .collect()
    }

    pub fn pk_for_table(&self, schema: &str, table: &str) -> Vec<String> {
        let key = format!("{}.{}", schema, table);
        self.tables
            .get(&key)
            .map(|t| t.pk.clone())
            .unwrap_or_default()
    }
}

impl AppConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let mut config: AppConfig = toml::from_str(&content)?;
        Self::apply_env_overrides(&mut config);
        Ok(config)
    }

    fn apply_env_overrides(config: &mut AppConfig) {
        // ── Source connection ──
        if let Ok(v) = std::env::var("PG_HOST") {
            config.source.host = v;
        }
        if let Ok(v) = std::env::var("PG_PORT") {
            if let Ok(p) = v.parse() {
                config.source.port = p;
            }
        }
        if let Ok(v) = std::env::var("PG_DATABASE") {
            config.source.database = v;
        }
        if let Ok(v) = std::env::var("PG_USER") {
            config.source.user = v;
        }
        if let Ok(v) = std::env::var("PG_PASSWORD") {
            config.source.password_override = Some(v);
        }
        if let Ok(v) = std::env::var("PG_SLOT_NAME") {
            config.source.slot_name = v;
        }
        if let Ok(v) = std::env::var("PG_PUBLICATION_NAME") {
            config.source.publication_name = v;
        }

        // ── TLS ──
        if let Ok(v) = std::env::var("PG_TLS_MODE") {
            if let Ok(mode) = v.parse() {
                config.source.tls_mode = mode;
            }
        }
        if let Ok(v) = std::env::var("PG_TLS_CA_CERT") {
            config.source.tls_ca_cert = Some(PathBuf::from(v));
        }

        // ── Staging ──
        if let Ok(v) = std::env::var("STAGING_ROOT") {
            config.staging.root = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("CLEANUP_AFTER_HOURS") {
            if let Ok(h) = v.parse() {
                config.staging.cleanup_after_hours = h;
            }
        }

        // ── WAL Capture tuning ──
        if let Ok(v) = std::env::var("MAX_BATCH_ROWS") {
            if let Ok(n) = v.parse() {
                config.wal_capture.max_batch_rows = n;
            }
        }
        if let Ok(v) = std::env::var("MAX_BATCH_BYTES") {
            if let Ok(n) = v.parse() {
                config.wal_capture.max_batch_bytes = n;
            }
        }
        if let Ok(v) = std::env::var("FLUSH_INTERVAL_SECONDS") {
            if let Ok(n) = v.parse() {
                config.wal_capture.flush_interval_seconds = n;
            }
        }
        if let Ok(v) = std::env::var("MAX_PARALLEL_TABLES") {
            if let Ok(n) = v.parse() {
                config.wal_capture.max_parallel_tables = n;
            }
        }
        if let Ok(v) = std::env::var("ROWS_PER_PARTITION") {
            if let Ok(n) = v.parse() {
                config.wal_capture.rows_per_partition = n;
            }
        }
        if let Ok(v) = std::env::var("MAX_TXN_MEMORY_BYTES") {
            if let Ok(n) = v.parse() {
                config.wal_capture.max_txn_memory_bytes = n;
            }
        }
        if let Ok(v) = std::env::var("CDC_STREAMING") {
            config.wal_capture.streaming = matches!(v.to_lowercase().as_str(), "true" | "1" | "on");
        }
        if let Ok(v) = std::env::var("WAL_CAPTURE_HEALTH_PORT") {
            if let Ok(n) = v.parse() {
                config.wal_capture.health_port = n;
            }
        }

        // ── Iceberg Writer tuning ──
        if let Ok(v) = std::env::var("WAREHOUSE_PATH") {
            config.iceberg_writer.warehouse_path = PathBuf::from(v.clone());
            config.iceberg_writer.catalog_db_path = PathBuf::from(format!("{}/catalog.db", v));
        }
        if let Ok(v) = std::env::var("POLL_INTERVAL_SECONDS") {
            if let Ok(n) = v.parse() {
                config.iceberg_writer.poll_interval_seconds = n;
            }
        }
        if let Ok(v) = std::env::var("MAX_FILES_PER_BATCH") {
            if let Ok(n) = v.parse() {
                config.iceberg_writer.max_files_per_batch = n;
            }
        }
        if let Ok(v) = std::env::var("MAX_RETRIES") {
            if let Ok(n) = v.parse() {
                config.iceberg_writer.max_retries = n;
            }
        }
        if let Ok(v) = std::env::var("ICEBERG_WRITER_HEALTH_PORT") {
            if let Ok(n) = v.parse() {
                config.iceberg_writer.health_port = n;
            }
        }
    }
}

impl Default for WalCaptureConfig {
    fn default() -> Self {
        Self {
            max_parallel_tables: default_max_parallel_tables(),
            max_parallel_workers_per_table: default_max_parallel_workers(),
            rows_per_partition: default_rows_per_partition(),
            max_batch_rows: default_max_batch_rows(),
            max_batch_bytes: default_max_batch_bytes(),
            flush_interval_seconds: default_flush_interval_seconds(),
            idle_timeout_seconds: default_idle_timeout_seconds(),
            max_txn_memory_bytes: default_max_txn_memory_bytes(),
            streaming: false,
            health_port: default_health_port_capture(),
        }
    }
}

impl Default for IcebergWriterConfig {
    fn default() -> Self {
        Self {
            warehouse_path: PathBuf::from("/data/iceberg"),
            catalog_db_path: PathBuf::from("/data/iceberg/catalog.db"),
            poll_interval_seconds: default_poll_interval(),
            max_files_per_batch: default_max_files_per_batch(),
            compaction_interval_hours: default_compaction_interval(),
            compaction_delete_threshold: default_compaction_threshold(),
            max_retries: default_max_retries(),
            health_port: default_health_port_writer(),
        }
    }
}

fn default_password_env() -> String {
    "PG_PASSWORD".to_string()
}
fn default_slot_name() -> String {
    "pgiceberg_slot".to_string()
}
fn default_publication_name() -> String {
    "pgiceberg_pub".to_string()
}
fn default_cleanup_after_hours() -> u32 {
    24
}
fn default_max_parallel_tables() -> usize {
    4
}
fn default_max_parallel_workers() -> usize {
    2
}
fn default_rows_per_partition() -> u64 {
    500_000
}
fn default_max_batch_rows() -> usize {
    50_000
}
fn default_max_batch_bytes() -> usize {
    64 * 1024 * 1024
}
fn default_flush_interval_seconds() -> u64 {
    30
}
fn default_idle_timeout_seconds() -> u64 {
    60
}
fn default_poll_interval() -> u64 {
    5
}
fn default_max_files_per_batch() -> i64 {
    100
}
fn default_compaction_interval() -> u32 {
    6
}
fn default_compaction_threshold() -> u32 {
    50
}
fn default_max_retries() -> i32 {
    3
}
fn default_max_txn_memory_bytes() -> usize {
    128 * 1024 * 1024 // 128 MB
}
fn default_health_port_capture() -> u16 {
    8081
}
fn default_health_port_writer() -> u16 {
    8082
}

#[cfg(test)]
#[path = "config_test.rs"]
mod config_test;
