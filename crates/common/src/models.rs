use crate::error::WalrusError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Postgres LSN represented as a u64 for arithmetic.
/// Display and parse use the standard `X/Y` hex format.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);

    pub fn parse(s: &str) -> Result<Self, WalrusError> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return Err(WalrusError::InvalidLsn(s.to_string()));
        }
        let hi = u64::from_str_radix(parts[0], 16)
            .map_err(|_| WalrusError::InvalidLsn(s.to_string()))?;
        let lo = u64::from_str_radix(parts[1], 16)
            .map_err(|_| WalrusError::InvalidLsn(s.to_string()))?;
        Ok(Lsn((hi << 32) | lo))
    }
}

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TablePhase {
    Pending,
    Backfilling,
    Streaming,
}

impl TablePhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            TablePhase::Pending => "pending",
            TablePhase::Backfilling => "backfilling",
            TablePhase::Streaming => "streaming",
        }
    }
}

impl std::str::FromStr for TablePhase {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "backfilling" => TablePhase::Backfilling,
            "streaming" => TablePhase::Streaming,
            _ => TablePhase::Pending,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileStatus {
    Pending,
    Processing,
    Completed,
    Deleted,
    Failed,
}

impl FileStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            FileStatus::Pending => "pending",
            FileStatus::Processing => "processing",
            FileStatus::Completed => "completed",
            FileStatus::Deleted => "deleted",
            FileStatus::Failed => "failed",
        }
    }
}

impl std::str::FromStr for FileStatus {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "processing" => FileStatus::Processing,
            "completed" => FileStatus::Completed,
            "deleted" => FileStatus::Deleted,
            "failed" => FileStatus::Failed,
            _ => FileStatus::Pending,
        })
    }
}

/// Row from `_pgiceberg.replication_state`
#[derive(Debug, Clone)]
pub struct ReplicationState {
    pub slot_name: String,
    pub publication_name: String,
    pub consistent_point: Option<Lsn>,
    pub snapshot_name: Option<String>,
    pub last_flushed_lsn: Option<Lsn>,
    pub last_acked_lsn: Option<Lsn>,
}

/// Row from `_pgiceberg.table_state`
#[derive(Debug, Clone)]
pub struct TableState {
    pub table_schema: String,
    pub table_name: String,
    pub phase: TablePhase,
    pub backfill_total_partitions: Option<i32>,
    pub backfill_done_partitions: i32,
    pub backfill_snapshot_name: Option<String>,
    pub last_committed_lsn: Option<Lsn>,
    pub iceberg_schema_version: i32,
    pub primary_key_columns: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    Backfill,
    CdcMixed,
}

impl FileType {
    pub fn as_str(&self) -> &'static str {
        match self {
            FileType::Backfill => "backfill",
            FileType::CdcMixed => "cdc_mixed",
        }
    }
}

impl std::fmt::Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for FileType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "backfill" => FileType::Backfill,
            _ => FileType::CdcMixed,
        })
    }
}

/// Row from `_pgiceberg.file_queue`
#[derive(Debug, Clone)]
pub struct FileQueueEntry {
    pub file_id: Uuid,
    pub table_schema: String,
    pub table_name: String,
    pub file_type: FileType,
    pub file_path: String,
    pub lsn_low: Option<Lsn>,
    pub lsn_high: Option<Lsn>,
    pub row_count: i64,
    pub partition_id: Option<i32>,
    pub status: FileStatus,
    pub created_at: DateTime<Utc>,
    pub retry_count: i32,
    pub error_message: Option<String>,
}

/// Row from `_pgiceberg.ddl_events`
#[derive(Debug, Clone)]
pub struct DdlEvent {
    pub event_id: i64,
    pub source_txn: Option<String>,
    pub ddl_tag: String,
    pub target_schema: String,
    pub target_table: String,
    pub ddl_sql: String,
    pub applied_to_iceberg: bool,
}

/// A single CDC record decoded from WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcRecord {
    pub table_schema: String,
    pub table_name: String,
    pub op: CdcOp,
    pub columns: Vec<CdcColumn>,
    /// Estimated byte size of this record (for memory-based flush threshold).
    pub estimated_bytes: usize,
    pub commit_lsn: Lsn,
    pub commit_ts: i64,
    /// Monotonic sequence number within a transaction for deterministic dedup ordering.
    pub seq: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcOp {
    Insert,
    Update,
    Delete,
}

impl CdcOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            CdcOp::Insert => "I",
            CdcOp::Update => "U",
            CdcOp::Delete => "D",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcColumn {
    pub name: String,
    pub type_oid: u32,
    pub value: Option<Vec<u8>>,
}

/// CTID partition for backfill scanning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CtidPartition {
    pub id: i32,
    pub start_page: u64,
    pub end_page: u64,
}

/// Relation metadata cached from WAL RelationMessages.
#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub replica_identity: ReplicaIdentity,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
    pub is_key: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}

#[cfg(test)]
#[path = "models_test.rs"]
mod models_test;
