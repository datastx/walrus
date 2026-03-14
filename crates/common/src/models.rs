use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Postgres LSN represented as a u64 for arithmetic.
/// Display and parse use the standard `X/Y` hex format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);

    pub fn parse(s: &str) -> anyhow::Result<Self> {
        let parts: Vec<&str> = s.split('/').collect();
        anyhow::ensure!(parts.len() == 2, "Invalid LSN format: {}", s);
        let hi = u64::from_str_radix(parts[0], 16)?;
        let lo = u64::from_str_radix(parts[1], 16)?;
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

/// Row from `_pgiceberg.file_queue`
#[derive(Debug, Clone)]
pub struct FileQueueEntry {
    pub file_id: Uuid,
    pub table_schema: String,
    pub table_name: String,
    pub file_type: String,
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
#[derive(Debug, Clone)]
pub struct CdcRecord {
    pub table_schema: String,
    pub table_name: String,
    pub op: CdcOp,
    pub columns: Vec<CdcColumn>,
    /// Estimated byte size of this record (for memory-based flush threshold).
    pub estimated_bytes: usize,
    pub commit_lsn: Lsn,
    pub commit_ts: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone)]
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
mod tests {
    use super::*;

    #[test]
    fn test_lsn_parse_and_display() {
        let lsn = Lsn::parse("0/16B6C50").unwrap();
        assert_eq!(lsn.0, 0x16B6C50);
        assert_eq!(lsn.to_string(), "0/16B6C50");

        let lsn2 = Lsn::parse("16/B374D848").unwrap();
        assert_eq!(lsn2.0, 0x16_B374D848);
        assert_eq!(lsn2.to_string(), "16/B374D848");
    }

    #[test]
    fn test_lsn_ordering() {
        let a = Lsn::parse("0/100").unwrap();
        let b = Lsn::parse("0/200").unwrap();
        assert!(a < b);
    }

    #[test]
    fn test_lsn_zero() {
        assert_eq!(Lsn::ZERO.to_string(), "0/0");
    }

    #[test]
    fn test_table_phase_roundtrip() {
        for phase in [
            TablePhase::Pending,
            TablePhase::Backfilling,
            TablePhase::Streaming,
        ] {
            assert_eq!(phase.as_str().parse::<TablePhase>().unwrap(), phase);
        }
    }

    #[test]
    fn test_ctid_partition_equality() {
        let a = CtidPartition {
            id: 0,
            start_page: 0,
            end_page: 100,
        };
        let b = CtidPartition {
            id: 0,
            start_page: 0,
            end_page: 100,
        };
        assert_eq!(a, b);
    }
}
