use thiserror::Error;

#[derive(Debug, Error)]
pub enum WalrusError {
    #[error("invalid LSN format: {0}")]
    InvalidLsn(String),
    #[error("invalid snapshot name: {0}")]
    InvalidSnapshotName(String),
}
