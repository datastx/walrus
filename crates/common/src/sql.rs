use crate::error::WalrusError;

/// Quote a SQL identifier by wrapping it in double quotes and escaping any
/// embedded double quotes (SQL standard identifier quoting).
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Validate a PostgreSQL snapshot name. Snapshot names are hex strings with a
/// dash separator produced by `pg_export_snapshot()`, e.g. `00000003-00000001-1`.
/// We reject anything that could break out of the single-quoted SQL literal.
pub fn validate_snapshot_name(name: &str) -> Result<(), WalrusError> {
    if name.is_empty() {
        return Err(WalrusError::InvalidSnapshotName(
            "empty snapshot name".to_string(),
        ));
    }
    for c in name.chars() {
        if !matches!(c, '0'..='9' | 'A'..='F' | 'a'..='f' | '-') {
            return Err(WalrusError::InvalidSnapshotName(name.to_string()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_ident_simple() {
        assert_eq!(quote_ident("users"), "\"users\"");
    }

    #[test]
    fn test_quote_ident_with_embedded_quote() {
        assert_eq!(quote_ident("my\"table"), "\"my\"\"table\"");
    }

    #[test]
    fn test_validate_snapshot_name_valid() {
        assert!(validate_snapshot_name("00000003-00000001-1").is_ok());
        assert!(validate_snapshot_name("ABCDEF-123456-7").is_ok());
    }

    #[test]
    fn test_validate_snapshot_name_invalid() {
        assert!(validate_snapshot_name("").is_err());
        assert!(validate_snapshot_name("'; DROP TABLE --").is_err());
        assert!(validate_snapshot_name("abc xyz").is_err());
    }
}
