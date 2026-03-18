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
            let dt_lower = data_type.to_lowercase();
            assert!(
                dt_lower.contains("varchar") || dt_lower.contains("character varying"),
                "expected varchar type, got: {}",
                data_type
            );
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
            let nt = new_type.as_ref().unwrap().to_lowercase();
            assert!(
                nt.contains("varchar") || nt.contains("character varying"),
                "expected varchar type, got: {}",
                new_type.as_ref().unwrap()
            );
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

#[test]
fn test_parse_comment_on_table() {
    let target = parse_comment_sql("COMMENT ON TABLE public.users IS 'User accounts'");
    assert_eq!(
        target,
        CommentTarget::Table {
            comment: Some("User accounts".to_string())
        }
    );
}

#[test]
fn test_parse_comment_on_column() {
    let target = parse_comment_sql("COMMENT ON COLUMN public.users.email IS 'Primary email'");
    assert_eq!(
        target,
        CommentTarget::Column {
            column_name: "email".to_string(),
            comment: Some("Primary email".to_string()),
        }
    );
}

#[test]
fn test_parse_comment_on_table_null() {
    let target = parse_comment_sql("COMMENT ON TABLE public.users IS NULL");
    assert_eq!(target, CommentTarget::Table { comment: None });
}

#[test]
fn test_parse_comment_malformed() {
    let target = parse_comment_sql("NOT VALID SQL AT ALL %%%");
    assert_eq!(target, CommentTarget::Other);
}
