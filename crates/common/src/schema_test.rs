use super::*;

#[test]
fn test_pg_oid_to_arrow_basic_types() {
    assert_eq!(pg_type_oid_to_arrow(16), DataType::Boolean);
    assert_eq!(pg_type_oid_to_arrow(23), DataType::Int32);
    assert_eq!(pg_type_oid_to_arrow(20), DataType::Int64);
    assert_eq!(pg_type_oid_to_arrow(25), DataType::Utf8);
    assert_eq!(pg_type_oid_to_arrow(701), DataType::Float64);
}

#[test]
fn test_pg_oid_to_arrow_timestamp() {
    assert_eq!(
        pg_type_oid_to_arrow(1114),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        pg_type_oid_to_arrow(1184),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
}

#[test]
fn test_pg_oid_to_arrow_uuid() {
    assert_eq!(pg_type_oid_to_arrow(2950), DataType::FixedSizeBinary(16));
}

#[test]
fn test_pg_oid_to_arrow_arrays() {
    match pg_type_oid_to_arrow(1009) {
        DataType::List(inner) => assert_eq!(inner.data_type(), &DataType::Utf8),
        other => panic!("Expected List, got {:?}", other),
    }
}

#[test]
fn test_pg_oid_unknown_falls_back_to_utf8() {
    assert_eq!(pg_type_oid_to_arrow(99999), DataType::Utf8);
}

#[test]
fn test_pg_type_name_to_arrow() {
    assert_eq!(pg_type_name_to_arrow("integer"), DataType::Int32);
    assert_eq!(pg_type_name_to_arrow("text"), DataType::Utf8);
    assert_eq!(
        pg_type_name_to_arrow("timestamp with time zone"),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
}

#[test]
fn test_pg_type_name_to_iceberg() {
    assert_eq!(pg_type_name_to_iceberg("integer"), "int");
    assert_eq!(pg_type_name_to_iceberg("bigint"), "long");
    assert_eq!(pg_type_name_to_iceberg("text"), "string");
    assert_eq!(pg_type_name_to_iceberg("timestamptz"), "timestamptz");
}

#[test]
fn test_cdc_metadata_fields() {
    let fields = cdc_metadata_fields();
    assert_eq!(fields.len(), 3);
    assert_eq!(fields[0].name(), "_pgiceberg_op");
    assert_eq!(fields[1].name(), "_pgiceberg_ts");
    assert_eq!(fields[2].name(), "_pgiceberg_seq");
}
