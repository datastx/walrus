use arrow_schema::{DataType, Field, TimeUnit};

/// Maps a Postgres type OID to an Arrow DataType.
/// This mapping is used by Service 1 (Parquet writing) and must be consistent
/// with Service 2's reads via DuckDB.
pub fn pg_type_oid_to_arrow(oid: u32) -> DataType {
    match oid {
        16 => DataType::Boolean,                                  // bool
        21 => DataType::Int16,                                    // int2 / smallint
        23 => DataType::Int32,                                    // int4 / integer
        20 => DataType::Int64,                                    // int8 / bigint
        700 => DataType::Float32,                                 // float4 / real
        701 => DataType::Float64,                                 // float8 / double
        1700 => DataType::Decimal128(38, 18),                     // numeric (default precision)
        25 | 1043 => DataType::Utf8,                              // text / varchar
        17 => DataType::Binary,                                   // bytea
        1082 => DataType::Date32,                                 // date
        1114 => DataType::Timestamp(TimeUnit::Microsecond, None), // timestamp
        1184 => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), // timestamptz
        1083 => DataType::Time64(TimeUnit::Microsecond),          // time
        2950 => DataType::FixedSizeBinary(16),                    // uuid
        114 | 3802 => DataType::Utf8,                             // json / jsonb
        869 | 650 => DataType::Utf8,                              // inet / cidr
        1009 | 1015 => DataType::List(
            // text[] / varchar[]
            Box::new(Field::new("item", DataType::Utf8, true)).into(),
        ),
        1007 => DataType::List(
            // int4[]
            Box::new(Field::new("item", DataType::Int32, true)).into(),
        ),
        1016 => DataType::List(
            // int8[]
            Box::new(Field::new("item", DataType::Int64, true)).into(),
        ),
        _ => DataType::Utf8, // fallback: serialize as text
    }
}

/// Maps a Postgres type name (from information_schema) to Arrow DataType.
pub fn pg_type_name_to_arrow(type_name: &str) -> DataType {
    match type_name.to_lowercase().as_str() {
        "boolean" | "bool" => DataType::Boolean,
        "smallint" | "int2" => DataType::Int16,
        "integer" | "int" | "int4" => DataType::Int32,
        "bigint" | "int8" => DataType::Int64,
        "real" | "float4" => DataType::Float32,
        "double precision" | "float8" => DataType::Float64,
        "numeric" | "decimal" => DataType::Decimal128(38, 18),
        "text" | "character varying" | "varchar" => DataType::Utf8,
        "bytea" => DataType::Binary,
        "date" => DataType::Date32,
        "timestamp without time zone" | "timestamp" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        "timestamp with time zone" | "timestamptz" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        "time without time zone" | "time" => DataType::Time64(TimeUnit::Microsecond),
        "uuid" => DataType::FixedSizeBinary(16),
        "json" | "jsonb" => DataType::Utf8,
        "inet" | "cidr" => DataType::Utf8,
        s if s.ends_with("[]") || s.starts_with("ARRAY") => {
            let inner = s.trim_end_matches("[]");
            DataType::List(Box::new(Field::new("item", pg_type_name_to_arrow(inner), true)).into())
        }
        _ => DataType::Utf8,
    }
}

/// Maps a Postgres type name to the Iceberg type string used in schema definitions.
pub fn pg_type_name_to_iceberg(type_name: &str) -> &'static str {
    match type_name.to_lowercase().as_str() {
        "boolean" | "bool" => "boolean",
        "smallint" | "int2" | "integer" | "int" | "int4" => "int",
        "bigint" | "int8" => "long",
        "real" | "float4" => "float",
        "double precision" | "float8" => "double",
        "numeric" | "decimal" => "decimal(38,18)",
        "text" | "character varying" | "varchar" => "string",
        "bytea" => "binary",
        "date" => "date",
        "timestamp without time zone" | "timestamp" => "timestamp",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        "time without time zone" | "time" => "time",
        "uuid" => "uuid",
        "json" | "jsonb" => "string",
        _ => "string",
    }
}

/// Arrow schema fields appended to every CDC Parquet file.
pub fn cdc_metadata_fields() -> Vec<Field> {
    vec![
        Field::new("_pgiceberg_op", DataType::Utf8, false),
        Field::new("_pgiceberg_ts", DataType::Int64, false),
        Field::new("_pgiceberg_seq", DataType::Int64, false),
    ]
}

#[cfg(test)]
#[path = "schema_test.rs"]
mod schema_test;
