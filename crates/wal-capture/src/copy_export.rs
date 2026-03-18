//! Binary COPY export for PostgreSQL backfill.
//!
//! Uses `COPY (SELECT ...) TO STDOUT (FORMAT binary)` for high-throughput
//! bulk data export.  This is significantly faster than row-by-row SELECT
//! queries because:
//! - The server uses a more efficient copy-out code path
//! - Per-row protocol overhead is eliminated
//! - Binary encoding avoids text serialisation round-trips
//!
//! Types with complex binary representations (numeric, arrays, inet/cidr)
//! are cast to `::text` in the COPY sub-query so their wire format is plain
//! UTF-8 bytes, stored as Arrow `Utf8` columns.

use arrow::record_batch::RecordBatch;
use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::*;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::BytesMut;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use pgiceberg_common::sql::quote_ident;
use std::path::Path;
use std::sync::Arc;
use tokio_postgres::Client;
use tracing::debug;

/// Postgres epoch (2000-01-01) as microseconds since Unix epoch.
const PG_EPOCH_USEC: i64 = 946_684_800_000_000;

/// Postgres epoch as days since Unix epoch.
const PG_EPOCH_DAYS: i32 = 10_957;

/// Signature bytes at the start of a binary COPY stream.
const BINARY_HEADER_MAGIC: [u8; 11] = *b"PGCOPY\n\xff\r\n\0";

/// Rows per Arrow RecordBatch written to Parquet.
const BATCH_SIZE: usize = 10_000;

// ── public types ────────────────────────────────────────────────────────

/// Column metadata needed to parse binary COPY output.
pub struct CopyColumn {
    pub name: String,
    /// Original PostgreSQL type OID from `pg_attribute`.
    pub pg_oid: u32,
    /// OID that determines binary parsing.  Equals `pg_oid` when the type
    /// has a simple binary representation; set to `25` (text) for complex
    /// types that are cast to `::text` in the COPY query.
    wire_oid: u32,
    /// Arrow data type for this column in the output Parquet file.
    arrow_type: DataType,
}

// ── public API ──────────────────────────────────────────────────────────

/// Query column metadata (name, type OID) from `pg_attribute`.
pub async fn get_columns(
    client: &Client,
    schema: &str,
    table: &str,
) -> anyhow::Result<Vec<CopyColumn>> {
    let rows = client
        .query(
            "SELECT a.attname, a.atttypid::oid \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = $1 AND n.nspname = $2 \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&table, &schema],
        )
        .await?;

    let columns = rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let oid: u32 = row.get(1);
            let (wire_oid, arrow_type) = wire_type(oid);
            CopyColumn {
                name,
                pg_oid: oid,
                wire_oid,
                arrow_type,
            }
        })
        .collect();
    Ok(columns)
}

/// Execute `COPY ... TO STDOUT (FORMAT binary)` and collect the raw bytes.
///
/// Complex types are cast to `::text` in the sub-query so their binary
/// representation is plain UTF-8.
pub async fn run_copy(
    client: &Client,
    schema: &str,
    table: &str,
    columns: &[CopyColumn],
    start_page: u64,
    end_page: u64,
) -> anyhow::Result<BytesMut> {
    let quoted_table = format!("{}.{}", quote_ident(schema), quote_ident(table));
    let select_cols: String = columns
        .iter()
        .map(|c| {
            let quoted = quote_ident(&c.name);
            if c.wire_oid != c.pg_oid {
                format!("{}::text", quoted)
            } else {
                quoted
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let inner_query = if start_page == 0 && end_page == 0 {
        format!("SELECT {} FROM {} WHERE false", select_cols, quoted_table)
    } else {
        format!(
            "SELECT {} FROM {} WHERE ctid >= '({},0)'::tid AND ctid < '({},0)'::tid",
            select_cols, quoted_table, start_page, end_page
        )
    };

    let copy_stmt = format!("COPY ({}) TO STDOUT (FORMAT binary)", inner_query);
    debug!(%copy_stmt, "Running COPY export");

    let stream = client.copy_out(copy_stmt.as_str()).await?;
    futures::pin_mut!(stream);
    let mut buf = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk?);
    }
    Ok(buf)
}

/// Parse binary COPY data and write to a Parquet file.
///
/// Returns the number of rows written.
pub fn write_binary_to_parquet(
    data: &[u8],
    columns: &[CopyColumn],
    output_path: &Path,
) -> anyhow::Result<i64> {
    let mut pos = parse_header(data)?;
    let num_fields = columns.len();

    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, c.arrow_type.clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(output_path)?;
    let props = default_writer_properties();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut row_count: i64 = 0;
    let mut batch: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(BATCH_SIZE);

    loop {
        anyhow::ensure!(pos + 2 <= data.len(), "Unexpected end of binary COPY data");
        let field_count = i16::from_be_bytes([data[pos], data[pos + 1]]);
        if field_count == -1 {
            break;
        }
        pos += 2;

        anyhow::ensure!(
            field_count as usize == num_fields,
            "COPY field count mismatch: expected {num_fields}, got {field_count}"
        );

        let mut row = Vec::with_capacity(num_fields);
        for _ in 0..num_fields {
            anyhow::ensure!(pos + 4 <= data.len(), "Unexpected end of field length");
            let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;
            if len == -1 {
                row.push(None);
            } else {
                let len = len as usize;
                anyhow::ensure!(pos + len <= data.len(), "Field data exceeds buffer");
                row.push(Some(data[pos..pos + len].to_vec()));
                pos += len;
            }
        }

        batch.push(row);
        row_count += 1;

        if batch.len() >= BATCH_SIZE {
            let rb = build_record_batch(&batch, columns, &schema)?;
            writer.write(&rb)?;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        let rb = build_record_batch(&batch, columns, &schema)?;
        writer.write(&rb)?;
    }

    writer.close()?;
    Ok(row_count)
}

// ── private helpers ─────────────────────────────────────────────────────

/// Map a PG type OID to `(wire_oid, arrow_type)`.
///
/// Types with simple binary encodings keep their original OID.
/// Complex types are remapped to OID 25 (text) — the COPY query will
/// include a `::text` cast for the column.
fn wire_type(oid: u32) -> (u32, DataType) {
    match oid {
        16 => (16, DataType::Boolean),      // bool
        21 => (21, DataType::Int16),        // int2
        23 => (23, DataType::Int32),        // int4
        20 => (20, DataType::Int64),        // int8
        700 => (700, DataType::Float32),    // float4
        701 => (701, DataType::Float64),    // float8
        25 | 1043 => (oid, DataType::Utf8), // text / varchar
        17 => (17, DataType::Binary),       // bytea
        1082 => (1082, DataType::Date32),   // date
        1114 => (1114, DataType::Timestamp(TimeUnit::Microsecond, None)), // timestamp
        1184 => (
            1184,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ), // timestamptz
        1083 => (1083, DataType::Time64(TimeUnit::Microsecond)), // time
        2950 => (2950, DataType::FixedSizeBinary(16)), // uuid
        114 => (114, DataType::Utf8),       // json
        3802 => (3802, DataType::Utf8),     // jsonb
        // Complex types → cast to text in the COPY query
        _ => (25, DataType::Utf8),
    }
}

fn parse_header(data: &[u8]) -> anyhow::Result<usize> {
    anyhow::ensure!(data.len() >= 19, "Binary COPY data too short for header");
    anyhow::ensure!(
        data[..11] == BINARY_HEADER_MAGIC,
        "Invalid binary COPY header signature"
    );
    let ext_len = u32::from_be_bytes([data[15], data[16], data[17], data[18]]) as usize;
    let end = 19 + ext_len;
    anyhow::ensure!(data.len() >= end, "Header extension exceeds data length");
    Ok(end)
}

fn build_record_batch(
    rows: &[Vec<Option<Vec<u8>>>],
    columns: &[CopyColumn],
    schema: &Arc<Schema>,
) -> anyhow::Result<RecordBatch> {
    let mut arrays: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(columns.len());
    for (col_idx, col) in columns.iter().enumerate() {
        let values: Vec<Option<&[u8]>> = rows.iter().map(|r| r[col_idx].as_deref()).collect();
        arrays.push(parse_binary_column(&values, col.wire_oid)?);
    }
    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}

/// Convert a batch of raw binary field values into an Arrow array.
fn parse_binary_column(
    values: &[Option<&[u8]>],
    wire_oid: u32,
) -> anyhow::Result<Arc<dyn arrow_array::Array>> {
    match wire_oid {
        16 => {
            // bool: 1 byte, 0 = false, 1 = true
            let v: Vec<Option<bool>> = values.iter().map(|v| v.map(|b| b[0] != 0)).collect();
            Ok(Arc::new(BooleanArray::from(v)))
        }
        21 => {
            // int2: 2 bytes big-endian
            let v: Vec<Option<i16>> = values
                .iter()
                .map(|v| v.map(|b| i16::from_be_bytes([b[0], b[1]])))
                .collect();
            Ok(Arc::new(Int16Array::from(v)))
        }
        23 => {
            // int4: 4 bytes big-endian
            let v: Vec<Option<i32>> = values
                .iter()
                .map(|v| v.map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]])))
                .collect();
            Ok(Arc::new(Int32Array::from(v)))
        }
        20 => {
            // int8: 8 bytes big-endian
            let v: Vec<Option<i64>> = values
                .iter()
                .map(|v| v.map(|b| i64::from_be_bytes(b[..8].try_into().unwrap())))
                .collect();
            Ok(Arc::new(Int64Array::from(v)))
        }
        700 => {
            // float4: 4 bytes big-endian IEEE 754
            let v: Vec<Option<f32>> = values
                .iter()
                .map(|v| v.map(|b| f32::from_be_bytes([b[0], b[1], b[2], b[3]])))
                .collect();
            Ok(Arc::new(Float32Array::from(v)))
        }
        701 => {
            // float8: 8 bytes big-endian IEEE 754
            let v: Vec<Option<f64>> = values
                .iter()
                .map(|v| v.map(|b| f64::from_be_bytes(b[..8].try_into().unwrap())))
                .collect();
            Ok(Arc::new(Float64Array::from(v)))
        }
        17 => {
            // bytea: raw bytes
            let v: Vec<Option<&[u8]>> = values.to_vec();
            Ok(Arc::new(BinaryArray::from(v)))
        }
        1082 => {
            // date: i32 days since PG epoch (2000-01-01) → Unix epoch days
            // Use saturating_add to handle PG's infinity/−infinity (i32::MAX/MIN).
            let v: Vec<Option<i32>> = values
                .iter()
                .map(|v| {
                    v.map(|b| {
                        i32::from_be_bytes([b[0], b[1], b[2], b[3]]).saturating_add(PG_EPOCH_DAYS)
                    })
                })
                .collect();
            Ok(Arc::new(Date32Array::from(v)))
        }
        1114 => {
            // timestamp: i64 µs since PG epoch → Unix epoch
            // Use saturating_add to handle PG's infinity/−infinity (i64::MAX/MIN).
            let v: Vec<Option<i64>> = values
                .iter()
                .map(|v| {
                    v.map(|b| {
                        i64::from_be_bytes(b[..8].try_into().unwrap()).saturating_add(PG_EPOCH_USEC)
                    })
                })
                .collect();
            Ok(Arc::new(
                TimestampMicrosecondArray::from(v).with_timezone_opt(None::<String>),
            ))
        }
        1184 => {
            // timestamptz: i64 µs since PG epoch → Unix epoch (UTC)
            // Use saturating_add to handle PG's infinity/−infinity (i64::MAX/MIN).
            let v: Vec<Option<i64>> = values
                .iter()
                .map(|v| {
                    v.map(|b| {
                        i64::from_be_bytes(b[..8].try_into().unwrap()).saturating_add(PG_EPOCH_USEC)
                    })
                })
                .collect();
            Ok(Arc::new(
                TimestampMicrosecondArray::from(v).with_timezone("UTC"),
            ))
        }
        1083 => {
            // time: i64 µs since midnight
            let v: Vec<Option<i64>> = values
                .iter()
                .map(|v| v.map(|b| i64::from_be_bytes(b[..8].try_into().unwrap())))
                .collect();
            Ok(Arc::new(Time64MicrosecondArray::from(v)))
        }
        2950 => {
            // uuid: 16 raw bytes
            let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
            for v in values {
                match v {
                    Some(b) => builder.append_value(b)?,
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        3802 => {
            // jsonb: 1 version byte + UTF-8 JSON text
            let v: Vec<Option<&str>> = values
                .iter()
                .map(|v| {
                    v.and_then(|b| {
                        if b.len() > 1 {
                            std::str::from_utf8(&b[1..]).ok()
                        } else {
                            Some("")
                        }
                    })
                })
                .collect();
            Ok(Arc::new(StringArray::from(v)))
        }
        _ => {
            // text / varchar / json / any text-casted type: raw UTF-8
            let v: Vec<Option<&str>> = values
                .iter()
                .map(|v| v.and_then(|b| std::str::from_utf8(b).ok()))
                .collect();
            Ok(Arc::new(StringArray::from(v)))
        }
    }
}

fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .set_max_row_group_size(100_000)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_type_simple_types() {
        assert_eq!(wire_type(16).0, 16); // bool keeps its OID
        assert_eq!(wire_type(23).0, 23); // int4 keeps its OID
        assert_eq!(wire_type(25).0, 25); // text keeps its OID
    }

    #[test]
    fn test_wire_type_complex_types_cast_to_text() {
        assert_eq!(wire_type(1700).0, 25); // numeric → text
        assert_eq!(wire_type(869).0, 25); // inet → text
        assert_eq!(wire_type(1009).0, 25); // text[] → text
        assert_eq!(wire_type(9999).0, 25); // unknown → text
    }

    #[test]
    fn test_parse_header_valid() {
        let mut data = Vec::new();
        data.extend_from_slice(&BINARY_HEADER_MAGIC);
        data.extend_from_slice(&0u32.to_be_bytes()); // flags
        data.extend_from_slice(&0u32.to_be_bytes()); // ext len = 0
        data.extend_from_slice(&(-1i16).to_be_bytes()); // trailer
        let pos = parse_header(&data).unwrap();
        assert_eq!(pos, 19);
    }

    #[test]
    fn test_parse_header_with_extension() {
        let mut data = Vec::new();
        data.extend_from_slice(&BINARY_HEADER_MAGIC);
        data.extend_from_slice(&0u32.to_be_bytes()); // flags
        data.extend_from_slice(&4u32.to_be_bytes()); // ext len = 4
        data.extend_from_slice(&[0, 0, 0, 0]); // extension data
        data.extend_from_slice(&(-1i16).to_be_bytes()); // trailer
        let pos = parse_header(&data).unwrap();
        assert_eq!(pos, 23);
    }

    #[test]
    fn test_parse_binary_column_int4() {
        let val1 = 42i32.to_be_bytes().to_vec();
        let val2 = (-1i32).to_be_bytes().to_vec();
        let values = vec![Some(val1.as_slice()), None, Some(val2.as_slice())];
        let array = parse_binary_column(&values, 23).unwrap();
        let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 42);
        assert!(int_array.is_null(1));
        assert_eq!(int_array.value(2), -1);
    }

    #[test]
    fn test_parse_binary_column_bool() {
        let values = vec![Some([1u8].as_slice()), Some([0u8].as_slice()), None];
        let array = parse_binary_column(&values, 16).unwrap();
        let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));
        assert!(bool_array.is_null(2));
    }

    #[test]
    fn test_parse_binary_column_text() {
        let val = b"hello".to_vec();
        let values = vec![Some(val.as_slice()), None];
        let array = parse_binary_column(&values, 25).unwrap();
        let str_array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_array.value(0), "hello");
        assert!(str_array.is_null(1));
    }

    #[test]
    fn test_parse_binary_column_timestamp() {
        // 2024-01-01 00:00:00 UTC in PG epoch microseconds
        // PG epoch is 2000-01-01, so 24 years = 8766 days = 757382400 seconds
        let pg_usec: i64 = 757_382_400_000_000;
        let val = pg_usec.to_be_bytes().to_vec();
        let values = vec![Some(val.as_slice())];
        let array = parse_binary_column(&values, 1114).unwrap();
        let ts_array = array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        // Should be PG value + epoch offset
        assert_eq!(ts_array.value(0), pg_usec + PG_EPOCH_USEC);
    }

    #[test]
    fn test_write_binary_to_parquet_empty() {
        let mut data = Vec::new();
        data.extend_from_slice(&BINARY_HEADER_MAGIC);
        data.extend_from_slice(&0u32.to_be_bytes());
        data.extend_from_slice(&0u32.to_be_bytes());
        data.extend_from_slice(&(-1i16).to_be_bytes());

        let columns = vec![CopyColumn {
            name: "id".to_string(),
            pg_oid: 23,
            wire_oid: 23,
            arrow_type: DataType::Int32,
        }];

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        let count = write_binary_to_parquet(&data, &columns, &path).unwrap();
        assert_eq!(count, 0);
        assert!(path.exists());
    }

    #[test]
    fn test_write_binary_to_parquet_with_rows() {
        let mut data = Vec::new();
        // Header
        data.extend_from_slice(&BINARY_HEADER_MAGIC);
        data.extend_from_slice(&0u32.to_be_bytes()); // flags
        data.extend_from_slice(&0u32.to_be_bytes()); // ext len

        // Row 1: id=42, name="alice"
        data.extend_from_slice(&2i16.to_be_bytes()); // 2 fields
        data.extend_from_slice(&4i32.to_be_bytes()); // id len = 4
        data.extend_from_slice(&42i32.to_be_bytes()); // id value
        data.extend_from_slice(&5i32.to_be_bytes()); // name len = 5
        data.extend_from_slice(b"alice"); // name value

        // Row 2: id=99, name=NULL
        data.extend_from_slice(&2i16.to_be_bytes()); // 2 fields
        data.extend_from_slice(&4i32.to_be_bytes()); // id len = 4
        data.extend_from_slice(&99i32.to_be_bytes()); // id value
        data.extend_from_slice(&(-1i32).to_be_bytes()); // name is NULL

        // Trailer
        data.extend_from_slice(&(-1i16).to_be_bytes());

        let columns = vec![
            CopyColumn {
                name: "id".to_string(),
                pg_oid: 23,
                wire_oid: 23,
                arrow_type: DataType::Int32,
            },
            CopyColumn {
                name: "name".to_string(),
                pg_oid: 25,
                wire_oid: 25,
                arrow_type: DataType::Utf8,
            },
        ];

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        let count = write_binary_to_parquet(&data, &columns, &path).unwrap();
        assert_eq!(count, 2);

        // Verify by reading back
        let file = std::fs::File::open(&path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.into_iter().collect();
        assert_eq!(batches.len(), 1);
        let batch = batches[0].as_ref().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 42);
        assert_eq!(id_col.value(1), 99);

        let name_col = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "alice");
        assert!(name_col.is_null(1));
    }
}
