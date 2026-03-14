use arrow::record_batch::RecordBatch;
use arrow_array::*;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::decoder::CachedRelation;
use pgiceberg_common::models::CdcRecord;
use pgiceberg_common::schema::{cdc_metadata_fields, pg_type_oid_to_arrow};

/// Write tokio-postgres Row objects to a Parquet file.
/// Used during backfill where we get standard query results.
pub fn write_rows_to_parquet(
    rows: &[tokio_postgres::Row],
    output_path: &Path,
) -> anyhow::Result<()> {
    if rows.is_empty() {
        // Write an empty file so the queue entry makes sense
        let schema = Arc::new(Schema::new(vec![Field::new(
            "_empty",
            DataType::Boolean,
            true,
        )]));
        let file = File::create(output_path)?;
        let props = default_writer_properties();
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.close()?;
        return Ok(());
    }

    let pg_columns = rows[0].columns();
    let mut fields = Vec::with_capacity(pg_columns.len());
    for col in pg_columns {
        let arrow_type = pg_type_oid_to_arrow(col.type_().oid());
        fields.push(Field::new(col.name(), arrow_type, true));
    }
    let schema = Arc::new(Schema::new(fields));

    let file = File::create(output_path)?;
    let props = default_writer_properties();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    // Process in chunks of 10,000 rows
    for chunk in rows.chunks(10_000) {
        let batch = rows_to_record_batch(chunk, &schema)?;
        writer.write(&batch)?;
    }

    writer.close()?;
    Ok(())
}

/// Write CDC records to a Parquet file with _pgiceberg_op and _pgiceberg_ts columns.
/// Used during CDC streaming.
pub fn write_cdc_records_to_parquet(
    records: &[CdcRecord],
    relation: &CachedRelation,
    output_path: &Path,
) -> anyhow::Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    let mut fields: Vec<Field> = relation
        .columns
        .iter()
        .map(|c| Field::new(&c.name, pg_type_oid_to_arrow(c.type_oid), true))
        .collect();
    fields.extend(cdc_metadata_fields());
    let schema = Arc::new(Schema::new(fields));

    let file = File::create(output_path)?;
    let props = default_writer_properties();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    for chunk in records.chunks(10_000) {
        let batch = cdc_records_to_batch(chunk, relation, &schema)?;
        writer.write(&batch)?;
    }

    writer.close()?;
    Ok(())
}

fn rows_to_record_batch(
    rows: &[tokio_postgres::Row],
    schema: &Arc<Schema>,
) -> anyhow::Result<RecordBatch> {
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(schema.fields().len());

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array = build_array_from_pg_rows(rows, col_idx, field.data_type())?;
        columns.push(array);
    }

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

fn build_array_from_pg_rows(
    rows: &[tokio_postgres::Row],
    col_idx: usize,
    data_type: &DataType,
) -> anyhow::Result<Arc<dyn arrow_array::Array>> {
    match data_type {
        DataType::Boolean => {
            let values: Vec<Option<bool>> = rows.iter().map(|r| r.get(col_idx)).collect();
            Ok(Arc::new(BooleanArray::from(values)))
        }
        DataType::Int16 => {
            let values: Vec<Option<i16>> = rows.iter().map(|r| r.get(col_idx)).collect();
            Ok(Arc::new(Int16Array::from(values)))
        }
        DataType::Int32 => {
            let values: Vec<Option<i32>> = rows.iter().map(|r| r.get(col_idx)).collect();
            Ok(Arc::new(Int32Array::from(values)))
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = rows.iter().map(|r| r.get(col_idx)).collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::Float32 => {
            let values: Vec<Option<f32>> = rows.iter().map(|r| r.get(col_idx)).collect();
            Ok(Arc::new(Float32Array::from(values)))
        }
        DataType::Float64 => {
            let values: Vec<Option<f64>> = rows.iter().map(|r| r.get(col_idx)).collect();
            Ok(Arc::new(Float64Array::from(values)))
        }
        DataType::Utf8 => {
            let values: Vec<Option<String>> = rows.iter().map(|r| r.get(col_idx)).collect();
            Ok(Arc::new(StringArray::from(values)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let values: Vec<Option<chrono::NaiveDateTime>> =
                rows.iter().map(|r| r.get(col_idx)).collect();
            let micros: Vec<Option<i64>> = values
                .iter()
                .map(|v| v.map(|dt| dt.and_utc().timestamp_micros()))
                .collect();
            Ok(Arc::new(
                TimestampMicrosecondArray::from(micros).with_timezone_opt(None::<String>),
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
            let values: Vec<Option<chrono::DateTime<chrono::Utc>>> =
                rows.iter().map(|r| r.get(col_idx)).collect();
            let micros: Vec<Option<i64>> = values
                .iter()
                .map(|v| v.map(|dt| dt.timestamp_micros()))
                .collect();
            Ok(Arc::new(
                TimestampMicrosecondArray::from(micros).with_timezone(tz.clone()),
            ))
        }
        _ => {
            // Fallback: convert to string
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|r| r.try_get::<_, String>(col_idx).ok())
                .collect();
            Ok(Arc::new(StringArray::from(values)))
        }
    }
}

fn cdc_records_to_batch(
    records: &[CdcRecord],
    relation: &CachedRelation,
    schema: &Arc<Schema>,
) -> anyhow::Result<RecordBatch> {
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::new();

    // Build data columns from tuple data
    for (col_idx, rel_col) in relation.columns.iter().enumerate() {
        let array = build_array_from_cdc_columns(records, col_idx, rel_col.type_oid)?;
        columns.push(array);
    }

    // _pgiceberg_op
    let ops: Vec<&str> = records.iter().map(|r| r.op.as_str()).collect();
    columns.push(Arc::new(StringArray::from(ops)));

    // _pgiceberg_ts
    let timestamps: Vec<i64> = records.iter().map(|r| r.commit_ts).collect();
    columns.push(Arc::new(Int64Array::from(timestamps)));

    // _pgiceberg_seq
    let seqs: Vec<i64> = records.iter().map(|r| r.seq).collect();
    columns.push(Arc::new(Int64Array::from(seqs)));

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

fn build_array_from_cdc_columns(
    records: &[CdcRecord],
    col_idx: usize,
    type_oid: u32,
) -> anyhow::Result<Arc<dyn arrow_array::Array>> {
    // For CDC records, column data is stored as raw text bytes in CdcColumn.value
    let values: Vec<Option<&[u8]>> = records
        .iter()
        .map(|r| r.columns.get(col_idx).and_then(|c| c.value.as_deref()))
        .collect();

    match pg_type_oid_to_arrow(type_oid) {
        DataType::Boolean => {
            let parsed: Vec<Option<bool>> = values
                .iter()
                .map(|v| {
                    v.and_then(|b| std::str::from_utf8(b).ok())
                        .map(|s| s == "t")
                })
                .collect();
            Ok(Arc::new(BooleanArray::from(parsed)))
        }
        DataType::Int32 => {
            let parsed: Vec<Option<i32>> = values
                .iter()
                .map(|v| {
                    v.and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse().ok())
                })
                .collect();
            Ok(Arc::new(Int32Array::from(parsed)))
        }
        DataType::Int64 => {
            let parsed: Vec<Option<i64>> = values
                .iter()
                .map(|v| {
                    v.and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse().ok())
                })
                .collect();
            Ok(Arc::new(Int64Array::from(parsed)))
        }
        DataType::Float64 => {
            let parsed: Vec<Option<f64>> = values
                .iter()
                .map(|v| {
                    v.and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse().ok())
                })
                .collect();
            Ok(Arc::new(Float64Array::from(parsed)))
        }
        _ => {
            // Default: treat as string
            let strings: Vec<Option<&str>> = values
                .iter()
                .map(|v| v.and_then(|b| std::str::from_utf8(b).ok()))
                .collect();
            Ok(Arc::new(StringArray::from(strings)))
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
#[path = "parquet_writer_test.rs"]
mod parquet_writer_test;
