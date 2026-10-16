use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
    Int64Array, LargeStringArray, StringArray,
};
use arrow::datatypes::{DataType as ArrowType, Schema as ArrowSchema};
use morsel_core::{Batch, Column, ColumnData, DataType, Field, Schema, BATCH_ROWS};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::catalog::{Catalog, Table};

pub fn load_parquet_dir_impl(dir: &Path, threads: usize) -> Result<Catalog> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
            files.push(path);
        }
    }
    files.sort();

    let num_threads = threads.max(1).min(files.len().max(1));
    let files_queue = std::sync::Mutex::new(files.into_iter());
    let results_mutex = std::sync::Mutex::new(Vec::new());

    std::thread::scope(|s| {
        for _ in 0..num_threads {
            s.spawn(|| loop {
                let next_file = {
                    let mut q = files_queue.lock().unwrap();
                    q.next()
                };
                let path = match next_file {
                    Some(p) => p,
                    None => break,
                };
                let res = load_parquet_file(&path);
                let mut r = results_mutex.lock().unwrap();
                r.push(res);
            });
        }
    });

    let mut tables = std::collections::HashMap::new();
    for res in results_mutex.into_inner().unwrap() {
        let (name, table) = res?;
        tables.insert(name, table);
    }

    Ok(Catalog { tables })
}

fn convert_arrow_schema(arrow_schema: &ArrowSchema) -> Result<Schema> {
    let mut fields = Vec::with_capacity(arrow_schema.fields().len());
    for f in arrow_schema.fields() {
        let name = f.name().clone();
        let nullable = f.is_nullable();
        let dtype = match f.data_type() {
            ArrowType::Boolean => DataType::Bool,
            ArrowType::Int32 => DataType::Int32,
            ArrowType::Int64 => DataType::Int64,
            ArrowType::Float64 => DataType::Float64,
            ArrowType::Decimal128(p, s) => DataType::Decimal {
                precision: *p,
                scale: *s as u8,
            },
            ArrowType::Date32 => DataType::Date32,
            ArrowType::Utf8 | ArrowType::LargeUtf8 => DataType::Utf8,
            other => bail!(
                "unsupported arrow type {:?} for column '{}'",
                other,
                f.name()
            ),
        };
        fields.push(Field {
            name,
            dtype,
            nullable,
        });
    }
    Ok(Schema { fields })
}

fn convert_arrow_column(array: &ArrayRef, dtype: DataType) -> Result<Column> {
    let len = array.len();
    let validity = if array.null_count() > 0 {
        Some((0..len).map(|i| !array.is_null(i)).collect())
    } else {
        None
    };

    let data = match dtype {
        DataType::Bool => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean array");
            let mut vals = Vec::with_capacity(len);
            for i in 0..len {
                vals.push(!arr.is_null(i) && arr.value(i));
            }
            ColumnData::Bool(vals)
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("int32 array");
            let mut vals = Vec::with_capacity(len);
            for i in 0..len {
                vals.push(if arr.is_null(i) { 0 } else { arr.value(i) });
            }
            ColumnData::Int32(vals)
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 array");
            let mut vals = Vec::with_capacity(len);
            for i in 0..len {
                vals.push(if arr.is_null(i) { 0 } else { arr.value(i) });
            }
            ColumnData::Int64(vals)
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float64 array");
            let mut vals = Vec::with_capacity(len);
            for i in 0..len {
                vals.push(if arr.is_null(i) { 0.0 } else { arr.value(i) });
            }
            ColumnData::Float64(vals)
        }
        DataType::Decimal { .. } => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("decimal128 array");
            let mut vals = Vec::with_capacity(len);
            for i in 0..len {
                vals.push(if arr.is_null(i) { 0 } else { arr.value(i) });
            }
            ColumnData::Decimal(vals)
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("date32 array");
            let mut vals = Vec::with_capacity(len);
            for i in 0..len {
                vals.push(if arr.is_null(i) { 0 } else { arr.value(i) });
            }
            ColumnData::Date32(vals)
        }
        DataType::Utf8 => {
            let mut offsets = Vec::with_capacity(len + 1);
            let mut bytes = Vec::new();
            offsets.push(0);
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                for i in 0..len {
                    if !arr.is_null(i) {
                        bytes.extend_from_slice(arr.value(i).as_bytes());
                    }
                    offsets.push(bytes.len() as u32);
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                for i in 0..len {
                    if !arr.is_null(i) {
                        bytes.extend_from_slice(arr.value(i).as_bytes());
                    }
                    offsets.push(bytes.len() as u32);
                }
            } else {
                panic!("expected utf8 or large_utf8 array");
            }
            ColumnData::Utf8 { offsets, bytes }
        }
    };

    Ok(Column {
        dtype,
        data,
        validity,
    })
}

fn load_parquet_file(path: &Path) -> Result<(String, Table)> {
    let file_stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid file name: {:?}", path))?
        .to_string();

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(BATCH_ROWS);

    let arrow_schema = Arc::clone(builder.schema());
    let schema = Arc::new(convert_arrow_schema(&arrow_schema)?);

    let reader = builder.build()?;
    let mut batches = Vec::new();
    let mut total_rows = 0;

    for maybe_rb in reader {
        let rb = maybe_rb?;
        let rb_len = rb.num_rows();
        if rb_len == 0 {
            continue;
        }

        // Chunk record batch into slices of at most BATCH_ROWS
        let mut offset = 0;
        while offset < rb_len {
            let chunk_len = (rb_len - offset).min(BATCH_ROWS);
            let sliced_rb = rb.slice(offset, chunk_len);

            let mut cols = Vec::with_capacity(sliced_rb.num_columns());
            for (col_idx, arr) in sliced_rb.columns().iter().enumerate() {
                let dtype = schema.fields[col_idx].dtype;
                let col = convert_arrow_column(arr, dtype)?;
                cols.push(Arc::new(col));
            }
            let batch = Batch::new(Arc::clone(&schema), cols);
            total_rows += batch.len;
            batches.push(batch);
            offset += chunk_len;
        }
    }

    Ok((
        file_stem,
        Table {
            schema,
            batches,
            rows: total_rows,
        },
    ))
}
