use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
    Int64Array, LargeStringArray, StringArray,
};
use arrow::datatypes::{DataType as ArrowType, Schema as ArrowSchema};
use morsel_core::{Batch, Column, ColumnData, DataType, Field, Schema, BATCH_ROWS};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};

use crate::catalog::{Catalog, Table};

struct TableMeta {
    name: String,
    path: PathBuf,
    schema: Arc<Schema>,
    arrow_metadata: ArrowReaderMetadata,
}

struct RowGroupTask {
    table_idx: usize,
    rg_idx: usize,
}

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

    let mut tables_meta = Vec::with_capacity(files.len());
    let mut tasks = Vec::new();

    for (t_idx, path) in files.iter().enumerate() {
        let file_stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("invalid file name: {:?}", path))?
            .to_string();

        let file = File::open(path)?;
        let arrow_metadata = ArrowReaderMetadata::load(&file, Default::default())?;
        let arrow_schema = Arc::clone(arrow_metadata.schema());
        let schema = Arc::new(convert_arrow_schema(&arrow_schema)?);
        let num_rgs = arrow_metadata.metadata().num_row_groups();

        for rg_idx in 0..num_rgs {
            tasks.push(RowGroupTask {
                table_idx: t_idx,
                rg_idx,
            });
        }

        tables_meta.push(TableMeta {
            name: file_stem,
            path: path.clone(),
            schema,
            arrow_metadata,
        });
    }

    if tasks.is_empty() {
        let mut tables = std::collections::HashMap::new();
        for meta in tables_meta {
            tables.insert(
                meta.name,
                Table {
                    schema: meta.schema,
                    batches: Vec::new(),
                    rows: 0,
                },
            );
        }
        return Ok(Catalog { tables });
    }

    type TableResult = Mutex<Vec<(usize, Vec<Batch>)>>;
    let table_results: Vec<TableResult> = (0..tables_meta.len())
        .map(|_| Mutex::new(Vec::new()))
        .collect();

    let num_threads = threads.max(1);
    let next_task = AtomicUsize::new(0);
    let error_cell: Mutex<Option<anyhow::Error>> = Mutex::new(None);

    std::thread::scope(|s| {
        for _ in 0..num_threads {
            s.spawn(|| loop {
                let task_idx = next_task.fetch_add(1, Ordering::Relaxed);
                if task_idx >= tasks.len() {
                    break;
                }
                if error_cell.lock().unwrap().is_some() {
                    break;
                }

                let task = &tasks[task_idx];
                let meta = &tables_meta[task.table_idx];

                let res = (|| -> Result<Vec<Batch>> {
                    let file = File::open(&meta.path)?;
                    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                        file,
                        meta.arrow_metadata.clone(),
                    )
                    .with_batch_size(BATCH_ROWS)
                    .with_row_groups(vec![task.rg_idx]);
                    let reader = builder.build()?;
                    let mut rg_batches = Vec::new();

                    for maybe_rb in reader {
                        let rb = maybe_rb?;
                        let rb_len = rb.num_rows();
                        if rb_len == 0 {
                            continue;
                        }

                        if rb_len <= BATCH_ROWS {
                            let mut cols = Vec::with_capacity(rb.num_columns());
                            for (col_idx, arr) in rb.columns().iter().enumerate() {
                                let dtype = meta.schema.fields[col_idx].dtype;
                                let col = convert_arrow_column(arr, dtype)?;
                                cols.push(Arc::new(col));
                            }
                            rg_batches.push(Batch::new(Arc::clone(&meta.schema), cols));
                        } else {
                            let mut offset = 0;
                            while offset < rb_len {
                                let chunk_len = (rb_len - offset).min(BATCH_ROWS);
                                let sliced_rb = rb.slice(offset, chunk_len);
                                let mut cols = Vec::with_capacity(sliced_rb.num_columns());
                                for (col_idx, arr) in sliced_rb.columns().iter().enumerate() {
                                    let dtype = meta.schema.fields[col_idx].dtype;
                                    let col = convert_arrow_column(arr, dtype)?;
                                    cols.push(Arc::new(col));
                                }
                                rg_batches.push(Batch::new(Arc::clone(&meta.schema), cols));
                                offset += chunk_len;
                            }
                        }
                    }
                    Ok(rg_batches)
                })();

                match res {
                    Ok(batches) => {
                        table_results[task.table_idx]
                            .lock()
                            .unwrap()
                            .push((task.rg_idx, batches));
                    }
                    Err(e) => {
                        let mut err_guard = error_cell.lock().unwrap();
                        if err_guard.is_none() {
                            *err_guard = Some(e);
                        }
                        break;
                    }
                }
            });
        }
    });

    if let Some(err) = error_cell.into_inner().unwrap() {
        return Err(err);
    }

    let mut tables = std::collections::HashMap::new();
    for (meta, res_mutex) in tables_meta.into_iter().zip(table_results) {
        let mut rgs = res_mutex.into_inner().unwrap();
        rgs.sort_by_key(|(rg_idx, _)| *rg_idx);

        let mut batches = Vec::new();
        let mut total_rows = 0;
        for (_, rg_batches) in rgs {
            for b in rg_batches {
                total_rows += b.len;
                batches.push(b);
            }
        }

        tables.insert(
            meta.name,
            Table {
                schema: meta.schema,
                batches,
                rows: total_rows,
            },
        );
    }

    Ok(Catalog { tables })
}

pub fn convert_arrow_schema(arrow_schema: &ArrowSchema) -> Result<Schema> {
    let mut fields = Vec::with_capacity(arrow_schema.fields().len());
    for f in arrow_schema.fields() {
        let name = f.name().clone();
        let nullable = f.is_nullable();
        let dtype = match f.data_type() {
            ArrowType::Boolean => DataType::Bool,
            ArrowType::Int32 => DataType::Int32,
            ArrowType::Int64 => DataType::Int64,
            ArrowType::Float64 => DataType::Float64,
            ArrowType::Decimal128(p, s) => {
                if *s < 0 {
                    bail!(
                        "unsupported negative scale {} in Decimal128 for column '{}'",
                        s,
                        f.name()
                    );
                }
                DataType::Decimal {
                    precision: *p,
                    scale: *s as u8,
                }
            }
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

pub fn convert_arrow_column(array: &ArrayRef, dtype: DataType) -> Result<Column> {
    let len = array.len();
    let validity = if array.null_count() > 0 {
        if let Some(nb) = array.nulls() {
            Some((0..len).map(|i| nb.is_valid(i)).collect())
        } else {
            Some((0..len).map(|i| !array.is_null(i)).collect())
        }
    } else {
        None
    };

    let offset = array.offset();

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
            let slice = &arr.values().as_ref()[offset..offset + len];
            ColumnData::Int32(slice.to_vec())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 array");
            let slice = &arr.values().as_ref()[offset..offset + len];
            ColumnData::Int64(slice.to_vec())
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float64 array");
            let slice = &arr.values().as_ref()[offset..offset + len];
            ColumnData::Float64(slice.to_vec())
        }
        DataType::Decimal { .. } => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("decimal128 array");
            let slice = &arr.values().as_ref()[offset..offset + len];
            ColumnData::Decimal(slice.to_vec())
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("date32 array");
            let slice = &arr.values().as_ref()[offset..offset + len];
            ColumnData::Date32(slice.to_vec())
        }
        DataType::Utf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                let raw_offsets = arr.value_offsets();
                let base_offset = raw_offsets[0] as usize;
                let end_offset = raw_offsets[len] as usize;
                let mut offsets = Vec::with_capacity(len + 1);
                for &off in raw_offsets {
                    offsets.push((off as usize - base_offset) as u32);
                }
                let bytes = arr.value_data()[base_offset..end_offset].to_vec();
                ColumnData::Utf8 { offsets, bytes }
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                let raw_offsets = arr.value_offsets();
                let base_offset = raw_offsets[0] as usize;
                let end_offset = raw_offsets[len] as usize;
                let mut offsets = Vec::with_capacity(len + 1);
                for &off in raw_offsets {
                    offsets.push((off as usize - base_offset) as u32);
                }
                let bytes = arr.value_data()[base_offset..end_offset].to_vec();
                ColumnData::Utf8 { offsets, bytes }
            } else {
                bail!("expected utf8 or large_utf8 array");
            }
        }
    };

    Ok(Column {
        dtype,
        data,
        validity,
    })
}

pub fn load_parquet_file(path: &Path) -> Result<(String, Table)> {
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

        if rb_len <= BATCH_ROWS {
            let mut cols = Vec::with_capacity(rb.num_columns());
            for (col_idx, arr) in rb.columns().iter().enumerate() {
                let dtype = schema.fields[col_idx].dtype;
                let col = convert_arrow_column(arr, dtype)?;
                cols.push(Arc::new(col));
            }
            let batch = Batch::new(Arc::clone(&schema), cols);
            total_rows += batch.len;
            batches.push(batch);
        } else {
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

pub fn convert_arrow_column_old(array: &ArrayRef, dtype: DataType) -> Result<Column> {
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

pub fn load_parquet_file_old(path: &Path) -> Result<(String, Table)> {
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

        let mut offset = 0;
        while offset < rb_len {
            let chunk_len = (rb_len - offset).min(BATCH_ROWS);
            let sliced_rb = rb.slice(offset, chunk_len);

            let mut cols = Vec::with_capacity(sliced_rb.num_columns());
            for (col_idx, arr) in sliced_rb.columns().iter().enumerate() {
                let dtype = schema.fields[col_idx].dtype;
                let col = convert_arrow_column_old(arr, dtype)?;
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

pub fn load_parquet_dir_old(dir: &Path, threads: usize) -> Result<Catalog> {
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
                let res = load_parquet_file_old(&path);
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
