use std::fs::File;
use std::sync::Arc;
use tempfile::tempdir;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
    StringArray, UInt64Array,
};
use arrow::datatypes::{DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use morsel_core::{DataType, Scalar, BATCH_ROWS};
use morsel_exec::catalog::load_parquet_dir;
use parquet::arrow::ArrowWriter;

#[test]
fn loads_types_and_rows() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("all_types.parquet");
    let file = File::create(&file_path).unwrap();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("b", ArrowType::Boolean, true),
        ArrowField::new("i32", ArrowType::Int32, true),
        ArrowField::new("i64", ArrowType::Int64, true),
        ArrowField::new("f64", ArrowType::Float64, true),
        ArrowField::new("dec", ArrowType::Decimal128(15, 2), true),
        ArrowField::new("d32", ArrowType::Date32, true),
        ArrowField::new("s", ArrowType::Utf8, true),
    ]));

    let n: usize = 5000;
    let b_vec: Vec<Option<bool>> = (0..n).map(|i| Some(i % 2 == 0)).collect();
    let i32_vec: Vec<Option<i32>> = (0..n).map(|i| Some(i as i32)).collect();
    let i64_vec: Vec<Option<i64>> = (0..n).map(|i| Some(i as i64 * 10)).collect();
    let f64_vec: Vec<Option<f64>> = (0..n).map(|i| Some(i as f64 + 0.5)).collect();
    let dec_vec: Vec<Option<i128>> = (0..n).map(|i| Some(i as i128 * 100 + 25)).collect();
    let d32_vec: Vec<Option<i32>> = (0..n).map(|i| Some(i as i32 + 10000)).collect();
    let s_vec: Vec<Option<String>> = (0..n).map(|i| Some(format!("str_{}", i))).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(b_vec)),
        Arc::new(Int32Array::from(i32_vec)),
        Arc::new(Int64Array::from(i64_vec)),
        Arc::new(Float64Array::from(f64_vec)),
        Arc::new(
            Decimal128Array::from(dec_vec)
                .with_precision_and_scale(15, 2)
                .unwrap(),
        ),
        Arc::new(Date32Array::from(d32_vec)),
        Arc::new(StringArray::from(s_vec)),
    ];

    let rb = RecordBatch::try_new(Arc::clone(&arrow_schema), columns).unwrap();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
    writer.write(&rb).unwrap();
    writer.close().unwrap();

    let catalog = load_parquet_dir(dir.path(), 2).unwrap();
    assert_eq!(catalog.tables.len(), 1);

    let table = catalog.tables.get("all_types").expect("table all_types");
    assert_eq!(table.rows, 5000);
    assert_eq!(table.schema.fields[0].dtype, DataType::Bool);
    assert_eq!(table.schema.fields[1].dtype, DataType::Int32);
    assert_eq!(table.schema.fields[2].dtype, DataType::Int64);
    assert_eq!(table.schema.fields[3].dtype, DataType::Float64);
    assert_eq!(
        table.schema.fields[4].dtype,
        DataType::Decimal {
            precision: 15,
            scale: 2
        }
    );
    assert_eq!(table.schema.fields[5].dtype, DataType::Date32);
    assert_eq!(table.schema.fields[6].dtype, DataType::Utf8);

    for batch in &table.batches {
        assert!(batch.len <= BATCH_ROWS);
    }

    let check_row = |row: usize| -> (Scalar, Scalar, Scalar, Scalar, Scalar, Scalar, Scalar) {
        let mut rem = row;
        for batch in &table.batches {
            if rem < batch.len {
                return (
                    batch.columns[0].get(rem),
                    batch.columns[1].get(rem),
                    batch.columns[2].get(rem),
                    batch.columns[3].get(rem),
                    batch.columns[4].get(rem),
                    batch.columns[5].get(rem),
                    batch.columns[6].get(rem),
                );
            }
            rem -= batch.len;
        }
        panic!("row {} out of bounds", row);
    };

    for &row in &[0, 2047, 2048, 4999] {
        let (b, i32_val, i64_val, f64_val, dec_val, d32_val, s_val) = check_row(row);
        assert_eq!(b, Scalar::Bool(row % 2 == 0));
        assert_eq!(i32_val, Scalar::Int32(row as i32));
        assert_eq!(i64_val, Scalar::Int64(row as i64 * 10));
        assert_eq!(f64_val, Scalar::Float64(row as f64 + 0.5));
        assert_eq!(dec_val, Scalar::Decimal(row as i128 * 100 + 25, 2));
        assert_eq!(d32_val, Scalar::Date32(row as i32 + 10000));
        assert_eq!(s_val, Scalar::Utf8(format!("str_{}", row)));
    }
}

#[test]
fn table_name_is_file_stem() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("customer_orders.parquet");
    let file = File::create(&file_path).unwrap();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "id",
        ArrowType::Int32,
        false,
    )]));
    let rb = RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
    writer.write(&rb).unwrap();
    writer.close().unwrap();

    let catalog = load_parquet_dir(dir.path(), 1).unwrap();
    assert!(catalog.tables.contains_key("customer_orders"));
}

#[test]
fn unsupported_type_errors() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("unsupported.parquet");
    let file = File::create(&file_path).unwrap();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "bad_col",
        ArrowType::UInt64,
        false,
    )]));
    let rb = RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![Arc::new(UInt64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
    writer.write(&rb).unwrap();
    writer.close().unwrap();

    let res = load_parquet_dir(dir.path(), 1);
    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.contains("bad_col"),
        "error message '{}' should name bad_col",
        err_msg
    );
}

#[test]
fn negative_scale_decimal_errors() {
    let arrow_schema = ArrowSchema::new(vec![ArrowField::new(
        "neg_scale",
        ArrowType::Decimal128(10, -2),
        false,
    )]);
    let res = morsel_exec::parquet_load::convert_arrow_schema(&arrow_schema);
    assert!(
        res.is_err(),
        "should error on negative scale, but got: {:?}",
        res
    );
}
