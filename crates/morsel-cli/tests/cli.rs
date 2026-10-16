use std::fs::File;
use std::process::Command;
use std::sync::Arc;
use tempfile::tempdir;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
    StringArray,
};
use arrow::datatypes::{DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use serde_json::Value;

fn morsel_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_morsel"))
}

#[test]
fn test_csv_formatting_all_types() {
    let dir = tempdir().unwrap();
    let parquet_path = dir.path().join("all_types.parquet");
    let file = File::create(&parquet_path).unwrap();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("b", ArrowType::Boolean, true),
        ArrowField::new("i32", ArrowType::Int32, true),
        ArrowField::new("i64", ArrowType::Int64, true),
        ArrowField::new("f64", ArrowType::Float64, true),
        ArrowField::new("dec", ArrowType::Decimal128(15, 2), true),
        ArrowField::new("d32", ArrowType::Date32, true),
        ArrowField::new("s", ArrowType::Utf8, true),
    ]));

    // Row 0: non-null values
    // Row 1: null values
    // Row 2: negative values / edge values
    let b_vec: Vec<Option<bool>> = vec![Some(true), None, Some(false)];
    let i32_vec: Vec<Option<i32>> = vec![Some(42), None, Some(-100)];
    let i64_vec: Vec<Option<i64>> = vec![Some(9999999999), None, Some(-42)];
    let f64_vec: Vec<Option<f64>> = vec![Some(123.5), None, Some(-0.25)];
    let dec_vec: Vec<Option<i128>> = vec![Some(12345), None, Some(-50)]; // 123.45 and -0.50
    let d32_vec: Vec<Option<i32>> = vec![Some(9204), None, Some(0)]; // 1995-03-15 and 1970-01-01
    let s_vec: Vec<Option<String>> = vec![
        Some("hello, world".to_string()),
        None,
        Some("say \"hi\"".to_string()),
    ];

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

    let sql_path = dir.path().join("query.sql");
    std::fs::write(
        &sql_path,
        "SELECT b, i32, i64, f64, dec, d32, s FROM all_types",
    )
    .unwrap();

    let out_csv = dir.path().join("out.csv");

    let output = Command::new(morsel_binary())
        .args([
            "query",
            "--data",
            dir.path().to_str().unwrap(),
            "--sql-file",
            sql_path.to_str().unwrap(),
            "--out",
            out_csv.to_str().unwrap(),
        ])
        .output()
        .expect("failed to execute morsel");

    assert!(
        output.status.success(),
        "query failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let csv_content = std::fs::read_to_string(&out_csv).unwrap();
    let lines: Vec<&str> = csv_content.lines().collect();

    assert_eq!(lines.len(), 4);
    assert_eq!(lines[0], "b,i32,i64,f64,dec,d32,s");
    assert_eq!(
        lines[1],
        "true,42,9999999999,123.5,123.45,1995-03-15,\"hello, world\""
    );
    assert_eq!(lines[2], ",,,,,,");
    assert_eq!(
        lines[3],
        "false,-100,-42,-0.25,-0.50,1970-01-01,\"say \"\"hi\"\"\""
    );
}

#[test]
fn test_unknown_column_error_and_exit_code() {
    let dir = tempdir().unwrap();
    let parquet_path = dir.path().join("tiny.parquet");
    let file = File::create(&parquet_path).unwrap();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "x",
        ArrowType::Int32,
        false,
    )]));
    let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1, 2, 3]))];
    let rb = RecordBatch::try_new(Arc::clone(&arrow_schema), columns).unwrap();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
    writer.write(&rb).unwrap();
    writer.close().unwrap();

    let sql_path = dir.path().join("query.sql");
    std::fs::write(&sql_path, "SELECT nonexistent_col FROM tiny").unwrap();

    let output = Command::new(morsel_binary())
        .args([
            "query",
            "--data",
            dir.path().to_str().unwrap(),
            "--sql-file",
            sql_path.to_str().unwrap(),
        ])
        .output()
        .expect("failed to execute morsel");

    assert_eq!(output.status.code(), Some(1));

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr_lines: Vec<&str> = stderr.trim().lines().collect();

    // Must print exactly one line to stderr naming the stage
    assert_eq!(
        stderr_lines.len(),
        1,
        "expected single line to stderr, got: {:?}",
        stderr_lines
    );
    assert!(
        stderr_lines[0].starts_with("plan error:"),
        "expected stderr to start with 'plan error:', got: {}",
        stderr_lines[0]
    );
    assert!(
        stderr_lines[0].contains("nonexistent_col"),
        "expected stderr to mention nonexistent_col, got: {}",
        stderr_lines[0]
    );
}

#[test]
fn test_timing_json_line_contents() {
    let dir = tempdir().unwrap();
    let parquet_path = dir.path().join("tiny.parquet");
    let file = File::create(&parquet_path).unwrap();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "x",
        ArrowType::Int32,
        false,
    )]));
    let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![10, 20, 30]))];
    let rb = RecordBatch::try_new(Arc::clone(&arrow_schema), columns).unwrap();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
    writer.write(&rb).unwrap();
    writer.close().unwrap();

    let sql_path = dir.path().join("query.sql");
    std::fs::write(&sql_path, "SELECT x FROM tiny").unwrap();

    let timing_path = dir.path().join("timing.json");

    let output = Command::new(morsel_binary())
        .args([
            "query",
            "--data",
            dir.path().to_str().unwrap(),
            "--sql-file",
            sql_path.to_str().unwrap(),
            "--threads",
            "2",
            "--repeat",
            "3",
            "--timing-json",
            timing_path.to_str().unwrap(),
        ])
        .output()
        .expect("failed to execute morsel");

    assert!(
        output.status.success(),
        "query failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let timing_content = std::fs::read_to_string(&timing_path).unwrap();
    let lines: Vec<&str> = timing_content.lines().collect();

    assert_eq!(lines.len(), 3, "expected 3 lines for --repeat 3");

    for line in lines {
        let v: Value = serde_json::from_str(line).expect("valid JSON line");

        assert!(v.get("load_ms").and_then(Value::as_f64).is_some());
        assert!(v.get("plan_ms").and_then(Value::as_f64).is_some());
        assert!(v.get("execute_ms").and_then(Value::as_f64).is_some());
        assert_eq!(v.get("rows").and_then(Value::as_u64), Some(3));
        assert_eq!(v.get("threads").and_then(Value::as_u64), Some(2));
        assert_eq!(
            v.get("query_file").and_then(Value::as_str),
            Some(sql_path.to_str().unwrap())
        );
        assert_eq!(
            v.get("data_dir").and_then(Value::as_str),
            Some(dir.path().to_str().unwrap())
        );
        let commit = v.get("git_commit").and_then(Value::as_str).unwrap();
        assert!(!commit.is_empty());
        let hostname = v.get("hostname").and_then(Value::as_str).unwrap();
        assert!(!hostname.is_empty());
    }
}
