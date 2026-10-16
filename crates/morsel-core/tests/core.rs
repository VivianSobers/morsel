use morsel_core::{Batch, Column, ColumnData, DataType, Field, Scalar, Schema};
use std::sync::Arc;

#[test]
fn scalar_round_trip() {
    let cases = vec![
        (
            DataType::Bool,
            vec![Scalar::Bool(true), Scalar::Null, Scalar::Bool(false)],
        ),
        (
            DataType::Int32,
            vec![Scalar::Int32(42), Scalar::Null, Scalar::Int32(-10)],
        ),
        (
            DataType::Int64,
            vec![Scalar::Int64(1234567890), Scalar::Null, Scalar::Int64(-999)],
        ),
        (
            DataType::Float64,
            vec![Scalar::Float64(2.5), Scalar::Null, Scalar::Float64(-0.5)],
        ),
        (
            DataType::Decimal {
                precision: 15,
                scale: 2,
            },
            vec![
                Scalar::Decimal(150, 2),
                Scalar::Null,
                Scalar::Decimal(-2000, 2),
            ],
        ),
        (
            DataType::Date32,
            vec![Scalar::Date32(10500), Scalar::Null, Scalar::Date32(0)],
        ),
        (
            DataType::Utf8,
            vec![
                Scalar::Utf8("hello".to_string()),
                Scalar::Null,
                Scalar::Utf8("world".to_string()),
            ],
        ),
    ];

    for (dtype, values) in cases {
        let col = Column::from_scalars(dtype, &values);
        assert_eq!(col.len(), 3);
        assert!(col.validity.is_some());
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
        assert_eq!(col.get(0), values[0]);
        assert_eq!(col.get(1), Scalar::Null);
        assert_eq!(col.get(2), values[2]);

        // Non-null column should have validity None
        let non_null_values = vec![values[0].clone(), values[2].clone()];
        let col_no_null = Column::from_scalars(dtype, &non_null_values);
        assert_eq!(col_no_null.len(), 2);
        assert!(col_no_null.validity.is_none());
        assert_eq!(col_no_null.get(0), non_null_values[0]);
        assert_eq!(col_no_null.get(1), non_null_values[1]);
    }
}

#[test]
fn utf8_offsets() {
    let strings = ["", "ab", "c"];
    let scalars: Vec<Scalar> = strings
        .iter()
        .map(|s| Scalar::Utf8(s.to_string()))
        .collect();
    let col = Column::from_scalars(DataType::Utf8, &scalars);
    match &col.data {
        ColumnData::Utf8 { offsets, bytes } => {
            assert_eq!(offsets, &[0, 0, 2, 3]);
            assert_eq!(bytes, b"abc");
        }
        _ => panic!("expected ColumnData::Utf8"),
    }
    assert_eq!(col.utf8_value(0), "");
    assert_eq!(col.utf8_value(1), "ab");
    assert_eq!(col.utf8_value(2), "c");
}

#[test]
fn take_reorders_and_keeps_nulls() {
    let values = vec![
        Scalar::Int32(10),
        Scalar::Null,
        Scalar::Int32(30),
        Scalar::Int32(40),
    ];
    let col = Column::from_scalars(DataType::Int32, &values);
    let indices = [2, 1, 3, 0];
    let taken = col.take(&indices);
    assert_eq!(taken.len(), 4);
    assert_eq!(taken.get(0), Scalar::Int32(30));
    assert!(taken.is_null(1));
    assert_eq!(taken.get(1), Scalar::Null);
    assert_eq!(taken.get(2), Scalar::Int32(40));
    assert_eq!(taken.get(3), Scalar::Int32(10));

    // Also test take on Utf8
    let utf8_values = vec![
        Scalar::Utf8("a".to_string()),
        Scalar::Null,
        Scalar::Utf8("ccc".to_string()),
    ];
    let utf8_col = Column::from_scalars(DataType::Utf8, &utf8_values);
    let utf8_taken = utf8_col.take(&[2, 0, 1]);
    assert_eq!(utf8_taken.len(), 3);
    assert_eq!(utf8_taken.get(0), Scalar::Utf8("ccc".to_string()));
    assert_eq!(utf8_taken.get(1), Scalar::Utf8("a".to_string()));
    assert!(utf8_taken.is_null(2));
}

#[test]
fn batch_slice_shares_schema() {
    let n = 4100;
    let scalars: Vec<Scalar> = (0..n).map(Scalar::Int32).collect();
    let col = Arc::new(Column::from_scalars(DataType::Int32, &scalars));
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "id".to_string(),
            dtype: DataType::Int32,
            nullable: false,
        }],
    });
    let batch = Batch::new(Arc::clone(&schema), vec![col]);
    assert_eq!(batch.len, 4100);

    let sliced = batch.slice(2048, 5);
    assert_eq!(sliced.len, 5);
    assert_eq!(sliced.schema, schema);
    assert_eq!(sliced.columns.len(), 1);
    assert_eq!(sliced.columns[0].get(0), Scalar::Int32(2048));
    assert_eq!(sliced.columns[0].get(4), Scalar::Int32(2052));
}

#[test]
#[should_panic]
fn batch_new_panics_on_length_mismatch() {
    let col1 = Arc::new(Column::from_scalars(
        DataType::Int32,
        &[Scalar::Int32(1), Scalar::Int32(2)],
    ));
    let col2 = Arc::new(Column::from_scalars(DataType::Int32, &[Scalar::Int32(1)]));
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "a".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "b".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
        ],
    });
    Batch::new(schema, vec![col1, col2]);
}
