use morsel_core::{Column, ColumnData, DataType, Scalar};
use morsel_exec::eval::eval_comparison_generic;
use morsel_exec::kernel::comparison::{eval_comparison_col_col_fast, eval_comparison_col_lit_fast};
use morsel_plan::BinOp;

struct SimpleRng(u64);

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.0
    }

    fn next_i32(&mut self) -> i32 {
        (self.next_u64() >> 32) as i32
    }

    fn next_i64(&mut self) -> i64 {
        self.next_u64() as i64
    }

    fn next_f64(&mut self) -> f64 {
        let bits = self.next_u64();
        // Occasionally produce NaN (about 1 in 1000)
        if bits.is_multiple_of(1000) {
            f64::NAN
        } else {
            (bits as f64) / (u64::MAX as f64) * 2000.0 - 1000.0
        }
    }

    fn next_bool_null(&mut self, null_rate_pct: u64) -> bool {
        (self.next_u64() % 100) >= null_rate_pct
    }
}

fn make_lit_column(scalar: &Scalar, len: usize) -> Column {
    let (dtype, data) = match scalar {
        Scalar::Int32(x) => (DataType::Int32, ColumnData::Int32(vec![*x; len])),
        Scalar::Int64(x) => (DataType::Int64, ColumnData::Int64(vec![*x; len])),
        Scalar::Float64(f) => (DataType::Float64, ColumnData::Float64(vec![*f; len])),
        Scalar::Date32(d) => (DataType::Date32, ColumnData::Date32(vec![*d; len])),
        Scalar::Decimal(v, scale) => (
            DataType::Decimal {
                precision: 38,
                scale: *scale,
            },
            ColumnData::Decimal(vec![*v; len]),
        ),
        Scalar::Utf8(s) => {
            let b = s.as_bytes();
            let mut offsets = Vec::with_capacity(len + 1);
            offsets.push(0);
            let mut bytes = Vec::with_capacity(len * b.len());
            for i in 0..len {
                bytes.extend_from_slice(b);
                offsets.push((i + 1) as u32 * b.len() as u32);
            }
            (DataType::Utf8, ColumnData::Utf8 { offsets, bytes })
        }
        Scalar::Null => (DataType::Int32, ColumnData::Int32(vec![0; len])),
        _ => panic!("unsupported scalar for make_lit_column"),
    };
    let validity = if matches!(scalar, Scalar::Null) {
        Some(vec![false; len])
    } else {
        None
    };
    Column {
        dtype,
        data,
        validity,
    }
}

#[test]
fn test_comparison_fast_vs_generic() {
    const N: usize = 10_000;
    let mut rng = SimpleRng::new(42);

    let all_ops = [
        BinOp::Eq,
        BinOp::NotEq,
        BinOp::Lt,
        BinOp::LtEq,
        BinOp::Gt,
        BinOp::GtEq,
    ];
    let eq_ops = [BinOp::Eq, BinOp::NotEq];

    // 1. Int32
    {
        let mut v1 = Vec::with_capacity(N);
        let mut v2 = Vec::with_capacity(N);
        let mut val1 = Vec::with_capacity(N);
        let mut val2 = Vec::with_capacity(N);
        for _ in 0..N {
            v1.push(rng.next_i32() % 500);
            v2.push(rng.next_i32() % 500);
            val1.push(rng.next_bool_null(15));
            val2.push(rng.next_bool_null(15));
        }
        let col1 = Column {
            dtype: DataType::Int32,
            data: ColumnData::Int32(v1),
            validity: Some(val1),
        };
        let col2 = Column {
            dtype: DataType::Int32,
            data: ColumnData::Int32(v2),
            validity: Some(val2),
        };
        let lit = Scalar::Int32(42);
        let lit_col = make_lit_column(&lit, N);

        for &op in &all_ops {
            let fast_col = eval_comparison_col_col_fast(op, &col1, &col2)
                .expect("Int32 col-col fast path must be implemented");
            let generic_col = eval_comparison_generic(op, &col1, &col2, N).unwrap();
            assert_eq!(fast_col, generic_col, "Int32 col-col mismatch for {:?}", op);

            let fast_lit = eval_comparison_col_lit_fast(op, &col1, &lit)
                .expect("Int32 col-lit fast path must be implemented");
            let generic_lit = eval_comparison_generic(op, &col1, &lit_col, N).unwrap();
            assert_eq!(fast_lit, generic_lit, "Int32 col-lit mismatch for {:?}", op);
        }
    }

    // 2. Int64
    {
        let mut v1 = Vec::with_capacity(N);
        let mut v2 = Vec::with_capacity(N);
        let mut val1 = Vec::with_capacity(N);
        let mut val2 = Vec::with_capacity(N);
        for _ in 0..N {
            v1.push(rng.next_i64() % 1000);
            v2.push(rng.next_i64() % 1000);
            val1.push(rng.next_bool_null(15));
            val2.push(rng.next_bool_null(15));
        }
        let col1 = Column {
            dtype: DataType::Int64,
            data: ColumnData::Int64(v1),
            validity: Some(val1),
        };
        let col2 = Column {
            dtype: DataType::Int64,
            data: ColumnData::Int64(v2),
            validity: Some(val2),
        };
        let lit = Scalar::Int64(100);
        let lit_col = make_lit_column(&lit, N);

        for &op in &all_ops {
            let fast_col = eval_comparison_col_col_fast(op, &col1, &col2)
                .expect("Int64 col-col fast path must be implemented");
            let generic_col = eval_comparison_generic(op, &col1, &col2, N).unwrap();
            assert_eq!(fast_col, generic_col, "Int64 col-col mismatch for {:?}", op);

            let fast_lit = eval_comparison_col_lit_fast(op, &col1, &lit)
                .expect("Int64 col-lit fast path must be implemented");
            let generic_lit = eval_comparison_generic(op, &col1, &lit_col, N).unwrap();
            assert_eq!(fast_lit, generic_lit, "Int64 col-lit mismatch for {:?}", op);
        }
    }

    // 3. Float64
    {
        let mut v1 = Vec::with_capacity(N);
        let mut v2 = Vec::with_capacity(N);
        let mut val1 = Vec::with_capacity(N);
        let mut val2 = Vec::with_capacity(N);
        for _ in 0..N {
            v1.push(rng.next_f64());
            v2.push(rng.next_f64());
            val1.push(rng.next_bool_null(15));
            val2.push(rng.next_bool_null(15));
        }
        let col1 = Column {
            dtype: DataType::Float64,
            data: ColumnData::Float64(v1),
            validity: Some(val1),
        };
        let col2 = Column {
            dtype: DataType::Float64,
            data: ColumnData::Float64(v2),
            validity: Some(val2),
        };
        let lit = Scalar::Float64(0.0);
        let lit_col = make_lit_column(&lit, N);

        for &op in &all_ops {
            let fast_col = eval_comparison_col_col_fast(op, &col1, &col2)
                .expect("Float64 col-col fast path must be implemented");
            let generic_col = eval_comparison_generic(op, &col1, &col2, N).unwrap();
            assert_eq!(
                fast_col, generic_col,
                "Float64 col-col mismatch for {:?}",
                op
            );

            let fast_lit = eval_comparison_col_lit_fast(op, &col1, &lit)
                .expect("Float64 col-lit fast path must be implemented");
            let generic_lit = eval_comparison_generic(op, &col1, &lit_col, N).unwrap();
            assert_eq!(
                fast_lit, generic_lit,
                "Float64 col-lit mismatch for {:?}",
                op
            );
        }
    }

    // 4. Date32
    {
        let mut v1 = Vec::with_capacity(N);
        let mut v2 = Vec::with_capacity(N);
        let mut val1 = Vec::with_capacity(N);
        let mut val2 = Vec::with_capacity(N);
        for _ in 0..N {
            v1.push((rng.next_i32() % 10000).abs());
            v2.push((rng.next_i32() % 10000).abs());
            val1.push(rng.next_bool_null(15));
            val2.push(rng.next_bool_null(15));
        }
        let col1 = Column {
            dtype: DataType::Date32,
            data: ColumnData::Date32(v1),
            validity: Some(val1),
        };
        let col2 = Column {
            dtype: DataType::Date32,
            data: ColumnData::Date32(v2),
            validity: Some(val2),
        };
        let lit = Scalar::Date32(9500);
        let lit_col = make_lit_column(&lit, N);

        for &op in &all_ops {
            let fast_col = eval_comparison_col_col_fast(op, &col1, &col2)
                .expect("Date32 col-col fast path must be implemented");
            let generic_col = eval_comparison_generic(op, &col1, &col2, N).unwrap();
            assert_eq!(
                fast_col, generic_col,
                "Date32 col-col mismatch for {:?}",
                op
            );

            let fast_lit = eval_comparison_col_lit_fast(op, &col1, &lit)
                .expect("Date32 col-lit fast path must be implemented");
            let generic_lit = eval_comparison_generic(op, &col1, &lit_col, N).unwrap();
            assert_eq!(
                fast_lit, generic_lit,
                "Date32 col-lit mismatch for {:?}",
                op
            );
        }
    }

    // 5. Decimal with equal scale (scale 2)
    {
        let dtype = DataType::Decimal {
            precision: 38,
            scale: 2,
        };
        let mut v1 = Vec::with_capacity(N);
        let mut v2 = Vec::with_capacity(N);
        let mut val1 = Vec::with_capacity(N);
        let mut val2 = Vec::with_capacity(N);
        for _ in 0..N {
            v1.push((rng.next_i64() % 50000) as i128);
            v2.push((rng.next_i64() % 50000) as i128);
            val1.push(rng.next_bool_null(15));
            val2.push(rng.next_bool_null(15));
        }
        let col1 = Column {
            dtype,
            data: ColumnData::Decimal(v1),
            validity: Some(val1),
        };
        let col2 = Column {
            dtype,
            data: ColumnData::Decimal(v2),
            validity: Some(val2),
        };
        let lit = Scalar::Decimal(15000, 2);
        let lit_col = make_lit_column(&lit, N);

        for &op in &all_ops {
            let fast_col = eval_comparison_col_col_fast(op, &col1, &col2)
                .expect("Decimal col-col fast path must be implemented");
            let generic_col = eval_comparison_generic(op, &col1, &col2, N).unwrap();
            assert_eq!(
                fast_col, generic_col,
                "Decimal col-col mismatch for {:?}",
                op
            );

            let fast_lit = eval_comparison_col_lit_fast(op, &col1, &lit)
                .expect("Decimal col-lit fast path must be implemented");
            let generic_lit = eval_comparison_generic(op, &col1, &lit_col, N).unwrap();
            assert_eq!(
                fast_lit, generic_lit,
                "Decimal col-lit mismatch for {:?}",
                op
            );
        }
    }

    // 6. Utf8 equality (Eq, NotEq)
    {
        let words = ["AIR", "FOB", "RAIL", "SHIP", "TRUCK", "MAIL", "REG AIR"];
        let mut strings1 = Vec::with_capacity(N);
        let mut strings2 = Vec::with_capacity(N);
        let mut val1 = Vec::with_capacity(N);
        let mut val2 = Vec::with_capacity(N);
        for _ in 0..N {
            strings1.push(words[(rng.next_u64() as usize) % words.len()]);
            strings2.push(words[(rng.next_u64() as usize) % words.len()]);
            val1.push(rng.next_bool_null(15));
            val2.push(rng.next_bool_null(15));
        }
        let mut b1 = Vec::new();
        let mut o1 = Vec::with_capacity(N + 1);
        o1.push(0);
        for s in &strings1 {
            b1.extend_from_slice(s.as_bytes());
            o1.push(b1.len() as u32);
        }
        let mut b2 = Vec::new();
        let mut o2 = Vec::with_capacity(N + 1);
        o2.push(0);
        for s in &strings2 {
            b2.extend_from_slice(s.as_bytes());
            o2.push(b2.len() as u32);
        }
        let col1 = Column {
            dtype: DataType::Utf8,
            data: ColumnData::Utf8 {
                offsets: o1,
                bytes: b1,
            },
            validity: Some(val1),
        };
        let col2 = Column {
            dtype: DataType::Utf8,
            data: ColumnData::Utf8 {
                offsets: o2,
                bytes: b2,
            },
            validity: Some(val2),
        };
        let lit = Scalar::Utf8("SHIP".to_string());
        let lit_col = make_lit_column(&lit, N);

        for &op in &eq_ops {
            let fast_col = eval_comparison_col_col_fast(op, &col1, &col2)
                .expect("Utf8 col-col fast path must be implemented");
            let generic_col = eval_comparison_generic(op, &col1, &col2, N).unwrap();
            assert_eq!(fast_col, generic_col, "Utf8 col-col mismatch for {:?}", op);

            let fast_lit = eval_comparison_col_lit_fast(op, &col1, &lit)
                .expect("Utf8 col-lit fast path must be implemented");
            let generic_lit = eval_comparison_generic(op, &col1, &lit_col, N).unwrap();
            assert_eq!(fast_lit, generic_lit, "Utf8 col-lit mismatch for {:?}", op);
        }
    }
}
