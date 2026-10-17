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

fn assert_columns_equal(col1: &Column, col2: &Column, msg: &str) {
    assert_eq!(col1.dtype, col2.dtype, "{}: dtype mismatch", msg);
    assert_eq!(col1.validity, col2.validity, "{}: validity mismatch", msg);
    assert_eq!(col1.len(), col2.len(), "{}: length mismatch", msg);
    for i in 0..col1.len() {
        let s1 = col1.get(i);
        let s2 = col2.get(i);
        match (&s1, &s2) {
            (Scalar::Float64(a), Scalar::Float64(b)) => {
                if a.is_nan() && b.is_nan() {
                    continue;
                }
                assert_eq!(a, b, "{}: mismatch at row {}", msg, i);
            }
            _ => {
                assert_eq!(s1, s2, "{}: mismatch at row {}", msg, i);
            }
        }
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

#[test]
fn test_arithmetic_fast_vs_generic() {
    use morsel_exec::eval::eval_arithmetic_generic;
    use morsel_exec::kernel::arithmetic::{
        eval_arithmetic_col_col_fast, eval_arithmetic_col_lit_fast,
    };

    const N: usize = 10_000;
    let mut rng = SimpleRng::new(123);
    let arith_ops = [BinOp::Add, BinOp::Sub, BinOp::Mul];

    // 1. Int64
    {
        let mut v1 = Vec::with_capacity(N);
        let mut v2 = Vec::with_capacity(N);
        let mut val1 = Vec::with_capacity(N);
        let mut val2 = Vec::with_capacity(N);
        for _ in 0..N {
            v1.push((rng.next_i64() % 100_000).abs());
            v2.push((rng.next_i64() % 100_000).abs());
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
        let lit = Scalar::Int64(500);
        let lit_col = make_lit_column(&lit, N);

        for &op in &arith_ops {
            let fast_col = eval_arithmetic_col_col_fast(op, &col1, &col2)
                .unwrap()
                .expect("Int64 col-col arithmetic fast path must be implemented");
            let generic_col = eval_arithmetic_generic(op, &col1, &col2, N).unwrap();
            assert_eq!(fast_col, generic_col, "Int64 col-col mismatch for {:?}", op);

            let fast_lit = eval_arithmetic_col_lit_fast(op, &col1, &lit)
                .unwrap()
                .expect("Int64 col-lit arithmetic fast path must be implemented");
            let generic_lit = eval_arithmetic_generic(op, &col1, &lit_col, N).unwrap();
            assert_eq!(fast_lit, generic_lit, "Int64 col-lit mismatch for {:?}", op);
        }
    }

    // 2. Float64
    {
        let mut v1 = Vec::with_capacity(N);
        let mut v2 = Vec::with_capacity(N);
        let mut val1 = Vec::with_capacity(N);
        let mut val2 = Vec::with_capacity(N);
        for _ in 0..N {
            v1.push((rng.next_f64()).abs());
            v2.push((rng.next_f64()).abs() + 1.0);
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
        let lit = Scalar::Float64(2.5);
        let lit_col = make_lit_column(&lit, N);

        for &op in &arith_ops {
            let fast_col = eval_arithmetic_col_col_fast(op, &col1, &col2)
                .unwrap()
                .expect("Float64 col-col arithmetic fast path must be implemented");
            let generic_col = eval_arithmetic_generic(op, &col1, &col2, N).unwrap();
            assert_columns_equal(
                &fast_col,
                &generic_col,
                &format!("Float64 col-col {:?}", op),
            );

            let fast_lit = eval_arithmetic_col_lit_fast(op, &col1, &lit)
                .unwrap()
                .expect("Float64 col-lit arithmetic fast path must be implemented");
            let generic_lit = eval_arithmetic_generic(op, &col1, &lit_col, N).unwrap();
            assert_columns_equal(
                &fast_lit,
                &generic_lit,
                &format!("Float64 col-lit {:?}", op),
            );
        }
    }

    // 3. Decimal col vs col (equal scale and differing scale)
    {
        let dtype1 = DataType::Decimal {
            precision: 38,
            scale: 2,
        };
        let dtype2 = DataType::Decimal {
            precision: 38,
            scale: 2,
        };
        let mut v1 = Vec::with_capacity(N);
        let mut v2 = Vec::with_capacity(N);
        let mut val1 = Vec::with_capacity(N);
        let mut val2 = Vec::with_capacity(N);
        for _ in 0..N {
            v1.push(((rng.next_i64() % 100_000).abs()) as i128);
            v2.push(((rng.next_i64() % 100_000).abs()) as i128);
            val1.push(rng.next_bool_null(15));
            val2.push(rng.next_bool_null(15));
        }
        let col1 = Column {
            dtype: dtype1,
            data: ColumnData::Decimal(v1),
            validity: Some(val1),
        };
        let col2 = Column {
            dtype: dtype2,
            data: ColumnData::Decimal(v2),
            validity: Some(val2),
        };
        let lit_dec = Scalar::Decimal(1234, 2);
        let lit_dec_col = make_lit_column(&lit_dec, N);

        for &op in &arith_ops {
            let fast_col = eval_arithmetic_col_col_fast(op, &col1, &col2)
                .unwrap()
                .expect("Decimal col-col arithmetic fast path must be implemented");
            let generic_col = eval_arithmetic_generic(op, &col1, &col2, N).unwrap();
            assert_eq!(
                fast_col, generic_col,
                "Decimal col-col mismatch for {:?}",
                op
            );

            let fast_lit = eval_arithmetic_col_lit_fast(op, &col1, &lit_dec)
                .unwrap()
                .expect("Decimal col-lit arithmetic fast path must be implemented");
            let generic_lit = eval_arithmetic_generic(op, &col1, &lit_dec_col, N).unwrap();
            assert_eq!(
                fast_lit, generic_lit,
                "Decimal col-lit mismatch for {:?}",
                op
            );
        }
    }

    // 4. Decimal or Int with literal
    {
        let dtype_dec = DataType::Decimal {
            precision: 38,
            scale: 2,
        };
        let mut v_dec = Vec::with_capacity(N);
        let mut val_dec = Vec::with_capacity(N);
        for _ in 0..N {
            v_dec.push(((rng.next_i64() % 50_000).abs()) as i128);
            val_dec.push(rng.next_bool_null(15));
        }
        let col_dec = Column {
            dtype: dtype_dec,
            data: ColumnData::Decimal(v_dec),
            validity: Some(val_dec),
        };
        let lit_int = Scalar::Int32(5);
        let lit_int_col = make_lit_column(&lit_int, N);

        for &op in &arith_ops {
            let fast_lit = eval_arithmetic_col_lit_fast(op, &col_dec, &lit_int)
                .unwrap()
                .expect("Decimal col with Int lit fast path must be implemented");
            let generic_lit = eval_arithmetic_generic(op, &col_dec, &lit_int_col, N).unwrap();
            assert_eq!(
                fast_lit, generic_lit,
                "Decimal with Int lit mismatch for {:?}",
                op
            );
        }
    }
}

#[test]
fn test_boolean_fast_vs_generic() {
    use morsel_exec::eval::{eval_and_generic, eval_not_generic, eval_or_generic};
    use morsel_exec::kernel::boolean::{eval_and_fast, eval_not_fast, eval_or_fast};

    const N: usize = 10_000;
    let mut rng = SimpleRng::new(999);

    let mut v1 = Vec::with_capacity(N);
    let mut v2 = Vec::with_capacity(N);
    let mut val1 = Vec::with_capacity(N);
    let mut val2 = Vec::with_capacity(N);
    for _ in 0..N {
        v1.push((rng.next_u64() % 2) == 1);
        v2.push((rng.next_u64() % 2) == 1);
        val1.push(rng.next_bool_null(20));
        val2.push(rng.next_bool_null(20));
    }
    let col1 = Column {
        dtype: DataType::Bool,
        data: ColumnData::Bool(v1),
        validity: Some(val1),
    };
    let col2 = Column {
        dtype: DataType::Bool,
        data: ColumnData::Bool(v2),
        validity: Some(val2),
    };

    // Test NOT
    let fast_not = eval_not_fast(&col1).expect("NOT fast path must be implemented");
    let generic_not = eval_not_generic(&col1, N).unwrap();
    assert_eq!(fast_not, generic_not, "NOT mismatch");

    // Test AND
    let fast_and = eval_and_fast(&col1, &col2).expect("AND fast path must be implemented");
    let generic_and = eval_and_generic(&col1, &col2, N).unwrap();
    assert_eq!(fast_and, generic_and, "AND mismatch");

    // Test OR
    let fast_or = eval_or_fast(&col1, &col2).expect("OR fast path must be implemented");
    let generic_or = eval_or_generic(&col1, &col2, N).unwrap();
    assert_eq!(fast_or, generic_or, "OR mismatch");
}

#[test]
fn test_filter_selection_fast_vs_generic() {
    use morsel_exec::eval::eval_predicate_generic;
    use morsel_exec::kernel::filter::bool_to_selection;

    const N: usize = 10_000;
    let mut rng = SimpleRng::new(777);

    // Case 1: Random bools with 20% nulls
    let mut data1 = Vec::with_capacity(N);
    let mut val1 = Vec::with_capacity(N);
    for _ in 0..N {
        data1.push((rng.next_u64() % 2) == 1);
        val1.push(rng.next_bool_null(20));
    }
    let col1 = Column {
        dtype: DataType::Bool,
        data: ColumnData::Bool(data1.clone()),
        validity: Some(val1.clone()),
    };
    let fast1 = bool_to_selection(&data1, Some(&val1));
    let generic1 = eval_predicate_generic(&col1);
    assert_eq!(fast1, generic1, "Case 1 random with nulls mismatch");

    // Case 2: Random bools without validity (None)
    let col2 = Column {
        dtype: DataType::Bool,
        data: ColumnData::Bool(data1.clone()),
        validity: None,
    };
    let fast2 = bool_to_selection(&data1, None);
    let generic2 = eval_predicate_generic(&col2);
    assert_eq!(fast2, generic2, "Case 2 random without nulls mismatch");

    // Case 3: All false
    let data_all_false = vec![false; N];
    let col3 = Column {
        dtype: DataType::Bool,
        data: ColumnData::Bool(data_all_false.clone()),
        validity: Some(val1.clone()),
    };
    let fast3 = bool_to_selection(&data_all_false, Some(&val1));
    let generic3 = eval_predicate_generic(&col3);
    assert_eq!(fast3, generic3, "Case 3 all false mismatch");
    assert!(fast3.is_empty());

    // Case 4: All true
    let data_all_true = vec![true; N];
    let val_all_valid = vec![true; N];
    let col4 = Column {
        dtype: DataType::Bool,
        data: ColumnData::Bool(data_all_true.clone()),
        validity: Some(val_all_valid.clone()),
    };
    let fast4 = bool_to_selection(&data_all_true, Some(&val_all_valid));
    let generic4 = eval_predicate_generic(&col4);
    assert_eq!(fast4, generic4, "Case 4 all true mismatch");
    assert_eq!(fast4.len(), N);

    // Case 5: Highly selective (~1% true)
    let mut data_sparse = Vec::with_capacity(N);
    for _ in 0..N {
        data_sparse.push(rng.next_u64().is_multiple_of(100));
    }
    let col5 = Column {
        dtype: DataType::Bool,
        data: ColumnData::Bool(data_sparse.clone()),
        validity: Some(val1.clone()),
    };
    let fast5 = bool_to_selection(&data_sparse, Some(&val1));
    let generic5 = eval_predicate_generic(&col5);
    assert_eq!(fast5, generic5, "Case 5 sparse mismatch");

    // Case 6: Non-multiple of 64 length
    let data_short = data1[0..137].to_vec();
    let val_short = val1[0..137].to_vec();
    let col6 = Column {
        dtype: DataType::Bool,
        data: ColumnData::Bool(data_short.clone()),
        validity: Some(val_short.clone()),
    };
    let fast6 = bool_to_selection(&data_short, Some(&val_short));
    let generic6 = eval_predicate_generic(&col6);
    assert_eq!(fast6, generic6, "Case 6 non-multiple of 64 mismatch");
}
