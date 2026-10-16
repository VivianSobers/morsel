use std::sync::Arc;

use morsel_core::{Batch, Column, DataType, Field, Scalar, Schema};
use morsel_exec::eval::{compare_scalars, eval, eval_predicate};
use morsel_plan::{BinOp, DateField, Expr};

#[test]
fn decimal_scale_rules() {
    // 1.50 * (1 - 0.06) = 1.4100 (scale 4)
    // 1.4100 * (1 + 0.08) = 1.522800 (scale 6)
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "price".to_string(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "discount".to_string(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "tax".to_string(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
        ],
    });

    let batch = Batch::new(
        schema,
        vec![
            Arc::new(Column::from_scalars(
                DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                &[Scalar::Decimal(150, 2)],
            )),
            Arc::new(Column::from_scalars(
                DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                &[Scalar::Decimal(6, 2)],
            )),
            Arc::new(Column::from_scalars(
                DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                &[Scalar::Decimal(8, 2)],
            )),
        ],
    );

    // 1 - discount
    let one_minus_disc = Expr::Binary {
        op: BinOp::Sub,
        left: Box::new(Expr::Literal(Scalar::Int32(1))),
        right: Box::new(Expr::Column(1)),
    };
    // price * (1 - discount)
    let discounted = Expr::Binary {
        op: BinOp::Mul,
        left: Box::new(Expr::Column(0)),
        right: Box::new(one_minus_disc),
    };
    let res1 = eval(&discounted, &batch).unwrap();
    assert_eq!(
        res1.dtype,
        DataType::Decimal {
            precision: 38,
            scale: 4
        }
    );
    assert_eq!(res1.get(0), Scalar::Decimal(14100, 4));

    // discounted * (1 + tax)
    let one_plus_tax = Expr::Binary {
        op: BinOp::Add,
        left: Box::new(Expr::Literal(Scalar::Int32(1))),
        right: Box::new(Expr::Column(2)),
    };
    let with_tax = Expr::Binary {
        op: BinOp::Mul,
        left: Box::new(discounted),
        right: Box::new(one_plus_tax),
    };
    let res2 = eval(&with_tax, &batch).unwrap();
    assert_eq!(
        res2.dtype,
        DataType::Decimal {
            precision: 38,
            scale: 6
        }
    );
    assert_eq!(res2.get(0), Scalar::Decimal(1522800, 6));
}

#[test]
fn decimal_add_aligns_scales() {
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "a".to_string(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "b".to_string(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 3,
                },
                nullable: false,
            },
        ],
    });
    let batch = Batch::new(
        schema,
        vec![
            Arc::new(Column::from_scalars(
                DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                &[Scalar::Decimal(150, 2)], // 1.50
            )),
            Arc::new(Column::from_scalars(
                DataType::Decimal {
                    precision: 15,
                    scale: 3,
                },
                &[Scalar::Decimal(5, 3)], // 0.005
            )),
        ],
    );
    let add_expr = Expr::Binary {
        op: BinOp::Add,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };
    let res = eval(&add_expr, &batch).unwrap();
    assert_eq!(
        res.dtype,
        DataType::Decimal {
            precision: 38,
            scale: 3
        }
    );
    assert_eq!(res.get(0), Scalar::Decimal(1505, 3));
}

#[test]
fn div_gives_float() {
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
    let batch = Batch::new(
        schema,
        vec![
            Arc::new(Column::from_scalars(DataType::Int32, &[Scalar::Int32(10)])),
            Arc::new(Column::from_scalars(DataType::Int32, &[Scalar::Int32(4)])),
        ],
    );
    let div_expr = Expr::Binary {
        op: BinOp::Div,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };
    let res = eval(&div_expr, &batch).unwrap();
    assert_eq!(res.dtype, DataType::Float64);
    assert_eq!(res.get(0), Scalar::Float64(2.5));
}

#[test]
fn three_valued_and_or() {
    // Row 0: NULL AND false = false
    // Row 1: NULL OR true = true
    // Row 2: NULL AND true = NULL
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "a".to_string(),
                dtype: DataType::Bool,
                nullable: true,
            },
            Field {
                name: "b".to_string(),
                dtype: DataType::Bool,
                nullable: true,
            },
        ],
    });
    let batch = Batch::new(
        schema,
        vec![
            Arc::new(Column::from_scalars(
                DataType::Bool,
                &[Scalar::Null, Scalar::Null, Scalar::Null],
            )),
            Arc::new(Column::from_scalars(
                DataType::Bool,
                &[Scalar::Bool(false), Scalar::Bool(true), Scalar::Bool(true)],
            )),
        ],
    );

    let and_expr = Expr::Binary {
        op: BinOp::And,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };
    let and_res = eval(&and_expr, &batch).unwrap();
    assert_eq!(and_res.get(0), Scalar::Bool(false));
    assert_eq!(and_res.get(2), Scalar::Null);

    let or_expr = Expr::Binary {
        op: BinOp::Or,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };
    let or_res = eval(&or_expr, &batch).unwrap();
    assert_eq!(or_res.get(1), Scalar::Bool(true));
}

#[test]
fn like_patterns() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "s".to_string(),
            dtype: DataType::Utf8,
            nullable: false,
        }],
    });
    let batch = Batch::new(
        schema,
        vec![Arc::new(Column::from_scalars(
            DataType::Utf8,
            &[
                Scalar::Utf8("xx special yy requests zz".to_string()),
                Scalar::Utf8("requests special".to_string()),
                Scalar::Utf8("forest green".to_string()),
                Scalar::Utf8("YELLOW BRASS".to_string()),
                Scalar::Utf8("PROMO LUXURY".to_string()),
                Scalar::Utf8("X".to_string()),
                Scalar::Utf8("XY".to_string()),
            ],
        ))],
    );

    // '%special%requests%'
    let like1 = Expr::Like {
        expr: Box::new(Expr::Column(0)),
        pattern: "%special%requests%".to_string(),
        negated: false,
    };
    let res1 = eval(&like1, &batch).unwrap();
    assert_eq!(res1.get(0), Scalar::Bool(true));
    assert_eq!(res1.get(1), Scalar::Bool(false));

    // 'forest%'
    let like2 = Expr::Like {
        expr: Box::new(Expr::Column(0)),
        pattern: "forest%".to_string(),
        negated: false,
    };
    let res2 = eval(&like2, &batch).unwrap();
    assert_eq!(res2.get(2), Scalar::Bool(true));
    assert_eq!(res2.get(0), Scalar::Bool(false));

    // '%BRASS'
    let like3 = Expr::Like {
        expr: Box::new(Expr::Column(0)),
        pattern: "%BRASS".to_string(),
        negated: false,
    };
    let res3 = eval(&like3, &batch).unwrap();
    assert_eq!(res3.get(3), Scalar::Bool(true));

    // 'PROMO%'
    let like4 = Expr::Like {
        expr: Box::new(Expr::Column(0)),
        pattern: "PROMO%".to_string(),
        negated: false,
    };
    let res4 = eval(&like4, &batch).unwrap();
    assert_eq!(res4.get(4), Scalar::Bool(true));

    // '_' single byte
    let like5 = Expr::Like {
        expr: Box::new(Expr::Column(0)),
        pattern: "_".to_string(),
        negated: false,
    };
    let res5 = eval(&like5, &batch).unwrap();
    assert_eq!(res5.get(5), Scalar::Bool(true));
    assert_eq!(res5.get(6), Scalar::Bool(false));
}

#[test]
fn in_list_and_negated() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "id".to_string(),
            dtype: DataType::Int32,
            nullable: false,
        }],
    });
    let batch = Batch::new(
        schema,
        vec![Arc::new(Column::from_scalars(
            DataType::Int32,
            &[Scalar::Int32(2), Scalar::Int32(5)],
        ))],
    );

    let in_list = Expr::InList {
        expr: Box::new(Expr::Column(0)),
        list: vec![Scalar::Int32(1), Scalar::Int32(2), Scalar::Int32(3)],
        negated: false,
    };
    let res1 = eval(&in_list, &batch).unwrap();
    assert_eq!(res1.get(0), Scalar::Bool(true));
    assert_eq!(res1.get(1), Scalar::Bool(false));

    let not_in = Expr::InList {
        expr: Box::new(Expr::Column(0)),
        list: vec![Scalar::Int32(1), Scalar::Int32(2), Scalar::Int32(3)],
        negated: true,
    };
    let res2 = eval(&not_in, &batch).unwrap();
    assert_eq!(res2.get(0), Scalar::Bool(false));
    assert_eq!(res2.get(1), Scalar::Bool(true));
}

#[test]
fn case_when_else_and_no_else() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "val".to_string(),
            dtype: DataType::Int32,
            nullable: false,
        }],
    });
    let batch = Batch::new(
        schema,
        vec![Arc::new(Column::from_scalars(
            DataType::Int32,
            &[Scalar::Int32(1), Scalar::Int32(2), Scalar::Int32(3)],
        ))],
    );

    // CASE WHEN val = 1 THEN 'one' WHEN val = 2 THEN 'two' ELSE 'other' END
    let case_with_else = Expr::Case {
        whens: vec![
            (
                Expr::Binary {
                    op: BinOp::Eq,
                    left: Box::new(Expr::Column(0)),
                    right: Box::new(Expr::Literal(Scalar::Int32(1))),
                },
                Expr::Literal(Scalar::Utf8("one".to_string())),
            ),
            (
                Expr::Binary {
                    op: BinOp::Eq,
                    left: Box::new(Expr::Column(0)),
                    right: Box::new(Expr::Literal(Scalar::Int32(2))),
                },
                Expr::Literal(Scalar::Utf8("two".to_string())),
            ),
        ],
        else_expr: Some(Box::new(Expr::Literal(Scalar::Utf8("other".to_string())))),
    };
    let res1 = eval(&case_with_else, &batch).unwrap();
    assert_eq!(res1.get(0), Scalar::Utf8("one".to_string()));
    assert_eq!(res1.get(1), Scalar::Utf8("two".to_string()));
    assert_eq!(res1.get(2), Scalar::Utf8("other".to_string()));

    // No ELSE -> NULL
    let case_no_else = Expr::Case {
        whens: vec![(
            Expr::Binary {
                op: BinOp::Eq,
                left: Box::new(Expr::Column(0)),
                right: Box::new(Expr::Literal(Scalar::Int32(1))),
            },
            Expr::Literal(Scalar::Utf8("one".to_string())),
        )],
        else_expr: None,
    };
    let res2 = eval(&case_no_else, &batch).unwrap();
    assert_eq!(res2.get(0), Scalar::Utf8("one".to_string()));
    assert_eq!(res2.get(1), Scalar::Null);
    assert_eq!(res2.get(2), Scalar::Null);
}

#[test]
fn extract_year() {
    // 1998-12-01 in Date32 (days since 1970-01-01)
    // 1998-12-01 is 10561 days
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "dt".to_string(),
            dtype: DataType::Date32,
            nullable: false,
        }],
    });
    let batch = Batch::new(
        schema,
        vec![Arc::new(Column::from_scalars(
            DataType::Date32,
            &[Scalar::Date32(10561)],
        ))],
    );
    let extract = Expr::Extract {
        field: DateField::Year,
        expr: Box::new(Expr::Column(0)),
    };
    let res = eval(&extract, &batch).unwrap();
    assert_eq!(res.dtype, DataType::Int64);
    assert_eq!(res.get(0), Scalar::Int64(1998));
}

#[test]
fn substring_one_based() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "code".to_string(),
            dtype: DataType::Utf8,
            nullable: false,
        }],
    });
    let batch = Batch::new(
        schema,
        vec![Arc::new(Column::from_scalars(
            DataType::Utf8,
            &[Scalar::Utf8("13-123".to_string())],
        ))],
    );
    let substr = Expr::Substring {
        expr: Box::new(Expr::Column(0)),
        start: 1,
        len: 2,
    };
    let res = eval(&substr, &batch).unwrap();
    assert_eq!(res.dtype, DataType::Utf8);
    assert_eq!(res.get(0), Scalar::Utf8("13".to_string()));
}

#[test]
fn substring_non_positive_start() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "text".to_string(),
            dtype: DataType::Utf8,
            nullable: false,
        }],
    });
    let batch = Batch::new(
        schema,
        vec![Arc::new(Column::from_scalars(
            DataType::Utf8,
            &[Scalar::Utf8("hello".to_string())],
        ))],
    );
    let substr1 = Expr::Substring {
        expr: Box::new(Expr::Column(0)),
        start: 0,
        len: 3,
    };
    let res1 = eval(&substr1, &batch).unwrap();
    assert_eq!(res1.get(0), Scalar::Utf8("he".to_string()));

    let substr2 = Expr::Substring {
        expr: Box::new(Expr::Column(0)),
        start: -1,
        len: 3,
    };
    let res2 = eval(&substr2, &batch).unwrap();
    assert_eq!(res2.get(0), Scalar::Utf8("h".to_string()));
}

#[test]
fn cast_int_to_decimal() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "n".to_string(),
            dtype: DataType::Int32,
            nullable: false,
        }],
    });
    let batch = Batch::new(
        schema,
        vec![Arc::new(Column::from_scalars(
            DataType::Int32,
            &[Scalar::Int32(42)],
        ))],
    );
    let cast = Expr::Cast {
        expr: Box::new(Expr::Column(0)),
        to: DataType::Decimal {
            precision: 15,
            scale: 2,
        },
    };
    let res = eval(&cast, &batch).unwrap();
    assert_eq!(
        res.dtype,
        DataType::Decimal {
            precision: 15,
            scale: 2
        }
    );
    assert_eq!(res.get(0), Scalar::Decimal(4200, 2));
}

#[test]
fn predicate_null_is_false() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "cond".to_string(),
            dtype: DataType::Bool,
            nullable: true,
        }],
    });
    let batch = Batch::new(
        schema,
        vec![Arc::new(Column::from_scalars(
            DataType::Bool,
            &[
                Scalar::Bool(true),
                Scalar::Null,
                Scalar::Bool(false),
                Scalar::Bool(true),
            ],
        ))],
    );
    let pred = Expr::Column(0);
    let indices = eval_predicate(&pred, &batch).unwrap();
    assert_eq!(indices, vec![0, 3]);
}

#[test]
fn float_mul_decimal() {
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "f".to_string(),
                dtype: DataType::Float64,
                nullable: false,
            },
            Field {
                name: "d".to_string(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
        ],
    });
    let batch = Batch::new(
        schema,
        vec![
            Arc::new(Column::from_scalars(
                DataType::Float64,
                &[Scalar::Float64(100.0)],
            )),
            Arc::new(Column::from_scalars(
                DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                &[Scalar::Decimal(500, 2)],
            )),
        ],
    );
    let expr = Expr::Binary {
        op: BinOp::Mul,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };
    let res = eval(&expr, &batch).unwrap();
    assert_eq!(res.dtype, DataType::Float64);
    assert_eq!(res.get(0), Scalar::Float64(500.0));
}

#[test]
fn all_null_batch_preserves_arithmetic_type() {
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "a".to_string(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: true,
            },
            Field {
                name: "b".to_string(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: true,
            },
        ],
    });

    let batch = Batch::new(
        schema,
        vec![
            Arc::new(Column::from_scalars(
                DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                &[Scalar::Null],
            )),
            Arc::new(Column::from_scalars(
                DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                &[Scalar::Null],
            )),
        ],
    );

    let expr = Expr::Binary {
        op: BinOp::Add,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };

    let col = eval(&expr, &batch).unwrap();
    assert_eq!(
        col.dtype,
        DataType::Decimal {
            precision: 38,
            scale: 2
        }
    );
}

#[test]
fn decimal_mul_overflow_returns_error() {
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "a".to_string(),
                dtype: DataType::Decimal {
                    precision: 38,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "b".to_string(),
                dtype: DataType::Decimal {
                    precision: 38,
                    scale: 2,
                },
                nullable: false,
            },
        ],
    });

    let batch = Batch::new(
        schema,
        vec![
            Arc::new(Column::from_scalars(
                DataType::Decimal {
                    precision: 38,
                    scale: 2,
                },
                &[Scalar::Decimal(10i128.pow(20), 2)],
            )),
            Arc::new(Column::from_scalars(
                DataType::Decimal {
                    precision: 38,
                    scale: 2,
                },
                &[Scalar::Decimal(10i128.pow(20), 2)],
            )),
        ],
    );

    let expr = Expr::Binary {
        op: BinOp::Mul,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };

    let res = eval(&expr, &batch);
    assert!(res.is_err());
}

#[test]
fn decimal_comparison_rescaling_overflow() {
    let a = Scalar::Decimal(10i128.pow(30), 2);
    let b = Scalar::Decimal(5, 18);
    let ord = compare_scalars(&a, &b).unwrap();
    assert_eq!(ord, std::cmp::Ordering::Greater);
}

#[test]
fn compare_scalars_decimal_with_integer() {
    let dec = Scalar::Decimal(2400, 2);
    let int32 = Scalar::Int32(24);
    let int64 = Scalar::Int64(25);

    assert_eq!(
        compare_scalars(&dec, &int32).unwrap(),
        std::cmp::Ordering::Equal
    );
    assert_eq!(
        compare_scalars(&int32, &dec).unwrap(),
        std::cmp::Ordering::Equal
    );
    assert_eq!(
        compare_scalars(&dec, &int64).unwrap(),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        compare_scalars(&int64, &dec).unwrap(),
        std::cmp::Ordering::Greater
    );
}

#[test]
fn commutative_date_addition() {
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "n".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "d".to_string(),
                dtype: DataType::Date32,
                nullable: false,
            },
        ],
    });
    let batch = Batch::new(
        schema,
        vec![
            Arc::new(Column::from_scalars(DataType::Int32, &[Scalar::Int32(10)])),
            Arc::new(Column::from_scalars(
                DataType::Date32,
                &[Scalar::Date32(1000)],
            )),
        ],
    );
    let expr = Expr::Binary {
        op: BinOp::Add,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };
    let res = eval(&expr, &batch).unwrap();
    assert_eq!(res.dtype, DataType::Date32);
    assert_eq!(res.get(0), Scalar::Date32(1010));
}

#[test]
fn case_differing_branch_types() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "val".to_string(),
            dtype: DataType::Int32,
            nullable: false,
        }],
    });
    let batch = Batch::new(
        schema,
        vec![Arc::new(Column::from_scalars(
            DataType::Int32,
            &[Scalar::Int32(1), Scalar::Int32(2)],
        ))],
    );

    let case_expr = Expr::Case {
        whens: vec![(
            Expr::Binary {
                op: BinOp::Eq,
                left: Box::new(Expr::Column(0)),
                right: Box::new(Expr::Literal(Scalar::Int32(1))),
            },
            Expr::Literal(Scalar::Int32(1)),
        )],
        else_expr: Some(Box::new(Expr::Literal(Scalar::Int64(10_000_000_000)))),
    };
    let res = eval(&case_expr, &batch).unwrap();
    assert_eq!(res.dtype, DataType::Int64);
    assert_eq!(res.get(0), Scalar::Int64(1));
    assert_eq!(res.get(1), Scalar::Int64(10_000_000_000));
}

#[test]
fn like_multiline_and_escapes() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "s".to_string(),
            dtype: DataType::Utf8,
            nullable: false,
        }],
    });
    let batch = Batch::new(
        schema,
        vec![Arc::new(Column::from_scalars(
            DataType::Utf8,
            &[
                Scalar::Utf8("hello\nworld".to_string()),
                Scalar::Utf8("100% discount".to_string()),
                Scalar::Utf8("a_b".to_string()),
            ],
        ))],
    );

    let like_multi = Expr::Like {
        expr: Box::new(Expr::Column(0)),
        pattern: "%world".to_string(),
        negated: false,
    };
    let res = eval(&like_multi, &batch).unwrap();
    assert_eq!(res.get(0), Scalar::Bool(true));

    let like_esc_pct = Expr::Like {
        expr: Box::new(Expr::Column(0)),
        pattern: r"100\% discount".to_string(),
        negated: false,
    };
    let res = eval(&like_esc_pct, &batch).unwrap();
    assert_eq!(res.get(1), Scalar::Bool(true));

    let like_esc_underscore = Expr::Like {
        expr: Box::new(Expr::Column(0)),
        pattern: r"a\_b".to_string(),
        negated: false,
    };
    let res = eval(&like_esc_underscore, &batch).unwrap();
    assert_eq!(res.get(2), Scalar::Bool(true));
}

#[test]
fn div_by_zero_returns_null() {
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "a".to_string(),
                dtype: DataType::Float64,
                nullable: false,
            },
            Field {
                name: "b".to_string(),
                dtype: DataType::Float64,
                nullable: false,
            },
        ],
    });
    let batch = Batch::new(
        schema,
        vec![
            Arc::new(Column::from_scalars(
                DataType::Float64,
                &[Scalar::Float64(10.0), Scalar::Float64(0.0)],
            )),
            Arc::new(Column::from_scalars(
                DataType::Float64,
                &[Scalar::Float64(0.0), Scalar::Float64(0.0)],
            )),
        ],
    );
    let div_expr = Expr::Binary {
        op: BinOp::Div,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };
    let res = eval(&div_expr, &batch).unwrap();
    assert_eq!(res.dtype, DataType::Float64);
    assert_eq!(res.get(0), Scalar::Null);
    assert_eq!(res.get(1), Scalar::Null);
    assert!(res.is_null(0));
    assert!(res.is_null(1));
}

#[test]
fn nan_comparison_is_false() {
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "a".to_string(),
                dtype: DataType::Float64,
                nullable: false,
            },
            Field {
                name: "b".to_string(),
                dtype: DataType::Float64,
                nullable: false,
            },
        ],
    });
    let batch = Batch::new(
        schema,
        vec![
            Arc::new(Column::from_scalars(
                DataType::Float64,
                &[Scalar::Float64(f64::NAN), Scalar::Float64(f64::NAN)],
            )),
            Arc::new(Column::from_scalars(
                DataType::Float64,
                &[Scalar::Float64(5.0), Scalar::Float64(f64::NAN)],
            )),
        ],
    );

    let eq_expr = Expr::Binary {
        op: BinOp::Eq,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };
    let res = eval(&eq_expr, &batch).unwrap();
    assert_eq!(res.get(0), Scalar::Bool(false));
    assert_eq!(res.get(1), Scalar::Bool(false));

    let neq_expr = Expr::Binary {
        op: BinOp::NotEq,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };
    let res = eval(&neq_expr, &batch).unwrap();
    assert_eq!(res.get(0), Scalar::Bool(true));
    assert_eq!(res.get(1), Scalar::Bool(true));

    let lt_expr = Expr::Binary {
        op: BinOp::Lt,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };
    let res = eval(&lt_expr, &batch).unwrap();
    assert_eq!(res.get(0), Scalar::Bool(false));
}

#[test]
fn float64_cross_type_comparisons() {
    let f = Scalar::Float64(4.9);
    let dec = Scalar::Decimal(4064928, 2);
    let int = Scalar::Int32(5);

    assert_eq!(compare_scalars(&f, &dec).unwrap(), std::cmp::Ordering::Less);
    assert_eq!(
        compare_scalars(&dec, &f).unwrap(),
        std::cmp::Ordering::Greater
    );
    assert_eq!(compare_scalars(&f, &int).unwrap(), std::cmp::Ordering::Less);
    assert_eq!(
        compare_scalars(&int, &f).unwrap(),
        std::cmp::Ordering::Greater
    );
}
