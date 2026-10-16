use morsel_core::{DataType, Field, Schema};
use morsel_plan::{AggExpr, AggFunc, BinOp, Expr, JoinType, LogicalPlan};

#[test]
fn decimal_mul_scale_adds() {
    let schema = Schema {
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
                    scale: 2,
                },
                nullable: false,
            },
        ],
    };
    let expr = Expr::Binary {
        op: BinOp::Mul,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };
    assert_eq!(
        expr.data_type(&schema).unwrap(),
        DataType::Decimal {
            precision: 38,
            scale: 4,
        }
    );
}

#[test]
fn div_is_float() {
    let schema = Schema {
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
            Field {
                name: "d".to_string(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
        ],
    };
    let int_div = Expr::Binary {
        op: BinOp::Div,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };
    assert_eq!(int_div.data_type(&schema).unwrap(), DataType::Float64);

    let dec_div = Expr::Binary {
        op: BinOp::Div,
        left: Box::new(Expr::Column(2)),
        right: Box::new(Expr::Column(1)),
    };
    assert_eq!(dec_div.data_type(&schema).unwrap(), DataType::Float64);
}

#[test]
fn aggregate_schema_order() {
    let table_schema = Schema {
        fields: vec![
            Field {
                name: "id".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "val".to_string(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
        ],
    };
    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0, 1],
        table_schema,
    };
    let agg_plan = LogicalPlan::Aggregate {
        input: Box::new(scan),
        group_by: vec![(Expr::Column(0), "g_id".to_string())],
        aggs: vec![
            (
                AggExpr {
                    func: AggFunc::Sum,
                    arg: Some(Expr::Column(1)),
                },
                "total".to_string(),
            ),
            (
                AggExpr {
                    func: AggFunc::CountStar,
                    arg: None,
                },
                "cnt".to_string(),
            ),
        ],
    };

    let schema = agg_plan.schema().unwrap();
    assert_eq!(schema.fields.len(), 3);
    assert_eq!(schema.fields[0].name, "g_id");
    assert_eq!(schema.fields[0].dtype, DataType::Int32);
    assert_eq!(schema.fields[1].name, "total");
    assert_eq!(
        schema.fields[1].dtype,
        DataType::Decimal {
            precision: 38,
            scale: 2,
        }
    );
    assert_eq!(schema.fields[2].name, "cnt");
    assert_eq!(schema.fields[2].dtype, DataType::Int64);
}

#[test]
fn left_join_right_side_nullable() {
    let left = LogicalPlan::Scan {
        table: "t1".to_string(),
        columns: vec![0],
        table_schema: Schema {
            fields: vec![Field {
                name: "a".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            }],
        },
    };
    let right = LogicalPlan::Scan {
        table: "t2".to_string(),
        columns: vec![0],
        table_schema: Schema {
            fields: vec![Field {
                name: "b".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            }],
        },
    };
    let join = LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::Left,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };
    let schema = join.schema().unwrap();
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.fields[0].name, "a");
    assert!(!schema.fields[0].nullable);
    assert_eq!(schema.fields[1].name, "b");
    assert!(schema.fields[1].nullable);
}

#[test]
fn semi_join_outputs_left_only() {
    let left = LogicalPlan::Scan {
        table: "t1".to_string(),
        columns: vec![0],
        table_schema: Schema {
            fields: vec![Field {
                name: "a".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            }],
        },
    };
    let right = LogicalPlan::Scan {
        table: "t2".to_string(),
        columns: vec![0],
        table_schema: Schema {
            fields: vec![Field {
                name: "b".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            }],
        },
    };
    let join = LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::Semi,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };
    let schema = join.schema().unwrap();
    assert_eq!(schema.fields.len(), 1);
    assert_eq!(schema.fields[0].name, "a");
}

#[test]
fn scan_picks_columns_in_order() {
    let table_schema = Schema {
        fields: vec![
            Field {
                name: "c0".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "c1".to_string(),
                dtype: DataType::Utf8,
                nullable: true,
            },
            Field {
                name: "c2".to_string(),
                dtype: DataType::Date32,
                nullable: false,
            },
        ],
    };
    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![2, 0],
        table_schema,
    };
    let schema = scan.schema().unwrap();
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.fields[0].name, "c2");
    assert_eq!(schema.fields[0].dtype, DataType::Date32);
    assert_eq!(schema.fields[1].name, "c0");
    assert_eq!(schema.fields[1].dtype, DataType::Int32);
}

#[test]
fn date_add_date_rejected() {
    let schema = Schema {
        fields: vec![
            Field {
                name: "d1".to_string(),
                dtype: DataType::Date32,
                nullable: false,
            },
            Field {
                name: "d2".to_string(),
                dtype: DataType::Date32,
                nullable: false,
            },
        ],
    };
    let expr = Expr::Binary {
        op: BinOp::Add,
        left: Box::new(Expr::Column(0)),
        right: Box::new(Expr::Column(1)),
    };
    assert!(expr.data_type(&schema).is_err());
}
