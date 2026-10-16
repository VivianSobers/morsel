use morsel_core::{DataType, Field, Scalar, Schema};
use morsel_plan::{AggFunc, BinOp, Expr, LogicalPlan, SchemaProvider};
use morsel_sql::plan_sql;
use std::collections::HashMap;

struct MockCatalog {
    schemas: HashMap<String, Schema>,
}

impl MockCatalog {
    fn new() -> Self {
        let mut schemas = HashMap::new();
        schemas.insert(
            "lineitem".to_string(),
            Schema::new(vec![
                Field {
                    name: "l_orderkey".into(),
                    dtype: DataType::Int64,
                    nullable: false,
                },
                Field {
                    name: "l_partkey".into(),
                    dtype: DataType::Int64,
                    nullable: false,
                },
                Field {
                    name: "l_suppkey".into(),
                    dtype: DataType::Int64,
                    nullable: false,
                },
                Field {
                    name: "l_linenumber".into(),
                    dtype: DataType::Int32,
                    nullable: false,
                },
                Field {
                    name: "l_quantity".into(),
                    dtype: DataType::Decimal {
                        precision: 15,
                        scale: 2,
                    },
                    nullable: false,
                },
                Field {
                    name: "l_extendedprice".into(),
                    dtype: DataType::Decimal {
                        precision: 15,
                        scale: 2,
                    },
                    nullable: false,
                },
                Field {
                    name: "l_discount".into(),
                    dtype: DataType::Decimal {
                        precision: 15,
                        scale: 2,
                    },
                    nullable: false,
                },
                Field {
                    name: "l_tax".into(),
                    dtype: DataType::Decimal {
                        precision: 15,
                        scale: 2,
                    },
                    nullable: false,
                },
                Field {
                    name: "l_returnflag".into(),
                    dtype: DataType::Utf8,
                    nullable: false,
                },
                Field {
                    name: "l_linestatus".into(),
                    dtype: DataType::Utf8,
                    nullable: false,
                },
                Field {
                    name: "l_shipdate".into(),
                    dtype: DataType::Date32,
                    nullable: false,
                },
                Field {
                    name: "l_commitdate".into(),
                    dtype: DataType::Date32,
                    nullable: false,
                },
                Field {
                    name: "l_receiptdate".into(),
                    dtype: DataType::Date32,
                    nullable: false,
                },
                Field {
                    name: "l_shipinstruct".into(),
                    dtype: DataType::Utf8,
                    nullable: false,
                },
                Field {
                    name: "l_shipmode".into(),
                    dtype: DataType::Utf8,
                    nullable: false,
                },
                Field {
                    name: "l_comment".into(),
                    dtype: DataType::Utf8,
                    nullable: false,
                },
            ]),
        );
        schemas.insert(
            "t".to_string(),
            Schema::new(vec![
                Field {
                    name: "a".into(),
                    dtype: DataType::Int64,
                    nullable: false,
                },
                Field {
                    name: "b".into(),
                    dtype: DataType::Int64,
                    nullable: false,
                },
            ]),
        );
        Self { schemas }
    }
}

impl SchemaProvider for MockCatalog {
    fn table_schema(&self, name: &str) -> Option<Schema> {
        self.schemas.get(name).cloned().or_else(|| {
            for (k, v) in &self.schemas {
                if k.eq_ignore_ascii_case(name) {
                    return Some(v.clone());
                }
            }
            None
        })
    }
}

#[test]
fn binds_q6_shape() {
    let cat = MockCatalog::new();
    let sql = "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24";
    let plan = plan_sql(sql, &cat).expect("plan q6");

    match plan {
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => {
            assert!(group_by.is_empty(), "q6 is global agg");
            assert_eq!(aggs.len(), 1);
            assert_eq!(aggs[0].0.func, AggFunc::Sum);
            assert_eq!(aggs[0].1, "revenue");

            match *input {
                LogicalPlan::Filter { input, .. } => match *input {
                    LogicalPlan::Scan { table, columns, .. } => {
                        assert_eq!(table.to_lowercase(), "lineitem");
                        assert_eq!(
                            columns.len(),
                            4,
                            "4 columns used in q6: extendedprice, discount, shipdate, quantity"
                        );
                    }
                    other => panic!("expected Scan, got {:?}", other),
                },
                other => panic!("expected Filter, got {:?}", other),
            }
        }
        other => panic!("expected Aggregate, got {:?}", other),
    }
}

#[test]
fn date_interval_folds() {
    let cat = MockCatalog::new();
    let sql = "SELECT date '1998-12-01' - interval '90' day AS d";
    let plan = plan_sql(sql, &cat).expect("plan date interval");
    match plan {
        LogicalPlan::Project { exprs, .. } => {
            assert_eq!(exprs.len(), 1);
            assert_eq!(exprs[0].1, "d");
            // 1998-09-02 in days since 1970-01-01 is 10471
            let expected_days = chrono::NaiveDate::from_ymd_opt(1998, 9, 2)
                .unwrap()
                .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                .num_days() as i32;
            assert_eq!(exprs[0].0, Expr::Literal(Scalar::Date32(expected_days)));
        }
        other => panic!("expected Project, got {:?}", other),
    }
}

#[test]
fn between_is_two_comparisons() {
    let cat = MockCatalog::new();
    let sql = "SELECT * FROM t WHERE a BETWEEN 10 AND 20";
    let plan = plan_sql(sql, &cat).expect("plan between");
    match plan {
        LogicalPlan::Filter { predicate, .. } => match predicate {
            Expr::Binary { op, left, right } => {
                assert_eq!(op, BinOp::And);
                match (*left, *right) {
                    (
                        Expr::Binary {
                            op: BinOp::GtEq, ..
                        },
                        Expr::Binary {
                            op: BinOp::LtEq, ..
                        },
                    ) => {}
                    (l, r) => panic!("expected GtEq and LtEq, got {:?} and {:?}", l, r),
                }
            }
            other => panic!("expected Binary And, got {:?}", other),
        },
        other => panic!("expected Filter, got {:?}", other),
    }
}

#[test]
fn unknown_column_errors() {
    let cat = MockCatalog::new();
    let sql = "SELECT nonexistent FROM t";
    let res = plan_sql(sql, &cat);
    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("nonexistent"),
        "error should name unknown column: {}",
        err_msg
    );
}

#[test]
fn output_names_match_aliases() {
    let cat = MockCatalog::new();
    let sql = "SELECT a AS col_a, b + 1 AS col_b FROM t";
    let plan = plan_sql(sql, &cat).expect("plan aliases");
    let schema = plan.schema().expect("plan schema");
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.fields[0].name, "col_a");
    assert_eq!(schema.fields[1].name, "col_b");
}
