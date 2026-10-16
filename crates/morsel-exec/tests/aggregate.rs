use std::collections::HashMap;
use std::sync::Arc;

use morsel_core::{Batch, Column, DataType, Field, Scalar, Schema, BATCH_ROWS};
use morsel_exec::catalog::{Catalog, Table};
use morsel_exec::execute::execute;
use morsel_plan::{AggExpr, AggFunc, Expr, LogicalPlan};

fn make_table(schema: Arc<Schema>, batches: Vec<Batch>) -> Table {
    let rows = batches.iter().map(|b| b.len).sum();
    Table {
        schema,
        batches,
        rows,
    }
}

#[test]
fn global_agg_on_empty_input() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "val".to_string(),
            dtype: DataType::Int64,
            nullable: true,
        }],
    });
    let table = make_table(Arc::clone(&schema), vec![]);
    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0],
        table_schema,
    };
    let agg = LogicalPlan::Aggregate {
        input: Box::new(scan),
        group_by: vec![],
        aggs: vec![
            (
                AggExpr {
                    func: AggFunc::Count,
                    arg: Some(Expr::Column(0)),
                },
                "cnt".to_string(),
            ),
            (
                AggExpr {
                    func: AggFunc::CountStar,
                    arg: None,
                },
                "cnt_star".to_string(),
            ),
            (
                AggExpr {
                    func: AggFunc::Sum,
                    arg: Some(Expr::Column(0)),
                },
                "sum".to_string(),
            ),
            (
                AggExpr {
                    func: AggFunc::Avg,
                    arg: Some(Expr::Column(0)),
                },
                "avg".to_string(),
            ),
            (
                AggExpr {
                    func: AggFunc::Min,
                    arg: Some(Expr::Column(0)),
                },
                "min".to_string(),
            ),
            (
                AggExpr {
                    func: AggFunc::Max,
                    arg: Some(Expr::Column(0)),
                },
                "max".to_string(),
            ),
        ],
    };

    let res = execute(&agg, &catalog, 4).unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].len, 1);
    assert_eq!(res[0].columns[0].get(0), Scalar::Int64(0));
    assert_eq!(res[0].columns[1].get(0), Scalar::Int64(0));
    assert_eq!(res[0].columns[2].get(0), Scalar::Null);
    assert_eq!(res[0].columns[3].get(0), Scalar::Null);
    assert_eq!(res[0].columns[4].get(0), Scalar::Null);
    assert_eq!(res[0].columns[5].get(0), Scalar::Null);
}

#[test]
fn count_ignores_nulls() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "val".to_string(),
            dtype: DataType::Int64,
            nullable: true,
        }],
    });
    let col = Arc::new(Column::from_scalars(
        DataType::Int64,
        &[
            Scalar::Int64(10),
            Scalar::Null,
            Scalar::Int64(20),
            Scalar::Null,
        ],
    ));
    let batch = Batch::new(Arc::clone(&schema), vec![col]);
    let table = make_table(Arc::clone(&schema), vec![batch]);
    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0],
        table_schema,
    };
    let agg = LogicalPlan::Aggregate {
        input: Box::new(scan),
        group_by: vec![],
        aggs: vec![
            (
                AggExpr {
                    func: AggFunc::Count,
                    arg: Some(Expr::Column(0)),
                },
                "c".to_string(),
            ),
            (
                AggExpr {
                    func: AggFunc::CountStar,
                    arg: None,
                },
                "c_star".to_string(),
            ),
        ],
    };

    let res = execute(&agg, &catalog, 2).unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].columns[0].get(0), Scalar::Int64(2));
    assert_eq!(res[0].columns[1].get(0), Scalar::Int64(4));
}

#[test]
fn count_distinct() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "val".to_string(),
            dtype: DataType::Int64,
            nullable: true,
        }],
    });
    let col = Arc::new(Column::from_scalars(
        DataType::Int64,
        &[
            Scalar::Int64(1),
            Scalar::Int64(2),
            Scalar::Int64(2),
            Scalar::Null,
            Scalar::Int64(3),
            Scalar::Int64(3),
            Scalar::Int64(3),
        ],
    ));
    let batch = Batch::new(Arc::clone(&schema), vec![col]);
    let table = make_table(Arc::clone(&schema), vec![batch]);
    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0],
        table_schema,
    };
    let agg = LogicalPlan::Aggregate {
        input: Box::new(scan),
        group_by: vec![],
        aggs: vec![(
            AggExpr {
                func: AggFunc::CountDistinct,
                arg: Some(Expr::Column(0)),
            },
            "cd".to_string(),
        )],
    };

    let res = execute(&agg, &catalog, 2).unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].columns[0].get(0), Scalar::Int64(3));
}

#[test]
fn avg_is_float() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "val".to_string(),
            dtype: DataType::Int64,
            nullable: false,
        }],
    });
    let col = Arc::new(Column::from_scalars(
        DataType::Int64,
        &[Scalar::Int64(10), Scalar::Int64(25)],
    ));
    let batch = Batch::new(Arc::clone(&schema), vec![col]);
    let table = make_table(Arc::clone(&schema), vec![batch]);
    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0],
        table_schema,
    };
    let agg = LogicalPlan::Aggregate {
        input: Box::new(scan),
        group_by: vec![],
        aggs: vec![(
            AggExpr {
                func: AggFunc::Avg,
                arg: Some(Expr::Column(0)),
            },
            "avg_val".to_string(),
        )],
    };

    let res = execute(&agg, &catalog, 1).unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].columns[0].dtype, DataType::Float64);
    assert_eq!(res[0].columns[0].get(0), Scalar::Float64(17.5));
}

#[test]
fn decimal_sum_exact() {
    // 60,000,000 copies of 104949.50 at scale 2 equals 6_296_970_000_000.00 exactly
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "val".to_string(),
            dtype: DataType::Decimal {
                precision: 15,
                scale: 2,
            },
            nullable: false,
        }],
    });

    let total_rows: usize = 60_000_000;
    let batch_rows = BATCH_ROWS;
    let full_batches = total_rows / batch_rows;
    let rem_rows = total_rows % batch_rows;

    let full_col = Arc::new(Column::from_scalars(
        DataType::Decimal {
            precision: 15,
            scale: 2,
        },
        &vec![Scalar::Decimal(10494950, 2); batch_rows],
    ));
    let full_batch = Batch::new(Arc::clone(&schema), vec![full_col]);

    let mut batches = Vec::with_capacity(full_batches + 1);
    for _ in 0..full_batches {
        batches.push(full_batch.clone());
    }
    if rem_rows > 0 {
        let rem_col = Arc::new(Column::from_scalars(
            DataType::Decimal {
                precision: 15,
                scale: 2,
            },
            &vec![Scalar::Decimal(10494950, 2); rem_rows],
        ));
        batches.push(Batch::new(Arc::clone(&schema), vec![rem_col]));
    }

    let table = Table {
        schema: Arc::clone(&schema),
        batches,
        rows: total_rows,
    };
    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0],
        table_schema,
    };
    let agg = LogicalPlan::Aggregate {
        input: Box::new(scan),
        group_by: vec![],
        aggs: vec![(
            AggExpr {
                func: AggFunc::Sum,
                arg: Some(Expr::Column(0)),
            },
            "sum_val".to_string(),
        )],
    };

    let res = execute(&agg, &catalog, 8).unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(
        res[0].columns[0].get(0),
        Scalar::Decimal(629_697_000_000_000, 2)
    );
}

#[test]
fn group_by_two_keys_matches_naive() {
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "k1".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "k2".to_string(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "v".to_string(),
                dtype: DataType::Int64,
                nullable: false,
            },
        ],
    });

    let n: usize = 10_000;
    let k1_vals: Vec<Scalar> = (0..n).map(|i| Scalar::Int32((i % 7) as i32)).collect();
    let k2_vals: Vec<Scalar> = (0..n)
        .map(|i| Scalar::Utf8(format!("cat_{}", i % 5)))
        .collect();
    let v_vals: Vec<Scalar> = (0..n).map(|i| Scalar::Int64((i % 100) as i64)).collect();

    let col_k1 = Arc::new(Column::from_scalars(DataType::Int32, &k1_vals));
    let col_k2 = Arc::new(Column::from_scalars(DataType::Utf8, &k2_vals));
    let col_v = Arc::new(Column::from_scalars(DataType::Int64, &v_vals));
    let batch = Batch::new(Arc::clone(&schema), vec![col_k1, col_k2, col_v]);
    let table = make_table(Arc::clone(&schema), vec![batch]);

    let mut naive_counts: HashMap<(i32, String), (i64, i64)> = HashMap::new();
    for i in 0..n {
        let k1 = (i % 7) as i32;
        let k2 = format!("cat_{}", i % 5);
        let v = (i % 100) as i64;
        let entry = naive_counts.entry((k1, k2)).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += v;
    }

    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0, 1, 2],
        table_schema,
    };
    let agg = LogicalPlan::Aggregate {
        input: Box::new(scan),
        group_by: vec![
            (Expr::Column(0), "k1".to_string()),
            (Expr::Column(1), "k2".to_string()),
        ],
        aggs: vec![
            (
                AggExpr {
                    func: AggFunc::CountStar,
                    arg: None,
                },
                "cnt".to_string(),
            ),
            (
                AggExpr {
                    func: AggFunc::Sum,
                    arg: Some(Expr::Column(2)),
                },
                "sum_v".to_string(),
            ),
        ],
    };

    let res = execute(&agg, &catalog, 4).unwrap();
    let mut actual_counts = HashMap::new();
    for b in &res {
        for i in 0..b.len {
            let k1 = match b.columns[0].get(i) {
                Scalar::Int32(x) => x,
                _ => unreachable!(),
            };
            let k2 = match b.columns[1].get(i) {
                Scalar::Utf8(s) => s,
                _ => unreachable!(),
            };
            let cnt = match b.columns[2].get(i) {
                Scalar::Int64(c) => c,
                _ => unreachable!(),
            };
            let sum_v = match b.columns[3].get(i) {
                Scalar::Int64(s) => s,
                _ => unreachable!(),
            };
            actual_counts.insert((k1, k2), (cnt, sum_v));
        }
    }

    assert_eq!(actual_counts, naive_counts);
}

#[test]
fn threads_do_not_change_result() {
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "g".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "v".to_string(),
                dtype: DataType::Int64,
                nullable: false,
            },
        ],
    });

    let n: usize = 20_000;
    let g_vals: Vec<Scalar> = (0..n).map(|i| Scalar::Int32((i % 20) as i32)).collect();
    let v_vals: Vec<Scalar> = (0..n).map(|i| Scalar::Int64(i as i64)).collect();
    let col_g = Arc::new(Column::from_scalars(DataType::Int32, &g_vals));
    let col_v = Arc::new(Column::from_scalars(DataType::Int64, &v_vals));
    let batch = Batch::new(Arc::clone(&schema), vec![col_g, col_v]);
    let table = make_table(Arc::clone(&schema), vec![batch]);

    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0, 1],
        table_schema,
    };
    let agg = LogicalPlan::Aggregate {
        input: Box::new(scan),
        group_by: vec![(Expr::Column(0), "g".to_string())],
        aggs: vec![(
            AggExpr {
                func: AggFunc::Sum,
                arg: Some(Expr::Column(1)),
            },
            "s".to_string(),
        )],
    };

    let res1 = execute(&agg, &catalog, 1).unwrap();
    let res3 = execute(&agg, &catalog, 3).unwrap();
    let res16 = execute(&agg, &catalog, 16).unwrap();

    let to_map = |batches: &[Batch]| -> HashMap<i32, i64> {
        let mut m = HashMap::new();
        for b in batches {
            for i in 0..b.len {
                let g = match b.columns[0].get(i) {
                    Scalar::Int32(x) => x,
                    _ => unreachable!(),
                };
                let s = match b.columns[1].get(i) {
                    Scalar::Int64(x) => x,
                    _ => unreachable!(),
                };
                m.insert(g, s);
            }
        }
        m
    };

    assert_eq!(to_map(&res1), to_map(&res3));
    assert_eq!(to_map(&res1), to_map(&res16));
}

#[test]
fn agg_state_merge_handles_incompatible_scalars() {
    let mut s1 = morsel_exec::aggregate::AggState::new(AggFunc::Min, Some(DataType::Int32));
    s1.min = Some(Scalar::Int32(1));
    let mut s2 = morsel_exec::aggregate::AggState::new(AggFunc::Min, Some(DataType::Utf8));
    s2.min = Some(Scalar::Utf8("a".to_string()));
    let res = s1.merge(s2);
    assert!(res.is_err());
}
