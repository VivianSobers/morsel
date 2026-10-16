use std::collections::HashMap;
use std::sync::Arc;

use morsel_core::{Batch, Column, DataType, Field, Scalar, Schema};
use morsel_exec::catalog::{Catalog, Table};
use morsel_exec::execute::execute;
use morsel_plan::{AggExpr, AggFunc, BinOp, Expr, JoinType, LogicalPlan};

fn make_table(schema: Arc<Schema>, batches: Vec<Batch>) -> Table {
    let rows = batches.iter().map(|b| b.len).sum();
    Table {
        schema,
        batches,
        rows,
    }
}

#[test]
fn inner_join_duplicate_build_keys() {
    // left: (id: 1, name: "p1")
    let left_schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "l_id".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "l_name".to_string(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ],
    });
    let left_batch = Batch::new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Column::from_scalars(DataType::Int32, &[Scalar::Int32(1)])),
            Arc::new(Column::from_scalars(
                DataType::Utf8,
                &[Scalar::Utf8("p1".to_string())],
            )),
        ],
    );

    // right: (id: 1, val: "a"), (id: 1, val: "b")
    let right_schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "r_id".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "r_val".to_string(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ],
    });
    let right_batch = Batch::new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Column::from_scalars(
                DataType::Int32,
                &[Scalar::Int32(1), Scalar::Int32(1)],
            )),
            Arc::new(Column::from_scalars(
                DataType::Utf8,
                &[Scalar::Utf8("a".to_string()), Scalar::Utf8("b".to_string())],
            )),
        ],
    );

    let mut tables = HashMap::new();
    tables.insert(
        "left_t".to_string(),
        make_table(Arc::clone(&left_schema), vec![left_batch]),
    );
    tables.insert(
        "right_t".to_string(),
        make_table(Arc::clone(&right_schema), vec![right_batch]),
    );
    let catalog = Catalog { tables };

    let scan_l = LogicalPlan::Scan {
        table: "left_t".to_string(),
        columns: vec![0, 1],
        table_schema: (*left_schema).clone(),
    };
    let scan_r = LogicalPlan::Scan {
        table: "right_t".to_string(),
        columns: vec![0, 1],
        table_schema: (*right_schema).clone(),
    };
    let join = LogicalPlan::Join {
        left: Box::new(scan_l),
        right: Box::new(scan_r),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };

    let res = execute(&join, &catalog, 2).unwrap();
    let total_rows: usize = res.iter().map(|b| b.len).sum();
    assert_eq!(total_rows, 2);
    assert_eq!(res[0].columns.len(), 4);
    assert_eq!(res[0].columns[0].get(0), Scalar::Int32(1));
    assert_eq!(res[0].columns[1].get(0), Scalar::Utf8("p1".to_string()));
    assert_eq!(res[0].columns[3].get(0), Scalar::Utf8("a".to_string()));
    assert_eq!(res[0].columns[3].get(1), Scalar::Utf8("b".to_string()));
}

#[test]
fn null_keys_never_match() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "id".to_string(),
            dtype: DataType::Int32,
            nullable: true,
        }],
    });
    let b1 = Batch::new(
        Arc::clone(&schema),
        vec![Arc::new(Column::from_scalars(
            DataType::Int32,
            &[Scalar::Null],
        ))],
    );
    let b2 = Batch::new(
        Arc::clone(&schema),
        vec![Arc::new(Column::from_scalars(
            DataType::Int32,
            &[Scalar::Null],
        ))],
    );

    let mut tables = HashMap::new();
    tables.insert("l".to_string(), make_table(Arc::clone(&schema), vec![b1]));
    tables.insert("r".to_string(), make_table(Arc::clone(&schema), vec![b2]));
    let catalog = Catalog { tables };

    let scan_l = LogicalPlan::Scan {
        table: "l".to_string(),
        columns: vec![0],
        table_schema: (*schema).clone(),
    };
    let scan_r = LogicalPlan::Scan {
        table: "r".to_string(),
        columns: vec![0],
        table_schema: (*schema).clone(),
    };
    let inner = LogicalPlan::Join {
        left: Box::new(scan_l.clone()),
        right: Box::new(scan_r.clone()),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };
    let res_inner = execute(&inner, &catalog, 1).unwrap();
    let rows_inner: usize = res_inner.iter().map(|b| b.len).sum();
    assert_eq!(rows_inner, 0);

    let left_join = LogicalPlan::Join {
        left: Box::new(scan_l),
        right: Box::new(scan_r),
        join_type: JoinType::Left,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };
    let res_left = execute(&left_join, &catalog, 1).unwrap();
    let rows_left: usize = res_left.iter().map(|b| b.len).sum();
    assert_eq!(rows_left, 1);
    assert_eq!(res_left[0].columns[0].get(0), Scalar::Null);
    assert_eq!(res_left[0].columns[1].get(0), Scalar::Null);
}

#[test]
fn left_join_fills_nulls_and_count_skips_them() {
    // left: [id: 1, id: 2]
    // right: [id: 1, val: 50]
    let left_schema = Arc::new(Schema {
        fields: vec![Field {
            name: "l_id".to_string(),
            dtype: DataType::Int32,
            nullable: false,
        }],
    });
    let right_schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "r_id".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "r_val".to_string(),
                dtype: DataType::Int64,
                nullable: false,
            },
        ],
    });

    let left_b = Batch::new(
        Arc::clone(&left_schema),
        vec![Arc::new(Column::from_scalars(
            DataType::Int32,
            &[Scalar::Int32(1), Scalar::Int32(2)],
        ))],
    );
    let right_b = Batch::new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Column::from_scalars(DataType::Int32, &[Scalar::Int32(1)])),
            Arc::new(Column::from_scalars(DataType::Int64, &[Scalar::Int64(50)])),
        ],
    );

    let mut tables = HashMap::new();
    tables.insert(
        "l".to_string(),
        make_table(Arc::clone(&left_schema), vec![left_b]),
    );
    tables.insert(
        "r".to_string(),
        make_table(Arc::clone(&right_schema), vec![right_b]),
    );
    let catalog = Catalog { tables };

    let scan_l = LogicalPlan::Scan {
        table: "l".to_string(),
        columns: vec![0],
        table_schema: (*left_schema).clone(),
    };
    let scan_r = LogicalPlan::Scan {
        table: "r".to_string(),
        columns: vec![0, 1],
        table_schema: (*right_schema).clone(),
    };
    let join = LogicalPlan::Join {
        left: Box::new(scan_l),
        right: Box::new(scan_r),
        join_type: JoinType::Left,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };
    // Aggregate over join: group by l_id (Column 0), count(r_val) (Column 2)
    let agg = LogicalPlan::Aggregate {
        input: Box::new(join),
        group_by: vec![(Expr::Column(0), "l_id".to_string())],
        aggs: vec![(
            AggExpr {
                func: AggFunc::Count,
                arg: Some(Expr::Column(2)),
            },
            "c_val".to_string(),
        )],
    };

    let res = execute(&agg, &catalog, 2).unwrap();
    let mut map = HashMap::new();
    for b in &res {
        for i in 0..b.len {
            let id = match b.columns[0].get(i) {
                Scalar::Int32(x) => x,
                _ => unreachable!(),
            };
            let cnt = match b.columns[1].get(i) {
                Scalar::Int64(x) => x,
                _ => unreachable!(),
            };
            map.insert(id, cnt);
        }
    }
    assert_eq!(map.get(&1), Some(&1));
    assert_eq!(map.get(&2), Some(&0));
}

#[test]
fn semi_join_emits_left_once() {
    let left_schema = Arc::new(Schema {
        fields: vec![Field {
            name: "id".to_string(),
            dtype: DataType::Int32,
            nullable: false,
        }],
    });
    let right_schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "id".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "v".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
        ],
    });

    let left_b = Batch::new(
        Arc::clone(&left_schema),
        vec![Arc::new(Column::from_scalars(
            DataType::Int32,
            &[Scalar::Int32(1)],
        ))],
    );
    let right_b = Batch::new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Column::from_scalars(
                DataType::Int32,
                &[Scalar::Int32(1), Scalar::Int32(1)],
            )),
            Arc::new(Column::from_scalars(
                DataType::Int32,
                &[Scalar::Int32(10), Scalar::Int32(20)],
            )),
        ],
    );

    let mut tables = HashMap::new();
    tables.insert(
        "l".to_string(),
        make_table(Arc::clone(&left_schema), vec![left_b]),
    );
    tables.insert(
        "r".to_string(),
        make_table(Arc::clone(&right_schema), vec![right_b]),
    );
    let catalog = Catalog { tables };

    let scan_l = LogicalPlan::Scan {
        table: "l".to_string(),
        columns: vec![0],
        table_schema: (*left_schema).clone(),
    };
    let scan_r = LogicalPlan::Scan {
        table: "r".to_string(),
        columns: vec![0, 1],
        table_schema: (*right_schema).clone(),
    };
    let semi = LogicalPlan::Join {
        left: Box::new(scan_l),
        right: Box::new(scan_r),
        join_type: JoinType::Semi,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };

    let res = execute(&semi, &catalog, 2).unwrap();
    let rows: usize = res.iter().map(|b| b.len).sum();
    assert_eq!(rows, 1);
    assert_eq!(res[0].columns.len(), 1);
    assert_eq!(res[0].columns[0].get(0), Scalar::Int32(1));
}

#[test]
fn anti_join_with_residual_filter() {
    // left: [order: 10, supp: 1], [order: 20, supp: 5]
    // right: [order: 10, supp: 1], [order: 10, supp: 2], [order: 20, supp: 5]
    // On: left.order = right.order
    // Filter: left.supp != right.supp
    // For order 10: right has supp 2 != 1 -> filter passes -> anti-join excludes order 10!
    // For order 20: right has supp 5 == 5 -> filter fails -> anti-join includes order 20!
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "order_id".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "supp_id".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
        ],
    });

    let left_b = Batch::new(
        Arc::clone(&schema),
        vec![
            Arc::new(Column::from_scalars(
                DataType::Int32,
                &[Scalar::Int32(10), Scalar::Int32(20)],
            )),
            Arc::new(Column::from_scalars(
                DataType::Int32,
                &[Scalar::Int32(1), Scalar::Int32(5)],
            )),
        ],
    );
    let right_b = Batch::new(
        Arc::clone(&schema),
        vec![
            Arc::new(Column::from_scalars(
                DataType::Int32,
                &[Scalar::Int32(10), Scalar::Int32(10), Scalar::Int32(20)],
            )),
            Arc::new(Column::from_scalars(
                DataType::Int32,
                &[Scalar::Int32(1), Scalar::Int32(2), Scalar::Int32(5)],
            )),
        ],
    );

    let mut tables = HashMap::new();
    tables.insert(
        "l".to_string(),
        make_table(Arc::clone(&schema), vec![left_b]),
    );
    tables.insert(
        "r".to_string(),
        make_table(Arc::clone(&schema), vec![right_b]),
    );
    let catalog = Catalog { tables };

    let scan_l = LogicalPlan::Scan {
        table: "l".to_string(),
        columns: vec![0, 1],
        table_schema: (*schema).clone(),
    };
    let scan_r = LogicalPlan::Scan {
        table: "r".to_string(),
        columns: vec![0, 1],
        table_schema: (*schema).clone(),
    };
    let anti = LogicalPlan::Join {
        left: Box::new(scan_l),
        right: Box::new(scan_r),
        join_type: JoinType::Anti,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: Some(Expr::Binary {
            op: BinOp::NotEq,
            left: Box::new(Expr::Column(1)),
            right: Box::new(Expr::Column(3)),
        }),
    };

    let res = execute(&anti, &catalog, 2).unwrap();
    let rows: usize = res.iter().map(|b| b.len).sum();
    assert_eq!(rows, 1);
    assert_eq!(res[0].columns[0].get(0), Scalar::Int32(20));
}

#[test]
fn cross_join_with_single_row() {
    let left_schema = Arc::new(Schema {
        fields: vec![Field {
            name: "x".to_string(),
            dtype: DataType::Int32,
            nullable: false,
        }],
    });
    let right_schema = Arc::new(Schema {
        fields: vec![Field {
            name: "y".to_string(),
            dtype: DataType::Utf8,
            nullable: false,
        }],
    });

    let left_b = Batch::new(
        Arc::clone(&left_schema),
        vec![Arc::new(Column::from_scalars(
            DataType::Int32,
            &[Scalar::Int32(1), Scalar::Int32(2), Scalar::Int32(3)],
        ))],
    );
    let right_b = Batch::new(
        Arc::clone(&right_schema),
        vec![Arc::new(Column::from_scalars(
            DataType::Utf8,
            &[Scalar::Utf8("constant".to_string())],
        ))],
    );

    let mut tables = HashMap::new();
    tables.insert(
        "l".to_string(),
        make_table(Arc::clone(&left_schema), vec![left_b]),
    );
    tables.insert(
        "r".to_string(),
        make_table(Arc::clone(&right_schema), vec![right_b]),
    );
    let catalog = Catalog { tables };

    let scan_l = LogicalPlan::Scan {
        table: "l".to_string(),
        columns: vec![0],
        table_schema: (*left_schema).clone(),
    };
    let scan_r = LogicalPlan::Scan {
        table: "r".to_string(),
        columns: vec![0],
        table_schema: (*right_schema).clone(),
    };
    let cross = LogicalPlan::CrossJoin {
        left: Box::new(scan_l),
        right: Box::new(scan_r),
    };

    let res = execute(&cross, &catalog, 2).unwrap();
    let rows: usize = res.iter().map(|b| b.len).sum();
    assert_eq!(rows, 3);
    assert_eq!(res[0].columns.len(), 2);
    assert_eq!(res[0].columns[0].get(0), Scalar::Int32(1));
    assert_eq!(
        res[0].columns[1].get(0),
        Scalar::Utf8("constant".to_string())
    );
}

#[test]
fn join_threads_do_not_change_result() {
    let left_schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "id".to_string(),
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
    let right_schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "id".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "tag".to_string(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ],
    });

    let n: usize = 5000;
    let l_ids: Vec<Scalar> = (0..n).map(|i| Scalar::Int32((i % 500) as i32)).collect();
    let l_vs: Vec<Scalar> = (0..n).map(|i| Scalar::Int64(i as i64)).collect();
    let left_b = Batch::new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Column::from_scalars(DataType::Int32, &l_ids)),
            Arc::new(Column::from_scalars(DataType::Int64, &l_vs)),
        ],
    );

    let r_ids: Vec<Scalar> = (0..500usize).map(|i| Scalar::Int32(i as i32)).collect();
    let r_tags: Vec<Scalar> = (0..500)
        .map(|i| Scalar::Utf8(format!("tag_{}", i)))
        .collect();
    let right_b = Batch::new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Column::from_scalars(DataType::Int32, &r_ids)),
            Arc::new(Column::from_scalars(DataType::Utf8, &r_tags)),
        ],
    );

    let mut tables = HashMap::new();
    tables.insert(
        "l".to_string(),
        make_table(Arc::clone(&left_schema), vec![left_b]),
    );
    tables.insert(
        "r".to_string(),
        make_table(Arc::clone(&right_schema), vec![right_b]),
    );
    let catalog = Catalog { tables };

    let scan_l = LogicalPlan::Scan {
        table: "l".to_string(),
        columns: vec![0, 1],
        table_schema: (*left_schema).clone(),
    };
    let scan_r = LogicalPlan::Scan {
        table: "r".to_string(),
        columns: vec![0, 1],
        table_schema: (*right_schema).clone(),
    };
    let join = LogicalPlan::Join {
        left: Box::new(scan_l),
        right: Box::new(scan_r),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };

    let res1 = execute(&join, &catalog, 1).unwrap();
    let res3 = execute(&join, &catalog, 3).unwrap();
    let res16 = execute(&join, &catalog, 16).unwrap();

    let to_vec = |batches: &[Batch]| -> Vec<(i32, i64, String)> {
        let mut v = Vec::new();
        for b in batches {
            for i in 0..b.len {
                let id = match b.columns[0].get(i) {
                    Scalar::Int32(x) => x,
                    _ => unreachable!(),
                };
                let val = match b.columns[1].get(i) {
                    Scalar::Int64(x) => x,
                    _ => unreachable!(),
                };
                let tag = match b.columns[3].get(i) {
                    Scalar::Utf8(s) => s,
                    _ => unreachable!(),
                };
                v.push((id, val, tag));
            }
        }
        v
    };

    assert_eq!(to_vec(&res1), to_vec(&res3));
    assert_eq!(to_vec(&res1), to_vec(&res16));
}

#[test]
fn join_keys_differing_types_and_scales() {
    let left_schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "l_int".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "l_dec".to_string(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
        ],
    });
    let left_batch = Batch::new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Column::from_scalars(DataType::Int32, &[Scalar::Int32(42)])),
            Arc::new(Column::from_scalars(
                DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                &[Scalar::Decimal(150, 2)],
            )),
        ],
    );

    let right_schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "r_int".to_string(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "r_dec".to_string(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 4,
                },
                nullable: false,
            },
        ],
    });
    let right_batch = Batch::new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Column::from_scalars(DataType::Int64, &[Scalar::Int64(42)])),
            Arc::new(Column::from_scalars(
                DataType::Decimal {
                    precision: 15,
                    scale: 4,
                },
                &[Scalar::Decimal(15000, 4)],
            )),
        ],
    );

    let mut tables = HashMap::new();
    tables.insert(
        "left_t".to_string(),
        make_table(Arc::clone(&left_schema), vec![left_batch]),
    );
    tables.insert(
        "right_t".to_string(),
        make_table(Arc::clone(&right_schema), vec![right_batch]),
    );
    let catalog = Catalog { tables };

    let join = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan {
            table: "left_t".to_string(),
            columns: vec![0, 1],
            table_schema: (*left_schema).clone(),
        }),
        right: Box::new(LogicalPlan::Scan {
            table: "right_t".to_string(),
            columns: vec![0, 1],
            table_schema: (*right_schema).clone(),
        }),
        join_type: JoinType::Inner,
        on: vec![
            (Expr::Column(0), Expr::Column(0)),
            (Expr::Column(1), Expr::Column(1)),
        ],
        filter: None,
    };

    let res = execute(&join, &catalog, 1).unwrap();
    let total_rows: usize = res.iter().map(|b| b.len).sum();
    assert_eq!(total_rows, 1);
}

#[test]
fn batch_builder_zero_column_schema() {
    let schema = Arc::new(Schema { fields: vec![] });
    let mut builder = morsel_exec::join::BatchBuilder::new(schema);
    builder.push_row(std::iter::empty());
    let batches = builder.finish();
    assert_eq!(batches.len(), 0);
}

#[test]
fn test_inner_join_builds_on_smaller_left() {
    let left_schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "l_id".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "l_val".to_string(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ],
    });
    let left_batch = Batch::new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Column::from_scalars(
                DataType::Int32,
                &[Scalar::Int32(10), Scalar::Int32(20)],
            )),
            Arc::new(Column::from_scalars(
                DataType::Utf8,
                &[
                    Scalar::Utf8("L10".to_string()),
                    Scalar::Utf8("L20".to_string()),
                ],
            )),
        ],
    );

    let right_schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "r_id".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "r_num".to_string(),
                dtype: DataType::Int64,
                nullable: false,
            },
        ],
    });
    let r_ids: Vec<Scalar> = (0..20)
        .map(|i| Scalar::Int32(if i % 2 == 0 { 10 } else { 20 }))
        .collect();
    let r_nums: Vec<Scalar> = (0..20).map(|i| Scalar::Int64(i as i64 * 100)).collect();
    let right_batch = Batch::new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Column::from_scalars(DataType::Int32, &r_ids)),
            Arc::new(Column::from_scalars(DataType::Int64, &r_nums)),
        ],
    );

    let mut tables = HashMap::new();
    tables.insert(
        "t_left".to_string(),
        make_table(Arc::clone(&left_schema), vec![left_batch]),
    );
    tables.insert(
        "t_right".to_string(),
        make_table(Arc::clone(&right_schema), vec![right_batch]),
    );
    let catalog = Catalog { tables };

    let join = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan {
            table: "t_left".to_string(),
            columns: vec![0, 1],
            table_schema: (*left_schema).clone(),
        }),
        right: Box::new(LogicalPlan::Scan {
            table: "t_right".to_string(),
            columns: vec![0, 1],
            table_schema: (*right_schema).clone(),
        }),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: Some(Expr::Binary {
            op: BinOp::Lt,
            left: Box::new(Expr::Column(3)),
            right: Box::new(Expr::Literal(Scalar::Int64(1000))),
        }),
    };

    let res = execute(&join, &catalog, 4).unwrap();
    let total_rows: usize = res.iter().map(|b| b.len).sum();
    // 20 rows in right: indices 0..9 have r_num < 1000 (i.e. 0, 100, ..., 900)
    assert_eq!(total_rows, 10);
    assert_eq!(res[0].columns.len(), 4);
    assert_eq!(res[0].columns[0].dtype, DataType::Int32);
    assert_eq!(res[0].columns[1].dtype, DataType::Utf8);
    assert_eq!(res[0].columns[2].dtype, DataType::Int32);
    assert_eq!(res[0].columns[3].dtype, DataType::Int64);
}
