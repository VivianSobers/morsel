use std::collections::HashMap;
use std::sync::Arc;

use morsel_core::{Batch, Column, DataType, Field, Scalar, Schema, BATCH_ROWS};
use morsel_exec::catalog::{Catalog, Table};
use morsel_exec::execute::execute;
use morsel_plan::{Expr, LogicalPlan, SortKey};

fn make_table(schema: Arc<Schema>, batches: Vec<Batch>) -> Table {
    let rows = batches.iter().map(|b| b.len).sum();
    Table {
        schema,
        batches,
        rows,
    }
}

fn collect_rows(batches: &[Batch]) -> Vec<Vec<Scalar>> {
    let mut rows = Vec::new();
    for b in batches {
        for i in 0..b.len {
            let row: Vec<Scalar> = (0..b.columns.len()).map(|c| b.columns[c].get(i)).collect();
            rows.push(row);
        }
    }
    rows
}

#[test]
fn multi_key_asc_desc() {
    // Schema: (k1: Int32, k2: Int32)
    // Rows: (1, 10), (2, 20), (1, 30), (2, 10)
    // Order: k1 ASC, k2 DESC
    // Expected: (1, 30), (1, 10), (2, 20), (2, 10)
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "k1".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "k2".to_string(),
                dtype: DataType::Int32,
                nullable: false,
            },
        ],
    });
    let batch = Batch::new(
        Arc::clone(&schema),
        vec![
            Arc::new(Column::from_scalars(
                DataType::Int32,
                &[
                    Scalar::Int32(1),
                    Scalar::Int32(2),
                    Scalar::Int32(1),
                    Scalar::Int32(2),
                ],
            )),
            Arc::new(Column::from_scalars(
                DataType::Int32,
                &[
                    Scalar::Int32(10),
                    Scalar::Int32(20),
                    Scalar::Int32(30),
                    Scalar::Int32(10),
                ],
            )),
        ],
    );

    let mut tables = HashMap::new();
    tables.insert(
        "t".to_string(),
        make_table(Arc::clone(&schema), vec![batch]),
    );
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0, 1],
        table_schema: (*schema).clone(),
    };
    let sort = LogicalPlan::Sort {
        input: Box::new(scan),
        keys: vec![
            SortKey {
                expr: Expr::Column(0),
                asc: true,
                nulls_first: false,
            },
            SortKey {
                expr: Expr::Column(1),
                asc: false,
                nulls_first: false,
            },
        ],
    };

    let res = execute(&sort, &catalog, 2).unwrap();
    let rows = collect_rows(&res);
    assert_eq!(
        rows,
        vec![
            vec![Scalar::Int32(1), Scalar::Int32(30)],
            vec![Scalar::Int32(1), Scalar::Int32(10)],
            vec![Scalar::Int32(2), Scalar::Int32(20)],
            vec![Scalar::Int32(2), Scalar::Int32(10)],
        ]
    );
}

#[test]
fn nulls_first_and_last() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "k".to_string(),
            dtype: DataType::Int32,
            nullable: true,
        }],
    });
    let batch = Batch::new(
        Arc::clone(&schema),
        vec![Arc::new(Column::from_scalars(
            DataType::Int32,
            &[Scalar::Int32(20), Scalar::Null, Scalar::Int32(10)],
        ))],
    );

    let mut tables = HashMap::new();
    tables.insert(
        "t".to_string(),
        make_table(Arc::clone(&schema), vec![batch]),
    );
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0],
        table_schema: (*schema).clone(),
    };

    // ASC NULLS FIRST -> NULL, 10, 20
    let sort_nf = LogicalPlan::Sort {
        input: Box::new(scan.clone()),
        keys: vec![SortKey {
            expr: Expr::Column(0),
            asc: true,
            nulls_first: true,
        }],
    };
    let res_nf = execute(&sort_nf, &catalog, 1).unwrap();
    let rows_nf = collect_rows(&res_nf);
    assert_eq!(
        rows_nf,
        vec![
            vec![Scalar::Null],
            vec![Scalar::Int32(10)],
            vec![Scalar::Int32(20)],
        ]
    );

    // ASC NULLS LAST -> 10, 20, NULL
    let sort_nl = LogicalPlan::Sort {
        input: Box::new(scan),
        keys: vec![SortKey {
            expr: Expr::Column(0),
            asc: true,
            nulls_first: false,
        }],
    };
    let res_nl = execute(&sort_nl, &catalog, 1).unwrap();
    let rows_nl = collect_rows(&res_nl);
    assert_eq!(
        rows_nl,
        vec![
            vec![Scalar::Int32(10)],
            vec![Scalar::Int32(20)],
            vec![Scalar::Null],
        ]
    );
}

#[test]
fn topk_equals_sort_then_limit() {
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "id".to_string(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "score".to_string(),
                dtype: DataType::Int64,
                nullable: false,
            },
        ],
    });

    let n: usize = 5000;
    let mut id_vals = Vec::with_capacity(n);
    let mut score_vals = Vec::with_capacity(n);
    let mut x: i64 = 123456789;
    for i in 0..n {
        x = (x.wrapping_mul(1103515245).wrapping_add(12345)) & 0x7fff_ffff;
        id_vals.push(Scalar::Int64(i as i64));
        score_vals.push(Scalar::Int64(x % 1000));
    }

    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < n {
        let len = (n - offset).min(BATCH_ROWS);
        let b = Batch::new(
            Arc::clone(&schema),
            vec![
                Arc::new(Column::from_scalars(
                    DataType::Int64,
                    &id_vals[offset..offset + len],
                )),
                Arc::new(Column::from_scalars(
                    DataType::Int64,
                    &score_vals[offset..offset + len],
                )),
            ],
        );
        batches.push(b);
        offset += len;
    }

    let table = make_table(Arc::clone(&schema), batches);
    let mut tables = HashMap::new();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0, 1],
        table_schema: (*schema).clone(),
    };
    let sort = LogicalPlan::Sort {
        input: Box::new(scan),
        keys: vec![
            SortKey {
                expr: Expr::Column(1),
                asc: true,
                nulls_first: false,
            },
            SortKey {
                expr: Expr::Column(0),
                asc: true,
                nulls_first: false,
            },
        ],
    };

    // Full sort then limit in memory
    let full_sort_res = execute(&sort, &catalog, 4).unwrap();
    let full_rows = collect_rows(&full_sort_res);

    for k in [1, 10, 100, 6000] {
        let topk_plan = LogicalPlan::Limit {
            input: Box::new(sort.clone()),
            n: k,
        };
        let topk_res = execute(&topk_plan, &catalog, 4).unwrap();
        let topk_rows = collect_rows(&topk_res);

        let expected_k = k.min(full_rows.len());
        assert_eq!(topk_rows.len(), expected_k);
        assert_eq!(topk_rows, full_rows[..expected_k].to_vec());
    }
}

#[test]
fn sort_utf8_bytewise() {
    let schema = Arc::new(Schema {
        fields: vec![Field {
            name: "word".to_string(),
            dtype: DataType::Utf8,
            nullable: false,
        }],
    });
    let batch = Batch::new(
        Arc::clone(&schema),
        vec![Arc::new(Column::from_scalars(
            DataType::Utf8,
            &[
                Scalar::Utf8("banana".to_string()),
                Scalar::Utf8("Apple".to_string()),
                Scalar::Utf8("apple".to_string()),
                Scalar::Utf8("Banana".to_string()),
            ],
        ))],
    );

    let mut tables = HashMap::new();
    tables.insert(
        "t".to_string(),
        make_table(Arc::clone(&schema), vec![batch]),
    );
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0],
        table_schema: (*schema).clone(),
    };
    let sort = LogicalPlan::Sort {
        input: Box::new(scan),
        keys: vec![SortKey {
            expr: Expr::Column(0),
            asc: true,
            nulls_first: false,
        }],
    };

    let res = execute(&sort, &catalog, 1).unwrap();
    let rows = collect_rows(&res);
    // ASCII byte order: 'Apple' (65), 'Banana' (66), 'apple' (97), 'banana' (98)
    assert_eq!(
        rows,
        vec![
            vec![Scalar::Utf8("Apple".to_string())],
            vec![Scalar::Utf8("Banana".to_string())],
            vec![Scalar::Utf8("apple".to_string())],
            vec![Scalar::Utf8("banana".to_string())],
        ]
    );
}

#[test]
fn sort_threads_do_not_change_result() {
    let schema = Arc::new(Schema {
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

    let n: usize = 10_000;
    let mut id_vals = Vec::with_capacity(n);
    let mut v_vals = Vec::with_capacity(n);
    let mut x: i64 = 987654321;
    for i in 0..n {
        x = (x.wrapping_mul(1103515245).wrapping_add(12345)) & 0x7fff_ffff;
        id_vals.push(Scalar::Int32(i as i32));
        v_vals.push(Scalar::Int64(x % 500));
    }

    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < n {
        let len = (n - offset).min(BATCH_ROWS);
        batches.push(Batch::new(
            Arc::clone(&schema),
            vec![
                Arc::new(Column::from_scalars(
                    DataType::Int32,
                    &id_vals[offset..offset + len],
                )),
                Arc::new(Column::from_scalars(
                    DataType::Int64,
                    &v_vals[offset..offset + len],
                )),
            ],
        ));
        offset += len;
    }

    let table = make_table(Arc::clone(&schema), batches);
    let mut tables = HashMap::new();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0, 1],
        table_schema: (*schema).clone(),
    };
    let sort = LogicalPlan::Sort {
        input: Box::new(scan),
        keys: vec![
            SortKey {
                expr: Expr::Column(1),
                asc: true,
                nulls_first: false,
            },
            SortKey {
                expr: Expr::Column(0),
                asc: true,
                nulls_first: false,
            },
        ],
    };

    let res1 = execute(&sort, &catalog, 1).unwrap();
    let res3 = execute(&sort, &catalog, 3).unwrap();
    let res16 = execute(&sort, &catalog, 16).unwrap();

    let rows1 = collect_rows(&res1);
    let rows3 = collect_rows(&res3);
    let rows16 = collect_rows(&res16);

    assert_eq!(rows1, rows3);
    assert_eq!(rows1, rows16);
}
