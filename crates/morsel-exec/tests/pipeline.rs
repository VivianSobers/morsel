use std::collections::HashMap;
use std::sync::Arc;

use morsel_core::{Batch, Column, DataType, Field, Scalar, Schema, BATCH_ROWS};
use morsel_exec::catalog::{Catalog, Table};
use morsel_exec::execute::execute;
use morsel_plan::{BinOp, Expr, LogicalPlan};

fn make_table(rows: usize) -> Table {
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                name: "id".to_string(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "val".to_string(),
                dtype: DataType::Int64,
                nullable: false,
            },
        ],
    });

    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < rows {
        let len = (rows - offset).min(BATCH_ROWS);
        let id_scalars: Vec<Scalar> = (offset..offset + len)
            .map(|i| Scalar::Int64(i as i64))
            .collect();
        let val_scalars: Vec<Scalar> = (offset..offset + len)
            .map(|i| Scalar::Int64((i * 10) as i64))
            .collect();
        let col_id = Arc::new(Column::from_scalars(DataType::Int64, &id_scalars));
        let col_val = Arc::new(Column::from_scalars(DataType::Int64, &val_scalars));
        batches.push(Batch::new(Arc::clone(&schema), vec![col_id, col_val]));
        offset += len;
    }

    Table {
        schema,
        batches,
        rows,
    }
}

fn count_rows(batches: &[Batch]) -> usize {
    batches.iter().map(|b| b.len).sum()
}

fn collect_ids(batches: &[Batch]) -> Vec<i64> {
    let mut ids = Vec::new();
    for b in batches {
        for i in 0..b.len {
            if let Scalar::Int64(x) = b.columns[0].get(i) {
                ids.push(x);
            }
        }
    }
    ids
}

#[test]
fn same_result_for_1_3_16_threads() {
    let n = 300_001;
    let table = make_table(n);
    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    // Filter: id >= 100_000
    // Project: id, id + val
    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0, 1],
        table_schema,
    };
    let filter = LogicalPlan::Filter {
        input: Box::new(scan),
        predicate: Expr::Binary {
            op: BinOp::GtEq,
            left: Box::new(Expr::Column(0)),
            right: Box::new(Expr::Literal(Scalar::Int64(100_000))),
        },
    };
    let project = LogicalPlan::Project {
        input: Box::new(filter),
        exprs: vec![
            (Expr::Column(0), "id".to_string()),
            (
                Expr::Binary {
                    op: BinOp::Add,
                    left: Box::new(Expr::Column(0)),
                    right: Box::new(Expr::Column(1)),
                },
                "sum".to_string(),
            ),
        ],
    };

    let res1 = execute(&project, &catalog, 1).unwrap();
    let res3 = execute(&project, &catalog, 3).unwrap();
    let res16 = execute(&project, &catalog, 16).unwrap();

    let ids1 = collect_ids(&res1);
    let ids3 = collect_ids(&res3);
    let ids16 = collect_ids(&res16);

    assert_eq!(ids1.len(), 200_001);
    assert_eq!(ids1, ids3);
    assert_eq!(ids1, ids16);
}

#[test]
fn table_smaller_than_one_morsel() {
    let table = make_table(500);
    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0],
        table_schema,
    };
    let res = execute(&scan, &catalog, 4).unwrap();
    assert_eq!(count_rows(&res), 500);
}

#[test]
fn empty_table() {
    let table = make_table(0);
    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0],
        table_schema,
    };
    let filter = LogicalPlan::Filter {
        input: Box::new(scan),
        predicate: Expr::Binary {
            op: BinOp::Eq,
            left: Box::new(Expr::Column(0)),
            right: Box::new(Expr::Literal(Scalar::Int64(0))),
        },
    };
    let res = execute(&filter, &catalog, 2).unwrap();
    assert_eq!(count_rows(&res), 0);
}

#[test]
fn more_threads_than_morsels() {
    let table = make_table(5000);
    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0],
        table_schema,
    };
    let res = execute(&scan, &catalog, 16).unwrap();
    assert_eq!(count_rows(&res), 5000);
}

#[test]
fn limit_returns_n_rows() {
    let table = make_table(5000);
    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0],
        table_schema,
    };
    let limit = LogicalPlan::Limit {
        input: Box::new(scan),
        n: 10,
    };
    let res = execute(&limit, &catalog, 4).unwrap();
    assert_eq!(count_rows(&res), 10);
}

#[test]
fn rows_not_multiple_of_batch() {
    let table = make_table(2500);
    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0],
        table_schema,
    };
    let res = execute(&scan, &catalog, 2).unwrap();
    assert_eq!(count_rows(&res), 2500);
}

#[test]
fn thirty_two_threads_works() {
    let table = make_table(100_000);
    let mut tables = HashMap::new();
    let table_schema = (*table.schema).clone();
    tables.insert("t".to_string(), table);
    let catalog = Catalog { tables };

    let scan = LogicalPlan::Scan {
        table: "t".to_string(),
        columns: vec![0],
        table_schema,
    };
    let res = execute(&scan, &catalog, 32).unwrap();
    assert_eq!(count_rows(&res), 100_000);
}
