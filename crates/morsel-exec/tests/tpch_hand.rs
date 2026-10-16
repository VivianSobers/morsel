use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use morsel_core::{Batch, Scalar};
use morsel_exec::catalog::load_parquet_dir;
use morsel_exec::execute::execute;
use morsel_plan::{AggExpr, AggFunc, BinOp, Expr, LogicalPlan, SchemaProvider, SortKey};

fn write_csv(batches: &[Batch], out_path: &Path) {
    let mut file = File::create(out_path).expect("create csv file");
    if batches.is_empty() {
        return;
    }
    let schema = &batches[0].schema;
    let header = schema
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>()
        .join(",");
    writeln!(file, "{}", header).unwrap();

    for batch in batches {
        for row_idx in 0..batch.len {
            let mut row_strs = Vec::with_capacity(batch.columns.len());
            for col in &batch.columns {
                let s = col.get(row_idx);
                let text = match s {
                    Scalar::Null => "".to_string(),
                    Scalar::Bool(b) => b.to_string(),
                    Scalar::Int32(x) => x.to_string(),
                    Scalar::Int64(x) => x.to_string(),
                    Scalar::Float64(f) => format!("{:.2}", f),
                    Scalar::Decimal(v, scale) => {
                        if scale == 0 {
                            v.to_string()
                        } else {
                            let sign = if v < 0 { "-" } else { "" };
                            let v_abs = v.abs();
                            let div = 10i128.pow(scale as u32);
                            let whole = v_abs / div;
                            let frac = v_abs % div;
                            format!("{}{}.{:0width$}", sign, whole, frac, width = scale as usize)
                        }
                    }
                    Scalar::Date32(days) => {
                        let secs = days as i64 * 86400;
                        if let Some(dt) = chrono::DateTime::from_timestamp(secs, 0) {
                            dt.format("%Y-%m-%d").to_string()
                        } else {
                            days.to_string()
                        }
                    }
                    Scalar::Utf8(st) => {
                        if st.contains(',') || st.contains('"') || st.contains('\n') {
                            format!("\"{}\"", st.replace('"', "\"\""))
                        } else {
                            st
                        }
                    }
                };
                row_strs.push(text);
            }
            writeln!(file, "{}", row_strs.join(",")).unwrap();
        }
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn test_tpch_q1_and_q6() {
    let tpch_dir = match std::env::var("MORSEL_TPCH_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => {
            eprintln!("MORSEL_TPCH_DIR is not set; skipping tpch_hand test");
            return;
        }
    };

    let catalog = load_parquet_dir(&tpch_dir, 16).expect("load parquet dir");
    let lineitem_schema = catalog
        .table_schema("lineitem")
        .expect("lineitem table in catalog");

    let col_idx = |name: &str| -> usize {
        lineitem_schema
            .index_of(name)
            .unwrap_or_else(|| panic!("column {} not in lineitem", name))
    };

    let idx_returnflag = col_idx("l_returnflag");
    let idx_linestatus = col_idx("l_linestatus");
    let idx_quantity = col_idx("l_quantity");
    let idx_extendedprice = col_idx("l_extendedprice");
    let idx_discount = col_idx("l_discount");
    let idx_tax = col_idx("l_tax");
    let idx_shipdate = col_idx("l_shipdate");

    // ==========================================
    // TPC-H Q6 Plan
    // ==========================================
    // select sum(l_extendedprice * l_discount) as revenue
    // from lineitem
    // where l_shipdate >= date '1994-01-01'
    //   and l_shipdate < date '1995-01-01'
    //   and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    //   and l_quantity < 24;
    {
        let q6_scan = LogicalPlan::Scan {
            table: "lineitem".to_string(),
            columns: vec![idx_extendedprice, idx_discount, idx_shipdate, idx_quantity],
            table_schema: lineitem_schema.clone(),
        };

        // Scan columns: 0: extprice, 1: discount, 2: shipdate, 3: quantity
        // l_shipdate >= 1994-01-01 (day 8766)
        let pred1 = Expr::Binary {
            op: BinOp::GtEq,
            left: Box::new(Expr::Column(2)),
            right: Box::new(Expr::Literal(Scalar::Date32(8766))),
        };
        // l_shipdate < 1995-01-01 (day 9131)
        let pred2 = Expr::Binary {
            op: BinOp::Lt,
            left: Box::new(Expr::Column(2)),
            right: Box::new(Expr::Literal(Scalar::Date32(9131))),
        };
        // l_discount >= 0.05
        let pred3 = Expr::Binary {
            op: BinOp::GtEq,
            left: Box::new(Expr::Column(1)),
            right: Box::new(Expr::Literal(Scalar::Decimal(5, 2))),
        };
        // l_discount <= 0.07
        let pred4 = Expr::Binary {
            op: BinOp::LtEq,
            left: Box::new(Expr::Column(1)),
            right: Box::new(Expr::Literal(Scalar::Decimal(7, 2))),
        };
        // l_quantity < 24
        let pred5 = Expr::Binary {
            op: BinOp::Lt,
            left: Box::new(Expr::Column(3)),
            right: Box::new(Expr::Literal(Scalar::Decimal(2400, 2))),
        };

        let q6_pred = Expr::Binary {
            op: BinOp::And,
            left: Box::new(Expr::Binary {
                op: BinOp::And,
                left: Box::new(pred1),
                right: Box::new(pred2),
            }),
            right: Box::new(Expr::Binary {
                op: BinOp::And,
                left: Box::new(Expr::Binary {
                    op: BinOp::And,
                    left: Box::new(pred3),
                    right: Box::new(pred4),
                }),
                right: Box::new(pred5),
            }),
        };

        let q6_filter = LogicalPlan::Filter {
            input: Box::new(q6_scan),
            predicate: q6_pred,
        };

        let q6_agg = LogicalPlan::Aggregate {
            input: Box::new(q6_filter),
            group_by: vec![],
            aggs: vec![(
                AggExpr {
                    func: AggFunc::Sum,
                    arg: Some(Expr::Binary {
                        op: BinOp::Mul,
                        left: Box::new(Expr::Column(0)),
                        right: Box::new(Expr::Column(1)),
                    }),
                },
                "revenue".to_string(),
            )],
        };

        let res_q6 = execute(&q6_agg, &catalog, 16).expect("execute q6");
        let tmp_csv = repo_root().join("target").join("q06_actual.csv");
        if let Some(parent) = tmp_csv.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        write_csv(&res_q6, &tmp_csv);

        let ref_csv = repo_root()
            .join("bench")
            .join("refs")
            .join("sf1")
            .join("q06.csv");
        let check_script = repo_root().join("bench").join("check.py");
        if check_script.exists() && ref_csv.exists() {
            let status = Command::new("python3")
                .arg(&check_script)
                .arg(&tmp_csv)
                .arg(&ref_csv)
                .arg("--query")
                .arg("6")
                .status()
                .expect("run check.py for q6");
            assert!(status.success(), "check.py failed for q6");
        }
    }

    // ==========================================
    // TPC-H Q1 Plan
    // ==========================================
    {
        let q1_scan = LogicalPlan::Scan {
            table: "lineitem".to_string(),
            columns: vec![
                idx_returnflag,
                idx_linestatus,
                idx_quantity,
                idx_extendedprice,
                idx_discount,
                idx_tax,
                idx_shipdate,
            ],
            table_schema: lineitem_schema.clone(),
        };

        // Scan columns:
        // 0: l_returnflag, 1: l_linestatus, 2: l_quantity, 3: l_extendedprice,
        // 4: l_discount, 5: l_tax, 6: l_shipdate
        // Filter: l_shipdate <= date '1998-09-02' (day 10471)
        let q1_pred = Expr::Binary {
            op: BinOp::LtEq,
            left: Box::new(Expr::Column(6)),
            right: Box::new(Expr::Literal(Scalar::Date32(10471))),
        };
        let q1_filter = LogicalPlan::Filter {
            input: Box::new(q1_scan),
            predicate: q1_pred,
        };

        // Project:
        // 0: l_returnflag
        // 1: l_linestatus
        // 2: l_quantity
        // 3: l_extendedprice
        // 4: l_extendedprice * (1 - l_discount)
        // 5: l_extendedprice * (1 - l_discount) * (1 + l_tax)
        // 6: l_discount
        let one_minus_disc = Expr::Binary {
            op: BinOp::Sub,
            left: Box::new(Expr::Literal(Scalar::Int32(1))),
            right: Box::new(Expr::Column(4)),
        };
        let disc_price = Expr::Binary {
            op: BinOp::Mul,
            left: Box::new(Expr::Column(3)),
            right: Box::new(one_minus_disc),
        };
        let one_plus_tax = Expr::Binary {
            op: BinOp::Add,
            left: Box::new(Expr::Literal(Scalar::Int32(1))),
            right: Box::new(Expr::Column(5)),
        };
        let charge = Expr::Binary {
            op: BinOp::Mul,
            left: Box::new(disc_price.clone()),
            right: Box::new(one_plus_tax),
        };

        let q1_project = LogicalPlan::Project {
            input: Box::new(q1_filter),
            exprs: vec![
                (Expr::Column(0), "l_returnflag".to_string()),
                (Expr::Column(1), "l_linestatus".to_string()),
                (Expr::Column(2), "l_quantity".to_string()),
                (Expr::Column(3), "l_extendedprice".to_string()),
                (disc_price, "disc_price".to_string()),
                (charge, "charge".to_string()),
                (Expr::Column(4), "l_discount".to_string()),
            ],
        };

        let q1_agg = LogicalPlan::Aggregate {
            input: Box::new(q1_project),
            group_by: vec![
                (Expr::Column(0), "l_returnflag".to_string()),
                (Expr::Column(1), "l_linestatus".to_string()),
            ],
            aggs: vec![
                (
                    AggExpr {
                        func: AggFunc::Sum,
                        arg: Some(Expr::Column(2)),
                    },
                    "sum_qty".to_string(),
                ),
                (
                    AggExpr {
                        func: AggFunc::Sum,
                        arg: Some(Expr::Column(3)),
                    },
                    "sum_base_price".to_string(),
                ),
                (
                    AggExpr {
                        func: AggFunc::Sum,
                        arg: Some(Expr::Column(4)),
                    },
                    "sum_disc_price".to_string(),
                ),
                (
                    AggExpr {
                        func: AggFunc::Sum,
                        arg: Some(Expr::Column(5)),
                    },
                    "sum_charge".to_string(),
                ),
                (
                    AggExpr {
                        func: AggFunc::Avg,
                        arg: Some(Expr::Column(2)),
                    },
                    "avg_qty".to_string(),
                ),
                (
                    AggExpr {
                        func: AggFunc::Avg,
                        arg: Some(Expr::Column(3)),
                    },
                    "avg_price".to_string(),
                ),
                (
                    AggExpr {
                        func: AggFunc::Avg,
                        arg: Some(Expr::Column(6)),
                    },
                    "avg_disc".to_string(),
                ),
                (
                    AggExpr {
                        func: AggFunc::CountStar,
                        arg: None,
                    },
                    "count_order".to_string(),
                ),
            ],
        };

        let q1_sort = LogicalPlan::Sort {
            input: Box::new(q1_agg),
            keys: vec![
                SortKey {
                    expr: Expr::Column(0),
                    asc: true,
                    nulls_first: false,
                },
                SortKey {
                    expr: Expr::Column(1),
                    asc: true,
                    nulls_first: false,
                },
            ],
        };

        let res_q1 = execute(&q1_sort, &catalog, 16).expect("execute q1");
        let tmp_csv = repo_root().join("target").join("q01_actual.csv");
        write_csv(&res_q1, &tmp_csv);

        let ref_csv = repo_root()
            .join("bench")
            .join("refs")
            .join("sf1")
            .join("q01.csv");
        let check_script = repo_root().join("bench").join("check.py");
        if check_script.exists() && ref_csv.exists() {
            let status = Command::new("python3")
                .arg(&check_script)
                .arg(&tmp_csv)
                .arg(&ref_csv)
                .arg("--query")
                .arg("1")
                .status()
                .expect("run check.py for q1");
            assert!(status.success(), "check.py failed for q1");
        }
    }
}
