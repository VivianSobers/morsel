use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use morsel_core::{Batch, Scalar};
use morsel_exec::catalog::{load_parquet_dir, Catalog};
use morsel_exec::execute::execute;
use morsel_plan::{AggExpr, AggFunc, BinOp, Expr, JoinType, LogicalPlan, SchemaProvider, SortKey};

pub fn write_csv(batches: &[Batch], out_path: &Path) {
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

    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

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
                    Scalar::Float64(f) => f.to_string(),
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
                        if let Some(d) =
                            epoch.checked_add_signed(chrono::Duration::days(days as i64))
                        {
                            d.format("%Y-%m-%d").to_string()
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

pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

pub fn date_to_days(s: &str) -> i32 {
    let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").expect("valid date");
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (date - epoch).num_days() as i32
}

pub fn check_query(query_num: usize, plan: &LogicalPlan, catalog: &Catalog) {
    let res = execute(plan, catalog, 16)
        .unwrap_or_else(|e| panic!("execute q{:02} failed: {:?}", query_num, e));
    let tmp_csv = repo_root()
        .join("target")
        .join(format!("q{:02}_actual.csv", query_num));
    if let Some(parent) = tmp_csv.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    write_csv(&res, &tmp_csv);

    let refs_dir = if let Ok(d) = std::env::var("MORSEL_TPCH_REFS") {
        PathBuf::from(d)
    } else {
        repo_root().join("bench").join("refs").join("sf1")
    };
    let ref_csv = refs_dir.join(format!("q{:02}.csv", query_num));
    let check_script = repo_root().join("bench").join("check.py");
    if check_script.exists() && ref_csv.exists() {
        let status = Command::new("python3")
            .arg(&check_script)
            .arg(&tmp_csv)
            .arg(&ref_csv)
            .arg("--query")
            .arg(query_num.to_string())
            .status()
            .expect("run check.py");
        assert!(status.success(), "check.py failed for q{:02}", query_num);
    } else {
        panic!(
            "Missing check.py ({:?}) or ref_csv ({:?})",
            check_script, ref_csv
        );
    }
}

pub fn plan_q01(catalog: &Catalog) -> LogicalPlan {
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
        table_schema: lineitem_schema,
    };

    let q1_pred = Expr::Binary {
        op: BinOp::LtEq,
        left: Box::new(Expr::Column(6)),
        right: Box::new(Expr::Literal(Scalar::Date32(date_to_days("1998-09-02")))),
    };
    let q1_filter = LogicalPlan::Filter {
        input: Box::new(q1_scan),
        predicate: q1_pred,
    };

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

    LogicalPlan::Sort {
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
    }
}

pub fn plan_q06(catalog: &Catalog) -> LogicalPlan {
    let lineitem_schema = catalog
        .table_schema("lineitem")
        .expect("lineitem table in catalog");
    let col_idx = |name: &str| -> usize {
        lineitem_schema
            .index_of(name)
            .unwrap_or_else(|| panic!("column {} not in lineitem", name))
    };

    let idx_extendedprice = col_idx("l_extendedprice");
    let idx_discount = col_idx("l_discount");
    let idx_shipdate = col_idx("l_shipdate");
    let idx_quantity = col_idx("l_quantity");

    let q6_scan = LogicalPlan::Scan {
        table: "lineitem".to_string(),
        columns: vec![idx_extendedprice, idx_discount, idx_shipdate, idx_quantity],
        table_schema: lineitem_schema,
    };

    let pred1 = Expr::Binary {
        op: BinOp::GtEq,
        left: Box::new(Expr::Column(2)),
        right: Box::new(Expr::Literal(Scalar::Date32(date_to_days("1994-01-01")))),
    };
    let pred2 = Expr::Binary {
        op: BinOp::Lt,
        left: Box::new(Expr::Column(2)),
        right: Box::new(Expr::Literal(Scalar::Date32(date_to_days("1995-01-01")))),
    };
    let pred3 = Expr::Binary {
        op: BinOp::GtEq,
        left: Box::new(Expr::Column(1)),
        right: Box::new(Expr::Literal(Scalar::Decimal(5, 2))),
    };
    let pred4 = Expr::Binary {
        op: BinOp::LtEq,
        left: Box::new(Expr::Column(1)),
        right: Box::new(Expr::Literal(Scalar::Decimal(7, 2))),
    };
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

    LogicalPlan::Aggregate {
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
    }
}

pub fn plan_q03(catalog: &Catalog) -> LogicalPlan {
    let cust_schema = catalog.table_schema("customer").unwrap();
    let orders_schema = catalog.table_schema("orders").unwrap();
    let lineitem_schema = catalog.table_schema("lineitem").unwrap();

    let idx_c_custkey = cust_schema.index_of("c_custkey").unwrap();
    let idx_c_mktsegment = cust_schema.index_of("c_mktsegment").unwrap();

    let idx_o_orderkey = orders_schema.index_of("o_orderkey").unwrap();
    let idx_o_custkey = orders_schema.index_of("o_custkey").unwrap();
    let idx_o_orderdate = orders_schema.index_of("o_orderdate").unwrap();
    let idx_o_shippriority = orders_schema.index_of("o_shippriority").unwrap();

    let idx_l_orderkey = lineitem_schema.index_of("l_orderkey").unwrap();
    let idx_l_extendedprice = lineitem_schema.index_of("l_extendedprice").unwrap();
    let idx_l_discount = lineitem_schema.index_of("l_discount").unwrap();
    let idx_l_shipdate = lineitem_schema.index_of("l_shipdate").unwrap();

    // Customer: filter c_mktsegment = 'BUILDING', keep c_custkey
    let cust_scan = LogicalPlan::Scan {
        table: "customer".to_string(),
        columns: vec![idx_c_custkey, idx_c_mktsegment],
        table_schema: cust_schema,
    };
    let cust_filter = LogicalPlan::Filter {
        input: Box::new(cust_scan),
        predicate: Expr::Binary {
            op: BinOp::Eq,
            left: Box::new(Expr::Column(1)),
            right: Box::new(Expr::Literal(Scalar::Utf8("BUILDING".to_string()))),
        },
    };
    let cust_project = LogicalPlan::Project {
        input: Box::new(cust_filter),
        exprs: vec![(Expr::Column(0), "c_custkey".to_string())],
    };

    // Orders: filter o_orderdate < 1995-03-15
    let orders_scan = LogicalPlan::Scan {
        table: "orders".to_string(),
        columns: vec![
            idx_o_orderkey,
            idx_o_custkey,
            idx_o_orderdate,
            idx_o_shippriority,
        ],
        table_schema: orders_schema,
    };
    let orders_filter = LogicalPlan::Filter {
        input: Box::new(orders_scan),
        predicate: Expr::Binary {
            op: BinOp::Lt,
            left: Box::new(Expr::Column(2)),
            right: Box::new(Expr::Literal(Scalar::Date32(date_to_days("1995-03-15")))),
        },
    };

    // Join orders (left) and customer (right) on o_custkey = c_custkey
    let orders_cust_join = LogicalPlan::Join {
        left: Box::new(orders_filter),
        right: Box::new(cust_project),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(1), Expr::Column(0))],
        filter: None,
    };
    // Result schema: 0: o_orderkey, 1: o_custkey, 2: o_orderdate, 3: o_shippriority, 4: c_custkey
    let orders_cust_project = LogicalPlan::Project {
        input: Box::new(orders_cust_join),
        exprs: vec![
            (Expr::Column(0), "o_orderkey".to_string()),
            (Expr::Column(2), "o_orderdate".to_string()),
            (Expr::Column(3), "o_shippriority".to_string()),
        ],
    };

    // Lineitem: filter l_shipdate > 1995-03-15
    let lineitem_scan = LogicalPlan::Scan {
        table: "lineitem".to_string(),
        columns: vec![
            idx_l_orderkey,
            idx_l_extendedprice,
            idx_l_discount,
            idx_l_shipdate,
        ],
        table_schema: lineitem_schema,
    };
    let lineitem_filter = LogicalPlan::Filter {
        input: Box::new(lineitem_scan),
        predicate: Expr::Binary {
            op: BinOp::Gt,
            left: Box::new(Expr::Column(3)),
            right: Box::new(Expr::Literal(Scalar::Date32(date_to_days("1995-03-15")))),
        },
    };
    let disc_price = Expr::Binary {
        op: BinOp::Mul,
        left: Box::new(Expr::Column(1)),
        right: Box::new(Expr::Binary {
            op: BinOp::Sub,
            left: Box::new(Expr::Literal(Scalar::Int32(1))),
            right: Box::new(Expr::Column(2)),
        }),
    };
    let lineitem_project = LogicalPlan::Project {
        input: Box::new(lineitem_filter),
        exprs: vec![
            (Expr::Column(0), "l_orderkey".to_string()),
            (disc_price, "disc_price".to_string()),
        ],
    };

    // Join lineitem (left) and orders_cust (right) on l_orderkey = o_orderkey
    let join_all = LogicalPlan::Join {
        left: Box::new(lineitem_project),
        right: Box::new(orders_cust_project),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };
    // Join output: 0: l_orderkey, 1: disc_price, 2: o_orderkey, 3: o_orderdate, 4: o_shippriority

    let agg = LogicalPlan::Aggregate {
        input: Box::new(join_all),
        group_by: vec![
            (Expr::Column(0), "l_orderkey".to_string()),
            (Expr::Column(3), "o_orderdate".to_string()),
            (Expr::Column(4), "o_shippriority".to_string()),
        ],
        aggs: vec![(
            AggExpr {
                func: AggFunc::Sum,
                arg: Some(Expr::Column(1)),
            },
            "revenue".to_string(),
        )],
    };
    // Agg output: 0: l_orderkey, 1: o_orderdate, 2: o_shippriority, 3: revenue

    // Reorder to match: l_orderkey, revenue, o_orderdate, o_shippriority
    let project_out = LogicalPlan::Project {
        input: Box::new(agg),
        exprs: vec![
            (Expr::Column(0), "l_orderkey".to_string()),
            (Expr::Column(3), "revenue".to_string()),
            (Expr::Column(1), "o_orderdate".to_string()),
            (Expr::Column(2), "o_shippriority".to_string()),
        ],
    };

    let sort = LogicalPlan::Sort {
        input: Box::new(project_out),
        keys: vec![
            SortKey {
                expr: Expr::Column(1),
                asc: false,
                nulls_first: false,
            },
            SortKey {
                expr: Expr::Column(2),
                asc: true,
                nulls_first: false,
            },
        ],
    };

    LogicalPlan::Limit {
        input: Box::new(sort),
        n: 10,
    }
}

pub fn plan_q04(catalog: &Catalog) -> LogicalPlan {
    let orders_schema = catalog.table_schema("orders").unwrap();
    let lineitem_schema = catalog.table_schema("lineitem").unwrap();

    let idx_o_orderkey = orders_schema.index_of("o_orderkey").unwrap();
    let idx_o_orderdate = orders_schema.index_of("o_orderdate").unwrap();
    let idx_o_orderpriority = orders_schema.index_of("o_orderpriority").unwrap();

    let idx_l_orderkey = lineitem_schema.index_of("l_orderkey").unwrap();
    let idx_l_commitdate = lineitem_schema.index_of("l_commitdate").unwrap();
    let idx_l_receiptdate = lineitem_schema.index_of("l_receiptdate").unwrap();

    let orders_scan = LogicalPlan::Scan {
        table: "orders".to_string(),
        columns: vec![idx_o_orderkey, idx_o_orderdate, idx_o_orderpriority],
        table_schema: orders_schema,
    };
    let orders_filter = LogicalPlan::Filter {
        input: Box::new(orders_scan),
        predicate: Expr::Binary {
            op: BinOp::And,
            left: Box::new(Expr::Binary {
                op: BinOp::GtEq,
                left: Box::new(Expr::Column(1)),
                right: Box::new(Expr::Literal(Scalar::Date32(date_to_days("1993-07-01")))),
            }),
            right: Box::new(Expr::Binary {
                op: BinOp::Lt,
                left: Box::new(Expr::Column(1)),
                right: Box::new(Expr::Literal(Scalar::Date32(date_to_days("1993-10-01")))),
            }),
        },
    };
    let orders_project = LogicalPlan::Project {
        input: Box::new(orders_filter),
        exprs: vec![
            (Expr::Column(0), "o_orderkey".to_string()),
            (Expr::Column(2), "o_orderpriority".to_string()),
        ],
    };

    let lineitem_scan = LogicalPlan::Scan {
        table: "lineitem".to_string(),
        columns: vec![idx_l_orderkey, idx_l_commitdate, idx_l_receiptdate],
        table_schema: lineitem_schema,
    };
    let lineitem_filter = LogicalPlan::Filter {
        input: Box::new(lineitem_scan),
        predicate: Expr::Binary {
            op: BinOp::Lt,
            left: Box::new(Expr::Column(1)),
            right: Box::new(Expr::Column(2)),
        },
    };
    let lineitem_project = LogicalPlan::Project {
        input: Box::new(lineitem_filter),
        exprs: vec![(Expr::Column(0), "l_orderkey".to_string())],
    };

    let semi_join = LogicalPlan::Join {
        left: Box::new(orders_project),
        right: Box::new(lineitem_project),
        join_type: JoinType::Semi,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };

    let agg = LogicalPlan::Aggregate {
        input: Box::new(semi_join),
        group_by: vec![(Expr::Column(1), "o_orderpriority".to_string())],
        aggs: vec![(
            AggExpr {
                func: AggFunc::CountStar,
                arg: None,
            },
            "order_count".to_string(),
        )],
    };

    LogicalPlan::Sort {
        input: Box::new(agg),
        keys: vec![SortKey {
            expr: Expr::Column(0),
            asc: true,
            nulls_first: false,
        }],
    }
}

pub fn plan_q05(catalog: &Catalog) -> LogicalPlan {
    let region_schema = catalog.table_schema("region").unwrap();
    let nation_schema = catalog.table_schema("nation").unwrap();
    let supp_schema = catalog.table_schema("supplier").unwrap();
    let cust_schema = catalog.table_schema("customer").unwrap();
    let orders_schema = catalog.table_schema("orders").unwrap();
    let lineitem_schema = catalog.table_schema("lineitem").unwrap();

    let idx_r_regionkey = region_schema.index_of("r_regionkey").unwrap();
    let idx_r_name = region_schema.index_of("r_name").unwrap();

    let idx_n_nationkey = nation_schema.index_of("n_nationkey").unwrap();
    let idx_n_name = nation_schema.index_of("n_name").unwrap();
    let idx_n_regionkey = nation_schema.index_of("n_regionkey").unwrap();

    let idx_s_suppkey = supp_schema.index_of("s_suppkey").unwrap();
    let idx_s_nationkey = supp_schema.index_of("s_nationkey").unwrap();

    let idx_c_custkey = cust_schema.index_of("c_custkey").unwrap();
    let idx_c_nationkey = cust_schema.index_of("c_nationkey").unwrap();

    let idx_o_orderkey = orders_schema.index_of("o_orderkey").unwrap();
    let idx_o_custkey = orders_schema.index_of("o_custkey").unwrap();
    let idx_o_orderdate = orders_schema.index_of("o_orderdate").unwrap();

    let idx_l_orderkey = lineitem_schema.index_of("l_orderkey").unwrap();
    let idx_l_suppkey = lineitem_schema.index_of("l_suppkey").unwrap();
    let idx_l_extendedprice = lineitem_schema.index_of("l_extendedprice").unwrap();
    let idx_l_discount = lineitem_schema.index_of("l_discount").unwrap();

    // 1. Region filter r_name = 'ASIA'
    let region_scan = LogicalPlan::Scan {
        table: "region".to_string(),
        columns: vec![idx_r_regionkey, idx_r_name],
        table_schema: region_schema,
    };
    let region_filter = LogicalPlan::Filter {
        input: Box::new(region_scan),
        predicate: Expr::Binary {
            op: BinOp::Eq,
            left: Box::new(Expr::Column(1)),
            right: Box::new(Expr::Literal(Scalar::Utf8("ASIA".to_string()))),
        },
    };
    let region_project = LogicalPlan::Project {
        input: Box::new(region_filter),
        exprs: vec![(Expr::Column(0), "r_regionkey".to_string())],
    };

    // 2. Nation join Region
    let nation_scan = LogicalPlan::Scan {
        table: "nation".to_string(),
        columns: vec![idx_n_nationkey, idx_n_name, idx_n_regionkey],
        table_schema: nation_schema,
    };
    let nation_region_join = LogicalPlan::Join {
        left: Box::new(nation_scan),
        right: Box::new(region_project),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(2), Expr::Column(0))],
        filter: None,
    };
    let nation_asia = LogicalPlan::Project {
        input: Box::new(nation_region_join),
        exprs: vec![
            (Expr::Column(0), "n_nationkey".to_string()),
            (Expr::Column(1), "n_name".to_string()),
        ],
    };

    // 3. Supplier join nation_asia
    let supp_scan = LogicalPlan::Scan {
        table: "supplier".to_string(),
        columns: vec![idx_s_suppkey, idx_s_nationkey],
        table_schema: supp_schema,
    };
    let supp_nation_join = LogicalPlan::Join {
        left: Box::new(supp_scan),
        right: Box::new(nation_asia),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(1), Expr::Column(0))],
        filter: None,
    };
    let supp_asia = LogicalPlan::Project {
        input: Box::new(supp_nation_join),
        exprs: vec![
            (Expr::Column(0), "s_suppkey".to_string()),
            (Expr::Column(1), "s_nationkey".to_string()),
            (Expr::Column(3), "n_name".to_string()),
        ],
    };

    // 4. Customer and Orders (o_orderdate >= 1994-01-01 and < 1995-01-01)
    let cust_scan = LogicalPlan::Scan {
        table: "customer".to_string(),
        columns: vec![idx_c_custkey, idx_c_nationkey],
        table_schema: cust_schema,
    };
    let orders_scan = LogicalPlan::Scan {
        table: "orders".to_string(),
        columns: vec![idx_o_orderkey, idx_o_custkey, idx_o_orderdate],
        table_schema: orders_schema,
    };
    let orders_filter = LogicalPlan::Filter {
        input: Box::new(orders_scan),
        predicate: Expr::Binary {
            op: BinOp::And,
            left: Box::new(Expr::Binary {
                op: BinOp::GtEq,
                left: Box::new(Expr::Column(2)),
                right: Box::new(Expr::Literal(Scalar::Date32(date_to_days("1994-01-01")))),
            }),
            right: Box::new(Expr::Binary {
                op: BinOp::Lt,
                left: Box::new(Expr::Column(2)),
                right: Box::new(Expr::Literal(Scalar::Date32(date_to_days("1995-01-01")))),
            }),
        },
    };
    let orders_cust_join = LogicalPlan::Join {
        left: Box::new(orders_filter),
        right: Box::new(cust_scan),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(1), Expr::Column(0))],
        filter: None,
    };
    let orders_cust = LogicalPlan::Project {
        input: Box::new(orders_cust_join),
        exprs: vec![
            (Expr::Column(0), "o_orderkey".to_string()),
            (Expr::Column(4), "c_nationkey".to_string()),
        ],
    };

    // 5. Lineitem join orders_cust
    let lineitem_scan = LogicalPlan::Scan {
        table: "lineitem".to_string(),
        columns: vec![
            idx_l_orderkey,
            idx_l_suppkey,
            idx_l_extendedprice,
            idx_l_discount,
        ],
        table_schema: lineitem_schema,
    };
    let disc_price = Expr::Binary {
        op: BinOp::Mul,
        left: Box::new(Expr::Column(2)),
        right: Box::new(Expr::Binary {
            op: BinOp::Sub,
            left: Box::new(Expr::Literal(Scalar::Int32(1))),
            right: Box::new(Expr::Column(3)),
        }),
    };
    let lineitem_project = LogicalPlan::Project {
        input: Box::new(lineitem_scan),
        exprs: vec![
            (Expr::Column(0), "l_orderkey".to_string()),
            (Expr::Column(1), "l_suppkey".to_string()),
            (disc_price, "disc_price".to_string()),
        ],
    };
    let line_orders_cust_join = LogicalPlan::Join {
        left: Box::new(lineitem_project),
        right: Box::new(orders_cust),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };
    let line_orders_cust = LogicalPlan::Project {
        input: Box::new(line_orders_cust_join),
        exprs: vec![
            (Expr::Column(1), "l_suppkey".to_string()),
            (Expr::Column(4), "c_nationkey".to_string()),
            (Expr::Column(2), "disc_price".to_string()),
        ],
    };

    // 6. Join line_orders_cust and supp_asia on l_suppkey = s_suppkey AND c_nationkey = s_nationkey
    let join_supp = LogicalPlan::Join {
        left: Box::new(line_orders_cust),
        right: Box::new(supp_asia),
        join_type: JoinType::Inner,
        on: vec![
            (Expr::Column(0), Expr::Column(0)),
            (Expr::Column(1), Expr::Column(1)),
        ],
        filter: None,
    };
    // Join output: 0: l_suppkey, 1: c_nationkey, 2: disc_price, 3: s_suppkey, 4: s_nationkey, 5: n_name

    let agg = LogicalPlan::Aggregate {
        input: Box::new(join_supp),
        group_by: vec![(Expr::Column(5), "n_name".to_string())],
        aggs: vec![(
            AggExpr {
                func: AggFunc::Sum,
                arg: Some(Expr::Column(2)),
            },
            "revenue".to_string(),
        )],
    };

    LogicalPlan::Sort {
        input: Box::new(agg),
        keys: vec![SortKey {
            expr: Expr::Column(1),
            asc: false,
            nulls_first: false,
        }],
    }
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

    let p6 = plan_q06(&catalog);
    check_query(6, &p6, &catalog);

    let p1 = plan_q01(&catalog);
    check_query(1, &p1, &catalog);
}

#[test]
fn test_tpch_q3_q4_q5() {
    let tpch_dir = match std::env::var("MORSEL_TPCH_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => {
            eprintln!("MORSEL_TPCH_DIR is not set; skipping tpch_hand test");
            return;
        }
    };
    let catalog = load_parquet_dir(&tpch_dir, 16).expect("load parquet dir");

    let p3 = plan_q03(&catalog);
    check_query(3, &p3, &catalog);

    let p4 = plan_q04(&catalog);
    check_query(4, &p4, &catalog);

    let p5 = plan_q05(&catalog);
    check_query(5, &p5, &catalog);
}

pub fn plan_q10(catalog: &Catalog) -> LogicalPlan {
    let cust_schema = catalog.table_schema("customer").unwrap();
    let orders_schema = catalog.table_schema("orders").unwrap();
    let lineitem_schema = catalog.table_schema("lineitem").unwrap();
    let nation_schema = catalog.table_schema("nation").unwrap();

    let idx_c_custkey = cust_schema.index_of("c_custkey").unwrap();
    let idx_c_name = cust_schema.index_of("c_name").unwrap();
    let idx_c_address = cust_schema.index_of("c_address").unwrap();
    let idx_c_nationkey = cust_schema.index_of("c_nationkey").unwrap();
    let idx_c_phone = cust_schema.index_of("c_phone").unwrap();
    let idx_c_acctbal = cust_schema.index_of("c_acctbal").unwrap();
    let idx_c_comment = cust_schema.index_of("c_comment").unwrap();

    let idx_n_nationkey = nation_schema.index_of("n_nationkey").unwrap();
    let idx_n_name = nation_schema.index_of("n_name").unwrap();

    let idx_o_orderkey = orders_schema.index_of("o_orderkey").unwrap();
    let idx_o_custkey = orders_schema.index_of("o_custkey").unwrap();
    let idx_o_orderdate = orders_schema.index_of("o_orderdate").unwrap();

    let idx_l_orderkey = lineitem_schema.index_of("l_orderkey").unwrap();
    let idx_l_extendedprice = lineitem_schema.index_of("l_extendedprice").unwrap();
    let idx_l_discount = lineitem_schema.index_of("l_discount").unwrap();
    let idx_l_returnflag = lineitem_schema.index_of("l_returnflag").unwrap();

    let nation_scan = LogicalPlan::Scan {
        table: "nation".to_string(),
        columns: vec![idx_n_nationkey, idx_n_name],
        table_schema: nation_schema,
    };

    let cust_scan = LogicalPlan::Scan {
        table: "customer".to_string(),
        columns: vec![
            idx_c_custkey,
            idx_c_name,
            idx_c_address,
            idx_c_nationkey,
            idx_c_phone,
            idx_c_acctbal,
            idx_c_comment,
        ],
        table_schema: cust_schema,
    };
    let cust_nation_join = LogicalPlan::Join {
        left: Box::new(cust_scan),
        right: Box::new(nation_scan),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(3), Expr::Column(0))],
        filter: None,
    };
    let cust_nation = LogicalPlan::Project {
        input: Box::new(cust_nation_join),
        exprs: vec![
            (Expr::Column(0), "c_custkey".to_string()),
            (Expr::Column(1), "c_name".to_string()),
            (Expr::Column(5), "c_acctbal".to_string()),
            (Expr::Column(8), "n_name".to_string()),
            (Expr::Column(2), "c_address".to_string()),
            (Expr::Column(4), "c_phone".to_string()),
            (Expr::Column(6), "c_comment".to_string()),
        ],
    };

    let orders_scan = LogicalPlan::Scan {
        table: "orders".to_string(),
        columns: vec![idx_o_orderkey, idx_o_custkey, idx_o_orderdate],
        table_schema: orders_schema,
    };
    let orders_filter = LogicalPlan::Filter {
        input: Box::new(orders_scan),
        predicate: Expr::Binary {
            op: BinOp::And,
            left: Box::new(Expr::Binary {
                op: BinOp::GtEq,
                left: Box::new(Expr::Column(2)),
                right: Box::new(Expr::Literal(Scalar::Date32(date_to_days("1993-10-01")))),
            }),
            right: Box::new(Expr::Binary {
                op: BinOp::Lt,
                left: Box::new(Expr::Column(2)),
                right: Box::new(Expr::Literal(Scalar::Date32(date_to_days("1994-01-01")))),
            }),
        },
    };
    let orders_project = LogicalPlan::Project {
        input: Box::new(orders_filter),
        exprs: vec![
            (Expr::Column(0), "o_orderkey".to_string()),
            (Expr::Column(1), "o_custkey".to_string()),
        ],
    };

    let lineitem_scan = LogicalPlan::Scan {
        table: "lineitem".to_string(),
        columns: vec![
            idx_l_orderkey,
            idx_l_extendedprice,
            idx_l_discount,
            idx_l_returnflag,
        ],
        table_schema: lineitem_schema,
    };
    let lineitem_filter = LogicalPlan::Filter {
        input: Box::new(lineitem_scan),
        predicate: Expr::Binary {
            op: BinOp::Eq,
            left: Box::new(Expr::Column(3)),
            right: Box::new(Expr::Literal(Scalar::Utf8("R".to_string()))),
        },
    };
    let disc_price = Expr::Binary {
        op: BinOp::Mul,
        left: Box::new(Expr::Column(1)),
        right: Box::new(Expr::Binary {
            op: BinOp::Sub,
            left: Box::new(Expr::Literal(Scalar::Int32(1))),
            right: Box::new(Expr::Column(2)),
        }),
    };
    let lineitem_project = LogicalPlan::Project {
        input: Box::new(lineitem_filter),
        exprs: vec![
            (Expr::Column(0), "l_orderkey".to_string()),
            (disc_price, "disc_price".to_string()),
        ],
    };

    let line_orders_join = LogicalPlan::Join {
        left: Box::new(lineitem_project),
        right: Box::new(orders_project),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };
    let line_orders = LogicalPlan::Project {
        input: Box::new(line_orders_join),
        exprs: vec![
            (Expr::Column(3), "o_custkey".to_string()),
            (Expr::Column(1), "disc_price".to_string()),
        ],
    };

    let join_all = LogicalPlan::Join {
        left: Box::new(line_orders),
        right: Box::new(cust_nation),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };

    let agg = LogicalPlan::Aggregate {
        input: Box::new(join_all),
        group_by: vec![
            (Expr::Column(2), "c_custkey".to_string()),
            (Expr::Column(3), "c_name".to_string()),
            (Expr::Column(4), "c_acctbal".to_string()),
            (Expr::Column(7), "c_phone".to_string()),
            (Expr::Column(5), "n_name".to_string()),
            (Expr::Column(6), "c_address".to_string()),
            (Expr::Column(8), "c_comment".to_string()),
        ],
        aggs: vec![(
            AggExpr {
                func: AggFunc::Sum,
                arg: Some(Expr::Column(1)),
            },
            "revenue".to_string(),
        )],
    };

    let project_out = LogicalPlan::Project {
        input: Box::new(agg),
        exprs: vec![
            (Expr::Column(0), "c_custkey".to_string()),
            (Expr::Column(1), "c_name".to_string()),
            (Expr::Column(7), "revenue".to_string()),
            (Expr::Column(2), "c_acctbal".to_string()),
            (Expr::Column(4), "n_name".to_string()),
            (Expr::Column(5), "c_address".to_string()),
            (Expr::Column(3), "c_phone".to_string()),
            (Expr::Column(6), "c_comment".to_string()),
        ],
    };

    let sort = LogicalPlan::Sort {
        input: Box::new(project_out),
        keys: vec![SortKey {
            expr: Expr::Column(2),
            asc: false,
            nulls_first: false,
        }],
    };

    LogicalPlan::Limit {
        input: Box::new(sort),
        n: 20,
    }
}

pub fn plan_q12(catalog: &Catalog) -> LogicalPlan {
    let orders_schema = catalog.table_schema("orders").unwrap();
    let lineitem_schema = catalog.table_schema("lineitem").unwrap();

    let idx_o_orderkey = orders_schema.index_of("o_orderkey").unwrap();
    let idx_o_orderpriority = orders_schema.index_of("o_orderpriority").unwrap();

    let idx_l_orderkey = lineitem_schema.index_of("l_orderkey").unwrap();
    let idx_l_shipmode = lineitem_schema.index_of("l_shipmode").unwrap();
    let idx_l_commitdate = lineitem_schema.index_of("l_commitdate").unwrap();
    let idx_l_receiptdate = lineitem_schema.index_of("l_receiptdate").unwrap();
    let idx_l_shipdate = lineitem_schema.index_of("l_shipdate").unwrap();

    let orders_scan = LogicalPlan::Scan {
        table: "orders".to_string(),
        columns: vec![idx_o_orderkey, idx_o_orderpriority],
        table_schema: orders_schema,
    };

    let lineitem_scan = LogicalPlan::Scan {
        table: "lineitem".to_string(),
        columns: vec![
            idx_l_orderkey,
            idx_l_shipmode,
            idx_l_commitdate,
            idx_l_receiptdate,
            idx_l_shipdate,
        ],
        table_schema: lineitem_schema,
    };

    let pred_mode = Expr::InList {
        expr: Box::new(Expr::Column(1)),
        list: vec![
            Scalar::Utf8("MAIL".to_string()),
            Scalar::Utf8("SHIP".to_string()),
        ],
        negated: false,
    };
    let pred_commit = Expr::Binary {
        op: BinOp::Lt,
        left: Box::new(Expr::Column(2)),
        right: Box::new(Expr::Column(3)),
    };
    let pred_ship = Expr::Binary {
        op: BinOp::Lt,
        left: Box::new(Expr::Column(4)),
        right: Box::new(Expr::Column(2)),
    };
    let pred_rec1 = Expr::Binary {
        op: BinOp::GtEq,
        left: Box::new(Expr::Column(3)),
        right: Box::new(Expr::Literal(Scalar::Date32(date_to_days("1994-01-01")))),
    };
    let pred_rec2 = Expr::Binary {
        op: BinOp::Lt,
        left: Box::new(Expr::Column(3)),
        right: Box::new(Expr::Literal(Scalar::Date32(date_to_days("1995-01-01")))),
    };

    let lineitem_pred = Expr::Binary {
        op: BinOp::And,
        left: Box::new(Expr::Binary {
            op: BinOp::And,
            left: Box::new(pred_mode),
            right: Box::new(pred_commit),
        }),
        right: Box::new(Expr::Binary {
            op: BinOp::And,
            left: Box::new(pred_ship),
            right: Box::new(Expr::Binary {
                op: BinOp::And,
                left: Box::new(pred_rec1),
                right: Box::new(pred_rec2),
            }),
        }),
    };

    let lineitem_filter = LogicalPlan::Filter {
        input: Box::new(lineitem_scan),
        predicate: lineitem_pred,
    };
    let lineitem_project = LogicalPlan::Project {
        input: Box::new(lineitem_filter),
        exprs: vec![
            (Expr::Column(0), "l_orderkey".to_string()),
            (Expr::Column(1), "l_shipmode".to_string()),
        ],
    };

    let join = LogicalPlan::Join {
        left: Box::new(lineitem_project),
        right: Box::new(orders_scan),
        join_type: JoinType::Inner,
        on: vec![(Expr::Column(0), Expr::Column(0))],
        filter: None,
    };

    let is_urgent = Expr::Binary {
        op: BinOp::Eq,
        left: Box::new(Expr::Column(3)),
        right: Box::new(Expr::Literal(Scalar::Utf8("1-URGENT".to_string()))),
    };
    let is_high = Expr::Binary {
        op: BinOp::Eq,
        left: Box::new(Expr::Column(3)),
        right: Box::new(Expr::Literal(Scalar::Utf8("2-HIGH".to_string()))),
    };
    let high_cond = Expr::Binary {
        op: BinOp::Or,
        left: Box::new(is_urgent),
        right: Box::new(is_high),
    };
    let high_case = Expr::Case {
        whens: vec![(high_cond.clone(), Expr::Literal(Scalar::Int64(1)))],
        else_expr: Some(Box::new(Expr::Literal(Scalar::Int64(0)))),
    };
    let low_cond = Expr::Not(Box::new(high_cond));
    let low_case = Expr::Case {
        whens: vec![(low_cond, Expr::Literal(Scalar::Int64(1)))],
        else_expr: Some(Box::new(Expr::Literal(Scalar::Int64(0)))),
    };

    let project_cases = LogicalPlan::Project {
        input: Box::new(join),
        exprs: vec![
            (Expr::Column(1), "l_shipmode".to_string()),
            (high_case, "high_case".to_string()),
            (low_case, "low_case".to_string()),
        ],
    };

    let agg = LogicalPlan::Aggregate {
        input: Box::new(project_cases),
        group_by: vec![(Expr::Column(0), "l_shipmode".to_string())],
        aggs: vec![
            (
                AggExpr {
                    func: AggFunc::Sum,
                    arg: Some(Expr::Column(1)),
                },
                "high_line_count".to_string(),
            ),
            (
                AggExpr {
                    func: AggFunc::Sum,
                    arg: Some(Expr::Column(2)),
                },
                "low_line_count".to_string(),
            ),
        ],
    };

    LogicalPlan::Sort {
        input: Box::new(agg),
        keys: vec![SortKey {
            expr: Expr::Column(0),
            asc: true,
            nulls_first: false,
        }],
    }
}

pub fn plan_q13(catalog: &Catalog) -> LogicalPlan {
    let cust_schema = catalog.table_schema("customer").unwrap();
    let orders_schema = catalog.table_schema("orders").unwrap();

    let idx_c_custkey = cust_schema.index_of("c_custkey").unwrap();

    let idx_o_orderkey = orders_schema.index_of("o_orderkey").unwrap();
    let idx_o_custkey = orders_schema.index_of("o_custkey").unwrap();
    let idx_o_comment = orders_schema.index_of("o_comment").unwrap();

    let cust_scan = LogicalPlan::Scan {
        table: "customer".to_string(),
        columns: vec![idx_c_custkey],
        table_schema: cust_schema,
    };

    let orders_scan = LogicalPlan::Scan {
        table: "orders".to_string(),
        columns: vec![idx_o_orderkey, idx_o_custkey, idx_o_comment],
        table_schema: orders_schema,
    };
    let orders_filter = LogicalPlan::Filter {
        input: Box::new(orders_scan),
        predicate: Expr::Like {
            expr: Box::new(Expr::Column(2)),
            pattern: "%special%requests%".to_string(),
            negated: true,
        },
    };
    let orders_project = LogicalPlan::Project {
        input: Box::new(orders_filter),
        exprs: vec![
            (Expr::Column(0), "o_orderkey".to_string()),
            (Expr::Column(1), "o_custkey".to_string()),
        ],
    };

    let left_join = LogicalPlan::Join {
        left: Box::new(cust_scan),
        right: Box::new(orders_project),
        join_type: JoinType::Left,
        on: vec![(Expr::Column(0), Expr::Column(1))],
        filter: None,
    };

    let inner_agg = LogicalPlan::Aggregate {
        input: Box::new(left_join),
        group_by: vec![(Expr::Column(0), "c_custkey".to_string())],
        aggs: vec![(
            AggExpr {
                func: AggFunc::Count,
                arg: Some(Expr::Column(1)),
            },
            "c_count".to_string(),
        )],
    };

    let outer_agg = LogicalPlan::Aggregate {
        input: Box::new(inner_agg),
        group_by: vec![(Expr::Column(1), "c_count".to_string())],
        aggs: vec![(
            AggExpr {
                func: AggFunc::CountStar,
                arg: None,
            },
            "custdist".to_string(),
        )],
    };

    LogicalPlan::Sort {
        input: Box::new(outer_agg),
        keys: vec![
            SortKey {
                expr: Expr::Column(1),
                asc: false,
                nulls_first: false,
            },
            SortKey {
                expr: Expr::Column(0),
                asc: false,
                nulls_first: false,
            },
        ],
    }
}

#[test]
fn test_tpch_q10_q12_q13() {
    let tpch_dir = match std::env::var("MORSEL_TPCH_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => {
            eprintln!("MORSEL_TPCH_DIR is not set; skipping tpch_hand test");
            return;
        }
    };
    let catalog = load_parquet_dir(&tpch_dir, 16).expect("load parquet dir");

    let p10 = plan_q10(&catalog);
    check_query(10, &p10, &catalog);

    let p12 = plan_q12(&catalog);
    check_query(12, &p12, &catalog);

    let p13 = plan_q13(&catalog);
    check_query(13, &p13, &catalog);
}
