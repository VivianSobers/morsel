use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use morsel_core::{DataType, Field, Schema};
use morsel_plan::{LogicalPlan, SchemaProvider};
use morsel_sql::{optimize, plan_sql};

struct MockCatalog {
    schemas: HashMap<String, Schema>,
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

fn tpch_schemas() -> MockCatalog {
    let mut schemas = HashMap::new();
    schemas.insert(
        "region".into(),
        Schema::new(vec![
            Field {
                name: "r_regionkey".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "r_name".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "r_comment".into(),
                dtype: DataType::Utf8,
                nullable: true,
            },
        ]),
    );
    schemas.insert(
        "nation".into(),
        Schema::new(vec![
            Field {
                name: "n_nationkey".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "n_name".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "n_regionkey".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "n_comment".into(),
                dtype: DataType::Utf8,
                nullable: true,
            },
        ]),
    );
    schemas.insert(
        "supplier".into(),
        Schema::new(vec![
            Field {
                name: "s_suppkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "s_name".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "s_address".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "s_nationkey".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "s_phone".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "s_acctbal".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "s_comment".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ]),
    );
    schemas.insert(
        "customer".into(),
        Schema::new(vec![
            Field {
                name: "c_custkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "c_name".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "c_address".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "c_nationkey".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "c_phone".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "c_acctbal".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "c_mktsegment".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "c_comment".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ]),
    );
    schemas.insert(
        "part".into(),
        Schema::new(vec![
            Field {
                name: "p_partkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "p_name".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "p_mfgr".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "p_brand".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "p_type".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "p_size".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "p_container".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "p_retailprice".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "p_comment".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ]),
    );
    schemas.insert(
        "partsupp".into(),
        Schema::new(vec![
            Field {
                name: "ps_partkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "ps_suppkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "ps_availqty".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "ps_supplycost".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "ps_comment".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ]),
    );
    schemas.insert(
        "orders".into(),
        Schema::new(vec![
            Field {
                name: "o_orderkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "o_custkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "o_orderstatus".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "o_totalprice".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "o_orderdate".into(),
                dtype: DataType::Date32,
                nullable: false,
            },
            Field {
                name: "o_orderpriority".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "o_clerk".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "o_shippriority".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "o_comment".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ]),
    );
    schemas.insert(
        "lineitem".into(),
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
                dtype: DataType::Int64,
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
    MockCatalog { schemas }
}

fn tpch_stats() -> HashMap<String, usize> {
    let mut stats = HashMap::new();
    stats.insert("region".into(), 5);
    stats.insert("nation".into(), 25);
    stats.insert("supplier".into(), 10000);
    stats.insert("customer".into(), 150000);
    stats.insert("part".into(), 200000);
    stats.insert("partsupp".into(), 800000);
    stats.insert("orders".into(), 1500000);
    stats.insert("lineitem".into(), 6001215);
    stats
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn has_cross_join(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::CrossJoin { .. } => true,
        LogicalPlan::Scan { .. } => false,
        LogicalPlan::Filter { input, .. } => has_cross_join(input),
        LogicalPlan::Project { input, .. } => has_cross_join(input),
        LogicalPlan::Aggregate { input, .. } => has_cross_join(input),
        LogicalPlan::Sort { input, .. } => has_cross_join(input),
        LogicalPlan::Limit { input, .. } => has_cross_join(input),
        LogicalPlan::Join { left, right, .. } => has_cross_join(left) || has_cross_join(right),
    }
}

fn scans_have_filter_ancestor(plan: &LogicalPlan) -> bool {
    // In Q3, all base scans should have a Filter directly on top of them
    // (pushed down through joins)
    fn check(plan: &LogicalPlan, direct_filter: bool) -> bool {
        match plan {
            LogicalPlan::Scan { .. } => direct_filter,
            LogicalPlan::Filter { input, .. } => check(input, true),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. } => check(input, false),
            LogicalPlan::Join { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
                check(left, false) && check(right, false)
            }
        }
    }
    check(plan, false)
}

#[test]
fn all_22_queries_plan() {
    let catalog = tpch_schemas();
    let stats = tpch_stats();
    let root = repo_root();

    for q in 1..=22 {
        let sql_path = root.join("queries/tpch").join(format!("q{:02}.sql", q));
        let sql = fs::read_to_string(&sql_path)
            .unwrap_or_else(|e| panic!("failed to read {:?}: {}", sql_path, e));

        let plan =
            plan_sql(&sql, &catalog).unwrap_or_else(|e| panic!("failed to plan Q{:02}: {}", q, e));

        let optimized = optimize(plan, &stats)
            .unwrap_or_else(|e| panic!("failed to optimize Q{:02}: {}", q, e));

        let plan_schema = optimized
            .schema()
            .unwrap_or_else(|e| panic!("failed to get schema for Q{:02}: {}", q, e));

        let ref_path = root.join("bench/refs/sf1").join(format!("q{:02}.csv", q));
        let ref_content = fs::read_to_string(&ref_path)
            .unwrap_or_else(|e| panic!("failed to read {:?}: {}", ref_path, e));
        let header = ref_content.lines().next().expect("ref csv header");
        let expected_col_count = header.split(',').count();

        assert_eq!(
            plan_schema.fields.len(),
            expected_col_count,
            "Q{:02} col count mismatch: plan has {}, ref has {}",
            q,
            plan_schema.fields.len(),
            expected_col_count
        );
    }
}

#[test]
fn no_cross_join_left_in_q5_q7_q8_q9() {
    let catalog = tpch_schemas();
    let stats = tpch_stats();
    let root = repo_root();

    for q in [5, 7, 8, 9] {
        let sql_path = root.join("queries/tpch").join(format!("q{:02}.sql", q));
        let sql = fs::read_to_string(&sql_path).unwrap();
        let plan = plan_sql(&sql, &catalog).unwrap();
        let optimized = optimize(plan, &stats).unwrap();
        assert!(
            !has_cross_join(&optimized),
            "Q{:02} contains CrossJoin after optimization: {:#?}",
            q,
            optimized
        );
    }
}

#[test]
fn filters_pushed_to_scans_in_q3() {
    let catalog = tpch_schemas();
    let stats = tpch_stats();
    let root = repo_root();

    let sql_path = root.join("queries/tpch/q03.sql");
    let sql = fs::read_to_string(&sql_path).unwrap();
    let plan = plan_sql(&sql, &catalog).unwrap();
    let optimized = optimize(plan, &stats).unwrap();

    assert!(
        scans_have_filter_ancestor(&optimized),
        "Q03 scans do not all have direct filter ancestors: {:#?}",
        optimized
    );
}
